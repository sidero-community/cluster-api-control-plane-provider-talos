// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// --- fixtures --------------------------------------------------------------

// newRemediationTCP returns an initialized TalosControlPlane with the given number of replicas.
func newRemediationTCP(replicas int32) *controlplanev1.TalosControlPlane {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = new(replicas)
	tcp.Status.Initialization.ControlPlaneInitialized = new(true)

	return tcp
}

// newRMMachine returns a Ready control plane machine controlled by tcp that passes its health check, created at the
// given time. It carries the Machine finalizer, so that deleting it leaves it around with a deletion timestamp, as the
// core Machine controller does.
func newRMMachine(tcp *controlplanev1.TalosControlPlane, name string, created time.Time, mutators ...func(*clusterv1.Machine)) *clusterv1.Machine {
	base := func(m *clusterv1.Machine) {
		m.CreationTimestamp = metav1.NewTime(created)
		m.Finalizers = []string{clusterv1.MachineFinalizer}
		m.Status.Conditions = []metav1.Condition{
			{
				Type:               clusterv1.ReadyCondition,
				Status:             metav1.ConditionTrue,
				Reason:             clusterv1.ReadyReason,
				LastTransitionTime: metav1.NewTime(created),
			},
			{
				Type:               clusterv1.MachineHealthCheckSucceededCondition,
				Status:             metav1.ConditionTrue,
				Reason:             clusterv1.MachineHealthCheckSucceededReason,
				LastTransitionTime: metav1.NewTime(created),
			},
		}
	}

	return newPTMachine(tcp, name, append([]func(*clusterv1.Machine){ptHooked, base}, mutators...)...)
}

// rmFlagged marks the machine the way a MachineHealthCheck does when it leaves the remediation to the owner.
func rmFlagged(m *clusterv1.Machine) {
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.MachineHealthCheckSucceededCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.MachineHealthCheckNodeDeletedReason,
	})
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.MachineOwnerRemediatedCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.MachineOwnerRemediatedWaitingForRemediationReason,
	})
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.ReadyCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.NotReadyReason,
	})
}

func rmAnnotation(key, value string) func(*clusterv1.Machine) {
	return func(m *clusterv1.Machine) {
		if m.Annotations == nil {
			m.Annotations = map[string]string{}
		}

		m.Annotations[key] = value
	}
}

// recordedEtcd returns what the etcd health check records on a control plane whose etcd members are the named
// machines: a healthy etcd service that listed every member, except on the machines named unhealthy.
func recordedEtcd(names []string, unhealthy ...string) map[string]etcdMachineStatus {
	statuses := make(map[string]etcdMachineStatus, len(names))

	for _, name := range names {
		if slices.Contains(unhealthy, name) {
			statuses[name] = etcdMachineStatus{}

			continue
		}

		statuses[name] = etcdMachineStatus{healthy: true, members: etcdMembersFor(names...)}
	}

	return statuses
}

func remediationValue(t *testing.T, machine string, at time.Time, retryCount int) string {
	t.Helper()

	value, err := (&remediationData{Machine: machine, Timestamp: metav1.NewTime(at), RetryCount: retryCount}).marshal()
	require.NoError(t, err)

	return value
}

// current lists the machines as stored by the API server.
func (f *preTerminateFixture) current(t *testing.T) []clusterv1.Machine {
	t.Helper()

	var list clusterv1.MachineList
	require.NoError(t, f.r.List(context.Background(), &list))

	return list.Items
}

func (f *preTerminateFixture) remediate(t *testing.T, statuses map[string]etcdMachineStatus) (ctrl.Result, bool, error) {
	t.Helper()

	return f.r.reconcileUnhealthyMachines(context.Background(), f.cluster, f.tcp, f.current(t), statuses)
}

// storedTCP returns the TalosControlPlane as stored by the API server, without the changes that are only persisted
// by the deferred patch at the end of a reconcile.
func (f *preTerminateFixture) storedTCP(t *testing.T) *controlplanev1.TalosControlPlane {
	t.Helper()

	tcp := &controlplanev1.TalosControlPlane{}
	require.NoError(t, f.r.Get(context.Background(), client.ObjectKeyFromObject(f.tcp), tcp))

	return tcp
}

func (f *preTerminateFixture) deletedMachines(t *testing.T) []string {
	t.Helper()

	var names []string

	for _, m := range f.current(t) {
		if !m.DeletionTimestamp.IsZero() {
			names = append(names, m.Name)
		}
	}

	return names
}

// expectRemediated checks that the machine was deleted, and nothing else done to it, and that the remediation is
// recorded on the TalosControlPlane, in memory and on the API server.
func (f *preTerminateFixture) expectRemediated(t *testing.T, name string) *remediationData {
	t.Helper()

	m := f.get(t, name)
	assert.False(t, m.DeletionTimestamp.IsZero(), "machine %s should be deleted", name)
	assert.NotContains(t, m.Annotations, etcdLeavingAnnotation, "the pre-terminate hook removes the etcd member, not remediation")
	assert.Contains(t, m.Annotations, PreTerminateHookCleanupAnnotation, "the pre-terminate hook must stay to remove the etcd member")

	condition := conditions.Get(m, clusterv1.MachineOwnerRemediatedCondition)
	require.NotNil(t, condition)
	assert.Equal(t, remediationMachineDeletingReason, condition.Reason)

	require.Contains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	assert.Equal(t, f.tcp.Annotations[remediationInProgressAnnotation], f.storedTCP(t).Annotations[remediationInProgressAnnotation])

	data, err := remediationDataFromAnnotation(f.tcp.Annotations[remediationInProgressAnnotation])
	require.NoError(t, err)
	assert.Equal(t, name, data.Machine)

	return data
}

// expectNotRemediated checks that the machine is not deleted and that its OwnerRemediated condition says why.
func (f *preTerminateFixture) expectNotRemediated(t *testing.T, name, reason, messageSubstring string) {
	t.Helper()

	m := f.get(t, name)
	assert.True(t, m.DeletionTimestamp.IsZero(), "machine %s should not be deleted", name)

	condition := conditions.Get(m, clusterv1.MachineOwnerRemediatedCondition)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, reason, condition.Reason)
	assert.Contains(t, condition.Message, messageSubstring)
}

// --- remediation is a deletion ---------------------------------------------

// Remediating a machine deletes it, and nothing else: no etcd call, no leaving mark, the hook left in place. The
// pre-terminate hook removes the etcd member once the machine is drained, as for any other deletion.
func TestRemediation_DeletesTheFlaggedMachineOnly(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	res, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
	require.NoError(t, err)
	assert.True(t, remediating)
	assert.Equal(t, remediationRequeueAfter, res.RequeueAfter)

	data := f.expectRemediated(t, "m1")
	assert.Zero(t, data.RetryCount)

	assert.Equal(t, []string{"m1"}, f.deletedMachines(t))
	assert.Empty(t, f.dialer.dialed, "remediation judges quorum from the recorded statuses and leaves etcd to the hook")
	assert.Contains(t, strings.Join(f.events(), "\n"), remediatingMachineEvent)
}

// The quorum pre-check reads the statuses the etcd health check recorded in the same reconcile, and never asks etcd
// again: the fake dialer has no client at all, so any Talos call would show up in dialed.
func TestRemediation_QuorumPreCheckReadsTheRecordedEtcdStatuses(t *testing.T) {
	healthy := func(members ...string) etcdMachineStatus {
		return etcdMachineStatus{healthy: true, members: etcdMembersFor(members...)}
	}

	for _, tc := range []struct {
		name     string
		machines []string
		statuses map[string]etcdMachineStatus
		reason   string
		message  string
	}{
		{
			name:     "remediates when the other members were recorded healthy",
			machines: []string{"m1", "m2", "m3"},
			statuses: map[string]etcdMachineStatus{"m1": {}, "m2": healthy("m1", "m2", "m3"), "m3": healthy("m1", "m2", "m3")},
		},
		{
			name:     "refuses when another member was recorded unhealthy",
			machines: []string{"m1", "m2", "m3"},
			statuses: map[string]etcdMachineStatus{"m1": {}, "m2": healthy("m1", "m2", "m3"), "m3": {}},
			reason:   cannotBeRemediatedReason,
			message:  "etcd quorum loss: 1 of 3 etcd members are healthy",
		},
		{
			name:     "refuses when another machine has no recorded status",
			machines: []string{"m1", "m2", "m3"},
			statuses: map[string]etcdMachineStatus{"m1": {}, "m2": healthy("m1", "m2", "m3")},
			reason:   cannotBeRemediatedReason,
			message:  "etcd quorum loss",
		},
		{
			name:     "refuses on a 2 member cluster whose other member is unhealthy",
			machines: []string{"m1", "m2"},
			statuses: map[string]etcdMachineStatus{"m1": healthy("m1", "m2"), "m2": {}},
			reason:   cannotBeRemediatedReason,
			message:  "etcd quorum loss",
		},
		{
			// The member of the failed machine can only be removed with the votes of both members.
			name:     "refuses on a 2 member cluster whose remediated member is unhealthy",
			machines: []string{"m1", "m2"},
			statuses: map[string]etcdMachineStatus{"m1": {}, "m2": healthy("m1", "m2")},
			reason:   cannotBeRemediatedReason,
			message:  "removing a member needs 2",
		},
		{
			name:     "defers when no member list was recorded",
			machines: []string{"m1", "m2", "m3"},
			statuses: map[string]etcdMachineStatus{
				"m1": {},
				"m2": {healthy: true, listErr: errors.New("connection refused")},
				"m3": {healthy: true, listErr: errors.New("connection refused")},
			},
			reason:  remediationDeferredReason,
			message: "Failed to check etcd cluster health before remediation",
		},
		{
			// etcdHealthcheck records nothing when no machine is left to inspect.
			name:     "defers when nothing was recorded",
			machines: []string{"m1", "m2", "m3"},
			reason:   remediationDeferredReason,
			message:  "Failed to check etcd cluster health before remediation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			tcp := newRemediationTCP(int32(len(tc.machines)))

			machines := make([]*clusterv1.Machine, 0, len(tc.machines))
			for i, name := range tc.machines {
				var mutators []func(*clusterv1.Machine)
				if i == 0 {
					mutators = append(mutators, rmFlagged)
				}

				machines = append(machines, newRMMachine(tcp, name, now.Add(time.Duration(i-10)*time.Hour), mutators...))
			}

			f := newPreTerminateFixtureWithTCP(t, tcp, machines...)

			_, remediating, err := f.remediate(t, tc.statuses)
			require.NoError(t, err)

			if tc.reason == "" {
				assert.True(t, remediating)
				f.expectRemediated(t, "m1")
			} else {
				assert.False(t, remediating)
				f.expectNotRemediated(t, "m1", tc.reason, tc.message)
				assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
				assert.Empty(t, f.deletedMachines(t))
			}

			assert.Empty(t, f.dialer.dialed, "the recorded statuses must be used, etcd is not asked again")
		})
	}
}

func TestRemediation_Preconditions(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name     string
		tcp      func() *controlplanev1.TalosControlPlane
		machines func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine
		deleting string
		reason   string
		message  string
	}{
		{
			name: "another machine is still provisioning",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
					newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
					newRMMachine(tcp, "m3", now.Add(-time.Hour), ptNoNodeRef),
				}
			},
			reason:  remediationDeferredReason,
			message: "provisioning of control plane machine m3",
		},
		{
			name:     "another machine is being deleted",
			deleting: "m2",
			reason:   remediationDeferredReason,
			message:  "deletion of control plane machine m2",
		},
		{
			name: "single machine",
			tcp: func() *controlplanev1.TalosControlPlane {
				return newRemediationTCP(1)
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged)}
			},
			reason:  cannotBeRemediatedReason,
			message: "less or equal to 1",
		},
		{
			name: "another machine is being updated in place",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
					newRMMachine(tcp, "m2", now.Add(-2*time.Hour), rmAnnotation(clusterv1.UpdateInProgressAnnotation, "")),
					newRMMachine(tcp, "m3", now.Add(-time.Hour)),
				}
			},
			reason:  remediationDeferredReason,
			message: "in-place update of control plane machine m2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcp := newRemediationTCP(3)
			if tc.tcp != nil {
				tcp = tc.tcp()
			}

			machines := []*clusterv1.Machine{
				newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
				newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
				newRMMachine(tcp, "m3", now.Add(-time.Hour)),
			}
			if tc.machines != nil {
				machines = tc.machines(tcp)
			}

			f := newPreTerminateFixtureWithTCP(t, tcp, machines...)

			if tc.deleting != "" {
				require.NoError(t, f.r.Delete(context.Background(), f.get(t, tc.deleting)))
			}

			_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
			require.NoError(t, err)
			assert.False(t, remediating)

			f.expectNotRemediated(t, "m1", tc.reason, tc.message)
			assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
		})
	}
}

// A flagged machine that failed to provision doesn't defer the remediation of another flagged machine.
func TestRemediation_AFlaggedProvisioningMachineDoesNotDefer(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged, rmAnnotation(clusterv1.RemediateMachineAnnotation, "")),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour), rmFlagged, ptNoNodeRef))

	// m1 was flagged for something other than etcd: with its member unhealthy, the 2 member etcd cluster would have
	// lost quorum, and removing the member could not commit.
	statuses := map[string]etcdMachineStatus{
		"m1": {healthy: true, members: etcdMembersFor("m1", "m2")},
		"m2": {healthy: true, members: etcdMembersFor("m1", "m2")},
		"m3": {},
	}

	_, remediating, err := f.remediate(t, statuses)
	require.NoError(t, err)
	assert.True(t, remediating)

	f.expectRemediated(t, "m1")
}

func TestRemediation_SkipsATargetAlreadyDeleting(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	require.NoError(t, f.r.Delete(context.Background(), f.get(t, "m1")))

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m2", "m3"}))
	require.NoError(t, err)
	assert.False(t, remediating)

	assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	assert.Equal(t, clusterv1.MachineOwnerRemediatedWaitingForRemediationReason,
		conditions.Get(f.get(t, "m1"), clusterv1.MachineOwnerRemediatedCondition).Reason)
}

func TestRemediation_OnlyRemediatesMachinesTheControlPlaneControls(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	foreign := newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged)
	foreign.OwnerReferences = nil

	f := newPreTerminateFixtureWithTCP(t, tcp,
		foreign,
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
	require.NoError(t, err)
	assert.False(t, remediating)
	assert.Empty(t, f.deletedMachines(t))
}

func TestRemediation_CleansUpStaleOwnerRemediated(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	// m1 was marked for remediation, but is healthy again.
	recovered := newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged, func(m *clusterv1.Machine) {
		conditions.Set(m, metav1.Condition{
			Type:   clusterv1.MachineHealthCheckSucceededCondition,
			Status: metav1.ConditionTrue,
			Reason: clusterv1.MachineHealthCheckSucceededReason,
		})
	})

	f := newPreTerminateFixtureWithTCP(t, tcp,
		recovered,
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}))
	require.NoError(t, err)
	assert.False(t, remediating)

	m1 := f.get(t, "m1")
	assert.False(t, conditions.Has(m1, clusterv1.MachineOwnerRemediatedCondition))
	assert.True(t, m1.DeletionTimestamp.IsZero())
}

// The remediation is recorded on the API server before the machine is deleted, so that a controller that stops right
// after the deletion doesn't lose it.
func TestRemediation_IsRecordedBeforeTheMachineIsDeleted(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	var recordedAtDelete *bool

	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			stored := &controlplanev1.TalosControlPlane{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(f.tcp), stored); err != nil {
				return err
			}

			_, recorded := stored.Annotations[remediationInProgressAnnotation]
			recordedAtDelete = &recorded

			return c.Delete(ctx, obj, opts...)
		},
	})

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
	require.NoError(t, err)
	assert.True(t, remediating)

	require.NotNil(t, recordedAtDelete, "the machine was not deleted")
	assert.True(t, *recordedAtDelete, "the remediation must be recorded before the machine is deleted")

	f.expectRemediated(t, "m1")
}

func TestRemediation_DoesNotDeleteWhenItCantBeRecorded(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*controlplanev1.TalosControlPlane); ok {
				return errors.New("injected patch failure")
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
	require.Error(t, err)
	assert.True(t, remediating, "the rest of the reconcile must not delete machines")

	f.expectNotRemediated(t, "m1", remediationInternalErrorReason, "Failed to record the remediation")
	assert.Empty(t, f.deletedMachines(t))
}

// A failed deletion leaves the remediation recorded: reconcileRemediationInProgress withdraws it once the machine is
// still not deleting after remediationStartTimeout.
func TestRemediation_KeepsTheRecordWhenTheDeletionFails(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour)))

	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errors.New("injected delete failure")
		},
	})

	_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
	require.Error(t, err)
	assert.True(t, remediating)

	f.expectNotRemediated(t, "m1", remediationInternalErrorReason, "Failed to delete the machine")
	assert.Contains(t, f.storedTCP(t).Annotations, remediationInProgressAnnotation)
}

// --- selection and quorum arithmetic ---------------------------------------

func TestMachineToBeRemediated(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	healthy := newPTMachine(tcp, "healthy", func(m *clusterv1.Machine) { m.CreationTimestamp = metav1.NewTime(now.Add(-5 * time.Hour)) })
	oldest := newRMMachine(tcp, "oldest", now.Add(-4*time.Hour), rmFlagged)
	brokenEtcd := newRMMachine(tcp, "broken-etcd", now.Add(-3*time.Hour), rmFlagged)
	provisioning := newRMMachine(tcp, "provisioning", now.Add(-time.Hour), rmFlagged, ptNoNodeRef)
	annotated := newRMMachine(tcp, "annotated", now, rmFlagged, rmAnnotation(clusterv1.RemediateMachineAnnotation, ""))

	foreign := newRMMachine(tcp, "foreign", now.Add(-6*time.Hour), rmFlagged)
	foreign.OwnerReferences = nil

	// Only broken-etcd was recorded with an unhealthy etcd service.
	statuses := map[string]etcdMachineStatus{
		"healthy":     {healthy: true},
		"oldest":      {healthy: true},
		"broken-etcd": {},
		"annotated":   {healthy: true},
	}

	pick := func(machines ...*clusterv1.Machine) string {
		m := machineToBeRemediated(tcp, machines, statuses)
		if m == nil {
			return ""
		}

		return m.Name
	}

	assert.Empty(t, pick(healthy, foreign), "only flagged machines the control plane controls are remediated")
	assert.Equal(t, "annotated", pick(healthy, oldest, brokenEtcd, provisioning, annotated))
	assert.Equal(t, "provisioning", pick(healthy, oldest, brokenEtcd, provisioning))
	assert.Equal(t, "broken-etcd", pick(healthy, oldest, brokenEtcd), "a machine whose etcd member was recorded unhealthy goes first")
	assert.Equal(t, "oldest", pick(healthy, oldest))
}

func TestMachineToBeRemediated_NeedsAFailedHealthCheck(t *testing.T) {
	tcp := newRemediationTCP(3)

	// OwnerRemediated=False alone, without a failed health check, doesn't make a Machine a remediation target.
	for _, status := range []metav1.ConditionStatus{metav1.ConditionUnknown, metav1.ConditionTrue} {
		m := newRMMachine(tcp, "m1", time.Now(), rmFlagged, func(m *clusterv1.Machine) {
			conditions.Set(m, metav1.Condition{Type: clusterv1.MachineHealthCheckSucceededCondition, Status: status, Reason: "Test"})
		})

		assert.Nil(t, machineToBeRemediated(tcp, []*clusterv1.Machine{m}, nil), "HealthCheckSucceeded=%s", status)
	}

	m := newRMMachine(tcp, "m1", time.Now(), rmFlagged)
	conditions.Delete(m, clusterv1.MachineHealthCheckSucceededCondition)

	assert.Nil(t, machineToBeRemediated(tcp, []*clusterv1.Machine{m}, nil), "HealthCheckSucceeded unset")
}

func TestCanSafelyRemoveEtcdMember(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name      string
		machines  int
		members   []*machineapi.EtcdMember
		unhealthy []int
		safe      bool
	}{
		{name: "1 member", machines: 1, safe: false},
		{name: "2 members, all healthy", machines: 2, safe: true},
		{name: "2 members, target unhealthy", machines: 2, unhealthy: []int{0}, safe: false},
		{name: "2 members, other unhealthy", machines: 2, unhealthy: []int{1}, safe: false},
		{name: "3 members, target unhealthy", machines: 3, unhealthy: []int{0}, safe: true},
		{name: "3 members, target and another unhealthy", machines: 3, unhealthy: []int{0, 2}, safe: false},
		{name: "5 members, target and another unhealthy", machines: 5, unhealthy: []int{0, 4}, safe: true},
		{name: "5 members, target and 2 others unhealthy", machines: 5, unhealthy: []int{0, 3, 4}, safe: false},
		{
			name:     "3 members, one is a learner",
			machines: 3,
			members: []*machineapi.EtcdMember{
				{Id: 1, Hostname: "m0"},
				{Id: 2, Hostname: "m1"},
				{Id: 3, Hostname: "m2", IsLearner: true},
			},
			safe: false,
		},
		{
			name:     "3 members, the target member can't be identified",
			machines: 3,
			members:  etcdMembersFor("other", "m1", "m2"),
			safe:     true,
		},
		{
			name:     "2 members, the target member can't be identified",
			machines: 2,
			members:  etcdMembersFor("other", "m1"),
			safe:     false,
		},
		{
			name:     "3 members, a member without machine",
			machines: 2,
			members:  etcdMembersFor("m0", "m1", "orphan"),
			safe:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			machines := make([]*clusterv1.Machine, 0, tc.machines)
			names := make([]string, 0, tc.machines)
			healthy := map[string]bool{}

			for i := range tc.machines {
				name := fmt.Sprintf("m%d", i)
				names = append(names, name)
				machines = append(machines, newTestMachine(name, now.Add(time.Duration(i)*time.Minute)))
				healthy[name] = true
			}

			for _, i := range tc.unhealthy {
				healthy[names[i]] = false
			}

			members := tc.members
			if members == nil {
				members = etcdMembersFor(names...)
			}

			safe, details := canSafelyRemoveEtcdMember(members, machines[0], machines, healthy)
			assert.Equal(t, tc.safe, safe, details)
		})
	}
}

// --- remediation and the rest of the reconcile -----------------------------

// etcdServiceRunning is what ServiceInfo("etcd") reports on a machine whose etcd runs and is healthy, with the events
// the etcd health check requires.
func etcdServiceRunning() []talosclient.ServiceInfo {
	return []talosclient.ServiceInfo{{Service: &machineapi.ServiceInfo{
		Id:     "etcd",
		State:  "Running",
		Health: &machineapi.ServiceHealth{Healthy: true},
		Events: &machineapi.ServiceEvents{Events: []*machineapi.ServiceEvent{{State: "Running"}}},
	}}}
}

// The statuses the etcd health check records in reconcileEtcdMembers are what remediation judges quorum by, without
// another Talos call.
func TestRemediation_UsesWhatTheEtcdHealthCheckRecorded(t *testing.T) {
	now := time.Now()
	tcp := newRemediationTCP(3)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
		newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m3", now.Add(-time.Hour), rmFlagged))

	for _, name := range []string{"m1", "m2"} {
		f.dialer.clients[name] = &fakeEtcdCalls{name: name, services: etcdServiceRunning(), members: etcdMembersFor("m1", "m2", "m3")}
	}

	// m3 is unreachable.
	f.dialer.dialErrs["m3"] = errors.New("connection refused")

	machines := &clusterv1.MachineList{Items: f.current(t)}

	_, statuses, err := f.r.reconcileEtcdMembers(context.Background(), f.cluster, f.tcp, machines)
	require.Error(t, err, "the health check fails on the unreachable machine")

	require.Len(t, statuses, 3)
	assert.True(t, statuses["m1"].healthy)
	assert.True(t, statuses["m2"].healthy)
	assert.False(t, statuses["m3"].healthy)

	dialed := len(f.dialer.dialed)

	_, err = f.r.reconcileRemediationAndMachines(context.Background(), f.cluster, f.tcp, machines, statuses)
	require.NoError(t, err)

	f.expectRemediated(t, "m3")
	assert.Len(t, f.dialer.dialed, dialed, "remediation must not inspect etcd again")
}

func TestDesiredMachineAnnotations_RecordsTheRemediationOnTheReplacement(t *testing.T) {
	tcp := newRemediationTCP(3)

	assert.NotContains(t, (&TalosControlPlaneReconciler{}).desiredMachineAnnotations(tcp), remediationForAnnotation)

	value := remediationValue(t, "m0", time.Now(), 1)
	tcp.Annotations = map[string]string{remediationInProgressAnnotation: value}

	annotations := (&TalosControlPlaneReconciler{}).desiredMachineAnnotations(tcp)
	assert.Equal(t, value, annotations[remediationForAnnotation])
	assert.Contains(t, annotations, PreTerminateHookCleanupAnnotation)
}

func TestBootCheckMachines_SkipsFlaggedMachines(t *testing.T) {
	tcp := newRemediationTCP(3)

	machines := collections.FromMachines(
		newRMMachine(tcp, "m1", time.Now(), rmFlagged),
		newRMMachine(tcp, "m2", time.Now()),
		newRMMachine(tcp, "m3", time.Now()))

	var names []string
	for _, m := range bootCheckMachines(machines) {
		names = append(names, m.Name)
	}

	assert.ElementsMatch(t, []string{"m2", "m3"}, names)
}

// checkRemediationRetryLimits ignores an unreadable remediation-for annotation rather than block remediation.
func TestCheckRemediationRetryLimits_IgnoresAnInvalidAnnotation(t *testing.T) {
	m := newTestMachine("m1", time.Now(), func(m *clusterv1.Machine) {
		m.Annotations = map[string]string{remediationForAnnotation: "not json"}
	})

	data, reason, _ := checkRemediationRetryLimits(m, remediationLimitsFor(controlplanev1.TalosControlPlaneRemediationSpec{}), time.Now(), logr.Discard())
	assert.Empty(t, reason)
	assert.Zero(t, data.RetryCount)
}

// Before the control plane is initialized, the checks that protect a running cluster can't run: a machine is
// replaced when nothing was bootstrapped yet, or when it never joined and the bootstrapped etcd is known to live on
// another machine; the bootstrapped machine itself needs an operator.
func TestRemediation_BeforeInitialization(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name         string
		bootstrapped bool
		replicas     int32
		targetNode   bool
		etcdData     map[string]bool
		etcdDataErr  error
		reason       string
		message      string
	}{
		{
			name:     "replaces a machine when nothing was bootstrapped",
			replicas: 3,
		},
		{
			name:     "replaces a single machine when nothing was bootstrapped",
			replicas: 1,
		},
		{
			name:         "defers for a machine with a node once bootstrapped",
			bootstrapped: true,
			replicas:     3,
			targetNode:   true,
			reason:       remediationDeferredReason,
			message:      "waiting for the control plane to be initialized",
		},
		{
			name:         "refuses a single bootstrapped machine",
			bootstrapped: true,
			replicas:     1,
			reason:       cannotBeRemediatedReason,
			message:      "less or equal to 1",
		},
		{
			name:         "replaces a machine without a node when another machine holds etcd data",
			bootstrapped: true,
			replicas:     3,
			etcdData:     map[string]bool{"m2": false, "m3": true},
		},
		{
			name:         "refuses when no other machine holds etcd data",
			bootstrapped: true,
			replicas:     3,
			etcdData:     map[string]bool{"m2": false, "m3": false},
			reason:       cannotBeRemediatedReason,
			message:      "no other control plane machine is known to hold the bootstrapped etcd data",
		},
		{
			name:         "refuses when the other machines can't be checked",
			bootstrapped: true,
			replicas:     3,
			etcdDataErr:  errors.New("connection refused"),
			reason:       cannotBeRemediatedReason,
			message:      "bootstrapped etcd data: [machine \"m2\": connection refused, machine \"m3\": connection refused]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcp := newRemediationTCP(tc.replicas)
			tcp.Status.Initialization.ControlPlaneInitialized = nil
			tcp.Status.Bootstrapped = tc.bootstrapped

			// Before initialization no machine has a node, unless the test says so for the target.
			machines := []*clusterv1.Machine{newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged, ptNoNodeRef)}
			if tc.targetNode {
				machines[0].Status.NodeRef = clusterv1.MachineNodeReference{Name: "m1"}
			}

			for i := int32(2); i <= tc.replicas; i++ {
				machines = append(machines, newRMMachine(tcp, fmt.Sprintf("m%d", i), now.Add(-time.Duration(i)*time.Hour), ptNoNodeRef))
			}

			f := newPreTerminateFixtureWithTCP(t, tcp, machines...)

			var checked []string

			f.r.etcdDataChecker = func(_ context.Context, _ *controlplanev1.TalosControlPlane, m clusterv1.Machine) (bool, error) {
				checked = append(checked, m.Name)

				return tc.etcdData[m.Name], tc.etcdDataErr
			}

			_, remediating, err := f.remediate(t, nil)
			require.NoError(t, err)

			if tc.reason == "" {
				assert.True(t, remediating)
				f.expectRemediated(t, "m1")
			} else {
				assert.False(t, remediating)
				f.expectNotRemediated(t, "m1", tc.reason, tc.message)
				assert.Empty(t, f.deletedMachines(t))
			}

			assert.NotContains(t, checked, "m1", "the target is never asked whether it holds etcd data")

			if tc.etcdData == nil && tc.etcdDataErr == nil {
				assert.Empty(t, checked, "etcd data is only looked for once a machine was bootstrapped")
			}

			assert.Empty(t, f.dialer.dialed, "etcd is never inspected before the control plane is initialized")
		})
	}
}

// A version upgrade about to be propagated from the cluster topology rolls the machines out anyway: remediation
// waits for it, unless the TalosControlPlane is already at the version of the current upgrade step.
func TestRemediation_DefersDuringTopologyUpgrade(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name            string
		topologyVersion string
		upgradeStep     string
		deferred        bool
	}{
		{name: "topology at the same version", topologyVersion: "v1.34.0"},
		{name: "topology at a newer version", topologyVersion: "v1.35.0", deferred: true},
		{name: "upgrade step at the control plane version", topologyVersion: "v1.36.0", upgradeStep: "v1.34.0"},
		{name: "upgrade step at another version", topologyVersion: "v1.36.0", upgradeStep: "v1.35.0", deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcp := newRemediationTCP(3)

			f := newPreTerminateFixtureWithTCP(t, tcp,
				newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
				newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
				newRMMachine(tcp, "m3", now.Add(-time.Hour)))

			f.cluster.Spec.Topology = clusterv1.Topology{
				ClassRef: clusterv1.ClusterClassRef{Name: "class"},
				Version:  tc.topologyVersion,
			}

			if tc.upgradeStep != "" {
				f.cluster.Annotations = map[string]string{clusterv1.ClusterTopologyUpgradeStepAnnotation: tc.upgradeStep}
			}

			_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m1"))
			require.NoError(t, err)

			if tc.deferred {
				assert.False(t, remediating)
				f.expectNotRemediated(t, "m1", remediationDeferredReason, "waiting for a version upgrade")

				return
			}

			assert.True(t, remediating)
			f.expectRemediated(t, "m1")
		})
	}
}

func TestRemediation_RetryLimits(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	defaults := remediationLimitsFor(controlplanev1.TalosControlPlaneRemediationSpec{})

	for _, tc := range []struct {
		name           string
		spec           controlplanev1.TalosControlPlaneRemediationSpec
		lastRemediated time.Duration
		retryCount     int
		reason         string
		message        string
		wantRetryCount int
	}{
		{
			name:           "waits for the retry period",
			lastRemediated: defaults.retryPeriod / 2,
			reason:         remediationDeferredReason,
			message:        "already failed in the latest",
		},
		{
			name:           "stops after the maximum number of retries",
			lastRemediated: 2 * defaults.retryPeriod,
			retryCount:     defaults.maxRetry,
			reason:         cannotBeRemediatedReason,
			message:        fmt.Sprintf("already failed %d times", defaults.maxRetry),
		},
		{
			name:           "counts a retry",
			lastRemediated: 2 * defaults.retryPeriod,
			retryCount:     1,
			wantRetryCount: 2,
		},
		{
			name:           "starts a new sequence after the min healthy period",
			lastRemediated: 2 * defaults.minHealthyPeriod,
			retryCount:     defaults.maxRetry,
			wantRetryCount: 0,
		},
		{
			name:           "reads the retry period from the spec",
			spec:           controlplanev1.TalosControlPlaneRemediationSpec{RetryPeriodSeconds: ptr.To[int32](30)},
			lastRemediated: time.Minute,
			retryCount:     1,
			wantRetryCount: 2,
		},
		{
			name:           "reads the maximum number of retries from the spec",
			spec:           controlplanev1.TalosControlPlaneRemediationSpec{MaxRetry: ptr.To[int32](1)},
			lastRemediated: 2 * defaults.retryPeriod,
			retryCount:     1,
			reason:         cannotBeRemediatedReason,
			message:        "already failed 1 times",
		},
		{
			name:           "reads the min healthy period from the spec",
			spec:           controlplanev1.TalosControlPlaneRemediationSpec{MinHealthyPeriodSeconds: ptr.To[int32](60)},
			lastRemediated: 2 * time.Minute,
			retryCount:     defaults.maxRetry,
			wantRetryCount: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcp := newRemediationTCP(3)
			tcp.Spec.Remediation = tc.spec
			lastRemediated := now.Add(-tc.lastRemediated)

			f := newPreTerminateFixtureWithTCP(t, tcp,
				newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
				newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
				newRMMachine(tcp, "m3", lastRemediated,
					rmFlagged, rmAnnotation(remediationForAnnotation, remediationValue(t, "m0", lastRemediated, tc.retryCount))))

			_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2", "m3"}, "m3"))
			require.NoError(t, err)

			if tc.reason != "" {
				assert.False(t, remediating)
				f.expectNotRemediated(t, "m3", tc.reason, tc.message)

				// Only a refusal that needs an operator is reported as an event; a deferral is waited out quietly.
				if tc.reason == cannotBeRemediatedReason {
					assert.Contains(t, strings.Join(f.events(), "\n"), cannotRemediateMachineEvent)
				} else {
					assert.NotContains(t, strings.Join(f.events(), "\n"), cannotRemediateMachineEvent)
				}

				return
			}

			assert.True(t, remediating)
			assert.Equal(t, tc.wantRetryCount, f.expectRemediated(t, "m3").RetryCount)
		})
	}
}

// The remediation-in-progress annotation lives from right before the remediated machine is deleted until its
// replacement is created, as in KCP: bootControlPlane removes it, and reconcileRemediationInProgress removes it when
// it is stale.
func TestRemediationInProgress(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	members := []string{"m1", "m2", "m3"}

	t.Run("blocks a second remediation until the replacement is created", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m0", now.Add(-time.Minute), 0)}

		// m0 is gone, its replacement does not exist yet.
		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)))

		_, remediating, err := f.remediate(t, recordedEtcd([]string{"m1", "m2"}, "m1"))
		require.NoError(t, err)
		assert.False(t, remediating)

		f.expectNotRemediated(t, "m1", remediationDeferredReason, "previous remediation")
		assert.Contains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	})

	t.Run("is stale once a machine newer than the remediation exists", func(t *testing.T) {
		// The controller stopped between creating m3, the replacement, and removing the annotation.
		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m0", now.Add(-10*time.Minute), 0)}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m3", now.Add(-5*time.Minute)))

		_, remediating, err := f.remediate(t, recordedEtcd(members, "m1"))
		require.NoError(t, err)

		// The next remediation proceeds in the same reconcile.
		assert.True(t, remediating)
		f.expectRemediated(t, "m1")
	})

	t.Run("is dropped when the remediated machine is gone and no replacement is needed", func(t *testing.T) {
		// The replicas were lowered while m0 was being remediated.
		tcp := newRemediationTCP(2)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m0", now.Add(-10*time.Minute), 0)}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)))

		_, _, err := f.remediate(t, recordedEtcd([]string{"m1", "m2"}))
		require.NoError(t, err)

		assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	})

	t.Run("is kept while the replacement is still to be created", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m0", now.Add(-10*time.Minute), 0)}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)))

		_, _, err := f.remediate(t, recordedEtcd([]string{"m1", "m2"}))
		require.NoError(t, err)

		assert.Contains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	})

	t.Run("is withdrawn when the remediated machine was never deleted", func(t *testing.T) {
		// The deletion of m1 failed, or the controller stopped after recording the remediation.
		recordedAt := now.Add(-remediationStartTimeout - time.Minute)

		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m1", recordedAt, 0)}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m3", now.Add(-time.Hour)))

		_, remediating, err := f.remediate(t, recordedEtcd(members, "m1"))
		require.NoError(t, err)

		// m1 is remediated again, as a new remediation.
		assert.True(t, remediating)

		data := f.expectRemediated(t, "m1")
		assert.True(t, data.Timestamp.After(recordedAt))
		assert.Zero(t, data.RetryCount)
	})

	t.Run("is kept right after it was recorded", func(t *testing.T) {
		// The cached Machines may not show yet that m1 is being deleted.
		value := remediationValue(t, "m1", now.Add(-10*time.Second), 0)

		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: value}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour), rmFlagged),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m3", now.Add(-time.Hour)))

		_, remediating, err := f.remediate(t, recordedEtcd(members, "m1"))
		require.NoError(t, err)
		assert.False(t, remediating)

		assert.Equal(t, value, f.tcp.Annotations[remediationInProgressAnnotation])
		assert.Empty(t, f.deletedMachines(t))
	})

	t.Run("is kept while the remediated machine is still being deleted", func(t *testing.T) {
		value := remediationValue(t, "m0", now.Add(-time.Hour), 0)

		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: value}

		// m0 is still being deleted an hour later, held by the pre-terminate hook, say: no replacement yet.
		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m0", now.Add(-4*time.Hour), rmFlagged),
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)))

		require.NoError(t, f.r.Delete(context.Background(), f.get(t, "m0")))

		_, _, err := f.remediate(t, recordedEtcd([]string{"m1", "m2"}))
		require.NoError(t, err)

		assert.Equal(t, value, f.tcp.Annotations[remediationInProgressAnnotation])
	})

	t.Run("an invalid annotation is removed", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: "m0"}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-3*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m3", now.Add(-time.Hour)))

		_, _, err := f.remediate(t, recordedEtcd(members))
		require.NoError(t, err)

		assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	})
}

// bootControlPlane removes the remediation-in-progress annotation, on the API server too, once the replacement
// exists; clearRemediationInProgress is what it calls.
func TestClearRemediationInProgress(t *testing.T) {
	tcp := newRemediationTCP(3)
	tcp.Annotations = map[string]string{
		remediationInProgressAnnotation: remediationValue(t, "m0", time.Now(), 0),
		"keep":                          "me",
	}

	f := newPreTerminateFixtureWithTCP(t, tcp)

	require.NoError(t, f.r.clearRemediationInProgress(context.Background(), f.tcp))

	assert.NotContains(t, f.tcp.Annotations, remediationInProgressAnnotation)
	assert.NotContains(t, f.storedTCP(t).Annotations, remediationInProgressAnnotation)
	assert.Equal(t, "me", f.storedTCP(t).Annotations["keep"])

	require.NoError(t, f.r.clearRemediationInProgress(context.Background(), f.tcp), "clearing twice is fine")
}

func TestRemediationAndScaling(t *testing.T) {
	now := time.Now()

	t.Run("no scale down in the reconcile that remediated a machine", func(t *testing.T) {
		// 4 machines for 3 replicas: without remediation, the control plane would scale down.
		tcp := newRemediationTCP(3)
		ptEtcdHealthy(tcp)

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-4*time.Hour), rmFlagged),
			newRMMachine(tcp, "m2", now.Add(-3*time.Hour)),
			newRMMachine(tcp, "m3", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m4", now.Add(-time.Hour)))

		res, err := f.r.reconcileRemediationAndMachines(context.Background(), f.cluster, f.tcp,
			&clusterv1.MachineList{Items: f.current(t)}, recordedEtcd([]string{"m1", "m2", "m3", "m4"}, "m1"))
		require.NoError(t, err)
		assert.NotZero(t, res.RequeueAfter)

		f.expectRemediated(t, "m1")
		assert.Equal(t, []string{"m1"}, f.deletedMachines(t))

		// The scale down did not run at all.
		assert.False(t, conditions.Has(f.tcp, string(controlplanev1.ResizedCondition)))
	})
}

func TestCanSafelyAddEtcdMemberTo(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name      string
		machines  int
		members   []*machineapi.EtcdMember
		unhealthy []int
		safe      bool
	}{
		{name: "no member", machines: 0, safe: true},
		{name: "1 member", machines: 1, safe: true},
		{name: "1 member, unhealthy", machines: 1, unhealthy: []int{0}, safe: false},
		{name: "2 members, all healthy", machines: 2, safe: true},
		{name: "2 members, one unhealthy", machines: 2, unhealthy: []int{0}, safe: false},
		{name: "3 members, all healthy", machines: 3, safe: true},
		{name: "3 members, one unhealthy", machines: 3, unhealthy: []int{0}, safe: false},
		{name: "4 members, one unhealthy", machines: 4, unhealthy: []int{0}, safe: true},
		{name: "4 members, two unhealthy", machines: 4, unhealthy: []int{0, 1}, safe: false},
		{
			name:     "3 members, one is a learner",
			machines: 3,
			members: []*machineapi.EtcdMember{
				{Id: 1, Hostname: "m0"},
				{Id: 2, Hostname: "m1"},
				{Id: 3, Hostname: "m2", IsLearner: true},
			},
			safe: false,
		},
		{
			name:     "3 members, a member without machine",
			machines: 2,
			members:  etcdMembersFor("m0", "m1", "orphan"),
			safe:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			machines := make([]*clusterv1.Machine, 0, tc.machines)
			names := make([]string, 0, tc.machines)
			healthy := map[string]bool{}

			for i := range tc.machines {
				name := fmt.Sprintf("m%d", i)
				names = append(names, name)
				machines = append(machines, newTestMachine(name, now.Add(time.Duration(i)*time.Minute)))
				healthy[name] = true
			}

			for _, i := range tc.unhealthy {
				healthy[names[i]] = false
			}

			members := tc.members
			if members == nil {
				members = etcdMembersFor(names...)
			}

			safe, details := canSafelyAddEtcdMemberTo(members, machines, healthy)
			assert.Equal(t, tc.safe, safe, details)
		})
	}
}

// --- status ----------------------------------------------------------------

func TestReconcileRemediationStatus(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	t.Run("not remediating", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		f := newPreTerminateFixtureWithTCP(t, tcp)

		f.r.reconcileRemediationStatus(tcp, collections.FromMachines(
			newRMMachine(tcp, "m1", now),
			newRMMachine(tcp, "m2", now)))

		c := conditions.Get(tcp, controlplanev1.RemediatingCondition)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionFalse, c.Status)
		assert.Equal(t, controlplanev1.NotRemediatingReason, c.Reason)
		assert.Empty(t, c.Message)
		assert.Zero(t, tcp.Status.LastRemediation)
	})

	t.Run("unhealthy machines not to be remediated", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		f := newPreTerminateFixtureWithTCP(t, tcp)

		// Flagged by a MachineHealthCheck with an external remediation: OwnerRemediated is never set.
		unhealthy := func(m *clusterv1.Machine) {
			conditions.Set(m, metav1.Condition{
				Type:   clusterv1.MachineHealthCheckSucceededCondition,
				Status: metav1.ConditionFalse,
				Reason: clusterv1.MachineHealthCheckNodeDeletedReason,
			})
		}

		f.r.reconcileRemediationStatus(tcp, collections.FromMachines(
			newRMMachine(tcp, "m1", now, unhealthy),
			newRMMachine(tcp, "m2", now)))

		c := conditions.Get(tcp, controlplanev1.RemediatingCondition)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionFalse, c.Status)
		assert.Equal(t, controlplanev1.NotRemediatingReason, c.Reason)
		assert.Equal(t, "Machine m1 is not healthy (not to be remediated by TalosControlPlane)", c.Message)
	})

	t.Run("remediating", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		f := newPreTerminateFixtureWithTCP(t, tcp)

		flagged := newRMMachine(tcp, "m1", now, rmFlagged)
		conditions.Set(flagged, metav1.Condition{
			Type:    clusterv1.MachineOwnerRemediatedCondition,
			Status:  metav1.ConditionFalse,
			Reason:  remediationDeferredReason,
			Message: "TalosControlPlane waiting for the deletion of control plane machine m3 to complete before triggering remediation",
		})

		f.r.reconcileRemediationStatus(tcp, collections.FromMachines(flagged, newRMMachine(tcp, "m2", now)))

		c := conditions.Get(tcp, controlplanev1.RemediatingCondition)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Equal(t, controlplanev1.RemediatingReason, c.Reason)
		assert.Contains(t, c.Message, "waiting for the deletion of control plane machine m3")
		assert.Contains(t, c.Message, "m1")
	})

	t.Run("last remediation from the remediation in progress", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		tcp.Annotations = map[string]string{remediationInProgressAnnotation: remediationValue(t, "m0", now, 2)}
		f := newPreTerminateFixtureWithTCP(t, tcp)

		f.r.reconcileRemediationStatus(tcp, collections.FromMachines(
			newRMMachine(tcp, "m1", now, rmAnnotation(remediationForAnnotation, remediationValue(t, "m9", now.Add(-time.Hour), 1)))))

		assert.Equal(t, controlplanev1.LastRemediationStatus{Machine: "m0", Time: metav1.NewTime(now), RetryCount: ptr.To[int32](2)}, tcp.Status.LastRemediation)
	})

	t.Run("last remediation from the newest replacement", func(t *testing.T) {
		tcp := newRemediationTCP(3)
		f := newPreTerminateFixtureWithTCP(t, tcp)

		f.r.reconcileRemediationStatus(tcp, collections.FromMachines(
			newRMMachine(tcp, "m1", now, rmAnnotation(remediationForAnnotation, remediationValue(t, "m8", now.Add(-2*time.Hour), 0))),
			newRMMachine(tcp, "m2", now, rmAnnotation(remediationForAnnotation, remediationValue(t, "m9", now.Add(-time.Hour), 1))),
			newRMMachine(tcp, "m3", now, rmAnnotation(remediationForAnnotation, "not json"))))

		assert.Equal(t, controlplanev1.LastRemediationStatus{Machine: "m9", Time: metav1.NewTime(now.Add(-time.Hour)), RetryCount: ptr.To[int32](1)}, tcp.Status.LastRemediation)
	})
}
