// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func ptEtcdHealthy(tcp *controlplanev1.TalosControlPlane) {
	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.EtcdClusterHealthyCondition),
		Status: metav1.ConditionTrue,
		Reason: controlplanev1.EtcdClusterHealthyReason,
	})
}

func ptNoNodeRef(m *clusterv1.Machine) {
	m.Status.NodeRef = clusterv1.MachineNodeReference{}
}

func (f *preTerminateFixture) collection() collections.Machines {
	return collections.FromMachineList(f.machines)
}

func TestPreflight_PassesForAHealthyControlPlane(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, f.collection()))
}

func TestPreflight_PassesWithNoMachines(t *testing.T) {
	f := newPreTerminateFixture(t)

	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, collections.New()))
}

func TestPreflight_WaitsForDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingDrainingNodeReason)),
		newPTMachine(tcp, "cp-2", ptHooked))

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)

	assert.Equal(t, "waiting for machines to be deleted: cp-1", failure.message)
	assert.Empty(t, f.dialer.dialed, "nothing else is checked while a deletion is in flight")
}

func TestPreflight_RequiresANodeForEveryMachine(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked, ptNoNodeRef))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)

	assert.Contains(t, failure.message, `machine "cp-2" does not have a corresponding Node yet`)
	assert.NotContains(t, f.dialer.dialed, "cp-2", "a machine without a node is not asked about its services")
}

// The Kubernetes control plane components are static pods, not Talos services: a kube-apiserver
// that cannot start on its configuration is only visible through the kubelet's view of it.
func TestPreflight_RequiresControlPlaneStaticPods(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices(), staticPods: []staticPodStatus{
		{id: "kube-system/kube-apiserver-cp-2", ready: false},
		{id: "kube-system/kube-controller-manager-cp-2", ready: true},
		{id: "kube-system/kube-scheduler-cp-2", ready: true},
	}}

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)

	assert.Contains(t, failure.message, `machine "cp-2": Service kube-apiserver is unhealthy: static pod is not ready`)
}

func TestPreflight_RequiresHealthyComponents(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: unhealthyServices()}

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)

	assert.Contains(t, failure.message, "control plane components are not healthy")
	assert.Contains(t, failure.message, `machine "cp-2"`)
}

func TestPreflight_RequiresHealthyEtcd(t *testing.T) {
	t.Run("condition false", func(t *testing.T) {
		tcp := newPreTerminateTCP()
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.EtcdClusterHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.EtcdClusterUnhealthyReason,
			Message: "expected to have 2 members, got 3",
		})

		f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked))
		f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

		failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
		require.NotNil(t, failure)

		assert.Contains(t, failure.message, "etcd cluster is not healthy: expected to have 2 members, got 3")
	})

	t.Run("condition missing", func(t *testing.T) {
		tcp := newPreTerminateTCP()

		f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked))
		f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

		failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
		require.NotNil(t, failure)

		assert.Contains(t, failure.message, "etcd cluster health is unknown")
	})
}

// The victim of a scale-down may be exactly the broken machine a rollout is replacing, so it
// is held to none of the per-machine requirements.
func TestPreflight_ExcludesTheScaleDownVictim(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)

	victim := newPTMachine(tcp, "cp-1", ptHooked, ptNoNodeRef)

	f := newPreTerminateFixtureWithTCP(t, tcp, victim, newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: unhealthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, f.collection(), victim))
	assert.NotContains(t, f.dialer.dialed, "cp-1")
}

// --- scale-up while a remediation is in progress ---------------------------

// rmInProgress records a remediation in progress on the TalosControlPlane.
func rmInProgress(t *testing.T, tcp *controlplanev1.TalosControlPlane) {
	t.Helper()

	if tcp.Annotations == nil {
		tcp.Annotations = map[string]string{}
	}

	tcp.Annotations[remediationInProgressAnnotation] = remediationValue(t, "m0", time.Now().Add(-time.Minute), 0)
}

// While the replacement of a remediated machine is to be created, a control plane that fails the regular checks is
// still allowed to grow when one machine can take the join and etcd keeps quorum with one more member.
func TestPreflightForScaleUp_RelaxedWhileRemediating(t *testing.T) {
	// 5 replicas, m0 remediated and gone, m4 unhealthy: the regular checks fail on m4 and on the etcd condition.
	setup := func(t *testing.T, inProgress bool) (*preTerminateFixture, map[string]etcdMachineStatus) {
		t.Helper()

		tcp := newPreTerminateTCP()
		tcp.Spec.Replicas = ptr.To[int32](5)

		if inProgress {
			rmInProgress(t, tcp)
		}

		f := newPreTerminateFixtureWithTCP(t, tcp,
			newPTMachine(tcp, "m1", ptHooked),
			newPTMachine(tcp, "m2", ptHooked),
			newPTMachine(tcp, "m3", ptHooked),
			newPTMachine(tcp, "m4", ptHooked))

		for _, name := range []string{"m1", "m2", "m3"} {
			f.dialer.clients[name] = &fakeEtcdCalls{name: name, serviceList: healthyServices()}
		}

		f.dialer.clients["m4"] = &fakeEtcdCalls{name: "m4", serviceList: unhealthyServices()}

		return f, recordedEtcd([]string{"m1", "m2", "m3", "m4"}, "m4")
	}

	t.Run("passes when the replacement is viable", func(t *testing.T) {
		f, statuses := setup(t, true)

		assert.Nil(t, f.r.preflightChecksForScaleUp(context.Background(), f.tcp, f.collection(), statuses))
	})

	t.Run("still applies the regular checks without a remediation in progress", func(t *testing.T) {
		f, statuses := setup(t, false)

		failure := f.r.preflightChecksForScaleUp(context.Background(), f.tcp, f.collection(), statuses)
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "control plane components are not healthy")
	})

	t.Run("refuses when adding a member could cost etcd its quorum", func(t *testing.T) {
		f, _ := setup(t, true)

		// m3 and m4 unhealthy: 2 of 5 voting members with the new one, below the quorum of 3.
		statuses := recordedEtcd([]string{"m1", "m2", "m3", "m4"}, "m3", "m4")

		failure := f.r.preflightChecksForScaleUp(context.Background(), f.tcp, f.collection(), statuses)
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "remediation in progress")
		assert.Contains(t, failure.message, "adding an etcd member could result in etcd quorum loss")
	})

	t.Run("refuses when no machine can take the join", func(t *testing.T) {
		f, statuses := setup(t, true)

		for _, name := range []string{"m1", "m2", "m3"} {
			f.dialer.clients[name] = &fakeEtcdCalls{name: name, serviceList: unhealthyServices()}
		}

		failure := f.r.preflightChecksForScaleUp(context.Background(), f.tcp, f.collection(), statuses)
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "no control plane machine has all its components healthy")
	})

	t.Run("refuses when etcd could not be checked", func(t *testing.T) {
		f, _ := setup(t, true)

		failure := f.r.preflightChecksForScaleUp(context.Background(), f.tcp, f.collection(), nil)
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "etcd cluster health could not be checked")
	})

	t.Run("waits for deleting machines in any case", func(t *testing.T) {
		f, statuses := setup(t, true)

		machines := f.collection()
		deletionTimestamp := metav1.Now()
		machines["m4"].DeletionTimestamp = &deletionTimestamp

		failure := f.r.preflightChecksForScaleUp(context.Background(), f.tcp, machines, statuses)
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "waiting for machines to be deleted: m4")
	})
}
