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
