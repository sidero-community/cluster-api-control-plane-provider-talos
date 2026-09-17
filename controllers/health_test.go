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
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// A machine on its way out is not expected to be healthy: its node is drained and its etcd
// member may already be gone. Counting it would make the components condition flap through
// every deletion.
func TestReconcileNodeHealth_IgnoresDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	victim := newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingWaitingForPreTerminateHookReason))
	peer := newPTMachine(tcp, "cp-2", ptHooked)

	f := newPreTerminateFixture(t, victim, peer)
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: unhealthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	_, err := f.r.reconcileNodeHealth(context.Background(), f.cluster, f.tcp, f.machines)
	require.NoError(t, err)

	assert.True(t, conditions.IsTrue(f.tcp, string(controlplanev1.ControlPlaneComponentsHealthyCondition)))
	assert.NotContains(t, f.dialer.dialed, "cp-1", "a deleting machine is not asked about its services")
}

func TestReconcileNodeHealth_ReportsAnUnhealthyService(t *testing.T) {
	tcp := newPreTerminateTCP()
	cp1 := newPTMachine(tcp, "cp-1", ptHooked)
	cp2 := newPTMachine(tcp, "cp-2", ptHooked)

	f := newPreTerminateFixture(t, cp1, cp2)
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: unhealthyServices()}

	res, err := f.r.reconcileNodeHealth(context.Background(), f.cluster, f.tcp, f.machines)
	require.Error(t, err)
	assert.Positive(t, res.RequeueAfter)

	cond := conditions.Get(f.tcp, string(controlplanev1.ControlPlaneComponentsHealthyCondition))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, controlplanev1.ControlPlaneComponentsUnhealthyReason, cond.Reason)
	assert.Contains(t, cond.Message, `machine "cp-2"`)
	assert.Contains(t, cond.Message, "kubelet")
}
