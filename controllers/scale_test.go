// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func newScaleControlPlane(f *preTerminateFixture) *ControlPlane {
	return &ControlPlane{TCP: f.tcp, Cluster: f.cluster, Machines: f.collection()}
}

// --- scale up --------------------------------------------------------------

func TestScaleUp_WaitsForDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](3)
	ptEtcdHealthy(tcp)

	f := newPreTerminateFixtureWithTCP(t, tcp,
		newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingDrainingNodeReason)),
		newPTMachine(tcp, "cp-2", ptHooked))

	res, err := f.r.scaleUpControlPlane(context.Background(), f.cluster, f.tcp, newScaleControlPlane(f))
	require.NoError(t, err)
	assert.Equal(t, preflightFailedRequeueAfter, res.RequeueAfter)

	var machines clusterv1.MachineList
	require.NoError(t, f.r.Client.List(context.Background(), &machines))
	assert.Len(t, machines.Items, 2, "no replacement while a deletion is in flight")

	cond := conditions.Get(f.tcp, string(controlplanev1.ResizedCondition))
	require.NotNil(t, cond)
	assert.Equal(t, controlplanev1.ScalingUpReason, cond.Reason)
	assert.Contains(t, cond.Message, "Scaling up control plane to 3 replicas (actual 2): waiting for machines to be deleted: cp-1")
	assert.Contains(t, strings.Join(f.events(), "\n"), controlPlaneUnhealthyEvent)
}

func TestScaleUp_WaitsForHealthyEtcd(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](3)

	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	res, err := f.r.scaleUpControlPlane(context.Background(), f.cluster, f.tcp, newScaleControlPlane(f))
	require.NoError(t, err)
	assert.Equal(t, preflightFailedRequeueAfter, res.RequeueAfter)

	cond := conditions.Get(f.tcp, string(controlplanev1.ResizedCondition))
	require.NotNil(t, cond)
	assert.Contains(t, cond.Message, "etcd cluster health is unknown")
}
