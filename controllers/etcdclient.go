// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"

	"github.com/cosi-project/runtime/pkg/safe"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"google.golang.org/grpc"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// etcdCalls is the subset of the Talos API the etcd cleanup and health-check paths use.
// talosCalls satisfies it with a real client; tests substitute a fake through
// TalosControlPlaneReconciler.etcdDialer.
type etcdCalls interface {
	ServiceInfo(ctx context.Context, id string, callOptions ...grpc.CallOption) ([]talosclient.ServiceInfo, error)
	ServiceList(ctx context.Context, callOptions ...grpc.CallOption) (*machineapi.ServiceListResponse, error)
	// StaticPodStatuses returns the kubelet's status of the static pods on the node, as Talos
	// records it.
	StaticPodStatuses(ctx context.Context) ([]staticPodStatus, error)
	EtcdMemberList(ctx context.Context, req *machineapi.EtcdMemberListRequest, callOptions ...grpc.CallOption) (*machineapi.EtcdMemberListResponse, error)
	EtcdForfeitLeadership(ctx context.Context, req *machineapi.EtcdForfeitLeadershipRequest, callOptions ...grpc.CallOption) (*machineapi.EtcdForfeitLeadershipResponse, error)
	EtcdLeaveCluster(ctx context.Context, req *machineapi.EtcdLeaveClusterRequest, callOptions ...grpc.CallOption) error
	EtcdRemoveMemberByID(ctx context.Context, req *machineapi.EtcdRemoveMemberByIDRequest, callOptions ...grpc.CallOption) error
	Close() error
}

// etcdClientFor opens a Talos client scoped to the given machines. Endpoints come from the
// Machines' status addresses, so nothing here knows about any infrastructure provider.
func (r *TalosControlPlaneReconciler) etcdClientFor(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (etcdCalls, error) {
	if r.etcdDialer != nil {
		return r.etcdDialer(ctx, tcp, machines...)
	}

	c, err := r.talosconfigForMachines(ctx, tcp, machines...)
	if err != nil {
		return nil, err
	}

	return &talosCalls{Client: c}, nil
}

// talosCalls is a Talos client with the resource reads the health check needs on top of the
// machine API.
type talosCalls struct {
	*talosclient.Client
}

// StaticPodStatuses lists the StaticPodStatus resources of the node the client points at.
func (c *talosCalls) StaticPodStatuses(ctx context.Context) ([]staticPodStatus, error) {
	list, err := safe.StateListAll[*k8s.StaticPodStatus](ctx, c.COSI)
	if err != nil {
		return nil, fmt.Errorf("failed to list the static pod statuses: %w", err)
	}

	statuses := make([]staticPodStatus, 0, list.Len())

	if err := list.ForEachErr(func(status *k8s.StaticPodStatus) error {
		ready, err := staticPodReady(status.TypedSpec().PodStatus)
		if err != nil {
			return fmt.Errorf("static pod %s: %w", status.Metadata().ID(), err)
		}

		statuses = append(statuses, staticPodStatus{id: status.Metadata().ID(), ready: ready})

		return nil
	}); err != nil {
		return nil, err
	}

	return statuses, nil
}
