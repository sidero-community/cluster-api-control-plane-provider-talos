// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// EtcdHealthcheckReason runs the etcd health check and returns the reason the EtcdClusterHealthy condition gets
// when the health check fails.
func (r *TalosControlPlaneReconciler) EtcdHealthcheckReason(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (string, error) {
	health, err := r.etcdHealthcheck(ctx, tcp, machines)

	return health.conditionReason(), err
}

// EtcdHealthcheckStatuses runs the etcd health check and returns, for each machine, whether its etcd service was
// observed healthy and how many etcd members it listed.
func (r *TalosControlPlaneReconciler) EtcdHealthcheckStatuses(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (healthy map[string]bool, members map[string]int, err error) {
	health, err := r.etcdHealthcheck(ctx, tcp, machines)

	healthy = map[string]bool{}
	members = map[string]int{}

	for name, status := range health.statuses {
		healthy[name] = status.healthy
		members[name] = len(status.members)
	}

	return healthy, members, err
}
