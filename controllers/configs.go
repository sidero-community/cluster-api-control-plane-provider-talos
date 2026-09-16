// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// talosconfigForMachine will generate a talosconfig that uses *all* found addresses as the endpoints.
//
// NOTE: There is no client.WithNodes(...) here, so no multiplexing is done. The request will hit any
// of the controlplane nodes in machines list.
func (r *TalosControlPlaneReconciler) talosconfigForMachines(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	if len(machines) == 0 {
		return nil, fmt.Errorf("at least one machine should be provided")
	}

	clusterName := tcp.GetLabels()[clusterv1.ClusterNameLabel]

	for _, ref := range tcp.GetOwnerReferences() {
		if ref.Kind != "Cluster" {
			continue
		}

		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if gv.Group == clusterv1.GroupVersion.Group {
			clusterName = ref.Name

			break
		}
	}

	if clusterName == "" {
		return nil, fmt.Errorf("failed to determine the cluster name of the control plane")
	}

	addrList := []string{}

	var (
		talosconfigSecret corev1.Secret
	)

	if err := r.Client.Get(ctx,
		types.NamespacedName{
			Namespace: tcp.GetNamespace(),
			Name:      clusterName + "-talosconfig",
		},
		&talosconfigSecret,
	); err != nil {
		return nil, err
	}

	t, err := talosconfig.FromBytes(talosconfigSecret.Data["talosconfig"])
	if err != nil {
		return nil, err
	}

	for _, machine := range machines {
		for _, addr := range machine.Status.Addresses {
			if addr.Type == clusterv1.MachineExternalIP || addr.Type == clusterv1.MachineInternalIP {
				addrList = append(addrList, addr.Address)
			}
		}

		if len(addrList) == 0 {
			return nil, fmt.Errorf("no addresses were found for node %q", machine.Name)
		}
	}

	// Endpoints were pre-populated by the CABPT controller; the given machines only narrow the
	// request down to specific nodes.
	return talosclient.New(ctx,
		talosclient.WithDefaultGRPCDialOptions(),
		talosclient.WithEndpoints(addrList...),
		talosclient.WithConfig(t),
	)
}
