// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
)

const etcdLeavingAnnotation = "controlplane.cluster.x-k8s.io/etcd-leaving"

func (r *TalosControlPlaneReconciler) scaleUpControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, controlPlane *ControlPlane) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling up control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	// A replacement joins an etcd cluster that has to be able to take it: no deletion still in
	// flight, every existing machine with a node, components and etcd healthy. Like KCP.
	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines); failure != nil {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingUpReason, summary, failure), nil
	}

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.ScalingUpReason,
		Message: summary,
	})

	// Create a new Machine w/ join
	r.Log.Info("scaling up control plane", "Desired", desiredReplicas, "Existing", numMachines)

	return r.bootControlPlane(ctx, cluster, tcp)
}

func (r *TalosControlPlaneReconciler) scaleDownControlPlane(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	controlPlane *ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling down control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.ScalingDownReason,
		Message: summary,
	})

	if numMachines == 1 {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.ResizedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.ScalingDownReason,
			Message: "Cannot scale down control plane nodes to 0",
		})

		return ctrl.Result{}, nil
	}

	if numMachines == 0 {
		return ctrl.Result{}, fmt.Errorf("no machines found")
	}

	victim, err := selectMachineForScaleDown(controlPlane, machinesRequireUpgrade)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The victim is exempt from the checks: it may be exactly the broken machine a rollout is
	// replacing. Everything else has to be healthy before one more member is taken away.
	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines, victim); failure != nil {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingDownReason, summary, failure), nil
	}

	if err := r.ensureNodesBooted(ctx, controlPlane.TCP, collections.ToMachineList(controlPlane.Machines).Items); err != nil {
		r.Log.Info("waiting for all nodes to finish boot sequence", "error", err)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.Log.Info("scaling down control plane", "Desired", desiredReplicas, "Existing", numMachines)

	return r.deleteControlPlaneMachine(ctx, victim)
}

// deleteControlPlaneMachine requests the deletion of a control plane machine, and nothing
// else: its etcd membership is resolved by the deletion pipeline at the pre-terminate phase
// (reconcileMachinePreTerminateHooks) and its Node is deleted by the core Machine controller
// once the infrastructure is gone -- exactly what happens on a `kubectl delete machine`.
func (r *TalosControlPlaneReconciler) deleteControlPlaneMachine(ctx context.Context, machine *clusterv1.Machine) (ctrl.Result, error) {
	r.Log.Info("deleting machine", "machine", machine.Name, "node", machine.Status.NodeRef.Name)

	if err := r.Client.Delete(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, errors.Wrapf(err, "failed to delete control plane machine %q", machine.Name)
	}

	return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
}

func selectMachineForScaleDown(controlPlane *ControlPlane, outdatedMachines collections.Machines) (*clusterv1.Machine, error) {
	machines := controlPlane.Machines
	switch {
	case controlPlane.MachineWithDeleteAnnotation(outdatedMachines).Len() > 0:
		machines = controlPlane.MachineWithDeleteAnnotation(outdatedMachines)
	case controlPlane.MachineWithDeleteAnnotation(machines).Len() > 0:
		machines = controlPlane.MachineWithDeleteAnnotation(machines)
	case outdatedMachines.Len() > 0:
		machines = outdatedMachines
	}

	return machines.Oldest(), nil
}
