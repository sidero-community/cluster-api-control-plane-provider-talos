// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	// preflightFailedRequeueAfter is how long a scale operation waits before the preflight
	// checks are run again.
	preflightFailedRequeueAfter = 10 * time.Second

	// controlPlaneUnhealthyEvent is recorded on the TalosControlPlane while a scale operation
	// is held back by the preflight checks. Same name as KCP's.
	controlPlaneUnhealthyEvent = "ControlPlaneUnhealthy"
)

// preflightFailure explains why the control plane is not ready for a scale operation.
type preflightFailure struct {
	message string
}

// preflightChecks mirrors KCP's preflightChecks: a scale operation may only start when no
// machine is deleting, every machine has a Node, the control plane components are healthy
// and etcd is healthy. excludeFor names machines that are about to be deleted, which are
// therefore not required to be healthy or to have a Node. A nil result means go ahead.
func (r *TalosControlPlaneReconciler) preflightChecks(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	machines collections.Machines,
	excludeFor ...*clusterv1.Machine,
) *preflightFailure {
	if machines.Len() == 0 {
		return nil
	}

	// Serialization: one membership change at a time. Nothing else is worth checking while a
	// deletion is in flight, and the deleting machine would fail the checks below anyway.
	if deleting := machines.Filter(collections.HasDeletionTimestamp); deleting.Len() > 0 {
		names := deleting.Names()
		sort.Strings(names)

		return &preflightFailure{message: "waiting for machines to be deleted: " + strings.Join(names, ", ")}
	}

	excluded := map[string]struct{}{}

	for _, machine := range excludeFor {
		if machine != nil {
			excluded[machine.Name] = struct{}{}
		}
	}

	names := machines.Names()
	sort.Strings(names)

	var (
		failures  []string
		checkable []clusterv1.Machine
	)

	for _, name := range names {
		machine := machines[name]

		if _, ok := excluded[name]; ok {
			continue
		}

		if !machine.Status.NodeRef.IsDefined() {
			failures = append(failures, fmt.Sprintf("machine %q does not have a corresponding Node yet", name))

			continue
		}

		checkable = append(checkable, *machine)
	}

	// Checked fresh rather than read off the condition so the excluded machines really are
	// excluded: the condition covers every non-deleting machine, victim included.
	if err := r.nodesHealthcheck(ctx, tcp, healthCheckable(checkable)); err != nil {
		failures = append(failures, fmt.Sprintf("control plane components are not healthy: %v", err))
	}

	// etcd health is read off the condition set earlier in the same reconcile: a fresh check
	// could not exclude the victim, whose member is still in the list until the hook removes it.
	if c := conditions.Get(tcp, string(controlplanev1.EtcdClusterHealthyCondition)); c == nil || c.Status != metav1.ConditionTrue {
		message := "etcd cluster health is unknown"
		if c != nil && c.Message != "" {
			message = "etcd cluster is not healthy: " + c.Message
		}

		failures = append(failures, message)
	}

	if len(failures) == 0 {
		return nil
	}

	return &preflightFailure{message: strings.Join(failures, "; ")}
}

// holdScaleOperation records why a scale operation is waiting -- on the Resized condition, in
// the log and as an event on the TalosControlPlane -- and returns the result that retries it.
func (r *TalosControlPlaneReconciler) holdScaleOperation(tcp *controlplanev1.TalosControlPlane, reason, summary string, failure *preflightFailure) ctrl.Result {
	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: summary + ": " + failure.message,
	})

	r.Log.Info("waiting for control plane to pass preflight checks", "operation", reason, "failures", failure.message)
	r.recordControlPlaneEvent(tcp, corev1.EventTypeWarning, controlPlaneUnhealthyEvent,
		"Waiting for control plane to pass preflight checks to continue reconciliation: "+failure.message)

	return ctrl.Result{RequeueAfter: preflightFailedRequeueAfter}
}

// recordControlPlaneEvent records an event on the TalosControlPlane, if a recorder is wired up.
func (r *TalosControlPlaneReconciler) recordControlPlaneEvent(tcp *controlplanev1.TalosControlPlane, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(tcp, nil, eventType, reason, "Preflight", "%s", message)
}
