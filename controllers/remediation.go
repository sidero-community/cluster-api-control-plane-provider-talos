// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// The remediation annotations, see the API package. While remediation-in-progress is set, from right before the
// unhealthy Machine is deleted until its replacement is created, no other Machine is remediated.
const (
	remediationInProgressAnnotation = controlplanev1.RemediationInProgressAnnotation
	remediationForAnnotation        = controlplanev1.RemediationForAnnotation
)

const (
	// remediationStartTimeout is how long after a remediation was recorded the remediated Machine may still be seen
	// not deleting, before the remediation is considered not started. That happens when deleting the Machine failed,
	// or the controller stopped between recording the remediation and deleting the Machine; it is also seen for a
	// short while when the cached Machines lag behind.
	remediationStartTimeout = time.Minute

	// remediationRequeueAfter is when the control plane is reconciled again after a remediation deleted a Machine.
	remediationRequeueAfter = 20 * time.Second
)

// Reasons of the OwnerRemediated condition of control plane Machines, see the API package.
const (
	remediationDeferredReason        = controlplanev1.MachineRemediationDeferredReason
	cannotBeRemediatedReason         = controlplanev1.MachineCannotBeRemediatedReason
	remediationMachineDeletingReason = controlplanev1.MachineRemediationMachineDeletingReason
	remediationInternalErrorReason   = controlplanev1.MachineRemediationInternalErrorReason
)

// Event reasons of the remediation.
const (
	// remediatingMachineEvent is recorded on a Machine when its remediation deletes it.
	remediatingMachineEvent = "RemediatingMachine"

	// cannotRemediateMachineEvent is recorded on the TalosControlPlane when a Machine can't be remediated for a
	// reason that needs an operator, as opposed to a transient state the remediation waits out.
	cannotRemediateMachineEvent = "CannotRemediateMachine"
)

// remediationData is stored in remediationInProgressAnnotation and remediationForAnnotation.
type remediationData struct {
	// Machine is the name of the remediated Machine.
	Machine string `json:"machine"`

	// Timestamp is when the remediation happened.
	Timestamp metav1.Time `json:"timestamp"`

	// RetryCount is the number of retries of the remediation sequence: a retry happens when a Machine that
	// replaced a remediated Machine is remediated too, within the min healthy period.
	RetryCount int `json:"retryCount"`
}

func remediationDataFromAnnotation(value string) (*remediationData, error) {
	data := &remediationData{}
	if err := json.Unmarshal([]byte(value), data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal remediation data %q: %w", value, err)
	}

	return data, nil
}

func (d *remediationData) marshal() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("failed to marshal remediation data: %w", err)
	}

	return string(b), nil
}

// toStatus returns the remediation data as surfaced in status.lastRemediation.
func (d *remediationData) toStatus() controlplanev1.LastRemediationStatus {
	return controlplanev1.LastRemediationStatus{
		Machine:    d.Machine,
		Time:       d.Timestamp,
		RetryCount: new(int32(d.RetryCount)), //nolint:gosec // the retry count is bounded by maxRetry
	}
}

// remediationLimits are the retry limits of a TalosControlPlane: spec.remediation with the defaults applied to the
// fields left empty, as the webhook does, so that objects that never went through the webhook get the same limits.
type remediationLimits struct {
	maxRetry         int
	retryPeriod      time.Duration
	minHealthyPeriod time.Duration
}

func remediationLimitsFor(spec controlplanev1.TalosControlPlaneRemediationSpec) remediationLimits {
	return remediationLimits{
		maxRetry:         int(ptr.Deref(spec.MaxRetry, controlplanev1.DefaultRemediationMaxRetry)),
		retryPeriod:      time.Duration(ptr.Deref(spec.RetryPeriodSeconds, controlplanev1.DefaultRemediationRetryPeriodSeconds)) * time.Second,
		minHealthyPeriod: time.Duration(ptr.Deref(spec.MinHealthyPeriodSeconds, controlplanev1.DefaultRemediationMinHealthyPeriodSeconds)) * time.Second,
	}
}

// remediationInProgress reports whether a remediation is waiting for its replacement Machine to be created.
func remediationInProgress(tcp *controlplanev1.TalosControlPlane) bool {
	_, ok := tcp.Annotations[remediationInProgressAnnotation]

	return ok
}

// reconcileRemediationAndMachines runs the two steps that delete control plane Machines: remediation, then scaling
// and rollout. When remediation deleted a Machine, or tried to, scaling and rollout are skipped in this reconcile, so
// that a single reconcile never deletes two Machines and the next one starts from a fresh list of Machines.
//
// etcdStatuses is what the etcd health check of this reconcile recorded on each Machine.
func (r *TalosControlPlaneReconciler) reconcileRemediationAndMachines(
	ctx context.Context,
	cluster *clusterv1.Cluster,
	tcp *controlplanev1.TalosControlPlane,
	machines *clusterv1.MachineList,
	etcdStatuses map[string]etcdMachineStatus,
) (ctrl.Result, error) {
	result, remediating, err := r.reconcileUnhealthyMachines(ctx, cluster, tcp, machines.Items, etcdStatuses)
	if remediating {
		return result, err
	}

	machinesResult, machinesErr := r.reconcileMachines(ctx, cluster, tcp, machines, etcdStatuses)

	return util.LowestNonZeroResult(result, machinesResult), kerrors.NewAggregate([]error{err, machinesErr})
}

// reconcileUnhealthyMachines remediates the control plane Machines that a MachineHealthCheck marked as unhealthy
// (HealthCheckSucceeded=False) and left to their owner to remediate (OwnerRemediated=False). It is modelled on the
// KubeadmControlPlane remediation, and like it, remediating a Machine means deleting it and nothing else: its etcd
// member is removed by the pre-terminate hook once the Machine is drained (reconcileMachinePreTerminateHooks), which
// checks again that etcd keeps quorum, and the regular scale up then creates a replacement.
//
// A Machine is only remediated when:
//   - no other remediation is in progress and the retry limits of spec.remediation allow it,
//   - the cluster topology, if the Cluster has one, is not waiting to propagate a version upgrade,
//   - no control plane Machine is being updated in place,
//   - once the control plane is initialized: it has more than one Machine, no other Machine is being deleted or
//     still provisioning, and removing the etcd member of the Machine keeps etcd quorum, judged from what the etcd
//     health check of this reconcile recorded (etcdStatuses) rather than by inspecting etcd again,
//   - before the control plane is initialized: see remediationBeforeInitialization.
//
// Otherwise the reason is surfaced in the OwnerRemediated condition of the Machine. At most one Machine is deleted
// per reconcile; remediating reports whether remediation acted on a Machine in this reconcile, by deleting it or
// trying to: no other Machine may be deleted in the same reconcile then.
func (r *TalosControlPlaneReconciler) reconcileUnhealthyMachines(
	ctx context.Context,
	cluster *clusterv1.Cluster,
	tcp *controlplanev1.TalosControlPlane,
	machineList []clusterv1.Machine,
	etcdStatuses map[string]etcdMachineStatus,
) (_ ctrl.Result, remediating bool, retErr error) {
	log := r.Log.WithValues("namespace", tcp.Namespace, "talosControlPlane", tcp.Name)
	now := time.Now().UTC()

	machines := make([]*clusterv1.Machine, 0, len(machineList))
	for i := range machineList {
		machines = append(machines, &machineList[i])
	}

	r.reconcileRemediationInProgress(tcp, machines, now, log)

	// The MachineHealthCheck leaves OwnerRemediated to the owner once set: remove it from Machines that are healthy
	// again, so that a Machine that recovered while its remediation was deferred is not deleted later.
	if err := r.cleanupOwnerRemediated(ctx, machines); err != nil {
		return ctrl.Result{}, false, err
	}

	target := machineToBeRemediated(tcp, machines, etcdStatuses)
	if target == nil || !target.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, false, nil
	}

	log = log.WithValues("machine", target.Name)

	patchHelper, err := patch.NewHelper(target, r.Client)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	defer func() {
		if err := patchHelper.Patch(ctx, target, patch.WithOwnedConditions{Conditions: []string{
			clusterv1.MachineOwnerRemediatedCondition,
		}}); err != nil && !apierrors.IsNotFound(err) {
			retErr = kerrors.NewAggregate([]error{retErr, fmt.Errorf("failed to patch control plane machine %s: %w", target.Name, err)})
		}
	}()

	// setOwnerRemediated says on the Machine why it is not remediated now. A reason that needs an operator is also
	// recorded as an event on the TalosControlPlane; a transient state is waited out quietly.
	setOwnerRemediated := func(reason, message string) {
		log.Info("a control plane machine needs remediation: "+message, "reason", reason)

		conditions.Set(target, metav1.Condition{
			Type:    clusterv1.MachineOwnerRemediatedCondition,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		})

		if reason == cannotBeRemediatedReason {
			r.recordRemediationEvent(tcp, corev1.EventTypeWarning, cannotRemediateMachineEvent,
				fmt.Sprintf("Machine %s needs remediation, but %s", target.Name, message))
		}
	}

	if remediationInProgress(tcp) {
		setOwnerRemediated(remediationDeferredReason, "TalosControlPlane waiting for the previous remediation to complete before triggering remediation")

		return ctrl.Result{}, false, nil
	}

	// A version upgrade that the topology controller is about to propagate to the TalosControlPlane will roll the
	// machines out anyway: don't replace one with the old version first. Like KCP, the intermediate step of an
	// upgrade in progress is allowed, since it is what the TalosControlPlane is already at.
	if message := topologyUpgradePending(cluster, tcp); message != "" {
		setOwnerRemediated(remediationDeferredReason, message)

		return ctrl.Result{}, false, nil
	}

	data, reason, message := checkRemediationRetryLimits(target, remediationLimitsFor(tcp.Spec.Remediation), now, log)
	if reason != "" {
		setOwnerRemediated(reason, message)

		return ctrl.Result{}, false, nil
	}

	// An in-place update is a transitional state like provisioning: the machine being updated has its components
	// restarted or its node rebooted, and remediating a machine mid-update, the updated one included, would need the
	// update to be cancelled first.
	for _, m := range machines {
		if isUpdatingInPlace(m) {
			setOwnerRemediated(remediationDeferredReason, fmt.Sprintf("TalosControlPlane waiting for the in-place update of control plane machine %s to complete before triggering remediation", m.Name))

			return ctrl.Result{}, false, nil
		}
	}

	if ptr.Deref(tcp.Status.Initialization.ControlPlaneInitialized, false) {
		// The checks that apply once the control plane is initialized: like KCP, the TalosControlPlane only
		// remediates when it can safely assume that the operation preserves the state of the existing cluster, or at
		// least does not make it worse.
		if len(machines) <= 1 {
			setOwnerRemediated(cannotBeRemediatedReason, "TalosControlPlane can't remediate if current replicas are less or equal to 1")

			return ctrl.Result{}, false, nil
		}

		for _, m := range machines {
			if m.Name == target.Name {
				continue
			}

			// One membership change at a time, as for any other deletion: the pre-terminate hook removes the etcd
			// member of a deleting Machine, and the quorum check below can't account for a member that is on its
			// way out.
			if !m.DeletionTimestamp.IsZero() {
				setOwnerRemediated(remediationDeferredReason, fmt.Sprintf("TalosControlPlane waiting for the deletion of control plane machine %s to complete before triggering remediation", m.Name))

				return ctrl.Result{}, false, nil
			}

			if !collections.IsUnhealthy(m) && !m.Status.NodeRef.IsDefined() {
				setOwnerRemediated(remediationDeferredReason, fmt.Sprintf("TalosControlPlane waiting for the provisioning of control plane machine %s to complete before triggering remediation", m.Name))

				return ctrl.Result{}, false, nil
			}
		}

		safe, details, err := canSafelyRemediateMachine(target, machines, etcdStatuses)
		if err != nil {
			log.Info("failed to check etcd cluster health before remediation", "error", err)
			setOwnerRemediated(remediationDeferredReason, "Failed to check etcd cluster health before remediation")

			return ctrl.Result{}, false, nil
		}

		log.Info("checked etcd quorum before remediation", "safe", safe, "details", details)

		if !safe {
			setOwnerRemediated(cannotBeRemediatedReason, "TalosControlPlane can't remediate this machine because this could result in etcd quorum loss: "+details)

			return ctrl.Result{}, false, nil
		}
	} else if reason, message := r.remediationBeforeInitialization(ctx, tcp, target, machines); reason != "" {
		setOwnerRemediated(reason, message)

		return ctrl.Result{}, false, nil
	}

	value, err := data.marshal()
	if err != nil {
		return ctrl.Result{}, false, err
	}

	// Record the remediation before the Machine is deleted, rather than in the deferred patch of the
	// TalosControlPlane: if the controller stopped, or the deferred patch failed, after the Machine was deleted, the
	// remediation would be lost, and with it the retry count and what keeps other Machines from being remediated
	// before the replacement is up. If the deletion doesn't happen, reconcileRemediationInProgress withdraws the
	// record.
	if err := r.patchRemediationInProgress(ctx, tcp, value); err != nil {
		setOwnerRemediated(remediationInternalErrorReason, "Failed to record the remediation, please check controller logs for errors")

		return ctrl.Result{}, true, err
	}

	// Deleting the Machine is the whole remediation, exactly as for a scale down or a `kubectl delete machine`: the
	// pre-terminate hook removes the etcd member once the node is drained, holding the deletion if quorum would not
	// survive it, and the core Machine controller deletes the Node once the infrastructure is gone.
	if _, err := r.deleteControlPlaneMachine(ctx, target); err != nil {
		setOwnerRemediated(remediationInternalErrorReason, "Failed to delete the machine, please check controller logs for errors")

		return ctrl.Result{}, true, fmt.Errorf("failed to delete unhealthy machine %s: %w", target.Name, err)
	}

	message = fmt.Sprintf("Deleting unhealthy control plane machine %s to remediate it, a replacement will be created (retry %d of %d)",
		target.Name, data.RetryCount, remediationLimitsFor(tcp.Spec.Remediation).maxRetry)

	log.Info(message, "retryCount", data.RetryCount)
	r.recordRemediationEvent(target, corev1.EventTypeNormal, remediatingMachineEvent, message)

	conditions.Set(target, metav1.Condition{
		Type:    clusterv1.MachineOwnerRemediatedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  remediationMachineDeletingReason,
		Message: "Machine is deleting",
	})

	return ctrl.Result{RequeueAfter: remediationRequeueAfter}, true, nil
}

// topologyUpgradePending returns why remediation must wait for the cluster topology, or "" if it needn't: when the
// Cluster has a topology whose version differs from the TalosControlPlane's, the topology controller is about to
// upgrade the control plane, unless the TalosControlPlane is already at the version of the current upgrade step.
// Same rule as KCP's.
func topologyUpgradePending(cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) string {
	if cluster == nil || !cluster.Spec.Topology.IsDefined() {
		return ""
	}

	stepVersion := cluster.GetAnnotations()[clusterv1.ClusterTopologyUpgradeStepAnnotation]

	if cluster.Spec.Topology.Version == tcp.Spec.Version || (stepVersion != "" && stepVersion == tcp.Spec.Version) {
		return ""
	}

	version := cluster.Spec.Topology.Version
	if stepVersion != "" {
		version = stepVersion
	}

	return fmt.Sprintf("TalosControlPlane can't remediate while waiting for a version upgrade to %s to be propagated from Cluster.spec.topology", version)
}

// remediationBeforeInitialization decides whether an unhealthy Machine may be remediated before the control plane is
// initialized, when the checks that protect a running cluster can't be run. It returns the reason and message to
// refuse or defer with, or an empty reason to go ahead.
//
// Until a machine is bootstrapped, no etcd exists anywhere and there is nothing to lose: a first machine that never
// comes up is simply replaced, as KCP replaces a failed init machine. Once a machine was bootstrapped, its etcd holds
// the cluster, so a machine is only replaced when it never joined the control plane (no node yet) and another machine
// is known to hold etcd data, so that the bootstrapped etcd survives the deletion whichever machine was bootstrapped.
// A machine that registered a node has the API server answering: initialization is imminent, and the regular checks
// apply then. The bootstrapped machine itself, failing before the API server comes up, needs an operator, like a
// single-replica control plane does for KCP.
func (r *TalosControlPlaneReconciler) remediationBeforeInitialization(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	target *clusterv1.Machine,
	machines []*clusterv1.Machine,
) (reason, message string) {
	if !tcp.Status.Bootstrapped {
		return "", ""
	}

	if target.Status.NodeRef.IsDefined() {
		return remediationDeferredReason, "TalosControlPlane waiting for the control plane to be initialized before triggering remediation"
	}

	if len(machines) <= 1 {
		return cannotBeRemediatedReason, "TalosControlPlane can't remediate if current replicas are less or equal to 1"
	}

	holder, err := r.etcdDataHolder(ctx, tcp, target, machines)
	if holder == "" {
		message := "TalosControlPlane can't remediate before the control plane is initialized: no other control plane machine is known to hold the bootstrapped etcd data"
		if err != nil {
			message += ": " + err.Error()
		}

		return cannotBeRemediatedReason, message
	}

	r.Log.Info("the bootstrapped etcd data is held by another machine, remediating before the control plane is initialized",
		"machine", target.Name, "etcdDataHolder", holder)

	return "", ""
}

// etcdDataHolder returns the name of a control plane machine other than the target that holds an etcd data directory,
// or "" if none is known to, along with why the others could not be checked.
func (r *TalosControlPlaneReconciler) etcdDataHolder(ctx context.Context, tcp *controlplanev1.TalosControlPlane, target *clusterv1.Machine, machines []*clusterv1.Machine) (string, error) {
	var errs []error

	for _, m := range machines {
		if m.Name == target.Name || !m.DeletionTimestamp.IsZero() {
			continue
		}

		holds, err := r.hasEtcdDataOn(ctx, tcp, *m)
		if err != nil {
			errs = append(errs, fmt.Errorf("machine %q: %w", m.Name, err))

			continue
		}

		if holds {
			return m.Name, nil
		}
	}

	return "", kerrors.NewAggregate(errs)
}

// hasEtcdDataOn reports whether the machine holds an etcd data directory, i.e. it was bootstrapped or joined etcd,
// the same way bootstrapCluster tells a bootstrapped cluster. Tests substitute it through
// TalosControlPlaneReconciler.etcdDataChecker.
func (r *TalosControlPlaneReconciler) hasEtcdDataOn(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine clusterv1.Machine) (bool, error) {
	if r.etcdDataChecker != nil {
		return r.etcdDataChecker(ctx, tcp, machine)
	}

	c, err := r.talosconfigForMachines(ctx, tcp, machine)
	if err != nil {
		return false, err
	}

	defer c.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	return hasEtcdData(ctx, c)
}

// patchRemediationInProgress sets the remediation-in-progress annotation of the TalosControlPlane to value and
// patches it right away. Only the annotation is patched; the other changes made to the TalosControlPlane in this
// reconcile are left to the deferred patch.
func (r *TalosControlPlaneReconciler) patchRemediationInProgress(ctx context.Context, tcp *controlplanev1.TalosControlPlane, value string) error {
	before := tcp.DeepCopy()
	after := tcp.DeepCopy()

	annotations.AddAnnotations(after, map[string]string{remediationInProgressAnnotation: value})

	if err := r.Patch(ctx, after, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to patch the remediation-in-progress annotation: %w", err)
	}

	tcp.Annotations = after.Annotations

	return nil
}

// clearRemediationInProgress removes the remediation-in-progress annotation of the TalosControlPlane and patches it
// right away, once the Machine replacing the remediated one exists. Patching immediately rather than at the end of
// the reconcile keeps a crash in between from blocking the next remediation; reconcileRemediationInProgress drops
// the annotation anyway once it sees a Machine newer than the remediation.
func (r *TalosControlPlaneReconciler) clearRemediationInProgress(ctx context.Context, tcp *controlplanev1.TalosControlPlane) error {
	if !remediationInProgress(tcp) {
		return nil
	}

	before := tcp.DeepCopy()
	after := tcp.DeepCopy()

	delete(after.Annotations, remediationInProgressAnnotation)

	if err := r.Patch(ctx, after, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to remove the remediation-in-progress annotation: %w", err)
	}

	tcp.Annotations = after.Annotations

	return nil
}

// cleanupOwnerRemediated removes the OwnerRemediated condition from Machines whose MachineHealthCheck reports
// them as healthy again.
func (r *TalosControlPlaneReconciler) cleanupOwnerRemediated(ctx context.Context, machines []*clusterv1.Machine) error {
	var errs []error

	for _, m := range machines {
		if !m.DeletionTimestamp.IsZero() ||
			!conditions.IsTrue(m, clusterv1.MachineHealthCheckSucceededCondition) ||
			!conditions.IsFalse(m, clusterv1.MachineOwnerRemediatedCondition) {
			continue
		}

		patchHelper, err := patch.NewHelper(m, r.Client)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		conditions.Delete(m, clusterv1.MachineOwnerRemediatedCondition)

		if err := patchHelper.Patch(ctx, m, patch.WithOwnedConditions{Conditions: []string{
			clusterv1.MachineOwnerRemediatedCondition,
		}}); err != nil {
			errs = append(errs, err)
		}
	}

	return kerrors.NewAggregate(errs)
}

// reconcileRemediationInProgress removes the remediation-in-progress annotation when it is stale. The annotation is
// normally removed by bootControlPlane when it creates the Machine replacing the remediated one (see
// desiredMachineAnnotations, which records the remediation on that Machine), so a Machine newer than the remediation
// means the controller stopped in between. The annotation is also stale when the remediated Machine is gone and the
// control plane has all the Machines it wants, so no replacement is coming, and when the remediation never started:
// the remediated Machine is still not being deleted remediationStartTimeout after the remediation was recorded.
//
// The annotation is removed from the object in memory; the deferred patch of the reconcile persists it, as in KCP.
func (r *TalosControlPlaneReconciler) reconcileRemediationInProgress(
	tcp *controlplanev1.TalosControlPlane,
	machines []*clusterv1.Machine,
	now time.Time,
	log logr.Logger,
) {
	value, ok := tcp.Annotations[remediationInProgressAnnotation]
	if !ok {
		return
	}

	data, err := remediationDataFromAnnotation(value)
	if err != nil {
		log.Info("removing invalid remediation-in-progress annotation", "error", err)
		delete(tcp.Annotations, remediationInProgressAnnotation)

		return
	}

	var (
		remediated  *clusterv1.Machine
		nonDeleting int32
	)

	for _, m := range machines {
		if m.CreationTimestamp.After(data.Timestamp.Time) {
			log.Info("removing the stale remediation-in-progress annotation, a machine newer than the remediation exists",
				"remediatedMachine", data.Machine, "newerMachine", m.Name)
			delete(tcp.Annotations, remediationInProgressAnnotation)

			return
		}

		if m.Name == data.Machine {
			remediated = m
		}

		if m.DeletionTimestamp.IsZero() {
			nonDeleting++
		}
	}

	switch {
	case remediated == nil && nonDeleting >= tcp.Spec.GetReplicas():
		log.Info("removing the remediation-in-progress annotation, the remediated machine is gone and no replacement is needed",
			"remediatedMachine", data.Machine, "replicas", tcp.Spec.GetReplicas())
	// The remediation is recorded before the Machine is deleted. A remediated Machine that is still not being deleted
	// means that the deletion failed or the controller stopped in between: the remediation did not happen.
	case remediated != nil && remediated.DeletionTimestamp.IsZero() && now.After(data.Timestamp.Add(remediationStartTimeout)):
		log.Info("removing the remediation-in-progress annotation, the remediated machine was not deleted", "remediatedMachine", data.Machine)
	default:
		return
	}

	delete(tcp.Annotations, remediationInProgressAnnotation)
}

// machineToBeRemediated returns the Machine to remediate first among the Machines controlled by the
// TalosControlPlane that a MachineHealthCheck marked for remediation by their owner, picking the most broken one
// like KubeadmControlPlane: a Machine with the remediate-machine annotation, then a Machine that failed to provision
// (no nodeRef), then a Machine whose etcd member the health check of this reconcile did not see healthy (etcdStatuses
// has no healthy entry for it), then the oldest Machine.
func machineToBeRemediated(tcp *controlplanev1.TalosControlPlane, machines []*clusterv1.Machine, etcdStatuses map[string]etcdMachineStatus) *clusterv1.Machine {
	var candidates []*clusterv1.Machine

	for _, m := range machines {
		if metav1.IsControlledBy(m, tcp) && collections.IsUnhealthyAndOwnerRemediated(m) {
			candidates = append(candidates, m)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// first orders a before b when only a has the property.
	first := func(a, b bool) int {
		switch {
		case a == b:
			return 0
		case a:
			return -1
		default:
			return 1
		}
	}

	slices.SortStableFunc(candidates, func(a, b *clusterv1.Machine) int {
		if c := first(annotations.HasRemediateMachine(a), annotations.HasRemediateMachine(b)); c != 0 {
			return c
		}

		if c := first(!a.Status.NodeRef.IsDefined(), !b.Status.NodeRef.IsDefined()); c != 0 {
			return c
		}

		if c := first(!etcdStatuses[a.Name].healthy, !etcdStatuses[b.Name].healthy); c != 0 {
			return c
		}

		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return first(a.CreationTimestamp.Before(&b.CreationTimestamp), b.CreationTimestamp.Before(&a.CreationTimestamp))
		}

		return strings.Compare(a.Name, b.Name)
	})

	return candidates[0]
}

// checkRemediationRetryLimits returns the remediation data to record for the target, or the reason and message
// why the retry limits don't allow to remediate it now.
//
// A remediation of a Machine that replaced a Machine remediated less than the min healthy period ago is a retry of
// that remediation: it waits for the retry period after the previous one, and stops after the max number of retries,
// so that a Machine that always fails, e.g. because of a quota issue, isn't replaced forever.
func checkRemediationRetryLimits(target *clusterv1.Machine, limits remediationLimits, now time.Time, log logr.Logger) (data *remediationData, reason, message string) {
	data = &remediationData{
		Machine:   target.Name,
		Timestamp: metav1.Time{Time: now},
	}

	value, ok := target.Annotations[remediationForAnnotation]
	if !ok {
		return data, "", ""
	}

	last, err := remediationDataFromAnnotation(value)
	if err != nil {
		log.Info("ignoring invalid remediation-for annotation", "error", err)

		return data, "", ""
	}

	// A replacement that stayed healthy for the min healthy period starts a new retry sequence.
	if !last.Timestamp.Add(limits.minHealthyPeriod).After(now) {
		return data, "", ""
	}

	data.RetryCount = last.RetryCount

	if last.Timestamp.Add(limits.retryPeriod).After(now) {
		return data, remediationDeferredReason, fmt.Sprintf("TalosControlPlane can't remediate this machine because the operation already failed in the latest %s (retryPeriodSeconds)", limits.retryPeriod)
	}

	if data.RetryCount >= limits.maxRetry {
		return data, cannotBeRemediatedReason, fmt.Sprintf("TalosControlPlane can't remediate this machine because the operation already failed %d times (maxRetry)", limits.maxRetry)
	}

	data.RetryCount++

	return data, "", ""
}

// recordedEtcdFrom returns, from what the etcd health check of this reconcile recorded on each Machine (etcdStatuses),
// which machines run a healthy etcd service, and the etcd member list one of them reported: the list a healthy
// machine other than last reported, or last's own when it is the only healthy machine that reported one. It returns
// an error if no healthy machine reported a member list.
//
// The health check inspects every Machine that is neither being deleted nor leaving etcd, so a Machine without a
// recorded status is one that could not be inspected, and counts as unhealthy, as does one without a nodeRef.
func recordedEtcdFrom(machines []*clusterv1.Machine, etcdStatuses map[string]etcdMachineStatus, last *clusterv1.Machine) ([]*machineapi.EtcdMember, map[string]bool, error) {
	healthy := make(map[string]bool, len(machines))

	for _, m := range machines {
		healthy[m.Name] = m.Status.NodeRef.IsDefined() && etcdStatuses[m.Name].healthy
	}

	candidates := make([]*clusterv1.Machine, 0, len(machines)+1)

	for _, m := range machines {
		if last == nil || m.Name != last.Name {
			candidates = append(candidates, m)
		}
	}

	if last != nil {
		candidates = append(candidates, last)
	}

	var listErr error

	for _, m := range candidates {
		if !healthy[m.Name] {
			continue
		}

		status := etcdStatuses[m.Name]
		if len(status.members) > 0 {
			return status.members, healthy, nil
		}

		if status.listErr != nil {
			listErr = status.listErr
		}
	}

	if listErr == nil {
		listErr = errors.New("no healthy etcd member reported the etcd member list")
	}

	return nil, healthy, listErr
}

// canSafelyRemediateMachine checks whether the etcd member of the target can be removed without losing etcd quorum,
// from what the etcd health check of this reconcile recorded (see recordedEtcd). The pre-terminate hook runs the
// same kind of check again, against fresh statuses, before it removes the member.
func canSafelyRemediateMachine(target *clusterv1.Machine, machines []*clusterv1.Machine, etcdStatuses map[string]etcdMachineStatus) (bool, string, error) {
	members, healthy, err := recordedEtcdFrom(machines, etcdStatuses, target)
	if err != nil {
		return false, "", err
	}

	safe, details := canSafelyRemoveEtcdMember(members, target, machines, healthy)

	return safe, details, nil
}

// canSafelyAddEtcdMember checks whether a new etcd member can join without putting etcd quorum at risk, from what
// the etcd health check of this reconcile recorded (see recordedEtcd). It is what lets the replacement of a
// remediated Machine be created while the control plane is not fully healthy (see preflightChecksForScaleUp).
func canSafelyAddEtcdMember(machines []*clusterv1.Machine, etcdStatuses map[string]etcdMachineStatus) (bool, string, error) {
	members, healthy, err := recordedEtcdFrom(machines, etcdStatuses, nil)
	if err != nil {
		return false, "", err
	}

	safe, details := canSafelyAddEtcdMemberTo(members, machines, healthy)

	return safe, details, nil
}

// canSafelyRemoveEtcdMember reports whether the etcd member of the target can be removed without losing etcd quorum,
// like KubeadmControlPlane's targetEtcdClusterHealthy. healthy tells which machines run a healthy etcd service.
//
// Removing a member is itself a change that needs a quorum of the current voting members to commit, and the remaining
// voting members then need a quorum of their own: in a 3 member cluster, the 2 other members must be healthy, in a 5
// member cluster, 3 of the 4 others. Learners block remediation until they are promoted. Members that can't be matched
// to a machine count as unhealthy; when the target's member can't be identified, it is assumed to be one of them, so
// anything unknown makes the check stricter.
func canSafelyRemoveEtcdMember(members []*machineapi.EtcdMember, target *clusterv1.Machine, machines []*clusterv1.Machine, healthy map[string]bool) (bool, string) {
	var (
		learners, remainingVoting, remainingHealthy int
		targetMember                                *machineapi.EtcdMember
	)

	for _, member := range members {
		if member.IsLearner {
			learners++

			continue
		}

		if targetMember == nil && etcdMemberMatchesMachine(member, target) {
			targetMember = member

			continue
		}

		remainingVoting++

		for _, m := range machines {
			if m.Name != target.Name && healthy[m.Name] && etcdMemberMatchesMachine(member, m) {
				remainingHealthy++

				break
			}
		}
	}

	if learners > 0 {
		return false, fmt.Sprintf("%d etcd members are learners that did not join the cluster yet", learners)
	}

	currentVoting, currentHealthy := remainingVoting, remainingHealthy
	if targetMember != nil {
		currentVoting++

		if healthy[target.Name] {
			currentHealthy++
		}
	}

	if quorum := currentVoting/2 + 1; currentHealthy < quorum {
		return false, fmt.Sprintf("%d of %d etcd members are healthy, removing a member needs %d", currentHealthy, currentVoting, quorum)
	}

	if quorum := remainingVoting/2 + 1; remainingHealthy < quorum {
		return false, fmt.Sprintf("%d of the %d remaining etcd members would be healthy, quorum needs %d", remainingHealthy, remainingVoting, quorum)
	}

	return true, fmt.Sprintf("%d of the %d remaining etcd members are healthy", remainingHealthy, remainingVoting)
}

// canSafelyAddEtcdMemberTo reports whether one more etcd member can join without putting quorum at risk, like
// KubeadmControlPlane's targetEtcdClusterHealthy with addEtcdMember. healthy tells which machines run a healthy etcd
// service.
//
// The new member votes as soon as it is promoted and is assumed unhealthy until then, the worst case, except when
// the cluster has at most one member, where assuming the worst would make growing it impossible. Learners block the
// addition until they are promoted. Members that can't be matched to a healthy machine count as unhealthy.
func canSafelyAddEtcdMemberTo(members []*machineapi.EtcdMember, machines []*clusterv1.Machine, healthy map[string]bool) (bool, string) {
	var learners, voting, healthyMembers int

	for _, member := range members {
		if member.IsLearner {
			learners++

			continue
		}

		voting++

		for _, m := range machines {
			if healthy[m.Name] && etcdMemberMatchesMachine(member, m) {
				healthyMembers++

				break
			}
		}
	}

	if learners > 0 {
		return false, fmt.Sprintf("%d etcd members are learners that did not join the cluster yet", learners)
	}

	targetVoting, targetHealthy := voting+1, healthyMembers
	if voting <= 1 {
		targetHealthy++
	}

	if quorum := targetVoting/2 + 1; targetHealthy < quorum {
		return false, fmt.Sprintf("%d of %d etcd members would be healthy with the new member, quorum needs %d", targetHealthy, targetVoting, quorum)
	}

	return true, fmt.Sprintf("%d of %d etcd members would be healthy with the new member", targetHealthy, targetVoting)
}

// reconcileRemediationStatus surfaces the remediation state on the TalosControlPlane: the Remediating condition,
// aggregated from the OwnerRemediated conditions of the Machines to remediate, and status.lastRemediation, the
// remediation in progress or the most recent one recorded on a Machine. Like KCP.
func (r *TalosControlPlaneReconciler) reconcileRemediationStatus(tcp *controlplanev1.TalosControlPlane, machines collections.Machines) {
	log := r.Log.WithValues("namespace", tcp.Namespace, "talosControlPlane", tcp.Name)

	setRemediatingCondition(tcp, machines.Filter(collections.IsUnhealthyAndOwnerRemediated), machines.Filter(collections.IsUnhealthy), log)
	setLastRemediation(tcp, machines, log)
}

func setRemediatingCondition(tcp *controlplanev1.TalosControlPlane, machinesToBeRemediated, unhealthyMachines collections.Machines, log logr.Logger) {
	if len(machinesToBeRemediated) == 0 {
		var message string

		if len(unhealthyMachines) > 0 {
			names := unhealthyMachines.Names()
			sort.Strings(names)

			message = fmt.Sprintf("Machines %s are not healthy (not to be remediated by TalosControlPlane)", strings.Join(names, ", "))
			if len(names) == 1 {
				message = fmt.Sprintf("Machine %s is not healthy (not to be remediated by TalosControlPlane)", names[0])
			}
		}

		conditions.Set(tcp, metav1.Condition{
			Type:    controlplanev1.RemediatingCondition,
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.NotRemediatingReason,
			Message: message,
		})

		return
	}

	remediatingCondition, err := conditions.NewAggregateCondition(
		machinesToBeRemediated.UnsortedList(), clusterv1.MachineOwnerRemediatedCondition,
		conditions.TargetConditionType(controlplanev1.RemediatingCondition),
	)
	if err != nil {
		log.Error(err, "failed to aggregate the OwnerRemediated conditions of the machines")

		conditions.Set(tcp, metav1.Condition{
			Type:    controlplanev1.RemediatingCondition,
			Status:  metav1.ConditionUnknown,
			Reason:  controlplanev1.RemediatingInternalErrorReason,
			Message: "Please check controller logs for errors",
		})

		return
	}

	conditions.Set(tcp, metav1.Condition{
		Type:    controlplanev1.RemediatingCondition,
		Status:  metav1.ConditionTrue,
		Reason:  controlplanev1.RemediatingReason,
		Message: remediatingCondition.Message,
	})
}

// setLastRemediation surfaces the remediation in progress, if any, or else the most recent remediation recorded on
// the Machines, in status.lastRemediation. An unreadable record is skipped rather than fail the status.
func setLastRemediation(tcp *controlplanev1.TalosControlPlane, machines collections.Machines, log logr.Logger) {
	var last *remediationData

	if value, ok := tcp.Annotations[remediationInProgressAnnotation]; ok {
		data, err := remediationDataFromAnnotation(value)
		if err != nil {
			log.Info("ignoring invalid remediation-in-progress annotation", "error", err)
		} else {
			last = data
		}
	}

	if last == nil {
		for _, m := range machines.UnsortedList() {
			value, ok := m.Annotations[remediationForAnnotation]
			if !ok {
				continue
			}

			data, err := remediationDataFromAnnotation(value)
			if err != nil {
				log.Info("ignoring invalid remediation-for annotation", "machine", m.Name, "error", err)

				continue
			}

			if last == nil || last.Timestamp.Before(&data.Timestamp) {
				last = data
			}
		}
	}

	if last != nil {
		tcp.Status.LastRemediation = last.toStatus()
	}
}

// recordRemediationEvent records an event of the remediation on the object, if a recorder is wired up.
func (r *TalosControlPlaneReconciler) recordRemediationEvent(obj client.Object, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(obj, nil, eventType, reason, "Remediate", "%s", message)
}
