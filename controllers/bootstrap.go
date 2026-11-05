// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// bootstrapRetryAfter is how long to wait before looking at the bootstrap again while the first
// machine is still coming up.
const bootstrapRetryAfter = 20 * time.Second

// initializeControlPlane creates the first control plane machine, the one that will be
// bootstrapped, as KCP's initializeControlPlane creates the machine that runs `kubeadm init`.
// There are no preflight checks: there is no control plane yet to protect.
func (r *TalosControlPlaneReconciler) initializeControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, controlPlane *ControlPlane) (ctrl.Result, error) {
	r.Log.Info("initializing control plane", "Desired", tcp.Spec.GetReplicas(), "Existing", len(controlPlane.Machines))

	result, err := r.bootControlPlane(ctx, cluster, tcp, controlPlane)
	if err != nil {
		r.recordEvent(tcp, corev1.EventTypeWarning, "FailedInitialization", "Initialize",
			fmt.Sprintf("Failed to create initial control plane Machine for cluster %s: %v", klog.KObj(cluster), err))

		return result, err
	}

	r.Log.Info("created the first control plane machine, it is bootstrapped once it reports an address")

	return result, nil
}

// reconcileBootstrap makes sure that exactly one control plane machine received the Talos
// bootstrap request, the one that starts etcd and that every other machine joins. It is the
// counterpart of what the Kubeadm bootstrap provider does for the first KCP machine with
// `kubeadm init` and its init lock:
//
//   - The oldest machine that can be reached is bootstrapped as soon as it can be, before any
//     other machine is created: the scale-up preflight checks wait for its node, and nothing can
//     register a node until etcd and the API server run.
//   - A machine that already holds an etcd data directory is taken as bootstrapped instead,
//     and no machine is bootstrapped while another one cannot be asked: two bootstrapped
//     machines are two etcd clusters.
//   - status.bootstrappedMachine records the machine, as the lock records the init machine.
//     Should it be gone before the control plane is initialized, the lock is released: the
//     remaining machines are checked for etcd data, the machine holding it is recorded, and
//     when none does another machine is bootstrapped. Once the control plane is initialized the
//     record only informs; the regular etcd checks protect the cluster from then on.
//
// It returns a non-zero result while the bootstrap is pending.
func (r *TalosControlPlaneReconciler) reconcileBootstrap(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (ctrl.Result, error) {
	if tcp.Status.Bootstrapped {
		if ptr.Deref(tcp.Status.Initialization.ControlPlaneInitialized, false) || tcp.Status.BootstrappedMachine == "" {
			return ctrl.Result{}, nil
		}

		// The lock is held for as long as the Machine exists, deleting or not: a deleting
		// machine may still be the only one running etcd.
		if slices.ContainsFunc(machines, func(m clusterv1.Machine) bool { return m.Name == tcp.Status.BootstrappedMachine }) {
			return ctrl.Result{}, nil
		}

		if result, released := r.releaseBootstrap(ctx, tcp, machines); !released {
			return result, nil
		}
	}

	return r.bootstrapControlPlane(ctx, tcp, machines)
}

// bootstrapControlPlane bootstraps the oldest machine that can be reached, unless a machine is
// found to hold etcd data already.
func (r *TalosControlPlaneReconciler) bootstrapControlPlane(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (ctrl.Result, error) {
	candidates := bootstrapCandidates(machines)
	if len(candidates) == 0 {
		return r.holdBootstrap(tcp, controlplanev1.WaitingForTalosBootReason,
			"Waiting for the first control plane machine to report an address"), nil
	}

	holder, err := r.etcdDataHolderAmong(ctx, tcp, candidates)
	if holder != nil {
		r.recordBootstrapped(tcp, holder, "etcd data found on the machine")

		return ctrl.Result{}, nil
	}

	if err != nil {
		// Safe side: a machine that cannot be asked may be the one that was bootstrapped.
		return r.holdBootstrap(tcp, controlplanev1.WaitingForTalosBootReason,
			"Cannot tell whether a control plane machine already holds etcd data: "+err.Error()), nil
	}

	target := candidates[0]

	r.Log.Info("bootstrapping the control plane", "machine", target.Name, "address", machineBootstrapAddress(target))

	if err := r.bootstrapMachine(ctx, tcp, target); err != nil {
		return r.holdBootstrap(tcp, controlplanev1.WaitingForTalosBootReason,
			fmt.Sprintf("Failed to bootstrap control plane machine %s: %v", target.Name, err)), nil
	}

	r.recordBootstrapped(tcp, &target, "bootstrap request accepted")
	r.recordEvent(tcp, corev1.EventTypeNormal, "ControlPlaneBootstrapped", "Bootstrap",
		fmt.Sprintf("Control plane Machine %s received the bootstrap request", target.Name))

	return ctrl.Result{}, nil
}

// releaseBootstrap handles the bootstrapped machine being gone before the control plane was
// initialized. It re-records the machine found to hold etcd data, if any; otherwise it clears
// the bootstrap so that another machine is bootstrapped, once every remaining machine could be
// asked. It reports whether a new bootstrap is due.
func (r *TalosControlPlaneReconciler) releaseBootstrap(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (ctrl.Result, bool) {
	gone := tcp.Status.BootstrappedMachine
	preamble := fmt.Sprintf("Control plane machine %s is gone before the control plane was initialized", gone)

	var unreachable []string

	for _, m := range machines {
		if m.DeletionTimestamp.IsZero() && machineBootstrapAddress(m) == "" {
			unreachable = append(unreachable, m.Name)
		}
	}

	if len(unreachable) > 0 {
		return r.holdBootstrap(tcp, controlplanev1.BootstrappedMachineGoneReason,
			preamble+"; waiting for machines to report an address before telling whether one holds the etcd data: "+strings.Join(unreachable, ", ")), false
	}

	holder, err := r.etcdDataHolderAmong(ctx, tcp, bootstrapCandidates(machines))
	if holder != nil {
		r.Log.Info("the bootstrapped machine is gone, another machine holds the etcd data", "gone", gone, "machine", holder.Name)
		r.recordBootstrapped(tcp, holder, "etcd data found on the machine after machine "+gone+" was deleted")
		r.recordEvent(tcp, corev1.EventTypeWarning, "BootstrappedMachineGone", "Bootstrap",
			fmt.Sprintf("%s; Machine %s holds the etcd data", preamble, holder.Name))

		return ctrl.Result{}, false
	}

	if err != nil {
		return r.holdBootstrap(tcp, controlplanev1.BootstrappedMachineGoneReason,
			preamble+"; cannot tell whether a remaining machine holds the etcd data: "+err.Error()), false
	}

	r.Log.Info("the bootstrapped machine is gone and no machine holds etcd data, bootstrapping again", "gone", gone)
	r.recordEvent(tcp, corev1.EventTypeWarning, "BootstrappedMachineGone", "Bootstrap",
		preamble+"; no remaining machine holds etcd data, bootstrapping another machine")

	tcp.Status.Bootstrapped = false
	tcp.Status.BootstrappedMachine = ""

	return ctrl.Result{}, true
}

// bootstrapCandidates returns the machines that can be bootstrapped, oldest first: the ones not
// being deleted that report an address.
func bootstrapCandidates(machines []clusterv1.Machine) []clusterv1.Machine {
	candidates := make([]clusterv1.Machine, 0, len(machines))

	for _, m := range machines {
		if !m.DeletionTimestamp.IsZero() || machineBootstrapAddress(m) == "" {
			continue
		}

		candidates = append(candidates, m)
	}

	slices.SortFunc(candidates, func(a, b clusterv1.Machine) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}

		return strings.Compare(a.Name, b.Name)
	})

	return candidates
}

// etcdDataHolderAmong returns the first of the machines that holds an etcd data directory, and
// otherwise why some of them could not be asked.
func (r *TalosControlPlaneReconciler) etcdDataHolderAmong(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (*clusterv1.Machine, error) {
	var errs []error

	for i := range machines {
		holds, err := r.hasEtcdDataOn(ctx, tcp, machines[i])
		if err != nil {
			r.Log.Info("could not check for etcd data on machine", "machine", machines[i].Name, "error", err.Error())
			errs = append(errs, fmt.Errorf("machine %q: %w", machines[i].Name, err))

			continue
		}

		if holds {
			return &machines[i], nil
		}
	}

	return nil, kerrors.NewAggregate(errs)
}

// bootstrapMachine sends the Talos bootstrap request to the machine's node. A node that was
// bootstrapped already answers AlreadyExists, which counts as done. Tests substitute the request
// through TalosControlPlaneReconciler.bootstrapper.
func (r *TalosControlPlaneReconciler) bootstrapMachine(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine clusterv1.Machine) error {
	if r.bootstrapper != nil {
		return r.bootstrapper(ctx, tcp, machine)
	}

	address := machineBootstrapAddress(machine)
	if address == "" {
		return fmt.Errorf("machine %q doesn't have any InternalIP or ExternalIP address yet", machine.Name)
	}

	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	c, err := r.talosconfigForMachines(ctx, tcp, machine)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	if err := c.Bootstrap(talosclient.WithNode(ctx, address), &machineapi.BootstrapRequest{}); err != nil && status.Code(err) != codes.AlreadyExists {
		return errors.Wrap(err, "bootstrap request failed")
	}

	return nil
}

// machineBootstrapAddress returns the address to reach the machine's Talos API on: its first
// InternalIP, or its first ExternalIP, or "" when it reports neither.
func machineBootstrapAddress(machine clusterv1.Machine) string {
	for _, addressType := range []clusterv1.MachineAddressType{clusterv1.MachineInternalIP, clusterv1.MachineExternalIP} {
		for _, addr := range machine.Status.Addresses {
			if addr.Type == addressType {
				return addr.Address
			}
		}
	}

	return ""
}

// recordBootstrapped records machine as the bootstrapped machine.
func (r *TalosControlPlaneReconciler) recordBootstrapped(tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine, why string) {
	r.Log.Info("control plane bootstrapped", "machine", machine.Name, "reason", why)

	tcp.Status.Bootstrapped = true
	tcp.Status.BootstrappedMachine = machine.Name

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.MachinesBootstrapped),
		Status:  metav1.ConditionTrue,
		Reason:  controlplanev1.MachinesBootstrappedReason,
		Message: fmt.Sprintf("Machine %s bootstrapped the control plane", machine.Name),
	})
}

// holdBootstrap records why the bootstrap is waiting and asks for another look later.
func (r *TalosControlPlaneReconciler) holdBootstrap(tcp *controlplanev1.TalosControlPlane, reason, message string) ctrl.Result {
	r.Log.Info("control plane bootstrap pending", "reason", reason, "message", message)

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.MachinesBootstrapped),
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})

	return ctrl.Result{RequeueAfter: bootstrapRetryAfter}
}
