// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// The machine helpers of the bootstrap tests, as later releases define them next to the
// remediation tests.

// newRMMachine returns a Ready control plane machine controlled by tcp that passes its health check, created at the
// given time. It carries the Machine finalizer, so that deleting it leaves it around with a deletion timestamp, as the
// core Machine controller does.
func newRMMachine(tcp *controlplanev1.TalosControlPlane, name string, created time.Time, mutators ...func(*clusterv1.Machine)) *clusterv1.Machine {
	base := func(m *clusterv1.Machine) {
		m.CreationTimestamp = metav1.NewTime(created)
		m.Finalizers = []string{clusterv1.MachineFinalizer}
		m.Status.Conditions = []metav1.Condition{
			{
				Type:               clusterv1.ReadyCondition,
				Status:             metav1.ConditionTrue,
				Reason:             clusterv1.ReadyReason,
				LastTransitionTime: metav1.NewTime(created),
			},
			{
				Type:               clusterv1.MachineHealthCheckSucceededCondition,
				Status:             metav1.ConditionTrue,
				Reason:             clusterv1.MachineHealthCheckSucceededReason,
				LastTransitionTime: metav1.NewTime(created),
			},
		}
	}

	return newPTMachine(tcp, name, append([]func(*clusterv1.Machine){ptHooked, base}, mutators...)...)
}

// rmFlagged marks the machine the way a MachineHealthCheck does when it leaves the remediation to the owner.
func rmFlagged(m *clusterv1.Machine) {
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.MachineHealthCheckSucceededCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.MachineHealthCheckNodeDeletedReason,
	})
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.MachineOwnerRemediatedCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.MachineOwnerRemediatedWaitingForRemediationReason,
	})
	conditions.Set(m, metav1.Condition{
		Type:   clusterv1.ReadyCondition,
		Status: metav1.ConditionFalse,
		Reason: clusterv1.NotReadyReason,
	})
}
