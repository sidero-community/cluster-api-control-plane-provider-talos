// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3

import clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

// Aliases for the v1beta2 condition surface preserved on the
// TalosControlPlaneV1Beta2Status round-trip holder. These mirror the standard
// CAPI Ready/Available reasons so callers operating on the deprecated v1alpha3
// API can refer to them via named constants.

const (
	// ReadyV1Beta2Condition reports whether the TalosControlPlane is ready.
	ReadyV1Beta2Condition = clusterv1.ReadyCondition

	// ReadyV1Beta2Reason surfaces when the TalosControlPlane is ready.
	ReadyV1Beta2Reason = clusterv1.ReadyReason

	// NotReadyV1Beta2Reason surfaces when the TalosControlPlane is not ready.
	NotReadyV1Beta2Reason = clusterv1.NotReadyReason

	// ReadyUnknownV1Beta2Reason surfaces when the TalosControlPlane readiness is unknown.
	ReadyUnknownV1Beta2Reason = clusterv1.ReadyUnknownReason
)

const (
	// AvailableV1Beta2Condition reports whether the TalosControlPlane is available.
	AvailableV1Beta2Condition = clusterv1.AvailableCondition

	// AvailableV1Beta2Reason surfaces when the TalosControlPlane is available.
	AvailableV1Beta2Reason = clusterv1.AvailableReason

	// NotAvailableV1Beta2Reason surfaces when the TalosControlPlane is not available.
	NotAvailableV1Beta2Reason = clusterv1.NotAvailableReason
)
