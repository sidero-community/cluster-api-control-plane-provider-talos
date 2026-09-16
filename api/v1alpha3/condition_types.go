// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The condition shape of the v1alpha3 API. It is the Cluster API v1beta1 condition this version
// was built on, defined here so the served schema outlives that package.

// ConditionType is a valid value for Condition.Type.
type ConditionType string

// ConditionSeverity expresses the severity of a Condition Type failing.
type ConditionSeverity string

const (
	// ConditionSeverityError specifies that a condition with `Status=False` is an error.
	ConditionSeverityError ConditionSeverity = "Error"

	// ConditionSeverityWarning specifies that a condition with `Status=False` is a warning.
	ConditionSeverityWarning ConditionSeverity = "Warning"

	// ConditionSeverityInfo specifies that a condition with `Status=False` is informative.
	ConditionSeverityInfo ConditionSeverity = "Info"

	// ConditionSeverityNone is the default severity.
	ConditionSeverityNone ConditionSeverity = ""
)

// Condition defines an observation of a TalosControlPlane's operational state.
type Condition struct {
	// Type of condition in CamelCase.
	Type ConditionType `json:"type"`

	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`

	// Severity provides an explicit classification of Reason code when Status is False.
	// +optional
	Severity ConditionSeverity `json:"severity,omitempty"`

	// LastTransitionTime is the last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// Reason is a brief CamelCase word for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human readable message indicating details about the transition.
	// +optional
	Message string `json:"message,omitempty"`
}

// Conditions is a list of Condition.
type Conditions []Condition
