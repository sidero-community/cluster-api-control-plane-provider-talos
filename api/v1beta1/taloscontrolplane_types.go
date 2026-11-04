// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

const (
	// TalosControlPlaneFinalizer is the finalizer used by the controller to clean up owned Machines.
	TalosControlPlaneFinalizer = "talos.controlplane.cluster.x-k8s.io/finalizer"

	// TalosControlPlaneFinalizerLegacy is kept temporarily so the controller can migrate
	// existing objects away from the old non-path-qualified finalizer value.
	TalosControlPlaneFinalizerLegacy = "talos.controlplane.cluster.x-k8s.io"

	// RemediationInProgressAnnotation is set on the TalosControlPlane while a remediation is in
	// progress: from right before the unhealthy Machine is deleted until the Machine replacing it
	// is created. Its value is a JSON object with the name of the remediated Machine, the time of
	// the remediation and the retry count of the remediation sequence. While it is set, no other
	// Machine is remediated. Same key as the KubeadmControlPlane's.
	RemediationInProgressAnnotation = "controlplane.cluster.x-k8s.io/remediation-in-progress"

	// RemediationForAnnotation is set on the Machine created to replace a remediated Machine, with
	// the value RemediationInProgressAnnotation had when it was created, so that a failure of the
	// replacement counts as a retry of the same remediation. Same key as the KubeadmControlPlane's.
	RemediationForAnnotation = "controlplane.cluster.x-k8s.io/remediation-for"
)

// Defaults of the remediation limits, applied by the webhook to the fields left empty. They are
// the limits the provider applied before they became configurable.
const (
	// DefaultRemediationMaxRetry is the default maximum number of retries of a remediation sequence.
	DefaultRemediationMaxRetry = int32(3)

	// DefaultRemediationRetryPeriodSeconds is the default minimum time between two retries, 5 minutes.
	DefaultRemediationRetryPeriodSeconds = int32(5 * 60)

	// DefaultRemediationMinHealthyPeriodSeconds is the default time a replacement must stay healthy
	// for its own failure to start a new remediation sequence, 1 hour.
	DefaultRemediationMinHealthyPeriodSeconds = int32(60 * 60)
)

type ControlPlaneConfig struct {
	ControlPlaneConfig cabptv1.TalosConfigSpec `json:"controlplane"`
}

// TalosControlPlaneMachineTemplate defines how control plane Machines should be shaped.
// It mirrors the upstream Cluster API v1beta2 KubeadmControlPlane.MachineTemplate layout:
// metadata + spec, where spec carries the infrastructure reference, readiness gates and
// the machine deletion configuration.
type TalosControlPlaneMachineTemplate struct {
	// ObjectMeta is the standard object's metadata.
	// +optional
	ObjectMeta clusterv1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec is the specification of the desired behavior of the machine template.
	// +optional
	Spec TalosControlPlaneMachineTemplateSpec `json:"spec,omitempty,omitzero"`
}

// TalosControlPlaneMachineTemplateSpec defines the desired Machine spec carried by the
// TalosControlPlane MachineTemplate.
// +kubebuilder:validation:MinProperties=1
type TalosControlPlaneMachineTemplateSpec struct {
	// InfrastructureRef is a required reference to a custom resource offered by an infrastructure provider.
	// For ClusterClass / topology, this field is populated from
	// ClusterClass.spec.controlPlane.machineInfrastructure.templateRef.
	// +required
	InfrastructureRef clusterv1.ContractVersionedObjectReference `json:"infrastructureRef,omitempty,omitzero"`

	// ReadinessGates specifies additional conditions to include when evaluating the Machine
	// Ready condition.
	// +optional
	// +listType=map
	// +listMapKey=conditionType
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	ReadinessGates []clusterv1.MachineReadinessGate `json:"readinessGates,omitempty"`

	// Deletion contains configuration options for Machine deletion.
	// +optional
	Deletion TalosControlPlaneMachineTemplateDeletionSpec `json:"deletion,omitempty,omitzero"`
}

// TalosControlPlaneMachineTemplateDeletionSpec contains configuration options for Machine deletion.
// +kubebuilder:validation:MinProperties=1
type TalosControlPlaneMachineTemplateDeletionSpec struct {
	// NodeDrainTimeoutSeconds is the total amount of time, in seconds, that the controller will
	// spend draining a control plane node. The default value is 0, meaning that the node can be
	// drained without any time limitations.
	// NOTE: NodeDrainTimeoutSeconds is different from `kubectl drain --timeout`.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeDrainTimeoutSeconds *int32 `json:"nodeDrainTimeoutSeconds,omitempty"`

	// NodeVolumeDetachTimeoutSeconds is the total amount of time, in seconds, that the controller
	// will spend waiting for all volumes to be detached. The default value is 0, meaning that the
	// volumes can be detached without any time limitations.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeVolumeDetachTimeoutSeconds *int32 `json:"nodeVolumeDetachTimeoutSeconds,omitempty"`

	// NodeDeletionTimeoutSeconds defines, in seconds, how long the controller will attempt to
	// delete the Node that is hosted by a Machine after the Machine is marked for deletion. A
	// duration of 0 will retry deletion indefinitely. Defaults to 10 seconds when omitted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeDeletionTimeoutSeconds *int32 `json:"nodeDeletionTimeoutSeconds,omitempty"`
}

// RolloutStrategyType defines the rollout strategies for a KubeadmControlPlane.
type RolloutStrategyType string

const (
	// RollingUpdateStrategyType replaces the old control planes by new one using rolling update
	// i.e. gradually scale up or down the old control planes and scale up or down the new one.
	RollingUpdateStrategyType RolloutStrategyType = "RollingUpdate"
	// OnDeleteStrategyType doesn't replace the nodes automatically, but if the machine is removed,
	// new one will be created from the new spec.
	OnDeleteStrategyType RolloutStrategyType = "OnDelete"
)

// TalosControlPlaneSpec defines the desired state of TalosControlPlane
type TalosControlPlaneSpec struct {
	// Number of desired machines. Defaults to 1. When stacked etcd is used only
	// odd numbers are permitted, as per [etcd best practice](https://etcd.io/docs/v3.3.12/faq/#why-an-odd-number-of-cluster-members).
	// This is a pointer to distinguish between explicit zero and not specified.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Version defines the desired Kubernetes version.
	// +kubebuilder:validation:MinLength:=2
	// +kubebuilder:validation:Pattern:=^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)([-0-9a-zA-Z_\.+]*)?$
	Version string `json:"version"`

	// MachineTemplate contains information about how control plane Machines should be shaped.
	// This is the Cluster API v1beta2 control plane contract field consumed by ClusterClass / topology.
	// +optional
	MachineTemplate TalosControlPlaneMachineTemplate `json:"machineTemplate,omitempty,omitzero"`

	// MachineNamingStrategy allows changing the naming pattern used when creating control plane Machines.
	// InfraMachines and bootstrap configs use the same name as the corresponding Machine.
	// +optional
	MachineNamingStrategy *MachineNamingStrategy `json:"machineNamingStrategy,omitempty"`

	// ControlPlaneConfig is a two TalosConfigSpecs
	// to use for initializing and joining machines to the control plane.
	ControlPlaneConfig ControlPlaneConfig `json:"controlPlaneConfig"`

	// The RolloutStrategy to use to replace control plane machines with
	// new ones.
	// +optional
	// +kubebuilder:default={type: "RollingUpdate", rollingUpdate: {maxSurge: 1}}
	RolloutStrategy *RolloutStrategy `json:"rolloutStrategy,omitempty"`

	// remediation controls how unhealthy control plane Machines are remediated. A Machine that a
	// MachineHealthCheck hands to the TalosControlPlane (HealthCheckSucceeded=False and
	// OwnerRemediated=False) is deleted and replaced, within these limits.
	// +optional
	Remediation TalosControlPlaneRemediationSpec `json:"remediation,omitempty,omitzero"`
}

// TalosControlPlaneRemediationSpec configures the remediation of unhealthy control plane Machines.
// It has the same shape as the KubeadmControlPlane's remediation spec, with finite defaults.
type TalosControlPlaneRemediationSpec struct {
	// maxRetry is the maximum number of retries while attempting to remediate an unhealthy machine.
	// A retry happens when a machine that was created as a replacement for an unhealthy machine also fails.
	// For example, given a control plane with three machines M1, M2, M3:
	//
	//	M1 become unhealthy; remediation happens, and M1-1 is created as a replacement.
	//	If M1-1 (replacement of M1) has problems while bootstrapping it will become unhealthy, and then be
	//	remediated; such operation is considered a retry, remediation-retry #1.
	//	If M1-2 (replacement of M1-1) becomes unhealthy, remediation-retry #2 will happen, etc.
	//
	// A retry could happen only after retryPeriodSeconds from the previous retry.
	// If a machine is marked as unhealthy after minHealthyPeriodSeconds from the previous remediation expired,
	// this is not considered a retry anymore because the new issue is assumed unrelated from the previous one.
	//
	// Defaults to 3, so that a machine that keeps failing for the same reason, a quota or an image
	// problem say, is not replaced forever.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxRetry *int32 `json:"maxRetry,omitempty"`

	// retryPeriodSeconds is the duration, in seconds, that the TalosControlPlane waits before
	// remediating a machine being created as a replacement for an unhealthy machine (a retry).
	//
	// Defaults to 300.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetryPeriodSeconds *int32 `json:"retryPeriodSeconds,omitempty"`

	// minHealthyPeriodSeconds defines the duration, in seconds, after which the TalosControlPlane
	// considers any failure of a machine unrelated to the previous one. In this case the remediation
	// is not considered a retry anymore, and thus the retry counter restarts from 0. For example,
	// assuming minHealthyPeriodSeconds is set to 1h (default)
	//
	//	M1 become unhealthy; remediation happens, and M1-1 is created as a replacement.
	//	If M1-1 (replacement of M1) has problems within the 1hr after the creation, also
	//	this machine will be remediated and this operation is considered a retry - a problem related
	//	to the original issue happened to M1 -.
	//
	//	If instead the problem on M1-1 is happening after minHealthyPeriodSeconds expired, e.g. four days after
	//	m1-1 has been created as a remediation of M1, the problem on M1-1 is considered unrelated to
	//	the original issue happened to M1.
	//
	// Defaults to 3600.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinHealthyPeriodSeconds *int32 `json:"minHealthyPeriodSeconds,omitempty"`
}

// GetReplicas reads spec replicas in a safe way.
// If replicas is nil it will return 0.
func (s *TalosControlPlaneSpec) GetReplicas() int32 {
	if s.Replicas == nil {
		return 0
	}

	return *s.Replicas
}

// RolloutStrategy describes how to replace existing machines
// with new ones.
type RolloutStrategy struct {
	// Rolling update config params. Present only if
	// RolloutStrategyType = RollingUpdate.
	// +optional
	RollingUpdate *RollingUpdate `json:"rollingUpdate,omitempty"`

	// Change rollout strategy.
	//
	// Supported strategies:
	//  * "RollingUpdate".
	//  * "OnDelete"
	//
	// Default is RollingUpdate.
	// +optional
	Type RolloutStrategyType `json:"type,omitempty"`
}

// RollingUpdate is used to control the desired behavior of rolling update.
type RollingUpdate struct {
	// The maximum number of control planes that can be scheduled above or under the
	// desired number of control planes.
	// Value can be an absolute number 1 or 0.
	// Defaults to 1.
	// Example: when this is set to 1, the control plane can be scaled
	// up immediately when the rolling update starts.
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`
}

// TalosControlPlaneStatus defines the observed state of TalosControlPlane.
type TalosControlPlaneStatus struct {
	// Conditions represents the observations of a TalosControlPlane's current state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// initialization provides observations of the TalosControlPlane initialization process.
	// NOTE: Fields in this struct are part of the Cluster API contract and are used to orchestrate
	// initial Machine provisioning.
	// +optional
	Initialization TalosControlPlaneInitializationStatus `json:"initialization,omitempty,omitzero"`

	// Selector is the label selector in string format to avoid introspection
	// by clients, and is used to provide the CRD-based integration for the
	// scale subresource and additional integrations for things like kubectl
	// describe.. The string will be in the same format as the query-param syntax.
	// More info about label selectors: http://kubernetes.io/docs/user-guide/labels#label-selectors
	// +optional
	Selector string `json:"selector,omitempty"`

	// Total number of non-terminated machines targeted by this control plane
	// (their labels match the selector).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Total number of fully running and ready control plane machines.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// AvailableReplicas is the number of available replicas for this ControlPlane.
	// A machine is considered available when Machine's Available condition is true.
	// +optional
	AvailableReplicas *int32 `json:"availableReplicas,omitempty"`

	// UpToDateReplicas is the number of up-to-date replicas targeted by this ControlPlane.
	// A machine is considered up to date when Machine's UpToDate condition is true.
	// +optional
	UpToDateReplicas *int32 `json:"upToDateReplicas,omitempty"`

	// version represents the minimum Kubernetes version for the control plane machines
	// in the cluster.
	// +optional
	Version *string `json:"version,omitempty"`

	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Bootstrapped denotes whether any nodes received bootstrap request
	// which is required to start etcd and Kubernetes components in Talos.
	// +optional
	Bootstrapped bool `json:"bootstrapped,omitempty"`

	// lastRemediation stores info about the last remediation performed: the one in progress, if
	// any, or the most recent one recorded on a Machine the TalosControlPlane created.
	// +optional
	LastRemediation LastRemediationStatus `json:"lastRemediation,omitempty,omitzero"`
}

// LastRemediationStatus stores info about the last remediation performed by the TalosControlPlane.
type LastRemediationStatus struct {
	// machine is the name of the Machine remediated last.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Machine string `json:"machine,omitempty"`

	// time is when the last remediation happened. It is represented in RFC3339 form and is in UTC.
	// +required
	Time metav1.Time `json:"time,omitempty,omitzero"`

	// retryCount is the number of retries of the remediation sequence the last remediation belongs
	// to. A retry happens when a machine that was created as a replacement for an unhealthy machine
	// also fails.
	// +required
	// +kubebuilder:validation:Minimum=0
	RetryCount *int32 `json:"retryCount,omitempty"`
}

// TalosControlPlaneInitializationStatus provides observations of the TalosControlPlane initialization process.
// +kubebuilder:validation:MinProperties=1
type TalosControlPlaneInitializationStatus struct {
	// controlPlaneInitialized is true when the TalosControlPlane provider reports that the
	// Kubernetes control plane is initialized; A control plane is considered initialized when
	// it can accept requests, no matter if this happens before the control plane is fully
	// provisioned or not.
	// NOTE: this field is part of the Cluster API contract, and it is used to orchestrate
	// initial Machine provisioning.
	// +optional
	ControlPlaneInitialized *bool `json:"controlPlaneInitialized,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=taloscontrolplanes,shortName=tcp,scope=Namespaced,categories=cluster-api
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Initialized",type=boolean,JSONPath=".status.initialization.controlPlaneInitialized",description="This denotes whether or not the control plane has been initialized"
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".status.replicas",description="Total number of non-terminated machines targeted by this control plane"
// +kubebuilder:printcolumn:name="Ready Replicas",type=integer,JSONPath=".status.readyReplicas",description="Total number of fully running and ready control plane machines"
// +kubebuilder:printcolumn:name="Available Replicas",type=integer,JSONPath=".status.availableReplicas",description="Total number of available machines targeted by this control plane"
// +kubebuilder:printcolumn:name="Up-to-date Replicas",type=integer,JSONPath=".status.upToDateReplicas",description="Total number of up-to-date machines targeted by this control plane"

// TalosControlPlane is the Schema for the taloscontrolplanes API
type TalosControlPlane struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TalosControlPlaneSpec   `json:"spec,omitempty"`
	Status TalosControlPlaneStatus `json:"status,omitempty"`
}

// GetConditions returns the set of conditions for this object.
func (r *TalosControlPlane) GetConditions() []metav1.Condition {
	return r.Status.Conditions
}

// SetConditions sets the conditions on this object.
func (r *TalosControlPlane) SetConditions(conditions []metav1.Condition) {
	r.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// TalosControlPlaneList contains a list of TalosControlPlane
type TalosControlPlaneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TalosControlPlane `json:"items"`
}

func init() {
	register(&TalosControlPlane{}, &TalosControlPlaneList{})
}
