// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package v1alpha3 contains API Schema definitions for the controlplane v1alpha3 API group
// +kubebuilder:object:generate=true
// +groupName=controlplane.cluster.x-k8s.io
package v1alpha3

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "controlplane.cluster.x-k8s.io", Version: "v1alpha3"}

	// schemeBuilder collects the registrations; types files add theirs from init.
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = schemeBuilder.AddToScheme

	// objectTypes are the kinds registered under GroupVersion.
	objectTypes []runtime.Object

	// localSchemeBuilder is what the generated conversions register with.
	localSchemeBuilder = &schemeBuilder
)

// register queues objects for registration under GroupVersion.
func register(objects ...runtime.Object) {
	objectTypes = append(objectTypes, objects...)
}

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, objectTypes...)
	metav1.AddToGroupVersion(s, GroupVersion)

	return nil
}
