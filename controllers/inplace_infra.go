// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/external"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// contractIdentityFields are the InfraMachine spec fields every infrastructure provider sets on
// a machine under the Cluster API contract, rather than copying them from the template.
var contractIdentityFields = []string{"providerID"}

// infraIdentityFields names, per InfraMachine kind, the further spec fields the provider sets on
// a machine: they identify the hardware or instance the machine runs on, are absent from the
// template, and the provider makes them immutable.
//
// When the control plane's infrastructure template rotates, a machine can only be updated in
// place if its InfraMachine can be rebuilt from the new template with these fields carried over.
// For a kind not listed here they are unknown, so such a machine is replaced, as it always was.
var infraIdentityFields = map[schema.GroupKind][]string{
	// Cluster API Provider Tinkerbell: the Hardware the machine claimed.
	{Group: "infrastructure.cluster.x-k8s.io", Kind: "TinkerbellMachine"}: {"hardwareName"},
}

// canRebuildInfraMachine reports whether the InfraMachine of a machine whose infrastructure
// template rotated can be rebuilt from the current template (see infraIdentityFields).
func canRebuildInfraMachine(infraMachine *unstructured.Unstructured) bool {
	if infraMachine == nil {
		return false
	}

	_, known := infraIdentityFields[infraMachine.GroupVersionKind().GroupKind()]

	return known
}

// rebuildInfraMachine returns the InfraMachine current would be had it been cloned from the
// control plane's current infrastructure template: the template's spec, the fields the provider
// set on the machine, and cloned-from annotations naming the template.
//
// Every other spec field of current is dropped, so fields the old template set and the new one
// does not are removed. Whether that change can happen on a running machine is for the
// CanUpdateMachine extension to decide; this only describes it.
func rebuildInfraMachine(ctx context.Context, c client.Reader, tcp *controlplanev1.TalosControlPlane, current *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gk := current.GroupVersionKind().GroupKind()

	identityFields, known := infraIdentityFields[gk]
	if !known {
		return nil, fmt.Errorf("cannot rebuild a %s from its template: the fields its provider sets are unknown", gk)
	}

	templateRef := tcp.Spec.MachineTemplate.Spec.InfrastructureRef

	template, err := external.GetObjectFromContractVersionedRef(ctx, c, templateRef, tcp.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get infrastructure template %s %s/%s: %w", templateRef.Kind, tcp.Namespace, templateRef.Name, err)
	}

	spec, found, err := unstructured.NestedMap(template.Object, "spec", "template", "spec")
	if err != nil {
		return nil, fmt.Errorf("failed to read spec.template.spec of %s %s/%s: %w", templateRef.Kind, tcp.Namespace, templateRef.Name, err)
	}

	if !found {
		spec = map[string]any{}
	}

	currentSpec, _, err := unstructured.NestedMap(current.Object, "spec")
	if err != nil {
		return nil, fmt.Errorf("failed to read spec of %s %s/%s: %w", gk.Kind, current.GetNamespace(), current.GetName(), err)
	}

	for _, field := range append(append([]string{}, contractIdentityFields...), identityFields...) {
		if value, ok := currentSpec[field]; ok {
			spec[field] = value
		}
	}

	desired := current.DeepCopy()

	if err := unstructured.SetNestedMap(desired.Object, spec, "spec"); err != nil {
		return nil, fmt.Errorf("failed to set spec of %s %s/%s: %w", gk.Kind, current.GetNamespace(), current.GetName(), err)
	}

	annotations := desired.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	// Written exactly as MatchesTemplateClonedFrom reads them, so the machine counts as up to
	// date once the update has finished.
	annotations[clusterv1.TemplateClonedFromNameAnnotation] = templateRef.Name
	annotations[clusterv1.TemplateClonedFromGroupKindAnnotation] = schema.GroupKind{Group: templateRef.APIGroup, Kind: templateRef.Kind}.String()
	desired.SetAnnotations(annotations)

	return desired, nil
}
