// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

var (
	tinkerbellMachineGVK         = schema.GroupVersionKind{Group: "infrastructure.cluster.x-k8s.io", Version: "v1beta2", Kind: "TinkerbellMachine"}
	tinkerbellMachineTemplateGVK = schema.GroupVersionKind{Group: "infrastructure.cluster.x-k8s.io", Version: "v1beta2", Kind: "TinkerbellMachineTemplate"}
)

const templateGroupKind = "TinkerbellMachineTemplate.infrastructure.cluster.x-k8s.io"

// newRotatedInPlaceFixture is newInPlaceFixture for a machine whose infrastructure template
// rotated: its TinkerbellMachine was cloned from "old-template" and still carries that
// template's templateInline, while the control plane now points at "new-template", which
// provisions from a Task instead. The fake client serves the template through a CRD carrying
// the contract label, as the API server does.
func newRotatedInPlaceFixture(t *testing.T) *inPlaceFixture {
	t.Helper()

	f := newInPlaceFixture(t, nil)

	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, runtimev1.AddToScheme(scheme))
	require.NoError(t, cabptv1.AddToScheme(scheme))
	require.NoError(t, controlplanev1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))

	for _, gvk := range []schema.GroupVersionKind{tinkerbellMachineGVK, tinkerbellMachineTemplateGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}

	// What external.GetObjectFromContractVersionedRef reads to pick the version that serves
	// the Cluster API v1beta2 contract.
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "tinkerbellmachinetemplates.infrastructure.cluster.x-k8s.io",
			Labels: map[string]string{clusterv1.GroupVersion.String(): "v1beta2"},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: tinkerbellMachineTemplateGVK.Group,
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: tinkerbellMachineTemplateGVK.Kind, Plural: "tinkerbellmachinetemplates"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: tinkerbellMachineTemplateGVK.Version, Served: true, Storage: true},
			},
		},
	}

	template := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "new-template", "namespace": "default"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"bootOptions": map[string]any{"bootMode": "netboot"},
			"tasks": []any{map[string]any{
				"name":    "install",
				"taskRef": map[string]any{"name": "talos-install", "namespace": "tinkerbell"},
			}},
		}}},
	}}
	template.SetGroupVersionKind(tinkerbellMachineTemplateGVK)

	infra := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"name":      "machine-1",
			"namespace": "default",
			"annotations": map[string]any{
				clusterv1.TemplateClonedFromNameAnnotation:      "old-template",
				clusterv1.TemplateClonedFromGroupKindAnnotation: templateGroupKind,
			},
		},
		"spec": map[string]any{
			"bootOptions":    map[string]any{"bootMode": "netboot"},
			"templateInline": "version: \"0.1\"\n",
			"hardwareName":   "hw-1",
			"providerID":     "tinkerbell://default/hw-1",
		},
	}}
	infra.SetGroupVersionKind(tinkerbellMachineGVK)

	f.reconciler.Client = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(crd, template, infra.DeepCopy(), f.machine, f.controlPlane.talosConfigs["machine-1"]).
		Build()
	f.reconciler.Scheme = scheme

	f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{
		APIGroup: "infrastructure.cluster.x-k8s.io",
		Kind:     "TinkerbellMachineTemplate",
		Name:     "new-template",
	}
	f.controlPlane.infraObjects["machine-1"] = infra

	return f
}

// A rotated TinkerbellMachine can be rebuilt from the new template, so the machine is offered
// to the extension instead of being replaced.
func TestNextInPlaceCandidate_OffersRotatedTinkerbellMachine(t *testing.T) {
	f := newRotatedInPlaceFixture(t)

	candidate := f.reconciler.nextInPlaceCandidate(f.controlPlane, collections.FromMachines(f.machine))

	require.NotNil(t, candidate)
	assert.Equal(t, "machine-1", candidate.Name)
}

// For a kind whose provider-set fields are unknown the InfraMachine cannot be rebuilt, so a
// rotated template still means a replacement. Claiming the machine would leave it outdated
// forever and spin.
func TestNextInPlaceCandidate_SkipsRotatedInfraTemplateOfAnUnknownKind(t *testing.T) {
	f := newInPlaceFixture(t, nil)

	infra := f.controlPlane.infraObjects["machine-1"]
	infra.SetKind("GenericInfrastructureMachine")
	infra.SetAnnotations(map[string]string{
		clusterv1.TemplateClonedFromNameAnnotation:      "old-template",
		clusterv1.TemplateClonedFromGroupKindAnnotation: "GenericInfrastructureMachineTemplate.infrastructure.cluster.x-k8s.io",
	})
	f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{
		APIGroup: "infrastructure.cluster.x-k8s.io",
		Kind:     "GenericInfrastructureMachineTemplate",
		Name:     "new-template",
	}

	assert.Nil(t, f.reconciler.nextInPlaceCandidate(f.controlPlane, collections.FromMachines(f.machine)))
}

// The rebuilt InfraMachine is the new template's spec plus the fields the provider set, with
// the old template's fields gone and the cloned-from annotations naming the new template.
func TestBuildInPlacePlan_RebuildsRotatedInfraMachine(t *testing.T) {
	f := newRotatedInPlaceFixture(t)

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)
	require.True(t, plan.rebuildsInfraMachine())

	spec := plan.desiredInfraMachine.Object["spec"].(map[string]any)
	assert.NotContains(t, spec, "templateInline", "the superseded template's field must go")
	assert.Contains(t, spec, "tasks")
	assert.Equal(t, "hw-1", spec["hardwareName"], "the claimed Hardware must be kept")
	assert.Equal(t, "tinkerbell://default/hw-1", spec["providerID"])
	assert.Equal(t, map[string]any{"bootMode": "netboot"}, spec["bootOptions"])

	assert.Equal(t, "new-template", plan.desiredInfraMachine.GetAnnotations()[clusterv1.TemplateClonedFromNameAnnotation])
	assert.Equal(t, templateGroupKind, plan.desiredInfraMachine.GetAnnotations()[clusterv1.TemplateClonedFromGroupKindAnnotation])

	assert.Contains(t, plan.currentInfraMachine.Object["spec"], "templateInline", "the current object must not be mutated")

	f.controlPlane.infraObjects["machine-1"] = plan.desiredInfraMachine
	assert.True(t, MatchesTemplateClonedFrom(f.controlPlane.infraObjects, f.tcp)(f.machine),
		"the rebuilt InfraMachine must count as cloned from the current template")
}

// The extension is shown the InfraMachine as it is and as it will be, and the update is
// accepted only if its patch covers the difference.
func TestCanUpdateMachine_ComparesCurrentWithRebuiltInfraMachine(t *testing.T) {
	f := newRotatedInPlaceFixture(t)

	var seen *runtimehooksv1.CanUpdateMachineRequest

	f.caller.respond = func(req *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
		seen = req
		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.MachinePatch = mergePatch(t, map[string]any{"spec": map[string]any{"version": req.Desired.Machine.Spec.Version}})
		resp.InfrastructureMachinePatch = mergePatch(t, map[string]any{"spec": map[string]any{
			"templateInline": nil,
			"tasks": []any{map[string]any{
				"name":    "install",
				"taskRef": map[string]any{"name": "talos-install", "namespace": "tinkerbell"},
			}},
		}})
	}

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	ok, err := f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.NoError(t, err)
	assert.True(t, ok)

	require.NotNil(t, seen)
	assert.Contains(t, string(seen.Current.InfrastructureMachine.Raw), "templateInline")
	assert.NotContains(t, string(seen.Desired.InfrastructureMachine.Raw), "templateInline")
	assert.Contains(t, string(seen.Desired.InfrastructureMachine.Raw), "talos-install")
}

// An extension that absorbs only the version leaves the InfraMachine change uncovered, so the
// machine goes to the rollout path.
func TestCanUpdateMachine_DeclinesAnUncoveredInfraMachineChange(t *testing.T) {
	f := newRotatedInPlaceFixture(t)

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	ok, err := f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.NoError(t, err)
	assert.False(t, ok)
}

// The trigger writes the rebuilt InfraMachine. The superseded template's field must be gone
// afterwards even though, as on real clusters, the object was not created by an apply.
func TestTriggerInPlaceUpdate_WritesTheRebuiltInfraMachine(t *testing.T) {
	f := newRotatedInPlaceFixture(t)

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	require.NoError(t, f.reconciler.triggerInPlaceUpdate(context.Background(), plan))

	infra := &unstructured.Unstructured{}
	infra.SetGroupVersionKind(tinkerbellMachineGVK)
	require.NoError(t, f.reconciler.Client.Get(context.Background(), ctrlKey("default", "machine-1"), infra))

	spec := infra.Object["spec"].(map[string]any)
	assert.NotContains(t, spec, "templateInline")
	assert.Contains(t, spec, "tasks")
	assert.Equal(t, "hw-1", spec["hardwareName"])
	assert.Equal(t, "tinkerbell://default/hw-1", spec["providerID"])

	annotations := infra.GetAnnotations()
	assert.Equal(t, "new-template", annotations[clusterv1.TemplateClonedFromNameAnnotation])
	assert.Contains(t, annotations, clusterv1.UpdateInProgressAnnotation,
		"the Machine controller waits for this on the InfraMachine before it calls UpdateMachine")

	var machine clusterv1.Machine
	require.NoError(t, f.reconciler.Client.Get(context.Background(), ctrlKey("default", "machine-1"), &machine))
	assert.Contains(t, machine.Annotations[runtimev1.PendingHooksAnnotation], "UpdateMachine")
}

// The UpToDate condition says which check a control plane Machine fails.
func TestNotUpToDateMessage(t *testing.T) {
	f := newRotatedInPlaceFixture(t)
	f.controlPlane.talosConfigs["machine-1"].Spec.TalosVersion = "v1.12"

	message := f.controlPlane.notUpToDateMessage(f.machine)

	assert.Equal(t, "* Version v1.33.0, v1.34.0 required\n"+
		"* TinkerbellMachine is not up-to-date: cloned from old-template, new-template required\n"+
		"* TalosConfig is not up-to-date", message)
}

func mergePatch(t *testing.T, patch map[string]any) runtimehooksv1.Patch {
	t.Helper()

	raw, err := json.Marshal(patch)
	require.NoError(t, err)

	return runtimehooksv1.Patch{PatchType: runtimehooksv1.JSONMergePatchType, Patch: raw}
}
