// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"testing"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	utilfeature "k8s.io/component-base/featuregate/testing"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimecatalog "sigs.k8s.io/cluster-api/api/runtime/catalog"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/util/collections"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// fakeCaller stands in for the runtime extension.
type fakeCaller struct {
	extensions []string

	// respond fills in the response; nil means "claim the whole Machine spec diff".
	respond func(req *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse)

	calls int
}

func (f *fakeCaller) GetAllExtensions(context.Context, runtimecatalog.Hook, metav1.Object) ([]string, error) {
	return f.extensions, nil
}

func (f *fakeCaller) CallExtension(_ context.Context, _ runtimecatalog.Hook, _ metav1.Object, _ string, request, response any) error {
	f.calls++

	req, ok := request.(*runtimehooksv1.CanUpdateMachineRequest)
	if !ok {
		return nil
	}

	resp, ok := response.(*runtimehooksv1.CanUpdateMachineResponse)
	if !ok {
		return nil
	}

	resp.Status = runtimehooksv1.ResponseStatusSuccess

	if f.respond != nil {
		f.respond(req, resp)

		return nil
	}

	// Claim the version change, which is the whole diff these tests set up.
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"version": req.Desired.Machine.Spec.Version}})
	if err != nil {
		return err
	}

	resp.MachinePatch = runtimehooksv1.Patch{PatchType: runtimehooksv1.JSONMergePatchType, Patch: patch}

	return nil
}

type inPlaceFixture struct {
	reconciler   *TalosControlPlaneReconciler
	tcp          *controlplanev1.TalosControlPlane
	controlPlane *ControlPlane
	machine      *clusterv1.Machine
	caller       *fakeCaller
}

// newInPlaceFixture builds a single-machine control plane that is outdated only by its
// Kubernetes version, which the extension can absorb.
func newInPlaceFixture(t *testing.T, machineAnnotations map[string]string) *inPlaceFixture {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, runtimev1.AddToScheme(scheme))
	require.NoError(t, cabptv1.AddToScheme(scheme))
	require.NoError(t, controlplanev1.AddToScheme(scheme))

	tcp := &controlplanev1.TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "tcp", Namespace: "default"},
		Spec: controlplanev1.TalosControlPlaneSpec{
			Version: "v1.34.0",
			ControlPlaneConfig: controlplanev1.ControlPlaneConfig{
				ControlPlaneConfig: cabptv1.TalosConfigSpec{GenerateType: "controlplane"},
			},
		},
	}

	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "machine-1",
			Namespace:   "default",
			Annotations: machineAnnotations,
		},
		Spec: clusterv1.MachineSpec{ClusterName: "test", Version: "v1.33.0"},
		Status: clusterv1.MachineStatus{
			NodeRef: clusterv1.MachineNodeReference{Name: "node-1"},
		},
	}

	talosConfig := &cabptv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "machine-1", Namespace: "default"},
		Spec:       cabptv1.TalosConfigSpec{GenerateType: "controlplane"},
	}

	infra := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2",
		"kind":       "TinkerbellMachine",
		"metadata":   map[string]any{"name": "machine-1", "namespace": "default"},
		"spec":       map[string]any{"hardwareName": "hw-1"},
	}}

	caller := &fakeCaller{extensions: []string{"can-update-machine"}}

	return &inPlaceFixture{
		reconciler: &TalosControlPlaneReconciler{
			Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(machine, talosConfig).Build(),
			Log:           ctrl.Log.WithName("test"),
			Scheme:        scheme,
			RuntimeClient: caller,
		},
		tcp: tcp,
		controlPlane: &ControlPlane{
			TCP:          tcp,
			Machines:     collections.FromMachines(machine),
			infraObjects: map[string]*unstructured.Unstructured{"machine-1": infra},
			talosConfigs: map[string]*cabptv1.TalosConfig{"machine-1": talosConfig},
		},
		machine: machine,
		caller:  caller,
	}
}

// The gate has to hold even with the extension registered: without the feature gate the
// provider must behave exactly as it did before.
func TestReconcileInPlaceUpdates_DisabledWithoutFeatureGate(t *testing.T) {
	f := newInPlaceFixture(t, nil)

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine))

	require.NoError(t, err)
	assert.Empty(t, decision.claimed)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.rollout.Names(), "with the gate off every outdated machine is rolled out")
	assert.Zero(t, f.caller.calls, "the extension must not be consulted while the gate is off")
}

// addMachine grows the fixture's control plane by one outdated machine that the extension
// could take, mirroring machine-1.
func (f *inPlaceFixture) addMachine(name string, annotations map[string]string) *clusterv1.Machine {
	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations},
		Spec:       clusterv1.MachineSpec{ClusterName: "test", Version: "v1.33.0"},
		Status:     clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: "node-" + name}},
	}

	f.controlPlane.Machines.Insert(machine)
	f.controlPlane.talosConfigs[name] = &cabptv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       cabptv1.TalosConfigSpec{GenerateType: "controlplane"},
	}
	f.controlPlane.infraObjects[name] = &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2",
		"kind":       "TinkerbellMachine",
		"metadata":   map[string]any{"name": name, "namespace": "default"},
		"spec":       map[string]any{"hardwareName": "hw-" + name},
	}}

	return machine
}

// While one machine is mid-update the remaining outdated ones must wait for their turn,
// not be handed to the rollout path: replacing a machine while another is being updated in
// place is exactly the concurrent control plane change the one-at-a-time rule exists to
// prevent.
func TestReconcileInPlaceUpdates_HoldsOthersWhileOneIsUpdating(t *testing.T) {
	utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)

	f := newInPlaceFixture(t, map[string]string{clusterv1.UpdateInProgressAnnotation: ""})
	second := f.addMachine("machine-2", nil)
	third := f.addMachine("machine-3", nil)

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine, second, third))

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.claimed.Names())
	assert.Empty(t, decision.rollout, "machines waiting for an in-place update in flight must not be rolled out")
	assert.Zero(t, f.caller.calls, "no second machine is offered while one is updating")
}

// Nothing the extension could take (here: no node yet) goes to the rollout path, as it
// always has.
func TestReconcileInPlaceUpdates_RollsOutWhenNoCandidate(t *testing.T) {
	utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)

	f := newInPlaceFixture(t, nil)
	f.machine.Status.NodeRef = clusterv1.MachineNodeReference{}

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine))

	require.NoError(t, err)
	assert.Empty(t, decision.claimed)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.rollout.Names())
	assert.Zero(t, f.caller.calls)
}

// An unhealthy etcd defers the whole question. Previously it sent every outdated machine to
// the rollout path, which is the worst possible reaction to an unhealthy control plane.
func TestReconcileInPlaceUpdates_DefersWhileEtcdIsUnhealthy(t *testing.T) {
	utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)

	// The fixture has no talosconfig secret, so the etcd health check cannot reach any node.
	f := newInPlaceFixture(t, nil)

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine))

	require.NoError(t, err)
	assert.Empty(t, decision.claimed)
	assert.Empty(t, decision.rollout, "an unhealthy etcd must defer, not roll out")
	assert.Zero(t, f.caller.calls)
}

// healthyControlPlane gives the fixture a second, up-to-date machine and fakes through which
// etcd and the control plane components of both machines look healthy, so that the in-place
// gates are passed; the fakes can then be degraded one at a time.
func (f *inPlaceFixture) healthyControlPlane() (second *clusterv1.Machine, dialer *fakeEtcdDialer) {
	second = f.addMachine("machine-2", nil)
	members := etcdMembersFor("node-1", "node-machine-2")

	// What a healthy etcd reports to the health check: running, healthy, with its events.
	etcd := func() []talosclient.ServiceInfo {
		return []talosclient.ServiceInfo{{Service: &machineapi.ServiceInfo{
			Id:     "etcd",
			State:  "Running",
			Health: &machineapi.ServiceHealth{Healthy: true},
			Events: &machineapi.ServiceEvents{Events: []*machineapi.ServiceEvent{{State: "Running", Msg: "Health check successful"}}},
		}}}
	}

	dialer = newFakeEtcdDialer()
	dialer.clients["machine-1"] = &fakeEtcdCalls{name: "node-1", services: etcd(), members: members, serviceList: healthyServices()}
	dialer.clients["machine-2"] = &fakeEtcdCalls{name: "node-machine-2", services: etcd(), members: members, serviceList: healthyServices()}
	f.reconciler.etcdDialer = dialer.dial

	return second, dialer
}

// A control plane component that is not healthy on another machine defers the next in-place
// update, as KCP's preflight checks hold a rollout: an update that left a kube-apiserver broken
// must not roll on to the next machine.
func TestReconcileInPlaceUpdates_DefersWhileComponentsAreUnhealthy(t *testing.T) {
	utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)

	f := newInPlaceFixture(t, nil)
	_, dialer := f.healthyControlPlane()
	dialer.clients["machine-2"].staticPods = []staticPodStatus{
		{id: "kube-system/kube-apiserver-node-machine-2", ready: false},
		{id: "kube-system/kube-controller-manager-node-machine-2", ready: true},
		{id: "kube-system/kube-scheduler-node-machine-2", ready: true},
	}

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine))

	require.NoError(t, err)
	assert.Empty(t, decision.claimed)
	assert.Empty(t, decision.rollout, "unhealthy components must defer, not roll out")
	assert.Zero(t, f.caller.calls)
}

// The candidate's own components are not a reason to hold its update: the update may be
// exactly what repairs them.
func TestReconcileInPlaceUpdates_IgnoresTheCandidatesOwnComponents(t *testing.T) {
	utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)

	f := newInPlaceFixture(t, nil)
	_, dialer := f.healthyControlPlane()
	dialer.clients["machine-1"].staticPods = []staticPodStatus{
		{id: "kube-system/kube-apiserver-node-1", ready: false},
	}

	decision, err := f.reconciler.reconcileInPlaceUpdates(
		context.Background(), f.tcp, f.controlPlane, collections.FromMachines(f.machine))

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.claimed.Names())
	assert.Empty(t, decision.rollout)
	assert.Equal(t, 1, f.caller.calls, "the extension is offered the candidate")
	assert.Zero(t, dialer.clients["machine-1"].staticPodCalls, "the candidate's components are not consulted")
}

// A change the extension cannot absorb sends that machine, and only that machine, to the
// rollout path; the rest are re-offered once the replacement has settled.
func TestOfferInPlace_DeclinedMachineIsRolledOut(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.caller.respond = func(_ *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
		resp.Status = runtimehooksv1.ResponseStatusSuccess // no patches at all
	}

	decision, err := f.reconciler.offerInPlace(context.Background(), f.tcp, f.controlPlane, f.machine)

	require.NoError(t, err)
	assert.Empty(t, decision.claimed)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.rollout.Names())
	assert.Empty(t, f.machine.Annotations, "a declined machine must not be marked as updating")
}

// A change the extension covers is triggered and claimed, and nothing is rolled out.
func TestOfferInPlace_ClaimsTriggeredMachine(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	// No InfraMachine to write: the fake client cannot apply an unregistered kind, and the
	// infrastructure is carried through unchanged anyway.
	delete(f.controlPlane.infraObjects, "machine-1")

	decision, err := f.reconciler.offerInPlace(context.Background(), f.tcp, f.controlPlane, f.machine)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"machine-1"}, decision.claimed.Names())
	assert.Empty(t, decision.rollout)

	var machine clusterv1.Machine
	require.NoError(t, f.reconciler.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "machine-1"}, &machine))
	assert.Contains(t, machine.Annotations, clusterv1.UpdateInProgressAnnotation)
}

// A machine already carrying the annotation stays claimed so the rollout path leaves it
// alone, and no second machine is started concurrently.
func TestReconcileInPlaceUpdates_ClaimsMachinesAlreadyUpdating(t *testing.T) {
	f := newInPlaceFixture(t, map[string]string{clusterv1.UpdateInProgressAnnotation: ""})

	assert.True(t, isUpdatingInPlace(f.machine))
}

func TestNextInPlaceCandidate_SkipsMachineWithoutNodeRef(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.machine.Status.NodeRef = clusterv1.MachineNodeReference{}

	assert.Nil(t, f.reconciler.nextInPlaceCandidate(f.controlPlane, collections.FromMachines(f.machine)))
}

func TestNextInPlaceCandidate_SkipsDeletingMachine(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	now := metav1.Now()
	f.machine.DeletionTimestamp = &now

	assert.Nil(t, f.reconciler.nextInPlaceCandidate(f.controlPlane, collections.FromMachines(f.machine)))
}

func TestCanUpdateMachine_CoveredDiffIsAccepted(t *testing.T) {
	f := newInPlaceFixture(t, nil)

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	ok, err := f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.NoError(t, err)
	assert.True(t, ok)
}

// Anything the extension leaves unpatched must send the machine down the rollout path.
func TestCanUpdateMachine_UncoveredDiffIsDeclined(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.caller.respond = func(_ *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
		resp.Status = runtimehooksv1.ResponseStatusSuccess // no patches at all
	}

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	ok, err := f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCanUpdateMachine_NoExtensionRegistered(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.caller.extensions = nil

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	ok, err := f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.NoError(t, err)
	assert.False(t, ok)
}

// Cluster API supports exactly one extension per hook; more is an error rather than an
// arbitrary choice between them.
func TestCanUpdateMachine_MultipleExtensionsIsAnError(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.caller.extensions = []string{"a", "b"}

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	_, err = f.reconciler.canUpdateMachine(context.Background(), plan, f.controlPlane)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one is supported")
}

func TestBuildInPlacePlan_UsesDesiredSpecFromTCP(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.TalosVersion = "v1.13"

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	assert.Equal(t, "v1.34.0", plan.desiredMachine.Spec.Version)
	assert.Equal(t, "v1.13", plan.desiredTalosConfig.Spec.TalosVersion)
	assert.Equal(t, "v1.33.0", plan.machine.Spec.Version, "the current machine must not be mutated")
}

// The trigger is what commits Cluster API to the in-place path, so all three objects must end
// up annotated and the hook must end up pending.
func TestTriggerInPlaceUpdate_AnnotatesAndMarksHookPending(t *testing.T) {
	f := newInPlaceFixture(t, nil)

	plan, err := f.reconciler.buildInPlacePlan(context.Background(), f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)

	require.NoError(t, f.reconciler.triggerInPlaceUpdate(context.Background(), plan))

	var machine clusterv1.Machine
	require.NoError(t, f.reconciler.Client.Get(context.Background(),
		ctrlKey("default", "machine-1"), &machine))

	assert.Contains(t, machine.Annotations, clusterv1.UpdateInProgressAnnotation)
	assert.Contains(t, machine.Annotations[runtimev1.PendingHooksAnnotation], "UpdateMachine")

	var talosConfig cabptv1.TalosConfig
	require.NoError(t, f.reconciler.Client.Get(context.Background(),
		ctrlKey("default", "machine-1"), &talosConfig))

	assert.Contains(t, talosConfig.Annotations, clusterv1.UpdateInProgressAnnotation,
		"CABPT's webhook admits the spec change only because of this annotation")
	assert.Equal(t, "controlplane", talosConfig.Spec.GenerateType)
}

// ctrlKey is a small helper to keep the object lookups above readable.
func ctrlKey(namespace, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
