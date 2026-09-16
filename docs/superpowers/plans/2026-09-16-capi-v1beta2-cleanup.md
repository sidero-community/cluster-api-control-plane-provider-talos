# CACPPT v1beta2-only status and init removal: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every deprecated Cluster API and controller-runtime API from CACPPT: the v1beta1-contract status block, the `init` configuration and init-node path, rate-limited requeues, the `client.Apply` patch type, the legacy event recorder and controller-runtime's scheme builder.

**Architecture:** Mechanical replacements first (requeue, recorder, server-side apply, scheme builders), then the two API removals: `init` with its bootstrap path, and `status.deprecated.v1beta1` with the v1alpha3 conversion deriving the legacy fields from v1beta2 ones. The v1alpha3 API keeps its schema with local condition types.

**Tech Stack:** Go 1.26.5, Cluster API v1.14.2, controller-runtime v0.24.1, k8s.io/client-go v0.36.3 (`tools/events`), controller-gen v0.21.0, conversion-gen v0.36.0, gomega/testify suite.

**Spec:** `docs/design/2026-09-16-capi-v1beta2-cleanup.md`

## Global Constraints

- Branch `talos-1.14`; new `.go` files start with the MPL header every other file carries.
- Commit messages: conventional type, imperative lower-case header under 89 characters, a body, no sign-off trailer.
- `Requeue: true` becomes `RequeueAfter: 20 * time.Second` everywhere.
- Unit tests: `GOTOOLCHAIN=auto go test ./api/... ./controllers/... ./internal/runtimeclient/... -count=1` (the integration package needs a cluster).
- Regeneration: `go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 object:headerFile=./hack/boilerplate.go.txt paths="./..."`, `go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 crd:crdVersions=v1 paths="./api/..." output:crd:dir=config/crd/bases output:webhook:dir=config/webhook webhook`, `go run k8s.io/code-generator/cmd/conversion-gen@v0.36.0 --output-file=zz_generated.conversion.go --go-header-file=./hack/boilerplate.go.txt ./api/v1alpha3`.
- The bootstrap provider's v1alpha3 API keeps `Status.TalosConfig`; this plan only stops reading it.

---

## File structure

| File | Responsibility |
|---|---|
| `controllers/taloscontrolplane_controller.go` | Requeue sites, `Recorder` type, the `first`/init special case, status maintenance (modify) |
| `controllers/scale.go` | Requeue site (modify) |
| `controllers/configs.go` | Delete the init-node path (modify) |
| `controllers/preterminate.go` | Events-API call (modify) |
| `internal/ssa/ssa.go` | `client.Client.Apply` (modify) |
| `main.go` | `GetEventRecorder` (modify) |
| `api/v1beta1/taloscontrolplane_types.go`, `api/v1beta1/taloscontrolplane_webhook.go` | Remove `InitConfig` and `Deprecated` (modify) |
| `api/v1alpha3/condition_types.go` | Local condition types (create) |
| `api/v1alpha3/conditions.go`, `api/v1alpha3/v1beta2_conditions.go`, `api/v1alpha3/taloscontrolplane_types.go` | Use them (modify) |
| `api/v1alpha3/conversion.go`, `api/v1alpha3/conversion_test.go` | `ControlPlaneConfig` conversion without init; derived legacy status (modify) |
| `api/v1alpha3/groupversion_info.go`, `api/v1beta1/groupversion_info.go` | `runtime.NewSchemeBuilder` (modify) |
| `controllers/controllers_test.go`, `controllers/preterminate_test.go`, `controllers/inplace_test.go` | Test updates (modify) |
| `config/crd/bases/*.yaml`, `api/**/zz_generated.*.go` | Regenerated |
| `README.md`, `hack/release.toml` | Docs (modify) |

---

### Task 1: RequeueAfter instead of Requeue

**Files:**
- Modify: `controllers/taloscontrolplane_controller.go` (lines with `Requeue: true` at the not-found, no-owner, paused, infra-not-provisioned and patch-helper sites; the `!res.Requeue` clause in the deferred status requeue), `controllers/scale.go` (`result.Requeue = true`)
- Modify: `controllers/controllers_test.go`

- [ ] **Step 1: Write the failing test**

Append to `controllers/controllers_test.go` inside the suite (it already has `setupCluster`, `newReconciler`, `newFakeClient`, `util`, `ctrl`, gomega):

```go
// A paused cluster is looked at again after a fixed interval, not through the rate limiter.
func (suite *ControllersSuite) TestReconcileRequeuesAPausedClusterAfterAFixedInterval() {
	g := NewWithT(suite.T())

	fakeClient := newFakeClient()
	cluster, tcp, _ := suite.setupCluster(fakeClient, "test-paused", nil)

	cluster.Spec.Paused = ptr.To(true)
	g.Expect(fakeClient.Update(suite.ctx, cluster)).To(Succeed())

	r := newReconciler(fakeClient, withCluster(util.ObjectKey(cluster)))

	// The paused check runs before the finalizer is added, so one reconcile is enough.
	result, err := r.Reconcile(suite.ctx, ctrl.Request{NamespacedName: util.ObjectKey(tcp)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).To(Equal(ctrl.Result{RequeueAfter: 20 * time.Second}))
}
```

Add `"k8s.io/utils/ptr"` and `"time"` to the imports if missing.

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -run 'TestSuite/TestReconcileRequeuesAPausedClusterAfterAFixedInterval$' -count=1`
Expected: FAIL, the result is `{Requeue: true}`.

- [ ] **Step 3: Replace every site**

In `controllers/taloscontrolplane_controller.go` replace each `return ctrl.Result{Requeue: true}, nil` with `return ctrl.Result{RequeueAfter: 20 * time.Second}, nil` (five sites), and change the deferred guard to:

```go
		if reterr == nil && res.RequeueAfter <= 0 && tcp.ObjectMeta.DeletionTimestamp.IsZero() {
```

In `controllers/scale.go` replace `result.Requeue = true` with `result.RequeueAfter = 20 * time.Second` (add `"time"` to its imports if missing). In the scale-down branch of the main reconcile, `if res.Requeue || res.RequeueAfter > 0 {` becomes `if res.RequeueAfter > 0 {`.

- [ ] **Step 4: Run the suite**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -count=1`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add controllers/taloscontrolplane_controller.go controllers/scale.go controllers/controllers_test.go
git commit -m "refactor: requeue after a fixed interval instead of through the rate limiter

controller-runtime deprecates Result.Requeue; the 20 second interval is
the one the controller already uses to come back later."
```

---

### Task 2: Events-API recorder

**Files:**
- Modify: `main.go`, `controllers/taloscontrolplane_controller.go` (`Recorder` field), `controllers/preterminate.go` (`recordMachineEvent`), `controllers/preterminate_test.go`

- [ ] **Step 1: Update the test fixture first**

In `controllers/preterminate_test.go` change the fixture field to `recorder *events.FakeRecorder`, construct it with `events.NewFakeRecorder(64)`, import `"k8s.io/client-go/tools/events"` instead of `record`. The events channel still carries strings; the existing `case e := <-f.recorder.Events:` assertion keeps working because the fake formats `"<type> <reason> <note>"`.

- [ ] **Step 2: Run to verify it fails to compile**

Run: `GOTOOLCHAIN=auto go vet ./controllers/`
Expected: `cannot use recorder (variable of type *events.FakeRecorder) as record.EventRecorder value`.

- [ ] **Step 3: Switch the recorder**

`controllers/taloscontrolplane_controller.go`: `Recorder events.EventRecorder` with the import `"k8s.io/client-go/tools/events"` replacing `"k8s.io/client-go/tools/record"`.

`controllers/preterminate.go`:

```go
// recordMachineEvent records an event on the Machine, if a recorder is wired up.
func (r *TalosControlPlaneReconciler) recordMachineEvent(machine *clusterv1.Machine, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(machine, nil, eventType, reason, "PreTerminate", "%s", message)
}
```

`main.go`: `Recorder: mgr.GetEventRecorder("taloscontrolplane-controller"),`.

- [ ] **Step 4: Run the pre-terminate tests**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -run 'PreTerminate' -count=1 && GOTOOLCHAIN=auto go build ./...`
Expected: `ok`, build OK.

- [ ] **Step 5: Commit**

```bash
git add main.go controllers/taloscontrolplane_controller.go controllers/preterminate.go controllers/preterminate_test.go
git commit -m "refactor: record Machine events through the events API

controller-runtime deprecates the legacy recorder."
```

---

### Task 3: Server-side apply through client.Client.Apply

**Files:**
- Modify: `internal/ssa/ssa.go`
- Create: `internal/ssa/ssa_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/ssa/ssa_test.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package ssa_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/siderolabs/cluster-api-control-plane-provider-talos/internal/ssa"
)

func TestPatchCreatesAndOwnsTheObject(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Data:       map[string]string{"k": "v"},
	}

	require.NoError(t, ssa.Patch(context.Background(), c, cm))

	stored := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cfg"}, stored))
	assert.Equal(t, "v", stored.Data["k"])
	assert.NotEmpty(t, stored.ResourceVersion, "the server's result is copied back")
}

func TestDryRunPatchDoesNotPersist(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
	}

	require.NoError(t, ssa.DryRunPatch(context.Background(), c, cm))

	stored := &corev1.ConfigMap{}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cfg"}, stored)
	assert.Error(t, err, "a dry run must not create the object")
}
```

- [ ] **Step 2: Run it**

Run: `GOTOOLCHAIN=auto go test ./internal/ssa/ -count=1`
Expected: both PASS or FAIL depending on the fake client's handling of the deprecated patch type; either way this pins the behaviour before the rewrite. Note the result.

- [ ] **Step 3: Rewrite the package on the new API**

```go
package ssa

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagerName is the field manager the control plane provider applies as.
const ManagerName = "capi-taloscontrolplane"

// Patch server-side applies obj, taking ownership of the fields it sets, and copies the
// server's result back into obj.
func Patch(ctx context.Context, c client.Client, obj client.Object) error {
	return apply(ctx, c, obj, client.FieldOwner(ManagerName), client.ForceOwnership)
}

// DryRunPatch server-side applies obj without persisting it, returning the object as the
// API server would store it.
//
// In-place update planning uses this to normalise both the current and the desired objects
// before diffing them, so that defaulting and admission do not show up as spurious changes
// and needlessly force a rolling replacement.
func DryRunPatch(ctx context.Context, c client.Client, obj client.Object) error {
	return apply(ctx, c, obj, client.FieldOwner(ManagerName), client.ForceOwnership, client.DryRunAll)
}

func apply(ctx context.Context, c client.Client, obj client.Object, opts ...client.ApplyOption) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("server-side apply failed to encode %s: %w", client.ObjectKeyFromObject(obj), err)
	}

	u := &unstructured.Unstructured{Object: content}

	if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), opts...); err != nil {
		return fmt.Errorf("server-side apply failed for %s: %w", client.ObjectKeyFromObject(obj), err)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj); err != nil {
		return fmt.Errorf("server-side apply failed to decode the result for %s: %w", client.ObjectKeyFromObject(obj), err)
	}

	return nil
}
```

- [ ] **Step 4: Run the package and the in-place trigger tests**

Run: `GOTOOLCHAIN=auto go test ./internal/ssa/ ./controllers/ -run 'TestPatch|TestDryRunPatch|InPlace|Inplace' -count=1`
Expected: PASS. If the fake client does not update the unstructured object in place after `Apply`, fetch the stored object with `c.Get` inside `apply` when `DryRunAll` is not among the options and decode that instead; keep the dry-run path decoding whatever `Apply` returned.

- [ ] **Step 5: Commit**

```bash
git add internal/ssa
git commit -m "refactor: server-side apply through the client's Apply method

controller-runtime deprecates the client.Apply patch type."
```

---

### Task 4: Scheme builders

**Files:**
- Modify: `api/v1beta1/groupversion_info.go`, `api/v1alpha3/groupversion_info.go`, the `init()` in `api/v1beta1/taloscontrolplane_types.go`, `api/v1beta1/taloscontrolplanetemplate_types.go`, `api/v1alpha3/taloscontrolplane_types.go`
- Create: `api/v1beta1/groupversion_info_test.go`

- [ ] **Step 1: Write the guarding test**

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func TestAddToSchemeRegistersEveryKind(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, controlplanev1.AddToScheme(scheme))

	for _, kind := range []string{"TalosControlPlane", "TalosControlPlaneList", "TalosControlPlaneTemplate", "TalosControlPlaneTemplateList"} {
		assert.True(t, scheme.Recognizes(controlplanev1.GroupVersion.WithKind(kind)), kind)
	}
}
```

Run: `GOTOOLCHAIN=auto go test ./api/v1beta1/ -run TestAddToSchemeRegistersEveryKind -count=1` — Expected: PASS (refactor guard).

- [ ] **Step 2: Replace the builders**

`api/v1beta1/groupversion_info.go` body:

```go
import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "controlplane.cluster.x-k8s.io", Version: "v1beta1"}

	// schemeBuilder collects the registrations; types files add theirs from init.
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = schemeBuilder.AddToScheme

	// objectTypes are the kinds registered under GroupVersion.
	objectTypes []runtime.Object
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
```

`api/v1alpha3/groupversion_info.go`: the same with `Version: "v1alpha3"` and `localSchemeBuilder = &schemeBuilder` in the `var` block. Replace `SchemeBuilder.Register(` with `register(` in the three `init()` functions.

- [ ] **Step 3: Run**

Run: `GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go test ./api/... ./controllers/... -count=1`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add api
git commit -m "refactor(api): register schemes with runtime.NewSchemeBuilder

controller-runtime deprecates scheme.Builder for api packages."
```

---

### Task 5: Remove the init configuration and the init-node path

**Files:**
- Modify: `api/v1beta1/taloscontrolplane_types.go` (`ControlPlaneConfig`), `api/v1beta1/taloscontrolplane_webhook.go` (`validateControlPlaneConfig`)
- Modify: `controllers/configs.go`, `controllers/taloscontrolplane_controller.go`
- Modify: `api/v1alpha3/conversion.go`, `api/v1alpha3/conversion_test.go`
- Regenerate: deepcopy, conversions, CRDs

- [ ] **Step 1: Write the failing conversion test**

In `api/v1alpha3/conversion_test.go` replace the assertion on `InitConfig.ImageFactory` in the object-level round-trip test with a new test:

```go
// v1alpha3 still carries `init`; the hub does not, so it is dropped on the way up and restored
// from the stash on the way back down.
func TestInitConfigDoesNotReachTheHub(t *testing.T) {
	t.Parallel()

	spoke := &TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
		Spec: TalosControlPlaneSpec{
			ControlPlaneConfig: ControlPlaneConfig{
				InitConfig:         cabptv1alpha3.TalosConfigSpec{GenerateType: "init"},
				ControlPlaneConfig: cabptv1alpha3.TalosConfigSpec{GenerateType: "controlplane"},
			},
		},
	}

	hub := &cpv1beta1.TalosControlPlane{}
	require.NoError(t, spoke.ConvertTo(hub))
	require.Equal(t, "controlplane", hub.Spec.ControlPlaneConfig.ControlPlaneConfig.GenerateType)

	back := &TalosControlPlane{}
	require.NoError(t, back.ConvertFrom(hub))
	require.Empty(t, back.Spec.ControlPlaneConfig.InitConfig.GenerateType, "nothing restores init from a hub object that never had it")
}
```

and in the existing object round-trip test build the hub without `InitConfig` and assert only on `ControlPlaneConfig.ImageFactory`.

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./api/v1alpha3/ -run TestInitConfigDoesNotReachTheHub -count=1`
Expected: FAIL (the hub still has `InitConfig` and the round trip carries it).

- [ ] **Step 3: Remove the field and the path**

`api/v1beta1/taloscontrolplane_types.go`:

```go
type ControlPlaneConfig struct {
	ControlPlaneConfig cabptv1.TalosConfigSpec `json:"controlplane"`
}
```

`api/v1beta1/taloscontrolplane_webhook.go`: delete the `init` line of `validateControlPlaneConfig`.

`controllers/taloscontrolplane_controller.go`: `bootstrapConfig := &tcp.Spec.ControlPlaneConfig.ControlPlaneConfig` with the `InitConfig` special case deleted; in the `default:` branch of the machine-count switch delete the `if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() { ... }` block (keep whatever follows it); drop the `first` parameter from `bootControlPlane` if nothing else uses it.

`controllers/configs.go`: delete the `if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() { return r.talosconfigFromWorkloadCluster(...) }` branch, the whole `talosconfigFromWorkloadCluster` function, the `cabptv1alpha3` import and the comment block about it, and `reflect` if unused.

`api/v1alpha3/conversion.go`: add

```go
// Convert_v1alpha3_ControlPlaneConfig_To_v1beta1_ControlPlaneConfig drops `init`, which the hub
// no longer has; the provider has bootstrapped through the Cluster API Bootstrap call since v0.4.0.
func Convert_v1alpha3_ControlPlaneConfig_To_v1beta1_ControlPlaneConfig(in *ControlPlaneConfig, out *cpv1beta1.ControlPlaneConfig, s apimachineryconversion.Scope) error {
	return Convert_v1alpha3_TalosConfigSpec_To_v1beta1_TalosConfigSpec(&in.ControlPlaneConfig, &out.ControlPlaneConfig, s)
}
```

and in `ConvertTo` delete the `InitConfig.ImageFactory` restore line.

- [ ] **Step 4: Regenerate and run**

Run the three generation commands, then `GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go test ./api/... ./controllers/... -count=1`.
Expected: build OK, all tests pass; the CRD YAML no longer lists `init`.

- [ ] **Step 5: Commit**

```bash
git add api config controllers
git commit -m "feat(api)!: remove the init configuration and the init-node bootstrap path

spec.controlPlaneConfig.init has been deprecated since v0.4.0; clusters
bootstrap through the Cluster API Bootstrap call. The path that read a
talosconfig out of the bootstrap provider's v1alpha3 status goes with it."
```

---

### Task 6: v1alpha3 conditions without the CAPI v1beta1 package

**Files:**
- Create: `api/v1alpha3/condition_types.go`
- Modify: `api/v1alpha3/conditions.go`, `api/v1alpha3/v1beta2_conditions.go`, `api/v1alpha3/taloscontrolplane_types.go`, `api/v1alpha3/conversion.go`

- [ ] **Step 1: Add the local types**

Create `api/v1alpha3/condition_types.go` with the MPL header and:

```go
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
	ConditionSeverityError   ConditionSeverity = "Error"
	ConditionSeverityWarning ConditionSeverity = "Warning"
	ConditionSeverityInfo    ConditionSeverity = "Info"
	ConditionSeverityNone    ConditionSeverity = ""
)

// Condition defines an observation of a TalosControlPlane's operational state.
type Condition struct {
	Type               ConditionType          `json:"type"`
	Status             corev1.ConditionStatus `json:"status"`
	Severity           ConditionSeverity      `json:"severity,omitempty"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime"`
	Reason             string                 `json:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"`
}

// Conditions is a list of Condition.
type Conditions []Condition
```

In `conditions.go` and `taloscontrolplane_types.go` replace `clusterv1.ConditionType`/`clusterv1.Conditions` with the local types and drop the v1beta1 import; in `v1beta2_conditions.go` map the constants onto `sigs.k8s.io/cluster-api/api/core/v1beta2` (`ReadyCondition`, `ReadyReason`, `NotReadyReason`, `ReadyUnknownReason`, `AvailableReason`, `NotAvailableReason`, `InternalErrorReason`) the way the file's existing names pair with them.

In `conversion.go` replace the two `Convert_v1beta1_Condition_To_v1_Condition`/`Convert_v1_Condition_To_v1beta1_Condition` wrappers with:

```go
func Convert_v1alpha3_Condition_To_v1_Condition(in *Condition, out *metav1.Condition, _ apimachineryconversion.Scope) error {
	*out = metav1.Condition{Type: string(in.Type), Status: metav1.ConditionStatus(in.Status), LastTransitionTime: in.LastTransitionTime, Reason: in.Reason, Message: in.Message}

	return nil
}

func Convert_v1_Condition_To_v1alpha3_Condition(in *metav1.Condition, out *Condition, _ apimachineryconversion.Scope) error {
	*out = Condition{Type: ConditionType(in.Type), Status: corev1.ConditionStatus(in.Status), LastTransitionTime: in.LastTransitionTime, Reason: in.Reason, Message: in.Message}

	return nil
}
```

- [ ] **Step 2: Regenerate and run**

Run the deepcopy and conversion-gen commands, then `GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go test ./api/... -count=1`.
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add api
git commit -m "refactor(api): give v1alpha3 its own condition types"
```

---

### Task 7: Remove status.deprecated.v1beta1 and derive the legacy fields

**Files:**
- Modify: `api/v1beta1/taloscontrolplane_types.go` (delete `Deprecated`, the two deprecated status types, `GetV1Beta1Conditions`, `SetV1Beta1Conditions`, `V1Beta1DeprecatedStatus`, and the v1beta1-style condition and reason constants)
- Modify: `controllers/taloscontrolplane_controller.go` (`updateStatus` and the deferred requeue guard)
- Modify: `api/v1alpha3/conversion.go`, `api/v1alpha3/conversion_test.go`
- Modify: `controllers/controllers_test.go` (four `V1Beta1DeprecatedStatus().UpdatedReplicas` assertions)
- Regenerate: deepcopy, conversions, CRDs

- [ ] **Step 1: Write the failing conversion test**

Append to `api/v1alpha3/conversion_test.go`:

```go
// Every legacy status field a v1alpha3 client reads is derived from the v1beta2 status.
func TestControlPlaneStatusConvertFromDerivesTheLegacyFields(t *testing.T) {
	t.Parallel()

	transition := metav1.NewTime(time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC))
	hub := &cpv1beta1.TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
		Status: cpv1beta1.TalosControlPlaneStatus{
			Initialization:   cpv1beta1.TalosControlPlaneInitializationStatus{ControlPlaneInitialized: ptr.To(true)},
			Replicas:         3,
			ReadyReplicas:    2,
			UpToDateReplicas: ptr.To[int32](1),
			Conditions: []metav1.Condition{{
				Type: "Available", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: transition,
			}},
		},
	}

	spoke := &TalosControlPlane{}
	require.NoError(t, spoke.ConvertFrom(hub))

	require.True(t, spoke.Status.Initialized)
	require.True(t, spoke.Status.Ready)
	require.EqualValues(t, 1, spoke.Status.UnavailableReplicas)
	require.EqualValues(t, 1, spoke.Status.UpdatedReplicas)
	require.Nil(t, spoke.Status.FailureReason)
	require.Equal(t, Conditions{{Type: "Available", Status: corev1.ConditionTrue, Reason: "Available", LastTransitionTime: transition}}, spoke.Status.Conditions)
}
```

Add `"time"`, `corev1 "k8s.io/api/core/v1"` and `"k8s.io/utils/ptr"` to the imports.

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./api/v1alpha3/ -run TestControlPlaneStatusConvertFromDerivesTheLegacyFields -count=1`
Expected: FAIL (`Ready` false, `UnavailableReplicas` 0, conditions empty: the current conversion reads the deprecated block).

- [ ] **Step 3: Remove the block and derive**

In `api/v1beta1/taloscontrolplane_types.go` delete everything listed under Files for this task; the v1beta2-style constants stay.

In `controllers/taloscontrolplane_controller.go`:
- `updateStatus`: delete every `deprecated.` assignment and the `deprecated := tcp.V1Beta1DeprecatedStatus()` line; keep `Replicas`, `ReadyReplicas`, `AvailableReplicas`, `UpToDateReplicas`, `Initialization.ControlPlaneInitialized` and the conditions.
- the deferred requeue guard becomes:

```go
		if reterr == nil && res.RequeueAfter <= 0 && tcp.ObjectMeta.DeletionTimestamp.IsZero() {
			if tcp.Status.ReadyReplicas == 0 || tcp.Status.ReadyReplicas < tcp.Status.Replicas {
				res = ctrl.Result{RequeueAfter: 20 * time.Second}
			}
		}
```

In `api/v1alpha3/conversion.go`:
- `Convert_v1alpha3_TalosControlPlaneStatus_To_v1beta1_TalosControlPlaneStatus`: keep `out.Conditions = nil` and the `in.V1Beta2` read; delete the whole "Move legacy conditions ... to the deprecated field" block; keep the `Initialized` to `ControlPlaneInitialized` move.
- `Convert_v1beta1_TalosControlPlaneStatus_To_v1alpha3_TalosControlPlaneStatus`: delete the `out.Conditions = nil` reset and the `in.Deprecated` block; after the generated call add:

```go
	// The legacy fields are derived from the v1beta2 status.
	out.Initialized = ptr.Deref(in.Initialization.ControlPlaneInitialized, false)
	out.Ready = in.ReadyReplicas > 0
	out.UnavailableReplicas = in.Replicas - in.ReadyReplicas
	out.UpdatedReplicas = ptr.Deref(in.UpToDateReplicas, 0)
	out.FailureReason = nil
	out.FailureMessage = nil
```

(keep the existing `V1Beta2` population that follows). Remove the `clusterv1beta1` import once unused.

In `controllers/controllers_test.go` replace each `g.Expect(tcp.V1Beta1DeprecatedStatus().UpdatedReplicas).To(BeEquivalentTo(N))` with `g.Expect(tcp.Status.UpToDateReplicas).To(HaveValue(BeEquivalentTo(N)))`.

- [ ] **Step 4: Regenerate and run everything**

Run the three generation commands, then `GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go vet ./... && GOTOOLCHAIN=auto go test ./api/... ./controllers/... ./internal/runtimeclient/... -count=1`.
Expected: build and vet OK, all tests pass, the CRD YAML no longer has `deprecated`.

- [ ] **Step 5: Commit**

```bash
git add api config controllers
git commit -m "feat(api)!: drop the Cluster API v1beta1 compatibility block from TalosControlPlane status

status.deprecated.v1beta1 mirrored the old contract's conditions, ready,
initialized and replica counters next to the v1beta2 fields. Nothing
reads it and CAPI core v1.12+ reads only the v1beta2 fields; v1alpha3
derives every legacy field from them on conversion."
```

---

### Task 8: Documentation and release notes

**Files:**
- Modify: `README.md`, `hack/release.toml`

- [ ] **Step 1: Document the removals**

In `README.md` remove any mention of `init` under the control plane configuration (search for "init") and add under the compatibility section:

```markdown
The `v0.7.x` series drops `spec.controlPlaneConfig.init` (deprecated since v0.4.0) and the
`status.deprecated.v1beta1` block; v1alpha3 clients still see the old-shape status, derived from
the v1beta2 fields.
```

In `hack/release.toml` add:

```
[notes.api]
    title = "API"
    description = """\
`spec.controlPlaneConfig.init` and the init-node bootstrap path are removed; clusters have
bootstrapped through the Cluster API Bootstrap call since v0.4.0. `status.deprecated.v1beta1`
is removed; only the v1beta2 conditions, initialization and replica counters remain. v1alpha3
clients see the legacy fields derived from them.
"""
```

- [ ] **Step 2: Commit**

```bash
git add README.md hack/release.toml
git commit -m "docs: describe the init and v1beta1 status removals"
```

---

## Self-review

- Spec section 1 (status removal, derivations, v1alpha3 local types): Tasks 6, 7.
- Spec section 2 (init removal): Task 5.
- Spec section 3 (requeue, apply, recorder, scheme builder): Tasks 1, 2, 3, 4.
- Consequences and docs: Task 8.
- Names: `register(...)`, `schemeBuilder`, `localSchemeBuilder`, `Convert_v1alpha3_Condition_To_v1_Condition`, `Convert_v1alpha3_ControlPlaneConfig_To_v1beta1_ControlPlaneConfig`, `ssa.Patch`, `ssa.DryRunPatch`; each defined where introduced.
