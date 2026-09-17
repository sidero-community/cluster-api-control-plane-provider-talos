# KCP-style machine lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make control plane Machine deletion follow KCP: unconditional hook, quorum-safe etcd removal, delete-only scale-down, preflight-gated scale operations, health checks that ignore deleting machines.

**Architecture:** All changes live in `controllers/`. The hook (`preterminate.go`) gains a quorum verdict; a new `preflight.go` gates `scaleUpControlPlane`/`scaleDownControlPlane` in `scale.go`; `health.go` opens clients through the `etcdCalls` seam so everything is unit-testable with the existing `fakeEtcdDialer`.

**Tech Stack:** Go 1.26, controller-runtime v0.24, Cluster API v1.14.2 (`sigs.k8s.io/cluster-api/util/collections`, `conditions`), Talos machinery v1.14 (`ServiceInfo`, `ServiceList`, `EtcdMemberList`).

**Spec:** `docs/design/2026-09-17-kcp-machine-lifecycle.md`

## Global Constraints

- Conventional commits with a body, no sign-off trailer.
- Unit test command: `GOTOOLCHAIN=go1.26.5 go test $(go list ./... | grep -v /integration)` (the integration suite needs a live cluster).
- `preTerminateRequeueAfter` (15s) for hook holds, `preflightFailedRequeueAfter` = 10s for scale holds, 20s after a delete request.
- Events: Machine events via `recordMachineEvent` (action `PreTerminate`); TalosControlPlane events via a new `recordControlPlaneEvent` (action `Preflight`).
- Never delete workload `Node` objects.

---

### Task 1: The hook is unconditional

**Files:**
- Modify: `main.go` (flag, var, reconciler literal)
- Modify: `controllers/taloscontrolplane_controller.go:72-75` (field)
- Modify: `controllers/preterminate.go` (`desiredMachineAnnotations`, `stampPreTerminateHooks`)
- Modify: `controllers/preterminate_test.go:361-408`, `controllers/helpers_test.go:73-96`
- Modify: `README.md` flags table

**Interfaces:**
- Produces: `TalosControlPlaneReconciler` without `EnableMachinePreTerminateHook`.

- [ ] **Step 1: Delete the flag-off tests and drop the field from fixtures**

Remove `TestDesiredMachineAnnotations_NoStampWhenDisabled` and `TestPreTerminateHook_NoStampWhenFlagOff`. Rename `TestDesiredMachineAnnotations_StampsHookWhenEnabled` to `TestDesiredMachineAnnotations_AlwaysStampsHook` and build its reconciler as `&TalosControlPlaneReconciler{}`. Remove `EnableMachinePreTerminateHook: true` from `newPreTerminateFixtureWithTCP` and from `newReconciler` in `helpers_test.go`.

- [ ] **Step 2: Run the package to see it fail to compile only on the removed field** (it still compiles: the field exists). Expected: PASS. This task is a removal; the red is the field's disappearance in Step 3 breaking nothing.

- [ ] **Step 3: Remove the field, the flag and the checks**

`controllers/preterminate.go`:

```go
func (r *TalosControlPlaneReconciler) desiredMachineAnnotations(tcp *controlplanev1.TalosControlPlane) map[string]string {
	annotations := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[PreTerminateHookCleanupAnnotation] = ""

	return annotations
}
```

In `stampPreTerminateHooks` delete the `if !r.EnableMachinePreTerminateHook { return nil }` guard. In the controller struct delete the field and its comment. In `main.go` delete `enableMachinePreTerminateHook`, its `fs.BoolVar`, and the literal's field. README: drop the flag's table row and say the hook is unconditional.

- [ ] **Step 4: Run** `GOTOOLCHAIN=go1.26.5 go build ./... && GOTOOLCHAIN=go1.26.5 go test ./controllers/ -run 'PreTerminate|DesiredMachineAnnotations'`. Expected: PASS.

- [ ] **Step 5: Commit** `feat(controllers)!: always put the pre-terminate hook on control plane machines`

---

### Task 2: Health checks through the seam, deleting machines excluded

**Files:**
- Modify: `controllers/etcdclient.go` (interface)
- Modify: `controllers/health.go` (`nodesHealthcheck`, new `healthCheckable`)
- Modify: `controllers/etcd.go` (`etcdHealthcheck` uses `healthCheckable`)
- Modify: `controllers/taloscontrolplane_controller.go` (`reconcileNodeHealth`)
- Test: `controllers/health_test.go` (new), `controllers/preterminate_test.go` (fake gains `ServiceList`)

**Interfaces:**
- Produces: `etcdCalls.ServiceList(ctx, ...grpc.CallOption) (*machineapi.ServiceListResponse, error)`; `healthCheckable(machines []clusterv1.Machine) []clusterv1.Machine`; `fakeEtcdCalls.serviceList` field and test helpers `healthyServices()`, `unhealthyServices()`.

- [ ] **Step 1: Extend the fake and write the failing tests**

In `preterminate_test.go` add to `fakeEtcdCalls`: `serviceList *machineapi.ServiceListResponse`, `serviceListErr error`, `serviceListCalls int`, and

```go
func (f *fakeEtcdCalls) ServiceList(_ context.Context, _ ...grpc.CallOption) (*machineapi.ServiceListResponse, error) {
	f.serviceListCalls++

	if f.serviceListErr != nil {
		return nil, f.serviceListErr
	}

	if f.serviceList == nil {
		return &machineapi.ServiceListResponse{}, nil
	}

	return f.serviceList, nil
}

func healthyServices() *machineapi.ServiceListResponse {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{
		{Id: "etcd", State: "Running", Health: &machineapi.ServiceHealth{Healthy: true}},
		{Id: "kubelet", State: "Running", Health: &machineapi.ServiceHealth{Healthy: true}},
	}}}}
}

func unhealthyServices() *machineapi.ServiceListResponse {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{
		{Id: "etcd", State: "Running", Health: &machineapi.ServiceHealth{Healthy: true}},
		{Id: "kubelet", State: "Failed", Health: &machineapi.ServiceHealth{Healthy: false}},
	}}}}
}
```

Include `serviceListCalls` in `totalEtcdCalls`. New `controllers/health_test.go`:

```go
package controllers

func TestReconcileNodeHealth_IgnoresDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	victim := newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingWaitingForPreTerminateHookReason))
	peer := newPTMachine(tcp, "cp-2", ptHooked)

	f := newPreTerminateFixture(t, victim, peer)
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: unhealthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	_, err := f.r.reconcileNodeHealth(context.Background(), f.cluster, f.tcp, f.machines)
	require.NoError(t, err)

	assert.True(t, conditions.IsTrue(f.tcp, string(controlplanev1.ControlPlaneComponentsHealthyCondition)))
	assert.NotContains(t, f.dialer.dialed, "cp-1", "a deleting machine is not expected to be healthy")
}

func TestReconcileNodeHealth_ReportsAnUnhealthyService(t *testing.T) {
	tcp := newPreTerminateTCP()
	cp1 := newPTMachine(tcp, "cp-1", ptHooked)
	cp2 := newPTMachine(tcp, "cp-2", ptHooked)

	f := newPreTerminateFixture(t, cp1, cp2)
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: unhealthyServices()}

	res, err := f.r.reconcileNodeHealth(context.Background(), f.cluster, f.tcp, f.machines)
	require.Error(t, err)
	assert.Positive(t, res.RequeueAfter)

	cond := conditions.Get(f.tcp, string(controlplanev1.ControlPlaneComponentsHealthyCondition))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, controlplanev1.ControlPlaneComponentsUnhealthyReason, cond.Reason)
	assert.Contains(t, cond.Message, `machine "cp-2"`)
	assert.Contains(t, cond.Message, "kubelet")
}
```

- [ ] **Step 2: Run** `go test ./controllers/ -run TestReconcileNodeHealth -v`. Expected: FAIL (the fake does not satisfy `etcdCalls` yet / `nodesHealthcheck` dials through `talosconfigForMachines` and fails on the missing talosconfig secret).

- [ ] **Step 3: Implement**

`etcdclient.go`: add `ServiceList(ctx context.Context, callOptions ...grpc.CallOption) (*machineapi.ServiceListResponse, error)` to `etcdCalls`; reword the comment to "the subset of the Talos machine API the etcd cleanup and health-check paths use".

`health.go`:

```go
// healthCheckable narrows machines to the ones whose services are expected to be healthy:
// not deleting, and not marked as leaving etcd. Mirrors etcdHealthcheck.
func healthCheckable(machines []clusterv1.Machine) []clusterv1.Machine {
	out := make([]clusterv1.Machine, 0, len(machines))

	for _, machine := range machines {
		if !machine.DeletionTimestamp.IsZero() || machine.Annotations[etcdLeavingAnnotation] == "true" {
			continue
		}

		out = append(out, machine)
	}

	return out
}
```

`nodesHealthcheck`: replace `r.talosconfigForMachines(ctx, tcp, machine)` with `r.etcdClientFor(ctx, tcp, machine)` (variable type becomes `etcdCalls`). `reconcileNodeHealth`: call `r.nodesHealthcheck(ctx, tcp, healthCheckable(machines.Items))`. `etcdHealthcheck`: replace its inline filter loop with `machines := healthCheckable(ownedMachines)`.

- [ ] **Step 4: Run** the two tests, then `go test ./controllers/`. Expected: PASS.

- [ ] **Step 5: Commit** `fix(controllers): keep deleting machines out of the control plane health check`

---

### Task 3: Preflight checks

**Files:**
- Create: `controllers/preflight.go`
- Test: `controllers/preflight_test.go`

**Interfaces:**
- Consumes: `healthCheckable`, `nodesHealthcheck` (Task 2), `etcdLeavingAnnotation`.
- Produces:
  - `type preflightFailure struct{ message string }`
  - `func (r *TalosControlPlaneReconciler) preflightChecks(ctx, tcp, machines collections.Machines, excludeFor ...*clusterv1.Machine) *preflightFailure`
  - `func (r *TalosControlPlaneReconciler) holdScaleOperation(tcp, reason, summary string, failure *preflightFailure) ctrl.Result`
  - `func (r *TalosControlPlaneReconciler) recordControlPlaneEvent(tcp, eventType, reason, message string)`
  - consts `preflightFailedRequeueAfter = 10 * time.Second`, `controlPlaneUnhealthyEvent = "ControlPlaneUnhealthy"`.

- [ ] **Step 1: Write the failing tests** (`preflight_test.go`, package `controllers`)

```go
func ptEtcdHealthy(tcp *controlplanev1.TalosControlPlane) {
	conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.EtcdClusterHealthyCondition), Status: metav1.ConditionTrue, Reason: controlplanev1.EtcdClusterHealthyReason})
}

func ptNoNodeRef(m *clusterv1.Machine) { m.Status.NodeRef = clusterv1.MachineNodeReference{} }

func (f *preTerminateFixture) collection() collections.Machines { return collections.FromMachineList(f.machines) }

func TestPreflight_PassesForAHealthyControlPlane(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, f.collection()))
}

func TestPreflight_PassesWithNoMachines(t *testing.T) {
	f := newPreTerminateFixture(t)
	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, collections.New()))
}

func TestPreflight_WaitsForDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp,
		newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingDrainingNodeReason)),
		newPTMachine(tcp, "cp-2", ptHooked))

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)
	assert.Equal(t, "waiting for machines to be deleted: cp-1", failure.message)
	assert.Empty(t, f.dialer.dialed, "nothing else is checked while a deletion is in flight")
}

func TestPreflight_RequiresANodeForEveryMachine(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked, ptNoNodeRef))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)
	assert.Contains(t, failure.message, `machine "cp-2" does not have a corresponding Node yet`)
	assert.NotContains(t, f.dialer.dialed, "cp-2", "a machine without a node is not asked about its services")
}

func TestPreflight_RequiresHealthyComponents(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: unhealthyServices()}

	failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
	require.NotNil(t, failure)
	assert.Contains(t, failure.message, "control plane components are not healthy")
	assert.Contains(t, failure.message, `machine "cp-2"`)
}

func TestPreflight_RequiresHealthyEtcd(t *testing.T) {
	t.Run("condition false", func(t *testing.T) {
		tcp := newPreTerminateTCP()
		conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.EtcdClusterHealthyCondition), Status: metav1.ConditionFalse, Reason: controlplanev1.EtcdClusterUnhealthyReason, Message: "expected to have 2 members, got 3"})
		f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked))
		f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

		failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "etcd cluster is not healthy: expected to have 2 members, got 3")
	})
	t.Run("condition missing", func(t *testing.T) {
		f := newPreTerminateFixture(t, newPTMachine(newPreTerminateTCP(), "cp-1", ptHooked))
		f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}

		failure := f.r.preflightChecks(context.Background(), f.tcp, f.collection())
		require.NotNil(t, failure)
		assert.Contains(t, failure.message, "etcd cluster health is unknown")
	})
}

func TestPreflight_ExcludesTheScaleDownVictim(t *testing.T) {
	tcp := newPreTerminateTCP()
	ptEtcdHealthy(tcp)
	victim := newPTMachine(tcp, "cp-1", ptHooked, ptNoNodeRef)
	f := newPreTerminateFixtureWithTCP(t, tcp, victim, newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: unhealthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	assert.Nil(t, f.r.preflightChecks(context.Background(), f.tcp, f.collection(), victim))
	assert.NotContains(t, f.dialer.dialed, "cp-1")
}
```

- [ ] **Step 2: Run** `go test ./controllers/ -run TestPreflight`. Expected: FAIL to compile (`preflightChecks` undefined).

- [ ] **Step 3: Implement** `controllers/preflight.go`

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	// preflightFailedRequeueAfter is how long a scale operation waits before the preflight
	// checks are run again.
	preflightFailedRequeueAfter = 10 * time.Second

	// controlPlaneUnhealthyEvent is recorded on the TalosControlPlane while a scale operation
	// is held back by the preflight checks. Same name as KCP's.
	controlPlaneUnhealthyEvent = "ControlPlaneUnhealthy"
)

// preflightFailure explains why the control plane is not ready for a scale operation.
type preflightFailure struct {
	message string
}

// preflightChecks mirrors KCP's preflightChecks: a scale operation may only start when no
// machine is deleting, every machine has a Node, the control plane components are healthy
// and etcd is healthy. excludeFor names machines that are about to be deleted, which are
// therefore not required to be healthy or to have a Node. A nil result means go ahead.
func (r *TalosControlPlaneReconciler) preflightChecks(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	machines collections.Machines,
	excludeFor ...*clusterv1.Machine,
) *preflightFailure {
	if machines.Len() == 0 {
		return nil
	}

	// Serialization: one membership change at a time. Nothing else is worth checking while a
	// deletion is in flight, and the deleting machine would fail the checks below anyway.
	if deleting := machines.Filter(collections.HasDeletionTimestamp); deleting.Len() > 0 {
		names := deleting.Names()
		sort.Strings(names)

		return &preflightFailure{message: "waiting for machines to be deleted: " + strings.Join(names, ", ")}
	}

	excluded := map[string]struct{}{}

	for _, machine := range excludeFor {
		if machine != nil {
			excluded[machine.Name] = struct{}{}
		}
	}

	names := machines.Names()
	sort.Strings(names)

	var (
		failures  []string
		checkable []clusterv1.Machine
	)

	for _, name := range names {
		machine := machines[name]

		if _, ok := excluded[name]; ok {
			continue
		}

		if !machine.Status.NodeRef.IsDefined() {
			failures = append(failures, fmt.Sprintf("machine %q does not have a corresponding Node yet", name))

			continue
		}

		checkable = append(checkable, *machine)
	}

	if err := r.nodesHealthcheck(ctx, tcp, healthCheckable(checkable)); err != nil {
		failures = append(failures, fmt.Sprintf("control plane components are not healthy: %v", err))
	}

	if c := conditions.Get(tcp, string(controlplanev1.EtcdClusterHealthyCondition)); c == nil || c.Status != metav1.ConditionTrue {
		message := "etcd cluster health is unknown"
		if c != nil && c.Message != "" {
			message = "etcd cluster is not healthy: " + c.Message
		}

		failures = append(failures, message)
	}

	if len(failures) == 0 {
		return nil
	}

	return &preflightFailure{message: strings.Join(failures, "; ")}
}

// holdScaleOperation records why a scale operation is waiting -- on the Resized condition, in
// the log and as an event on the TalosControlPlane -- and returns the result that retries it.
func (r *TalosControlPlaneReconciler) holdScaleOperation(tcp *controlplanev1.TalosControlPlane, reason, summary string, failure *preflightFailure) ctrl.Result {
	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: summary + ": " + failure.message,
	})

	r.Log.Info("waiting for control plane to pass preflight checks", "operation", reason, "failures", failure.message)
	r.recordControlPlaneEvent(tcp, corev1.EventTypeWarning, controlPlaneUnhealthyEvent,
		"Waiting for control plane to pass preflight checks to continue reconciliation: "+failure.message)

	return ctrl.Result{RequeueAfter: preflightFailedRequeueAfter}
}

// recordControlPlaneEvent records an event on the TalosControlPlane, if a recorder is wired up.
func (r *TalosControlPlaneReconciler) recordControlPlaneEvent(tcp *controlplanev1.TalosControlPlane, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(tcp, nil, eventType, reason, "Preflight", "%s", message)
}
```

- [ ] **Step 4: Run** `go test ./controllers/ -run TestPreflight -v`. Expected: PASS.

- [ ] **Step 5: Commit** `feat(controllers): add KCP-style preflight checks for scale operations`

---

### Task 4: Scale-up waits for the preflight checks

**Files:**
- Modify: `controllers/scale.go` (`scaleUpControlPlane`)
- Test: `controllers/scale_test.go` (new)

- [ ] **Step 1: Write the failing test**

```go
package controllers

func newScaleControlPlane(f *preTerminateFixture) *ControlPlane {
	return &ControlPlane{TCP: f.tcp, Cluster: f.cluster, Machines: f.collection()}
}

func TestScaleUp_WaitsForDeletingMachines(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](3)
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp,
		newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingDrainingNodeReason)),
		newPTMachine(tcp, "cp-2", ptHooked))

	res, err := f.r.scaleUpControlPlane(context.Background(), f.cluster, f.tcp, newScaleControlPlane(f))
	require.NoError(t, err)
	assert.Equal(t, preflightFailedRequeueAfter, res.RequeueAfter)

	var machines clusterv1.MachineList
	require.NoError(t, f.r.Client.List(context.Background(), &machines))
	assert.Len(t, machines.Items, 2, "no replacement while a deletion is in flight")

	cond := conditions.Get(f.tcp, string(controlplanev1.ResizedCondition))
	require.NotNil(t, cond)
	assert.Equal(t, controlplanev1.ScalingUpReason, cond.Reason)
	assert.Contains(t, cond.Message, "Scaling up control plane to 3 replicas (actual 2): waiting for machines to be deleted: cp-1")
	assert.Contains(t, strings.Join(f.events(), "\n"), controlPlaneUnhealthyEvent)
}

func TestScaleUp_WaitsForHealthyEtcd(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](3)
	f := newPreTerminateFixtureWithTCP(t, tcp, newPTMachine(tcp, "cp-1", ptHooked), newPTMachine(tcp, "cp-2", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: healthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}

	res, err := f.r.scaleUpControlPlane(context.Background(), f.cluster, f.tcp, newScaleControlPlane(f))
	require.NoError(t, err)
	assert.Equal(t, preflightFailedRequeueAfter, res.RequeueAfter)
	assert.Contains(t, conditions.Get(f.tcp, string(controlplanev1.ResizedCondition)).Message, "etcd cluster health is unknown")
}
```

- [ ] **Step 2: Run** `go test ./controllers/ -run TestScaleUp -v`. Expected: FAIL (scale-up goes straight to `bootControlPlane`, so the condition message lacks the preflight text and the requeue differs).

- [ ] **Step 3: Implement**

```go
func (r *TalosControlPlaneReconciler) scaleUpControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, controlPlane *ControlPlane) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling up control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines); failure != nil {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingUpReason, summary, failure), nil
	}

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.ScalingUpReason,
		Message: summary,
	})

	r.Log.Info("scaling up control plane", "Desired", desiredReplicas, "Existing", numMachines)

	return r.bootControlPlane(ctx, cluster, tcp)
}
```

- [ ] **Step 4: Run** `go test ./controllers/`. Expected: PASS (including `TestRollingUpdate`, whose surge scale-up now passes the checks against the suite's fake Talos servers).

- [ ] **Step 5: Commit** `feat(controllers): hold scale-up until the control plane passes the preflight checks`

---

### Task 5: Scale-down only deletes the Machine

**Files:**
- Modify: `controllers/scale.go` (`scaleDownControlPlane`, `deleteControlPlaneMachine`; delete `deleteNode`)
- Modify: `controllers/etcd.go` (delete `gracefulEtcdLeave`)
- Modify: `controllers/upgrade.go`, `controllers/taloscontrolplane_controller.go:1079` (call sites drop `cluster`)
- Test: `controllers/scale_test.go`; delete the four `TestDeleteControlPlaneMachine_*` tests in `preterminate_test.go`

**Interfaces:**
- Produces: `scaleDownControlPlane(ctx, tcp, controlPlane, machinesRequireUpgrade)`, `deleteControlPlaneMachine(ctx, machine) (ctrl.Result, error)`.

- [ ] **Step 1: Write the failing tests** (append to `scale_test.go`; delete the old `TestDeleteControlPlaneMachine_*` tests)

```go
func ptDeleteAnnotation(m *clusterv1.Machine) {
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}

	m.Annotations[clusterv1.DeleteMachineAnnotation] = ""
}

func TestScaleDown_DeletesTheVictimAndNothingElse(t *testing.T) {
	tcp := newPreTerminateTCP()
	victim := newPTMachine(tcp, "cp-1", ptHooked)
	f := newPreTerminateFixture(t, victim, newPTMachine(tcp, "cp-2", ptHooked))

	res, err := f.r.deleteControlPlaneMachine(context.Background(), victim)
	require.NoError(t, err)
	assert.Equal(t, 20*time.Second, res.RequeueAfter)

	assert.Empty(t, f.dialer.dialed, "etcd membership is the deletion pipeline's job")

	var remaining clusterv1.MachineList
	require.NoError(t, f.r.Client.List(context.Background(), &remaining))
	require.Len(t, remaining.Items, 1)
	assert.Equal(t, "cp-2", remaining.Items[0].Name)
}

func TestScaleDown_WaitsForDeletingMachinesBeforeAnythingElse(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](2)
	ptEtcdHealthy(tcp)
	f := newPreTerminateFixtureWithTCP(t, tcp,
		newPTMachine(tcp, "cp-1", ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingDrainingNodeReason)),
		newPTMachine(tcp, "cp-2", ptHooked),
		newPTMachine(tcp, "cp-3", ptHooked))

	res, err := f.r.scaleDownControlPlane(context.Background(), f.tcp, newScaleControlPlane(f), collections.New())
	require.NoError(t, err)
	assert.Equal(t, preflightFailedRequeueAfter, res.RequeueAfter)
	assert.Empty(t, f.dialer.dialed)

	cond := conditions.Get(f.tcp, string(controlplanev1.ResizedCondition))
	require.NotNil(t, cond)
	assert.Equal(t, controlplanev1.ScalingDownReason, cond.Reason)
	assert.Contains(t, cond.Message, "waiting for machines to be deleted: cp-1")

	var remaining clusterv1.MachineList
	require.NoError(t, f.r.Client.List(context.Background(), &remaining))
	assert.Len(t, remaining.Items, 3, "no second deletion while one is in flight")
}

func TestScaleDown_ExcludesTheVictimFromThePreflightChecks(t *testing.T) {
	tcp := newPreTerminateTCP()
	tcp.Spec.Replicas = ptr.To[int32](2)
	ptEtcdHealthy(tcp)
	victim := newPTMachine(tcp, "cp-1", ptHooked, ptDeleteAnnotation, ptNoNodeRef)
	f := newPreTerminateFixtureWithTCP(t, tcp, victim, newPTMachine(tcp, "cp-2", ptHooked), newPTMachine(tcp, "cp-3", ptHooked))
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", serviceList: unhealthyServices()}
	f.dialer.clients["cp-2"] = &fakeEtcdCalls{name: "cp-2", serviceList: healthyServices()}
	f.dialer.clients["cp-3"] = &fakeEtcdCalls{name: "cp-3", serviceList: healthyServices()}

	_, err := f.r.scaleDownControlPlane(context.Background(), f.tcp, newScaleControlPlane(f), collections.New())
	require.NoError(t, err)

	// The fixture has no talosconfig secret, so ensureNodesBooted cannot run; what matters is
	// that the preflight checks let the operation past them.
	assert.NotContains(t, strings.Join(f.events(), "\n"), controlPlaneUnhealthyEvent)
	assert.NotContains(t, conditions.Get(f.tcp, string(controlplanev1.ResizedCondition)).Message, "does not have a corresponding Node")
	assert.NotContains(t, f.dialer.dialed, "cp-1")
}
```

- [ ] **Step 2: Run** `go test ./controllers/ -run TestScaleDown`. Expected: FAIL to compile (signatures) after the old tests are removed.

- [ ] **Step 3: Implement** in `scale.go`

```go
func (r *TalosControlPlaneReconciler) scaleDownControlPlane(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	controlPlane *ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling down control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.ScalingDownReason,
		Message: summary,
	})

	if numMachines == 1 {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.ResizedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.ScalingDownReason,
			Message: "Cannot scale down control plane nodes to 0",
		})

		return ctrl.Result{}, nil
	}

	if numMachines == 0 {
		return ctrl.Result{}, fmt.Errorf("no machines found")
	}

	victim, err := selectMachineForScaleDown(controlPlane, machinesRequireUpgrade)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The victim is excluded: it may be exactly the unhealthy machine a rollout is replacing.
	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines, victim); failure != nil {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingDownReason, summary, failure), nil
	}

	if err := r.ensureNodesBooted(ctx, controlPlane.TCP, collections.ToMachineList(controlPlane.Machines).Items); err != nil {
		r.Log.Info("waiting for all nodes to finish boot sequence", "error", err)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.Log.Info("scaling down control plane", "Desired", desiredReplicas, "Existing", numMachines)

	return r.deleteControlPlaneMachine(ctx, victim)
}

// deleteControlPlaneMachine requests the deletion of a control plane machine, and nothing
// else: its etcd membership is resolved by the deletion pipeline at the pre-terminate phase
// (reconcileMachinePreTerminateHooks) and its Node is deleted by the core Machine controller
// once the infrastructure is gone -- exactly what happens on a `kubectl delete machine`.
func (r *TalosControlPlaneReconciler) deleteControlPlaneMachine(ctx context.Context, machine *clusterv1.Machine) (ctrl.Result, error) {
	r.Log.Info("deleting machine", "machine", machine.Name, "node", machine.Status.NodeRef.Name)

	if err := r.Client.Delete(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, errors.Wrapf(err, "failed to delete control plane machine %q", machine.Name)
	}

	return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
}
```

Delete `deleteNode` and the unused imports (`v1`, `types`, `util`, `client`); delete `gracefulEtcdLeave` from `etcd.go`. Update the two callers to drop `cluster` (`upgrade.go` and `reconcileMachines`). Keep the `waitForNodeRefs`-style behaviour through the preflight's NodeRef check.

- [ ] **Step 4: Run** `go build ./... && go test ./controllers/`. Expected: PASS.

- [ ] **Step 5: Commit** `feat(controllers)!: scale down by deleting the Machine only, like KCP`

---

### Task 6: Quorum safeguard in the hook

**Files:**
- Modify: `controllers/preterminate.go`
- Test: `controllers/preterminate_test.go`

**Interfaces:**
- Produces: `listEtcdMembers(ctx, c etcdCalls) ([]*machineapi.EtcdMember, error)`, `matchEtcdMember(members, victim) (*machineapi.EtcdMember, error)`, `quorumAfterRemoval(...) *quorumShortfall`, `holdForQuorum(...)`, `clearEtcdCleanupObservedAt(...)`, const `etcdQuorumAtRiskEvent = "EtcdQuorumAtRisk"`.

- [ ] **Step 1: Make `runningEtcd()` healthy and give every existing peer services**

`runningEtcd()` returns `{Id: "etcd", State: "Running", Health: &machineapi.ServiceHealth{Healthy: true}}`. Add `unhealthyEtcd()` returning `{Id: "etcd", State: "Running", Health: &machineapi.ServiceHealth{Healthy: false}}`. Add `services: runningEtcd()` to the peer fakes in `GracefulLeaveFromVictim`, `RemovesViaPeerWhenVictimUnreachable`, `RemovesViaPeerWhenLeaveFails`, `RetainsHookBeforeDeadline`, `FailsOpenPastDeadline`, `ProcessesOnlyTheOldestDeletingMachine` (cp-peer).

- [ ] **Step 2: Write the failing tests**

```go
// --- quorum safeguard -----------------------------------------------------

func threeMemberFixture(t *testing.T, third *fakeEtcdCalls, victimMutators ...func(*clusterv1.Machine)) *preTerminateFixture {
	t.Helper()

	tcp := newPreTerminateTCP()
	mutators := append([]func(*clusterv1.Machine){ptHooked, ptDeleting(time.Now(), clusterv1.MachineDeletingWaitingForPreTerminateHookReason)}, victimMutators...)
	victim := newPTMachine(tcp, "cp-1", mutators...)
	f := newPreTerminateFixture(t, victim, newPTMachine(tcp, "cp-2", ptHooked), newPTMachine(tcp, "cp-3", ptHooked))

	f.dialer.clients["cp-2"] = &fakeEtcdCalls{
		name:     "cp-2",
		members:  []*machineapi.EtcdMember{{Id: 1, Hostname: "cp-1"}, {Id: 2, Hostname: "cp-2"}, {Id: 3, Hostname: "cp-3"}},
		services: runningEtcd(),
	}
	f.dialer.clients["cp-1"] = &fakeEtcdCalls{name: "cp-1", services: runningEtcd()}

	if third != nil {
		f.dialer.clients["cp-3"] = third
	}

	return f
}

func TestPreTerminateHook_HoldsWhenRemovalWouldLoseQuorum(t *testing.T) {
	f := threeMemberFixture(t, &fakeEtcdCalls{name: "cp-3", services: unhealthyEtcd()})

	res, err := f.run(context.Background())
	require.NoError(t, err, "a quorum hold is not a failure")

	assert.Equal(t, preTerminateRequeueAfter, res.RequeueAfter)
	assert.True(t, f.hasHook(t, "cp-1"), "the member stays until a majority would survive its removal")
	assert.Zero(t, f.dialer.clients["cp-1"].leaveCalls)
	assert.Empty(t, f.dialer.clients["cp-2"].removedIDs)

	events := strings.Join(f.events(), "\n")
	assert.Contains(t, events, etcdQuorumAtRiskEvent)
	assert.Contains(t, events, "1 healthy members of 2, below the quorum of 2")

	_, anchored := f.get(t, "cp-1").Annotations[etcdCleanupObservedAtAnnotation]
	assert.False(t, anchored, "the fail-open clock does not run during a quorum hold")
}

func TestPreTerminateHook_UnreachablePeerCountsAgainstQuorum(t *testing.T) {
	f := threeMemberFixture(t, nil)
	f.dialer.dialErrs["cp-3"] = fmt.Errorf("connection refused")

	_, err := f.run(context.Background())
	require.NoError(t, err)

	assert.True(t, f.hasHook(t, "cp-1"))
	assert.Zero(t, f.dialer.clients["cp-1"].leaveCalls)
}

func TestPreTerminateHook_QuorumHoldIgnoresTheDeadline(t *testing.T) {
	f := threeMemberFixture(t, &fakeEtcdCalls{name: "cp-3", services: unhealthyEtcd()}, ptObservedAt(time.Now().Add(-time.Hour)))

	_, err := f.run(context.Background())
	require.NoError(t, err)

	assert.True(t, f.hasHook(t, "cp-1"), "quorum loss never fails open")
	assert.NotContains(t, strings.Join(f.events(), "\n"), etcdCleanupOrphanedEvent)
}

func TestPreTerminateHook_ProceedsWhenQuorumIsKept(t *testing.T) {
	f := threeMemberFixture(t, &fakeEtcdCalls{name: "cp-3", services: runningEtcd()})

	_, err := f.run(context.Background())
	require.NoError(t, err)

	assert.False(t, f.hasHook(t, "cp-1"))
	assert.Equal(t, 1, f.dialer.clients["cp-1"].leaveCalls)
}

// A peer that is itself deleting but whose etcd is still running is still a voting member.
func TestPreTerminateHook_CountsADeletingPeerWithRunningEtcd(t *testing.T) {
	f := threeMemberFixture(t, &fakeEtcdCalls{name: "cp-3", services: runningEtcd()})
	later := f.get(t, "cp-3")
	ts := metav1.NewTime(time.Now().Add(time.Minute))
	later.DeletionTimestamp = &ts
	later.Finalizers = []string{clusterv1.MachineFinalizer}
	ptHooked(later)
	require.NoError(t, f.r.Client.Update(context.Background(), later))
	f.machines.Items[2] = *later

	_, err := f.run(context.Background())
	require.NoError(t, err)

	assert.False(t, f.hasHook(t, "cp-1"), "cp-2 and cp-3 both still vote, so cp-1 may leave")
}
```

Note for the last test: the fixture lists machines in the order given (cp-1, cp-2, cp-3); the fake client requires a finalizer to keep a deleting object, and `ptDeleting` cannot be used after creation, hence the manual update.

- [ ] **Step 3: Run** `go test ./controllers/ -run 'Quorum|DeletingPeer|UnreachablePeer'`. Expected: FAIL (`etcdQuorumAtRiskEvent` undefined; the leave proceeds).

- [ ] **Step 4: Implement** in `preterminate.go`

Add the event constant to the event block:

```go
	etcdQuorumAtRiskEvent         = "EtcdQuorumAtRisk"
```

Replace `findEtcdMember` with:

```go
// listEtcdMembers returns the etcd member list as reported by the machine the client points at.
func listEtcdMembers(ctx context.Context, c etcdCalls) ([]*machineapi.EtcdMember, error) {
	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	response, err := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if err != nil {
		return nil, err
	}

	var members []*machineapi.EtcdMember

	for _, message := range response.Messages {
		members = append(members, message.Members...)
	}

	return members, nil
}

// matchEtcdMember looks the machine up in a member list. A nil member with a nil error means
// the machine is not a member (anymore).
func matchEtcdMember(members []*machineapi.EtcdMember, victim *clusterv1.Machine) (*machineapi.EtcdMember, error) {
	hostname := machineHostName(victim)
	if hostname == "" {
		return nil, errors.Errorf("machine %q has no hostname to match against the etcd member list", victim.Name)
	}

	for _, member := range members {
		if strings.EqualFold(member.Hostname, hostname) {
			return member, nil
		}
	}

	return nil, nil
}
```

In `reconcilePreTerminateHookForMachine`, replace the `findEtcdMember` call with:

```go
	members, err := listEtcdMembers(ctx, peerClient)
	if err != nil {
		return r.failOpenOrRetry(ctx, victim, errors.Wrapf(err, "failed to list etcd members via machine %q", peer.Name))
	}

	member, err := matchEtcdMember(members, victim)
	if err != nil {
		return r.failOpenOrRetry(ctx, victim, err)
	}

	if member == nil {
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"machine is no longer an etcd member, skipping etcd member removal")
	}

	// Safeguard, as in KCP: the member only leaves if a healthy majority of what remains can
	// keep the cluster serving. Losing quorum is worse than a parked deletion, so unlike the
	// connectivity failures below this hold never fails open.
	if shortfall := r.quorumAfterRemoval(ctx, tcp, owned, victim, len(members)); shortfall != nil {
		return r.holdForQuorum(ctx, victim, member, shortfall)
	}
```

Add:

```go
// quorumShortfall says how far the etcd cluster would be from quorum after a removal.
type quorumShortfall struct {
	healthy, members, quorum int
}

// quorumAfterRemoval checks whether the etcd cluster keeps a healthy majority once the
// victim's member is gone. Every owned machine other than the victim is a candidate voter --
// including ones that are themselves deleting but whose etcd is still running -- except
// machines already marked as leaving etcd. An unreachable machine counts as unhealthy.
func (r *TalosControlPlaneReconciler) quorumAfterRemoval(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	owned []*clusterv1.Machine,
	victim *clusterv1.Machine,
	memberCount int,
) *quorumShortfall {
	healthy := 0

	for _, machine := range owned {
		if machine.Name == victim.Name || machine.Annotations[etcdLeavingAnnotation] == "true" {
			continue
		}

		if r.etcdHealthyOn(ctx, tcp, machine) {
			healthy++
		}
	}

	members := memberCount - 1
	quorum := members/2 + 1

	if healthy >= quorum {
		return nil
	}

	return &quorumShortfall{healthy: healthy, members: members, quorum: quorum}
}

// etcdHealthyOn reports whether the machine's etcd service reports itself healthy.
func (r *TalosControlPlaneReconciler) etcdHealthyOn(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) bool {
	c, err := r.etcdClientFor(ctx, tcp, *machine)
	if err != nil {
		r.Log.Info("could not reach machine to check etcd health", "machine", machine.Name, "error", err.Error())

		return false
	}

	defer c.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	svcs, err := c.ServiceInfo(ctx, "etcd")
	if err != nil {
		r.Log.Info("could not check etcd health", "machine", machine.Name, "error", err.Error())

		return false
	}

	for _, svc := range svcs {
		if svc.Service.GetHealth().GetHealthy() {
			return true
		}
	}

	return false
}

// holdForQuorum keeps the hook, says why, and resets the fail-open clock so it only measures
// time spent actually trying to remove the member.
func (r *TalosControlPlaneReconciler) holdForQuorum(ctx context.Context, victim *clusterv1.Machine, member *machineapi.EtcdMember, shortfall *quorumShortfall) (ctrl.Result, error) {
	if err := r.clearEtcdCleanupObservedAt(ctx, victim); err != nil {
		return ctrl.Result{}, err
	}

	message := fmt.Sprintf("removing etcd member %q would leave %d healthy members of %d, below the quorum of %d; holding the deletion",
		member.Hostname, shortfall.healthy, shortfall.members, shortfall.quorum)

	r.Log.Info(message, "machine", victim.Name)
	r.recordMachineEvent(victim, corev1.EventTypeWarning, etcdQuorumAtRiskEvent, message)

	return ctrl.Result{RequeueAfter: preTerminateRequeueAfter}, nil
}

// clearEtcdCleanupObservedAt drops the fail-open anchor, if present.
func (r *TalosControlPlaneReconciler) clearEtcdCleanupObservedAt(ctx context.Context, machine *clusterv1.Machine) error {
	if _, ok := machine.Annotations[etcdCleanupObservedAtAnnotation]; !ok {
		return nil
	}

	patchHelper, err := patch.NewHelper(machine, r.Client)
	if err != nil {
		return err
	}

	delete(machine.Annotations, etcdCleanupObservedAtAnnotation)

	return errors.Wrapf(patchHelper.Patch(ctx, machine),
		"failed to reset the etcd cleanup deadline on machine %q", machine.Name)
}
```

- [ ] **Step 5: Run** `go test ./controllers/`. Expected: PASS.

- [ ] **Step 6: Commit** `feat(controllers): hold a control plane machine deletion that would cost etcd its quorum`

---

### Task 7: Documentation and release notes

**Files:**
- Modify: `README.md` "Machine deletion and etcd" section
- Modify: `hack/release.toml`

- [ ] **Step 1: README**

Rewrite the section so it says: the hook is unconditional (no flag); scale-down deletes the Machine only and the core Machine controller deletes the Node; add "Preflight checks" (the four checks, `Resized` condition message, `ControlPlaneUnhealthy` event, 10s retry) and "Quorum safeguard" (the rule, `EtcdQuorumAtRisk` event, holds without fail-open, escape hatch) subsections; flags table keeps `--etcd-cleanup-timeout` only; events list gains both new events.

- [ ] **Step 2: release.toml**

`previous = "v0.8.0"`; replace the notes with `[notes.lifecycle]` (the four behaviours) and `[notes.flags]` (`--enable-machine-pre-terminate-hook` removed; the hook is always on).

- [ ] **Step 3: Commit** `docs: describe the KCP-style machine lifecycle`

---

### Task 8: Verification

- [ ] `GOTOOLCHAIN=go1.26.5 go build ./... && GOTOOLCHAIN=go1.26.5 go vet ./...`
- [ ] `GOTOOLCHAIN=go1.26.5 go test -race $(go list ./... | grep -v /integration)`
- [ ] `GOTOOLCHAIN=go1.26.5 staticcheck ./...` (if installed)
- [ ] `grep -rn 'EnableMachinePreTerminateHook\|deleteNode\|gracefulEtcdLeave' --include='*.go' .` returns nothing.
- [ ] Then superpowers:finishing-a-development-branch.

## Self-review

- Spec coverage: §1 Task 1; §2 Task 5; §3 Tasks 3-5; §4 Task 6; §5 Task 2; §6 unchanged by design; docs Task 7.
- Type consistency: `preflightChecks(ctx, tcp, collections.Machines, ...*Machine) *preflightFailure` used identically in Tasks 3-5; `deleteControlPlaneMachine(ctx, *Machine)` in Task 5 and its test; `etcdCalls.ServiceList` signature matches `*talosclient.Client.ServiceList`.
