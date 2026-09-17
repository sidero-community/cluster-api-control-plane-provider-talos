# KCP-style machine lifecycle for TalosControlPlane

**Date:** 2026-09-17
**Status:** approved (decisions A, B, C below)
**Reference:** `sigs.k8s.io/cluster-api@v1.14.2/controlplane/kubeadm/reconcilers/kubeadmcontrolplane`
(`kubeadmcontrolplane_controller.go` `reconcilePreTerminateHook`, `preflight.go`, `scale.go`)

## Goal

Make control plane `Machine` deletion in this provider follow the Kubeadm Control Plane
provider's machine reconciliation pattern end to end:

1. **Interception.** Every owned `Machine` carries a pre-terminate hook, unconditionally.
2. **etcd member removal.** The hook removes the member while the node is drained but still
   powered on, and refuses to do so when that would cost etcd its quorum.
3. **Infrastructure teardown.** Lifting the hook is the only thing the provider does to let
   the deletion continue. It never deletes workload `Node` objects and never removes an etcd
   member ahead of the deletion request.
4. **Self-healing.** A deleted `Machine` is replaced once it is gone, but only after the
   same preflight checks KCP runs: nothing else deleting, every machine has a node, control
   plane components and etcd healthy.

## Background: where the provider diverges from KCP today

Steps 1 to 3 were added on 2026-09-06 (`controllers/preterminate.go`) and step 4 works by
accident of counting: deleting machines still count as replicas, so the scale-up branch fires
once the machine is gone. The differences that remain:

| Area | KCP | This provider (main @ a04e43c) |
| --- | --- | --- |
| Hook | Always stamped | Gated by `--enable-machine-pre-terminate-hook` (default `true`); a legacy inline etcd-leave path survives for the flag-off case |
| Scale-down | Forward leadership, delete the `Machine`, done | Deletes the workload `Node` itself, both after requesting deletion and on every pass for any deleting machine; inline etcd leave for unhooked victims |
| Preflight | Blocks scale-up and scale-down while any machine is deleting, lacks a NodeRef, or components/etcd are unhealthy | Scale-up is unconditional; scale-down waits for NodeRefs and the etcd condition only |
| Quorum | Holds the deletion if removing the member leaves no healthy majority | Removes the member regardless, fails open after `--etcd-cleanup-timeout` |
| Health checks | Deleting machines are excluded | `nodesHealthcheck` scans deleting machines, so the components condition flaps through every deletion |
| Remediation | `reconcileUnhealthyMachines` acts on `OwnerRemediated` | Not implemented (out of scope here, see below) |

Deleting the `Node` early is wrong on Talos: kubelet on the still-running node re-registers
it, and the core Machine controller deletes the `Node` itself after the infrastructure is gone
(`core/reconcilers/machine` `isDeleteNodeAllowed`, which only refuses when no other active
control plane machine exists).

## Design

### 1. The hook is unconditional

- `--enable-machine-pre-terminate-hook` and `TalosControlPlaneReconciler.EnableMachinePreTerminateHook`
  are removed. `desiredMachineAnnotations` always adds the hook; `stampPreTerminateHooks`
  always adopts machines that predate it (still never a machine that is already deleting).
- `--etcd-cleanup-timeout` stays.
- **Decision A (2026-09-17):** remove the flag rather than deprecate it. It is ten days old
  and fork-only; removing it is what makes the legacy path unreachable.

### 2. Scale-down only deletes the Machine

`scaleDownControlPlane` becomes:

1. Refuse to scale to zero (unchanged).
2. Select the victim (unchanged: delete annotation, then outdated, then oldest).
3. Run the preflight checks with the victim excluded (section 3).
4. `ensureNodesBooted` (unchanged, Talos-specific).
5. Delete the `Machine`. Nothing else: no etcd leave, no `Node` deletion. The hook does the
   etcd work at the pre-terminate phase, exactly as for a `kubectl delete machine`.
6. Requeue after 20s (existing convention; the provider also watches Machines).

Removed: `deleteNode`, the inline `gracefulEtcdLeave` path in `deleteControlPlaneMachine`,
`gracefulEtcdLeave` itself, and the workload-cluster client lookup in scale-down.
`etcdLeavingAnnotation` and `markEtcdLeaving` stay: the hook still sets it before the member
leaves, and the health checks and peer selection still honour it, which also keeps a machine
marked by the old inline path (crash between mark and delete) out of the health checks.

### 3. Preflight checks

New `controllers/preflight.go`:

```go
const preflightFailedRequeueAfter = 10 * time.Second

// preflightChecks reports why the control plane is not ready for a scale operation, or nil.
// excludeFor names machines that are about to be deleted and are therefore not required to
// be healthy or to have a Node.
func (r *TalosControlPlaneReconciler) preflightChecks(
    ctx context.Context,
    tcp *controlplanev1.TalosControlPlane,
    machines collections.Machines,
    excludeFor ...*clusterv1.Machine,
) *preflightFailure

type preflightFailure struct{ message string }
```

Checks, in KCP's order, all failures aggregated into one message:

1. No machines at all: pass (initialisation is a separate branch anyway).
2. Any machine with a deletion timestamp: `waiting for machines to be deleted: a, b`.
3. Any machine, other than the excluded ones, without a NodeRef:
   `machine X does not have a corresponding Node yet`.
4. Control plane components: a fresh `nodesHealthcheck` over the machines that are neither
   excluded, deleting nor marked leaving etcd. Fresh rather than the condition so the victim
   of a scale-down can itself be unhealthy, like KCP's `excludeFor`.
5. etcd: `EtcdClusterHealthyCondition` must be `True` on the TalosControlPlane. It is set
   earlier in the same reconcile by `reconcileEtcdMembers`, whose check already skips
   deleting and leaving machines. (A fresh check cannot exclude the victim: the member count
   would no longer match.)

Callers:

- `scaleUpControlPlane`: preflight before `bootControlPlane`. On failure set `Resized` to
  `False`/`ScalingUp` with the message
  `Scaling up control plane to N replicas (actual M): <failures>`, log, emit a Warning event
  `ControlPlaneUnhealthy` on the TalosControlPlane
  (`Waiting for control plane to pass preflight checks to continue reconciliation: <failures>`)
  and requeue after 10s.
- `scaleDownControlPlane`: preflight after victim selection, with the victim excluded, before
  `ensureNodesBooted`. Same condition and event with reason `ScalingDown`.

This is what makes self-healing safe: a machine that failed open with an orphaned etcd member
keeps `EtcdClusterHealthy` false (member count mismatch) until `auditEtcd` removes the orphan,
and only then is the replacement created.

### 4. Quorum safeguard in the hook

In `reconcilePreTerminateHookForMachine`, once the peer has confirmed the victim is a member
and before leadership is forfeited:

```
remaining := owned machines that are not the victim, not deleting, not marked leaving
healthy   := number of remaining machines whose etcd service is Running and Healthy
              (ServiceInfo("etcd") through etcdClientFor; unreachable counts as unhealthy)
membersAfter := len(member list) - 1
quorum       := membersAfter/2 + 1
if healthy < quorum: hold
```

A hold:

- keeps the hook, logs, emits a Warning event `EtcdQuorumAtRisk` on the Machine
  (`removing etcd member "m" would leave H healthy members of N, below the quorum of Q; holding the deletion`),
- returns `RequeueAfter: preTerminateRequeueAfter` with no error,
- clears `etcd-cleanup-observed-at` so the fail-open clock restarts when the hold lifts,
- **never fails open.** Decision B (2026-09-17): match KCP; the documented escape hatch of
  removing the hook annotation remains the way out.

Connectivity failures to the peer or the victim keep today's fail-open behaviour; only the
quorum verdict holds indefinitely.

`findEtcdMember` is split into `listEtcdMembers(ctx, peer)` and `matchEtcdMember(members, victim)`
so the same list serves the match and the count.

### 5. Health checks exclude deleting machines

`reconcileNodeHealth` filters out machines that are deleting or marked leaving before calling
`nodesHealthcheck`, mirroring `etcdHealthcheck`. `nodesHealthcheck` opens its clients through
`etcdClientFor` so the preflight checks can be unit-tested with the existing fake dialer; the
`etcdCalls` interface gains `ServiceList`.

### 6. Replacement provisioning

Unchanged in mechanism: deleting machines count toward `numMachines`, so nothing is created
while a deletion is in flight, and `scaleUpControlPlane` creates the replacement once the
machine is gone and the preflight checks pass.

## Walkthrough: `kubectl delete machine cp-2` on a 3-replica control plane

1. Core Machine controller drains the node, waits for volumes, then blocks on the hook
   (`Deleting` reason `WaitingForPreTerminateHook`).
2. Provider (hook first in the reconcile): peer `cp-1` lists members, `cp-2` is one; `cp-1`
   and `cp-3` report healthy etcd, so 2 >= quorum(2); `cp-2` forfeits leadership and leaves;
   hook released; event `EtcdMemberLeft`.
   If `cp-3` were down: hold with `EtcdQuorumAtRisk`, no member removed, until `cp-3`
   recovers or the operator removes the annotation.
3. Infrastructure provider deletes the InfraMachine; the Machine controller deletes the Node
   and the Machine.
4. Meanwhile `reconcileMachines` sees 3 machines (one deleting) and does nothing; the health
   checks ignore `cp-2`.
5. Next reconcile after `cp-2` is gone: 2 < 3, preflight passes (no deletions, NodeRefs,
   components and etcd healthy with 2 members), `cp-4` is created.

## Error handling

- Preflight failures are not errors: condition + event + requeue. Talos call failures inside
  the fresh components check make that machine unhealthy, which fails the preflight with the
  underlying error in the message.
- Quorum hold: no error, no deadline.
- Everything else in the hook keeps the existing fail-open contract.

## Testing

Unit tests with the existing fakes (`fakeEtcdCalls`/`fakeEtcdDialer` in
`controllers/preterminate_test.go`; `fakeEtcdCalls` gains `serviceList`):

- preflight: pass; each failure kind; excluded victim without NodeRef or with unhealthy
  services still passes; deleting machines excluded from the components check.
- scale-up: blocked by a deleting machine (no Machine created, condition message, event, 10s).
- scale-down: victim deleted with no Talos calls; blocked by a deleting machine before
  `ensureNodesBooted` runs; victim excluded from the NodeRef requirement.
- hook: hold when quorum would be lost (no leave, event, hook and anchor state), proceed
  when kept, two-member cluster needs one healthy peer, hold ignores the deadline.
- node health: deleting machine with unhealthy services leaves the condition `True`.
- The suite's `TestRollingUpdate` remains the end-to-end check that a rollout still scales
  up and down with the preflight checks in place.

## Documentation and release

- README "Machine deletion and etcd": hook unconditional (flag row removed), scale-down no
  longer removes the member inline or deletes Nodes, new "Preflight checks" and "Quorum
  safeguard" subsections, events list gains `EtcdQuorumAtRisk` and `ControlPlaneUnhealthy`.
- `hack/release.toml`: previous `v0.8.0`; notes for the lifecycle change and the removed
  flag (a CLI break, so the next release is a minor bump).

## Out of scope

- **Owner remediation** (`OwnerRemediated` from MachineHealthCheck, KCP's
  `reconcileUnhealthyMachines`, `spec.remediationStrategy`). Decision C (2026-09-17): a
  follow-up spec. Until then an MHC on Talos control plane machines marks them and waits.
- Failure-domain-aware victim selection and creation (KCP balances; this provider picks a
  random domain on create and the oldest machine on delete).
- KCP's per-machine health conditions; this provider keeps its control-plane-level conditions.
