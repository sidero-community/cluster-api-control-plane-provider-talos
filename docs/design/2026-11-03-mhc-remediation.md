# MachineHealthCheck remediation on the pre-terminate lifecycle

**Date:** 2026-11-03
**Status:** approved 2026-11-03 with the recommendations for D1 to D6, implemented (see Outcome)
**References:**
- `sigs.k8s.io/cluster-api@v1.14.3/controlplane/kubeadm/reconcilers/kubeadmcontrolplane`
  (`remediation.go`, `preflight.go`, `kubeadmcontrolplane_controller.go` `reconcilePreTerminateHook`)
- `sigs.k8s.io/cluster-api@v1.14.3/core/reconcilers/machine/machine_controller.go` (`reconcileDelete`,
  `isDeleteNodeAllowed`)
- upstream `siderolabs/cluster-api-control-plane-provider-talos@8d3a83a` (`controllers/remediation.go`,
  `controllers/scale.go`, `controllers/etcd.go`; removed from this fork in `dc49223`)
- this fork: `controllers/preterminate.go`, `controllers/preflight.go`, `controllers/scale.go`,
  `controllers/etcd.go`, `docs/design/2026-09-17-kcp-machine-lifecycle.md`

## Goal

Remediate control plane `Machine`s that a `MachineHealthCheck` hands to this provider
(`HealthCheckSucceeded=False`, `OwnerRemediated=False`): remove the broken machine, take its etcd
member out cleanly, let Cluster API delete the `Node`, create a replacement, and stop after a bounded
number of failed replacements. Do it the way `KubeadmControlPlane` (KCP) does, reusing what the fork
already built for deletion, and taking the pieces upstream got right.

## Background: three designs for the same problem

Three implementations exist. Each decomposes the problem into the same six steps, and each gets some
of them right.

| Step | KCP (reference) | Upstream CACPPT | This fork today |
| --- | --- | --- | --- |
| 1. Decide to remediate | `reconcileUnhealthyMachines`: pick target, retry limits, size/provisioning/deleting checks, quorum projection, topology-upgrade deferral | Same shape, ported; `remediationStartTimeout`; defers until initialized; constants instead of API fields | none |
| 2. Remove the etcd member | Pre-terminate hook, after drain and volume detach; quorum re-checked in the hook | Inline, **before** `Delete`: mark leaving, graceful leave, else via a peer; `reconcileEtcdLeavingMachines` recovers a mark without a deletion | Pre-terminate hook; quorum hold that never fails open; connectivity failures fail open after `--etcd-cleanup-timeout` |
| 3. Delete the Machine | `Delete`, record `remediation-in-progress` in the deferred patch | Patch `remediation-in-progress` **before** `Delete`, so a crash cannot lose the retry count | scale-down/rollout: `Delete` only |
| 4. Delete the Node | Core Machine controller, after the infrastructure is gone | Provider deletes the `Node` itself during scale-down | Core Machine controller |
| 5. Replace | Scale-up with a **relaxed** preflight while remediation is in progress; replacement gets `remediation-for`; in-progress cleared at creation | Scale-up with the regular gates; in-progress kept until the replacement is `Ready` (or 30 min); scale-down blocked meanwhile | Scale-up behind the KCP preflight checks |
| 6. Observe | `Remediating` condition, `status.lastRemediation`, `OwnerRemediated` reasons | `OwnerRemediated` reasons (MachineSet values) | `EtcdQuorumAtRisk`, `EtcdMemberLeft`… events on the Machine |

### Where upstream is wrong for Talos

Upstream removes the etcd member first and deletes the `Machine` afterwards. Cluster API's deletion
pipeline then drains the node **after** its etcd and therefore its local API server are gone: pods
are evicted against a control plane that is one member short for no reason, and on a node that may
still be serving. KCP moved this work into the pre-terminate hook precisely to avoid that ordering,
and the fork followed KCP on 2026-09-17. Upstream also deletes the workload `Node` itself, which on
Talos races the kubelet re-registering it; the core Machine controller deletes the `Node` once the
infrastructure is gone and only refuses when no other control plane machine is left.

The inline model needs two pieces of machinery the hook model does not: the "marked leaving but not
deleting" state and its recovery loop (`reconcileEtcdLeavingMachines`), and a start timeout to
withdraw a recorded remediation whose member removal never happened. Both disappear when the member
leaves inside the deletion.

### Where upstream is right

- Everything in step 1 is a faithful port of KCP and applies unchanged to the hook model: target
  selection, retry accounting on annotations, the deferral rules, and `canSafelyRemoveEtcdMember`.
- Recording the remediation **before** touching the `Machine` protects the retry limit against a
  controller restart. KCP loses the count in that window and accepts it; keeping it costs one patch.
- Feeding the quorum projection from the etcd health check of the **same reconcile** instead of
  probing again (KCP does the equivalent with its per-machine conditions). The fork ported that health
  check in `1b77811`, so `etcdHealth.statuses` already exists.
- `identifyEtcdMember`: finding the member of a machine that never got a `nodeRef` by elimination.
  KCP has the same idea (`tryInferMemberToBeDeleted`). The fork's hook currently fails on such a
  machine and only releases it after the fail-open deadline.
- Preferring a machine flagged for remediation when a scale-down is due anyway (already ported with
  the failure-domain work).

### Where the fork is right

- The hook (step 2) and leaving step 4 to Cluster API, for the reasons above.
- The quorum hold in the hook: a second, independent check at the last moment before the member is
  removed, which also covers `kubectl delete machine` and every path remediation never saw. KCP checks
  twice as well.
- The preflight checks: they already block scale-up and scale-down while the remediated machine is
  deleting and while its replacement is provisioning, which is what makes upstream's "keep
  in-progress until Ready, block scale-down" bookkeeping unnecessary.
- Fail-open on connectivity failures with an escape hatch. KCP returns errors forever; a parked
  deletion is the worse outcome.

## Design

The result is KCP's decision logic (as ported by upstream) in front of the fork's deletion pipeline,
with KCP's API and status, and two small upgrades to the hook.

### 1. API

`spec.remediation` on `TalosControlPlane` (and so on `TalosControlPlaneTemplate`), same shape as
KCP's `KubeadmControlPlaneRemediationSpec`:

```go
type TalosControlPlaneRemediationSpec struct {
    // maxRetry is the maximum number of retries of a remediation sequence: a retry is the
    // remediation of a replacement that failed within minHealthyPeriodSeconds of the previous one.
    MaxRetry *int32 `json:"maxRetry,omitempty"`
    // retryPeriodSeconds is the minimum time between two retries of the same sequence.
    RetryPeriodSeconds *int32 `json:"retryPeriodSeconds,omitempty"`
    // minHealthyPeriodSeconds is how long a replacement must stay healthy for its own failure to
    // start a new sequence instead of counting as a retry. Defaults to 1h.
    MinHealthyPeriodSeconds *int32 `json:"minHealthyPeriodSeconds,omitempty"`
}
```

`status.lastRemediation {machine, time, retryCount}` as in KCP. Both are hub-only (`v1beta1`) and
ride the conversion annotation through `v1alpha3`, like `imageFactory` does in CABPT.

**D1 (defaults).** KCP defaults to unlimited retries with no retry period; upstream hardcodes 3
retries, 5 minutes apart. Recommendation: upstream's values as the defaults (`maxRetry: 3`,
`retryPeriodSeconds: 300`, `minHealthyPeriodSeconds: 3600`), applied in the webhook so they are
visible on the object. A quota or image problem should not replace machines forever.

### 2. Reconcile order

Unchanged in spirit from upstream's wiring: hooks are serviced first, as today, and remediation runs
as the last phase together with scaling so that one reconcile deletes at most one `Machine`.

```
reconcileMachinePreTerminateHooks            (unchanged: first, independent of the gates below)
reconcileEtcdMembers      -> etcd statuses   (ported health check; now also returns statuses)
reconcileNodeHealth
reconcileConditions
reconcileKubeconfig
reconcileRemediationAndMachines(statuses):
    acted := reconcileUnhealthyMachines(statuses)
    if acted { return requeue }              (no scale/rollout in the same reconcile)
    reconcileMachines                        (in-place, rollout, scale as today)
```

### 3. Deciding to remediate (`controllers/remediation.go`)

Port upstream's `reconcileUnhealthyMachines` with these changes, in order of evaluation:

1. `reconcileRemediationInProgress` and `cleanupOwnerRemediated` first, as upstream (the latter is
   KCP's "machine is healthy again" cleanup; MHC never clears `OwnerRemediated` itself).
2. Target: `machineToBeRemediated` as upstream, plus one criterion between "no nodeRef" and "oldest":
   a machine whose etcd was observed unhealthy by this reconcile's health check goes first. This is
   KCP's `EtcdMemberHealthy` tie-break, which the ported statuses make available for free.
3. Topology deferral, verbatim from KCP: with `cluster.spec.topology` set and
   `topology.version != spec.version` (unless it matches the current upgrade-step annotation), defer
   with `RemediationDeferred`. The fork supports ClusterClass; upstream does not have this rule.
4. Retry limits from `spec.remediation` (upstream's `checkRemediationRetryLimits` reading the fields
   instead of constants).
5. Size and state checks. Once `status.initialization.controlPlaneInitialized` is true, exactly
   KCP's: more than one machine, no healthy machine still provisioning, no machine deleting,
   then the quorum projection. "Leaving etcd" no longer exists as a non-deleting state, so upstream's
   checks on it are dropped.
6. Quorum projection: `canSafelyRemoveEtcdMember(members, target, machines, healthy)` unchanged from
   upstream, with `healthy` and `members` taken from the health-check statuses. When the statuses are
   missing (the health check could not run), defer rather than probe again.
7. Record `remediation-in-progress` on the TalosControlPlane with an immediate patch (upstream), then
   `Delete` the `Machine`. Nothing else: no etcd call, no leadership forfeit. Set
   `OwnerRemediated=False/RemediationMachineDeleting`, record a `RemediatingMachine` event on the
   `Machine`, return a requeue.

Deferral and refusal reasons on `OwnerRemediated` use the KCP values (`RemediationDeferred`,
`CannotBeRemediated`, `RemediationMachineDeleting`, `InternalError`); the messages name
`TalosControlPlane`.

**D3 (before initialization).** KCP skips the size and quorum checks until the control plane is
initialized, so a first machine that never bootstraps is replaced; upstream defers everything until
initialized, so it never is. Talos adds a wrinkle: `status.bootstrapped` records that `talosctl
bootstrap` was issued to a machine, and that machine's etcd then holds the (empty) cluster.
Recommendation: before initialization, allow remediation when `status.bootstrapped` is false (no etcd
exists anywhere, nothing to lose), or when the target has no `nodeRef` and another machine exists (it
never joined). A bootstrapped single machine that fails before the API server comes up is
`CannotBeRemediated`, like KCP's "replicas <= 1" rule; it needs an operator.

**D4 (in-place updates).** Defer remediation while any machine carries the in-place
`UpdateInProgress` annotation or a pending `UpdateMachine` hook, as one more "transitional state"
check next to provisioning and deleting. Remediating a machine mid-update is a later refinement that
needs cancellation support in the extension.

### 4. The hook (`controllers/preterminate.go`)

Unchanged in structure. Two upgrades:

- **Nameless victims.** When `nodeNamesForMachine(victim)` is empty (the remediated machine never
  registered a node and has no `MachineHostName`), use `identifyEtcdMember(members, victim, owned)`:
  if the member is known and present, remove it through the peer; if known and absent, release with
  `EtcdCleanupSkipped`; if it cannot be told apart, release the hook anyway (**D6**). KCP releases
  immediately in that case and leaves the member to the audit; the fork currently retries until the
  fail-open deadline, which buys nothing because the name will never appear on a deleting machine.
- **Quorum hold reuse.** `quorumAfterRemoval` probes the remaining machines serially under one
  budget. Switch it to the concurrent `probeEtcd` from the health check. The live probe stays, since
  hooks run before the health check in the reconcile and the hook must not trust stale numbers.

The hook keeps marking the victim `etcd-leaving` before the member leaves, keeps the quorum hold with
no deadline, and keeps failing open on connectivity after `--etcd-cleanup-timeout`.

### 5. Replacement and bookkeeping

- `bootControlPlane` copies the `remediation-in-progress` value onto the new `Machine` as
  `remediation-for` (upstream and KCP both do this), and the in-progress annotation is cleared
  **when the replacement is created**, with an immediate patch (**D2**). This is KCP's lifetime.
  Upstream keeps it until the replacement is `Ready` and blocks scale-down meanwhile; the fork's
  preflight checks already refuse any scale operation while the replacement has no `Node` or its
  member is not healthy, so the longer lifetime adds only a timeout to maintain.
- Stale cleanup as KCP: if a machine newer than the recorded timestamp exists, the annotation is
  dropped (covers a crash between creation and the clearing patch).
- Withdrawal as upstream: if the remediated machine still exists, is not deleting, one minute after
  the record, the record is dropped (covers a crash between the record and the `Delete`).
- `status.lastRemediation` mirrors the in-progress record or the newest `remediation-for`.
- `Remediating` condition on the TalosControlPlane, aggregated from `OwnerRemediated` over the
  machines to be remediated, `False/NotRemediating` otherwise (KCP's `setRemediatingCondition`).

### 6. Scale-down, rollout and preflight

- Scale-down and rollout never run in the reconcile that remediated (section 2), and the preflight
  checks already hold them while the remediated machine is deleting. No new gate.
- Victim selection already prefers MHC-flagged machines (`f6e4a52`), so a scale-down that is due
  anyway takes the broken machine first.
- **D5 (relaxed preflight for the replacement).** While `remediation-in-progress` is set, KCP lets
  the scale-up that creates the replacement through a weaker check: not "every machine healthy", but
  "adding one member keeps quorum and at least one machine has healthy components". This is what lets
  a 5-replica control plane recover from two failures where the second one is not MHC-flagged.
  Recommendation: adopt it as a second phase, implemented with the same `canSafelyRemoveEtcdMember`
  projection with `addEtcdMember` semantics, and keep the strict checks for every other scale-up.

### 7. What stays out of the controller

- `Node` deletion: the core Machine controller. Operators using MHC on control planes should set
  `spec.machineTemplate.spec.deletion.nodeDrainTimeoutSeconds` and
  `nodeVolumeDetachTimeoutSeconds`: Cluster API only short-circuits drain on an **unreachable** node;
  a node with a live kubelet and a dead control plane drains until the timeout, which is unlimited by
  default. Document this next to the MHC example; a webhook default is a possible follow-up.
- MHC's own gate (`unhealthyLessThanOrEqualTo`, default 100%): the MHC stops flagging when too many
  machines are unhealthy. Recommend `1` for control plane checks in the docs.
- External remediation (`remediationTemplate`): MHC never sets `OwnerRemediated`; nothing to do.

## Walkthrough: 3 replicas, `cp-2` powered off

1. MHC: `cp-2` `HealthCheckSucceeded=False`, `OwnerRemediated=False`.
2. Reconcile: hook phase, nothing deleting. Health check: `cp-2` unreachable, `cp-1`/`cp-3` healthy
   with 3 members; `EtcdClusterHealthy=False/EtcdClusterUnhealthy` (quorum kept). Remediation: target
   `cp-2`; no retry history; initialized, 3 machines, none provisioning or deleting; projection: 3
   voting, 2 healthy ≥ 2; after removal 2 voting, 2 healthy ≥ 2: safe. Record in-progress, delete
   `cp-2`, `RemediationMachineDeleting`, requeue. `Remediating=True`.
3. Machine controller: drain skips (unreachable), volume wait skips, parks at the hook.
4. Hook: peer `cp-1` lists members, `cp-2` present; hold check: `cp-1`, `cp-3` healthy, 2 ≥ 2; mark
   leaving; graceful leave fails (unreachable); removed through `cp-1`; hook released;
   `EtcdMemberRemovedViaPeer`.
5. Infrastructure deleted; core deletes the `Node`; `Machine` gone.
6. Reconcile: 2 < 3; preflight passes (no deletions, nodes present, components healthy, membership
   `cp-1`, `cp-3` verified); `cp-4` created in `cp-2`'s failure domain with `remediation-for`;
   in-progress cleared; `status.lastRemediation` updated.
7. If `cp-4` fails within an hour: retry #1 after 5 minutes, up to 3, then `CannotBeRemediated`.

Two of three unhealthy: the projection refuses (`CannotBeRemediated`, "1 of 3 etcd members are
healthy, removing a member needs 2"), `EtcdClusterHealthy` reports `EtcdClusterQuorumLost`, and
nothing is deleted. That is correct: etcd has to be restored by hand.

## Error handling

- Every deferral and refusal is a condition on the `Machine` and an event, never an error; the
  reconcile continues to scaling.
- A failed in-progress patch stops the remediation before anything is touched (`InternalError`).
- A failed `Delete` after the record: the withdrawal rule drops the record after a minute, and the
  next reconcile tries again.
- The hook keeps its contract: hold on quorum, fail open on connectivity, escape hatch by removing the
  annotation.

## Testing

Upstream's `remediation_test.go` splits cleanly. Port as is: retry limits, target selection,
`canSafelyRemoveEtcdMember`, "uses observed etcd statuses", "recorded before the machine is touched",
"does not start when it cannot be recorded", "defers while another machine is deleting/provisioning",
"single machine", "stale `OwnerRemediated` cleanup", "skips a target already deleting", "no scale
down in the reconcile that remediated". Drop everything about leaving etcd before deleting and the
legacy annotation conversions. Add: the topology and in-place deferrals, the pre-initialization rule,
hook tests for a nameless victim (identified, absent, unknown), an end-to-end suite test for the
walkthrough including the retry chain, and, with D5, the relaxed preflight.

## Documentation and release

README: a "Remediation" subsection under "Machine deletion and etcd" (the deletion story already
lists remediation as one of its paths), the `spec.remediation` fields, the drain-timeout advice, the
MHC `unhealthyLessThanOrEqualTo` advice, and the new condition and events. `hack/release.toml`: a
`[notes.remediation]` entry. New API fields and a new condition make this a minor bump.

## Build order

1. API: `spec.remediation`, `status.lastRemediation`, `Remediating` condition, conversion stash,
   webhook defaults (D1).
2. `reconcileEtcdMembers` returns the statuses; `reconcileRemediationAndMachines` wiring.
3. `controllers/remediation.go` as in section 3; `bootControlPlane` stamps `remediation-for` and
   clears in-progress (section 5).
4. Hook upgrades (section 4).
5. Tests, README, release notes.
6. Relaxed preflight during remediation (D5).

## Decisions

Approved 2026-11-03 as recommended.

| | Question | Decision |
| --- | --- | --- |
| D1 | Defaults for `spec.remediation` | upstream's finite values: 3 retries, 5 min apart, 1 h healthy period |
| D2 | Lifetime of `remediation-in-progress` | KCP: cleared when the replacement is created; preflight covers the rest |
| D3 | Remediation before initialization | allowed while not bootstrapped, or for a machine without a node when others exist; otherwise `CannotBeRemediated` |
| D4 | Machines updating in place | defer while any in-place update is in progress |
| D5 | Relaxed preflight for the replacement | yes, as the last build step |
| D6 | Hook on a victim whose member cannot be identified | release immediately (KCP), not after the fail-open deadline |

## Outcome

Implemented on 2026-11-03 on top of the port a peer session had made with upstream's answers
(`controllers/remediation.go`), which this rewrote to the decisions above. Two refinements came
out of the implementation:

- **D3, the bootstrapped machine.** Before initialization no machine has a `nodeRef` (there is
  no API server to register with), so "a machine without a node when others exist" does not tell
  the bootstrapped machine from the ones waiting to join, and `status.bootstrapped` stays set once
  the bootstrap was issued: deleting the bootstrapped machine would leave a control plane that is
  never bootstrapped again. The rule as implemented therefore requires, once bootstrapped, that
  another machine is found to hold etcd data (the same `/var/lib/etcd/member` probe
  `bootstrapCluster` uses, through `TalosControlPlaneReconciler.etcdDataChecker` in tests) before
  a machine without a node is replaced; otherwise `CannotBeRemediated`. A flagged machine that has
  a node while the control plane is not yet observed initialized is deferred, not refused:
  initialization is imminent.
- **D4, the target's own update** (2026-11-04). Remediation defers only for another machine's
  in-place update, no longer for the flagged machine's own: deleting the target stops its
  update, since Cluster API stops calling the hook for a deleting Machine and the bootstrap
  provider's extension (after v0.9.1) cancels a running Talos upgrade when it sees the
  deletion, and a machine that an update broke is exactly what remediation is for. Alongside,
  the control plane health check now covers the control plane static pods through the
  kubelet's view of them, and the next in-place update waits for them to be ready on every
  other machine, as KCP's preflight checks hold a rollout.
- **No scale-down gate.** The port held every scale-down while `remediation-in-progress` was set.
  With D2's lifetime the annotation only exists while the remediated machine is deleting (the
  preflight checks already hold scale-down then) or while the control plane is short of a machine
  (a scale-up, not a scale-down), so the gate was dropped, as in KCP.

Also in the implementation: the `remediation-in-progress` record is dropped when the remediated
machine is gone and the control plane already has all the replicas it wants (replicas lowered
meanwhile), which KCP lacks; `CannotBeRemediated` is reported as a `CannotRemediateMachine`
warning event on the `TalosControlPlane` and a deletion as a `RemediatingMachine` event on the
`Machine`; deferrals are conditions only. The quorum hold in the hook keeps its lenient health
criterion (service healthy) and gained concurrency, rather than switching to the health check's
stricter `etcdServiceError`.
