# A v1beta2-only status and no init path in CACPPT

Date: 2026-09-16. Status: approved design, awaiting implementation plan.

Follows the Talos 1.14 and CAPI 1.14 bumps on the `talos-1.14` branch. Companion
to the CABPT design in `cluster-api-bootstrap-provider-talos`
(`docs/design/2026-09-16-unattended-install-lifecycle-upgrades.md`).

## Goal

Remove every deprecated Cluster API and controller-runtime API the provider still
uses:

1. the Cluster API v1beta1-contract compatibility block,
   `status.deprecated.v1beta1`, and its maintenance;
2. the `init` control plane configuration and the legacy init-node bootstrap
   path, deprecated since v0.4.0;
3. rate-limited `Requeue`, the `client.Apply` patch type, the legacy event
   recorder and controller-runtime's `scheme.Builder`.

## Decisions

| Question | Decision |
|---|---|
| `status.deprecated.v1beta1` | Removed now. CAPI core v1.12+ and clusterctl read only v1beta2 fields; nothing in the fleet reads the block; Sidero's capi-utils reads a top-level `status.ready` the v1beta1 type has not had since the v1beta2 move. |
| `spec.controlPlaneConfig.init` | Removed from v1beta1 together with the init-node path. Clusters have bootstrapped through the Cluster API `Bootstrap` call since v0.4.0. v1alpha3 keeps the field in its schema and drops it on conversion. |
| `Requeue: true` | `RequeueAfter: 20 * time.Second`, the interval the controller already uses for the same "come back later" situations. |

## 1. Status

### v1beta1 API

`TalosControlPlaneStatus.Deprecated`, `TalosControlPlaneDeprecatedStatus`,
`TalosControlPlaneV1Beta1DeprecatedStatus` and the `V1Beta1DeprecatedStatus()`
helper are removed, with the v1beta1-style condition and reason constants. The
controller stops maintaining `ready`, `initialized`, `updatedReplicas` and
`unavailableReplicas`; `status.initialization.controlPlaneInitialized`,
`replicas`, `readyReplicas`, `availableReplicas`, `upToDateReplicas`,
`status.version` and `status.conditions` remain. CRDs are regenerated.

### v1alpha3 API

v1alpha3 keeps its schema and defines the old condition types locally instead of
importing CAPI's deprecated v1beta1 package. Conversion from the hub derives the
legacy fields:

| v1alpha3 field | Derived from |
|---|---|
| `conditions` | each `metav1.Condition`: same type, status, reason, message and transition time; no severity |
| `initialized` | `initialization.controlPlaneInitialized` |
| `ready` | `readyReplicas > 0` |
| `unavailableReplicas` | `replicas - readyReplicas` |
| `updatedReplicas` | `upToDateReplicas` |
| `failureReason`, `failureMessage` | empty; the controller never set them |

Conversion to the hub does not turn v1alpha3 conditions into v1beta2 conditions
(unchanged); the hub status is restored from the conversion annotation.

## 2. Init configuration

- `ControlPlaneConfig.InitConfig` is removed from v1beta1 and the webhook no
  longer validates `init.imageFactory`.
- `talosconfigForMachines` always uses the `<cluster>-talosconfig` secret;
  `talosconfigFromWorkloadCluster` and its read of CABPT's v1alpha3
  `status.talosConfig` are deleted.
- The first control plane machine is generated from `controlplane` like every
  other; the `first` special case goes.
- v1alpha3 keeps `init` in its schema; conversion to the hub drops it and the
  round trip restores it from the annotation.

## 3. controller-runtime

- The six `ctrl.Result{Requeue: true}` sites become `RequeueAfter: 20 *
  time.Second`; the guard in `Reconcile` that inspects `res.Requeue` drops that
  clause.
- `internal/ssa` uses `client.Client.Apply` with an apply configuration built
  from the object's unstructured form, `FieldOwner` and `ForceOwnership`; the
  dry-run variant adds `DryRunAll` and copies the server's result back into the
  typed object. The fake client's apply support is verified by the existing
  in-place trigger tests.
- `Recorder` becomes the events-API `events.EventRecorder`; the single
  `recordMachineEvent` call uses `Eventf` with the Machine as the regarding
  object and `PreTerminate` as the action. Tests use `events.NewFakeRecorder`.
- Both API packages replace `scheme.Builder` with `runtime.NewSchemeBuilder`,
  keeping `AddToScheme` and the `localSchemeBuilder` the generated conversions
  use.

## Consequences

- The v1beta1 CRD loses `status.deprecated` and `spec.controlPlaneConfig.init`.
  Once the CRD is applied, an `init` block in a submitted v1beta1 object is
  pruned by the API server, or rejected by clients that validate fields
  strictly; no object in the fleet sets it.
- Bootstrap behaviour is unchanged for every cluster created with the
  `Bootstrap` call, which is every cluster since v0.4.0.

## Testing

- Conversion tests for every derived v1alpha3 field and for the dropped `init`.
- The controller suite asserts only on v1beta2 conditions and counters; the
  four tests that read the deprecated block move to their v1beta2 equivalents.
- The pre-terminate tests record events through the events-API fake.
- The in-place trigger tests exercise server-side apply through the new client
  method.

## Out of scope

- `status.versions`, the v1.14 replacement for `status.version`; a separate
  follow-up.
- Removing the v1alpha3 API version itself.
