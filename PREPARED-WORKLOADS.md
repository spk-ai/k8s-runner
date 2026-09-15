# Prepared Workloads

Dependent native implementation with read-only inspection added to prepared
runner `4023808`. This branch requires the matching
`feat/prepared-workload-inspection` API, based on registry API `4f957e5`.
It is not a drop-in image for installed controllers.

## Native Contract

- `PrepareWorkload` requires a canonical workload UUID and expected backend.
  Existing volume bindings are read-only inputs: a missing or replaced bound
  claim is never recreated. New volumes use the existing ownership/spec checks.
  All named volumes appear in the returned binding, not just main's mounts.
- The Pod starts with `agyn.io/workload-binding` in `spec.schedulingGates`.
  Its annotation stores backend/workload/volume identities, not credentials.
  Kubernetes >=1.30 is verified before resource creation; Pod creation requests
  strict field validation. Trusted admission must preserve the gate.
- The gated Pod starts in native `preparing` state. Temporary Secrets are staged
  in memory until its UID is confirmed, then created with that exact Pod owner
  in the CREATE itself. A crash cannot interrupt a separate ownership PATCH.
  After every Secret CREATE is acknowledged and validated, a Pod UID/resource-
  version checked PATCH commits `prepared` state without removing its gate.
  Incomplete setup cannot activate; failed/uncertain Pod creation writes no
  credentials. Kubernetes GC owns even delayed credential writes after Pod
  removal. No prepared-path name-only Secret deletion or adoption is used.
- The caller must persist/verify the binding before `ActivateWorkload`.
  Activation checks exact Pod/backend/claim identity, applies a per-Pod
  `agyn.io/workload-<pod-uid>` finalizer to every claim, and only then removes its
  gate with Pod UID/resource-version preconditions. Other gates/finalizers stay.
  Another Pod's hold prevents concurrent activation on the same claim.
- Successful deletion and lost replies do not release holds. Only a later
  `RemovePreparedWorkload` observing the original Pod absent can remove its own
  holds. It never removes another controller's finalizer or deletes a PVC.
  An activation never creates a Pod, so a delayed gate patch cannot activate a
  same-name replacement after the original UID is gone.
- Repeating activation on the same already-active binding is read-only. It is
  not permission to replay a task message or repeat uncertain preparation.
  JSON-Patch revision conflicts can surface as InvalidArgument or Aborted; the
  immutable activation may be retried, not retargeted.
- Legacy Stop/Remove reject prepared Pods. Legacy Start rejects held claims,
  but is otherwise still available. Drain/migrate all writers and enforce access
  before using the new path for production isolation. Never fall back on
  Unimplemented; distinct RPCs prevent old servers ignoring new preconditions.

## Read-Only Inspection

`InspectPreparedWorkload` takes the stored complete workload binding. It checks
the backend, Pod UID, original binding annotation, named claim set, claim UIDs
and owner labels. Active Pods require their existing claim holds; inspection
never repairs holds or removes a scheduling gate. An unactivated Pod must not
be scheduled or have evidence of container execution. Two Pod reads must retain
the same resource version. A concurrent change requires another read-only
attempt, not activation as a probe or a name-only fallback.

The response reports the validated Pod snapshot, native activation state,
deletion-pending flag and resource version. Activation is not container readiness.
This is not an atomic multi-resource snapshot, authenticated receipt, recovery
of an unknown prepare intent, or node/storage fencing.

Native `preparing` is not inspectable as a completed prepared workload. Recovery
of a lost prepare response still requires a separate, identity-checked discovery
contract; it must not infer completion from Pod absence or replay preparation.

Inspection adds no RBAC mutation rights. The focused tests assert GET-only
Kubernetes actions for successful, conflicting and failed inspections.

## Permissions And Limits

The chart adds PVC/Secret `patch` within its existing namespaced rules. No
Secret list/watch, namespace list/write, wildcard grant or new cluster-wide
mutation permission is added. The existing GET-only named-namespace grant is
still required. External RBAC must supply these same permissions.

The protocol currently limits preparations to 64 volume specs and binding
annotations to 64 KiB. UUIDs must be canonical, nonzero strings. Native Pod UIDs
are Kubernetes UUIDs, not caller-selected or inferred from a Pod name.

This is an execution identity guard, not an authenticated authorization receipt,
an admission webhook, storage fencing, or an exactly-once side-effect engine.
Out-of-protocol gate/identity/finalizer modification, privileged force deletion,
node partitions, cloned storage/cluster identity and legacy writers require
separate enforcement. A late preparation can leave a gated orphan; a late hold
write can require another bound cleanup attempt. Both need reconciliation.

## Atomic Secret Ownership

`fix/prepared-secret-ownership` is a dependent follow-up to prepared-inspection
runner `73c3a20`, not a standalone upstream-base patch. It adds no API schema or
RBAC grant. The legacy Start path retains its original startup rollback behavior.
The installed platform is not changed by these native fixtures.

The ordinary and full race suites each pass 521 test entries. Prepared tests pass
1,880 entries over 20 race-enabled repetitions. Build and unfiltered vet pass.
Opt-in live entries skip outside their gates; the dedicated child entry skips
unless launched by its parent. These skips are not claimed as native acceptance.

The explicit Kubernetes run passes eight scenarios plus the parent: the existing
execution/resume, PVC hold and stale activation cases, four real SIGKILL cases,
and a delayed Secret CREATE case. The child runs the production native prepare
method with chart-scoped impersonation and is killed after a committed Pod,
first Secret, last Secret or readiness PATCH, before that reply reaches the
method. The parent independently checks retained state, exact Pod ownership,
activation denial for incomplete setup, exact removal and observed Secret GC.
Two held Secret CREATEs also commit after owner deletion and are collected.

The fixture reads the interrupted Pod binding as an operator to perform cleanup.
That does not implement automatic registry/controller recovery of unknown prepare
outcomes. Delayed Pod/PVC creation, old unowned credentials, durable external
credential revocation, Secret-GC completion tracking, all-writer upgrades and
node/storage fencing remain separate work. No model credentials or A2A agent
are used. Pod absence alone is not evidence that all credentials are gone.

Owner semantics follow Kubernetes [owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/).

## Verification

The inspection follow-up passes all **493 ordinary native race-test entries**,
build and vet. The three live scenarios plus their parent pass again, now with
inspection before and after activation, after removal, and against a same-name
replacement. The bounded fixture uses no models or installed platform services.
The registry and controller are not part of this native fixture.

On 2026-09-14, build/vet and all 468 ordinary race-test entries pass. The
prepared tests pass another 1,060 entries across 20 repetitions. The three live
scenarios and parent pass on Kubernetes `v1.33.1+k3s1`, including asynchronous
Secret GC. Four other opt-in Kubernetes fixtures were not enabled; the direct
transport child helper is exercised through its parent. The initial live fixture
omitted required supporting-resource configuration and was rejected before any
workload creation; it was corrected without weakening runner validation.

Unit/race tests cover invalid and missing identities, backend changes, missing
resume PVCs (including loss between reads), concurrent-owner exclusion,
UID/resource-version races, lost activation replies, retry across runner
restart, namespace-version compatibility, protected Secret ownership, and
removal only after native absence. Existing lifecycle tests stay enabled.

Opt-in native acceptance uses a new namespace, chart service-account permissions,
GET-only namespace RBAC, a deny-network policy and bounded model-free containers.
It verifies real execution/resume, deletion protection and stale activation
against Kubernetes. Fixture-only temporary resources are removed and absence is
confirmed. It uses no A2A controller, registry, model credentials or actual Ziti
transport, and does not claim those acceptance scopes.

Generate the required local API before building this dependent runner. Run this
in the matching `api-prepared-inspection` checkout, with adjacent checkouts:

```bash
buf generate . --template ../runner-prepared-secret-ownership/buf.gen.yaml \
  --output ../runner-prepared-secret-ownership --include-imports \
  --path proto/agynio/api/runner/v1 \
  --path proto/agynio/api/runners/v1 --path proto/agynio/api/gateway/v1
```

```bash
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/explicit-kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 \
go test -race ./internal/server -run '^TestLivePreparedWorkloads$' -count=1 -timeout=8m
```

Kubernetes reference: [Pod scheduling readiness](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/),
[API concurrency and validation](https://kubernetes.io/docs/reference/using-api/api-concepts/),
[finalizer lifecycle](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/).
