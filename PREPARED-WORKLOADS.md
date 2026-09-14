# Prepared Workloads

Dependent native implementation on combined runner `2968787` and the matching
`feat/prepared-workloads` API. This does not migrate the registry/orchestrator
and is not a drop-in image for installed controllers.

## Native Contract

- `PrepareWorkload` requires a canonical workload UUID and expected backend.
  Existing volume bindings are read-only inputs: a missing or replaced bound
  claim is never recreated. New volumes use the existing ownership/spec checks.
  All named volumes appear in the returned binding, not just main's mounts.
- The Pod starts with `agyn.io/workload-binding` in `spec.schedulingGates`.
  Its annotation stores backend/workload/volume identities, not credentials.
  Kubernetes >=1.30 is verified before resource creation; Pod creation requests
  strict field validation. Trusted admission must preserve the gate.
- Temporary Secrets get an owner reference to the exact returned Pod UID via
  UID/resource-version checked patches. Normal Kubernetes GC removes them after
  Pod deletion. Partial preparation still needs orphan reconciliation.
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

## Permissions And Limits

The chart adds named PVC/Secret `patch` to its existing namespaced rules. No
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

## Verification

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

```bash
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/explicit-kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 \
go test -race ./internal/server -run '^TestLivePreparedWorkloads$' -count=1 -timeout=8m
```

Kubernetes reference: [Pod scheduling readiness](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/),
[API concurrency and validation](https://kubernetes.io/docs/reference/using-api/api-concepts/),
[finalizer lifecycle](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/).
