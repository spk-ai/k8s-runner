# Prepared Workloads

This guide records the original preparation, inspection, Secret-ownership and
observation acceptance. Dependency revisions and results below are historical;
see [README.md](README.md) for the rebased build dependency.

Dependent preparation-observation implementation on atomic-Secret runner
`1f33556`. This branch requires the matching `feat/prepared-outcome-observation`
API, based on inspection API `24b73ca`. It is not a drop-in image for installed
controllers. No installed platform deployment changes in these fixtures.

## Contract Owners

Start at [prepared_workload.go](internal/server/prepared_workload.go); related
contracts are in [anchored_workload.go](internal/server/anchored_workload.go) and
[pvc.go](internal/server/pvc.go).

Deployment requires Kubernetes >=1.30, strict Pod-create validation, gate-aware
schedulers and trusted admission preserving identity and gates. Drain/migrate
all writers and coordinate registry/controller/native/API versions. Distinct
RPCs must fail closed on Unimplemented; legacy methods are not a safe fallback.

## Read-Only Inspection

See `InspectPreparedWorkload` in
[prepared_inspection.go](internal/server/prepared_inspection.go).
Activation is not readiness; inspection is not an atomic multi-resource snapshot,
authenticated receipt or node/storage fence. Incomplete preparation has its
separate retirement-discovery path below.

## Lost Preparation Observation

See `ObserveWorkloadPreparation` in
[prepared_observation.go](internal/server/prepared_observation.go).
The controller must persist retirement authority and validate durable identities
before cleanup. No missing outcome authorizes another preparation; late creation,
old ownerless Secrets and authenticated all-writer enforcement need separate
reconciliation. The evidence below retains its original observation-only scope.

On 2026-09-15, the full native race suite passes 551 test entries, with seven
opt-in/child entries skipped outside their gates; build and unfiltered vet pass.
The isolated Kubernetes run passes eight scenarios plus parent with no skips.
Its four SIGKILL cases now use this real RPC to recover interrupted bindings,
then independently compare Pod/PVC identities and observe exact removal and
Secret GC. It still includes real execution/resume, stale activation, claim
holds and delayed Secret creation after Pod deletion. Registry/controller
recovery has a separate combined process fixture; neither fixture runs A2A or
native agent sessions. The old ownership fix's evidence below is historical.

## Permissions And Limits

External RBAC must supply the [chart's workload permissions](charts/k8s-runner/values.yaml)
and [named-namespace grant](charts/k8s-runner/templates/volume-backend-rbac.yaml)
before rolling out the runner image.

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

That earlier fixture read the interrupted binding as an operator. The current
fixture uses the new observation RPC, but native-only acceptance does not prove
registry/controller recovery. Delayed Pod/PVC creation, old unowned credentials, durable external
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

The [native fixture](internal/server/prepared_workload_live_test.go) requires
explicit disposable-cluster permission, including namespace/RBAC creation and
service-account impersonation. It is not acceptance of the A2A controller,
registry, model credentials or actual Ziti transport. Do not use installed
workspaces or provider credentials.

To reproduce the historical observation branch, generate its required local API
in the matching `api-prepared-observation` checkout, with adjacent checkouts:

```bash
buf generate . --template ../runner-prepared-observation/buf.gen.yaml \
  --output ../runner-prepared-observation --include-imports \
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
