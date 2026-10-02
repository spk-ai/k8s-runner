# Native Resource Anchors

This guide preserves the original resource-anchor acceptance and integration
gates. Use [README.md](README.md) for the rebased build dependency; the revisions,
checklist and results below are historical, not current release acceptance.

Native source and isolated Kubernetes acceptance pass on `feat/resource-anchors`,
based on preparation observation `6fdcc41` and API `3b25d03` (base `d6449dd`).
Registry/controller integration remains incomplete. Nothing is installed.
This is a dependent capability proposal, not production readiness or a safe
mixed-writer upgrade. Preserve the reviewed installed prepared/DNS stack.

## Contract Owners

Start at [resource_anchors.go](internal/server/resource_anchors.go),
[anchored_workload.go](internal/server/anchored_workload.go) and
[prepared_workload.go](internal/server/prepared_workload.go) for native contracts.

Registry persistence must precede native creation authority. Labels and backend
IDs are assertions, not caller authentication. A same-name owner is not a
replacement for a persisted UID, and owner absence is not child cleanup evidence.

## Implementation And Acceptance Checklist

Native acceptance is recorded below. Remaining integration gates at that revision:

- [ ] Registry persistence and all-writer guards before native write authority.
- [ ] Agent/sandbox controller migration and interrupted preparation recovery.
- [ ] Checked volume retirement and stale-create cleanup without workspace loss.
- [ ] Coordinated DNS-compatible A2A/agent rollout and production enforcement.

Do not treat a timeout, NotFound check or read-then-create as proof that an
in-flight Kubernetes request cannot commit later.

Kubernetes documents same-namespace [owner references](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/),
asynchronous [garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)
and [conditional API updates](https://kubernetes.io/docs/reference/using-api/api-concepts/).
Controller authorization, orphan metadata reconciliation, authenticated routes,
admission enforcement, node/storage fencing, external credential revocation and
coordinated upgrades remain required beyond the native capability.

## Verification And Reproduction

The ordinary and unfiltered race suites pass 610 entries each, with seven
explicit opt-in/child skips. The focused anchor suite passes 53 entries and
1,060 entries across 20 race repetitions. Build and unfiltered vet pass. This
includes lost selection/activation acknowledgements, corrupt metadata and nil
lookup results, consumed-anchor rejection, exact UID/revision deletion and the
scope of the new ConfigMap grant (get/create/patch/delete, no list/watch).

The final real Kubernetes run passes 15 scenarios plus parent, with no skips,
in 236.223 seconds. The initial 14-scenario run also passes.
It retains all eight preceding prepared-workload cases, including four runner
SIGKILL checkpoints and late Secret writes. New cases cover durable workspace
reuse for agent/sandbox owners, zero-volume execution, actual activation PATCH
404 after revocation, actual stale DELETE 409 after activation, and late Pod/PVC
CREATE after owner removal. Late gated Pod GC is observed before any explicit
Pod cleanup, including when a new same-name anchor exists with a different UID.
That replacement anchor survives old-child GC. A late PVC remains under its
persistent owner and is reused.

These are real native gRPC/Kubernetes operations using bounded fixed Node
programs, not provider agents or the A2A controller. The parent is the owner of
fixture bindings; there is no production registry persistence or authenticated
overlay in this fixture. Native anchor-claim lost acknowledgements are tested
with fake clients, separately from the existing real SIGKILL checkpoints.

Generate only the required API paths from the matching API worktree, using
this repository's `buf.gen.yaml`; do not commit unrelated generated LLM code.
Run `go test -race ./...`, `go build ./...` and `go vet ./...`. For live acceptance:

```sh
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/private/kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 \
go test -race ./internal/server -run '^TestLivePreparedWorkloads$' -count=1 -timeout=14m
```

The fixture requires explicit local-operator permission to create a disposable
namespace, chart-derived workload Role and an exact Namespace GET grant. It
impersonates that service account for native operations, denies network access
and checks identity before cleanup. Namespace deletion retires only fixture
volumes/anchors after exact workload removal and hold checks; this is not the
missing production anchored-volume deletion contract. No finalizers are
stripped and no installed workload, credential binding or PVC is changed.

The dependent [anchored retirement proposal](ANCHORED-VOLUME-RETIREMENT.md)
adds a distinct checked PVC-and-owner deletion capability and its own evidence.
It does not change the historical scope of the anchor acceptance above.
