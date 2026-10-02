# Existing Workspace Adoption

Dependent native proposal requiring the matching API contract and a future
registry admission/migration coordinator. It is not installed or a drop-in
production upgrade. Existing repository licensing is unchanged.

Rebased API dependency: [`spk-ai/api` `c21440b`](https://github.com/spk-ai/api/commit/c21440bbd571d439c7c8aa316600d0d06392ccbf),
branch `sync/2026-09-24-volume-adoption`, including upstream flavor contracts.
Generate the native stubs from that checkout; the published BSR module does not
yet contain this proposal.

## Contract Owners

See [volume_anchor_adoption.go](internal/server/volume_anchor_adoption.go) and
[pvc.go](internal/server/pvc.go) for native adoption and reuse contracts.

## Recovery Boundary

The caller must hold a durable owner-wide admission block before reserving,
drain every old writer and retain that block until the finalized native evidence
is committed. No boolean caller assertion or empty Pod list establishes node
fencing. Registry adoption fields, SQL guards and the coordinator are separate
required work. Existing allocation reservations must not be fabricated for
migrated storage.

Owner loss after attachment can leave a terminating PVC under an adoption hold.
Retain it for operator reconciliation; removing the hold or creating a replacement
is not recovery of that original workspace. See Kubernetes' [finalizer semantics](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
and [owner garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/).

The journal is retained indefinitely for now. Authenticated callers, old-writer
and node/storage fencing, full durable-state backups, retention policy and a
coordinated rollout remain release gates. Completed-turn recovery does not prove
an interrupted executed turn can safely be retried.

## Verification

The results below are historical native acceptance, not a rerun of the rebased stack.

On 2026-09-15, the ordinary and unfiltered race suites each passed 838 test
entries with seven opt-in live/helper skips. Build and unfiltered vet passed.
The corrected isolated Kubernetes matrix passed all 16 scenarios plus parent,
including 12 actual SIGKILLs, with no failures/skips and confirmed fixture cleanup.
An independent before/after observation matched all 108 pre-existing PVCs/PVs,
52 deployments, namespaces, cluster RBAC and Docker container identities.

After generating matching APIs from the sibling contract checkout:

```sh
GOMAXPROCS=4 go test -race ./...
GOMAXPROCS=4 go vet ./...
GOMAXPROCS=4 go build ./...
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/local-kubeconfig \
RUNNER_LIVE_NODE_IMAGE='node@sha256:<reviewed-image-digest>' \
GOMAXPROCS=4 go test ./internal/server \
  -run '^TestLivePreparedWorkloads/volume-adoption-' -count=1 -timeout=13m
```

The [fake-client tests](internal/server/volume_anchor_adoption_test.go) and
[native fixture](internal/server/prepared_workload_live_test.go) own the scenario
matrix. Native reproduction requires an explicitly authorized disposable cluster
with namespace/RBAC creation and service-account impersonation rights. It is not
a registry/A2A/provider deployment test.

Cleanup validates exact test ownership. The owner-GC scenarios explicitly remove
only the new fixture's sole adoption hold after proving native retention; all
other cleanup uses confirmed workload removal and namespace ownership checks.
No installed namespace, workspace, service or provider credential is used.
