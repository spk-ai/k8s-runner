# Existing Workspace Adoption

Dependent native proposal requiring the matching API contract and a future
registry admission/migration coordinator. It is not installed or a drop-in
production upgrade. Existing repository licensing is unchanged.

Rebased API dependency: [`spk-ai/api` `c21440b`](https://github.com/spk-ai/api/commit/c21440bbd571d439c7c8aa316600d0d06392ccbf),
branch `sync/2026-09-24-volume-adoption`, including upstream flavor contracts.
Generate the native stubs from that checkout; the published BSR module does not
yet contain this proposal.

## Contract Owners

The four adoption handlers in
[volume_anchor_adoption.go](internal/server/volume_anchor_adoption.go) own
metadata reservation, journal UID pinning, original-PVC/spec validation,
conditional apply and separate finalization. Ordinary reuse is guarded by
[validatePVCReuseSpec](internal/server/pvc.go).
No adoption operation allocates storage, supplies credentials or executes a turn.

## Recovery Boundary

The caller must hold a durable owner-wide admission block before reserving,
drain every old writer and retain that block until the finalized native evidence
is committed. No boolean caller assertion or empty Pod list establishes node
fencing. Registry adoption fields, SQL guards and the coordinator are separate
required work. Existing allocation reservations must not be fabricated for
migrated storage.

If an owner disappears after attachment, Kubernetes may request dependent PVC
deletion. The adoption hold retains that original claim and backing volume;
native recovery refuses the now-missing owner rather than removing the hold or
creating a replacement. This is retention for operator reconciliation, not an
automatic recovery of a terminating PVC. See Kubernetes' [finalizer semantics](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
and [owner garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/).

The journal is retained indefinitely for now. Authenticated callers, old-writer
and node/storage fencing, full durable-state backups, retention policy and a
coordinated rollout remain release gates. Completed-turn recovery does not prove
an interrupted executed turn can safely be retried.

## Verification

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

The focused fake API evaluates actual JSON Patch tests and advances revisions.
It covers invalid/changed receipts, missing/replaced resources, incomplete Pod
inventory, active references and holds, lost replies at all six writes, CAS
conflicts without retargeting, partial metadata and existing workload reuse.

The opt-in fixture uses the chart's restricted service account, loopback gRPC,
a unique namespace, network-denied resource-bounded Node Pods and fresh 1 MiB
workspaces. Both agent and sandbox owner kinds run normal adoption, six actual
SIGKILL checkpoints and owner-GC retention. Successful adoption is followed by
a distinct Pod reading the original file and confirmed compute removal. These
are model-free native tests, not a registry/A2A/provider deployment test.

Cleanup validates exact test ownership. The owner-GC scenarios explicitly remove
only the new fixture's sole adoption hold after proving native retention; all
other cleanup uses confirmed workload removal and namespace ownership checks.
No installed namespace, workspace, service or provider credential is used.
