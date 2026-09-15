# Existing Workspace Adoption

Dependent native proposal requiring the matching API contract and a future
registry admission/migration coordinator. It is not installed or a drop-in
production upgrade. Existing repository licensing is unchanged.

API dependency: [`spk-ai/api` `66fd206`](https://github.com/spk-ai/api/commit/66fd206cf20b0ccea3e0a01b1f0c7ae60a0cbf75),
branch `feat/volume-anchor-adoption`, built on the preparation-revocation contract.
Generate the native stubs from that checkout; the published BSR module does not
yet contain this proposal.

## Native Transition

`ReserveVolumeAnchorAdoption` accepts a canonical operation UUID, complete
unanchored checked volume and matching owner intent. The PVC must be Bound,
retain its original UID/name/identity, have no workload holds and have no Pod
references in a complete versioned namespace inventory. Terminal, deleting and
unmanaged Pods count as references. It does not mutate the PVC.

The reservation creates an immutable volume-owner ConfigMap marked
`volume-adopting-v1`, then an immutable `volume-adoption-<volume-id>` journal.
The owner intent pins the operation, original PVC and its native spec SHA-256.
An owner UID/resource-version PATCH pins the journal UID before a reservation
is acknowledged. A missing pinned journal cannot be recreated on retry.

`ApplyVolumeAnchorAdoption` verifies the entire receipt, rechecks drain and
atomically attaches owner references, receipt/state annotations and
`agyn.io/workload-adopt-<operation-id>` using PVC UID/resource-version tests.
The hold belongs to the existing workload-hold namespace so retirement retains
it; ordinary workload cleanup cannot own its non-Pod suffix. The PVC spec and
unrelated labels, annotations and finalizers are preserved.

`FinalizeVolumeAnchorAdoption` is separate from persisting the applied binding.
It changes the owner to active metadata, re-observes the original storage and
drain, then atomically marks the PVC ready and removes only its own hold. An
active owner with an applied/held PVC still fails ordinary reuse. Incomplete
adoption annotations and foreign adoption holds also fail closed.

`ObserveVolumeAnchorAdoption` never mutates anything. The exact journal, owner,
PVC/backend identities and original spec must match. A repeated finalization
is read-only after readiness is observed; a lost write response is not success.
No adoption RPC creates/deletes a PVC or Pod, supplies credentials, resizes data,
relabels an owner, or dispatches an agent turn.

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
