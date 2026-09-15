# Anchored Volume Retirement

Dependent on the matching API `feat/anchored-volume-removal` branch and native
resource anchors. This source proposal is not installed or a drop-in release.

The new `RemoveVolumeAnchored` handler retires an exact bound PVC and its
persistent ConfigMap owner. The controller must first persist retirement intent
and exclude workload admission. Ordinary between-turn release still retains
the PVC and volume owner.

The handler validates the backend incarnation, complete identity labels and
owner UID before any mutation. A workload hold returns PENDING without mutation.
An atomic UID/resourceVersion-checked owner patch records the retiring PVC UID;
both previous and current preparation readers reject the non-active owner.
PVC deletion is conditional on its original UID and observed resourceVersion.
A later read must confirm PVC absence before conditional owner deletion. Only
independent absence observations of both objects produce ABSENT.

Unknown first provision, changed active identities, foreign ownership and
unsupported wire fields fail closed. A different-UID late child of the revoked
owner is never adopted or deleted by its discovered UID. The handler waits for
owner-based collection. Replaced owners are not cleanup targets. An observed
ABSENT is not a promise that no delayed child can appear in the future.

## Verification

The full source race suite passes 636 test entries, with seven explicitly gated
live/child skips. Fake-client cases cover target validation, workload/finalizer
holds, lost marker/PVC/owner acknowledgements, immutable retirement targets,
preparation rejection and separate PVC/owner absence.

Two real Kubernetes scenarios plus their parent pass under:

```sh
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/private/kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 \
go test -race ./internal/server \
  -run '^TestLivePreparedWorkloads$/^retired-volume-late-create-' -count=1 -timeout=5m
```

The fixture captures an actual original PVC CREATE, retires its exact Pod/PVC
and owners, then commits the old CREATE after retirement. Independent reads
observe natural Kubernetes collection without native deletion of the new UID.
The second scenario also replaces the volume owner with the same name and a
new UID: GC and stale retirement preserve the replacement. This models a late
duplicate write, not an interrupted network connection or production fencing.
All fixture resources are owned, bounded and disposed without stripping holds.

The matching controller branch has separate real PostgreSQL/native/process
crash acceptance. Registry receipts in this native-only fixture are not real.
No provider credentials or installed workspace are used. Authentication,
remaining late-resource reconciliation, durable credential cleanup, storage/node
fencing, all-writer migration and coordinated A2A rollout remain release work.

Kubernetes documents the relevant [owner-based garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)
and [PVC protection](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storage-object-in-use-protection).
