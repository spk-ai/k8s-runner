# Preparation Revocation

Dependent proposal for interrupted first provisioning, requiring the matching
API, registry and controller changes. It is not an installed or standalone
production capability.

See [preparation_revocation.go](internal/server/preparation_revocation.go) and
[anchored_workload.go](internal/server/anchored_workload.go) for native contracts.

Background owner deletion is not immediate child deletion or generic
future-write/node fencing. Reconcile late children without discarding persistent
workspaces. Journals are retained indefinitely for now;
authenticated all-writer enforcement and a safe retention policy are required
before broader deployment. Existing repository licensing is unchanged.

## Verification

Historical revocation-branch evidence follows; use [README.md](README.md) for
the rebased build dependency.

On 2026-09-15 the source race suite passed 678 test entries with seven explicitly
gated live/helper skips. Build and vet passed. The native opt-in run passed all
14 scenarios plus parent (15 entries, no failures/skips): both owner kinds with
and without volumes before first creation, six real SIGKILL checkpoints, both
activation/revocation CAS orderings, and actually delayed Pod/PVC CREATEs.

The delayed-create cases verified old activation rejection, natural gated-Pod
garbage collection and exact PVC reuse by an explicit new model-free workload.
Fake-client tests alone do not establish these Kubernetes properties.

After generating matching APIs from the sibling contract checkout:

```sh
GOMAXPROCS=4 go test -race ./... -count=1
RUNNER_LIVE_PREPARED_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/local/kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 \
GOMAXPROCS=4 go test -race ./internal/server \
  -run '^TestLivePreparedWorkloads$/^preparation-revocation-' -count=1 -timeout=12m
```

The fixture uses its own labeled, quota-limited namespace and scoped RBAC. It
checks exact ownership before cleanup. Installed PVCs, deployment specifications
and readiness, namespaces, ClusterRoles and bindings were unchanged afterward;
no fixture namespace or task Pod remained. No provider credentials were used.
