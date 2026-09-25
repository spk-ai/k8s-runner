# k8s-runner

k8s-runner is the Kubernetes-native implementation of the RunnerService gRPC API.

See [AGENTS.md](AGENTS.md) for source owners and contribution rules, and
[docs/catalog.json](docs/catalog.json) for operational and historical documents.

The `sync/2026-09-24-volume-adoption` branch rebases the tested contribution
stack onto upstream `3bd3355`. Generate from `spk-ai/api` `c21440b` on
`sync/2026-09-24-volume-adoption`, which includes upstream flavor contracts and
the unpublished lifecycle proposals. The older dependency revisions below are
historical acceptance records, not the build input for this combination.

The dependent [preparation-revocation proposal](PREPARATION-REVOCATION.md)
recovers unbound interrupted provisioning using durable native evidence.

The dependent [existing-workspace adoption proposal](VOLUME-ANCHOR-ADOPTION.md)
attaches persistent ownership to an original PVC without reallocating storage.

Architecture: [k8s-runner](https://github.com/agynio/architecture/blob/main/architecture/k8s-runner.md)

## Volume Inventory Integrity

See `ListVolumes` in [query.go](internal/server/query.go) for the inventory contract.

A damaged inventory requires operator ownership reconciliation before volume
reconciliation can proceed. Inventory is not deletion authorization or
node/late-create fencing; the controller must not infer deletion permission from
a stale or scoped registry scan.

After generating the APIs, run `go test -race ./...`. Focused tests are
`go test -race ./internal/server -run '^TestListVolumes' -count=1`.

## Volume Backend Identity

See [volume_backend.go](internal/server/volume_backend.go) for the native contract.
The backend-identity API extension and all consumers must be coordinated;
a different namespace UID is not evidence that an original PVC disappeared.

When managing RBAC externally, install the scoped namespace permission in
[volume-backend-rbac.yaml](charts/k8s-runner/templates/volume-backend-rbac.yaml)
as well as the [workload rules](charts/k8s-runner/values.yaml).
`workloadNamespace` must match `KUBE_NAMESPACE`. A namespace Role alone cannot
grant access to the cluster-scoped namespace object. See
[Kubernetes named-resource RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources).

Historical backend-identity acceptance: all 186 independent race tests, including
the Helm rendering check, pass; build and vet pass. Wrong/unavailable namespaces,
inventory/absence races and runner restart are covered. A separate
controller/registry acceptance uses real Kubernetes and PostgreSQL. Those
backend-identity checks alone do not authenticate the runner route, bind
workload-start requests, fence delayed operations, protect cloned cluster
identities or perform a rollout/adoption. It is not a drop-in upgrade.

## Checked Volume Removal

See [volume_removal.go](internal/server/volume_removal.go) and
[anchored_volume_removal.go](internal/server/anchored_volume_removal.go).
Deploy only after coordinating the API, durable registry intents and every
orchestrator/sandbox cleanup caller. This branch is not a stock-image drop-in.

The Kubernetes fake does not itself enforce delete preconditions; native
acceptance is required separately from [unit tests](internal/server/volume_removal_test.go).
Caller authentication, durable intent storage, all-writer/late-create and
node/storage fencing are not implemented by this runner change alone.

### Native checked-removal acceptance

The [native removal fixture](internal/server/volume_removal_live_test.go) runs
within the [PVC fixture](internal/server/pvc_live_test.go). Use only an explicitly
authorized disposable local cluster; see [operator permissions](#persistent-claim-reuse).

```sh
RUNNER_LIVE_PVC_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/local-kubeconfig \
go test -race ./internal/server -run '^TestLivePVCOwnership$' -count=1 -timeout=5m
```

This fixture is restricted to unbacked claims, synthetic finalizers and no Pods.
Do not use installed workspaces or strip Kubernetes/unrelated finalizers to
complete cleanup. Native-only checks are not deployed orchestrator/registry/A2A
acceptance. The fixture commit is an integration artifact, separate from the
focused runner production patch and unit tests.

## Prepared Workloads

See [Prepared Workloads](PREPARED-WORKLOADS.md) for API, Kubernetes and RBAC
prerequisites and historical native acceptance.
Control-plane persistence/caller migration, reconciliation, authenticated
enforcement and coordinated A2A rollout remain pending. It is not a drop-in image.

## Control Transport

Listener access rules live in [transports.go](cmd/k8s-runner/transports.go).
Neither TCP connectivity nor `Ready` proves successful enrollment or an
available overlay terminator.

Previously both listeners shared a full gRPC server, so a client that could
reach the TCP port could bypass the overlay's service-access policy. Network
policies may restrict that reachability; this is an application-level closure,
not a claim that every deployed workload could reach the port. OpenZiti's
[Dial and Bind policies](https://netfoundry.io/docs/openziti/learn/core-concepts/security/authorization/policies/overview/)
remain responsible for who may access and provide the overlay service.

Clients using raw TCP for control while the runner has Ziti enabled must move
to the overlay before rollout. Disabling Ziti is not a secure workaround.
`ZITI_ENABLED=false` retains the existing standalone plaintext API for trusted
development or independently protected deployments; this patch does not make
that mode suitable for untrusted agents. Production must enforce the intended
transport profile and audit overlay policies and all callers.

The [transport tests](cmd/k8s-runner/transports_test.go) and
[startup process fixture](cmd/k8s-runner/transports_process_test.go) use loopback
connections, not real Kubernetes operations, enrollment or provider calls.
Historical acceptance on the independent transport branch: full
`go test -race ./...` passes 152 tests including subtests; the child entry point
runs only when launched by its parent test. Build and vet also pass.

This is not an audit of live Dial/Bind policies, real overlay reconnection or
revocation, per-owner authorization, runner/backend incarnation binding,
late-operation/node fencing, or a coordinated deployed A2A acceptance. Those
remain separate requirements; neither this health endpoint nor overlay
connectivity is proof of the expected storage backend.

## Local Development

Full setup: [Local Development](https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md)

### Prepare environment

Use an explicitly selected development cluster and permission to change its
platform deployments. Install the tools required by the linked bootstrap guide;
the Go toolchain requirement is in [go.mod](go.mod), and API generation uses
[Buf](buf.gen.yaml). The setup below changes cluster state.

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

See [bootstrap](https://github.com/agynio/bootstrap) for details.

### Run from sources

Use the checked-in [DevSpace workflow](devspace.yaml) after bootstrap. Its
published-schema generation is not a substitute for the matching local API
required by this contribution stack.

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```

### E2E tests

E2E coverage runs from the centralized suite in
[`agynio/e2e`](https://github.com/agynio/e2e) using the `k8s_runner` service tag.
See [E2E Testing](https://github.com/agynio/architecture/blob/main/architecture/operations/e2e-testing.md).

## Compute resource capability

`compute-resources` is an opt-in RunnerService capability. It requires the
`ContainerSpec.resources` API addition in `agynio/api` and an orchestrator that
transmits the selected environment flavor's bounds. Upgrade the API, runner,
then orchestrator before opting an agent profile in. Older runners must reject
the unknown required capability, rather than silently ignore new protobuf fields.

Choose operator-owned `SUPPORTING_CONTAINER_RESOURCES` bounds for the intended
workload pool. The JSON schema and validation are in
[ComputeResources](internal/config/catalog.go) and
[compute_resources.go](internal/config/compute_resources.go); request enforcement
and flavor compatibility are in [workload.go](internal/server/workload.go) and
the [compatibility tests](internal/server/flavor_compatibility_test.go).

Bounds are **per container**, not one shared task budget. Supporting-container
allocations are additional to the main flavor. This does not limit the number
of tasks/containers, ephemeral storage, PIDs or network traffic. It does not
harden privileged Docker or change security profiles. Deployment admission,
aggregate quotas and adversarial sandboxing remain separate concerns.

### Local API generation and enforcement test

Until the API addition is published to BSR, generate against sibling source
checkouts instead of the default published input. Use the rebased API revision
named at the top of this guide; adjust sibling checkout names as needed:

```bash
cd ../api
buf generate . --template ../k8s-runner/buf.gen.yaml \
  --path proto/agynio/api/runner/v1 --path proto/agynio/api/runners/v1 \
  --path proto/agynio/api/gateway/v1 --include-imports --output ../k8s-runner
cd ../k8s-runner
go test ./...
```

The [enforcement fixture](internal/server/compute_resources_live_test.go) requires
Linux/cgroup-v2, a digest-pinned Node.js image and authorization for a disposable
local cluster. It deliberately stresses CPU and causes a bounded OOM kill; never
run it against installed workloads or with provider credentials.

```bash
RUNNER_LIVE_RESOURCE_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/lab-kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:<verified-digest> \
go test -v ./internal/server -run '^TestLiveComputeResources$' -count=1 -timeout=6m
```

This test does not establish end-to-end agent continuation or production
security. See Kubernetes' [CPU and memory enforcement](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).

## Failed Startup Secrets

Rollback and uncertain-create rules live in
[startup_secrets.go](internal/server/startup_secrets.go).

Deploy the chart's updated Secret rule before the new runner image: cleanup
requires `get` in addition to `create` and `delete` in the workload namespace.
Operators overriding `rbac.rules` must update their rule explicitly; an image-only
rollout is insufficient. An unconfirmed cleanup diagnostic is not retry authority.

The [native startup fixture](internal/server/startup_secrets_live_test.go) requires
a trusted disposable cluster and permission to create a namespace and its
ServiceAccount/Role/RoleBinding and impersonate that account. Use only synthetic
credentials and unbacked claims; no existing workspace or agent belongs in this
fixture.

```bash
RUNNER_LIVE_STARTUP_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/lab-kubeconfig \
go test -race -v ./internal/server -run '^TestLiveStartupSecretCleanup$' -count=1 -timeout=5m
```

Process-crash orphan reconciliation, late-create fencing, named-PVC authorization
and post-success Stop/Remove cleanup are separate lifecycle work. This change
does not add a garbage collector, delete durable workspaces, change the Runner
API, or establish end-to-end A2A recovery under first-provision rejection.

## Persistent claim reuse

Custom callers must set a stable, owner-specific `labels.volume_key` before
upgrading; a key derived from a transient Pod/workload ID would break workspace
continuation.

See [pvc.go](internal/server/pvc.go) and
[prepared_workload.go](internal/server/prepared_workload.go) for reuse contracts,
and [Existing Workspace Adoption](VOLUME-ANCHOR-ADOPTION.md) for migration gates.
Legacy claims missing identity require audited operator reconciliation, not
relabeling or a new empty workspace.

These checks do not authenticate RPC callers, fence old workloads or prevent a
privileged writer from replacing a claim after validation. `ReadWriteOnce` is
not a single-writer lock: Kubernetes permits same-node Pods to share it. See
[Kubernetes access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).
Admission controls, lifecycle fencing and secure RPC authorization remain
separate requirements. Startup Secret rollback is a separate change; deployments
using pull credentials should include that fix when enabling these rejections.

The [native PVC fixture](internal/server/pvc_live_test.go) requires a disposable
local cluster, namespace/RBAC creation and impersonation rights. It must remain
restricted to unbacked claims and no Pods, never installed workspaces.

```sh
RUNNER_LIVE_PVC_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/test-kubeconfig \
go test -race ./internal/server -run '^TestLivePVCOwnership$' -count=1 -timeout=4m -v
```

Explicitly select the intended cluster; do not use a default kubeconfig. Where
trust-manager injects CA bundles, the operator also needs permission to read
the controller metadata. Investigate unexpected injected objects or backed
claims instead of bypassing the fixture's cleanup refusal.

## Docker capability notes

Review [capabilities.go](internal/server/capabilities.go) before enabling Docker.
Even the rootless implementation can require Pod Security Admission exceptions;
baseline/restricted clusters may reject it. Rootless Docker is not a substitute
for the deployment's sandboxing policy.

### Kata (microVM) docker runtimes

For a Kata implementation selected through `CAPABILITY_IMPLEMENTATIONS`, the
cluster must provide the matching RuntimeClass and schedule onto KVM-capable
nodes. This cannot be validated on local k3d/mac setups.

## Workload ResourceQuota

Configure `workloadResourceQuota` using [chart values](charts/k8s-runner/values.yaml)
and the [quota template](charts/k8s-runner/templates/workload-resourcequota.yaml).
Size the entire namespace, including other producers and init/sidecar costs;
a main-container flavor is not a complete task budget.
See [ResourceQuota](https://v1-33.docs.kubernetes.io/docs/concepts/policy/resource-quotas/)
and [sidecar accounting](https://v1-33.docs.kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#resource-sharing-within-containers).

Update the existing `KUBE_NAMESPACE` entry when changing `workloadNamespace`;
do not add a second entry in `extraEnvVars`. Protect quota and runner configuration
with operator RBAC; this is not protection against a cluster administrator
changing or deleting them.

Install and verify the quota before enabling workload producers. The selected
runner and workload profile must provide CPU and memory requests and limits
for every main, init and supporting container. Do not assume legacy workload
profiles can run under the budget.
The caller must retain durable state and distinguish a rejected start from an
ambiguous operation before attempting recovery.

Account for retained Pod objects as well as running compute. A PVC count or
requested-storage quota is not enforcement of bytes written inside a filesystem.
This is also not a task scheduler, tenant fairness policy, node partition fence,
PID/IO cap, or proof of fail-closed platform bootstrap. Inspect existing namespace
usage and all producers before adopting a budget.

Render validation is maintained in the [chart tests](internal/chart/quota_test.go)
and [CI chart check](scripts/verify-workload-egress-networkpolicy.sh).

The chart contribution is independent of A2A and agent runtimes. Live runner
admission, including supporting-container accounting and release/reuse, needs
separate acceptance with the resource-aware integration build.

### Combined local quota acceptance

This lab branch combines the separate chart and container-resource contributions;
it is not a proposed bundled upstream change. The
[quota fixture](internal/server/compute_quota_live_test.go) requires a digest-pinned
Node image whose default user is UID 1000. Its native-only scope is not a separate
CNI proof or provider/agent acceptance.

After local API generation and `helm dependency build charts/k8s-runner`, load
the non-root probe image into the explicitly selected trusted local cluster:

```sh
env -u RUNNER_LIVE_RESOURCE_TEST GOMAXPROCS=4 \
  RUNNER_LIVE_QUOTA_TEST=trusted-local \
  RUNNER_LIVE_KUBECONFIG=/absolute/path/to/lab-kubeconfig \
  RUNNER_LIVE_NODE_IMAGE=local-probe@sha256:<verified-loaded-digest> \
  go test -v ./internal/server -run '^TestLiveWorkloadQuota$' -count=1 -timeout=6m
```

The test passed on local K3s `v1.33.1+k3s1` on 2026-09-14 in 35.82 seconds,
using runner source `dc67264` and API `3c84a6a`. Each of the four CPU/memory
quota keys independently rejected a start before Pod creation. Effective quota
usage included normal sidecars, a restartable init and a larger regular init.
A retained Succeeded Pod consumed `count/pods` but no compute quota. Six
concurrent starts with one object slot admitted exactly one; five rejected Pods
were independently confirmed absent. The existing neighbor kept progressing.
After StopWorkload and observed Pod deletion, all quota usage returned to zero;
ownership-checked cleanup confirmed namespace deletion. Ordinary full runner
race tests pass with live tests disabled. This does not prove A2A recovery from
quota rejection, node fencing, production sizing or hardened agent execution.

## Workload egress NetworkPolicy

Use [chart values](charts/k8s-runner/values.yaml) for the supported underlay
options and port-selection rationale, and the
[egress template](charts/k8s-runner/templates/workload-egress-networkpolicy.yaml)
for the rendered rules. Supply the actual workload namespace, cluster Pod/Service
CIDRs and additional internal ranges for the installation.

Bootstrap must coordinate `ziti-workload-dns`, the enrollment controller, router
and any runtime Istio ingress gateway. Derive endpoint CIDRs/selectors from the
live `ziti-controller-client`, `ziti-router-edge` and `istio-ingressgateway`
services and their backends, not copied lab addresses. When narrowing ports,
review the deployed backend ports using the guidance in chart values.

Where runtime `ziti.<base-domain>:443` uses Istio TLS passthrough, retain its SNI
route to the controller client service while keeping `.agyn` application traffic
on the overlay. Verify enrollment and runtime access against the installed CNI;
the [render check](scripts/verify-workload-egress-networkpolicy.sh) does not prove
live routing or enforcement.

## Workload ingress isolation

Egress restrictions alone do not prevent other pods from opening connections to
a workload. For installations whose workload access uses outbound OpenZiti
connections, opt in through `workloadIngressNetworkPolicy` in
[chart values](charts/k8s-runner/values.yaml). Review the
[ingress template](charts/k8s-runner/templates/workload-ingress-networkpolicy.yaml)
against any direct-ingress requirements before enabling it. Helm merges selector
maps with defaults; set a default key to `null` to remove that key.

Verify live direct Pod-IP and Service-IP connections from another pod are denied,
with working listeners and positive controls, and verify overlay enrollment,
terminal access and agent execution still work. Rendering the chart alone does
not prove the CNI enforces it. NetworkPolicies are additive: another ingress
policy can allow traffic denied by this policy's empty rule set. Review all
policies selecting the workloads, including namespace-wide ones.

This is a pod-network boundary, not a sandbox or per-session authorization
mechanism. It does not isolate containers sharing a pod, prevent incoming node
traffic, constrain traffic inside OpenZiti, or restrict the public destinations
allowed by the egress policy. Nonprivileged runtime configuration, overlay
authorization and credential isolation remain separate requirements. See the
[Kubernetes NetworkPolicy semantics](https://kubernetes.io/docs/concepts/services-networking/network-policies/).
