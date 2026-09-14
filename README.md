# k8s-runner

k8s-runner is the Kubernetes-native implementation of the RunnerService gRPC API.

Architecture: [k8s-runner](https://github.com/agynio/architecture/blob/main/architecture/k8s-runner.md)

## Local Development

Full setup: [Local Development](https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md)

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

See [bootstrap](https://github.com/agynio/bootstrap) for details.

### Run from sources

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

Set `SUPPORTING_CONTAINER_RESOURCES` to an explicit operator-owned JSON object:

```json
{"requestsCpu":"50m","requestsMemory":"64Mi","limitsCpu":"500m","limitsMemory":"256Mi"}
```

These are example bounds, not built-in defaults or production sizing advice.
Invalid configuration fails startup. The runner advertises `compute-resources`
only when valid supporting bounds are configured. An empty/unset variable
disables it, even if the catalog lists the capability.

Requests requiring this capability must include all four main-container fields.
Supporting containers can omit the entire resource message to use the configured
bounds, but explicit empty/partial messages are rejected. This includes normal
sidecars, init containers, restartable init containers and Docker containers
injected by the runner. Every quantity must be positive, representable, use
whole millicores/bytes, and have its request no greater than its limit. Validation
occurs before Kubernetes access, including PVC or secret creation. Resource
fields without the required capability are rejected. Legacy requests with
neither the capability nor fields retain their previous behavior.

Bounds are **per container**, not one shared task budget. Supporting-container
allocations are additional to the main flavor. This does not limit the number
of tasks/containers, ephemeral storage, PIDs or network traffic. It does not
harden privileged Docker or change security profiles. Deployment admission,
aggregate quotas and adversarial sandboxing remain separate concerns.

### Local API generation and enforcement test

Until the API addition is published to BSR, generate against sibling source
checkouts instead of the default published input:

```bash
cd ../api
buf generate . --template ../k8s-runner/buf.gen.yaml \
  --path proto/agynio/api/runner/v1 --path proto/agynio/api/runners/v1 \
  --path proto/agynio/api/gateway/v1 --include-imports --output ../k8s-runner
cd ../k8s-runner
go test ./...
```

The opt-in Linux/cgroup-v2 test uses an explicitly selected trusted test cluster,
a digest-pinned Node.js image and its own temporary namespace. It calls the real
runner over loopback gRPC, checks cgroups before stress, then checks CPU
throttling, a bounded OOM kill, supporting-container cgroups and a healthy
neighbor. It uses no PVCs, model credentials or external Pod networking. Cleanup
verifies Pod and namespace removal and refuses foreign Pod/PVC ownership.
Ordinary tests skip it. Only run against a disposable local lab:

```bash
RUNNER_LIVE_RESOURCE_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/lab-kubeconfig \
RUNNER_LIVE_NODE_IMAGE=node:22-bookworm-slim@sha256:<verified-digest> \
go test -v ./internal/server -run '^TestLiveComputeResources$' -count=1 -timeout=6m
```

This test does not establish end-to-end agent continuation or production
security. See Kubernetes' [CPU and memory enforcement](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).

## Failed Startup Secrets

Pull and inline-file secrets carry a unique `agyn.io/startup-attempt` annotation.
On a rejected startup, the runner cleans up that attempt's temporary secrets,
including failures during PVC provisioning and partial secret creation. Durable
PVCs are deliberately retained for their separate volume lifecycle.

Deploy the chart's updated Secret rule before the new runner image: cleanup
requires `get` in addition to `create` and `delete` in the workload namespace.
No Secret `list` or `watch` permission is added. Operators overriding `rbac.rules`
must update their rule explicitly; an image-only rollout is insufficient.

Cleanup uses a fresh, five-second context even if the RPC caller canceled. It
checks Pod absence, secret ownership/content and recorded UIDs, deletes with UID
and resource-version preconditions, and observes absence. A conflicting secret
is never adopted. A lost secret-create acknowledgement can be reconciled only
when the exact attempt's object is found. Conflicts, changed resources and held
deletions remain unconfirmed; cleanup never removes finalizers or retries writes.

A timeout, disconnect or server error during Pod creation may still mean a Pod
was accepted. Its secrets are retained even if a subsequent read would say
NotFound. The original gRPC failure code is preserved; unconfirmed cleanup adds
`startup_secret_cleanup_unconfirmed` to the diagnostic and logs the workload and
attempt IDs without dumping secret contents. This is not a retry authorization.

The opt-in test exercises native PVC count/storage, Secret count and Pod count
quota rejection through loopback RunnerService gRPC. It uses synthetic secrets,
a new namespace, an absent unique StorageClass and a zero-Pod quota throughout.
Runner calls impersonate a dedicated service account bound to the chart's rules
in the fixture namespace, not the operator's administrator identity. Secret list
access must remain forbidden. The operator kubeconfig must be able to create
the fixture's ServiceAccount/Role/RoleBinding and impersonate that account.
No image is pulled, no agent runs and no existing
PVC is changed. It also verifies
that an explicit second request reuses a partially created claim by UID/spec.
Cleanup refuses unknown namespace/resource ownership. Use only a trusted local
test cluster:

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

Every named `VolumeSpec` must supply a nonempty `labels.volume_key`, identifying
its durable volume record. Current Agyn orchestrator requests already do this.
Custom callers must set a stable, owner-specific key before upgrading; a key
derived from a transient Pod/workload ID would break workspace continuation.
Unkeyed named-volume requests are now rejected, even if that claim exists.

An existing claim must retain the same key, runner/orchestrator management
labels, and any agent-instance, agent-class, sandbox or sandbox-owner labels.
Per-volume labels cannot override conflicting workload ownership labels.
Per-start workload IDs and thread IDs do not participate in ownership matching.
Missing or conflicting identity returns `FailedPrecondition` before Pod creation;
the runner never adopts, relabels, renames or deletes the conflicting claim.
Legacy claims missing identity require an audited operator reconciliation, not
automatic backfilling or a new empty workspace.

Validation applies to an ordinary lookup, a successful creation response and a
fresh lookup after a competing create returns `AlreadyExists`. Uncertain API
errors are returned, not treated as absence or permission to recreate storage.
Deleting/lost claims and claims with garbage-collection owner references are
rejected. Reuse requires a filesystem, compatible single-node/single-Pod access
mode, sufficient requested capacity and a matching explicitly selected storage
class. An unspecified class retains the cluster's original choice. Larger
claims are preserved without resizing. Pending claims are permitted because
binding may require the workload to be scheduled first.

These checks do not authenticate RPC callers, fence old workloads or prevent a
privileged writer from replacing a claim after validation. `ReadWriteOnce` is
not a single-writer lock: Kubernetes permits same-node Pods to share it. See
[Kubernetes access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).
Admission controls, lifecycle fencing and secure RPC authorization remain
separate requirements. Startup Secret rollback is a separate change; deployments
using pull credentials should include that fix when enabling these rejections.

The ordinary Go suite covers mismatched/missing ownership, unchanged reuse,
validation before lookup, creation races and uncertain errors. The opt-in native
test uses the chart's service-account permissions, a unique owned namespace,
an absent storage class and a zero-Pod quota. No agent/image or existing
workspace is used. Eight real API reads are held after `404` responses before
competing creates verify that exactly one claim identity is retained:

```sh
RUNNER_LIVE_PVC_TEST=trusted-local \
RUNNER_LIVE_KUBECONFIG=/absolute/path/to/test-kubeconfig \
go test -race ./internal/server -run '^TestLivePVCOwnership$' -count=1 -timeout=4m -v
```

The operator needs namespace/RBAC creation and impersonation rights. Cleanup
checks namespace/object UIDs and refuses to remove unexpected resources or
claims that acquired backing storage. Test configuration must explicitly select
the intended disposable local cluster; it never uses a default kubeconfig.
Controller-injected Kubernetes/Istio CAs and trust-manager bundles are accepted
only with certificate-only contents. Trust-manager bundles also require the
matching controller UID, bundle label and content hash; reading that controller's
metadata requires operator permission. Other injected objects prevent cleanup.

## Docker capability notes

The `docker` capability injects a Docker sidecar. For the **rootless**
implementation, the sidecar runs nested `runc` and requires additional
permissions and mounts to allow `docker run` to work:

- `allowPrivilegeEscalation: true` for rootlesskit/newuidmap.
- `seccompProfile: Unconfined` and `appArmorProfile: Unconfined` because
  default RuntimeDefault/AppArmor profiles block mount-related syscalls
  (for example mounting `/proc`) required by nested `runc`.
- `procMount: Unmasked` to avoid `/proc` mount masking interfering with
  nested `runc` container setup.
- `pod.spec.hostUsers: false` with an init container that writes
  `/etc/subuid` and `/etc/subgid` entries inside the pod user namespace.
- HostPath mount for `/dev/net/tun` (type `CharDevice`).
- `docker-data` emptyDir mounted at `/home/rootless/.local/share` so dockerd
  can create its own `docker/` data root with correct ownership.

These settings can require Pod Security Admission exceptions for docker-capable
workloads (baseline/restricted clusters may reject them).

### Kata (microVM) docker runtimes

`CAPABILITY_IMPLEMENTATIONS` also supports `docker: kata-qemu` (and optionally
`docker: kata-fc`). When enabled, k8s-runner keeps the privileged DinD sidecar
behavior but sets `pod.spec.runtimeClassName` to match the selected Kata
implementation. The cluster must provide the matching RuntimeClass and schedule
onto KVM-capable nodes. This cannot be validated on local k3d/mac setups.

## Workload ResourceQuota

`workloadResourceQuota` adds an opt-in, namespace-wide Kubernetes admission
budget. It is disabled by default and has no implicit resource allowance.
Enabling it requires all four CPU/memory totals plus `count/pods`:

```yaml
workloadNamespace: agyn-workloads
workloadResourceQuota:
  enabled: true
  name: agent-workload-budget
  hard:
    requests.cpu: "2"
    requests.memory: "2Gi"
    limits.cpu: "4"
    limits.memory: "4Gi"
    count/pods: "8"
```

These are example values, not production sizing recommendations. The quota
applies to every Pod in `workloadNamespace`, including workloads created by
other runner instances or tools; it has no agent label selector or scope filter.
Kubernetes evaluates the assembled Pod, including the effective init/sidecar
budget, rather than trusting a main-container flavor as a complete task cost.
See [ResourceQuota](https://v1-33.docs.kubernetes.io/docs/concepts/policy/resource-quotas/)
and [sidecar accounting](https://v1-33.docs.kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#resource-sharing-within-containers).

The runner's rendered environment must contain exactly one literal
`KUBE_NAMESPACE` matching `workloadNamespace`. A missing, dynamic, duplicate or
mismatched binding fails Helm rendering. Update the existing `env` entry when
changing the workload namespace; do not add a second entry in `extraEnvVars`.
The policy does not change the runner's command, security context, RBAC or
resource allocations. Protect both the quota and runner configuration with
operator RBAC; this is not protection against a cluster administrator changing
or deleting them.

Install and verify the quota before enabling workload producers. The selected
runner and workload profile must provide CPU and memory requests and limits
for every main, init and supporting container: this policy deliberately does
not inject fallback container limits. A legacy unbounded Pod is rejected, not
silently admitted. On quota rejection Kubernetes returns `403 Forbidden`; the
existing runner maps this to gRPC `PermissionDenied` with the Kubernetes status
message. This contribution does not reclassify errors or add automatic retries.
The caller must retain durable state and distinguish a rejected start from an
ambiguous operation before attempting recovery.

CPU/memory quotas cover nonterminal Pods. `count/pods` also bounds retained Pod
objects, so failed objects cannot accumulate without consuming object capacity.
Additional native quota keys, such as `count/persistentvolumeclaims`, are passed
through; Kubernetes validates their quantities. A PVC count or requested-storage
quota is not enforcement of bytes written inside a filesystem. This is also
not a task scheduler, tenant fairness policy, node partition fence, PID/IO cap,
or proof of fail-closed platform bootstrap. Inspect existing namespace usage and
all producers before adopting a budget; no resources or retained volumes are
deleted by this chart feature.

Structured render checks use the existing Kubernetes YAML/types and run in CI:

```sh
helm dependency build charts/k8s-runner
go test ./internal/chart -count=1
```

The chart contribution is independent of A2A and agent runtimes. Live runner
admission, including supporting-container accounting and release/reuse, needs
separate acceptance with the resource-aware integration build.

### Combined local quota acceptance

This lab branch combines the separate chart and container-resource contributions;
it is not a proposed bundled upstream change. `TestLiveWorkloadQuota` renders
the real chart into its own temporary namespace and uses a real loopback gRPC
RunnerService backed by Kubernetes. It requires a digest-pinned Node image whose
default user is UID 1000. Every probe checks its UID and actual cgroup bounds.
No provider credentials, agent calls, PVCs, host mounts or stress loops are used.
The namespace has deny-all network policies; this is not a separate CNI proof.

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

The Helm chart can install the static egress NetworkPolicy used by egress v1.
Enable `workloadEgressNetworkPolicy.enabled` and set `workloadNamespace` to the
namespace where runner-created workload pods run. The policy selects workload
pods with `agyn.dev/managed-by=agents-orchestrator`, allows OpenZiti synthetic
addresses (`100.64.0.0/10`), cluster DNS, and public internet, and excludes
`workloadEgressNetworkPolicy.clusterPodCIDR`,
`workloadEgressNetworkPolicy.clusterServiceCIDR`, and
`workloadEgressNetworkPolicy.additionalInternalCIDRs` from public internet
egress. `blockedCIDRs` remains as a deprecated compatibility alias.

Ziti underlay egress is configured with first-class chart values. Enable
`zitiWorkloadDNS` to allow workload pods to reach the Ziti workload DNS pods on
TCP/UDP 53. Configure `zitiUnderlay.endpoints` for the enrollment controller,
runtime Istio ingress gateway, and router underlay endpoints returned by
`ziti-workload-dns`. Each endpoint can allow a concrete service ClusterIP `/32`,
endpoint/backend pod CIDRs through `backendCIDRs`, a namespace/pod selector,
or a combination. Runtime `ziti.<base-domain>:443` resolves to
the Istio ingress gateway so TLS passthrough can route SNI to the controller
client service. This keeps `.agyn` application traffic on the overlay while
allowing only the underlay endpoints required for sidecar startup:

```yaml
workloadEgressNetworkPolicy:
  zitiWorkloadDNS:
    enabled: true
  zitiUnderlay:
    endpoints:
      - name: controller
        cidr: "10.43.245.186/32"
        port: 2496
      - name: ingress-gateway
        cidr: "10.43.245.188/32"
        backendCIDRs:
          - "10.42.2.4/32"
        namespaceSelector:
          kubernetes.io/metadata.name: istio-system
        podSelector:
          istio: ingressgateway
        port: 443
      - name: router
        cidr: "10.43.245.187/32"
        port: 2496
```

Bootstrap should derive the underlay endpoint CIDRs from the live
`ziti-controller-client`, `istio-ingressgateway`, and `ziti-router-edge`
ClusterIPs. For the runtime ingress gateway endpoint, bootstrap should also
set endpoint pod CIDRs and the Istio ingress gateway namespace/pod selectors
when available so CNIs can follow endpoint pods instead of only the Service
ClusterIP. Set the controller and router ports to the configured
OpenZiti underlay port, and set the runtime ingress gateway port to `443`.
The deprecated `zitiControllerEnrollment` and `zitiRuntimeIngressGateway` values
remain as named compatibility helpers for deployments that prefer fixed keys.
`zitiRuntimeIngressGateway` accepts the same runtime backend CIDR and selector
fields as `zitiUnderlay.endpoints`, but new bootstrap config should use
`zitiUnderlay.endpoints` for the complete endpoint set.

The runner runtime does not create or update NetworkPolicy resources, and its
ServiceAccount does not need `networkpolicies` RBAC.

## Workload ingress isolation

Egress restrictions alone do not prevent other pods from opening connections to
a workload. For installations whose workload access uses outbound OpenZiti
connections, opt in to a separate default-deny ingress policy:

```yaml
workloadNamespace: agyn-workloads
workloadIngressNetworkPolicy:
  enabled: true
```

This policy selects `agyn.dev/managed-by=agents-orchestrator` by default. It does
not select the runner service, add runtime RBAC, or change egress. It is disabled
by default to preserve installations with direct workload ingress. Operators can
set `name` and a nonempty `podSelectorLabels` map; an empty rendered selector is
rejected to avoid accidentally isolating the entire namespace. Helm merges
selector maps with defaults; set a default key to `null` to remove that key.

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
