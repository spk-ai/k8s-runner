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
