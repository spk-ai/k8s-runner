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

## Failed Startup Secrets

Pull and inline-file secrets carry a unique `agyn.io/startup-attempt` annotation.
On a rejected startup, the runner cleans up that attempt's temporary secrets,
including failures during PVC provisioning and partial secret creation. Durable
PVCs are deliberately retained for their separate volume lifecycle.

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
No image is pulled, no agent runs and no existing PVC is changed. It also verifies
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
