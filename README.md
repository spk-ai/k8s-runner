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
