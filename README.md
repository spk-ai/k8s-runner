# k8s-runner

k8s-runner is the Kubernetes-native implementation of the RunnerService gRPC API.

Architecture: [k8s-runner](https://github.com/agynio/architecture/blob/main/architecture/k8s-runner.md)

## Volume Inventory Integrity

`ListVolumes` returns `FailedPrecondition` without a partial response if any
runner-managed PVC has a missing, empty, whitespace-padded or duplicate
`volume_key`. A successful inventory is used by reconcilers as evidence that
unlisted volumes are absent, so silently omitting malformed managed claims can
incorrectly close a retained workspace's record. Unmanaged PVCs remain excluded.

This changes the former skip-missing-key behavior. A damaged inventory requires
operator ownership reconciliation before volume reconciliation can proceed;
the runner does not adopt, relabel or delete claims to repair it. This is not
deletion authorization or node/late-create fencing. Valid orphan inventory still needs a controller
that does not infer deletion permission from a stale or scoped registry scan.

After generating the APIs, run `go test -race ./...`. Focused tests are
`go test -race ./internal/server -run '^TestListVolumes' -count=1`.

## Volume Backend Identity

This dependent proposal requires the backend-identity API extension. Inventory
and checked removal now identify the storage scope as
`kubernetes-namespace/v1/<namespace-name>/<namespace-uid>`. The UID comes from
the Kubernetes API, never configuration, a caller header or the expected target.
[Kubernetes object IDs](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids)
distinguish a namespace from another incarnation using the same name.

The runner reads that identity before and after inventory/deletion observations.
A missing, terminating, inaccessible or replaced namespace cannot establish PVC
absence. Existing PVC UID/resource-version deletion preconditions remain in use.
Ordinary namespace metadata updates and runner restarts preserve the identity.
Both inventory envelopes and removal responses carry the observed identity;
the controller/registry must pin and validate it, including empty inventory.

When `rbac.create=true`, the chart adds a separate ClusterRole/Binding granting
only `get` on the named `workloadNamespace` object. It grants no namespace list,
write or other-namespace access. `workloadNamespace` must match `KUBE_NAMESPACE`;
when managing RBAC externally, install the same scoped permission. A namespace
Role alone cannot grant access to the cluster-scoped namespace object. See
[Kubernetes named-resource RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources).

All 185 independent race tests, including the Helm rendering check, pass; build
and vet pass. Wrong/unavailable namespaces, inventory/absence races and runner
restart are covered. A separate controller/registry acceptance uses real
Kubernetes and PostgreSQL. This branch alone does not authenticate the runner
route, bind workload-start requests, fence delayed operations, protect cloned
cluster identities or perform a rollout/adoption. It is not a drop-in upgrade.

## Checked Volume Removal

This branch requires the proposed checked-volume API. `ListVolumes` additionally
returns each PVC UID and only persistent identity labels; workload/turn labels
and unrelated metadata are not included in that identity.

`RemoveVolumeChecked` validates the durable expected name, key, UID and complete
persistent ownership labels against a fresh GET, then uses that UID and the GET's
resource version as Kubernetes delete preconditions. It refuses missing identity,
foreign/replacement claims and claims with owner references. Conflicts are not
retried against a different target. A terminating claim or DELETE acknowledgement
returns `PENDING`; only GET/NotFound returns `ABSENT`. It never clears finalizers.

Legacy `RemoveVolume` returns `FailedPrecondition`. So does
`RemoveWorkload(remove_volumes=true)`, before touching a Pod, Secret or PVC.
There is no compatibility escape flag. Ordinary workload removal retains disks.
Deploy only after coordinating the API, durable registry intents and every
orchestrator/sandbox cleanup caller. This branch is not a stock-image drop-in.

Unit tests assert both delete preconditions, replacement/ownership conflicts,
finalizer handling, backend failures and bypass rejection. The Kubernetes fake
does not itself enforce preconditions; native acceptance is required separately.
Caller authentication, durable intent storage, all-writer/late-create and
node/storage fencing are not implemented by this runner change alone.

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
