package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

func pvcNamespaceCA(cm *corev1.ConfigMap) bool {
	if len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return false
	}
	key := "ca.crt"
	if cm.Name != "" && cm.Labels["trust.cert-manager.io/bundle"] == cm.Name {
		if len(cm.OwnerReferences) != 1 {
			return false
		}
		owner := cm.OwnerReferences[0]
		if owner.APIVersion != "trust.cert-manager.io/v1alpha1" || owner.Kind != "Bundle" || owner.Name != cm.Name || owner.UID == "" || owner.Controller == nil || !*owner.Controller {
			return false
		}
		for name, value := range cm.Data {
			key = name
			if cm.Annotations["trust.cert-manager.io/hash"] != fmt.Sprintf("%x", sha256.Sum256([]byte(value))) {
				return false
			}
		}
	} else if cm.Name == "istio-ca-root-cert" && cm.Labels["istio.io/config"] == "true" {
		key = "root-cert.pem"
	} else if cm.Name != "kube-root-ca.crt" {
		return false
	}
	remaining := bytes.TrimSpace([]byte(cm.Data[key]))
	if len(remaining) == 0 {
		return false
	}
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return false
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return false
		}
		remaining = bytes.TrimSpace(rest)
	}
	return true
}

func TestPVCNamespaceCACleanup(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	for _, name := range []string{"kube-root-ca.crt", "istio-ca-root-cert", "test-trust-bundle"} {
		key := "ca.crt"
		if name == "istio-ca-root-cert" {
			key = "root-cert.pem"
		}
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"istio.io/config": "true"}}, Data: map[string]string{key: certificate}}
		invalidCases := []string{"name", "extra-data", "binary-data", "non-certificate", "trailing-data", "missing-label"}
		if name == "test-trust-bundle" {
			controller := true
			cm.Labels = map[string]string{"trust.cert-manager.io/bundle": name}
			cm.Annotations = map[string]string{"trust.cert-manager.io/hash": fmt.Sprintf("%x", sha256.Sum256([]byte(certificate)))}
			cm.OwnerReferences = []metav1.OwnerReference{{APIVersion: "trust.cert-manager.io/v1alpha1", Kind: "Bundle", Name: name, UID: "bundle-uid", Controller: &controller}}
			invalidCases = append(invalidCases, "missing-owner", "wrong-owner", "missing-controller", "wrong-hash")
		}
		if !pvcNamespaceCA(cm) {
			t.Fatal("valid controller CA rejected")
		}
		for _, invalid := range invalidCases {
			if invalid == "missing-label" && name == "kube-root-ca.crt" {
				continue
			}
			t.Run(name+"/"+invalid, func(t *testing.T) {
				bad := cm.DeepCopy()
				switch invalid {
				case "name":
					bad.Name = "user-config"
				case "extra-data":
					bad.Data["user-data"] = "keep"
				case "binary-data":
					bad.BinaryData = map[string][]byte{"data": []byte("keep")}
				case "non-certificate":
					bad.Data[key] = "keep"
				case "trailing-data":
					bad.Data[key] += "keep"
				case "missing-label":
					bad.Labels = nil
				case "missing-owner":
					bad.OwnerReferences = nil
				case "wrong-owner":
					bad.OwnerReferences[0].Name = "other-bundle"
				case "missing-controller":
					bad.OwnerReferences[0].Controller = nil
				case "wrong-hash":
					bad.Annotations = nil
				}
				if pvcNamespaceCA(bad) {
					t.Fatal("unexpected data accepted as a controller CA")
				}
			})
		}
	}
}

type pvcRaceTransport struct {
	next    http.RoundTripper
	path    string
	arrived *atomic.Int32
	ready   chan struct{}
	count   int32
}

func (b pvcRaceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := b.next.RoundTrip(req)
	if err == nil && req.Method == http.MethodGet && req.URL.Path == b.path && resp.StatusCode == http.StatusNotFound {
		if b.arrived.Add(1) == b.count {
			close(b.ready)
		}
		select {
		case <-b.ready:
		case <-req.Context().Done():
			resp.Body.Close()
			return nil, req.Context().Err()
		}
	}
	return resp, err
}

// Native claims stay unbound in a unique namespace, with no Pods permitted.
func TestLivePVCOwnership(t *testing.T) {
	if os.Getenv("RUNNER_LIVE_PVC_TEST") != "trusted-local" {
		t.Skip("requires explicit trusted-local Kubernetes PVC acceptance")
	}
	kubeconfig := os.Getenv("RUNNER_LIVE_KUBECONFIG")
	if !filepath.IsAbs(kubeconfig) {
		t.Fatal("RUNNER_LIVE_KUBECONFIG must be an explicit absolute path")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 15 * time.Second
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	run := uuid.NewString()
	const ownerLabel = "agyn.io/pvc-ownership-test"
	namespace, storageClass := "runner-pvc-"+run[:12], "unprovisioned-"+run
	if _, err := kube.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("test storage class must not exist")
	}
	resourceFor := func(group, name string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: group, Version: "v1", Resource: name}
	}
	owned := map[schema.GroupVersionResource]map[string]types.UID{}
	for _, name := range []string{"pods", "services", "secrets", "configmaps", "serviceaccounts", "persistentvolumeclaims", "resourcequotas"} {
		owned[resourceFor("", name)] = map[string]types.UID{}
	}
	for _, name := range []string{"roles", "rolebindings"} {
		owned[resourceFor(rbacv1.GroupName, name)] = map[string]types.UID{}
	}
	labels := map[string]string{ownerLabel: run}
	var namespaceUID types.UID
	attempted := false
	t.Cleanup(func() {
		if !attempted {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		ns, err := kube.CoreV1().Namespaces().Get(cleanup, namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil || ns.Labels[ownerLabel] != run || namespaceUID != "" && ns.UID != namespaceUID {
			t.Errorf("namespace ownership unconfirmed; cleanup refused: %v", err)
			return
		}
		for gvr, names := range owned {
			items, err := meta.Resource(gvr).Namespace(namespace).List(cleanup, metav1.ListOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			for _, item := range items.Items {
				if gvr.Resource == "serviceaccounts" && item.Name == "default" {
					continue
				}
				if gvr.Resource == "configmaps" {
					cm, err := kube.CoreV1().ConfigMaps(namespace).Get(cleanup, item.Name, metav1.GetOptions{})
					if err == nil && cm.UID == item.UID && pvcNamespaceCA(cm) {
						if cm.Labels["trust.cert-manager.io/bundle"] == cm.Name {
							bundle, err := meta.Resource(schema.GroupVersionResource{Group: "trust.cert-manager.io", Version: "v1alpha1", Resource: "bundles"}).Get(cleanup, cm.Name, metav1.GetOptions{})
							if err != nil || bundle.UID != cm.OwnerReferences[0].UID {
								t.Error("trust bundle controller identity unconfirmed; cleanup refused")
								return
							}
						}
						continue
					}
					t.Error("controller CA identity/content unconfirmed; cleanup refused")
					return
				}
				uid, planned := names[item.Name]
				if !planned || uid != "" && uid != item.UID || item.Labels[ownerLabel] != run {
					t.Errorf("unexpected/replaced %s %s; cleanup refused", gvr.Resource, item.Name)
					return
				}
			}
		}
		claims, err := kube.CoreV1().PersistentVolumeClaims(namespace).List(cleanup, metav1.ListOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		for _, claim := range claims.Items {
			if claim.Spec.VolumeName != "" || claim.Status.Phase == corev1.ClaimBound || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass {
				t.Error("a claim acquired backing storage; cleanup refused")
				return
			}
		}
		if err := kube.CoreV1().Namespaces().Delete(cleanup, namespace, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ns.UID, ResourceVersion: &ns.ResourceVersion}}); err != nil {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextTimeout(cleanup, time.Second, 50*time.Second, true, func(ctx context.Context) (bool, error) {
			current, err := kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err == nil && current.UID != ns.UID {
				return false, fmt.Errorf("namespace replaced during cleanup")
			}
			return false, err
		}); err != nil {
			t.Error(err)
		} else {
			t.Logf("cleanup confirmed namespace=%s uid=%s absent; no existing workspace or agent touched", namespace, ns.UID)
		}
	})
	attempted = true
	ns, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			attempted = false
		}
		t.Fatal(err)
	}
	namespaceUID = ns.UID
	t.Logf("PVC ownership run=%s namespace=%s uid=%s", run, namespace, ns.UID)
	owned[resourceFor("", "serviceaccounts")]["pvc-runner"] = ""
	account, err := kube.CoreV1().ServiceAccounts(namespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pvc-runner", Labels: labels}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned[resourceFor("", "serviceaccounts")][account.Name] = account.UID
	data, err := os.ReadFile("../../charts/k8s-runner/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chart struct {
		RBAC struct {
			Rules []rbacv1.PolicyRule `json:"rules"`
		} `json:"rbac"`
	}
	if err := yaml.Unmarshal(data, &chart); err != nil || len(chart.RBAC.Rules) == 0 {
		t.Fatalf("chart RBAC: %v", err)
	}
	owned[resourceFor(rbacv1.GroupName, "roles")]["pvc-runner"] = ""
	role, err := kube.RbacV1().Roles(namespace).Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "pvc-runner", Labels: labels}, Rules: chart.RBAC.Rules}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned[resourceFor(rbacv1.GroupName, "roles")][role.Name] = role.UID
	owned[resourceFor(rbacv1.GroupName, "rolebindings")]["pvc-runner"] = ""
	binding, err := kube.RbacV1().RoleBindings(namespace).Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "pvc-runner", Labels: labels},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: namespace, Name: account.Name}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned[resourceFor(rbacv1.GroupName, "rolebindings")][binding.Name] = binding.UID
	runnerConfig := rest.CopyConfig(cfg)
	runnerConfig.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + namespace + ":" + account.Name,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + namespace, "system:authenticated"}}
	runnerKube, err := kubernetes.NewForConfig(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runnerKube.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatal("runner unexpectedly has Secret list permission")
	}
	owned[resourceFor("", "resourcequotas")]["no-pods"] = ""
	quota, err := kube.CoreV1().ResourceQuotas(namespace).Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "no-pods", Labels: labels},
		// Quota admission can temporarily charge concurrent creates before they
		// reach the name conflict. Leave room for all attempts plus the seed PVC.
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{"count/pods": resource.MustParse("0"), "persistentvolumeclaims": resource.MustParse("16"), "requests.storage": resource.MustParse("16Mi")}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned[resourceFor("", "resourcequotas")][quota.Name] = quota.UID
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := kube.CoreV1().ResourceQuotas(namespace).Get(ctx, quota.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		value, present := current.Status.Hard["count/pods"]
		return present && value.IsZero(), nil
	}); err != nil {
		t.Fatal(err)
	}
	server := New(Options{Clientset: runnerKube, Namespace: namespace, StorageClass: &storageClass, StorageSize: "1Mi", Logger: zap.NewNop()})
	request := func(name, owner string) *runnerv1.StartWorkloadRequest {
		return &runnerv1.StartWorkloadRequest{WorkloadId: uuid.NewString(), Main: &runnerv1.ContainerSpec{Name: "main", Image: "never-run.invalid/test:0"},
			Labels: map[string]string{ownerLabel: run, "agent-id": "test-agent", "agent-instance-id": owner},
			Volumes: []*runnerv1.VolumeSpec{{Name: "workspace", PersistentName: name, Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED,
				Size: "1Mi", Labels: map[string]string{volumeKeyLabelKey: name + "-" + owner}}}}
	}
	ensure := func(server *Server, req *runnerv1.StartWorkloadRequest) (string, error) {
		labels, err := buildLabels(req.WorkloadId, req.AdditionalProperties, req.Labels)
		if err != nil {
			return "", err
		}
		return server.ensurePVC(ctx, req.Volumes[0], labels)
	}
	ownedClaims := owned[resourceFor("", "persistentvolumeclaims")]
	ownedClaims["workspace"] = ""
	req := request("workspace", "task-a")
	if _, err := ensure(server, req); err != nil {
		t.Fatal(err)
	}
	claim, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, "workspace", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ownedClaims[claim.Name] = claim.UID
	for _, name := range []string{"same-owner", "cross-task", "missing-key", "omitted-owner", "class-owner-change"} {
		t.Run(name, func(t *testing.T) {
			next := request("workspace", "task-a")
			next.Labels[workloadKeyLabelKey], next.Labels["thread-id"] = uuid.NewString(), uuid.NewString()
			want := codes.FailedPrecondition
			switch name {
			case "same-owner":
				want = codes.PermissionDenied
			case "cross-task":
				next = request("workspace", "task-b")
			case "missing-key":
				next.Volumes[0].Labels = nil
				want = codes.InvalidArgument
			case "omitted-owner":
				delete(next.Labels, "agent-instance-id")
			case "class-owner-change":
				next.Labels["agent-id"] = "other-agent"
			}
			_, err := server.StartWorkload(ctx, next)
			if status.Code(err) != want {
				t.Fatalf("expected %s, got %v", want, err)
			}
			if name == "same-owner" && !strings.Contains(status.Convert(err).Message(), "exceeded quota") {
				t.Fatalf("claim reuse did not reach zero-Pod quota: %v", err)
			}
			current, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if err != nil || current.UID != claim.UID || !reflect.DeepEqual(current.Spec, claim.Spec) || !reflect.DeepEqual(current.Labels, claim.Labels) {
				t.Fatalf("existing PVC identity/spec changed: %v", err)
			}
		})
	}
	t.Run("checked-removal", func(t *testing.T) {
		testLiveCheckedVolumeRemoval(t, ctx, kube, runnerConfig, server, ownerLabel, run, ownedClaims)
	})
	t.Run("simultaneous-create", func(t *testing.T) {
		const contenders = 8
		ownedClaims["racing"] = ""
		var arrived atomic.Int32
		ready := make(chan struct{})
		raceConfig := rest.CopyConfig(runnerConfig)
		raceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
			return pvcRaceTransport{next, "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/racing", &arrived, ready, contenders}
		})
		raceKube, err := kubernetes.NewForConfig(raceConfig)
		if err != nil {
			t.Fatal(err)
		}
		racer := New(Options{Clientset: raceKube, Namespace: namespace, StorageClass: &storageClass, StorageSize: "1Mi", Logger: zap.NewNop()})
		type outcome struct {
			owner, name string
			err         error
		}
		results := make(chan outcome, contenders)
		for i := 0; i < contenders; i++ {
			owner := fmt.Sprintf("task-%d", i%2)
			go func() { name, err := ensure(racer, request("racing", owner)); results <- outcome{owner, name, err} }()
		}
		outcomes := make([]outcome, 0, contenders)
		for i := 0; i < contenders; i++ {
			outcomes = append(outcomes, <-results)
		}
		current, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, "racing", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ownedClaims[current.Name] = current.UID
		if arrived.Load() != contenders {
			t.Fatalf("only %d real NotFound responses overlapped", arrived.Load())
		}
		winner := current.Labels["agent-instance-id"]
		successes := 0
		foreignDenied := 0
		for _, result := range outcomes {
			if result.owner == winner {
				if result.err != nil || result.name != current.Name {
					t.Errorf("same-owner contender rejected: %v", result.err)
				} else {
					successes++
				}
			} else if status.Code(result.err) != codes.FailedPrecondition {
				t.Errorf("foreign contender was not rejected: %v", result.err)
			} else {
				foreignDenied++
			}
		}
		if successes != contenders/2 || foreignDenied != contenders/2 {
			t.Fatalf("unexpected contention outcomes: same-owner successes=%d foreign denials=%d", successes, foreignDenied)
		}
		t.Logf("eight real GET/404 responses preceded competing creates; one PVC uid=%s retained, four matching owners accepted, four foreign owners denied", current.UID)
	})
}
