package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

const sweepGrace = 10 * time.Minute

var sweepNow = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

// The sweep lists metadata only, so its fixture is a metadata store.
func newSweepMetadata(t *testing.T, secrets ...*metav1.PartialObjectMetadata) *metadatafake.FakeMetadataClient {
	t.Helper()
	scheme := metadatafake.NewTestScheme()
	if err := metav1.AddMetaToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := make([]runtime.Object, 0, len(secrets))
	for _, secret := range secrets {
		objects = append(objects, secret)
	}
	return metadatafake.NewSimpleMetadataClient(scheme, objects...)
}

// The fake metadata client drops DeleteOptions. Like the identity clientset,
// this wrapper requires and enforces the identity preconditions itself.
type preconditionMetadata struct {
	*metadatafake.FakeMetadataClient
	t *testing.T
}

func (m preconditionMetadata) Resource(resource schema.GroupVersionResource) metadata.Getter {
	return preconditionGetter{m.FakeMetadataClient.Resource(resource), m, resource}
}

type preconditionGetter struct {
	metadata.Getter
	store    preconditionMetadata
	resource schema.GroupVersionResource
}

func (g preconditionGetter) Namespace(namespace string) metadata.ResourceInterface {
	return preconditionResource{g.Getter.Namespace(namespace), g.store, g.resource, namespace}
}

type preconditionResource struct {
	metadata.ResourceInterface
	store     preconditionMetadata
	resource  schema.GroupVersionResource
	namespace string
}

func (r preconditionResource) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	p := opts.Preconditions
	if p == nil || p.UID == nil || p.ResourceVersion == nil {
		r.store.t.Error("sweep deleted a Secret without UID and resource version preconditions")
		return errors.New("fixture requires preconditions")
	}
	obj, err := r.store.Tracker().Get(r.resource, r.namespace, name)
	if err != nil {
		return err
	}
	meta := obj.(metav1.Object)
	if *p.UID != meta.GetUID() || *p.ResourceVersion != meta.GetResourceVersion() {
		return apierrors.NewConflict(corev1.Resource("secrets"), name, errors.New("fixture identity conflict"))
	}
	return r.ResourceInterface.Delete(ctx, name, opts, subresources...)
}

func sweepSecret(workloadID, suffix string, age time.Duration) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name: podNameFromID(workloadID) + suffix, Namespace: "default", UID: types.UID(uuid.NewString()), ResourceVersion: "7",
			CreationTimestamp: metav1.NewTime(sweepNow.Add(-age)),
			Labels:            map[string]string{managedByLabelKey: managedByLabelValue, workloadIDLabelKey: workloadID},
			Annotations:       map[string]string{startupAttemptAnnotation: uuid.NewString()},
		},
	}
}

// mirror projects Secrets the start path wrote through the typed client into
// the metadata view the sweep reads, with a chosen age. It reads the tracker
// directly so it can run while a reactor holds the fake client.
func mirror(t *testing.T, client *fake.Clientset, age time.Duration) []*metav1.PartialObjectMetadata {
	t.Helper()
	obj, err := client.Tracker().List(secretsResource, corev1.SchemeGroupVersion.WithKind("Secret"), "default")
	if err != nil {
		t.Fatal(err)
	}
	list := obj.(*corev1.SecretList)
	result := make([]*metav1.PartialObjectMetadata, 0, len(list.Items))
	for _, secret := range list.Items {
		meta := secret.ObjectMeta.DeepCopy()
		meta.CreationTimestamp = metav1.NewTime(sweepNow.Add(-age))
		result = append(result, &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: *meta})
	}
	return result
}

func remainingSweepSecrets(t *testing.T, client *metadatafake.FakeMetadataClient) []string {
	t.Helper()
	list, err := client.Resource(secretsResource).Namespace("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, item := range list.Items {
		names = append(names, item.Name)
	}
	return names
}

func sweepServer(t *testing.T, client *fake.Clientset, meta *metadatafake.FakeMetadataClient) (*Server, *observer.ObservedLogs) {
	core, logs := observer.New(zap.DebugLevel)
	server := New(Options{Clientset: client, Metadata: preconditionMetadata{meta, t}, Namespace: "default", StorageSize: "1Gi", Logger: zap.New(core)})
	return server, logs
}

func addPod(t *testing.T, client *fake.Clientset, workloadID string) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podNameFromID(workloadID), Namespace: "default", UID: types.UID(uuid.NewString())}}
	if err := client.Tracker().Add(pod); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyStartOwnsCredentialsByCreatedPod(t *testing.T) {
	server, client, req := startupFixture(t)
	if _, err := server.StartWorkload(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), podNameFromID(req.WorkloadId), metav1.GetOptions{})
	if err != nil || pod.UID == "" {
		t.Fatalf("pod: %v", err)
	}
	want := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}}
	secrets, err := client.CoreV1().Secrets("default").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(secrets.Items) != 2 {
		t.Fatalf("secrets = %d, %v", len(secrets.Items), err)
	}
	for _, secret := range secrets.Items {
		if !reflect.DeepEqual(secret.OwnerReferences, want) {
			t.Fatalf("secret %s owners = %+v, want the created Pod", secret.Name, secret.OwnerReferences)
		}
	}
	patches := 0
	for _, action := range client.Actions() {
		if !action.Matches("patch", "secrets") {
			continue
		}
		patches++
		patch := action.(clienttesting.PatchAction)
		var ops []map[string]any
		if patch.GetPatchType() != types.JSONPatchType || json.Unmarshal(patch.GetPatch(), &ops) != nil || len(ops) != 3 ||
			ops[0]["op"] != "test" || ops[0]["path"] != "/metadata/uid" || ops[0]["value"] == "" ||
			ops[1]["op"] != "test" || ops[1]["path"] != "/metadata/resourceVersion" || ops[1]["value"] != "1" ||
			ops[2]["op"] != "add" || ops[2]["path"] != "/metadata/ownerReferences" {
			t.Fatalf("owner patch is not conditioned on the created incarnation: %s", patch.GetPatch())
		}
	}
	if patches != 2 {
		t.Fatalf("owner patches = %d, want 2", patches)
	}
}

func TestLegacyOwnerNeverClaimsAReplacementSecret(t *testing.T) {
	server, client, req := startupFixture(t)
	pull := podNameFromID(req.WorkloadId) + "-pull"
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		// Between the Secret CREATE and the owner patch the original is replaced.
		obj, err := client.Tracker().Get(secretsResource, "default", pull)
		if err != nil {
			t.Fatal(err)
		}
		replacement := obj.(*corev1.Secret).DeepCopy()
		replacement.UID = types.UID(uuid.NewString())
		if err := client.Tracker().Update(secretsResource, replacement, "default"); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	core, logs := observer.New(zap.DebugLevel)
	server.logger = zap.New(core)
	if _, err := server.StartWorkload(context.Background(), req); err != nil {
		t.Fatalf("a created Pod must not be reported as a failed start: %v", err)
	}
	obj, err := client.Tracker().Get(secretsResource, "default", pull)
	if err != nil || len(obj.(*corev1.Secret).OwnerReferences) != 0 {
		t.Fatalf("replacement Secret was claimed: %v", err)
	}
	inline, err := client.CoreV1().Secrets("default").Get(context.Background(), podNameFromID(req.WorkloadId)+"-inline-files", metav1.GetOptions{})
	if err != nil || len(inline.OwnerReferences) != 1 {
		t.Fatalf("unchanged Secret lost its owner: %v", err)
	}
	failures := logs.FilterMessage("startup secret owner not attached").All()
	if len(failures) != 1 || failures[0].ContextMap()["workload_id"] != req.WorkloadId || !strings.Contains(fmt.Sprint(failures[0].ContextMap()["error"]), pull) {
		t.Fatalf("owner failure was not reported with the workload and Secret: %+v", logs.All())
	}
	data, _ := json.Marshal(logs.All())
	if strings.Contains(string(data), "not-a-credential") || strings.Contains(string(data), "fixture inline data") {
		t.Fatal("owner diagnostics exposed secret data")
	}
}

func TestLegacyOwnerFailureKeepsStartAndCredentials(t *testing.T) {
	server, client, req := startupFixture(t)
	client.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), "fixture", errors.New("patch denied"))
	})
	if _, err := server.StartWorkload(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	startupObjects(t, client, 1, 2, 1)
	for _, action := range client.Actions() {
		if action.Matches("delete", "secrets") {
			t.Fatal("a running Pod's credentials were removed")
		}
	}
}

// Ownership and Stop/Remove credential removal happen after the Pod write was
// accepted, so a caller giving up at that point must not skip them.
func TestCredentialReleaseOutlivesCallerCancellation(t *testing.T) {
	server, client, req := startupFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checked := map[string]int{}
	server.clientset = startupContextClient{client, func(call context.Context) {
		deadline, ok := call.Deadline()
		if call.Err() != nil || !ok || time.Until(deadline) > 5*time.Second {
			t.Fatal("credential release inherited cancellation or has no bounded deadline")
		}
	}}
	client.PrependReactor("*", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		checked[action.GetVerb()]++
		return false, nil, nil
	})
	client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		cancel()
		return false, nil, nil
	})
	if _, err := server.StartWorkload(ctx, req); err != nil {
		t.Fatal(err)
	}
	if checked["patch"] != 2 {
		t.Fatalf("owner patches after cancellation = %d, want 2", checked["patch"])
	}
	stopCtx, stop := context.WithCancel(context.Background())
	defer stop()
	client.PrependReactor("delete", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		stop()
		return false, nil, nil
	})
	if _, err := server.StopWorkload(stopCtx, &runnerv1.StopWorkloadRequest{WorkloadId: req.WorkloadId}); err != nil {
		t.Fatal(err)
	}
	startupObjects(t, client, 0, 0, 1)
}

// End to end against the two fakes: the owner patch is refused, an outside
// actor deletes the Pod, and only the sweep can release the credentials.
func TestSweepReleasesCredentialsOfExternallyDeletedPod(t *testing.T) {
	_, client, req := startupFixture(t)
	client.PrependReactor("patch", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), "fixture", errors.New("patch denied"))
	})
	meta := newSweepMetadata(t)
	server, logs := sweepServer(t, client, meta)
	if _, err := server.StartWorkload(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, secret := range mirror(t, client, 2*sweepGrace) {
		if err := meta.Tracker().Add(secret); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
		t.Fatalf("sweep with a live Pod: deleted=%d err=%v", deleted, err)
	}
	if err := client.CoreV1().Pods("default").Delete(context.Background(), podNameFromID(req.WorkloadId), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 2 || err != nil {
		t.Fatalf("sweep after external deletion: deleted=%d err=%v", deleted, err)
	}
	if remaining := remainingSweepSecrets(t, meta); len(remaining) != 0 {
		t.Fatalf("orphans remain: %v", remaining)
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
		t.Fatalf("repeated sweep: deleted=%d err=%v", deleted, err)
	}
	released := logs.FilterMessage("deleted orphaned workload secret").All()
	if len(released) != 2 {
		t.Fatalf("deletions logged = %d, want 2", len(released))
	}
	for _, entry := range released {
		fields := entry.ContextMap()
		if len(fields) != 2 || fields["workload_id"] != req.WorkloadId || !strings.HasPrefix(fmt.Sprint(fields["secret"]), podNameFromID(req.WorkloadId)) {
			t.Fatalf("deletion log must name only the workload and Secret: %+v", fields)
		}
	}
	data, _ := json.Marshal(logs.All())
	if strings.Contains(string(data), "not-a-credential") || strings.Contains(string(data), "fixture inline data") {
		t.Fatal("sweep diagnostics exposed secret data")
	}
}

func TestSweepDeletesOnlyConfirmedOrphans(t *testing.T) {
	orphan := uuid.NewString()
	alive := uuid.NewString()
	indexed := uuid.NewString()
	keep := map[string]*metav1.PartialObjectMetadata{}
	add := func(name string, secret *metav1.PartialObjectMetadata, mutate func(*metav1.PartialObjectMetadata)) {
		if mutate != nil {
			mutate(secret)
		}
		keep[name] = secret
	}
	add("pod-present", sweepSecret(alive, "-pull", 2*sweepGrace), nil)
	add("young", sweepSecret(uuid.NewString(), "-pull", sweepGrace-time.Second), nil)
	add("no-timestamp", sweepSecret(uuid.NewString(), "-pull", 0), func(s *metav1.PartialObjectMetadata) { s.CreationTimestamp = metav1.Time{} })
	add("unmanaged", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { delete(s.Labels, managedByLabelKey) })
	add("foreign-manager", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { s.Labels[managedByLabelKey] = "helm" })
	add("no-workload", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { delete(s.Labels, workloadIDLabelKey) })
	add("invalid-workload", sweepSecret("not-a-uuid", "-pull", 2*sweepGrace), nil)
	add("mismatched-name", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { s.Labels[workloadIDLabelKey] = uuid.NewString() })
	add("foreign-name", sweepSecret(uuid.NewString(), "-config", 2*sweepGrace), nil)
	add("padded-index", sweepSecret(uuid.NewString(), "-pull-01", 2*sweepGrace), nil)
	add("no-attempt", sweepSecret(uuid.NewString(), "-inline-files", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { delete(s.Annotations, startupAttemptAnnotation) })
	add("owned", sweepSecret(uuid.NewString(), "-inline-files", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) {
		s.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "gone", UID: types.UID(uuid.NewString())}}
	})
	add("finalized", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { s.Finalizers = []string{"example.com/hold"} })
	add("terminating", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) {
		now := metav1.NewTime(sweepNow)
		s.DeletionTimestamp = &now
	})
	add("prepared", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { s.Annotations[preparedBindingAnnotation] = "{}" })
	add("anchored", sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace), func(s *metav1.PartialObjectMetadata) { s.Annotations[resourceAnchorAnnotation] = "{}" })
	orphans := []*metav1.PartialObjectMetadata{
		sweepSecret(orphan, "-pull", 2*sweepGrace), sweepSecret(orphan, "-inline-files", sweepGrace),
		sweepSecret(indexed, "-pull-0", 2*sweepGrace), sweepSecret(indexed, "-pull-12", 2*sweepGrace),
	}
	all := append([]*metav1.PartialObjectMetadata{}, orphans...)
	for _, secret := range keep {
		all = append(all, secret)
	}
	client := fake.NewSimpleClientset()
	addPod(t, client, alive)
	meta := newSweepMetadata(t, all...)
	server, _ := sweepServer(t, client, meta)
	// Judged directly too, so the checks do not rely on the list selector.
	foreign := sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace)
	foreign.Namespace = "other"
	keep["foreign-namespace"] = foreign
	for name, secret := range keep {
		if _, ok := server.orphanCandidate(secret, sweepGrace, sweepNow); ok != (name == "pod-present") {
			t.Fatalf("%s: candidate = %t", name, ok)
		}
	}
	delete(keep, "foreign-namespace")
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != len(orphans) || err != nil {
		t.Fatalf("deleted=%d err=%v, want %d confirmed orphans", deleted, err, len(orphans))
	}
	remaining := remainingSweepSecrets(t, meta)
	if len(remaining) != len(keep) {
		t.Fatalf("remaining = %v, want every retained case", remaining)
	}
	for name, secret := range keep {
		if _, err := meta.Tracker().Get(secretsResource, "default", secret.Name); err != nil {
			t.Fatalf("%s was deleted: %v", name, err)
		}
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Fatalf("sweep read credential content through the typed client: %s", action.GetVerb())
		}
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
		t.Fatalf("second pass deleted=%d err=%v", deleted, err)
	}
}

func TestSweepRetainsWhenPodAbsenceIsUnconfirmed(t *testing.T) {
	workloadID := uuid.NewString()
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, io.ErrUnexpectedEOF
	})
	meta := newSweepMetadata(t, sweepSecret(workloadID, "-pull", 2*sweepGrace))
	server, _ := sweepServer(t, client, meta)
	deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow)
	if deleted != 0 || err == nil || !strings.Contains(err.Error(), "absence unconfirmed") {
		t.Fatalf("deleted=%d err=%v, want retention with an unconfirmed-absence error", deleted, err)
	}
	if remaining := remainingSweepSecrets(t, meta); len(remaining) != 1 {
		t.Fatalf("remaining = %v", remaining)
	}
}

func TestSweepRetainsSecretChangedAfterListing(t *testing.T) {
	workloadID := uuid.NewString()
	secret := sweepSecret(workloadID, "-pull", 2*sweepGrace)
	client := fake.NewSimpleClientset()
	meta := newSweepMetadata(t, secret)
	client.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		// A delayed owner patch lands between the listing and the deletion.
		changed := secret.DeepCopy()
		changed.ResourceVersion = "8"
		changed.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: podNameFromID(workloadID), UID: "late"}}
		if err := meta.Tracker().Update(secretsResource, changed, "default"); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	server, _ := sweepServer(t, client, meta)
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
		t.Fatalf("deleted=%d err=%v; a changed Secret must be retained quietly", deleted, err)
	}
	if remaining := remainingSweepSecrets(t, meta); len(remaining) != 1 {
		t.Fatalf("remaining = %v", remaining)
	}
}

func TestSweepPassIsBounded(t *testing.T) {
	secrets := make([]*metav1.PartialObjectMetadata, 0, secretSweepMaxDeletions+5)
	for range secretSweepMaxDeletions + 5 {
		secrets = append(secrets, sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace))
	}
	meta := newSweepMetadata(t, secrets...)
	server, _ := sweepServer(t, fake.NewSimpleClientset(), meta)
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != secretSweepMaxDeletions || err != nil {
		t.Fatalf("first pass deleted=%d err=%v", deleted, err)
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 5 || err != nil {
		t.Fatalf("second pass deleted=%d err=%v", deleted, err)
	}
}

func TestSweepListFailureDeletesNothing(t *testing.T) {
	meta := newSweepMetadata(t, sweepSecret(uuid.NewString(), "-pull", 2*sweepGrace))
	meta.PrependReactor("list", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), "", errors.New("list not granted"))
	})
	server, _ := sweepServer(t, fake.NewSimpleClientset(), meta)
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err == nil || !strings.Contains(err.Error(), "list not granted") {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if _, err := (&Server{namespace: "default", logger: zap.NewNop()}).sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); err == nil {
		t.Fatal("sweep without a metadata client must fail closed")
	}
}

// A start in flight may have written credentials whose Pod CREATE has not
// returned; an uncertain CREATE may still be accepted. Neither is evidence of
// absence until the start has ended and the grace period has passed.
func TestSweepNeverJudgesAnInFlightStart(t *testing.T) {
	_, client, req := startupFixture(t)
	meta := newSweepMetadata(t)
	server, _ := sweepServer(t, client, meta)
	entered, release := make(chan struct{}), make(chan struct{})
	// Blocked outside the fake's reactor lock, so the sweep can still read Pods.
	server.clientset = blockingPodCreate{client, entered, release}
	var wg sync.WaitGroup
	var startErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, startErr = server.StartWorkload(context.Background(), req)
	}()
	<-entered
	for _, secret := range mirror(t, client, 2*sweepGrace) {
		if err := meta.Tracker().Add(secret); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
			t.Fatalf("sweep judged an in-flight start: deleted=%d err=%v", deleted, err)
		}
	}
	close(release)
	wg.Wait()
	if status.Code(startErr) != codes.Internal {
		t.Fatalf("start = %v, want the uncertain Pod CREATE", startErr)
	}
	startupObjects(t, client, 0, 2, 1)
	// The start has ended, but its credentials are younger than the grace.
	young := mirror(t, client, sweepGrace/2)
	for _, secret := range young {
		if err := meta.Tracker().Update(secretsResource, secret, "default"); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow); deleted != 0 || err != nil {
		t.Fatalf("sweep inside the grace period: deleted=%d err=%v", deleted, err)
	}
	if deleted, err := server.sweepOrphanedSecrets(context.Background(), sweepGrace, sweepNow.Add(sweepGrace)); deleted != 2 || err != nil {
		t.Fatalf("sweep after the grace period with the Pod confirmed absent: deleted=%d err=%v", deleted, err)
	}
}

type blockingPodCreate struct {
	kubernetes.Interface
	entered, release chan struct{}
}

func (c blockingPodCreate) CoreV1() typedcore.CoreV1Interface {
	return blockingPodCore{c.Interface.CoreV1(), c}
}

type blockingPodCore struct {
	typedcore.CoreV1Interface
	block blockingPodCreate
}

func (c blockingPodCore) Pods(namespace string) typedcore.PodInterface {
	return blockingPods{c.CoreV1Interface.Pods(namespace), c.block}
}

type blockingPods struct {
	typedcore.PodInterface
	block blockingPodCreate
}

func (p blockingPods) Create(context.Context, *corev1.Pod, metav1.CreateOptions) (*corev1.Pod, error) {
	close(p.block.entered)
	<-p.block.release
	return nil, io.ErrUnexpectedEOF
}

func TestRunSecretSweepStartsImmediatelyAndStops(t *testing.T) {
	meta := newSweepMetadata(t)
	listed := make(chan struct{}, 4)
	meta.PrependReactor("list", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		select {
		case listed <- struct{}{}:
		default:
		}
		return false, nil, nil
	})
	server, _ := sweepServer(t, fake.NewSimpleClientset(), meta)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { server.RunSecretSweep(ctx, time.Hour, sweepGrace); close(done) }()
	select {
	case <-listed:
	case <-time.After(5 * time.Second):
		t.Fatal("no sweep pass at startup")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep loop ignored cancellation")
	}
}
