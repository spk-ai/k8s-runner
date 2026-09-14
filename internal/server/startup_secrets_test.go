package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	clienttesting "k8s.io/client-go/testing"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

// The simple client does not assign API identities or enforce deletion
// preconditions. Model both so cleanup cannot pass by deleting by name alone.
func newIdentityClientset() *fake.Clientset {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		meta := action.(clienttesting.CreateAction).GetObject().(metav1.Object)
		meta.SetUID(types.UID(uuid.NewString()))
		meta.SetResourceVersion("1")
		return false, nil, nil
	})
	client.PrependReactor("delete", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletion := action.(clienttesting.DeleteAction)
		p := deletion.GetDeleteOptions().Preconditions
		if p != nil {
			obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("secrets"), action.GetNamespace(), deletion.GetName())
			if err != nil {
				return true, nil, err
			}
			meta := obj.(metav1.Object)
			if p.UID != nil && *p.UID != meta.GetUID() || p.ResourceVersion != nil && *p.ResourceVersion != meta.GetResourceVersion() {
				return true, nil, apierrors.NewConflict(corev1.Resource("secrets"), deletion.GetName(), errors.New("fixture identity conflict"))
			}
		}
		return false, nil, nil
	})
	return client
}

func startupFixture(t *testing.T) (*Server, *fake.Clientset, *runnerv1.StartWorkloadRequest) {
	t.Helper()
	client := newIdentityClientset()
	server := New(Options{Clientset: client, Namespace: "default", StorageSize: "1Gi", Logger: zap.NewNop()})
	return server, client, startupRequest()
}

func startupRequest() *runnerv1.StartWorkloadRequest {
	return &runnerv1.StartWorkloadRequest{WorkloadId: uuid.NewString(),
		Main: &runnerv1.ContainerSpec{Name: "main", Image: "fixture:1",
			InlineFileMounts: []*runnerv1.InlineFileMount{{Path: "/config/test"}}},
		ImagePullCredentials: []*runnerv1.ImagePullCredential{{Registry: "registry.invalid", Username: "fixture", Password: "not-a-credential"}},
		InlineFiles:          map[string][]byte{"/config/test": []byte("fixture inline data")},
		Volumes:              []*runnerv1.VolumeSpec{{Name: "workspace", PersistentName: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED}},
	}
}

func startupObjects(t *testing.T, client *fake.Clientset, pods, secrets, claims int) {
	t.Helper()
	ctx := context.Background()
	p, err := client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil || len(p.Items) != pods {
		t.Fatalf("Pod count = %d, want %d; error = %v", len(p.Items), pods, err)
	}
	s, err := client.CoreV1().Secrets("default").List(ctx, metav1.ListOptions{})
	if err != nil || len(s.Items) != secrets {
		t.Fatalf("Secret count = %d, want %d; error = %v", len(s.Items), secrets, err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").List(ctx, metav1.ListOptions{})
	if err != nil || len(pvc.Items) != claims {
		t.Fatalf("PVC count = %d, want %d; error = %v", len(pvc.Items), claims, err)
	}
}

func TestStartupSecretsPVCRejectionCleansCredentials(t *testing.T) {
	for _, failure := range []string{"quota", "second-volume", "invalid-volume"} {
		t.Run(failure, func(t *testing.T) {
			server, client, req := startupFixture(t)
			claims := 0
			if failure == "second-volume" {
				claims = 1
				req.Volumes = append(req.Volumes, &runnerv1.VolumeSpec{Name: "second", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED})
			}
			if failure == "invalid-volume" {
				req.Volumes[0].Name = "invalid/name"
			} else {
				client.PrependReactor("create", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					name := action.(clienttesting.CreateAction).GetObject().(metav1.Object).GetName()
					if failure == "second-volume" && name == "workspace" {
						return false, nil, nil
					}
					return true, nil, apierrors.NewForbidden(corev1.Resource("persistentvolumeclaims"), name, errors.New("fixture quota"))
				})
			}
			if _, err := server.StartWorkload(context.Background(), req); err == nil {
				t.Fatal("rejected startup succeeded")
			}
			startupObjects(t, client, 0, 0, claims)
		})
	}
}

func TestStartupSecretsUncertainPodCreateRetainsCredentials(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			server, client, req := startupFixture(t)
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if accepted {
					if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), action.(clienttesting.CreateAction).GetObject(), "default"); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, io.ErrUnexpectedEOF
			})
			if _, err := server.StartWorkload(context.Background(), req); status.Code(err) != codes.Internal {
				t.Fatalf("error = %v, want Internal", err)
			}
			pods := 0
			if accepted {
				pods = 1
			}
			startupObjects(t, client, pods, 2, 1)
		})
	}
}

func TestStartupSecretsLostCreateAcknowledgementIsReconciled(t *testing.T) {
	server, client, req := startupFixture(t)
	client.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		secret := action.(clienttesting.CreateAction).GetObject().(*corev1.Secret)
		if !strings.HasSuffix(secret.Name, "-inline-files") {
			return false, nil, nil
		}
		secret.UID = types.UID(uuid.NewString())
		secret.ResourceVersion = "1"
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), secret, "default"); err != nil {
			t.Fatal(err)
		}
		return true, nil, io.ErrUnexpectedEOF
	})
	if _, err := server.StartWorkload(context.Background(), req); err == nil {
		t.Fatal("lost acknowledgement was treated as success")
	}
	startupObjects(t, client, 0, 0, 1)
}

func TestStartupSecretsRejectedStartsRetainOnlyDurableClaims(t *testing.T) {
	for _, stage := range []string{"pull-first", "pull-second", "inline", "inline-invalid", "container-invalid", "pod-quota", "pod-invalid", "pod-bad-request"} {
		t.Run(stage, func(t *testing.T) {
			server, client, req := startupFixture(t)
			claims, code := 1, codes.PermissionDenied
			if strings.HasPrefix(stage, "pull-") {
				claims = 0
			}
			if stage == "pull-second" {
				req.ImagePullCredentials = append(req.ImagePullCredentials, &runnerv1.ImagePullCredential{Registry: "second.invalid", Username: "fixture", Password: "second-fixture"})
			}
			switch stage {
			case "inline-invalid":
				req.InlineFiles["relative"] = []byte("fixture")
				code = codes.InvalidArgument
			case "container-invalid":
				req.Main.Mounts = []*runnerv1.VolumeMount{{Volume: "missing", MountPath: "/missing"}}
				code = codes.InvalidArgument
			case "pod-invalid":
				code = codes.InvalidArgument
			case "pod-bad-request":
				code = codes.Internal
			}
			client.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
				name := action.(clienttesting.CreateAction).GetObject().(metav1.Object).GetName()
				resource := action.GetResource().Resource
				reject := resource == "pods" && strings.HasPrefix(stage, "pod-") || resource == "secrets" &&
					(stage == "pull-first" || stage == "pull-second" && strings.HasSuffix(name, "-pull-1") || stage == "inline" && strings.HasSuffix(name, "-inline-files"))
				if !reject {
					return false, nil, nil
				}
				err := apierrors.NewForbidden(corev1.Resource(resource), name, errors.New("fixture quota"))
				if stage == "pod-invalid" {
					err = apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, name, field.ErrorList{field.Required(field.NewPath("spec"), "fixture")})
				} else if stage == "pod-bad-request" {
					err = apierrors.NewBadRequest("fixture malformed request")
				}
				return true, nil, err
			})
			_, err := server.StartWorkload(context.Background(), req)
			if status.Code(err) != code || strings.Contains(err.Error(), "cleanup_unconfirmed") {
				t.Fatalf("error = %v, want %s with confirmed cleanup", err, code)
			}
			startupObjects(t, client, 0, 0, claims)
			for _, action := range client.Actions() {
				if action.Matches("delete", "secrets") {
					p := action.(clienttesting.DeleteAction).GetDeleteOptions().Preconditions
					if p == nil || p.UID == nil || *p.UID == "" || p.ResourceVersion == nil || *p.ResourceVersion == "" {
						t.Fatal("cleanup attempted deletion without UID and resource version")
					}
				}
			}
		})
	}
}

func TestStartupSecretsCleanupRefusesForeignOrChangedResources(t *testing.T) {
	for _, change := range []string{"uid", "attempt", "owner", "data", "delete-race", "pod-present", "pod-read-failed", "delete-denied", "secret-read-failed"} {
		t.Run(change, func(t *testing.T) {
			server, client, req := startupFixture(t)
			core, logs := observer.New(zap.DebugLevel)
			server.logger = zap.New(core)
			pull := "workload-" + req.WorkloadId + "-pull"
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("secrets"), "default", pull)
				if err != nil {
					t.Fatal(err)
				}
				secret := obj.(*corev1.Secret)
				switch change {
				case "uid":
					secret.UID = "replacement"
				case "attempt":
					secret.Annotations[startupAttemptAnnotation] = "different-attempt"
				case "owner":
					secret.Labels[workloadIDLabelKey] = "another-workload"
				case "data":
					secret.Data[corev1.DockerConfigJsonKey] = []byte("external edit")
				case "pod-present":
					if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), action.(clienttesting.CreateAction).GetObject(), "default"); err != nil {
						t.Fatal(err)
					}
				}
				if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), secret, "default"); err != nil {
					t.Fatal(err)
				}
				return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "fixture", errors.New("fixture quota"))
			})
			client.PrependReactor("get", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if change == "pod-read-failed" && action.Matches("get", "pods") || change == "secret-read-failed" && action.(clienttesting.GetAction).GetName() == pull {
					return true, nil, io.ErrUnexpectedEOF
				}
				return false, nil, nil
			})
			client.PrependReactor("delete", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.(clienttesting.DeleteAction).GetName() != pull {
					return false, nil, nil
				}
				if change == "delete-denied" {
					return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), pull, errors.New("fixture cleanup denied"))
				}
				if change == "delete-race" {
					obj, _ := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("secrets"), "default", pull)
					obj.(*corev1.Secret).ResourceVersion = "2"
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), obj, "default"); err != nil {
						t.Fatal(err)
					}
				}
				return false, nil, nil
			})
			_, err := server.StartWorkload(context.Background(), req)
			if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "startup_secret_cleanup_unconfirmed") {
				t.Fatalf("error = %v, want original rejection plus unconfirmed cleanup", err)
			}
			pods, secrets := 0, 1
			if strings.HasPrefix(change, "pod-") {
				secrets = 2
			}
			if change == "pod-present" {
				pods = 1
			}
			startupObjects(t, client, pods, secrets, 1)
			data, err := json.Marshal(logs.All())
			if err != nil || strings.Contains(string(data), "not-a-credential") || strings.Contains(string(data), "fixture inline data") {
				t.Fatal("cleanup diagnostics exposed secret data")
			}
		})
	}
}

func TestStartupSecretsCreateConflictDoesNotAdoptExistingSecret(t *testing.T) {
	server, client, req := startupFixture(t)
	name := "workload-" + req.WorkloadId + "-inline-files"
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: "foreign", ResourceVersion: "1",
		Labels: map[string]string{managedByLabelKey: managedByLabelValue, workloadIDLabelKey: req.WorkloadId}}, Data: map[string][]byte{"foreign": []byte("fixture")}}
	if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), foreign, "default"); err != nil {
		t.Fatal(err)
	}
	if _, err := server.StartWorkload(context.Background(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("error = %v, want AlreadyExists", err)
	}
	startupObjects(t, client, 0, 1, 1)
	for _, action := range client.Actions() {
		if action.Matches("delete", "secrets") && action.(clienttesting.DeleteAction).GetName() == name {
			t.Fatal("conflicting secret was adopted")
		}
	}
}

type startupContextClient struct {
	kubernetes.Interface
	verify func(context.Context)
}

func (c startupContextClient) CoreV1() typedcore.CoreV1Interface {
	return startupContextCore{c.Interface.CoreV1(), c.verify}
}

type startupContextCore struct {
	typedcore.CoreV1Interface
	verify func(context.Context)
}

func (c startupContextCore) Secrets(namespace string) typedcore.SecretInterface {
	return startupContextSecret{c.CoreV1Interface.Secrets(namespace), c.verify}
}

type startupContextSecret struct {
	typedcore.SecretInterface
	verify func(context.Context)
}

func (c startupContextSecret) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Secret, error) {
	c.verify(ctx)
	return c.SecretInterface.Get(ctx, name, opts)
}

func (c startupContextSecret) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	c.verify(ctx)
	return c.SecretInterface.Delete(ctx, name, opts)
}

func TestStartupSecretsCanceledCallerStillGetsBoundedCleanup(t *testing.T) {
	server, client, req := startupFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checked := 0
	server.clientset = startupContextClient{client, func(cleanup context.Context) {
		checked++
		deadline, ok := cleanup.Deadline()
		if cleanup.Err() != nil || !ok || time.Until(deadline) > 5*time.Second {
			t.Fatal("cleanup inherited cancellation or has no bounded deadline")
		}
	}}
	client.PrependReactor("create", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, nil, apierrors.NewForbidden(corev1.Resource("persistentvolumeclaims"), "workspace", errors.New("fixture quota"))
	})
	if _, err := server.StartWorkload(ctx, req); status.Code(err) != codes.PermissionDenied || strings.Contains(err.Error(), "cleanup_unconfirmed") {
		t.Fatalf("error = %v, want original rejection and successful cleanup", err)
	}
	if checked < 3 {
		t.Fatal("cleanup did not read, delete and observe absence")
	}
	startupObjects(t, client, 0, 0, 0)
}

func TestStartupSecretsUnknownCreateCannotBeProvedAbsentByOneRead(t *testing.T) {
	server, client, req := startupFixture(t)
	client.PrependReactor("create", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, io.ErrUnexpectedEOF
	})
	_, err := server.StartWorkload(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "startup_secret_cleanup_unconfirmed") {
		t.Fatalf("error = %v, want unconfirmed create", err)
	}
	startupObjects(t, client, 0, 0, 0)
}

func TestStartupSecretsLostDeleteAcknowledgementRequiresObservedAbsence(t *testing.T) {
	server, client, req := startupFixture(t)
	client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "fixture", errors.New("fixture quota"))
	})
	client.PrependReactor("delete", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("secrets"), "default", action.(clienttesting.DeleteAction).GetName()); err != nil {
			t.Fatal(err)
		}
		return true, nil, io.ErrUnexpectedEOF
	})
	_, err := server.StartWorkload(context.Background(), req)
	if status.Code(err) != codes.PermissionDenied || strings.Contains(err.Error(), "cleanup_unconfirmed") {
		t.Fatalf("error = %v, want rejection and observed cleanup", err)
	}
	startupObjects(t, client, 0, 0, 1)
}

func TestStartupSecretsHeldDeletionDoesNotPretendCleanupSucceeded(t *testing.T) {
	server, client, req := startupFixture(t)
	client.PrependReactor("create", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("persistentvolumeclaims"), "fixture", errors.New("fixture quota"))
	})
	deletes := 0
	client.PrependReactor("delete", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletes++
		name := action.(clienttesting.DeleteAction).GetName()
		obj, _ := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("secrets"), "default", name)
		secret := obj.(*corev1.Secret)
		now := metav1.Now()
		secret.DeletionTimestamp = &now
		secret.Finalizers = []string{"fixture.example/held"}
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), secret, "default"); err != nil {
			t.Fatal(err)
		}
		return true, nil, nil
	})
	started := time.Now()
	_, err := server.StartWorkload(context.Background(), req)
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "startup_secret_cleanup_unconfirmed") || deletes != 1 {
		t.Fatalf("error = %v, delete attempts = %d; want unconfirmed cleanup without retry", err, deletes)
	}
	if elapsed := time.Since(started); elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Fatalf("cleanup deadline was not bounded: %v", elapsed)
	}
	startupObjects(t, client, 0, 1, 0)
}

func TestStartupSecretsConcurrentDuplicateDoesNotCleanTheWinner(t *testing.T) {
	server, client, req := startupFixture(t)
	results := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() { <-start; _, err := server.StartWorkload(context.Background(), req); results <- err }()
	}
	close(start)
	first, second := <-results, <-results
	if first != nil && second != nil || first == nil && second == nil {
		t.Fatalf("want one winner and one conflict, got %v, %v", first, second)
	}
	if first == nil {
		first = second
	}
	if status.Code(first) != codes.AlreadyExists {
		t.Fatalf("error = %v, want AlreadyExists", first)
	}
	startupObjects(t, client, 1, 2, 1)
	for _, action := range client.Actions() {
		if action.Matches("delete", "secrets") {
			t.Fatal("duplicate start tried to clean the winning attempt")
		}
	}
}

func TestStartupSecretsCreateRejectionIsConservative(t *testing.T) {
	resource := corev1.Resource("pods")
	for _, err := range []error{apierrors.NewForbidden(resource, "fixture", errors.New("quota")), apierrors.NewUnauthorized("fixture"),
		apierrors.NewBadRequest("fixture"), apierrors.NewNotFound(resource, "fixture"), apierrors.NewAlreadyExists(resource, "fixture"),
		apierrors.NewConflict(resource, "fixture", errors.New("fixture")), apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "fixture", nil)} {
		if !createRejected(err) {
			t.Fatalf("explicit rejection considered uncertain: %v", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF,
		apierrors.NewTimeoutError("fixture", 1), apierrors.NewServerTimeout(resource, "create", 1),
		apierrors.NewInternalError(errors.New("fixture")), apierrors.NewServiceUnavailable("fixture"), apierrors.NewTooManyRequests("fixture", 1)} {
		if createRejected(err) {
			t.Fatalf("uncertain create authorized cleanup: %v", err)
		}
	}
}
