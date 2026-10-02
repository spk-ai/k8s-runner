package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

const startupAttemptAnnotation = "agyn.io/startup-attempt"

type startupSecret struct {
	spec            *corev1.Secret
	uid             types.UID
	resourceVersion string
	uncertain       bool
}

// PVCs belong to the durable volume lifecycle. Only this attempt's temporary
// credentials participate in startup rollback, before a Pod could be running.
type startupSecrets struct {
	server             *Server
	workloadID         string
	attempt            string
	entries            []*startupSecret
	podCreateUncertain bool
	prepared           bool
}

func newStartupSecrets(server *Server, workloadID string) *startupSecrets {
	return &startupSecrets{server: server, workloadID: workloadID, attempt: uuid.NewString()}
}

func (s *startupSecrets) stageOrCreate(ctx context.Context, spec *corev1.Secret) error {
	spec = spec.DeepCopy()
	if spec.Annotations == nil {
		spec.Annotations = map[string]string{}
	}
	spec.Annotations[startupAttemptAnnotation] = s.attempt
	entry := &startupSecret{spec: spec, uncertain: true}
	s.entries = append(s.entries, entry)
	if s.prepared {
		// The gated Pod must exist before any temporary credential is written.
		return nil
	}
	created, err := s.server.clientset.CoreV1().Secrets(s.server.namespace).Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		if createRejected(err) {
			// A conflict never authorizes adopting or deleting the existing object.
			s.entries = s.entries[:len(s.entries)-1]
		}
		return err
	}
	if created == nil || created.UID == "" {
		return fmt.Errorf("secret_create_identity_missing")
	}
	entry.uid, entry.resourceVersion, entry.uncertain = created.UID, created.ResourceVersion, false
	return nil
}

// A legacy Pod is admitted ungated, so its credentials must exist before it is
// created and cannot carry its UID in their CREATE. Without an owner they
// outlive a Pod removed by anything other than Stop/Remove. The reference is
// added afterwards, and only to the exact incarnation this attempt created and
// nobody has changed since: the patch tests both UID and resource version, so a
// delayed or retried write fails rather than claiming a replacement Secret.
// The owner is the UID the Pod CREATE returned. If that Pod is already gone,
// garbage collection removes the Secret, which is then unused by definition.
// A crash or failure before this point leaves the Secret ownerless for the
// orphan sweep; attaching never deletes anything itself.
func (s *startupSecrets) attachPodOwner(parent context.Context, pod *corev1.Pod) error {
	if len(s.entries) == 0 {
		return nil
	}
	if s.prepared || pod == nil || pod.UID == "" || pod.Name != podNameFromID(s.workloadID) {
		return fmt.Errorf("legacy Pod identity unavailable; startup secrets left without owner")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	owner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}}
	secrets := s.server.clientset.CoreV1().Secrets(s.server.namespace)
	var failures []error
	for _, entry := range s.entries {
		if entry.uncertain || entry.uid == "" || entry.resourceVersion == "" {
			failures = append(failures, fmt.Errorf("startup secret %s identity unknown", entry.spec.Name))
			continue
		}
		patch, err := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": entry.uid},
			{"op": "test", "path": "/metadata/resourceVersion", "value": entry.resourceVersion},
			{"op": "add", "path": "/metadata/ownerReferences", "value": owner},
		})
		if err != nil {
			return err
		}
		current, err := secrets.Patch(ctx, entry.spec.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
		if err != nil {
			failures = append(failures, fmt.Errorf("startup secret %s owner not attached: %w", entry.spec.Name, err))
			continue
		}
		if current == nil || current.UID != entry.uid || !reflect.DeepEqual(current.OwnerReferences, owner) {
			failures = append(failures, fmt.Errorf("startup secret %s owner unconfirmed", entry.spec.Name))
		}
	}
	return errors.Join(failures...)
}

// Ownership is part of CREATE, not a later PATCH that a crash could interrupt.
// Even a delayed write after Pod removal remains tied to the deleted Pod UID.
func (s *startupSecrets) createPrepared(ctx context.Context, pod *corev1.Pod) error {
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}
	secrets := s.server.clientset.CoreV1().Secrets(s.server.namespace)
	for _, entry := range s.entries {
		spec := entry.spec.DeepCopy()
		spec.OwnerReferences = []metav1.OwnerReference{owner}
		current, err := secrets.Create(ctx, spec, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
		if err != nil {
			return grpcErrorFromKube(s.server.logger, err, codes.Internal)
		}
		if current == nil || current.UID == "" || current.Name != spec.Name || current.Namespace != s.server.namespace || current.ResourceVersion == "" || current.DeletionTimestamp != nil ||
			current.Annotations[startupAttemptAnnotation] != s.attempt || !reflect.DeepEqual(current.OwnerReferences, spec.OwnerReferences) || !reflect.DeepEqual(current.Data, spec.Data) || current.Type != spec.Type {
			return status.Error(codes.FailedPrecondition, "prepared_secret_identity_mismatch")
		}
		for key, value := range entry.spec.Labels {
			if current.Labels[key] != value {
				return status.Error(codes.FailedPrecondition, "prepared_secret_identity_mismatch")
			}
		}
	}
	return nil
}

// A read returning NotFound after a timeout is not evidence that an in-flight
// create cannot still finish. Only explicit API rejection allows Pod rollback.
func createRejected(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) ||
		apierrors.IsBadRequest(err) || apierrors.IsNotFound(err) || apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err)
}

func (s *startupSecrets) cleanup(parent context.Context) error {
	if len(s.entries) == 0 {
		return nil
	}
	if s.podCreateUncertain {
		return fmt.Errorf("Pod creation is uncertain; startup secrets retained")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	_, err := s.server.clientset.CoreV1().Pods(s.server.namespace).Get(ctx, podNameFromID(s.workloadID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("Pod absence unconfirmed; startup secrets retained")
	}
	var failures []error
	for _, entry := range s.entries {
		if err := s.remove(ctx, entry); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (s *startupSecrets) remove(ctx context.Context, entry *startupSecret) error {
	secrets := s.server.clientset.CoreV1().Secrets(s.server.namespace)
	current, err := secrets.Get(ctx, entry.spec.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && !entry.uncertain {
		return nil
	}
	if err != nil {
		return fmt.Errorf("startup secret %s creation or removal unconfirmed: %w", entry.spec.Name, err)
	}
	if current.UID == "" || current.ResourceVersion == "" || entry.uid != "" && current.UID != entry.uid ||
		current.Annotations[startupAttemptAnnotation] != s.attempt || current.Type != entry.spec.Type ||
		!reflect.DeepEqual(current.Data, entry.spec.Data) {
		return fmt.Errorf("startup secret %s identity or content changed; retaining it", entry.spec.Name)
	}
	for key, value := range entry.spec.Labels {
		if current.Labels[key] != value {
			return fmt.Errorf("startup secret %s ownership changed; retaining it", entry.spec.Name)
		}
	}
	deleteErr := secrets.Delete(ctx, current.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &current.UID, ResourceVersion: &current.ResourceVersion,
	}})
	// Observe absence even after a lost deletion acknowledgement. Never retry a
	// conflicting write or remove a finalizer to make cleanup appear successful.
	return wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		remaining, err := secrets.Get(ctx, current.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("startup secret %s deletion unconfirmed: %w", current.Name, err)
		}
		if remaining.UID != current.UID {
			return false, fmt.Errorf("startup secret %s replaced during cleanup", current.Name)
		}
		if deleteErr != nil {
			return false, fmt.Errorf("startup secret %s deletion unconfirmed: %w", current.Name, deleteErr)
		}
		return false, nil
	})
}
