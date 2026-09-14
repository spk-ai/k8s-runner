package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

const startupAttemptAnnotation = "agyn.io/startup-attempt"

type startupSecret struct {
	spec      *corev1.Secret
	uid       types.UID
	uncertain bool
}

// PVCs belong to the durable volume lifecycle. Only this attempt's temporary
// credentials participate in startup rollback, before a Pod could be running.
type startupSecrets struct {
	server             *Server
	workloadID         string
	attempt            string
	entries            []*startupSecret
	podCreateUncertain bool
}

func newStartupSecrets(server *Server, workloadID string) *startupSecrets {
	return &startupSecrets{server: server, workloadID: workloadID, attempt: uuid.NewString()}
}

func (s *startupSecrets) create(ctx context.Context, spec *corev1.Secret) (*corev1.Secret, error) {
	spec = spec.DeepCopy()
	if spec.Annotations == nil {
		spec.Annotations = map[string]string{}
	}
	spec.Annotations[startupAttemptAnnotation] = s.attempt
	entry := &startupSecret{spec: spec, uncertain: true}
	s.entries = append(s.entries, entry)
	created, err := s.server.clientset.CoreV1().Secrets(s.server.namespace).Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		if createRejected(err) {
			// A conflict never authorizes adopting or deleting the existing object.
			s.entries = s.entries[:len(s.entries)-1]
		}
		return nil, err
	}
	if created == nil || created.UID == "" {
		return nil, fmt.Errorf("secret_create_identity_missing")
	}
	entry.uid, entry.uncertain = created.UID, false
	return created, nil
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
