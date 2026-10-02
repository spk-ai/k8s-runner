package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/metadata"
)

const (
	secretSweepPageSize     = 200
	secretSweepMaxPages     = 50
	secretSweepMaxDeletions = 100
	secretSweepPassTimeout  = 2 * time.Minute
)

var secretsResource = corev1.SchemeGroupVersion.WithResource("secrets")

// trackStart marks a workload whose start is running in this process. Its
// credentials may exist before its Pod does, so the sweep must not judge them.
func (s *Server) trackStart(workloadID string) func() {
	s.startsMu.Lock()
	if s.startsInFlight == nil {
		s.startsInFlight = map[string]int{}
	}
	s.startsInFlight[workloadID]++
	s.startsMu.Unlock()
	return func() {
		s.startsMu.Lock()
		defer s.startsMu.Unlock()
		if s.startsInFlight[workloadID]--; s.startsInFlight[workloadID] <= 0 {
			delete(s.startsInFlight, workloadID)
		}
	}
}

func (s *Server) startInFlight(workloadID string) bool {
	s.startsMu.Lock()
	defer s.startsMu.Unlock()
	return s.startsInFlight[workloadID] > 0
}

// RunSecretSweep releases ownerless startup Secrets whose Pod is gone, once at
// start and then every interval until ctx ends. Pod-owned Secrets are left to
// Kubernetes garbage collection; this covers only what ownership cannot: a
// crash or failure between credential creation and the owner reference.
func (s *Server) RunSecretSweep(ctx context.Context, interval, grace time.Duration) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		deleted, err := s.sweepOrphanedSecrets(ctx, grace, time.Now())
		switch {
		case err != nil && ctx.Err() == nil:
			s.logger.Warn("workload secret sweep incomplete", zap.Int("deleted", deleted), zap.Error(err))
		case deleted > 0:
			s.logger.Info("workload secret sweep released orphans", zap.Int("deleted", deleted))
		default:
			s.logger.Debug("workload secret sweep found no orphans")
		}
		timer.Reset(interval)
	}
}

// Listing reads metadata only: the sweep never needs, holds or logs credential
// content. Each pass is bounded in pages, deletions and time; whatever is left
// waits for the next pass.
func (s *Server) sweepOrphanedSecrets(parent context.Context, grace time.Duration, now time.Time) (int, error) {
	if s.metadata == nil {
		return 0, errors.New("metadata client unavailable")
	}
	ctx, cancel := context.WithTimeout(parent, secretSweepPassTimeout)
	defer cancel()
	secrets := s.metadata.Resource(secretsResource).Namespace(s.namespace)
	options := metav1.ListOptions{LabelSelector: managedByLabelKey + "=" + managedByLabelValue + "," + workloadIDLabelKey, Limit: secretSweepPageSize}
	deleted := 0
	var failures []error
	for page := 0; page < secretSweepMaxPages; page++ {
		list, err := secrets.List(ctx, options)
		if err != nil {
			return deleted, errors.Join(append(failures, fmt.Errorf("list workload secrets: %w", err))...)
		}
		for i := range list.Items {
			if deleted >= secretSweepMaxDeletions {
				return deleted, errors.Join(failures...)
			}
			item := &list.Items[i]
			workloadID, ok := s.orphanCandidate(item, grace, now)
			if !ok {
				continue
			}
			removed, err := s.releaseOrphan(ctx, secrets, item, workloadID)
			if err != nil {
				failures = append(failures, err)
			}
			if removed {
				deleted++
			}
		}
		if list.Continue == "" {
			break
		}
		options.Continue = list.Continue
	}
	return deleted, errors.Join(failures...)
}

// orphanCandidate accepts only what this runner's legacy start path writes:
// its manager label, a parseable workload id, the exact name derived from that
// id, and an attempt annotation. Anything owned, finalized, terminating or
// younger than the grace period belongs to someone else's decision.
func (s *Server) orphanCandidate(item *metav1.PartialObjectMetadata, grace time.Duration, now time.Time) (string, bool) {
	if item.Namespace != s.namespace || item.UID == "" || item.ResourceVersion == "" ||
		item.Labels[managedByLabelKey] != managedByLabelValue || item.Annotations[startupAttemptAnnotation] == "" ||
		len(item.OwnerReferences) != 0 || len(item.Finalizers) != 0 || item.DeletionTimestamp != nil {
		return "", false
	}
	for _, key := range []string{preparedBindingAnnotation, preparedStateAnnotation, preparationRecoveryAnnotation, resourceAnchorAnnotation} {
		if _, ok := item.Annotations[key]; ok {
			return "", false
		}
	}
	workloadID := item.Labels[workloadIDLabelKey]
	if _, err := uuid.Parse(workloadID); err != nil || !legacySecretName(item.Name, workloadID) {
		return "", false
	}
	if item.CreationTimestamp.IsZero() || now.Sub(item.CreationTimestamp.Time) < grace {
		return "", false
	}
	return workloadID, true
}

func legacySecretName(name, workloadID string) bool {
	prefix := podNameFromID(workloadID)
	if name == prefix+"-pull" || name == prefix+"-inline-files" {
		return true
	}
	index, ok := strings.CutPrefix(name, prefix+"-pull-")
	if !ok {
		return false
	}
	n, err := strconv.Atoi(index)
	return err == nil && n >= 0 && strconv.Itoa(n) == index
}

// Only a NotFound read of the workload's Pod is evidence of absence. A start
// cannot reuse an existing Secret name (CREATE conflicts are never adopted), so
// once the Pod is absent no new Pod can come to depend on this incarnation; the
// grace period covers a start in another process still between the two writes.
func (s *Server) releaseOrphan(ctx context.Context, secrets metadata.ResourceInterface, item *metav1.PartialObjectMetadata, workloadID string) (bool, error) {
	if s.startInFlight(workloadID) {
		return false, nil
	}
	_, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podNameFromID(workloadID), metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("workload %s Pod absence unconfirmed: %w", workloadID, err)
	}
	if s.startInFlight(workloadID) {
		return false, nil
	}
	// The listed UID and resource version are preconditions, so a Secret that
	// was replaced or gained an owner after the listing is left alone.
	err = secrets.Delete(ctx, item.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &item.UID, ResourceVersion: &item.ResourceVersion,
	}})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("delete orphaned secret %s: %w", item.Name, err)
	}
	s.logger.Info("deleted orphaned workload secret", zap.String("workload_id", workloadID), zap.String("secret", item.Name))
	return true, nil
}
