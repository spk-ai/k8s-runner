package server

import (
	"context"
	"maps"
	"reflect"
	"slices"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const resourceAnchorAnnotation = "agyn.io/resource-anchor"
const resourceAnchorVersionAnnotation = "agyn.io/resource-anchor-version"
const resourceAnchorPodAnnotation = "agyn.io/anchor-pod-uid"
const resourceAnchorActivationAnnotation = "agyn.io/anchor-activation"

func anchorIdentityLabels(kind runnerv1.ResourceAnchorKind, labels map[string]string) map[string]string {
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		return volumeIdentityLabels(labels)
	}
	result := map[string]string{}
	for _, key := range []string{managedByLabelKey, workloadManagedByLabelKey, "managed-by", "agent-instance-id", "agent-id", "thread-id", "sandbox-id", "sandbox-owner-id"} {
		if value, present := labels[key]; present {
			result[key] = value
		}
	}
	return result
}

func validateResourceAnchor(a *runnerv1.ResourceAnchor, bound bool) error {
	if a == nil || a.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD && a.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME ||
		!validPreparedID(a.ResourceId) || !validPreparedBackend(a.BackendId) || bound && !validPreparedID(a.InstanceUid) || !bound && a.InstanceUid != "" || len(a.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "valid_resource_anchor_required")
	}
	labels := a.IdentityLabels
	if labels[managedByLabelKey] != managedByLabelValue || labels[workloadManagedByLabelKey] != workloadManagedByLabelValue || labels["managed-by"] != workloadManagedByLabelValue || !maps.Equal(labels, anchorIdentityLabels(a.Kind, labels)) {
		return status.Error(codes.InvalidArgument, "resource_anchor_identity_required")
	}
	for _, value := range labels {
		if value == "" || len(validation.IsValidLabelValue(value)) != 0 {
			return status.Error(codes.InvalidArgument, "resource_anchor_label_invalid")
		}
	}
	agent := labels["agent-instance-id"] != "" && labels["agent-id"] != "" && labels["sandbox-id"] == "" && labels["sandbox-owner-id"] == ""
	sandbox := labels["sandbox-id"] != "" && labels["sandbox-owner-id"] != "" && labels["agent-instance-id"] == "" && labels["agent-id"] == "" && labels["thread-id"] == ""
	if !agent && !sandbox || agent && a.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD && labels["thread-id"] == "" ||
		a.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME && labels[volumeKeyLabelKey] != a.ResourceId {
		return status.Error(codes.InvalidArgument, "resource_anchor_owner_required")
	}
	return nil
}

func resourceAnchorName(a *runnerv1.ResourceAnchor) string {
	if a.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		return "volume-anchor-" + a.ResourceId
	}
	return "workload-anchor-" + a.ResourceId
}

func resourceAnchorOwners(a *runnerv1.ResourceAnchor) []metav1.OwnerReference {
	if a == nil {
		return nil
	}
	return []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: resourceAnchorName(a), UID: types.UID(a.InstanceUid)}}
}

func matchResourceAnchor(cm *corev1.ConfigMap, expected *runnerv1.ResourceAnchor, namespace string) error {
	if cm == nil || cm.Name != resourceAnchorName(expected) || cm.Namespace != namespace || !validPreparedID(string(cm.UID)) || cm.ResourceVersion == "" ||
		expected.InstanceUid != "" && string(cm.UID) != expected.InstanceUid || len(cm.OwnerReferences) != 0 || cm.Immutable == nil || !*cm.Immutable ||
		cm.Annotations[resourceAnchorVersionAnnotation] != "v1" || !maps.Equal(cm.Labels, expected.IdentityLabels) || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return status.Error(codes.FailedPrecondition, "resource_anchor_identity_mismatch")
	}
	pod, activation := cm.Annotations[resourceAnchorPodAnnotation], cm.Annotations[resourceAnchorActivationAnnotation]
	if pod != "" && (!validPreparedID(pod) || expected.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD) || activation != "" && (activation != pod || !validPreparedID(activation)) {
		return status.Error(codes.FailedPrecondition, "resource_anchor_lifecycle_invalid")
	}
	stored := &runnerv1.ResourceAnchor{}
	data := cm.Data["identity.json"]
	if len(data) > 16*1024 || protojson.Unmarshal([]byte(data), stored) != nil {
		return status.Error(codes.FailedPrecondition, "resource_anchor_intent_invalid")
	}
	intent := proto.Clone(expected).(*runnerv1.ResourceAnchor)
	intent.InstanceUid = ""
	if !proto.Equal(stored, intent) {
		return status.Error(codes.FailedPrecondition, "resource_anchor_intent_mismatch")
	}
	return nil
}

func (s *Server) requireResourceAnchor(ctx context.Context, expected *runnerv1.ResourceAnchor) error {
	_, err := s.readResourceAnchor(ctx, expected)
	return err
}

func (s *Server) readResourceAnchor(ctx context.Context, expected *runnerv1.ResourceAnchor) (*corev1.ConfigMap, error) {
	if err := validateResourceAnchor(expected, true); err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	cm, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Get(ctx, resourceAnchorName(expected), metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := matchResourceAnchor(cm, expected, s.namespace); err != nil {
		return nil, err
	}
	if cm.DeletionTimestamp != nil {
		return nil, status.Error(codes.FailedPrecondition, "resource_anchor_retiring")
	}
	return cm, nil
}

// ReserveResourceAnchor reserves metadata only. Persist the UID before authorizing
// creation; recover matching metadata only while that authority remains unused.
// A same-name replacement cannot substitute for a pinned generation. Lost replies
// leave metadata, not compute. Labels and backend IDs are not authentication.
// @see runners::internal/server/resource_anchors
func (s *Server) ReserveResourceAnchor(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest) (*runnerv1.ReserveResourceAnchorResponse, error) {
	if err := validateResourceAnchor(req.GetIntent(), false); err != nil {
		return nil, err
	}
	intent := proto.Clone(req.Intent).(*runnerv1.ResourceAnchor)
	if _, err := s.checkVolumeBackend(ctx, intent.BackendId); err != nil {
		return nil, err
	}
	objects := s.clientset.CoreV1().ConfigMaps(s.namespace)
	if intent.Kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
		if _, err := objects.Get(ctx, preparationRevocationName(intent), metav1.GetOptions{}); err == nil {
			return nil, status.Error(codes.FailedPrecondition, "resource_anchor_permanently_revoked")
		} else if !apierrors.IsNotFound(err) {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
	}
	cm, err := objects.Get(ctx, resourceAnchorName(intent), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		data, marshalErr := protojson.Marshal(intent)
		if marshalErr != nil || len(data) > 16*1024 {
			return nil, status.Error(codes.InvalidArgument, "resource_anchor_intent_too_large")
		}
		immutable := true
		cm, err = objects.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resourceAnchorName(intent), Namespace: s.namespace,
			Labels: maps.Clone(intent.IdentityLabels), Annotations: map[string]string{resourceAnchorVersionAnnotation: "v1"}}, Immutable: &immutable,
			Data: map[string]string{"identity.json": string(data)}}, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
		if apierrors.IsAlreadyExists(err) {
			cm, err = objects.Get(ctx, resourceAnchorName(intent), metav1.GetOptions{})
		}
	}
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := matchResourceAnchor(cm, intent, s.namespace); err != nil {
		return nil, err
	}
	if cm.DeletionTimestamp != nil {
		return nil, status.Error(codes.FailedPrecondition, "resource_anchor_retiring")
	}
	if _, err := s.checkVolumeBackend(ctx, intent.BackendId); err != nil {
		return nil, err
	}
	intent.InstanceUid = string(cm.UID)
	return &runnerv1.ReserveResourceAnchorResponse{Anchor: intent}, nil
}

// RemoveWorkloadAnchor revokes only a workload owner with UID/revision conditions.
// An activation claim requires exact selected-Pod retirement first, even with a lost
// gate PATCH reply. ABSENT describes the owner, not child/credential cleanup;
// delayed gated children need observed GC and persistent volume owners remain.
func (s *Server) RemoveWorkloadAnchor(ctx context.Context, req *runnerv1.RemoveWorkloadAnchorRequest) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
	if err := validateResourceAnchor(req.GetExpected(), true); err != nil {
		return nil, err
	}
	expected := proto.Clone(req.Expected).(*runnerv1.ResourceAnchor)
	if expected.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_required")
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	objects := s.clientset.CoreV1().ConfigMaps(s.namespace)
	cm, err := objects.Get(ctx, resourceAnchorName(expected), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
			return nil, err
		}
		// This confirms only the anchor's absence. Delayed children may still be
		// written with this deleted UID and require separately observed GC.
		return &runnerv1.RemoveWorkloadAnchorResponse{Anchor: expected, State: runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT}, nil
	}
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := matchResourceAnchor(cm, expected, s.namespace); err != nil {
		return nil, err
	}
	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podNameFromID(expected.ResourceId), metav1.GetOptions{})
	if err == nil {
		if pod == nil || !validPreparedID(string(pod.UID)) || pod.ResourceVersion == "" || pod.Name != podNameFromID(expected.ResourceId) || pod.Namespace != s.namespace {
			return nil, status.Error(codes.FailedPrecondition, "anchor_pod_identity_unconfirmed")
		}
		if cm.Annotations[resourceAnchorActivationAnnotation] == string(pod.UID) {
			return nil, status.Error(codes.FailedPrecondition, "activated_anchor_requires_exact_pod_removal")
		}
		if !reflect.DeepEqual(pod.OwnerReferences, resourceAnchorOwners(expected)) || !maps.Equal(anchorIdentityLabels(expected.Kind, pod.Labels), expected.IdentityLabels) ||
			pod.Labels[workloadIDLabelKey] != expected.ResourceId || pod.Spec.NodeName != "" || !hasPreparedGate(pod) ||
			pod.Annotations[preparedStateAnnotation] != "preparing" && pod.Annotations[preparedStateAnnotation] != "prepared" || pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending {
			return nil, status.Error(codes.FailedPrecondition, "workload_anchor_requires_unexecuted_or_absent_pod")
		}
		statuses := append(slices.Clone(pod.Status.InitContainerStatuses), pod.Status.ContainerStatuses...)
		statuses = append(statuses, pod.Status.EphemeralContainerStatuses...)
		for _, container := range statuses {
			if container.State.Running != nil || container.State.Terminated != nil || container.LastTerminationState.Terminated != nil {
				return nil, status.Error(codes.FailedPrecondition, "workload_anchor_has_execution_evidence")
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if cm.DeletionTimestamp == nil {
		if err := objects.Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	return &runnerv1.RemoveWorkloadAnchorResponse{Anchor: expected, State: runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_PENDING}, nil
}
