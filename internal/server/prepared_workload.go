package server

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
)

const (
	preparedGate              = "agyn.io/workload-binding"
	preparedBindingAnnotation = "agyn.io/prepared-binding"
	preparedStateAnnotation   = "agyn.io/prepared-state"
	preparedHoldPrefix        = "agyn.io/workload-"
	maxPreparedVolumes        = 64
)

type workloadPreparation struct {
	binding  *runnerv1.WorkloadBinding
	expected map[string]*runnerv1.VolumeListItem
	anchors  map[string]*runnerv1.ResourceAnchor
}

func validPreparedID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id != uuid.Nil
}

func validPreparedBackend(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 512
}

func canonicalBinding(value *runnerv1.WorkloadBinding) (*runnerv1.WorkloadBinding, error) {
	if value == nil || !validPreparedID(value.WorkloadId) || !validPreparedID(value.InstanceUid) || !validPreparedBackend(value.BackendId) || len(value.Volumes) > maxPreparedVolumes {
		return nil, status.Error(codes.InvalidArgument, "valid_workload_binding_required")
	}
	seenNames, seenKeys := map[string]bool{}, map[string]bool{}
	for _, volume := range value.Volumes {
		if err := validateVolumeRemovalTarget(volume); err != nil {
			return nil, err
		}
		if volume.BackendId != value.BackendId || seenNames[volume.InstanceId] || seenKeys[volume.VolumeKey] {
			return nil, status.Error(codes.InvalidArgument, "ambiguous_workload_volume_binding")
		}
		seenNames[volume.InstanceId], seenKeys[volume.VolumeKey] = true, true
	}
	if err := validateBindingAnchors(value); err != nil {
		return nil, err
	}
	result := proto.Clone(value).(*runnerv1.WorkloadBinding)
	slices.SortFunc(result.Volumes, func(a, b *runnerv1.VolumeListItem) int { return strings.Compare(a.InstanceId, b.InstanceId) })
	return result, nil
}

func preparedVolume(pvc *corev1.PersistentVolumeClaim, backend string) (*runnerv1.VolumeListItem, error) {
	anchor, err := volumeAnchorFromPVC(pvc, backend)
	if err != nil {
		return nil, err
	}
	return &runnerv1.VolumeListItem{
		InstanceId: pvc.Name, InstanceUid: string(pvc.UID), BackendId: backend,
		VolumeKey: pvc.Labels[volumeKeyLabelKey], IdentityLabels: volumeIdentityLabels(pvc.Labels),
		Anchor: anchor,
	}, nil
}

func matchPreparedPVC(pvc *corev1.PersistentVolumeClaim, expected *runnerv1.VolumeListItem, namespace string) error {
	if pvc == nil || pvc.Name != expected.InstanceId || pvc.Namespace != namespace || string(pvc.UID) != expected.InstanceUid ||
		pvc.ResourceVersion == "" || !maps.Equal(volumeIdentityLabels(pvc.Labels), expected.IdentityLabels) {
		return status.Error(codes.FailedPrecondition, "prepared_volume_identity_mismatch")
	}
	return matchAnchoredMetadata(pvc.ObjectMeta, expected.Anchor)
}

func (s *Server) PrepareWorkload(ctx context.Context, req *runnerv1.PrepareWorkloadRequest) (*runnerv1.PrepareWorkloadResponse, error) {
	return s.prepareWorkload(ctx, req, nil, nil)
}

func (s *Server) prepareWorkload(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, anchor *runnerv1.ResourceAnchor, anchors map[string]*runnerv1.ResourceAnchor) (*runnerv1.PrepareWorkloadResponse, error) {
	workload := req.GetWorkload()
	if workload == nil || !validPreparedID(workload.WorkloadId) || !validPreparedBackend(req.GetBackendId()) || len(workload.Volumes) > maxPreparedVolumes {
		return nil, status.Error(codes.InvalidArgument, "valid_workload_preparation_required")
	}
	p := &workloadPreparation{binding: &runnerv1.WorkloadBinding{WorkloadId: workload.WorkloadId, BackendId: req.BackendId, Anchor: anchor}, expected: map[string]*runnerv1.VolumeListItem{}, anchors: anchors}
	labels, err := buildLabels(workload.WorkloadId, workload.AdditionalProperties, workload.Labels)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid_prepared_workload_labels")
	}
	desired := map[string]*corev1.PersistentVolumeClaim{}
	for _, volume := range workload.Volumes {
		if volume.GetKind() != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
			continue
		}
		pvc, err := s.desiredPVC(volume, labels)
		if err != nil {
			return nil, err
		}
		if desired[pvc.Name] != nil {
			return nil, status.Error(codes.InvalidArgument, "duplicate_prepared_claim")
		}
		desired[pvc.Name] = pvc
	}
	for _, expected := range req.ExpectedVolumes {
		if err := validateVolumeRemovalTarget(expected); err != nil {
			return nil, err
		}
		pvc := desired[expected.InstanceId]
		if pvc == nil || expected.BackendId != req.BackendId || p.expected[expected.InstanceId] != nil || !maps.Equal(volumeIdentityLabels(pvc.Labels), expected.IdentityLabels) || !proto.Equal(expected.Anchor, anchors[expected.VolumeKey]) {
			return nil, status.Error(codes.InvalidArgument, "preparation_volume_contract_mismatch")
		}
		p.expected[expected.InstanceId] = proto.Clone(expected).(*runnerv1.VolumeListItem)
	}
	// Scheduling gates are stable from 1.30. Older servers may drop an unsupported
	// gate and execute before the create response can be inspected.
	serverVersion, err := s.clientset.Discovery().ServerVersion()
	if err != nil || serverVersion == nil {
		return nil, status.Error(codes.FailedPrecondition, "prepared_workload_kubernetes_version_unconfirmed")
	}
	parsed, err := version.ParseSemantic(serverVersion.GitVersion)
	if err != nil || !parsed.AtLeast(version.MustParseSemantic("v1.30.0")) {
		return nil, status.Error(codes.FailedPrecondition, "prepared_workload_requires_kubernetes_1_30")
	}
	if _, err := s.checkVolumeBackend(ctx, req.BackendId); err != nil {
		return nil, err
	}
	// Reject missing/replaced resume volumes before credentials or other resources
	// are created. The second lookup below also never creates a known binding.
	for name, expected := range p.expected {
		pvc, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := matchPreparedPVC(pvc, expected, s.namespace); err != nil {
			return nil, err
		}
		if err := p.validatePVC(pvc, desired[name]); err != nil {
			return nil, err
		}
	}
	response, err := s.startWorkload(ctx, workload, p)
	if err != nil {
		return nil, err
	}
	return &runnerv1.PrepareWorkloadResponse{Workload: response, Binding: proto.Clone(p.binding).(*runnerv1.WorkloadBinding)}, nil
}

func (p *workloadPreparation) resolvePVC(ctx context.Context, s *Server, spec *runnerv1.VolumeSpec, labels map[string]string) (string, error) {
	desired, err := s.desiredPVC(spec, labels)
	if err != nil {
		return "", err
	}
	anchor := p.anchors[desired.Labels[volumeKeyLabelKey]]
	if p.binding.Anchor != nil {
		if err := s.requireResourceAnchor(ctx, p.binding.Anchor); err != nil {
			return "", err
		}
		if err := s.requireResourceAnchor(ctx, anchor); err != nil {
			return "", err
		}
		if err := attachResourceAnchor(&desired.ObjectMeta, anchor); err != nil {
			return "", err
		}
	}
	if p.expected[desired.Name] == nil {
		if _, err := s.ensurePVCObject(ctx, desired, anchor); err != nil {
			return "", err
		}
	}
	pvc, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return "", grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := p.validatePVC(pvc, desired); err != nil {
		return "", err
	}
	actual, err := preparedVolume(pvc, p.binding.BackendId)
	if err != nil {
		return "", err
	}
	if err := validateVolumeRemovalTarget(actual); err != nil {
		return "", status.Error(codes.FailedPrecondition, "prepared_volume_identity_missing")
	}
	if expected := p.expected[desired.Name]; expected != nil && !proto.Equal(expected, actual) {
		return "", status.Error(codes.FailedPrecondition, "prepared_volume_identity_mismatch")
	}
	p.binding.Volumes = append(p.binding.Volumes, actual)
	return pvc.Name, nil
}

func (p *workloadPreparation) gate(ctx context.Context, s *Server, pod *corev1.Pod) error {
	if _, err := s.checkVolumeBackend(ctx, p.binding.BackendId); err != nil {
		return err
	}
	if err := s.requireBindingAnchors(ctx, p.binding); err != nil {
		return err
	}
	if p.binding.Anchor != nil {
		if err := attachResourceAnchor(&pod.ObjectMeta, p.binding.Anchor); err != nil {
			return err
		}
	}
	data, err := protojson.Marshal(p.binding)
	if err != nil || len(data) > 64*1024 {
		return status.Error(codes.InvalidArgument, "prepared_binding_too_large")
	}
	pod.Annotations[preparedBindingAnnotation] = string(data)
	pod.Annotations[preparedStateAnnotation] = "preparing"
	pod.Annotations[preparationRecoveryAnnotation] = preparationRecoveryVersion
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: preparedGate}}
	return nil
}

func (p *workloadPreparation) accept(ctx context.Context, s *Server, pod *corev1.Pod) error {
	if pod == nil {
		return status.Error(codes.FailedPrecondition, "prepared_pod_identity_missing")
	}
	p.binding.InstanceUid = string(pod.UID)
	binding, err := canonicalBinding(p.binding)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "prepared_pod_identity_missing")
	}
	if err := s.matchPreparedPod(pod, binding); err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "preparing" {
		return status.Error(codes.FailedPrecondition, "prepared_pod_not_gated")
	}
	if _, err := s.checkVolumeBackend(ctx, binding.BackendId); err != nil {
		return err
	}
	if err := s.requireBindingAnchors(ctx, binding); err != nil {
		return err
	}
	if err := s.claimAnchorPod(ctx, binding, false); err != nil {
		return err
	}
	p.binding = binding
	return nil
}

func (p *workloadPreparation) complete(ctx context.Context, s *Server) error {
	if err := s.requireBindingAnchors(ctx, p.binding); err != nil {
		return err
	}
	pods := s.clientset.CoreV1().Pods(s.namespace)
	pod, err := pods.Get(ctx, podNameFromID(p.binding.WorkloadId), metav1.GetOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(pod, p.binding); err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || !hasPreparedGate(pod) || pod.Annotations[preparedStateAnnotation] != "preparing" {
		return status.Error(codes.FailedPrecondition, "prepared_setup_not_completable")
	}
	if _, err := s.checkVolumeBackend(ctx, p.binding.BackendId); err != nil {
		return err
	}
	// This commits only credential setup. Execution still requires the registry's
	// separate activation authorization and an exact-binding gate removal.
	patched, err := pods.Patch(ctx, pod.Name, types.JSONPatchType, preparedObjectPatch(pod.ObjectMeta,
		preparedPatchOperation{"replace", "/metadata/annotations/agyn.io~1prepared-state", "prepared"}), metav1.PatchOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(patched, p.binding); err != nil {
		return err
	}
	if patched.DeletionTimestamp != nil || patched.Spec.NodeName != "" || !hasPreparedGate(patched) || patched.Annotations[preparedStateAnnotation] != "prepared" {
		return status.Error(codes.FailedPrecondition, "prepared_setup_unconfirmed")
	}
	return nil
}

func hasPreparedGate(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool { return gate.Name == preparedGate })
}

func (s *Server) matchPreparedPod(pod *corev1.Pod, expected *runnerv1.WorkloadBinding) error {
	if pod == nil || pod.Name != podNameFromID(expected.WorkloadId) || pod.Namespace != s.namespace || string(pod.UID) != expected.InstanceUid ||
		pod.ResourceVersion == "" || pod.Labels[managedByLabelKey] != managedByLabelValue || pod.Labels[workloadIDLabelKey] != expected.WorkloadId {
		return status.Error(codes.FailedPrecondition, "prepared_workload_identity_mismatch")
	}
	if err := matchAnchoredMetadata(pod.ObjectMeta, expected.Anchor); err != nil {
		return err
	}
	stored := &runnerv1.WorkloadBinding{}
	if err := protojson.Unmarshal([]byte(pod.Annotations[preparedBindingAnnotation]), stored); err != nil || stored.InstanceUid != "" {
		return status.Error(codes.FailedPrecondition, "prepared_workload_binding_missing")
	}
	stored.InstanceUid = string(pod.UID)
	stored, err := canonicalBinding(stored)
	if err != nil || !proto.Equal(stored, expected) {
		return status.Error(codes.FailedPrecondition, "prepared_workload_binding_mismatch")
	}
	names := map[string]bool{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			name := volume.PersistentVolumeClaim.ClaimName
			if names[name] {
				return status.Error(codes.FailedPrecondition, "ambiguous_prepared_pod_volumes")
			}
			names[name] = true
		}
	}
	if len(names) != len(expected.Volumes) {
		return status.Error(codes.FailedPrecondition, "prepared_pod_volumes_mismatch")
	}
	for _, volume := range expected.Volumes {
		if !names[volume.InstanceId] {
			return status.Error(codes.FailedPrecondition, "prepared_pod_volumes_mismatch")
		}
	}
	return nil
}

type preparedPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

func preparedObjectPatch(meta metav1.ObjectMeta, changes ...preparedPatchOperation) []byte {
	operations := []preparedPatchOperation{{"test", "/metadata/uid", string(meta.UID)}, {"test", "/metadata/resourceVersion", meta.ResourceVersion}}
	operations = append(operations, changes...)
	data, err := json.Marshal(operations)
	if err != nil {
		panic(err) // Callers supply only Kubernetes metadata, strings and gates.
	}
	return data
}

func (s *Server) ActivateWorkload(ctx context.Context, req *runnerv1.ActivateWorkloadRequest) (*runnerv1.ActivateWorkloadResponse, error) {
	expected, err := canonicalBinding(req.GetExpected())
	if err != nil {
		return nil, err
	}
	if err := s.requireBindingAnchors(ctx, expected); err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	pods := s.clientset.CoreV1().Pods(s.namespace)
	pod, err := pods.Get(ctx, podNameFromID(expected.WorkloadId), metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := s.matchPreparedPod(pod, expected); err != nil {
		return nil, err
	}
	active := pod.Annotations[preparedStateAnnotation] == "active"
	if pod.DeletionTimestamp != nil || active && hasPreparedGate(pod) || !active && (pod.Annotations[preparedStateAnnotation] != "prepared" || !hasPreparedGate(pod) || pod.Spec.NodeName != "") {
		return nil, status.Error(codes.FailedPrecondition, "prepared_workload_not_activatable")
	}
	if err := s.claimAnchorPod(ctx, expected, true); err != nil {
		return nil, err
	}
	hold := preparedHoldPrefix + expected.InstanceUid
	claims := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace)
	for _, target := range expected.Volumes {
		pvc, err := claims.Get(ctx, target.InstanceId, metav1.GetOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := matchPreparedPVC(pvc, target, s.namespace); err != nil {
			return nil, err
		}
		if pvc.DeletionTimestamp != nil || pvc.Status.Phase == corev1.ClaimLost {
			return nil, status.Error(codes.FailedPrecondition, "prepared_volume_not_activatable")
		}
		for _, finalizer := range pvc.Finalizers {
			if strings.HasPrefix(finalizer, preparedHoldPrefix) && finalizer != hold {
				return nil, status.Error(codes.FailedPrecondition, "prepared_volume_in_use")
			}
		}
		if !slices.Contains(pvc.Finalizers, hold) {
			if active {
				return nil, status.Error(codes.FailedPrecondition, "active_volume_protection_missing")
			}
			finalizers := append(slices.Clone(pvc.Finalizers), hold)
			patched, err := claims.Patch(ctx, pvc.Name, types.JSONPatchType, preparedObjectPatch(pvc.ObjectMeta, preparedPatchOperation{"add", "/metadata/finalizers", finalizers}), metav1.PatchOptions{})
			if err != nil {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
			if err := matchPreparedPVC(patched, target, s.namespace); err != nil {
				return nil, err
			}
			if patched.DeletionTimestamp != nil || !slices.Contains(patched.Finalizers, hold) {
				return nil, status.Error(codes.FailedPrecondition, "prepared_volume_protection_unconfirmed")
			}
		}
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	if !active {
		if err := s.requireBindingAnchors(ctx, expected); err != nil {
			return nil, err
		}
		gates := slices.DeleteFunc(slices.Clone(pod.Spec.SchedulingGates), func(gate corev1.PodSchedulingGate) bool { return gate.Name == preparedGate })
		if gates == nil {
			gates = []corev1.PodSchedulingGate{}
		}
		// Holds are retained even when this PATCH fails or its reply is lost.
		// Only confirmed absence of this exact Pod may release them. UID/RV tests
		// prevent a delayed activation from enabling a replacement/terminating Pod.
		patched, err := pods.Patch(ctx, pod.Name, types.JSONPatchType, preparedObjectPatch(pod.ObjectMeta,
			preparedPatchOperation{"replace", "/spec/schedulingGates", gates},
			preparedPatchOperation{"replace", "/metadata/annotations/agyn.io~1prepared-state", "active"}), metav1.PatchOptions{})
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := s.matchPreparedPod(patched, expected); err != nil {
			return nil, err
		}
		if hasPreparedGate(patched) || patched.Annotations[preparedStateAnnotation] != "active" {
			return nil, status.Error(codes.FailedPrecondition, "workload_activation_unconfirmed")
		}
	}
	return &runnerv1.ActivateWorkloadResponse{Binding: expected}, nil
}

func (s *Server) RemovePreparedWorkload(ctx context.Context, req *runnerv1.RemovePreparedWorkloadRequest) (*runnerv1.RemovePreparedWorkloadResponse, error) {
	expected, err := canonicalBinding(req.GetExpected())
	if err != nil {
		return nil, err
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	pods := s.clientset.CoreV1().Pods(s.namespace)
	pod, err := pods.Get(ctx, podNameFromID(expected.WorkloadId), metav1.GetOptions{})
	if err == nil {
		if err := s.matchPreparedPod(pod, expected); err != nil {
			return nil, err
		}
		if pod.DeletionTimestamp == nil {
			err := pods.Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID, ResourceVersion: &pod.ResourceVersion}})
			if err != nil {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
		}
		return &runnerv1.RemovePreparedWorkloadResponse{State: runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING, Binding: expected}, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	// Activation never creates a Pod. After this UID is absent, a delayed gate
	// PATCH cannot execute, even if a new Pod appears with the old name.
	hold := preparedHoldPrefix + expected.InstanceUid
	claims := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace)
	for _, target := range expected.Volumes {
		pvc, err := claims.Get(ctx, target.InstanceId, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}
		if err := matchPreparedPVC(pvc, target, s.namespace); err != nil {
			return nil, err
		}
		if slices.Contains(pvc.Finalizers, hold) {
			finalizers := slices.DeleteFunc(slices.Clone(pvc.Finalizers), func(value string) bool { return value == hold })
			if _, err := claims.Patch(ctx, pvc.Name, types.JSONPatchType, preparedObjectPatch(pvc.ObjectMeta, preparedPatchOperation{"add", "/metadata/finalizers", finalizers}), metav1.PatchOptions{}); err != nil {
				return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
			}
		}
	}
	if _, err := s.checkVolumeBackend(ctx, expected.BackendId); err != nil {
		return nil, err
	}
	return &runnerv1.RemovePreparedWorkloadResponse{State: runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT, Binding: expected}, nil
}
