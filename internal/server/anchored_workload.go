package server

import (
	"context"
	"maps"
	"reflect"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func matchAnchoredMetadata(meta metav1.ObjectMeta, expected *runnerv1.ResourceAnchor) error {
	if expected == nil {
		if len(meta.OwnerReferences) != 0 || meta.Annotations[resourceAnchorAnnotation] != "" {
			return status.Error(codes.FailedPrecondition, "unexpected_resource_anchor")
		}
		return nil
	}
	if err := validateResourceAnchor(expected, true); err != nil {
		return err
	}
	stored := &runnerv1.ResourceAnchor{}
	data := meta.Annotations[resourceAnchorAnnotation]
	if len(data) > 16*1024 || protojson.Unmarshal([]byte(data), stored) != nil || !proto.Equal(stored, expected) ||
		!reflect.DeepEqual(meta.OwnerReferences, resourceAnchorOwners(expected)) || !maps.Equal(anchorIdentityLabels(expected.Kind, meta.Labels), expected.IdentityLabels) {
		return status.Error(codes.FailedPrecondition, "resource_anchor_ownership_mismatch")
	}
	return nil
}

func attachResourceAnchor(meta *metav1.ObjectMeta, anchor *runnerv1.ResourceAnchor) error {
	if err := validateResourceAnchor(anchor, true); err != nil {
		return err
	}
	data, err := protojson.Marshal(anchor)
	if err != nil || len(data) > 16*1024 || len(meta.OwnerReferences) != 0 {
		return status.Error(codes.InvalidArgument, "resource_anchor_cannot_be_attached")
	}
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	meta.Annotations[resourceAnchorAnnotation] = string(data)
	meta.OwnerReferences = resourceAnchorOwners(anchor)
	return matchAnchoredMetadata(*meta, anchor)
}

func volumeAnchorFromPVC(pvc *corev1.PersistentVolumeClaim, backend string) (*runnerv1.ResourceAnchor, error) {
	var anchor *runnerv1.ResourceAnchor
	if data := pvc.Annotations[resourceAnchorAnnotation]; data != "" {
		anchor = &runnerv1.ResourceAnchor{}
		if len(data) > 16*1024 || protojson.Unmarshal([]byte(data), anchor) != nil || anchor.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME || anchor.ResourceId != pvc.Labels[volumeKeyLabelKey] || anchor.BackendId != backend {
			return nil, status.Error(codes.FailedPrecondition, "volume_anchor_binding_invalid")
		}
	}
	if err := matchAnchoredMetadata(pvc.ObjectMeta, anchor); err != nil {
		return nil, err
	}
	return anchor, nil
}

func validateBindingAnchors(binding *runnerv1.WorkloadBinding) error {
	work := binding.Anchor
	if work != nil {
		if err := validateResourceAnchor(work, true); err != nil {
			return err
		}
		if work.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD || work.ResourceId != binding.WorkloadId || work.BackendId != binding.BackendId {
			return status.Error(codes.InvalidArgument, "workload_anchor_binding_mismatch")
		}
	}
	for _, volume := range binding.Volumes {
		if (work == nil) != (volume.GetAnchor() == nil) {
			return status.Error(codes.InvalidArgument, "mixed_resource_anchor_generation")
		}
		if work != nil {
			owner := maps.Clone(volume.Anchor.IdentityLabels)
			delete(owner, volumeKeyLabelKey)
			if !maps.Equal(owner, volumeIdentityLabels(work.IdentityLabels)) {
				return status.Error(codes.InvalidArgument, "volume_anchor_owner_mismatch")
			}
		}
	}
	return nil
}

func (s *Server) requireBindingAnchors(ctx context.Context, binding *runnerv1.WorkloadBinding) error {
	if binding.Anchor == nil {
		return nil
	}
	if err := s.requireResourceAnchor(ctx, binding.Anchor); err != nil {
		return err
	}
	for _, volume := range binding.Volumes {
		if err := s.requireResourceAnchor(ctx, volume.Anchor); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) claimAnchorPod(ctx context.Context, binding *runnerv1.WorkloadBinding, activation bool) error {
	if binding.Anchor == nil {
		return nil
	}
	cm, err := s.readResourceAnchor(ctx, binding.Anchor)
	if err != nil {
		return err
	}
	key := resourceAnchorPodAnnotation
	if activation {
		if cm.Annotations[key] != binding.InstanceUid {
			return status.Error(codes.FailedPrecondition, "anchor_pod_selection_required")
		}
		key = resourceAnchorActivationAnnotation
	} else if cm.Annotations[resourceAnchorActivationAnnotation] != "" {
		return status.Error(codes.FailedPrecondition, "anchor_already_authorized_activation")
	}
	if selected := cm.Annotations[key]; selected != "" {
		if selected != binding.InstanceUid {
			return status.Error(codes.FailedPrecondition, "anchor_pod_incarnation_changed")
		}
		return nil
	}
	path := "/metadata/annotations/agyn.io~1anchor-pod-uid"
	if activation {
		path = "/metadata/annotations/agyn.io~1anchor-activation"
	}
	// Revocation DELETE and these state transitions contend on the same owner
	// UID/resource version, before any scheduling-gate PATCH can be sent.
	patched, err := s.clientset.CoreV1().ConfigMaps(s.namespace).Patch(ctx, cm.Name, types.JSONPatchType,
		preparedObjectPatch(cm.ObjectMeta, preparedPatchOperation{"add", path, binding.InstanceUid}), metav1.PatchOptions{})
	if err != nil {
		return grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if err := matchResourceAnchor(patched, binding.Anchor, s.namespace); err != nil {
		return err
	}
	if patched.DeletionTimestamp != nil || patched.Annotations[key] != binding.InstanceUid {
		return status.Error(codes.FailedPrecondition, "anchor_pod_claim_unconfirmed")
	}
	return nil
}

func (s *Server) PrepareAnchoredWorkload(ctx context.Context, req *runnerv1.PrepareAnchoredWorkloadRequest) (*runnerv1.PrepareAnchoredWorkloadResponse, error) {
	p := req.GetPreparation()
	work := req.GetWorkloadAnchor()
	if p.GetWorkload() == nil || len(p.Workload.Volumes) > maxPreparedVolumes || len(req.VolumeAnchors) > maxPreparedVolumes {
		return nil, status.Error(codes.InvalidArgument, "anchored_preparation_required")
	}
	if err := validateResourceAnchor(work, true); err != nil {
		return nil, err
	}
	labels, err := buildLabels(p.Workload.WorkloadId, p.Workload.AdditionalProperties, p.Workload.Labels)
	if err != nil || work.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD || work.ResourceId != p.Workload.WorkloadId || work.BackendId != p.BackendId || !maps.Equal(work.IdentityLabels, anchorIdentityLabels(work.Kind, labels)) {
		return nil, status.Error(codes.InvalidArgument, "workload_anchor_intent_mismatch")
	}
	anchors := map[string]*runnerv1.ResourceAnchor{}
	for _, anchor := range req.VolumeAnchors {
		if err := validateResourceAnchor(anchor, true); err != nil {
			return nil, err
		}
		if anchor.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME || anchor.BackendId != p.BackendId || anchors[anchor.ResourceId] != nil {
			return nil, status.Error(codes.InvalidArgument, "volume_anchor_set_invalid")
		}
		anchors[anchor.ResourceId] = proto.Clone(anchor).(*runnerv1.ResourceAnchor)
	}
	seen := map[string]bool{}
	for _, volume := range p.Workload.Volumes {
		if volume.GetKind() != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
			continue
		}
		pvc, err := s.desiredPVC(volume, labels)
		if err != nil {
			return nil, err
		}
		key := pvc.Labels[volumeKeyLabelKey]
		anchor := anchors[key]
		if anchor == nil || seen[key] || !maps.Equal(anchor.IdentityLabels, volumeIdentityLabels(pvc.Labels)) {
			return nil, status.Error(codes.InvalidArgument, "volume_anchor_intent_mismatch")
		}
		seen[key] = true
	}
	if len(seen) != len(anchors) {
		return nil, status.Error(codes.InvalidArgument, "unreferenced_volume_anchor")
	}
	cm, err := s.readResourceAnchor(ctx, work)
	if err != nil {
		return nil, err
	}
	if cm.Annotations[resourceAnchorPodAnnotation] != "" {
		return nil, status.Error(codes.FailedPrecondition, "preparation_anchor_already_consumed")
	}
	for _, anchor := range anchors {
		if err := s.requireResourceAnchor(ctx, anchor); err != nil {
			return nil, err
		}
	}
	response, err := s.prepareWorkload(ctx, p, proto.Clone(work).(*runnerv1.ResourceAnchor), anchors)
	if err != nil {
		return nil, err
	}
	return &runnerv1.PrepareAnchoredWorkloadResponse{Workload: response.Workload, Binding: response.Binding}, nil
}

func (p *workloadPreparation) validatePVC(existing, desired *corev1.PersistentVolumeClaim) error {
	if p.binding.Anchor == nil {
		return validatePVCReuse(existing, desired)
	}
	anchor := p.anchors[desired.Labels[volumeKeyLabelKey]]
	if anchor == nil || existing == nil {
		return status.Error(codes.FailedPrecondition, "anchored_volume_required")
	}
	if err := matchAnchoredMetadata(existing.ObjectMeta, anchor); err != nil {
		return err
	}
	return validatePVCReuseSpec(existing, desired)
}
