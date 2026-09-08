package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
)

type dockerConfig struct {
	Auths map[string]dockerAuth `json:"auths"`
}

type dockerAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

func (s *Server) StartWorkload(ctx context.Context, req *runnerv1.StartWorkloadRequest) (*runnerv1.StartWorkloadResponse, error) {
	if req == nil || req.Main == nil {
		return nil, status.Error(codes.InvalidArgument, "main_container_required")
	}
	if strings.TrimSpace(req.Main.Image) == "" {
		return nil, status.Error(codes.InvalidArgument, "main_container_image_required")
	}

	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		workloadID = uuid.NewString()
	} else if _, err := uuid.Parse(workloadID); err != nil {
		return nil, status.Error(codes.InvalidArgument, "workload_id_invalid")
	}
	podName := podNameFromID(workloadID)

	labels, err := buildLabels(workloadID, req.AdditionalProperties, req.Labels)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid_label: %v", err)
	}

	capabilityPlan, err := resolveCapabilityPlan(req, s.capabilityImplementations)
	if err != nil {
		return nil, err
	}

	imagePullSecrets, secretNames, err := s.buildImagePullSecrets(ctx, workloadID, req.ImagePullCredentials)
	if err != nil {
		return nil, err
	}

	volumes, pvcNames, err := s.buildVolumes(ctx, req.Volumes, labels)
	if err != nil {
		return nil, err
	}

	inlineFiles, err := validateInlineFiles(req)
	if err != nil {
		s.deleteImagePullSecrets(ctx, workloadID, secretNames)
		return nil, err
	}
	inlineSecretName, inlineFileKeys, err := s.createInlineFilesSecret(ctx, workloadID, inlineFiles)
	if err != nil {
		s.deleteImagePullSecrets(ctx, workloadID, secretNames)
		return nil, err
	}
	if inlineSecretName != "" {
		secretNames = append(secretNames, inlineSecretName)
		volumes = append(volumes, corev1.Volume{
			Name: inlineFilesVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: inlineSecretName},
			},
		})
	}

	containers, initContainers, sidecarNames, err := buildContainers(req, volumes, inlineFileKeys)
	if err != nil {
		s.deleteImagePullSecrets(ctx, workloadID, secretNames)
		return nil, err
	}
	hostUsers := capabilityPlan.apply(&containers, &initContainers, &volumes, &sidecarNames)

	annotations := map[string]string{}
	if len(pvcNames) > 0 {
		annotations[pvcAnnotationKey] = strings.Join(pvcNames, ",")
	}
	if len(secretNames) > 0 {
		annotations[secretAnnotationKey] = strings.Join(secretNames, ",")
	}
	if capabilityPlan.dockerImplementation == config.DockerImplementationRootless {
		annotations[dockerAppArmorLegacyAnnotationKey] = dockerSecurityProfileUnconfined
		annotations[dockerSeccompPodAnnotationKey] = dockerSecurityProfileUnconfined
		annotations[dockerSeccompContainerAnnotationKey] = dockerSecurityProfileUnconfined
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   s.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: initContainers,
			Containers:     containers,
			Volumes:        volumes,
			// Pull credentials live as secrets alongside every other
			// workload's in this shared namespace. Without this a workload
			// with API access could read its neighbours'.
			AutomountServiceAccountToken: ptr.To(false),
		},
	}
	if hostUsers != nil {
		pod.Spec.HostUsers = hostUsers
	}
	if capabilityPlan.runtimeClassName != nil {
		pod.Spec.RuntimeClassName = capabilityPlan.runtimeClassName
	}

	if len(imagePullSecrets) > 0 {
		pod.Spec.ImagePullSecrets = imagePullSecrets
	}

	if req.DnsConfig != nil && len(req.DnsConfig.Nameservers) > 0 {
		pod.Spec.DNSPolicy = corev1.DNSNone
		pod.Spec.DNSConfig = &corev1.PodDNSConfig{
			Nameservers: req.DnsConfig.Nameservers,
			Searches:    req.DnsConfig.Searches,
		}
	}

	if _, err := s.clientset.CoreV1().Pods(s.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		s.deleteImagePullSecrets(ctx, workloadID, secretNames)
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	sidecars := make([]*runnerv1.SidecarInstance, 0, len(sidecarNames))
	for _, name := range sidecarNames {
		sidecars = append(sidecars, &runnerv1.SidecarInstance{
			Name:   name,
			Id:     fmt.Sprintf("%s:%s", podName, name),
			Status: "starting",
		})
	}

	return &runnerv1.StartWorkloadResponse{
		Id: workloadID,
		Containers: &runnerv1.WorkloadContainers{
			Main:     podName,
			Sidecars: sidecars,
		},
		Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
	}, nil
}

func (s *Server) StopWorkload(ctx context.Context, req *runnerv1.StopWorkloadRequest) (*runnerv1.StopWorkloadResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}

	podName := podNameFromID(workloadID)

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	secretNames := parseSecretAnnotation(pod.Annotations)

	deleteOptions := metav1.DeleteOptions{}
	if grace := int64(req.GetTimeoutSec()); grace > 0 {
		deleteOptions.GracePeriodSeconds = &grace
	}
	if err := s.clientset.CoreV1().Pods(s.namespace).Delete(ctx, podName, deleteOptions); err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	s.deleteImagePullSecrets(ctx, workloadID, secretNames)

	return &runnerv1.StopWorkloadResponse{}, nil
}

func (s *Server) RemoveWorkload(ctx context.Context, req *runnerv1.RemoveWorkloadRequest) (*runnerv1.RemoveWorkloadResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}

	podName := podNameFromID(workloadID)

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	secretNames := parseSecretAnnotation(pod.Annotations)

	var grace *int64
	if req.GetForce() {
		zero := int64(0)
		grace = &zero
	}
	deleteOptions := metav1.DeleteOptions{GracePeriodSeconds: grace}
	if err := s.clientset.CoreV1().Pods(s.namespace).Delete(ctx, podName, deleteOptions); err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	s.deleteImagePullSecrets(ctx, workloadID, secretNames)

	if req.GetRemoveVolumes() {
		pvcNames := parsePVCAnnotation(pod.Annotations)
		var deleteErrs []error
		for _, pvc := range pvcNames {
			if err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Delete(ctx, pvc, metav1.DeleteOptions{}); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				deleteErrs = append(deleteErrs, fmt.Errorf("delete pvc %s: %w", pvc, err))
			}
		}
		if len(deleteErrs) > 0 {
			s.logger.Error("failed to delete pvcs", zap.String("workload_id", workloadID), zap.Errors("errors", deleteErrs))
			return nil, status.Error(codes.Internal, "pvc_cleanup_failed")
		}
	}

	return &runnerv1.RemoveWorkloadResponse{}, nil
}

func (s *Server) InspectWorkload(ctx context.Context, req *runnerv1.InspectWorkloadRequest) (*runnerv1.InspectWorkloadResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}

	podName := podNameFromID(workloadID)

	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}
	if len(pod.Spec.Containers) == 0 {
		return nil, status.Error(codes.Internal, "pod_missing_containers")
	}

	mainContainer := pod.Spec.Containers[0]
	image := mainContainer.Image
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == mainContainer.Name {
			image = status.Image
			break
		}
	}

	stateStatus := strings.ToLower(string(pod.Status.Phase))
	stateRunning := pod.Status.Phase == corev1.PodRunning
	containers := workloadContainersForPod(pod)

	return &runnerv1.InspectWorkloadResponse{
		Id:           workloadID,
		Name:         workloadID,
		Image:        image,
		ConfigImage:  mainContainer.Image,
		ConfigLabels: pod.Labels,
		Mounts:       mountsForPod(pod, mainContainer.Name),
		StateStatus:  stateStatus,
		StateRunning: stateRunning,
		Containers:   containers,
	}, nil
}

func workloadContainersForPod(pod *corev1.Pod) []*runnerv1.WorkloadContainer {
	if pod == nil {
		return nil
	}

	initStatuses := containerStatusLookup(pod.Status.InitContainerStatuses)
	containerStatuses := containerStatusLookup(pod.Status.ContainerStatuses)
	containers := make([]*runnerv1.WorkloadContainer, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))

	for _, container := range pod.Spec.InitContainers {
		containers = append(containers, workloadContainerFromSpec(container, containerStatusForName(initStatuses, container.Name), runnerv1.ContainerRole_CONTAINER_ROLE_INIT))
	}

	if len(pod.Spec.Containers) == 0 {
		return containers
	}

	main := pod.Spec.Containers[0]
	containers = append(containers, workloadContainerFromSpec(main, containerStatusForName(containerStatuses, main.Name), runnerv1.ContainerRole_CONTAINER_ROLE_MAIN))
	for _, sidecar := range pod.Spec.Containers[1:] {
		containers = append(containers, workloadContainerFromSpec(sidecar, containerStatusForName(containerStatuses, sidecar.Name), runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR))
	}

	return containers
}

func containerStatusLookup(statuses []corev1.ContainerStatus) map[string]corev1.ContainerStatus {
	lookup := make(map[string]corev1.ContainerStatus, len(statuses))
	for _, status := range statuses {
		lookup[status.Name] = status
	}
	return lookup
}

func containerStatusForName(lookup map[string]corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	status, ok := lookup[name]
	if !ok {
		return nil
	}
	return &status
}

func workloadContainerFromSpec(spec corev1.Container, status *corev1.ContainerStatus, role runnerv1.ContainerRole) *runnerv1.WorkloadContainer {
	image := spec.Image
	containerID := ""
	restartCount := int32(0)
	containerStatus := runnerv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED
	var reason *string
	var message *string
	var exitCode *int32
	var startedAt *timestamppb.Timestamp
	var finishedAt *timestamppb.Timestamp

	if status != nil {
		if status.Image != "" {
			image = status.Image
		}
		containerID = status.ContainerID
		restartCount = status.RestartCount
		containerStatus, reason, message, exitCode, startedAt, finishedAt = containerState(status.State)
	}

	return &runnerv1.WorkloadContainer{
		ContainerId:  containerID,
		Name:         spec.Name,
		Role:         role,
		Image:        image,
		Status:       containerStatus,
		Reason:       reason,
		Message:      message,
		ExitCode:     exitCode,
		RestartCount: restartCount,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
	}
}

func containerState(state corev1.ContainerState) (runnerv1.ContainerStatus, *string, *string, *int32, *timestamppb.Timestamp, *timestamppb.Timestamp) {
	if state.Running != nil {
		return runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING, nil, nil, nil, timestampOrNil(state.Running.StartedAt), nil
	}
	if state.Waiting != nil {
		reason := optionalString(state.Waiting.Reason)
		message := optionalString(state.Waiting.Message)
		return runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING, reason, message, nil, nil, nil
	}
	if state.Terminated != nil {
		reason := optionalString(state.Terminated.Reason)
		message := optionalString(state.Terminated.Message)
		exitCode := int32(state.Terminated.ExitCode)
		return runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, reason, message, &exitCode, timestampOrNil(state.Terminated.StartedAt), timestampOrNil(state.Terminated.FinishedAt)
	}

	return runnerv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED, nil, nil, nil, nil, nil
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func timestampOrNil(timestamp metav1.Time) *timestamppb.Timestamp {
	if timestamp.IsZero() {
		return nil
	}
	return timestamppb.New(timestamp.Time)
}

func (s *Server) TouchWorkload(ctx context.Context, req *runnerv1.TouchWorkloadRequest) (*runnerv1.TouchWorkloadResponse, error) {
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}

	podName := podNameFromID(workloadID)

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	patch := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%s"}}}`, touchedAtAnnotationKey, timestamp)
	if _, err := s.clientset.CoreV1().Pods(s.namespace).Patch(ctx, podName, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	return &runnerv1.TouchWorkloadResponse{}, nil
}

func buildLabels(workloadID string, additional map[string]string, explicit map[string]string) (map[string]string, error) {
	labels := map[string]string{
		managedByLabelKey:         managedByLabelValue,
		workloadManagedByLabelKey: workloadManagedByLabelValue,
		workloadIDLabelKey:        workloadID,
	}

	for key, value := range additional {
		if !strings.HasPrefix(key, "label.") {
			continue
		}
		labelKey := strings.TrimPrefix(key, "label.")
		if labelKey == "" {
			return nil, fmt.Errorf("empty label key")
		}
		if err := addLabel(labels, labelKey, value); err != nil {
			return nil, err
		}
	}

	if err := addLabels(labels, explicit); err != nil {
		return nil, err
	}

	return labels, nil
}

func addLabels(target map[string]string, source map[string]string) error {
	for key, value := range source {
		if err := addLabel(target, key, value); err != nil {
			return err
		}
	}
	return nil
}

func addLabel(target map[string]string, labelKey, value string) error {
	if labelKey == "" {
		return fmt.Errorf("empty label key")
	}
	if labelKey == managedByLabelKey || labelKey == workloadManagedByLabelKey || labelKey == workloadIDLabelKey {
		return fmt.Errorf("reserved label key %q", labelKey)
	}
	if errs := validation.IsQualifiedName(labelKey); len(errs) > 0 {
		return fmt.Errorf("invalid label key %q: %s", labelKey, strings.Join(errs, ", "))
	}
	if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
		return fmt.Errorf("invalid label value for %q: %s", labelKey, strings.Join(errs, ", "))
	}
	target[labelKey] = value
	return nil
}

func buildDockerConfigJSON(registry, username, password string) ([]byte, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	config := dockerConfig{
		Auths: map[string]dockerAuth{
			registry: {
				Username: username,
				Password: password,
				Auth:     auth,
			},
		},
	}
	return json.Marshal(config)
}

func (s *Server) buildImagePullSecrets(
	ctx context.Context,
	workloadID string,
	credentials []*runnerv1.ImagePullCredential,
) ([]corev1.LocalObjectReference, []string, error) {
	if len(credentials) == 0 {
		return nil, nil, nil
	}

	type validatedCredential struct {
		registry string
		username string
		password string
	}

	validated := make([]validatedCredential, 0, len(credentials))
	for _, credential := range credentials {
		if credential == nil {
			return nil, nil, status.Error(codes.InvalidArgument, "image_pull_credential_required")
		}
		registry := strings.TrimSpace(credential.GetRegistry())
		if registry == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "image_pull_registry_required")
		}
		username := strings.TrimSpace(credential.GetUsername())
		if username == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "image_pull_username_required")
		}
		password := credential.GetPassword()
		if password == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "image_pull_password_required")
		}
		validated = append(validated, validatedCredential{
			registry: registry,
			username: username,
			password: password,
		})
	}

	// One Secret per workload rather than one per credential. Every catalog
	// image resolves to the same image proxy host, so the auths map holds one
	// entry: the key selects which credential, the request path selects which
	// image, and only the proxy sees both. The indexed form is kept for a spec
	// that still carries several upstream registries.
	secretRefs := make([]corev1.LocalObjectReference, 0, len(validated))
	secretNames := make([]string, 0, len(validated))
	for idx, credential := range validated {
		secretName := fmt.Sprintf("workload-%s-pull", workloadID)
		if len(validated) > 1 {
			secretName = fmt.Sprintf("workload-%s-pull-%d", workloadID, idx)
		}
		configJSON, err := buildDockerConfigJSON(credential.registry, credential.username, credential.password)
		if err != nil {
			s.deleteImagePullSecrets(ctx, workloadID, secretNames)
			return nil, nil, status.Errorf(codes.Internal, "docker_config_json_failed: %v", err)
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: s.namespace,
				Labels: map[string]string{
					managedByLabelKey:  managedByLabelValue,
					workloadIDLabelKey: workloadID,
				},
			},
			Type: corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{
				corev1.DockerConfigJsonKey: configJSON,
			},
		}

		if _, err := s.clientset.CoreV1().Secrets(s.namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			s.deleteImagePullSecrets(ctx, workloadID, secretNames)
			return nil, nil, grpcErrorFromKube(s.logger, err, codes.Internal)
		}

		secretRefs = append(secretRefs, corev1.LocalObjectReference{Name: secretName})
		secretNames = append(secretNames, secretName)
	}

	return secretRefs, secretNames, nil
}

func (s *Server) deleteImagePullSecrets(ctx context.Context, workloadID string, secretNames []string) {
	if len(secretNames) == 0 {
		return
	}

	var deleteErrs []error
	for _, secretName := range secretNames {
		if err := s.clientset.CoreV1().Secrets(s.namespace).Delete(ctx, secretName, metav1.DeleteOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			deleteErrs = append(deleteErrs, fmt.Errorf("delete secret %s: %w", secretName, err))
		}
	}

	if len(deleteErrs) > 0 {
		s.logger.Error("failed to delete pull secrets", zap.String("workload_id", workloadID), zap.Errors("errors", deleteErrs))
	}
}

func (s *Server) buildVolumes(ctx context.Context, volumes []*runnerv1.VolumeSpec, labels map[string]string) ([]corev1.Volume, []string, error) {
	volumeNames := make(map[string]struct{})
	createdVolumes := make([]corev1.Volume, 0, len(volumes))
	pvcNames := make([]string, 0)
	for _, volume := range volumes {
		if volume == nil {
			continue
		}
		name := strings.TrimSpace(volume.Name)
		if name == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "volume_name_required")
		}
		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
			return nil, nil, status.Errorf(codes.InvalidArgument, "invalid_volume_name: %s", strings.Join(errs, ", "))
		}
		if _, exists := volumeNames[name]; exists {
			return nil, nil, status.Errorf(codes.InvalidArgument, "duplicate_volume_name: %s", name)
		}
		volumeNames[name] = struct{}{}

		switch volume.Kind {
		case runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL:
			createdVolumes = append(createdVolumes, corev1.Volume{
				Name: name,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			})
		case runnerv1.VolumeKind_VOLUME_KIND_NAMED:
			pvcName, err := s.ensurePVC(ctx, volume, labels)
			if err != nil {
				return nil, nil, err
			}
			createdVolumes = append(createdVolumes, corev1.Volume{
				Name: name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			})
			pvcNames = append(pvcNames, pvcName)
		default:
			return nil, nil, status.Error(codes.InvalidArgument, "volume_kind_required")
		}
	}

	return createdVolumes, pvcNames, nil
}

func (s *Server) ensurePVC(ctx context.Context, volume *runnerv1.VolumeSpec, labels map[string]string) (string, error) {
	pvcName := strings.TrimSpace(volume.PersistentName)
	if pvcName == "" {
		pvcName = strings.TrimSpace(volume.Name)
	}
	if pvcName == "" {
		return "", status.Error(codes.InvalidArgument, "pvc_name_required")
	}
	if errs := validation.IsDNS1123Label(pvcName); len(errs) > 0 {
		return "", status.Errorf(codes.InvalidArgument, "invalid_pvc_name: %s", strings.Join(errs, ", "))
	}

	if _, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Get(ctx, pvcName, metav1.GetOptions{}); err == nil {
		return pvcName, nil
	} else if !apierrors.IsNotFound(err) {
		return "", grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	requestSize, err := resource.ParseQuantity(s.storageSize)
	if err != nil {
		return "", status.Errorf(codes.Internal, "invalid_storage_size: %v", err)
	}
	if size := strings.TrimSpace(volume.GetSize()); size != "" {
		requestSize, err = resource.ParseQuantity(size)
		if err != nil {
			return "", status.Errorf(codes.InvalidArgument, "invalid_volume_size: %v", err)
		}
	}

	storageClass := s.storageClass
	if name := strings.TrimSpace(volume.GetStorageClass()); name != "" {
		resolved, ok := s.catalog.StorageClassNameFor(name)
		if !ok {
			return "", status.Errorf(codes.InvalidArgument, "unknown_storage_class: %s", name)
		}
		// An entry mapping to "" means the cluster default: nil, not a pointer
		// to "", which would disable dynamic provisioning.
		storageClass = nil
		if resolved != "" {
			storageClass = &resolved
		}
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: s.namespace,
			Labels: map[string]string{
				managedByLabelKey: managedByLabelValue,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: requestSize},
			},
		},
	}
	if storageClass != nil {
		pvc.Spec.StorageClassName = storageClass
	}
	for key, value := range labels {
		if key == workloadIDLabelKey {
			continue
		}
		pvc.Labels[key] = value
	}
	if err := addLabels(pvc.Labels, volume.GetLabels()); err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid_volume_label: %v", err)
	}

	if _, err := s.clientset.CoreV1().PersistentVolumeClaims(s.namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return "", grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	s.logger.Info("created pvc", zap.String("pvc", pvcName))
	return pvcName, nil
}

func buildContainers(req *runnerv1.StartWorkloadRequest, volumes []corev1.Volume, inlineFileKeys map[string]string) ([]corev1.Container, []corev1.Container, []string, error) {
	volumeLookup := make(map[string]struct{}, len(volumes))
	for _, volume := range volumes {
		volumeLookup[volume.Name] = struct{}{}
	}

	containers := make([]corev1.Container, 0, 1+len(req.Sidecars))
	initContainers := make([]corev1.Container, 0, len(req.InitContainers))
	nameLookup := make(map[string]struct{}, 1+len(req.Sidecars)+len(req.InitContainers))

	mainContainer, err := buildContainer(req.Main, "main", volumeLookup, inlineFileKeys)
	if err != nil {
		return nil, nil, nil, err
	}
	containers = append(containers, mainContainer)
	nameLookup[mainContainer.Name] = struct{}{}

	sidecarNames := make([]string, 0, len(req.Sidecars))
	for idx, sidecar := range req.Sidecars {
		container, err := buildContainer(sidecar, fmt.Sprintf("sidecar-%d", idx+1), volumeLookup, inlineFileKeys)
		if err != nil {
			return nil, nil, nil, err
		}
		if _, exists := nameLookup[container.Name]; exists {
			return nil, nil, nil, status.Errorf(codes.InvalidArgument, "duplicate_container_name: %s", container.Name)
		}
		nameLookup[container.Name] = struct{}{}
		containers = append(containers, container)
		sidecarNames = append(sidecarNames, container.Name)
	}

	for idx, initContainer := range req.InitContainers {
		container, err := buildContainer(initContainer, fmt.Sprintf("init-%d", idx+1), volumeLookup, inlineFileKeys)
		if err != nil {
			return nil, nil, nil, err
		}
		if _, exists := nameLookup[container.Name]; exists {
			return nil, nil, nil, status.Errorf(codes.InvalidArgument, "duplicate_container_name: %s", container.Name)
		}
		nameLookup[container.Name] = struct{}{}
		initContainers = append(initContainers, container)
	}

	return containers, initContainers, sidecarNames, nil
}

func buildContainer(spec *runnerv1.ContainerSpec, fallbackName string, volumeLookup map[string]struct{}, inlineFileKeys map[string]string) (corev1.Container, error) {
	if spec == nil {
		return corev1.Container{}, status.Error(codes.InvalidArgument, "container_spec_required")
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		name = fallbackName
	}
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return corev1.Container{}, status.Errorf(codes.InvalidArgument, "invalid_container_name: %s", strings.Join(errs, ", "))
	}
	image := strings.TrimSpace(spec.Image)
	if image == "" {
		return corev1.Container{}, status.Error(codes.InvalidArgument, "container_image_required")
	}

	volumeMounts := make([]corev1.VolumeMount, 0, len(spec.Mounts)+len(spec.InlineFileMounts))
	for _, mount := range spec.Mounts {
		if mount == nil {
			continue
		}
		volumeName := strings.TrimSpace(mount.Volume)
		if volumeName == "" {
			return corev1.Container{}, status.Error(codes.InvalidArgument, "volume_mount_name_required")
		}
		if _, ok := volumeLookup[volumeName]; !ok {
			return corev1.Container{}, status.Errorf(codes.InvalidArgument, "volume_not_defined: %s", volumeName)
		}
		mountPath := strings.TrimSpace(mount.MountPath)
		if mountPath == "" {
			return corev1.Container{}, status.Error(codes.InvalidArgument, "mount_path_required")
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			ReadOnly:  mount.ReadOnly,
		})
	}

	for _, mount := range spec.InlineFileMounts {
		if mount == nil {
			continue
		}
		mountPath := strings.TrimSpace(mount.GetPath())
		secretKey, ok := inlineFileKeys[mountPath]
		if !ok {
			return corev1.Container{}, status.Errorf(codes.InvalidArgument, "inline_file_not_defined: %s", mountPath)
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      inlineFilesVolumeName,
			MountPath: mountPath,
			SubPath:   secretKey,
			ReadOnly:  true,
		})
	}

	var envVars []corev1.EnvVar
	for _, env := range spec.Env {
		if env == nil {
			continue
		}
		name := strings.TrimSpace(env.Name)
		if name == "" {
			return corev1.Container{}, status.Error(codes.InvalidArgument, "env_name_required")
		}
		if errs := validation.IsEnvVarName(name); len(errs) > 0 {
			return corev1.Container{}, status.Errorf(codes.InvalidArgument, "invalid_env_name: %s", strings.Join(errs, ", "))
		}
		envVars = append(envVars, corev1.EnvVar{Name: name, Value: env.Value})
	}

	container := corev1.Container{
		Name:         name,
		Image:        image,
		Args:         append([]string{}, spec.Cmd...),
		Env:          envVars,
		WorkingDir:   strings.TrimSpace(spec.WorkingDir),
		VolumeMounts: volumeMounts,
	}
	if entrypoint := strings.TrimSpace(spec.Entrypoint); entrypoint != "" {
		if strings.ContainsAny(entrypoint, " \t\n\r") {
			return corev1.Container{}, status.Error(codes.InvalidArgument, "entrypoint_must_be_single_path")
		}
		// Entrypoint is a single binary path; use Cmd for args.
		container.Command = []string{entrypoint}
	}

	if len(spec.RequiredCapabilities) > 0 {
		caps := make([]corev1.Capability, 0, len(spec.RequiredCapabilities))
		for _, capability := range spec.RequiredCapabilities {
			capName := strings.TrimSpace(capability)
			if capName == "" {
				continue
			}
			caps = append(caps, corev1.Capability(capName))
		}
		if len(caps) > 0 {
			container.SecurityContext = &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{
					Add: caps,
				},
			}
		}
	}

	if policy, ok := spec.AdditionalProperties["restart_policy"]; ok && policy == "Always" {
		always := corev1.ContainerRestartPolicyAlways
		container.RestartPolicy = &always
	}

	return container, nil
}

func parseAnnotationList(annotations map[string]string, key string) []string {
	if annotations == nil {
		return nil
	}
	value := strings.TrimSpace(annotations[key])
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			result = append(result, name)
		}
	}
	return result
}

func parsePVCAnnotation(annotations map[string]string) []string {
	return parseAnnotationList(annotations, pvcAnnotationKey)
}

func parseSecretAnnotation(annotations map[string]string) []string {
	return parseAnnotationList(annotations, secretAnnotationKey)
}

type volumeInfo struct {
	mountType string
	source    string
}

func mountsForPod(pod *corev1.Pod, mainContainerName string) []*runnerv1.TargetMount {
	if pod == nil {
		return nil
	}
	volumeSources := make(map[string]volumeInfo)
	for _, volume := range pod.Spec.Volumes {
		switch {
		case volume.PersistentVolumeClaim != nil:
			volumeSources[volume.Name] = volumeInfo{mountType: "pvc", source: volume.PersistentVolumeClaim.ClaimName}
		case volume.EmptyDir != nil:
			volumeSources[volume.Name] = volumeInfo{mountType: "emptydir", source: volume.Name}
		}
	}

	var mounts []*runnerv1.TargetMount
	for _, container := range pod.Spec.Containers {
		if container.Name != mainContainerName {
			continue
		}
		for _, mount := range container.VolumeMounts {
			info, ok := volumeSources[mount.Name]
			if !ok {
				continue
			}
			mounts = append(mounts, &runnerv1.TargetMount{
				Type:        info.mountType,
				Source:      info.source,
				Destination: mount.MountPath,
				ReadOnly:    mount.ReadOnly,
			})
		}
	}

	return mounts
}

func validateInlineFiles(req *runnerv1.StartWorkloadRequest) (map[string][]byte, error) {
	inlineFiles := req.GetInlineFiles()
	if len(inlineFiles) == 0 {
		if hasInlineFileMounts(req) {
			return nil, status.Error(codes.InvalidArgument, "inline_file_not_defined")
		}
		return nil, nil
	}

	for filePath := range inlineFiles {
		if err := validateInlineFilePath(filePath); err != nil {
			return nil, err
		}
	}

	referenced := make(map[string]struct{})
	containers := append([]*runnerv1.ContainerSpec{req.GetMain()}, req.GetSidecars()...)
	containers = append(containers, req.GetInitContainers()...)
	for _, container := range containers {
		if container == nil {
			continue
		}
		for _, mount := range container.GetInlineFileMounts() {
			if mount == nil {
				continue
			}
			mountPath := strings.TrimSpace(mount.GetPath())
			if err := validateInlineFilePath(mountPath); err != nil {
				return nil, err
			}
			if _, ok := inlineFiles[mountPath]; !ok {
				return nil, status.Errorf(codes.InvalidArgument, "inline_file_not_defined: %s", mountPath)
			}
			referenced[mountPath] = struct{}{}
		}
	}

	for filePath := range inlineFiles {
		if _, ok := referenced[filePath]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "inline_file_not_referenced: %s", filePath)
		}
	}

	return inlineFiles, nil
}

func hasInlineFileMounts(req *runnerv1.StartWorkloadRequest) bool {
	containers := append([]*runnerv1.ContainerSpec{req.GetMain()}, req.GetSidecars()...)
	containers = append(containers, req.GetInitContainers()...)
	for _, container := range containers {
		if len(container.GetInlineFileMounts()) > 0 {
			return true
		}
	}
	return false
}

func validateInlineFilePath(filePath string) error {
	if filePath == "" || !path.IsAbs(filePath) || path.Clean(filePath) != filePath {
		return status.Errorf(codes.InvalidArgument, "inline_file_path_invalid: %s", filePath)
	}
	return nil
}

func (s *Server) createInlineFilesSecret(ctx context.Context, workloadID string, inlineFiles map[string][]byte) (string, map[string]string, error) {
	if len(inlineFiles) == 0 {
		return "", nil, nil
	}

	paths := make([]string, 0, len(inlineFiles))
	for filePath := range inlineFiles {
		paths = append(paths, filePath)
	}
	sort.Strings(paths)

	data := make(map[string][]byte, len(paths))
	keys := make(map[string]string, len(paths))
	for idx, filePath := range paths {
		key := fmt.Sprintf("inline-file-%d", idx)
		data[key] = append([]byte(nil), inlineFiles[filePath]...)
		keys[filePath] = key
	}

	secretName := fmt.Sprintf("workload-%s-inline-files", workloadID)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: s.namespace,
			Labels: map[string]string{
				managedByLabelKey:  managedByLabelValue,
				workloadIDLabelKey: workloadID,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	if _, err := s.clientset.CoreV1().Secrets(s.namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return "", nil, grpcErrorFromKube(s.logger, err, codes.Internal)
	}

	return secretName, keys, nil
}
