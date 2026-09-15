package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	runnerv1 "github.com/agynio/k8s-runner/internal/.gen/agynio/api/runner/v1"
	"github.com/agynio/k8s-runner/internal/config"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type preparedSecretCrashSpec struct {
	Kubeconfig string          `json:"kubeconfig"`
	Namespace  string          `json:"namespace"`
	Account    string          `json:"account"`
	Stage      string          `json:"stage"`
	Request    json.RawMessage `json:"request,omitempty"`
	Revocation json.RawMessage `json:"revocation,omitempty"`
	Adoption   json.RawMessage `json:"adoption,omitempty"`
}

func TestPreparedCrashSpecSeparatesOperations(t *testing.T) {
	for _, operation := range []string{"prepare", "revoke", "adopt"} {
		spec := preparedSecretCrashSpec{}
		if operation == "revoke" {
			spec.Revocation = json.RawMessage(`{"workloadAnchor":{}}`)
		} else if operation == "adopt" {
			spec.Adoption = json.RawMessage(`{"adoption":{}}`)
		} else {
			spec.Request = json.RawMessage(`{"workload":{}}`)
		}
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		var decoded preparedSecretCrashSpec
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if (len(decoded.Request) != 0) != (operation == "prepare") || (len(decoded.Revocation) != 0) != (operation == "revoke") || (len(decoded.Adoption) != 0) != (operation == "adopt") {
			t.Fatal("unused operation serialized as a nonempty request")
		}
	}
}

type preparedSecretTransport func(*http.Request) (*http.Response, error)

func (f preparedSecretTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestPreparedSecretsCrashProcess(t *testing.T) {
	if os.Getenv("RUNNER_PREPARED_SECRET_CHILD") != "trusted-local" {
		t.Skip("requires parent-owned native crash fixture")
	}
	path := os.Getenv("RUNNER_PREPARED_SECRET_SPEC")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute parent fixture file required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var spec preparedSecretCrashSpec
	if err := json.Unmarshal(data, &spec); err != nil || !filepath.IsAbs(spec.Kubeconfig) || !strings.HasPrefix(spec.Namespace, "runner-prepared-") || spec.Account == "" {
		t.Fatal("invalid parent fixture configuration")
	}
	revoking := len(spec.Revocation) != 0
	adopting := len(spec.Adoption) != 0
	stages := []string{"pod-reply", "first-secret-reply", "last-secret-reply", "readiness-reply"}
	if revoking {
		stages = []string{"revocation-claim-reply", "revocation-receipt-reply", "revocation-delete-reply"}
	}
	if adopting {
		stages = []string{"adoption-owner-reply", "adoption-journal-reply", "adoption-pin-reply", "adoption-apply-reply", "adoption-final-owner-reply", "adoption-final-pvc-reply"}
	}
	if !slices.Contains(stages, spec.Stage) || revoking && len(spec.Request) != 0 || adopting && (revoking || len(spec.Request) != 0) {
		t.Fatal("invalid parent fixture operation")
	}
	req := &runnerv1.PrepareWorkloadRequest{}
	revocation := &runnerv1.RevokeWorkloadPreparationRequest{}
	adoptionReserve := &runnerv1.ReserveVolumeAnchorAdoptionRequest{}
	adoptionApply := &runnerv1.ApplyVolumeAnchorAdoptionRequest{}
	adoptionFinalize := &runnerv1.FinalizeVolumeAnchorAdoptionRequest{}
	var adoptionAnchor *runnerv1.ResourceAnchor
	var adoptionPVC string
	if adopting {
		switch spec.Stage {
		case "adoption-apply-reply":
			if err := protojson.Unmarshal(spec.Adoption, adoptionApply); err != nil || validateVolumeAdoption(adoptionApply.Adoption, true) != nil {
				t.Fatal("invalid apply request")
			}
			adoptionAnchor, adoptionPVC = adoptionApply.Adoption.Anchor, adoptionApply.Adoption.Previous.InstanceId
		case "adoption-final-owner-reply", "adoption-final-pvc-reply":
			if err := protojson.Unmarshal(spec.Adoption, adoptionFinalize); err != nil || validateVolumeAdoption(adoptionFinalize.Adoption, true) != nil {
				t.Fatal("invalid finalize request")
			}
			adoptionAnchor, adoptionPVC = adoptionFinalize.Adoption.Anchor, adoptionFinalize.Adoption.Previous.InstanceId
		default:
			if err := protojson.Unmarshal(spec.Adoption, adoptionReserve); err != nil || validateVolumeAdoptionInput(adoptionReserve.Id, adoptionReserve.Expected, adoptionReserve.Intent, false) != nil {
				t.Fatal("invalid reserve request")
			}
			adoptionAnchor, adoptionPVC = adoptionReserve.Intent, adoptionReserve.Expected.InstanceId
		}
	} else if revoking {
		if err := protojson.Unmarshal(spec.Revocation, revocation); err != nil || revocation.WorkloadAnchor == nil {
			t.Fatal("invalid revocation request")
		}
	} else if err := protojson.Unmarshal(spec.Request, req); err != nil || req.Workload == nil {
		t.Fatal("invalid synthetic request")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", spec.Kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + spec.Namespace + ":" + spec.Account,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + spec.Namespace, "system:authenticated"}}
	checkpoint := os.NewFile(3, "parent-checkpoint")
	if checkpoint == nil {
		t.Fatal("parent checkpoint pipe required")
	}
	defer checkpoint.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	secrets := 0
	adoptionCreates := 0
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return preparedSecretTransport(func(request *http.Request) (*http.Response, error) {
			response, err := next.RoundTrip(request)
			if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
				return response, err
			}
			path := "/api/v1/namespaces/" + spec.Namespace
			if request.Method == http.MethodPost && request.URL.Path == path+"/secrets" {
				secrets++
			}
			hit := spec.Stage == "pod-reply" && request.Method == http.MethodPost && request.URL.Path == path+"/pods" ||
				request.Method == http.MethodPost && request.URL.Path == path+"/secrets" && (spec.Stage == "first-secret-reply" && secrets == 1 || spec.Stage == "last-secret-reply" && secrets == 2) ||
				spec.Stage == "readiness-reply" && request.Method == http.MethodPatch && request.URL.Path == path+"/pods/"+podNameFromID(req.Workload.WorkloadId)
			if revoking {
				ownerPath := path + "/configmaps/" + resourceAnchorName(revocation.WorkloadAnchor)
				hit = spec.Stage == "revocation-claim-reply" && request.Method == http.MethodPatch && request.URL.Path == ownerPath ||
					spec.Stage == "revocation-receipt-reply" && request.Method == http.MethodPost && request.URL.Path == path+"/configmaps" ||
					spec.Stage == "revocation-delete-reply" && request.Method == http.MethodDelete && request.URL.Path == ownerPath
			}
			if adopting {
				if request.Method == http.MethodPost && request.URL.Path == path+"/configmaps" {
					adoptionCreates++
				}
				ownerPath, pvcPath := path+"/configmaps/"+resourceAnchorName(adoptionAnchor), path+"/persistentvolumeclaims/"+adoptionPVC
				hit = request.Method == http.MethodPost && request.URL.Path == path+"/configmaps" && (spec.Stage == "adoption-owner-reply" && adoptionCreates == 1 || spec.Stage == "adoption-journal-reply" && adoptionCreates == 2) ||
					request.Method == http.MethodPatch && request.URL.Path == ownerPath && (spec.Stage == "adoption-pin-reply" || spec.Stage == "adoption-final-owner-reply") ||
					request.Method == http.MethodPatch && request.URL.Path == pvcPath && (spec.Stage == "adoption-apply-reply" || spec.Stage == "adoption-final-pvc-reply")
			}
			if hit {
				if _, err := fmt.Fprintln(checkpoint, "committed"); err != nil {
					return nil, err
				}
				<-ctx.Done()
				response.Body.Close()
				return nil, ctx.Err()
			}
			return response, err
		})
	})
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := New(Options{Clientset: client, Namespace: spec.Namespace, StorageSize: "1Mi", Logger: zap.NewNop(),
		SupportingContainerResources: &config.ComputeResources{RequestsCPU: "50m", RequestsMemory: "64Mi", LimitsCPU: "250m", LimitsMemory: "128Mi"}})
	if adopting {
		switch spec.Stage {
		case "adoption-apply-reply":
			_, err = s.ApplyVolumeAnchorAdoption(ctx, adoptionApply)
		case "adoption-final-owner-reply", "adoption-final-pvc-reply":
			_, err = s.FinalizeVolumeAnchorAdoption(ctx, adoptionFinalize)
		default:
			_, err = s.ReserveVolumeAnchorAdoption(ctx, adoptionReserve)
		}
	} else if revoking {
		_, err = s.RevokeWorkloadPreparation(ctx, revocation)
	} else {
		_, err = s.PrepareWorkload(ctx, req)
	}
	t.Fatalf("parent did not kill at the requested write: %v", err)
}

// Start a real runner-method process, wait for a committed API write, then kill
// it before that response reaches the native method. No defer/rollback can run.
func killPreparedSecretProcess(t *testing.T, spec preparedSecretCrashSpec) {
	t.Helper()
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	command := exec.Command(executable, "-test.run=^TestPreparedSecretsCrashProcess$", "-test.timeout=90s")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GOMAXPROCS=2",
		"RUNNER_PREPARED_SECRET_CHILD=trusted-local", "RUNNER_PREPARED_SECRET_SPEC=" + path}
	command.ExtraFiles = []*os.File{writer}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	if err := reader.SetReadDeadline(time.Now().Add(45 * time.Second)); err != nil {
		t.Fatal(err)
	}
	marker := make([]byte, len("committed\n"))
	if _, err := io.ReadFull(reader, marker); err != nil || string(marker) != "committed\n" {
		t.Fatalf("child checkpoint not observed: %v", err)
	}
	if err := command.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("checkpoint process was not alive: %v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	state, ok := command.ProcessState.Sys().(syscall.WaitStatus)
	if err == nil || !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL {
		t.Fatalf("child termination was not SIGKILL: %v", err)
	}
}
