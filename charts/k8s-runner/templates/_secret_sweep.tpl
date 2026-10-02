{{/*
The workload rules plus, only when the orphan sweep is enabled, Secret list.
The namespaced Role renders this directly; the cluster-wide path receives the
same list through configureSecretSweep, since service-base reads rbac.rules.
*/}}
{{- define "k8s-runner.workloadRules" -}}
{{- $rules := .Values.rbac.rules -}}
{{- if .Values.workloadSecretSweep.enabled -}}
{{- $rules = append $rules (dict "apiGroups" (list "") "resources" (list "secrets") "verbs" (list "list")) -}}
{{- end -}}
{{- toYaml $rules -}}
{{- end -}}

{{- define "k8s-runner.configureSecretSweep" -}}
{{- with .Values.workloadSecretSweep }}
{{- if .enabled }}
{{- $env := list (dict "name" "WORKLOAD_SECRET_SWEEP_INTERVAL" "value" (required "workloadSecretSweep.interval is required" .interval | toString)) (dict "name" "WORKLOAD_SECRET_SWEEP_GRACE" "value" (required "workloadSecretSweep.grace is required" .grace | toString)) -}}
{{- $_ := set $.Values "extraEnvVars" (concat ($.Values.extraEnvVars | default (list)) $env) -}}
{{- if $.Values.rbac.clusterWide }}
{{- $_ := set $.Values.rbac "rules" (include "k8s-runner.workloadRules" $ | fromYamlArray) -}}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
