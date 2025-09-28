{{- define "azolla-benchmark.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "azolla-benchmark.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "azolla-benchmark.labels" -}}
app.kubernetes.io/name: {{ include "azolla-benchmark.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "azolla-benchmark.selectorLabels" -}}
app.kubernetes.io/name: {{ include "azolla-benchmark.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "azolla-benchmark.benchmarkServiceAccountName" -}}
{{- if .Values.benchmarkClient.serviceAccount.name -}}
{{- .Values.benchmarkClient.serviceAccount.name -}}
{{- else -}}
{{- printf "%s-benchmark" (include "azolla-benchmark.fullname" .) -}}
{{- end -}}
{{- end -}}
