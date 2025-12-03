{{/*
Expand the name of the chart.
*/}}
{{- define "apollo-agent-minio.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "apollo-agent-minio.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "apollo-agent-minio.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "apollo-agent-minio.labels" -}}
helm.sh/chart: {{ include "apollo-agent-minio.chart" . }}
{{ include "apollo-agent-minio.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "apollo-agent-minio.selectorLabels" -}}
app.kubernetes.io/name: {{ include "apollo-agent-minio.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
MinIO labels
*/}}
{{- define "apollo-agent-minio.minio.labels" -}}
{{ include "apollo-agent-minio.labels" . }}
app.kubernetes.io/component: minio
{{- end }}

{{/*
Apollo Agent labels
*/}}
{{- define "apollo-agent-minio.apollo-agent.labels" -}}
{{ include "apollo-agent-minio.labels" . }}
app.kubernetes.io/component: apollo-agent
{{- end }}

