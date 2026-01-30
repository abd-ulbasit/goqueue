{{/*
Expand the name of the chart.
*/}}
{{- define "goqueue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "goqueue.fullname" -}}
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
{{- define "goqueue.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "goqueue.labels" -}}
helm.sh/chart: {{ include "goqueue.chart" . }}
{{ include "goqueue.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "goqueue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "goqueue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "goqueue.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "goqueue.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the headless service name for StatefulSet
*/}}
{{- define "goqueue.headlessServiceName" -}}
{{- printf "%s-headless" (include "goqueue.fullname" .) }}
{{- end }}

{{/*
Create the ConfigMap name
*/}}
{{- define "goqueue.configMapName" -}}
{{- printf "%s-config" (include "goqueue.fullname" .) }}
{{- end }}

{{/*
Create the secret name
*/}}
{{- define "goqueue.secretName" -}}
{{- printf "%s-secret" (include "goqueue.fullname" .) }}
{{- end }}

{{/*
Return the image name
*/}}
{{- define "goqueue.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Return the cluster peers list
This creates a comma-separated list of all peer addresses in the StatefulSet
*/}}
{{- define "goqueue.clusterPeers" -}}
{{- $fullname := include "goqueue.fullname" . -}}
{{- $headless := include "goqueue.headlessServiceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $port := int .Values.service.internalPort -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $peers = append $peers (printf "%s-%d.%s.%s.svc.cluster.local:%d" $fullname $i $headless $namespace $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
Return the HTTP service port
*/}}
{{- define "goqueue.httpPort" -}}
{{- .Values.service.httpPort | default 8080 }}
{{- end }}

{{/*
Return the gRPC service port
*/}}
{{- define "goqueue.grpcPort" -}}
{{- .Values.service.grpcPort | default 9000 }}
{{- end }}

{{/*
Return the internal service port
*/}}
{{- define "goqueue.internalPort" -}}
{{- .Values.service.internalPort | default 7000 }}
{{- end }}
