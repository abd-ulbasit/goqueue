# GoQueue Helm Chart

Production-ready Helm chart for deploying GoQueue on Kubernetes.

## Quick Start

```bash
# Add the repository
helm repo add goqueue https://abd-ulbasit.github.io/goqueue

# Install with defaults (3-node cluster)
helm install my-goqueue goqueue/goqueue

# Install with custom values
helm install my-goqueue goqueue/goqueue -f my-values.yaml
```

## Prerequisites

- Kubernetes 1.25+
- Helm 3.0+
- PV provisioner support in the underlying infrastructure

## Installing the Chart

```bash
# Create namespace
kubectl create namespace goqueue

# Install the chart
helm install my-goqueue goqueue/goqueue -n goqueue
```

## Configuration

See [values.yaml](values.yaml) for the full list of parameters.

### Common Configurations

#### Single Node (Development)

```yaml
replicaCount: 1
persistence:
  size: 1Gi
resources:
  limits:
    cpu: "500m"
    memory: 512Mi
prometheus:
  enabled: false
traefik:
  enabled: false
```

#### 3-Node Cluster (Production-Lite)

```yaml
replicaCount: 3
persistence:
  size: 10Gi
  storageClass: gp3
resources:
  limits:
    cpu: "2"
    memory: 2Gi
```

#### 5-Node Cluster (Production)

```yaml
replicaCount: 5
config:
  cluster:
    replicationFactor: 3
    minInSyncReplicas: 2
persistence:
  size: 100Gi
  storageClass: premium-rwo
resources:
  limits:
    cpu: "4"
    memory: 8Gi
podDisruptionBudget:
  minAvailable: 3
```

## Monitoring

The chart can install Prometheus and Grafana via optional dependencies:

```yaml
prometheus:
  enabled: true

grafana:
  enabled: true
  dashboards:
    enabled: true
```

If you have existing monitoring, disable the bundled stack and configure ServiceMonitor:

```yaml
prometheus:
  enabled: false

metrics:
  serviceMonitor:
    enabled: true
    additionalLabels:
      release: prometheus  # Match your Prometheus Operator
```

## Persistence

Each broker gets its own PersistentVolumeClaim:

```yaml
persistence:
  enabled: true
  storageClass: "gp3"  # AWS EBS
  size: 50Gi
```

## Ingress

HTTP and gRPC can be exposed via Ingress:

```yaml
ingress:
  enabled: true
  className: traefik
  hosts:
    - host: goqueue.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: goqueue-tls
      hosts:
        - goqueue.example.com
```

## Uninstalling

```bash
helm uninstall my-goqueue -n goqueue

# PVCs are NOT deleted automatically (to prevent data loss)
# To delete all data:
kubectl delete pvc -l app.kubernetes.io/name=goqueue -n goqueue
```
