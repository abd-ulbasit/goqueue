# GoQueue Operations Runbook

> **Purpose**: Step-by-step procedures for operating GoQueue in production environments.

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Deployment](#deployment)
3. [Day-to-Day Operations](#day-to-day-operations)
4. [Monitoring & Alerting](#monitoring--alerting)
5. [Troubleshooting](#troubleshooting)
6. [Incident Response](#incident-response)
7. [Maintenance Procedures](#maintenance-procedures)
8. [Disaster Recovery](#disaster-recovery)

---

## Quick Reference

### Key Metrics to Watch

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| `goqueue_messages_total` | Increasing | N/A | Stagnant |
| `goqueue_consumer_lag` | < 1000 | > 5000 | > 10000 |
| `goqueue_message_latency_p99` | < 10ms | > 50ms | > 100ms |
| `goqueue_disk_usage_percent` | < 70% | > 80% | > 90% |
| `goqueue_inflight_messages` | < capacity | > 80% capacity | At capacity |

### Emergency Contacts

| Role | Contact | When to Escalate |
|------|---------|------------------|
| On-call Engineer | PagerDuty | Any P1/P2 incident |
| Platform Team | #platform-oncall | Infrastructure issues |
| GoQueue Maintainers | GitHub Issues | Bug reports |

### Critical Commands

```bash
# Check cluster health
kubectl get pods -n goqueue -l app.kubernetes.io/name=goqueue

# View leader/follower status
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster status

# Emergency: Pause all consumers
kubectl exec -n goqueue goqueue-0 -- goqueue-admin consumer pause --all

# Emergency: Scale down
kubectl scale statefulset/goqueue -n goqueue --replicas=0
```

---

## Deployment

### Prerequisites

1. Kubernetes cluster 1.28+
2. Helm 3.x installed
3. `kubectl` configured for target cluster
4. PVC storage class available

### Fresh Deployment

```bash
# Add Helm repository (if published)
helm repo add goqueue https://charts.goqueue.io
helm repo update

# Or install from local chart
cd deploy/kubernetes/helm/goqueue

# Create namespace
kubectl create namespace goqueue

# Install with default values (development)
helm install goqueue . -n goqueue

# Install with production values
helm install goqueue . -n goqueue \
  --set replicaCount=3 \
  --set resources.requests.memory=2Gi \
  --set resources.limits.memory=4Gi \
  --set persistence.size=100Gi \
  --set metrics.enabled=true

# Verify deployment
kubectl get pods -n goqueue -w
```

### Upgrade Procedure

```bash
# 1. Check current version
helm list -n goqueue

# 2. Review changes
helm diff upgrade goqueue . -n goqueue -f values-prod.yaml

# 3. Backup current configuration
kubectl get configmap goqueue-config -n goqueue -o yaml > backup-config.yaml

# 4. Perform rolling upgrade
helm upgrade goqueue . -n goqueue -f values-prod.yaml

# 5. Monitor rollout
kubectl rollout status statefulset/goqueue -n goqueue

# 6. Verify health
kubectl exec -n goqueue goqueue-0 -- goqueue-admin health
```

### Rollback Procedure

```bash
# List revision history
helm history goqueue -n goqueue

# Rollback to previous revision
helm rollback goqueue -n goqueue

# Or rollback to specific revision
helm rollback goqueue 3 -n goqueue
```

---

## Day-to-Day Operations

### Checking Cluster Health

```bash
# Overall cluster status
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster status

# Individual node health
for i in 0 1 2; do
  echo "=== goqueue-$i ==="
  kubectl exec -n goqueue goqueue-$i -- goqueue-admin health
done

# Check metrics endpoint
kubectl port-forward -n goqueue svc/goqueue-metrics 9000:9000 &
curl http://localhost:9000/metrics | grep goqueue_
```

### Topic Management

```bash
# List all topics
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic list

# Create a topic
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic create \
  --name my-topic \
  --partitions 8 \
  --replication-factor 3

# Describe topic
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic describe my-topic

# Delete topic (careful!)
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic delete my-topic
```

### Consumer Group Management

```bash
# List consumer groups
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group list

# Describe group (see lag, members)
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe my-group

# Reset consumer offset (use with caution)
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group reset-offset \
  --group my-group \
  --topic my-topic \
  --to-earliest
```

### Viewing Logs

```bash
# Follow logs for all pods
kubectl logs -n goqueue -l app.kubernetes.io/name=goqueue -f

# Logs for specific pod
kubectl logs -n goqueue goqueue-0 -f

# Previous container logs (after restart)
kubectl logs -n goqueue goqueue-0 --previous

# Logs with timestamps
kubectl logs -n goqueue goqueue-0 --timestamps

# Search for errors
kubectl logs -n goqueue goqueue-0 | grep -i error
```

---

## Monitoring & Alerting

### Grafana Dashboard Access

```bash
# Port forward to Grafana
kubectl port-forward -n monitoring svc/grafana 3000:3000

# Default credentials: admin/admin
# Dashboard: GoQueue Overview
```

### Key Dashboards

1. **GoQueue Overview**: High-level cluster health
2. **GoQueue Topics**: Per-topic throughput and latency
3. **GoQueue Consumers**: Consumer lag and processing rates
4. **GoQueue Nodes**: Per-node resource usage

### Alert Runbooks

#### Alert: GoQueueHighConsumerLag

**Severity**: Warning → Critical

**Meaning**: Consumers are falling behind producers.

**Investigation**:
```bash
# Check consumer lag
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe <group-name>

# Check consumer pod status
kubectl get pods -n <consumer-namespace> -l app=<consumer-app>

# Check consumer logs
kubectl logs -n <consumer-namespace> <consumer-pod> | tail -100
```

**Resolution**:
1. Scale up consumers if processing is slow
2. Check for errors in consumer logs
3. Verify downstream services are healthy
4. Consider increasing consumer parallelism

#### Alert: GoQueueHighDiskUsage

**Severity**: Warning (80%) → Critical (90%)

**Meaning**: Broker disk is filling up.

**Investigation**:
```bash
# Check disk usage
kubectl exec -n goqueue goqueue-0 -- df -h /data

# Check topic sizes
kubectl exec -n goqueue goqueue-0 -- goqueue-admin storage stats

# Check retention settings
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic describe <topic>
```

**Resolution**:
1. Reduce retention period for high-volume topics
2. Enable compaction for key-based topics
3. Delete old/unused topics
4. Expand PVC if needed (see Maintenance)

#### Alert: GoQueueBrokerDown

**Severity**: Critical

**Meaning**: A broker pod is unhealthy.

**Investigation**:
```bash
# Check pod status
kubectl describe pod -n goqueue goqueue-<n>

# Check events
kubectl get events -n goqueue --sort-by='.lastTimestamp'

# Check node status
kubectl get nodes
```

**Resolution**:
1. Check if node is healthy
2. Review pod events for OOM/eviction
3. Check disk pressure
4. If stuck, delete pod for recreation

#### Alert: GoQueueCriticalConsumerLag

**Severity**: Critical  
**Threshold**: > 100,000 messages behind for 2 minutes

**Meaning**: Consumer group is critically behind. Messages are being delayed significantly, potentially impacting business operations.

**Comparison with Other Systems**:
- Kafka: Burrow monitors consumer lag, alerts at configurable thresholds
- SQS: ApproximateAgeOfOldestMessage metric
- RabbitMQ: queue_size monitoring

**Investigation**:
```bash
# Check current lag across all consumer groups
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group list

# Check specific consumer group
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe <group-name>

# Check if consumers are connected
kubectl exec -n goqueue goqueue-0 -- goqueue-admin connections list

# Check consumer pod health
kubectl get pods -n <consumer-namespace> -l app=<consumer-app>

# Check consumer logs for errors
kubectl logs -n <consumer-namespace> -l app=<consumer-app> --tail=100 | grep -i error
```

**Root Causes**:
| Cause | Diagnosis | Fix |
|-------|-----------|-----|
| Consumer crashed | Pod not running | Restart/fix consumer |
| Processing too slow | CPU high, low throughput | Scale consumers horizontally |
| Downstream bottleneck | Consumer waiting on DB/API | Fix downstream service |
| Poison message | Same message retrying | Move to DLQ, fix handler |
| Network partition | Connection drops | Check network/firewall |

**Resolution**:
1. **Immediate**: Scale up consumer replicas if processing is slow
2. Check for poison messages causing repeated failures
3. Verify downstream services (databases, APIs) are healthy
4. Consider temporarily pausing low-priority consumers to prioritize critical ones
5. If persistent, increase `max.poll.records` or batch size

#### Alert: GoQueueNoOffsetCommits

**Severity**: Critical  
**Threshold**: No commits for 10 minutes

**Meaning**: Consumer group has stopped committing offsets entirely. This typically indicates all consumers are dead or stuck.

**Investigation**:
```bash
# Check last commit timestamp
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe <group-name>

# Check if consumers are connected
kubectl exec -n goqueue goqueue-0 -- goqueue-admin connections list | grep <group-name>

# Check consumer pods
kubectl get pods -n <consumer-namespace> -l app=<consumer-app> -o wide

# Check consumer logs for deadlock/hang
kubectl logs -n <consumer-namespace> <consumer-pod> --tail=200
```

**Root Causes**:
- All consumer pods crashed or evicted
- Consumer code stuck in infinite loop or deadlock
- Network partition between consumers and broker
- Broker itself down (check cluster health first)

**Resolution**:
1. Restart consumer pods if they're stuck
2. Check for deadlocks in consumer code (thread dumps)
3. Verify network connectivity: `kubectl exec <consumer-pod> -- curl http://goqueue:8080/healthz`
4. Check if broker is accepting connections

#### Alert: GoQueueNodeDown

**Severity**: Critical  
**Threshold**: Unhealthy nodes > 0 for 30 seconds

**Meaning**: One or more GoQueue broker nodes are down, reducing cluster capacity and potentially causing partition unavailability.

**Investigation**:
```bash
# Check pod status
kubectl get pods -n goqueue -l app.kubernetes.io/name=goqueue

# Check specific pod events
kubectl describe pod -n goqueue <pod-name>

# Check node health
kubectl get nodes

# Check cluster membership from a healthy node
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster members
```

**Root Causes**:
| Cause | Diagnosis | Fix |
|-------|-----------|-----|
| OOMKilled | `kubectl describe pod` shows OOM | Increase memory limits |
| Node failure | Node in NotReady state | Wait for node recovery |
| Disk full | Pod stuck, disk 100% | Clear space, expand PVC |
| Health check fail | Pod restarting | Check logs for cause |
| Eviction | Evicted status | Check node pressure |

**Resolution**:
1. If OOMKilled: Increase memory limits in Helm values
2. If node failure: Pod will reschedule automatically
3. If disk full: Free space or expand PVC
4. If persistent crashes: Check logs for root cause

#### Alert: GoQueueOfflinePartitions

**Severity**: Critical  
**Threshold**: > 0 offline partitions for 30 seconds

**Meaning**: One or more partitions have no leader and cannot accept reads or writes. **DATA IS UNAVAILABLE.**

**Why This Is Critical**:
- Producers cannot publish to affected partitions
- Consumers cannot read from affected partitions
- Messages are being dropped or queuing in producers

**Investigation**:
```bash
# Check which partitions are offline
kubectl exec -n goqueue goqueue-0 -- goqueue-admin partitions list --offline

# Check cluster health
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster status

# Check if replicas exist but aren't taking leadership
kubectl exec -n goqueue goqueue-0 -- goqueue-admin partitions describe <topic> <partition>
```

**Root Causes**:
- All replicas for a partition are down (worst case)
- Leader died and no ISR replica can take over
- Unclean leader election is disabled and leader failed with no ISR

**Resolution**:
1. **Check if any replica is available**: If yes, it should auto-elect
2. **If no replicas available**: Wait for pods to recover
3. **If stuck**: Enable unclean leader election (DATA LOSS RISK):
   ```bash
   kubectl exec -n goqueue goqueue-0 -- goqueue-admin config set unclean.leader.election.enable true
   ```
4. After recovery: Check for data consistency

#### Alert: GoQueueUnderReplicatedPartitions

**Severity**: Warning → Critical  
**Threshold**: > 0 under-replicated partitions for 5 minutes

**Meaning**: Some partitions have fewer in-sync replicas (ISR) than the configured replication factor. Data durability is at risk.

**Comparison**:
- Kafka: `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
- RabbitMQ: `rabbitmq_queue_messages_ready_total` (similar concept)

**Investigation**:
```bash
# Check under-replicated partitions
kubectl exec -n goqueue goqueue-0 -- goqueue-admin partitions list --under-replicated

# Check ISR for specific partition
kubectl exec -n goqueue goqueue-0 -- goqueue-admin partitions describe <topic> <partition>

# Check follower lag
kubectl exec -n goqueue goqueue-0 -- goqueue-admin replication status
```

**Root Causes**:
| Cause | Diagnosis | Fix |
|-------|-----------|-----|
| Follower down | Pod not running | Wait for pod recovery |
| Follower slow | High replication lag | Check follower disk/network |
| Network partition | Connection drops | Check network |
| New follower | Just added, catching up | Wait for sync |

**Resolution**:
1. If follower is down: Wait for pod to recover
2. If follower is slow: Check disk I/O, network bandwidth
3. If persistent: Consider reducing producer throughput temporarily

#### Alert: GoQueueHighDiskSpace

**Severity**: Warning (80%) → Critical (90%)

**Meaning**: Broker disk is filling up. If it reaches 100%, the broker will stop accepting writes.

**Investigation**:
```bash
# Check disk usage
kubectl exec -n goqueue goqueue-0 -- df -h /data

# Check which topics are using most space
kubectl exec -n goqueue goqueue-0 -- goqueue-admin storage stats

# Check retention settings
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic list --all
```

**Resolution**:
1. **Reduce retention**: Lower `retention.ms` for high-volume topics
2. **Delete data**: Remove old/unused topics
3. **Expand storage**: Increase PVC size (see Maintenance)
4. **Enable compaction**: For key-based topics

#### Alert: GoQueueFsyncErrors

**Severity**: Critical

**Meaning**: fsync operations are failing. **DATA DURABILITY IS AT RISK.** Messages may not be persisted to disk.

**Investigation**:
```bash
# Check disk health
kubectl exec -n goqueue goqueue-0 -- dmesg | grep -i error

# Check disk I/O errors
kubectl exec -n goqueue goqueue-0 -- iostat -x 1 5

# Check filesystem
kubectl exec -n goqueue goqueue-0 -- df -h
```

**Root Causes**:
- Disk hardware failure
- Filesystem corruption
- Out of disk space (different alert but related)
- Network storage (EBS) connectivity issue

**Resolution**:
1. **Immediate**: Stop writes to affected broker
2. Check underlying storage health (EBS, GCE PD, etc.)
3. If hardware failure: Replace disk
4. If connectivity: Check AWS/GCP status

#### Alert: GoQueueHighFsyncLatency

**Severity**: Warning  
**Threshold**: p99 > 100ms for 5 minutes

**Meaning**: Disk writes are slow. This increases end-to-end latency and may cause timeouts.

**Investigation**:
```bash
# Check disk I/O stats
kubectl exec -n goqueue goqueue-0 -- iostat -x 1 10

# Check await time (time spent waiting for I/O)
# await > 10ms indicates disk contention

# Check if IOPS is at limit (for cloud disks)
# AWS: Check CloudWatch for EBS IOPS
```

**Resolution**:
1. Upgrade storage class (gp2 → gp3, HDD → SSD)
2. Increase IOPS provisioning on cloud disks
3. Enable write-back caching (if durability allows)
4. Reduce fsync frequency (trade durability for speed)

#### Alert: GoQueueHighErrorRate

**Severity**: Warning  
**Threshold**: > 1% of publishes failing for 5 minutes

**Meaning**: A significant portion of publish attempts are failing.

**Investigation**:
```bash
# Check error breakdown by type
kubectl exec -n goqueue goqueue-0 -- goqueue-admin metrics | grep error

# Check recent logs
kubectl logs -n goqueue goqueue-0 --tail=100 | grep -i error

# Check if specific topic is affected
kubectl exec -n goqueue goqueue-0 -- goqueue-admin topics stats
```

**Root Causes**:
| Error Type | Cause | Fix |
|------------|-------|-----|
| ValidationError | Bad message format | Fix producer code |
| TopicNotFound | Topic doesn't exist | Create topic |
| RateLimited | Too many requests | Increase quota or reduce rate |
| InternalError | Broker issue | Check logs |
| SchemaValidationError | Message doesn't match schema | Fix producer |

**Resolution**:
1. Check error type distribution in metrics
2. Fix producers sending invalid messages
3. Increase rate limits if legitimate traffic
4. Create missing topics

#### Alert: GoQueueFrequentRebalances

**Severity**: Warning  
**Threshold**: > 5 rebalances in 5 minutes

**Meaning**: Consumer groups are rebalancing frequently, causing processing pauses and reduced throughput.

**Investigation**:
```bash
# Check rebalance history
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe <group-name>

# Check consumer stability
kubectl get pods -n <consumer-namespace> -l app=<consumer-app> --watch

# Check consumer logs for session timeouts
kubectl logs -n <consumer-namespace> <consumer-pod> | grep -i "rebalance\|timeout\|heartbeat"
```

**Root Causes**:
| Cause | Diagnosis | Fix |
|-------|-----------|-----|
| Consumers crashing | Pods restarting | Fix consumer bugs |
| Session timeout | Slow heartbeat | Increase session.timeout.ms |
| Long processing | poll interval exceeded | Increase max.poll.interval.ms |
| Network instability | Connection drops | Check network |
| Rapid scaling | Many pods joining/leaving | Slow down deployment |

**Resolution**:
1. Increase `session.timeout.ms` (default 30s)
2. Increase `max.poll.interval.ms` for slow processing
3. Fix consumer code if crashing
4. Use rolling deployment with slow rollout

---

## Troubleshooting

### Pod Won't Start

**Symptoms**: Pod stuck in Pending/CrashLoopBackOff

**Diagnosis**:
```bash
# Check pod status
kubectl describe pod -n goqueue goqueue-0

# Check events
kubectl get events -n goqueue --field-selector involvedObject.name=goqueue-0

# Check PVC binding
kubectl get pvc -n goqueue
```

**Common Causes**:
- PVC not binding → Check storage class
- Insufficient resources → Check node capacity
- Image pull error → Check image name/credentials
- Health check failing → Check logs

### High Latency

**Symptoms**: Message latency above threshold

**Diagnosis**:
```bash
# Check producer latency
kubectl exec -n goqueue goqueue-0 -- goqueue-admin metrics | grep latency

# Check disk I/O
kubectl exec -n goqueue goqueue-0 -- iostat -x 1 5

# Check network
kubectl exec -n goqueue goqueue-0 -- netstat -s
```

**Resolution**:
1. Check if disk is bottleneck (high await)
2. Check network latency between nodes
3. Consider faster storage class
4. Tune batch size and flush intervals

### Messages Not Being Consumed

**Symptoms**: Messages stuck, consumer lag increasing

**Diagnosis**:
```bash
# Check consumer group
kubectl exec -n goqueue goqueue-0 -- goqueue-cli group describe <group>

# Check for rebalancing
kubectl logs -n goqueue goqueue-0 | grep -i rebalance

# Check inflight messages
kubectl exec -n goqueue goqueue-0 -- goqueue-admin inflight list
```

**Resolution**:
1. Check if consumers are connected
2. Look for poison messages causing processing failures
3. Check for stuck transactions
4. Verify visibility timeout settings

### Split Brain / Data Inconsistency

**Symptoms**: Different data on different nodes

**Diagnosis**:
```bash
# Check cluster membership
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster members

# Check leader election
kubectl exec -n goqueue goqueue-0 -- goqueue-admin cluster leader

# Check replication status
kubectl exec -n goqueue goqueue-0 -- goqueue-admin replication status
```

**Resolution**:
1. Identify the authoritative leader
2. If network partition, fix connectivity first
3. Consider manual failover if leader is unhealthy
4. Contact maintainers for data reconciliation

---

## Incident Response

### P1: Complete Cluster Outage

**Definition**: All brokers down, no messages flowing

**Immediate Actions**:
1. Page on-call team
2. Check all pod statuses
3. Check underlying infrastructure (nodes, network)
4. Check for recent changes (deployments, config changes)

**Recovery Steps**:
```bash
# 1. Check pod status
kubectl get pods -n goqueue

# 2. If all pods down, check events
kubectl get events -n goqueue --sort-by='.lastTimestamp' | tail -20

# 3. If stuck, force delete and let StatefulSet recreate
kubectl delete pod -n goqueue goqueue-0 --force --grace-period=0

# 4. If PVC issue, check storage
kubectl get pvc -n goqueue
kubectl describe pvc -n goqueue goqueue-data-goqueue-0
```

### P2: Single Broker Failure

**Definition**: One broker down, cluster still operational

**Immediate Actions**:
1. Verify cluster is still operational
2. Check which partitions were on failed broker
3. Verify leadership has transferred

**Recovery Steps**:
```bash
# 1. Check pod status
kubectl describe pod -n goqueue goqueue-<n>

# 2. Check if pod is rescheduling
kubectl get pod -n goqueue goqueue-<n> -w

# 3. If stuck, investigate and potentially force delete
kubectl delete pod -n goqueue goqueue-<n>
```

### P3: High Consumer Lag

**Definition**: Consumers falling behind, not yet impacting production

**Actions**:
1. Identify affected consumer groups
2. Check consumer health
3. Scale consumers if needed
4. Investigate root cause

---

## Maintenance Procedures

### Planned Maintenance Window

**Pre-maintenance**:
```bash
# 1. Notify stakeholders
# 2. Check current cluster health
kubectl exec -n goqueue goqueue-0 -- goqueue-admin health

# 3. Create PDB to limit disruption
kubectl apply -f - <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: goqueue-pdb
  namespace: goqueue
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: goqueue
EOF
```

### Expanding Storage (PVC Resize)

**Prerequisites**: Storage class must support expansion

```bash
# 1. Check if storage class supports expansion
kubectl get sc <storage-class> -o yaml | grep allowVolumeExpansion

# 2. Edit PVC size (StatefulSet PVCs)
kubectl patch pvc goqueue-data-goqueue-0 -n goqueue \
  -p '{"spec": {"resources": {"requests": {"storage": "200Gi"}}}}'

# 3. Repeat for each PVC
for i in 0 1 2; do
  kubectl patch pvc goqueue-data-goqueue-$i -n goqueue \
    -p '{"spec": {"resources": {"requests": {"storage": "200Gi"}}}}'
done

# 4. Verify expansion (may require pod restart)
kubectl exec -n goqueue goqueue-0 -- df -h /data
```

### Node Maintenance (Drain)

```bash
# 1. Identify which broker is on the node
kubectl get pods -n goqueue -o wide | grep <node-name>

# 2. Cordon the node
kubectl cordon <node-name>

# 3. Drain with PDB respect
kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data

# 4. Perform maintenance

# 5. Uncordon
kubectl uncordon <node-name>
```

---

## Disaster Recovery

### Backup Procedures

```bash
# Backup topic metadata
kubectl exec -n goqueue goqueue-0 -- goqueue-admin backup metadata > metadata-backup.json

# Backup is primarily handled by:
# 1. PVC snapshots (cloud provider)
# 2. Cross-region replication (if configured)
# 3. WAL archival (if configured)
```

### Restore from Backup

```bash
# 1. Ensure cluster is stopped
kubectl scale statefulset/goqueue -n goqueue --replicas=0

# 2. Restore PVC from snapshot (example for AWS)
# aws ec2 create-volume --snapshot-id <snapshot-id> ...

# 3. Start cluster
kubectl scale statefulset/goqueue -n goqueue --replicas=3

# 4. Verify data
kubectl exec -n goqueue goqueue-0 -- goqueue-cli topic list
```

### Cross-Region Failover

```bash
# If primary region is down:
# 1. Verify DR region cluster is healthy
kubectl --context dr-cluster get pods -n goqueue

# 2. Update DNS/traffic to DR region
# (depends on your DNS/LB setup)

# 3. Notify applications of new endpoint

# 4. Monitor DR cluster closely during failover
```

---

## Appendix

### Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broker.port` | 8080 | HTTP API port |
| `broker.grpcPort` | 9000 | gRPC port |
| `broker.raftPort` | 7000 | Raft consensus port |
| `storage.dataDir` | /data | Data directory |
| `storage.walDir` | /data/wal | WAL directory |
| `retention.maxAge` | 168h | Message retention |
| `retention.maxSize` | 10GB | Max topic size |

### Useful Scripts

See [deploy/testing/chaos/](../deploy/testing/chaos/) for chaos testing scripts.
See [deploy/testing/load/](../deploy/testing/load/) for load testing scripts.

### External Resources

- [GoQueue Architecture](./ARCHITECTURE.md)
- [GoQueue GitHub](https://github.com/your-org/goqueue)
- [Kubernetes StatefulSet Docs](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
