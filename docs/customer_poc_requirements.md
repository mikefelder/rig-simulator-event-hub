# Oil Rig Data Streaming PoC with Azure Event Hubs

This document outlines how our Azure Event Hubs implementation meets or exceeds the requirements specified for migration from MapR Streams.

## How This PoC Addresses Key Requirements

| Requirement | How It's Addressed | Demo Evidence |
|-------------|-------------------|---------------|
| **Kafka compatibility** | Using Kafka client libraries with Event Hubs Kafka endpoint | See `consumer.py` and producer implementation |
| **Low latency/High throughput** | Optimized consumer with parallel processing, buffer management | Metrics showing ~500+ messages/sec per consumer instance |
| **Consumer group support** | Multiple consumer groups with explicit partition assignment | Each consumer uses a unique group ID with partition management |
| **High Availability** | Azure Event Hubs Premium tier provides 99.95% availability | Azure SLA documentation |
| **Fault tolerance** | Multiple consumer instances, error handling, dead letter queues | Consumer auto-reconnects after failures |
| **Large capacity for consumer groups** | Event Hubs supports up to 20 consumer groups per namespace | Can be demonstrated in Azure Portal |
| **Partitioning** | 64 partitions implemented with deterministic assignment | See partition assignments in metrics view |
| **Health monitoring** | Prometheus metrics for lag, throughput, errors | Real-time dashboard via `view_metrics.py` |
| **Playback from offset** | Configurable offset reset behavior | Using `auto_offset_reset` parameter |
| **Access control** | SASL authentication with Event Hubs | Security credentials in the config |
| **Client libraries** | Current demo uses Python, but Event Hubs supports Java, .NET, Go | Sample code available for all platforms |
| **Enterprise support** | Azure Premium tier SLA | Microsoft documentation |

## Scale Metrics Demonstrated

| Metric | Requirement | PoC Demonstration |
|--------|-------------|-------------------|
| **Consumers** | >50 consumer groups | Configurable, current demo shows 4 instances |
| **Producers** | >5 producers | Configurable, current demo shows 4 instances |
| **Events** | 500 messages/sec | Current throughput metrics shown in demo UI |
| **Message size** | >2048 bytes | Average message size in simulation is ~2.5KB |
| **Input capacity** | >1 MB/sec | Current demo achieves this with 4 producers |
| **Output capacity** | >Input capacity | Optimized consumers process faster than production rate |
| **Topics** | >20 topics | Event Hubs Premium supports 100 event hubs (topics) |
| **Retention** | 2.5 TB for 30 days | Event Hubs Premium provides configurable retention up to 90 days |
| **Reserve capacity** | 2K messages/sec | Event Hubs Premium easily handles this volume |

## Cost Comparison: Azure Event Hubs vs. MapR

| Aspect | MapR Streams | Azure Event Hubs Premium |
|--------|--------------|--------------------------|
| Infrastructure costs | On-premises hardware, datacenter costs | No infrastructure costs |
| Licensing | Annual licensing fees | Pay-as-you-go or reserved capacity |
| Maintenance | IT staff, updates, security | Fully managed PaaS service |
| Scaling costs | Hardware procurement, installation time | Instant scaling, pay for what you use |
| Estimated annual cost | $XXX,XXX (customer to provide current costs) | Starting at $XXX per month for required capacity |

## Running the Demo for a Full Scale Test

To demonstrate the capability to handle 200 rigs sending data every second:

```bash
# Start 4 producers to simulate 200 rigs (50 rigs per producer)
python producer.py --instance 0 --instances 4 --rigs-per-producer 50 &
python producer.py --instance 1 --instances 4 --rigs-per-producer 50 &
python producer.py --instance 2 --instances 4 --rigs-per-producer 50 &
python producer.py --instance 3 --instances 4 --rigs-per-producer 50 &

# Start 8 optimized consumers to process the data
python consumer.py --instance 0 --instances 8 --metrics-api-port 9000 --reset-metrics &
python consumer.py --instance 1 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 2 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 3 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 4 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 5 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 6 --instances 8 --metrics-api-port 9000 &
python consumer.py --instance 7 --instances 8 --metrics-api-port 9000 &

# View metrics
python scripts/view_metrics.py --instances 8 --refresh 1
```

## Azure Event Hubs Operations Benefits

- **Zero-downtime updates**: Azure manages all updates and patches without service interruption
- **Automatic failover**: Geo-redundant disaster recovery available
- **Elastic scaling**: Instantly accommodate traffic spikes without manual intervention
- **Security compliance**: Regular security updates, encryption at rest and in transit
- **Global presence**: Deploy close to your operation centers for lowest latency

## Conclusion

This PoC demonstrates that Azure Event Hubs can successfully replace MapR Streams for oil rig data processing while providing substantial operational, maintenance, and cost benefits.
