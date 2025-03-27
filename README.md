# Rig Simulator Event Hub

A simulation system for oil rig telemetry data using Azure Event Hubs (Kafka API).

## System Architecture

This system consists of two main components:

1. **Producer**: Simulates multiple oil rigs (default: 200) sending telemetry data to Azure Event Hubs
2. **Consumer**: Processes the telemetry data from Event Hubs, detects alerts, and logs warnings

The system uses Kafka protocol to communicate with Azure Event Hubs, which provides a scalable message bus.

## Configuration

All configuration settings are managed in `config/config.py`. Key settings include:

- `NUM_RIGS`: Total number of simulated rigs (default: 200)
- `MESSAGE_INTERVAL`: Seconds between messages from each rig (default: 1.0)
- `NUM_PARTITIONS`: Number of partitions in the Event Hub (default: 64)
- `PRODUCER_THREAD_POOL_SIZE`: Maximum threads for the producer (default: 200)
- `CONSUMER_THREAD_POOL_SIZE`: Maximum threads for message processing (default: 32)

## Running Multiple Producer Instances

For higher throughput, you can run multiple producer instances, each handling a subset of rigs.

### Basic Usage

To run a single producer instance that handles all rigs:

```bash
python producer/producer.py
```

### Running Multiple Producer Instances

To run multiple producer instances, each handling a portion of the 200 rigs:

```bash
# Terminal 1 - First instance handles first 50 rigs (0-49)
python producer/producer.py --instance 0 --instances 4

# Terminal 2 - Second instance handles next 50 rigs (50-99)
python producer/producer.py --instance 1 --instances 4

# Terminal 3 - Third instance handles next 50 rigs (100-149)
python producer/producer.py --instance 2 --instances 4

# Terminal 4 - Fourth instance handles last 50 rigs (150-199)
python producer/producer.py --instance 3 --instances 4
```

### Producer Arguments

- `--instance`: The instance number (0-based index)
- `--instances`: Total number of producer instances you're running

## Running Multiple Consumer Instances

Since the Event Hub has 64 partitions, you can run multiple consumer instances to improve throughput.

### Basic Usage

To run a single consumer instance:

```bash
python consumer/consumer.py
```

### Running Multiple Consumer Instances

To run multiple consumer instances (each will automatically be assigned different partitions):

```bash
# Terminal 1 - First consumer instance
python consumer/consumer.py --instance 0 --instances 4

# Terminal 2 - Second consumer instance
python consumer/consumer.py --instance 1 --instances 4

# Terminal 3 - Third consumer instance
python consumer/consumer.py --instance 2 --instances 4

# Terminal 4 - Fourth consumer instance
python consumer/consumer.py --instance 3 --instances 4
```

### Consumer Arguments

- `--instance`: The instance number (0-based index)
- `--instances`: Total number of consumer instances you're running

## Scaling Recommendations

### Producer Scaling

- The producer is CPU-intensive due to message generation and compression
- Start with 2-4 producer instances, each handling 50-100 rigs
- Monitor CPU usage and increase instances if CPU is saturated
- Each instance will have its own Kafka connection to Azure Event Hubs

### Consumer Scaling

- Best practice: Use 1 consumer instance per 16-32 partitions
- With 64 partitions, 2-4 consumer instances is optimal
- Add more consumer instances if processing lag builds up
- All consumers in the same consumer group will automatically coordinate

## Monitoring

### Prometheus Metrics

Both the producer and consumer expose Prometheus metrics that you can view on your local machine:

#### Accessing Metrics Directly

- **Producer metrics**: http://localhost:9090/metrics
- **Consumer metrics**: http://localhost:9091/metrics, http://localhost:9092/metrics, etc. (port = 9091 + instance_number)

These endpoints return plain text metrics data in the Prometheus format.

#### Using Prometheus UI

For a better visualization experience:

1. Install Prometheus: https://prometheus.io/download/
2. Create a `prometheus.yml` configuration file:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'rig-producer'
    static_configs:
      - targets: ['localhost:9090']
  - job_name: 'rig-consumer-1'
    static_configs:
      - targets: ['localhost:9091']
  - job_name: 'rig-consumer-2'
    static_configs:
      - targets: ['localhost:9092']
  - job_name: 'rig-consumer-3'
    static_configs:
      - targets: ['localhost:9093']
  - job_name: 'rig-consumer-4'
    static_configs:
      - targets: ['localhost:9094']
```

3. Start Prometheus with this config:
   ```bash
   prometheus --config.file=prometheus.yml
   ```

4. Access the Prometheus UI at http://localhost:9090 (the default Prometheus UI port)

#### Using Grafana (Optional)

For even better visualizations:

1. Install Grafana: https://grafana.com/docs/grafana/latest/installation/
2. Add Prometheus as a data source
3. Create dashboards to visualize your metrics

### Available Metrics

#### Producer Metrics
- `rig_messages_sent_total`: Total number of messages sent
- `rig_message_size_bytes`: Size distribution of messages in bytes
- `rig_send_latency_seconds`: Message send latency distribution

#### Consumer Metrics
- `rig_messages_processed_total`: Total number of messages processed
- `rig_processing_latency_seconds`: Message processing latency distribution
- `rig_processing_errors_total`: Number of processing errors
- `rig_consumer_lag`: Lag per partition (shows how far behind the consumer is)
- `rig_critical_alerts_total`: Number of critical alerts detected

### Monitoring Consumer Lag and Unprocessed Messages

To view the number of unprocessed messages (consumer lag) in Azure Event Hubs:

1. **Azure Portal Metrics View**:
   - Navigate to your Event Hubs namespace
   - Select your Event Hub (eh-rig-sim-01)
   - Click on "Metrics" in the left menu
   - Add metric: "Consumer Group Lag" 
   - Filter by consumer group: "rig-data-processor"
   - Optional: Split by Partition ID to see lag per partition

2. **Using the Monitoring View**:
   - The difference between "Incoming Messages" and "Outgoing Messages" metrics shows messages being queued up

3. **From your local metrics**:
   - The `rig_consumer_lag` metric in your Prometheus endpoint shows lag per partition
   - Access this at http://localhost:9091/metrics (or other consumer ports)
   - Use the view_metrics.py script with the `--show-lag` option (requires implementation)

## Troubleshooting

- Check logs for connection issues or errors
- Verify that the expected number of messages are being produced/consumed
- Use the `troubleshoot.py` script to analyze message distribution
- If a consumer falls behind, consider adding more consumer instances

## Message Format

Each simulated rig generates messages with the following structure:

```json
{
  "messageId": "uuid",
  "rigId": "RIG_XXX",
  "timestamp": "2025-03-24T19:14:48.280813Z",
  "operationalStatus": "DRILLING|OPERATIONAL|STANDBY|MAINTENANCE",
  "location": {
    "latitude": 29.33332,
    "longitude": -86.30856
  },
  "measurements": {
    "depth": 4866.73,
    "weight_on_bit": 48.87,
    "rotary_speed": 103.63,
    "rate_of_penetration": 13.37,
    "torque": 1456.65,
    "pressure": 4968.31,
    "temperature": 105.98,
    "mud_flow_rate": 752.15
  },
  "alerts": {
    "items": [
      {
        "alertId": "uuid",
        "severity": "WARNING|CRITICAL",
        "message": "alert message",
        "timestamp": "2025-03-24T19:14:48.280813Z"
      }
    ]
  }
}
````