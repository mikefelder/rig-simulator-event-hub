# Rig Data Streaming POC with Azure Event Hubs for Kafka

This proof of concept demonstrates streaming data from oil rigs using Azure Event Hubs for Kafka. The system simulates 200 rigs sending data every second, with approximately 1000 key-value pairs per message.

## Features Demonstrated

- Kafka compatibility and consumer group support
- Low latency/High throughput
- Broker maintains state of each consumer group
- High Availability
- Fault tolerance
- Large capacity for consumer groups and topics
- Partitions on topic
- System health monitoring and alerts
- Ability to stream and playback messages from fixed defined offset
- Access control
- Wildcard subscriptions
- Client library support (Python implementation)

## Scale Requirements Met

- Consumers: >50 consumer groups (multiple instances per group)
- Producers: >5 producers
- Events: 500 messages/sec
- Message size: >2048 bytes
- Input capacity: >1 MB/sec
- Topics: >20 topics
- Retention capacity: 85 GB per day (2.5 TB for 30 days)
- Reserve Capacity: 2K messages/sec

## Project Structure

```
.
├── config/
│   └── config.py         # Configuration settings
├── producer/
│   ├── __init__.py
│   ├── rig_simulator.py  # Simulates rig data generation
│   └── producer.py       # Kafka producer implementation
├── consumer/
│   ├── __init__.py
│   └── consumer.py       # Kafka consumer implementation
├── monitoring/
│   ├── __init__.py
│   └── metrics.py        # Prometheus metrics implementation
├── requirements.txt      # Project dependencies
└── .env                 # Environment variables (not in repo)
```

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your Azure Event Hubs credentials:
   ```
   EVENT_HUB_NAMESPACE=your-namespace
   EVENT_HUB_NAME=your-event-hub
   EVENT_HUB_CONNECTION_STRING=your-connection-string
   ```

4. Run the producer:
   ```bash
   python -m producer.producer
   ```

5. Run the consumer:
   ```bash
   python -m consumer.consumer
   ```

## Monitoring

The system includes Prometheus metrics for monitoring:
- Message throughput
- Latency
- Error rates
- Consumer group lag
- Producer performance

Access metrics at `http://localhost:9090/metrics`

## Performance Considerations

- The system uses multiple partitions for parallel processing
- Consumer groups are implemented for load balancing
- Message batching is enabled for optimal throughput
- Compression is enabled to reduce network bandwidth
- Monitoring is implemented for real-time performance tracking

## Security

- SASL authentication is implemented
- TLS encryption is enabled
- Access control is managed through Azure Event Hubs policies

## Fault Tolerance

- Automatic reconnection on connection loss
- Message retry logic
- Dead letter queue for failed messages
- Consumer group rebalancing 