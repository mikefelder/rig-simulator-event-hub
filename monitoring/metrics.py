"""
Monitoring metrics for the rig data streaming system.
"""
from prometheus_client import Counter, Histogram, Gauge, Summary
from typing import Dict, Any
import time

# Message metrics
messages_sent = Counter('rig_messages_sent_total', 'Total number of messages sent')
messages_processed = Counter('rig_messages_processed_total', 'Total number of messages processed')
message_size = Histogram('rig_message_size_bytes', 'Size of messages in bytes')

# Latency metrics
send_latency = Histogram('rig_send_latency_seconds', 'Message send latency in seconds')
processing_latency = Histogram('rig_processing_latency_seconds', 'Message processing latency in seconds')
end_to_end_latency = Histogram('rig_end_to_end_latency_seconds', 'End-to-end message latency in seconds')

# Error metrics
error_count = Counter('rig_processing_errors_total', 'Total number of processing errors')
critical_alerts = Counter('rig_critical_alerts_total', 'Total number of critical alerts')

# Consumer metrics
consumer_lag = Gauge('rig_consumer_lag', 'Consumer lag per partition')
consumer_group_lag = Gauge('rig_consumer_group_lag', 'Consumer group lag per topic')

# System metrics
active_consumers = Gauge('rig_active_consumers', 'Number of active consumers')
active_producers = Gauge('rig_active_producers', 'Number of active producers')
partition_count = Gauge('rig_partition_count', 'Number of partitions per topic')

# Performance metrics
messages_per_second = Gauge('rig_messages_per_second', 'Messages processed per second')
bytes_per_second = Gauge('rig_bytes_per_second', 'Bytes processed per second')
processing_time = Summary('rig_processing_time_seconds', 'Time spent processing messages')

# Health metrics
system_health = Gauge('rig_system_health', 'Overall system health (1=healthy, 0=unhealthy)')
connection_status = Gauge('rig_connection_status', 'Connection status (1=connected, 0=disconnected)')

def update_message_metrics(message: Dict[str, Any], size: int) -> None:
    """Update metrics for a processed message."""
    messages_processed.inc()
    message_size.observe(size)
    
    # Update end-to-end latency if timestamp is available
    if 'timestamp' in message:
        try:
            message_time = float(message['timestamp'])
            current_time = time.time()
            end_to_end_latency.observe(current_time - message_time)
        except (ValueError, TypeError):
            pass

def update_error_metrics(error_type: str) -> None:
    """Update error-related metrics."""
    error_count.inc()
    system_health.set(0)  # Mark system as unhealthy

def update_consumer_metrics(topic: str, partition: int, lag: int) -> None:
    """Update consumer-related metrics."""
    consumer_lag.labels(partition=partition).set(lag)
    consumer_group_lag.labels(topic=topic).set(lag)

def update_system_metrics(active_consumer_count: int, active_producer_count: int) -> None:
    """Update system-level metrics."""
    active_consumers.set(active_consumer_count)
    active_producers.set(active_producer_count)
    
    # Update system health based on active components
    if active_consumer_count > 0 and active_producer_count > 0:
        system_health.set(1)
    else:
        system_health.set(0)

def update_performance_metrics(messages_per_sec: float, bytes_per_sec: float) -> None:
    """Update performance-related metrics."""
    messages_per_second.set(messages_per_sec)
    bytes_per_second.set(bytes_per_sec)

def update_connection_status(is_connected: bool) -> None:
    """Update connection status metric."""
    connection_status.set(1 if is_connected else 0)
    if not is_connected:
        system_health.set(0)  # Mark system as unhealthy if disconnected 