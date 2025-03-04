"""
Kafka consumer for processing rig data from Azure Event Hubs.
"""
import json
import logging
import time
from typing import Dict, Any
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from concurrent.futures import ThreadPoolExecutor

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    CONSUMER_GROUP_ID,
    CONSUMER_AUTO_OFFSET_RESET,
    CONSUMER_ENABLE_AUTO_COMMIT,
    CONSUMER_MAX_POLL_RECORDS,
    CONSUMER_SESSION_TIMEOUT_MS,
    CONSUMER_HEARTBEAT_INTERVAL_MS,
    TOPIC_PREFIX,
    NUM_PARTITIONS,
    MAX_RETRIES,
    RETRY_DELAY,
    DEAD_LETTER_TOPIC,
    MAX_CONCURRENT_CONSUMERS,
    CONSUMER_THREAD_POOL_SIZE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
messages_processed = Counter('rig_messages_processed_total', 'Total number of messages processed')
processing_latency = Histogram('rig_processing_latency_seconds', 'Message processing latency in seconds')
error_count = Counter('rig_processing_errors_total', 'Total number of processing errors')
consumer_lag = Gauge('rig_consumer_lag', 'Consumer lag per partition')
critical_alerts = Counter('rig_critical_alerts_total', 'Total number of critical alerts')

class RigDataConsumer:
    def __init__(self):
        # Create topics list
        self.topics = [f"{TOPIC_PREFIX}-{i:02d}" for i in range(NUM_PARTITIONS)]
        
        # Create consumer
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=KAFKA_SASL_MECHANISM,
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
            enable_auto_commit=CONSUMER_ENABLE_AUTO_COMMIT,
            max_poll_records=CONSUMER_MAX_POLL_RECORDS,
            session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
            heartbeat_interval_ms=CONSUMER_HEARTBEAT_INTERVAL_MS
        )
        
        # Thread pool for parallel message processing
        self.executor = ThreadPoolExecutor(max_workers=CONSUMER_THREAD_POOL_SIZE)
        
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message from a rig."""
        try:
            # Extract key metrics
            rig_id = message.get('rig_id')
            timestamp = message.get('timestamp')
            
            # Check for critical alerts
            if 'alerts' in message and message['alerts']['level'] == 'critical':
                critical_alerts.inc()
                logger.warning(f"Critical alert from rig {rig_id}: {message['alerts']['message']}")
            
            # Process pressure metrics
            pressure = message.get('pressure', {})
            if pressure.get('status') == 'warning':
                logger.warning(f"Pressure warning from rig {rig_id}: {pressure['value']} {pressure['unit']}")
            
            # Process temperature metrics
            temperature = message.get('temperature', {})
            if temperature.get('status') == 'warning':
                logger.warning(f"Temperature warning from rig {rig_id}: {temperature['value']} {temperature['unit']}")
            
            # Process equipment status
            equipment_status = message.get('equipment_status', {})
            if equipment_status.get('safety_system') == 'critical':
                logger.error(f"Critical safety system status from rig {rig_id}")
            
            # Process maintenance metrics
            maintenance = message.get('maintenance_metrics', {})
            if maintenance.get('maintenance_due'):
                logger.warning(f"Maintenance due for rig {rig_id}")
            
            # Update consumer lag metrics
            for topic_partition in self.consumer.assignment():
                lag = self.consumer.end_offsets([topic_partition])[topic_partition] - \
                      self.consumer.position(topic_partition)
                consumer_lag.labels(partition=topic_partition.partition).set(lag)
            
            messages_processed.inc()
            
        except Exception as e:
            error_count.inc()
            logger.error(f"Error processing message: {str(e)}")
            self.handle_error(message)
    
    def handle_error(self, message: Dict[str, Any]) -> None:
        """Handle processing errors by sending to dead letter queue."""
        try:
            # In a real implementation, you would send to a dead letter queue
            # For this POC, we'll just log the error
            logger.error(f"Message sent to DLQ: {json.dumps(message)}")
        except Exception as e:
            logger.error(f"Error handling failed message: {str(e)}")
    
    def process_batch(self, messages) -> None:
        """Process a batch of messages in parallel."""
        futures = []
        for message in messages:
            try:
                data = json.loads(message.value.decode('utf-8'))
                futures.append(self.executor.submit(self.process_message, data))
            except Exception as e:
                error_count.inc()
                logger.error(f"Error decoding message: {str(e)}")
        
        # Wait for all messages in batch to be processed
        for future in futures:
            future.result()
    
    def start(self) -> None:
        """Start the consumer and begin processing messages."""
        logger.info("Starting Rig Data Consumer...")
        
        # Start Prometheus metrics server
        start_http_server(9091)  # Different port from producer
        
        try:
            while True:
                try:
                    # Poll for messages
                    message_pack = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_pack.items():
                        with processing_latency.time():
                            self.process_batch(messages)
                            
                except Exception as e:
                    error_count.inc()
                    logger.error(f"Error in message polling loop: {str(e)}")
                    time.sleep(RETRY_DELAY)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.close()
    
    def close(self) -> None:
        """Close the consumer and thread pool."""
        self.consumer.close()
        self.executor.shutdown()

def main():
    consumer = RigDataConsumer()
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main() 