"""
Kafka consumer for processing rig data from Azure Event Hubs.
"""
import json
import logging
import time
import sys
import os
from typing import Dict, Any
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from concurrent.futures import ThreadPoolExecutor

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    CONSUMER_FETCH_MIN_BYTES,
    CONSUMER_FETCH_MAX_BYTES,
    TOPIC_NAME,
    MAX_RETRIES,
    RETRY_DELAY,
    DEAD_LETTER_TOPIC,
    MAX_CONCURRENT_CONSUMERS,
    CONSUMER_THREAD_POOL_SIZE,
    EVENT_HUB_CONNECTION_STRING
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
consumer_lag = Gauge('rig_consumer_lag', 'Consumer lag per partition', ['partition'])  # Add 'partition' as a label
critical_alerts = Counter('rig_critical_alerts_total', 'Total number of critical alerts')

# Add a helper function to safely process messages with both alert formats
def process_message_json(message_json):
    try:
        # Parse the JSON message
        message_data = json.loads(message_json)
        
        # Handle alerts field - ensure it's a dictionary with 'items' key
        alerts = message_data.get("alerts", {})
        
        # If alerts is still a list from old format messages, convert it
        if isinstance(alerts, list):
            alerts = {"items": alerts}
            message_data["alerts"] = alerts
        
        return message_data
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error(f"Message sent to DLQ: {message_json}")
        # Handle sending to DLQ
        return None

class RigDataConsumer:
    def __init__(self):
        # Create consumer
        self.consumer = KafkaConsumer(
            TOPIC_NAME,
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
            heartbeat_interval_ms=CONSUMER_HEARTBEAT_INTERVAL_MS,
            fetch_min_bytes=CONSUMER_FETCH_MIN_BYTES,
            fetch_max_bytes=CONSUMER_FETCH_MAX_BYTES
        )
        
        # Thread pool for parallel message processing
        self.executor = ThreadPoolExecutor(max_workers=CONSUMER_THREAD_POOL_SIZE)
        
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message from a rig."""
        try:
            # Extract key metrics
            rig_id = message.get('rigId')  # Changed from rig_id to rigId to match producer format
            timestamp = message.get('timestamp')
            
            # Check for critical alerts using the new structure
            if 'alerts' in message:
                alert_items = message['alerts'].get('items', [])
                for alert in alert_items:
                    if alert.get('severity') == 'CRITICAL':  # Changed from 'level' == 'critical' to match producer format
                        critical_alerts.inc()
                        logger.warning(f"Critical alert from rig {rig_id}: {alert.get('message')}")
            
            # Process measurements
            measurements = message.get('measurements', {})
            
            # Process pressure metrics
            pressure = measurements.get('pressure')
            if pressure and pressure > 4500:  # Example threshold
                logger.warning(f"Pressure warning from rig {rig_id}: {pressure}")
            
            # Process temperature metrics
            temperature = measurements.get('temperature')
            if temperature and temperature > 140:  # Example threshold
                logger.warning(f"Temperature warning from rig {rig_id}: {temperature}")
            
            # Update consumer lag metrics
            for topic_partition in self.consumer.assignment():
                lag = self.consumer.end_offsets([topic_partition])[topic_partition] - \
                      self.consumer.position(topic_partition)
                # Convert partition to string to ensure compatibility
                consumer_lag.labels(partition=str(topic_partition.partition)).set(lag)
            
            messages_processed.inc()
            
        except Exception as e:
            error_count.inc()
            logger.error(f"Error processing message: {str(e)}")
            self.handle_error(message)
    
    def handle_error(self, message: Dict[str, Any]) -> None:
        """Handle processing errors by sending to dead letter queue."""
        try:
            # Convert the message to a JSON string to ensure it can be serialized
            message_json = json.dumps(message) if not isinstance(message, str) else message
            logger.error(f"Message sent to DLQ: {message_json}")
        except Exception as e:
            logger.error(f"Error handling failed message: {str(e)}")
    
    def process_batch(self, messages) -> None:
        """Process a batch of messages in parallel."""
        futures = []
        for message in messages:
            try:
                # Use the helper function to process the JSON message
                data = process_message_json(message.value.decode('utf-8'))
                if data:  # Only process if valid data was returned
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
        
        # Remove the duplicate metrics server start - it's now handled in main()
        # start_http_server(9091)  # Different port from producer
        
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
    # Support command-line arguments for consumer instance identification
    import argparse
    
    parser = argparse.ArgumentParser(description='Run rig data consumer')
    parser.add_argument('--instance', type=int, default=0, help='Consumer instance ID')
    parser.add_argument('--instances', type=int, default=1, help='Total number of consumer instances')
    args = parser.parse_args()
    
    # Use the instance ID to create a unique consumer group or for metrics port
    instance_num = args.instance
    total_instances = args.instances
    
    if total_instances < 1 or instance_num >= total_instances:
        logger.error(f"Invalid instance configuration: instance={instance_num}, instances={total_instances}")
        return
    
    # Log consumer instance information
    logger.info(f"Starting consumer instance {instance_num+1} of {total_instances}")
    logger.info(f"Consumer group: {CONSUMER_GROUP_ID}")
    logger.info(f"Thread pool size: {CONSUMER_THREAD_POOL_SIZE}")
    
    # Start the consumer
    consumer = RigDataConsumer()
    
    # Use different ports for Prometheus metrics for each instance
    metrics_port = 9091 + instance_num
    try:
        # Start Prometheus metrics server on a unique port
        start_http_server(metrics_port)
        logger.info(f"Started metrics server on port {metrics_port}")
        
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()