"""
Kafka producer for publishing rig data to Azure Event Hubs.
"""
import time
import logging
from typing import List
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import Counter, Histogram, start_http_server

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    PRODUCER_BATCH_SIZE,
    PRODUCER_LINGER_MS,
    PRODUCER_COMPRESSION_TYPE,
    PRODUCER_MAX_BLOCK_MS,
    PRODUCER_RETRIES,
    PRODUCER_RETRY_BACKOFF_MS,
    NUM_RIGS,
    MESSAGE_INTERVAL,
    TOPIC_PREFIX,
    NUM_PARTITIONS,
    PRODUCER_THREAD_POOL_SIZE
)
from producer.rig_simulator import RigSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
messages_sent = Counter('rig_messages_sent_total', 'Total number of messages sent')
message_size = Histogram('rig_message_size_bytes', 'Size of messages in bytes')
send_latency = Histogram('rig_send_latency_seconds', 'Message send latency in seconds')

class RigDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=KAFKA_SASL_MECHANISM,
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            batch_size=PRODUCER_BATCH_SIZE,
            linger_ms=PRODUCER_LINGER_MS,
            compression_type=PRODUCER_COMPRESSION_TYPE,
            max_block_ms=PRODUCER_MAX_BLOCK_MS,
            retries=PRODUCER_RETRIES,
            retry_backoff_ms=PRODUCER_RETRY_BACKOFF_MS
        )
        
        # Create rig simulators
        self.rigs = [RigSimulator(f"RIG_{i:03d}") for i in range(NUM_RIGS)]
        
        # Create topics
        self.topics = [f"{TOPIC_PREFIX}-{i:02d}" for i in range(NUM_PARTITIONS)]
        
        # Thread pool for parallel message sending
        self.executor = ThreadPoolExecutor(max_workers=PRODUCER_THREAD_POOL_SIZE)
        
    def send_message(self, rig_id: str, message: str) -> None:
        """Send a message to a topic based on rig_id hash."""
        try:
            # Determine topic based on rig_id hash
            topic_index = hash(rig_id) % NUM_PARTITIONS
            topic = self.topics[topic_index]
            
            # Record message size
            message_size.observe(len(message))
            
            # Send message with timing
            with send_latency.time():
                future = self.producer.send(topic, message.encode('utf-8'))
                future.get(timeout=10)  # Wait for send to complete
                messages_sent.inc()
                
        except Exception as e:
            logger.error(f"Error sending message for rig {rig_id}: {str(e)}")
            
    def run_rig(self, rig: RigSimulator) -> None:
        """Run a single rig simulator and send its data."""
        while True:
            try:
                message = rig.generate_message()
                self.send_message(rig.rig_id, message)
                time.sleep(MESSAGE_INTERVAL)
            except Exception as e:
                logger.error(f"Error in rig {rig.rig_id}: {str(e)}")
                time.sleep(MESSAGE_INTERVAL)
                
    def start(self) -> None:
        """Start the producer and all rig simulators."""
        logger.info("Starting Rig Data Producer...")
        
        # Start Prometheus metrics server
        start_http_server(9090)
        
        # Start all rigs in parallel
        futures = [
            self.executor.submit(self.run_rig, rig)
            for rig in self.rigs
        ]
        
        # Wait for all rigs to complete
        for future in futures:
            future.result()
            
    def close(self) -> None:
        """Close the producer and thread pool."""
        self.producer.close()
        self.executor.shutdown()

def main():
    producer = RigDataProducer()
    try:
        producer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main() 