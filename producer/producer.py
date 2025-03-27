"""
Kafka producer for publishing rig data to Azure Event Hubs.
"""
import json
import logging
import time
import sys
import os
from typing import List, Dict, Any
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import Counter, Histogram, start_http_server

# Add the project root cleardirectory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SSL_CHECK_HOSTNAME,
    KAFKA_API_VERSION,
    PRODUCER_BATCH_SIZE,
    PRODUCER_LINGER_MS,
    PRODUCER_COMPRESSION_TYPE,
    PRODUCER_MAX_BLOCK_MS,
    PRODUCER_RETRIES,
    PRODUCER_RETRY_BACKOFF_MS,
    PRODUCER_BUFFER_MEMORY,
    PRODUCER_MAX_REQUEST_SIZE,
    NUM_RIGS,
    MESSAGE_INTERVAL,
    TOPIC_NAME,
    PRODUCER_THREAD_POOL_SIZE
)
from rig_simulator import RigSimulator  # Changed from producer.rig_simulator to direct import

# Configure logging with more detail
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
messages_sent = Counter('rig_messages_sent_total', 'Total number of messages sent')
message_size = Histogram('rig_message_size_bytes', 'Size of messages in bytes')
send_latency = Histogram('rig_send_latency_seconds', 'Message send latency in seconds')

class RigDataProducer:
    def __init__(self, start_rig=0, end_rig=NUM_RIGS):
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Simulating rigs from {start_rig} to {end_rig-1} ({end_rig-start_rig} total)")
        logger.info(f"Each rig sends 1 message every {MESSAGE_INTERVAL} seconds")
        logger.info(f"Expected message rate: {(end_rig-start_rig)/MESSAGE_INTERVAL:.1f} messages per second")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                security_protocol=KAFKA_SECURITY_PROTOCOL,
                sasl_mechanism=KAFKA_SASL_MECHANISM,
                sasl_plain_username=KAFKA_SASL_USERNAME,
                sasl_plain_password=KAFKA_SASL_PASSWORD,
                ssl_check_hostname=KAFKA_SSL_CHECK_HOSTNAME,
                api_version=KAFKA_API_VERSION,
                batch_size=PRODUCER_BATCH_SIZE,
                linger_ms=PRODUCER_LINGER_MS,
                compression_type=PRODUCER_COMPRESSION_TYPE,
                max_block_ms=PRODUCER_MAX_BLOCK_MS,
                retries=PRODUCER_RETRIES,
                retry_backoff_ms=PRODUCER_RETRY_BACKOFF_MS,
                buffer_memory=PRODUCER_BUFFER_MEMORY,
                max_request_size=PRODUCER_MAX_REQUEST_SIZE,
                request_timeout_ms=120000,  # 2-minute request timeout
                connections_max_idle_ms=540000,  # 9 minutes
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            raise
        
        # Create only the rig simulators for this instance's range
        self.rigs = [RigSimulator(f"RIG_{i:03d}") for i in range(start_rig, end_rig)]
        
        # Thread pool for parallel message sending
        self.executor = ThreadPoolExecutor(max_workers=min(PRODUCER_THREAD_POOL_SIZE, len(self.rigs)))
        
    def send_message(self, rig_id: str, message: str) -> None:
        """Send a message to the Event Hub with round-robin partitioning."""
        try:
            # Record message size
            message_size.observe(len(message))
            
            # Send message with timing using round-robin partitioning (no key specified)
            # This ensures messages are distributed evenly across all partitions
            with send_latency.time():
                future = self.producer.send(
                    TOPIC_NAME,
                    value=message.encode('utf-8')
                    # No key parameter - this enables round-robin partitioning
                )
                future.get(timeout=30)  # Increased timeout to 30 seconds
                messages_sent.inc()
                
        except Exception as e:
            logger.error(f"Error sending message for rig {rig_id}: {str(e)}")
            # Try to reconnect if connection is lost
            try:
                self.producer.flush(timeout=10)
                logger.info("Successfully reconnected to Kafka")
            except Exception as flush_error:
                logger.error(f"Failed to reconnect to Kafka: {str(flush_error)}")
            
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
        
        # Calculate a unique port for each instance using a different range (9100, 9101, 9102, etc.)
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--instance', type=int, default=0)
        args, _ = parser.parse_known_args()
        metrics_port = 9100 + args.instance  # Changed from 9090 to 9100 to avoid conflict with consumers
        
        # Start Prometheus metrics server with unique port
        logger.info(f"Starting Prometheus metrics server on port {metrics_port}")
        start_http_server(metrics_port)
        
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
        try:
            # Use timeout parameter instead of timeout_ms
            self.producer.flush(timeout=10)
            self.producer.close()
            self.executor.shutdown()
            logger.info("Producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {str(e)}")

def main():
    # Support command-line arguments to specify which subset of rigs to handle
    import argparse
    
    parser = argparse.ArgumentParser(description='Run rig data producer')
    parser.add_argument('--instance', type=int, default=0, help='Producer instance number')
    parser.add_argument('--instances', type=int, default=1, help='Total number of producer instances')
    args = parser.parse_args()
    
    # Calculate which rigs this instance should handle
    instance_num = args.instance
    total_instances = args.instances
    
    if total_instances < 1 or instance_num >= total_instances:
        logger.error(f"Invalid instance configuration: instance={instance_num}, instances={total_instances}")
        return
    
    # Log producer instance information
    logger.info(f"Starting producer instance {instance_num+1} of {total_instances}")
    
    # Calculate rig range for this instance
    rigs_per_instance = NUM_RIGS // total_instances
    start_rig = instance_num * rigs_per_instance
    end_rig = (instance_num + 1) * rigs_per_instance if instance_num < total_instances - 1 else NUM_RIGS
    logger.info(f"This instance will handle rigs {start_rig} to {end_rig-1} ({end_rig-start_rig} rigs)")
    
    # Start the producer with the specified rig range
    producer = RigDataProducer(start_rig=start_rig, end_rig=end_rig)
    try:
        producer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()