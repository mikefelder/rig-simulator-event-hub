"""
Utility script to troubleshoot Kafka consumer issues.
"""
import json
import logging
import sys
import os
import time
from collections import Counter
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    TOPIC_NAME
)

def debug_message_structure():
    """
    Connect to Kafka and display the full structure of messages for debugging.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        auto_offset_reset='latest',
        group_id="troubleshooter"
    )
    
    logger.info("Starting message structure debugger...")
    try:
        for message in consumer:
            try:
                msg_data = json.loads(message.value.decode('utf-8'))
                logger.info("Message structure:")
                logger.info(json.dumps(msg_data, indent=2))
                
                # Specifically examine the alerts structure
                if 'alerts' in msg_data:
                    logger.info("Alert structure type: %s", type(msg_data['alerts']).__name__)
                    logger.info("Alert content: %s", json.dumps(msg_data['alerts'], indent=2))
                
            except Exception as e:
                logger.error(f"Error processing debug message: {str(e)}")
    
    finally:
        consumer.close()
        logger.info("Debugger closed")

def analyze_message_volume():
    """
    Analyze message volume and distribution per rig.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        auto_offset_reset='latest',
        group_id="volume_analyzer"
    )
    
    logger.info("Starting message volume analyzer...")
    try:
        rig_counter = Counter()
        start_time = time.time()
        message_count = 0
        duration = 10  # analyze for 10 seconds
        
        while time.time() - start_time < duration:
            messages = consumer.poll(timeout_ms=100)
            for topic_partition, msg_list in messages.items():
                for message in msg_list:
                    try:
                        msg_data = json.loads(message.value.decode('utf-8'))
                        rig_id = msg_data.get('rigId')
                        if rig_id:
                            rig_counter[rig_id] += 1
                        message_count += 1
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
        
        elapsed = time.time() - start_time
        msgs_per_second = message_count / elapsed if elapsed > 0 else 0
        
        logger.info(f"Analysis complete after {elapsed:.2f} seconds")
        logger.info(f"Total messages: {message_count}")
        logger.info(f"Messages per second: {msgs_per_second:.2f}")
        logger.info(f"Number of unique rigs: {len(rig_counter)}")
        logger.info("Top 10 rigs by message count:")
        for rig_id, count in rig_counter.most_common(10):
            logger.info(f"  {rig_id}: {count} messages")
            
    finally:
        consumer.close()
        logger.info("Analyzer closed")

if __name__ == "__main__":
    # Uncomment the function you want to run
    # debug_message_structure()
    analyze_message_volume()
