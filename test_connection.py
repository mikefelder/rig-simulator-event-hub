from kafka import KafkaProducer
import json
import time
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SSL_CHECK_HOSTNAME,
    KAFKA_API_VERSION,
    TOPIC_NAME
)

def test_connection():
    print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        # Create producer with minimal configuration
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=KAFKA_SASL_MECHANISM,
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            ssl_check_hostname=KAFKA_SSL_CHECK_HOSTNAME,
            api_version=KAFKA_API_VERSION,
            request_timeout_ms=30000
        )
        
        print("Successfully created Kafka producer")
        
        # Send a test message
        test_message = {"test": "message", "timestamp": time.time()}
        future = producer.send(TOPIC_NAME, json.dumps(test_message).encode('utf-8'))
        future.get(timeout=10)
        
        print("Successfully sent test message")
        
        # Close the producer
        producer.close()
        print("Successfully closed producer")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection() 