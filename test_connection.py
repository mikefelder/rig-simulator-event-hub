from kafka import KafkaProducer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get connection details
bootstrap_servers = f"{os.getenv('EVENT_HUB_NAMESPACE')}:9093"
connection_string = os.getenv('EVENT_HUB_CONNECTION_STRING')

print(f"Connecting to: {bootstrap_servers}")

# Create producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=connection_string,
    ssl_check_hostname=True,
    api_version=(1, 0, 0)
)

# Try to send a test message
try:
    future = producer.send('test-topic', b'test message')
    future.get(timeout=10)
    print("Successfully sent test message!")
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    producer.close() 