import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Event Hubs Configuration
EVENT_HUB_NAMESPACE = os.getenv('EVENT_HUB_NAMESPACE')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')
EVENT_HUB_CONNECTION_STRING = os.getenv('EVENT_HUB_CONNECTION_STRING')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "$ConnectionString"
KAFKA_SASL_PASSWORD = EVENT_HUB_CONNECTION_STRING

# Producer Configuration
PRODUCER_BATCH_SIZE = 16384  # 16KB batch size
PRODUCER_LINGER_MS = 5  # Wait 5ms to batch messages
PRODUCER_COMPRESSION_TYPE = "gzip"
PRODUCER_MAX_BLOCK_MS = 60000  # 1 minute timeout
PRODUCER_RETRIES = 3
PRODUCER_RETRY_BACKOFF_MS = 1000

# Consumer Configuration
CONSUMER_GROUP_ID = "rig-data-processor"
CONSUMER_AUTO_OFFSET_RESET = "earliest"
CONSUMER_ENABLE_AUTO_COMMIT = False
CONSUMER_MAX_POLL_RECORDS = 500
CONSUMER_SESSION_TIMEOUT_MS = 60000
CONSUMER_HEARTBEAT_INTERVAL_MS = 20000

# Monitoring Configuration
PROMETHEUS_PORT = 9090
METRICS_UPDATE_INTERVAL = 1  # seconds

# Rig Simulation Configuration
NUM_RIGS = 200
MESSAGE_INTERVAL = 1  # seconds
NUM_KEY_VALUE_PAIRS = 1000
MIN_MESSAGE_SIZE = 2048  # bytes

# Topics Configuration
TOPIC_PREFIX = "rig-data"
NUM_PARTITIONS = 32  # For high throughput and parallel processing

# Error Handling
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
DEAD_LETTER_TOPIC = "rig-data-dlq"

# Performance Tuning
MAX_CONCURRENT_CONSUMERS = 8
CONSUMER_THREAD_POOL_SIZE = 16
PRODUCER_THREAD_POOL_SIZE = 8 