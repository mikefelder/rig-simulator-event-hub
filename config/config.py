import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Event Hubs Configuration
EVENT_HUB_NAMESPACE = os.getenv('EVENT_HUB_NAMESPACE')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')
EVENT_HUB_CONNECTION_STRING = os.getenv('EVENT_HUB_CONNECTION_STRING')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = f"{EVENT_HUB_NAMESPACE}:9093"  # Namespace already includes full domain
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "$ConnectionString"
KAFKA_SASL_PASSWORD = EVENT_HUB_CONNECTION_STRING
KAFKA_SSL_CHECK_HOSTNAME = True
KAFKA_API_VERSION = (1, 0, 0)  # Azure Event Hubs supports Kafka 1.0

# Producer Configuration
PRODUCER_BATCH_SIZE = 32768  # Increased to 32KB batch size for better throughput
PRODUCER_LINGER_MS = 10  # Increased to 10ms to allow more messages to batch
PRODUCER_COMPRESSION_TYPE = "gzip"
PRODUCER_MAX_BLOCK_MS = 60000  # 1 minute timeout
PRODUCER_RETRIES = 5  # Increased retries
PRODUCER_RETRY_BACKOFF_MS = 1000
PRODUCER_BUFFER_MEMORY = 33554432  # 32MB buffer memory
PRODUCER_MAX_REQUEST_SIZE = 1048576  # 1MB max request size

# Consumer Configuration
CONSUMER_GROUP_ID = "rig-data-processor"
CONSUMER_AUTO_OFFSET_RESET = "earliest"
CONSUMER_ENABLE_AUTO_COMMIT = False
CONSUMER_MAX_POLL_RECORDS = 1000  # Increased to process more messages per poll
CONSUMER_SESSION_TIMEOUT_MS = 60000
CONSUMER_HEARTBEAT_INTERVAL_MS = 20000
CONSUMER_FETCH_MIN_BYTES = 65536  # 64KB minimum fetch size
CONSUMER_FETCH_MAX_BYTES = 524288  # 512KB maximum fetch size

# Monitoring Configuration
PROMETHEUS_PORT = 9090
METRICS_UPDATE_INTERVAL = 1  # seconds

# Rig Simulation Configuration
NUM_RIGS = 200
MESSAGE_INTERVAL = 0.002  # Reduced to 2ms to achieve ~500 messages/second
NUM_KEY_VALUE_PAIRS = 1000
MIN_MESSAGE_SIZE = 2048  # bytes

# Topics Configuration
TOPIC_NAME = EVENT_HUB_NAME
NUM_PARTITIONS = 64  # Increased to support higher throughput
PARTITION_KEY_PREFIX = "RIG_"  # Prefix for partition keys

# Error Handling
MAX_RETRIES = 5  # Increased retries
RETRY_DELAY = 5  # seconds
DEAD_LETTER_TOPIC = "rig-data-dlq"

# Performance Tuning
MAX_CONCURRENT_CONSUMERS = 16  # Increased for better parallel processing
CONSUMER_THREAD_POOL_SIZE = 32  # Increased thread pool size
PRODUCER_THREAD_POOL_SIZE = 16  # Increased thread pool size 