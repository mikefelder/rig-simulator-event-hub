"""
Tool to help understand and fix partition lag issues
"""
import argparse
import time
import json
import sys
import os
from kafka import KafkaConsumer, TopicPartition
import logging

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    TOPIC_NAME,
    CONSUMER_GROUP_ID
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_partition_info(group_id=CONSUMER_GROUP_ID):
    """Get information about partition assignments and lag."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        group_id=group_id + "-inspector",  # Use a different group to avoid interfering
        auto_offset_reset="latest",
        enable_auto_commit=False
    )
    
    # Get partition metadata
    partitions = consumer.partitions_for_topic(TOPIC_NAME)
    if not partitions:
        logger.error(f"Topic {TOPIC_NAME} not found")
        return
    
    # Create topic partitions
    topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in partitions]
    
    # Get end offsets (latest message position)
    end_offsets = consumer.end_offsets(topic_partitions)
    
    # Try to get committed offsets for the regular consumer group
    try:
        committed_offsets = {}
        for tp in topic_partitions:
            offset = consumer.committed(tp)
            committed_offsets[tp] = offset
    except Exception as e:
        logger.error(f"Error getting committed offsets: {e}")
        committed_offsets = {tp: None for tp in topic_partitions}
    
    # Print partition information
    print(f"\nPartition Information for Topic: {TOPIC_NAME}")
    print(f"Consumer Group: {group_id}")
    print("-" * 80)
    print(f"{'Partition':10} {'End Offset':15} {'Committed Offset':20} {'Lag':10} {'% Complete':12}")
    print("-" * 80)
    
    total_messages = 0
    total_processed = 0
    most_lagged = []
    
    for tp in sorted(topic_partitions, key=lambda x: x.partition):
        end = end_offsets[tp]
        committed = committed_offsets[tp]
        
        if committed is not None:
            lag = end - committed
            if end > 0:
                percent = (committed / end) * 100
            else:
                percent = 100
            
            total_messages += end
            total_processed += committed
            
            if lag > 0:
                most_lagged.append((tp.partition, lag))
            
            print(f"{tp.partition:<10} {end:<15} {committed:<20} {lag:<10} {percent:.2f}%")
        else:
            print(f"{tp.partition:<10} {end:<15} {'Unknown':<20} {'Unknown':<10} {'Unknown':<12}")
    
    # Print summary
    if total_messages > 0:
        overall_percent = (total_processed / total_messages) * 100
        print("-" * 80)
        print(f"Total Messages: {total_messages}")
        print(f"Processed: {total_processed}")
        print(f"Overall Progress: {overall_percent:.2f}%")
    
    # Show most lagged partitions
    if most_lagged:
        print("\nTop 10 Most Lagged Partitions:")
        for partition, lag in sorted(most_lagged, key=lambda x: x[1], reverse=True)[:10]:
            print(f"Partition {partition}: {lag} messages behind")
    
    consumer.close()

def analyze_message_sample(partition, sample_size=5, group_id=CONSUMER_GROUP_ID):
    """Analyze a sample of messages from a specified partition."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    
    # Assign to specific partition
    tp = TopicPartition(TOPIC_NAME, partition)
    consumer.assign([tp])
    
    # Get the committed offset for the regular consumer group
    try:
        # Create a temporary consumer with the actual group ID
        temp_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=KAFKA_SASL_MECHANISM,
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            group_id=group_id
        )
        committed = temp_consumer.committed(tp)
        temp_consumer.close()
    except Exception as e:
        logger.error(f"Could not get committed offset: {e}")
        committed = 0
    
    # Start from the committed offset
    if committed:
        consumer.seek(tp, committed)
        print(f"Starting from committed offset {committed} for partition {partition}")
    else:
        print(f"No committed offset found for partition {partition}, starting from beginning")
    
    # Get end position
    end_offset = consumer.end_offsets([tp])[tp]
    
    print(f"\nAnalyzing messages in partition {partition}")
    print(f"End offset: {end_offset}")
    print(f"Current offset: {committed if committed else 'Beginning'}")
    print(f"Lag: {end_offset - (committed if committed else 0)}")
    
    print("\nSample Messages:")
    print("-" * 80)
    
    count = 0
    rig_ids = set()
    timestamps = []
    
    # Collect sample of messages
    for message in consumer:
        try:
            value = json.loads(message.value.decode('utf-8'))
            
            # Extract key data points
            rig_id = value.get('rigId', 'Unknown')
            rig_ids.add(rig_id)
            timestamp = value.get('timestamp', 'Unknown')
            timestamps.append(timestamp)
            
            # Print summary of message
            print(f"Offset: {message.offset}")
            print(f"Key: {message.key.decode('utf-8') if message.key else 'None'}")
            print(f"Timestamp: {timestamp}")
            print(f"Rig ID: {rig_id}")
            print(f"Message size: {len(message.value)} bytes")
            print("-" * 40)
            
            count += 1
            if count >= sample_size:
                break
                
        except Exception as e:
            print(f"Error processing message: {e}")
            print("-" * 40)
    
    print("\nSummary:")
    print(f"Unique Rig IDs in sample: {', '.join(rig_ids)}")
    if timestamps:
        print(f"Earliest timestamp: {min(timestamps)}")
        print(f"Latest timestamp: {max(timestamps)}")
    
    consumer.close()

def reset_consumer_group(group_id=CONSUMER_GROUP_ID, to_beginning=False):
    """Reset the consumer group offsets."""
    if not group_id:
        logger.error("Consumer group ID is required")
        return
        
    # First, let's make sure the group has no active members
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        group_id=group_id + "-resetter"
    )
    
    # Get partition metadata
    partitions = consumer.partitions_for_topic(TOPIC_NAME)
    if not partitions:
        logger.error(f"Topic {TOPIC_NAME} not found")
        return
    
    # Create topic partitions
    topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in partitions]
    
    # Get end offsets or beginning offsets
    if to_beginning:
        # Get beginning offsets
        target_offsets = consumer.beginning_offsets(topic_partitions)
        target_desc = "beginning"
    else:
        # Get end offsets
        target_offsets = consumer.end_offsets(topic_partitions)
        target_desc = "end"
    
    # Print the plan
    print(f"\nPlan to reset consumer group '{group_id}' to the {target_desc} of each partition")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Partitions: {len(topic_partitions)}")
    print("\nThis will affect ALL consumers using this group ID!")
    confirmation = input("Are you sure you want to continue? (yes/no): ")
    
    if confirmation.lower() != "yes":
        print("Operation canceled")
        consumer.close()
        return
    
    # Set offsets
    reset_offsets = {}
    for tp in topic_partitions:
        reset_offsets[tp] = target_offsets[tp]
    
    try:
        # Actually reset the offsets
        consumer.commit(reset_offsets)
        print(f"\nSuccessfully reset consumer group '{group_id}' to the {target_desc} of each partition")
    except Exception as e:
        logger.error(f"Error resetting offsets: {e}")
        print(f"Failed to reset offsets: {e}")
    
    consumer.close()

def main():
    parser = argparse.ArgumentParser(description='Analyze and fix Event Hub partition issues')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show partition information')
    info_parser.add_argument('--group', type=str, default=CONSUMER_GROUP_ID, help='Consumer group ID')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze messages in a partition')
    analyze_parser.add_argument('partition', type=int, help='Partition number to analyze')
    analyze_parser.add_argument('--sample', type=int, default=5, help='Number of messages to sample')
    analyze_parser.add_argument('--group', type=str, default=CONSUMER_GROUP_ID, help='Consumer group ID')
    
    # Reset command
    reset_parser = subparsers.add_parser('reset', help='Reset consumer group offsets')
    reset_parser.add_argument('--group', type=str, default=CONSUMER_GROUP_ID, help='Consumer group ID')
    reset_parser.add_argument('--to-beginning', action='store_true', help='Reset to beginning instead of end')
    
    args = parser.parse_args()
    
    if args.command == 'info':
        get_partition_info(args.group)
    elif args.command == 'analyze':
        analyze_message_sample(args.partition, args.sample, args.group)
    elif args.command == 'reset':
        reset_consumer_group(args.group, args.to_beginning)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
