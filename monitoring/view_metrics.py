"""
Simple script to view metrics from producer and consumer instances.
"""
import requests
import argparse
import time
import os
import sys
import re
from tabulate import tabulate

def get_metrics(port):
    """Retrieve metrics from the specified port."""
    try:
        response = requests.get(f"http://localhost:{port}/metrics", timeout=2)
        if response.status_code == 200:
            return response.text
        else:
            return f"Error: HTTP {response.status_code}"
    except requests.exceptions.ConnectionError:
        return "Error: Connection refused. Is the application running?"
    except Exception as e:
        return f"Error: {str(e)}"

def parse_metrics(metrics_text, metric_names):
    """Extract specific metrics from the raw text."""
    results = {}
    for line in metrics_text.split('\n'):
        if line.startswith('#'):
            continue
        
        for metric_name in metric_names:
            if line.startswith(metric_name) and '{' not in line:
                parts = line.split()
                if len(parts) >= 2:
                    results[metric_name] = float(parts[1])
                    break
    
    return results

def parse_lag_metrics(metrics_text):
    """Extract consumer lag metrics with partition labels."""
    lag_metrics = {}
    # Pattern to match lines like: rig_consumer_lag{partition="0"} 42
    pattern = r'rig_consumer_lag\{partition="(\d+)"\}\s+([\d\.]+)'
    
    for line in metrics_text.split('\n'):
        match = re.search(pattern, line)
        if match:
            partition = match.group(1)
            lag_value = float(match.group(2))
            lag_metrics[partition] = lag_value
    
    return lag_metrics

def parse_partition_assignments(metrics_text):
    """Extract partition assignments from the metrics output."""
    assigned_partitions = set()
    # Pattern to match partition assignments: rig_consumer_lag{partition="X"} Y
    pattern = r'rig_consumer_lag\{partition="(\d+)"\}\s+[\d\.]+'
    
    for line in metrics_text.split('\n'):
        for match in re.finditer(pattern, line):
            partition = match.group(1)
            assigned_partitions.add(partition)
    
    return sorted(list(assigned_partitions), key=int)

def display_metrics(producer_port=9090, consumer_ports=None, show_lag=False):
    """Display key metrics from producer and consumer instances."""
    if consumer_ports is None:
        consumer_ports = [9091]
    
    while True:
        os.system('clear' if os.name == 'posix' else 'cls')
        
        # Producer metrics
        producer_metrics_text = get_metrics(producer_port)
        if "Error" not in producer_metrics_text:
            producer_metrics = parse_metrics(producer_metrics_text, ['rig_messages_sent_total'])
            print(f"Producer Metrics (port {producer_port}):")
            print(f"  Messages sent: {producer_metrics.get('rig_messages_sent_total', 'N/A')}")
        else:
            print(f"Producer Metrics (port {producer_port}): {producer_metrics_text}")
        
        print("\nConsumer Metrics:")
        table_data = []
        
        for i, port in enumerate(consumer_ports):
            consumer_metrics_text = get_metrics(port)
            if "Error" not in consumer_metrics_text:
                consumer_metrics = parse_metrics(consumer_metrics_text, [
                    'rig_messages_processed_total', 
                    'rig_processing_errors_total',
                    'rig_critical_alerts_total'
                ])
                
                # Get assigned partitions
                assigned_partitions = parse_partition_assignments(consumer_metrics_text)
                active_partitions = len(assigned_partitions)
                
                row = [
                    f"Consumer {i}",
                    port,
                    consumer_metrics.get('rig_messages_processed_total', 'N/A'),
                    consumer_metrics.get('rig_processing_errors_total', 'N/A'),
                    consumer_metrics.get('rig_critical_alerts_total', 'N/A'),
                    active_partitions
                ]
                
                # Calculate total lag across all partitions for this consumer
                if show_lag:
                    lag_metrics = parse_lag_metrics(consumer_metrics_text)
                    total_lag = sum(lag_metrics.values())
                    row.append(f"{total_lag:.0f}")
                
                table_data.append(row)
            else:
                row = [f"Consumer {i}", port, "Error", "Error", "Error", "Error"]
                if show_lag:
                    row.append("Error")
                table_data.append(row)
        
        headers = ["Instance", "Port", "Messages Processed", "Errors", "Critical Alerts", "Active Partitions"]
        if show_lag:
            headers.append("Total Lag")
        
        print(tabulate(table_data, headers=headers))
        
        # Display partition assignments for each consumer
        print("\nPartition Assignments:")
        for i, port in enumerate(consumer_ports):
            consumer_metrics_text = get_metrics(port)
            if "Error" not in consumer_metrics_text:
                assigned_partitions = parse_partition_assignments(consumer_metrics_text)
                if assigned_partitions:
                    partition_chunks = [assigned_partitions[j:j+10] for j in range(0, len(assigned_partitions), 10)]
                    print(f"\nConsumer {i} (Port {port}) - {len(assigned_partitions)} partitions:")
                    for chunk in partition_chunks:
                        print("  " + ", ".join(chunk))
                else:
                    print(f"\nConsumer {i} (Port {port}): No partitions assigned")
        
        # Display detailed lag per partition if requested
        if show_lag:
            print("\nConsumer Lag Details (per partition):")
            for i, port in enumerate(consumer_ports):
                consumer_metrics_text = get_metrics(port)
                if "Error" not in consumer_metrics_text:
                    lag_metrics = parse_lag_metrics(consumer_metrics_text)
                    if lag_metrics:
                        print(f"\nConsumer {i} (Port {port}):")
                        lag_data = sorted([(int(p), v) for p, v in lag_metrics.items()], key=lambda x: x[0])
                        
                        # Group partitions to fit on screen
                        columns = 5  # Adjust based on terminal width
                        for j in range(0, len(lag_data), columns):
                            chunk = lag_data[j:j+columns]
                            line = "  ".join([f"Partition {p}: {v:.0f}" for p, v in chunk])
                            print(f"  {line}")
                    else:
                        print(f"\nConsumer {i} (Port {port}): No lag metrics available")
        
        print("\nPress Ctrl+C to exit. Refreshing in 2 seconds...")
        time.sleep(2)

def main():
    parser = argparse.ArgumentParser(description='View metrics from producer and consumer instances')
    parser.add_argument('--producer-port', type=int, default=9090, help='Producer metrics port')
    parser.add_argument('--consumer-instances', type=int, default=1, help='Number of consumer instances')
    parser.add_argument('--show-lag', action='store_true', help='Show consumer lag metrics per partition')
    args = parser.parse_args()
    
    consumer_ports = [9091 + i for i in range(args.consumer_instances)]
    
    try:
        display_metrics(args.producer_port, consumer_ports, args.show_lag)
    except KeyboardInterrupt:
        print("\nExiting metrics viewer.")

if __name__ == "__main__":
    main()
