#!/usr/bin/env python3
"""
Script to view metrics from all running consumer instances,
including messages processed per second.
"""
import argparse
import time
import requests
import re
import concurrent.futures
from tabulate import tabulate
import os
import sys
import signal

# Add color support
from colorama import init, Fore, Style
init()

# Configure default values
DEFAULT_BASE_PORT = 9091
DEFAULT_INSTANCES = 4
DEFAULT_REFRESH_INTERVAL = 2

# Dictionary to store previous message counts for rate calculation
prev_message_counts = {}
prev_timestamp = time.time()

def format_number(num):
    """Format number with commas as thousands separators."""
    return f"{num:,}" if num is not None else "N/A"

def get_metrics_from_prometheus(port):
    """Fetch metrics from Prometheus endpoint."""
    try:
        response = requests.get(f"http://localhost:{port}/metrics", timeout=1)
        if response.status_code == 200:
            return response.text
        return None
    except requests.exceptions.RequestException:
        return None

def parse_prometheus_metrics(metrics_text, instance_id):
    """Parse Prometheus metrics text to extract key metrics."""
    if not metrics_text:
        return None
    
    metrics = {
        "messages_processed": None,
        "errors": None,
        "critical_alerts": None,
        "partitions": [],
        "lag": 0
    }
    
    # Parse messages processed
    match = re.search(r'rig_messages_processed_total\s+(\d+)', metrics_text)
    if match:
        metrics["messages_processed"] = int(match.group(1))
    
    # Parse errors
    match = re.search(r'rig_processing_errors_total\s+(\d+)', metrics_text)
    if match:
        metrics["errors"] = int(match.group(1))
    
    # Parse critical alerts
    match = re.search(r'rig_critical_alerts_total\s+(\d+)', metrics_text)
    if match:
        metrics["critical_alerts"] = int(match.group(1))
    
    # Parse partitions and lag
    partition_pattern = r'rig_consumer_lag\{partition="(\d+)"\}\s+(\d+)'
    for match in re.finditer(partition_pattern, metrics_text):
        partition = int(match.group(1))
        lag = int(match.group(2))
        metrics["partitions"].append(partition)
        metrics["lag"] += lag
    
    return metrics

def calculate_messages_per_second(instance_id, current_count):
    """Calculate messages processed per second."""
    global prev_message_counts, prev_timestamp
    
    if instance_id not in prev_message_counts:
        prev_message_counts[instance_id] = current_count
        return 0
    
    now = time.time()
    time_diff = now - prev_timestamp
    count_diff = current_count - prev_message_counts[instance_id]
    
    # Update previous values for next calculation
    prev_message_counts[instance_id] = current_count
    
    if time_diff > 0:
        return count_diff / time_diff
    return 0

def update_global_timestamp():
    """Update the global timestamp for rate calculations."""
    global prev_timestamp
    prev_timestamp = time.time()

def get_all_metrics(base_port, instances):
    """Get metrics from all consumer instances in parallel."""
    ports = [base_port + i for i in range(instances)]
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=instances) as executor:
        future_to_port = {executor.submit(get_metrics_from_prometheus, port): port for port in ports}
        
        for future in concurrent.futures.as_completed(future_to_port):
            port = future_to_port[future]
            instance_id = port - base_port
            try:
                metrics_text = future.result()
                metrics = parse_prometheus_metrics(metrics_text, instance_id)
                
                if metrics:
                    # Calculate messages per second
                    msg_per_sec = calculate_messages_per_second(instance_id, metrics["messages_processed"])
                    metrics["msg_per_sec"] = msg_per_sec
                    
                    results.append({
                        "instance_id": instance_id,
                        "port": port,
                        "metrics": metrics
                    })
            except Exception as e:
                print(f"Error fetching metrics from port {port}: {e}")
    
    # Sort by instance ID
    results.sort(key=lambda x: x["instance_id"])
    return results

def display_metrics(metrics_list, show_partitions=False):
    """Display metrics in a formatted table."""
    if not metrics_list:
        print(f"{Fore.RED}No consumer metrics available. Are the consumers running?{Style.RESET_ALL}")
        return
    
    # Updated headers with clearer descriptions for both error types
    headers = ["Instance", "Port", "Messages Processed", "Msg/sec", 
               "Processing Errors", "Rig Critical Alerts", 
               "Active Partitions", "Total Lag"]
    
    # Add a footnote to explain the columns
    footnote = (
        f"\n{Fore.CYAN}* Processing Errors: Actual errors that occurred during message processing"
        f"\n* Rig Critical Alerts: Count of CRITICAL severity alerts detected in rig data{Style.RESET_ALL}"
    )
    
    table_data = []
    
    for item in metrics_list:
        instance_id = item["instance_id"]
        port = item["port"]
        metrics = item["metrics"]
        
        row = [
            f"Consumer {instance_id}",
            port,
            format_number(metrics["messages_processed"]),
            f"{metrics.get('msg_per_sec', 0):.2f}",
            format_number(metrics["errors"]),
            format_number(metrics["critical_alerts"]),
            len(metrics["partitions"]),
            format_number(metrics["lag"])
        ]
        table_data.append(row)
    
    print("\nConsumer Metrics:")
    print(tabulate(table_data, headers=headers, tablefmt="simple"))
    print(footnote)
    
    # Display partition assignments
    if show_partitions:
        print("\nPartition Assignments:\n")
        for item in metrics_list:
            instance_id = item["instance_id"]
            port = item["port"]
            metrics = item["metrics"]
            partitions = sorted(metrics["partitions"])
            
            print(f"{Fore.GREEN}Consumer {instance_id} (Port {port}) - {len(partitions)} partitions:{Style.RESET_ALL}")
            
            # Group partitions into rows of 10
            for i in range(0, len(partitions), 10):
                row = partitions[i:i+10]
                print(f"  {', '.join(str(p) for p in row)}")
            print()

def display_lag_details(metrics_list):
    """Display detailed lag information per partition."""
    if not metrics_list:
        return
    
    print("\nConsumer Lag Details (per partition):\n")
    
    for item in metrics_list:
        instance_id = item["instance_id"]
        port = item["port"]
        metrics_text = get_metrics_from_prometheus(port)
        
        if not metrics_text:
            continue
        
        print(f"{Fore.YELLOW}Consumer {instance_id} (Port {port}):{Style.RESET_ALL}")
        
        # Parse partition lag
        partition_lags = {}
        partition_pattern = r'rig_consumer_lag\{partition="(\d+)"\}\s+(\d+)'
        for match in re.finditer(partition_pattern, metrics_text):
            partition = int(match.group(1))
            lag = int(match.group(2))
            partition_lags[partition] = lag
        
        # Sort partitions and display in rows of 5
        sorted_partitions = sorted(partition_lags.keys())
        
        # Prepare data for display in rows of 5
        rows = []
        current_row = []
        
        for partition in sorted_partitions:
            lag = partition_lags[partition]
            lag_color = Fore.GREEN if lag < 1000 else (Fore.YELLOW if lag < 10000 else Fore.RED)
            current_row.append(f"  Partition {partition}: {lag_color}{lag}{Style.RESET_ALL}")
            
            if len(current_row) >= 5:
                rows.append(current_row)
                current_row = []
        
        if current_row:
            rows.append(current_row)
        
        # Print rows
        for row in rows:
            print("  ".join(row))
        
        print()

def check_producer(producer_port=9090):
    """Check if producer metrics are available."""
    try:
        response = requests.get(f"http://localhost:{producer_port}/metrics", timeout=1)
        if response.status_code == 200:
            return True
        return False
    except requests.exceptions.RequestException:
        return False

def main():
    parser = argparse.ArgumentParser(description='View metrics from all consumer instances')
    parser.add_argument('--base-port', type=int, default=DEFAULT_BASE_PORT,
                       help=f'Base port for consumer metrics (default: {DEFAULT_BASE_PORT})')
    parser.add_argument('--instances', type=int, default=DEFAULT_INSTANCES,
                       help=f'Number of consumer instances (default: {DEFAULT_INSTANCES})')
    parser.add_argument('--refresh', type=float, default=DEFAULT_REFRESH_INTERVAL,
                       help=f'Refresh interval in seconds (default: {DEFAULT_REFRESH_INTERVAL})')
    parser.add_argument('--producer-port', type=int, default=9090,
                       help='Port for producer metrics (default: 9090)')
    parser.add_argument('--no-partitions', action='store_true',
                       help='Hide partition assignment details')
    parser.add_argument('--no-lag', action='store_true',
                       help='Hide detailed lag information')
    
    args = parser.parse_args()
    
    try:
        while True:
            # Clear screen
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # Check producer status
            producer_status = check_producer(args.producer_port)
            if producer_status:
                print(f"{Fore.GREEN}Producer Metrics (port {args.producer_port}): Available{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}Producer Metrics (port {args.producer_port}): Error: Connection refused. Is the application running?{Style.RESET_ALL}")
            
            # Get and display consumer metrics
            metrics = get_all_metrics(args.base_port, args.instances)
            display_metrics(metrics, not args.no_partitions)
            
            # Display lag details if requested
            if not args.no_lag:
                display_lag_details(metrics)
            
            # Update timestamp for next rate calculation
            update_global_timestamp()
            
            print(f"\nPress Ctrl+C to exit. Refreshing in {args.refresh} seconds...")
            time.sleep(args.refresh)
    
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
