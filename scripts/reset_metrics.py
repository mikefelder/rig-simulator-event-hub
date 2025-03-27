#!/usr/bin/env python3
"""
Script to reset metrics for all running consumer instances.
"""
import argparse
import requests
import subprocess
import time
import os
import signal
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def reset_via_api(start_port=9000, instances=4):
    """Reset metrics via the management API."""
    success_count = 0
    for i in range(instances):
        port = start_port + i
        url = f"http://localhost:{port}/reset-metrics"
        
        try:
            logger.info(f"Resetting metrics for instance {i} via API (port {port})...")
            response = requests.post(url, timeout=5)
            if response.status_code == 200:
                logger.info(f"Successfully reset metrics for instance {i}")
                success_count += 1
            else:
                logger.error(f"Failed to reset metrics for instance {i}: {response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to instance {i}: {e}")
    
    return success_count

def reset_via_signal(instances=4):
    """Reset metrics by sending USR1 signal to consumer processes."""
    # Find consumer processes
    try:
        logger.info("Looking for consumer processes...")
        result = subprocess.run(
            ["pgrep", "-f", "consumer.py"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        pids = result.stdout.strip().split("\n")
        
        if not pids or pids[0] == '':
            logger.error("No consumer processes found")
            return 0
        
        logger.info(f"Found {len(pids)} consumer processes")
        
        # Send signal to each process
        success_count = 0
        for pid in pids:
            pid = pid.strip()
            if pid:
                try:
                    logger.info(f"Sending SIGUSR1 to process {pid}")
                    os.kill(int(pid), signal.SIGUSR1)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send signal to process {pid}: {e}")
        
        # Give processes time to reset metrics
        if success_count > 0:
            logger.info("Waiting for metrics to reset...")
            time.sleep(2)
            
        return success_count
            
    except subprocess.CalledProcessError:
        logger.error("Failed to find consumer processes")
        return 0

def main():
    parser = argparse.ArgumentParser(description='Reset metrics for all consumer instances')
    parser.add_argument('--method', choices=['api', 'signal', 'both'], default='both', 
                       help='Method to use for resetting metrics')
    parser.add_argument('--api-port', type=int, default=9000, 
                       help='Base port for metrics API (default: 9000)')
    parser.add_argument('--instances', type=int, default=4, 
                       help='Number of consumer instances')
    
    args = parser.parse_args()
    
    success = False
    
    if args.method in ['api', 'both']:
        logger.info("Attempting to reset metrics via API...")
        count = reset_via_api(args.api_port, args.instances)
        if count > 0:
            success = True
            logger.info(f"Reset metrics for {count} instances via API")
    
    if args.method in ['signal', 'both'] and not success:
        logger.info("Attempting to reset metrics via signal...")
        count = reset_via_signal(args.instances)
        if count > 0:
            success = True
            logger.info(f"Reset metrics for {count} instances via signal")
    
    if success:
        logger.info("Metrics reset completed successfully")
    else:
        logger.error("Failed to reset metrics for any instances")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
