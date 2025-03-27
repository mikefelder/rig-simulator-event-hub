#!/bin/bash
# Full-scale Event Hubs demo script
# Demonstrates 200 rigs sending data at 1 message per second

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to clean up all processes
cleanup() {
    echo -e "\n${RED}Received interrupt, shutting down all processes...${NC}"
    # Kill all child processes in the process group
    pkill -P $$ || true
    # Also attempt explicit killing of any possibly remaining processes
    pkill -f "producer/producer.py" || true
    pkill -f "consumer/consumer.py" || true
    # Extra measures to ensure everything is killed
    kill -- -$$ || true  # Kill entire process group
    echo -e "\n${GREEN}Demo stopped. All processes terminated.${NC}"
    exit 0
}

# Set up trap for Ctrl+C and other terminal signals
trap cleanup SIGINT SIGTERM EXIT

echo -e "${YELLOW}=====================================================================${NC}"
echo -e "${GREEN}Starting Full-Scale Azure Event Hubs Demo - 200 Rigs${NC}"
echo -e "${YELLOW}=====================================================================${NC}"

# Configuration
PRODUCER_COUNT=4
CONSUMER_COUNT=8
RIGS_PER_PRODUCER=50
METRICS_BASE_PORT=9000

# Store PIDs of background processes
PIDS=()

# Reset any existing metrics
echo -e "\n${GREEN}Resetting any existing metrics...${NC}"
python scripts/reset_metrics.py --method both || true

# Start producers
echo -e "\n${GREEN}Starting $PRODUCER_COUNT producers to simulate $((PRODUCER_COUNT * RIGS_PER_PRODUCER)) rigs...${NC}"
for i in $(seq 0 $((PRODUCER_COUNT-1))); do
    echo -e "${GREEN}Starting producer $i with $RIGS_PER_PRODUCER rigs${NC}"
    python producer/producer.py --instance $i --instances $PRODUCER_COUNT &
    PIDS+=($!)
    sleep 2 # Stagger starts to avoid thundering herd
done

# Start consumers
echo -e "\n${GREEN}Starting $CONSUMER_COUNT optimized consumers...${NC}"
for i in $(seq 0 $((CONSUMER_COUNT-1))); do
    echo -e "${GREEN}Starting consumer $i of $CONSUMER_COUNT${NC}"
    if [ "$i" -eq 0 ]; then
        # First consumer resets metrics
        python consumer/consumer.py --instance $i --instances $CONSUMER_COUNT --metrics-api-port $METRICS_BASE_PORT --reset-metrics &
        PIDS+=($!)
    else
        python consumer/consumer.py --instance $i --instances $CONSUMER_COUNT --metrics-api-port $METRICS_BASE_PORT &
        PIDS+=($!)
    fi
    sleep 2 # Stagger starts to avoid thundering herd
done

# Wait for everything to initialize
echo -e "\n${GREEN}Waiting for services to initialize...${NC}"
sleep 10

# Show metrics
echo -e "\n${GREEN}Starting metrics viewer...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the demo when finished${NC}\n"

# Run the metrics viewer in the foreground
# This will run until user hits Ctrl+C, which will trigger our cleanup trap
python scripts/view_metrics.py --instances $CONSUMER_COUNT --refresh 1
