#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# ==============================================================================
# run_test - test out orchestration setup: 
# - simulation.py, producer.py, consumer.py
# - logging
# - docker-compose.yml ---> RedPanda(Kafka), TimescaleDB
# - continuous aggregates --> materialized views / Dashboard queries
# ==============================================================================


# ==============================================================================
# CONFIGURATION (Matching your specific project tree structure)
# ==============================================================================
COMPOSE_FILE="config/docker-compose.yml"
SCHEMA_FILE="database/schema.sql"
AGGREGATES_FILE="database/continuous_aggregates.sql"
CONSUMER_SCRIPT="src/consumer.py"
PRODUCER_SCRIPT="src/producer.py"

# Text styling for clean terminal outputs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]$(date +'%Y-%m-%d %H:%M:%S')${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]$(date +'%Y-%m-%d %H:%M:%S')${NC} $1"; }
log_err()  { echo -e "${RED}[ERROR]$(date +'%Y-%m-%d %H:%M:%S')${NC} $1"; }

# Check if uv is installed before running the test
if ! command -v uv &> /dev/null; then
    log_err "uv is not installed. Please install it first (https://astral.sh)."
    exit 1
fi

# ==============================================================================
# CLEANUP ARCHITECTURE
# ==============================================================================
cleanup() {
    echo ""
    log_warn "Shutdown signal received! Cleaning up test cluster..."
    
    log_info "Stopping Python background processes..."
    kill $(jobs -p) 2>/dev/null || true
    
    log_info "Tearing down Docker containers..."
    docker compose -f "$COMPOSE_FILE" down --volumes 2>/dev/null || true
    
    log_info "Cleanup complete. Exiting."
}

# Trap script exits, interruptions (Ctrl+C), or terminations to run cleanup
trap cleanup EXIT INT TERM

# ==============================================================================
# STEP 1: DEPLOY INFRASTRUCTURE
# ==============================================================================
log_info "Booting up Redpanda and TimescaleDB containers..."
docker compose -f "$COMPOSE_FILE" up -d

log_info "Waiting for Redpanda stream broker to pass health checks..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' smartgrid-redpanda 2>/dev/null)" == "healthy" ]; do
    sleep 2
done
log_info "Redpanda is ready!"

log_info "Waiting for TimescaleDB to pass health checks..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' smartgrid-db 2>/dev/null)" == "healthy" ]; do
    sleep 2
done
log_info "TimescaleDB is ready!"

# ==============================================================================
# STEP 2: INITIALIZE DATABASE SCHEMAS
# ==============================================================================
log_info "Applying high-density storage schema to TimescaleDB..."
docker exec -i smartgrid-db psql -U postgres -d smartgrid < "$SCHEMA_FILE"

log_info "Applying continuous aggregates configuration..."
docker exec -i smartgrid-db psql -U postgres -d smartgrid < "$AGGREGATES_FILE"
log_info "Database optimization pipelines initialized successfully."

# ==============================================================================
# STEP 3: RUN STREAMING PIPELINE VIA UV
# ==============================================================================
# Inject the root directory into the Python Path so internal module loads work flawlessly
export PYTHONPATH="${PYTHONPATH}:${PWD}"

log_info "Starting Kafka/TimescaleDB consumer pipeline via uv..."
uv run python "$CONSUMER_SCRIPT" &

# Give the consumer a brief window to establish its socket hooks and subscriptions
sleep 3

log_info "Starting Smart Grid simulation producer cluster via uv..."
uv run python "$PRODUCER_SCRIPT" &

log_info "------------------------------------------------------------------"
log_info "Pipeline fully active! Streaming metrics in real-time via uv."
log_info "Monitor the logs directory with: tail -f logs/app_log.jsonl"
log_info "Press [CTRL+C] at any time to terminate the test and wipe containers."
log_info "------------------------------------------------------------------"

# Wait indefinitely on the background execution pipelines
wait

 #==============================================================================
# STEP 4: RUN UNIT AND INTEGRATION TESTS VIA UV PYTEST
# ==============================================================================
# log_info "Initiating test runner suite via uv run pytest..."

# # Disable "exit immediately on error" temporarily so pytest can complete, 
# # and we can accurately relay test suite health statuses.
# set +e
# uv run pytest tests/
# TEST_EXIT_CODE=$?
# set -e

# if [ $TEST_EXIT_CODE -eq 0 ]; then
#     log_info "🎉 All unit and integration test blocks passed smoothly!"
# else
#     log_err "❌ Test framework failures encountered (Exit Code: $TEST_EXIT_CODE)."
# fi

# # Explicitly trigger exit to invoke our trapped cleanup block and wipe Docker footprints
# exit $TEST_EXIT_CODE