set -e

# Usage: ./run.sh [dev|production]
# Examples:
#   ./run.sh dev        # Run dev environment (ports 9200, 5601, 5000-5003)
#   ./run.sh production # Run production environment (ports 9201, 5602, 5010-5013)

ENVIRONMENT=${1:-dev}

# Set environment-specific ports
if [ "$ENVIRONMENT" = "production" ]; then
    export ELASTICSEARCH_PORT=9201
    export ELASTICSEARCH_TRANSPORT_PORT=9301
    export KIBANA_PORT=5602
    export TAP_YFINANCE_PORT=5010
    export TAP_YAHOOQUERY_PORT=5011
    export TAP_POLYGON_PORT=5012
    export TAP_FMP_PORT=5013
    export TAP_FRED_PORT=5014
else
    export ELASTICSEARCH_PORT=9200
    export ELASTICSEARCH_TRANSPORT_PORT=9300
    export KIBANA_PORT=5601
    export TAP_YFINANCE_PORT=5000
    export TAP_YAHOOQUERY_PORT=5001
    export TAP_POLYGON_PORT=5002
    export TAP_FMP_PORT=5003
    export TAP_FRED_PORT=5004
fi

export ENVIRONMENT

echo "Starting $ENVIRONMENT environment..."

# sudo chown root:root ./filebeat/filebeat.yml && \ ---> if not root it might need to be set to root privaleges on linux
# or run the below...
# psudo chmod go-w ./filebeat/filebeat.yml

COMPOSE_FILE="docker-compose-${ENVIRONMENT}.yml"

# Extract FINANCIAL_ELT_TAPS_TO_RUN from .env safely
TAPS_TO_RUN=""
if [ -f .env ]; then
    TAPS_TO_RUN=$(grep "^FINANCIAL_ELT_TAPS_TO_RUN=" .env | cut -d'=' -f2)
fi

# Always start core services
CORE_SERVICES="elasticsearch kibana filebeat"

# Parse FINANCIAL_ELT_TAPS_TO_RUN to get tap services
if [ -n "$TAPS_TO_RUN" ]; then
    TAP_SERVICES=$(echo "$TAPS_TO_RUN" | tr ',' ' ')
    SERVICES="$CORE_SERVICES $TAP_SERVICES"
    echo "Selected taps from .env: $TAP_SERVICES"
else
    echo "Warning: FINANCIAL_ELT_TAPS_TO_RUN not set in .env, starting all services"
    SERVICES=""
fi

echo "Building containers using $COMPOSE_FILE..."
if [ -n "$SERVICES" ]; then
    echo "Building only selected services: $SERVICES"
    sudo docker compose -p ds-mono-repo-$ENVIRONMENT -f $COMPOSE_FILE build $SERVICES
else
    sudo docker compose -p ds-mono-repo-$ENVIRONMENT -f $COMPOSE_FILE build
fi

echo "Build completed. Starting containers..."
if [ -n "$SERVICES" ]; then
    echo "Starting only selected services: $SERVICES"
    sudo docker compose -p ds-mono-repo-$ENVIRONMENT -f $COMPOSE_FILE up -d $SERVICES
else
    sudo docker compose -p ds-mono-repo-$ENVIRONMENT -f $COMPOSE_FILE up -d
fi
echo "Containers started."

sleep 3
./setup_filebeat.sh $ENVIRONMENT
sleep 2
sudo docker compose logs kibana | grep Go  # get the url with code to access kibana