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
else
    export ELASTICSEARCH_PORT=9200
    export ELASTICSEARCH_TRANSPORT_PORT=9300
    export KIBANA_PORT=5601
    export TAP_YFINANCE_PORT=5000
    export TAP_YAHOOQUERY_PORT=5001
    export TAP_POLYGON_PORT=5002
    export TAP_FMP_PORT=5003
fi

export ENVIRONMENT

echo "Starting $ENVIRONMENT environment..."

# sudo chown root:root ./filebeat/filebeat.yml && \ ---> if not root it might need to be set to root privaleges on linux
# or run the below...
# psudo chmod go-w ./filebeat/filebeat.yml

COMPOSE_FILE="docker-compose-${ENVIRONMENT}.yml"

echo "Building containers using $COMPOSE_FILE..."
sudo docker compose -f $COMPOSE_FILE build
echo "Build completed. Starting containers..."
sudo docker compose -f $COMPOSE_FILE up -d
echo "Containers started."

sleep 3
./setup_filebeat.sh $ENVIRONMENT
sleep 2
sudo docker compose logs kibana | grep Go  # get the url with code to access kibana