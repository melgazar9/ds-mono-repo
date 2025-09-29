#!/bin/bash

set -e

# Usage: ./cleanup.sh [dev|production|all]
# Examples:
#   ./cleanup.sh dev        # Stop and remove dev environment
#   ./cleanup.sh production # Stop and remove production environment
#   ./cleanup.sh all        # Stop and remove both environments

ENVIRONMENT=${1:-all}
DOCKER="sudo docker"

cleanup_environment() {
    local env=$1
    local compose_file="docker-compose-${env}.yml"

    if [ -f "$compose_file" ]; then
        echo "Stopping and removing $env environment..."
        echo "Will run: $DOCKER compose -p ds-mono-repo-$env -f $compose_file down -v"
        read -p "Continue? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            $DOCKER compose -p ds-mono-repo-$env -f $compose_file down -v
            echo "$env environment cleaned up."
        else
            echo "Cancelled."
        fi
    else
        echo "Warning: $compose_file not found, skipping $env environment"
    fi
}

case $ENVIRONMENT in
    dev)
        cleanup_environment "dev"
        ;;
    production)
        cleanup_environment "production"
        ;;
    all)
        cleanup_environment "dev"
        cleanup_environment "production"

        # Additional cleanup for orphaned containers and resources
        echo "Cleaning up orphaned resources..."

        # Remove containers matching our naming pattern
        $DOCKER ps -aq --filter "name=elasticsearch-" | xargs -r $DOCKER rm -f 2>/dev/null || true
        $DOCKER ps -aq --filter "name=kibana-" | xargs -r $DOCKER rm -f 2>/dev/null || true
        $DOCKER ps -aq --filter "name=filebeat-" | xargs -r $DOCKER rm -f 2>/dev/null || true
        $DOCKER ps -aq --filter "name=tap-" | xargs -r $DOCKER rm -f 2>/dev/null || true

        # Clean up networks and volumes
        $DOCKER network ls --filter "name=ds_elastic_" -q | xargs -r $DOCKER network rm 2>/dev/null || true
        $DOCKER volume ls --filter "name=ds_elasticsearch_data_" -q | xargs -r $DOCKER volume rm 2>/dev/null || true
        $DOCKER volume ls --filter "name=ds_kibana_data_" -q | xargs -r $DOCKER volume rm 2>/dev/null || true
        $DOCKER volume ls --filter "name=ds_filebeat_data_" -q | xargs -r $DOCKER volume rm 2>/dev/null || true

        echo "All environments and orphaned resources cleaned up."
        ;;
    *)
        echo "Usage: $0 [dev|production|all]"
        echo "  dev        - Stop and remove dev environment"
        echo "  production - Stop and remove production environment"
        echo "  all        - Stop and remove both environments (default)"
        exit 1
        ;;
esac