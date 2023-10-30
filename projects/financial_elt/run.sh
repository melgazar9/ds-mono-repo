set -e

sudo docker-compose up --build > logs/docker_logs_$(date +%s).log 2>&1