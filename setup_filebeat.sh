ENVIRONMENT=${1:-dev}

sudo chown root:root ./filebeat/filebeat.yml
sudo chmod go-w ./filebeat/filebeat.yml
sudo docker compose -f docker-compose.$ENVIRONMENT.yml restart filebeat
