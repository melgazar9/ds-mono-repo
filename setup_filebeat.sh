sudo chown root:root ./filebeat/filebeat.yml
sudo chmod go-w ./filebeat/filebeat.yml
sudo docker compose restart filebeat
