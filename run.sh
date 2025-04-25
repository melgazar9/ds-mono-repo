sudo chown root:root ./filebeat/filebeat.yml && \
sudo docker compose build && \
sudo docker compose up -d

# get the url with code to access kibana
sudo docker compose logs kibana | grep Go
