set -e

#sudo chown root:root ./filebeat/filebeat.yml && \ ---> if not root it might need to be set to root privaleges on linux

sudo docker compose build && \
sudo docker compose up -d

# get the url with code to access kibana
sudo docker compose logs kibana | grep Go
