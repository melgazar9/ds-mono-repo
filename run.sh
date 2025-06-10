set -e

# sudo chown root:root ./filebeat/filebeat.yml && \ ---> if not root it might need to be set to root privaleges on linux
# or run the below...
# psudo chmod go-w ./filebeat/filebeat.yml

sudo docker compose build && \
sudo docker compose up -d

sleep 3
./setup_filebeat.sh
sleep 2
sudo docker compose logs kibana | grep Go  # get the url with code to access kibana