set -e

# log to a local file
# sudo docker-compose up --build > logs/docker_logs_$(date +%s).log 2>&1

# log to syslog
# sudo docker-compose up --build | logger 2>&1

# gcp cloud logging
sudo docker-compose up --build | tee >(while IFS= read -r line; do gcloud logging write tap_yfinance_log --severity=INFO --message="$line"; done)
