### docker ###

sudo docker build \
  --build-arg ENVIRONMENT=${ENVIRONMENT} \
  --build-arg GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} \
  --build-arg GCP_PROJECT_ID=${GCP_PROJECT_ID} \
  -t financial-elt:latest . && \
sudo docker run \
  --env-file .env \
  --name financial-elt \
  -p 5000:5000 \
  -d \
  --log-opt max-size=100m \
  --log-opt max-file=30 \
  financial-elt:latest

### docker-compose ###

# sudo docker-compose up -d --build
