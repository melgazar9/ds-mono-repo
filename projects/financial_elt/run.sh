### docker ###

sudo docker build \
  --build-arg ENVIRONMENT=dev \
  --build-arg GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} \
  --build-arg GCP_PROJECT_ID=${GCP_PROJECT_ID} \
  -t financial-elt:latest . && \
sudo docker run \
  --env-file .env \
  --name financial-elt \
  -p 5000:5000 \
  --restart always -d \
  financial-elt:latest

### docker-compose ###

# sudo docker-compose up -d --build
