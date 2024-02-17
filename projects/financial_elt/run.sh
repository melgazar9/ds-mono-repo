### docker ###

sudo docker build \
  --build-arg ENVIRONMENT=dev \
  --build-arg GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} \
  -t financial-elt:latest . && \
sudo docker run \
  --name financial-elt \
  -p 5000:5000 \
  --env-file .env \
  --restart always -d \
  financial-elt:latest


### docker-compose ###

# sudo docker-compose up -d --build
