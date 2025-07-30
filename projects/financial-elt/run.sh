set -e

### docker ###

sudo docker build \
  --build-arg ENVIRONMENT=${ENVIRONMENT} \
  -t financial-elt:latest . && \
sudo docker run \
  --env-file .env \
  --name financial-elt \
  --ulimit core=-1 \
  --oom-score-adj=-1000 \
  -p 5000:5000 \
  --restart always \
  -d \
  --memory="30g" \
  --cpus="16" \
  --network host \
  financial-elt:latest
