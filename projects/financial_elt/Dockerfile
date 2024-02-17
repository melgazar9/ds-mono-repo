FROM amd64/python:3.10.12-slim-buster

### Copy files and credentials ###

ARG CI=true
ARG ENVIRONMENT
ARG GOOGLE_APPLICATION_CREDENTIALS
RUN env
ENV PATH="/root/.local/bin:$PATH"
ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME
COPY . $APP_HOME

COPY $GOOGLE_APPLICATION_CREDENTIALS /root/.config/gcloud/application_default_credentials.json
COPY $GOOGLE_APPLICATION_CREDENTIALS tap-yfinance/.secrets/


### Installation ###

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    pip install -U pip && \
    pip install -r requirements.txt

WORKDIR tap-yfinance

RUN sed -i "s/\${GCP_PROJECT_ID}/$GCP_PROJECT_ID/g" logging.yaml

RUN pipx ensurepath && \
    pipx install "meltano[gcs]==3.3.1" && \
    pipx inject meltano google-cloud-logging

ENV PYTHONPATH .

RUN meltano install

WORKDIR $APP_HOME

EXPOSE 5000

CMD ["python", "host.py"]
