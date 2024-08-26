FROM amd64/python:3.10.12-slim-buster

### Copy files ###

ARG CI=true
ENV PATH="/root/.local/bin:$PATH"

RUN env
ARG CI=true
ARG ENVIRONMENT
ARG GOOGLE_APPLICATION_CREDENTIALS
ARG GCP_PROJECT_ID
ENV APP_HOME=/app

ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME

COPY . .

COPY $GOOGLE_APPLICATION_CREDENTIALS /root/.config/gcloud/application_default_credentials.json
COPY $GOOGLE_APPLICATION_CREDENTIALS tap-yfinance/.secrets/

### Installation ###

RUN apt-get update &&\
    apt-get install -y git && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    pip install -U pip && \
    pip install -U pipx

RUN ulimit -c unlimited

WORKDIR tap-yfinance

# RUN sed -i "s/\${GCP_PROJECT_ID}/$GCP_PROJECT_ID/g" logging.yaml

RUN pipx ensurepath && \
    pipx install poetry==1.8.2 && \
    poetry install

RUN pipx install "meltano[gcs]==3.3.2"
RUN pipx inject meltano google-cloud-logging
ENV PYTHONPATH .

RUN meltano install

WORKDIR $APP_HOME

EXPOSE 5000

CMD ["poetry", "run", "python", "host.py"]
