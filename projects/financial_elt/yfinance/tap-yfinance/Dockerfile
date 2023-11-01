FROM amd64/python:3.10.12-slim-buster

ARG CI=true
RUN env
ENV PATH="/root/.local/bin:$PATH"
ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME
COPY . $APP_HOME
RUN apt-get update &&\
    apt-get install -y cron &&\
    apt-get install -y git &&\
    rm -rf /var/lib/{apt,dpkg,cache,log}/

# Installing meltano[gcs] for google cloud storage. Change this to change the state backend depending on requirements
# (e.g. meltano[azure] for azure cloud storage and meltano[s3] for s3 cloud storage)
RUN pip install -U pip && \
    pip install poetry==1.6.1 && \
    pip install pipx==1.2.0 && \
    poetry install && \
    pipx install meltano[gcs]==3.1.0 && \
    pipx ensurepath && \
    meltano install

EXPOSE 5000

CMD ["poetry", "run", "python", "host.py"]