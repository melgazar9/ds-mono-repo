FROM amd64/python:3.10.12-slim-buster

### Copy files ###

ARG CI=true
ENV PATH="/root/.local/bin:$PATH"

RUN env
ARG CI=true
ARG ENVIRONMENT
ENV APP_HOME=/app

ENV PYTHONUNBUFFERED=1

WORKDIR $APP_HOME
COPY . .


### Installation ###

RUN apt-get update &&\
    apt-get install -y git && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    pip install -U pip && \
    pip install -U pipx

RUN ulimit -c unlimited
WORKDIR tap-yfinance
RUN pipx ensurepath && \
    pipx install poetry==1.8.2 && \
    poetry install

RUN pipx install "meltano==3.6.0"

ENV PYTHONPATH=.
RUN meltano install
WORKDIR $APP_HOME
EXPOSE 5000
CMD ["poetry", "run", "python", "host.py"]
