FROM amd64/python:3.12-slim-bookworm

### Copy files ###

ARG CI=true
ENV PATH="/root/.local/bin:$PATH"
ARG CI=true
ARG ENVIRONMENT
ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME
COPY . .


### Installation ###

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    pip install -U pip && \
    pip install -U pipx && \
    pipx ensurepath && \
    pipx install poetry==2.1.3

RUN ulimit -c unlimited

WORKDIR tap-yfinance
RUN pipx install "meltano==3.7.3" && \
    poetry install

ENV PYTHONPATH=.
RUN meltano install
WORKDIR $APP_HOME

EXPOSE 5000

CMD ["poetry", "run", "python", "host.py"]
