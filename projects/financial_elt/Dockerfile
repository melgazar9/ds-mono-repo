FROM amd64/python:3.12-slim-bookworm

ARG CI=true
ARG ENVIRONMENT
ENV PATH="/root/.local/bin:$PATH"
ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME

COPY . .

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    pip install --no-cache-dir -U pip pipx && \
    pipx ensurepath && \
    pipx install poetry==2.1.3

RUN ulimit -c unlimited

WORKDIR /app/tap-yfinance

RUN pipx install "meltano==3.7.3" && \
    poetry install && \
    meltano install

WORKDIR /app/tap-polygon
RUN meltano install

WORKDIR $APP_HOME

EXPOSE 5000

CMD ["poetry", "run", "python", "host.py"]
