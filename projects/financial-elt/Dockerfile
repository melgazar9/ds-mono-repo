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
    pipx install poetry==2.1.3 && \
    poetry install

RUN ulimit -c unlimited

WORKDIR $APP_HOME/tap-yfinance
RUN pipx install "meltano==3.7.8" && meltano install

WORKDIR $APP_HOME/tap-yahooquery
RUN meltano install

WORKDIR $APP_HOME/tap-polygon
RUN meltano install
WORKDIR $APP_HOME

# Create directories to hold the symlinked state files
RUN mkdir -p /app/tap-yfinance/state && \
    mkdir -p /app/tap-yahooquery/state && \
    mkdir -p /app/tap-polygon/state

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]

EXPOSE 5000

CMD ["bash", "-c", "poetry run python host.py"]
