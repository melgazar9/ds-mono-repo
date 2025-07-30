### Run
- Docker compose:
  - `sudo docker-compose up`
- Docker standalone:
  - `./run.sh`
- Run natively
  - `poetry install && poetry run python host.py`

### Note
- If running this tap for all tickers and streams (e.g. stocks, crypto, futures, forex, and financials across different intervals), Yahoo will likely block your IP because they assume their server is being spammed given there are hundreds of thousands of requests. A workaround is to run `setup.sh` prior to building the docker image. Although this is setup for NordVPN, you can modify it to work for other vpn services. In `setup.sh` I have also turned on meshnet, which will allow a more secure ssh without port forward (22) while staying connected to a VPN and avoid being flagged as a spam user by Yahoo.

### Overview
- This project hosts ELT on schedule for financial data. The jobs are orchestrated to trigger endpoints that run the data pipelines on schedule.
- The directories contain open sourced `meltano` taps (e.g. `tap-fmp` is here: https://github.com/melgazar9/tap-fmp).
  - Targets can be set by using the meltano CLI (e.g. `meltano el tap-yfinance target-snowflake` or `target-bigquery`)
- The script `host.py` hosts all Financial ELT apps specified in `.env`.
  - `routes.py` contains all routes for each sub-app.
  - `.env` contains credentials and secrets. See `.env_example` in the root directory to see what needs to be defined.
- The `dbt_financial` directory handles all transformations after the data has been populated in the dwh.

**Installation**
  - Simply install docker (or docker-compose) and build the image.