### Run
- Docker compose:
  - `sudo docker-compose up`
- Docker standalone:
  - `sudo docker build -t financial-elt .`
  - (or `sudo docker run --rm -it --env-file=.env financial-elt`)
- Run natively
  - `python host.py`

### Note
- If running this tap for all tickers and streams (e.g. stocks, crypto, futures, forex, and financials across different intervals), Yahoo will likely block your IP because they assume their server is being spammed given there are hundreds of thousands of requests. A workaround is to run `setup.sh` prior to building the docker image. Although this is setup for NordVPN, you can modify it to work for other vpn services. In `setup.sh` I have also turned on meshnet, which will allow a more secure ssh without port forward (22) while staying connected to a VPN and avoid being flagged as a spam user by Yahoo.

### Overview
- This project hosts ELT on schedule for financial data. The jobs are orchestrated to trigger endpoints that run the data pipelines on schedule.
- The directory `tap-yfinance` contains the open sourced `meltano` tap-yfinance project https://github.com/melgazar9/tap-yfinance.
  - Targets can be set by using the meltano CLI (e.g. `meltano el tap-yfinance target-snowflake` or `target-bigquery`)
- The script `host.py` hosts all Financial ELT apps specified in `config.ini`.
  - `config.ini` has configurations for each project such as scheduling parameters
  - `routes.py` contains all routes for each sub-app.
  - `.env` contains credentials and secrets. See `.env_example` to see what needs to be defined.
- The `dbt_financial` directory handles all transformations after the data has been populated in the dwh.

**Installation**
  - Simply install docker (or docker-compose) and build the image.
  - The `dev` and `production` branches are CI/CD.
