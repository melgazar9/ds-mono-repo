### Run
- Docker compose:
  - `sudo docker-compose up`
- Docker standalone:
  - `sudo docker build -t financial-elt .`
  - (or `sudo docker run --rm -it --env-file=.env financial-elt`)
- Run natively
  - `python host.py`

### Overview
- This project hosts ELT on schedule for financial data. The jobs are orchestrated to trigger endpoints that run the data pipelines on schedule.
- The directory `yfinance/tap-yfinance` contains the open sourced `meltano` tap-yfinance project `https://github.com/melgazar9/tap-yfinance`.
  - Targets can be set by using the meltano CLI (e.g. `meltano el tap-yfinance target-snowflake` or `target-bigquery`)
- The script `host.py` hosts all Financial ELT apps specified in `config.ini`.
  - `config.ini` has configurations for each project such as scheduling parameters
  - `routes.py` contains all routes for each sub-app.
  - `.env` contains credentials and secrets. See `.env_example` to see what needs to be defined.
- The `dbt_financial` directory handles all transformations after the data has been populated in the dwh.

**Installation**
  - Simply install docker (or docker-compose) and build the image.
  - The `dev` and `production` branches are CI/CD.