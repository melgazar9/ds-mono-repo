### Run
- If using docker-compose:
  - `sudo docker-compose up` (or `sudo docker-compose up --build`)
- Docker standalone:
  - `sudo docker run --rm -it --env-file=.env financial-elt`
- Run natively
  - `python host_financial_elt.py`

### Overview
- This directory contains ETL / ELT processes that are executed by a scheduler.
- The script `host_financial_elt.py` hosts all apps specified in the `.env` file.
  - `config.ini` has configurations for each project such as scheduling parameters
  - `routes.py` contains all routes for each sub-app.
  - `.env` contains credentials and secret information. See `.env_example` for an example.
- The `dbt` directory handles all transformations after the data has been populated in the dwh using the dbt tool
- The `financials` directory handles yfinance financials (e.g earnings, quarterly reports, expenses, etc...).
- The `stock_prices` directory handles yfinance stock price ETL
  - Load stock prices into a pandas dataframe
  - Apply minor transformations such as:
    - Add yahoo_ticker column
    - Handle UTC timestamp and tz-aware timestamp columns
  - Upload to db/dwh of choice (current target is Bigquery / Snowflake dwh)

**Installation**

- BigQuery and Snowflake are fully supported, however MySQL may not work for all `dbt` transformations.
- If `BigQuery` in target, then set `GOOGLE_APPLICATION_CREDENTIALS` to the location of the `t-emissary-XXX.json` in the `.env`.
- If `Snowflake` in target, then set Snowflake credentials in the `.env`.
- If using target as MySQL, as mentioned not all dbt transformations are currently supported. However, for `etl_stock_prices`, see below for installation requirements:
  - On Linux you'll likely need to run the below command before continuing with `pip install -r requirements.txt`
    - Ubuntu / Debian: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`
      - OR (on debian) `sudo apt-get install mysql-client libmysqlclient-dev libssl-dev default-mysql-client`
      - OR (on debian) if there still issues entering mysql:
        - `sudo apt-get purge mysql-server mysql-client mysql-common` 
        - `sudo apt-get install mysql-server`
        - `sudo apt-get install python-dev default-libmysqlclient-dev build-essential`
    - if `mysql not found` and on gcp check out this link: `https://cloud.google.com/architecture/setup-mysql`
      - if `_mysql` not found and on gcp, try `sudo apt install libmariadb3 libmariadb-dev`
      - when setting environment variables set `MYSQL_HOST=<public_ip_address>`
    - Redhat / CentOS: `sudo yum install python3-devel mysql-devel`
    - Current setup relies on python version 3.9.15
  
  - If `pip install -r requirements.txt` errors out because if `mysql-client`
    - `echo "LD_PRELOAD=/lib/x86_64-linux-gnu/libstdc++.so.6 python" >> ~/.bashrc`
    - `source ~/.bashrc`

- Once MySQL is installed run `pip install -r requirements.txt`