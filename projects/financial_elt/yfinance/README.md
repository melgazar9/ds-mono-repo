- MySQL needs to be installed
  - On Linux you'll likely need to run the below command before continuing with `pip install -r requirements.txt`
    - Ubuntu / Debian: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`
      - OR (on debian) `sudo apt-get install mysql-client libmysqlclient-dev libssl-dev default-mysql-client`
      - OR (on debian) if there still issues entering mysql:
        - `sudo apt-get purge mysql-server mysql-client mysql-common` 
        - `sudo apt-get install mysql-server`
    - if `mysql not found` and on gcp check out this link: `https://cloud.google.com/architecture/setup-mysql`
      - if `_mysql` not found and on gcp, try `sudo apt install libmariadb3 libmariadb-dev`
      - when setting environment variables set `MYSQL_HOST=<public_ip_address>`
    - Redhat / CentOS: `sudo yum install python3-devel mysql-devel`
    - Current setup relies on python version 3.9.15
  
  - If `pip install -r requirements.txt` errors out because if `mysql-client`
    - `echo "LD_PRELOAD=/lib/x86_64-linux-gnu/libstdc++.so.6 python" >> ~/.bashrc`
    - `source ~/.bashrc`

- Once MySQL is installed run `pip install -r requirements.txt`
