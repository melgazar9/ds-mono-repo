- MySQL needs to be installed
  - On Linux you'll likely need to run the below command before continuing with `pip install -r requirements.txt`
    - Ubuntu / Debian: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`
    - Redhat / CentOS: `sudo yum install python3-devel mysql-devel`
    
    OR (on debian)
      - sudo apt-get install mysql-client 
      - sudo apt-get install libmysqlclient-dev 
      - sudo apt-get install libssl-dev
  - If `pip install -r requirements.txt` errors out because if `mysql-client`
    - `echo "LD_PRELOAD=/lib/x86_64-linux-gnu/libstdc++.so.6 python" >> ~/.bashrc`
    - `source ~/.bashrc`
- Once MySQL is installed run `pip install -r requirements.txt`
