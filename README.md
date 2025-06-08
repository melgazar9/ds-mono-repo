### Overview
- This repository acts as a mono-repo that contain projects related to data science.
- Each project may have its own docker image and can reference source code from the root directory inside ds_core/.
- The primary benefit of this repo structure is to avoid maintaining multiple versions of similar code across projects.

### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- Projects can run individually or be orchestrated from the root directory using docker compose. 
- If using docker compose from the root directory to run all projects, call `./run.sh`
  - Need to set up `.env` and `config` files in each project if running from the project dir, otherwise set up `.env` in the project root directory and config files in the project directory. 
  - Poetry is used for package management.
  - Dockerfile lives in the project directory but docker-compose.yml lives in the root directory
  - When setting up for the first time, need to go to Kibana and set up filebeat logs. `Create data view --> name: filebeat logs and index pattern: filebeat-*`
    - From here Kibana can be searched: set message as the column and search (for example) container.name: financial-elt

- If running a single project natively (e.g. for testing/development):
  - It's recommended to manage different python versions using `pyenv`
  - `cd projects/<my-project> && export PYTHONPATH=</path/to/ds-mono-repo>:$PYTHONPATH`
  - `poetry install`
  - Ensure `poetry run which python` and `poetry run which pip` (or `poetry run whereis python` for mac users) point to the virtual environment location.
  - If not, for macOS silicon users, try the below steps
    - Change the aliases in `~/.zshrc`
      - `alias python=python3.10`
      - `alias pip="python3.10 -m pip"`
      - `source ~/.zshrc`
      - If still encountering errors try the below steps:
        - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
          - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
    - If everything looks good run `poetry install`


### Handing credentials
  - Access credentials through the global system environment `.env`
  - When running code in pycharm
    - Set the working directory to the project directory.
    - Set the environment variables needed to run the project.
    - Similarly, when debugging, set the debug working directory to the project directory.
