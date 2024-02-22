### Overview
- This repository acts as a mono-repo that contain projects related to data science.
- Each project may have its own docker image to run and can reference source code from the root directory inside ds_core/.
- The primary benefit of this repo structure is to avoid maintaining multiple versions of similar code across projects.

### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- If running inside a docker container:
  - `cd projects/myproject`
  - set up `.env` and `config` files in each project
  - `sudo docker-compose up --build` after setting credentials / configurations.
- If running natively:
  - It's recommended to manage different python versions using `pyenv`
  - `poetry install`
  - `cd projects/<my-project> && export PYTHONPATH=</path/to/ds-mono-repo>:$PYTHONPATH`
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
