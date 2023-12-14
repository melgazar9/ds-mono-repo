### Overview
- This repository acts as a mono-repo that contain projects related to data science.
- Each project may have its own docker image to run.

### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- If running inside a docker container:
  - `cd projects/myproject`
  - set up `.env` and `config.ini` files
  - `sudo docker-compose up --build` after setting credentials.
- If running natively:
  - It's recommended to version python virtual environments with `pyenv` or similar
  - `python -m venv venv && source activate venv/bin/activate`
  - Ensure `which python` and `which pip` (or `whereis python` and `whereis pip` for mac users) point to the virtual environment location.
  - If so, run `pip install -r requirements.txt`
  - If not, for macOS silicon users, try the below instructions
    - Change the aliases in `~/.zshrc`
      - `alias python=python3.9`
      - `alias pip="python3.9 -m pip"`
      - `source ~/.zshrc`
    
      - If still encountering errors try the below instructions
        - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
          - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
    - If everything looks good run `pip install -r requirements.txt`


### Handing credentials
  - Access credentials through the global system environment `.env`.
  - When running code in pycharm
    - Set the working directory to the project directory.
    - Set the environment variables needed to run the project.
    - Similarly, when debugging, set the debug working directory to the project directory.
