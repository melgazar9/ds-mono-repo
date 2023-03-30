### Overview
- This repository acts as a mono-repo that contain projects related to data science.
- Each project will have its own docker image to run.
- Projects that are related can be ran with a `host_projects.py` script. Example:
  - `cd projects/project1`
  - Within project1, there exists subproject_1, and subproject_2, set up a config.ini file that gets called by `host_project1.py`
  - `if host_subproject_1 in projects_to_host then serve project in app` (similarly for subproject_2, 3, etc...)

### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- If running inside a docker container
  - `cd projects/myproject`
  - set up `.env` and `config.ini` files
  - `sudo docker-compose up --build` and the project will run if the credentials are properly set.
- If running natively (not inside a docker container)
  - It's recommended to version python virtual environments with `pyenv` or something similar
  - `python -m venv venv && source activate venv/bin/activate`
  - Ensure `which python` and `which pip` (or `whereis python` and `whereis pip` for mac users) point to the virtual environment location!
  - If so, then run `pip install -r requirements.txt`
  - If not, for macOS silicon users, you might need to follow the below instructions
    - Change the aliases in `~/.zshrc`
      - `alias python=python3.9`
      - `alias pip="python3.9 -m pip"`
      - Then run `source ~/.zshrc`
    
      - If you still encounter errors try the below instructions
        - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
          - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
    - If everything looks good, run `pip install -r requirements.txt`


### Handing credentials
  - Access credentials through the global system environment `.env`.
  - When running code in pycharm
    - Set the working directory to the project directory.
    - Set the environment variables needed to run the project.
    - Similarly, when debugging set the directory for debugging to the project directory.
