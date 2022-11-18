### Overview
- This repository acts as a mono-repo that contain projects related to data science.
- **By default,** projects reference the virtual env and requirements.txt in the root folder of this repo.
  - If there exists a requirements.txt folder in a specific projects, then create a new virtual environment within that project directory and follow the README instructions within that project.
  - Docker is not set up to run the projects (yet).
  - Current setup relies on python version 3.9.15

### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- `python -m venv venv && source activate venv/bin/activate`
- Ensure `which python` and `which pip` (or `whereis python` and `whereis pip` for mac users) point to the virtual environment location!
- If so, then go ahead and run `pip install -r requirements.txt`
- If not, for macOS silicon users, you might need to follow the below instructions
  - Change the aliases in `~/.zshrc`
    - `alias python=python3.9`
    - `alias pip="python3.9 -m pip"`
    - Then run `source ~/.zshrc`
    
    - If you still encounter errors try the below instructions
      - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
        - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
  - If everything looks good, go ahead and run `pip install -r requirements.txt`


### Handing credentials
  - Access credentials through the global system environment.
  - When running code in pycharm
    - Set the working directory to the project directory.
    - Set the environment variables needed to run the project.
    - Similarly when debugging, set the directory for debugging to the project directory.