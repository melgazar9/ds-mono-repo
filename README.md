### Overview
- This repository acts as a "mono-repo" that contains all projects related to data science.


### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git
- Set up a virtual environment and install requirements.txt with respect to your system
  - For Ubuntu users, run `pip install -r requirements_ubuntu.txt`
  - For macOS silicon users, run `pip install -r requirements_m1.txt`
  - For Windows users, run `pip install -r requirements_windows.txt`
- For macOS silicon users, it's likely you'll need to follow the below instructions
  - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
    - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
  - If you encounter `ImportError` even after installing and you're on macOS silicon, try the below fix
    - `pip3 install <package_name> && python3 -c "import <package_name>; print('ok')"`   
- **Running DB connections**
  - By default, the credentials directory can be placed outside of the main directory
    - Example
      - `mkdir credentials && cd credentials/;`
      - `mkdir credentials`
      - `git clone https://github.com/melgazar9/ds-mono-repo.git`
      - `cd credentials` - place your credentials files here. By default all the code should read the credential files.
        - If there is a problem with auto-reading the credentials files you can always specificy the location directly.
    - If using pycharm you'll need to specify the working directories for each project directly
      - Example
        - When running code in pycharm, set the working directory to the project directory. Similarly when debugging, set the directory for debugging to the project directory.
