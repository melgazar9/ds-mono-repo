### Overview
- This repository acts as a "mono-repo" that contains all projects related to data science.


### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git`
- Set up a virtual environment and install requirements.txt with respect to your system
  - For Ubuntu users, run `pip install -r requirements_ubuntu.txt`
  - For macOS silicon users, run `pip install -r requirements_m1.txt`
  - For Windows users, run `pip install -r requirements_windows.txt`
- For macOS silicon users, it's likely you'll need to follow the below instructions
  - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
    - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
  - If you encounter `ImportError` even after installing and you're on macOS silicon, try the below fix
    - `pip3 install <package_name> && python3 -c "import <package_name>; print('ok')"`   
- **Handing credentials**
  - It is best practice to leave credentials files outside of the repository directory. By default credentials will be accessed in the previous directory from the cloned repository (e.g. credentials directory = `<cloned_repository>/../credentials/`.
    - Example
      - `mkdir credentials && cd credentials/;`
      - `vi <my_db_credentials.yaml>` - create a yaml credentials file
      - `cd ../ && git clone https://github.com/melgazar9/ds-mono-repo.git` - By default the code base will access `../credentials/` as the credentials directory.
      - If there is a problem with auto-reading the credentials files you can always specificy the location directly.
    - If using pycharm you'll need to specify the working directories for each project directly
      - Example
        - When running code in pycharm, set the working directory to the project directory. Similarly when debugging, set the directory for debugging to the project directory.
