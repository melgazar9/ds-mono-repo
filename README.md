### Overview
- This repository acts as a "mono-repo" that contains all projects related to data science.


### Setup
- `git clone https://github.com/melgazar9/ds-mono-repo.git && cd ds-mono-repo`
- `python -m venv venv && source activate venv/bin/activate`
- `python setup.py install` to access all modules regardless of the working directory.
- Set up a virtual environment and install requirements.txt with respect to your system
  - For Ubuntu users, run `pip install -r requirements_ubuntu.txt`
  - For macOS silicon users, run `pip install -r requirements_m1.txt`
    - To save `requirements.txt`, run `pip list --format=freeze > requirements_m1.txt`
  - For Windows users, run `pip install -r requirements_windows.txt`
- For macOS silicon users, it's likely you'll need to follow the below instructions
  - `vi venv/lib/python3.8/site-packages/IPython/core/interactiveshell.py`
    - Change `p.readlink()` to `p._from_parts(os.readlink(p,))`
  - Run `python setup.py install` to access all modules regardless of the working directory.
  - If you encounter `ImportError` even after installing and you're on macOS silicon, try the below fix
    - `python -m pip install <package_name> && python -c "import <package_name>; print('ok')"`
- **Handing credentials**
  - Access credentials via system environment or in the `~/.credentials` location.
  - When running code in pycharm, set the working directory to the project directory. Similarly when debugging, set the directory for debugging to the project directory.

Influences

- https://github.com/mlflow/mlflow/tree/master/mlflow
- https://github.com/pyparakeets/budgies/blob/main/threshold_optimizer/threshold_optimizer.py
