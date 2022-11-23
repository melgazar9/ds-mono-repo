0 */6 * * * cd ~/ds-mono-repo/projects/financial_elt/yfinance/; git pull; source ~/.bashrc; source venv/bin/activate; pip install -r requirements.txt; python populate_database.py > logs/populate_database_$(date +%s).log 2>&1
~
~
~
~
~
~
~
~
~
~
~
~
                                                                                             25,5          All