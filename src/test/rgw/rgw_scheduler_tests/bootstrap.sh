virtualenv -p /usr/bin/python3 scheduler-venv
source scheduler-venv/bin/activate
pip install -e '.[test]'
python setup.py install
#pip install -r requirements.txt
