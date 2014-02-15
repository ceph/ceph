import os
from pecan.deploy import deploy

cur_path = os.path.dirname(__file__)
application = deploy(cur_path + '/config.py')
