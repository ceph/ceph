import sys
import os

module_path = os.environ['CYTHON_MODULES'] + '/lib.' + str(sys.version_info[0])
sys.path.append(module_path)
