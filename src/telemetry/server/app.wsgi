import sys
import os
pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, pwd)

from ceph_telemetry import create_app

app = create_app(__name__)

application = app
