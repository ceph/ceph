import os
from ceph_volume import terminal

char = os.environ.get('INVALID')
terminal.stdout(char)
