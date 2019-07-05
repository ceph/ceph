import os
from subprocess import check_output

import ceph.mon_command_api._rebuild
from ceph.mon_command_api._rebuild import main

def test_api_unchanged():
    name = ceph.mon_command_api._rebuild.__file__.replace('_rebuild.py', 'generated_api.py')
    with open(name, 'w') as f:
        main(os.environ['CEPH_CONF'], "2", f)
    out = check_output('git diff ' + name, shell=True)
    assert out == ''
