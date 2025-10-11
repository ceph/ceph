import random
import requests
import string
import time

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass as e:
        return e
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        s = s + strpart[:(this_part_size % chunk)]
        yield s
        if (x == size):
            return

# syncs all the regions except for the one passed in
def region_sync_meta(targets, region):

    for (k, r) in targets.items():
        if r == region:
            continue
        conf = r.conf
        if conf.sync_agent_addr:
            ret = requests.post('http://{addr}:{port}/metadata/incremental'.format(addr = conf.sync_agent_addr, port = conf.sync_agent_port))
            assert ret.status_code == 200
        if conf.sync_meta_wait:
            time.sleep(conf.sync_meta_wait)


def get_grantee(policy, permission):
    '''
    Given an object/bucket policy, extract the grantee with the required permission
    '''

    for g in policy.acl.grants:
        if g.permission == permission:
            return g.id
