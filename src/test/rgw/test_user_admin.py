import subprocess
import os
import string
import sys
import logging
import rgw_admin_user

def bash(cmd, **kwargs):
    log.debug('running cmd: %s', ' '.join(cmd))
    check_retcode = kwargs.pop('check_retcode', True)
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0]
    log.debug('command returned status=%d stdout=%s', process.returncode, s.decode('utf-8'))
    if check_retcode:
        assert(process.returncode == 0)
    return (s, process.returncode)


class TestUserAdmin:
    """test driver"""

    def __init__(self):
        self.admin_user = rgw_admin_user.LibRGWAdminUser()

    def test_version(self):
        ver = self.admin_user.version()
        assert(ver == (1,0,0))

    def test_create_user(self):
        uid = "tlhm"
        display_name = "thierry lhermite"
        access_key = "strawberry"
        secret_key = "rhubarb"
        email = "thierry@whouse.gov"
        caps = ""
        access = ""
        admin = True
        system = False
        ret = self.admin_user.create_user(uid, display_name, access_key, secret_key, email, caps, access, admin, system)

    def test_user_info(self):
        uid = 'tlhm'
        user_info = self.admin_user.user_info(uid)
        acc = user_info.access_key
        key = user_info.secret_key
        assert(acc == "strawberry")
        assert(key == "rhubarb")

def setup_module():
    pass

if __name__ == "__main__":
    tests = TestUserAdmin()
    tests.test_version()
    tests.test_create_user()
    tests.test_user_info()

