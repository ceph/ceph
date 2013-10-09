import subprocess
from pytest import raises


class Script(object):
    script_name = 'teuthology'

    def test_help(self):
        args = (self.script_name, '--help')
        out = subprocess.check_output(args)
        assert out.startswith('usage')

    def test_invalid(self):
        args = (self.script_name, 'INVALID')
        with raises(subprocess.CalledProcessError):
            subprocess.check_call(args)
