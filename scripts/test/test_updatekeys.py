from script import Script
import subprocess
from pytest import raises
from pytest import skip


class TestUpdatekeys(Script):
    script_name = 'teuthology-updatekeys'

    def test_invalid(self):
        skip("teuthology.lock needs to be partially refactored to allow" +
             "teuthology-updatekeys to return nonzero in all erorr cases")

    def test_all_and_targets(self):
        args = (self.script_name, '-a', '-t', 'foo')
        with raises(subprocess.CalledProcessError):
            subprocess.check_call(args)

    def test_no_args(self):
        with raises(subprocess.CalledProcessError):
            subprocess.check_call(self.script_name)
