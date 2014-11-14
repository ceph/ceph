from docopt import docopt

from script import Script
from scripts import coverage


doc = coverage.__doc__


class TestCoverage(Script):
    script_name = 'teuthology-coverage'

    def test_args(self):
        args = docopt(doc, [
            "--skip-init",
            "--lcov-output=some/other/dir",
            "--html-output=html/output/dir",
            "--cov-tools-dir=cov/tools/dir",
            "--verbose",
            "some/test/dir"]
        )
        assert args["--skip-init"]
        assert args["--lcov-output"] == "some/other/dir"
        assert args["<test_dir>"] == "some/test/dir"
        assert args["--html-output"] == "html/output/dir"
        assert args["--cov-tools-dir"] == "cov/tools/dir"
        assert args["--verbose"]
