import docopt

from script import Script
from scripts import ls

doc = ls.__doc__


class TestLs(Script):
    script_name = 'teuthology-ls'

    def test_args(self):
        args = docopt.docopt(doc, ["--verbose", "some/archive/dir"])
        assert args["--verbose"]
        assert args["<archive_dir>"] == "some/archive/dir"
