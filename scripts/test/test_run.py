import docopt

from script import Script
from scripts import run

doc = run.__doc__


class TestRun(Script):
    script_name = 'teuthology'

    def test_all_args(self):
        args = docopt.docopt(doc, [
            "--verbose",
            "--archive", "some/archive/dir",
            "--description", "the_description",
            "--owner", "the_owner",
            "--lock",
            "--machine-type", "machine_type",
            "--os-type", "os_type",
            "--os-version", "os_version",
            "--block",
            "--name", "the_name",
            "--suite-path", "some/suite/dir",
            "path/to/config.yml",
        ])
        assert args["--verbose"]
        assert args["--archive"] == "some/archive/dir"
        assert args["--description"] == "the_description"
        assert args["--owner"] == "the_owner"
        assert args["--lock"]
        assert args["--machine-type"] == "machine_type"
        assert args["--os-type"] == "os_type"
        assert args["--os-version"] == "os_version"
        assert args["--block"]
        assert args["--name"] == "the_name"
        assert args["--suite-path"] == "some/suite/dir"
        assert args["<config>"] == ["path/to/config.yml"]

    def test_multiple_configs(self):
        args = docopt.docopt(doc, [
            "config1.yml",
            "config2.yml",
        ])
        assert args["<config>"] == ["config1.yml", "config2.yml"]
        # make sure defaults are working
        assert args["--os-type"] == "ubuntu"
