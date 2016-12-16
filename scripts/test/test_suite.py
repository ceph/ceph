import docopt
from script import Script
from scripts import suite

doc = suite.__doc__


class TestSuite(Script):
    script_name = 'teuthology-suite'

    def test_repo(self):
        assert suite.expand_short_repo_name('foo', 'https://github.com/ceph/ceph-ci.git') == 'https://github.com/ceph/foo'
        assert suite.expand_short_repo_name('foo/bar', 'https://github.com/ceph/ceph-ci.git') == 'https://github.com/foo/bar'
    
    def test_args(self):
        args = docopt.docopt(doc, [
            "--ceph-repo", "foo",
            "--suite-repo", "foo/bar"
        ])
        conf = suite.process_args(args)
        assert args["ceph_repo"] == "https://github.com/ceph/foo"
        assert args["suite_repo"] == "https://github.com/foo/bar"
