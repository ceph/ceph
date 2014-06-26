import logging
import os.path
from pytest import raises
import shutil

from .. import repo_utils
repo_utils.log.setLevel(logging.WARNING)


class TestRepoUtils(object):
    empty_repo = 'https://github.com/ceph/empty'
    local_dir = '/tmp/empty'

    def setup(self):
        assert not os.path.exists(self.local_dir)

    def teardown(self):
        shutil.rmtree(self.local_dir, ignore_errors=True)

    def test_existing_branch(self):
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)

    def test_non_existing_branch(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'blah')
        assert not os.path.exists(self.local_dir)

    def test_multiple_calls_same_branch(self):
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)

    def test_multiple_calls_different_branches(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'blah')
        assert not os.path.exists(self.local_dir)
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'blah')
        assert not os.path.exists(self.local_dir)
        repo_utils.checkout_repo(self.empty_repo, self.local_dir, 'master')
        assert os.path.exists(self.local_dir)
