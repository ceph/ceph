import logging
import os.path
from pytest import raises
import shutil
import subprocess

from .. import repo_utils
repo_utils.log.setLevel(logging.WARNING)


class TestRepoUtils(object):
    src_path = '/tmp/empty_src'
    repo_url = 'file://' + src_path
    dest_path = '/tmp/empty_dest'

    def setup(self):
        assert not os.path.exists(self.dest_path)
        proc = subprocess.Popen(
            ('git', 'init', self.src_path),
        )
        assert proc.wait() == 0
        proc = subprocess.Popen(
            ('git', 'commit', '--allow-empty', '--allow-empty-message',
             '--no-edit'),
            cwd=self.src_path,
        )
        assert proc.wait() == 0

    def teardown(self):
        shutil.rmtree(self.dest_path, ignore_errors=True)

    def test_existing_branch(self):
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)

    def test_non_existing_branch(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                          'blah')
        assert not os.path.exists(self.dest_path)

    def test_multiple_calls_same_branch(self):
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)

    def test_multiple_calls_different_branches(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                          'blah1')
        assert not os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                          'blah2')
        assert not os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
