import logging
import os
import os.path
from pytest import raises
import shutil
import subprocess

from .. import repo_utils
from .. import parallel
repo_utils.log.setLevel(logging.WARNING)


class TestRepoUtils(object):
    src_path = '/tmp/empty_src'
    online_repo_url = 'https://github.com/ceph/empty.git'
    offline_repo_url = 'file://' + src_path
    repo_url = None
    dest_path = '/tmp/empty_dest'

    @classmethod
    def setup_class(cls):
        if 'TEST_ONLINE' in os.environ:
            cls.repo_url = cls.online_repo_url
        else:
            cls.repo_url = cls.offline_repo_url

    def setup_method(self, method):
        assert not os.path.exists(self.dest_path)
        proc = subprocess.Popen(
            ('git', 'init', self.src_path),
            stdout=subprocess.PIPE,
        )
        assert proc.wait() == 0
        proc = subprocess.Popen(
            ('git', 'config', 'user.email', 'test@ceph.com'),
            cwd=self.src_path,
            stdout=subprocess.PIPE,
        )
        assert proc.wait() == 0
        proc = subprocess.Popen(
            ('git', 'config', 'user.name', 'Test User'),
            cwd=self.src_path,
            stdout=subprocess.PIPE,
        )
        assert proc.wait() == 0
        proc = subprocess.Popen(
            ('git', 'commit', '--allow-empty', '--allow-empty-message',
             '--no-edit'),
            cwd=self.src_path,
            stdout=subprocess.PIPE,
        )
        assert proc.wait() == 0

    def teardown_method(self, method):
        shutil.rmtree(self.dest_path, ignore_errors=True)

    def test_clone_repo_existing_branch(self):
        repo_utils.clone_repo(self.repo_url, self.dest_path, 'master')
        assert os.path.exists(self.dest_path)

    def test_clone_repo_non_existing_branch(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.clone_repo(self.repo_url, self.dest_path, 'nobranch')
        assert not os.path.exists(self.dest_path)

    def test_fetch_branch_no_repo(self):
        fake_dest_path = '/tmp/not_a_repo'
        assert not os.path.exists(fake_dest_path)
        with raises(OSError):
            repo_utils.fetch_branch(fake_dest_path, 'master')
        assert not os.path.exists(fake_dest_path)

    def test_fetch_branch_fake_branch(self):
        repo_utils.clone_repo(self.repo_url, self.dest_path, 'master')
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.fetch_branch(self.dest_path, 'nobranch')

    def test_enforce_existing_branch(self):
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)

    def test_enforce_non_existing_branch(self):
        with raises(repo_utils.BranchNotFoundError):
            repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                          'blah')
        assert not os.path.exists(self.dest_path)

    def test_enforce_multiple_calls_same_branch(self):
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)
        repo_utils.enforce_repo_state(self.repo_url, self.dest_path,
                                      'master')
        assert os.path.exists(self.dest_path)

    def test_enforce_multiple_calls_different_branches(self):
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

    def test_enforce_invalid_branch(self):
        with raises(ValueError):
            repo_utils.enforce_repo_state(self.repo_url, self.dest_path, 'a b')

    def test_simultaneous_access(self):
        count = 5
        with parallel.parallel() as p:
            for i in range(count):
                p.spawn(repo_utils.enforce_repo_state, self.repo_url,
                        self.dest_path, 'master')
            for result in p:
                assert result is None
