import pytest
import requests

from datetime import datetime
from mock import patch

from teuthology.config import config, YamlConfig
from teuthology.exceptions import ScheduleFailError
from teuthology.suite import run


class TestRun(object):
    klass = run.Run

    def setup(self):
        self.args_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            ceph_branch='ceph_branch',
            ceph_sha1='ceph_sha1',
            teuthology_branch='teuthology_branch',
            kernel_branch=None,
            kernel_flavor='kernel_flavor',
            distro='ubuntu',
            machine_type='machine_type',
            base_yaml_paths=list(),
        )
        self.args = YamlConfig.from_dict(self.args_dict)

    @patch('teuthology.suite.run.util.fetch_repos')
    def test_name(self, m_fetch_repos):
        stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        with patch.object(run.Run, 'create_initial_config',
                          return_value=run.JobConfig()):
            name = run.Run(self.args).name
        assert str(stamp) in name

    @patch('teuthology.suite.run.util.fetch_repos')
    def test_name_user(self, m_fetch_repos):
        self.args.user = 'USER'
        with patch.object(run.Run, 'create_initial_config',
                          return_value=run.JobConfig()):
            name = run.Run(self.args).name
        assert name.startswith('USER-')

    @patch('teuthology.suite.run.util.git_branch_exists')
    @patch('teuthology.suite.run.util.package_version_for_hash')
    @patch('teuthology.suite.run.util.git_ls_remote')
    def test_branch_nonexistent(
        self,
        m_git_ls_remote,
        m_package_version_for_hash,
        m_git_branch_exists,
    ):
        config.gitbuilder_host = 'example.com'
        m_git_ls_remote.side_effect = [
            # First call will be for the ceph hash
            None,
            # Second call will be for the suite hash
            'suite_hash',
        ]
        m_package_version_for_hash.return_value = 'a_version'
        m_git_branch_exists.return_value = True
        self.args.ceph_branch = 'ceph_sha1'
        self.args.ceph_sha1 = None
        with pytest.raises(ScheduleFailError):
            self.klass(self.args)

    @patch('teuthology.suite.run.util.fetch_repos')
    @patch('requests.head')
    @patch('teuthology.suite.run.util.git_branch_exists')
    @patch('teuthology.suite.run.util.package_version_for_hash')
    @patch('teuthology.suite.run.util.git_ls_remote')
    def test_sha1_exists(
        self,
        m_git_ls_remote,
        m_package_version_for_hash,
        m_git_branch_exists,
        m_requests_head,
        m_fetch_repos,
    ):
        config.gitbuilder_host = 'example.com'
        m_package_version_for_hash.return_value = 'ceph_hash'
        m_git_branch_exists.return_value = True
        resp = requests.Response()
        resp.reason = 'OK'
        resp.status_code = 200
        m_requests_head.return_value = resp
        # only one call to git_ls_remote in this case
        m_git_ls_remote.return_value = "suite_branch"
        run = self.klass(self.args)
        assert run.base_config.sha1 == 'ceph_sha1'
        assert run.base_config.branch == 'ceph_branch'

    @patch('requests.head')
    @patch('teuthology.suite.util.git_branch_exists')
    @patch('teuthology.suite.util.package_version_for_hash')
    def test_sha1_nonexistent(
        self,
        m_package_version_for_hash,
        m_git_branch_exists,
        m_requests_head,
    ):
        config.gitbuilder_host = 'example.com'
        m_package_version_for_hash.return_value = 'ceph_hash'
        m_git_branch_exists.return_value = True
        resp = requests.Response()
        resp.reason = 'Not Found'
        resp.status_code = 404
        m_requests_head.return_value = resp
        self.args.ceph_sha1 = 'ceph_hash_dne'
        with pytest.raises(ScheduleFailError):
            self.klass(self.args)
