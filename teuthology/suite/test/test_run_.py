import os
import pytest
import requests
import yaml

from datetime import datetime
from mock import patch, call, ANY, DEFAULT
from StringIO import StringIO

from teuthology.config import config, YamlConfig
from teuthology.exceptions import ScheduleFailError
from teuthology.suite import run
from teuthology import packaging


class TestRun(object):
    klass = run.Run

    def setup(self):
        self.args_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            suite_relpath='',
            ceph_branch='ceph_branch',
            ceph_sha1='ceph_sha1',
            email='address@example.com',
            teuthology_branch='teuthology_branch',
            kernel_branch=None,
            kernel_flavor='kernel_flavor',
            distro='ubuntu',
            machine_type='machine_type',
            base_yaml_paths=list(),
        )
        self.args = YamlConfig.from_dict(self.args_dict)

    @patch('teuthology.suite.run.util.fetch_repos')
    @patch('teuthology.suite.run.util.git_ls_remote')
    @patch('teuthology.suite.run.Run.choose_ceph_version')
    @patch('teuthology.suite.run.util.git_validate_sha1')
    def test_email_addr(self, m_git_validate_sha1, m_choose_ceph_version,
                        m_git_ls_remote, m_fetch_repos):
        # neuter choose_X_branch
        m_git_validate_sha1.return_value = self.args_dict['ceph_sha1']
        m_choose_ceph_version.return_value = self.args_dict['ceph_sha1']
        self.args_dict['teuthology_branch'] = 'master'
        self.args_dict['suite_branch'] = 'master'
        m_git_ls_remote.return_value = 'suite_sha1'

        runobj = self.klass(self.args)
        assert runobj.base_config.email == self.args_dict['email']

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

    @patch('teuthology.suite.run.util.fetch_repos')
    @patch('teuthology.suite.run.util.git_branch_exists')
    @patch('teuthology.suite.run.util.package_version_for_hash')
    @patch('teuthology.suite.run.util.git_ls_remote')
    def test_regression(
        self,
        m_git_ls_remote,
        m_package_version_for_hash,
        m_git_branch_exists,
        m_fetch_repos,
    ):
        config.use_shaman = False
        config.gitbuilder_host = 'example.com'
        m_package_version_for_hash.return_value = 'ceph_hash'
        m_git_branch_exists.return_value = True
        m_git_ls_remote.return_value = "suite_branch"
        self.args_dict = {
            'base_yaml_paths': [],
            'ceph_branch': 'master',
            'machine_type': 'smithi',
            'kernel_flavor': 'basic',
            'kernel_branch': 'testing',
            'suite': 'krbd',
        }
        self.args = YamlConfig.from_dict(self.args_dict)
        with patch.multiple(
            'teuthology.packaging.GitbuilderProject',
            _get_package_sha1=DEFAULT,
        ) as m:
            assert m != dict()
            m['_get_package_sha1'].return_value = 'SHA1'
            conf = dict(
                os_type='ubuntu',
                os_version='16.04',
            )
            assert packaging.GitbuilderProject('ceph', conf).sha1 == 'SHA1'
            run_ = self.klass(self.args)
            assert run_.base_config['kernel']['sha1'] == 'SHA1'


class TestScheduleSuite(object):
    klass = run.Run

    def setup(self):
        self.args_dict = dict(
            suite='suite',
            suite_relpath='',
            suite_dir='suite_dir',
            suite_branch='master',
            ceph_branch='ceph_branch',
            ceph_sha1='ceph_sha1',
            teuthology_branch='master',
            kernel_branch=None,
            kernel_flavor='kernel_flavor',
            distro='ubuntu',
            distro_version='14.04',
            machine_type='machine_type',
            base_yaml_paths=list(),
        )
        self.args = YamlConfig.from_dict(self.args_dict)

    @patch('teuthology.suite.run.Run.schedule_jobs')
    @patch('teuthology.suite.run.Run.write_rerun_memo')
    @patch('teuthology.suite.util.has_packages_for_distro')
    @patch('teuthology.suite.util.get_package_versions')
    @patch('teuthology.suite.util.get_install_task_flavor')
    @patch('__builtin__.file')
    @patch('teuthology.suite.run.build_matrix')
    @patch('teuthology.suite.util.git_ls_remote')
    @patch('teuthology.suite.util.package_version_for_hash')
    @patch('teuthology.suite.util.git_validate_sha1')
    @patch('teuthology.suite.util.get_arch')
    def test_successful_schedule(
        self,
        m_get_arch,
        m_git_validate_sha1,
        m_package_version_for_hash,
        m_git_ls_remote,
        m_build_matrix,
        m_file,
        m_get_install_task_flavor,
        m_get_package_versions,
        m_has_packages_for_distro,
        m_write_rerun_memo,
        m_schedule_jobs,
    ):
        m_get_arch.return_value = 'x86_64'
        m_git_validate_sha1.return_value = self.args.ceph_sha1
        m_package_version_for_hash.return_value = 'ceph_version'
        m_git_ls_remote.return_value = 'suite_hash'
        build_matrix_desc = 'desc'
        build_matrix_frags = ['frag1.yml', 'frag2.yml']
        build_matrix_output = [
            (build_matrix_desc, build_matrix_frags),
        ]
        m_build_matrix.return_value = build_matrix_output
        frag1_read_output = 'field1: val1'
        frag2_read_output = 'field2: val2'
        m_file.side_effect = [
            StringIO(frag1_read_output),
            StringIO(frag2_read_output),
        ]
        m_get_install_task_flavor.return_value = 'basic'
        m_get_package_versions.return_value = dict()
        m_has_packages_for_distro.return_value = True
        # schedule_jobs() is just neutered; check calls below

        self.args.newest = 0
        runobj = self.klass(self.args)
        runobj.base_args = list()
        count = runobj.schedule_suite()
        assert(count == 1)
        assert runobj.base_config['suite_sha1'] == 'suite_hash'
        m_has_packages_for_distro.assert_has_calls(
            [call('ceph_sha1', 'ubuntu', '14.04', 'basic', {})],
        )
        frags = (frag1_read_output, frag2_read_output)
        expected_job = dict(
            yaml=yaml.load('\n'.join(frags)),
            sha1='ceph_sha1',
            args=[
                '--description',
                os.path.join(self.args.suite, build_matrix_desc),
                '--',
                ANY,
                build_matrix_frags[0],
                build_matrix_frags[1],
            ],
            desc=os.path.join(self.args.suite, build_matrix_desc),
        )

        m_schedule_jobs.assert_has_calls(
            [call([], [expected_job], runobj.name)],
        )
        m_write_rerun_memo.assert_called_once_with()

    @patch('teuthology.suite.util.find_git_parent')
    @patch('teuthology.suite.run.Run.schedule_jobs')
    @patch('teuthology.suite.util.has_packages_for_distro')
    @patch('teuthology.suite.util.get_package_versions')
    @patch('teuthology.suite.util.get_install_task_flavor')
    @patch('__builtin__.file')
    @patch('teuthology.suite.run.build_matrix')
    @patch('teuthology.suite.util.git_ls_remote')
    @patch('teuthology.suite.util.package_version_for_hash')
    @patch('teuthology.suite.util.git_validate_sha1')
    @patch('teuthology.suite.util.get_arch')
    def test_newest_failure(
        self,
        m_get_arch,
        m_git_validate_sha1,
        m_package_version_for_hash,
        m_git_ls_remote,
        m_build_matrix,
        m_file,
        m_get_install_task_flavor,
        m_get_package_versions,
        m_has_packages_for_distro,
        m_schedule_jobs,
        m_find_git_parent,
    ):
        m_get_arch.return_value = 'x86_64'
        m_git_validate_sha1.return_value = self.args.ceph_sha1
        m_package_version_for_hash.return_value = 'ceph_version'
        m_git_ls_remote.return_value = 'suite_hash'
        build_matrix_desc = 'desc'
        build_matrix_frags = ['frag.yml']
        build_matrix_output = [
            (build_matrix_desc, build_matrix_frags),
        ]
        m_build_matrix.return_value = build_matrix_output
        m_file.side_effect = [StringIO('field: val\n') for i in xrange(11)]
        m_get_install_task_flavor.return_value = 'basic'
        m_get_package_versions.return_value = dict()
        m_has_packages_for_distro.side_effect = [
            False for i in xrange(11)
        ]

        m_find_git_parent.side_effect = lambda proj, sha1: sha1 + '^'

        self.args.newest = 10
        runobj = self.klass(self.args)
        runobj.base_args = list()
        with pytest.raises(ScheduleFailError) as exc:
            runobj.schedule_suite()
        assert 'Exceeded 10 backtracks' in str(exc.value)
        m_find_git_parent.assert_has_calls(
            [call('ceph', 'ceph_sha1' + i * '^') for i in xrange(10)]
        )

    @patch('teuthology.suite.util.find_git_parent')
    @patch('teuthology.suite.run.Run.schedule_jobs')
    @patch('teuthology.suite.run.Run.write_rerun_memo')
    @patch('teuthology.suite.util.has_packages_for_distro')
    @patch('teuthology.suite.util.get_package_versions')
    @patch('teuthology.suite.util.get_install_task_flavor')
    @patch('__builtin__.file')
    @patch('teuthology.suite.run.build_matrix')
    @patch('teuthology.suite.util.git_ls_remote')
    @patch('teuthology.suite.util.package_version_for_hash')
    @patch('teuthology.suite.util.git_validate_sha1')
    @patch('teuthology.suite.util.get_arch')
    def test_newest_success(
        self,
        m_get_arch,
        m_git_validate_sha1,
        m_package_version_for_hash,
        m_git_ls_remote,
        m_build_matrix,
        m_file,
        m_get_install_task_flavor,
        m_get_package_versions,
        m_has_packages_for_distro,
        m_write_rerun_memo,
        m_schedule_jobs,
        m_find_git_parent,
    ):
        m_get_arch.return_value = 'x86_64'
        # rig has_packages_for_distro to fail this many times, so
        # everything will run NUM_FAILS+1 times
        NUM_FAILS = 5
        m_git_validate_sha1.return_value = self.args.ceph_sha1
        m_package_version_for_hash.return_value = 'ceph_version'
        m_git_ls_remote.return_value = 'suite_hash'
        build_matrix_desc = 'desc'
        build_matrix_frags = ['frag.yml']
        build_matrix_output = [
            (build_matrix_desc, build_matrix_frags),
        ]
        m_build_matrix.return_value = build_matrix_output
        m_file.side_effect = [
            StringIO('field: val\n') for i in xrange(NUM_FAILS+1)
        ]
        m_get_install_task_flavor.return_value = 'basic'
        m_get_package_versions.return_value = dict()
        # NUM_FAILS, then success
        m_has_packages_for_distro.side_effect = \
            [False for i in xrange(NUM_FAILS)] + [True]

        m_find_git_parent.side_effect = lambda proj, sha1: sha1 + '^'

        self.args.newest = 10
        runobj = self.klass(self.args)
        runobj.base_args = list()
        count = runobj.schedule_suite()
        assert count == 1
        m_has_packages_for_distro.assert_has_calls(
            [call('ceph_sha1' + '^' * i, 'ubuntu', '14.04', 'basic', {})
             for i in xrange(NUM_FAILS+1)]
        )
        m_find_git_parent.assert_has_calls(
            [call('ceph', 'ceph_sha1' + i * '^') for i in xrange(NUM_FAILS)]
        )
