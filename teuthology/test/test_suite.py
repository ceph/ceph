import os
import requests
from datetime import datetime
from pytest import raises, skip

from teuthology.config import config
from teuthology import suite


class TestSuiteOffline(object):
    def test_name_timestamp_passed(self):
        stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype', timestamp=stamp)
        assert str(stamp) in name

    def test_name_timestamp_not_passed(self):
        stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype')
        assert str(stamp) in name

    def test_name_user(self):
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype', user='USER')
        assert name.startswith('USER-')

    def test_distro_defaults_saya(self):
        assert suite.get_distro_defaults('ubuntu', 'saya') == ('armv7l',
                                                               'saucy', 'deb')

    def test_distro_defaults_plana(self):
        assert suite.get_distro_defaults('ubuntu', 'plana') == ('x86_64',
                                                                'precise',
                                                                'deb')

    def test_gitbuilder_url(self):
        ref_url = "http://gitbuilder.ceph.com/ceph-deb-squeeze-x86_64-basic/"
        assert suite.get_gitbuilder_url('ceph', 'squeeze', 'deb', 'x86_64',
                                        'basic') == ref_url

    def test_substitute_placeholders(self):
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            ceph_branch='ceph_branch',
            ceph_hash='ceph_hash',
            teuthology_branch='teuthology_branch',
            machine_type='machine_type',
            distro='distro',
            s3_branch='s3_branch',
        )
        output_dict = suite.substitute_placeholders(suite.dict_templ,
                                                    input_dict)
        assert output_dict['suite'] == 'suite'
        assert isinstance(suite.dict_templ['suite'], suite.Placeholder)
        assert isinstance(
            suite.dict_templ['overrides']['admin_socket']['branch'],
            suite.Placeholder)


class TestSuiteOnline(object):
    def setup(self):
        if 'TEST_ONLINE' not in os.environ:
            skip("To run these sets, set the environment variable TEST_ONLINE")

    def test_ceph_hash(self):
        resp = requests.get(
            'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
        ref_hash = resp.json()['object']['sha']
        assert suite.get_hash('ceph') == ref_hash

    def test_all_master_branches(self):
        # Don't attempt to send email
        config.results_email = None
        job_config = suite.create_initial_config('suite', 'master',
                                                 'master', 'master', 'testing',
                                                 'basic', 'centos', 'plana')
        assert ((job_config.branch, job_config.teuthology_branch,
                 job_config.suite_branch) == ('master', 'master', 'master'))

    def test_config_bogus_kernel_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', None, 'master', 't',
                                        'bogus_kernel_branch', 'f', 'd', 'm')

    def test_config_bogus_kernel_flavor(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', None, 'master', 't', 'k',
                                        'bogus_kernel_flavor', 'd', 'm')

    def test_config_bogus_ceph_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', None, 'bogus_ceph_branch', 't',
                                        'k', 'f', 'd', 'm')

    def test_config_bogus_suite_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', 'bogus_suite_branch', 'master',
                                        't', 'k', 'f', 'd', 'm')

    def test_config_bogus_teuthology_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', None, 'master',
                                        'bogus_teuth_branch', 'k', 'f', 'd',
                                        'm')

    def test_config_substitution(self):
        # Don't attempt to send email
        config.results_email = None
        job_config = suite.create_initial_config('MY_SUITE', 'master',
                                                 'master', 'master', 'testing',
                                                 'basic', 'centos', 'plana')
        assert job_config['suite'] == 'MY_SUITE'

    def test_config_kernel_section(self):
        # Don't attempt to send email
        config.results_email = None
        job_config = suite.create_initial_config('MY_SUITE', 'master',
                                                 'master', 'master', 'testing',
                                                 'basic', 'centos', 'plana')
        assert job_config['kernel']['kdb'] is True


# maybe use notario for the above?
