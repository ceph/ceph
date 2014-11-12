import os
import requests
from pytest import raises, skip

from teuthology.config import config
from teuthology import suite


class TestSuiteOnline(object):
    def setup(self):
        if 'TEST_ONLINE' not in os.environ:
            skip("To run these sets, set the environment variable TEST_ONLINE")

    def test_ceph_hash_simple(self):
        resp = requests.get(
            'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
        ref_hash = resp.json()['object']['sha']
        assert suite.get_hash('ceph') == ref_hash

    def test_kernel_hash_saya(self):
        # We don't currently have these packages.
        assert suite.get_hash('kernel', 'master', 'basic', 'saya') is None

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
