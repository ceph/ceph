import os
import requests
import yaml
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


class TestSuiteOnline(object):
    def setup(self):
        if 'TEST_ONLINE' not in os.environ:
            skip("To run these sets, set the environment variable TEST_ONLINE")

    def test_ceph_hash(self):
        resp = requests.get(
            'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
        ref_hash = resp.json()['object']['sha']
        assert suite.get_hash('ceph') == ref_hash

    def test_config_bogus_kernel_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', 'c', 't', 'bogus_kernel_branch',
                                        'f', 'd', 'm')

    def test_config_bogus_kernel_flavor(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', 'c', 't', 'k',
                                        'bogus_kernel_flavor', 'd', 'm')

    def test_config_bogus_ceph_branch(self):
        # Don't attempt to send email
        config.results_email = None
        with raises(suite.ScheduleFailError):
            suite.create_initial_config('s', 'bogus_ceph_branch', 't', 'k',
                                        'f', 'd', 'm')

    def test_config_substitution(self):
        # Don't attempt to send email
        config.results_email = None
        job_config_str = suite.create_initial_config('MY_SUITE', 'master',
                                                     'master', 'testing',
                                                     'basic', 'centos',
                                                     'plana')
        job_config = yaml.safe_load(job_config_str)
        assert job_config['suite'] == 'MY_SUITE'


# maybe use notario for the above?
