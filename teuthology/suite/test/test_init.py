import os

from copy import deepcopy

from mock import patch, Mock, DEFAULT

from teuthology import suite
from scripts.suite import main
from teuthology.config import config

import pytest
import time


def get_fake_time_and_sleep():
    # Below we set m_time.side_effect, but we also set m_time.return_value.
    # The reason for this is that we need to store a 'fake time' that
    # increments when m_sleep() is called; we could use any variable name we
    # wanted for the return value, but since 'return_value' is already a
    # standard term in mock, and since setting side_effect causes return_value
    # to be ignored, it's safe to just reuse the name here.
    m_time = Mock()
    m_time.return_value = time.time()

    def m_time_side_effect():
        # Fake the slow passage of time
        m_time.return_value += 0.1
        return m_time.return_value
    m_time.side_effect = m_time_side_effect

    def f_sleep(seconds):
        m_time.return_value += seconds
    m_sleep = Mock(wraps=f_sleep)
    return m_time, m_sleep


def setup_module():
    global m_time
    global m_sleep
    m_time, m_sleep = get_fake_time_and_sleep()
    global patcher_time_sleep
    patcher_time_sleep = patch.multiple(
        'teuthology.suite.time',
        time=m_time,
        sleep=m_sleep,
    )
    patcher_time_sleep.start()


def teardown_module():
    patcher_time_sleep.stop()


@patch.object(suite.ResultsReporter, 'get_jobs')
def test_wait_success(m_get_jobs, caplog):
    results = [
        [{'status': 'queued', 'job_id': '2'}],
        [],
    ]
    final = [
        {'status': 'pass', 'job_id': '1',
         'description': 'DESC1', 'log_href': 'http://URL1'},
        {'status': 'fail', 'job_id': '2',
         'description': 'DESC2', 'log_href': 'http://URL2'},
        {'status': 'pass', 'job_id': '3',
         'description': 'DESC3', 'log_href': 'http://URL3'},
    ]

    def get_jobs(name, **kwargs):
        if kwargs['fields'] == ['job_id', 'status']:
            return in_progress.pop(0)
        else:
            return final
    m_get_jobs.side_effect = get_jobs
    suite.Run.WAIT_PAUSE = 1

    in_progress = deepcopy(results)
    assert 0 == suite.wait('name', 1, 'http://UPLOAD_URL')
    assert m_get_jobs.called_with('name', fields=['job_id', 'status'])
    assert 0 == len(in_progress)
    assert 'fail http://UPLOAD_URL/name/2' in caplog.text

    in_progress = deepcopy(results)
    assert 0 == suite.wait('name', 1, None)
    assert m_get_jobs.called_with('name', fields=['job_id', 'status'])
    assert 0 == len(in_progress)
    assert 'fail http://URL2' in caplog.text


@patch.object(suite.ResultsReporter, 'get_jobs')
def test_wait_fails(m_get_jobs):
    results = []
    results.append([{'status': 'queued', 'job_id': '2'}])
    results.append([{'status': 'queued', 'job_id': '2'}])
    results.append([{'status': 'queued', 'job_id': '2'}])

    def get_jobs(name, **kwargs):
        return results.pop(0)
    m_get_jobs.side_effect = get_jobs
    suite.Run.WAIT_PAUSE = 1
    suite.Run.WAIT_MAX_JOB_TIME = 1
    with pytest.raises(suite.WaitException):
        suite.wait('name', 1, None)


REPO_SHORTHAND = [
    ['https://github.com/dude/foo', 'bar',
     'https://github.com/dude/bar.git'],
    ['https://github.com/dude/foo/', 'bar',
     'https://github.com/dude/bar.git'],
    ['https://github.com/ceph/ceph', 'ceph',
     'https://github.com/ceph/ceph.git'],
    ['https://github.com/ceph/ceph/', 'ceph',
     'https://github.com/ceph/ceph.git'],
    ['https://github.com/ceph/ceph.git', 'ceph',
     'https://github.com/ceph/ceph.git'],
    ['https://github.com/ceph/ceph', 'ceph-ci',
     'https://github.com/ceph/ceph-ci.git'],
    ['https://github.com/ceph/ceph-ci', 'ceph',
     'https://github.com/ceph/ceph.git'],
    ['git://git.ceph.com/ceph.git', 'ceph',
     'git://git.ceph.com/ceph.git'],
    ['git://git.ceph.com/ceph.git', 'ceph-ci',
     'git://git.ceph.com/ceph-ci.git'],
    ['git://git.ceph.com/ceph-ci.git', 'ceph',
     'git://git.ceph.com/ceph.git'],
    ['https://github.com/ceph/ceph.git', 'ceph/ceph-ci',
     'https://github.com/ceph/ceph-ci.git'],
    ['https://github.com/ceph/ceph.git', 'https://github.com/ceph/ceph-ci',
     'https://github.com/ceph/ceph-ci'],
    ['https://github.com/ceph/ceph.git', 'https://github.com/ceph/ceph-ci/',
     'https://github.com/ceph/ceph-ci/'],
    ['https://github.com/ceph/ceph.git', 'https://github.com/ceph/ceph-ci.git',
     'https://github.com/ceph/ceph-ci.git'],
]


@pytest.mark.parametrize(['orig', 'shorthand', 'result'], REPO_SHORTHAND)
def test_expand_short_repo_name(orig, shorthand, result):
    assert suite.expand_short_repo_name(shorthand, orig) == result


class TestSuiteMain(object):
    def test_main(self):
        suite_name = 'SUITE'
        throttle = '3'
        machine_type = 'burnupi'

        def prepare_and_schedule(obj):
            assert obj.base_config.suite == suite_name
            assert obj.args.throttle == throttle

        def fake_str(*args, **kwargs):
            return 'fake'

        def fake_bool(*args, **kwargs):
            return True

        with patch.multiple(
                'teuthology.suite.run.util',
                fetch_repos=DEFAULT,
                package_version_for_hash=fake_str,
                git_branch_exists=fake_bool,
                git_ls_remote=fake_str,
                ):
            with patch.multiple(
                'teuthology.suite.run.Run',
                prepare_and_schedule=prepare_and_schedule,
            ):
                main([
                    '--ceph', 'master',
                    '--suite', suite_name,
                    '--throttle', throttle,
                    '--machine-type', machine_type,
                ])

    def test_schedule_suite_noverify(self):
        suite_name = 'noop'
        suite_dir = os.path.dirname(__file__)
        throttle = '3'
        machine_type = 'burnupi'

        with patch.multiple(
            'teuthology.suite.util',
            fetch_repos=DEFAULT,
            teuthology_schedule=DEFAULT,
            get_arch=lambda x: 'x86_64',
            get_gitbuilder_hash=DEFAULT,
            git_ls_remote=lambda *args: '1234',
            package_version_for_hash=DEFAULT,
        ) as m:
            m['package_version_for_hash'].return_value = 'fake-9.5'
            config.suite_verify_ceph_hash = False
            main([
                '--ceph', 'master',
                '--suite', suite_name,
                '--suite-dir', suite_dir,
                '--suite-relpath', '',
                '--throttle', throttle,
                '--machine-type', machine_type
            ])
            m_sleep.assert_called_with(int(throttle))
            m['get_gitbuilder_hash'].assert_not_called()

    def test_schedule_suite(self):
        suite_name = 'noop'
        suite_dir = os.path.dirname(__file__)
        throttle = '3'
        machine_type = 'burnupi'

        with patch.multiple(
            'teuthology.suite.util',
            fetch_repos=DEFAULT,
            teuthology_schedule=DEFAULT,
            get_arch=lambda x: 'x86_64',
            get_gitbuilder_hash=DEFAULT,
            git_ls_remote=lambda *args: '12345',
            package_version_for_hash=DEFAULT,
        ) as m:
            m['package_version_for_hash'].return_value = 'fake-9.5'
            config.suite_verify_ceph_hash = True
            main([
                '--ceph', 'master',
                '--suite', suite_name,
                '--suite-dir', suite_dir,
                '--suite-relpath', '',
                '--throttle', throttle,
                '--machine-type', machine_type
            ])
            m_sleep.assert_called_with(int(throttle))
