import textwrap
from teuthology.config import config
from teuthology import results
from teuthology import report

from unittest.mock import patch, DEFAULT


class TestResultsEmail(object):
    reference = {
        'run_name': 'test_name',
        'jobs': [
            # Running
            {'description': 'description for job with name test_name',
             'job_id': 30481,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/30481/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': None,
             'status': 'running',
             },
            # Waiting
            {'description': 'description for job with name test_name',
             'job_id': 62965,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/30481/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': None,
             'status': 'waiting',
             },
            # Queued
            {'description': 'description for job with name test_name',
             'job_id': 79063,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/30481/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': None,
             'status': 'queued',
             },
            # Failed
            {'description': 'description for job with name test_name',
             'job_id': 88979,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/88979/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': 35190,
             'success': False,
             'status': 'fail',
             'failure_reason': 'Failure reason!',
             },
            # Dead
            {'description': 'description for job with name test_name',
             'job_id': 69152,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/69152/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': 5225,
             'success': False,
             'status': 'dead',
             'failure_reason': 'Dead reason!',
             },
            # Passed
            {'description': 'description for job with name test_name',
             'job_id': 68369,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/68369/teuthology.log',  # noqa
             'owner': 'job@owner',
             'duration': 33771,
             'success': True,
             'status': 'pass',
             },
        ],
        'subject': '1 fail, 1 dead, 1 running, 1 waiting, 1 queued, 1 pass in test_name',  # noqa
        'body': textwrap.dedent("""
    Test Run: test_name
    =================================================================
    info:    http://example.com/test_name/
    logs:    http://qa-proxy.ceph.com/teuthology/test_name/
    failed:  1
    dead:    1
    running: 1
    waiting: 1
    queued:  1
    passed:  1


    Fail
    =================================================================
    [88979]  description for job with name test_name
    -----------------------------------------------------------------
    time:   09:46:30
    info:   http://example.com/test_name/88979/
    log:    http://qa-proxy.ceph.com/teuthology/test_name/88979/

        Failure reason!



    Dead
    =================================================================
    [69152]  description for job with name test_name
    -----------------------------------------------------------------
    time:   01:27:05
    info:   http://example.com/test_name/69152/
    log:    http://qa-proxy.ceph.com/teuthology/test_name/69152/

        Dead reason!



    Running
    =================================================================
    [30481] description for job with name test_name
    info:   http://example.com/test_name/30481/



    Waiting
    =================================================================
    [62965] description for job with name test_name
    info:   http://example.com/test_name/62965/



    Queued
    =================================================================
    [79063] description for job with name test_name
    info:   http://example.com/test_name/79063/



    Pass
    =================================================================
    [68369] description for job with name test_name
    time:   09:22:51
    info:   http://example.com/test_name/68369/
    """).strip(),
    }

    def setup(self):
        config.results_ui_server = "http://example.com/"
        config.archive_server = "http://qa-proxy.ceph.com/teuthology/"

    def test_build_email_body(self):
        run_name = self.reference['run_name']
        with patch.multiple(
                report,
                ResultsReporter=DEFAULT,
                ):
            reporter = report.ResultsReporter()
            reporter.get_jobs.return_value = self.reference['jobs']
            (subject, body) = results.build_email_body(
                run_name, _reporter=reporter)
        assert subject == self.reference['subject']
        assert body == self.reference['body']
