import textwrap
from ..config import config
from .. import results

from teuthology import report

from mock import patch, DEFAULT


class TestResultsEmail(object):
    reference = {
        'run_name': 'test_name',
        'jobs': [
            # Hung
            {'description': 'description for job with name test_name',
             'job_id': 30481,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/30481/teuthology.log',  # noqa
             'owner': 'job@owner',
             'pid': 80399,
             'duration': None,
             'status': 'running',
             },
            # Failed
            {'description': 'description for job with name test_name',
             'job_id': 88979,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/88979/teuthology.log',  # noqa
             'owner': 'job@owner',
             'pid': 3903,
             'duration': 35190,
             'success': False,
             'status': 'fail',
             'failure_reason': 'Failure reason!',
             },
            # Passed
            {'description': 'description for job with name test_name',
             'job_id': 68369,
             'name': 'test_name',
             'log_href': 'http://qa-proxy.ceph.com/teuthology/test_name/68369/teuthology.log',  # noqa
             'owner': 'job@owner',
             'pid': 38524,
             'duration': 33771,
             'success': True,
             'status': 'pass',
             },
        ],
        'subject': '1 failed, 1 hung, 1 passed in test_name',
        'body': textwrap.dedent("""
    Test Run: test_name
    =================================================================
    info:   http://example.com/test_name/
    logs:   http://qa-proxy.ceph.com/teuthology/test_name/
    failed: 1
    hung:   1
    passed: 1

    Failed
    =================================================================
    [88979]  description for job with name test_name
    -----------------------------------------------------------------
    time:   35190s
    info:   http://example.com/test_name/88979/
    log:    http://qa-proxy.ceph.com/teuthology/test_name/88979/

        Failure reason!


    Hung
    =================================================================
    [30481] description for job with name test_name
    info:   http://example.com/test_name/30481/

    Passed
    =================================================================
    [68369] description for job with name test_name
    time:   33771s
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
