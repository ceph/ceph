import os
import textwrap
from ..config import config
from .. import results
from .fake_archive import FakeArchive


class TestResultsEmail(object):
    reference = {
        'run_name': 'test_name',
        'jobs': [
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 30481,
                      'name': 'test_name',
                      'owner': 'job@owner',
                      'pid': 80399},
             'job_id': 30481},
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 88979,
                      'name': 'test_name',
                      'owner': 'job@owner',
                      'pid': 3903},
                'job_id': 88979,
                'summary': {
                    'description': 'description for job with name test_name',
                    'duration': 35190, 'failure_reason': 'Failure reason!',
                    'owner': 'job@owner',
                    'success': False}},
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 68369,
                      'name': 'test_name',
                      'owner': 'job@owner',
                      'pid': 38524},
             'job_id': 68369,
             'summary': {
                 'description': 'description for job with name test_name',
                 'duration': 33771, 'owner': 'job@owner', 'success':
                 True}},
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
        self.archive = FakeArchive()
        self.archive.setup()
        self.archive_base = self.archive.archive_base

    def teardown(self):
        self.archive.teardown()

    def test_build_email_body(self):
        run_name = self.reference['run_name']
        run_dir = os.path.join(self.archive_base, run_name)
        self.archive.populate_archive(run_name, self.reference['jobs'])
        (subject, body) = results.build_email_body(
            run_name,
            run_dir,
            36000)
        assert subject == self.reference['subject']
        print body
        assert body == self.reference['body']
