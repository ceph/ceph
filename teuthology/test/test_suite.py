import os
import shutil
import random
import yaml
import textwrap
from .. import suite


class TestResultsEmail(object):
    archive_base_dir = 'test_archive'

    reference = {
        'name': 'test_name',
        'jobs': [
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 30481, 'name': 'test_name', 'owner': 'job@owner',
                      'pid': 80399},
             'job_id': 30481},
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 88979, 'name': 'test_name', 'owner': 'job@owner',
                      'pid': 3903},
                'job_id': 88979,
                'summary': {
                    'description': 'description for job with name test_name',
                    'duration': 35190, 'failure_reason': 'Failure reason!',
                    'owner': 'job@owner', 'success': False}},
            {'info': {'description': 'description for job with name test_name',
                      'job_id': 68369, 'name': 'test_name', 'owner': 'job@owner',
                      'pid': 38524},
             'job_id': 68369,
             'summary': {'description': 'description for job with name test_name',
                         'duration': 33771, 'owner': 'job@owner', 'success':
                         True}},
        ],
        'subject': '1 failed, 1 hung, 1 passed in test_name',
        'body': textwrap.dedent("""
    Test Run: test_name
    =================================================================
    logs:   http://qa-proxy.ceph.com/teuthology/test_archive/
    failed: 1
    hung:   1
    passed: 1

    Failed
    =================================================================
    [88979]  description for job with name test_name
    -----------------------------------------------------------------
    time:   35190s
    log:    http://qa-proxy.ceph.com/teuthology/test_archive/88979/

        Failure reason!


    Hung
    =================================================================
    [30481] description for job with name test_name

    Passed
    =================================================================
    [68369] description for job with name test_name
    time:    33771s
    """).strip(),
    }

    def get_random_metadata(self, name, hung=False):
        """
        Generate a random info dict for a fake job. If 'hung' is not True, also
        generate a summary dict.

        :param name: test name e.g. 'test_foo'
        :param hung: simulate a hung job e.g. don't return a summary.yaml
        :return: a dict with keys 'job_id', 'info' and possibly 'summary', with
                corresponding values
        """
        rand = random.Random()

        description = 'description for job with name %s' % name
        owner = 'job@owner'
        duration = rand.randint(1, 36000)
        pid = rand.randint(1000, 99999)
        job_id = rand.randint(1, 99999)

        info = {
            'description': description,
            'job_id': job_id,
            'name': name,
            'owner': owner,
            'pid': pid,
        }

        metadata = {
            'info': info,
            'job_id': job_id,
        }

        if not hung:
            success = True if rand.randint(0, 1) != 1 else False

            summary = {
                'description': description,
                'duration': duration,
                'owner': owner,
                'success': success,
            }

            if not success:
                summary['failure_reason'] = 'Failure reason!'
            metadata['summary'] = summary

        return metadata

    def setup(self):
        os.mkdir(self.archive_base_dir)

    def populate_archive(self, jobs):
        for job in jobs:
            archive_dir = os.path.join(self.archive_base_dir, str(job['job_id']))
            os.mkdir(archive_dir)

            with file(os.path.join(archive_dir, 'info.yaml'), 'w') as yfile:
                yaml.safe_dump(job['info'], yfile)

            if 'summary' in job:
                with file(os.path.join(archive_dir, 'summary.yaml'), 'w') as yfile:
                    yaml.safe_dump(job['summary'], yfile)

    def teardown(self):
        shutil.rmtree(self.archive_base_dir)

    def test_build_email_body(self):
        self.populate_archive(self.reference['jobs'])
        (subject, body) = suite.build_email_body(self.reference['name'], self.archive_base_dir, 36000)
        assert subject == self.reference['subject']
        assert body == self.reference['body']
