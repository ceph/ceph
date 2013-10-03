import os
import shutil
import yaml
import random


class FakeArchive(object):
    def __init__(self, archive_base="./test_archive"):
        self.archive_base = archive_base

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
        if os.path.exists(self.archive_base):
            shutil.rmtree(self.archive_base)
        os.mkdir(self.archive_base)

    def populate_archive(self, jobs):
        for job in jobs:
            archive_dir = os.path.join(self.archive_base, str(job['job_id']))
            os.mkdir(archive_dir)

            with file(os.path.join(archive_dir, 'info.yaml'), 'w') as yfile:
                yaml.safe_dump(job['info'], yfile)

            if 'summary' in job:
                summary_path = os.path.join(archive_dir, 'summary.yaml')
                with file(summary_path, 'w') as yfile:
                    yaml.safe_dump(job['summary'], yfile)

    def teardown(self):
        shutil.rmtree(self.archive_base)

