import os
import shutil
import yaml
import random


class FakeArchive(object):
    def __init__(self, archive_base="./test_archive"):
        self.archive_base = archive_base

    def get_random_metadata(self, run_name, job_id=None, hung=False):
        """
        Generate a random info dict for a fake job. If 'hung' is not True, also
        generate a summary dict.

        :param run_name:   Run name e.g. 'test_foo'
        :param job_id: Job ID e.g. '12345'
        :param hung:   Simulate a hung job e.g. don't return a summary.yaml
        :return:       A dict with keys 'job_id', 'info' and possibly
                       'summary', with corresponding values
        """
        rand = random.Random()

        description = 'description for job with id %s' % job_id
        owner = 'job@owner'
        duration = rand.randint(1, 36000)
        pid = rand.randint(1000, 99999)
        job_id = rand.randint(1, 99999)

        info = {
            'description': description,
            'job_id': job_id,
            'run_name': run_name,
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

    def teardown(self):
        shutil.rmtree(self.archive_base)

    def populate_archive(self, run_name, jobs):
        run_archive_dir = os.path.join(self.archive_base, run_name)
        os.mkdir(run_archive_dir)
        for job in jobs:
            archive_dir = os.path.join(run_archive_dir, str(job['job_id']))
            os.mkdir(archive_dir)

            with file(os.path.join(archive_dir, 'info.yaml'), 'w') as yfile:
                yaml.safe_dump(job['info'], yfile)

            if 'summary' in job:
                summary_path = os.path.join(archive_dir, 'summary.yaml')
                with file(summary_path, 'w') as yfile:
                    yaml.safe_dump(job['summary'], yfile)

    def create_fake_run(self, run_name, job_count, yaml_path, num_hung=0):
        """
        Creates a fake run using run_name. Uses the YAML specified for each
        job's config.yaml

        Returns a list of job_ids
        """
        assert os.path.exists(yaml_path)
        assert job_count > 0
        jobs = []
        made_hung = 0
        for i in range(job_count):
            if made_hung < num_hung:
                jobs.append(self.get_random_metadata(run_name, hung=True))
                made_hung += 1
            else:
                jobs.append(self.get_random_metadata(run_name, hung=False))
            #job_config = yaml.safe_load(yaml_path)
        self.populate_archive(run_name, jobs)
        for job in jobs:
            job_id = job['job_id']
            job_yaml_path = os.path.join(self.archive_base, run_name,
                                         str(job_id), 'config.yaml')
            shutil.copyfile(yaml_path, job_yaml_path)
        return jobs

