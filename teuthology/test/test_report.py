import yaml
import json
import fake_archive
from .. import report


class TestSerializer(object):
    def setup(self):
        self.archive = fake_archive.FakeArchive()
        self.archive.setup()
        self.archive_base = self.archive.archive_base
        self.reporter = report.ResultsReporter(archive_base=self.archive_base)

    def teardown(self):
        self.archive.teardown()

    def test_all_runs_one_run(self):
        run_name = "test_all_runs"
        yaml_path = "examples/3node_ceph.yaml"
        job_count = 3
        self.archive.create_fake_run(run_name, job_count, yaml_path)
        assert [run_name] == self.reporter.serializer.all_runs

    def test_all_runs_three_runs(self):
        run_count = 3
        runs = {}
        for i in range(run_count):
            run_name = "run #%s" % i
            yaml_path = "examples/3node_ceph.yaml"
            job_count = 3
            job_ids = self.archive.create_fake_run(
                run_name,
                job_count,
                yaml_path)
            runs[run_name] = job_ids
        assert sorted(runs.keys()) == sorted(self.reporter.serializer.all_runs)

    def test_jobs_for_run(self):
        run_name = "test_jobs_for_run"
        yaml_path = "examples/3node_ceph.yaml"
        job_count = 3
        jobs = self.archive.create_fake_run(run_name, job_count, yaml_path)
        job_ids = [str(job['job_id']) for job in jobs]

        got_jobs = self.reporter.serializer.jobs_for_run(run_name)
        assert sorted(job_ids) == sorted(got_jobs.keys())

    def test_running_jobs_for_run(self):
        run_name = "test_jobs_for_run"
        yaml_path = "examples/3node_ceph.yaml"
        job_count = 10
        num_hung = 3
        self.archive.create_fake_run(run_name, job_count, yaml_path,
                                     num_hung=num_hung)

        got_jobs = self.reporter.serializer.running_jobs_for_run(run_name)
        assert len(got_jobs) == job_count - num_hung

    def test_json_for_job(self):
        run_name = "test_json_for_job"
        yaml_path = "examples/3node_ceph.yaml"
        job_count = 1
        jobs = self.archive.create_fake_run(run_name, job_count, yaml_path)
        job = jobs[0]

        with file(yaml_path) as yaml_file:
            obj_from_yaml = yaml.safe_load(yaml_file)
        full_obj = obj_from_yaml.copy()
        full_obj.update(job['info'])
        full_obj.update(job['summary'])

        out_json = self.reporter.serializer.json_for_job(
            run_name, str(job['job_id']))
        out_obj = json.loads(out_json)
        assert full_obj == out_obj


