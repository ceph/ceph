import fake_archive
from .. import report


class TestReport(object):
    def setup(self):
        self.archive = fake_archive.FakeArchive()
        self.archive.setup()
        self.archive_base = self.archive.archive_base
        self.reporter = report.ResultsReporter(archive_base=self.archive_base)

    def teardown(self):
        self.archive.teardown()

    def test_all_runs(self):
        run_name = "test_all_runs"
        yaml_path = "examples/3node_ceph.yaml"
        job_count = 3
        self.archive.create_fake_run(run_name, job_count, yaml_path)
        assert [run_name] == self.reporter.serializer.all_runs


