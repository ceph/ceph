from teuthology import job_status


class TestJobStatus(object):
    def test_get_only_success_true(self):
        summary = dict(success=True)
        status = job_status.get_status(summary)
        assert status == 'pass'

    def test_get_only_success_false(self):
        summary = dict(success=False)
        status = job_status.get_status(summary)
        assert status == 'fail'

    def test_get_status_pass(self):
        summary = dict(status='pass')
        status = job_status.get_status(summary)
        assert status == 'pass'

    def test_get_status_fail(self):
        summary = dict(status='fail')
        status = job_status.get_status(summary)
        assert status == 'fail'

    def test_get_status_dead(self):
        summary = dict(status='dead')
        status = job_status.get_status(summary)
        assert status == 'dead'

    def test_get_status_none(self):
        summary = dict()
        status = job_status.get_status(summary)
        assert status is None

    def test_set_status_pass(self):
        summary = dict()
        job_status.set_status(summary, 'pass')
        assert summary == dict(status='pass', success=True)

    def test_set_status_dead(self):
        summary = dict()
        job_status.set_status(summary, 'dead')
        assert summary == dict(status='dead', success=False)

    def test_set_then_get_status_dead(self):
        summary = dict()
        job_status.set_status(summary, 'dead')
        status = job_status.get_status(summary)
        assert status == 'dead'

    def test_set_status_none(self):
        summary = dict()
        job_status.set_status(summary, None)
        assert summary == dict()

    def test_legacy_fail(self):
        summary = dict(success=True)
        summary['success'] = False
        status = job_status.get_status(summary)
        assert status == 'fail'
