from humanfriendly import format_timespan
from mock import Mock, patch
from pytest import mark
from teuthology.config import config
from teuthology.run_tasks import build_email_body as email_body
from textwrap import dedent

class TestSleepBeforeTeardownEmail(object):
    def setup(self):
        config.results_ui_server = "http://example.com/"
        config.archive_server = "http://qa-proxy.ceph.com/teuthology/"

    @mark.parametrize(
        ['status', 'owner', 'suite_name', 'run_name', 'job_id', 'dura'],
        [
            [
                'pass',
                'noreply@host',
                'dummy',
                'run-name',
                123,
                3600,
            ],
            [
                'fail',
                'noname',
                'yummy',
                'next-run',
                1000,
                99999,
            ],
        ]
    )
    @patch("teuthology.run_tasks.time.time")
    def test_sleep_before_teardown_email_body(self, m_time, status, owner,
                                              suite_name, run_name, job_id, dura):
        ctx = Mock()
        archive_path='archive/path'
        archive_dir='/archive/dir'
        date_sec=3661
        date_str='1970-01-01 01:01:01'
        m_time.return_value=float(date_sec)
        duration_sec=dura
        duration_str=format_timespan(duration_sec)
        ref_body=dedent("""
            Teuthology job {run}/{job} has fallen asleep at {date} for {duration_str}

            Owner: {owner}
            Suite Name: {suite}
            Sleep Date: {date}
            Sleep Time: {duration_sec} seconds ({duration_str})
            Job Info: http://example.com/{run}/
            Job Logs: http://qa-proxy.ceph.com/teuthology/path/{job}/
            Task Stack: a/b/c
            Current Status: {status}"""
            .format(duration_sec=duration_sec, duration_str=duration_str,
                owner=owner, suite=suite_name, run=run_name,
                job=job_id, status=status, date=date_str))
        print(ref_body)
        ctx.config = dict(
            archive_path=archive_path,
            job_id=job_id,
            suite=suite_name,
            )
        if status == 'pass':
            ctx.summary = dict(
                success=True,
            )
        elif status == 'fail':
            ctx.summary = dict(
                success=False,
            )
        else:
            ctx.summary = dict()

        ctx.owner   = owner
        ctx.name    = run_name
        ctx.archive_dir = archive_dir
        tasks = [('a', None), ('b', None), ('c', None)]
        (subj, body) = email_body(ctx, tasks, dura)
        assert body == ref_body.lstrip('\n')
