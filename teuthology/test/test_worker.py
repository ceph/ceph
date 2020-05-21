import beanstalkc
import os

from unittest.mock import patch, Mock, MagicMock
from datetime import datetime, timedelta

from teuthology import worker

from teuthology.contextutil import MaxWhileTries


class TestWorker(object):
    def setup(self):
        self.ctx = Mock()
        self.ctx.verbose = True
        self.ctx.archive_dir = '/archive/dir'
        self.ctx.log_dir = '/log/dir'
        self.ctx.tube = 'tube'

    @patch("os.path.exists")
    def test_restart_file_path_doesnt_exist(self, m_exists):
        m_exists.return_value = False
        result = worker.sentinel(worker.restart_file_path)
        assert not result

    @patch("os.path.getmtime")
    @patch("os.path.exists")
    @patch("teuthology.worker.datetime")
    def test_needs_restart(self, m_datetime, m_exists, m_getmtime):
        m_exists.return_value = True
        m_datetime.utcfromtimestamp.return_value = datetime.utcnow() + timedelta(days=1)
        result = worker.sentinel(worker.restart_file_path)
        assert result

    @patch("os.path.getmtime")
    @patch("os.path.exists")
    @patch("teuthology.worker.datetime")
    def test_does_not_need_restart(self, m_datetime, m_exists, getmtime):
        m_exists.return_value = True
        m_datetime.utcfromtimestamp.return_value = datetime.utcnow() - timedelta(days=1)
        result = worker.sentinel(worker.restart_file_path)
        assert not result

    @patch("os.symlink")
    def test_symlink_success(self, m_symlink):
        worker.symlink_worker_log("path/to/worker.log", "path/to/archive")
        m_symlink.assert_called_with("path/to/worker.log", "path/to/archive/worker.log")

    @patch("teuthology.worker.log")
    @patch("os.symlink")
    def test_symlink_failure(self, m_symlink, m_log):
        m_symlink.side_effect = IOError
        worker.symlink_worker_log("path/to/worker.log", "path/to/archive")
        # actually logs the exception
        assert m_log.exception.called

    @patch("teuthology.worker.run_with_watchdog")
    @patch("teuthology.worker.teuth_config")
    @patch("subprocess.Popen")
    @patch("os.environ")
    @patch("os.mkdir")
    @patch("yaml.safe_dump")
    @patch("tempfile.NamedTemporaryFile")
    def test_run_job_with_watchdog(self, m_tempfile, m_safe_dump, m_mkdir,
                                   m_environ, m_popen, m_t_config,
                                   m_run_watchdog):
        config = {
            "suite_path": "suite/path",
            "config": {"foo": "bar"},
            "verbose": True,
            "owner": "the_owner",
            "archive_path": "archive/path",
            "name": "the_name",
            "description": "the_description",
            "job_id": "1",
        }
        m_tmp = MagicMock()
        temp_file = Mock()
        temp_file.name = "the_name"
        m_tmp.__enter__.return_value = temp_file
        m_tempfile.return_value = m_tmp
        env = dict(PYTHONPATH="python/path")
        m_environ.copy.return_value = env
        m_p = Mock()
        m_p.returncode = 0
        m_popen.return_value = m_p
        m_t_config.results_server = True
        worker.run_job(config, "teuth/bin/path", "archive/dir", verbose=False)
        m_run_watchdog.assert_called_with(m_p, config)
        expected_args = [
            'teuth/bin/path/teuthology',
            '-v',
            '--lock',
            '--block',
            '--owner', 'the_owner',
            '--archive', 'archive/path',
            '--name', 'the_name',
            '--description',
            'the_description',
            '--',
            "the_name"
        ]
        m_popen.assert_called_with(args=expected_args, env=env)

    @patch("time.sleep")
    @patch("teuthology.worker.symlink_worker_log")
    @patch("teuthology.worker.teuth_config")
    @patch("subprocess.Popen")
    @patch("os.environ")
    @patch("os.mkdir")
    @patch("yaml.safe_dump")
    @patch("tempfile.NamedTemporaryFile")
    def test_run_job_no_watchdog(self, m_tempfile, m_safe_dump, m_mkdir,
                                 m_environ, m_popen, m_t_config, m_symlink_log,
                                 m_sleep):
        config = {
            "suite_path": "suite/path",
            "config": {"foo": "bar"},
            "verbose": True,
            "owner": "the_owner",
            "archive_path": "archive/path",
            "name": "the_name",
            "description": "the_description",
            "worker_log": "worker/log.log",
            "job_id": "1",
        }
        m_tmp = MagicMock()
        temp_file = Mock()
        temp_file.name = "the_name"
        m_tmp.__enter__.return_value = temp_file
        m_tempfile.return_value = m_tmp
        env = dict(PYTHONPATH="python/path")
        m_environ.copy.return_value = env
        m_p = Mock()
        m_p.returncode = 1
        m_popen.return_value = m_p
        m_t_config.results_server = False
        worker.run_job(config, "teuth/bin/path", "archive/dir", verbose=False)
        m_symlink_log.assert_called_with(config["worker_log"], config["archive_path"])

    @patch("teuthology.worker.report.try_push_job_info")
    @patch("teuthology.worker.symlink_worker_log")
    @patch("time.sleep")
    def test_run_with_watchdog_no_reporting(self, m_sleep, m_symlink_log, m_try_push):
        config = {
            "name": "the_name",
            "job_id": "1",
            "worker_log": "worker_log",
            "archive_path": "archive/path",
            "teuthology_branch": "master"
        }
        process = Mock()
        process.poll.return_value = "not None"
        worker.run_with_watchdog(process, config)
        m_symlink_log.assert_called_with(config["worker_log"], config["archive_path"])
        m_try_push.assert_called_with(
            dict(name=config["name"], job_id=config["job_id"]),
            dict(status='dead')
        )

    @patch("subprocess.Popen")
    @patch("teuthology.worker.symlink_worker_log")
    @patch("time.sleep")
    def test_run_with_watchdog_with_reporting(self, m_sleep, m_symlink_log, m_popen):
        config = {
            "name": "the_name",
            "job_id": "1",
            "worker_log": "worker_log",
            "archive_path": "archive/path",
            "teuthology_branch": "jewel"
        }
        process = Mock()
        process.poll.return_value = "not None"
        m_proc = Mock()
        m_proc.poll.return_value = "not None"
        m_popen.return_value = m_proc
        worker.run_with_watchdog(process, config)
        m_symlink_log.assert_called_with(config["worker_log"], config["archive_path"])

    @patch("os.path.isdir")
    @patch("teuthology.worker.fetch_teuthology")
    @patch("teuthology.worker.fetch_qa_suite")
    def test_prep_job(self, m_fetch_qa_suite,
                      m_fetch_teuthology, m_isdir):
        config = dict(
            name="the_name",
            job_id="1",
        )
        archive_dir = '/archive/dir'
        log_file_path = '/worker/log'
        m_fetch_teuthology.return_value = '/teuth/path'
        m_fetch_qa_suite.return_value = '/suite/path'
        m_isdir.return_value = True
        got_config, teuth_bin_path = worker.prep_job(
            config,
            log_file_path,
            archive_dir,
        )
        assert got_config['worker_log'] == log_file_path
        assert got_config['archive_path'] == os.path.join(
            archive_dir,
            config['name'],
            config['job_id'],
        )
        assert got_config['teuthology_branch'] == 'master'
        assert m_fetch_teuthology.called_once_with_args(branch='master')
        assert teuth_bin_path == '/teuth/path/virtualenv/bin'
        assert m_fetch_qa_suite.called_once_with_args(branch='master')
        assert got_config['suite_path'] == '/suite/path'

    def build_fake_jobs(self, m_connection, m_job, job_bodies):
        """
        Given patched copies of:
            beanstalkc.Connection
            beanstalkc.Job
        And a list of basic job bodies, return a list of mocked Job objects
        """
        # Make sure instantiating m_job returns a new object each time
        m_job.side_effect = lambda **kwargs: Mock(spec=beanstalkc.Job)
        jobs = []
        job_id = 0
        for job_body in job_bodies:
            job_id += 1
            job = m_job(conn=m_connection, jid=job_id, body=job_body)
            job.jid = job_id
            job.body = job_body
            jobs.append(job)
        return jobs

    @patch("teuthology.worker.run_job")
    @patch("teuthology.worker.prep_job")
    @patch("beanstalkc.Job", autospec=True)
    @patch("teuthology.worker.fetch_qa_suite")
    @patch("teuthology.worker.fetch_teuthology")
    @patch("teuthology.worker.beanstalk.watch_tube")
    @patch("teuthology.worker.beanstalk.connect")
    @patch("os.path.isdir", return_value=True)
    @patch("teuthology.worker.setup_log_file")
    def test_main_loop(
        self, m_setup_log_file, m_isdir, m_connect, m_watch_tube,
        m_fetch_teuthology, m_fetch_qa_suite, m_job, m_prep_job, m_run_job,
                       ):
        m_connection = Mock()
        jobs = self.build_fake_jobs(
            m_connection,
            m_job,
            [
                'foo: bar',
                'stop_worker: true',
            ],
        )
        m_connection.reserve.side_effect = jobs
        m_connect.return_value = m_connection
        m_prep_job.return_value = (dict(), '/bin/path')
        worker.main(self.ctx)
        # There should be one reserve call per item in the jobs list
        expected_reserve_calls = [
            dict(timeout=60) for i in range(len(jobs))
        ]
        got_reserve_calls = [
            call[1] for call in m_connection.reserve.call_args_list
        ]
        assert got_reserve_calls == expected_reserve_calls
        for job in jobs:
            job.bury.assert_called_once_with()
            job.delete.assert_called_once_with()

    @patch("teuthology.worker.report.try_push_job_info")
    @patch("teuthology.worker.run_job")
    @patch("beanstalkc.Job", autospec=True)
    @patch("teuthology.worker.fetch_qa_suite")
    @patch("teuthology.worker.fetch_teuthology")
    @patch("teuthology.worker.beanstalk.watch_tube")
    @patch("teuthology.worker.beanstalk.connect")
    @patch("os.path.isdir", return_value=True)
    @patch("teuthology.worker.setup_log_file")
    def test_main_loop_13925(
        self, m_setup_log_file, m_isdir, m_connect, m_watch_tube,
        m_fetch_teuthology, m_fetch_qa_suite, m_job, m_run_job,
        m_try_push_job_info,
                       ):
        m_connection = Mock()
        jobs = self.build_fake_jobs(
            m_connection,
            m_job,
            [
                'name: name',
                'name: name\nstop_worker: true',
            ],
        )
        m_connection.reserve.side_effect = jobs
        m_connect.return_value = m_connection
        m_fetch_qa_suite.side_effect = [
            '/suite/path',
            MaxWhileTries(),
            MaxWhileTries(),
        ]
        worker.main(self.ctx)
        assert len(m_run_job.call_args_list) == 0
        assert len(m_try_push_job_info.call_args_list) == len(jobs)
        for i in range(len(jobs)):
            push_call = m_try_push_job_info.call_args_list[i]
            assert push_call[0][1]['status'] == 'dead'
