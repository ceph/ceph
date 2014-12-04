import subprocess

from mock import patch, Mock, MagicMock
from datetime import datetime, timedelta

from .. import worker


class TestWorker(object):

    @patch("os.path.exists")
    def test_restart_file_path_doesnt_exist(self, m_exists):
        m_exists.return_value = False
        result = worker.need_restart()
        assert not result

    @patch("os.path.getmtime")
    @patch("os.path.exists")
    @patch("teuthology.worker.datetime")
    def test_needs_restart(self, m_datetime, m_exists, m_getmtime):
        m_exists.return_value = True
        m_datetime.utcfromtimestamp.return_value = datetime.utcnow() + timedelta(days=1)
        result = worker.need_restart()
        assert result

    @patch("os.path.getmtime")
    @patch("os.path.exists")
    @patch("teuthology.worker.datetime")
    def test_does_not_need_restart(self, m_datetime, m_exists, getmtime):
        m_exists.return_value = True
        m_datetime.utcfromtimestamp.return_value = datetime.utcnow() - timedelta(days=1)
        result = worker.need_restart()
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
    @patch("yaml.safe_dump")
    @patch("tempfile.NamedTemporaryFile")
    def test_run_job_with_watchdog(self, m_tempfile, m_safe_dump, m_environ,
                                   m_popen, m_t_config, m_run_watchdog):
        config = {
            "suite_path": "suite/path",
            "config": {"foo": "bar"},
            "verbose": True,
            "owner": "the_owner",
            "archive_path": "archive/path",
            "name": "the_name",
            "description": "the_description"
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
        worker.run_job(config, "teuth/bin/path")
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
    @patch("yaml.safe_dump")
    @patch("tempfile.NamedTemporaryFile")
    def test_run_job_no_watchdog(self, m_tempfile, m_safe_dump, m_environ,
                                 m_popen, m_t_config, m_symlink_log, m_sleep):
        config = {
            "suite_path": "suite/path",
            "config": {"foo": "bar"},
            "verbose": True,
            "owner": "the_owner",
            "archive_path": "archive/path",
            "name": "the_name",
            "description": "the_description",
            "worker_log": "worker/log.log"
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
        worker.run_job(config, "teuth/bin/path")
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
            "teuthology_branch": "argonaut"
        }
        process = Mock()
        process.poll.return_value = "not None"
        m_proc = Mock()
        m_proc.poll.return_value = "not None"
        m_popen.return_value = m_proc
        worker.run_with_watchdog(process, config)
        m_symlink_log.assert_called_with(config["worker_log"], config["archive_path"])
        expected_cmd = "teuthology-report -v -D -r the_name -j 1"
        m_popen.assert_called_with(
            expected_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
