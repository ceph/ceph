import pytest
import docopt

from unittest.mock import patch, call, Mock

from teuthology import run
from scripts import run as scripts_run


class TestRun(object):
    """ Tests for teuthology.run """

    @patch("teuthology.log.setLevel")
    @patch("teuthology.setup_log_file")
    @patch("os.mkdir")
    def test_set_up_logging(self, m_mkdir, m_setup_log_file, m_setLevel):
        run.set_up_logging(True, "path/to/archive")
        m_mkdir.assert_called_with("path/to/archive")
        m_setup_log_file.assert_called_with("path/to/archive/teuthology.log")
        assert m_setLevel.called

    # because of how we import things, mock merge_configs from run - where it's used
    # see: http://www.voidspace.org.uk/python/mock/patch.html#where-to-patch
    @patch("teuthology.run.merge_configs")
    def test_setup_config(self, m_merge_configs):
        config = {"job_id": 1, "foo": "bar"}
        m_merge_configs.return_value = config
        result = run.setup_config(["some/config.yaml"])
        assert m_merge_configs.called
        assert result["job_id"] == "1"
        assert result["foo"] == "bar"

    @patch("teuthology.run.merge_configs")
    def test_setup_config_targets_ok(self, m_merge_configs):
        config = {"targets": list(range(4)), "roles": list(range(2))}
        m_merge_configs.return_value = config
        result = run.setup_config(["some/config.yaml"])
        assert result["targets"] == [0, 1, 2, 3]
        assert result["roles"] == [0, 1]

    @patch("teuthology.run.merge_configs")
    def test_setup_config_targets_invalid(self, m_merge_configs):
        config = {"targets": range(2), "roles": range(4)}
        m_merge_configs.return_value = config
        with pytest.raises(AssertionError):
            run.setup_config(["some/config.yaml"])

    @patch("teuthology.run.open")
    def test_write_initial_metadata(self, m_open):
        config = {"job_id": "123", "foo": "bar"}
        run.write_initial_metadata(
            "some/archive/dir",
            config,
            "the_name",
            "the_description",
            "the_owner",
        )
        expected = [
            call('some/archive/dir/pid', 'w'),
            call('some/archive/dir/owner', 'w'),
            call('some/archive/dir/orig.config.yaml', 'w'),
            call('some/archive/dir/info.yaml', 'w')
        ]
        assert m_open.call_args_list == expected

    def test_get_machine_type(self):
        result = run.get_machine_type(None, {"machine-type": "the_machine_type"})
        assert result == "the_machine_type"

    def test_get_summary(self):
        result = run.get_summary("the_owner", "the_description")
        assert result == {"owner": "the_owner", "description": "the_description", "success": True}
        result = run.get_summary("the_owner", None)
        assert result == {"owner": "the_owner", "success": True}

    def test_validate_tasks_invalid(self):
        config = {"tasks": [{"kernel": "can't be here"}]}
        with pytest.raises(AssertionError) as excinfo:
            run.validate_tasks(config)
        assert excinfo.value.args[0].startswith("kernel installation")

    def test_validate_task_no_tasks(self):
        result = run.validate_tasks({})
        assert result == []

    def test_validate_tasks_valid(self):
        expected = [{"foo": "bar"}, {"bar": "foo"}]
        result = run.validate_tasks({"tasks": expected})
        assert result == expected

    def test_validate_tasks_is_list(self):
        with pytest.raises(AssertionError) as excinfo:
            run.validate_tasks({"tasks": {"foo": "bar"}})
        assert excinfo.value.args[0].startswith("Expected list")

    def test_get_initial_tasks_invalid(self):
        with pytest.raises(AssertionError) as excinfo:
            run.get_initial_tasks(True, {"targets": "can't be here",
                                         "roles": "roles" }, "machine_type")
        assert excinfo.value.args[0].startswith("You cannot")

    def test_get_inital_tasks(self):
        config = {"roles": range(2), "kernel": "the_kernel", "use_existing_cluster": False}
        result = run.get_initial_tasks(True, config, "machine_type")
        assert {"internal.lock_machines": (2, "machine_type")} in result
        assert {"kernel": "the_kernel"} in result
        # added because use_existing_cluster == False
        assert {'internal.vm_setup': None} in result
        assert {'internal.buildpackages_prep': None} in result

    @patch("teuthology.run.fetch_qa_suite")
    def test_fetch_tasks_if_needed(self, m_fetch_qa_suite):
        config = {"suite_path": "/some/suite/path", "suite_branch": "feature_branch"}
        m_fetch_qa_suite.return_value = "/some/other/suite/path"
        result = run.fetch_tasks_if_needed(config)
        m_fetch_qa_suite.assert_called_with("feature_branch")
        assert result == "/some/other/suite/path"

    @patch("teuthology.run.get_status")
    @patch("teuthology.run.nuke")
    @patch("yaml.safe_dump")
    @patch("teuthology.report.try_push_job_info")
    @patch("teuthology.run.email_results")
    @patch("teuthology.run.open")
    @patch("sys.exit")
    def test_report_outcome(self, m_sys_exit, m_open, m_email_results, m_try_push_job_info, m_safe_dump, m_nuke, m_get_status):
        m_get_status.return_value = "fail"
        fake_ctx = Mock()
        summary = {"failure_reason": "reasons"}
        summary_dump = "failure_reason: reasons\n"
        config = {"nuke-on-error": True, "email-on-error": True}
        config_dump = "nuke-on-error: true\nemail-on-error: true\n"
        m_safe_dump.side_effect = [None, summary_dump, config_dump]
        run.report_outcome(config, "the/archive/path", summary, fake_ctx)
        assert m_nuke.called
        m_try_push_job_info.assert_called_with(config, summary)
        m_open.assert_called_with("the/archive/path/summary.yaml", "w")
        assert m_email_results.called
        assert m_open.called
        assert m_sys_exit.called

    @patch("teuthology.run.set_up_logging")
    @patch("teuthology.run.setup_config")
    @patch("teuthology.run.get_user")
    @patch("teuthology.run.write_initial_metadata")
    @patch("teuthology.report.try_push_job_info")
    @patch("teuthology.run.get_machine_type")
    @patch("teuthology.run.get_summary")
    @patch("yaml.safe_dump")
    @patch("teuthology.run.validate_tasks")
    @patch("teuthology.run.get_initial_tasks")
    @patch("teuthology.run.fetch_tasks_if_needed")
    @patch("teuthology.run.run_tasks")
    @patch("teuthology.run.report_outcome")
    def test_main(self, m_report_outcome, m_run_tasks, m_fetch_tasks_if_needed, m_get_initial_tasks, m_validate_tasks,
                  m_safe_dump, m_get_summary, m_get_machine_type, m_try_push_job_info, m_write_initial_metadata,
                  m_get_user, m_setup_config, m_set_up_logging):
        """ This really should be an integration test of some sort. """
        config = {"job_id": 1}
        m_setup_config.return_value = config
        m_get_machine_type.return_value = "machine_type"
        doc = scripts_run.__doc__
        args = docopt.docopt(doc, [
            "--verbose",
            "--archive", "some/archive/dir",
            "--description", "the_description",
            "--lock",
            "--os-type", "os_type",
            "--os-version", "os_version",
            "--block",
            "--name", "the_name",
            "--suite-path", "some/suite/dir",
            "path/to/config.yml",
        ])
        m_get_user.return_value = "the_owner"
        m_get_summary.return_value = dict(success=True, owner="the_owner", description="the_description")
        m_validate_tasks.return_value = ['task3']
        m_get_initial_tasks.return_value = ['task1', 'task2']
        m_fetch_tasks_if_needed.return_value = "some/suite/dir"
        run.main(args)
        m_set_up_logging.assert_called_with(True, "some/archive/dir")
        m_setup_config.assert_called_with(["path/to/config.yml"])
        m_write_initial_metadata.assert_called_with(
            "some/archive/dir",
            config,
            "the_name",
            "the_description",
            "the_owner"
        )
        m_try_push_job_info.assert_called_with(config, dict(status='running'))
        m_get_machine_type.assert_called_with(None, config)
        m_get_summary.assert_called_with("the_owner", "the_description")
        m_get_initial_tasks.assert_called_with(True, config, "machine_type")
        m_fetch_tasks_if_needed.assert_called_with(config)
        assert m_report_outcome.called
        args, kwargs = m_run_tasks.call_args
        fake_ctx = kwargs["ctx"]._conf
        # fields that must be in ctx for the tasks to behave
        expected_ctx = ["verbose", "archive", "description", "owner", "lock", "machine_type", "os_type", "os_version",
                        "block", "name", "suite_path", "config", "summary"]
        for key in expected_ctx:
            assert key in fake_ctx
        assert isinstance(fake_ctx["config"], dict)
        assert isinstance(fake_ctx["summary"], dict)
        assert "tasks" in fake_ctx["config"]
        # ensures that values missing in args are added with the correct value
        assert fake_ctx["owner"] == "the_owner"
        assert fake_ctx["machine_type"] == "machine_type"
        # ensures os_type and os_version are property overwritten
        assert fake_ctx["config"]["os_type"] == "os_type"
        assert fake_ctx["config"]["os_version"] == "os_version"

    def test_get_teuthology_command(self):
        doc = scripts_run.__doc__
        args = docopt.docopt(doc, [
            "--archive", "some/archive/dir",
            "--description", "the_description",
            "--lock",
            "--block",
            "--name", "the_name",
            "--suite-path", "some/suite/dir",
            "path/to/config.yml", "path/to/config2.yaml",
        ])
        result = run.get_teuthology_command(args)
        result = result.split()
        expected = [
            "teuthology",
            "path/to/config.yml", "path/to/config2.yaml",
            "--suite-path", "some/suite/dir",
            "--lock",
            "--description", "the_description",
            "--name", "the_name",
            "--block",
            "--archive", "some/archive/dir",
        ]
        assert len(result) == len(expected)
        for arg in expected:
            assert arg in result
