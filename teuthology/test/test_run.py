import pytest

from mock import patch, call, Mock

from teuthology import run


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
        assert result.job_id == "1"
        assert result["foo"] == "bar"

    @patch("teuthology.run.merge_configs")
    def test_setup_config_targets_ok(self, m_merge_configs):
        config = {"targets": range(4), "roles": range(2)}
        m_merge_configs.return_value = config
        result = run.setup_config(["some/config.yaml"])
        assert result.targets
        assert result.roles

    @patch("teuthology.run.merge_configs")
    def test_setup_config_targets_invalid(self, m_merge_configs):
        config = {"targets": range(2), "roles": range(4)}
        m_merge_configs.return_value = config
        with pytest.raises(AssertionError):
            run.setup_config(["some/config.yaml"])

    @patch("__builtin__.file")
    def test_write_initial_metadata(self, m_file):
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
        assert m_file.call_args_list == expected

    def test_get_machine_type(self):
        result = run.get_machine_type(None, {"machine-type": "the_machine_type"})
        assert result == "the_machine_type"

    def test_get_summary(self):
        result = run.get_summary("the_owner", "the_description")
        assert result == {"owner": "the_owner", "description": "the_description", "success": True}
        result = run.get_summary("the_owner", None)
        assert result == {"owner": "the_owner", "success": True}

    def test_validate_tasks_invalid(self):
        config = {"tasks": {"kernel": "can't be here"}}
        with pytest.raises(AssertionError):
            run.validate_tasks(config)

    def test_validate_task_no_tasks(self):
        result = run.validate_tasks({})
        assert result == []

    def test_get_initial_tasks_invalid(self):
        with pytest.raises(AssertionError):
            run.get_initial_tasks(True, {"targets": "can't be here"}, "machine_type")

    def test_get_inital_tasks(self):
        config = {"roles": range(2), "kernel": "the_kernel", "use_existing_cluster": False}
        result = run.get_initial_tasks(True, config, "machine_type")
        assert {"internal.lock_machines": (2, "machine_type")} in result
        assert {"kernel": "the_kernel"} in result
        # added because use_existing_cluster == False
        assert {'internal.vm_setup': None} in result

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
    @patch("__builtin__.file")
    @patch("sys.exit")
    def test_report_outcome(self, m_sys_exit, m_file, m_email_results, m_try_push_job_info, m_safe_dump, m_nuke, m_get_status):
        config = {"nuke-on-error": True, "email-on-error": True}
        m_get_status.return_value = "fail"
        fake_ctx = Mock()
        summary = {"failure_reason": "reasons"}
        run.report_outcome(config, "the/archive/path", summary, fake_ctx)
        assert m_nuke.called
        m_try_push_job_info.assert_called_with(config, summary)
        m_file.assert_called_with("the/archive/path/summary.yaml", "w")
        assert m_email_results.called
        assert m_file.called
        assert m_sys_exit.called
