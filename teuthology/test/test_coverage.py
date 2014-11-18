from mock import patch

from teuthology import coverage


class TestCoverage(object):

    @patch('teuthology.log.setLevel')
    @patch('teuthology.setup_log_file')
    @patch('teuthology.coverage.analyze')
    def test_main(self, m_analyze, m_setup_log_file, m_setLevel):
        args = {
            "--skip-init": False,
            "--lcov-output": "some/other/dir",
            "--html-output": "html/output/dir",
            "--cov-tools-dir": "cov/tools/dir",
            "--verbose": True,
            "<test_dir>": "some/test/dir",
        }
        coverage.main(args)
        assert m_setLevel.called
        m_setup_log_file.assert_called_with("some/test/dir/coverage.log")
        m_analyze.assert_called_with(
            "some/test/dir",
            "cov/tools/dir",
            "some/other/dir",
            "html/output/dir",
            False
        )
