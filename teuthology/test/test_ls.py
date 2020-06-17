import pytest

from unittest.mock import patch, Mock

from teuthology import ls


class TestLs(object):
    """ Tests for teuthology.ls """

    @patch('os.path.isdir')
    @patch('os.listdir')
    def test_get_jobs(self, m_listdir, m_isdir):
        m_listdir.return_value = ["1", "a", "3"]
        m_isdir.return_value = True
        results = ls.get_jobs("some/archive/dir")
        assert results == ["1", "3"]

    @patch("yaml.safe_load_all")
    @patch("teuthology.ls.get_jobs")
    def test_ls(self, m_get_jobs,  m_safe_load_all):
        m_get_jobs.return_value = ["1", "2"]
        m_safe_load_all.return_value = [{"failure_reason": "reasons"}]
        ls.ls("some/archive/div", True)

    @patch("teuthology.ls.open")
    @patch("teuthology.ls.get_jobs")
    def test_ls_ioerror(self, m_get_jobs, m_open):
        m_get_jobs.return_value = ["1", "2"]
        m_open.side_effect = IOError()
        with pytest.raises(IOError):
            ls.ls("some/archive/dir", True)

    @patch("teuthology.ls.open")
    @patch("os.popen")
    @patch("os.path.isdir")
    @patch("os.path.isfile")
    def test_print_debug_info(self, m_isfile, m_isdir, m_popen, m_open):
        m_isfile.return_value = True
        m_isdir.return_value = True
        m_popen.return_value = Mock()
        cmdline = Mock()
        cmdline.find = Mock(return_value=0)
        m1 = Mock()
        m2 = Mock()
        m2.read = Mock(return_value=cmdline)
        m_open.side_effect = [m1, m2]
        ls.print_debug_info("the_job", "job/dir", "some/archive/dir")
