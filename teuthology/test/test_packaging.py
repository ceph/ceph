import pytest

from mock import patch, Mock

from teuthology import packaging


class TestPackaging(object):

    @patch("teuthology.packaging.misc")
    def test_get_package_name_deb(self, m_misc):
        m_misc.get_system_type.return_value = "deb"
        assert packaging.get_package_name('sqlite', Mock()) == "sqlite3"

    @patch("teuthology.packaging.misc")
    def test_get_package_name_rpm(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        assert packaging.get_package_name('sqlite', Mock()) is None

    @patch("teuthology.packaging.misc")
    def test_get_package_name_not_found(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        assert packaging.get_package_name('notthere', Mock()) is None

    @patch("teuthology.packaging.misc")
    def test_get_service_name_deb(self, m_misc):
        m_misc.get_system_type.return_value = "deb"
        assert packaging.get_service_name('httpd', Mock()) == 'apache2'

    @patch("teuthology.packaging.misc")
    def test_get_service_name_rpm(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        assert packaging.get_service_name('httpd', Mock()) == 'httpd'

    @patch("teuthology.packaging.misc")
    def test_get_service_name_not_found(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        assert packaging.get_service_name('notthere', Mock()) is None

    @patch("teuthology.packaging.misc")
    def test_install_package_deb(self, m_misc):
        m_misc.get_system_type.return_value = "deb"
        m_remote = Mock()
        expected = [
            'DEBIAN_FRONTEND=noninteractive',
            'sudo',
            '-E',
            'apt-get',
            '-y',
            'install',
            'apache2'
        ]
        packaging.install_package('apache2', m_remote)
        m_remote.run.assert_called_with(args=expected)

    @patch("teuthology.packaging.misc")
    def test_install_package_rpm(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        m_remote = Mock()
        expected = [
            'sudo',
            'yum',
            '-y',
            'install',
            'httpd'
        ]
        packaging.install_package('httpd', m_remote)
        m_remote.run.assert_called_with(args=expected)

    @patch("teuthology.packaging.misc")
    def test_remove_package_deb(self, m_misc):
        m_misc.get_system_type.return_value = "deb"
        m_remote = Mock()
        expected = [
            'DEBIAN_FRONTEND=noninteractive',
            'sudo',
            '-E',
            'apt-get',
            '-y',
            'purge',
            'apache2'
        ]
        packaging.remove_package('apache2', m_remote)
        m_remote.run.assert_called_with(args=expected)

    @patch("teuthology.packaging.misc")
    def test_remove_package_rpm(self, m_misc):
        m_misc.get_system_type.return_value = "rpm"
        m_remote = Mock()
        expected = [
            'sudo',
            'yum',
            '-y',
            'erase',
            'httpd'
        ]
        packaging.remove_package('httpd', m_remote)
        m_remote.run.assert_called_with(args=expected)

    def test_get_koji_package_name(self):
        build_info = dict(version="3.10.0", release="123.20.1")
        result = packaging.get_koji_package_name("kernel", build_info)
        assert result == "kernel-3.10.0-123.20.1.x86_64.rpm"

    @patch("teuthology.packaging.config")
    def test_get_kojiroot_base_url(self, m_config):
        m_config.kojiroot_url = "http://kojiroot.com"
        build_info = dict(
            package_name="kernel",
            version="3.10.0",
            release="123.20.1",
        )
        result = packaging.get_kojiroot_base_url(build_info)
        expected = "http://kojiroot.com/kernel/3.10.0/123.20.1/x86_64/"
        assert result == expected

    @patch("teuthology.packaging.config")
    def test_get_koji_build_info_success(self, m_config):
        m_config.kojihub_url = "http://kojihub.com"
        m_proc = Mock()
        expected = dict(foo="bar")
        m_proc.exitstatus = 0
        m_proc.stdout.getvalue.return_value = str(expected)
        m_remote = Mock()
        m_remote.run.return_value = m_proc
        result = packaging.get_koji_build_info(1, m_remote, dict())
        assert result == expected
        args, kwargs = m_remote.run.call_args
        expected_args = [
            'python', '-c',
            'import koji; '
            'hub = koji.ClientSession("http://kojihub.com"); '
            'print hub.getBuild(1)',
        ]
        assert expected_args == kwargs['args']

    @patch("teuthology.packaging.config")
    def test_get_koji_build_info_fail(self, m_config):
        m_config.kojihub_url = "http://kojihub.com"
        m_proc = Mock()
        m_proc.exitstatus = 1
        m_remote = Mock()
        m_remote.run.return_value = m_proc
        m_ctx = Mock()
        m_ctx.summary = dict()
        with pytest.raises(RuntimeError):
            packaging.get_koji_build_info(1, m_remote, m_ctx)
