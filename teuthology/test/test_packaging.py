import pytest

from mock import patch, Mock

from teuthology import packaging

KOJI_TASK_RPMS_MATRIX = [
    ('tasks/6745/9666745/kernel-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel'),
    ('tasks/6745/9666745/kernel-modules-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-modules'),
    ('tasks/6745/9666745/kernel-tools-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-tools'),
    ('tasks/6745/9666745/kernel-tools-libs-devel-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-tools-libs-devel'),
    ('tasks/6745/9666745/kernel-headers-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-headers'),
    ('tasks/6745/9666745/kernel-tools-debuginfo-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-tools-debuginfo'),
    ('tasks/6745/9666745/kernel-debuginfo-common-x86_64-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-debuginfo-common-x86_64'),
    ('tasks/6745/9666745/perf-debuginfo-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'perf-debuginfo'),
    ('tasks/6745/9666745/kernel-modules-extra-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-modules-extra'),
    ('tasks/6745/9666745/kernel-tools-libs-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-tools-libs'),
    ('tasks/6745/9666745/kernel-core-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-core'),
    ('tasks/6745/9666745/kernel-debuginfo-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-debuginfo'),
    ('tasks/6745/9666745/python-perf-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'python-perf'),
    ('tasks/6745/9666745/kernel-devel-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'kernel-devel'),
    ('tasks/6745/9666745/python-perf-debuginfo-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'python-perf-debuginfo'),
    ('tasks/6745/9666745/perf-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm', 'perf'),
]

KOJI_TASK_RPMS = [rpm[0] for rpm in KOJI_TASK_RPMS_MATRIX]


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

    @patch("teuthology.packaging.config")
    def test_get_koji_task_result_success(self, m_config):
        m_config.kojihub_url = "http://kojihub.com"
        m_proc = Mock()
        expected = dict(foo="bar")
        m_proc.exitstatus = 0
        m_proc.stdout.getvalue.return_value = str(expected)
        m_remote = Mock()
        m_remote.run.return_value = m_proc
        result = packaging.get_koji_task_result(1, m_remote, dict())
        assert result == expected
        args, kwargs = m_remote.run.call_args
        expected_args = [
            'python', '-c',
            'import koji; '
            'hub = koji.ClientSession("http://kojihub.com"); '
            'print hub.getTaskResult(1)',
        ]
        assert expected_args == kwargs['args']

    @patch("teuthology.packaging.config")
    def test_get_koji_task_result_fail(self, m_config):
        m_config.kojihub_url = "http://kojihub.com"
        m_proc = Mock()
        m_proc.exitstatus = 1
        m_remote = Mock()
        m_remote.run.return_value = m_proc
        m_ctx = Mock()
        m_ctx.summary = dict()
        with pytest.raises(RuntimeError):
            packaging.get_koji_task_result(1, m_remote, m_ctx)

    @patch("teuthology.packaging.config")
    def test_get_koji_task_rpm_info_success(self, m_config):
        m_config.koji_task_url = "http://kojihub.com/work"
        expected = dict(
            base_url="http://kojihub.com/work/tasks/6745/9666745/",
            version="4.1.0-0.rc2.git2.1.fc23.x86_64",
            rpm_name="kernel-4.1.0-0.rc2.git2.1.fc23.x86_64.rpm",
            package_name="kernel",
        )
        result = packaging.get_koji_task_rpm_info('kernel', KOJI_TASK_RPMS)
        assert expected == result

    @patch("teuthology.packaging.config")
    def test_get_koji_task_rpm_info_fail(self, m_config):
        m_config.koji_task_url = "http://kojihub.com/work"
        with pytest.raises(RuntimeError):
            packaging.get_koji_task_rpm_info('ceph', KOJI_TASK_RPMS)

    def test_get_package_version_deb_found(self):
        remote = Mock()
        remote.os.package_type = "deb"
        proc = Mock()
        proc.exitstatus = 0
        proc.stdout.getvalue.return_value = "2.2"
        remote.run.return_value = proc
        result = packaging.get_package_version(remote, "apache2")
        assert result == "2.2"

    def test_get_package_version_deb_command(self):
        remote = Mock()
        remote.os.package_type = "deb"
        packaging.get_package_version(remote, "apache2")
        args, kwargs = remote.run.call_args
        expected_args = ['dpkg-query', '-W', '-f', '${Version}', 'apache2']
        assert expected_args == kwargs['args']

    def test_get_package_version_rpm_found(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        proc = Mock()
        proc.exitstatus = 0
        proc.stdout.getvalue.return_value = "2.2"
        remote.run.return_value = proc
        result = packaging.get_package_version(remote, "httpd")
        assert result == "2.2"

    def test_get_package_version_rpm_command(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        packaging.get_package_version(remote, "httpd")
        args, kwargs = remote.run.call_args
        expected_args = ['rpm', '-q', 'httpd', '--qf', '%{VERSION}']
        assert expected_args == kwargs['args']

    def test_get_package_version_not_found(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        proc = Mock()
        proc.exitstatus = 1
        proc.stdout.getvalue.return_value = "not installed"
        remote.run.return_value = proc
        result = packaging.get_package_version(remote, "httpd")
        assert result is None

    def test_get_package_version_invalid_version(self):
        # this tests the possibility that the package is not found
        # but the exitstatus is still 0.  Not entirely sure we'll ever
        # hit this condition, but I want to test the codepath regardless
        remote = Mock()
        remote.os.package_type = "rpm"
        proc = Mock()
        proc.exitstatus = 0
        proc.stdout.getvalue.return_value = "not installed"
        remote.run.return_value = proc
        result = packaging.get_package_version(remote, "httpd")
        assert result is None

    @pytest.mark.parametrize("input, expected", KOJI_TASK_RPMS_MATRIX)
    def test_get_koji_task_result_package_name(self, input, expected):
        assert packaging._get_koji_task_result_package_name(input) == expected


class TestGitbuilderProject(object):

    def _get_remote(self, arch="x86_64", system_type="deb", distro="ubuntu",
                    codename="trusty", version="14.04"):
        rem = Mock()
        rem.system_type = system_type
        rem.os.name = distro
        rem.os.codename = codename
        rem.os.version = version
        rem.arch = arch

        return rem

    @patch("teuthology.packaging.config")
    @patch("teuthology.packaging._get_config_value_for_remote")
    def test_init_from_remote_base_url(self, m_get_config_value, m_config):
        m_config.baseurl_template = 'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'
        m_config.gitbuilder_host = "gitbuilder.ceph.com"
        m_get_config_value.return_value = None
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = packaging.GitbuilderProject("ceph", {}, ctx=ctx, remote=rem)
        result = gp.base_url
        expected = "http://gitbuilder.ceph.com/ceph-deb-trusty-x86_64-basic/ref/master"
        assert result == expected

    @patch("teuthology.packaging.config")
    def test_init_from_config_base_url(self, m_config):
        m_config.baseurl_template = 'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'
        m_config.gitbuilder_host = "gitbuilder.ceph.com"
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            sha1="sha1",
        )
        gp = packaging.GitbuilderProject("ceph", config)
        result = gp.base_url
        expected = "http://gitbuilder.ceph.com/ceph-deb-trusty-x86_64-basic/sha1/sha1"
        assert result == expected

    @patch("teuthology.packaging.config")
    @patch("teuthology.packaging._get_config_value_for_remote")
    @patch("requests.get")
    def test_get_package_version_found(self, m_get, m_get_config_value,
                                       m_config):
        m_config.baseurl_template = 'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'
        m_config.gitbuilder_host = "gitbuilder.ceph.com"
        m_get_config_value.return_value = None
        resp = Mock()
        resp.ok = True
        resp.text = "0.90.0"
        m_get.return_value = resp
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = packaging.GitbuilderProject("ceph", {}, ctx=ctx, remote=rem)
        assert gp.version == "0.90.0"

    @patch("teuthology.packaging.config")
    @patch("teuthology.packaging._get_config_value_for_remote")
    @patch("requests.get")
    def test_get_package_version_not_found(self, m_get, m_get_config_value,
                                           m_config):
        m_config.baseurl_template = 'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'
        m_config.gitbuilder_host = "gitbuilder.ceph.com"
        m_get_config_value.return_value = None
        resp = Mock()
        resp.ok = False
        m_get.return_value = resp
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = packaging.GitbuilderProject("ceph", {}, ctx=ctx, remote=rem)
        assert not gp.version

    GITBUILDER_DISTRO_MATRIX = [
        ('rhel', '7.0', None, 'centos7'),
        ('centos', '6.5', None, 'centos6_5'),
        ('fedora', '20', None, 'fc20'),
        ('ubuntu', '14.04', 'trusty', 'trusty'),
        ('ubuntu', '14.04', None, 'trusty'),
        ('debian', '7.0', None, 'wheezy'),
        ('debian', '7', None, 'wheezy'),
        ('ubuntu', '12.04', None, 'precise'),
        ('ubuntu', '14.04', None, 'trusty'),
    ]

    @pytest.mark.parametrize(
        "distro, version, codename, expected",
        GITBUILDER_DISTRO_MATRIX
    )
    def test_get_distro_remote(self, distro, version, codename, expected):
        rem = self._get_remote(distro=distro, version=version,
                               codename=codename)
        ctx = dict(foo="bar")
        gp = packaging.GitbuilderProject("ceph", {}, ctx=ctx, remote=rem)
        assert gp.distro == expected

    @pytest.mark.parametrize(
        "distro, version, codename, expected",
        GITBUILDER_DISTRO_MATRIX + [
            ('rhel', None, None, 'centos7'),
            ('centos', None, None, 'centos7'),
            ('fedora', None, None, 'fc20'),
            ('ubuntu', None, None, 'trusty'),
            ('debian', None, None, 'wheezy'),
        ]
    )
    def test_get_distro_config(self, distro, version, codename, expected):
        config = dict(
            os_type=distro,
            os_version=version
        )
        gp = packaging.GitbuilderProject("ceph", config)
        assert gp.distro == expected
