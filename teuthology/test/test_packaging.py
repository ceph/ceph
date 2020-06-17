import pytest

from unittest.mock import patch, Mock

from teuthology import packaging
from teuthology.exceptions import VersionNotFoundError

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

    def test_get_package_name_deb(self):
        remote = Mock()
        remote.os.package_type = "deb"
        assert packaging.get_package_name('sqlite', remote) == "sqlite3"

    def test_get_package_name_rpm(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        assert packaging.get_package_name('sqlite', remote) is None

    def test_get_package_name_not_found(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        assert packaging.get_package_name('notthere', remote) is None

    def test_get_service_name_deb(self):
        remote = Mock()
        remote.os.package_type = "deb"
        assert packaging.get_service_name('httpd', remote) == 'apache2'

    def test_get_service_name_rpm(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        assert packaging.get_service_name('httpd', remote) == 'httpd'

    def test_get_service_name_not_found(self):
        remote = Mock()
        remote.os.package_type = "rpm"
        assert packaging.get_service_name('notthere', remote) is None

    def test_install_package_deb(self):
        m_remote = Mock()
        m_remote.os.package_type = "deb"
        expected = [
            'DEBIAN_FRONTEND=noninteractive',
            'sudo',
            '-E',
            'apt-get',
            '-y',
            '--force-yes',
            'install',
            'apache2'
        ]
        packaging.install_package('apache2', m_remote)
        m_remote.run.assert_called_with(args=expected)

    def test_install_package_rpm(self):
        m_remote = Mock()
        m_remote.os.package_type = "rpm"
        expected = [
            'sudo',
            'yum',
            '-y',
            'install',
            'httpd'
        ]
        packaging.install_package('httpd', m_remote)
        m_remote.run.assert_called_with(args=expected)

    def test_remove_package_deb(self):
        m_remote = Mock()
        m_remote.os.package_type = "deb"
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

    def test_remove_package_rpm(self):
        m_remote = Mock()
        m_remote.os.package_type = "rpm"
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
            'print(hub.getBuild(1))',
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
            'print(hub.getTaskResult(1))',
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
        expected_args = ['rpm', '-q', 'httpd', '--qf', '%{VERSION}-%{RELEASE}']
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

    @patch("requests.get")
    def test_get_response_success(self, m_get):
        resp = Mock()
        resp.ok = True
        m_get.return_value = resp
        result = packaging._get_response("google.com")
        assert result == resp

    @patch("requests.get")
    def test_get_response_failed_wait(self, m_get):
        resp = Mock()
        resp.ok = False
        m_get.return_value = resp
        packaging._get_response("google.com", wait=True, sleep=1, tries=2)
        assert m_get.call_count == 2

    @patch("requests.get")
    def test_get_response_failed_no_wait(self, m_get):
        resp = Mock()
        resp.ok = False
        m_get.return_value = resp
        packaging._get_response("google.com", sleep=1, tries=2)
        assert m_get.call_count == 1


class TestBuilderProject(object):
    klass = None

    def setup(self):
        if self.klass is None:
            pytest.skip()

    def _get_remote(self, arch="x86_64", system_type="deb", distro="ubuntu",
                    codename="trusty", version="14.04"):
        rem = Mock()
        rem.system_type = system_type
        rem.os.name = distro
        rem.os.codename = codename
        rem.os.version = version
        rem.arch = arch

        return rem

    def test_init_from_remote_base_url(self, expected=None):
        assert expected is not None
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        result = gp.base_url
        assert result == expected

    def test_init_from_remote_base_url_debian(self, expected=None):
        assert expected is not None
        # remote.os.codename returns and empty string on debian
        rem = self._get_remote(distro="debian", codename='', version="7.1")
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        result = gp.base_url
        assert result == expected

    def test_init_from_config_base_url(self, expected=None):
        assert expected is not None
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            sha1="sha1",
        )
        gp = self.klass("ceph", config)
        result = gp.base_url
        print(self.m_get.call_args_list)
        assert result == expected

    def test_init_from_config_branch_ref(self):
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            branch='jewel',
        )
        gp = self.klass("ceph", config)
        result = gp.uri_reference
        expected = 'ref/jewel'
        assert result == expected

    def test_init_from_config_tag_ref(self):
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            tag='v10.0.1',
        )
        gp = self.klass("ceph", config)
        result = gp.uri_reference
        expected = 'ref/v10.0.1'
        assert result == expected

    def test_init_from_config_tag_overrides_branch_ref(self, caplog):
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            branch='jewel',
            tag='v10.0.1',
        )
        gp = self.klass("ceph", config)
        result = gp.uri_reference
        expected = 'ref/v10.0.1'
        assert result == expected
        expected_log = 'More than one of ref, tag, branch, or sha1 supplied; using tag'
        assert expected_log in caplog.text
        return gp

    def test_init_from_config_branch_overrides_sha1(self, caplog):
        config = dict(
            os_type="ubuntu",
            os_version="14.04",
            branch='jewel',
            sha1='sha1',
        )
        gp = self.klass("ceph", config)
        result = gp.uri_reference
        expected = 'ref/jewel'
        assert result == expected
        expected_log = 'More than one of ref, tag, branch, or sha1 supplied; using branch'
        assert expected_log in caplog.text
        return gp

    REFERENCE_MATRIX = [
        ('the_ref', 'the_tag', 'the_branch', 'the_sha1', dict(ref='the_ref')),
        (None, 'the_tag', 'the_branch', 'the_sha1', dict(tag='the_tag')),
        (None, None, 'the_branch', 'the_sha1', dict(branch='the_branch')),
        (None, None, None, 'the_sha1', dict(sha1='the_sha1')),
        (None, None, 'the_branch', None, dict(branch='the_branch')),
    ]

    @pytest.mark.parametrize(
        "ref, tag, branch, sha1, expected",
        REFERENCE_MATRIX,
    )
    def test_choose_reference(self, ref, tag, branch, sha1, expected):
        config = dict(
            os_type='ubuntu',
            os_version='18.04',
        )
        if ref:
            config['ref'] = ref
        if tag:
            config['tag'] = tag
        if branch:
            config['branch'] = branch
        if sha1:
            config['sha1'] = sha1
        gp = self.klass("ceph", config)
        assert gp._choose_reference() == expected

    def test_get_package_version_found(self):
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        assert gp.version == "0.90.0"

    @patch("teuthology.packaging._get_response")
    def test_get_package_version_not_found(self, m_get_response):
        rem = self._get_remote()
        ctx = dict(foo="bar")
        resp = Mock()
        resp.ok = False
        m_get_response.return_value = resp
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        with pytest.raises(VersionNotFoundError):
            gp.version

    def test_get_package_sha1_fetched_found(self):
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        assert gp.sha1 == "the_sha1"

    def test_get_package_sha1_fetched_not_found(self):
        rem = self._get_remote()
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        assert not gp.sha1

    DISTRO_MATRIX = [None] * 12

    @pytest.mark.parametrize(
        "matrix_index",
        range(len(DISTRO_MATRIX)),
    )
    def test_get_distro_remote(self, matrix_index):
        (distro, version, codename, expected) = \
            self.DISTRO_MATRIX[matrix_index]
        rem = self._get_remote(distro=distro, version=version,
                               codename=codename)
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        assert gp.distro == expected

    DISTRO_MATRIX_NOVER = [
        ('rhel', None, None, 'centos7'),
        ('centos', None, None, 'centos7'),
        ('fedora', None, None, 'fedora25'),
        ('ubuntu', None, None, 'bionic'),
        ('debian', None, None, 'jessie'),
    ]

    @pytest.mark.parametrize(
        "matrix_index",
        range(len(DISTRO_MATRIX) + len(DISTRO_MATRIX_NOVER)),
    )
    def test_get_distro_config(self, matrix_index):
        (distro, version, codename, expected) = \
            (self.DISTRO_MATRIX + self.DISTRO_MATRIX_NOVER)[matrix_index]
        config = dict(
            os_type=distro,
            os_version=version
        )
        gp = self.klass("ceph", config)
        assert gp.distro == expected

    DIST_RELEASE_MATRIX = [
        ('rhel', '7.0', None, 'el7'),
        ('centos', '6.5', None, 'el6'),
        ('centos', '7.0', None, 'el7'),
        ('centos', '7.1', None, 'el7'),
        ('fedora', '20', None, 'fc20'),
        ('debian', '7.0', None, 'debian'),
        ('debian', '7', None, 'debian'),
        ('debian', '7.1', None, 'debian'),
        ('ubuntu', '12.04', None, 'ubuntu'),
        ('ubuntu', '14.04', None, 'ubuntu'),
    ]

    @pytest.mark.parametrize(
        "matrix_index",
        range(len(DIST_RELEASE_MATRIX)),
    )
    def test_get_dist_release(self, matrix_index):
        (distro, version, codename, expected) = \
            (self.DIST_RELEASE_MATRIX)[matrix_index]
        rem = self._get_remote(distro=distro, version=version,
                               codename=codename)
        ctx = dict(foo="bar")
        gp = self.klass("ceph", {}, ctx=ctx, remote=rem)
        assert gp.dist_release == expected


class TestGitbuilderProject(TestBuilderProject):
    klass = packaging.GitbuilderProject

    def setup(self):
        self.p_config = patch('teuthology.packaging.config')
        self.m_config = self.p_config.start()
        self.m_config.baseurl_template = \
            'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'
        self.m_config.gitbuilder_host = "gitbuilder.ceph.com"
        self.p_get_config_value = \
            patch('teuthology.packaging._get_config_value_for_remote')
        self.m_get_config_value = self.p_get_config_value.start()
        self.m_get_config_value.return_value = None
        self.p_get = patch('requests.get')
        self.m_get = self.p_get.start()
        self.p_get_response = patch("teuthology.packaging._get_response")
        self.m_get_response = self.p_get_response.start()

    def teardown(self):
        self.p_config.stop()
        self.p_get_config_value.stop()
        self.p_get.stop()
        self.p_get_response.stop()

    def test_init_from_remote_base_url(self, expected=None):
        super(TestGitbuilderProject, self)\
            .test_init_from_remote_base_url(
                "http://gitbuilder.ceph.com/"
                "ceph-deb-trusty-x86_64-basic/ref/master"
            )

    def test_init_from_remote_base_url_debian(self):
        super(TestGitbuilderProject, self)\
            .test_init_from_remote_base_url_debian(
                "http://gitbuilder.ceph.com/"
                "ceph-deb-wheezy-x86_64-basic/ref/master"
        )

    def test_init_from_config_base_url(self):
        super(TestGitbuilderProject, self).test_init_from_config_base_url(
            "http://gitbuilder.ceph.com/ceph-deb-trusty-x86_64-basic/sha1/sha1"
        )

    def test_get_package_version_found(self):
        resp = Mock()
        resp.ok = True
        resp.text = "0.90.0"
        self.m_get_response.return_value = resp
        super(TestGitbuilderProject, self)\
            .test_get_package_version_found()

    def test_version_strip_v(self):
        resp = Mock()
        resp.ok = True
        resp.text = "v0.90.0"
        self.m_get_response.return_value = resp
        super(TestGitbuilderProject, self)\
            .test_get_package_version_found()

    def test_get_package_sha1_fetched_found(self):
        resp = Mock()
        resp.ok = True
        resp.text = "the_sha1"
        self.m_get.return_value = resp
        super(TestGitbuilderProject, self)\
            .test_get_package_sha1_fetched_found()

    def test_get_package_sha1_fetched_not_found(self):
        resp = Mock()
        resp.ok = False
        self.m_get.return_value = resp
        super(TestGitbuilderProject, self)\
            .test_get_package_sha1_fetched_not_found()

    DISTRO_MATRIX = [
        ('rhel', '7.0', None, 'centos7'),
        ('centos', '6.5', None, 'centos6'),
        ('centos', '7.0', None, 'centos7'),
        ('centos', '7.1', None, 'centos7'),
        ('fedora', '20', None, 'fedora20'),
        ('ubuntu', '14.04', 'trusty', 'trusty'),
        ('ubuntu', '14.04', None, 'trusty'),
        ('debian', '7.0', None, 'wheezy'),
        ('debian', '7', None, 'wheezy'),
        ('debian', '7.1', None, 'wheezy'),
        ('ubuntu', '12.04', None, 'precise'),
        ('ubuntu', '14.04', None, 'trusty'),
    ]


class TestShamanProject(TestBuilderProject):
    klass = packaging.ShamanProject

    def setup(self):
        self.p_config = patch('teuthology.packaging.config')
        self.m_config = self.p_config.start()
        self.m_config.use_shaman = True
        self.m_config.shaman_host = 'shaman.ceph.com'
        self.p_get_config_value = \
            patch('teuthology.packaging._get_config_value_for_remote')
        self.m_get_config_value = self.p_get_config_value.start()
        self.m_get_config_value.return_value = None
        self.p_get = patch('requests.get')
        self.m_get = self.p_get.start()

    def teardown(self):
        self.p_config.stop()
        self.p_get_config_value.stop()
        self.p_get.stop()

    def test_init_from_remote_base_url(self):
        # Here, we really just need to make sure ShamanProject._search()
        # queries the right URL. So let's make _get_base_url() just pass that
        # URL through and test that value.
        def m_get_base_url(obj):
            obj._search()
            return self.m_get.call_args_list[0][0][0]
        with patch(
            'teuthology.packaging.ShamanProject._get_base_url',
            new=m_get_base_url,
        ):
            super(TestShamanProject, self)\
                .test_init_from_remote_base_url(
                    "https://shaman.ceph.com/api/search?status=ready"
                    "&project=ceph&flavor=default"
                    "&distros=ubuntu%2F14.04%2Fx86_64&ref=master"
                )

    def test_init_from_remote_base_url_debian(self):
        # Here, we really just need to make sure ShamanProject._search()
        # queries the right URL. So let's make _get_base_url() just pass that
        # URL through and test that value.
        def m_get_base_url(obj):
            obj._search()
            return self.m_get.call_args_list[0][0][0]
        with patch(
            'teuthology.packaging.ShamanProject._get_base_url',
            new=m_get_base_url,
        ):
            super(TestShamanProject, self)\
                .test_init_from_remote_base_url_debian(
                    "https://shaman.ceph.com/api/search?status=ready"
                    "&project=ceph&flavor=default"
                    "&distros=debian%2F7.1%2Fx86_64&ref=master"
                )

    def test_init_from_config_base_url(self):
        # Here, we really just need to make sure ShamanProject._search()
        # queries the right URL. So let's make _get_base_url() just pass that
        # URL through and test that value.
        def m_get_base_url(obj):
            obj._search()
            return self.m_get.call_args_list[0][0][0]
        with patch(
            'teuthology.packaging.ShamanProject._get_base_url',
            new=m_get_base_url,
        ):
            super(TestShamanProject, self).test_init_from_config_base_url(
                "https://shaman.ceph.com/api/search?status=ready&project=ceph" \
                "&flavor=default&distros=ubuntu%2F14.04%2Fx86_64&sha1=sha1"
            )

    @patch('teuthology.packaging.ShamanProject._get_package_sha1')
    def test_init_from_config_tag_ref(self, m_get_package_sha1):
        m_get_package_sha1.return_value = 'the_sha1'
        super(TestShamanProject, self).test_init_from_config_tag_ref()

    def test_init_from_config_tag_overrides_branch_ref(self, caplog):
        with patch(
            'teuthology.packaging.repo_utils.ls_remote',
        ) as m_ls_remote:
            m_ls_remote.return_value = 'sha1_from_my_tag'
            obj = super(TestShamanProject, self)\
                .test_init_from_config_tag_overrides_branch_ref(caplog)
            search_uri = obj._search_uri
        assert 'sha1=sha1_from_my_tag' in search_uri
        assert 'jewel' not in search_uri

    def test_init_from_config_branch_overrides_sha1(self, caplog):
        obj = super(TestShamanProject, self)\
            .test_init_from_config_branch_overrides_sha1(caplog)
        search_uri = obj._search_uri
        assert 'jewel' in search_uri
        assert 'sha1' not in search_uri

    def test_get_package_version_found(self):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = [
            dict(
                sha1='the_sha1',
                extra=dict(package_manager_version='0.90.0'),
            )
        ]
        self.m_get.return_value = resp
        super(TestShamanProject, self)\
            .test_get_package_version_found()

    def test_get_package_sha1_fetched_found(self):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = [dict(sha1='the_sha1')]
        self.m_get.return_value = resp
        super(TestShamanProject, self)\
            .test_get_package_sha1_fetched_found()

    def test_get_package_sha1_fetched_not_found(self):
        resp = Mock()
        resp.json.return_value = []
        self.m_get.return_value = resp
        super(TestShamanProject, self)\
            .test_get_package_sha1_fetched_not_found()

    DISTRO_MATRIX = [
        ('rhel', '7.0', None, 'centos/7'),
        ('centos', '6.5', None, 'centos/6'),
        ('centos', '7.0', None, 'centos/7'),
        ('centos', '7.1', None, 'centos/7'),
        ('fedora', '20', None, 'fedora/20'),
        ('ubuntu', '14.04', 'trusty', 'ubuntu/14.04'),
        ('ubuntu', '14.04', None, 'ubuntu/14.04'),
        ('debian', '7.0', None, 'debian/7.0'),
        ('debian', '7', None, 'debian/7'),
        ('debian', '7.1', None, 'debian/7.1'),
        ('ubuntu', '12.04', None, 'ubuntu/12.04'),
        ('ubuntu', '14.04', None, 'ubuntu/14.04'),
    ]

    DISTRO_MATRIX_NOVER = [
        ('rhel', None, None, 'centos/7'),
        ('centos', None, None, 'centos/7'),
        ('fedora', None, None, 'fedora/25'),
        ('ubuntu', None, None, 'ubuntu/18.04'),
        ('debian', None, None, 'debian/8.0'),
    ]
