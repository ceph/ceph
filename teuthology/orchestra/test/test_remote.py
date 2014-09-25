import fudge
import fudge.inspector
from pytest import skip

from cStringIO import StringIO, OutputType
from textwrap import dedent

from .. import remote
from ..run import RemoteProcess


class TestRemote(object):
    def test_shortname(self):
        r = remote.Remote(
            name='jdoe@xyzzy.example.com',
            shortname='xyz',
            ssh=fudge.Fake('SSHConnection'),
            )
        assert r.shortname == 'xyz'
        assert str(r) == 'jdoe@xyzzy.example.com'

    def test_shortname_default(self):
        r = remote.Remote(
            name='jdoe@xyzzy.example.com',
            ssh=fudge.Fake('SSHConnection'),
            )
        assert r.shortname == 'xyzzy'
        assert str(r) == 'jdoe@xyzzy.example.com'

    @fudge.with_fakes
    def test_run(self):
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        ssh.expects('get_transport').returns_fake().expects('getpeername')\
            .returns(('name', 22))
        run = fudge.Fake('run')
        args = [
            'something',
            'more',
            ]
        foo = object()
        ret = RemoteProcess(
            client=ssh,
            args='fakey',
            )
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        run.expects_call().with_args(
            client=fudge.inspector.arg.passes_test(lambda v: v is ssh),
            args=fudge.inspector.arg.passes_test(lambda v: v is args),
            foo=fudge.inspector.arg.passes_test(lambda v: v is foo),
            name=r.shortname,
            ).returns(ret)
        # monkey patch ook ook
        r._runner = run
        got = r.run(
            args=args,
            foo=foo,
            )
        assert got is ret
        assert got.remote is r

    @fudge.with_fakes
    def test_hostname(self):
        skip("skipping hostname test while the workaround is in place")
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        ssh.expects('get_transport').returns_fake().expects('getpeername')\
            .returns(('name', 22))
        run = fudge.Fake('run')
        args = [
            'hostname',
            '--fqdn',
            ]
        stdout = StringIO('test_hostname')
        stdout.seek(0)
        ret = RemoteProcess(
            client=ssh,
            args='fakey',
            )
        # status = self._stdout_buf.channel.recv_exit_status()
        ret._stdout_buf = fudge.Fake()
        ret._stdout_buf.channel = fudge.Fake()
        ret._stdout_buf.channel.expects('recv_exit_status').returns(0)
        ret.stdout = stdout
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        run.expects_call().with_args(
            client=ssh,
            args=args,
            stdout=fudge.inspector.arg.passes_test(
                lambda v: isinstance(v, OutputType)),
            name=r.shortname,
            ).returns(ret)
        # monkey patch ook ook
        r._runner = run
        assert r.hostname == 'test_hostname'

    @fudge.with_fakes
    def test_arch(self):
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        ssh.expects('get_transport').returns_fake().expects('getpeername')\
            .returns(('name', 22))
        run = fudge.Fake('run')
        args = [
            'uname',
            '-p',
            ]
        stdout = StringIO('test_arch')
        stdout.seek(0)
        ret = RemoteProcess(
            client=ssh,
            args='fakey',
            )
        # status = self._stdout_buf.channel.recv_exit_status()
        ret._stdout_buf = fudge.Fake()
        ret._stdout_buf.channel = fudge.Fake()
        ret._stdout_buf.channel.expects('recv_exit_status').returns(0)
        ret.stdout = stdout
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        run.expects_call().with_args(
            client=ssh,
            args=args,
            stdout=fudge.inspector.arg.passes_test(
                lambda v: isinstance(v, OutputType)),
            name=r.shortname,
            ).returns(ret)
        # monkey patch ook ook
        r._runner = run
        assert r.arch == 'test_arch'

    @fudge.with_fakes
    def test_host_key(self):
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        key = ssh.expects('get_transport').returns_fake().expects(
            'get_remote_server_key').returns_fake()
        key.expects('get_name').returns('key_type')
        key.expects('get_base64').returns('test ssh key')
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        assert r.host_key == 'key_type test ssh key'


class TestDistribution(object):
    str_centos_7_os_release = dedent("""
        NAME="CentOS Linux"
        VERSION="7 (Core)"
        ID="centos"
        ID_LIKE="rhel fedora"
        VERSION_ID="7"
        PRETTY_NAME="CentOS Linux 7 (Core)"
        ANSI_COLOR="0;31"
        CPE_NAME="cpe:/o:centos:centos:7"
        HOME_URL="https://www.centos.org/"
        BUG_REPORT_URL="https://bugs.centos.org/"
    """)

    str_debian_7_lsb_release = dedent("""
        Distributor ID: Debian
        Description:    Debian GNU/Linux 7.1 (wheezy)
        Release:        7.1
        Codename:       wheezy
    """)

    str_debian_7_os_release = dedent("""
        PRETTY_NAME="Debian GNU/Linux 7 (wheezy)"
        NAME="Debian GNU/Linux"
        VERSION_ID="7"
        VERSION="7 (wheezy)"
        ID=debian
        ANSI_COLOR="1;31"
        HOME_URL="http://www.debian.org/"
        SUPPORT_URL="http://www.debian.org/support/"
        BUG_REPORT_URL="http://bugs.debian.org/"
    """)

    str_ubuntu_12_04_lsb_release = dedent("""
        Distributor ID: Ubuntu
        Description:    Ubuntu 12.04.4 LTS
        Release:        12.04
        Codename:       precise
    """)

    str_ubuntu_12_04_os_release = dedent("""
        NAME="Ubuntu"
        VERSION="12.04.4 LTS, Precise Pangolin"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu precise (12.04.4 LTS)"
        VERSION_ID="12.04"
    """)

    str_rhel_6_4_lsb_release = dedent("""
        LSB Version:    :base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:core-4.0-noarch:graphics-4.0-amd64:graphics-4.0-noarch:printing-4.0-amd64:printing-4.0-noarch
        Distributor ID: RedHatEnterpriseServer
        Description:    Red Hat Enterprise Linux Server release 6.4 (Santiago)
        Release:        6.4
        Codename:       Santiago
    """)

    str_rhel_7_lsb_release = dedent("""
        LSB Version:    :core-4.1-amd64:core-4.1-noarch:cxx-4.1-amd64:cxx-4.1-noarch:desktop-4.1-amd64:desktop-4.1-noarch:languages-4.1-amd64:languages-4.1-noarch:printing-4.1-amd64:printing-4.1-noarch
        Distributor ID: RedHatEnterpriseServer
        Description:    Red Hat Enterprise Linux Server release 7.0 (Maipo)
        Release:        7.0
        Codename:       Maipo
    """)

    str_rhel_7_os_release = dedent("""
        NAME="Red Hat Enterprise Linux Server"
        VERSION="7.0 (Maipo)"
        ID="rhel"
        ID_LIKE="fedora"
        VERSION_ID="7.0"
        PRETTY_NAME="Red Hat Enterprise Linux Server 7.0 (Maipo)"
        ANSI_COLOR="0;31"
        CPE_NAME="cpe:/o:redhat:enterprise_linux:7.0:GA:server"
        HOME_URL="https://www.redhat.com/"
        BUG_REPORT_URL="https://bugzilla.redhat.com/"

        REDHAT_BUGZILLA_PRODUCT="Red Hat Enterprise Linux 7"
        REDHAT_BUGZILLA_PRODUCT_VERSION=7.0
        REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux"
        REDHAT_SUPPORT_PRODUCT_VERSION=7.0
    """)

    def test_centos_7_os_release(self):
        os = remote.OS.from_os_release(self.str_centos_7_os_release)
        assert os.name == 'centos'
        assert os.version == '7'
        assert os.package_type == 'rpm'

    def test_debian_7_lsb_release(self):
        os = remote.OS.from_lsb_release(self.str_debian_7_lsb_release)
        assert os.name == 'debian'
        assert os.version == '7.1'
        assert os.package_type == 'deb'

    def test_debian_7_os_release(self):
        os = remote.OS.from_os_release(self.str_debian_7_os_release)
        assert os.name == 'debian'
        assert os.version == '7'
        assert os.package_type == 'deb'

    def test_ubuntu_12_04_lsb_release(self):
        os = remote.OS.from_lsb_release(self.str_ubuntu_12_04_lsb_release)
        assert os.name == 'ubuntu'
        assert os.version == '12.04'
        assert os.package_type == 'deb'

    def test_ubuntu_12_04_os_release(self):
        os = remote.OS.from_os_release(self.str_ubuntu_12_04_os_release)
        assert os.name == 'ubuntu'
        assert os.version == '12.04'
        assert os.package_type == 'deb'

    def test_rhel_6_4_lsb_release(self):
        os = remote.OS.from_lsb_release(self.str_rhel_6_4_lsb_release)
        assert os.name == 'rhel'
        assert os.version == '6.4'
        assert os.package_type == 'rpm'

    def test_rhel_7_lsb_release(self):
        os = remote.OS.from_lsb_release(self.str_rhel_7_lsb_release)
        assert os.name == 'rhel'
        assert os.version == '7.0'
        assert os.package_type == 'rpm'

    def test_rhel_7_os_release(self):
        os = remote.OS.from_os_release(self.str_rhel_7_os_release)
        assert os.name == 'rhel'
        assert os.version == '7.0'
        assert os.package_type == 'rpm'
