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
    str_centos = dedent("""
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

    str_debian = dedent("""
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

    str_ubuntu = dedent("""
        NAME="Ubuntu"
        VERSION="12.04.4 LTS, Precise Pangolin"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu precise (12.04.4 LTS)"
        VERSION_ID="12.04"
    """)

    str_rhel = dedent("""
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

    def test_centos(self):
        os = remote.OS(self.str_centos)
        assert os.name == 'CentOS Linux'
        assert os.version == '7 (Core)'
        assert os.id == 'centos'
        assert os.id_like == 'rhel fedora'
        assert os.pretty_name == 'CentOS Linux 7 (Core)'
        assert os.version_id == '7'
        assert os.package_type == 'rpm'

    def test_debian(self):
        os = remote.OS(self.str_debian)
        assert os.name == 'Debian GNU/Linux'
        assert os.version == '7 (wheezy)'
        assert os.id == 'debian'
        assert os.id_like == ''
        assert os.pretty_name == 'Debian GNU/Linux 7 (wheezy)'
        assert os.version_id == '7'
        assert os.package_type == 'deb'

    def test_ubuntu(self):
        os = remote.OS(self.str_ubuntu)
        assert os.name == 'Ubuntu'
        assert os.version == '12.04.4 LTS, Precise Pangolin'
        assert os.id == 'ubuntu'
        assert os.id_like == 'debian'
        assert os.pretty_name == 'Ubuntu precise (12.04.4 LTS)'
        assert os.version_id == '12.04'
        assert os.package_type == 'deb'

    def test_rhel(self):
        os = remote.OS(self.str_rhel)
        assert os.name == 'Red Hat Enterprise Linux Server'
        assert os.version == '7.0 (Maipo)'
        assert os.id == 'rhel'
        assert os.id_like == 'fedora'
        assert os.pretty_name == 'Red Hat Enterprise Linux Server 7.0 (Maipo)'
        assert os.version_id == '7.0'
        assert os.package_type == 'rpm'
