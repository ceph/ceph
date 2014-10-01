import fudge
import fudge.inspector
from pytest import skip

from cStringIO import StringIO, OutputType

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
            '-m',
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
