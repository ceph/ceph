import fudge
import fudge.inspector

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
        assert str(r) == 'xyz'

    def test_shortname_default(self):
        r = remote.Remote(
            name='jdoe@xyzzy.example.com',
            ssh=fudge.Fake('SSHConnection'),
            )
        assert r.shortname == 'jdoe@xyzzy.example.com'
        assert str(r) == 'jdoe@xyzzy.example.com'

    @fudge.with_fakes
    def test_run(self):
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        run = fudge.Fake('run')
        args = [
            'something',
            'more',
            ]
        foo = object()
        ret = RemoteProcess(
            command='fakey',
            stdin=None,
            stdout=None,
            stderr=None,
            exitstatus=None,
            exited=None,
            )
        run.expects_call().with_args(
            client=fudge.inspector.arg.passes_test(lambda v: v is ssh),
            args=fudge.inspector.arg.passes_test(lambda v: v is args),
            foo=fudge.inspector.arg.passes_test(lambda v: v is foo),
            ).returns(ret)
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        # monkey patch ook ook
        r._runner = run
        got = r.run(
            args=args,
            foo=foo,
            )
        assert got is ret
        assert got.remote is r
