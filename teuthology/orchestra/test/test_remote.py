from nose.tools import eq_ as eq

import fudge
import fudge.inspector
import nose

from .. import remote
from ..run import RemoteProcess


def test_shortname():
    r = remote.Remote(
        name='jdoe@xyzzy.example.com',
        shortname='xyz',
        ssh=fudge.Fake('SSHConnection'),
        )
    eq(r.shortname, 'xyz')
    eq(str(r), 'xyz')


def test_shortname_default():
    r = remote.Remote(
        name='jdoe@xyzzy.example.com',
        ssh=fudge.Fake('SSHConnection'),
        )
    eq(r.shortname, 'jdoe@xyzzy.example.com')
    eq(str(r), 'jdoe@xyzzy.example.com')


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run():
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
