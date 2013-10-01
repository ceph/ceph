from .. import monkey
monkey.patch_all()

from cStringIO import StringIO

import os
from .. import connection, run
from .util import assert_raises

from pytest import skip

HOST = None


class TestIntegration():
    def setup(self):
        try:
            host = os.environ['ORCHESTRA_TEST_HOST']
        except KeyError:
            skip('To run integration tests, set environment ' +
                 'variable ORCHESTRA_TEST_HOST to user@host to use.')
        global HOST
        HOST = host

    def test_crash(self):
        ssh = connection.connect(HOST)
        e = assert_raises(
            run.CommandCrashedError,
            run.run,
            client=ssh,
            args=['sh', '-c', 'kill -ABRT $$'],
            )
        assert e.command == "sh -c 'kill -ABRT $$'"
        assert str(e) == "Command crashed: \"sh -c 'kill -ABRT $$'\""

    def test_lost(self):
        ssh = connection.connect(HOST)
        e = assert_raises(
            run.ConnectionLostError,
            run.run,
            client=ssh,
            args=['sh', '-c', 'kill -ABRT $PPID'],
            )
        assert e.command == "sh -c 'kill -ABRT $PPID'"
        assert str(e) == \
            "SSH connection was lost: \"sh -c 'kill -ABRT $PPID'\""

    def test_pipe(self):
        ssh = connection.connect(HOST)
        r = run.run(
            client=ssh,
            args=['cat'],
            stdin=run.PIPE,
            stdout=StringIO(),
            wait=False,
            )
        assert r.stdout.getvalue() == ''
        r.stdin.write('foo\n')
        r.stdin.write('bar\n')
        r.stdin.close()

        got = r.exitstatus.get()
        assert got == 0
        assert r.stdout.getvalue() == 'foo\nbar\n'

    def test_and(self):
        ssh = connection.connect(HOST)
        r = run.run(
            client=ssh,
            args=['true', run.Raw('&&'), 'echo', 'yup'],
            stdout=StringIO(),
            )
        assert r.stdout.getvalue() == 'yup\n'
