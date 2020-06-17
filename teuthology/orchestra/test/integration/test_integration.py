from teuthology.orchestra import monkey
monkey.patch_all()

from io import StringIO

import os
from teuthology.orchestra import connection, remote, run
from teuthology.orchestra.test.util import assert_raises
from teuthology.exceptions import CommandCrashedError, ConnectionLostError

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
            CommandCrashedError,
            run.run,
            client=ssh,
            args=['sh', '-c', 'kill -ABRT $$'],
            )
        assert e.command == "sh -c 'kill -ABRT $$'"
        assert str(e) == "Command crashed: \"sh -c 'kill -ABRT $$'\""

    def test_lost(self):
        ssh = connection.connect(HOST)
        e = assert_raises(
            ConnectionLostError,
            run.run,
            client=ssh,
            args=['sh', '-c', 'kill -ABRT $PPID'],
            name=HOST,
            )
        assert e.command == "sh -c 'kill -ABRT $PPID'"
        assert str(e) == \
            "SSH connection to {host} was lost: ".format(host=HOST) + \
            "\"sh -c 'kill -ABRT $PPID'\""

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

        r.wait()
        got = r.exitstatus
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

    def test_os(self):
        rem = remote.Remote(HOST)
        assert rem.os.name
        assert rem.os.version

    def test_17102(self, caplog):
        # http://tracker.ceph.com/issues/17102
        rem = remote.Remote(HOST)
        interval = 3
        rem.run(args="echo before; sleep %s; echo after" % interval)
        for record in caplog.records:
            if record.msg == 'before':
                before_time = record.created
            elif record.msg == 'after':
                after_time = record.created
        assert int(round(after_time - before_time)) == interval
