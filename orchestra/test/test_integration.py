from .. import monkey; monkey.patch_all()

from nose.tools import eq_ as eq

import os
import nose

from .. import connection, run

from .util import assert_raises

def setup():
    try:
        host = os.environ['ORCHESTRA_TEST_HOST']
    except KeyError:
        raise nose.SkipTest(
            'To run integration tests, set environment '
            + 'variable ORCHESTRA_TEST_HOST to user@host to use.',
            )
    global HOST
    HOST = host

def test_crash():
    ssh = connection.connect(HOST)
    e = assert_raises(
        run.CommandCrashedError,
        run.run,
        client=ssh,
        args=['sh', '-c', 'kill -ABRT $$'],
        )
    eq(e.command, "sh -c 'kill -ABRT $$'")
    eq(str(e), "Command crashed: \"sh -c 'kill -ABRT $$'\"")

def test_lost():
    ssh = connection.connect(HOST)
    e = assert_raises(
        run.ConnectionLostError,
        run.run,
        client=ssh,
        args=['sh', '-c', 'kill -ABRT $PPID'],
        )
    eq(e.command, "sh -c 'kill -ABRT $PPID'")
    eq(str(e), "SSH connection was lost: \"sh -c 'kill -ABRT $PPID'\"")
