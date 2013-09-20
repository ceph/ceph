from nose.tools import eq_ as eq
from cStringIO import StringIO

import fudge
import gevent.event
import nose
import logging

from .. import run

from .util import assert_raises


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_log_simple():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo 'bar baz'")
    in_ = fudge.Fake('ChannelFile(stdin)')
    out = fudge.Fake('ChannelFile(stdout)')
    err = fudge.Fake('ChannelFile(stderr)')
    cmd.returns((in_, out, err))
    in_.expects('close').with_args()
    in_chan = fudge.Fake('channel')
    in_chan.expects('shutdown_write').with_args()
    in_.has_attr(channel=in_chan)
    out.expects('xreadlines').with_args().returns(['foo', 'bar'])
    err.expects('xreadlines').with_args().returns(['bad'])
    logger = fudge.Fake('logger')
    log_err = fudge.Fake('log_err')
    logger.expects('getChild').with_args('err').returns(log_err)
    log_err.expects('log').with_args(logging.INFO, '[HOST]: bad')
    log_out = fudge.Fake('log_out')
    logger.expects('getChild').with_args('out').returns(log_out)
    log_out.expects('log').with_args(logging.INFO, '[HOST]: foo')
    log_out.expects('log').with_args(logging.INFO, '[HOST]: bar')
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(0)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo', 'bar baz'],
        )
    eq(r.exitstatus, 0)


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_capture_stdout():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo 'bar baz'")
    in_ = fudge.Fake('ChannelFile(stdin)')
    out = fudge.Fake('ChannelFile(stdout)')
    err = fudge.Fake('ChannelFile(stderr)')
    cmd.returns((in_, out, err))
    in_.expects('close').with_args()
    in_chan = fudge.Fake('channel')
    in_chan.expects('shutdown_write').with_args()
    in_.has_attr(channel=in_chan)
    out.remember_order()
    out.expects('read').with_args().returns('foo\nb')
    out.expects('read').with_args().returns('ar\n')
    out.expects('read').with_args().returns('')
    err.expects('xreadlines').with_args().returns(['bad'])
    logger = fudge.Fake('logger')
    log_err = fudge.Fake('log_err')
    logger.expects('getChild').with_args('err').returns(log_err)
    log_err.expects('log').with_args(logging.INFO, '[HOST]: bad')
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(0)
    out_f = StringIO()
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo', 'bar baz'],
        stdout=out_f,
        )
    eq(r.exitstatus, 0)
    assert r.stdout is out_f
    eq(r.stdout.getvalue(), 'foo\nbar\n')


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_bad():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(42)
    e = assert_raises(
        run.CommandFailedError,
        run.run,
        client=ssh,
        logger=logger,
        args=['foo'],
        )
    eq(e.command, 'foo')
    eq(e.exitstatus, 42)
    eq(str(e), "Command failed on HOST with status 42: 'foo'")


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_bad_nocheck():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(42)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        check_status=False,
        )
    eq(r.exitstatus, 42)


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_crash():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    transport.expects('is_active').with_args().returns(True)
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(-1)
    e = assert_raises(
        run.CommandCrashedError,
        run.run,
        client=ssh,
        logger=logger,
        args=['foo'],
        )
    eq(e.command, 'foo')
    eq(str(e), "Command crashed: 'foo'")


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_crash_nocheck():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(-1)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        check_status=False,
        )
    assert r.exitstatus is None


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_lost():
    ssh = fudge.Fake('SSHConnection')
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(-1)
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    transport.expects('is_active').with_args().returns(False)
    e = assert_raises(
        run.ConnectionLostError,
        run.run,
        client=ssh,
        logger=logger,
        args=['foo'],
        )

    eq(e.command, 'foo')
    eq(str(e), "SSH connection was lost: 'foo'")


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_lost_nocheck():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(-1)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        check_status=False,
        )
    assert r.exitstatus is None


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_nowait():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(42)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        wait=False,
        )
    eq(r.command, 'foo')
    assert isinstance(r.exitstatus, gevent.event.AsyncResult)
    e = assert_raises(
        run.CommandFailedError,
        r.exitstatus.get,
        )
    eq(e.exitstatus, 42)
    eq(str(e), "Command failed on HOST with status 42: 'foo'")


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_stdin_pipe():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(0)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        stdin=run.PIPE,
        wait=False,
        )
    r.stdin.write('bar')
    eq(r.command, 'foo')
    assert isinstance(r.exitstatus, gevent.event.AsyncResult)
    eq(r.exitstatus.ready(), False)
    got = r.exitstatus.get()
    eq(got, 0)


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_stdout_pipe():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('read').with_args().returns('one')
    out.expects('read').with_args().returns('two')
    out.expects('read').with_args().returns('')
    err.expects('xreadlines').with_args().returns([])
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(0)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        stdout=run.PIPE,
        wait=False,
        )
    eq(r.command, 'foo')
    assert isinstance(r.exitstatus, gevent.event.AsyncResult)
    eq(r.exitstatus.ready(), False)
    eq(r.stdout.read(), 'one')
    eq(r.stdout.read(), 'two')
    eq(r.stdout.read(), '')
    got = r.exitstatus.get()
    eq(got, 0)


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_stderr_pipe():
    ssh = fudge.Fake('SSHConnection')
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.expects('getpeername').with_args().returns(('HOST', 22))
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo")
    in_ = fudge.Fake('ChannelFile').is_a_stub()
    out = fudge.Fake('ChannelFile').is_a_stub()
    err = fudge.Fake('ChannelFile').is_a_stub()
    cmd.returns((in_, out, err))
    out.expects('xreadlines').with_args().returns([])
    err.expects('read').with_args().returns('one')
    err.expects('read').with_args().returns('two')
    err.expects('read').with_args().returns('')
    logger = fudge.Fake('logger').is_a_stub()
    channel = fudge.Fake('channel')
    out.has_attr(channel=channel)
    channel.expects('recv_exit_status').with_args().returns(0)
    r = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        stderr=run.PIPE,
        wait=False,
        )
    eq(r.command, 'foo')
    assert isinstance(r.exitstatus, gevent.event.AsyncResult)
    eq(r.exitstatus.ready(), False)
    eq(r.stderr.read(), 'one')
    eq(r.stderr.read(), 'two')
    eq(r.stderr.read(), '')
    got = r.exitstatus.get()
    eq(got, 0)


def test_quote_simple():
    got = run.quote(['a b', ' c', 'd e '])
    eq(got, "'a b' ' c' 'd e '")

def test_quote_and_quote():
    got = run.quote(['echo', 'this && is embedded', '&&', 'that was standalone'])
    eq(got, "echo 'this && is embedded' '&&' 'that was standalone'")

def test_quote_and_raw():
    got = run.quote(['true', run.Raw('&&'), 'echo', 'yay'])
    eq(got, "true && echo yay")
