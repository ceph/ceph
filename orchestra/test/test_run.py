from nose.tools import eq_ as eq
from cStringIO import StringIO

import fudge
import nose
import logging

from .. import run

from .util import assert_raises


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_log_simple():
    ssh = fudge.Fake('SSHConnection')
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo 'bar baz'")
    in_ = fudge.Fake('ChannelFile(stdin)')
    out = fudge.Fake('ChannelFile(stdout)')
    err = fudge.Fake('ChannelFile(stderr)')
    cmd.returns((in_, out, err))
    in_.expects('close').with_args()
    out.expects('xreadlines').with_args().returns(['foo', 'bar'])
    err.expects('xreadlines').with_args().returns(['bad'])
    logger = fudge.Fake('logger')
    log_err = fudge.Fake('log_err')
    logger.expects('getChild').with_args('err').returns(log_err)
    log_err.expects('log').with_args(logging.INFO, 'bad')
    log_out = fudge.Fake('log_out')
    logger.expects('getChild').with_args('out').returns(log_out)
    log_out.expects('log').with_args(logging.INFO, 'foo')
    log_out.expects('log').with_args(logging.INFO, 'bar')
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
    cmd = ssh.expects('exec_command')
    cmd.with_args("foo 'bar baz'")
    in_ = fudge.Fake('ChannelFile(stdin)')
    out = fudge.Fake('ChannelFile(stdout)')
    err = fudge.Fake('ChannelFile(stderr)')
    cmd.returns((in_, out, err))
    in_.expects('close').with_args()
    out.remember_order()
    out.expects('read').with_args().returns('foo\nb')
    out.expects('read').with_args().returns('ar\n')
    out.expects('read').with_args().returns('')
    err.expects('xreadlines').with_args().returns(['bad'])
    logger = fudge.Fake('logger')
    log_err = fudge.Fake('log_err')
    logger.expects('getChild').with_args('err').returns(log_err)
    log_err.expects('log').with_args(logging.INFO, 'bad')
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
    eq(str(e), "Command failed with status 42: 'foo'")


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_status_bad_nocheck():
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
    transport.expects('is_active').with_args().returns(True)
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
