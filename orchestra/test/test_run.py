from nose.tools import eq_ as eq

import fudge
import nose
import logging

from .. import run


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
    channel.expects('recv_exit_status').with_args().returns(42)
    got = run.run(
        client=ssh,
        logger=logger,
        args=['foo', 'bar baz'],
        )
    eq(got, 42)


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_crash_status():
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
    got = run.run(
        client=ssh,
        logger=logger,
        args=['foo'],
        )
    assert got is None
