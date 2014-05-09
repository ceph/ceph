from cStringIO import StringIO

import fudge
import gevent.event
import logging

from .. import run

from .util import assert_raises


class TestRun(object):
    @fudge.with_fakes
    def test_run_log_simple(self):
        fudge.clear_expectations()
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
        log_host = fudge.Fake('log_host')
        logger.expects('getChild').with_args('HOST').returns(log_host)
        log_err = fudge.Fake('log_err')
        log_host.expects('getChild').with_args('stderr').returns(log_err)
        log_err.expects('log').with_args(logging.INFO, 'bad')
        log_out = fudge.Fake('log_out')
        log_host.expects('getChild').with_args('stdout').returns(log_out)
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
        assert r.exitstatus == 0

    @fudge.with_fakes
    def test_run_capture_stdout(self):
        fudge.clear_expectations()
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
        log_host = fudge.Fake('log_host')
        logger.expects('getChild').with_args('HOST').returns(log_host)
        log_err = fudge.Fake('log_err')
        log_host.expects('getChild').with_args('stderr').returns(log_err)
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
        assert r.exitstatus == 0
        assert r.stdout is out_f
        assert r.stdout.getvalue() == 'foo\nbar\n'

    @fudge.with_fakes
    def test_run_status_bad(self):
        fudge.clear_expectations()
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
        assert e.command == 'foo'
        assert e.exitstatus == 42
        assert str(e) == "Command failed on HOST with status 42: 'foo'"

    @fudge.with_fakes
    def test_run_status_bad_nocheck(self):
        fudge.clear_expectations()
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
        assert r.exitstatus == 42

    @fudge.with_fakes
    def test_run_status_crash(self):
        fudge.clear_expectations()
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
        assert e.command == 'foo'
        assert str(e) == "Command crashed: 'foo'"

    @fudge.with_fakes
    def test_run_status_crash_nocheck(self):
        fudge.clear_expectations()
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

    @fudge.with_fakes
    def test_run_status_lost(self):
        fudge.clear_expectations()
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

        assert e.command == 'foo'
        assert str(e) == "SSH connection was lost: 'foo'"

    @fudge.with_fakes
    def test_run_status_lost_nocheck(self):
        fudge.clear_expectations()
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

    @fudge.with_fakes
    def test_run_nowait(self):
        fudge.clear_expectations()
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
        assert r.command == 'foo'
        assert isinstance(r.exitstatus, gevent.event.AsyncResult)
        e = assert_raises(
            run.CommandFailedError,
            r.exitstatus.get,
            )
        assert e.exitstatus == 42
        assert str(e) == "Command failed on HOST with status 42: 'foo'"

    @fudge.with_fakes
    def test_run_stdin_pipe(self):
        fudge.clear_expectations()
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
        assert r.command == 'foo'
        assert isinstance(r.exitstatus, gevent.event.AsyncResult)
        assert r.exitstatus.ready() == False
        got = r.exitstatus.get()
        assert got == 0

    @fudge.with_fakes
    def test_run_stdout_pipe(self):
        fudge.clear_expectations()
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
        assert r.command == 'foo'
        assert isinstance(r.exitstatus, gevent.event.AsyncResult)
        assert r.exitstatus.ready() == False
        assert r.stdout.read() == 'one'
        assert r.stdout.read() == 'two'
        assert r.stdout.read() == ''
        got = r.exitstatus.get()
        assert got == 0

    @fudge.with_fakes
    def test_run_stderr_pipe(self):
        fudge.clear_expectations()
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
        assert r.command == 'foo'
        assert isinstance(r.exitstatus, gevent.event.AsyncResult)
        assert r.exitstatus.ready() is False
        assert r.stderr.read() == 'one'
        assert r.stderr.read() == 'two'
        assert r.stderr.read() == ''
        got = r.exitstatus.get()
        assert got == 0

    def test_quote_simple(self):
        got = run.quote(['a b', ' c', 'd e '])
        assert got == "'a b' ' c' 'd e '"

    def test_quote_and_quote(self):
        got = run.quote(['echo', 'this && is embedded', '&&',
                         'that was standalone'])
        assert got == "echo 'this && is embedded' '&&' 'that was standalone'"

    def test_quote_and_raw(self):
        got = run.quote(['true', run.Raw('&&'), 'echo', 'yay'])
        assert got == "true && echo yay"
