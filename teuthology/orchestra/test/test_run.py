from cStringIO import StringIO

import socket

from mock import MagicMock, patch
from pytest import raises

from .. import run
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)


class TestRun(object):
    def setup(self):
        self.start_patchers()

    def teardown(self):
        self.stop_patchers()

    def start_patchers(self):
        self.m_remote_process = MagicMock(wraps=run.RemoteProcess)
        self.patcher_remote_proc = patch(
            'teuthology.orchestra.run.RemoteProcess',
            self.m_remote_process,
        )
        self.m_channelfile = MagicMock(spec=run.ChannelFile)
        self.m_ssh = MagicMock()
        self.m_stdin_buf = self.m_channelfile()
        self.m_stdout_buf = self.m_channelfile()
        self.m_stderr_buf = self.m_channelfile()
        self.m_ssh.exec_command.return_value = (
            self.m_stdin_buf,
            self.m_stdout_buf,
            self.m_stderr_buf,
        )
        self.m_transport = MagicMock()
        self.m_transport.getpeername.return_value = ('name', 22)
        self.m_ssh.get_transport.return_value = self.m_transport
        self.patcher_ssh = patch(
            'teuthology.orchestra.connection.paramiko.SSHClient',
            self.m_ssh,
        )
        self.patcher_ssh.start()
        # Tests must start this if they wish to use it
        # self.patcher_remote_proc.start()

    def stop_patchers(self):
        # If this patcher wasn't started, it's ok
        try:
            self.patcher_remote_proc.stop()
        except RuntimeError:
            pass
        self.patcher_ssh.stop()

    def test_exitstatus(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        proc = run.run(
            client=self.m_ssh,
            args=['foo', 'bar baz'],
        )
        assert proc.exitstatus == 0

    def test_capture_stdout(self):
        output = 'foo\nbar'

        def m_copyfileobj(src, dest):
            print output
            print dest
            dest.write(output)

        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        with patch(
            'teuthology.orchestra.run.shutil.copyfileobj',
            m_copyfileobj,
        ):
            proc = run.run(
                client=self.m_ssh,
                args=['foo', 'bar baz'],
                stdout=StringIO(),
            )
        assert proc.stdout.getvalue() == output

    def test_status_bad(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        with raises(CommandFailedError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "Command failed on name with status 42: 'foo'"

    def test_status_bad_nocheck(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus == 42

    def test_status_crash(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        with raises(CommandCrashedError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "Command crashed: 'foo'"

    def test_status_crash_nocheck(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus is None

    def test_status_lost(self):
        m_transport = MagicMock()
        m_transport.getpeername.return_value = ('name', 22)
        m_transport.is_active.return_value = False
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        self.m_ssh.get_transport.return_value = m_transport
        with raises(ConnectionLostError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "SSH connection to name was lost: 'foo'"

    def test_status_lost_socket(self):
        m_transport = MagicMock()
        m_transport.getpeername.side_effect = socket.error
        self.m_ssh.get_transport.return_value = m_transport
        with raises(ConnectionLostError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "SSH connection was lost: 'foo'"

    def test_status_lost_nocheck(self):
        m_transport = MagicMock()
        m_transport.getpeername.return_value = ('name', 22)
        m_transport.is_active.return_value = False
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        self.m_ssh.get_transport.return_value = m_transport
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus is None

    def test_status_bad_nowait(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            wait=False,
        )
        with raises(CommandFailedError) as exc:
            proc.wait()
        assert proc.returncode == 42
        assert str(exc.value) == "Command failed on name with status 42: 'foo'"

    def test_stdin_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stdin=run.PIPE,
            wait=False
        )
        assert proc.poll() is None
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0

    def test_stdout_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        self.m_stdout_buf.read.side_effect = [
            'one', 'two', '',
        ]
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stdout=run.PIPE,
            wait=False
        )
        assert proc.poll() is None
        assert proc.stdout.read() == 'one'
        assert proc.stdout.read() == 'two'
        assert proc.stdout.read() == ''
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0

    def test_stderr_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        self.m_stderr_buf.read.side_effect = [
            'one', 'two', '',
        ]
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stderr=run.PIPE,
            wait=False
        )
        assert proc.poll() is None
        assert proc.stderr.read() == 'one'
        assert proc.stderr.read() == 'two'
        assert proc.stderr.read() == ''
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0


class TestQuote(object):
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


class TestRaw(object):
    def test_eq(self):
        str_ = "I am a raw something or other"
        raw = run.Raw(str_)
        assert raw == run.Raw(str_)
