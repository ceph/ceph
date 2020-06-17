"""
Paramiko run support
"""

import io

from paramiko import ChannelFile

import gevent
import gevent.event
import socket
import pipes
import logging
import shutil

from teuthology.contextutil import safe_while
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)

log = logging.getLogger(__name__)


class RemoteProcess(object):
    """
    An object to begin and monitor execution of a process on a remote host
    """
    __slots__ = [
        'client', 'args', 'check_status', 'command', 'hostname',
        'stdin', 'stdout', 'stderr',
        '_stdin_buf', '_stdout_buf', '_stderr_buf',
        'returncode', 'exitstatus', 'timeout',
        'greenlets',
        '_wait', 'logger',
        # for orchestra.remote.Remote to place a backreference
        'remote',
        'label',
        ]

    deadlock_warning = "Using PIPE for %s without wait=False would deadlock"

    def __init__(self, client, args, check_status=True, hostname=None,
                 label=None, timeout=None, wait=True, logger=None, cwd=None):
        """
        Create the object. Does not initiate command execution.

        :param client:       paramiko.SSHConnection to run the command with
        :param args:         Command to run.
        :type args:          String or list of strings
        :param check_status: Whether to raise CommandFailedError on non-zero
                             exit status, and . Defaults to True. All signals
                             and connection loss are made to look like SIGHUP.
        :param hostname:     Name of remote host (optional)
        :param label:        Can be used to label or describe what the
                             command is doing.
        :param timeout:      timeout value for arg that is passed to
                             exec_command of paramiko
        :param wait:         Whether self.wait() will be called automatically
        :param logger:       Alternative logger to use (optional)
        :param cwd:          Directory in which the command will be executed
                             (optional)
        """
        self.client = client
        self.args = args
        if isinstance(args, list):
            self.command = quote(args)
        else:
            self.command = args

        if cwd:
            self.command = '(cd {cwd} && exec {cmd})'.format(
                           cwd=cwd, cmd=self.command)

        self.check_status = check_status
        self.label = label
        if timeout:
            self.timeout = timeout
        if hostname:
            self.hostname = hostname
        else:
            (self.hostname, port) = client.get_transport().getpeername()[0:2]

        self.greenlets = []
        self.stdin, self.stdout, self.stderr = (None, None, None)
        self.returncode = self.exitstatus = None
        self._wait = wait
        self.logger = logger or log

    def execute(self):
        """
        Execute remote command
        """
        for line in self.command.split('\n'):
            log.getChild(self.hostname).info('%s> %s' % (self.label or '', line))

        if hasattr(self, 'timeout'):
            (self._stdin_buf, self._stdout_buf, self._stderr_buf) = \
                self.client.exec_command(self.command, timeout=self.timeout)
        else:
            (self._stdin_buf, self._stdout_buf, self._stderr_buf) = \
                self.client.exec_command(self.command)
        (self.stdin, self.stdout, self.stderr) = \
            (self._stdin_buf, self._stdout_buf, self._stderr_buf)

    def add_greenlet(self, greenlet):
        self.greenlets.append(greenlet)

    def setup_stdin(self, stream_obj):
        self.stdin = KludgeFile(wrapped=self.stdin)
        if stream_obj is not PIPE:
            greenlet = gevent.spawn(copy_and_close, stream_obj, self.stdin)
            self.add_greenlet(greenlet)
            self.stdin = None
        elif self._wait:
            # FIXME: Is this actually true?
            raise RuntimeError(self.deadlock_warning % 'stdin')

    def setup_output_stream(self, stream_obj, stream_name):
        if stream_obj is not PIPE:
            # Log the stream
            host_log = self.logger.getChild(self.hostname)
            stream_log = host_log.getChild(stream_name)
            self.add_greenlet(
                gevent.spawn(
                    copy_file_to,
                    getattr(self, stream_name),
                    stream_log,
                    stream_obj,
                )
            )
            setattr(self, stream_name, stream_obj)
        elif self._wait:
            # FIXME: Is this actually true?
            raise RuntimeError(self.deadlock_warning % stream_name)

    def wait(self):
        """
        Block until remote process finishes.

        :returns: self.returncode
        """

        status = self._get_exitstatus()
        if status != 0:
            log.debug("got remote process result: {}".format(status))
        for greenlet in self.greenlets:
            try:
                greenlet.get(block=True,timeout=60)
            except gevent.Timeout:
                log.debug("timed out waiting; will kill: {}".format(greenlet))
                greenlet.kill(block=False)
        for stream in ('stdout', 'stderr'):
            if hasattr(self, stream):
                stream_obj = getattr(self, stream)
                # Despite ChannelFile having a seek() method, it raises
                # "IOError: File does not support seeking."
                if hasattr(stream_obj, 'seek') and \
                        not isinstance(stream_obj, ChannelFile):
                    stream_obj.seek(0)

        self._raise_for_status()
        return status

    def _raise_for_status(self):
        if self.returncode is None:
            self._get_exitstatus()
        if self.check_status:
            if self.returncode in (None, -1):
                # command either died due to a signal, or the connection
                # was lost
                transport = self.client.get_transport()
                if transport is None or not transport.is_active():
                    # look like we lost the connection
                    raise ConnectionLostError(command=self.command,
                                              node=self.hostname)

                # connection seems healthy still, assuming it was a
                # signal; sadly SSH does not tell us which signal
                raise CommandCrashedError(command=self.command)
            if self.returncode != 0:
                raise CommandFailedError(
                    command=self.command, exitstatus=self.returncode,
                    node=self.hostname, label=self.label
                )

    def _get_exitstatus(self):
        """
        :returns: the remote command's exit status (return code). Note that
                  if the connection is lost, or if the process was killed by a
                  signal, this returns None instead of paramiko's -1.
        """
        status = self._stdout_buf.channel.recv_exit_status()
        self.exitstatus = self.returncode = status
        if status == -1:
            status = None
        return status

    @property
    def finished(self):
        gevent.wait(self.greenlets, timeout=0.1)
        ready = self._stdout_buf.channel.exit_status_ready()
        if ready:
            self._get_exitstatus()
        return ready

    def poll(self):
        """
        :returns: self.returncode if the process is finished; else None
        """
        if self.finished:
            self._raise_for_status()
            return self.returncode
        return None

    def __repr__(self):
        return '{classname}(client={client!r}, args={args!r}, check_status={check}, hostname={name!r})'.format(  # noqa
            classname=self.__class__.__name__,
            client=self.client,
            args=self.args,
            check=self.check_status,
            name=self.hostname,
            )


class Raw(object):

    """
    Raw objects are passed to remote objects and are not processed locally.
    """
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return '{cls}({value!r})'.format(
            cls=self.__class__.__name__,
            value=self.value,
            )

    def __eq__(self, value):
        return self.value == value


def quote(args):
    """
    Internal quote wrapper.
    """
    def _quote(args):
        """
        Handle quoted string, testing for raw charaters.
        """
        for a in args:
            if isinstance(a, Raw):
                yield a.value
            else:
                yield pipes.quote(a)
    if isinstance(args, list):
        return ' '.join(_quote(args))
    else:
        return args


def copy_to_log(f, logger, loglevel=logging.INFO, capture=None):
    """
    Copy line by line from file in f to the log from logger

    :param f: source stream object
    :param logger: the destination logger object
    :param loglevel: the level of logging data
    :param capture: an optional stream object for data copy
    """
    # Work-around for http://tracker.ceph.com/issues/8313
    if isinstance(f, ChannelFile):
        f._flags += ChannelFile.FLAG_BINARY
    for line in f:
        if capture:
            if isinstance(capture, io.StringIO):
                if isinstance(line, str):
                    capture.write(line)
                else:
                    capture.write(line.decode('utf-8', 'replace'))
            elif isinstance(capture, io.BytesIO):
                if isinstance(line, str):
                    capture.write(line.encode())
                else:
                    capture.write(line)
        line = line.rstrip()
        # Second part of work-around for http://tracker.ceph.com/issues/8313
        try:
            if isinstance(line, bytes):
                line = line.decode('utf-8', 'replace')
            logger.log(loglevel, line)
        except (UnicodeDecodeError, UnicodeEncodeError):
            logger.exception("Encountered unprintable line in command output")


def copy_and_close(src, fdst):
    """
    copyfileobj call wrapper.
    """
    if src is not None:
        if isinstance(src, bytes):
            src = io.BytesIO(src)
        elif isinstance(src, str):
            src = io.StringIO(src)
        shutil.copyfileobj(src, fdst)
    fdst.close()


def copy_file_to(src, logger, stream=None):
    """
    Copy file
    :param src: file to be copied.
    :param logger: the logger object
    :param stream: an optional file-like object which will receive a copy of
                   src.
    """
    copy_to_log(src, logger, capture=stream)

def spawn_asyncresult(fn, *args, **kwargs):
    """
    Spawn a Greenlet and pass it's results to an AsyncResult.

    This function is useful to shuffle data from a Greenlet to
    AsyncResult, which then again is useful because any Greenlets that
    raise exceptions will cause tracebacks to be shown on stderr by
    gevent, even when ``.link_exception`` has been called. Using an
    AsyncResult avoids this.
    """
    r = gevent.event.AsyncResult()

    def wrapper():
        """
        Internal wrapper.
        """
        try:
            value = fn(*args, **kwargs)
        except Exception as e:
            r.set_exception(e)
        else:
            r.set(value)
    gevent.spawn(wrapper)

    return r


class Sentinel(object):

    """
    Sentinel -- used to define PIPE file-like object.
    """
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

PIPE = Sentinel('PIPE')


class KludgeFile(object):

    """
    Wrap Paramiko's ChannelFile in a way that lets ``f.close()``
    actually cause an EOF for the remote command.
    """
    def __init__(self, wrapped):
        self._wrapped = wrapped

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def close(self):
        """
        Close and shutdown.
        """
        self._wrapped.close()
        self._wrapped.channel.shutdown_write()


def run(
    client, args,
    stdin=None, stdout=None, stderr=None,
    logger=None,
    check_status=True,
    wait=True,
    name=None,
    label=None,
    timeout=None,
    cwd=None,
    # omit_sudo is used by vstart_runner.py
    omit_sudo=False
):
    """
    Run a command remotely.  If any of 'args' contains shell metacharacters
    that you want to pass unquoted, pass it as an instance of Raw(); otherwise
    it will be quoted with pipes.quote() (single quote, and single quotes
    enclosed in double quotes).

    :param client: SSHConnection to run the command with
    :param args: command to run
    :type args: list of string
    :param stdin: Standard input to send; either a string, a file-like object,
                  None, or `PIPE`. `PIPE` means caller is responsible for
                  closing stdin, or command may never exit.
    :param stdout: What to do with standard output. Either a file-like object,
                   a `logging.Logger`, `PIPE`, or `None` for copying to default
                   log. `PIPE` means caller is responsible for reading, or
                   command may never exit.
    :param stderr: What to do with standard error. See `stdout`.
    :param logger: If logging, write stdout/stderr to "out" and "err" children
                   of this logger. Defaults to logger named after this module.
    :param check_status: Whether to raise CommandFailedError on non-zero exit
                         status, and . Defaults to True. All signals and
                         connection loss are made to look like SIGHUP.
    :param wait: Whether to wait for process to exit. If False, returned
                 ``r.exitstatus`` s a `gevent.event.AsyncResult`, and the
                 actual status is available via ``.get()``.
    :param name: Human readable name (probably hostname) of the destination
                 host
    :param label: Can be used to label or describe what the command is doing.
    :param timeout: timeout value for args to complete on remote channel of
                    paramiko
    :param cwd: Directory in which the command should be executed.
    """
    try:
        transport = client.get_transport()
        if transport:
            (host, port) = transport.getpeername()[0:2]
        else:
            raise ConnectionLostError(command=quote(args), node=name)
    except socket.error:
        raise ConnectionLostError(command=quote(args), node=name)

    if name is None:
        name = host

    if timeout:
        log.info("Running command with timeout %d", timeout)
    r = RemoteProcess(client, args, check_status=check_status, hostname=name,
                      label=label, timeout=timeout, wait=wait, logger=logger,
                      cwd=cwd)
    r.execute()
    r.setup_stdin(stdin)
    r.setup_output_stream(stderr, 'stderr')
    r.setup_output_stream(stdout, 'stdout')
    if wait:
        r.wait()
    return r


def wait(processes, timeout=None):
    """
    Wait for all given processes to exit.

    Raise if any one of them fails.

    Optionally, timeout after 'timeout' seconds.
    """
    if timeout:
        log.info("waiting for %d", timeout)
    if timeout and timeout > 0:
        with safe_while(tries=(timeout // 6)) as check_time:
            not_ready = list(processes)
            while len(not_ready) > 0:
                check_time()
                for proc in list(not_ready):
                    if proc.finished:
                        not_ready.remove(proc)

    for proc in processes:
        proc.wait()
