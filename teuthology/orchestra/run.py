"""
Paramiko run support
"""
from cStringIO import StringIO
from paramiko import ChannelFile

import gevent
import gevent.event
import socket
import pipes
import logging
import shutil

from ..contextutil import safe_while
from ..exceptions import (CommandCrashedError, CommandFailedError,
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
        # for orchestra.remote.Remote to place a backreference
        'remote',
        'label',
        ]

    def __init__(self, client, args, check_status=True, hostname=None, label=None, timeout=None):
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
        """
        self.client = client
        self.args = args
        if isinstance(args, basestring):
            self.command = args
        else:
            self.command = quote(args)

        self.check_status = check_status
        self.label = label
        if timeout:
            self.timeout = timeout
        if hostname:
            self.hostname = hostname
        else:
            (self.hostname, port) = client.get_transport().getpeername()

        self.greenlets = []
        self.stdin, self.stdout, self.stderr = (None, None, None)
        self.returncode = self.exitstatus = None

    def execute(self):
        """
        Execute remote command
        """
        prefix = "Running:"
        if self.label:
            prefix = "Running ({label}):".format(label=self.label)
        log.getChild(self.hostname).info(u"{prefix} {cmd!r}".format(
            cmd=self.command, prefix=prefix))

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

    def wait(self):
        """
        Block until remote process finishes.

        :returns: self.returncode
        """
        for greenlet in self.greenlets:
            greenlet.get()

        status = self._get_exitstatus()
        self.exitstatus = self.returncode = status
        if self.check_status:
            if status is None:
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
            if status != 0:
                raise CommandFailedError(command=self.command,
                                         exitstatus=status, node=self.hostname,
                                         label=self.label)
        return status

    def _get_exitstatus(self):
        """
        :returns: the remote command's exit status (return code). Note that
                  if the connection is lost, or if the process was killed by a
                  signal, this returns None instead of paramiko's -1.
        """
        status = self._stdout_buf.channel.recv_exit_status()
        if status == -1:
            status = None
        return status

    @property
    def finished(self):
        return self._stdout_buf.channel.exit_status_ready()

    def poll(self):
        """
        :returns: self.returncode if the process is finished; else None
        """
        if self.finished:
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
    if isinstance(args, basestring):
        return args

    def _quote(args):
        """
        Handle quoted string, testing for raw charaters.
        """
        for a in args:
            if isinstance(a, Raw):
                yield a.value
            else:
                yield pipes.quote(a)
    return ' '.join(_quote(args))


def copy_to_log(f, logger, loglevel=logging.INFO):
    """
    Interface to older xreadlines api.
    """
    # Work-around for http://tracker.ceph.com/issues/8313
    if isinstance(f, ChannelFile):
        f._flags += ChannelFile.FLAG_BINARY

    # i can't seem to get fudge to fake an iterable, so using this old
    # api for now
    for line in f.xreadlines():
        line = line.rstrip()
        # Second part of work-around for http://tracker.ceph.com/issues/8313
        try:
            line = unicode(line, 'utf-8', 'replace').encode('utf-8')
            logger.log(loglevel, line.decode('utf-8'))
        except (UnicodeDecodeError, UnicodeEncodeError):
            logger.exception("Encountered unprintable line in command output")


def copy_and_close(src, fdst):
    """
    copyfileobj call wrapper.
    """
    if src is not None:
        if isinstance(src, basestring):
            src = StringIO(src)
        shutil.copyfileobj(src, fdst)
    fdst.close()


def copy_file_to(f, dst):
    """
    Copy file
    :param f: file to be copied.
    :param dst: destination
    :param host: original host location
    """
    if hasattr(dst, 'log'):
        # looks like a Logger to me; not using isinstance to make life
        # easier for unit tests
        handler = copy_to_log
    else:
        handler = shutil.copyfileobj
    return handler(f, dst)


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
    """
    try:
        (host, port) = client.get_transport().getpeername()
    except socket.error:
        raise ConnectionLostError(command=quote(args), node=name)

    if name is None:
        name = host

    if timeout:
        log.info("Running command with timeout %d", timeout)
    r = RemoteProcess(client, args, check_status=check_status, hostname=name,
                      label=label, timeout=timeout)
    r.execute()

    r.stdin = KludgeFile(wrapped=r.stdin)

    g_in = None
    if stdin is not PIPE:
        g_in = gevent.spawn(copy_and_close, stdin, r.stdin)
        r.add_greenlet(g_in)
        r.stdin = None
    else:
        assert not wait, \
            "Using PIPE for stdin without wait=False would deadlock."

    if logger is None:
        logger = log

    g_err = None
    if stderr is not PIPE:
        if stderr is None:
            stderr = logger.getChild(name).getChild('stderr')
        g_err = gevent.spawn(copy_file_to, r.stderr, stderr)
        r.add_greenlet(g_err)
        r.stderr = stderr
    else:
        assert not wait, \
            "Using PIPE for stderr without wait=False would deadlock."

    g_out = None
    if stdout is not PIPE:
        if stdout is None:
            stdout = logger.getChild(name).getChild('stdout')
        g_out = gevent.spawn(copy_file_to, r.stdout, stdout)
        r.add_greenlet(g_out)
        r.stdout = stdout
    else:
        assert not wait, \
            "Using PIPE for stdout without wait=False would deadlock."

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
        with safe_while(tries=(timeout / 6)) as check_time:
            not_ready = list(processes)
            while len(not_ready) > 0:
                check_time()
                for proc in list(not_ready):
                    if proc.finished:
                        not_ready.remove(proc)

    for proc in processes:
        proc.wait()
