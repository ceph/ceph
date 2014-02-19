from cStringIO import StringIO

import gevent
import gevent.event
import pipes
import logging
import shutil

log = logging.getLogger(__name__)

class RemoteProcess(object):
    __slots__ = [
        'command', 'stdin', 'stdout', 'stderr', 'exitstatus', 'exited',
        # for orchestra.remote.Remote to place a backreference
        'remote',
        ]
    def __init__(self, command, stdin, stdout, stderr, exitstatus, exited):
        self.command = command
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.exitstatus = exitstatus
        self.exited = exited

class Raw(object):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return '{cls}({value!r})'.format(
            cls=self.__class__.__name__,
            value=self.value,
            )

def quote(args):
    def _quote(args):
        for a in args:
            if isinstance(a, Raw):
                yield a.value
            else:
                yield pipes.quote(a)
    return ' '.join(_quote(args))


def execute(client, args):
    """
    Execute a command remotely.

    Caller needs to handle stdin etc.

    :param client: SSHConnection to run the command with
    :param args: command to run
    :type args: string or list of strings

    Returns a RemoteProcess, where exitstatus is a callable that will
    block until the exit status is available.
    """
    if isinstance(args, basestring):
        cmd = args
    else:
        cmd = quote(args)
    (host, port) = client.get_transport().getpeername()
    log.debug('Running [{h}]: {cmd!r}'.format(h=host, cmd=cmd))
    (in_, out, err) = client.exec_command(cmd)

    def get_exitstatus():
        status = out.channel.recv_exit_status()
        # -1 on connection loss *and* signals; map to more pythonic None
        if status == -1:
            status = None
        return status

    def exitstatus_ready():
        return out.channel.exit_status_ready()

    r = RemoteProcess(
        command=cmd,
        stdin=in_,
        stdout=out,
        stderr=err,
        # this is a callable that will block until the status is
        # available
        exitstatus=get_exitstatus,
        exited=exitstatus_ready,
        )
    return r

def copy_to_log(f, logger, host, loglevel=logging.INFO):
    # i can't seem to get fudge to fake an iterable, so using this old
    # api for now
    for line in f.xreadlines():
        line = line.rstrip()
        logger.log(loglevel, '[' + host + ']: ' + line)

def copy_and_close(src, fdst):
    if src is not None:
        if isinstance(src, basestring):
            src = StringIO(src)
        shutil.copyfileobj(src, fdst)
    fdst.close()

def copy_file_to(f, dst, host):
    if hasattr(dst, 'log'):
        # looks like a Logger to me; not using isinstance to make life
        # easier for unit tests
        handler = copy_to_log
        return handler(f, dst, host)
    else:
        handler = shutil.copyfileobj
        return handler(f, dst)


class CommandFailedError(Exception):
    def __init__(self, command, exitstatus, node=None):
        self.command = command
        self.exitstatus = exitstatus
        self.node = node

    def __str__(self):
        return "Command failed on {node} with status {status}: {command!r}".format(
            node=self.node,
            status=self.exitstatus,
            command=self.command,
            )


class CommandCrashedError(Exception):
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return "Command crashed: {command!r}".format(
            command=self.command,
            )


class ConnectionLostError(Exception):
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return "SSH connection was lost: {command!r}".format(
            command=self.command,
            )

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
        try:
            value = fn(*args, **kwargs)
        except Exception as e:
            r.set_exception(e)
        else:
            r.set(value)
    gevent.spawn(wrapper)

    return r

class Sentinel(object):
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
        self._wrapped.close()
        self._wrapped.channel.shutdown_write()

def run(
    client, args,
    stdin=None, stdout=None, stderr=None,
    logger=None,
    check_status=True,
    wait=True,
    ):
    """
    Run a command remotely.

    :param client: SSHConnection to run the command with
    :param args: command to run
    :type args: list of string
    :param stdin: Standard input to send; either a string, a file-like object, None, or `PIPE`. `PIPE` means caller is responsible for closing stdin, or command may never exit.
    :param stdout: What to do with standard output. Either a file-like object, a `logging.Logger`, `PIPE`, or `None` for copying to default log. `PIPE` means caller is responsible for reading, or command may never exit.
    :param stderr: What to do with standard error. See `stdout`.
    :param logger: If logging, write stdout/stderr to "out" and "err" children of this logger. Defaults to logger named after this module.
    :param check_status: Whether to raise CalledProcessError on non-zero exit status, and . Defaults to True. All signals and connection loss are made to look like SIGHUP.
    :param wait: Whether to wait for process to exit. If False, returned ``r.exitstatus`` s a `gevent.event.AsyncResult`, and the actual status is available via ``.get()``.
    """
    r = execute(client, args)

    r.stdin = KludgeFile(wrapped=r.stdin)

    g_in = None
    if stdin is not PIPE:
        g_in = gevent.spawn(copy_and_close, stdin, r.stdin)
        r.stdin = None
    else:
        assert not wait, "Using PIPE for stdin without wait=False would deadlock."

    if logger is None:
        logger = log
    (host,port) = client.get_transport().getpeername()
    g_err = None
    if stderr is not PIPE:
        if stderr is None:
            stderr = logger.getChild('err')
        g_err = gevent.spawn(copy_file_to, r.stderr, stderr, host)
        r.stderr = stderr
    else:
        assert not wait, "Using PIPE for stderr without wait=False would deadlock."

    g_out = None
    if stdout is not PIPE:
        if stdout is None:
            stdout = logger.getChild('out')
        g_out = gevent.spawn(copy_file_to, r.stdout, stdout, host)
        r.stdout = stdout
    else:
        assert not wait, "Using PIPE for stdout without wait=False would deadlock."

    def _check_status(status):
        if g_err is not None:
            g_err.get()
        if g_out is not None:
            g_out.get()
        if g_in is not None:
            g_in.get()

        status = status()
        if check_status:
            if status is None:
                # command either died due to a signal, or the connection
                # was lost
                transport = client.get_transport()
                if not transport.is_active():
                    # look like we lost the connection
                    raise ConnectionLostError(command=r.command)

                # connection seems healthy still, assuming it was a
                # signal; sadly SSH does not tell us which signal
                raise CommandCrashedError(command=r.command)
            if status != 0:
                (host,port) = client.get_transport().getpeername()
                raise CommandFailedError(command=r.command, exitstatus=status, node=host)
        return status

    if wait:
        r.exitstatus = _check_status(r.exitstatus)
    else:
        r.exitstatus = spawn_asyncresult(_check_status, r.exitstatus)

    return r


def wait(processes):
    """
    Wait for all given processes to exit.

    Raise if any one of them fails.
    """
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        proc.exitstatus.get()
