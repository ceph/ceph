from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read
import subprocess
from select import select
from ceph_volume import terminal

import logging

logger = logging.getLogger(__name__)


def log_output(descriptor, message, terminal_logging):
    """
    log output to both the logger and the terminal if terminal_logging is
    enabled
    """
    if not message:
        return
    message = message.strip()
    line = '%s %s' % (descriptor, message)
    if terminal_logging:
        getattr(terminal, descriptor)(message)
    logger.info(line)


def log_descriptors(reads, process, terminal_logging):
    """
    Helper to send output to the terminal while polling the subprocess
    """
    # these fcntl are set to O_NONBLOCK for the filedescriptors coming from
    # subprocess so that the logging does not block. Without these a prompt in
    # a subprocess output would hang and nothing would get printed. Note how
    # these are just set when logging subprocess, not globally.
    stdout_flags = fcntl(process.stdout, F_GETFL) # get current p.stdout flags
    stderr_flags = fcntl(process.stderr, F_GETFL) # get current p.stderr flags
    fcntl(process.stdout, F_SETFL, stdout_flags | O_NONBLOCK)
    fcntl(process.stderr, F_SETFL, stderr_flags | O_NONBLOCK)
    descriptor_names = {
        process.stdout.fileno(): 'stdout',
        process.stderr.fileno(): 'stderr'
    }
    for descriptor in reads:
        descriptor_name = descriptor_names[descriptor]
        try:
            log_output(descriptor_name, read(descriptor, 1024), terminal_logging)
        except (IOError, OSError):
            # nothing else to log
            pass


def run(command, **kw):
    """
    A real-time-logging implementation of a remote subprocess.Popen call where
    a command is just executed on the remote end and no other handling is done.

    :param command: The command to pass in to the remote subprocess.Popen as a list
    :param stop_on_error: If a nonzero exit status is return, it raises a ``RuntimeError``
    """
    stop_on_error = kw.pop('stop_on_error', True)
    command_msg = "Running command: %s" % ' '.join(command)
    logger.info(command_msg)
    terminal.write(command_msg)
    terminal_logging = kw.get('terminal_logging', True)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        **kw
    )

    while True:
        reads, _, _ = select(
            [process.stdout.fileno(), process.stderr.fileno()],
            [], []
        )
        log_descriptors(reads, process, terminal_logging)

        if process.poll() is not None:
            # ensure we do not have anything pending in stdout or stderr
            log_descriptors(reads, process, terminal_logging)

            break

    returncode = process.wait()
    if returncode != 0:
        msg = "command returned non-zero exit status: %s" % returncode
        if stop_on_error:
            raise RuntimeError(msg)
        else:
            if terminal_logging:
                terminal.warning(msg)
            logger.warning(msg)


def call(command, **kw):
    """
    Similar to ``subprocess.Popen`` with the following changes:

    * returns stdout, stderr, and exit code (vs. just the exit code)
    * logs the full contents of stderr and stdout (separately) to the file log

    By default, no terminal output is given, not even the command that is going
    to run.

    Useful when system calls are needed to act on output, and that same output
    shouldn't get displayed on the terminal.

    :param terminal_logging: Log command to terminal, defaults to False
    """
    terminal_logging = kw.pop('terminal_logging', False)
    command_msg = "Running command: %s" % ' '.join(command)
    logger.info(command_msg)
    if terminal_logging:
        terminal.write(command_msg)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        **kw
    )

    returncode = process.wait()
    stdout_stream = process.stdout.read()
    stderr_stream = process.stderr.read()
    if not isinstance(stdout_stream, str):
        stdout_stream = stdout_stream.decode('utf-8')
    if not isinstance(stderr_stream, str):
        stderr_stream = stderr_stream.decode('utf-8')
    stdout = stdout_stream.splitlines()
    stderr = stderr_stream.splitlines()

    # the following can get a messed up order in the log if the system call
    # returns output with both stderr and stdout intermingled. This separates
    # that.
    for line in stdout:
        log_output('stdout', line, False)
    for line in stderr:
        log_output('stderr', line, False)
    return stdout, stderr, returncode
