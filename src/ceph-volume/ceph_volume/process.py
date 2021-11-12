from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read, path
import subprocess
from select import select
from ceph_volume import terminal
from ceph_volume.util import as_bytes

import logging

logger = logging.getLogger(__name__)
host_rootfs = '/rootfs'
run_host_cmd = [
        'nsenter',
        '--root={}'.format(host_rootfs),
        '--mount={}/proc/1/ns/mnt'.format(host_rootfs),
        '--ipc={}/proc/1/ns/ipc'.format(host_rootfs),
        '--net={}/proc/1/ns/net'.format(host_rootfs),
        '--uts={}/proc/1/ns/uts'.format(host_rootfs)
]

def which(executable):
    """
    Proxy function to ceph_volume.util.system.which because the ``system``
    module does import ``process``
    """
    from ceph_volume.util import system
    return system.which(executable)


def log_output(descriptor, message, terminal_logging, logfile_logging):
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
    if logfile_logging:
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
            message = read(descriptor, 1024)
            if not isinstance(message, str):
                message = message.decode('utf-8')
            log_output(descriptor_name, message, terminal_logging, True)
        except (IOError, OSError):
            # nothing else to log
            pass


def obfuscate(command_, on=None):
    """
    Certain commands that are useful to log might contain information that
    should be replaced by '*' like when creating OSDs and the keyrings are
    being passed, which should not be logged.

    :param on: A string (will match a flag) or an integer (will match an index)

    If matching on a flag (when ``on`` is a string) it will obfuscate on the
    value for that flag. That is a command like ['ls', '-l', '/'] that calls
    `obfuscate(command, on='-l')` will obfustace '/' which is the value for
    `-l`.

    The reason for `on` to allow either a string or an integer, altering
    behavior for both is because it is easier for ``run`` and ``call`` to just
    pop a value to obfuscate (vs. allowing an index or a flag)
    """
    command = command_[:]
    msg = "Running command: %s" % ' '.join(command)
    if on in [None, False]:
        return msg

    if isinstance(on, int):
        index = on

    else:
        try:
            index = command.index(on) + 1
        except ValueError:
            # if the flag just doesn't exist then it doesn't matter just return
            # the base msg
            return msg

    try:
        command[index] = '*' * len(command[index])
    except IndexError: # the index was completely out of range
        return msg

    return "Running command: %s" % ' '.join(command)


def run(command, run_on_host=False, **kw):
    """
    A real-time-logging implementation of a remote subprocess.Popen call where
    a command is just executed on the remote end and no other handling is done.

    :param command: The command to pass in to the remote subprocess.Popen as a list
    :param stop_on_error: If a nonzero exit status is return, it raises a ``RuntimeError``
    :param fail_msg: If a nonzero exit status is returned this message will be included in the log
    """
    executable = which(command.pop(0))
    command.insert(0, executable)
    if run_on_host and path.isdir(host_rootfs):
        command = run_host_cmd + command
    stop_on_error = kw.pop('stop_on_error', True)
    command_msg = obfuscate(command, kw.pop('obfuscate', None))
    fail_msg = kw.pop('fail_msg', None)
    logger.info(command_msg)
    terminal.write(command_msg)
    terminal_logging = kw.pop('terminal_logging', True)

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
        if fail_msg:
            logger.warning(fail_msg)
            if terminal_logging:
                terminal.warning(fail_msg)
        if stop_on_error:
            raise RuntimeError(msg)
        else:
            if terminal_logging:
                terminal.warning(msg)
            logger.warning(msg)


def call(command, run_on_host=False, **kw):
    """
    Similar to ``subprocess.Popen`` with the following changes:

    * returns stdout, stderr, and exit code (vs. just the exit code)
    * logs the full contents of stderr and stdout (separately) to the file log

    By default, no terminal output is given, not even the command that is going
    to run.

    Useful when system calls are needed to act on output, and that same output
    shouldn't get displayed on the terminal.

    Optionally, the command can be displayed on the terminal and the log file,
    and log file output can be turned off. This is useful to prevent sensitive
    output going to stderr/stdout and being captured on a log file.

    :param terminal_verbose: Log command output to terminal, defaults to False, and
                             it is forcefully set to True if a return code is non-zero
    :param logfile_verbose: Log stderr/stdout output to log file. Defaults to True
    :param verbose_on_failure: On a non-zero exit status, it will forcefully set logging ON for
                               the terminal. Defaults to True
    """
    executable = which(command.pop(0))
    command.insert(0, executable)
    if run_on_host and path.isdir(host_rootfs):
        command = run_host_cmd + command
    terminal_verbose = kw.pop('terminal_verbose', False)
    logfile_verbose = kw.pop('logfile_verbose', True)
    verbose_on_failure = kw.pop('verbose_on_failure', True)
    show_command = kw.pop('show_command', False)
    command_msg = "Running command: %s" % ' '.join(command)
    stdin = kw.pop('stdin', None)
    logger.info(command_msg)
    if show_command:
        terminal.write(command_msg)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        close_fds=True,
        **kw
    )

    if stdin:
        stdout_stream, stderr_stream = process.communicate(as_bytes(stdin))
    else:
        stdout_stream = process.stdout.read()
        stderr_stream = process.stderr.read()
    returncode = process.wait()
    if not isinstance(stdout_stream, str):
        stdout_stream = stdout_stream.decode('utf-8')
    if not isinstance(stderr_stream, str):
        stderr_stream = stderr_stream.decode('utf-8')
    stdout = stdout_stream.splitlines()
    stderr = stderr_stream.splitlines()

    if returncode != 0:
        # set to true so that we can log the stderr/stdout that callers would
        # do anyway as long as verbose_on_failure is set (defaults to True)
        if verbose_on_failure:
            terminal_verbose = True
        # logfiles aren't disruptive visually, unlike the terminal, so this
        # should always be on when there is a failure
        logfile_verbose = True

    # the following can get a messed up order in the log if the system call
    # returns output with both stderr and stdout intermingled. This separates
    # that.
    for line in stdout:
        log_output('stdout', line, terminal_verbose, logfile_verbose)
    for line in stderr:
        log_output('stderr', line, terminal_verbose, logfile_verbose)
    return stdout, stderr, returncode
