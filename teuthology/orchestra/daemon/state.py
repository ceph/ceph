import logging
import struct

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run

log = logging.getLogger(__name__)


class DaemonState(object):
    """
    Daemon State.  A daemon exists for each instance of each role.
    """
    def __init__(self, remote, role, id_, *command_args, **command_kwargs):
        """
        Pass remote command information as parameters to remote site

        :param remote: Remote site
        :param role: Role (osd, rgw, mon, mds)
        :param id_: Id within role (osd.1, osd.2, for eaxmple)
        :param command_args: positional arguments (used in restart commands)
        :param command_kwargs: keyword arguments (used in restart commands)
        """
        self.remote = remote
        self.command_args = command_args
        self.role = role
        self.cluster, self.type_ = self.role.split('.')[0:2]
        self.id_ = id_
        self.log = command_kwargs.get('logger', log)
        self.fsid = command_kwargs.pop('fsid', None)
        self.proc = None
        self.command_kwargs = command_kwargs

    def check_status(self):
        """
        Check to see if the process has exited.

        :returns: The exit status, if any
        :raises:  CommandFailedError, if the process was run with
                  check_status=True
        """
        if self.proc:
            return self.proc.poll()

    @property
    def pid(self):
        raise NotImplementedError

    def reset(self):
        """
        clear remote run command value.
        """
        self.proc = None

    def restart(self, *args, **kwargs):
        """
        Restart with a new command passed in the arguments

        :param args: positional arguments passed to remote.run
        :param kwargs: keyword arguments passed to remote.run
        """
        self.log.info('Restarting daemon')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        cmd_args.extend(args)
        cmd_kwargs = self.command_kwargs
        cmd_kwargs.update(kwargs)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        self.log.info('Restarting daemon with args')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        # we only want to make a temporary mod of the args list
        # so we shallow copy the dict, and deepcopy the args list
        cmd_kwargs = self.command_kwargs.copy()
        from copy import deepcopy
        cmd_kwargs['args'] = deepcopy(self.command_kwargs['args'])
        cmd_kwargs['args'].extend(extra_args)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def running(self):
        """
        Are we running?
        :return: True if remote run command value is set, False otherwise.
        """
        return self.proc is not None

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote command.

        :param sig: signal to send
        """
        if self.running():
            try:
                self.proc.stdin.write(struct.pack('!b', sig))
            except IOError as e:
                log.exception('Failed to send signal %d: %s', sig, e.strerror)
            if not silent:
                self.log.info('Sent signal %d', sig)
        else:
            self.log.error('No such daemon running')

    def start(self, timeout=300):
        """
        Start this daemon instance.
        """
        if self.running():
            self.log.warn('Restarting a running daemon')
        self.restart()

    def stop(self, timeout=300):
        """
        Stop this daemon instance.

        Note: this can raise a CommandFailedError,
        CommandCrashedError, or ConnectionLostError.

        :param timeout: timeout to pass to orchestra.run.wait()
        """
        if not self.running():
            self.log.error('tried to stop a non-running daemon')
            return
        self.proc.stdin.close()
        self.log.debug('waiting for process to exit')
        try:
            run.wait([self.proc], timeout=timeout)
        except CommandFailedError:
            log.exception("Error while waiting for process to exit")
        self.proc = None
        self.log.info('Stopped')

    # FIXME why are there two wait methods?
    def wait(self, timeout=300):
        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
        self.log.debug('waiting for process to exit')
        try:
            run.wait([self.proc], timeout=timeout)
            self.log.info('Stopped')
        except:
            self.log.info('Failed')
            raise
        finally:
            self.proc = None

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        if self.proc:
            try:
                run.wait([self.proc])
            finally:
                self.proc = None
