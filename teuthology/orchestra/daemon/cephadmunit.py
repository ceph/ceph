import logging

from teuthology.orchestra.daemon.state import DaemonState

log = logging.getLogger(__name__)

class CephadmUnit(DaemonState):
    def __init__(self, remote, role, id_, *command_args,
                 **command_kwargs):
        super(CephadmUnit, self).__init__(
            remote, role, id_, *command_args, **command_kwargs)
        self._set_commands()
        self.log = command_kwargs.get('logger', log)
        self.use_cephadm = command_kwargs.get('use_cephadm')
        self.is_started = command_kwargs.get('started', False)
        if self.is_started:
            self._start_logger()

    def name(self):
        return '%s.%s' % (self.type_, self.id_)

    def _get_systemd_cmd(self, action):
        return ' '.join([
            'sudo', 'systemctl',
            action,
            'ceph-%s@%s.%s' % (self.fsid, self.type_, self.id_),
        ])

    def _set_commands(self):
        self.start_cmd = self._get_systemd_cmd('start')
        self.stop_cmd = self._get_systemd_cmd('stop')
        self.restart_cmd = self._get_systemd_cmd('restart')
        self.show_cmd = self._get_systemd_cmd('show')
        self.status_cmd = self._get_systemd_cmd('status')

    def kill_cmd(self, sig):
        return ' '.join([
            'sudo', 'docker', 'kill',
            '-s', str(int(sig)),
            'ceph-%s-%s.%s' % (self.fsid, self.type_, self.id_),
        ])

    def _start_logger(self):
        name = '%s.%s' % (self.type_, self.id_)
        #self.log.info('_start_logger %s' % name)
        self.remote_logger = self.remote.run(
            args=['sudo', 'journalctl',
                  '-f',
                  '-n', '0',
                  '-u',
                  'ceph-%s@%s.service' % (self.fsid, name)
            ],
            logger=logging.getLogger('journalctl@' + self.cluster + '.' + name),
            label=name,
            wait=False,
            check_status=False,
        )

    def _stop_logger(self):
        name = '%s.%s' % (self.type_, self.id_)
        # this is a horrible kludge, since i don't know how else to kill
        # the journalctl process at the other end :(
        #self.log.info('_stop_logger %s running pkill' % name)
        self.remote.run(
            args=['sudo', 'pkill', '-f',
                  ' '.join(['journalctl',
                            '-f',
                            '-n', '0',
                            '-u',
                            'ceph-%s@%s.service' % (self.fsid, name)]),
            ],
            check_status=False,
        )
        #self.log.info('_stop_logger %s waiting')
        self.remote_logger.wait()
        self.remote_logger = None
        #self.log.info('_stop_logger done')

    def reset(self):
        """
        Does nothing in this implementation
        """
        pass

    def restart(self, *args, **kwargs):
        """
        Restart with a new command passed in the arguments

        :param args: positional arguments passed to remote.run
        :param kwargs: keyword arguments passed to remote.run
        """
        if not self.running():
            self.log.info('Restarting %s (starting--it wasn\'t running)...' % self.name())
            self._start_logger()
            self.remote.sh(self.start_cmd)
            self.is_started = True
        else:
            self.log.info('Restarting %s...' % self.name())
            self.remote.sh(self.restart_cmd)

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        raise NotImplementedError

    def running(self):
        """
        Are we running?
        """
        return self.is_started

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote command

        :param sig: signal to send
        """
        if not silent:
            self.log.info('Senging signal %d to %s...' % (sig, self.name()))
        self.remote.sh(self.kill_cmd(sig))

    def start(self, timeout=300):
        """
        Start this daemon instance.
        """
        if self.running():
            self.log.warn('Restarting a running daemon')
            self.restart()
            return
        self._start_logger()
        self.remote.run(self.start_cmd)

    def stop(self, timeout=300):
        """
        Stop this daemon instance.

        Note: this can raise a CommandFailedError,
        CommandCrashedError, or ConnectionLostError.

        :param timeout: timeout to pass to orchestra.run.wait()
        """
        if not self.running():
            self.log.error('Tried to stop a non-running daemon')
            return
        self.log.info('Stopping %s...' % self.name())
        self.remote.sh(self.stop_cmd)
        self.is_started = False
        self._stop_logger()
        self.log.info('Stopped %s' % self.name())

    # FIXME why are there two wait methods?
    def wait(self, timeout=300):
        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
        self.log.info('Waiting for %s to exit...' % self.name())
        self.remote.sh(self.stop_cmd)
        self.is_started = False
        self._stop_logger()
        self.log.info('Finished waiting for %s to stop' % self.name())

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        self.wait()
