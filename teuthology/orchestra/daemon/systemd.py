import logging
import re
from cStringIO import StringIO

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology.orchestra.daemon.state import DaemonState

log = logging.getLogger(__name__)

systemd_cmd_templ = 'sudo systemctl {action} {daemon}@{id_}'


def get_systemd_cmd(action, daemon, id_):
    if daemon == 'rgw':
        daemon = 'radosgw'
        id_ = 'rgw.%s' % id_
    daemon = 'ceph-%s' % daemon
    cmd = systemd_cmd_templ.format(
        action=action,
        daemon=daemon,
        id_=id_,
    )
    return cmd


class SystemDState(DaemonState):
    def __init__(self, remote, role, id_, *command_args,
                 **command_kwargs):
        super(SystemDState, self).__init__(
                remote, role, id_, *command_args, **command_kwargs)
        self.log = command_kwargs.get('logger', log)
        self._set_commands()

    def _set_commands(self):
        self.start_cmd = get_systemd_cmd('start', self.type_, self.id_)
        self.stop_cmd = get_systemd_cmd('stop', self.type_, self.id_)
        self.restart_cmd = get_systemd_cmd('restart', self.type_, self.id_)
        self.show_cmd = get_systemd_cmd('show', self.type_, self.id_)
        self.status_cmd = get_systemd_cmd('status', self.type_, self.id_)
        self.output_cmd = 'sudo journalctl -u ' \
            '{role}@{id_} -t {role} -n 10'.format(
                role=self.role.replace('.', '-'), id_=self.id_,
            )

    def check_status(self):
        """
        Check to see if the process has exited.

        :returns: The exit status, if any
        :raises:  CommandFailedError, if the process was run with
                  check_status=True
        """
        proc = self.remote.run(
            args=self.show_cmd + ' | grep -i state',
            stdout=StringIO(),
        )

        def parse_line(line):
            key, value = line.strip().split('=', 1)
            return {key.strip(): value.strip()}
        show_dict = dict()
        for line in proc.stdout.readlines():
            show_dict.update(parse_line(line))
        active_state = show_dict['ActiveState']
        sub_state = show_dict['SubState']
        if active_state == 'active':
            return None
        self.log.info("State is: %s/%s", active_state, sub_state)
        proc = self.remote.run(
            # This will match a line like:
            #    Main PID: 13394 (code=exited, status=1/FAILURE)
            # Or (this is wrapped):
            #    Apr 26 21:29:33 ovh083 systemd[1]: ceph-osd@1.service:
            #    Main process exited, code=exited, status=1/FAILURE
            args=self.status_cmd + " | grep 'Main.*code=exited'",
            stdout=StringIO(),
        )
        line = proc.stdout.readlines()[-1]
        exit_code = int(re.match('.*status=(\d+).*', line).groups()[0])
        if exit_code:
            self.remote.run(
                args=self.output_cmd
            )
            raise CommandFailedError(
                self.start_cmd,
                exit_code,
                self.remote,
            )
        return exit_code

    @property
    def pid(self):
        proc_name = 'ceph-%s' % self.type_
        proc_regex = '"%s.*--id %s"' % (proc_name, self.id_)
        args = ['ps', '-ef',
                run.Raw('|'),
                'grep',
                run.Raw(proc_regex),
                run.Raw('|'),
                'grep', '-v',
                'grep', run.Raw('|'),
                'awk',
                run.Raw("{'print $2'}")]
        proc = self.remote.run(args=args, stdout=StringIO())
        pid_string = proc.stdout.getvalue().strip()
        if not pid_string.isdigit():
            return None
        return int(pid_string)


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
        self.log.info('Restarting daemon using systemd')
        if not self.running():
            self.log.info('starting a non-running daemon')
            self.remote.run(args=[run.Raw(self.start_cmd)])
        else:
            self.remote.run(args=[run.Raw(self.restart_cmd)])

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        self.log.warn(
                "restart_with_args() is not supported with systemd; performing"
                "normal restart")
        self.restart()

    def running(self):
        """
        Are we running?
        :return: The PID if remote run command value is set, False otherwise.
        """
        pid = self.pid
        if pid > 0:
            return pid
        else:
            return None

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote command

        :param sig: signal to send
        """
        self.log.warn("systemd may restart daemons automatically")
        pid = self.pid
        self.log.info("Sending signal %s to process %s", sig, pid)
        sig = '-' + str(sig)
        self.remote.run(args=['sudo', 'kill', str(sig), str(pid)])

    def start(self, timeout=300):
        """
        Start this daemon instance.
        """
        if self.running():
            self.log.warn('Restarting a running daemon')
            self.restart()
            return
        self.remote.run(args=[run.Raw(self.start_cmd)])

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
        self.remote.run(args=[run.Raw(self.stop_cmd)])
        self.log.info('Stopped')

    # FIXME why are there two wait methods?
    def wait(self, timeout=300):
        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
        self.log.error("wait() not suported in systemd")

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        # TODO: This ought to be possible, no?
        self.log.error("wait_for_exit() is not supported with systemd")