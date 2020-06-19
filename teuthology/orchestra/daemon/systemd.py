import logging
import re

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology.orchestra.daemon.state import DaemonState

log = logging.getLogger(__name__)

systemd_cmd_templ = 'sudo systemctl {action} {daemon}@{id_}'


class SystemDState(DaemonState):
    def __init__(self, remote, role, id_, *command_args,
                 **command_kwargs):
        super(SystemDState, self).__init__(
            remote, role, id_, *command_args, **command_kwargs)
        self._set_commands()
        self.log = command_kwargs.get('logger', log)

    @property
    def daemon_type(self):
        if self.type_ == 'rgw':
            return 'radosgw'
        return self.type_

    def _get_systemd_cmd(self, action):
        cmd = systemd_cmd_templ.format(
            action=action,
            daemon='%s-%s' % (self.cluster, self.daemon_type),
            id_=self.id_.replace('client.', ''),
        )
        return cmd

    def _set_commands(self):
        self.start_cmd = self._get_systemd_cmd('start')
        self.stop_cmd = self._get_systemd_cmd('stop')
        self.restart_cmd = self._get_systemd_cmd('restart')
        self.show_cmd = self._get_systemd_cmd('show')
        self.status_cmd = self._get_systemd_cmd('status')
        cluster_and_type = '%s-%s' % (self.cluster, self.daemon_type)
        if self.type_ == self.daemon_type:
            syslog_id = cluster_and_type
        else:
            syslog_id = self.daemon_type
        self.output_cmd = 'sudo journalctl -u ' \
            '{0}@{1} -t {2} -n 10'.format(
                cluster_and_type,
                self.id_.replace('client.', ''),
                syslog_id,
            )

    def check_status(self):
        """
        Check to see if the process has exited.

        :returns: The exit status, if any
        :raises:  CommandFailedError, if the process was run with
                  check_status=True
        """
        output = self.remote.sh(self.show_cmd + ' | grep -i state')

        def parse_line(line):
            key, value = line.strip().split('=', 1)
            return {key.strip(): value.strip()}
        show_dict = dict()
        for line in output.split('\n'):
            show_dict.update(parse_line(line))
        active_state = show_dict['ActiveState']
        sub_state = show_dict['SubState']
        if active_state == 'active':
            return None
        self.log.info("State is: %s/%s", active_state, sub_state)
        out = self.remote.sh(
            # This will match a line like:
            #    Main PID: 13394 (code=exited, status=1/FAILURE)
            # Or (this is wrapped):
            #    Apr 26 21:29:33 ovh083 systemd[1]: ceph-osd@1.service:
            #    Main process exited, code=exited, status=1/FAILURE
            self.status_cmd + " | grep 'Main.*code=exited'",
        )
        line = out.split('\n')[-1]
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
        """
        Method to retrieve daemon process id
        """
        proc_name = 'ceph-%s' % self.type_

        # process regex to match OSD, MON, MGR, MDS process command string
        # eg. "/usr/bin/ceph-<daemon-type> -f --cluster ceph --id <daemon-id>"
        proc_regex = '"%s.*--id %s "' % (proc_name, self.id_)

        # process regex to match RADOSGW process command string
        # eg. "/usr/bin/radosgw -f --cluster ceph --name <daemon-id=self.id_>"
        if self.type_ == "rgw":
            proc_regex = '"{}.*--name.*{}"'.format(self.daemon_type, self.id_)

        args = ['ps', '-ef',
                run.Raw('|'),
                'grep',
                run.Raw(proc_regex),
                run.Raw('|'),
                'grep', '-v',
                'grep', run.Raw('|'),
                'awk',
                run.Raw("{'print $2'}")]
        pid_string = self.remote.sh(args).strip()
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
        # check status will also fail if the process hasn't restarted
        self.check_status()

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
