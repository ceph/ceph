'''
Salt "manager" module

Usage: First, ensure that there is a role whose name corresponds
to the value of the master_role variable, below. Second, in your
task, instantiate a SaltManager object:

    from salt_manager import SaltManager

    sm = SaltManager(ctx)

Third, enjoy the SaltManager goodness - e.g.:

    sm.ping_minions()

Linter:
    flake8 --max-line-length=100
'''
import logging
import re

from teuthology.contextutil import safe_while
from teuthology.exceptions import CommandFailedError, MaxWhileTries
from teuthology.orchestra import run
from util import get_remote_for_role

log = logging.getLogger(__name__)
master_role = 'client.salt_master'


def systemctl_remote(remote, subcommand, service_name):
    """
    Caveat: only works for units ending in ".service"
    """
    def systemctl_cmd(subcommand, lines=0):
        return ('sudo systemctl {} --full --lines={} {}.service'
                .format(subcommand, lines, service_name))
    try:
        remote.run(args=systemctl_cmd(subcommand))
    except CommandFailedError:
        remote.run(args=systemctl_cmd('status', 100))
        raise


class SaltManager(object):

    def __init__(self, ctx):
        self.ctx = ctx
        self.master_remote = get_remote_for_role(self.ctx, master_role)

    def __cat_file_cluster(self, filename=None):
        """
        cat a file everywhere on the whole cluster
        """
        self.ctx.cluster.run(args=[
            'sudo', 'cat', filename])

    def __cat_file_remote(self, remote, filename=None):
        """
        cat a file on a particular remote
        """
        try:
            remote.run(args=[
                'sudo', 'cat', filename])
        except CommandFailedError:
            log.warning((
                "salt_manager: {} not found on {}"
                ).format(filename, remote.name))

    def __ping(self, ping_cmd, expected):
        try:
            def instances_of_str(search_str, output):
                return len(re.findall(search_str, output))
            with safe_while(sleep=15, tries=50,
                            action=ping_cmd) as proceed:
                while proceed():
                    output = self.master_remote.sh(ping_cmd)
                    no_master = instances_of_str('The salt master could not be contacted', output)
                    responded = instances_of_str('  True', output)
                    log.info("{} of {} minions responded".format(responded, expected))
                    if (expected == responded):
                        return None
        except MaxWhileTries:
            if no_master:
                cmd = 'sudo systemctl status --full --lines=100 salt-master.service'
                self.master_remote.run(args=cmd)

    def all_minions_cmd_run(self, cmd, abort_on_fail=True, show_stderr=False):
        """
        Use cmd.run to run a command on all nodes.
        """
        if not abort_on_fail:
            cmd += ' || true'
        redirect = "" if show_stderr else " 2>/dev/null"
        self.master_remote.run(args=(
            'sudo salt \\* cmd.run \'{}\'{}'.format(cmd, redirect)
            ))

    def all_minions_zypper_lu(self):
        """Run "zypper lu" on all nodes"""
        cmd = "zypper --non-interactive --no-gpg-checks list-updates"
        self.all_minions_cmd_run(cmd, abort_on_fail=False)

    def all_minions_zypper_ps(self):
        """Run "zypper ps -s" on all nodes"""
        cmd = "zypper ps -s || true"
        self.all_minions_cmd_run(cmd, abort_on_fail=False)

    def all_minions_zypper_ps_requires_reboot(self):
        number_of_minions = len(self.ctx.cluster.remotes)
        salt_cmd = "sudo salt \\* cmd.run \'zypper ps -s || true\' 2>/dev/null"
        number_with_no_processes = len(
            re.findall('No processes using deleted files found',
                       self.master_remote.sh(salt_cmd))
            )
        return number_with_no_processes != number_of_minions

    def all_minions_zypper_ref(self):
        """Run "zypper ref" on all nodes"""
        cmd = "zypper --non-interactive --gpg-auto-import-keys refresh"
        self.all_minions_cmd_run(cmd, abort_on_fail=False)

    def all_minions_zypper_status(self):
        """
        Implement someone's idea of a general 'zypper status'
        """
        self.all_minions_zypper_ref()
        self.all_minions_zypper_lu()
        self.all_minions_zypper_ps()

    def cat_salt_master_conf(self):
        self.__cat_file_remote(self.master_remote, filename="/etc/salt/master")

    def cat_salt_minion_confs(self):
        self.__cat_file_cluster(filename="/etc/salt/minion")

    def check_salt_daemons(self):
        self.master_remote.run(args=['sudo', 'salt-key', '-L'])
        systemctl_remote(self.master_remote, 'status', 'salt-master')
        for _remote in self.ctx.cluster.remotes.iterkeys():
            systemctl_remote(_remote, 'status', 'salt-minion')
            _remote.run(args='sudo cat /etc/salt/minion_id')
            _remote.run(args='sudo cat /etc/salt/minion.d/master.conf')

    def enable_master(self):
        """Enables salt-master.service on the Salt Master node"""
        systemctl_remote(self.master_remote, "enable", "salt-master")

    def enable_minions(self):
        """Enables salt-minion.service on all cluster nodes"""
        systemctl_remote(self.ctx.cluster, "enable", "salt-minion")

    def gather_logfile(self, logfile):
        for _remote in self.ctx.cluster.remotes.iterkeys():
            try:
                _remote.run(args=[
                    'sudo', 'test', '-f', '/var/log/{}'.format(logfile),
                    ])
            except CommandFailedError:
                continue
            log.info((
                "gathering logfile /var/log/{} from remote {}"
                ).format(logfile, _remote.hostname))
            _remote.run(args=[
                'sudo', 'cp', '-a', '/var/log/{}'.format(logfile),
                '/home/ubuntu/cephtest/archive/',
                run.Raw(';'),
                'sudo', 'chown', 'ubuntu',
                '/home/ubuntu/cephtest/archive/{}'.format(logfile)
                ])

    def gather_logs(self, logdir):
        for _remote in self.ctx.cluster.remotes.iterkeys():
            try:
                _remote.run(args=[
                    'sudo', 'test', '-d', '/var/log/{}/'.format(logdir),
                    ])
            except CommandFailedError:
                continue
            log.info("gathering {} logs from remote {}"
                     .format(logdir, _remote.hostname))
            _remote.run(args=[
                'sudo', 'cp', '-a', '/var/log/{}/'.format(logdir),
                '/home/ubuntu/cephtest/archive/',
                run.Raw(';'),
                'sudo', 'chown', '-R', 'ubuntu',
                '/home/ubuntu/cephtest/archive/{}/'.format(logdir),
                run.Raw(';'),
                'find', '/home/ubuntu/cephtest/archive/{}/'.format(logdir),
                '-type', 'f', '-print0',
                run.Raw('|'),
                'xargs', '-0', '--no-run-if-empty', '--', 'gzip', '--'
                ])

    def master_role(self):
        return master_role

    def master_rpm_q(self, pkg_name):
        """Run rpm -q on the Salt Master node"""
        # FIXME: should possibly take a list of pkg_names
        installed = True
        try:
            self.master_remote.run(args=[
                'rpm', '-q', pkg_name
            ])
        except CommandFailedError:
            installed = False
        return installed

    def ping_minion(self, mid):
        """Pings a minion; raises exception if it doesn't respond"""
        self.__ping(['sudo', 'salt', mid, 'test.ping'], 1)

    def ping_minions(self):
        """
        Pings minions; raises exception if they don't respond
        """
        number_of_minions = len(self.ctx.cluster.remotes)
        self.__ping(
            "sudo sh -c \'salt \\* test.ping\' 2>/dev/null || true",
            number_of_minions,
            )
        return number_of_minions

    def restart_master(self):
        """Starts salt-master.service on the Salt Master node"""
        systemctl_remote(self.master_remote, "restart", "salt-master")

    def restart_minions(self):
        """Restarts salt-minion.service on all cluster nodes"""
        systemctl_remote(self.ctx.cluster, "restart", "salt-minion")

    def start_master(self):
        """Starts salt-master.service on the Salt Master node"""
        systemctl_remote(self.master_remote, "start", "salt-master")

    def start_minions(self):
        """Starts salt-minion.service on all cluster nodes"""
        systemctl_remote(self.ctx.cluster, "start", "salt-minion")

    def sync_pillar_data(self, quiet=True):
        cmd = "sudo salt \\* saltutil.sync_all"
        if quiet:
            cmd += " 2>/dev/null"
        with safe_while(sleep=15, tries=10,
                        action=cmd) as proceed:
            while proceed():
                no_response = len(re.findall('Minion did not return', self.master_remote.sh(cmd)))
                if no_response:
                    log.info("Not all minions responded. Retrying.")
                else:
                    return None
