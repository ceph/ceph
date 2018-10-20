'''
Salt "manager" module

Usage: First, ensure that there is a role whose name corresponds
to the value of the master_role variable, below. Second, in your
task, instantiate a SaltManager object:

    from salt_manager import SaltManager

    sm = SaltManager(ctx, config)

Third, enjoy the SaltManager goodness - e.g.:

    sm.ping_minions()

'''
import logging
import re

from cStringIO import StringIO
from teuthology import misc
from teuthology.contextutil import safe_while
from teuthology.exceptions import (CommandFailedError, ConfigError)
from teuthology.orchestra.remote import Remote
from teuthology.orchestra import run
from util import get_remote_for_role

log = logging.getLogger(__name__)
master_role = 'client.salt_master'


class SaltManager(object):

    def __init__(self, ctx, config):
        config["master_remote"] = get_remote_for_role(ctx, master_role).name
        self.config = config
        self.ctx = ctx
        self.cluster = ctx.cluster
        self.master_remote = Remote(self.config.get('master_remote'))
        self.minions = ctx.cluster.remotes

    def __systemctl_cluster(self, subcommand=None, service=None):
        """
        Do something to a systemd service unit on all remotes (test nodes) at
        once
        """
        self.cluster.run(args=[
            'sudo', 'systemctl', subcommand, '{}.service'.format(service)])

    def __systemctl_remote(self, remote, subcommand=None, service=None):
        """
        Do something to a systemd service unit on a single remote (test node)
        """
        try:
            remote.run(args=[
                'sudo', 'systemctl', subcommand, '{}.service'.format(service)])
        except CommandFailedError:
            log.warning("Failed to {} {}.service!".format(subcommand, service))
            remote.run(args=[
                'sudo', 'systemctl', 'status', '--full', '--lines=50',
                '{}.service'.format(service), run.Raw('||'), 'true'])
            raise

    def __cat_file_cluster(self, filename=None):
        """
        cat a file everywhere on the whole cluster
        """
        self.cluster.run(args=[
            'sudo', 'cat', filename])

    def __cat_file_remote(self, remote, filename=None):
        """
        cat a file on a particular remote
        """
        try:
            remote.run(args=[
                'sudo', 'cat', filename])
        except CommandFailedError:
            log.warning("{} not found on {}".format(filename, remote.name))

    def __ping(self, ping_cmd, expected):
        with safe_while(sleep=15, tries=20,
                        action=ping_cmd) as proceed:
            while proceed():
                output = StringIO()
                self.master_remote.run(args=ping_cmd, stdout=output)
                responded = len(re.findall('True', output.getvalue()))
                output.close()
                log.info("{} of {} minions responded"
                         .format(responded, expected))
                if (expected == responded):
                    return None

    def check_salt_daemons(self):
        self.master_remote.run(args = ['sudo', 'salt-key', '-L'])
        self.master_remote.run(args = ['sudo', 'systemctl', 'status',
                'salt-master.service'])
        for _remote in self.ctx.cluster.remotes.iterkeys():
            _remote.run(args = ['sudo', 'systemctl', 'status',
                'salt-minion.service'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion_id'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion.d/master.conf'])

    def gather_logs(self, logdir):
        for _remote in self.ctx.cluster.remotes.iterkeys():
            try:
                _remote.run(args = [
                    'sudo', 'test', '-d', '/var/log/{}/'.format(logdir),
                    ])
            except CommandFailedError:
                continue
            log.info("Gathering {} logs from remote {}"
                .format(logdir, _remote.hostname))
            _remote.run(args = [
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

    def gather_logfile(self, logfile):
        for _remote in self.ctx.cluster.remotes.iterkeys():
            try:
                _remote.run(args = [
                    'sudo', 'test', '-f', '/var/log/{}'.format(logfile),
                    ])
            except CommandFailedError:
                continue
            log.info("Gathering logfile /var/log/{} from remote {}"
                .format(logfile, _remote.hostname))
            _remote.run(args = [
                'sudo', 'cp', '-a', '/var/log/{}'.format(logfile),
                '/home/ubuntu/cephtest/archive/',
                run.Raw(';'),
                'sudo', 'chown', 'ubuntu',
                '/home/ubuntu/cephtest/archive/{}'.format(logfile)
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
        number_of_minions = len(self.minions)
        self.__ping(
            [
            'sudo', 'sh', '-c', 'salt \* test.ping || true',
            ],
            number_of_minions)
        return number_of_minions

    def start_minions(self):
        """Starts salt-minion.service on all cluster nodes"""
        self.__systemctl_cluster(subcommand="start", service="salt-minion")

    def restart_minions(self):
        """Restarts salt-minion.service on all cluster nodes"""
        self.__systemctl_cluster(subcommand="restart", service="salt-minion")

    def start_master(self):
        """Starts salt-master.service on the Salt Master node"""
        self.__systemctl_remote(self.master_remote,
            subcommand="start", service="salt-master")

    def restart_master(self):
        """Starts salt-master.service on the Salt Master node"""
        self.__systemctl_remote(self.master_remote,
            subcommand="restart", service="salt-master")

    def sync_pillar_data(self):
        self.master_remote.run(args=[
            'sudo',
            'sh',
            '-c',
            'salt \\* saltutil.sync_all 2>/dev/null'
        ])

    def cat_salt_master_conf(self):
        self.__cat_file_remote(self.master_remote, filename="/etc/salt/master")

    def cat_salt_minion_confs(self):
        self.__cat_file_cluster(filename="/etc/salt/minion")

