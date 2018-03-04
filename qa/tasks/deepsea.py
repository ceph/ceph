'''
Task to deploy clusters with DeepSea
'''
import logging
import os.path
import time

from teuthology import misc
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)
from teuthology.orchestra import run
from teuthology.salt import Salt
from teuthology.task import Task
from teuthology.job_status import get_status
from util import get_remote_for_role

log = logging.getLogger(__name__)

class DeepSea(Task):
    """
    Automated DeepSea integration testing via teuthology: set up a Salt
    cluster, clone the DeepSea git repo, run DeepSea integration test(s) 

    The number of machines in the cluster is determined by the roles stanza, as
    usual. One, and only one, of the machines must have a role of type
    "master", e.g.  master.1 or master.a (the part following the dot is not
    significant, but must be there).

    The task starts the Salt Master daemon on the master node, and Salt Minion
    daemons on all the nodes (including the master node), and ensures that the
    minions are properly linked to the master. TODO: The role types are stored
    in the role grain on the minions.

    After that, the DeepSea git repo is cloned to the master node in
    accordance with the "repo" and "branch" options (see below).

    Finally, the task iterates over the list of commands given in the "exec"
    property, executing each one inside the 'qa/' directory of the DeepSea repo
    clone.

    This task takes three mandatory options:

        repo: (DeepSea git repo, e.g. https://github.com/SUSE/DeepSea.git)
        branch: (DeepSea git branch, e.g. master)
        exec: (list of commands, relative to qa/ of the DeepSea repo)

    Example:

        tasks
        - deepsea:
            repo: https://github.com/SUSE/DeepSea.git
            branch: wip-foo
            exec:
            - suites/basic/health-ok.sh

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)

        log.debug("Initial config is {}".format(config))

        # make sure self.config dict has values for important keys
        assert config is not None, \
            'deepsea task needs configuration (repo, branch, exec)'
        assert isinstance(config, dict), \
            'deepsea task only accepts a dict for configuration'
        assert 'exec' in config, \
            'deepsea task needs configuration (exec)'
        assert isinstance(self.config["exec"], list), \
            'exec property of deepsea yaml must be a list'

        def _check_config_key(key, default_value):
            if key not in config or not config[key]:
                config[key] = default_value

        _check_config_key('repo', '')
        _check_config_key('branch', 'master')

        log.debug("Munged config is {}".format(config))

        # prepare the list of commands to be executed on the master node
        self.exec_cmd = []
        qa='DeepSea/qa'
        if self.config['repo'] is '':
            qa = '/usr/lib/deepsea/qa'

        assert len(self.config["exec"]) > 0, \
            'deepsea exec list must have at least one element'
        for cmd in self.config["exec"]:
            self.exec_cmd.append('cd %s ; %s' % (qa, cmd))

        # determine the role id of the master role
        if(misc.num_instances_of_type(self.cluster, 'master') != 1):
            raise ConfigError('deepsea requires a single master role')
        id_ = next(misc.all_roles_of_type(self.ctx.cluster, 'master'))
        master_role = '.'.join(['master', str(id_)])

        # set remote name for salt to pick it up. Setting the remote itself will
        # crash the reporting tool since it doesn't know how to turn the object
        # into a string
        self.config["master_remote"] = get_remote_for_role(self.ctx,
                master_role).name
        self.log.info("master remote: {}".format(self.config["master_remote"]))
        self.salt = Salt(self.ctx, self.config)

    def setup(self):
        super(DeepSea, self).setup()

        if self.config["repo"] is '':
            self.salt.master_remote.run(args=[
                'sudo',
                'zypper',
                '--non-interactive',
                'install',
                'deepsea',
                'deepsea-qa'
                ])
        else:
            self.make_install()
        self.setup_salt()

    def make_install(self):
        self.log.info("DeepSea repo: {}".format(self.config["repo"]))
        self.log.info("DeepSea branch: {}".format(self.config["branch"]))

        self.salt.master_remote.run(args=[
            'git',
            'clone',
            '--depth',
            '1',
            '--branch',
            self.config["branch"],
            self.config["repo"],
            ])

        self.log.info("printing DeepSea branch name and sha1...")
        self.salt.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'git',
            'rev-parse',
            '--abbrev-ref',
            'HEAD',
            run.Raw(';'),
            'git',
            'rev-parse',
            'HEAD',
            ])

        self.log.info("Running \"make install\" in DeepSea clone...")
        self.salt.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install',
            ])

        self.log.info("installing deepsea dependencies...")
        self.salt.master_remote.run(args = [
            'sudo',
            'zypper',
            '--non-interactive',
            'install',
            '--no-recommends',
            run.Raw('$(rpmspec --requires -q DeepSea/deepsea.spec.in 2>/dev/null)')
            ])
        self.setup_salt()

    def setup_salt(self):
        self.log.info("listing minion keys...")
        self.salt.master_remote.run(args = ['sudo', 'salt-key', '-L'])

        self.log.info("iterating over all the test nodes...")
        for _remote in self.ctx.cluster.remotes.iterkeys():
            self.log.info("minion configuration for {}".format(_remote.hostname))
            _remote.run(args = ['sudo', 'systemctl', 'status',
                'salt-minion.service'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion_id'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion.d/master.conf'])

        self.salt.ping_minions()

    def begin(self):
        super(DeepSea, self).begin()
        for cmd in self.exec_cmd:
            self.log.info(
                "command to be executed on master node: {}".format(cmd)
                )
            self.salt.master_remote.run(args=[
                'sudo', 'sh', '-c',
                cmd
                ])

    def purge_osds(self):
        # replace this hack with DeepSea purge when it's ready
        for _remote in self.ctx.cluster.remotes.iterkeys():
            self.log.info("stopping OSD services on {}"
                .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            self.log.info("unmounting OSD data devices on {}"
                .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'for f in vdb2 vdc2 ; do test -b /dev/$f && umount /dev/$f || true ; done'
                ])

    def gather_logs(self, logdir):
        for _remote in self.ctx.cluster.remotes.iterkeys():
            try:
                _remote.run(args = [
                    'sudo', 'test', '-d', '/var/log/{}/'.format(logdir),
                    ])
            except CommandFailedError:
                continue
            self.log.info("Gathering {} logs from remote {}"
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
            self.log.info("Gathering logfile /var/log/{} from remote {}"
                .format(logfile, _remote.hostname))
            _remote.run(args = [
                'sudo', 'cp', '-a', '/var/log/{}'.format(logfile),
                '/home/ubuntu/cephtest/archive/',
                run.Raw(';'),
                'sudo', 'chown', 'ubuntu',
                '/home/ubuntu/cephtest/archive/{}'.format(logfile)
                ])

    def end(self):
        self.gather_logfile('deepsea.log')
        self.purge_osds()
        self.gather_logs('salt')
        self.gather_logs('ganesha')
        super(DeepSea, self).end()

task = DeepSea
