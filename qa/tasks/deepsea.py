'''
Task to deploy clusters with DeepSea
'''
import logging

from teuthology import misc
from teuthology.orchestra import run
from teuthology.salt import Salt
from teuthology.task import Task
from util import get_remote_for_role

log = logging.getLogger(__name__)

class DeepSea(Task):

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)

        if config is None:
            config = {}
        assert isinstance(config, dict), \
            'deepsea task only accepts a dict for configuration'
        self.config["repo"] = config.get('repo', 'https://github.com/SUSE/DeepSea.git')
        self.config["branch"] = config.get('branch', 'master')

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
        self.salt = Salt(self.ctx, self.config)

    def setup(self):
        super(DeepSea, self).setup()

        self.log.info("DeepSea repo: {}".format(self.config["repo"]))
        self.log.info("DeepSea branch: {}".format(self.config["branch"]))
        self.log.info("master remote: {}".format(self.config["master_remote"]))

        self.salt.master_remote.run(args=[
            'git',
            'clone',
            '--branch',
            self.config["branch"],
            self.config["repo"],
            run.Raw(';'),
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
            run.Raw('$(rpmspec --requires -q -v DeepSea/deepsea.spec | grep manual | awk \'{print $2}\')')
            ])

        self.log.info("listing minion keys...")
        self.salt.master_remote.run(args = ['sudo', 'salt-key', '-L'])

        self.log.info("iterating over all the test nodes...")
        for _remote, roles_for_host in self.ctx.cluster.remotes.iteritems():
            self.log.info("minion configuration for {}".format(_remote.hostname))
            _remote.run(args = ['sudo', 'systemctl', 'status',
                'salt-minion.service'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion_id'])
            _remote.run(args = ['sudo', 'cat', '/etc/salt/minion.d/master.conf'])

        self.salt.ping_minions()

        self.log.info("listing contents of DeepSea/qa/ directory tree...")
        self.salt.master_remote.run(args=[
            'ls',
            '-lR',
            '--color=never',
            'DeepSea/qa/'
            ])

        self.log.info("running basic-health-ok.sh workunit...")
        self.salt.master_remote.run(args=[
            'sudo',
            'DeepSea/qa/workunits/basic-health-ok.sh'
            ])


    def begin(self):
        super(DeepSea, self).begin()

    def end(self):
        super(DeepSea, self).end()

task = DeepSea
