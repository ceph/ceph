'''
Task that deploys a Ceph cluster using DeepSea
'''
import logging
import os.path
import time

from salt_manager import SaltManager
from teuthology import misc
from teuthology.exceptions import (CommandFailedError, ConfigError)
from teuthology.orchestra import run
from teuthology.misc import sh
from teuthology.task import Task
from util import check_config_key, copy_directory_recursively, get_remote_for_role

log = logging.getLogger(__name__)
health_ok_cmd = "health-ok.sh --teuthology"
cluster_roles = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'igw', 'ganesha']

class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve this).

    This task understands the following config keys:

        repo: (DeepSea git repo, e.g. https://github.com/SUSE/DeepSea.git)
        branch: (DeepSea git branch, e.g. master)
        install:
            package|pkg deepsea will be installed via package system
            source|src  deepsea will be installed via 'make install' (default)
        cli:
            true        deepsea CLI will be used (default)
            false       deepsea CLI will not be used
        deploy: (whether to deploy Ceph; defaults to False)
            health-ok:
            - list of commands to run as root

    Example:

        tasks
        - deepsea:
            repo: https://github.com/SUSE/DeepSea.git
            branch: wip-foo
            install: source

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)
        check_config_key(self.config, 'repo', 'https://github.com/SUSE/DeepSea.git')
        check_config_key(self.config, 'branch', 'master')
        install_conf = check_config_key(self.config, 'install', 'source')
        self._determine_install_method(install_conf)
        check_config_key(self.config, 'cli', True)
        self.health_ok_cmd = health_ok_cmd
        if self.config['cli']:
            self.health_ok_cmd += ' --cli'
        check_config_key(self.config, 'deploy', None)
        self.sm = SaltManager(self.ctx, self.config)
        self.master_remote = self.sm.master_remote
        #self.log.debug("ctx.config {}".format(ctx.config))
        self.roles = ctx.config['roles']
        self._introspect_roles()
        if self.config['deploy']:
            if not isinstance(self.config['deploy'], dict):
                raise ConfigError("deepsea: deploy config param takes a dict")
            check_config_key(self.config['deploy'], '_init_seq', True)
            # FIXME: set _dev_env based on number of cluster nodes (self.cluster_nodes)
            check_config_key(self.config['deploy'], '_dev_env', False)
        log.debug("Munged config is {}".format(self.config))

    def _determine_install_method(self, conf_val):
        install_lookup = {
                'source': self._install_deepsea_from_source,
                'src': self._install_deepsea_from_source,
                'package': self._install_deepsea_using_zypper,
                'pkg': self._install_deepsea_using_zypper,
            }
        if conf_val in ['source', 'src', 'package', 'pkg']:
            self._install_deepsea = install_lookup[conf_val]
        else:
            raise ConfigError("deepsea: unrecognized install config value ->{}<-"
                              .format(conf_val))

    def _purge_osds(self):
        # FIXME: purge osds only on nodes that have osd role
        for _remote in self.ctx.cluster.remotes.iterkeys():
            self.log.info("stopping OSD services on {}"
                .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            self.log.info("unmounting OSD partitions on {}"
                .format(_remote.hostname))
            # bluestore XFS partition is vd?1 - unmount up to five OSDs
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'for f in vdb1 vdc1 vdd1 vde1 vdf1 ; do test -b /dev/$f && umount /dev/$f || true ; done'
                ])
            # filestore XFS partition is vd?2 - unmount up to five OSDs
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'for f in vdb2 vdc2 vdd2 vde2 vdf2; do test -b /dev/$f && umount /dev/$f || true ; done'
                ])

    def _master_whoami(self):
        """Demonstrate that remote.run() does not run stuff as root"""
        self.master_remote.run(args=[
            'whoami',
        ])

    def _master_rpm_q(self, pkg_name):
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

    def _master_python_version(self, py_version):
        """
        Determine if a given python version is installed on the Salt Master node.
        """
        python_binary = 'python{}'.format(py_version)
        installed = True
        try:
            self.master_remote.run(args=[
                'type',
                python_binary,
                run.Raw('>'),
                '/dev/null',
                run.Raw('2>&1'),
            ])
        except CommandFailedError:
            installed = False
        if installed:
            self.master_remote.run(args=[
                python_binary,
                '--version'
            ])
        else:
            self.log.info('{} not installed on master node'.format(python_binary))
        return installed

    def _deepsea_cli_version(self):
        """Use DeepSea CLI to display the DeepSea version under test"""
        installed = True
        try:
            self.master_remote.run(args=[
                'type',
                'deepsea',
                run.Raw('>'),
                '/dev/null',
                run.Raw('2>&1'),
            ])
        except CommandFailedError:
            installed = False
        if installed:
            self.master_remote.run(args=[
                'deepsea',
                '--version',
            ])
        else:
            self.log.info("deepsea CLI not installed")

    def _install_deepsea_from_source(self):
        """Install DeepSea from source (unless already installed from RPM)"""
        if self._master_rpm_q('deepsea'):
            self.log.info("DeepSea already installed from RPM")
            return None
        self.log.info("Installing DeepSea from source - repo: {}, branch: {}"
                      .format(self.config["repo"], self.config["branch"]))
        self.master_remote.run(args=[
            'git',
            '--version',
            run.Raw(';'),
            'git',
            'clone',
            '--branch',
            self.config["branch"],
            self.config["repo"],
            run.Raw(';'),
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
            run.Raw(';'),
            'git',
            'describe',
            run.Raw('||'),
            'true',
            ])

        self.log.info("Running \"make install\" in DeepSea clone...")
        self.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install',
            ])

        self.log.info("installing deepsea dependencies...")
        self.master_remote.run(args = [
            'sudo',
            'zypper',
            '--non-interactive',
            'install',
            '--no-recommends',
            run.Raw('$(rpmspec --requires -q DeepSea/deepsea.spec.in 2>/dev/null)')
            ])

    def _install_deepsea_using_zypper(self):
        """Install DeepSea using zypper"""
        self.log.info("Installing DeepSea using zypper")
        self.master_remote.run(args=[
            'sudo',
            'zypper',
            '--non-interactive',
            'search',
            '--details',
            'deepsea'
            ])
        self.master_remote.run(args=[
            'sudo',
            'zypper',
            '--non-interactive',
            '--no-gpg-checks',
            'install',
            '--force',
            '--no-recommends',
            'deepsea',
            'deepsea-cli',
            'deepsea-qa'
            ])

    def _set_pillar_deepsea_minions(self):
        """Set deepsea_minions pillar value"""
        self.master_remote.run(args=[
            'sudo',
            'sh',
            '-c',
            'echo "deepsea_minions: \'*\'" > /srv/pillar/ceph/deepsea_minions.sls',
            run.Raw(';'),
            'cat',
            '/srv/pillar/ceph/deepsea_minions.sls',
        ])

    def _initialization_sequence(self):
        """
        Port of initialization_sequence from health-ok.sh
        """
        self._master_rpm_q('ceph')
        self._master_rpm_q('ceph-test')
        self._master_rpm_q('salt-master')
        self._master_rpm_q('salt-minion')
        self._master_rpm_q('salt-api')
        self._master_python_version(2)
        if not self._master_python_version(3):
            raise ConfigError("Python 3 not installed on master node - bailing out!")
        self._deepsea_cli_version()
        self._set_pillar_deepsea_minions()
        self.sm.sync_pillar_data()
        self.sm.cat_salt_master_conf()
        self.sm.cat_salt_minion_confs()

    def _introspect_roles(self):
        """
        Sets:

            self.role_nodes,
            self.cluster_nodes, and
            self.client_nodes.

        The latter are understood to be client-ONLY nodes, and:

        self.role_nodes == (self.cluster_nodes + self.client_nodes)

        Also sets:

            self.role_remotes,
            self.cluster_remotes, and
            self.client_remotes.

        These are dicts of teuthology "remote" objects, which look like this:

            { remote1_name: remote1_obj, ..., remoten_name: remoten_obj }

        Finally, sets:

            self.role_lookup_table

        which will look something like this:

            {
                "osd": { "osd.0": osd0remotename, ..., "osd.n": osdnremotename },
                "mon": { "mon.a": monaremotename, ..., "mon.n": monnremotename },
                ...
            }

        and

            self.remote_lookup_table

        which looks like this:

            {
                remote0name: [ "osd.0", "client.0" ],
                ...
                remotenname: [ remotenrole0, ..., remotenrole99 ],
            }

        (In other words, self.remote_lookup_table is just like the roles
        stanza, except the role lists are keyed by remote name.)
        """
        self.role_nodes = len(self.roles)
        self.role_remotes = {}
        self.cluster_remotes = {}
        self.role_lookup_table = {}
        self.remote_lookup_table = {}
        for node_roles_list in self.roles:
            remote = get_remote_for_role(self.ctx, node_roles_list[0])
            self.log.debug("Considering remote name {}, hostname {}"
                           .format(remote.name, remote.hostname))
            self.remote_lookup_table[remote.hostname] = node_roles_list
            # inner loop: roles (something like "osd.1" or "c2.mon.a")
            for role in node_roles_list:
                # FIXME: support multiple clusters as used in, e.g., rgw/multisite suite
                role_arr = role.split('.')
                if len(role_arr) != 2:
                    raise ConfigError("deepsea: unsupported role ->{}<- encountered!"
                                      .format(role))
                (role_type, role_idx) = role_arr
                remote = get_remote_for_role(self.ctx, role)
                self.role_remotes[remote.hostname] = remote
                if role_type not in self.role_lookup_table.keys():
                    self.role_lookup_table[role_type] = {}
                self.role_lookup_table[role_type][role] = remote.hostname
                if role_type in cluster_roles:
                    self.cluster_remotes[remote.hostname] = remote
        self.cluster_nodes = len(self.cluster_remotes)
        self.client_remotes = self.role_remotes
        for remote_name, remote_obj in self.cluster_remotes.iteritems():
            del(self.client_remotes[remote_name])
        self.client_nodes = len(self.client_remotes)
        self.log.info("ROLE INTROSPECTION REPORT")
        self.log.info("self.role_nodes == {}".format(self.role_nodes))
        self.log.info("self.cluster_nodes == {}".format(self.cluster_nodes))
        self.log.info("self.client_remotes == {}".format(self.client_remotes))
        self.log.info("self.role_remotes == {}".format(self.role_remotes))
        self.log.info("self.cluster_remotes == {}".format(self.cluster_remotes))
        self.log.info("self.client_nodes == {}".format(self.client_nodes))
        self.log.info("self.role_lookup_table == {}".format(self.role_lookup_table))
        self.log.info("self.remote_lookup_table == {}".format(self.remote_lookup_table))

    def _check_config_deploy_health_ok(self, deploy):
        check_config_key(deploy, 'health-ok', None)
        deploy_cmdlist = deploy['health-ok']
        if deploy_cmdlist == None:
            deploy_cmdlist = [self.health_ok_cmd]
        if not isinstance(deploy_cmdlist, list):
            raise ConfigError("deepsea: health-ok config param takes a list")
        if not deploy_cmdlist:
            raise ConfigError("deepsea: health-ok command list must not be empty")
        self.log.info("deepsea: deployment command list: {}"
                       .format(deploy_cmdlist))
        deploy['health-ok'] = deploy_cmdlist
        log.debug("Munged config is {}".format(self.config))
 
    def _copy_health_ok(self):
        """
        Copy health-ok.sh from teuthology VM to master_remote
        """
        suite_path = self.ctx.config.get('suite_path')
        log.info("suite_path is ->{}<-".format(suite_path))
        sh("ls -l {}".format(suite_path))
        health_ok_path = suite_path + "/deepsea/health-ok"
        sh("test -d " + health_ok_path)
        copy_directory_recursively(health_ok_path, self.master_remote, "health-ok")
        self.master_remote.run(args=[
            "pwd",
            run.Raw(";"),
            "ls",
            "-lR",
            "health-ok",
            ])

    def _run_deployment_commands(self, deploy):
        self._copy_health_ok()
        for cmd in deploy['health-ok']:
            if cmd.startswith('health-ok.sh'):
                cmd = "health-ok/" + cmd
            if deploy['_dev_env']:
                cmd = "DEV_ENV=\"true\" " + cmd
            self.master_remote.run(args=[
                'sudo',
                'bash',
                '-c',
                cmd,
                ])

    def _maybe_deploy_ceph(self):
        deploy = self.config.get('deploy')
        if deploy:
            self._check_config_deploy_health_ok(deploy)
            log.debug("Munged config is {}".format(self.config))
            if deploy['_init_seq']:
                self._initialization_sequence()
            else:
                self.log.warn("deepsea: skipping initialization sequence")
            self._run_deployment_commands(deploy)
        else:
            self.log.info("deepsea: deploy config param not given: not deploying Ceph")

    def setup(self):
        super(DeepSea, self).setup()
        log.debug("beginning of DeepSea task setup method...")
        self._install_deepsea()
        log.debug("end of DeepSea task setup...")

    def begin(self):
        super(DeepSea, self).begin()
        log.debug("beginning of DeepSea task begin method...")
        self._master_whoami()
        self._maybe_deploy_ceph()
        log.debug("end of DeepSea task begin method...")

    def end(self):
        super(DeepSea, self).end()
        log.debug("beginning of DeepSea task end method...")
        self.sm.gather_logfile('deepsea.log')
        self.sm.gather_logs('ganesha')
        log.debug("end of DeepSea task end method...")

    def teardown(self):
        super(DeepSea, self).teardown()
        log.debug("beginning of DeepSea task teardown method...")
        self._purge_osds()
        log.debug("end of DeepSea task teardown method...")


task = DeepSea
