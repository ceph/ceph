'''
Task that deploys a Ceph cluster using DeepSea
'''
import logging

from salt_manager import SaltManager
from teuthology.exceptions import (CommandFailedError, ConfigError)
from teuthology.orchestra import run
from teuthology.misc import (
    sh,
    sudo_write_file,
    write_file,
    )
from teuthology.task import Task
from util import (
    check_config_key,
    copy_directory_recursively,
    get_remote_for_role,
    sudo_append_to_file,
    )

log = logging.getLogger(__name__)
ceph_cluster_status = """# Display ceph cluster status
set -ex
ceph pg stat -f json-pretty
ceph health detail -f json-pretty
ceph osd tree
ceph osd pool ls detail -f json-pretty
ceph -s
"""
cluster_roles = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'igw', 'ganesha']
disable_update_in_stage_0 = """# Disable update in Stage 0
set -ex
cp /srv/salt/ceph/stage/prep/master/default.sls \
   /srv/salt/ceph/stage/prep/master/default-orig.sls
cp /srv/salt/ceph/stage/prep/master/default-no-update-no-reboot.sls \
   /srv/salt/ceph/stage/prep/master/default.sls
cp /srv/salt/ceph/stage/prep/minion/default.sls \
   /srv/salt/ceph/stage/prep/minion/default-orig.sls
cp /srv/salt/ceph/stage/prep/minion/default-no-update-no-reboot.sls \
   /srv/salt/ceph/stage/prep/minion/default.sls
"""
global_conf = '/srv/salt/ceph/configuration/files/ceph.conf.d/global.conf'
health_ok_cmd = "health-ok.sh --teuthology"
mon_conf = '/srv/salt/ceph/configuration/files/ceph.conf.d/mon.conf'
proposals_dir = "/srv/pillar/ceph/proposals"
salt_api_test = """# Salt API test script
set -e
TMPFILE=$(mktemp)
echo "Salt API test: BEGIN"
systemctl --no-pager --full status salt-api.service
curl http://$(hostname):8000/ | tee $TMPFILE # show curl output in log
test -s $TMPFILE
jq . $TMPFILE >/dev/null
echo -en "\\n" # this is just for log readability
rm $TMPFILE
echo "Salt API test: END"
"""


def remote_run_script_as_root(remote, path, data):
    """
    Wrapper around write_file to simplify the design pattern:
    1. use write_file to create bash script on the remote
    2. use Remote.run to run that bash script via "sudo bash $SCRIPT"
    """
    write_file(remote, path, data)
    remote.run(label=path, args='sudo bash {}'.format(path))


class DeepSeaDeploy(Task):
    """
    Deploy Ceph using DeepSea

    Assumes a Salt cluster is already running (use the "salt" task to achieve
    this) and DeepSea has already been installed (use the "install" or
    "deepsea" tasks to achieve that).

    This task understands the following config keys:

        cli:
            true        deepsea CLI will be used (default)
            false       deepsea CLI will not be used
        commands:
        - list of commands to run as root

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    def __init__(self, ctx, config):
        super(DeepSeaDeploy, self).__init__(ctx, config)
        self.roles = ctx.config['roles']
        self._introspect_roles()
        check_config_key(self.config, 'cli', True)
        self.health_ok_cmd = health_ok_cmd
        if self.config['cli']:
            self.health_ok_cmd += ' --cli'
        deploy_cmdlist = check_config_key(
                         self.config,
                         'commands',
                         [self.health_ok_cmd],
                         )
        if not isinstance(deploy_cmdlist, list) or not deploy_cmdlist:
            raise ConfigError(
                    "deepsea_deploy: commands config param takes a list")
        self.log.info("deepsea_deploy: deployment command list: {}"
                      .format(deploy_cmdlist))
        self.sm = SaltManager(self.ctx, self.config)
        self.master_remote = self.sm.master_remote
#       self.log.debug("ctx.config {}".format(ctx.config))
        log.debug("Munged config is {}".format(self.config))

    def _ceph_cluster_status(self):
        remote_run_script_as_root(
            self.master_remote,
            'ceph_cluster_status.sh',
            ceph_cluster_status,
            )

    def _ceph_conf_mon_allow_pool_delete(self):
        info_msg = (
            "deepsea_deploy: adjusted ceph.conf "
            "to allow pool deletes")
        data = "mon allow pool delete = true\n"
        sudo_append_to_file(
            self.master_remote,
            global_conf,
            data,
            )
        self.log.info(info_msg)

    def _ceph_conf_dashboard(self):
        info_msg = (
            "deepsea_deploy: adjusted ceph.conf "
            "for deployment of dashboard MGR module")
        data = "mgr initial modules = dashboard\n"
        sudo_append_to_file(
            self.master_remote,
            mon_conf,
            data,
            )
        self.log.info(info_msg)

    def _ceph_conf_small_cluster(self):
        """
        Apply necessary ceph.conf for small clusters
        """
        info_msg = (
            "deepsea_deploy: adjusting ceph.conf for operation with "
            "{} storage node(s)"
            ).format(self.cluster_nodes)
        data = None
        if self.cluster_nodes == 1:
            data = (
                   "mon pg warn min per osd = 16\n"
                   "osd pool default size = 2\n"
                   "osd crush chooseleaf type = 0 # failure domain == osd\n"
                   )
        elif self.cluster_nodes == 2 or self.cluster_nodes == 3:
            data = (
                   "mon pg warn min per osd = 8\n"
                   "osd pool default size = 2\n"
                   )
        if data:
            self.log.info(info_msg)
            sudo_append_to_file(
                self.master_remote,
                global_conf,
                data,
                )

    def _ceph_health_test(self):
        self.master_remote.run(args=[
            'sudo',
            'salt-call',
            'wait.until',
            run.Raw('status=HEALTH_OK'),
            'timeout=900',
            'check=1',
            ])

    def _copy_health_ok(self):
        """
        Copy health-ok.sh from teuthology VM to master_remote
        """
        suite_path = self.ctx.config.get('suite_path')
        log.info("suite_path is ->{}<-".format(suite_path))
        sh("ls -l {}".format(suite_path))
        health_ok_path = suite_path + "/deepsea/health-ok"
        sh("test -d " + health_ok_path)
        copy_directory_recursively(
                health_ok_path, self.master_remote, "health-ok")
        self.master_remote.run(args=[
            "pwd",
            run.Raw(";"),
            "ls",
            "-lR",
            "health-ok",
            ])

    def _deploy_ceph(self):
        self._initialization_sequence()
        self._run_commands()

    def _deepsea_cli_version(self):
        """
        Use DeepSea CLI to display the DeepSea version under test
        """
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

    def _dump_global_conf(self):
        self.master_remote.run(args=[
            'ls',
            '-l',
            global_conf,
            run.Raw(';'),
            'cat',
            global_conf,
            ])

    def _dump_mon_conf(self):
        self.master_remote.run(args=[
            'ls',
            '-l',
            mon_conf,
            run.Raw(';'),
            'cat',
            mon_conf,
            ])

    def _initialization_sequence(self):
        """
        Port of initialization_sequence from health-ok.sh
        """
        self.log.info("WWWW: starting deepsea_deploy initialization sequence")
        self.sm.master_rpm_q('ceph')
        self.sm.master_rpm_q('ceph-test')
        self.sm.master_rpm_q('salt-master')
        self.sm.master_rpm_q('salt-minion')
        self.sm.master_rpm_q('salt-api')
        self._master_python_version(2)
        if not self._master_python_version(3):
            raise ConfigError("Python 3 not installed on master node"
                              " - bailing out!")
        self._deepsea_cli_version()
        self._set_pillar_deepsea_minions()
        # Stage 0 does this, but we have no guarantee Stage 0 will run
        self.sm.sync_pillar_data()

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
                "osd": { "osd.0": osd0remname, ..., "osd.n": osdnremname },
                "mon": { "mon.a": monaremname, ..., "mon.n": monnremname },
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
                # FIXME: support multiple clusters as used in, e.g.,
                # rgw/multisite suite
                role_arr = role.split('.')
                if len(role_arr) != 2:
                    raise ConfigError(
                        "deepsea_deploy: unsupported role ->{}<- encountered!"
                        .format(role)
                        )
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
        self.dev_env = True if self.cluster_nodes < 4 else False
        self.log.info("ROLE INTROSPECTION REPORT")
        self.log.info("role_nodes == {}".format(self.role_nodes))
        self.log.info("cluster_nodes == {}".format(self.cluster_nodes))
        self.log.info("client_nodes == {}".format(self.client_nodes))
        self.log.info("role_remotes == {}".format(self.role_remotes))
        self.log.info("cluster_remotes == {}".format(self.cluster_remotes))
        self.log.info("client_remotes == {}".format(self.client_remotes))
        self.log.info("role_lookup_table == {}".format(self.role_lookup_table))
        self.log.info("remote_lookup_table == {}"
                      .format(self.remote_lookup_table))
        self.log.info("dev_env == {}".format(self.dev_env))

    def _master_python_version(self, py_version):
        """
        Determine if a given python version is installed on the Salt Master
        node.
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
            self.log.info('{} not installed on master node'
                          .format(python_binary))
        return installed

    def _pillar_items(self):
        self.master_remote.run(args=[
            'sudo',
            'salt',
            '*',
            'pillar.items',
            ])

    # FIXME: convert this into its own class
    def _policy_cfg(self, config):
        """
        Generate policy.cfg from the results of role introspection
        """
        self.log.info("deepsea_deploy: WWWW: generating policy.cfg")
        self.log.debug("deepsea_deploy: roles stanza from job yaml: {}"
                       .format(self.roles))
        if not config:
            config = {}
        check_config_key(config, "profile", "teuthology")
        self._policy_cfg_dump_proposals_dir()
        self._policy_cfg_build_base()
        self._policy_cfg_build_x('mon', required=True)
        self._policy_cfg_build_x('mgr', required=True)
        self._policy_cfg_build_x('mds')
        self._policy_cfg_build_x('rgw')
        self._policy_cfg_build_x('igw')
        self._policy_cfg_build_x('ganesha')
        self._policy_cfg_build_profile(config)
        self._policy_cfg_write()
        self._policy_cfg_cat()
        self._policy_cfg_dump_profile_ymls()

    def _policy_cfg_build_base(self):
        """
        policy.cfg boilerplate
        """
        self.policy_cfg = """# policy.cfg generated by deepsea_deploy.py
# Cluster assignment
cluster-ceph/cluster/*.sls
# Common configuration
config/stack/default/global.yml
config/stack/default/ceph/cluster.yml
# Role assignment - master
role-master/cluster/{master_minion}.sls
# Role assignment - admin
role-admin/cluster/*.sls
""".format(master_minion=self.master_remote.hostname)

    def _policy_cfg_build_profile(self, config):
        """
        Add storage profile to policy.cfg
        """
        profile = config['profile']
        if not isinstance(profile, str):
            raise ConfigError(
                "deepsea_deploy post-Stage 1: "
                "profile config param must be a string"
                )
        if profile == 'teuthology':
            raise ConfigError(
                "deepsea_deploy: teuthology profile not implemented yet"
                )
        elif profile == 'default':
            self.__policy_cfg_build_profile_x('default')
        else:
            ConfigError(
                "deepsea_deploy post-Stage 1: unknown profile ->{}<-"
                .format(profile)
                )

    def __policy_cfg_build_profile_x(self, profile):
        self.profile_ymls_to_dump = []
        no_osd_roles = ("deepsea_deploy: no osd roles configured"
                        ", but at least one of these is required.")
        role_dict = {}
        if 'osd' in self.role_lookup_table:
            role_dict = self.role_lookup_table['osd']
        else:
            raise ConfigError(no_osd_roles)
        self.log.debug((
            "deepsea_deploy: generating policy.cfg lines for osd "
            "profile ->{}<- based on {}"
            ).format(profile, role_dict))
        if len(role_dict) == 0:
            raise ConfigError(no_osd_roles)
        osd_remotes = list(set(role_dict.values()))
        for osd_remote in osd_remotes:
            self.log.debug("deepsea_deploy: {} has one or more osd roles"
                           .format(osd_remote))
            self.policy_cfg += """# Storage profile - {remote}
profile-{profile}/cluster/{remote}.sls
""".format(remote=osd_remote, profile=profile)
            ypp = ("profile-{}/stack/default/ceph/minions/{}.yml"
                   .format(profile, osd_remote))
            self.policy_cfg += ypp + "\n"
            self.profile_ymls_to_dump.append(
                "{}/{}".format(proposals_dir, ypp))

    def _policy_cfg_build_x(self, role_type, required=False):
        no_roles_of_type = ("deepsea_deploy: no {} roles configured"
                            .format(role_type))
        but_required = ", but at least one of these is required."
        role_dict = {}
        if role_type in self.role_lookup_table:
            role_dict = self.role_lookup_table[role_type]
        elif required:
            raise ConfigError(no_roles_of_type + but_required)
        else:
            self.log.debug(no_roles_of_type)
            return None
        self.log.debug(
            "deepsea_deploy: generating policy.cfg lines for {} based on {}"
            .format(role_type, role_dict)
        )
        if required:
            if len(role_dict.keys()) < 1:
                raise ConfigError(no_roles_of_type + but_required)
        for role_spec, remote_name in role_dict.iteritems():
            self.policy_cfg += (
                '# Role assignment - {}\n'
                'role-{}/cluster/{}.sls\n'
                ).format(role_spec, role_type, remote_name)

    def _policy_cfg_cat(self):
        """
        Dump the remote policy.cfg file to teuthology log.
        """
        self.master_remote.run(args=[
            'cat',
            proposals_dir + "/policy.cfg"
            ])

    def _policy_cfg_dump_profile_ymls(self):
        """
        Dump profile yml files that have been earmarked for dumping.
        """
        for yml_file in self.profile_ymls_to_dump:
            self.master_remote.run(args=[
                'sudo',
                'cat',
                yml_file,
                ])

    def _policy_cfg_dump_proposals_dir(self):
        """
        Dump the entire proposals directory hierarchy to the teuthology log.
        """
        self.master_remote.run(args=[
            'test',
            '-d',
            proposals_dir,
            run.Raw(';'),
            'ls',
            '-lR',
            proposals_dir + '/',
            ])

    def _policy_cfg_write(self):
        """
        Write policy_cfg to master remote.
        """
        sudo_write_file(
            self.master_remote,
            proposals_dir + "/policy.cfg",
            self.policy_cfg,
            perms="0644",
            owner="salt",
            )
        self.master_remote.run(args=[
            "ls",
            "-l",
            proposals_dir + "/policy.cfg"
            ])

    def _run_command_dict(self, cmd_dict):
        """
        Process commands given in form of dict - example:

            commands:
            - stage0:
                update: true,
                reboot: false
        """
        if len(cmd_dict) != 1:
            raise ConfigError(
                    "deepsea_deploy: command dict must have only one key")
        directive = cmd_dict.keys()[0]
        if directive == "stage0":
            config = cmd_dict['stage0']
            target = self._run_stage_0
        elif directive == "stage1":
            config = cmd_dict['stage1']
            target = self._run_stage_1
        elif directive == "policy_cfg":
            config = cmd_dict['policy_cfg']
            target = self._policy_cfg
        elif directive == "stage2":
            config = cmd_dict['stage2']
            target = self._run_stage_2
        elif directive == "stage3":
            config = cmd_dict['stage3']
            target = self._run_stage_3
        else:
            raise ConfigError(
                "deepsea_deploy: unknown directive ->{}<- in command dict"
                .format(directive))
        target(config)

    def _run_command_str(self, cmd, quiet=False):
        if cmd.startswith('health-ok.sh'):
            cmd = "health-ok/" + cmd
            if self.dev_env:
                cmd = "DEV_ENV=true " + cmd
        if not quiet:
            self.log.info("deepsea_deploy: WWWW: running command ->{}<-"
                          .format(cmd))
        self.master_remote.run(args=[
            'sudo',
            'bash',
            '-c',
            cmd,
            ])

    def _run_commands(self):
        for cmd in self.config['commands']:
            self.log.debug("deepsea_deploy: considering command {}"
                           .format(cmd))
            if isinstance(cmd, dict):
                self._run_command_dict(cmd)
            elif isinstance(cmd, str):
                self._run_command_str(cmd)
            else:
                raise ConfigError(
                          "deepsea_deploy: command must be either dict or str")

    def _run_stage(self, stage_num):
        """Run a stage. Dump journalctl on error."""
        self.log.info("deepsea_deploy: WWWW: running Stage {}"
                      .format(stage_num))
        cmd_str = None
        if self.config['cli']:
            cmd_str = (
                'timeout 60m deepsea '
                '--log-file=/var/log/salt/deepsea.log '
                '--log-level=debug '
                'stage run ceph.stage.{} --simple-output'
                ).format(stage_num)
        else:
            cmd_str = (
                'timeout 60m salt-run '
                '--no-color state.orch ceph.stage.{}'
                ).format(stage_num)
        if self.dev_env:
            cmd_str = 'DEV_ENV=true ' + cmd_str
        try:
            self._run_command_str(cmd_str, quiet=True)
        except CommandFailedError:
            self.log.error(
                "deepsea_deploy: WWWW: Stage {} failed. ".format(stage_num)
                + "Here comes journalctl!")
            self.master_remote.run(args=[
                'sudo',
                'journalctl',
                '--all',
                ])
            raise

    def _run_stage_0(self, config):
        """
        Run Stage 0
        """
        if not config:
            config = {}
        check_config_key(config, "update", True)
        check_config_key(config, "reboot", False)
        # FIXME: implement alternative defaults
        if not config['update']:
            remote_run_script_as_root(
                self.master_remote,
                'disable_update_in_stage_0.sh',
                disable_update_in_stage_0
                )
        self._run_stage(0)
        self.sm.all_minions_zypper_status()
        self._salt_api_test()

    def _run_stage_1(self, config):
        """
        Run Stage 1
        """
        if not config:
            config = {}
        self._run_stage(1)

    def _run_stage_2(self, config):
        """
        Run Stage 2
        """
        if not config:
            config = {}
        check_config_key(config, "conf", None)
        self._run_stage(2)
        self._pillar_items()
        self._ceph_conf_small_cluster()
        self._ceph_conf_mon_allow_pool_delete()
        self._ceph_conf_dashboard()
        self._dump_global_conf()
        self._dump_mon_conf()

    def _run_stage_3(self, config):
        """
        Run Stage 3
        """
        if not config:
            config = {}
        self._run_stage(3)
        self._ceph_cluster_status()
        self._ceph_health_test()

    def _salt_api_test(self):
        remote_run_script_as_root(
            self.master_remote,
            'salt_api_test.sh',
            salt_api_test,
            )

    def _set_pillar_deepsea_minions(self):
        """
        Set deepsea_minions pillar value
        """
        echo_cmd = (
            'echo "deepsea_minions: \'*\'" > '
            '/srv/pillar/ceph/deepsea_minions.sls'
        )
        self.master_remote.run(args=[
            'sudo',
            'sh',
            '-c',
            echo_cmd,
            run.Raw(';'),
            'cat',
            '/srv/pillar/ceph/deepsea_minions.sls',
            ])

    def setup(self):
        super(DeepSeaDeploy, self).setup()
        log.debug("beginning of DeepSeaDeploy task setup method...")
        self._copy_health_ok()
        log.debug("end of DeepSeaDeploy task setup...")

    def begin(self):
        super(DeepSeaDeploy, self).begin()
        log.debug("beginning of DeepSeaDeploy task begin method...")
        self._deploy_ceph()
        log.debug("end of DeepSeaDeploy task begin method...")

    def end(self):
        super(DeepSeaDeploy, self).end()
        log.debug("beginning of DeepSeaDeploy task end method...")
        self.sm.gather_logfile('deepsea.log')
        self.sm.gather_logs('ganesha')
        log.debug("end of DeepSeaDeploy task end method...")

    def teardown(self):
        super(DeepSeaDeploy, self).teardown()
#       log.debug("beginning of DeepSeaDeploy task teardown method...")
        pass
#       log.debug("end of DeepSeaDeploy task teardown method...")


task = DeepSeaDeploy
