'''
Task that deploys a Ceph cluster using DeepSea
'''
import logging

from salt_manager import SaltManager
from teuthology.exceptions import (CommandFailedError, ConfigError)
from teuthology.orchestra import run
from teuthology.misc import sh, write_file
from teuthology.task import Task
from util import (
        check_config_key,
        copy_directory_recursively,
        get_remote_for_role
    )

log = logging.getLogger(__name__)
health_ok_cmd = "health-ok.sh --teuthology"
cluster_roles = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'igw', 'ganesha']
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
        self.log.info("client_remotes == {}".format(self.client_remotes))
        self.log.info("role_remotes == {}".format(self.role_remotes))
        self.log.info("cluster_remotes == {}".format(self.cluster_remotes))
        self.log.info("client_nodes == {}".format(self.client_nodes))
        self.log.info("role_lookup_table == {}".format(self.role_lookup_table))
        self.log.info("remote_lookup_table == {}"
                      .format(self.remote_lookup_table))
        self.log.info("dev_env == {}".format(self.dev_env))

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

    def __run_stage(self, stage_num):
        """Run a stage. Dump journalctl on error."""
        self.log.info("WWWW: Running DeepSea Stage {}".format(stage_num))
        try:
            if self.config['cli']:
                self.__run_command_str(
                    (
                        'timeout 60m deepsea '
                        '--log-file=/var/log/salt/deepsea.log '
                        '--log-level=debug '
                        'stage run ceph.stage.{} --simple-output'
                    ).format(stage_num)
                )
            else:
                self.__run_command_str(
                    (
                        'timeout 60m salt-run --no-color '
                        'state.orch ceph.stage.{}'
                    ).format(stage_num)
                )
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

    def _salt_api_test(self):
        write_file(self.master_remote, 'salt_api_test.sh', salt_api_test)
        self.master_remote.run(args=[
            'bash',
            'salt_api_test.sh',
            ])

    def __run_stage_0(self, config):
        """
        Run Stage 0
        """
        if not config:
            config = {}
        check_config_key(config, "update", True)
        check_config_key(config, "reboot", False)
        # FIXME: implement alternative defaults
        self.__run_stage(0)
        self.sm.all_minions_zypper_ps()
        self._salt_api_test()

    def __run_command_dict(self, cmd_dict):
        """
        Process commands given in form of dict - example:

            commands:
            - stage0:
                update: true,
                reboot: false
        """
        if len(cmd_dict.keys()) != 1:
            raise ConfigError(
                    "deepsea_deploy: command dict must have only one key")
        directive = cmd_dict.keys()[0]
        if directive == "stage0":
            self.__run_stage_0(cmd_dict['stage0'])
        else:
            raise ConfigError(
                    "deepsea_deploy: unknown directive ->{}<- in command dict"
                    .format(directive))

    def __run_command_str(self, cmd):
        if cmd.startswith('health-ok.sh'):
            cmd = "health-ok/" + cmd
        if self.dev_env:
            cmd = "DEV_ENV=true " + cmd
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
                self.__run_command_dict(cmd)
            elif isinstance(cmd, str):
                self.__run_command_str(cmd)
            else:
                raise ConfigError(
                          "deepsea_deploy: command must be either dict or str")

    def _deploy_ceph(self):
        self._initialization_sequence()
        self._run_commands()

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
