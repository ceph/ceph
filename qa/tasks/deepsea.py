import logging

from salt_manager import SaltManager
from util import (
    copy_directory_recursively,
    get_remote_for_role,
    sudo_append_to_file,
    )

from teuthology.exceptions import (
    CommandFailedError,
    ConfigError,
    )
from teuthology.misc import (
    sh,
    sudo_write_file,
    write_file,
    )
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)
deepsea_ctx = {}


class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve
    this).

    This task understands the following config keys:

        cli:
            true        deepsea CLI will be used (default)
            false       deepsea CLI will not be used
        repo: (DeepSea git repo, e.g. https://github.com/SUSE/DeepSea.git)
        branch: (DeepSea git branch, e.g. master)
        install:
            package|pkg deepsea will be installed via package system
            source|src  deepsea will be installed via 'make install' (default)

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
        log.debug("beginning of constructor method")
        super(DeepSea, self).__init__(ctx, config)
        if deepsea_ctx:
            log.debug("deepsea_ctx already populated (we are in a subtask)")
            self.cluster_nodes = deepsea_ctx['cluster_nodes']
            self.deepsea_cli = deepsea_ctx['cli']
            self.dev_env = deepsea_ctx['dev_env']
            self.sm = deepsea_ctx['salt_manager_instance']
            self.master_remote = deepsea_ctx['master_remote']
            self.roles = deepsea_ctx['roles']
            self.role_lookup_table = deepsea_ctx['role_lookup_table']
            log.debug("end of constructor method")
            return None
        deepsea_ctx['cli'] = self.config.get('cli', True)
        self.deepsea_cli = deepsea_ctx['cli']
        deepsea_ctx['salt_manager_instance'] = SaltManager(self.ctx)
        self.sm = deepsea_ctx['salt_manager_instance']
        deepsea_ctx['master_remote'] = self.sm.master_remote
        self.master_remote = deepsea_ctx['master_remote']
        deepsea_ctx['roles'] = ctx.config['roles']
        self._introspect_roles(deepsea_ctx)
        self.cluster_nodes = deepsea_ctx['cluster_nodes']
        self.dev_env = deepsea_ctx['dev_env']
        self.role_lookup_table = deepsea_ctx['role_lookup_table']
        if 'install' in self.config:
            if self.config['install'] in ['package', 'pkg']:
                deepsea_ctx['install_method'] = 'package'
            elif self.config['install'] in ['source', 'src']:
                deepsea_ctx['install_method'] = 'source'
            else:
                raise ConfigError(
                        "deepsea: unrecognized install config value ->{}<-"
                        .format(self.config['install'])
                    )
        else:
            if 'repo' in self.config or 'branch' in self.config:
                deepsea_ctx['install_method'] = 'source'
            else:
                deepsea_ctx['install_method'] = 'package'
        # log.debug("ctx.config {}".format(ctx.config))
        log.debug("deepsea context: {}".format(deepsea_ctx))
        log.debug("end of constructor method")

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
        self.master_remote.run(args="pwd ; ls -lR health-ok")
        deepsea_ctx['health_ok_copied'] = True

    def _disable_gpg_checks(self):
        log.info("disabling zypper GPG checks on all test nodes")
        self.master_remote.run(args=(
            "echo \"pre-disable repo state\" ; "
            "zypper lr -upEP ; grep gpg /etc/zypp/repos.d/*"
            ))
        cmd = (
            'sed -i -e \'/gpgcheck/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'/gpgkey/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'$a gpgcheck=0\' /etc/zypp/repos.d/*'
            )
        log.info("zypper repos after disabling GPG checks")
        self.ctx.cluster.run(args=[
            'sudo', 'sh', '-c', cmd
            ])
        self.master_remote.run(args=(
            "echo \"post-disable repo state\" ; "
            "zypper lr -upEP ; grep gpg /etc/zypp/repos.d/*"
            ))

    def _install_deepsea(self):
        install_method = deepsea_ctx['install_method']
        if install_method == 'package':
            self._install_deepsea_using_zypper()
        elif install_method == 'source':
            self._install_deepsea_from_source()
        else:
            raise ConfigError(
                    "deepsea: unrecognized install config value ->{}<-"
                    .format(self.config['install'])
                )
        deepsea_ctx['deepsea_installed'] = True

    def _install_deepsea_from_source(self):
        log.info("WWWW: installing deepsea from source")
        if self.sm.master_rpm_q('deepsea'):
            log.info("DeepSea already installed from RPM")
            return None
        repo = self.config.get('repo', 'https://github.com/SUSE/DeepSea.git')
        branch = self.config.get('branch', 'master')
        log.info("Installing DeepSea from source - repo: {}, branch: {}"
                 .format(repo, branch))
        self.master_remote.run(args=[
            'git',
            '--version',
            run.Raw(';'),
            'git',
            'clone',
            '--branch',
            branch,
            repo,
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
        log.info("Running \"make install\" in DeepSea clone...")
        self.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install',
            ])
        log.info("installing deepsea dependencies...")
        rpmspec_cmd = (
                '$(rpmspec --requires -q DeepSea/deepsea.spec.in 2>/dev/null)'
            )
        self.master_remote.run(args=[
            'sudo',
            'zypper',
            '--non-interactive',
            'install',
            '--no-recommends',
            run.Raw(rpmspec_cmd)
            ])

    def _install_deepsea_using_zypper(self):
        log.info("WWWW: installing DeepSea using zypper")
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

    def _introspect_roles(self, deepsea_ctx):
        """
        Creates the following keys in deepsea_ctx:

            role_nodes,
            cluster_nodes, and
            client_nodes.

        The latter are understood to be client-ONLY nodes, and:

        role_nodes == (cluster_nodes + client_nodes)

        Also sets:

            role_remotes,
            cluster_remotes, and
            client_remotes.

        These are dicts of teuthology "remote" objects, which look like this:

            { remote1_name: remote1_obj, ..., remoten_name: remoten_obj }

        Finally, sets:

            role_lookup_table

        which will look something like this:

            {
                "osd": { "osd.0": osd0remname, ..., "osd.n": osdnremname },
                "mon": { "mon.a": monaremname, ..., "mon.n": monnremname },
                ...
            }

        and

            remote_lookup_table

        which looks like this:

            {
                remote0name: [ "osd.0", "client.0" ],
                ...
                remotenname: [ remotenrole0, ..., remotenrole99 ],
            }

        (In other words, remote_lookup_table is just like the roles
        stanza, except the role lists are keyed by remote name.)
        """
        # initialization phase
        cluster_roles = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'igw', 'ganesha']
        d_ctx = deepsea_ctx
        roles = d_ctx['roles']
        d_ctx['role_nodes'] = len(roles)
        role_remotes = {}
        cluster_remotes = {}
        role_lookup_table = {}
        remote_lookup_table = {}
        # introspection phase
        for node_roles_list in roles:
            assert isinstance(node_roles_list, list), \
                "node_roles_list is a list"
            assert node_roles_list, "node_roles_list is not empty"
            remote = get_remote_for_role(self.ctx, node_roles_list[0])
            log.debug("Considering remote name {}, hostname {}"
                      .format(remote.name, remote.hostname))
            remote_lookup_table[remote.hostname] = node_roles_list
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
                (role_type, _) = role_arr
                remote = get_remote_for_role(self.ctx, role)
                role_remotes[remote.hostname] = remote
                if role_type not in role_lookup_table:
                    role_lookup_table[role_type] = {}
                role_lookup_table[role_type][role] = remote.hostname
                if role_type in cluster_roles:
                    cluster_remotes[remote.hostname] = remote
        cluster_nodes = len(cluster_remotes)
        client_remotes = role_remotes
        for remote_name, _ in cluster_remotes.iteritems():
            del(client_remotes[remote_name])
        deepsea_ctx['client_nodes'] = len(client_remotes)
        deepsea_ctx['dev_env'] = True if cluster_nodes < 4 else False
        # report phase
        log.info("ROLE INTROSPECTION REPORT")
        assign_vars = [
            'cluster_nodes',
            'role_remotes',
            'cluster_remotes',
            'client_remotes',
            'role_lookup_table',
            'remote_lookup_table',
            ]
        for var in assign_vars:
            exec("deepsea_ctx['{var}'] = {var}".format(var=var))
        report_vars = [
            'role_nodes',
            'cluster_nodes',
            'client_nodes',
            'role_remotes',
            'cluster_remotes',
            'client_remotes',
            'role_lookup_table',
            'remote_lookup_table',
            'dev_env',
            ]
        for var in report_vars:
            log.info("{} == {}".format(var, deepsea_ctx[var]))

    def _purge_osds(self):
        # needed as long as teuthology install task purges /var/lib/ceph
        # in its teardown phase
        for _remote in self.ctx.cluster.remotes.iterkeys():
            log.info("stopping OSD services on {}"
                     .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            log.info("unmounting OSD partitions on {}"
                     .format(_remote.hostname))
            # unmount up to five OSDs
            # bluestore XFS partition is vd?1
            # filestore XFS partition is vd?2
            for_loop = (
                    'for f in vdb{pn} vdc{pn} vdd{pn} vde{pn} vdf{pn} ; '
                    'do test -b /dev/$f && umount /dev/$f || true ; '
                    'done'
                )
            for pn in [1, 2]:
                _remote.run(args=['sudo', 'sh', '-c', for_loop.format(pn=pn)])

    # Teuthology iterates through the tasks stanza twice: once to "execute"
    # the tasks and a second time to "unwind" them. During the first pass
    # it pushes each task onto a stack, and during the second pass it "unwinds"
    # the stack, with the result being that the tasks are unwound in reverse
    # order. During the execution phase it calls three methods: the
    # constructor, setup(), and begin() - in that order -, and during the
    # unwinding phase it calls end() and teardown() - in that order.

    # The task does not have to implement any of the methods. If not
    # implemented, the method in question will be called via inheritance.
    # If a method _is_ implemented, the implementation can optionally call
    # the parent's implementation of that method as well. This is illustrated
    # here:
    def setup(self):
        # log.debug("beginning of setup method")
        super(DeepSea, self).setup()
        pass
        # log.debug("end of setup method")

    def begin(self):
        log.debug("beginning of begin method")
        super(DeepSea, self).begin()
        if 'health_ok_copied' not in deepsea_ctx:
            self._copy_health_ok()
            assert deepsea_ctx['health_ok_copied']
        if 'deepsea_installed' not in deepsea_ctx:
            self._disable_gpg_checks()
            self._install_deepsea()
            assert deepsea_ctx['deepsea_installed']
        log.debug("end of begin method")

    def end(self):
        # log.debug("beginning of end method")
        super(DeepSea, self).end()
        pass
        # log.debug("end of end method")

    def teardown(self):
        # log.debug("beginning of teardown method")
        super(DeepSea, self).teardown()
        pass
        # log.debug("end of teardown method")


class CephConf(DeepSea):

    ceph_conf_d = '/srv/salt/ceph/configuration/files/ceph.conf.d'

    customize = {
        "client": "client.conf",
        "global": "global.conf",
        "mds": "mds.conf",
        "mgr": "mgr.conf",
        "mon": "mon.conf",
        "osd": "osd.conf",
        }

    def __init__(self, ctx, config):
        self.logger = log.getChild('ceph_conf')
        self.logger.debug("beginning of constructor method")
        super(CephConf, self).__init__(ctx, config)
        self.logger.debug("munged config is {}".format(self.config))
        self.logger.debug("end of constructor method")

    def _ceph_conf_d_full_path(self, component):
        if component in self.customize.keys():
            return "{}/{}".format(self.ceph_conf_d, self.customize[component])

    def _dashboard(self):
        info_msg = "adjusted ceph.conf for deployment of dashboard MGR module"
        data = "mgr initial modules = dashboard\n"
        sudo_append_to_file(
            self.master_remote,
            self._ceph_conf_d_full_path("mon"),
            data,
            )
        self.logger.info(info_msg)

    def _dump_customizations(self):
        for component in self.customize.keys():
            path = self._ceph_conf_d_full_path(component)
            path_exists = True
            try:
                self.master_remote.run(
                    args="test -f {}".format(path)
                )
            except CommandFailedError:
                self.logger.info(
                    "{} does not exist on the remote".format(path)
                    )
                continue
            if path_exists:
                self.master_remote.run(args=[
                    'ls',
                    '-l',
                    path,
                    run.Raw(';'),
                    'cat',
                    path,
                    ])
            else:
                self.logger.info("no {} file on remote".format(path))

    def _maybe_a_small_cluster(self):
        """
        Apply necessary ceph.conf for small clusters
        """
        info_msg = (
            "adjusting ceph.conf for operation with {} storage node(s)"
            .format(self.cluster_nodes)
            )
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
            self.logger.info(info_msg)
            sudo_append_to_file(
                self.master_remote,
                self._ceph_conf_d_full_path("global"),
                data,
                )

    def _mon_allow_pool_delete(self):
        info_msg = "adjusted ceph.conf to allow pool deletes"
        data = "mon allow pool delete = true\n"
        sudo_append_to_file(
            self.master_remote,
            self._ceph_conf_d_full_path("mon"),
            data,
            )
        self.logger.info(info_msg)

    def setup(self):
        self.logger.debug("beginning of setup method")
        if self.config.get('dashboard', True):
            self._dashboard()
        if self.config.get('mon_allow_pool_delete', True):
            self._mon_allow_pool_delete()
        if self.config.get('small_cluster', True):
            self._maybe_a_small_cluster()
        self.logger.debug("end of setup method")

    def begin(self):
        self.logger.debug("beginning of begin method")
        # TODO: custom ceph conf
        self._dump_customizations()
        self.logger.debug("end of begin method")


class Dummy(DeepSea):

    def __init__(self, ctx, config):
        self.logger = logging.getLogger('task.deepsea.dummy')
        self.logger.debug("beginning of constructor method")
        pass
        self.logger.debug("end of constructor method")

    def begin(self):
        self.logger.debug("beginning of begin method")
        self.logger.info("deepsea_ctx == {}".format(deepsea_ctx))
        self.logger.debug("end of begin method")


class Deploy:

    pass


class Orch(DeepSea):

    all_stages = [
        "0", "prep", "1", "discovery", "2", "configure", "3", "deploy",
        "4", "services", "5", "removal", "cephfs", "ganesha", "iscsi",
        "openattic", "openstack", "radosgw", "validate"
        ]

    stage_synonyms = {
        0: 'prep',
        1: 'discovery',
        2: 'configure',
        3: 'deploy',
        4: 'services',
        5: 'removal',
        }

    def __init__(self, ctx, config):
        self.logger = log.getChild('orch')
        self.logger.debug("beginning of constructor method")
        self.stage = config.get("stage", '')
        self.state_orch = config.get("state_orch", '')
        if not str(self.stage) and not self.state_orch:
            raise ConfigError(
                "deepsea.orch: nothing to do. Missing 'stage' or 'state_orch' "
                "key in config dict ->{}<-".format(self.config)
                )
        if str(self.stage) and str(self.stage) not in self.all_stages:
            raise ConfigError("unrecognized Stage ->{}<-".format(self.stage))
        super(Orch, self).__init__(ctx, config)
        self.scripts = Scripts(self.master_remote, self.logger)
        self.logger.debug("end of constructor method")

    def __log_stage_start(self, stage):
        self.logger.info(
            "WWWW: running Stage {} ({})"
            .format(stage, self.stage_synonyms[stage])
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

    def _is_int_between_0_and_5(self, num):
        try:
            num = int(num)
        except ValueError:
            return False
        if num < 0 or num > 5:
            return False
        return True

    # FIXME: run on each minion individually, and compare deepsea "roles"
    # with teuthology roles!
    def _pillar_items(self):
        self.master_remote.run(args=[
            'sudo',
            'salt',
            '*',
            'pillar.items',
            ])

    def _run_orch(self, orch_tuple):
        """Run an orchestration. Dump journalctl on error."""
        orch_type, orch_spec = orch_tuple
        if orch_type == 'orch':
            pass
        elif orch_type == 'stage':
            orch_spec = 'ceph.stage.{}'.format(orch_spec)
        else:
            raise ConfigError(
                "Unrecognized orchestration type ->{}<-"
                .format(orch_type)
                )
        cmd_str = None
        if self.deepsea_cli:
            cmd_str = (
                'timeout 60m deepsea '
                '--log-file=/var/log/salt/deepsea.log '
                '--log-level=debug '
                'stage run {} --simple-output'
                ).format(orch_spec)
        else:
            cmd_str = (
                'timeout 60m salt-run '
                '--no-color state.orch {}'
                ).format(orch_spec)
        if self.dev_env:
            cmd_str = 'DEV_ENV=true ' + cmd_str
        try:
            self.master_remote.run(args=[
                'sudo', 'bash', '-c', cmd_str,
                ])
        except CommandFailedError:
            log.error(
                "WWWW: orchestration {} failed. ".format(orch_spec)
                + "Here comes journalctl!"
                )
            self.master_remote.run(args=[
                'sudo',
                'journalctl',
                '--all',
                ])
            raise

    def _run_stage_0(self):
        """
        Run Stage 0
        """
        stage = 0
        self.__log_stage_start(stage)
        update = self.config.get("update", True)
        reboot = self.config.get("reboot", False)
        # FIXME: implement alternative defaults
        if not update:
            self.scripts.disable_update_in_stage_0()
        if reboot:
            log.warning("Stage {} reboot not supported".format(stage))
        self._run_orch(("stage", stage))
        self.sm.all_minions_zypper_status()
        self.scripts.salt_api_test()

    def _run_stage_1(self):
        """
        Run Stage 1
        """
        stage = 1
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))

    def _run_stage_2(self):
        """
        Run Stage 2
        """
        stage = 2
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self._pillar_items()

    def _run_stage_3(self):
        """
        Run Stage 3
        """
        stage = 3
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self.scripts.ceph_cluster_status()
        self._ceph_health_test()

    def _run_stage_4(self):
        """
        Run Stage 4
        """
        stage = 4
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))

    def _run_stage_5(self):
        """
        Run Stage 5
        """
        stage = 5
        self.__log_stage_start(stage)
        self._run_orch(("stage", 5))

    def begin(self):
        self.logger.debug("beginning of begin method")
        if self.state_orch:
            self.logger.info(
                "WWWW: running orchestration {}".format(self.state_orch)
                )
            self._run_orch(("orch", self.state_orch))
            self.logger.debug("end of begin method")
            return None
        # it's not an orch, so it must be a stage
        assert str(self.stage), "Neither state_orch, nor stage"
        if self._is_int_between_0_and_5(self.stage):
            exec('self._run_stage_{}()'.format(self.stage))
        elif self.stage == 'prep':
            self.logger.info("Running Stage 0 instead of Stage \"prep\"")
            self._run_stage_0()
        elif self.stage == 'discovery':
            self.logger.info("Running Stage 1 instead of Stage \"discovery\"")
            self._run_stage_1()
        elif self.stage == 'configure':
            self.logger.info("Running Stage 2 instead of Stage \"configure\"")
            self._run_stage_2()
        elif self.stage == 'deploy':
            self.logger.info("Running Stage 3 instead of Stage \"deploy\"")
            self._run_stage_3()
        elif self.stage == 'services':
            self.logger.info("Running Stage 4 instead of Stage \"services\"")
            self._run_stage_4()
        elif self.stage == 'removal':
            self.logger.info("Running Stage 5 instead of Stage \"removal\"")
            self._run_stage_5()
        elif self.stage in self.all_stages:
            self.logger.info(
                "Running non-numeric Stage \"{}\"".format(self.stage)
                )
            self._run_orch(("stage", self.stage))
        else:
            raise ConfigError('unsupported stage ->{}<-'.format(self.stage))
        self.logger.debug("end of begin method")


class Policy(DeepSea):

    proposals_dir = "/srv/pillar/ceph/proposals"

    def __init__(self, ctx, config):
        self.logger = log.getChild('policy')
        self.logger.debug("beginning of constructor method")
        self.profile = config.get("profile", "teuthology")
        super(Policy, self).__init__(ctx, config)
        self.logger.debug("end of constructor method")

    def __build_profile_x(self, profile):
        self.profile_ymls_to_dump = []
        no_osd_roles = ("deepsea_deploy: no osd roles configured"
                        ", but at least one of these is required.")
        role_dict = {}
        if 'osd' in self.role_lookup_table:
            role_dict = self.role_lookup_table['osd']
        else:
            raise ConfigError(no_osd_roles)
        self.logger.debug(
            "generating policy.cfg lines for osd profile ->{}<- based on {}"
            .format(profile, role_dict)
            )
        if len(role_dict) == 0:
            raise ConfigError(no_osd_roles)
        osd_remotes = list(set(role_dict.values()))
        for osd_remote in osd_remotes:
            self.logger.debug(
                "{} has one or more osd roles".format(osd_remote)
                )
            self.policy_cfg += """# Storage profile - {remote}
profile-{profile}/cluster/{remote}.sls
""".format(remote=osd_remote, profile=profile)
            ypp = ("profile-{}/stack/default/ceph/minions/{}.yml"
                   .format(profile, osd_remote))
            self.policy_cfg += ypp + "\n"
            self.profile_ymls_to_dump.append(
                "{}/{}".format(self.proposals_dir, ypp))

    def _build_base(self):
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

    def _build_profile(self):
        """
        Add storage profile to policy.cfg
        """
        if not isinstance(self.profile, str):
            raise ConfigError(
                "deepsea.policy.build_profile: "
                "profile config param must be a string"
                )
        if self.profile == 'teuthology':
            raise ConfigError(
                "deepsea.policy._build_profile: "
                "\"teuthology\" profile not implemented yet"
                )
        elif self.profile == 'default':
            self.__build_profile_x('default')
        else:
            ConfigError(
                "deepsea_deploy post-Stage 1: unknown profile ->{}<-"
                .format(self.profile)
                )

    def _cat_policy_cfg(self):
        """
        Dump the remote policy.cfg file to teuthology log.
        """
        self.master_remote.run(args=[
            'cat',
            self.proposals_dir + "/policy.cfg"
            ])

    def _build_x(self, role_type, required=False):
        no_roles_of_type = "no {} roles configured".format(role_type)
        but_required = ", but at least one of these is required."
        role_dict = {}
        if role_type in self.role_lookup_table:
            role_dict = self.role_lookup_table[role_type]
        elif required:
            raise ConfigError(no_roles_of_type + but_required)
        else:
            self.logger.debug(no_roles_of_type)
            return None
        self.logger.debug(
            "generating policy.cfg lines for {} based on {}"
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

    def _dump_profile_ymls(self):
        """
        Dump profile yml files that have been earmarked for dumping.
        """
        for yml_file in self.profile_ymls_to_dump:
            self.master_remote.run(args=[
                'sudo',
                'cat',
                yml_file,
                ])

    def _dump_proposals_dir(self):
        """
        Dump the entire proposals directory hierarchy to the teuthology log.
        """
        self.master_remote.run(args=[
            'test',
            '-d',
            self.proposals_dir,
            run.Raw(';'),
            'ls',
            '-lR',
            self.proposals_dir + '/',
            ])

    def _write_policy_cfg(self):
        """
        Write policy_cfg to master remote.
        """
        sudo_write_file(
            self.master_remote,
            self.proposals_dir + "/policy.cfg",
            self.policy_cfg,
            perms="0644",
            owner="salt",
            )
        self.master_remote.run(args=[
            "ls",
            "-l",
            self.proposals_dir + "/policy.cfg"
            ])

    def begin(self):
        """
        Generate policy.cfg from the results of role introspection
        """
        self.logger.debug("beginning of begin method")
        self.logger.info("WWWW: generating policy.cfg")
        self._dump_proposals_dir()
        self._build_base()
        self._build_x('mon', required=True)
        self._build_x('mgr', required=True)
        self._build_x('mds')
        self._build_x('rgw')
        self._build_x('igw')
        self._build_x('ganesha')
        self._build_profile()
        self._write_policy_cfg()
        self._cat_policy_cfg()
        self._dump_profile_ymls()
        self.logger.debug("end of begin method")


class CreatePools(DeepSea):

    def __init__(self, ctx, config):
        self.logger = log.getChild('create_pools')
        self.logger.debug("beginning of constructor method")
        self.logger.debug("initial config is {}".format(config))
        super(CreatePools, self).__init__(ctx, config)
        self.logger.debug(
            "post-parent-constructor config is {}".format(self.config)
            )
        self.logger.debug("munged config is {}".format(self.config))
        self.args = []
        if isinstance(self.config, list):
            self.args = self.config
        elif isinstance(self.config, dict):
            self.config = {
                "mds": self.config.get("mds", False),
                "openstack": self.config.get("openstack", False),
                "rbd": self.config.get("rbd", False),
                }
        else:
            self.config = {
                "openstack": False,
                "rbd": False
                }
        if not self.args:
            for key in self.config:
                if self.config[key] is None:
                    self.config[key] = True
                if self.config[key]:
                    self.args.append(key)
        if 'mds' in self.role_lookup_table:
            self.args.append('mds')
        self.args = list(set(self.args))
        self.scripts = Scripts(self.master_remote, self.logger)
        self.logger.debug("end of constructor method")

    def begin(self):
        self.logger.debug("beginning of begin method")
        self.scripts.create_all_pools_at_once(self.args)
        self.logger.debug("end of begin method")


class Preflight(DeepSea):

    def __init__(self, ctx, config):
        self.logger = log.getChild('preflight')
        self.logger.debug("beginning of constructor method")
        super(Preflight, self).__init__(ctx, config)
        self.logger.debug("end of constructor method")

    def _deepsea_version(self):
        if self.deepsea_cli:
            try:
                self.master_remote.run(args=[
                    'type',
                    'deepsea',
                    run.Raw('>'),
                    '/dev/null',
                    run.Raw('2>&1'),
                    ])
            except CommandFailedError:
                raise ConfigError(
                    "_deepsea_version: test case calls for deepsea CLI, "
                    "but it is not installed"
                    )
            self.master_remote.run(args='deepsea --version')
        else:
            self.master_remote.run(args="sudo salt-run deepsea.version")

    def _deepsea_minions(self):
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
            self.logger.info(
                '{} not installed on master node'.format(python_binary)
                )
        return installed

    def begin(self):
        self.logger.debug("beginning of begin method")
        self.logger.info("WWWW: preflight sequence")
        self.sm.master_rpm_q('ceph')
        self.sm.master_rpm_q('ceph-test')
        self.sm.master_rpm_q('salt-master')
        self.sm.master_rpm_q('salt-minion')
        self.sm.master_rpm_q('salt-api')
        self._master_python_version(2)
        if not self._master_python_version(3):
            raise ConfigError("Python 3 not installed on master node"
                              " - bailing out!")
        self._deepsea_version()
        self._deepsea_minions()
        # Stage 0 does this, but we have no guarantee Stage 0 will run
        self.sm.sync_pillar_data()
        self.logger.debug("end of begin method")

    def teardown(self):
        self.logger.debug("beginning of teardown method")
        super(DeepSea, self).teardown()
        #
        # the install task does "rm -r /var/lib/ceph" on every test node,
        # and that fails when there are OSDs running
        self._purge_osds()
        self.logger.debug("end of teardown method")


class Scripts:

    script_dict = {
        "ceph_cluster_status": """# Display ceph cluster status
set -ex
ceph pg stat -f json-pretty
ceph health detail -f json-pretty
ceph osd tree
ceph osd pool ls detail -f json-pretty
ceph -s
""",
        "create_all_pools_at_once": """# Pre-create Stage 4 pools
# with calculated number of PGs so we don't get health warnings
# during/after Stage 4 due to "too few" or "too many" PGs per OSD

set -ex

function json_total_osds {
    # total number of OSDs in the cluster
    ceph osd ls --format json | jq '. | length'
}

function pgs_per_pool {
    local TOTALPOOLS=$1
    test -n "$TOTALPOOLS"
    local TOTALOSDS=$(json_total_osds)
    test -n "$TOTALOSDS"
    # given the total number of pools and OSDs,
    # assume triple replication and equal number of PGs per pool
    # and aim for 100 PGs per OSD
    let "TOTALPGS = $TOTALOSDS * 100"
    let "PGSPEROSD = $TOTALPGS / $TOTALPOOLS / 3"
    echo $PGSPEROSD
}

function create_all_pools_at_once {
    # sample usage: create_all_pools_at_once foo bar
    local TOTALPOOLS="${#@}"
    local PGSPERPOOL=$(pgs_per_pool $TOTALPOOLS)
    for POOLNAME in "$@"
    do
        ceph osd pool create $POOLNAME $PGSPERPOOL $PGSPERPOOL replicated
    done
    ceph osd pool ls detail
}

MDS=""
OPENSTACK=""
RBD=""
for arg in "$@" ; do
    arg="${arg,,}"
    case "$arg" in
        mds) MDS="$arg" ;;
        openstack) OPENSTACK="$arg" ;;
        rbd) RBD="$arg" ;;
    esac
done

POOLS="write_test"
test "$MDS" && POOLS+=" cephfs_data cephfs_metadata"
if [ "$OPENSTACK" ] ; then
    ADD_POOLS="smoketest-cloud-backups
smoketest-cloud-volumes
smoketest-cloud-images
smoketest-cloud-vms
cloud-backups
cloud-volumes
cloud-images
cloud-vms
"
    for add_pool in $ADD_POOLS ; do
        POOLS+=" $add_pool"
    done
fi
test "$RBD" && POOLS+=" rbd"
create_all_pools_at_once $POOLS
ceph osd pool application enable write_test deepsea_qa
""",
        "disable_update_in_stage_0": """# Disable update in Stage 0
set -ex
cp /srv/salt/ceph/stage/prep/master/default.sls \
   /srv/salt/ceph/stage/prep/master/default-orig.sls
cp /srv/salt/ceph/stage/prep/master/default-no-update-no-reboot.sls \
   /srv/salt/ceph/stage/prep/master/default.sls
cp /srv/salt/ceph/stage/prep/minion/default.sls \
   /srv/salt/ceph/stage/prep/minion/default-orig.sls
cp /srv/salt/ceph/stage/prep/minion/default-no-update-no-reboot.sls \
   /srv/salt/ceph/stage/prep/minion/default.sls
""",
        "salt_api_test": """# Salt API test script
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
""",
        }

    def __init__(self, master_remote, logger):
        self.log = logger
        self.master_remote = master_remote

    def _remote_run_script_as_root(self, remote, path, data, args=None):
        """
        Wrapper around write_file to simplify the design pattern:
        1. use write_file to create bash script on the remote
        2. use Remote.run to run that bash script via "sudo bash $SCRIPT"
        """
        write_file(remote, path, data)
        cmd = 'sudo bash {}'.format(path)
        if args:
            cmd += ' ' + ' '.join(args)
        remote.run(label=path, args=[
            'sudo', 'bash', '-c', cmd
            ])

    def ceph_cluster_status(self):
        self._remote_run_script_as_root(
            self.master_remote,
            'ceph_cluster_status.sh',
            self.script_dict["ceph_cluster_status"],
            )

    def create_all_pools_at_once(self, args):
        self.log.info("creating pools: {}".format(' '.join(args)))
        self._remote_run_script_as_root(
            self.master_remote,
            'create_all_pools_at_once.sh',
            self.script_dict["create_all_pools_at_once"],
            args=args,
            )

    def disable_update_in_stage_0(self):
        self._remote_run_script_as_root(
            self.master_remote,
            'disable_update_in_stage_0.sh',
            self.script_dict["disable_update_in_stage_0"],
            )

    def salt_api_test(self):
        self._remote_run_script_as_root(
            self.master_remote,
            'salt_api_test.sh',
            self.script_dict["salt_api_test"],
            )


task = DeepSea
ceph_conf = CephConf
create_pools = CreatePools
deploy = Deploy
dummy = Dummy
orch = Orch
policy = Policy
preflight = Preflight
