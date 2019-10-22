"""
Task (and subtasks) for automating deployment of Ceph using DeepSea

Linter:
    flake8 --max-line-length=100
"""
import logging
import time
import yaml

from salt_manager import SaltManager
from scripts import Scripts
from teuthology import misc
from util import (
    copy_directory_recursively,
    enumerate_osds,
    get_remote_for_role,
    get_rpm_pkg_version,
    introspect_roles,
    remote_exec,
    remote_run_script_as_root,
    sudo_append_to_file,
    )

from teuthology.exceptions import (
    CommandFailedError,
    ConfigError,
    )
from teuthology.orchestra import run
from teuthology.task import Task
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)
deepsea_ctx = {}
proposals_dir = "/srv/pillar/ceph/proposals"
reboot_tries = 30


def anchored(log_message):
    global deepsea_ctx
    assert 'log_anchor' in deepsea_ctx, "deepsea_ctx not populated"
    return "{}{}".format(deepsea_ctx['log_anchor'], log_message)


def dump_file_that_might_not_exist(remote, fpath):
    try:
        remote.run(args="cat {}".format(fpath))
    except CommandFailedError:
        pass


class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve
    this).

    This task understands the following config keys which apply to
    this task and all its subtasks:

        allow_python2:  (default: True)
                        whether to continue if Python 2 is installed anywhere
                        in the test cluster
        alternative_defaults: (default: empty)
                        a dictionary of DeepSea alternative defaults
                        to be activated via the Salt Pillar
        cli:
            true        deepsea CLI will be used (the default)
            false       deepsea CLI will not be used
        dashboard_ssl:
            true        deploy MGR dashboard module with SSL (the default)
            false       deploy MGR dashboard module *without* SSL
        log_anchor      a string (default: "WWWW: ") which will precede
                        log messages emitted at key points during the
                        deployment
        quiet_salt:
            true        suppress stderr on salt commands (the default)
            false       let salt commands spam the log
        rgw_ssl:
            true        use SSL if RGW is deployed
            false       if RGW is deployed, do not use SSL (the default)
        drive_group:
            default     if a teuthology osd role is present on a node,
                        DeepSea will tell ceph-volume to make all available
                        disks into standalone OSDs
            teuthology  populate DeepSea storage profile for 1:1 mapping
                        between teuthology osd roles and actual osds
                        deployed (the default, but not yet implemented)
            (dict)      a dictionary is assumed to be a custom drive group
                        (yaml blob) to be passed verbatim to ceph-volume

    This task also understands the following config keys that affect
    the behavior of just this one task (no effect on subtasks):

        repo: (git repo for initial DeepSea install, e.g.
              "https://github.com/SUSE/DeepSea.git")
        branch: (git branch for initial deepsea install, e.g. "master")
        install:
            package|pkg deepsea will be installed via package system
            source|src  deepsea will be installed via 'make install' (default)
        upgrade_install:
            package|pkg post-upgrade deepsea will be installed via package system
            source|src  post-upgrade deepsea will be installed via 'make install' (default)
        upgrade_repo: (git repo for DeepSea re-install/upgrade - used by second
                      invocation of deepsea task only)
        upgrade_branch: (git branch for DeepSea re-install/upgrade - used by
                        second invocation of deepsea task only)

    Example:

        tasks
        - deepsea:
            repo: https://github.com/SUSE/DeepSea.git
            branch: wip-foo
            install: source

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    err_prefix = "(deepsea task) "

    log_anchor_str = "WWWW: "

    def __init__(self, ctx, config):
        global deepsea_ctx
        super(DeepSea, self).__init__(ctx, config)
        if deepsea_ctx:
            # context already populated (we are in a subtask, or a
            # re-invocation of the deepsea task)
            self.log = deepsea_ctx['logger_obj']
            if type(self).__name__ == 'DeepSea':
                # The only valid reason for a second invocation of the deepsea
                # task is to upgrade DeepSea (actually reinstall it)
                deepsea_ctx['reinstall_deepsea'] = True
                # deepsea_ctx['install_method'] is the _initial_ install method from the
                # first invocation. If initial install was from package, the
                # package must be removed for reinstall from source to work.
                # If reinstall method is 'package', removing the package here
                # will not hurt anything.
                if deepsea_ctx['install_method'] == 'package':
                    deepsea_ctx['master_remote'].run(args=[
                        'sudo',
                        'zypper',
                        '--non-interactive',
                        '--no-gpg-checks',
                        'remove',
                        'deepsea',
                        'deepsea-qa',
                        run.Raw('||'),
                        'true'
                        ])
                install_key = 'install'
                upgrade_install = self.config.get('upgrade_install', '')
                if upgrade_install:
                    install_key = 'upgrade_install'
                self.__populate_install_method_basic(install_key)
        if not deepsea_ctx:
            # populating context (we are *not* in a subtask)
            deepsea_ctx['logger_obj'] = log
            self.ctx['roles'] = self.ctx.config['roles']
            self.log = log
            self._populate_deepsea_context()
            introspect_roles(self.ctx, self.log, quiet=False)
        self.allow_python2 = deepsea_ctx['allow_python2']
        self.alternative_defaults = deepsea_ctx['alternative_defaults']
        self.dashboard_ssl = deepsea_ctx['dashboard_ssl']
        self.deepsea_cli = deepsea_ctx['cli']
        self.dev_env = self.ctx['dev_env']
        self.install_method = deepsea_ctx['install_method']
        self.log_anchor = deepsea_ctx['log_anchor']
        self.master_remote = deepsea_ctx['master_remote']
        self.nodes = self.ctx['nodes']
        self.nodes_storage = self.ctx['nodes_storage']
        self.nodes_storage_only = self.ctx['nodes_storage_only']
        self.quiet_salt = deepsea_ctx['quiet_salt']
        self.remotes = self.ctx['remotes']
        self.reinstall_deepsea = deepsea_ctx.get('reinstall_deepsea', False)
        self.repositories = deepsea_ctx['repositories']
        self.rgw_ssl = deepsea_ctx['rgw_ssl']
        self.roles = self.ctx['roles']
        self.role_types = self.ctx['role_types']
        self.role_lookup_table = self.ctx['role_lookup_table']
        self.scripts = Scripts(self.ctx, self.log)
        self.sm = deepsea_ctx['salt_manager_instance']
        self.drive_group = deepsea_ctx['drive_group']
        # self.log.debug("ctx.config {}".format(ctx.config))
        # self.log.debug("deepsea context: {}".format(deepsea_ctx))

    def __install_deepsea_from_source(self):
        info_msg_prefix = 'Reinstalling' if self.reinstall_deepsea else 'Installing'
        info_msg = info_msg_prefix + ' deepsea from source'
        self.log.info(anchored(info_msg))
        if self.sm.master_rpm_q('deepsea'):
            self.log.info("DeepSea already installed from RPM")
            return None
        upgrade_repo = self.config.get('upgrade_repo', '')
        upgrade_branch = self.config.get('upgrade_branch', '')
        repo = self.config.get('repo', 'https://github.com/SUSE/DeepSea.git')
        branch = self.config.get('branch', 'master')
        if self.reinstall_deepsea:
            if upgrade_repo:
                repo = upgrade_repo
            if upgrade_branch:
                branch = upgrade_branch
        self.log.info(
            "{} - repo: {}, branch: {}"
            .format(info_msg, repo, branch)
            )
        self.master_remote.run(args=[
            'sudo',
            'rm',
            '-rf',
            'DeepSea',
            run.Raw(';'),
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

    def __install_deepsea_using_zypper(self):
        info_msg_prefix = 'Reinstalling' if self.reinstall_deepsea else 'Installing'
        info_msg = info_msg_prefix + ' deepsea using zypper'
        self.log.info(anchored(info_msg))
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

    def _deepsea_minions(self):
        """
        Set deepsea_minions pillar value
        """
        deepsea_minions_sls = '/srv/pillar/ceph/deepsea_minions.sls'
        content = "deepsea_minions: \'*\'"
        self.log.info("Clobbering {} with content ->{}<-".format(
            deepsea_minions_sls, content))
        cmd = 'sudo tee {}'.format(deepsea_minions_sls)
        self.master_remote.sh(cmd, stdin=content)

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
                raise ConfigError(self.err_prefix + "Test case calls for "
                                  "deepsea CLI, but it is not installed")
            self.master_remote.run(args='deepsea --version')
        else:
            cmd_str = "sudo salt-run deepsea.version"
            if self.quiet_salt:
                cmd_str += " 2>/dev/null"
            self.master_remote.run(args=cmd_str)

    def _disable_gpg_checks(self):
        cmd = (
            'sed -i -e \'/gpgcheck/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'/gpgkey/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'$a gpgcheck=0\' /etc/zypp/repos.d/*'
            )
        self.ctx.cluster.run(args=[
            'sudo', 'sh', '-c', cmd
            ])

    def _install_deepsea(self):
        global deepsea_ctx
        install_method = deepsea_ctx['install_method']
        if install_method == 'package':
            self.__install_deepsea_using_zypper()
        elif install_method == 'source':
            self.__install_deepsea_from_source()
        else:
            raise ConfigError(self.err_prefix + "internal error")
        deepsea_ctx['deepsea_installed'] = True

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
            self.log.info(
                '{} not installed on master node'.format(python_binary)
                )
        return installed

    def _maybe_apply_alternative_defaults(self):
        global_yml = '/srv/pillar/ceph/stack/global.yml'
        if self.alternative_defaults:
            self.log.info(anchored("Applying alternative defaults"))
            data = ''
            for k, v in self.alternative_defaults.items():
                data += "{}: {}\n".format(k, v)
            if data:
                sudo_append_to_file(
                    self.master_remote,
                    global_yml,
                    data,
                    )
        dump_file_that_might_not_exist(self.master_remote, global_yml)

    def _populate_deepsea_context(self):
        global deepsea_ctx
        deepsea_ctx['allow_python2'] = self.config.get('allow_python2', True)
        deepsea_ctx['alternative_defaults'] = self.config.get('alternative_defaults', {})
        if not isinstance(deepsea_ctx['alternative_defaults'], dict):
            raise ConfigError(self.err_prefix + "alternative_defaults must be a dict")
        deepsea_ctx['cli'] = self.config.get('cli', True)
        deepsea_ctx['dashboard_ssl'] = self.config.get('dashboard_ssl', True)
        deepsea_ctx['log_anchor'] = self.config.get('log_anchor', self.log_anchor_str)
        if not isinstance(deepsea_ctx['log_anchor'], str):
            self.log.warning(
                "log_anchor was set to non-string value ->{}<-, "
                "changing to empty string"
                .format(deepsea_ctx['log_anchor'])
                )
            deepsea_ctx['log_anchor'] = ''
        deepsea_ctx['drive_group'] = self.config.get("drive_group", "teuthology")
        deepsea_ctx['quiet_salt'] = self.config.get('quiet_salt', True)
        deepsea_ctx['salt_manager_instance'] = SaltManager(self.ctx)
        deepsea_ctx['master_remote'] = (
                deepsea_ctx['salt_manager_instance'].master_remote
                )
        deepsea_ctx['repositories'] = self.config.get("repositories", None)
        deepsea_ctx['rgw_ssl'] = self.config.get('rgw_ssl', False)
        self.__populate_install_method('install')

    def __populate_install_method_basic(self, key):
        if self.config[key] in ['package', 'pkg']:
            deepsea_ctx['install_method'] = 'package'
        elif self.config[key] in ['source', 'src']:
            deepsea_ctx['install_method'] = 'source'
        else:
            raise ConfigError(self.err_prefix + "Unrecognized {} config "
                              "value ->{}<-".format(key, self.config[key]))

    def __populate_install_method(self, key):
        if key in self.config:
            self.__populate_install_method_basic(key)
        else:
            if 'repo' in self.config or 'branch' in self.config:
                deepsea_ctx['install_method'] = 'source'
            else:
                deepsea_ctx['install_method'] = 'package'

    def _purge_osds(self):
        # needed as long as teuthology install task purges /var/lib/ceph
        # in its teardown phase
        for _remote in self.ctx.cluster.remotes.keys():
            self.log.info("stopping OSD services on {}"
                          .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            self.log.info("unmounting OSD partitions on {}"
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

    def first_storage_only_node(self):
        if self.nodes_storage_only:
            return self.nodes_storage_only[0]
        else:
            return None

    def os_type_and_version(self):
        os_type = self.ctx.config.get('os_type', 'unknown')
        os_version = float(self.ctx.config.get('os_version', 0))
        return (os_type, os_version)

    def reboot_a_single_machine_now(self, remote, log_spec=None):
        global reboot_tries
        if not log_spec:
            log_spec = "node {} reboot now".format(remote.hostname)
        cmd_str = "sudo reboot"
        remote_exec(
            remote,
            cmd_str,
            self.log,
            log_spec,
            rerun=False,
            quiet=True,
            tries=reboot_tries,
            )

    def reboot_the_cluster_now(self, log_spec=None):
        global reboot_tries
        if not log_spec:
            log_spec = "all nodes reboot now"
        cmd_str = "salt \\* cmd.run reboot"
        if self.quiet_salt:
            cmd_str += " 2> /dev/null"
        remote_exec(
            self.master_remote,
            cmd_str,
            self.log,
            log_spec,
            rerun=False,
            quiet=True,
            tries=reboot_tries,
            )
        self.sm.ping_minions()

    def role_type_present(self, role_type):
        """
        Method for determining if _any_ test node has the given role type
        (teuthology role, not DeepSea role). Examples: "osd", "mon" (not
        "mon.a").

        If the role type is present, returns the hostname of the first remote
        with that role type.

        If the role type is absent, returns the empty string.
        """
        role_dict = self.role_lookup_table.get(role_type, {})
        host = role_dict[role_dict.keys()[0]] if role_dict else ''
        return host

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
        # self.log.debug("beginning of setup method")
        super(DeepSea, self).setup()
        pass
        # self.log.debug("end of setup method")

    def begin(self):
        global deepsea_ctx
        super(DeepSea, self).begin()
        if self.reinstall_deepsea:
            self._install_deepsea()
            return None
        self.sm.master_rpm_q('ceph')
        self.sm.master_rpm_q('ceph-test')
        self.sm.master_rpm_q('salt-master')
        self.sm.master_rpm_q('salt-minion')
        self.sm.master_rpm_q('salt-api')
        # the Salt Master node is assumed to be running an already
        # configured chrony for time synchronization within the cluster
        # and DeepSea Stage 3 will point the minions at the Salt Master's
        # chrony instance (?)
        self.sm.master_rpm_q('chrony')
        self.master_remote.run(
            args="sudo systemctl status --lines=0 chronyd.service"
            )
        if self.allow_python2:
            self._master_python_version(2)
        else:
            self.log.info(
                'allow_python2 is set to \'false\'. That means the '
                'test will now fail if a python2 binary is found on '
                'any of the test machines.'
                )
            self.ctx.cluster.run(args='if type python2 ; then false ; else true ; fi')
        if not self._master_python_version(3):
            raise ConfigError(self.err_prefix + "Python 3 not installed on master node")
        if 'deepsea_installed' not in deepsea_ctx:
            self._disable_gpg_checks()
            self.master_remote.run(args="zypper lr -upEP")
            self._install_deepsea()
            assert deepsea_ctx['deepsea_installed']
        self._deepsea_version()
        self._deepsea_minions()
        self._maybe_apply_alternative_defaults()
        # Stage 0 does this, but we have no guarantee Stage 0 will run
        self.sm.sync_pillar_data(quiet=self.quiet_salt)

    def end(self):
        self.log.debug("beginning of end method")
        super(DeepSea, self).end()
        success = self.ctx.summary.get('success', None)
        if success is None:
            self.log.warning("Problem with ctx summary key? ctx is {}".format(self.ctx))
        if not success:
            self.ctx.cluster.run(args="rpm -qa | sort")
        self.sm.gather_logs('/home/farm/.npm/_logs', 'dashboard-e2e-npm')
        self.sm.gather_logs('/home/farm/.protractor-report', 'dashboard-e2e-protractor')
        self.log.debug("end of end method")

    def teardown(self):
        self.log.debug("beginning of teardown method")
        super(DeepSea, self).teardown()
        # #
        # # the install task does "rm -r /var/lib/ceph" on every test node,
        # # and that fails when there are OSDs running
        # # FIXME - deprecated, remove after awhile
        # self._purge_osds()
        self.log.debug("end of teardown method")


class CephConf(DeepSea):
    """
    Adds custom options to ceph.conf.
    Edit yaml file between stage 2 and 3.
    Example:
        - deepsea.orch:
                stage: 2
        - deepsea.ceph_conf:
                global:
                  mon lease: 15
                  mon lease ack timeout: 25
                mon:
                  debug mon: 20
                osd:
                  debug filestore: 20
        - deepsea.orch:
                stage: 3
    """

    customize = {
        "client": "client.conf",
        "global": "global.conf",
        "mds": "mds.conf",
        "mgr": "mgr.conf",
        "mon": "mon.conf",
        "osd": "osd.conf",
        }

    deepsea_configuration_files = '/srv/salt/ceph/configuration/files'

    err_prefix = "(ceph_conf subtask) "

    targets = {
        "mon_allow_pool_delete": True,
        "osd_memory_target": True,
        "small_cluster": True,
        "rbd": False,
        }

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('ceph_conf')
        self.name = 'deepsea.ceph_conf'
        super(CephConf, self).__init__(ctx, config)
        self.log.debug("munged config is {}".format(self.config))

    def __ceph_conf_d_full_path(self, section):
        ceph_conf_d = self.deepsea_configuration_files + '/ceph.conf.d'
        if section in self.customize.keys():
            return "{}/{}".format(ceph_conf_d, self.customize[section])

    def __custom_ceph_conf(self, section, customizations):
        for conf_item, conf_value in customizations.items():
            data = '{} = {}\n'.format(conf_item, conf_value)
            sudo_append_to_file(
                self.master_remote,
                self.__ceph_conf_d_full_path(section),
                data
                )
            self.log.info(
                "Adding to ceph.conf, {} section: {}"
                .format(section, data)
                )

    def _customizations(self):
        for section in self.customize.keys():
            if section in self.config and isinstance(self.config[section], dict):
                self.__custom_ceph_conf(section, self.config[section])

    def _dump_customizations(self):
        for section in self.customize.keys():
            path = self.__ceph_conf_d_full_path(section)
            dump_file_that_might_not_exist(self.master_remote, path)

    def _list_ceph_conf_d(self):
        self.master_remote.run(
            args="ls -l {}".format(self.deepsea_configuration_files)
            )

    def _targets(self):
        for target, default in self.targets.items():
            method = getattr(self, target, None)
            assert method, "target ->{}<- has no method".format(target)
            if target in self.config:
                method()
            else:
                if default:
                    method()

    def mon_allow_pool_delete(self):
        info_msg = "adjusted ceph.conf to allow pool deletes"
        data = "mon allow pool delete = true\n"
        sudo_append_to_file(
            self.master_remote,
            self.__ceph_conf_d_full_path("mon"),
            data,
            )
        self.log.info(info_msg)

    def osd_memory_target(self):
        info_msg = "lowered osd_memory_target to 1GiB to facilitate testing in OpenStack"
        data = "osd memory target = 1105322466"  # https://tracker.ceph.com/issues/37507#note-4
        sudo_append_to_file(
            self.master_remote,
            self.__ceph_conf_d_full_path("osd"),
            data,
            )
        self.log.info(info_msg)

    def rbd(self):
        """
        Delete "rbd default features" from ceph.conf. By removing this line, we
        ensure that there will be no explicit "rbd default features" setting,
        so the default will be used.
        """
        info_msg = "adjusted ceph.conf by removing 'rbd default features' line"
        rbd_conf = '/srv/salt/ceph/configuration/files/rbd.conf'
        cmd = 'sudo sed -i \'/^rbd default features =/d\' {}'.format(rbd_conf)
        self.master_remote.run(args=cmd)
        self.log.info(info_msg)

    def small_cluster(self):
        """
        Apply necessary ceph.conf for small clusters
        """
        storage_nodes = len(self.nodes_storage)
        info_msg = (
            "adjusted ceph.conf for operation with {} storage node(s)"
            .format(storage_nodes)
            )
        data = None
        if storage_nodes == 1:
            data = (
                   "mon pg warn min per osd = 16\n"
                   "osd pool default size = 2\n"
                   "osd crush chooseleaf type = 0 # failure domain == osd\n"
                   )
        elif storage_nodes == 2 or storage_nodes == 3:
            data = (
                   "mon pg warn min per osd = 8\n"
                   "osd pool default size = 2\n"
                   )
        if data:
            sudo_append_to_file(
                self.master_remote,
                self.__ceph_conf_d_full_path("global"),
                data,
                )
            self.log.info(info_msg)

    def begin(self):
        self.log.info(anchored("Adding custom options to ceph.conf"))
        self._targets()
        self._customizations()
        self._list_ceph_conf_d()
        self._dump_customizations()

    def end(self):
        pass

    def teardown(self):
        pass


class CreatePools(DeepSea):

    err_prefix = "(create_pools subtask) "

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('create_pools')
        self.name = 'deepsea.create_pools'
        super(CreatePools, self).__init__(ctx, config)
        if not isinstance(self.config, dict):
            raise ConfigError(self.err_prefix + "config must be a dictionary")

    def begin(self):
        self.log.info(anchored("pre-creating pools"))
        args = []
        for key in self.config:
            if self.config[key] is None:
                self.config[key] = True
            if self.config[key]:
                args.append(key)
        args = list(set(args))
        self.scripts.run(
            self.master_remote,
            'create_all_pools_at_once.sh',
            args=args,
            )

    def end(self):
        pass

    def teardown(self):
        pass


class Dummy(DeepSea):

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('dummy')
        self.name = 'deepsea.dummy'
        super(Dummy, self).__init__(ctx, config)
        self.log.debug("munged config is {}".format(self.config))

    def begin(self):
        self.log.debug("beginning of begin method")
        global deepsea_ctx
        self.log.info("deepsea_ctx == {}".format(deepsea_ctx))
        self.log.debug("end of begin method")

    def end(self):
        pass

    def teardown(self):
        pass


class HealthOK(DeepSea):
    """
    Copy health_ok.sh to Salt Master node and run commands.

    This task understands the following config key:

        commands:
            [list of health-ok.sh commands]


    The list of commands will be executed as root on the Salt Master node.
    """

    err_prefix = "(health_ok subtask) "

    prefix = 'health-ok/'

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('health_ok')
        self.name = 'deepsea.health_ok'
        super(HealthOK, self).__init__(ctx, config)

    def _copy_health_ok(self):
        """
        Copy health-ok.sh from teuthology VM to master_remote
        """
        global deepsea_ctx
        suite_path = self.ctx.config.get('suite_path')
        log.info("suite_path is ->{}<-".format(suite_path))
        misc.sh("ls -l {}".format(suite_path))
        health_ok_path = suite_path + "/deepsea/health-ok"
        misc.sh("test -d " + health_ok_path)
        copy_directory_recursively(
                health_ok_path, self.master_remote, "health-ok")
        self.master_remote.run(args="pwd ; ls -lR health-ok")
        deepsea_ctx['health_ok_copied'] = True

    def _maybe_run_commands(self, commands):
        if not commands:
            self.log.warning(
                "The health_ok task was run, but no commands were specified. "
                "Doing nothing."
                )
            return None
        for cmd_str in commands:
            if not isinstance(cmd_str, str):
                raise ConfigError(
                    self.err_prefix +
                    "command ->{}<- is not a string".format(cmd_str)
                    )
            if cmd_str.startswith('health-ok.sh'):
                cmd_str = self.prefix + cmd_str
                if self.dev_env:
                    cmd_str = 'DEV_ENV=true ' + cmd_str
                if self.deepsea_cli:
                    cmd_str += ' --cli'
                if self.rgw_ssl:
                    cmd_str += ' --ssl'
            self.master_remote.run(args=[
                'sudo', 'bash', '-c', cmd_str,
                ])

    def setup(self):
        global deepsea_ctx
        if 'health_ok_copied' not in deepsea_ctx:
            self._copy_health_ok()
            assert deepsea_ctx['health_ok_copied']

    def begin(self):
        commands = self.config.get('commands', [])
        if not isinstance(commands, list):
            raise ConfigError(self.err_prefix + "commands must be a list")
        self._maybe_run_commands(commands)

    def end(self):
        pass

    def teardown(self):
        pass


class Orch(DeepSea):

    all_stages = [
        "0", "prep", "1", "discovery", "2", "configure", "3", "deploy",
        "4", "services", "5", "removal", "cephfs", "ganesha", "iscsi",
        "openattic", "openstack", "radosgw", "validate"
        ]

    err_prefix = "(orch subtask) "

    stage_synonyms = {
        0: 'prep',
        1: 'discovery',
        2: 'configure',
        3: 'deploy',
        4: 'services',
        5: 'removal',
        }

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('orch')
        self.name = 'deepsea.orch'
        super(Orch, self).__init__(ctx, config)
        self.stage = str(self.config.get("stage", ''))
        self.state_orch = str(self.config.get("state_orch", ''))
        self.reboots_explicitly_forbidden = not self.config.get("allow_reboots", True)
        self.survive_reboots = self._detect_reboots()
        if not self.stage and not self.state_orch:
            raise ConfigError(
                self.err_prefix +
                "nothing to do. Specify a value for 'stage' or "
                "'state_orch' key in config dict"
                )
        if self.stage and self.stage not in self.all_stages:
            raise ConfigError(
                self.err_prefix +
                "unrecognized Stage ->{}<-".format(self.stage)
                )
        self.log.debug("munged config is {}".format(self.config))

    def __ceph_health_test(self):
        cmd = 'sudo salt-call wait.until status=HEALTH_OK timeout=900 check=1'
        if self.quiet_salt:
            cmd += ' 2> /dev/null'
        self.master_remote.run(args=cmd)

    def __check_ceph_test_rpm_version(self):
        """Checks rpm version for ceph and ceph-test; logs warning if differs"""
        ceph_test_ver = get_rpm_pkg_version(self.master_remote, "ceph-test", self.log)
        ceph_ver = get_rpm_pkg_version(self.master_remote, "ceph", self.log)
        if ceph_test_ver != ceph_ver:
            self.log.warning(
                "ceph-test rpm version: {} differs from ceph version: {}"
                .format(ceph_test_ver, ceph_ver))

    def __check_salt_api_service(self):
        base_cmd = 'sudo systemctl status --full --lines={} {}.service'
        try:
            self.master_remote.run(args=base_cmd.format('0', 'salt-api'))
        except CommandFailedError:
            self.master_remote.run(args=base_cmd.format('100', 'salt-api'))
            raise
        self.scripts.run(
            self.master_remote,
            'salt_api_test.sh',
            )

    def __dump_drive_groups_yml(self):
        self.scripts.run(
            self.master_remote,
            'dump_drive_groups_yml.sh',
            )

    def __dump_lvm_status(self):
        self.log.info("Dumping LVM status on storage nodes ->{}<-"
                      .format(self.nodes_storage))
        for hostname in self.nodes_storage:
            remote = self.remotes[hostname]
            self.scripts.run(
                remote,
                'lvm_status.sh',
                )

    def __is_stage_between_0_and_5(self):
        """
        This is implemented as a separate function because the stage specified
        in the YAML might be a number or a string, and we really don't care
        what Python sees it as.
        """
        num = self.stage
        try:
            num = int(num)
        except ValueError:
            return False
        if num < 0 or num > 5:
            return False
        return True

    def __log_stage_start(self, stage):
        self.log.info(anchored(
            "Running DeepSea Stage {} ({})"
            .format(stage, self.stage_synonyms[stage])
            ))

    def __maybe_cat_ganesha_conf(self):
        ganesha_host = self.role_type_present('ganesha')
        if ganesha_host:
            ganesha_remote = self.remotes[ganesha_host]
            ganesha_remote.run(args="cat /etc/ganesha/ganesha.conf")

    def __mgr_dashboard_module_deploy(self):
        script = ("# deploy MGR dashboard module\n"
                  "set -ex\n"
                  "ceph mgr module enable dashboard\n")
        if self.dashboard_ssl:
            script += "ceph dashboard create-self-signed-cert\n"
        else:
            script += "ceph config set mgr mgr/dashboard/ssl false\n"
        remote_run_script_as_root(
            self.master_remote,
            'mgr_dashboard_module_deploy.sh',
            script,
            )

    def __zypper_ps_with_possible_reboot(self):
        if self.sm.all_minions_zypper_ps_requires_reboot():
            log_spec = "Detected updates requiring reboot"
            self.log.warning(anchored(log_spec))
            if self.reboots_explicitly_forbidden:
                self.log.info("Reboots explicitly forbidden in test configuration: not rebooting")
                self.log.warning("Processes using deleted files may cause instability")
            else:
                self.log.warning(anchored("Rebooting the whole cluster now!"))
                self.reboot_the_cluster_now(log_spec=log_spec)
                assert not self.sm.all_minions_zypper_ps_requires_reboot(), \
                    "No more updates requiring reboot anywhere in the whole cluster"

    def _configure_rgw(self):
        self.log.debug("self.rgw_ssl is ->{}<-".format(self.rgw_ssl))
        rgw_host = self.role_type_present('rgw')
        if rgw_host:
            self.log.debug(
                "detected rgw host ->{}<-".format(rgw_host)
                )
            self.log.info(anchored("configuring RGW"))
            self.scripts.run(
                self.master_remote,
                'rgw_init.sh',
                )
            if self.rgw_ssl:
                self.scripts.run(
                    self.master_remote,
                    'rgw_init_ssl.sh',
                    )

    # FIXME: run on each minion individually, and compare deepsea "roles"
    # with teuthology roles!
    def _pillar_items(self):
        cmd = "sudo salt \\* pillar.items"
        if self.quiet_salt:
            cmd += " 2>/dev/null"
        self.master_remote.run(args=cmd)

    def _run_orch(self, orch_tuple):
        """Run an orchestration. Dump journalctl on error."""
        global reboot_tries
        orch_type, orch_spec = orch_tuple
        if orch_type == 'orch':
            cli = False
            pass
        elif orch_type == 'stage':
            cli = self.deepsea_cli
            orch_spec = 'ceph.stage.{}'.format(orch_spec)
        else:
            raise ConfigError(
                self.err_prefix +
                "Unrecognized orchestration type ->{}<-".format(orch_type)
                )
        cmd_str = None
        if cli:
            cmd_str = (
                'timeout 60m deepsea '
                '--log-file=/var/log/salt/deepsea.log '
                '--log-level=debug '
                'salt-run state.orch {} --simple-output'
                ).format(orch_spec)
        else:
            cmd_str = (
                'timeout 60m salt-run '
                '--no-color state.orch {}'
                ).format(orch_spec)
            if self.quiet_salt:
                cmd_str += ' 2>/dev/null'
        if self.dev_env:
            cmd_str = 'DEV_ENV=true ' + cmd_str
        tries = 0
        if self.survive_reboots:
            tries = reboot_tries
        remote_exec(
            self.master_remote,
            cmd_str,
            self.log,
            "orchestration {}".format(orch_spec),
            rerun=True,
            quiet=True,
            tries=tries,
            )

    def _detect_reboots(self):
        """
        Check for all known states/stages/alt-defaults that
        may cause a reboot
        If there is a 'allow_reboot' flag, it takes presedence.
        """
        allow_reboot = self.config.get("allow_reboot", None)
        if allow_reboot is not None:
            self.log.info("Setting allow_reboot explicitly to {}"
                          .format(self.allow_reboot))
            return allow_reboot
        orchs_prone_to_reboot = ['ceph.maintenance.upgrade']
        if self.state_orch in orchs_prone_to_reboot:
            self.log.warning("This orchestration may trigger a reboot")
            return True
        #
        # The alternative_defaults stanza has been moved up to the deepsea task
        # (for two reasons: because it's a global setting and also so we can do
        # boilerplate overrides like qa/deepsea/boilerplate/disable_tuned.yaml).
        # That change makes the following heuristic becomes problematic: since
        # all the alternative defaults are concentrated in one place, if any of
        # them contains the string "reboot" (without preceding "no-"), **all**
        # orchestrations in the test will run with survive_reboots, not just
        # one.
        for k, v in self.alternative_defaults.items():
            if 'reboot' in v and 'no-reboot' not in v:
                self.log.warning("Orchestrations may trigger a reboot")
                return True
        self.log.info("Not allowing reboots for this orchestration")
        return False

    def _run_stage_0(self):
        """
        Run Stage 0
        """
        stage = 0
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self._pillar_items()
        self.sm.all_minions_zypper_ref()
        self.sm.all_minions_zypper_lu()
        self.__zypper_ps_with_possible_reboot()
        self.__check_salt_api_service()

    def _run_stage_1(self):
        """
        Run Stage 1
        """
        stage = 1
        self._configure_rgw()
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))

    def _run_stage_2(self):
        """
        Run Stage 2
        """
        stage = 2
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self.__check_ceph_test_rpm_version()
        self._pillar_items()
        self.__dump_drive_groups_yml()

    def _run_stage_3(self):
        """
        Run Stage 3
        """
        stage = 3
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        # self.__mgr_dashboard_module_deploy()
        self.sm.all_minions_cmd_run(
            'cat /etc/ceph/ceph.conf',
            abort_on_fail=False
            )
        self.__dump_lvm_status()
        self.scripts.run(
            self.master_remote,
            'ceph_cluster_status.sh',
            )
        self.__ceph_health_test()

    def _run_stage_4(self):
        """
        Run Stage 4
        """
        stage = 4
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self.__maybe_cat_ganesha_conf()
        self.__ceph_health_test()

    def _run_stage_5(self):
        """
        Run Stage 5
        """
        stage = 5
        self.__log_stage_start(stage)
        self._run_orch(("stage", 5))

    def begin(self):
        self.master_remote.sh('sudo salt-run jobs.active 2>/dev/null')
        if self.state_orch:
            self.log.info(anchored(
                "running orchestration {}".format(self.state_orch)
                ))
            self._run_orch(("orch", self.state_orch))
        else:
            # it's not an orch, so it must be a stage
            assert self.stage, "Neither state_orch, nor stage"
            if self.__is_stage_between_0_and_5():
                exec('self._run_stage_{}()'.format(self.stage))
            elif self.stage == 'prep':
                self.log.info("Running Stage 0 instead of Stage \"prep\"")
                self._run_stage_0()
            elif self.stage == 'discovery':
                self.log.info("Running Stage 1 instead of Stage \"discovery\"")
                self._run_stage_1()
            elif self.stage == 'configure':
                self.log.info("Running Stage 2 instead of Stage \"configure\"")
                self._run_stage_2()
            elif self.stage == 'deploy':
                self.log.info("Running Stage 3 instead of Stage \"deploy\"")
                self._run_stage_3()
            elif self.stage == 'services':
                self.log.info("Running Stage 4 instead of Stage \"services\"")
                self._run_stage_4()
            elif self.stage == 'removal':
                self.log.info("Running Stage 5 instead of Stage \"removal\"")
                self._run_stage_5()
            elif self.stage in self.all_stages:
                self.log.info("Running non-numeric Stage \"{}\"".format(self.stage))
                self._run_orch(("stage", self.stage))
            else:
                raise ConfigError(
                    self.err_prefix +
                    'unsupported stage ->{}<-'.format(self.stage)
                    )
        self.master_remote.sh('sudo salt-run jobs.active 2>/dev/null')

    def end(self):
        pass

    def teardown(self):
        pass


class Policy(DeepSea):

    err_prefix = "(policy subtask) "

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('policy')
        self.name = 'deepsea.policy'
        super(Policy, self).__init__(ctx, config)
        self.policy_cfg = ''
        self.munge_policy = self.config.get('munge_policy', {})

    def __build_drive_group_x(self, drive_group):
        # generate our own drive_group.yml (as opposed to letting
        # DeepSea generate one for us)
        if not self.nodes_storage:
            raise ConfigError(self.err_prefix + "no osd roles configured, "
                              "but at least one of these is required.")
        self.log.debug("building drive group ->{}<- for {} storage nodes"
                       .format(drive_group, len(self.nodes_storage)))
        if drive_group == 'teuthology':
            raise ConfigError(self.err_prefix + "\"teuthology\" drive group "
                              "generation not implemented yet")
        elif drive_group == 'custom':
            self.__roll_out_drive_group()
        else:
            ConfigError(self.err_prefix + "unknown drive group ->{}<-"
                        .format(self.drive_group))

    def __roll_out_drive_group(self, fpath="/srv/salt/ceph/configuration/files/drive_groups.yml"):
        misc.sudo_write_file(
            self.master_remote,
            fpath,
            yaml.dump(self.drive_group),
            perms="0644",
            )

    def _build_base(self):
        """
        policy.cfg boilerplate
        """
        self.policy_cfg = ("# policy.cfg generated by deepsea.policy subtask\n"
                           "# Cluster assignment\n"
                           "cluster-ceph/cluster/*.sls\n"
                           "# Common configuration\n"
                           "config/stack/default/global.yml\n"
                           "config/stack/default/ceph/cluster.yml\n"
                           "# Role assignment - master\n"
                           "role-master/cluster/{}.sls\n"
                           "# Role assignment - admin\n"
                           "role-admin/cluster/*.sls\n"
                           .format(self.master_remote.hostname))

    def _build_drive_groups_yml(self):
        """
        Generate a special-purpose drive_groups.yml
        (currently fails the test in all cases except when
        "drive_group: default" is explicitly given)
        """
        if isinstance(self.drive_group, str):
            if self.drive_group == 'teuthology':
                self.__build_drive_group_x('teuthology')
            elif self.drive_group == 'default':
                pass
            else:
                ConfigError(self.err_prefix + "unknown drive group ->{}<-"
                            .format(self.drive_group))
        elif isinstance(self.drive_group, dict):
            self.__build_drive_group_x('custom')
        else:
            raise ConfigError(self.err_prefix + "drive_group config param "
                              "must be a string or a dict")

    def _build_x(self, role_type, required=False):
        no_roles_of_type = "no {} roles configured".format(role_type)
        but_required = ", but at least one of these is required."
        role_dict = {}
        if role_type in self.role_lookup_table:
            role_dict = self.role_lookup_table[role_type]
        elif required:
            raise ConfigError(self.err_prefix + no_roles_of_type + but_required)
        else:
            self.log.debug(no_roles_of_type)
            return None
        self.log.debug("generating policy.cfg lines for {} based on {}"
                       .format(role_type, role_dict))
        if required:
            if len(role_dict.keys()) < 1:
                raise ConfigError(self.err_prefix + no_roles_of_type + but_required)
        for role_spec, remote_name in role_dict.items():
            if role_type == 'osd':
                role_type = 'storage'
            self.policy_cfg += ('# Role assignment - {}\n'
                                'role-{}/cluster/{}.sls\n'
                                .format(role_spec, role_type, remote_name))

    def _cat_policy_cfg(self):
        """
        Dump the final policy.cfg file to teuthology log.
        """
        cmd_str = "cat {}/policy.cfg".format(proposals_dir)
        self.master_remote.run(args=cmd_str)

    def _write_policy_cfg(self):
        """
        Write policy_cfg to master remote.
        """
        misc.sudo_write_file(
            self.master_remote,
            proposals_dir + "/policy.cfg",
            self.policy_cfg,
            perms="0644",
            owner="salt",
            )
        cmd_str = "ls -l {}/policy.cfg".format(proposals_dir)
        self.master_remote.run(args=cmd_str)

    def begin(self):
        """
        Generate policy.cfg from the results of role introspection
        """
        # FIXME: this should be run only once - check for that and
        # return an error otherwise
        if self.munge_policy:
            for k, v in self.munge_policy.items():
                if k == 'remove_storage_only_node':
                    delete_me = self.first_storage_only_node()
                    if not delete_me:
                        raise ConfigError(
                            self.err_prefix + "remove_storage_only_node "
                            "requires a storage-only node, but there is no such"
                            )
                    raise ConfigError(self.err_prefix + (
                        "munge_policy is a kludge - get rid of it! "
                        "This test needs to be reworked - deepsea.py "
                        "does not currently have a proper way of "
                        "changing (\"munging\") the policy.cfg file."
                        ))
                else:
                    raise ConfigError(self.err_prefix + "unrecognized "
                                      "munge_policy directive {}".format(k))
        else:
            self.log.info(anchored("generating policy.cfg"))
            self._build_base()
            self._build_x('mon', required=True)
            self._build_x('mgr', required=True)
            self._build_x('osd', required=True)
            self._build_drive_groups_yml()
            self._build_x('mds')
            self._build_x('rgw')
            self._build_x('igw')
            self._build_x('ganesha')
            self._build_x('prometheus')
            self._build_x('grafana')
            self._write_policy_cfg()
            self._cat_policy_cfg()

    def end(self):
        pass

    def teardown(self):
        pass


class Reboot(DeepSea):
    """
    A class that does nothing but unconditionally reboot - either a single node
    or the whole cluster.

    Configuration (reboot a single node)

    tasks:
    - deepsea.reboot:
          client.salt_master:

    Configuration (reboot the entire cluster)

    tasks:
    - deepsea.reboot:
          all:
    """

    err_prefix = '(reboot subtask) '

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('reboot')
        self.name = 'deepsea.reboot'
        super(Reboot, self).__init__(ctx, config)

    def begin(self):
        if not self.config:
            self.log.warning("empty config: nothing to do")
            return None
        config_keys = len(self.config)
        if config_keys > 1:
            raise ConfigError(
                self.err_prefix +
                "config dictionary may contain only one key. "
                "You provided ->{}<- keys ({})".format(len(config_keys), config_keys)
                )
        role_spec, repositories = self.config.items()[0]
        if role_spec == "all":
            remote = self.ctx.cluster
            log_spec = "all nodes reboot now"
            self.log.warning(anchored(log_spec))
            self.reboot_the_cluster_now(log_spec=log_spec)
        else:
            remote = get_remote_for_role(self.ctx, role_spec)
            log_spec = "node {} reboot now".format(remote.hostname)
            self.log.warning(anchored(log_spec))
            self.reboot_a_single_machine_now(remote, log_spec=log_spec)

    def end(self):
        pass

    def teardown(self):
        pass


class Repository(DeepSea):
    """
    A class for manipulating zypper repos on the test nodes.
    All it knows how to do is wipe out the existing repos (i.e. rename them to
    foo.repo.bck) and replace them with a given set of new ones.

    Configuration (one node):

    tasks:
    - deepsea.repository:
          client.salt_master:
              - name: repo_foo
                url: http://example.com/foo/
              - name: repo_bar
                url: http://example.com/bar/

    Configuration (all nodes):

    tasks:
    - deepsea.repository:
          all:
              - name: repo_foo
                url: http://example.com/foo/
              - name: repo_bar
                url: http://example.com/bar/

    To eliminate the need to duplicate the repos array, it can be specified
    in the configuration of the main deepsea task. Then the yaml will look
    like so:

    tasks:
    - deepsea:
          repositories:
              - name: repo_foo
                url: http://example.com/foo/
              - name: repo_bar
                url: http://example.com/bar/
    ...
    - deepsea.repository:
          client.salt_master:
    ...
    - deepsea.repository:
          all:

    One last note: we try to be careful and not clobber the repos twice.
    """

    err_prefix = '(repository subtask) '

    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('repository')
        self.name = 'deepsea.repository'
        super(Repository, self).__init__(ctx, config)

    def _repositories_to_remote(self, remote):
        args = []
        for repo in self.repositories:
            args += [repo['name'] + ':' + repo['url']]
        self.scripts.run(
            remote,
            'clobber_repositories.sh',
            args=args
            )

    def begin(self):
        if not self.config:
            self.log.warning("empty config: nothing to do")
            return None
        config_keys = len(self.config)
        if config_keys > 1:
            raise ConfigError(
                self.err_prefix +
                "config dictionary may contain only one key. "
                "You provided ->{}<- keys ({})".format(len(config_keys), config_keys)
                )
        role_spec, repositories = self.config.items()[0]
        if role_spec == "all":
            remote = self.ctx.cluster
        else:
            remote = get_remote_for_role(self.ctx, role_spec)
        if repositories is None:
            assert self.repositories, \
                "self.repositories must be populated if role_dict is None"
        else:
            assert isinstance(repositories, list), \
                "value of role key must be a list of repositories"
            self.repositories = repositories
        if not self.repositories:
            raise ConfigError(
                self.err_prefix +
                "No repositories specified. Bailing out!"
                )
        self._repositories_to_remote(remote)

    def end(self):
        pass

    def teardown(self):
        pass


class Script(DeepSea):
    """
    A class that runs a bash script on the node with given role, or on all nodes.

    Example 1 (run foo_bar.sh, with arguments, on Salt Master node):

    tasks:
        - deepsea.script:
              client.salt_master:
                  foo_bar.sh:
                      args:
                          - 'foo'
                          - 'bar'

    Example 2 (run foo_bar.sh, with no arguments, on all test nodes)

    tasks:
        - deepsea.script:
              all:
                  foo_bar.sh:
    """

    err_prefix = '(script subtask) '

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('script')
        self.name = 'deepsea.script'
        super(Script, self).__init__(ctx, config)

    def begin(self):
        if not self.config:
            self.log.warning("empty config: nothing to do")
            return None
        config_keys = len(self.config)
        if config_keys > 1:
            raise ConfigError(
                self.err_prefix +
                "config dictionary may contain only one key. "
                "You provided ->{}<- keys ({})".format(len(config_keys), config_keys)
                )
        role_spec, role_dict = self.config.items()[0]
        role_keys = len(role_dict)
        if role_keys > 1:
            raise ConfigError(
                self.err_prefix +
                "role dictionary may contain only one key. "
                "You provided ->{}<- keys ({})".format(len(role_keys), role_keys)
                )
        if role_spec == "all":
            remote = self.ctx.cluster
        else:
            remote = get_remote_for_role(self.ctx, role_spec)
        script_spec, script_dict = role_dict.items()[0]
        if script_dict is None:
            args = []
        if isinstance(script_dict, dict):
            if len(script_dict) > 1 or script_dict.keys()[0] != 'args':
                raise ConfigError(
                    self.err_prefix +
                    'script dicts may only contain one key (args)'
                    )
            args = script_dict.values()[0] or []
            if not isinstance(args, list):
                raise ConfigError(self.err_prefix + 'script args must be a list')
        self.scripts.run(
            remote,
            script_spec,
            args=args
            )

    def end(self):
        pass

    def teardown(self):
        pass


class Toolbox(DeepSea):
    """
    A class that contains various miscellaneous routines. For example:

    tasks:
    - deepsea.toolbox:
          foo:

    Runs the "foo" tool without any options.
    """

    err_prefix = '(toolbox subtask) '

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('toolbox')
        self.name = 'deepsea.toolbox'
        super(Toolbox, self).__init__(ctx, config)

    def _assert_store(self, file_or_blue, teuth_role):
        """
        file_or_blue can be either 'bluestore' or 'filestore'
        teuth_role is an 'osd' role uniquely specifying one of the storage nodes.
        Enumerates the OSDs on the node and asserts that each of these OSDs is
        either filestore or bluestore, as appropriate.
        """
        remote = get_remote_for_role(self.ctx, teuth_role)
        osds = enumerate_osds(remote, self.log)
        assert osds, "No OSDs were captured, so please check if they are active"
        self.log.info("Checking if OSDs ->{}<- are ->{}<-".format(osds, file_or_blue))
        all_green = True
        for osd in osds:
            store = remote.sh("sudo ceph osd metadata {} | jq -r .osd_objectstore"
                              .format(osd)).rstrip()
            self.log.info("OSD {} is ->{}<-.".format(osd, store))
            if store != file_or_blue:
                self.log.warning("OSD {} has objectstore ->{}<- which is not ->{}<-".
                                 format(osd, store, file_or_blue))
                all_green = False
        assert all_green, "One or more OSDs is not {}".format(file_or_blue)

    def rebuild_node(self, **kwargs):
        """
        Expects a teuthology 'osd' role specifying one of the storage nodes.
        Then runs 'rebuild.nodes' on the node, can be used for filestore to bluestore
        migration if you run it after you change the drive_groups.yml file.
        """
        role = kwargs.keys()[0]
        remote = get_remote_for_role(self.ctx, role)
        osds_before_rebuild = len(enumerate_osds(remote, self.log))
        self.log.info("Disengaging safety to prepare for rebuild")
        self.master_remote.sh("sudo salt-run disengage.safety 2>/dev/null")
        self.log.info("Rebuilding node {}".format(remote.hostname))
        self.master_remote.sh("sudo salt-run rebuild.node {} 2>/dev/null".format(remote.hostname))
        with safe_while(sleep=15, tries=10,
                        action="ceph osd tree") as proceed:
            while proceed():
                self.master_remote.sh("sudo ceph osd tree || true")
                if osds_before_rebuild == len(enumerate_osds(remote, self.log)):
                    break

    def _noout(self, add_or_rm, teuth_role):
        """
        add_or_rm is either 'add' or 'rm'
        teuth_role is an 'osd' role uniquely specifying one of the storage nodes.
        Enumerates the OSDs on the node and does 'add-noout' on each of them.
        """
        remote = get_remote_for_role(self.ctx, teuth_role)
        osds = enumerate_osds(remote, self.log)
        self.log.info("Running {}-noout for OSDs ->{}<-".format(add_or_rm, osds))
        for osd in osds:
            remote.sh("sudo ceph osd {}-noout osd.{}".format(add_or_rm, osd))

    def add_noout(self, **kwargs):
        """
        Expects one key - a teuthology 'osd' role specifying one of the storage nodes.
        Enumerates the OSDs on this node and does 'add-noout' on each of them.
        """
        role = kwargs.keys()[0]
        self._noout("add", role)

    def assert_bluestore(self, **kwargs):
        """
        Expects one key - a teuthology 'osd' role specifying one of the storage nodes.
        Enumerates the OSDs on this node and asserts that each one is a bluestore OSD.
        """
        role = kwargs.keys()[0]
        self._assert_store("bluestore", role)

    def assert_filestore(self, **kwargs):
        """
        Expects one key - a teuthology 'osd' role specifying one of the storage nodes.
        Enumerates the OSDs on this node and asserts that each one is a filestore OSD.
        """
        role = kwargs.keys()[0]
        self._assert_store("filestore", role)

    def rm_noout(self, **kwargs):
        """
        Expects one key - a teuthology 'osd' role specifying one of the storage nodes.
        Enumerates the OSDs on this node and does 'rm-noout' on each of them.
        """
        role = kwargs.keys()[0]
        self._noout("rm", role)

    def wait_for_health_ok(self, **kwargs):
        """
        Wait for HEALTH_OK - stop after HEALTH_OK is reached or timeout expires.
        Timeout defaults to 120 minutes, but can be specified by providing a
        configuration option. For example:

        tasks:
        - deepsea.toolbox
            wait_for_health_ok:
              timeout_minutes: 90
        """
        if kwargs:
            self.log.info("wait_for_health_ok: Considering config dict ->{}<-".format(kwargs))
            config_keys = len(kwargs)
            if config_keys > 1:
                raise ConfigError(
                    self.err_prefix +
                    "wait_for_health_ok config dictionary may contain only one key. "
                    "You provided ->{}<- keys ({})".format(len(config_keys), config_keys)
                    )
            timeout_spec, timeout_minutes = kwargs.items()[0]
        else:
            timeout_minutes = 120
        self.log.info("Waiting up to ->{}<- minutes for HEALTH_OK".format(timeout_minutes))
        remote = get_remote_for_role(self.ctx, "client.salt_master")
        cluster_status = ""
        for minute in range(1, timeout_minutes+1):
            remote.sh("sudo ceph status")
            cluster_status = remote.sh(
                "sudo ceph health detail --format json | jq -r '.status'"
                ).rstrip()
            if cluster_status == "HEALTH_OK":
                break
            self.log.info("Waiting for one minute for cluster to reach HEALTH_OK"
                          "({} minutes left to timeout)"
                          .format(timeout_minutes + 1 - minute))
            time.sleep(60)
        if cluster_status == "HEALTH_OK":
            self.log.info(anchored("Cluster is healthy"))
        else:
            raise RuntimeError("Cluster still not healthy (current status ->{}<-) "
                               "after reaching timeout"
                               .format(cluster_status))

    def begin(self):
        if not self.config:
            self.log.warning("empty config: nothing to do")
            return None
        self.log.info("Considering config dict ->{}<-".format(self.config))
        config_keys = len(self.config)
        if config_keys > 1:
            raise ConfigError(
                self.err_prefix +
                "config dictionary may contain only one key. "
                "You provided ->{}<- keys ({})".format(len(config_keys), config_keys)
                )
        tool_spec, kwargs = self.config.items()[0]
        kwargs = {} if not kwargs else kwargs
        method = getattr(self, tool_spec, None)
        if method:
            self.log.info("About to run tool ->{}<- from toolbox with config ->{}<-"
                          .format(tool_spec, kwargs))
            method(**kwargs)
        else:
            raise ConfigError(self.err_prefix + "No such tool ->{}<- in toolbox"
                              .format(tool_spec))

    def end(self):
        pass

    def teardown(self):
        pass


class Validation(DeepSea):
    """
    A container for "validation tests", which are understood to mean tests that
    validate the Ceph cluster (just) deployed by DeepSea.

    The tests implemented in this class should be small and not take long to
    finish. Anything more involved should be implemented in a separate task
    (see ses_qa.py for an example of such a task).

    The config YAML is a dictionary in which the keys are the names of tests
    (methods to be run) and the values are the config dictionaries of each test
    to be run.

    Validation tests with lines like this

        self._apply_config_default("foo_test", None)

    are triggered by default, while others have to be explicitly mentioned in
    the YAML.
    """

    err_prefix = '(validation subtask) '

    def __init__(self, ctx, config):
        global deepsea_ctx
        deepsea_ctx['logger_obj'] = log.getChild('validation')
        self.name = 'deepsea.validation'
        super(Validation, self).__init__(ctx, config)
        self._apply_config_default("ceph_version_sanity", None)
        self._apply_config_default("rados_striper", None)
        self._apply_config_default("systemd_units_active", None)

    def _apply_config_default(self, validation_test, default_config):
        """
        Use to activate tests that should always be run.
        """
        self.config[validation_test] = self.config.get(validation_test, default_config)

    def ceph_version_sanity(self, **kwargs):
        self.scripts.run(
            self.master_remote,
            'ceph_version_sanity.sh',
            )

    def ganesha_smoke_test(self, **kwargs):
        client_host = self.role_type_present("ganeshaclient")
        rgw = self.role_type_present("rgw")
        mds = self.role_type_present("mds")
        args = []
        if mds:
            args += ['--mds']
        if rgw:
            args += ['--rgw']
        if not args:
            raise ConfigError(self.err_prefix +
                              "ganesha_smoke_test needs an rgw or mds role, but neither was given")
        if client_host:
            self.master_remote.sh("sudo salt-run ganesha.report 2>/dev/null || true")
            remote = self.remotes[client_host]
            self.scripts.run(
                remote,
                'ganesha_smoke_test.sh',
                args=args,
                )
            self.master_remote.sh("sudo salt-run ganesha.report 2>/dev/null || true")
        else:
            raise ConfigError(self.err_prefix +
                              "ganesha_smoke_test needs a client role, but none was given")

    def grafana_service_check(self, **kwargs):
        grafana = self.role_type_present("grafana")
        if grafana:
            remote = self.remotes[grafana]
            remote.sh('sudo systemctl status grafana-server.service')
        else:
            raise ConfigError(self.err_prefix +
                              "grafana_service_check needs a grafana role, but none was given")

    def iscsi_smoke_test(self, **kwargs):
        igw_host = self.role_type_present("igw")
        if igw_host:
            remote = self.remotes[igw_host]
            self.scripts.run(
                remote,
                'iscsi_smoke_test.sh',
                )

    def rados_striper(self, **kwargs):
        """
        Verify that rados does not has the --striper option
        """
        cmd_str = 'sudo rados --striper 2>&1 || true'
        output = self.master_remote.sh(cmd_str)
        os_type, os_version = self.os_type_and_version()
        self.log.info(
            "Checking for expected output on OS ->{}<-"
            .format(os_type + " " + str(os_version))
            )
        if os_type == 'sle' and os_version >= 15:
            assert 'unrecognized command --striper' in output, \
                "ceph is compiled without libradosstriper"
        else:
            assert '--striper' not in output, \
                "ceph is compiled with libradosstriper"
        self.log.info("OK")

    def rados_write_test(self, **kwargs):
        self.scripts.run(
            self.master_remote,
            'rados_write_test.sh',
            )

    def systemd_units_active(self, **kwargs):
        """
        For all cluster nodes, determine which systemd services
        should be running and assert that the respective units
        are in "active" state.
        """
        # map role types to systemd units
        unit_map = {
            "mds": "ceph-mds@",
            "mgr": "ceph-mgr@",
            "mon": "ceph-mon@",
            "osd": "ceph-osd@",
            "rgw": "ceph-radosgw@",
            "ganesha": "nfs-ganesha"
            }
        # for each machine in the cluster
        idx = 0
        for rtl in self.role_types:
            node = self.nodes[idx]
            script = ("# validate systemd units on {}\n"
                      "set -ex\n").format(node)
            self.log.info("Machine {} ({}) has role types {}"
                          .format(idx, node, ','.join(rtl)))
            remote = self.remotes[node]
            run_script = False
            for role_type in rtl:
                if role_type in unit_map:
                    script += ("systemctl --state=active --type=service list-units "
                               "| grep -e '^{}'\n".format(unit_map[role_type]))
                    run_script = True
                else:
                    self.log.debug("Ignoring role_type {} which has no associated "
                                   "systemd unit".format(role_type))
            if run_script:
                remote_run_script_as_root(
                    remote,
                    "systemd_validation.sh",
                    script
                    )
            idx += 1

    def begin(self):
        self.log.debug("Processing tests: ->{}<-".format(self.config.keys()))
        for method_spec, kwargs in self.config.items():
            kwargs = {} if not kwargs else kwargs
            if not isinstance(kwargs, dict):
                raise ConfigError(self.err_prefix + "Method config must be a dict")
            self.log.info(anchored(
                "Running validation test {} with config ->{}<-"
                .format(method_spec, kwargs)
                ))
            method = getattr(self, method_spec, None)
            if method:
                method(**kwargs)
            else:
                raise ConfigError(self.err_prefix + "No such method ->{}<-"
                                  .format(method_spec))

    def end(self):
        pass

    def teardown(self):
        pass


task = DeepSea
ceph_conf = CephConf
create_pools = CreatePools
dummy = Dummy
health_ok = HealthOK
orch = Orch
policy = Policy
reboot = Reboot
repository = Repository
script = Script
toolbox = Toolbox
validation = Validation
