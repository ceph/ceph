"""
Task (and subtasks) for automating deployment of Ceph using DeepSea

Linter:
    flake8 --max-line-length=100
"""
import logging

from salt_manager import SaltManager
from util import (
    copy_directory_recursively,
    get_remote_for_role,
    sudo_append_to_file,
    )

from teuthology.exceptions import (
    CommandFailedError,
    ConnectionLostError,
    ConfigError,
    )
from teuthology.misc import (
    sh,
    sudo_write_file,
    write_file,
    )
from teuthology.orchestra import run
from teuthology.task import Task
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)
deepsea_ctx = {}
proposals_dir = "/srv/pillar/ceph/proposals"


def anchored(log_message):
    assert 'log_anchor' in deepsea_ctx, "deepsea_ctx not populated"
    return "{}{}".format(deepsea_ctx['log_anchor'], log_message)


def dump_file_that_might_not_exist(remote, fpath):
    try:
        remote.run(
            args="test -f {}".format(fpath)
            )
    except CommandFailedError:
        return None
    remote.run(
        args="ls -l {p} ; cat {p}".format(p=fpath)
        )


def remote_exec(remote, cmd_str, log_spec, quiet=True, rerun=False, tries=0):
    """
    Execute cmd_str and catch CommandFailedError and
    ConnectionLostError (and rerun command if retry is set)
    until one of the conditons are fulfilled:
    1) Execution succeeded
    2) Attempts are exceeded
    3) CommandFailedError is raised
    """
    cmd_str = "sudo bash -c '{}'".format(cmd_str)
    # if quiet:
    #     cmd_args += [run.Raw('2>'), "/dev/null"]
    already_rebooted_at_least_once = False
    if tries:
        remote.run(args="uptime")
        log.info("Running command ->{}<- on {}. This might cause the machine to reboot!"
                 .format(cmd_str, remote.hostname))
    with safe_while(sleep=60, tries=tries, action="wait for reconnect") as proceed:
        while proceed():
            try:
                if already_rebooted_at_least_once:
                    if not rerun:
                        remote.run(args="echo Back from reboot ; uptime")
                        break
                remote.run(args=cmd_str)
                break
            except CommandFailedError:
                log.error(anchored(
                    "{} failed. ".format(log_spec)
                    + "Here comes journalctl!"
                ))
                remote.run(args="sudo journalctl --all")
                raise
            except ConnectionLostError:
                already_rebooted_at_least_once = True
                if tries < 1:
                    raise
                log.warning("No connection established yet..")


def remote_run_script_as_root(remote, path, data, args=None):
    """
    Wrapper around write_file to simplify the design pattern:
    1. use write_file to create bash script on the remote
    2. use Remote.run to run that bash script via "sudo bash $SCRIPT"
    """
    write_file(remote, path, data)
    cmd = 'sudo bash {}'.format(path)
    if args:
        cmd += ' ' + ' '.join(args)
    remote.run(label=path, args=cmd)


class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve
    this).

    This task understands the following config keys which apply to
    this task and all its subtasks:

        alternative_defaults: (default: empty)
                        a dictionary of DeepSea alternative defaults
                        to be activated via the Salt Pillar
        cli:
            true        deepsea CLI will be used (the default)
            false       deepsea CLI will not be used
        log_anchor      a string (default: "WWWW: ") which will precede
                        log messages emitted at key points during the
                        deployment
        quiet_salt:
            true        suppress stderr on salt commands (the default)
            false       let salt commands spam the log
        rgw_ssl:
            true        use SSL if RGW is deployed
            false       if RGW is deployed, do not use SSL (the default)
        storage_profile:
            default     if a teuthology osd role is present on a node,
                        DeepSea will make all available disks into OSDs
            teuthology  populate DeepSea storage profile for 1:1 mapping
                        between teuthology osd roles and actual osds
                        deployed (the default, but not yet implemented)

    This task also understands the following config keys that affect
    the behavior of just this one task (no effect on subtasks):

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

    log_anchor_str = "WWWW: "

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)
        if deepsea_ctx:
            self.log = deepsea_ctx['logger_obj']
            self.log.debug(
                "deepsea_ctx already populated (we are in a subtask)")
        if not deepsea_ctx:
            deepsea_ctx['logger_obj'] = log
            self.log = log
            self.log.debug(
                "populating deepsea_ctx (we are *not* in a subtask)")
            self._populate_deepsea_context()
        self.alternative_defaults = deepsea_ctx['alternative_defaults']
        self.client_only_nodes = deepsea_ctx['client_only_nodes']
        self.cluster_nodes = deepsea_ctx['cluster_nodes']
        self.deepsea_cli = deepsea_ctx['cli']
        self.dev_env = deepsea_ctx['dev_env']
        self.gateway_nodes = deepsea_ctx['gateway_nodes']
        self.log_anchor = deepsea_ctx['log_anchor']
        self.master_remote = deepsea_ctx['master_remote']
        self.nodes = deepsea_ctx['nodes']
        self.quiet_salt = deepsea_ctx['quiet_salt']
        self.rgw_ssl = deepsea_ctx['rgw_ssl']
        self.roles = deepsea_ctx['roles']
        self.role_types = deepsea_ctx['role_types']
        self.role_lookup_table = deepsea_ctx['role_lookup_table']
        self.remotes = deepsea_ctx['remotes']
        self.scripts = Scripts(self.master_remote, deepsea_ctx['logger_obj'])
        self.sm = deepsea_ctx['salt_manager_instance']
        self.storage_profile = deepsea_ctx['storage_profile']
        self.storage_nodes = deepsea_ctx['storage_nodes']
        # self.log.debug("ctx.config {}".format(ctx.config))
        # self.log.debug("deepsea context: {}".format(deepsea_ctx))

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
                    "(deepsea task) test case calls for deepsea CLI, "
                    "but it is not installed"
                    )
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
        install_method = deepsea_ctx['install_method']
        if install_method == 'package':
            self._install_deepsea_using_zypper()
        elif install_method == 'source':
            self._install_deepsea_from_source()
        else:
            raise ConfigError(
                    "(deepsea task) unrecognized install config value ->{}<-"
                    .format(self.config['install'])
                )
        deepsea_ctx['deepsea_installed'] = True

    def _install_deepsea_from_source(self):
        self.log.info(anchored("installing deepsea from source"))
        if self.sm.master_rpm_q('deepsea'):
            self.log.info("DeepSea already installed from RPM")
            return None
        repo = self.config.get('repo', 'https://github.com/SUSE/DeepSea.git')
        branch = self.config.get('branch', 'master')
        self.log.info(
            "Installing DeepSea from source - repo: {}, branch: {}"
            .format(repo, branch)
            )
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

    def _install_deepsea_using_zypper(self):
        self.log.info(anchored("installing DeepSea using zypper"))
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

            nodes,
            cluster_nodes,
            gateway_nodes,
            storage_nodes, and
            client_only_nodes.

        These are all simple lists of hostnames.

        Also creates

            remotes,

        which is a dict of teuthology "remote" objects, which look like this:

            { remote1_name: remote1_obj, ..., remoten_name: remoten_obj }

        Also creates

            role_types

        which is just like the "roles" list, except it contains only unique
        role types per node.

        Finally, creates:

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
        cluster_roles = ['mon', 'mgr', 'osd', 'mds']
        gateway_roles = ['rgw', 'igw', 'ganesha']
        d_ctx = deepsea_ctx
        roles = d_ctx['roles']
        nodes = []
        cluster_nodes = []
        gateway_nodes = []
        storage_nodes = []
        client_only_nodes = []
        remotes = {}
        role_types = []
        role_lookup_table = {}
        remote_lookup_table = {}
        # introspection phase
        idx = 0
        for node_roles_list in roles:
            assert isinstance(node_roles_list, list), \
                "node_roles_list is a list"
            assert node_roles_list, "node_roles_list is not empty"
            remote = get_remote_for_role(self.ctx, node_roles_list[0])
            role_types.append([])
            self.log.debug("Considering remote name {}, hostname {}"
                           .format(remote.name, remote.hostname))
            nodes += [remote.hostname]
            remotes[remote.hostname] = remote
            remote_lookup_table[remote.hostname] = node_roles_list
            # inner loop: roles (something like "osd.1" or "c2.mon.a")
            for role in node_roles_list:
                # FIXME: support multiple clusters as used in, e.g.,
                # rgw/multisite suite
                role_arr = role.split('.')
                if len(role_arr) != 2:
                    raise ConfigError(
                        "(deepsea task) unsupported role ->{}<- encountered!"
                        .format(role)
                        )
                (role_type, _) = role_arr
                if role_type not in role_lookup_table:
                    role_lookup_table[role_type] = {}
                role_lookup_table[role_type][role] = remote.hostname
                if role_type in cluster_roles:
                    cluster_nodes += [remote.hostname]
                if role_type in gateway_roles:
                    gateway_nodes += [remote.hostname]
                if role_type == 'osd':
                    storage_nodes += [remote.hostname]
                if role_type not in role_types[idx]:
                    role_types[idx] += [role_type]
            idx += 1
        cluster_nodes = list(set(cluster_nodes))
        gateway_nodes = list(set(gateway_nodes))
        storage_nodes = list(set(storage_nodes))
        client_only_nodes = list(
            set(nodes).difference(set(cluster_nodes).union(set(gateway_nodes)))
            )
        self.log.debug(
            "client_only_nodes is ->{}<-".format(client_only_nodes)
            )
        assign_vars = [
            'nodes',
            'cluster_nodes',
            'gateway_nodes',
            'storage_nodes',
            'client_only_nodes',
            'remotes',
            'role_types',
            'role_lookup_table',
            'remote_lookup_table',
            ]
        for var in assign_vars:
            exec("deepsea_ctx['{var}'] = {var}".format(var=var))
        deepsea_ctx['dev_env'] = True if len(cluster_nodes) < 4 else False
        # report phase
        self.log.info("ROLE INTROSPECTION REPORT")
        report_vars = ['roles'] + assign_vars + ['dev_env']
        for var in report_vars:
            self.log.info("{} == {}".format(var, deepsea_ctx[var]))

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
            data = ''
            for k, v in self.alternative_defaults.items():
                data += "{}: {}\n".format(k, v)
                self.log.info("Applying alternative default {}: {}".format(k, v))
            if data:
                sudo_append_to_file(
                    self.master_remote,
                    global_yml,
                    data,
                    )
        dump_file_that_might_not_exist(self.master_remote, global_yml)

    def _populate_deepsea_context(self):
        deepsea_ctx['roles'] = self.ctx.config['roles']
        deepsea_ctx['alternative_defaults'] = self.config.get('alternative_defaults', {})
        if not isinstance(deepsea_ctx['alternative_defaults'], dict):
            raise ConfigError('(deepsea task) alternative_defaults must be a list')
        deepsea_ctx['cli'] = self.config.get('cli', True)
        deepsea_ctx['log_anchor'] = self.config.get('log_anchor', self.log_anchor_str)
        if not isinstance(deepsea_ctx['log_anchor'], str):
            self.log.warning(
                "log_anchor was set to non-string value ->{}<-, "
                "changing to empty string"
                .format(deepsea_ctx['log_anchor'])
                )
            deepsea_ctx['log_anchor'] = ''
        deepsea_ctx['storage_profile'] = self.config.get("storage_profile", "teuthology")
        if not isinstance(deepsea_ctx['storage_profile'], str):
            raise ConfigError(
                "(deepsea task) storage_profile config param must be a string"
                )
        deepsea_ctx['quiet_salt'] = self.config.get('quiet_salt', True)
        deepsea_ctx['salt_manager_instance'] = SaltManager(self.ctx)
        deepsea_ctx['master_remote'] = (
                deepsea_ctx['salt_manager_instance'].master_remote
                )
        deepsea_ctx['rgw_ssl'] = self.config.get('rgw_ssl', False)
        self._introspect_roles(deepsea_ctx)
        if 'install' in self.config:
            if self.config['install'] in ['package', 'pkg']:
                deepsea_ctx['install_method'] = 'package'
            elif self.config['install'] in ['source', 'src']:
                deepsea_ctx['install_method'] = 'source'
            else:
                raise ConfigError(
                        "(deepsea task) unrecognized install config value "
                        "->{}<-".format(self.config['install'])
                        )
        else:
            if 'repo' in self.config or 'branch' in self.config:
                deepsea_ctx['install_method'] = 'source'
            else:
                deepsea_ctx['install_method'] = 'package'

    def _purge_osds(self):
        # needed as long as teuthology install task purges /var/lib/ceph
        # in its teardown phase
        for _remote in self.ctx.cluster.remotes.iterkeys():
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

    def reboot_the_cluster_now(self, log_spec=None):
        if not log_spec:
            log_spec = "all nodes reboot now"
        cmd_str = "salt \\* cmd.run reboot"
        if self.quiet_salt:
            cmd_str += " 2> /dev/null"
        remote_exec(
            self.master_remote,
            cmd_str,
            log_spec,
            rerun=False,
            quiet=True,
            tries=5,
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
        self.log.debug("beginning of begin method")
        super(DeepSea, self).begin()
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
        self._master_python_version(2)
        if not self._master_python_version(3):
            raise ConfigError(
                "(deepsea task) Python 3 not installed on master node"
                " - bailing out!"
                )
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
        self.log.debug("end of begin method")

    def end(self):
        # self.log.debug("beginning of end method")
        super(DeepSea, self).end()
        pass
        # self.log.debug("end of end method")

    def teardown(self):
        self.log.debug("beginning of teardown method")
        super(DeepSea, self).teardown()
        #
        # the install task does "rm -r /var/lib/ceph" on every test node,
        # and that fails when there are OSDs running
        # FIXME - deprecated, remove after awhile
        self._purge_osds()
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
        deepsea_ctx['logger_obj'] = log.getChild('ceph_conf')
        super(CephConf, self).__init__(ctx, config)

    def _ceph_conf_d_full_path(self, section):
        if section in self.customize.keys():
            return "{}/{}".format(self.ceph_conf_d, self.customize[section])

    def _custom_ceph_conf(self, section, customizations):
        for conf_item, conf_value in customizations.iteritems():
            data = '{} = {}\n'.format(conf_item, conf_value)
            sudo_append_to_file(
                self.master_remote,
                self._ceph_conf_d_full_path(section),
                data
                )
            self.log.info(
                "Adding to ceph.conf, {} section: {}"
                .format(section, data)
                )

    def _dashboard(self):
        info_msg = "adjusted ceph.conf for deployment of dashboard MGR module"
        data = "mgr initial modules = dashboard\n"
        sudo_append_to_file(
            self.master_remote,
            self._ceph_conf_d_full_path("mon"),
            data,
            )
        self.log.info(info_msg)

    def _delete_rbd_default_features(self):
        """
        by removing this line, we ensure that there will be no "rbd default
        features" setting in ceph.conf, so the default value will be used
        """
        info_msg = "adjusted ceph.conf by removing 'rbd default features' line"
        rbd_conf = '/srv/salt/ceph/configuration/files/rbd.conf'
        cmd = 'sudo sed -i \'/^rbd default features =/d\' {}'.format(rbd_conf)
        self.master_remote.run(args=cmd)
        self.log.info(info_msg)

    def _dump_ceph_conf_d(self):
        self.master_remote.run(
            args="ls -lR {}".format(self.ceph_conf_d)
            )

    def _dump_customizations(self):
        for section in self.customize.keys():
            path = self._ceph_conf_d_full_path(section)
            try:
                self.master_remote.run(
                    args="test -f {}".format(path)
                    )
            except CommandFailedError:
                self.log.info(
                    "{} does not exist on the remote".format(path)
                    )
                continue
            self.master_remote.run(
                args="ls -l {p} ; cat {p}".format(p=path)
                )

    def _maybe_a_small_cluster(self):
        """
        Apply necessary ceph.conf for small clusters
        """
        storage_nodes = len(self.storage_nodes)
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
                self._ceph_conf_d_full_path("global"),
                data,
                )
            self.log.info(info_msg)

    def _mon_allow_pool_delete(self):
        info_msg = "adjusted ceph.conf to allow pool deletes"
        data = "mon allow pool delete = true\n"
        sudo_append_to_file(
            self.master_remote,
            self._ceph_conf_d_full_path("mon"),
            data,
            )
        self.log.info(info_msg)

    def begin(self):
        self.log.info(anchored("Adding custom options to ceph.conf"))
        if self.config.get('dashboard', True):
            self._dashboard()
        if self.config.get('mon_allow_pool_delete', True):
            self._mon_allow_pool_delete()
        if self.config.get('small_cluster', True):
            self._maybe_a_small_cluster()
        if self.config.get('rbd', False):
            self._delete_rbd_default_features()
        for section in self.customize.keys():
            if section in self.config and isinstance(self.config[section], dict):
                self._custom_ceph_conf(section, self.config[section])
        self._dump_ceph_conf_d()

    def teardown(self):
        pass


class CreatePools(DeepSea):

    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('create_pools')
        super(CreatePools, self).__init__(ctx, config)
        if not isinstance(self.config, dict):
            raise ConfigError(
                "(create_pools subtask) config must be a dictionary"
                )
        self.config = {
            "mds": self.config.get("mds", False),
            "openstack": self.config.get("openstack", False),
            "rbd": self.config.get("rbd", False),
            }

    def begin(self):
        self.log.info(anchored("pre-creating pools"))
        args = []
        for key in self.config:
            if self.config[key] is None:
                self.config[key] = True
            if self.config[key]:
                args.append(key)
        if 'mds' in self.role_lookup_table:
            args.append('mds')
        args = list(set(args))
        self.scripts.create_all_pools_at_once(args)

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


class Dummy(DeepSea):

    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('dummy')
        super(Dummy, self).__init__(ctx, config)

    def begin(self):
        self.log.debug("beginning of begin method")
        self.log.info("deepsea_ctx == {}".format(deepsea_ctx))
        self.log.debug("end of begin method")

    def teardown(self):
        pass


class Deploy:

    pass


class HealthOK(DeepSea):
    """
    Copy health_ok.sh to Salt Master node and run commands.

    This task understands the following config key:

        commands:
            [list of health-ok.sh commands]


    The list of commands will be executed as root on the Salt Master node.
    """

    prefix = 'health-ok/'

    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('health_ok')
        super(HealthOK, self).__init__(ctx, config)

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

    def setup(self):
        if 'health_ok_copied' not in deepsea_ctx:
            self._copy_health_ok()
            assert deepsea_ctx['health_ok_copied']

    def begin(self):
        commands = self.config.get('commands', [])
        if not isinstance(commands, list):
            raise ConfigError(
                "(health_ok subtask) commands must be a list of strings"
                )
        if not commands:
            self.log.warning(
                "The health_ok task was run, but no commands were specified. "
                "Doing nothing."
                )
        for cmd_str in commands:
            if not isinstance(cmd_str, str):
                raise ConfigError((
                    "(health_ok subtask) commands must be a list of strings. "
                    "Non-string command ->{}<- found!").format(cmd_str)
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

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


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
        deepsea_ctx['logger_obj'] = log.getChild('orch')
        super(Orch, self).__init__(ctx, config)
        # cast stage/state_orch value to str because it might be a number
        self.stage = str(self.config.get("stage", ''))
        self.state_orch = str(self.config.get("state_orch", ''))
        self.reboots_explicitly_forbidden = not self.config.get("allow_reboots", True)
        self.survive_reboots = self._detect_reboots()
        if not self.stage and not self.state_orch:
            raise ConfigError(
                "(orch subtask) nothing to do. Specify a value for 'stage' or "
                "'state_orch' key in config dict"
                )
        if self.stage and self.stage not in self.all_stages:
            raise ConfigError(
                "(orch subtask) unrecognized Stage ->{}<-".format(self.stage)
                )
        self.log.debug("munged config is {}".format(self.config))

    def __ceph_health_test(self):
        cmd = 'sudo salt-call wait.until status=HEALTH_OK timeout=900 check=1'
        if self.quiet_salt:
            cmd += ' 2> /dev/null'
        self.master_remote.run(args=cmd)

    def __check_salt_api_service(self):
        base_cmd = 'sudo systemctl status --full --lines={} {}.service'
        try:
            self.master_remote.run(args=base_cmd.format('0', 'salt-api'))
        except CommandFailedError:
            self.master_remote.run(args=base_cmd.format('100', 'salt-api'))
            raise
        self.scripts.salt_api_test()

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

    def __maybe_cat_ganesha_conf(self):
        ganesha_host = self.role_type_present('ganesha')
        if ganesha_host:
            ganesha_remote = self.remotes[ganesha_host]
            ganesha_remote.run(args="cat /etc/ganesha/ganesha.conf")

    def __log_stage_start(self, stage):
        self.log.info(anchored(
            "Running DeepSea Stage {} ({})"
            .format(stage, self.stage_synonyms[stage])
            ))

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
            self.scripts.rgw_init()
            if self.rgw_ssl:
                self.scripts.rgw_init_ssl()

    # FIXME: run on each minion individually, and compare deepsea "roles"
    # with teuthology roles!
    def _pillar_items(self):
        cmd = "sudo salt \\* pillar.items"
        if self.quiet_salt:
            cmd += " 2>/dev/null"
        self.master_remote.run(args=cmd)

    def _nfs_ganesha_no_root_squash(self):
        self.log.info("NFS-Ganesha set No_root_squash")
        ganeshaj2 = '/srv/salt/ceph/ganesha/files/ganesha.conf.j2'
        cmd = [
            "sudo", "sed", "-i",
            '/Access_Type = RW;/a \tSquash = No_root_squash;',
            ganeshaj2,
            ]
        self.master_remote.run(args=cmd)

    def _run_orch(self, orch_tuple):
        """Run an orchestration. Dump journalctl on error."""
        orch_type, orch_spec = orch_tuple
        if orch_type == 'orch':
            pass
        elif orch_type == 'stage':
            orch_spec = 'ceph.stage.{}'.format(orch_spec)
        else:
            raise ConfigError(
                "(orch subtask) Unrecognized orchestration type ->{}<-"
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
            if self.quiet_salt:
                cmd_str += ' 2>/dev/null'
        if self.dev_env:
            cmd_str = 'DEV_ENV=true ' + cmd_str
        tries = 0
        if self.survive_reboots:
            tries = 5
        remote_exec(
            self.master_remote,
            cmd_str,
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
        self._pillar_items()

    def _run_stage_3(self):
        """
        Run Stage 3
        """
        stage = 3
        self.__log_stage_start(stage)
        self._run_orch(("stage", stage))
        self.sm.all_minions_cmd_run(
            'cat /etc/ceph/ceph.conf',
            abort_on_fail=False
            )
        self.scripts.ceph_cluster_status()
        self.__ceph_health_test()

    def _run_stage_4(self):
        """
        Run Stage 4
        """
        stage = 4
        if self.role_type_present("ganesha"):
            self._nfs_ganesha_no_root_squash()
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
        self.log.debug("beginning of begin method")
        if self.state_orch:
            self.log.info(anchored(
                "running orchestration {}".format(self.state_orch)
                ))
            self._run_orch(("orch", self.state_orch))
            self.log.debug("end of begin method")
            return None
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
            self.log.info(
                "Running non-numeric Stage \"{}\"".format(self.stage)
                )
            self._run_orch(("stage", self.stage))
        else:
            raise ConfigError(
                '(orch subtask) unsupported stage ->{}<-'.format(self.stage)
                )
        self.log.debug("end of begin method")

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


class Policy(DeepSea):

    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('policy')
        super(Policy, self).__init__(ctx, config)
        self.munge_profile = self.config.get('munge_profile', {})

    def __build_profile_x(self, profile):
        self.profile_ymls_to_dump = []
        no_osd_roles = ("no osd roles configured, but at least "
                        "one of these is required.")
        role_dict = {}
        if 'osd' in self.role_lookup_table:
            role_dict = self.role_lookup_table['osd']
        else:
            raise ConfigError("(policy subtask) {}".format(no_osd_roles))
        self.log.debug(
            "generating policy.cfg lines for osd profile ->{}<- based on {}"
            .format(profile, role_dict)
            )
        if len(role_dict) == 0:
            raise ConfigError("(policy subtask) {}".format(no_osd_roles))
        osd_remotes = list(set(role_dict.values()))
        for osd_remote in osd_remotes:
            self.log.debug(
                "{} has one or more osd roles".format(osd_remote)
                )
            self.policy_cfg += """# Storage profile - {remote}
profile-{profile}/cluster/{remote}.sls
""".format(remote=osd_remote, profile=profile)
            ypp = ("profile-{}/stack/default/ceph/minions/{}.yml"
                   .format(profile, osd_remote))
            self.policy_cfg += ypp + "\n"
            self.profile_ymls_to_dump.append(
                "{}/{}".format(proposals_dir, ypp))

    def _build_base(self):
        """
        policy.cfg boilerplate
        """
        self.policy_cfg = """# policy.cfg generated by deepsea.policy subtask
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

    def _build_storage_profile(self):
        """
        Add storage profile to policy.cfg
        """
        if self.storage_profile == 'teuthology':
            raise ConfigError(
                "(policy subtask) \"teuthology\" storage profile not implemented yet"
                )
        elif self.storage_profile == 'default':
            self.__build_profile_x('default')
        else:
            ConfigError(
                "(policy subtask) unknown storage profile ->{}<-"
                .format(self.storage_profile)
                )

    def _build_x(self, role_type, required=False):
        no_roles_of_type = "no {} roles configured".format(role_type)
        but_required = ", but at least one of these is required."
        role_dict = {}
        if role_type in self.role_lookup_table:
            role_dict = self.role_lookup_table[role_type]
        elif required:
            raise ConfigError(
                "(policy subtask) {}".format(no_roles_of_type + but_required)
                )
        else:
            self.log.debug(no_roles_of_type)
            return None
        self.log.debug(
            "generating policy.cfg lines for {} based on {}"
            .format(role_type, role_dict)
            )
        if required:
            if len(role_dict.keys()) < 1:
                raise ConfigError(
                    "(policy subtask) {}"
                    .format(no_roles_of_type + but_required)
                    )
        for role_spec, remote_name in role_dict.iteritems():
            self.policy_cfg += (
                '# Role assignment - {}\n'
                'role-{}/cluster/{}.sls\n'
                ).format(role_spec, role_type, remote_name)

    def _cat_policy_cfg(self):
        """
        Dump the remote policy.cfg file to teuthology log.
        """
        self.master_remote.run(args=[
            'cat',
            proposals_dir + "/policy.cfg"
            ])

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
            proposals_dir,
            run.Raw(';'),
            'ls',
            '-lR',
            proposals_dir + '/',
            ])

    def _first_storage_only_node(self):
        for hostname in self.nodes:
            storage = hostname in self.storage_nodes
            cluster = hostname in self.cluster_nodes
            gateway = hostname in self.gateway_nodes
            if storage and not cluster and not gateway:
                return hostname
        return ''

    def _write_policy_cfg(self):
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

    def begin(self):
        """
        Generate policy.cfg from the results of role introspection
        """
        if self.munge_profile:
            for k, v in self.munge_profile.items():
                if k == 'proposals_remove_storage_only_node':
                    delete_me = self._first_storage_only_node()
                    if delete_me:
                        self.scripts.proposals_remove_storage_only_node(delete_me)
                    else:
                        raise ConfigError(
                            '(policy subtask) proposals_remove_storage_only_node '
                            'requires a storage-only node, but there is no such'
                            )
                else:
                    raise ConfigError(
                        "(policy subtask) unrecognized munge_profile directive {}"
                        .format(k)
                        )
        else:
            self.log.info(anchored("generating policy.cfg"))
            self._dump_proposals_dir()
            self._build_base()
            self._build_x('mon', required=True)
            self._build_x('mgr', required=True)
            self._build_x('mds')
            self._build_x('rgw')
            self._build_x('igw')
            self._build_x('ganesha')
            self._build_storage_profile()
            self._write_policy_cfg()
            self._cat_policy_cfg()
            self._dump_profile_ymls()

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


class Reboot(DeepSea):
    """
    A class that does nothing but unconditionally reboot the whole cluster.
    """
    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('reboot')
        super(Reboot, self).__init__(ctx, config)

    def begin(self):
        log_spec = "all nodes reboot now"
        self.log.warning(anchored(log_spec))
        self.reboot_the_cluster_now(log_spec=log_spec)

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


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
curl http://$(hostname):8000/ | tee $TMPFILE # show curl output in log
test -s $TMPFILE
jq . $TMPFILE >/dev/null
echo -en "\\n" # this is just for log readability
rm $TMPFILE
echo "Salt API test: END"
""",
        "rgw_init": """# Set up RGW
set -ex
USERSYML=/srv/salt/ceph/rgw/users/users.d/rgw.yml
cat <<EOF > $USERSYML
- { uid: "demo", name: "Demo", email: "demo@demo.nil" }
- { uid: "demo1", name: "Demo1", email: "demo1@demo.nil" }
EOF
cat $USERSYML
""",
        "rgw_init_ssl": """# Set up RGW-over-SSL
set -ex
CERTDIR=/srv/salt/ceph/rgw/cert
mkdir -p $CERTDIR
pushd $CERTDIR
openssl req -x509 \
        -nodes \
        -days 1095 \
        -newkey rsa:4096 \
        -keyout rgw.key \
        -out rgw.crt \
        -subj "/C=DE"
cat rgw.key > rgw.pem && cat rgw.crt >> rgw.pem
popd
GLOBALYML=/srv/pillar/ceph/stack/global.yml
cat <<EOF >> $GLOBALYML
rgw_init: default-ssl
EOF
cat $GLOBALYML
cp /srv/salt/ceph/configuration/files/rgw-ssl.conf \
    /srv/salt/ceph/configuration/files/ceph.conf.d/rgw.conf
""",
        "proposals_remove_storage_only_node": """# remove first storage-only node from proposals
PROPOSALSDIR=$1
STORAGE_PROFILE=$2
NODE_TO_DELETE=$3
#
echo "Before"
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/
#
basedirsls=$PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster
basediryml=$PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions
mv $basedirsls/${NODE_TO_DELETE}.sls $basedirsls/${NODE_TO_DELETE}.sls-DISABLED
mv $basediryml/${NODE_TO_DELETE}.yml $basedirsls/${NODE_TO_DELETE}.yml-DISABLED
#
echo "After"
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/
""",
        }

    def __init__(self, master_remote, logger):
        self.log = logger
        self.master_remote = master_remote

    def ceph_cluster_status(self):
        remote_run_script_as_root(
            self.master_remote,
            'ceph_cluster_status.sh',
            self.script_dict["ceph_cluster_status"],
            )

    def create_all_pools_at_once(self, args):
        self.log.info("creating pools: {}".format(' '.join(args)))
        remote_run_script_as_root(
            self.master_remote,
            'create_all_pools_at_once.sh',
            self.script_dict["create_all_pools_at_once"],
            args=args,
            )

    def disable_update_in_stage_0(self):
        remote_run_script_as_root(
            self.master_remote,
            'disable_update_in_stage_0.sh',
            self.script_dict["disable_update_in_stage_0"],
            )

    def proposals_remove_storage_only_node(self, hostname):
        remote_run_script_as_root(
            self.master_remote,
            'proposals_remove_storage_only_node.sh',
            self.script_dict["proposals_remove_storage_only_node"],
            args=[proposals_dir, self.storage_profile, hostname],
            )

    def rgw_init(self):
        remote_run_script_as_root(
            self.master_remote,
            'rgw_init.sh',
            self.script_dict["rgw_init"],
            )

    def rgw_init_ssl(self):
        remote_run_script_as_root(
            self.master_remote,
            'rgw_init_ssl.sh',
            self.script_dict["rgw_init_ssl"],
            )

    def salt_api_test(self):
        remote_run_script_as_root(
            self.master_remote,
            'salt_api_test.sh',
            self.script_dict["salt_api_test"],
            )


class Validation(DeepSea):
    """
    A container for "validation tests", which are understood to mean
    tests that validate the Ceph cluster (just) deployed by DeepSea.

    Basic validation tests are triggered by default, while others
    have to be explicitly mentioned in the YAML:

    systemd_units_active    triggered by default, validates that the systemd
                            units corresponding to the teuthology roles
                            stanza are active on the respective test nodes

    Each test can have its own configuration. See the deepsea suite YAML
    for examples.
    """
    def __init__(self, ctx, config):
        deepsea_ctx['logger_obj'] = log.getChild('validation')
        super(Validation, self).__init__(ctx, config)
        self._apply_config_default("systemd_units_active", None)

    def _apply_config_default(self, validation_test, default_config):
        """
        Use to activate tests that should always be run.
        """
        self.config[validation_test] = self.config.get(validation_test,
                                                       default_config)

    def systemd_units_active(self, config):
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
            script = (
                "# validate systemd units on {}\n"
                "set -ex\n").format(node)
            self.log.info(
                "Machine {} ({}) has role types {}"
                .format(idx, node, ','.join(rtl))
                )
            remote = self.remotes[node]
            run_script = False
            for role_type in rtl:
                if role_type in unit_map:
                    script += (
                        'systemctl --state=active --type=service list-units '
                        '| grep -e \'^{}\'\n').format(unit_map[role_type])
                    run_script = True
                else:
                    self.log.debug((
                        "Ignoring role_type {} which has no associated "
                        "systemd unit").format(role_type)
                        )
            if run_script:
                self.log.debug("About to run:\n" + script)
                remote_run_script_as_root(
                    remote, "systemd_validation.sh", script
                    )
            idx += 1

    def begin(self):
        """
        Process self.config, which is taken to be a dictionary in which the
        keys are the names of tests (methods to be run) and the values are the
        config dictionaries of each test to be run.
        """
        self.log.debug("Processing tests: {}".format(self.config))
        for validation_test, test_config in self.config.iteritems():
            self.log.debug(
                "Test {} has config ->{}<-"
                .format(validation_test, test_config)
                )
            method = getattr(self, validation_test, None)
            if method:
                method(test_config)
            else:
                raise ConfigError(
                    "(validation subtask) No such method ->{}<-"
                    .format(method)
                    )

    def teardown(self):
        # self.log.debug("beginning of teardown method")
        pass
        # self.log.debug("end of teardown method")


task = DeepSea
ceph_conf = CephConf
create_pools = CreatePools
deploy = Deploy
dummy = Dummy
health_ok = HealthOK
orch = Orch
policy = Policy
reboot = Reboot
validation = Validation
