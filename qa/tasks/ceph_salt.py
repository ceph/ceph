'''
Task that deploys a Ceph cluster on all the nodes
using Ceph-salt
Linter:
    flake8 --max-line-length=100
'''
import logging
import argparse
import uuid

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.task import Task

from tasks.ceph import get_mons
from tasks.salt_manager import SaltManager
from tasks.scripts import Scripts
from tasks.util import (
    introspect_roles,
    remote_exec,
    )


log = logging.getLogger(__name__)
ceph_salt_ctx = {}
reboot_tries = 50


def anchored(log_message):
    global ceph_salt_ctx
    assert 'log_anchor' in ceph_salt_ctx, "ceph_salt_ctx not populated"
    return "{}{}".format(ceph_salt_ctx['log_anchor'], log_message)


class CephSalt(Task):
    """
    Deploy a Ceph cluster on all remotes using
    Ceph-salt (https://github.com/SUSE/ceph-salt)

    Assumes a Salt cluster is already running (use the Salt task to achieve
    this).

    This task understands the following config keys which apply to
    this task and all its subtasks:

        log_anchor      a string (default: "WWWW: ") which will precede
                        log messages emitted at key points during the
                        deployment
        quiet_salt:
            true        suppress stderr on salt commands (the default)
            false       let salt commands spam the log
        allow_reboots:
            true        Allow cluster nodes to be rebooted if needed (default)
            false
        deploy:
            true        Enable role deployment on ceph-salt (default)
            false
        repo:           Ceph-salt repo for building it from source. If no repo
                        is provided Ceph-salt will be installed from packages
                        using zypper.
        branch:         Ceph-salt branch in case repo is provided. If no branch
                        is provided master is used by default.
        containers:
            registry:
                name: registry name
                location: <registry url>
                insecure: True/False    (Is registry insecure, True by default)
            ceph_image:  <container image path>


    For example:

        containers:
            registry:
                name: 'registry.suse.com'
                location: 'registry.suse.com:5000'
                insecure: True
            ceph_image: 'registry.suse.de/devel/storage/7.0/containers/ses/7/ceph/ceph'

    There is also the possibility of using overrides in order to set the
    container registry and image like:

        overrides:
            ceph_salt:
                containers:
                    registry:
                        name: 'registry.mirror.example.com'
                        location: 'registry.mirror.example.com:5000'
                    ceph_image: 'quay.io/ceph-ci/ceph'
    """

    err_prefix = "(ceph_salt task) "

    log_anchor_str = "WWWW: "

    def __init__(self, ctx, config):
        super(CephSalt, self).__init__(ctx, config)
        log.debug("beginning of constructor method")
        overrides = ctx.config.get('overrides', {})
        misc.deep_merge(self.config, overrides.get('ceph_salt', {}))
        if not ceph_salt_ctx:
            self._populate_ceph_salt_context()
            self.log_anchor = ceph_salt_ctx['log_anchor']
            introspect_roles(self.ctx, self.log, quiet=False)
            self.ctx['roles'] = self.ctx.config['roles']
            self.log = log
        self.reboots_explicitly_forbidden = not self.config.get(
                                                        "allow_reboots", True)
        self.master_remote = ceph_salt_ctx['master_remote']
        self.quiet_salt = ceph_salt_ctx['quiet_salt']
        self.nodes = self.ctx['nodes']
        self.nodes_storage = self.ctx['nodes_storage']
        self.nodes_storage_only = self.ctx['nodes_storage_only']
        self.remotes = self.ctx['remotes']
        self.roles = self.ctx['roles']
        self.sm = ceph_salt_ctx['salt_manager_instance']
        self.role_types = self.ctx['role_types']
        self.remote_lookup_table = self.ctx['remote_lookup_table']
        self.ceph_salt_deploy = ceph_salt_ctx['deploy']
        self.cluster = self.config.get('cluster', 'ceph')
        self.testdir = misc.get_testdir(self.ctx)
        self.config['cephadm_mode'] = 'cephadm-package'
        self.ctx.cephadm = 'cephadm'
        self.ctx.daemons = DaemonGroup(use_cephadm=self.ctx.cephadm)
        if not hasattr(self.ctx, 'ceph'):
            self.ctx.ceph = {}
            self.ctx.managers = {}
        self.ctx.ceph[self.cluster] = argparse.Namespace()
        self.ctx.ceph[self.cluster].thrashers = []
        self.scripts = Scripts(self.ctx, self.log)
        self.bootstrap_remote = None

    def _install_ceph_salt(self):
        '''
        Installs ceph-salt on master either from source if repo and/or
        branch are provided in the suite yaml or from rpm if not
        '''
        global ceph_salt_ctx
        if ceph_salt_ctx['repo']:
            if not ceph_salt_ctx['branch']:
                self.scripts.run(
                    self.master_remote,
                    'install_ceph_salt.sh',
                    args=ceph_salt_ctx['repo']
                    )
            else:
                self.scripts.run(
                    self.master_remote,
                    'install_ceph_salt.sh',
                    args=[ceph_salt_ctx['repo'], ceph_salt_ctx['branch']]
                    )
        else:
            self.scripts.run(
                self.master_remote,
                'install_ceph_salt.sh'
                )
        self.ctx.cluster.run(args='sudo systemctl restart salt-minion')
        self.master_remote.sh("sudo systemctl restart salt-master")

    def _populate_ceph_salt_context(self):
        global ceph_salt_ctx
        ceph_salt_ctx['log_anchor'] = self.config.get(
                                            'log_anchor', self.log_anchor_str)
        if not isinstance(ceph_salt_ctx['log_anchor'], str):
            self.log.warning(
                "log_anchor was set to non-string value ->{}<-, "
                "changing to empty string"
                .format(ceph_salt_ctx['log_anchor'])
                )
            ceph_salt_ctx['log_anchor'] = ''
        ceph_salt_ctx['deploy'] = self.config.get('deploy', True)
        ceph_salt_ctx['quiet_salt'] = self.config.get('quiet_salt', True)
        ceph_salt_ctx['salt_manager_instance'] = SaltManager(self.ctx)
        ceph_salt_ctx['master_remote'] = (
                ceph_salt_ctx['salt_manager_instance'].master_remote
                )
        ceph_salt_ctx['repo'] = self.config.get('repo', None)
        ceph_salt_ctx['branch'] = self.config.get('branch', None)
        containers = self.config.get('containers', {})
        self.log.info("Containers dict is: {}".format(containers))
        registries = containers.get('registry', {})
        ceph_salt_ctx['registry_name'] = registries.get('name', None)
        ceph_salt_ctx['registry_location'] = registries.get('location', None)
        ceph_salt_ctx['registry_insecure'] = registries.get('insecure', True)
        ceph_salt_ctx['container_image'] = containers.get('ceph_image', None)
        if not ceph_salt_ctx['container_image']:
            raise Exception("Configuration error occurred. "
                            "The 'image' value is undefined. Please provide "
                            "corresponding options in config or overrides")

    def _get_bootstrap_remote(self):
        '''
        Get the bootstrap node that's one with 'mon' and 'mgr' roles
        and will be used by ceph-salt for bootstraping the cluster and then
        by cephadm to deploy the rest of the nodes
        '''
        for host, roles in self.remote_lookup_table.items():
            # possibly use teuthology.is_type() here
            if ("mon" in [r.split('.')[0] for r in roles] and
                    "mgr" in [r.split('.')[0] for r in roles]):
                self.bootstrap_remote = self.remotes[host]
                break
        if not self.bootstrap_remote:
            raise ConfigError("No possible bootstrap minion found."
                              " Please check the provided roles")
        self.log.info("Bootstrap minion is: {}"
                      .format(self.bootstrap_remote.hostname))
        cluster_name = self.cluster
        fsid = str(uuid.uuid1())
        self.log.info('Cluster fsid is %s' % fsid)
        self.ctx.ceph[cluster_name].fsid = fsid
        fsid = self.ctx.ceph[cluster_name].fsid
        self.ctx.ceph[cluster_name].bootstrap_remote = self.bootstrap_remote
        for roles in self.remote_lookup_table[self.bootstrap_remote.hostname]:
            _, role, role_id = misc.split_role(roles)
            if role == 'mon':
                self.ctx.ceph[cluster_name].first_mon = role_id
                break
        for roles in self.remote_lookup_table[self.bootstrap_remote.hostname]:
            _, role, role_id = misc.split_role(roles)
            if role == 'mgr':
                self.ctx.ceph[cluster_name].first_mgr = role_id
                break
        self.log.info('First mon is mon.%s on %s' % (
                                        self.ctx.ceph[cluster_name].first_mon,
                                        self.bootstrap_remote.shortname))
        self.log.info('First mgr is mgr.%s on %s' % (
                                        self.ctx.ceph[cluster_name].first_mgr,
                                        self.bootstrap_remote.shortname))

        remotes_and_roles = self.ctx.cluster.remotes.items()
        roles = [role_list for (remote, role_list) in remotes_and_roles]
        ips = [host for (host, port) in
               (remote.ssh.get_transport().getpeername()
                for (remote, role_list) in remotes_and_roles)]
        self.ctx.ceph[cluster_name].mons = get_mons(
            roles, ips, self.cluster,
            mon_bind_msgr2=self.config.get('mon_bind_msgr2', True),
            mon_bind_addrvec=self.config.get('mon_bind_addrvec', True),
            )
        log.info('Monitor IPs: %s' % self.ctx.ceph[cluster_name].mons)

    def _ceph_salt_bootstrap(self):
        '''
        This function populates ceph-salt config according to the
        configuration on the yaml files on the suite regarding node roles,
        chrony server, dashboard credentials etc and then runs the cluster
        deployment
        '''
        fsid = self.ctx.ceph[self.cluster].fsid
        first_mon = self.ctx.ceph[self.cluster].first_mon
        first_mgr = self.ctx.ceph[self.cluster].first_mgr
        for host, _ in self.remote_lookup_table.items():
            self.master_remote.sh("sudo ceph-salt config /ceph_cluster/minions"
                                  " add {}".format(host))
            self.master_remote.sh("sudo ceph-salt config "
                                  "/ceph_cluster/roles/cephadm "
                                  "add {}".format(host))
            self.master_remote.sh("sudo ceph-salt config "
                                  "/ceph_cluster/roles/admin "
                                  "add {}".format(host))
        if len(self.remote_lookup_table.keys()) <= 3:
            self.master_remote.sh("sudo ceph-salt config "
                                  "/cephadm_bootstrap/ceph_conf add global")
            self.master_remote.sh("sudo ceph-salt config "
                                  "/cephadm_bootstrap/ceph_conf/global set"
                                  " \"osd crush chooseleaf type\" 0")
        self.master_remote.sh("sudo ceph-salt config "
                              "/ceph_cluster/roles/bootstrap "
                              "set {}".format(self.bootstrap_remote.hostname))
        self.master_remote.sh("sudo ceph-salt config "
                              "/system_update/packages disable")
        self.master_remote.sh("sudo ceph-salt config "
                              "/system_update/reboot disable")
        self.master_remote.sh("sudo ceph-salt config /ssh/ generate")
        self.master_remote.sh("sudo ceph-salt config /containers/"
                              "registries_conf/registries "
                              "add  prefix={name}"
                              " location={loc} insecure={insec}"
                              .format(name=ceph_salt_ctx['registry_name'],
                                      loc=ceph_salt_ctx['registry_location'],
                                      insec=ceph_salt_ctx['registry_insecure']
                                      ))
        self.master_remote.sh("sudo ceph-salt config /containers/images/ceph "
                              "set {}"
                              .format(ceph_salt_ctx['container_image']))
        self.ctx.ceph[self.cluster].image = ceph_salt_ctx['container_image']
        self.master_remote.sh("sudo ceph-salt "
                              "config /time_server/server_hostname set {}"
                              .format(self.master_remote.hostname))
        self.master_remote.sh("sudo ceph-salt config "
                              "/time_server/external_servers add"
                              " 0.pt.pool.ntp.org")
        self.master_remote.sh("sudo ceph-salt config "
                              "/cephadm_bootstrap/advanced "
                              "set mon-id {}".format(first_mon))
        self.master_remote.sh("sudo ceph-salt config "
                              "/cephadm_bootstrap/advanced set "
                              "mgr-id {}".format(first_mgr))
        self.master_remote.sh("sudo ceph-salt config "
                              "/cephadm_bootstrap/advanced set "
                              "fsid {}".format(fsid))
        self.master_remote.sh("sudo ceph-salt config "
                              "/cephadm_bootstrap/dashboard/username "
                              "set admin")
        self.master_remote.sh("sudo ceph-salt config "
                              "/cephadm_bootstrap/dashboard/password "
                              "set admin")
        self.master_remote.sh("sudo ceph-salt config ls")
        if self.ceph_salt_deploy:
            self.master_remote.sh("sudo stdbuf -o0 ceph-salt -ldebug apply"
                                  " --non-interactive")
            self.ctx.ceph[self.cluster].bootstrapped = True
            # register initial daemons
            self.ctx.daemons.register_daemon(
                self.bootstrap_remote, 'mon', first_mon,
                cluster=self.cluster,
                fsid=fsid,
                logger=log.getChild('mon.' + first_mon),
                wait=False,
                started=True,
                )
            self.ctx.daemons.register_daemon(
                self.bootstrap_remote, 'mgr', first_mgr,
                cluster=self.cluster,
                fsid=fsid,
                logger=log.getChild('mgr.' + first_mgr),
                wait=False,
                started=True,
                )
            # fetch keys and configs
            log.info('Fetching config...')
            self.ctx.ceph[self.cluster].config_file = misc.get_file(
                remote=self.bootstrap_remote,
                path='/etc/ceph/{}.conf'.format(self.cluster))
            log.info('Fetching client.admin keyring...')
            self.ctx.ceph[self.cluster].admin_keyring = misc.get_file(
                remote=self.bootstrap_remote,
                path='/etc/ceph/{}.client.admin.keyring'.format(self.cluster),
                sudo=True)
            log.info('Fetching mon keyring...')
            self.ctx.ceph[self.cluster].mon_keyring = misc.get_file(
                remote=self.bootstrap_remote,
                path='/var/lib/ceph/%s/mon.%s/keyring' % (fsid, first_mon),
                sudo=True)

    def __zypper_ps_with_possible_reboot(self):
        if self.sm.all_minions_zypper_ps_requires_reboot():
            log_spec = "Detected updates requiring reboot"
            self.log.warning(anchored(log_spec))
            if self.reboots_explicitly_forbidden:
                self.log.info("Reboots explicitly forbidden in test "
                              "configuration: not rebooting")
                self.log.warning("Processes using deleted files may "
                                 "cause instability")
            else:
                self.log.warning(anchored("Rebooting the whole cluster now!"))
                self.reboot_the_cluster_now(log_spec=log_spec)
                assert not self.sm.all_minions_zypper_ps_requires_reboot(), \
                    "No more updates requiring reboot anywhere "\
                    "in the whole cluster"

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

    def begin(self):
        global ceph_salt_ctx
        super(CephSalt, self).begin()
        self._get_bootstrap_remote()
        self._install_ceph_salt()
        self.sm.ping_minions()
        self.sm.all_minions_zypper_ref()
        self.sm.all_minions_zypper_up_if_needed()
        self.__zypper_ps_with_possible_reboot()
        self.sm.sync_pillar_data(quiet=self.quiet_salt)
        self._ceph_salt_bootstrap()

    def end(self):
        self.log.debug("beginning of end method")
        super(CephSalt, self).end()
        success = self.ctx.summary.get('success', None)
        if success is None:
            self.log.warning("Problem with ctx summary key? ctx is {}"
                             .format(self.ctx))
        if not success:
            self.ctx.cluster.run(args="rpm -qa | sort")
        self.log.debug("end of end method")

    def teardown(self):
        self.log.debug("beginning of teardown method")
        super(CephSalt, self).teardown()
        self.log.debug("end of teardown method")


task = CephSalt
