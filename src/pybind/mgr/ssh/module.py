import json
import errno
import logging
from functools import wraps

import string
import six
import os
import random
import tempfile
import multiprocessing.pool

from mgr_module import MgrModule
import orchestrator

from . import remotes

try:
    import remoto
    import remoto.process
except ImportError as e:
    remoto = None
    remoto_import_error = str(e)

logger = logging.getLogger(__name__)

DEFAULT_SSH_CONFIG = ('Host *\n'
                      'User root\n'
                      'StrictHostKeyChecking no\n')

# high-level TODO:
#  - bring over some of the protections from ceph-deploy that guard against
#    multiple bootstrapping / initialization

class SSHCompletionmMixin(object):
    def __init__(self, result):
        if isinstance(result, multiprocessing.pool.AsyncResult):
            self._result = [result]
        else:
            self._result = result
        assert isinstance(self._result, list)

    @property
    def result(self):
        return list(map(lambda r: r.get(), self._result))

class SSHReadCompletion(SSHCompletionmMixin, orchestrator.ReadCompletion):
    @property
    def is_complete(self):
        return all(map(lambda r: r.ready(), self._result))


class SSHWriteCompletion(SSHCompletionmMixin, orchestrator.WriteCompletion):

    @property
    def is_persistent(self):
        return all(map(lambda r: r.ready(), self._result))

    @property
    def is_effective(self):
        return all(map(lambda r: r.ready(), self._result))

    @property
    def is_errored(self):
        for r in self._result:
            if not r.ready():
                return False
            if not r.successful():
                return True
        return False

class SSHWriteCompletionReady(SSHWriteCompletion):
    def __init__(self, result):
        orchestrator.WriteCompletion.__init__(self)
        self._result = result

    @property
    def result(self):
        return self._result

    @property
    def is_persistent(self):
        return True

    @property
    def is_effective(self):
        return True

    @property
    def is_errored(self):
        return False

def log_exceptions(f):
    if six.PY3:
        return f
    else:
        # Python 2 does no exception chaining, thus the
        # real exception is lost
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception:
                logger.exception('something went wrong.')
                raise
        return wrapper


class SSHOrchestrator(MgrModule, orchestrator.Orchestrator):

    _STORE_HOST_PREFIX = "host"

    NATIVE_OPTIONS = []
    MODULE_OPTIONS = [
        {
            'name': 'ssh_config_file',
            'type': 'str',
            'default': None,
            'desc': 'customized SSH config file to connect to managed hosts',
        },
        {
            'name': 'inventory_cache_timeout',
            'type': 'int',
            'default': 10,
            'desc': 'seconds to cache device inventory',
        },
    ]

    COMMANDS = [
        {
            'cmd': 'ssh set-ssh-config',
            'desc': 'Set the ssh_config file (use -i <ssh_config>)',
            'perm': 'rw'
        },
        {
            'cmd': 'ssh clear-ssh-config',
            'desc': 'Clear the ssh_config file',
            'perm': 'rw'
        },
    ]

    def __init__(self, *args, **kwargs):
        super(SSHOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = self.get('mon_map')['fsid']

        self.config_notify()

        path = self.get_ceph_option('ceph_daemon_path')
        try:
            with open(path, 'r') as f:
                self._ceph_daemon = f.read()
        except IOError as e:
            raise RuntimeError("unable to read ceph-daemon at '%s': %s" % (
                path, str(e)))

        self._worker_pool = multiprocessing.pool.ThreadPool(1)

        self._reconfig_ssh()

        # the keys in inventory_cache are authoritative.
        #   You must not call remove_outdated()
        # The values are cached by instance.
        # cache is invalidated by
        # 1. timeout
        # 2. refresh parameter
        self.inventory_cache = orchestrator.OutdatablePersistentDict(
            self, self._STORE_HOST_PREFIX + '.devices')

        self.daemon_cache = orchestrator.OutdatablePersistentDict(
            self, self._STORE_HOST_PREFIX + '.daemons')

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))

    def get_unique_name(self, existing, prefix=None):
        """
        Generate a unique random service name
        """
        while True:
            if prefix:
                name = prefix + '-'
            else:
                name = ''
            name += ''.join(random.choice(string.ascii_lowercase)
                            for i in range(6))
            if len([d for d in existing if d.service_instance == name]):
                self.log('name %s exists, trying again', name)
                continue
            return name

    def _reconfig_ssh(self):
        temp_files = []
        ssh_options = []

        # ssh_config
        ssh_config_fname = self.ssh_config_file
        ssh_config = self.get_store("ssh_config")
        if ssh_config is not None or ssh_config_fname is None:
            if not ssh_config:
                ssh_config = DEFAULT_SSH_CONFIG
            f = tempfile.NamedTemporaryFile(prefix='ceph-mgr-ssh-conf-')
            os.fchmod(f.fileno(), 0o600);
            f.write(ssh_config.encode('utf-8'))
            f.flush() # make visible to other processes
            temp_files += [f]
            ssh_config_fname = f.name
        if ssh_config_fname:
            if not os.path.isfile(ssh_config_fname):
                raise Exception("ssh_config \"{}\" does not exist".format(
                    ssh_config_fname))
            ssh_options += ['-F', ssh_config_fname]

        # identity
        ssh_key = self.get_store("ssh_identity_key")
        ssh_pub = self.get_store("ssh_identity_pub")
        tpub = None
        tkey = None
        if ssh_key and ssh_pub:
            tkey = tempfile.NamedTemporaryFile(prefix='ceph-mgr-ssh-identity-')
            tkey.write(ssh_key.encode('utf-8'))
            os.fchmod(tkey.fileno(), 0o600);
            tkey.flush() # make visible to other processes
            tpub = open(tkey.name + '.pub', 'w')
            os.fchmod(tpub.fileno(), 0o600);
            tpub.write(ssh_pub)
            tpub.flush() # make visible to other processes
            temp_files += [tkey, tpub]
            ssh_options += ['-i', tkey.name]

        self._temp_files = temp_files
        if ssh_options:
            self._ssh_options = ' '.join(ssh_options)
        else:
            self._ssh_options = None
        self.log.info('ssh_options %s' % ssh_options)

    def handle_command(self, inbuf, command):
        if command["prefix"] == "ssh set-ssh-config":
            return self._set_ssh_config(inbuf, command)
        elif command["prefix"] == "ssh clear-ssh-config":
            return self._clear_ssh_config(inbuf, command)
        else:
            raise NotImplementedError(command["prefix"])

    @staticmethod
    def can_run():
        if remoto is not None:
            return True, ""
        else:
            return False, "loading remoto library:{}".format(
                    remoto_import_error)

    def available(self):
        """
        The SSH orchestrator is always available.
        """
        return self.can_run()

    def wait(self, completions):
        self.log.info("wait: completions={}".format(completions))

        complete = True
        for c in completions:
            if c.is_complete:
                continue

            if not isinstance(c, SSHReadCompletion) and \
                    not isinstance(c, SSHWriteCompletion):
                raise TypeError("unexpected completion: {}".format(c.__class__))

            complete = False

        return complete

    def _require_hosts(self, hosts):
        """
        Raise an error if any of the given hosts are unregistered.
        """
        if isinstance(hosts, six.string_types):
            hosts = [hosts]
        keys = self.inventory_cache.keys()
        unregistered_hosts = set(hosts) - keys
        if unregistered_hosts:
            logger.warning('keys = {}'.format(keys))
            raise RuntimeError("Host(s) {} not registered".format(
                ", ".join(map(lambda h: "'{}'".format(h),
                    unregistered_hosts))))

    def _set_ssh_config(self, inbuf, command):
        """
        Set an ssh_config file provided from stdin

        TODO:
          - validation
        """
        if len(inbuf) == 0:
            return errno.EINVAL, "", "empty ssh config provided"
        self.set_store("ssh_config", inbuf)
        return 0, "", ""

    def _clear_ssh_config(self, inbuf, command):
        """
        Clear the ssh_config file provided from stdin
        """
        self.set_store("ssh_config", None)
        self.ssh_config_tmp = None
        return 0, "", ""

    def _get_connection(self, host):
        """
        Setup a connection for running commands on remote host.
        """
        self.log.info("opening connection to host '{}' with ssh "
                "options '{}'".format(host, self._ssh_options))

        conn = remoto.Connection(
            'root@' + host,
            logger=self.log,
            ssh_options=self._ssh_options)

        conn.import_module(remotes)

        return conn

    def _executable_path(self, conn, executable):
        """
        Remote validator that accepts a connection object to ensure that a certain
        executable is available returning its full path if so.

        Otherwise an exception with thorough details will be raised, informing the
        user that the executable was not found.
        """
        executable_path = conn.remote_module.which(executable)
        if not executable_path:
            raise RuntimeError("Executable '{}' not found on host '{}'".format(
                executable, conn.hostname))
        self.log.info("Found executable '{}' at path '{}'".format(executable,
            executable_path))
        return executable_path

    def _run_ceph_daemon(self, host, entity, command, args,
                         stdin=None,
                         no_fsid=False):
        """
        Run ceph-daemon on the remote host with the given command + args
        """
        conn = self._get_connection(host)

        try:
            # get container image
            ret, image, err = self.mon_command({
                'prefix': 'config get',
                'who': entity,
                'key': 'container_image',
            })
            image = image.strip()
            self.log.debug('%s container image %s' % (entity, image))

            final_args = [
                '--image', image,
                command
            ]
            if not no_fsid:
                final_args += ['--fsid', self._cluster_fsid]
            final_args += args
            self.log.debug('args: %s' % final_args)
            self.log.debug('stdin: %s' % stdin)

            script = 'injected_argv = ' + json.dumps(final_args) + '\n'
            if stdin:
                script += 'injected_stdin = ' + json.dumps(stdin) + '\n'
            script += self._ceph_daemon
            #self.log.debug('script is %s' % script)

            out, err, code = remoto.process.check(
                conn,
                ['/usr/bin/python', '-u'],
                stdin=script.encode('utf-8'))
            self.log.debug('exit code %s out %s err %s' % (code, out, err))
            return out, code

        except Exception as ex:
            self.log.exception(ex)
            raise

        finally:
            conn.exit()

    def _get_hosts(self, wanted=None):
        return self.inventory_cache.items_filtered(wanted)

    def add_host(self, host):
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        @log_exceptions
        def run(host):
            self.inventory_cache[host] = orchestrator.OutdatableData()
            return "Added host '{}'".format(host)

        return SSHWriteCompletion(
            self._worker_pool.apply_async(run, (host,)))

    def remove_host(self, host):
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        @log_exceptions
        def run(host):
            del self.inventory_cache[host]
            return "Removed host '{}'".format(host)

        return SSHWriteCompletion(
            self._worker_pool.apply_async(run, (host,)))

    def get_hosts(self):
        """
        Return a list of hosts managed by the orchestrator.

        Notes:
          - skip async: manager reads from cache.

        TODO:
          - InventoryNode probably needs to be able to report labels
        """
        nodes = [orchestrator.InventoryNode(host_name, []) for host_name in self.inventory_cache]
        return orchestrator.TrivialReadCompletion(nodes)

    def _get_services(self, service_type=None, service_id=None,
                      node_name=None):
        daemons = {}
        for host, _ in self._get_hosts():
            self.log.info("refresh stale daemons for '{}'".format(host))
            out, code = self._run_ceph_daemon(
                host, 'mon', 'ls', [], no_fsid=True)
            daemons[host] = json.loads(''.join(out))

        result = []
        for host, ls in daemons.items():
            for d in ls:
                if not d['style'].startswith('ceph-daemon'):
                    self.log.debug('ignoring non-ceph-daemon on %s: %s' % (host, d))
                    continue
                if d['fsid'] != self._cluster_fsid:
                    self.log.debug('ignoring foreign daemon on %s: %s' % (host, d))
                    continue
                self.log.debug('including %s' % d)
                sd = orchestrator.ServiceDescription()
                sd.service_type = d['name'].split('.')[0]
                if service_type and service_type != sd.service_type:
                    continue
                if '.' in d['name']:
                    sd.service_instance = d['name'].split('.')[1]
                else:
                    sd.service_instance = host  # e.g., crash
                if service_id and service_id != sd.service_instance:
                    continue
                sd.nodename = host
                sd.container_id = d['container_id']
                sd.version = d['version']
                sd.status_desc = d['state']
                sd.status = {
                    'running': 1,
                    'stopped': 0,
                    'error': -1,
                    'unknown': -1,
                }[d['state']]
                result.append(sd)
        return result

    def describe_service(self, service_type=None, service_id=None,
                         node_name=None, refresh=False):
        if service_type not in ("mds", "osd", "mgr", "mon", "nfs", None):
            raise orchestrator.OrchestratorValidationError(
                service_type + " unsupported")
        result = self._get_services(service_type, service_id, node_name)
        return orchestrator.TrivialReadCompletion(result)

    def get_inventory(self, node_filter=None, refresh=False):
        """
        Return the storage inventory of nodes matching the given filter.

        :param node_filter: node filter

        TODO:
          - add filtering by label
        """
        if node_filter:
            hosts = node_filter.nodes
            self._require_hosts(hosts)
            hosts = self._get_hosts(hosts)
        else:
            # this implies the returned hosts are registered
            hosts = self._get_hosts()

        @log_exceptions
        def run(host, host_info):
            # type: (str, orchestrator.OutdatableData) -> orchestrator.InventoryNode


            if host_info.outdated(self.inventory_cache_timeout) or refresh:
                self.log.info("refresh stale inventory for '{}'".format(host))
                out, code = self._run_ceph_daemon(
                    host, 'osd',
                    'ceph-volume',
                    ['--', 'inventory', '--format=json'])
                data = json.loads(''.join(out))
                host_info = orchestrator.OutdatableData(data)
                self.inventory_cache[host] = host_info
            else:
                self.log.debug("reading cached inventory for '{}'".format(host))

            devices = orchestrator.InventoryDevice.from_ceph_volume_inventory_list(host_info.data)
            return orchestrator.InventoryNode(host, devices)

        results = []
        for key, host_info in hosts:
            result = self._worker_pool.apply_async(run, (key, host_info))
            results.append(result)

        return SSHReadCompletion(results)

    @log_exceptions
    def _create_osd(self, host, drive_group):
        # get bootstrap key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get',
            'entity': 'client.bootstrap-osd',
        })

        # generate config
        ret, config, err = self.mon_command({
            "prefix": "config generate-minimal-conf",
        })

        j = json.dumps({
            'config': config,
            'keyring': keyring,
        })

        devices = drive_group.data_devices.paths
        for device in devices:
            out, code = self._run_ceph_daemon(
                host, 'osd', 'ceph-volume',
                [
                    '--config-and-keyring', '-',
                    '--',
                    'lvm', 'prepare',
                    "--cluster-fsid", self._cluster_fsid,
                    "--{}".format(drive_group.objectstore),
                    "--data", device,
                ],
                stdin=j)
            self.log.debug('ceph-volume prepare: %s' % out)

        # check result
        out, code = self._run_ceph_daemon(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        self.log.debug('code %s out %s' % (code, out))
        j = json.loads('\n'.join(out))
        fsid = self._cluster_fsid
        for osd_id, osds in j.items():
            for osd in osds:
                if osd['tags']['ceph.cluster_fsid'] != fsid:
                    self.log.debug('mismatched fsid, skipping %s' % osd)
                    continue
                if len(list(set(devices) & set(osd['devices']))) == 0:
                    self.log.debug('mismatched devices, skipping %s' % osd)
                    continue

                # create
                ret, keyring, err = self.mon_command({
                    'prefix': 'auth get',
                    'entity': 'osd.%s' % str(osd_id),
                })
                self._create_daemon(
                    'osd', str(osd_id), host, keyring,
                    extra_args=[
                        '--osd-fsid', osd['tags']['ceph.osd_fsid'],
                    ])

        return "Created osd(s) on host '{}'".format(host)

    def create_osds(self, drive_group, all_hosts=None):
        """
        Create a new osd.

        The orchestrator CLI currently handles a narrow form of drive
        specification defined by a single block device using bluestore.

        :param drive_group: osd specification

        TODO:
          - support full drive_group specification
          - support batch creation
        """
        assert len(drive_group.hosts(all_hosts)) == 1
        assert len(drive_group.data_devices.paths) > 0
        assert all(map(lambda p: isinstance(p, six.string_types),
            drive_group.data_devices.paths))

        host = drive_group.hosts(all_hosts)[0]
        self._require_hosts(host)

        result = self._worker_pool.apply_async(self._create_osd, (host,
                drive_group))

        return SSHWriteCompletion(result)

    def _create_daemon(self, daemon_type, daemon_id, host, keyring,
                       extra_args=[]):
        conn = self._get_connection(host)
        try:
            name = '%s.%s' % (daemon_type, daemon_id)

            # generate config
            ret, config, err = self.mon_command({
                "prefix": "config generate-minimal-conf",
            })

            ret, crash_keyring, err = self.mon_command({
                'prefix': 'auth get-or-create',
                'entity': 'client.crash.%s' % host,
                'caps': ['mon', 'allow profile crash',
                         'mgr', 'allow profile crash'],
            })

            j = json.dumps({
                'config': config,
                'keyring': keyring,
                'crash_keyring': crash_keyring,
            })

            out, code = self._run_ceph_daemon(
                host, name, 'deploy',
                [
                    '--name', name,
                    '--config-and-keyrings', '-',
                ] + extra_args,
                stdin=j)
            self.log.debug('create_daemon code %s out %s' % (code, out))

            return "Created {} on host '{}'".format(name, host)

        except Exception as e:
            self.log.error("create_daemon({}): error: {}".format(host, e))
            raise

        finally:
            self.log.info("create_daemon({}): finished".format(host))
            conn.exit()

    def _remove_daemon(self, daemon_type, daemon_id, host):
        """
        Remove a daemon
        """
        conn = self._get_connection(host)
        try:
            name = '%s.%s' % (daemon_type, daemon_id)

            out, code = self._run_ceph_daemon(host, name, 'rm-daemon', [])
            self.log.debug('remove_daemon code %s out %s' % (code, out))

            return "Removed {} on host '{}'".format(name, host)

        except Exception as e:
            self.log.error("remove_daemon({}): error: {}".format(host, e))
            raise

        finally:
            self.log.info("remove_daemon({}): finished".format(host))
            conn.exit()

    def _create_mon(self, host, network):
        """
        Create a new monitor on the given host.
        """
        self.log.info("create_mon({}:{}): starting".format(host, network))

        # get mon. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        return self._create_daemon('mon', host, host, keyring,
                                   extra_args=['--mon-network', network])

    def update_mons(self, num, hosts):
        """
        Adjust the number of cluster monitors.
        """
        # current support limited to adding monitors.
        mon_map = self.get("mon_map")
        num_mons = len(mon_map["mons"])
        if num == num_mons:
            return SSHWriteCompletionReady("The requested number of monitors exist.")
        if num < num_mons:
            raise NotImplementedError("Removing monitors is not supported.")

        # check that all the hostnames are registered
        self._require_hosts(map(lambda h: h[0], hosts))

        # current support requires a network to be specified
        for host, network in hosts:
            if not network:
                raise RuntimeError("Host '{}' missing network "
                        "part".format(host))

        # explicit placement: enough hosts provided?
        num_new_mons = num - num_mons
        if len(hosts) < num_new_mons:
            raise RuntimeError("Error: {} hosts provided, expected {}".format(
                len(hosts), num_new_mons))

        self.log.info("creating {} monitors on hosts: '{}'".format(
            num_new_mons, ",".join(map(lambda h: ":".join(h), hosts))))

        # TODO: we may want to chain the creation of the monitors so they join
        # the quroum one at a time.
        results = []
        for host, network in hosts:
            result = self._worker_pool.apply_async(self._create_mon, (host,
                network))
            results.append(result)

        return SSHWriteCompletion(results)

    def _create_mgr(self, host, name):
        """
        Create a new manager instance on a host.
        """
        self.log.info("create_mgr({}, mgr.{}): starting".format(host, name))

        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mgr.%s' % name,
            'caps': ['mon', 'allow profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        return self._create_daemon('mgr', name, host, keyring)

    def _remove_mgr(self, mgr_id, host):
        name = 'mgr.' + mgr_id
        out, code = self._run_ceph_daemon(
            host, name, 'rm-daemon',
            ['--name', name])
        self.log.debug('remove_mgr code %s out %s' % (code, out))
        return "Removed {} from host '{}'".format(name, host)

    def update_mgrs(self, num, hosts):
        """
        Adjust the number of cluster managers.
        """
        daemons = self._get_services('mgr')
        num_mgrs = len(daemons)
        if num == num_mgrs:
            return SSHWriteCompletionReady("The requested number of managers exist.")

        # check that all the hosts are registered
        self._require_hosts(hosts)

        results = []
        if num < num_mgrs:
            num_to_remove = num_mgrs - num

            # first try to remove unconnected mgr daemons that the
            # cluster doesn't see
            connected = []
            mgr_map = self.get("mgr_map")
            if mgr_map["active_name"]:
                connected.append(mgr_map['active_name'])
            for standby in mgr_map['standbys']:
                connected.append(standby['name'])
            for daemon in daemons:
                if daemon.service_instance not in connected:
                    result = self._worker_pool.apply_async(
                        self._remove_mgr,
                        (daemon.service_instance, daemon.nodename))
                    results.append(result)
                    num_to_remove -= 1
                    if num_to_remove == 0:
                        break

            # otherwise, remove *any* mgr
            if num_to_remove > 0:
                for daemon in daemons:
                    result = self._worker_pool.apply_async(
                        self._remove_mgr,
                        (daemon.service_instance, daemon.nodename))
                    results.append(result)
                    num_to_remove -= 1
                    if num_to_remove == 0:
                        break

        else:
            # we assume explicit placement by which there are the same number of
            # hosts specified as the size of increase in number of daemons.
            num_new_mgrs = num - num_mgrs
            if len(hosts) < num_new_mgrs:
                raise RuntimeError(
                    "Error: {} hosts provided, expected {}".format(
                        len(hosts), num_new_mgrs))

            self.log.info("creating {} managers on hosts: '{}'".format(
                num_new_mgrs, ",".join(hosts)))

            for i in range(num_new_mgrs):
                name = self.get_unique_name(daemons)
                result = self._worker_pool.apply_async(self._create_mgr,
                                                       (hosts[i], name))
                results.append(result)

        return SSHWriteCompletion(results)

    def add_mds(self, spec):
        if len(spec.placement.nodes) < spec.count:
            raise RuntimeError("must specify at least %d hosts" % spec.count)
        daemons = self._get_services('mds')
        results = []
        num_added = 0
        for host in spec.placement.nodes:
            if num_added >= spec.count:
                break
            mds_id = self.get_unique_name(daemons, spec.name)
            self.log.debug('placing mds.%s on host %s' % (mds_id, host))
            results.append(
                self._worker_pool.apply_async(self._create_mds, (mds_id, host))
            )
            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.ServiceDescription()
            sd.service_instance = mds_id
            sd.service_type = 'mds'
            sd.nodename = host
            daemons.append(sd)
            num_added += 1
        return SSHWriteCompletion(results)

    def update_mds(self, spec):
        daemons = [
            d for d in self._get_services('mds')
            if d.service_instance.startswith(spec.name + '-')
        ]
        results = []
        if len(daemons) > spec.count:
            # remove some
            to_remove = len(daemons) - spec.count
            for d in daemons[0:to_remove]:
                results.append(self._worker_pool.apply_async(
                    self._remove_mds, (d.service_instance, d.nodename)))
        elif len(daemons) < spec.count:
            # add some
            spec.count -= len(daemons)
            return self.add_mds(spec)
        return SSHWriteCompletion(results)

    def _create_mds(self, mds_id, host):
        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mds.' + mds_id,
            'caps': ['mon', 'allow profile mds',
                     'osd', 'allow rwx',
                     'mds', 'allow'],
        })
        return self._create_daemon('mds', mds_id, host, keyring)

    def remove_mds(self, name):
        daemons = self._get_services('mds')
        results = []
        for d in daemons:
            if d.service_instance == name or d.service_instance.startswith(name + '-'):
                results.append(self._worker_pool.apply_async(
                    self._remove_mds, (d.service_instance, d.nodename)))
        if not results:
            raise RuntimeError('Unable to find mds.%s[-*] daemon(s)' % name)
        return SSHWriteCompletion(results)

    def _remove_mds(self, mds_id, host):
        name = 'mds.' + mds_id
        out, code = self._run_ceph_daemon(
            host, name, 'rm-daemon',
            ['--name', name])
        self.log.debug('remove_mds code %s out %s' % (code, out))
        return "Removed {} from host '{}'".format(name, host)
