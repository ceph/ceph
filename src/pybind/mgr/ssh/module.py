import json
import errno
import six
import os
import datetime
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

DATEFMT = '%Y-%m-%d %H:%M:%S.%f'

# high-level TODO:
#  - bring over some of the protections from ceph-deploy that guard against
#    multiple bootstrapping / initialization

class SSHReadCompletion(orchestrator.ReadCompletion):
    def __init__(self, result):
        if isinstance(result, multiprocessing.pool.AsyncResult):
            self._result = [result]
        else:
            self._result = result
        assert isinstance(self._result, list)

    @property
    def result(self):
        return list(map(lambda r: r.get(), self._result))

    @property
    def is_complete(self):
        return all(map(lambda r: r.ready(), self._result))

class SSHReadCompletionReady(SSHReadCompletion):
    def __init__(self, result):
        self._result = result

    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return True

class SSHWriteCompletion(orchestrator.WriteCompletion):
    def __init__(self, result):
        if isinstance(result, multiprocessing.pool.AsyncResult):
            self._result = [result]
        else:
            self._result = result
        assert isinstance(self._result, list)

    @property
    def result(self):
        return list(map(lambda r: r.get(), self._result))

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

class SSHConnection(object):
    """
    Tie tempfile lifetime (e.g. ssh_config) to a remoto connection.
    """
    def __init__(self):
        self.conn = None
        self.temp_file = None

    # proxy to the remoto connection
    def __getattr__(self, name):
        return getattr(self.conn, name)

class SSHOrchestrator(MgrModule, orchestrator.Orchestrator):

    _STORE_HOST_PREFIX = "host"
    _DEFAULT_INVENTORY_CACHE_TIMEOUT_MIN = 10

    MODULE_OPTIONS = [
        {'name': 'ssh_config_file'},
        {'name': 'inventory_cache_timeout_min'},
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
        self._cluster_fsid = None
        self._worker_pool = multiprocessing.pool.ThreadPool(1)

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
            if not isinstance(c, SSHReadCompletion) and \
                    not isinstance(c, SSHWriteCompletion):
                raise TypeError("unexpected completion: {}".format(c.__class__))

            if c.is_complete:
                continue

            complete = False

        return complete

    @staticmethod
    def time_from_string(timestr):
        # drop the 'Z' timezone indication, it's always UTC
        timestr = timestr.rstrip('Z')
        return datetime.datetime.strptime(timestr, DATEFMT)

    def _get_cluster_fsid(self):
        """
        Fetch and cache the cluster fsid.
        """
        if not self._cluster_fsid:
            self._cluster_fsid = self.get("mon_map")["fsid"]
        assert isinstance(self._cluster_fsid, six.string_types)
        return self._cluster_fsid

    def _require_hosts(self, hosts):
        """
        Raise an error if any of the given hosts are unregistered.
        """
        if isinstance(hosts, six.string_types):
            hosts = [hosts]
        unregistered_hosts = []
        for host in hosts:
            key = self._hostname_to_store_key(host)
            if not self.get_store(key):
                unregistered_hosts.append(host)
        if unregistered_hosts:
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
        ssh_options = None

        conn = SSHConnection()

        ssh_config = self.get_store("ssh_config")
        if ssh_config is not None:
            conn.temp_file = tempfile.NamedTemporaryFile()
            conn.temp_file.write(ssh_config.encode('utf-8'))
            conn.temp_file.flush() # make visible to other processes
            ssh_config_fname = conn.temp_file.name
        else:
            ssh_config_fname = self.get_localized_module_option("ssh_config_file")

        if ssh_config_fname:
            if not os.path.isfile(ssh_config_fname):
                raise Exception("ssh_config \"{}\" does not exist".format(ssh_config_fname))
            ssh_options = "-F {}".format(ssh_config_fname)

        self.log.info("opening connection to host '{}' with ssh "
                "options '{}'".format(host, ssh_options))

        conn.conn = remoto.Connection(host,
                logger=self.log,
                detect_sudo=True,
                ssh_options=ssh_options)

        conn.conn.import_module(remotes)

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

    def _build_ceph_conf(self):
        """
        Build a minimal `ceph.conf` containing the current monitor hosts.

        Notes:
          - ceph-volume complains if no section header (e.g. global) exists
          - other ceph cli tools complained about no EOF newline

        TODO:
          - messenger v2 syntax?
        """
        mon_map = self.get("mon_map")
        mon_addrs = map(lambda m: m["addr"], mon_map["mons"])
        mon_hosts = ", ".join(mon_addrs)
        return "[global]\nmon host = {}\n".format(mon_hosts)

    def _ensure_ceph_conf(self, conn, network=False):
        """
        Install ceph.conf on remote node if it doesn't exist.
        """
        conf = self._build_ceph_conf()
        if network:
            conf += "public_network = {}\n".format(network)
        conn.remote_module.write_conf("/etc/ceph/ceph.conf", conf)

    def _get_bootstrap_key(self, service_type):
        """
        Fetch a bootstrap key for a service type.

        :param service_type: name (e.g. mds, osd, mon, ...)
        """
        identity_dict = {
            'admin' : 'client.admin',
            'mds' : 'client.bootstrap-mds',
            'mgr' : 'client.bootstrap-mgr',
            'osd' : 'client.bootstrap-osd',
            'rgw' : 'client.bootstrap-rgw',
            'mon' : 'mon.'
        }

        identity = identity_dict[service_type]

        ret, out, err = self.mon_command({
            "prefix": "auth get",
            "entity": identity
        })

        if ret == -errno.ENOENT:
            raise RuntimeError("Entity '{}' not found: '{}'".format(identity, err))
        elif ret != 0:
            raise RuntimeError("Error retrieving key for '{}' ret {}: '{}'".format(
                identity, ret, err))

        return out

    def _bootstrap_mgr(self, conn):
        """
        Bootstrap a manager.

          1. install a copy of ceph.conf
          2. install the manager bootstrap key

        :param conn: remote host connection
        """
        self._ensure_ceph_conf(conn)
        keyring = self._get_bootstrap_key("mgr")
        keyring_path = "/var/lib/ceph/bootstrap-mgr/ceph.keyring"
        conn.remote_module.write_keyring(keyring_path, keyring)
        return keyring_path

    def _bootstrap_osd(self, conn):
        """
        Bootstrap an osd.

          1. install a copy of ceph.conf
          2. install the osd bootstrap key

        :param conn: remote host connection
        """
        self._ensure_ceph_conf(conn)
        keyring = self._get_bootstrap_key("osd")
        keyring_path = "/var/lib/ceph/bootstrap-osd/ceph.keyring"
        conn.remote_module.write_keyring(keyring_path, keyring)
        return keyring_path

    def _hostname_to_store_key(self, host):
        return "{}.{}".format(self._STORE_HOST_PREFIX, host)

    def _get_hosts(self, wanted=None):
        if wanted:
            hosts_info = []
            for host in wanted:
                key = self._hostname_to_store_key(host)
                info = self.get_store(key)
                if info:
                    hosts_info.append((key, info))
        else:
            hosts_info = six.iteritems(self.get_store_prefix(self._STORE_HOST_PREFIX))

        return list(map(lambda kv: (kv[0], json.loads(kv[1])), hosts_info))

    def add_host(self, host):
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        def run(host):
            key = self._hostname_to_store_key(host)
            self.set_store(key, json.dumps({
                "host": host,
                "inventory": None,
                "last_inventory_refresh": None
            }))
            return "Added host '{}'".format(host)

        return SSHWriteCompletion(
            self._worker_pool.apply_async(run, (host,)))

    def remove_host(self, host):
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        def run(host):
            key = self._hostname_to_store_key(host)
            self.set_store(key, None)
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
        nodes = []
        for key, host_info in self._get_hosts():
            node = orchestrator.InventoryNode(host_info["host"], [])
            nodes.append(node)
        return SSHReadCompletionReady(nodes)

    def _get_device_inventory(self, host):
        """
        Query storage devices on a remote node.

        :return: list of InventoryDevice
        """
        conn = self._get_connection(host)

        try:
            ceph_volume_executable = self._executable_path(conn, 'ceph-volume')
            command = [
                ceph_volume_executable,
                "inventory",
                "--format=json"
            ]

            out, err, code = remoto.process.check(conn, command)
            host_devices = json.loads(out[0])
            return host_devices

        except Exception as ex:
            self.log.exception(ex)
            raise

        finally:
            conn.exit()

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

        def run(key, host_info):
            updated = False
            host = host_info["host"]

            if not host_info["inventory"]:
                self.log.info("caching inventory for '{}'".format(host))
                host_info["inventory"] = self._get_device_inventory(host)
                updated = True
            else:
                timeout_min = int(self.get_module_option(
                    "inventory_cache_timeout_min",
                    self._DEFAULT_INVENTORY_CACHE_TIMEOUT_MIN))

                cutoff = datetime.datetime.utcnow() - datetime.timedelta(
                        minutes=timeout_min)

                last_update = self.time_from_string(host_info["last_inventory_refresh"])

                if last_update < cutoff or refresh:
                    self.log.info("refresh stale inventory for '{}'".format(host))
                    host_info["inventory"] = self._get_device_inventory(host)
                    updated = True
                else:
                    self.log.info("reading cached inventory for '{}'".format(host))
                    pass

            if updated:
                now = datetime.datetime.utcnow()
                now = now.strftime(DATEFMT)
                host_info["last_inventory_refresh"] = now
                self.set_store(key, json.dumps(host_info))

            devices = list(map(lambda di:
                orchestrator.InventoryDevice.from_ceph_volume_inventory(di),
                host_info["inventory"]))

            return orchestrator.InventoryNode(host, devices)

        results = []
        for key, host_info in hosts:
            result = self._worker_pool.apply_async(run, (key, host_info))
            results.append(result)

        return SSHReadCompletion(results)

    def _create_osd(self, host, drive_group):
        conn = self._get_connection(host)
        try:
            devices = drive_group.data_devices.paths
            self._bootstrap_osd(conn)

            for device in devices:
                ceph_volume_executable = self._executable_path(conn, "ceph-volume")
                command = [
                    ceph_volume_executable,
                    "lvm",
                    "create",
                    "--cluster-fsid", self._get_cluster_fsid(),
                    "--{}".format(drive_group.objectstore),
                    "--data", device
                ]
                remoto.process.run(conn, command)

            return "Created osd on host '{}'".format(host)

        except:
            raise

        finally:
            conn.exit()

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

    def _create_mon(self, host, network):
        """
        Create a new monitor on the given host.
        """
        self.log.info("create_mon({}:{}): starting".format(host, network))

        conn = self._get_connection(host)

        try:
            self._ensure_ceph_conf(conn, network)

            uid = conn.remote_module.path_getuid("/var/lib/ceph")
            gid = conn.remote_module.path_getgid("/var/lib/ceph")

            # install client admin key on target mon host
            admin_keyring = self._get_bootstrap_key("admin")
            admin_keyring_path = '/etc/ceph/ceph.client.admin.keyring'
            conn.remote_module.write_keyring(admin_keyring_path, admin_keyring, uid, gid)

            mon_path = "/var/lib/ceph/mon/ceph-{name}".format(name=host)
            conn.remote_module.create_mon_path(mon_path, uid, gid)

            # bootstrap key
            conn.remote_module.safe_makedirs("/var/lib/ceph/tmp")
            monitor_keyring = self._get_bootstrap_key("mon")
            mon_keyring_path = "/var/lib/ceph/tmp/ceph-{name}.mon.keyring".format(name=host)
            conn.remote_module.write_file(
                mon_keyring_path,
                monitor_keyring,
                0o600,
                None,
                uid,
                gid
            )

            # monitor map
            monmap_path = "/var/lib/ceph/tmp/ceph.{name}.monmap".format(name=host)
            remoto.process.run(conn,
                ['ceph', 'mon', 'getmap', '-o', monmap_path],
            )

            user_args = []
            if uid != 0:
                user_args = user_args + [ '--setuser', str(uid) ]
            if gid != 0:
                user_args = user_args + [ '--setgroup', str(gid) ]

            remoto.process.run(conn,
                ['ceph-mon', '--mkfs', '-i', host,
                 '--monmap', monmap_path, '--keyring', mon_keyring_path
                ] + user_args
            )

            remoto.process.run(conn,
                ['systemctl', 'enable', 'ceph.target'],
                timeout=7,
            )

            remoto.process.run(conn,
                ['systemctl', 'enable', 'ceph-mon@{name}'.format(name=host)],
                timeout=7,
            )

            remoto.process.run(conn,
                ['systemctl', 'start', 'ceph-mon@{name}'.format(name=host)],
                timeout=7,
            )

            return "Created mon on host '{}'".format(host)

        except Exception as e:
            self.log.error("create_mon({}:{}): error: {}".format(host, network, e))
            raise

        finally:
            self.log.info("create_mon({}:{}): finished".format(host, network))
            conn.exit()

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

    def _create_mgr(self, host):
        """
        Create a new manager instance on a host.
        """
        self.log.info("create_mgr({}): starting".format(host))

        conn = self._get_connection(host)

        try:
            bootstrap_keyring_path = self._bootstrap_mgr(conn)

            mgr_path = "/var/lib/ceph/mgr/ceph-{name}".format(name=host)
            conn.remote_module.safe_makedirs(mgr_path)
            keyring_path = os.path.join(mgr_path, "keyring")

            command = [
                'ceph',
                '--name', 'client.bootstrap-mgr',
                '--keyring', bootstrap_keyring_path,
                'auth', 'get-or-create', 'mgr.{name}'.format(name=host),
                'mon', 'allow profile mgr',
                'osd', 'allow *',
                'mds', 'allow *',
                '-o',
                keyring_path
            ]

            out, err, ret = remoto.process.check(conn, command)
            if ret != 0:
                raise Exception("oops")

            remoto.process.run(conn,
                ['systemctl', 'enable', 'ceph-mgr@{name}'.format(name=host)],
                timeout=7
            )

            remoto.process.run(conn,
                ['systemctl', 'start', 'ceph-mgr@{name}'.format(name=host)],
                timeout=7
            )

            remoto.process.run(conn,
                ['systemctl', 'enable', 'ceph.target'],
                timeout=7
            )

            return "Created mgr on host '{}'".format(host)

        except Exception as e:
            self.log.error("create_mgr({}): error: {}".format(host, e))
            raise

        finally:
            self.log.info("create_mgr({}): finished".format(host))
            conn.exit()

    def update_mgrs(self, num, hosts):
        """
        Adjust the number of cluster managers.
        """
        # current support limited to adding managers.
        mgr_map = self.get("mgr_map")
        num_mgrs = 1 if mgr_map["active_name"] else 0
        num_mgrs += len(mgr_map["standbys"])
        if num == num_mgrs:
            return SSHWriteCompletionReady("The requested number of managers exist.")
        if num < num_mgrs:
            raise NotImplementedError("Removing managers is not supported")

        # check that all the hosts are registered
        hosts = list(set(hosts))
        self._require_hosts(hosts)

        # we assume explicit placement by which there are the same number of
        # hosts specified as the size of increase in number of daemons.
        num_new_mgrs = num - num_mgrs
        if len(hosts) < num_new_mgrs:
            raise RuntimeError("Error: {} hosts provided, expected {}".format(
                len(hosts), num_new_mgrs))

        self.log.info("creating {} managers on hosts: '{}'".format(
            num_new_mgrs, ",".join(hosts)))

        results = []
        for i in range(num_new_mgrs):
            result = self._worker_pool.apply_async(self._create_mgr, (hosts[i],))
            results.append(result)

        return SSHWriteCompletion(results)
