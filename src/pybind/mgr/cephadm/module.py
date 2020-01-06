import json
import errno
import logging
from functools import wraps

import string
try:
    from typing import List, Dict, Optional, Callable, TypeVar, Type, Any
except ImportError:
    pass  # just for type checking


import six
import os
import random
import tempfile
import multiprocessing.pool
import shutil
import subprocess

from ceph.deployment import inventory
from mgr_module import MgrModule
import orchestrator
from orchestrator import OrchestratorError, HostSpec, OrchestratorValidationError

from . import remotes

try:
    import remoto
    import remoto.process
except ImportError as e:
    remoto = None
    remoto_import_error = str(e)

try:
    from typing import List
except ImportError:
    pass

logger = logging.getLogger(__name__)

DEFAULT_SSH_CONFIG = ('Host *\n'
                      'User root\n'
                      'StrictHostKeyChecking no\n'
                      'UserKnownHostsFile /dev/null\n')

# for py2 compat
try:
    from tempfile import TemporaryDirectory # py3
except ImportError:
    # define a minimal (but sufficient) equivalent for <= py 3.2
    class TemporaryDirectory(object): # type: ignore
        def __init__(self):
            self.name = tempfile.mkdtemp()

        def __enter__(self):
            if not self.name:
                self.name = tempfile.mkdtemp()
            return self.name

        def cleanup(self):
            shutil.rmtree(self.name)

        def __exit__(self, exc_type, exc_value, traceback):
            self.cleanup()


# high-level TODO:
#  - bring over some of the protections from ceph-deploy that guard against
#    multiple bootstrapping / initialization

def _name_to_entity_name(name):
    """
    Map from service names to ceph entity names (as seen in config)
    """
    if name.startswith('rgw.') or name.startswith('rbd-mirror'):
        return 'client.' + name
    else:
        return name


class AsyncCompletion(orchestrator.Completion):
    def __init__(self,
                 _first_promise=None,  # type: Optional[orchestrator.Completion]
                 value=orchestrator._Promise.NO_RESULT,  # type: Any
                 on_complete=None,  # type: Optional[Callable]
                 name=None,  # type: Optional[str]
                 many=False, # type: bool
                 ):

        assert CephadmOrchestrator.instance is not None
        self.many = many
        if name is None and on_complete is not None:
            name = on_complete.__name__
        super(AsyncCompletion, self).__init__(_first_promise, value, on_complete, name)

    @property
    def _progress_reference(self):
        # type: () -> Optional[orchestrator.ProgressReference]
        if hasattr(self._on_complete_, 'progress_id'):  # type: ignore
            return self._on_complete_  # type: ignore
        return None

    @property
    def _on_complete(self):
        # type: () -> Optional[Callable]
        if self._on_complete_ is None:
            return None

        def callback(result):
            try:
                self._on_complete_ = None
                self._finalize(result)
            except Exception as e:
                self.fail(e)

        def error_callback(e):
            self.fail(e)

        if six.PY3:
            _callback = self._on_complete_
        else:
            def _callback(*args, **kwargs):
                # Py2 only: _worker_pool doesn't call error_callback
                try:
                    return self._on_complete_(*args, **kwargs)
                except Exception as e:
                    self.fail(e)

        def run(value):
            assert CephadmOrchestrator.instance
            if self.many:
                if not value:
                    logger.info('calling map_async without values')
                    callback([])
                if six.PY3:
                    CephadmOrchestrator.instance._worker_pool.map_async(_callback, value,
                                                                    callback=callback,
                                                                    error_callback=error_callback)
                else:
                    CephadmOrchestrator.instance._worker_pool.map_async(_callback, value,
                                                                    callback=callback)
            else:
                if six.PY3:
                    CephadmOrchestrator.instance._worker_pool.apply_async(_callback, (value,),
                                                                      callback=callback, error_callback=error_callback)
                else:
                    CephadmOrchestrator.instance._worker_pool.apply_async(_callback, (value,),
                                                                      callback=callback)
            return self.ASYNC_RESULT

        return run

    @_on_complete.setter
    def _on_complete(self, inner):
        # type: (Callable) -> None
        self._on_complete_ = inner


def ssh_completion(cls=AsyncCompletion, **c_kwargs):
    # type: (Type[orchestrator.Completion], Any) -> Callable
    """
    See ./HACKING.rst for a how-to
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args):

            name = f.__name__
            many = c_kwargs.get('many', False)

            # Some weired logic to make calling functions with multiple arguments work.
            if len(args) == 1:
                [value] = args
                if many and value and isinstance(value[0], tuple):
                    return cls(on_complete=lambda x: f(*x), value=value, name=name, **c_kwargs)
                else:
                    return cls(on_complete=f, value=value, name=name, **c_kwargs)
            else:
                if many:
                    self, value = args

                    def call_self(inner_args):
                        if not isinstance(inner_args, tuple):
                            inner_args = (inner_args, )
                        return f(self, *inner_args)

                    return cls(on_complete=call_self, value=value, name=name, **c_kwargs)
                else:
                    return cls(on_complete=lambda x: f(*x), value=args, name=name, **c_kwargs)


        return wrapper
    return decorator


def async_completion(f):
    # type: (Callable) -> Callable[..., AsyncCompletion]
    """
    See ./HACKING.rst for a how-to

    :param f: wrapped function
    """
    return ssh_completion()(f)


def async_map_completion(f):
    # type: (Callable) -> Callable[..., AsyncCompletion]
    """
    See ./HACKING.rst for a how-to

    :param f: wrapped function

    kind of similar to

    >>> def sync_map(f):
    ...     return lambda x: map(f, x)

    """
    return ssh_completion(many=True)(f)


def trivial_completion(f):
    # type: (Callable) -> Callable[..., orchestrator.Completion]
    return ssh_completion(cls=orchestrator.Completion)(f)


def trivial_result(val):
    return AsyncCompletion(value=val, name='trivial_result')


def with_services(service_type=None,
                  service_name=None,
                  service_id=None,
                  node_name=None,
                  refresh=False):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            def on_complete(services):
                if kwargs:
                    kwargs['services'] = services
                    return func(self, *args, **kwargs)
                else:
                    args_ = args + (services,)
                    return func(self, *args_, **kwargs)
            return self._get_services(service_type=service_type,
                                      service_name=service_name,
                                      service_id=service_id,
                                      node_name=node_name,
                                      refresh=refresh).then(on_complete)
        return wrapper
    return decorator

class CephadmOrchestrator(MgrModule, orchestrator.Orchestrator):

    _STORE_HOST_PREFIX = "host"


    instance = None
    NATIVE_OPTIONS = []  # type: List[Any]
    MODULE_OPTIONS = [
        {
            'name': 'ssh_config_file',
            'type': 'str',
            'default': None,
            'desc': 'customized SSH config file to connect to managed hosts',
        },
        {
            'name': 'inventory_cache_timeout',
            'type': 'secs',
            'default': 10 * 60,
            'desc': 'seconds to cache device inventory',
        },
        {
            'name': 'service_cache_timeout',
            'type': 'secs',
            'default': 60,
            'desc': 'seconds to cache service (daemon) inventory',
        },
        {
            'name': 'mode',
            'type': 'str',
            'enum_allowed': ['root', 'cephadm-package'],
            'default': 'root',
            'desc': 'mode for remote execution of cephadm',
        },
        {
            'name': 'container_image_base',
            'default': 'ceph/ceph',
            'desc': 'Container image name, without the tag',
            'runtime': True,
        },
    ]

    def __init__(self, *args, **kwargs):
        super(CephadmOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = self.get('mon_map')['fsid']

        self.config_notify()

        path = self.get_ceph_option('cephadm_path')
        try:
            with open(path, 'r') as f:
                self._cephadm = f.read()
        except (IOError, TypeError) as e:
            raise RuntimeError("unable to read cephadm at '%s': %s" % (
                path, str(e)))

        self._worker_pool = multiprocessing.pool.ThreadPool(1)

        self._reconfig_ssh()

        CephadmOrchestrator.instance = self
        self.all_progress_references = list()  # type: List[orchestrator.ProgressReference]

        # load inventory
        i = self.get_store('inventory')
        if i:
            self.inventory = json.loads(i)
        else:
            self.inventory = dict()
        self.log.debug('Loaded inventory %s' % self.inventory)

        # The values are cached by instance.
        # cache is invalidated by
        # 1. timeout
        # 2. refresh parameter
        self.inventory_cache = orchestrator.OutdatablePersistentDict(
            self, self._STORE_HOST_PREFIX + '.devices')

        self.service_cache = orchestrator.OutdatablePersistentDict(
            self, self._STORE_HOST_PREFIX + '.services')

        # ensure the host lists are in sync
        for h in self.inventory.keys():
            if h not in self.inventory_cache:
                self.log.debug('adding inventory item for %s' % h)
                self.inventory_cache[h] = orchestrator.OutdatableData()
            if h not in self.service_cache:
                self.log.debug('adding service item for %s' % h)
                self.service_cache[h] = orchestrator.OutdatableData()
        for h in self.inventory_cache:
            if h not in self.inventory:
                del self.inventory_cache[h]
        for h in self.service_cache:
            if h not in self.inventory:
                del self.service_cache[h]

    def shutdown(self):
        self.log.error('shutdown')
        self._worker_pool.close()
        self._worker_pool.join()

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],  # type: ignore
                    self.get_module_option(opt['name']) or opt['default'])  # type: ignore
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))  # type: ignore
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,  # type: ignore
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))  # type: ignore

    def get_unique_name(self, existing, prefix=None, forcename=None):
        """
        Generate a unique random service name
        """
        if forcename:
            if len([d for d in existing if d.service_instance == forcename]):
                raise RuntimeError('specified name %s already in use', forcename)
            return forcename

        while True:
            if prefix:
                name = prefix + '.'
            else:
                name = ''
            name += ''.join(random.choice(string.ascii_lowercase)
                            for _ in range(6))
            if len([d for d in existing if d.service_instance == name]):
                self.log('name %s exists, trying again', name)
                continue
            return name

    def _save_inventory(self):
        self.set_store('inventory', json.dumps(self.inventory))

    def _reconfig_ssh(self):
        temp_files = []  # type: list
        ssh_options = []  # type: List[str]

        # ssh_config
        ssh_config_fname = self.ssh_config_file
        ssh_config = self.get_store("ssh_config")
        if ssh_config is not None or ssh_config_fname is None:
            if not ssh_config:
                ssh_config = DEFAULT_SSH_CONFIG
            f = tempfile.NamedTemporaryFile(prefix='cephadm-conf-')
            os.fchmod(f.fileno(), 0o600)
            f.write(ssh_config.encode('utf-8'))
            f.flush()  # make visible to other processes
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
        self.ssh_pub = ssh_pub
        self.ssh_key = ssh_key
        if ssh_key and ssh_pub:
            tkey = tempfile.NamedTemporaryFile(prefix='cephadm-identity-')
            tkey.write(ssh_key.encode('utf-8'))
            os.fchmod(tkey.fileno(), 0o600)
            tkey.flush()  # make visible to other processes
            tpub = open(tkey.name + '.pub', 'w')
            os.fchmod(tpub.fileno(), 0o600)
            tpub.write(ssh_pub)
            tpub.flush()  # make visible to other processes
            temp_files += [tkey, tpub]
            ssh_options += ['-i', tkey.name]

        self._temp_files = temp_files
        if ssh_options:
            self._ssh_options = ' '.join(ssh_options)  # type: Optional[str]
        else:
            self._ssh_options = None
        self.log.info('ssh_options %s' % ssh_options)

        if self.mode == 'root':
            self.ssh_user = 'root'
        elif self.mode == 'cephadm-package':
            self.ssh_user = 'cephadm'

    @staticmethod
    def can_run():
        if remoto is not None:
            return True, ""
        else:
            return False, "loading remoto library:{}".format(
                    remoto_import_error)

    def available(self):
        """
        The cephadm orchestrator is always available.
        """
        return self.can_run()

    def process(self, completions):
        """
        Does nothing, as completions are processed in another thread.
        """
        if completions:
            self.log.info("process: completions={0}".format(orchestrator.pretty_print(completions)))

            for p in completions:
                p.finalize()

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

    @orchestrator._cli_write_command(
        prefix='cephadm set-ssh-config',
        desc='Set the ssh_config file (use -i <ssh_config>)')
    def _set_ssh_config(self, inbuf=None):
        """
        Set an ssh_config file provided from stdin

        TODO:
          - validation
        """
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty ssh config provided"
        self.set_store("ssh_config", inbuf)
        return 0, "", ""

    @orchestrator._cli_write_command(
        prefix='cephadm clear-ssh-config',
        desc='Clear the ssh_config file')
    def _clear_ssh_config(self):
        """
        Clear the ssh_config file provided from stdin
        """
        self.set_store("ssh_config", None)
        self.ssh_config_tmp = None
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm generate-key',
        desc='Generate a cluster SSH key (if not present)')
    def _generate_key(self):
        if not self.ssh_pub or not self.ssh_key:
            self.log.info('Generating ssh key...')
            tmp_dir = TemporaryDirectory()
            path = tmp_dir.name + '/key'
            try:
                subprocess.call([
                    '/usr/bin/ssh-keygen',
                    '-C', 'ceph-%s' % self._cluster_fsid,
                    '-N', '',
                    '-f', path
                ])
                with open(path, 'r') as f:
                    secret = f.read()
                with open(path + '.pub', 'r') as f:
                    pub = f.read()
            finally:
                os.unlink(path)
                os.unlink(path + '.pub')
                tmp_dir.cleanup()
            self.set_store('ssh_identity_key', secret)
            self.set_store('ssh_identity_pub', pub)
            self._reconfig_ssh()
        return 0, '', ''

    @orchestrator._cli_write_command(
        'cephadm clear-key',
        desc='Clear cluster SSH key')
    def _clear_key(self):
        self.set_store('ssh_identity_key', None)
        self.set_store('ssh_identity_pub', None)
        self._reconfig_ssh()
        return 0, '', ''

    @orchestrator._cli_read_command(
        'cephadm get-pub-key',
        desc='Show SSH public key for connecting to cluster hosts')
    def _get_pub_key(self):
        if self.ssh_pub:
            return 0, self.ssh_pub, ''
        else:
            return -errno.ENOENT, '', 'No cluster SSH key defined'

    @orchestrator._cli_read_command(
        'cephadm get-user',
        desc='Show user for SSHing to cluster hosts')
    def _get_user(self):
        return 0, self.ssh_user, ''

    @orchestrator._cli_read_command(
        'cephadm check-host',
        'name=host,type=CephString',
        'Check whether we can access and manage a remote host')
    def _check_host(self, host):
        out, err, code = self._run_cephadm(host, '', 'check-host', [],
                                           error_ok=True, no_fsid=True)
        if code:
            return 1, '', err
        return 0, '%s ok' % host, err

    def _get_connection(self, host):
        """
        Setup a connection for running commands on remote host.
        """
        n = self.ssh_user + '@' + host
        self.log.info("Opening connection to {} with ssh options '{}'".format(
            n, self._ssh_options))
        conn = remoto.Connection(
            n,
            logger=self.log.getChild(n),
            ssh_options=self._ssh_options)

        r = conn.import_module(remotes)

        return conn, r

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

    def _run_cephadm(self, host, entity, command, args,
                     stdin=None,
                     no_fsid=False,
                     error_ok=False,
                     image=None):
        """
        Run cephadm on the remote host with the given command + args
        """
        conn, connr = self._get_connection(host)

        try:
            if not image:
                # get container image
                ret, image, err = self.mon_command({
                    'prefix': 'config get',
                    'who': _name_to_entity_name(entity),
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

            if self.mode == 'root':
                self.log.debug('args: %s' % (' '.join(final_args)))
                self.log.debug('stdin: %s' % stdin)
                script = 'injected_argv = ' + json.dumps(final_args) + '\n'
                if stdin:
                    script += 'injected_stdin = ' + json.dumps(stdin) + '\n'
                script += self._cephadm
                python = connr.choose_python()
                if not python:
                    raise RuntimeError(
                        'unable to find python on %s (tried %s in %s)' % (
                            host, remotes.PYTHONS, remotes.PATH))
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        [python, '-u'],
                        stdin=script.encode('utf-8'))
                except RuntimeError as e:
                    if error_ok:
                        return '', str(e), 1
                    raise
            elif self.mode == 'cephadm-package':
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        ['sudo', '/usr/bin/cephadm'] + final_args,
                        stdin=stdin)
                except RuntimeError as e:
                    if error_ok:
                        return '', str(e), 1
                    raise
            if code and not error_ok:
                raise RuntimeError(
                    'cephadm exited with an error code: %d, stderr:%s' % (
                        code, '\n'.join(err)))
            return out, err, code

        except Exception as ex:
            self.log.exception(ex)
            raise

        finally:
            conn.exit()

    def _get_hosts(self, wanted=None):
        return self.inventory_cache.items_filtered(wanted)

    @async_completion
    def add_host(self, host):
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        self.inventory[host] = {}
        self._save_inventory()
        self.inventory_cache[host] = orchestrator.OutdatableData()
        self.service_cache[host] = orchestrator.OutdatableData()
        return "Added host '{}'".format(host)

    @async_completion
    def remove_host(self, host):
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        del self.inventory[host]
        self._save_inventory()
        del self.inventory_cache[host]
        del self.service_cache[host]
        return "Removed host '{}'".format(host)

    @trivial_completion
    def get_hosts(self):
        """
        Return a list of hosts managed by the orchestrator.

        Notes:
          - skip async: manager reads from cache.

        TODO:
          - InventoryNode probably needs to be able to report labels
        """
        return [orchestrator.InventoryNode(host_name) for host_name in self.inventory_cache]

    @async_completion
    def add_host_label(self, host, label):
        if host not in self.inventory:
            raise OrchestratorError('host %s does not exist' % host)

        if 'labels' not in self.inventory[host]:
            self.inventory[host]['labels'] = list()
        if label not in self.inventory[host]['labels']:
            self.inventory[host]['labels'].append(label)
        self._save_inventory()
        return 'Added label %s to host %s' % (label, host)

    @async_completion
    def remove_host_label(self, host, label):
        if host not in self.inventory:
            raise OrchestratorError('host %s does not exist' % host)

        if 'labels' not in self.inventory[host]:
            self.inventory[host]['labels'] = list()
        if label in self.inventory[host]['labels']:
            self.inventory[host]['labels'].remove(label)
        self._save_inventory()
        return 'Removed label %s from host %s' % (label, host)

    @async_map_completion
    def _refresh_host_services(self, host):
        out, err, code = self._run_cephadm(
            host, 'mon', 'ls', [], no_fsid=True)
        data = json.loads(''.join(out))
        self.log.error('refreshed host %s services: %s' % (host, data))
        self.service_cache[host] = orchestrator.OutdatableData(data)
        return host, data

    def _get_services(self,
                      service_type=None,
                      service_name=None,
                      service_id=None,
                      node_name=None,
                      refresh=False):
        hosts = []
        wait_for_args = []
        services = {}
        keys = None
        if node_name is not None:
            keys = [node_name]
        for host, host_info in self.service_cache.items_filtered(keys):
            hosts.append(host)
            if host_info.outdated(self.service_cache_timeout) or refresh:
                self.log.info("refreshing stale services for '{}'".format(host))
                wait_for_args.append((host,))
            else:
                self.log.debug('have recent services for %s: %s' % (
                    host, host_info.data))
                services[host] = host_info.data

        def _get_services_result(results):
            for host, data in results:
                services[host] = data

            result = []
            for host, ls in services.items():
                for d in ls:
                    if not d['style'].startswith('cephadm'):
                        self.log.debug('ignoring non-cephadm on %s: %s' % (host, d))
                        continue
                    if d['fsid'] != self._cluster_fsid:
                        self.log.debug('ignoring foreign daemon on %s: %s' % (host, d))
                        continue
                    self.log.debug('including %s %s' % (host, d))
                    sd = orchestrator.ServiceDescription()
                    sd.service_type = d['name'].split('.')[0]
                    if service_type and service_type != sd.service_type:
                        continue
                    if '.' in d['name']:
                        sd.service_instance = '.'.join(d['name'].split('.')[1:])
                    else:
                        sd.service_instance = host  # e.g., crash
                    if service_id and service_id != sd.service_instance:
                        continue
                    if service_name and not sd.service_instance.startswith(service_name + '.'):
                        continue
                    sd.nodename = host
                    sd.container_id = d.get('container_id')
                    sd.container_image_name = d.get('container_image_name')
                    sd.container_image_id = d.get('container_image_id')
                    sd.version = d.get('version')
                    sd.status_desc = d['state']
                    sd.status = {
                        'running': 1,
                        'stopped': 0,
                        'error': -1,
                        'unknown': -1,
                    }[d['state']]
                    result.append(sd)
            return result

        return self._refresh_host_services(wait_for_args).then(
            _get_services_result)

    def describe_service(self, service_type=None, service_id=None,
                         node_name=None, refresh=False):
        if service_type not in ("mds", "osd", "mgr", "mon", 'rgw', "nfs", None):
            raise orchestrator.OrchestratorValidationError(
                service_type + " unsupported")
        result = self._get_services(service_type,
                                    service_id=service_id,
                                    node_name=node_name,
                                    refresh=refresh)
        return result

    def service_action(self, action, service_type,
                       service_name=None,
                       service_id=None):
        self.log.debug('service_action action %s type %s name %s id %s' % (
            action, service_type, service_name, service_id))

        def _proc_daemons(daemons):
            if service_name is None and service_id is None:
                raise ValueError('service_name or service_id required')
            args = []
            for d in daemons:
                args.append((d.service_type, d.service_instance,
                             d.nodename, action))
            if not args:
                if service_name:
                    n = service_name + '-*'
                else:
                    n = service_id
                raise orchestrator.OrchestratorError(
                    'Unable to find %s.%s daemon(s)' % (
                        service_type, n))
            return self._service_action(args)

        return self._get_services(
            service_type,
            service_name=service_name,
            service_id=service_id).then(_proc_daemons)

    @async_map_completion
    def _service_action(self, service_type, service_id, host, action):
        if action == 'redeploy':
            # stop, recreate the container+unit, then restart
            return self._create_daemon(service_type, service_id, host,
                                       None)
        elif action == 'reconfig':
            return self._create_daemon(service_type, service_id, host,
                                       None, reconfig=True)

        actions = {
            'start': ['reset-failed', 'start'],
            'stop': ['stop'],
            'restart': ['reset-failed', 'restart'],
        }
        name = '%s.%s' % (service_type, service_id)
        for a in actions[action]:
            out, err, code = self._run_cephadm(
                host, name, 'unit',
                ['--name', name, a],
                error_ok=True)
            self.service_cache.invalidate(host)
            self.log.debug('_service_action code %s out %s' % (code, out))
        return "{} {} from host '{}'".format(action, name, host)

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

        @async_map_completion
        def _get_inventory(host, host_info):
            # type: (str, orchestrator.OutdatableData) -> orchestrator.InventoryNode

            if host_info.outdated(self.inventory_cache_timeout) or refresh:
                self.log.info("refresh stale inventory for '{}'".format(host))
                out, err, code = self._run_cephadm(
                    host, 'osd',
                    'ceph-volume',
                    ['--', 'inventory', '--format=json'])
                data = json.loads(''.join(out))
                host_info = orchestrator.OutdatableData(data)
                self.inventory_cache[host] = host_info
            else:
                self.log.debug("reading cached inventory for '{}'".format(host))

            devices = inventory.Devices.from_json(host_info.data)
            return orchestrator.InventoryNode(host, devices)

        return _get_inventory(hosts)

    def blink_device_light(self, ident_fault, on, locs):
        @async_map_completion
        def blink(host, dev, path):
            cmd = [
                'lsmcli',
                'local-disk-%s-led-%s' % (
                    ident_fault,
                    'on' if on else 'off'),
                '--path', path or dev,
            ]
            out, err, code = self._run_cephadm(
                host, 'osd', 'shell', ['--'] + cmd,
                error_ok=True)
            if code:
                raise RuntimeError(
                    'Unable to affect %s light for %s:%s. Command: %s' % (
                        ident_fault, host, dev, ' '.join(cmd)))
            return "Set %s light for %s:%s %s" % (
                ident_fault, host, dev, 'on' if on else 'off')

        return blink(locs)

    @async_completion
    def _create_osd(self, all_hosts_, drive_group):
        all_hosts = orchestrator.InventoryNode.get_host_names(all_hosts_)
        assert len(drive_group.hosts(all_hosts)) == 1
        assert len(drive_group.data_devices.paths) > 0
        assert all(map(lambda p: isinstance(p, six.string_types),
            drive_group.data_devices.paths))

        host = drive_group.hosts(all_hosts)[0]
        self._require_hosts(host)

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
            out, err, code = self._run_cephadm(
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
        out, err, code = self._run_cephadm(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        self.log.debug('code %s out %s' % (code, out))
        osds_elems = json.loads('\n'.join(out))
        fsid = self._cluster_fsid
        for osd_id, osds in osds_elems.items():
            for osd in osds:
                if osd['tags']['ceph.cluster_fsid'] != fsid:
                    self.log.debug('mismatched fsid, skipping %s' % osd)
                    continue
                if len(list(set(devices) & set(osd['devices']))) == 0 and osd.get('lv_path') not in devices:
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

    def create_osds(self, drive_group):
        """
        Create a new osd.

        The orchestrator CLI currently handles a narrow form of drive
        specification defined by a single block device using bluestore.

        :param drive_group: osd specification

        TODO:
          - support full drive_group specification
          - support batch creation
        """

        return self.get_hosts().then(lambda hosts: self._create_osd(hosts, drive_group))

    @with_services('osd')
    def remove_osds(self, osd_ids, services):
        # type: (List[str], List[orchestrator.ServiceDescription]) -> AsyncCompletion
        args = [(d.name(), d.nodename) for d in services if
                d.service_instance in osd_ids]

        found = list(zip(*args))[0] if args else []
        not_found = {osd_id for osd_id in osd_ids if 'osd.%s' % osd_id not in found}
        if not_found:
            raise OrchestratorError('Unable to find ODS: %s' % not_found)
        return self._remove_daemon(args)

    def _create_daemon(self, daemon_type, daemon_id, host, keyring,
                       extra_args=None, extra_config=None,
                       reconfig=False):
        if not extra_args:
            extra_args = []
        name = '%s.%s' % (daemon_type, daemon_id)

        # generate config
        ret, config, err = self.mon_command({
            "prefix": "config generate-minimal-conf",
        })
        if extra_config:
            config += extra_config

        # crash_keyring
        ret, crash_keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.crash.%s' % host,
            'caps': ['mon', 'profile crash',
                     'mgr', 'profile crash'],
        })

        j = json.dumps({
            'config': config,
            'keyring': keyring,
            'crash_keyring': crash_keyring,
        })

        if reconfig:
            extra_args.append('--reconfig')

        out, err, code = self._run_cephadm(
            host, name, 'deploy',
            [
                '--name', name,
                '--config-and-keyrings', '-',
            ] + extra_args,
            stdin=j)
        self.log.debug('create_daemon code %s out %s' % (code, out))
        self.service_cache.invalidate(host)
        return "{} {} on host '{}'".format(
            'Reconfigured' if reconfig else 'Deployed', name, host)

    @async_map_completion
    def _remove_daemon(self, name, host):
        """
        Remove a daemon
        """
        out, err, code = self._run_cephadm(
            host, name, 'rm-daemon',
            ['--name', name])
        self.log.debug('_remove_daemon code %s out %s' % (code, out))
        self.service_cache.invalidate(host)
        return "Removed {} from host '{}'".format(name, host)

    def _update_service(self, daemon_type, add_func, spec):
        def ___update_service(daemons):
            if len(daemons) > spec.count:
                # remove some
                to_remove = len(daemons) - spec.count
                args = []
                for d in daemons[0:to_remove]:
                    args.append(
                        ('%s.%s' % (d.service_type, d.service_instance), d.nodename)
                    )
                return self._remove_daemon(args)
            elif len(daemons) < spec.count:
                # add some
                spec.count -= len(daemons)
                return add_func(spec)
            return []
        return self._get_services(daemon_type, service_name=spec.name).then(___update_service)

    @async_map_completion
    def _create_mon(self, host, network, name):
        """
        Create a new monitor on the given host.
        """
        self.log.info("create_mon({}:{}): starting".format(host, network))

        # get mon. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        # infer whether this is a CIDR network, addrvec, or plain IP
        extra_config = '[mon.%s]\n' % name
        if '/' in network:
            extra_config += 'public network = %s\n' % network
        elif network.startswith('[v') and network.endswith(']'):
            extra_config += 'public addrv = %s\n' % network
        elif ':' not in network:
            extra_config += 'public addr = %s\n' % network
        else:
            raise RuntimeError('Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)

        return self._create_daemon('mon', name or host, host, keyring,
                                   extra_config=extra_config)

    def update_mons(self, spec):
        # type: (orchestrator.StatefulServiceSpec) -> orchestrator.Completion
        """
        Adjust the number of cluster managers.
        """
        if not spec.placement.hosts and not spec.placement.label:
            # Improve Error message. Point to parse_host_spec examples
            raise orchestrator.OrchestratorValidationError("Mons need a host spec. (host, network, name(opt))")

        spec = NodeAssignment(spec=spec, get_hosts_func=self._get_hosts, service_type='mon').load()
        return self._update_mons(spec)

    def _update_mons(self, spec):
        # type: (orchestrator.StatefulServiceSpec) -> orchestrator.Completion
        """
        Adjust the number of cluster monitors.
        """
        # current support limited to adding monitors.
        mon_map = self.get("mon_map")
        num_mons = len(mon_map["mons"])
        if spec.count == num_mons:
            return orchestrator.Completion(value="The requested number of monitors exist.")
        if spec.count < num_mons:
            raise NotImplementedError("Removing monitors is not supported.")

        self.log.debug("Trying to update monitors on: {}".format(spec.placement.hosts))
        # check that all the hosts are registered
        [self._require_hosts(host.hostname) for host in spec.placement.hosts]

        # current support requires a network to be specified
        for host, network, _ in spec.placement.hosts:
            if not network:
                raise RuntimeError("Host '{}' is missing a network spec".format(host))

        def update_mons_with_daemons(daemons):
            for _, _, name in spec.placement.hosts:
                if name and len([d for d in daemons if d.service_instance == name]):
                    raise RuntimeError('name %s alrady exists', name)

            # explicit placement: enough hosts provided?
            num_new_mons = spec.count - num_mons
            if len(spec.placement.hosts) < num_new_mons:
                raise RuntimeError("Error: {} hosts provided, expected {}".format(
                    len(spec.placement.hosts), num_new_mons))
            self.log.info("creating {} monitors on hosts: '{}'".format(
                num_new_mons, ",".join(map(lambda h: ":".join(h), spec.placement.hosts))))
            # TODO: we may want to chain the creation of the monitors so they join
            # the quorum one at a time.
            return self._create_mon(spec.placement.hosts)
        return self._get_services('mon').then(update_mons_with_daemons)

    @async_map_completion
    def _create_mgr(self, host, name):
        """
        Create a new manager instance on a host.
        """
        self.log.info("create_mgr({}, mgr.{}): starting".format(host, name))

        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mgr.%s' % name,
            'caps': ['mon', 'profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        return self._create_daemon('mgr', name, host, keyring)

    def update_mgrs(self, spec):
        # type: (orchestrator.StatefulServiceSpec) -> orchestrator.Completion
        """
        Adjust the number of cluster managers.
        """
        spec = NodeAssignment(spec=spec, get_hosts_func=self._get_hosts, service_type='mgr').load()
        return self._get_services('mgr').then(lambda daemons: self._update_mgrs(spec, daemons))

    def _update_mgrs(self, spec, daemons):
        # type: (orchestrator.StatefulServiceSpec, List[orchestrator.ServiceDescription]) -> orchestrator.Completion
        num_mgrs = len(daemons)
        if spec.count == num_mgrs:
            return orchestrator.Completion(value="The requested number of managers exist.")

        self.log.debug("Trying to update managers on: {}".format(spec.placement.hosts))
        # check that all the hosts are registered
        [self._require_hosts(host.hostname) for host in spec.placement.hosts]

        if spec.count < num_mgrs:
            num_to_remove = num_mgrs - spec.count

            # first try to remove unconnected mgr daemons that the
            # cluster doesn't see
            connected = []
            mgr_map = self.get("mgr_map")
            if mgr_map.get("active_name", {}):
                connected.append(mgr_map.get('active_name', ''))
            for standby in mgr_map.get('standbys', []):
                connected.append(standby.get('name', ''))
            to_remove_damons = []
            for d in daemons:
                if d.service_instance not in connected:
                    to_remove_damons.append(('%s.%s' % (d.service_type, d.service_instance),
                                             d.nodename))
                    num_to_remove -= 1
                    if num_to_remove == 0:
                        break

            # otherwise, remove *any* mgr
            if num_to_remove > 0:
                for d in daemons:
                    to_remove_damons.append(('%s.%s' % (d.service_type, d.service_instance), d.nodename))
                    num_to_remove -= 1
                    if num_to_remove == 0:
                        break
            return self._remove_daemon(to_remove_damons)

        else:
            # we assume explicit placement by which there are the same number of
            # hosts specified as the size of increase in number of daemons.
            num_new_mgrs = spec.count - num_mgrs
            if len(spec.placement.hosts) < num_new_mgrs:
                raise RuntimeError(
                    "Error: {} hosts provided, expected {}".format(
                        len(spec.placement.hosts), num_new_mgrs))

            for host_spec in spec.placement.hosts:
                if host_spec.name and len([d for d in daemons if d.service_instance == host_spec.name]):
                    raise RuntimeError('name %s alrady exists', host_spec.name)

            for host_spec in spec.placement.hosts:
                if host_spec.name and len([d for d in daemons if d.service_instance == host_spec.name]):
                    raise RuntimeError('name %s alrady exists', host_spec.name)

            self.log.info("creating {} managers on hosts: '{}'".format(
                num_new_mgrs, ",".join([_spec.hostname for _spec in spec.placement.hosts])))

            args = []
            for host_spec in spec.placement.hosts:
                name = host_spec.name or self.get_unique_name(daemons)
                host = host_spec.hostname
                args.append((host, name))
        return self._create_mgr(args)

    def add_mds(self, spec):
        if not spec.placement.hosts or len(spec.placement.hosts) < spec.placement.count:
            raise RuntimeError("must specify at least %d hosts" % spec.placement.count)
        # ensure mds_join_fs is set for these daemons
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.name,
            'name': 'mds_join_fs',
            'value': spec.name,
        })
        return self._get_services('mds').then(lambda ds: self._add_mds(ds, spec))

    def _add_mds(self, daemons, spec):
        args = []
        num_added = 0
        for host, _, name in spec.placement.hosts:
            if num_added >= spec.count:
                break
            mds_id = self.get_unique_name(daemons, spec.name, name)
            self.log.debug('placing mds.%s on host %s' % (mds_id, host))
            args.append((mds_id, host))
            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.ServiceDescription()
            sd.service_instance = mds_id
            sd.service_type = 'mds'
            sd.nodename = host
            daemons.append(sd)
            num_added += 1
        return self._create_mds(args)

    def update_mds(self, spec):
        spec = NodeAssignment(spec=spec, get_hosts_func=self._get_hosts, service_type='mds').load()
        return self._update_service('mds', self.add_mds, spec)

    @async_map_completion
    def _create_mds(self, mds_id, host):
        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mds.' + mds_id,
            'caps': ['mon', 'profile mds',
                     'osd', 'allow rwx',
                     'mds', 'allow'],
        })
        return self._create_daemon('mds', mds_id, host, keyring)

    def remove_mds(self, name):
        self.log.debug("Attempting to remove volume: {}".format(name))

        def _remove_mds(daemons):
            args = []
            for d in daemons:
                if d.service_instance == name or d.service_instance.startswith(name + '.'):
                    args.append(
                        ('%s.%s' % (d.service_type, d.service_instance), d.nodename)
                    )
            if not args:
                raise OrchestratorError('Unable to find mds.%s[-*] daemon(s)' % name)
            return self._remove_daemon(args)
        return self._get_services('mds').then(_remove_mds)

    def add_rgw(self, spec):
        if not spec.placement.hosts or len(spec.placement.hosts) < spec.count:
            raise RuntimeError("must specify at least %d hosts" % spec.count)
        # ensure rgw_realm and rgw_zone is set for these daemons
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'client.rgw.' + spec.name,
            'name': 'rgw_zone',
            'value': spec.rgw_zone,
        })
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'client.rgw.' + spec.rgw_realm,
            'name': 'rgw_realm',
            'value': spec.rgw_realm,
        })

        def _add_rgw(daemons):
            args = []
            num_added = 0
            for host, _, name in spec.placement.hosts:
                if num_added >= spec.count:
                    break
                rgw_id = self.get_unique_name(daemons, spec.name, name)
                self.log.debug('placing rgw.%s on host %s' % (rgw_id, host))
                args.append((rgw_id, host))
                # add to daemon list so next name(s) will also be unique
                sd = orchestrator.ServiceDescription()
                sd.service_instance = rgw_id
                sd.service_type = 'rgw'
                sd.nodename = host
                daemons.append(sd)
                num_added += 1
            return self._create_rgw(args)

        return self._get_services('rgw').then(_add_rgw)

    @async_map_completion
    def _create_rgw(self, rgw_id, host):
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rgw.' + rgw_id,
            'caps': ['mon', 'allow rw',
                     'mgr', 'allow rw',
                     'osd', 'allow rwx'],
        })
        return self._create_daemon('rgw', rgw_id, host, keyring)

    def remove_rgw(self, name):

        def _remove_rgw(daemons):
            args = []
            for d in daemons:
                if d.service_instance == name or d.service_instance.startswith(name + '.'):
                    args.append(('%s.%s' % (d.service_type, d.service_instance),
                                d.nodename))
            if args:
                return self._remove_daemon(args)
            raise RuntimeError('Unable to find rgw.%s[-*] daemon(s)' % name)

        return self._get_services('rgw').then(_remove_rgw)

    def update_rgw(self, spec):
        spec = NodeAssignment(spec=spec, get_hosts_func=self._get_hosts, service_type='rgw').load()
        return self._update_service('rgw', self.add_rgw, spec)

    def add_rbd_mirror(self, spec):
        if not spec.placement.hosts or len(spec.placement.hosts) < spec.count:
            raise RuntimeError("must specify at least %d hosts" % spec.count)
        self.log.debug('nodes %s' % spec.placement.hosts)

        def _add_rbd_mirror(daemons):
            args = []
            num_added = 0
            for host, _, name in spec.placement.hosts:
                if num_added >= spec.count:
                    break
                daemon_id = self.get_unique_name(daemons, None, name)
                self.log.debug('placing rbd-mirror.%s on host %s' % (daemon_id,
                                                                     host))
                args.append((daemon_id, host))

                # add to daemon list so next name(s) will also be unique
                sd = orchestrator.ServiceDescription()
                sd.service_instance = daemon_id
                sd.service_type = 'rbd-mirror'
                sd.nodename = host
                daemons.append(sd)
                num_added += 1
            return self._create_rbd_mirror(args)

        return self._get_services('rbd-mirror').then(_add_rbd_mirror)

    @async_map_completion
    def _create_rbd_mirror(self, daemon_id, host):
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rbd-mirror.' + daemon_id,
            'caps': ['mon', 'profile rbd-mirror',
                     'osd', 'profile rbd'],
        })
        return self._create_daemon('rbd-mirror', daemon_id, host, keyring)

    def remove_rbd_mirror(self, name):
        def _remove_rbd_mirror(daemons):
            args = []
            for d in daemons:
                if not name or d.service_instance == name:
                    args.append(
                        ('%s.%s' % (d.service_type, d.service_instance),
                         d.nodename)
                    )
            if not args and name:
                raise RuntimeError('Unable to find rbd-mirror.%s daemon' % name)
            return self._remove_daemon(args)

        return self._get_services('rbd-mirror').then(_remove_rbd_mirror)

    def update_rbd_mirror(self, spec):
        spec = NodeAssignment(spec=spec, get_hosts_func=self._get_hosts, service_type='rbd-mirror').load()
        return self._update_service('rbd-mirror', self.add_rbd_mirror, spec)

    def _get_container_image_id(self, image_name):
        # pick a random host...
        host = None
        for host_name in self.inventory_cache:
            host = host_name
            break
        if not host:
            raise OrchestratorError('no hosts defined')
        self.log.debug('using host %s' % host)
        out, code = self._run_cephadm(
            host, None, 'pull', [],
            image=image_name,
            no_fsid=True)
        return out[0]

    def upgrade_check(self, image, version):
        if version:
            target = self.container_image_base + ':v' + version
        elif image:
            target = image
        else:
            raise OrchestratorError('must specify either image or version')
        return self._get_services().then(lambda daemons: self._upgrade_check(target, daemons))

    def _upgrade_check(self, target, services):
        # get service state
        target_id = self._get_container_image_id(target)
        self.log.debug('Target image %s id %s' % (target, target_id))
        r = {
            'target_image_name': target,
            'target_image_id': target_id,
            'needs_update': dict(),
            'up_to_date': list(),
        }
        for s in services:
            if target_id == s.container_image_id:
                r['up_to_date'].append(s.name())
            else:
                r['needs_update'][s.name()] = {
                    'current_name': s.container_image_name,
                    'current_id': s.container_image_id,
                }
        return trivial_result(json.dumps(r, indent=4, sort_keys=True))


class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a placement_spec

    `place(host_pool)` needs to return a List[HostSpec, ..]
    """

    def __init__(self, placement_spec):
        # type: (orchestrator.PlacementSpec) -> None
        self.placement_spec = placement_spec

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostSpec]
        raise NotImplementedError


class SimpleScheduler(BaseScheduler):
    """
    The most simple way to pick/schedule a set of hosts.
    1) Shuffle the provided host_pool
    2) Select from list up to :count
    """
    def __init__(self, placement_spec):
        super(SimpleScheduler, self).__init__(placement_spec)

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostSpec]
        if not host_pool:
            raise Exception('List of host candidates is empty')
        host_pool = [HostSpec(x, '', '') for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
        return host_pool[:count]


class NodeAssignment(object):
    """
    A class to detect if nodes are being passed imperative or declarative
    If the spec is populated via the `nodes/hosts` field it will not load
    any nodes into the list.
    If the spec isn't populated, i.e. when only num or label is present (declarative)
    it will use the provided `get_host_func` to load it from the inventory.

    Schedulers can be assigned to pick hosts from the pool.
    """

    def __init__(self,
                 spec=None,  # type: Optional[orchestrator.StatefulServiceSpec]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 get_hosts_func=None,  # type: Optional[Callable]
                 service_type=None,  # type: Optional[str]
                 ):
        assert spec and get_hosts_func and service_type
        self.spec = spec  # type: orchestrator.StatefulServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.service_type = service_type

    def load(self):
        # type: () -> orchestrator.StatefulServiceSpec
        """
        Load nodes into the spec.placement.nodes container.
        """
        self.load_labeled_nodes()
        self.assign_nodes()
        return self.spec

    def load_labeled_nodes(self):
        # type: () -> None
        """
        Assign nodes based on their label
        """
        # Querying for labeled nodes doesn't work currently.
        # Leaving this open for the next iteration
        # NOTE: This currently queries for all hosts without label restriction
        if self.spec.placement.label:
            logger.info("Found labels. Assinging nodes that match the label")
            candidates = [HostSpec(x[0], '', '') for x in self.get_hosts_func()]  # TODO: query for labels
            logger.info('Assigning nodes to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)

    def assign_nodes(self):
        # type: () -> None
        """
        Use the assigned scheduler to load nodes into the spec.placement.nodes container
        """
        # If no imperative or declarative host assignments, use the scheduler to pick from the
        # host pool (assuming `count` is set)
        if not self.spec.placement.label and not self.spec.placement.hosts and self.spec.placement.count:
            logger.info("Found num spec. Looking for labeled nodes.")
            # TODO: actually query for labels (self.service_type)
            candidates = self.scheduler.place([x[0] for x in self.get_hosts_func()],
                                              count=self.spec.placement.count)
            # Not enough nodes to deploy on
            if len(candidates) != self.spec.placement.count:
                logger.warning("Did not find enough labeled nodes to \
                               scale to <{}> services. Falling back to unlabeled nodes.".
                               format(self.spec.placement.count))
            else:
                logger.info('Assigning nodes to spec: {}'.format(candidates))
                self.spec.placement.set_hosts(candidates)
                return None

            candidates = self.scheduler.place([x[0] for x in self.get_hosts_func()], count=self.spec.placement.count)
            # Not enough nodes to deploy on
            if len(candidates) != self.spec.placement.count:
                raise OrchestratorValidationError("Cannot place {} services on {} hosts.".
                                                  format(self.spec.placement.count, len(candidates)))

            logger.info('Assigning nodes to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)
            return None

