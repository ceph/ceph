import json
import re
import logging
import secrets
import subprocess
import collections
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, List, Callable, Any, TypeVar, Generic, \
    Optional, Dict, Any, Tuple, NewType

from mgr_module import HandleCommandResult, MonCommandFailed

from ceph.deployment.service_spec import ServiceSpec, RGWSpec
from ceph.deployment.utils import is_ipv6, unwrap_ipv6
from orchestrator import OrchestratorError, DaemonDescription
from cephadm import utils
from mgr_util import create_self_signed_cert, ServerConfigException, verify_tls

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)

ServiceSpecs = TypeVar('ServiceSpecs', bound=ServiceSpec)
AuthEntity = NewType('AuthEntity', str)


class CephadmDaemonSpec(Generic[ServiceSpecs]):
    # typing.NamedTuple + Generic is broken in py36
    def __init__(self, host: str, daemon_id: str,
                 spec: Optional[ServiceSpecs] = None,
                 network: Optional[str] = None,
                 keyring: Optional[str] = None,
                 extra_args: Optional[List[str]] = None,
                 ceph_conf: str = '',
                 extra_files: Optional[Dict[str, Any]] = None,
                 daemon_type: Optional[str] = None,
                 ports: Optional[List[int]] = None,):
        """
        Used for
        * deploying new daemons. then everything is set
        * redeploying existing daemons, then only the first three attrs are set.

        Would be great to have a consistent usage where all properties are set.
        """
        self.host: str = host
        self.daemon_id = daemon_id
        daemon_type = daemon_type or (spec.service_type if spec else None)
        assert daemon_type is not None
        self.daemon_type: str = daemon_type

        # would be great to have the spec always available:
        self.spec: Optional[ServiceSpecs] = spec

        # mons
        self.network = network

        # for run_cephadm.
        self.keyring: Optional[str] = keyring

        # For run_cephadm. Would be great to have more expressive names.
        self.extra_args: List[str] = extra_args or []

        self.ceph_conf = ceph_conf
        self.extra_files = extra_files or {}

        # TCP ports used by the daemon
        self.ports:  List[int] = ports or []

    def name(self) -> str:
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def config_get_files(self) -> Dict[str, Any]:
        files = self.extra_files
        if self.ceph_conf:
            files['config'] = self.ceph_conf

        return files


class CephadmService(metaclass=ABCMeta):
    """
    Base class for service types. Often providing a create() and config() fn.
    """

    @property
    @abstractmethod
    def TYPE(self) -> str:
        pass

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    def make_daemon_spec(self, host: str,
                         daemon_id: str,
                         netowrk: str,
                         spec: ServiceSpecs,
                         daemon_type: Optional[str] = None,) -> CephadmDaemonSpec:
        return CephadmDaemonSpec(
            host=host,
            daemon_id=daemon_id,
            spec=spec,
            network=netowrk,
            daemon_type=daemon_type
        )

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        raise NotImplementedError()

    def generate_config(self, daemon_spec: CephadmDaemonSpec) -> Tuple[Dict[str, Any], List[str]]:
        raise NotImplementedError()

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]) -> None:
        """The post actions needed to be done after daemons are checked"""
        if self.mgr.config_dashboard:
            if 'dashboard' in self.mgr.get('mgr_map')['modules']:
                self.config_dashboard(daemon_descrs)
            else:
                logger.debug('Dashboard is not enabled. Skip configuration.')

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        """Config dashboard settings."""
        raise NotImplementedError()

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        # if this is called for a service type where it hasn't explcitly been
        # defined, return empty Daemon Desc
        return DaemonDescription()

    def _inventory_get_addr(self, hostname: str) -> str:
        """Get a host's address with its hostname."""
        return self.mgr.inventory.get_addr(hostname)

    def _set_service_url_on_dashboard(self,
                                      service_name: str,
                                      get_mon_cmd: str,
                                      set_mon_cmd: str,
                                      service_url: str) -> None:
        """A helper to get and set service_url via Dashboard's MON command.

           If result of get_mon_cmd differs from service_url, set_mon_cmd will
           be sent to set the service_url.
        """
        def get_set_cmd_dicts(out: str) -> List[dict]:
            cmd_dict = {
                'prefix': set_mon_cmd,
                'value': service_url
            }
            return [cmd_dict] if service_url != out else []

        self._check_and_set_dashboard(
            service_name=service_name,
            get_cmd=get_mon_cmd,
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def _check_and_set_dashboard(self,
                                 service_name: str,
                                 get_cmd: str,
                                 get_set_cmd_dicts: Callable[[str], List[dict]]) -> None:
        """A helper to set configs in the Dashboard.

        The method is useful for the pattern:
            - Getting a config from Dashboard by using a Dashboard command. e.g. current iSCSI
              gateways.
            - Parse or deserialize previous output. e.g. Dashboard command returns a JSON string.
            - Determine if the config need to be update. NOTE: This step is important because if a
              Dashboard command modified Ceph config, cephadm's config_notify() is called. Which
              kicks the serve() loop and the logic using this method is likely to be called again.
              A config should be updated only when needed.
            - Update a config in Dashboard by using a Dashboard command.

        :param service_name: the service name to be used for logging
        :type service_name: str
        :param get_cmd: Dashboard command prefix to get config. e.g. dashboard get-grafana-api-url
        :type get_cmd: str
        :param get_set_cmd_dicts: function to create a list, and each item is a command dictionary.
            e.g.
            [
                {
                   'prefix': 'dashboard iscsi-gateway-add',
                   'service_url': 'http://admin:admin@aaa:5000',
                   'name': 'aaa'
                },
                {
                    'prefix': 'dashboard iscsi-gateway-add',
                    'service_url': 'http://admin:admin@bbb:5000',
                    'name': 'bbb'
                }
            ]
            The function should return empty list if no command need to be sent.
        :type get_set_cmd_dicts: Callable[[str], List[dict]]
        """

        try:
            _, out, _ = self.mgr.check_mon_command({
                'prefix': get_cmd
            })
        except MonCommandFailed as e:
            logger.warning('Failed to get Dashboard config for %s: %s', service_name, e)
            return
        cmd_dicts = get_set_cmd_dicts(out.strip())
        for cmd_dict in list(cmd_dicts):
            try:
                inbuf = cmd_dict.pop('inbuf', None)
                _, out, _ = self.mgr.check_mon_command(cmd_dict, inbuf)
            except MonCommandFailed as e:
                logger.warning('Failed to set Dashboard config for %s: %s', service_name, e)

    def ok_to_stop(self, daemon_ids: List[str]) -> HandleCommandResult:
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        out = f'It is presumed safe to stop {names}'
        err = f'It is NOT safe to stop {names}'

        if self.TYPE not in ['mon', 'osd', 'mds']:
            logger.info(out)
            return HandleCommandResult(0, out, None)

        r = HandleCommandResult(*self.mgr.mon_command({
            'prefix': f'{self.TYPE} ok-to-stop',
            'ids': daemon_ids,
        }))

        if r.retval:
            err = f'{err}: {r.stderr}' if r.stderr else err
            logger.error(err)
            return HandleCommandResult(r.retval, r.stdout, err)

        out = f'{out}: {r.stdout}' if r.stdout else out
        logger.info(out)
        return HandleCommandResult(r.retval, out, r.stderr)

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before the daemon is removed.
        """
        assert self.TYPE == daemon.daemon_type
        logger.debug(f'Pre remove daemon {self.TYPE}.{daemon.daemon_id}')

    def post_remove(self, daemon: DaemonDescription) -> None:
        """
        Called after the daemon is removed.
        """
        assert self.TYPE == daemon.daemon_type
        logger.debug(f'Post remove daemon {self.TYPE}.{daemon.daemon_id}')

    def purge(self) -> None:
        """Called to carry out any purge tasks following service removal"""
        logger.debug(f'Purge called for {self.TYPE} - no action taken')


class CephService(CephadmService):
    def generate_config(self, daemon_spec: CephadmDaemonSpec) -> Tuple[Dict[str, Any], List[str]]:
        # Ceph.daemons (mon, mgr, mds, osd, etc)
        cephadm_config = self.get_config_and_keyring(
            daemon_spec.daemon_type,
            daemon_spec.daemon_id,
            host=daemon_spec.host,
            keyring=daemon_spec.keyring,
            extra_ceph_config=daemon_spec.ceph_conf)

        if daemon_spec.config_get_files():
            cephadm_config.update({'files': daemon_spec.config_get_files()})

        return cephadm_config, []

    def post_remove(self, daemon: DaemonDescription) -> None:
        super().post_remove(daemon)
        self.remove_keyring(daemon)

    def get_auth_entity(self, daemon_id: str, host: str = "") -> AuthEntity:
        """
        Map the daemon id to a cephx keyring entity name
        """
        if self.TYPE in ['rgw', 'rbd-mirror', 'nfs', "iscsi", 'haproxy', 'keepalived']:
            return AuthEntity(f'client.{self.TYPE}.{daemon_id}')
        elif self.TYPE == 'crash':
            if host == "":
                raise OrchestratorError("Host not provided to generate <crash> auth entity name")
            return AuthEntity(f'client.{self.TYPE}.{host}')
        elif self.TYPE == 'mon':
            return AuthEntity('mon.')
        elif self.TYPE in ['mgr', 'osd', 'mds']:
            return AuthEntity(f'{self.TYPE}.{daemon_id}')
        else:
            raise OrchestratorError("unknown daemon type")

    def get_config_and_keyring(self,
                               daemon_type: str,
                               daemon_id: str,
                               host: str,
                               keyring: Optional[str] = None,
                               extra_ceph_config: Optional[str] = None
                               ) -> Dict[str, Any]:
        # keyring
        if not keyring:
            entity: AuthEntity = self.get_auth_entity(daemon_id, host=host)
            ret, keyring, err = self.mgr.check_mon_command({
                'prefix': 'auth get',
                'entity': entity,
            })

        config = self.mgr.get_minimal_ceph_conf()

        if extra_ceph_config:
            config += extra_ceph_config

        return {
            'config': config,
            'keyring': keyring,
        }

    def remove_keyring(self, daemon: DaemonDescription) -> None:
        daemon_id: str = daemon.daemon_id
        host: str = daemon.hostname

        if daemon_id == 'mon':
            # do not remove the mon keyring
            return

        entity = self.get_auth_entity(daemon_id, host=host)

        logger.info(f'Remove keyring: {entity}')
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': entity,
        })


class MonService(CephService):
    TYPE = 'mon'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        """
        Create a new monitor on the given host.
        """
        assert self.TYPE == daemon_spec.daemon_type
        name, host, network = daemon_spec.daemon_id, daemon_spec.host, daemon_spec.network

        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': self.get_auth_entity(name),
        })

        extra_config = '[mon.%s]\n' % name
        if network:
            # infer whether this is a CIDR network, addrvec, or plain IP
            if '/' in network:
                extra_config += 'public network = %s\n' % network
            elif network.startswith('[v') and network.endswith(']'):
                extra_config += 'public addrv = %s\n' % network
            elif is_ipv6(network):
                extra_config += 'public addr = %s\n' % unwrap_ipv6(network)
            elif ':' not in network:
                extra_config += 'public addr = %s\n' % network
            else:
                raise OrchestratorError(
                    'Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)
        else:
            # try to get the public_network from the config
            ret, network, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            network = network.strip() if network else network
            if not network:
                raise OrchestratorError(
                    'Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP')
            if '/' not in network:
                raise OrchestratorError(
                    'public_network is set but does not look like a CIDR network: \'%s\'' % network)
            extra_config += 'public network = %s\n' % network

        daemon_spec.ceph_conf = extra_config
        daemon_spec.keyring = keyring

        return daemon_spec

    def _check_safe_to_destroy(self, mon_id: str) -> None:
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception as e:
            raise OrchestratorError('failed to parse quorum status')

        mons = [m['name'] for m in j['monmap']['mons']]
        if mon_id not in mons:
            logger.info('Safe to remove mon.%s: not in monmap (%s)' % (
                mon_id, mons))
            return
        new_mons = [m for m in mons if m != mon_id]
        new_quorum = [m for m in j['quorum_names'] if m != mon_id]
        if len(new_quorum) > len(new_mons) / 2:
            logger.info('Safe to remove mon.%s: new quorum should be %s (from %s)' %
                        (mon_id, new_quorum, new_mons))
            return
        raise OrchestratorError(
            'Removing %s would break mon quorum (new quorum %s, new mons %s)' % (mon_id, new_quorum, new_mons))

    def pre_remove(self, daemon: DaemonDescription) -> None:
        super().pre_remove(daemon)

        daemon_id: str = daemon.daemon_id
        self._check_safe_to_destroy(daemon_id)

        # remove mon from quorum before we destroy the daemon
        logger.info('Removing monitor %s from monmap...' % daemon_id)
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'mon rm',
            'name': daemon_id,
        })


class MgrService(CephService):
    TYPE = 'mgr'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        """
        Create a new manager instance on a host.
        """
        assert self.TYPE == daemon_spec.daemon_type
        mgr_id, host = daemon_spec.daemon_id, daemon_spec.host

        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(mgr_id),
            'caps': ['mon', 'profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        # Retrieve ports used by manager modules
        # In the case of the dashboard port and with several manager daemons
        # running in different hosts, it exists the possibility that the
        # user has decided to use different dashboard ports in each server
        # If this is the case then the dashboard port opened will be only the used
        # as default.
        ports = []
        config_ports = ''
        ret, mgr_services, err = self.mgr.check_mon_command({
            'prefix': 'mgr services',
        })
        if mgr_services:
            mgr_endpoints = json.loads(mgr_services)
            for end_point in mgr_endpoints.values():
                port = re.search(r'\:\d+\/', end_point)
                if port:
                    ports.append(int(port[0][1:-1]))

        if ports:
            daemon_spec.ports = ports

        daemon_spec.keyring = keyring

        return daemon_spec

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        for daemon in daemon_descrs:
            if self.mgr.daemon_is_self(daemon.daemon_type, daemon.daemon_id):
                return daemon
        # if no active mgr found, return empty Daemon Desc
        return DaemonDescription()

    def fail_over(self) -> None:
        if not self.mgr_map_has_standby():
            raise OrchestratorError('Need standby mgr daemon', event_kind_subject=(
                'daemon', 'mgr' + self.mgr.get_mgr_id()))

        self.mgr.events.for_daemon('mgr' + self.mgr.get_mgr_id(),
                                   'INFO', 'Failing over to other MGR')
        logger.info('Failing over to other MGR')

        # fail over
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'mgr fail',
            'who': self.mgr.get_mgr_id(),
        })

    def mgr_map_has_standby(self) -> bool:
        """
        This is a bit safer than asking our inventory. If the mgr joined the mgr map,
        we know it joined the cluster
        """
        mgr_map = self.mgr.get('mgr_map')
        num = len(mgr_map.get('standbys'))
        return bool(num)


class MdsService(CephService):
    TYPE = 'mds'

    def config(self, spec: ServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        assert spec.service_id

        # ensure mds_join_fs is set for these daemons
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.service_id,
            'name': 'mds_join_fs',
            'value': spec.service_id,
        })

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        mds_id, host = daemon_spec.daemon_id, daemon_spec.host

        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(mds_id),
            'caps': ['mon', 'profile mds',
                     'osd', 'allow rw tag cephfs *=*',
                     'mds', 'allow'],
        })
        daemon_spec.keyring = keyring

        return daemon_spec

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        active_mds_strs = list()
        for fs in self.mgr.get('fs_map')['filesystems']:
            mds_map = fs['mdsmap']
            if mds_map is not None:
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['state'] == 'up:active':
                        active_mds_strs.append(mds_status['name'])
        if len(active_mds_strs) != 0:
            for daemon in daemon_descrs:
                if daemon.daemon_id in active_mds_strs:
                    return daemon
        # if no mds found, return empty Daemon Desc
        return DaemonDescription()


class RgwService(CephService):
    TYPE = 'rgw'

    def config(self, spec: RGWSpec, rgw_id: str) -> None:
        assert self.TYPE == spec.service_type

        # create realm, zonegroup, and zone if needed
        self.create_realm_zonegroup_zone(spec, rgw_id)

        # ensure rgw_realm and rgw_zone is set for these daemons
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
            'name': 'rgw_zone',
            'value': spec.rgw_zone,
        })
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.rgw_realm}",
            'name': 'rgw_realm',
            'value': spec.rgw_realm,
        })
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
            'name': 'rgw_frontends',
            'value': spec.rgw_frontends_config_value(),
        })

        if spec.rgw_frontend_ssl_certificate:
            if isinstance(spec.rgw_frontend_ssl_certificate, list):
                cert_data = '\n'.join(spec.rgw_frontend_ssl_certificate)
            elif isinstance(spec.rgw_frontend_ssl_certificate, str):
                cert_data = spec.rgw_frontend_ssl_certificate
            else:
                raise OrchestratorError(
                    'Invalid rgw_frontend_ssl_certificate: %s'
                    % spec.rgw_frontend_ssl_certificate)
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.crt',
                'val': cert_data,
            })

        if spec.rgw_frontend_ssl_key:
            if isinstance(spec.rgw_frontend_ssl_key, list):
                key_data = '\n'.join(spec.rgw_frontend_ssl_key)
            elif isinstance(spec.rgw_frontend_ssl_certificate, str):
                key_data = spec.rgw_frontend_ssl_key
            else:
                raise OrchestratorError(
                    'Invalid rgw_frontend_ssl_key: %s'
                    % spec.rgw_frontend_ssl_key)
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.key',
                'val': key_data,
            })

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        rgw_id, host = daemon_spec.daemon_id, daemon_spec.host

        keyring = self.get_keyring(rgw_id)

        daemon_spec.keyring = keyring

        return daemon_spec

    def get_keyring(self, rgw_id: str) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(rgw_id),
            'caps': ['mon', 'allow *',
                     'mgr', 'allow rw',
                     'osd', 'allow rwx tag rgw'],
        })
        return keyring

    def create_realm_zonegroup_zone(self, spec: RGWSpec, rgw_id: str) -> None:
        if utils.get_cluster_health(self.mgr) != 'HEALTH_OK':
            raise OrchestratorError('Health not ok, will try again when health ok')

        # get keyring needed to run rados commands and strip out just the keyring
        keyring = self.get_keyring(rgw_id).split('key = ', 1)[1].rstrip()

        # We can call radosgw-admin within the container, cause cephadm gives the MGR the required keyring permissions

        def get_realms() -> List[str]:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'realm', 'list',
                   '--format=json']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = result.stdout
            if not out:
                return []
            try:
                j = json.loads(out)
                return j.get('realms', [])
            except Exception as e:
                raise OrchestratorError('failed to parse realm info')

        def create_realm() -> None:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'realm', 'create',
                   '--rgw-realm=%s' % spec.rgw_realm,
                   '--default']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.mgr.log.info('created realm: %s' % spec.rgw_realm)

        def get_zonegroups() -> List[str]:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'zonegroup', 'list',
                   '--format=json']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = result.stdout
            if not out:
                return []
            try:
                j = json.loads(out)
                return j.get('zonegroups', [])
            except Exception as e:
                raise OrchestratorError('failed to parse zonegroup info')

        def create_zonegroup() -> None:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'zonegroup', 'create',
                   '--rgw-zonegroup=default',
                   '--master', '--default']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.mgr.log.info('created zonegroup: default')

        def create_zonegroup_if_required() -> None:
            zonegroups = get_zonegroups()
            if 'default' not in zonegroups:
                create_zonegroup()

        def get_zones() -> List[str]:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'zone', 'list',
                   '--format=json']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = result.stdout
            if not out:
                return []
            try:
                j = json.loads(out)
                return j.get('zones', [])
            except Exception as e:
                raise OrchestratorError('failed to parse zone info')

        def create_zone() -> None:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'zone', 'create',
                   '--rgw-zonegroup=default',
                   '--rgw-zone=%s' % spec.rgw_zone,
                   '--master', '--default']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.mgr.log.info('created zone: %s' % spec.rgw_zone)

        changes = False
        realms = get_realms()
        if spec.rgw_realm not in realms:
            create_realm()
            changes = True

        zones = get_zones()
        if spec.rgw_zone not in zones:
            create_zonegroup_if_required()
            create_zone()
            changes = True

        # update period if changes were made
        if changes:
            cmd = ['radosgw-admin',
                   '--key=%s' % keyring,
                   '--user', 'rgw.%s' % rgw_id,
                   'period', 'update',
                   '--rgw-realm=%s' % spec.rgw_realm,
                   '--commit']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.mgr.log.info('updated period')


class RbdMirrorService(CephService):
    TYPE = 'rbd-mirror'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id),
            'caps': ['mon', 'profile rbd-mirror',
                     'osd', 'profile rbd'],
        })

        daemon_spec.keyring = keyring

        return daemon_spec


class CrashService(CephService):
    TYPE = 'crash'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id, host=host),
            'caps': ['mon', 'profile crash',
                     'mgr', 'profile crash'],
        })

        daemon_spec.keyring = keyring

        return daemon_spec


class CephadmExporterConfig:
    required_keys = ['crt', 'key', 'token', 'port']
    DEFAULT_PORT = '9443'

    def __init__(self, mgr, crt="", key="", token="", port=""):
        # type: (CephadmOrchestrator, str, str, str, str) -> None
        self.mgr = mgr
        self.crt = crt
        self.key = key
        self.token = token
        self.port = port

    @property
    def ready(self) -> bool:
        return all([self.crt, self.key, self.token, self.port])

    def load_from_store(self) -> None:
        cfg = self.mgr._get_exporter_config()

        assert isinstance(cfg, dict)
        self.crt = cfg.get('crt', "")
        self.key = cfg.get('key', "")
        self.token = cfg.get('token', "")
        self.port = cfg.get('port', "")

    def load_from_json(self, json_str: str) -> Tuple[int, str]:
        try:
            cfg = json.loads(json_str)
        except ValueError:
            return 1, "Invalid JSON provided - unable to load"

        if not all([k in cfg for k in CephadmExporterConfig.required_keys]):
            return 1, "JSON file must contain crt, key, token and port"

        self.crt = cfg.get('crt')
        self.key = cfg.get('key')
        self.token = cfg.get('token')
        self.port = cfg.get('port')

        return 0, ""

    def validate_config(self) -> Tuple[int, str]:
        if not self.ready:
            return 1, "Incomplete configuration. cephadm-exporter needs crt, key, token and port to be set"

        for check in [self._validate_tls, self._validate_token, self._validate_port]:
            rc, reason = check()
            if rc:
                return 1, reason

        return 0, ""

    def _validate_tls(self) -> Tuple[int, str]:

        try:
            verify_tls(self.crt, self.key)
        except ServerConfigException as e:
            return 1, str(e)

        return 0, ""

    def _validate_token(self) -> Tuple[int, str]:
        if not isinstance(self.token, str):
            return 1, "token must be a string"
        if len(self.token) < 8:
            return 1, "Token must be a string of at least 8 chars in length"

        return 0, ""

    def _validate_port(self) -> Tuple[int, str]:
        try:
            p = int(str(self.port))
            if p <= 1024:
                raise ValueError
        except ValueError:
            return 1, "Port must be a integer (>1024)"

        return 0, ""


class CephadmExporter(CephadmService):
    TYPE = 'cephadm-exporter'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type

        cfg = CephadmExporterConfig(self.mgr)
        cfg.load_from_store()

        if cfg.ready:
            rc, reason = cfg.validate_config()
            if rc:
                raise OrchestratorError(reason)
        else:
            logger.info(
                "Incomplete/Missing configuration, applying defaults")
            self.mgr._set_exporter_defaults()
            cfg.load_from_store()

        if not daemon_spec.ports:
            daemon_spec.ports = [int(cfg.port)]

        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonSpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec
        deps: List[str] = []

        cfg = CephadmExporterConfig(self.mgr)
        cfg.load_from_store()

        if cfg.ready:
            rc, reason = cfg.validate_config()
            if rc:
                raise OrchestratorError(reason)
        else:
            logger.info("Using default configuration for cephadm-exporter")
            self.mgr._set_exporter_defaults()
            cfg.load_from_store()

        config = {
            "crt": cfg.crt,
            "key": cfg.key,
            "token": cfg.token
        }
        return config, deps

    def purge(self) -> None:
        logger.info("Purging cephadm-exporter settings from mon K/V store")
        self.mgr._clear_exporter_config_settings()
