import errno
import json
import logging
import re
import socket
import time
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, List, Callable, TypeVar, \
    Optional, Dict, Any, Tuple, NewType, cast

from mgr_module import HandleCommandResult, MonCommandFailed

from ceph.deployment.service_spec import (
    ArgumentList,
    CephExporterSpec,
    GeneralArgList,
    InitContainerSpec,
    MONSpec,
    RGWSpec,
    ServiceSpec,
)
from ceph.deployment.utils import is_ipv6, unwrap_ipv6
from mgr_util import build_url, merge_dicts
from orchestrator import OrchestratorError, DaemonDescription, DaemonDescriptionStatus
from orchestrator._interface import daemon_type_to_service
from cephadm import utils

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)

ServiceSpecs = TypeVar('ServiceSpecs', bound=ServiceSpec)
AuthEntity = NewType('AuthEntity', str)


def get_auth_entity(daemon_type: str, daemon_id: str, host: str = "") -> AuthEntity:
    """
    Map the daemon id to a cephx keyring entity name
    """
    # despite this mapping entity names to daemons, self.TYPE within
    # the CephService class refers to service types, not daemon types
    if daemon_type in ['rgw', 'rbd-mirror', 'cephfs-mirror', 'nfs', "iscsi", 'nvmeof', 'ingress', 'ceph-exporter']:
        return AuthEntity(f'client.{daemon_type}.{daemon_id}')
    elif daemon_type in ['crash', 'agent', 'node-proxy']:
        if host == "":
            raise OrchestratorError(
                f'Host not provided to generate <{daemon_type}> auth entity name')
        return AuthEntity(f'client.{daemon_type}.{host}')
    elif daemon_type == 'mon':
        return AuthEntity('mon.')
    elif daemon_type in ['mgr', 'osd', 'mds']:
        return AuthEntity(f'{daemon_type}.{daemon_id}')
    else:
        raise OrchestratorError(f"unknown daemon type {daemon_type}")


def simplified_keyring(entity: str, contents: str) -> str:
    # strip down keyring
    #  - don't include caps (auth get includes them; get-or-create does not)
    #  - use pending key if present
    key = None
    for line in contents.splitlines():
        if ' = ' not in line:
            continue
        line = line.strip()
        (ls, rs) = line.split(' = ', 1)
        if ls == 'key' and not key:
            key = rs
        if ls == 'pending key':
            key = rs
    keyring = f'[{entity}]\nkey = {key}\n'
    return keyring


class CephadmDaemonDeploySpec:
    # typing.NamedTuple + Generic is broken in py36
    def __init__(self, host: str, daemon_id: str,
                 service_name: str,
                 network: Optional[str] = None,
                 keyring: Optional[str] = None,
                 extra_args: Optional[List[str]] = None,
                 ceph_conf: str = '',
                 extra_files: Optional[Dict[str, Any]] = None,
                 daemon_type: Optional[str] = None,
                 ip: Optional[str] = None,
                 ports: Optional[List[int]] = None,
                 port_ips: Optional[Dict[str, str]] = None,
                 rank: Optional[int] = None,
                 rank_generation: Optional[int] = None,
                 extra_container_args: Optional[ArgumentList] = None,
                 extra_entrypoint_args: Optional[ArgumentList] = None,
                 init_containers: Optional[List[InitContainerSpec]] = None,
                 ):
        """
        A data struction to encapsulate `cephadm deploy ...
        """
        self.host: str = host
        self.daemon_id = daemon_id
        self.service_name = service_name
        daemon_type = daemon_type or (service_name.split('.')[0])
        assert daemon_type is not None
        self.daemon_type: str = daemon_type

        # mons
        self.network = network

        # for run_cephadm.
        self.keyring: Optional[str] = keyring

        # FIXME: finish removing this
        # For run_cephadm. Would be great to have more expressive names.
        # self.extra_args: List[str] = extra_args or []
        assert not extra_args

        self.ceph_conf = ceph_conf
        self.extra_files = extra_files or {}

        # TCP ports used by the daemon
        self.ports: List[int] = ports or []
        # mapping of ports to IP addresses for ports
        # we know we will only bind to on a specific IP.
        # Useful for allowing multiple daemons to bind
        # to the same port on different IPs on the same node
        self.port_ips: Dict[str, str] = port_ips or {}
        self.ip: Optional[str] = ip

        # values to be populated during generate_config calls
        # and then used in _run_cephadm
        self.final_config: Dict[str, Any] = {}
        self.deps: List[str] = []

        self.rank: Optional[int] = rank
        self.rank_generation: Optional[int] = rank_generation

        self.extra_container_args = extra_container_args
        self.extra_entrypoint_args = extra_entrypoint_args
        self.init_containers = init_containers

    def __setattr__(self, name: str, value: Any) -> None:
        if value is not None and name in ('extra_container_args', 'extra_entrypoint_args'):
            for v in value:
                tname = str(type(v))
                if 'ArgumentSpec' not in tname:
                    raise TypeError(f"{name} is not all ArgumentSpec values: {v!r}(is {type(v)} in {value!r}")

        super().__setattr__(name, value)

    def name(self) -> str:
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def entity_name(self) -> str:
        return get_auth_entity(self.daemon_type, self.daemon_id, host=self.host)

    def config_get_files(self) -> Dict[str, Any]:
        files = self.extra_files
        if self.ceph_conf:
            files['config'] = self.ceph_conf

        return files

    @staticmethod
    def from_daemon_description(dd: DaemonDescription) -> 'CephadmDaemonDeploySpec':
        assert dd.hostname
        assert dd.daemon_id
        assert dd.daemon_type
        return CephadmDaemonDeploySpec(
            host=dd.hostname,
            daemon_id=dd.daemon_id,
            daemon_type=dd.daemon_type,
            service_name=dd.service_name(),
            ip=dd.ip,
            ports=dd.ports,
            rank=dd.rank,
            rank_generation=dd.rank_generation,
            extra_container_args=dd.extra_container_args,
            extra_entrypoint_args=dd.extra_entrypoint_args,
        )

    def to_daemon_description(self, status: DaemonDescriptionStatus, status_desc: str) -> DaemonDescription:
        return DaemonDescription(
            daemon_type=self.daemon_type,
            daemon_id=self.daemon_id,
            service_name=self.service_name,
            hostname=self.host,
            status=status,
            status_desc=status_desc,
            ip=self.ip,
            ports=self.ports,
            rank=self.rank,
            rank_generation=self.rank_generation,
            extra_container_args=cast(GeneralArgList, self.extra_container_args),
            extra_entrypoint_args=cast(GeneralArgList, self.extra_entrypoint_args),
        )

    @property
    def extra_args(self) -> List[str]:
        return []


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

    def allow_colo(self) -> bool:
        """
        Return True if multiple daemons of the same type can colocate on
        the same host.
        """
        return False

    def primary_daemon_type(self, spec: Optional[ServiceSpec] = None) -> str:
        """
        This is the type of the primary (usually only) daemon to be deployed.
        """
        return self.TYPE

    def per_host_daemon_type(self, spec: Optional[ServiceSpec] = None) -> Optional[str]:
        """
        If defined, this type of daemon will be deployed once for each host
        containing one or more daemons of the primary type.
        """
        return None

    def ranked(self) -> bool:
        """
        If True, we will assign a stable rank (0, 1, ...) and monotonically increasing
        generation (0, 1, ...) to each daemon we create/deploy.
        """
        return False

    def fence_old_ranks(self,
                        spec: ServiceSpec,
                        rank_map: Dict[int, Dict[int, Optional[str]]],
                        num_ranks: int) -> None:
        assert False

    def make_daemon_spec(
            self,
            host: str,
            daemon_id: str,
            network: str,
            spec: ServiceSpecs,
            daemon_type: Optional[str] = None,
            ports: Optional[List[int]] = None,
            ip: Optional[str] = None,
            rank: Optional[int] = None,
            rank_generation: Optional[int] = None,
    ) -> CephadmDaemonDeploySpec:
        return CephadmDaemonDeploySpec(
            host=host,
            daemon_id=daemon_id,
            service_name=spec.service_name(),
            network=network,
            daemon_type=daemon_type,
            ports=ports,
            ip=ip,
            rank=rank,
            rank_generation=rank_generation,
            extra_container_args=spec.extra_container_args if hasattr(
                spec, 'extra_container_args') else None,
            extra_entrypoint_args=spec.extra_entrypoint_args if hasattr(
                spec, 'extra_entrypoint_args') else None,
            init_containers=getattr(spec, 'init_containers', None),
        )

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        raise NotImplementedError()

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        raise NotImplementedError()

    def config(self, spec: ServiceSpec) -> None:
        """
        Configure the cluster for this service. Only called *once* per
        service apply. Not for every daemon.
        """
        pass

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
        # if this is called for a service type where it hasn't explicitly been
        # defined, return empty Daemon Desc
        return DaemonDescription()

    def get_keyring_with_caps(self, entity: AuthEntity, caps: List[str]) -> str:
        ret, keyring, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': entity,
            'caps': caps,
        })
        if err:
            ret, out, err = self.mgr.mon_command({
                'prefix': 'auth caps',
                'entity': entity,
                'caps': caps,
            })
            if err:
                self.mgr.log.warning(f"Unable to update caps for {entity}")

            # get keyring anyway
            ret, keyring, err = self.mgr.mon_command({
                'prefix': 'auth get',
                'entity': entity,
            })
            if err:
                raise OrchestratorError(f"Unable to fetch keyring for {entity}: {err}")
        return simplified_keyring(entity, keyring)

    def _inventory_get_fqdn(self, hostname: str) -> str:
        """Get a host's FQDN with its hostname.

           If the FQDN can't be resolved, the address from the inventory will
           be returned instead.
        """
        addr = self.mgr.inventory.get_addr(hostname)
        return socket.getfqdn(addr)

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

    def ok_to_stop_osd(
            self,
            osds: List[str],
            known: Optional[List[str]] = None,  # output argument
            force: bool = False) -> HandleCommandResult:
        r = HandleCommandResult(*self.mgr.mon_command({
            'prefix': "osd ok-to-stop",
            'ids': osds,
            'max': 16,
        }))
        j = None
        try:
            j = json.loads(r.stdout)
        except json.decoder.JSONDecodeError:
            self.mgr.log.warning("osd ok-to-stop didn't return structured result")
            raise
        if r.retval:
            return r
        if known is not None and j and j.get('ok_to_stop'):
            self.mgr.log.debug(f"got {j}")
            known.extend([f'osd.{x}' for x in j.get('osds', [])])
        return HandleCommandResult(
            0,
            f'{",".join(["osd.%s" % o for o in osds])} {"is" if len(osds) == 1 else "are"} safe to restart',
            ''
        )

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None    # output argument
    ) -> HandleCommandResult:
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        out = f'It appears safe to stop {",".join(names)}'
        err = f'It is NOT safe to stop {",".join(names)} at this time'

        if self.TYPE not in ['mon', 'osd', 'mds']:
            logger.debug(out)
            return HandleCommandResult(0, out)

        if self.TYPE == 'osd':
            return self.ok_to_stop_osd(daemon_ids, known, force)

        r = HandleCommandResult(*self.mgr.mon_command({
            'prefix': f'{self.TYPE} ok-to-stop',
            'ids': daemon_ids,
        }))

        if r.retval:
            err = f'{err}: {r.stderr}' if r.stderr else err
            logger.debug(err)
            return HandleCommandResult(r.retval, r.stdout, err)

        out = f'{out}: {r.stdout}' if r.stdout else out
        logger.debug(out)
        return HandleCommandResult(r.retval, out, r.stderr)

    def _enough_daemons_to_stop(self, daemon_type: str, daemon_ids: List[str], service: str, low_limit: int, alert: bool = False) -> Tuple[bool, str]:
        # Provides a warning about if it possible or not to stop <n> daemons in a service
        names = [f'{daemon_type}.{d_id}' for d_id in daemon_ids]
        number_of_running_daemons = len(
            [daemon
             for daemon in self.mgr.cache.get_daemons_by_type(daemon_type)
             if daemon.status == DaemonDescriptionStatus.running])
        if (number_of_running_daemons - len(daemon_ids)) >= low_limit:
            return False, f'It is presumed safe to stop {names}'

        num_daemons_left = number_of_running_daemons - len(daemon_ids)

        def plural(count: int) -> str:
            return 'daemon' if count == 1 else 'daemons'

        left_count = "no" if num_daemons_left == 0 else num_daemons_left

        if alert:
            out = (f'ALERT: Cannot stop {names} in {service} service. '
                   f'Not enough remaining {service} daemons. '
                   f'Please deploy at least {low_limit + 1} {service} daemons before stopping {names}. ')
        else:
            out = (f'WARNING: Stopping {len(daemon_ids)} out of {number_of_running_daemons} daemons in {service} service. '
                   f'Service will not be operational with {left_count} {plural(num_daemons_left)} left. '
                   f'At least {low_limit} {plural(low_limit)} must be running to guarantee service. ')
        return True, out

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before the daemon is removed.
        """
        assert daemon.daemon_type is not None
        assert self.TYPE == daemon_type_to_service(daemon.daemon_type)
        logger.debug(f'Pre remove daemon {self.TYPE}.{daemon.daemon_id}')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """
        assert daemon.daemon_type is not None
        assert self.TYPE == daemon_type_to_service(daemon.daemon_type)
        logger.debug(f'Post remove daemon {self.TYPE}.{daemon.daemon_id}')

    def purge(self, service_name: str) -> None:
        """Called to carry out any purge tasks following service removal"""
        logger.debug(f'Purge called for {self.TYPE} - no action taken')


class CephService(CephadmService):
    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
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

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        self.remove_keyring(daemon)

    def get_auth_entity(self, daemon_id: str, host: str = "") -> AuthEntity:
        return get_auth_entity(self.TYPE, daemon_id, host=host)

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
        assert daemon.daemon_id is not None
        assert daemon.hostname is not None
        daemon_id: str = daemon.daemon_id
        host: str = daemon.hostname

        assert daemon.daemon_type != 'mon'

        entity = self.get_auth_entity(daemon_id, host=host)

        logger.info(f'Removing key for {entity}')
        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth rm',
            'entity': entity,
        })


class MonService(CephService):
    TYPE = 'mon'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        """
        Create a new monitor on the given host.
        """
        assert self.TYPE == daemon_spec.daemon_type
        name, _, network = daemon_spec.daemon_id, daemon_spec.host, daemon_spec.network

        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': daemon_spec.entity_name(),
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

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def config(self, spec: ServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        self.set_crush_locations(self.mgr.cache.get_daemons_by_type('mon'), spec)

    def _get_quorum_status(self) -> Dict[Any, Any]:
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception as e:
            raise OrchestratorError(f'failed to parse mon quorum status: {e}')
        return j

    def _check_safe_to_destroy(self, mon_id: str) -> None:
        quorum_status = self._get_quorum_status()
        mons = [m['name'] for m in quorum_status['monmap']['mons']]
        if mon_id not in mons:
            logger.info('Safe to remove mon.%s: not in monmap (%s)' % (
                mon_id, mons))
            return
        new_mons = [m for m in mons if m != mon_id]
        new_quorum = [m for m in quorum_status['quorum_names'] if m != mon_id]
        if len(new_quorum) > len(new_mons) / 2:
            logger.info('Safe to remove mon.%s: new quorum should be %s (from %s)' %
                        (mon_id, new_quorum, new_mons))
            return
        raise OrchestratorError(
            'Removing %s would break mon quorum (new quorum %s, new mons %s)' % (mon_id, new_quorum, new_mons))

    def pre_remove(self, daemon: DaemonDescription) -> None:
        super().pre_remove(daemon)

        assert daemon.daemon_id is not None
        daemon_id: str = daemon.daemon_id
        self._check_safe_to_destroy(daemon_id)

        # remove mon from quorum before we destroy the daemon
        logger.info('Removing monitor %s from monmap...' % daemon_id)
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'mon rm',
            'name': daemon_id,
        })

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        # Do not remove the mon keyring.
        # super().post_remove(daemon)
        pass

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec.final_config, daemon_spec.deps = super().generate_config(daemon_spec)

        # realistically, we expect there to always be a mon spec
        # in a real deployment, but the way teuthology deploys some daemons
        # it's possible there might not be. For that reason we need to
        # verify the service is present in the spec store.
        if daemon_spec.service_name in self.mgr.spec_store:
            mon_spec = cast(MONSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
            if mon_spec.crush_locations:
                if daemon_spec.host in mon_spec.crush_locations:
                    # the --crush-location flag only supports a single bucket=loc pair so
                    # others will have to be handled later. The idea is to set the flag
                    # for the first bucket=loc pair in the list in order to facilitate
                    # replacing a tiebreaker mon (https://docs.ceph.com/en/quincy/rados/operations/stretch-mode/#other-commands)
                    c_loc = mon_spec.crush_locations[daemon_spec.host][0]
                    daemon_spec.final_config['crush_location'] = c_loc

        return daemon_spec.final_config, daemon_spec.deps

    def set_crush_locations(self, daemon_descrs: List[DaemonDescription], spec: ServiceSpec) -> None:
        logger.debug('Setting mon crush locations from spec')
        if not daemon_descrs:
            return
        assert self.TYPE == spec.service_type
        mon_spec = cast(MONSpec, spec)

        if not mon_spec.crush_locations:
            return

        quorum_status = self._get_quorum_status()
        mons_in_monmap = [m['name'] for m in quorum_status['monmap']['mons']]
        for dd in daemon_descrs:
            assert dd.daemon_id is not None
            assert dd.hostname is not None
            if dd.hostname not in mon_spec.crush_locations:
                continue
            if dd.daemon_id not in mons_in_monmap:
                continue
            # expected format for crush_locations from the quorum status is
            # {bucket1=loc1,bucket2=loc2} etc. for the number of bucket=loc pairs
            try:
                current_crush_locs = [m['crush_location'] for m in quorum_status['monmap']['mons'] if m['name'] == dd.daemon_id][0]
            except (KeyError, IndexError) as e:
                logger.warning(f'Failed setting crush location for mon {dd.daemon_id}: {e}\n'
                               'Mon may not have a monmap entry yet. Try re-applying mon spec once mon is confirmed up.')
            desired_crush_locs = '{' + ','.join(mon_spec.crush_locations[dd.hostname]) + '}'
            logger.debug(f'Found spec defined crush locations for mon on {dd.hostname}: {desired_crush_locs}')
            logger.debug(f'Current crush locations for mon on {dd.hostname}: {current_crush_locs}')
            if current_crush_locs != desired_crush_locs:
                logger.info(f'Setting crush location for mon {dd.daemon_id} to {desired_crush_locs}')
                try:
                    ret, out, err = self.mgr.check_mon_command({
                        'prefix': 'mon set_location',
                        'name': dd.daemon_id,
                        'args': mon_spec.crush_locations[dd.hostname]
                    })
                except Exception as e:
                    logger.error(f'Failed setting crush location for mon {dd.daemon_id}: {e}')


class MgrService(CephService):
    TYPE = 'mgr'

    def allow_colo(self) -> bool:
        if self.mgr.get_ceph_option('mgr_standby_modules'):
            # traditional mgr mode: standby daemons' modules listen on
            # ports and redirect to the primary.  we must not schedule
            # multiple mgrs on the same host or else ports will
            # conflict.
            return False
        else:
            # standby daemons do nothing, and therefore port conflicts
            # are not a concern.
            return True

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        """
        Create a new manager instance on a host.
        """
        assert self.TYPE == daemon_spec.daemon_type
        mgr_id, _ = daemon_spec.daemon_id, daemon_spec.host

        # get mgr. key
        keyring = self.get_keyring_with_caps(self.get_auth_entity(mgr_id),
                                             ['mon', 'profile mgr',
                                              'osd', 'allow *',
                                              'mds', 'allow *'])

        # Retrieve ports used by manager modules
        # In the case of the dashboard port and with several manager daemons
        # running in different hosts, it exists the possibility that the
        # user has decided to use different dashboard ports in each server
        # If this is the case then the dashboard port opened will be only the used
        # as default.
        ports = []
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

        daemon_spec.ports.append(self.mgr.service_discovery_port)
        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        for daemon in daemon_descrs:
            assert daemon.daemon_type is not None
            assert daemon.daemon_id is not None
            if self.mgr.daemon_is_self(daemon.daemon_type, daemon.daemon_id):
                return daemon
        # if no active mgr found, return empty Daemon Desc
        return DaemonDescription()

    def fail_over(self) -> None:
        # this has been seen to sometimes transiently fail even when there are multiple
        # mgr daemons. As long as there are multiple known mgr daemons, we should retry.
        class NoStandbyError(OrchestratorError):
            pass
        no_standby_exc = NoStandbyError('Need standby mgr daemon', event_kind_subject=(
            'daemon', 'mgr' + self.mgr.get_mgr_id()))
        for sleep_secs in [2, 8, 15]:
            try:
                if not self.mgr_map_has_standby():
                    raise no_standby_exc
                self.mgr.events.for_daemon('mgr' + self.mgr.get_mgr_id(),
                                           'INFO', 'Failing over to other MGR')
                logger.info('Failing over to other MGR')

                # fail over
                ret, out, err = self.mgr.check_mon_command({
                    'prefix': 'mgr fail',
                    'who': self.mgr.get_mgr_id(),
                })
                return
            except NoStandbyError:
                logger.info(
                    f'Failed to find standby mgr for failover. Retrying in {sleep_secs} seconds')
                time.sleep(sleep_secs)
        raise no_standby_exc

    def mgr_map_has_standby(self) -> bool:
        """
        This is a bit safer than asking our inventory. If the mgr joined the mgr map,
        we know it joined the cluster
        """
        mgr_map = self.mgr.get('mgr_map')
        num = len(mgr_map.get('standbys'))
        return bool(num)

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # ok to stop if there is more than 1 mgr and not trying to stop the active mgr

        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Mgr', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        mgr_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        active = self.get_active_daemon(mgr_daemons).daemon_id
        if active in daemon_ids:
            warn_message = 'ALERT: Cannot stop active Mgr daemon, Please switch active Mgrs with \'ceph mgr fail %s\'' % active
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        return HandleCommandResult(0, warn_message, '')


class MdsService(CephService):
    TYPE = 'mds'

    def allow_colo(self) -> bool:
        return True

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

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        mds_id, _ = daemon_spec.daemon_id, daemon_spec.host

        # get mds. key
        keyring = self.get_keyring_with_caps(self.get_auth_entity(mds_id),
                                             ['mon', 'profile mds',
                                              'osd', 'allow rw tag cephfs *=*',
                                              'mds', 'allow'])
        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

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

    def purge(self, service_name: str) -> None:
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': service_name,
            'name': 'mds_join_fs',
        })


class RgwService(CephService):
    TYPE = 'rgw'

    def allow_colo(self) -> bool:
        return True

    def config(self, spec: RGWSpec) -> None:  # type: ignore
        assert self.TYPE == spec.service_type

        # set rgw_realm rgw_zonegroup and rgw_zone, if present
        if spec.rgw_realm:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_realm',
                'value': spec.rgw_realm,
            })
        if spec.rgw_zonegroup:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_zonegroup',
                'value': spec.rgw_zonegroup,
            })
        if spec.rgw_zone:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_zone',
                'value': spec.rgw_zone,
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
                'key': f'rgw/cert/{spec.service_name()}',
                'val': cert_data,
            })

        if spec.zonegroup_hostnames:
            zg_update_cmd = {
                'prefix': 'rgw zonegroup modify',
                'realm_name': spec.rgw_realm,
                'zonegroup_name': spec.rgw_zonegroup,
                'zone_name': spec.rgw_zone,
                'hostnames': spec.zonegroup_hostnames,
            }
            logger.debug(f'rgw cmd: {zg_update_cmd}')
            ret, out, err = self.mgr.check_mon_command(zg_update_cmd)

        # TODO: fail, if we don't have a spec
        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)
        self.mgr.trigger_connect_dashboard_rgw()

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        rgw_id, _ = daemon_spec.daemon_id, daemon_spec.host
        spec = cast(RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        keyring = self.get_keyring(rgw_id)

        if daemon_spec.ports:
            port = daemon_spec.ports[0]
        else:
            # this is a redeploy of older instance that doesn't have an explicitly
            # assigned port, in which case we can assume there is only 1 per host
            # and it matches the spec.
            port = spec.get_port()

        # configure frontend
        args = []
        ftype = spec.rgw_frontend_type or "beast"
        if ftype == 'beast':
            if spec.ssl:
                if daemon_spec.ip:
                    args.append(
                        f"ssl_endpoint={build_url(host=daemon_spec.ip, port=port).lstrip('/')}")
                else:
                    args.append(f"ssl_port={port}")
                args.append(f"ssl_certificate=config://rgw/cert/{spec.service_name()}")
            else:
                if daemon_spec.ip:
                    args.append(f"endpoint={build_url(host=daemon_spec.ip, port=port).lstrip('/')}")
                else:
                    args.append(f"port={port}")
        elif ftype == 'civetweb':
            if spec.ssl:
                if daemon_spec.ip:
                    # note the 's' suffix on port
                    args.append(f"port={build_url(host=daemon_spec.ip, port=port).lstrip('/')}s")
                else:
                    args.append(f"port={port}s")  # note the 's' suffix on port
                args.append(f"ssl_certificate=config://rgw/cert/{spec.service_name()}")
            else:
                if daemon_spec.ip:
                    args.append(f"port={build_url(host=daemon_spec.ip, port=port).lstrip('/')}")
                else:
                    args.append(f"port={port}")
        else:
            raise OrchestratorError(f'Invalid rgw_frontend_type parameter: {ftype}. Valid values are: beast, civetweb.')

        if spec.rgw_frontend_extra_args is not None:
            args.extend(spec.rgw_frontend_extra_args)

        frontend = f'{ftype} {" ".join(args)}'
        daemon_name = utils.name_to_config_section(daemon_spec.name())

        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': daemon_name,
            'name': 'rgw_frontends',
            'value': frontend
        })

        if spec.rgw_user_counters_cache:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_user_counters_cache',
                'value': 'true',
            })
        if spec.rgw_bucket_counters_cache:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_bucket_counters_cache',
                'value': 'true',
            })

        if spec.rgw_user_counters_cache_size:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_user_counters_cache_size',
                'value': str(spec.rgw_user_counters_cache_size),
            })

        if spec.rgw_bucket_counters_cache_size:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_bucket_counters_cache_size',
                'value': str(spec.rgw_bucket_counters_cache_size),
            })

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def get_keyring(self, rgw_id: str) -> str:
        keyring = self.get_keyring_with_caps(self.get_auth_entity(rgw_id),
                                             ['mon', 'allow *',
                                              'mgr', 'allow rw',
                                              'osd', 'allow rwx tag rgw *=*'])
        return keyring

    def purge(self, service_name: str) -> None:
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(service_name),
            'name': 'rgw_realm',
        })
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(service_name),
            'name': 'rgw_zone',
        })
        self.mgr.check_mon_command({
            'prefix': 'config-key rm',
            'key': f'rgw/cert/{service_name}',
        })
        self.mgr.trigger_connect_dashboard_rgw()

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'rgw_frontends',
        })

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # if load balancer (ingress) is present block if only 1 daemon up otherwise ok
        # if no load balancer, warn if > 1 daemon, block if only 1 daemon
        def ingress_present() -> bool:
            running_ingress_daemons = [
                daemon for daemon in self.mgr.cache.get_daemons_by_type('ingress') if daemon.status == 1]
            running_haproxy_daemons = [
                daemon for daemon in running_ingress_daemons if daemon.daemon_type == 'haproxy']
            running_keepalived_daemons = [
                daemon for daemon in running_ingress_daemons if daemon.daemon_type == 'keepalived']
            # check that there is at least one haproxy and keepalived daemon running
            if running_haproxy_daemons and running_keepalived_daemons:
                return True
            return False

        # if only 1 rgw, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'RGW', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 rgw daemon.
        # Say okay if load balancer present or force flag set
        if ingress_present() or force:
            return HandleCommandResult(0, warn_message, '')

        # if reached here, > 1 RGW daemon, no load balancer and no force flag.
        # Provide warning
        warn_message = "WARNING: Removing RGW daemons can cause clients to lose connectivity. "
        return HandleCommandResult(-errno.EBUSY, '', warn_message)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        self.mgr.trigger_connect_dashboard_rgw()


class RbdMirrorService(CephService):
    TYPE = 'rbd-mirror'

    def allow_colo(self) -> bool:
        return True

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, _ = daemon_spec.daemon_id, daemon_spec.host

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id),
                                             ['mon', 'profile rbd-mirror',
                                              'osd', 'profile rbd'])

        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # if only 1 rbd-mirror, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(
            self.TYPE, daemon_ids, 'Rbdmirror', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')


class CrashService(CephService):
    TYPE = 'crash'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host),
                                             ['mon', 'profile crash',
                                              'mgr', 'profile crash'])

        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec


class CephExporterService(CephService):
    TYPE = 'ceph-exporter'
    DEFAULT_SERVICE_PORT = 9926

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        spec = cast(CephExporterSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_spec.daemon_id),
                                             ['mon', 'profile ceph-exporter',
                                              'mon', 'allow r',
                                              'mgr', 'allow r',
                                              'osd', 'allow r'])
        exporter_config = {}
        if spec.sock_dir:
            exporter_config.update({'sock-dir': spec.sock_dir})
        if spec.port:
            exporter_config.update({'port': f'{spec.port}'})
        if spec.prio_limit is not None:
            exporter_config.update({'prio-limit': f'{spec.prio_limit}'})
        if spec.stats_period:
            exporter_config.update({'stats-period': f'{spec.stats_period}'})

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        daemon_spec.final_config = merge_dicts(daemon_spec.final_config, exporter_config)
        return daemon_spec


class CephfsMirrorService(CephService):
    TYPE = 'cephfs-mirror'

    def config(self, spec: ServiceSpec) -> None:
        # make sure mirroring module is enabled
        mgr_map = self.mgr.get('mgr_map')
        mod_name = 'mirroring'
        if mod_name not in mgr_map.get('services', {}):
            self.mgr.check_mon_command({
                'prefix': 'mgr module enable',
                'module': mod_name
            })
            # we shouldn't get here (mon will tell the mgr to respawn), but no
            # harm done if we do.

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': daemon_spec.entity_name(),
            'caps': ['mon', 'profile cephfs-mirror',
                     'mds', 'allow r',
                     'osd', 'allow rw tag cephfs metadata=*, allow r tag cephfs data=*',
                     'mgr', 'allow r'],
        })

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec


class CephadmAgent(CephService):
    TYPE = 'agent'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        if not self.mgr.http_server.agent:
            raise OrchestratorError('Cannot deploy agent before creating cephadm endpoint')

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host), [])
        daemon_spec.keyring = keyring
        self.mgr.agent_cache.agent_keys[host] = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        agent = self.mgr.http_server.agent
        try:
            assert agent
            assert agent.ssl_certs.get_root_cert()
            assert agent.server_port
        except Exception:
            raise OrchestratorError(
                'Cannot deploy agent daemons until cephadm endpoint has finished generating certs')

        cfg = {'target_ip': self.mgr.get_mgr_ip(),
               'target_port': agent.server_port,
               'refresh_period': self.mgr.agent_refresh_rate,
               'listener_port': self.mgr.agent_starting_port,
               'host': daemon_spec.host,
               'device_enhanced_scan': str(self.mgr.device_enhanced_scan)}

        listener_cert, listener_key = agent.ssl_certs.generate_cert(daemon_spec.host, self.mgr.inventory.get_addr(daemon_spec.host))
        config = {
            'agent.json': json.dumps(cfg),
            'keyring': daemon_spec.keyring,
            'root_cert.pem': agent.ssl_certs.get_root_cert(),
            'listener.crt': listener_cert,
            'listener.key': listener_key,
        }

        return config, sorted([str(self.mgr.get_mgr_ip()), str(agent.server_port),
                               agent.ssl_certs.get_root_cert(),
                               str(self.mgr.get_module_option('device_enhanced_scan'))])
