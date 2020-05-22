import logging
from typing import TYPE_CHECKING, List

from mgr_module import MonCommandFailed

from ceph.deployment.service_spec import ServiceSpec, RGWSpec
from orchestrator import OrchestratorError, DaemonDescription
from cephadm import utils

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CephadmService:
    """
    Base class for service types. Often providing a create() and config() fn.
    """
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]):
        """The post actions needed to be done after daemons are checked"""
        raise NotImplementedError()

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        raise NotImplementedError()

    def _inventory_get_addr(self, hostname: str):
        """Get a host's address with its hostname."""
        return self.mgr.inventory.get_addr(hostname)

    def _set_service_url_on_dashboard(self,
                                      service_name: str,
                                      get_mon_cmd: str,
                                      set_mon_cmd: str,
                                      service_url: str):
        """A helper to get and set service_url via Dashboard's MON command."""
        try:
            _, out, _ = self.mgr.check_mon_command({
                'prefix': get_mon_cmd
            })
        except MonCommandFailed as e:
            logger.warning('Failed to get service URL for %s: %s', service_name, e)
            return
        if out.strip() != service_url:
            try:
                logger.info(
                    'Setting service URL %s for %s in the Dashboard', service_url, service_name)
                _, out, _ = self.mgr.check_mon_command({
                    'prefix': set_mon_cmd,
                    'value': service_url,
                })
            except MonCommandFailed as e:
                logger.warning('Failed to set service URL %s for %s in the Dashboard: %s',
                               service_url, service_name, e)


class MonService(CephadmService):
    def create(self, name, host, network):
        """
        Create a new monitor on the given host.
        """
        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        extra_config = '[mon.%s]\n' % name
        if network:
            # infer whether this is a CIDR network, addrvec, or plain IP
            if '/' in network:
                extra_config += 'public network = %s\n' % network
            elif network.startswith('[v') and network.endswith(']'):
                extra_config += 'public addrv = %s\n' % network
            elif ':' not in network:
                extra_config += 'public addr = %s\n' % network
            else:
                raise OrchestratorError('Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)
        else:
            # try to get the public_network from the config
            ret, network, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            network = network.strip() # type: ignore
            if not network:
                raise OrchestratorError('Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP')
            if '/' not in network:
                raise OrchestratorError('public_network is set but does not look like a CIDR network: \'%s\'' % network)
            extra_config += 'public network = %s\n' % network

        return self.mgr._create_daemon('mon', name, host,
                                       keyring=keyring,
                                       extra_config={'config': extra_config})


class MgrService(CephadmService):
    def create(self, mgr_id, host):
        """
        Create a new manager instance on a host.
        """
        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mgr.%s' % mgr_id,
            'caps': ['mon', 'profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        return self.mgr._create_daemon('mgr', mgr_id, host, keyring=keyring)


class MdsService(CephadmService):
    def config(self, spec: ServiceSpec):
        # ensure mds_join_fs is set for these daemons
        assert spec.service_id
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.service_id,
            'name': 'mds_join_fs',
            'value': spec.service_id,
        })

    def create(self, mds_id, host) -> str:
        # get mgr. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mds.' + mds_id,
            'caps': ['mon', 'profile mds',
                     'osd', 'allow rwx',
                     'mds', 'allow'],
        })
        return self.mgr._create_daemon('mds', mds_id, host, keyring=keyring)


class RgwService(CephadmService):
    def config(self, spec: RGWSpec):
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
            else:
                cert_data = spec.rgw_frontend_ssl_certificate
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.crt',
                'val': cert_data,
            })

        if spec.rgw_frontend_ssl_key:
            if isinstance(spec.rgw_frontend_ssl_key, list):
                key_data = '\n'.join(spec.rgw_frontend_ssl_key)
            else:
                key_data = spec.rgw_frontend_ssl_key  # type: ignore
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{spec.rgw_realm}/{spec.rgw_zone}.key',
                'val': key_data,
            })

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def create(self, rgw_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': f"{utils.name_to_config_section('rgw')}.{rgw_id}",
            'caps': ['mon', 'allow *',
                     'mgr', 'allow rw',
                     'osd', 'allow rwx'],
        })
        return self.mgr._create_daemon('rgw', rgw_id, host, keyring=keyring)


class RbdMirrorService(CephadmService):
    def create(self, daemon_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rbd-mirror.' + daemon_id,
            'caps': ['mon', 'profile rbd-mirror',
                     'osd', 'profile rbd'],
        })
        return self.mgr._create_daemon('rbd-mirror', daemon_id, host,
                                       keyring=keyring)


class CrashService(CephadmService):
    def create(self, daemon_id, host) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.crash.' + host,
            'caps': ['mon', 'profile crash',
                     'mgr', 'profile crash'],
        })
        return self.mgr._create_daemon('crash', daemon_id, host, keyring=keyring)
