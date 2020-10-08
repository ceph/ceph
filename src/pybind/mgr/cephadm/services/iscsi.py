import json
import logging
from typing import List, cast

from mgr_module import MonCommandFailed
from ceph.deployment.service_spec import IscsiServiceSpec

from orchestrator import DaemonDescription, OrchestratorError
from .cephadmservice import CephadmDaemonSpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class IscsiService(CephService):
    TYPE = 'iscsi'

    def config(self, spec: IscsiServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def prepare_create(self, daemon_spec: CephadmDaemonSpec[IscsiServiceSpec]) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        spec = daemon_spec.spec
        igw_id = daemon_spec.daemon_id

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(igw_id),
            'caps': ['mon', 'profile rbd, '
                            'allow command "osd blocklist", '
                            f'allow command "config-key get" with "key" prefix "iscsi/"',
                     'osd', 'allow rwx'],
        })

        if spec.ssl_cert:
            if isinstance(spec.ssl_cert, list):
                cert_data = '\n'.join(spec.ssl_cert)
            else:
                cert_data = spec.ssl_cert
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'iscsi/{utils.name_to_config_section("iscsi")}.{igw_id}/iscsi-gateway.crt',
                'val': cert_data,
            })

        if spec.ssl_key:
            if isinstance(spec.ssl_key, list):
                key_data = '\n'.join(spec.ssl_key)
            else:
                key_data = spec.ssl_key
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'iscsi/{utils.name_to_config_section("iscsi")}.{igw_id}/iscsi-gateway.key',
                'val': key_data,
            })

        # set up the mon-store options, they are all global for iscsi (atm)
        config_options_in_mon = [
            # (key, val)
            (f'{ spec.mon_config_prefix }/trusted_ip_list', spec.trusted_ip_list or ''),
            (f'{ spec.mon_config_prefix }/api_port', spec.api_port or ''),
            (f'{ spec.mon_config_prefix }/api_user', spec.api_user or ''),
            (f'{ spec.mon_config_prefix }/api_password', spec.api_password or ''),
            (f'{ spec.mon_config_prefix }/api_secure', spec.api_secure or 'False')]
        for key, val in config_options_in_mon:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': key,
                'val': val,
            })

        context = {
            'client_name': '{}.{}'.format(utils.name_to_config_section('iscsi'), igw_id),
            'spec': spec
        }
        igw_conf = self.mgr.template.render('services/iscsi/iscsi-gateway.cfg.j2', context)

        daemon_spec.keyring = keyring
        daemon_spec.extra_files = {'iscsi-gateway.cfg': igw_conf}

        return daemon_spec

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        def get_set_cmd_dicts(out: str) -> List[dict]:
            gateways = json.loads(out)['gateways']
            cmd_dicts = []
            for dd in daemon_descrs:
                spec = cast(IscsiServiceSpec,
                            self.mgr.spec_store.specs.get(dd.service_name(), None))
                if not spec:
                    logger.warning('No ServiceSpec found for %s', dd)
                    continue
                if not all([spec.api_user, spec.api_password]):
                    reason = 'api_user or api_password is not specified in ServiceSpec'
                    logger.warning(
                        'Unable to add iSCSI gateway to the Dashboard for %s: %s', dd, reason)
                    continue
                host = self._inventory_get_addr(dd.hostname)
                service_url = 'http://{}:{}@{}:{}'.format(
                    spec.api_user, spec.api_password, host, spec.api_port or '5000')
                gw = gateways.get(host)
                if not gw or gw['service_url'] != service_url:
                    logger.info('Adding iSCSI gateway %s to Dashboard', service_url)
                    cmd_dicts.append({
                        'prefix': 'dashboard iscsi-gateway-add',
                        'service_url': service_url,
                        'name': host
                    })
            return cmd_dicts

        self._check_and_set_dashboard(
            service_name='iSCSI',
            get_cmd='dashboard iscsi-gateway-list',
            get_set_cmd_dicts=get_set_cmd_dicts
        )
