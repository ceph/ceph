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
        assert spec.pool
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
                            'allow command "config-key get" with "key" prefix "iscsi/"',
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
            spec = cast(IscsiServiceSpec,
                        self.mgr.spec_store.specs.get(daemon_descrs[0].service_name(), None))
            if spec.api_secure and spec.ssl_cert and spec.ssl_key:
                cmd_dicts.append({
                    'prefix': 'dashboard set-iscsi-api-ssl-verification',
                    'value': "false"
                })
            else:
                cmd_dicts.append({
                    'prefix': 'dashboard set-iscsi-api-ssl-verification',
                    'value': "true"
                })
            for dd in daemon_descrs:
                spec = cast(IscsiServiceSpec,
                            self.mgr.spec_store.specs.get(dd.service_name(), None))
                if not spec:
                    logger.warning('No ServiceSpec found for %s', dd)
                    continue
                ip = utils.resolve_ip(dd.hostname)
                protocol = "http"
                if spec.api_secure and spec.ssl_cert and spec.ssl_key:
                    protocol = "https"
                service_url = '{}://{}:{}@{}:{}'.format(
                    protocol, spec.api_user or 'admin', spec.api_password or 'admin', ip, spec.api_port or '5000')
                gw = gateways.get(dd.hostname)
                if not gw or gw['service_url'] != service_url:
                    safe_service_url = '{}://{}:{}@{}:{}'.format(
                        protocol, '<api-user>', '<api-password>', ip, spec.api_port or '5000')
                    logger.info('Adding iSCSI gateway %s to Dashboard', safe_service_url)
                    cmd_dicts.append({
                        'prefix': 'dashboard iscsi-gateway-add',
                        'inbuf': service_url,
                        'name': dd.hostname
                    })
            return cmd_dicts

        self._check_and_set_dashboard(
            service_name='iSCSI',
            get_cmd='dashboard iscsi-gateway-list',
            get_set_cmd_dicts=get_set_cmd_dicts
        )
