import json
import logging
from typing import List, cast

from mgr_module import MonCommandFailed
from ceph.deployment.service_spec import IscsiServiceSpec

from orchestrator import DaemonDescription
from .cephadmservice import CephadmService
from .. import utils

logger = logging.getLogger(__name__)


class IscsiService(CephadmService):
    def config(self, spec: IscsiServiceSpec):
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def create(self, igw_id, host, spec) -> str:
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': utils.name_to_auth_entity('iscsi') + '.' + igw_id,
            'caps': ['mon', 'profile rbd, '
                            'allow command "osd blacklist", '
                            'allow command "config-key get" with "key" prefix "iscsi/"',
                     'osd', f'allow rwx pool={spec.pool}'],
        })

        if spec.ssl_cert:
            if isinstance(spec.ssl_cert, list):
                cert_data = '\n'.join(spec.ssl_cert)
            else:
                cert_data = spec.ssl_cert
            ret, out, err = self.mgr.mon_command({
                'prefix': 'config-key set',
                'key': f'iscsi/{utils.name_to_config_section("iscsi")}.{igw_id}/iscsi-gateway.crt',
                'val': cert_data,
            })

        if spec.ssl_key:
            if isinstance(spec.ssl_key, list):
                key_data = '\n'.join(spec.ssl_key)
            else:
                key_data = spec.ssl_key
            ret, out, err = self.mgr.mon_command({
                'prefix': 'config-key set',
                'key': f'iscsi/{utils.name_to_config_section("iscsi")}.{igw_id}/iscsi-gateway.key',
                'val': key_data,
            })

        context = {
            'client_name': '{}.{}'.format(utils.name_to_config_section('iscsi'), igw_id),
            'spec': spec
        }
        igw_conf = self.mgr.template.render('services/iscsi/iscsi-gateway.cfg.j2', context)
        extra_config = {'iscsi-gateway.cfg': igw_conf}
        return self.mgr._create_daemon('iscsi', igw_id, host, keyring=keyring,
                                       extra_config=extra_config)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]):
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
