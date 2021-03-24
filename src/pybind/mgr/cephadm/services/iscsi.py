import errno
import json
import logging
from typing import List, cast, Optional
from ipaddress import ip_address, IPv6Address

from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import IscsiServiceSpec

from orchestrator import DaemonDescription, DaemonDescriptionStatus
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class IscsiService(CephService):
    TYPE = 'iscsi'

    def config(self, spec: IscsiServiceSpec, daemon_id: str) -> None:  # type: ignore
        assert self.TYPE == spec.service_type
        assert spec.pool
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(IscsiServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        igw_id = daemon_spec.daemon_id

        keyring = self.get_keyring_with_caps(self.get_auth_entity(igw_id),
                                             ['mon', 'profile rbd, '
                                              'allow command "osd blocklist", '
                                              'allow command "config-key get" with "key" prefix "iscsi/"',
                                              'mgr', 'allow command "service status"',
                                              'osd', 'allow rwx'])

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

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        def get_set_cmd_dicts(out: str) -> List[dict]:
            gateways = json.loads(out)['gateways']
            cmd_dicts = []
            # TODO: fail, if we don't have a spec
            spec = cast(IscsiServiceSpec,
                        self.mgr.spec_store.all_specs.get(daemon_descrs[0].service_name(), None))
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
                assert dd.hostname is not None
                # todo: this can fail:
                spec = cast(IscsiServiceSpec,
                            self.mgr.spec_store.all_specs.get(dd.service_name(), None))
                if not spec:
                    logger.warning('No ServiceSpec found for %s', dd)
                    continue
                ip = utils.resolve_ip(dd.hostname)
                # IPv6 URL encoding requires square brackets enclosing the ip
                if type(ip_address(ip)) is IPv6Address:
                    ip = f'[{ip}]'
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

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 iscsi, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Iscsi', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nfs daemon. make sure none are down
        warn_message = (
            'ALERT: 1 iscsi daemon is already down. Please bring it back up before stopping this one')
        iscsi_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        for i in iscsi_daemons:
            if i.status != DaemonDescriptionStatus.running:
                return HandleCommandResult(-errno.EBUSY, '', warn_message)

        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        warn_message = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, warn_message, '')
