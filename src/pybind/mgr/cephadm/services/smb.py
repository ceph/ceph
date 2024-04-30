import logging
from typing import Any, Dict, Iterator, List, Tuple, cast

from ceph.deployment.service_spec import ServiceSpec, SMBSpec

from orchestrator import DaemonDescription
from .cephadmservice import (
    AuthEntity,
    CephService,
    CephadmDaemonDeploySpec,
    simplified_keyring,
)

logger = logging.getLogger(__name__)


class SMBService(CephService):
    TYPE = 'smb'

    def config(self, spec: ServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        logger.warning('config is a no-op')

    def prepare_create(
        self, daemon_spec: CephadmDaemonDeploySpec
    ) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        logger.debug('smb prepare_create')
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(
            daemon_spec
        )
        return daemon_spec

    def generate_config(
        self, daemon_spec: CephadmDaemonDeploySpec
    ) -> Tuple[Dict[str, Any], List[str]]:
        logger.debug('smb generate_config')
        assert self.TYPE == daemon_spec.daemon_type
        smb_spec = cast(
            SMBSpec, self.mgr.spec_store[daemon_spec.service_name].spec
        )
        config_blobs: Dict[str, Any] = {}

        config_blobs['cluster_id'] = smb_spec.cluster_id
        config_blobs['features'] = smb_spec.features
        config_blobs['config_uri'] = smb_spec.config_uri
        if smb_spec.join_sources:
            config_blobs['join_sources'] = smb_spec.join_sources
        if smb_spec.user_sources:
            config_blobs['user_sources'] = smb_spec.user_sources
        if smb_spec.custom_dns:
            config_blobs['custom_dns'] = smb_spec.custom_dns
        ceph_users = smb_spec.include_ceph_users or []
        config_blobs.update(
            self._ceph_config_and_keyring_for(
                smb_spec, daemon_spec.daemon_id, ceph_users
            )
        )
        logger.debug('smb generate_config: %r', config_blobs)
        return config_blobs, []

    def config_dashboard(
        self, daemon_descrs: List[DaemonDescription]
    ) -> None:
        # TODO ???
        logger.warning('config_dashboard is a no-op')

    def get_auth_entity(self, daemon_id: str, host: str = "") -> AuthEntity:
        # We want a clear, distinct auth entity for fetching the config versus
        # data path access.
        return AuthEntity(f'client.{self.TYPE}.config.{daemon_id}')

    def _rados_uri_to_pool(self, uri: str) -> str:
        """Given a psudo-uri possibly pointing to an object in a pool, return
        the name of the pool if a rados uri, otherwise return an empty string.
        """
        if not uri.startswith('rados://'):
            return ''
        pool = uri[8:].lstrip('/').split('/')[0]
        logger.debug('extracted pool %r from uri %r', pool, uri)
        return pool

    def _allow_config_key_command(self, name: str) -> str:
        # permit the samba container config access to the mon config key store
        # with keys like smb/config/<cluster_id>/*.
        return f'allow command "config-key get" with "key" prefix "smb/config/{name}/"'

    def _pools_in_spec(self, smb_spec: SMBSpec) -> Iterator[str]:
        uris = [smb_spec.config_uri]
        uris.extend(smb_spec.join_sources or [])
        uris.extend(smb_spec.user_sources or [])
        for uri in uris:
            pool = self._rados_uri_to_pool(uri)
            if pool:
                yield pool

    def _key_for_user(self, entity: str) -> str:
        ret, keyring, err = self.mgr.mon_command({
            'prefix': 'auth get',
            'entity': entity,
        })
        if ret != 0:
            raise ValueError(f'no auth key for user: {entity!r}')
        return '\n' + simplified_keyring(entity, keyring)

    def _ceph_config_and_keyring_for(
        self, smb_spec: SMBSpec, daemon_id: str, ceph_users: List[str]
    ) -> Dict[str, str]:
        ackc = self._allow_config_key_command(smb_spec.cluster_id)
        wanted_caps = ['mon', f'allow r, {ackc}']
        pools = list(self._pools_in_spec(smb_spec))
        if pools:
            wanted_caps.append('osd')
            wanted_caps.append(
                ', '.join(f'allow r pool={pool}' for pool in pools)
            )
        entity = self.get_auth_entity(daemon_id)
        keyring = self.get_keyring_with_caps(entity, wanted_caps)
        # add additional data-path users to the ceph keyring
        for ceph_user in ceph_users:
            keyring += self._key_for_user(ceph_user)
        return {
            'config': self.mgr.get_minimal_ceph_conf(),
            'keyring': keyring,
            'config_auth_entity': entity,
        }
