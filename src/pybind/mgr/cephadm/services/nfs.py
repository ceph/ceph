import errno
import logging
from typing import Dict, Tuple, Any, List, cast, Optional

from mgr_module import HandleCommandResult

from ceph.deployment.service_spec import NFSServiceSpec
import rados

from orchestrator import DaemonDescription

from cephadm.services.cephadmservice import AuthEntity, CephadmDaemonDeploySpec, CephService

logger = logging.getLogger(__name__)


class NFSService(CephService):
    TYPE = 'nfs'

    def config(self, spec: NFSServiceSpec, daemon_id: str) -> None:  # type: ignore
        assert self.TYPE == spec.service_type
        assert spec.pool
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type

        daemon_type = daemon_spec.daemon_type
        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(NFSServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        deps: List[str] = []

        # create the RADOS recovery pool keyring
        rados_user = f'{daemon_type}.{daemon_id}'
        rados_keyring = self.create_keyring(daemon_spec)

        # create the rados config object
        self.create_rados_config_obj(spec)

        # create the RGW keyring
        rgw_user = f'{rados_user}-rgw'
        rgw_keyring = self.create_rgw_keyring(daemon_spec)

        # generate the ganesha config
        def get_ganesha_conf() -> str:
            context = dict(user=rados_user,
                           nodeid=daemon_spec.name(),
                           pool=spec.pool,
                           namespace=spec.namespace if spec.namespace else '',
                           rgw_user=rgw_user,
                           url=spec.rados_config_location())
            return self.mgr.template.render('services/nfs/ganesha.conf.j2', context)

        # generate the cephadm config json
        def get_cephadm_config() -> Dict[str, Any]:
            config: Dict[str, Any] = {}
            config['pool'] = spec.pool
            if spec.namespace:
                config['namespace'] = spec.namespace
            config['userid'] = rados_user
            config['extra_args'] = ['-N', 'NIV_EVENT']
            config['files'] = {
                'ganesha.conf': get_ganesha_conf(),
            }
            config.update(
                self.get_config_and_keyring(
                    daemon_type, daemon_id,
                    keyring=rados_keyring,
                    host=host
                )
            )
            config['rgw'] = {
                'cluster': 'ceph',
                'user': rgw_user,
                'keyring': rgw_keyring,
            }
            logger.debug('Generated cephadm config-json: %s' % config)
            return config

        return get_cephadm_config(), deps

    def create_rados_config_obj(self,
                                spec: NFSServiceSpec,
                                clobber: bool = False) -> None:
        with self.mgr.rados.open_ioctx(spec.pool) as ioctx:
            if spec.namespace:
                ioctx.set_namespace(spec.namespace)

            obj = spec.rados_config_name()
            exists = True
            try:
                ioctx.stat(obj)
            except rados.ObjectNotFound:
                exists = False

            if exists and not clobber:
                # Assume an existing config
                logger.info('Rados config object exists: %s' % obj)
            else:
                # Create an empty config object
                logger.info('Creating rados config object: %s' % obj)
                ioctx.write_full(obj, ''.encode('utf-8'))

    def create_keyring(self, daemon_spec: CephadmDaemonDeploySpec) -> str:
        daemon_id = daemon_spec.daemon_id
        spec = cast(NFSServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        entity: AuthEntity = self.get_auth_entity(daemon_id)

        osd_caps = 'allow rw pool=%s' % (spec.pool)
        if spec.namespace:
            osd_caps = '%s namespace=%s' % (osd_caps, spec.namespace)

        logger.info('Creating key for %s' % entity)
        keyring = self.get_keyring_with_caps(entity,
                                             ['mon', 'allow r',
                                              'osd', osd_caps])

        return keyring

    def create_rgw_keyring(self, daemon_spec: CephadmDaemonDeploySpec) -> str:
        daemon_id = daemon_spec.daemon_id
        entity: AuthEntity = self.get_auth_entity(f'{daemon_id}-rgw')

        logger.info('Creating key for %s' % entity)
        keyring = self.get_keyring_with_caps(entity,
                                             ['mon', 'allow r',
                                              'osd', 'allow rwx tag rgw *=*'])

        return keyring

    def remove_rgw_keyring(self, daemon: DaemonDescription) -> None:
        assert daemon.daemon_id is not None
        daemon_id: str = daemon.daemon_id
        entity: AuthEntity = self.get_auth_entity(f'{daemon_id}-rgw')

        logger.info(f'Removing key for {entity}')
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': entity,
        })

    def post_remove(self, daemon: DaemonDescription) -> None:
        super().post_remove(daemon)
        self.remove_rgw_keyring(daemon)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 nfs, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'NFS', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nfs daemon.
        if force:
            return HandleCommandResult(0, warn_message, '')

        # if reached here, > 1 nfs daemon and no force flag.
        # Provide warning
        warn_message = "WARNING: Removing NFS daemons can cause clients to lose connectivity. "
        return HandleCommandResult(-errno.EBUSY, '', warn_message)
