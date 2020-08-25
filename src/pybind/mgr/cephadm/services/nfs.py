import logging
from typing import TYPE_CHECKING, Dict, Optional, Tuple, Any, List, Set, cast

from ceph.deployment.service_spec import NFSServiceSpec
import rados

from orchestrator import OrchestratorError, DaemonDescription

from cephadm import utils
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonSpec

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class NFSService(CephadmService):
    TYPE = 'nfs'

    def config(self, spec: NFSServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)

    def create(self, daemon_spec: CephadmDaemonSpec[NFSServiceSpec]) -> str:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = daemon_spec.spec

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))
        return self.mgr._create_daemon(daemon_spec)

    def generate_config(self, daemon_spec: CephadmDaemonSpec[NFSServiceSpec]) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        daemon_type = daemon_spec.daemon_type
        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = daemon_spec.spec

        deps: List[str] = []

        # create the keyring
        user = f'{daemon_type}.{daemon_id}'
        entity = utils.name_to_config_section(user)
        keyring = self.get_or_create_keyring(entity)

        # update the caps after get-or-create, the keyring might already exist!
        self.update_keyring_caps(entity, spec)

        # create the rados config object
        self.create_rados_config_obj(spec)

        # generate the ganesha config
        def get_ganesha_conf() -> str:
            context = dict(user=user,
                           nodeid=daemon_spec.name(),
                           pool=spec.pool,
                           namespace=spec.namespace if spec.namespace else '',
                           url=spec.rados_config_location())
            return self.mgr.template.render('services/nfs/ganesha.conf.j2', context)

        # generate the cephadm config json
        def get_cephadm_config() -> Dict[str, Any]:
            config: Dict[str, Any] = {}
            config['pool'] = spec.pool
            if spec.namespace:
                config['namespace'] = spec.namespace
            config['userid'] = user
            config['extra_args'] = ['-N', 'NIV_EVENT']
            config['files'] = {
                'ganesha.conf': get_ganesha_conf(),
            }
            config.update(
                self.mgr._get_config_and_keyring(
                    daemon_type, daemon_id,
                    keyring=keyring,
                    host=host
                )
            )
            logger.debug('Generated cephadm config-json: %s' % config)
            return config

        return get_cephadm_config(), deps

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]):

        def get_set_cmd_dicts(out: str) -> List[dict]:
            locations: Set[str] = set()
            for dd in daemon_descrs:
                spec = cast(NFSServiceSpec,
                            self.mgr.spec_store.specs.get(dd.service_name(), None))
                if not spec or not spec.service_id:
                    logger.warning('No ServiceSpec or service_id found for %s', dd)
                    continue
                location = '{}:{}'.format(spec.service_id, spec.pool)
                if spec.namespace:
                    location = '{}/{}'.format(location, spec.namespace)
                locations.add(location)
            new_value = ','.join(locations)
            if new_value and new_value != out:
                return [{'prefix': 'dashboard set-ganesha-clusters-rados-pool-namespace',
                         'value': new_value}]
            return []

        self._check_and_set_dashboard(
            service_name='Ganesha',
            get_cmd='dashboard get-ganesha-clusters-rados-pool-namespace',
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def get_or_create_keyring(self, entity: str) -> str:
        logger.info('Create keyring: %s' % entity)

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': entity,
        })

        return keyring

    def update_keyring_caps(self, entity: str, spec: NFSServiceSpec) -> None:
        osd_caps = 'allow rw pool=%s' % (spec.pool)
        if spec.namespace:
            osd_caps = '%s namespace=%s' % (osd_caps, spec.namespace)

        logger.info('Updating keyring caps: %s' % entity)
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'auth caps',
            'entity': entity,
            'caps': ['mon', 'allow r',
                     'osd', osd_caps],
        })

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
            except rados.ObjectNotFound as e:
                exists = False

            if exists and not clobber:
                # Assume an existing config
                logger.info('Rados config object exists: %s' % obj)
            else:
                # Create an empty config object
                logger.info('Creating rados config object: %s' % obj)
                ioctx.write_full(obj, ''.encode('utf-8'))
