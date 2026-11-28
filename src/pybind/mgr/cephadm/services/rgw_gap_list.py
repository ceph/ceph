from typing import List, Optional, Dict, Any, cast, TYPE_CHECKING

from ceph.deployment.service_spec import RGWGapListSpec, ServiceSpec
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext
import logging

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


@register_cephadm_service
class RgwGapListService(CephService):
    TYPE = 'rgw-gap-list'

    def allow_colo(self) -> bool:
        # The scanner coordinates through RADOS, so several instances can
        # share a host (count_per_host > 1).
        return True

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host
        spec = cast(RGWGapListSpec, deploy_ctx.service_spec)

        D = RGWGapListSpec  # shorthand for defaults
        data_pool = spec.pool or D.DEFAULT_POOL
        sync_pool = spec.sync_pool or D.DEFAULT_SYNC_POOL
        ns = spec.namespace or D.DEFAULT_NAMESPACE

        # Build OSD caps:
        # - read on each data pool (for aio_stat on rados objects)
        # - read/write on sync pool+namespace (for OMAP coordination objects)
        # - rwx on rgw-tagged pools: the script shells out to radosgw-admin,
        #   which is an admin tool and requires rwx on the RGW pools
        #   (read-only caps fail with EPERM loading the realm/zone)
        osd_caps_parts = []
        for dp in data_pool.split():
            osd_caps_parts.append(f'allow r pool={dp}')
        osd_caps_parts.append(
            f'allow rwx pool={sync_pool} namespace={ns}')
        osd_caps_parts.append('allow rwx tag rgw *=*')
        osd_caps = ', '.join(osd_caps_parts)

        keyring = self.get_keyring_with_caps(
            self.get_auth_entity(daemon_id, host=host),
            ['mon', 'allow r',
             'mgr', 'allow r',
             'osd', osd_caps])

        daemon_spec.keyring = keyring

        # Pass gap-list config to the cephadm binary via the config dict
        gap_list_config: Dict[str, Any] = {}
        gap_list_config['scan_interval'] = spec.scan_interval or D.DEFAULT_SCAN_INTERVAL
        gap_list_config['scan_timeout'] = spec.scan_timeout or D.DEFAULT_SCAN_TIMEOUT
        gap_list_config['max_age'] = (
            spec.max_age if spec.max_age is not None else D.DEFAULT_MAX_AGE)
        gap_list_config['pool'] = data_pool
        gap_list_config['sync_pool'] = sync_pool
        gap_list_config['namespace'] = ns
        gap_list_config['inflight'] = spec.inflight or D.DEFAULT_INFLIGHT
        if spec.bucket_list:
            gap_list_config['bucket_list'] = spec.bucket_list
        if spec.exclude_list:
            gap_list_config['exclude_list'] = spec.exclude_list
        if spec.extra_opts:
            gap_list_config['extra_opts'] = spec.extra_opts

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        daemon_spec.final_config['gap_list'] = gap_list_config
        return daemon_spec

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        # Track spec fields so config changes trigger a reconfig
        deps: List[str] = []
        if spec:
            s = cast(RGWGapListSpec, spec)
            D = RGWGapListSpec  # shorthand for defaults
            deps.append(f'scan_interval:{s.scan_interval or D.DEFAULT_SCAN_INTERVAL}')
            deps.append(f'scan_timeout:{s.scan_timeout or D.DEFAULT_SCAN_TIMEOUT}')
            deps.append(
                f'max_age:{s.max_age if s.max_age is not None else D.DEFAULT_MAX_AGE}')
            deps.append(f'pool:{s.pool or D.DEFAULT_POOL}')
            deps.append(f'sync_pool:{s.sync_pool or D.DEFAULT_SYNC_POOL}')
            deps.append(f'namespace:{s.namespace or D.DEFAULT_NAMESPACE}')
            deps.append(f'inflight:{s.inflight or D.DEFAULT_INFLIGHT}')
            if s.bucket_list:
                deps.append(f'bucket_list:{s.bucket_list}')
            if s.exclude_list:
                deps.append(f'exclude_list:{s.exclude_list}')
            if s.extra_opts:
                deps.append(f'extra_opts:{s.extra_opts}')
        return deps
