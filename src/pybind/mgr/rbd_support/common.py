import re

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING, Union


GLOBAL_POOL_KEY = (None, None)

# pg_pool_t::TYPE_REPLICATED
POOL_TYPE_REPLICATED = 1


class NotAuthorizedError(Exception):
    pass


if TYPE_CHECKING:
    from rbd_support.module import Module


def is_authorized(module: 'Module',
                  pool: Optional[str],
                  namespace: Optional[str]) -> bool:
    return module.is_authorized({"pool": pool or '',
                                 "namespace": namespace or ''})


def authorize_request(module: 'Module',
                      pool: Optional[str],
                      namespace: Optional[str]) -> None:
    if not is_authorized(module, pool, namespace):
        raise NotAuthorizedError("not authorized on pool={}, namespace={}".format(
            pool, namespace))


PoolKeyT = Union[Tuple[str, str], Tuple[None, None]]


def extract_pool_key(pool_spec: Optional[str]) -> PoolKeyT:
    if not pool_spec:
        return GLOBAL_POOL_KEY

    match = re.match(r'^([^/]+)(?:/([^/]+))?$', pool_spec)
    if not match:
        raise ValueError("Invalid pool spec: {}".format(pool_spec))
    return (match.group(1), match.group(2) or '')


def pool_supports_omap(pool: Dict[str, Any]) -> bool:
    # mirrors pg_pool_t::supports_omap(): replicated pools always support
    # omap, erasure coded pools only when created with omap support
    return (pool.get('type') == POOL_TYPE_REPLICATED
            or 'supports_omap' in pool.get('flags_names', '').split(','))


def get_rbd_pools(module: 'Module',
                  include_data_pools: bool = False) -> Dict[int, str]:
    osd_map = module.get('osd_map')
    # RBD keeps image metadata (directory, trash, mirroring, task queue) in
    # omap, so a pool without omap support can only be an RBD data pool.
    # Such pools still carry the 'rbd' application tag, but scanning them
    # for images or schedules only fails with EOPNOTSUPP.
    return {pool['pool']: pool['pool_name'] for pool in osd_map['pools']
            if 'rbd' in pool.get('application_metadata', {})
            and (include_data_pools or pool_supports_omap(pool))}
