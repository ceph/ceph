import re

from typing import Dict, Optional, Tuple, TYPE_CHECKING, Union


GLOBAL_POOL_KEY = (None, None)


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


def get_rbd_pools(module: 'Module') -> Dict[int, str]:
    osd_map = module.get('osd_map')
    return {pool['pool']: pool['pool_name'] for pool in osd_map['pools']
            if 'rbd' in pool.get('application_metadata', {})}
