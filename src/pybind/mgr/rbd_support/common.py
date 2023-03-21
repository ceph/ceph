import re

from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Union


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

def get_image_timestamps(
    status_list: List[dict], image_id: str
) -> Optional[List[Tuple[int, int]]]:
    try:
        matches = [
            item["remote_statuses"] for item in status_list if item.get("id") == image_id
        ]
        timestamps = []
        for match in matches:
            for status in match:
                desc = status.get("description")
                if desc:
                    local_ts = int(
                        desc.split('"local_snapshot_timestamp":')[1].split(",")[0]
                    )
                    remote_ts = int(
                        desc.split('"remote_snapshot_timestamp":')[1].split(",")[0]
                    )
                    timestamps.append((local_ts, remote_ts))
        return timestamps if timestamps else None
    except Exception as e:
        raise ValueError(f"Error getting timestamps for image {image_id}: {e}")
