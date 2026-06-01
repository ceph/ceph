import errno
import json
import logging
from typing import Any, Dict

import rados

from ..dir_map.load import MAX_RETURN
from ..exception import MirrorException
from ..utils import (
    MIRROR_OBJECT_NAME,
    SYNC_STAT_KEY_PREFIX,
    decode_sync_stat_val,
    get_metadata_pool,
    parse_sync_stat_omap_key,
)

log = logging.getLogger(__name__)


def nest_sync_stat_in_metrics(metrics, dir_path, peer_uuid, stat):
    metrics.setdefault(dir_path, {}).setdefault('peer', {})[peer_uuid] = stat


def load_sync_stat_by_keys(ioctx, keys):
    stats: Dict[str, Any] = {}
    if not keys:
        return stats
    try:
        with rados.ReadOpCtx() as read_op:
            it, ret = ioctx.get_omap_vals_by_keys(read_op, keys)
            if ret != 0:
                log.error('failed to read sync stat omap keys')
                raise MirrorException(-errno.EINVAL,
                                      'failed to read sync stat omap keys')
            ioctx.operate_read_op(read_op, MIRROR_OBJECT_NAME)
            for key, val in it:
                try:
                    stats[key] = decode_sync_stat_val(val)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning(f'failed to decode sync stat for key {key}: {e}')
    except rados.Error as e:
        log.error(f'failed to read sync stat omap keys: {e}')
        raise MirrorException(-e.errno, 'failed to read sync stat omap keys')
    return stats


def open_metadata_ioctx(rados_inst, fs_map, filesystem):
    metadata_pool_id = get_metadata_pool(filesystem, fs_map)
    if not metadata_pool_id:
        raise MirrorException(-errno.EINVAL,
                              f'cannot find metadata pool for filesystem {filesystem}')
    try:
        return rados_inst.open_ioctx2(metadata_pool_id)
    except rados.Error as e:
        log.error(f'failed to open metadata pool for {filesystem}: {e}')
        raise MirrorException(-e.errno,
                              f'failed to open metadata pool for {filesystem}')


def load_sync_stat_metrics(ioctx, filesystem, peer_uuid=None):
    metrics: Dict[str, Any] = {}
    prefix = f'{SYNC_STAT_KEY_PREFIX}/{filesystem}/'
    if peer_uuid:
        prefix += f'{peer_uuid}/'
    try:
        with rados.ReadOpCtx() as read_op:
            start = ""
            while True:
                it, ret = ioctx.get_omap_vals(read_op, start, prefix, MAX_RETURN)
                if ret != 0:
                    log.error('failed to read sync stat omap')
                    raise MirrorException(-errno.EINVAL,
                                          'failed to read sync stat omap')
                ioctx.operate_read_op(read_op, MIRROR_OBJECT_NAME)
                omap_vals = dict(it)
                if not omap_vals:
                    break
                for key, val in omap_vals.items():
                    parsed = parse_sync_stat_omap_key(key, filesystem)
                    if not parsed:
                        continue
                    peer, dir_path = parsed
                    if peer_uuid and peer != peer_uuid:
                        continue
                    try:
                        stat = decode_sync_stat_val(val)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        log.warning(f'failed to decode sync stat for key {key}: {e}')
                        continue
                    nest_sync_stat_in_metrics(metrics, dir_path, peer, stat)
                start = omap_vals.popitem()[0]
    except rados.Error as e:
        log.error(f'failed to read sync stat omap: {e}')
        raise MirrorException(-e.errno, 'failed to read sync stat omap')
    return metrics
