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
    norm_path,
    parse_sync_stat_omap_key,
    sync_stat_omap_key,
)
from .format import (
    default_sync_stat_metrics,
    format_and_order_sync_stat_for_display,
    format_peer_status_metrics,
)

log = logging.getLogger(__name__)

MIRROR_OBJECT_NOT_FOUND_MSG = (
    'cephfs_mirror object not found; enable snapshot mirroring with '
    '"ceph fs snapshot mirror enable <fs_name>"')


def _raise_on_sync_stat_read_error(err):
    if isinstance(err, rados.Error) and err.errno == errno.ENOENT:
        raise MirrorException(-errno.ENOENT, MIRROR_OBJECT_NOT_FOUND_MSG)
    if isinstance(err, rados.Error):
        log.error(f'failed to read sync stat omap: {err}')
        err_no = err.errno if err.errno is not None else errno.EINVAL
        raise MirrorException(-err_no, 'failed to read sync stat omap')
    raise err


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
        _raise_on_sync_stat_read_error(e)
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


def _fill_missing_sync_stat_defaults(metrics, dir_paths, peers, policy,
                                     live_instance_ids, peer_uuid=None):
    default_stat = default_sync_stat_metrics()
    for dir_path in dir_paths:
        for peer in peers:
            if peer_uuid is not None and peer != peer_uuid:
                continue
            if metrics.get(dir_path, {}).get('peer', {}).get(peer) is not None:
                continue
            format_peer_status_metrics(
                metrics, dir_path, peer,
                format_and_order_sync_stat_for_display(
                    default_stat, policy, dir_path, live_instance_ids))


def load_sync_stat_metrics(ioctx, filesystem, peer_uuid=None, policy=None,
                           live_instance_ids=None, peers=None):
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
                    format_peer_status_metrics(
                        metrics, dir_path, peer,
                        format_and_order_sync_stat_for_display(
                            stat, policy, dir_path, live_instance_ids))
                start = omap_vals.popitem()[0]
    except rados.Error as e:
        _raise_on_sync_stat_read_error(e)
    if policy is not None and peers:
        with policy.lock:
            dir_paths = list(policy.dir_states.keys())
        _fill_missing_sync_stat_defaults(
            metrics, dir_paths, peers, policy, live_instance_ids, peer_uuid)
    return metrics


def fetch_sync_stat_metrics(ioctx, filesystem, peers, mirrored_dir_path,
                            peer_uuid, policy=None, live_instance_ids=None):
    if mirrored_dir_path:
        dir_path = norm_path(mirrored_dir_path)
        keys = [sync_stat_omap_key(filesystem, peer, dir_path) for peer in peers]
        omap_stats = load_sync_stat_by_keys(ioctx, keys)
        metrics: Dict[str, Any] = {}
        default_stat = default_sync_stat_metrics()
        for peer in peers:
            omap_key = sync_stat_omap_key(filesystem, peer, dir_path)
            stat = omap_stats.get(omap_key, default_stat)
            format_peer_status_metrics(
                metrics, dir_path, peer,
                format_and_order_sync_stat_for_display(
                    stat, policy, dir_path, live_instance_ids))
        return metrics, False, dir_path

    metrics = load_sync_stat_metrics(
        ioctx, filesystem, peer_uuid, policy, live_instance_ids)
    return metrics, True, None
