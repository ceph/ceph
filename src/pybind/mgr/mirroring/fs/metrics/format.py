def default_sync_stat_metrics():
    return {
        'state': 'idle',
        'snaps_synced': 0,
        'snaps_deleted': 0,
        'snaps_renamed': 0,
    }


# Matches the format_time function used in peer_status
def _format_time(total_seconds_d):
    total_seconds = int(round(total_seconds_d))
    days = total_seconds // 86400
    total_seconds %= 86400
    hours = total_seconds // 3600
    total_seconds %= 3600
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    if days > 0:
        return (f'{days}d {hours:02d}h {minutes:02d}m {seconds:02d}s')
    if hours > 0:
        return f'{hours}h {minutes:02d}m {seconds:02d}s'
    if minutes > 0:
        return f'{minutes}m {seconds:02d}s'
    return f'{seconds}s'


# Matches the format_bytes function used in peer_status
def _format_bytes(bytes_val):
    kib = 1024.0
    mib = kib * 1024.0
    gib = mib * 1024.0
    tib = gib * 1024.0
    pib = tib * 1024.0
    if bytes_val >= pib:
        return f'{bytes_val / pib:.2f} PiB'
    if bytes_val >= tib:
        return f'{bytes_val / tib:.2f} TiB'
    if bytes_val >= gib:
        return f'{bytes_val / gib:.2f} GiB'
    if bytes_val >= mib:
        return f'{bytes_val / mib:.2f} MiB'
    if bytes_val >= kib:
        return f'{bytes_val / kib:.2f} KiB'
    return f'{bytes_val:.2f} B'


def _order_dict(d, keys):
    ordered = {}
    for key in keys:
        if key in d:
            ordered[key] = d[key]
    for key, val in d.items():
        if key not in ordered:
            ordered[key] = val
    return ordered


def _order_current_syncing_snap(snap):
    if not isinstance(snap, dict):
        return snap
    snap = dict(snap)
    crawl = snap.get('crawl')
    if isinstance(crawl, dict):
        snap['crawl'] = _order_dict(crawl, ('state', 'duration'))
    dq_wait = snap.get('datasync_queue_wait')
    if isinstance(dq_wait, dict):
        snap['datasync_queue_wait'] = _order_dict(dq_wait, ('state', 'duration'))
    bytes_obj = snap.get('bytes')
    if isinstance(bytes_obj, dict):
        snap['bytes'] = _order_dict(
            bytes_obj, ('sync_bytes', 'total_bytes', 'sync_percent'))
    files_obj = snap.get('files')
    if isinstance(files_obj, dict):
        snap['files'] = _order_dict(
            files_obj, ('sync_files', 'total_files', 'sync_percent'))
    return _order_dict(
        snap, ('id', 'name', 'sync-mode', 'avg_read_throughput_bytes',
               'avg_write_throughput_bytes', 'crawl', 'datasync_queue_wait',
               'bytes', 'files', 'eta'))


def format_peer_status_metrics(metrics, dir_path, peer_uuid, stat):
    metrics.setdefault(dir_path, {}).setdefault('peer', {})[peer_uuid] = stat


def _mark_sync_stat_stale(stat):
    out = dict(stat)
    out.pop('current_syncing_snap', None)
    out['state'] = 'stale'
    return out


def _apply_stale_sync_metrics(stat, policy=None, dir_path=None,
                              live_instance_ids=None):
    if not isinstance(stat, dict) or policy is None or dir_path is None:
        return stat

    persisted_instance_id = stat.get('_instance_id')
    if persisted_instance_id is None:
        return stat

    persisted_id = str(persisted_instance_id)
    if (live_instance_ids is not None and
            persisted_id not in live_instance_ids):
        return _mark_sync_stat_stale(stat)

    tracked_id = policy.get_tracked_instance_id(dir_path)
    if tracked_id is not None:
        tracked_id = str(tracked_id)
    if (tracked_id != persisted_id and
            stat.get('state') != 'idle'):
        return _mark_sync_stat_stale(stat)
    return stat


# to match the output of peer_status
def format_and_order_sync_stat_for_display(stat, policy=None, dir_path=None,
                                           live_instance_ids=None):
    if not isinstance(stat, dict):
        return stat

    out = dict(_apply_stale_sync_metrics(
        stat, policy, dir_path, live_instance_ids))
    last_synced_snap = out.get('last_synced_snap')
    if isinstance(last_synced_snap, dict):
        snap = dict(last_synced_snap)
        for key in ('crawl_duration', 'datasync_queue_wait_duration',
                    'sync_duration'):
            val = snap.get(key)
            if isinstance(val, (int, float)):
                snap[key] = _format_time(val)
        sync_bytes = snap.get('sync_bytes')
        if isinstance(sync_bytes, (int, float)):
            snap['sync_bytes'] = _format_bytes(sync_bytes)
        sync_time_stamp = snap.get('sync_time_stamp')
        if isinstance(sync_time_stamp, (int, float)):
            snap['sync_time_stamp'] = f'{sync_time_stamp:.6f}s'
        out['last_synced_snap'] = _order_dict(
            snap, ('id', 'name', 'crawl_duration',
                   'datasync_queue_wait_duration', 'sync_duration',
                   'sync_time_stamp', 'sync_bytes', 'sync_files'))

    current_syncing_snap = out.get('current_syncing_snap')
    if isinstance(current_syncing_snap, dict):
        out['current_syncing_snap'] = _order_current_syncing_snap(
            current_syncing_snap)

    out.pop('_instance_id', None)

    return _order_dict(
        out, ('state', 'failure_reason', 'current_syncing_snap',
              'last_synced_snap', 'snaps_synced', 'snaps_deleted',
              'snaps_renamed', 'metrics_updated_at'))
