groups = {
    'df_cluster': {
        'defaults': {'type': 'gauge'},
        'templates': {'desc': 'DF {}', 'name': 'cluster_{}'},
        'metrics': {
            'total_bytes': {},
            'total_objects': {},
            'total_used_bytes': {},
        },
    },
    'df_pool': {
        'defaults': {'labels': ['pool_id'], 'type': 'gauge'},
        'templates': {'desc': 'DF Pool {}', 'name': 'pool_{}'},
        'metrics': {
            'bytes_used': {},
            'dirty': {},
            'max_avail': {},
            'objects': {},
            'quota_bytes': {},
            'quota_objects': {},
            'raw_bytes_used': {},
            'rd': {},
            'rd_bytes': {},
            'wr': {},
            'wr_bytes': {}
        },
    },
    'metadata': {
        'defaults': {'type': 'untyped'},
        'metrics': {
            'disk_occupation': {
                'desc': 'Associate Ceph daemon with disk used',
                'labels': ['instance', 'device', 'ceph_daemon'],
            },
            'health_status': {'desc': 'Cluster health status'},
            'mon_quorum_count': {
                'desc': 'Monitors in quorum',
                'type': 'gauge',
            },
            'osd_metadata': {
                'desc': 'OSD Metadata',
                'labels': ['cluster_addr', 'device_class', 'id',
                           'public_addr'],
            },
            'pool_metadata': {
                'desc': 'POOL Metadata',
                'labels': ['pool_id', 'name'],
            },
        },
    },
    'osd_status': {
        'defaults': {'labels': ['ceph_daemon'], 'type': 'untyped'},
        'templates': {'desc': 'OSD Status {}', 'name': 'osd_{}'},
        'metrics': {
            'in': {},
            'up': {},
            'weight': {},
        },
    },
    'osd_stats': {
        'defaults': {'labels': ['ceph_daemon'], 'type': 'gauge'},
        'templates': {'desc': 'OSD stat {}', 'name': 'osd_{}'},
        'metrics': {
            'apply_latency_ms': {},
            'commit_latency_ms': {},
        },
    },
    'pg_states': {
        'defaults': {'type': 'gauge'},
        'templates': {'desc': 'PG {}', 'name': 'pg_{}'},
        'metrics': {
            'active': {},
            'backfill': {},
            'backfill-toofull': {},
            'clean': {},
            'creating': {},
            'deep': {},
            'degraded': {},
            'down': {},
            'forced-backfill': {},
            'forced-recovery': {},
            'incomplete': {},
            'inconsistent': {},
            'peered': {},
            'peering': {},
            'recovering': {},
            'remapped': {},
            'repair': {},
            'scrubbing': {},
            'stale': {},
            'undersized': {},
            'wait-backfill': {},
        },
    },
}
