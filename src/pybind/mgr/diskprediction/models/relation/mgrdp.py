from __future__ import absolute_import

from . import BaseDP


class MGRDpCeph(BaseDP):
    _fields = [
        'fsid', 'health', 'max_osd', 'size',
        'avail_size', 'raw_used', 'raw_used_percent'
    ]


class MGRDpHost(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpMon(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpOsd(BaseDP):
    _fields = [
        'fsid', 'host', '_id', 'uuid', 'up', '_in', 'weight', 'public_addr',
        'cluster_addr', 'heartbeat_back_addr', 'heartbeat_front_addr',
        'state', 'backend_filestore_dev_node', 'backend_filestore_partition_path',
        'ceph_release', 'devices', 'osd_data', 'osd_journal', 'rotational'
    ]


class MGRDpMds(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpPool(BaseDP):
    _fields = [
        'fsid', 'size', 'pool_name', 'pool_id', 'type', 'min_size',
        'pg_num', 'pgp_num', 'created_time', 'used', 'pgids'
    ]


class MGRDpRBD(BaseDP):
    _fields = ['fsid', '_id', 'name', 'pool_name', 'size', 'pgids']


class MGRDpPG(BaseDP):
    _fields = [
        'fsid', 'pgid', 'up_osds', 'acting_osds', 'state',
        'objects', 'degraded', 'misplaced', 'unfound'
    ]


class MGRDpDisk(BaseDP):
    _fields = [
        'fsid', 'osd_id', 'serial_number', 'disk_name'
    ]
