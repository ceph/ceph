from mgr_util import CephfsClient, open_filesystem
from .exception import MDSPartException
from enum import IntEnum, unique
import errno
import json
import logging
import datetime
import math

log = logging.getLogger(__name__)

@unique
class MetricType(IntEnum):
    cap_hit                 = 0
    read_latency            = 1
    write_latency           = 2
    metadata_latency        = 3
    dentry_lease            = 4
    opened_files            = 5
    pinned_icaps            = 6
    opened_inodes           = 7
    read_io_sizes           = 8
    write_io_sizes          = 9
    avg_read_latency        = 10
    stdev_read_latency      = 11
    avg_write_latency       = 12
    stdev_write_latency     = 13
    avg_metadata_latency    = 14
    stddev_metadata_latency  = 15

class PerfMetric:
    def __init__(self, mgr):
        log.debug("Init PerfMetric module.")
        self.mgr = mgr
        self.rados = mgr.rados
        self.fs_map = self.mgr.get('fs_map')
        self._verify_stats_module_support()

    def _get_stats(self):
        mgr_cmd = {'prefix': 'fs perf stats', 'format': 'json'}
        try:
            ret, buf, out = self.rados.mon_command(json.dumps(mgr_cmd), b'')
        except Exception as e:
            raise MDSPartException(f'error in \'perf stats\' query: {e}')
        if ret != 0:
            raise MDSPartException(f'error in \'perf stats\' query: {out}')
        return json.loads(buf.decode('utf-8'))

    def _verify_stats_module_support(self):
        mon_cmd = {'prefix': 'mgr module ls', 'format': 'json'}
        try:
            ret, buf, out = self.rados.mon_command(json.dumps(mon_cmd), b'')
        except Exception as e:
            raise MDSPartException(f'error checking \'stats\' module: {e}')
        if ret != 0:
            raise MDSPartException(f'error checking \'stats\' module: {out}')
        if 'stats' not in json.loads(buf.decode('utf-8'))['enabled_modules']:
            raise MDSPartException('\'stats\' module not enabled. Use'
                                 '\'ceph mgr module enable stats\' to enable')

    def _collect_client_metric(self, fs_name, fs_metric, dirpath_list, client_to_dirpath,  metric_type):
        log.debug(f'_collect_client_metric {fs_name} {metric_type}')
        dirpath_perf: dict = { dirpath: {} for dirpath in dirpath_list }
        for client_id, metrics in fs_metric['global_metrics'].get(fs_name, {}).items():
            metric = metrics[metric_type]
            if metric_type == MetricType.avg_metadata_latency:
                value = calc_lat(metric)
            elif metric_type == MetricType.metadata_latency:
                value = calc_lat(metric)
            elif metric_type == MetricType.stddev_metadata_latency:
                value = get_count(metric)

            dirpath = client_to_dirpath.get(client_id, "")
            #log.debug(f'{metric_type.name} {dirpath} {metric[0]} {metric[1]} {value}')
            if len(dirpath):
                dirpath_perf[dirpath][client_id] = value
        return dirpath_perf

    def _make_client_to_dirpath_map(self, fs_name, fs_metric, dirpath_list):
        log.debug(f'_make_client_to_dirpath_map {fs_name}')
        def _convert_root_through_dirpath_list(dirpath_list, target_dirpath):
            log.debug(f'dirpath_list: {dirpath_list}')
            log.debug(f'target_dirpath: {target_dirpath}')
            for dirpath in dirpath_list:
                if dirpath in target_dirpath:
                    return dirpath
            return ""

        client_to_dirpath = {}
        for client_id, metadata in fs_metric['client_metadata'].get(fs_name, {}).items():
            log.debug(f'{client_id}: {metadata}')
            dirpath = _convert_root_through_dirpath_list(dirpath_list, metadata['root'])
            if len(dirpath):
                client_to_dirpath[client_id] = dirpath

        log.debug(f'client_to_dirpath: {client_to_dirpath}')
        return client_to_dirpath

    def collect_avg_metadata_latency(self, fs_name, dirpath_list, fs_metric_types):
        perfs: dict = {}
        fs_metric = self._get_stats()
        if len(fs_metric['global_metrics'].get(fs_name, {})) == 0:
            return perfs
        client_to_dirpath = self._make_client_to_dirpath_map(fs_name, fs_metric, dirpath_list)
        for metric_type in fs_metric_types:
            perfs[metric_type.name] = self._collect_client_metric(fs_name, fs_metric, dirpath_list, client_to_dirpath, metric_type)
        return perfs

# calc_lat is borrowed from src/tools/cephfs/top/cephfs-top
# if the counters change in format/type, then this bit needs to be updated
# convert to ms
def calc_lat(c):
    return round(c[0] * 1000 + c[1] / 1000000, 2)

def calc_stdev(c):
    stdev = 0.0
    if c[1] > 1:
        stdev = math.sqrt(c[0] / (c[1] - 1)) / 1000000
    return round(stdev, 2)

def get_count(c):
    return c[1]

