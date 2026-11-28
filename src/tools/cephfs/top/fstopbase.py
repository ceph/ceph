# Metric definitions and FSTopBase, the curses-free part of cephfs-top
# that builds the per-client metrics and dumps them (--dump/--dumpfs).

import json
import math
import sys
import time

from collections import OrderedDict
from datetime import datetime
from enum import Enum, unique


@unique
class MetricType(Enum):
    METRIC_TYPE_NONE = 0
    METRIC_TYPE_PERCENTAGE = 1
    METRIC_TYPE_LATENCY = 2
    METRIC_TYPE_SIZE = 3
    METRIC_TYPE_STDEV = 4


ITEMS_PAD_LEN = 3
ITEMS_PAD = " " * ITEMS_PAD_LEN

# metadata provided by mgr/stats
FS_TOP_MAIN_WINDOW_COL_CLIENT_ID = "client_id"
FS_TOP_MAIN_WINDOW_COL_MNT_ROOT = "mount_root"
FS_TOP_MAIN_WINDOW_COL_MNTPT_HOST_ADDR = "mount_point@host/addr"

MAIN_WINDOW_TOP_LINE_ITEMS_START = [ITEMS_PAD,
                                    FS_TOP_MAIN_WINDOW_COL_CLIENT_ID,
                                    FS_TOP_MAIN_WINDOW_COL_MNT_ROOT]
MAIN_WINDOW_TOP_LINE_ITEMS_END = [FS_TOP_MAIN_WINDOW_COL_MNTPT_HOST_ADDR]

MAIN_WINDOW_TOP_LINE_METRICS_LEGACY = ["READ_LATENCY",
                                       "WRITE_LATENCY",
                                       "METADATA_LATENCY"
                                       ]

# adjust this map according to stats version and maintain order
# as emitted by mgr/stast
MAIN_WINDOW_TOP_LINE_METRICS = OrderedDict([
    ("CAP_HIT", MetricType.METRIC_TYPE_PERCENTAGE),
    ("READ_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("WRITE_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("METADATA_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("DENTRY_LEASE", MetricType.METRIC_TYPE_PERCENTAGE),
    ("OPENED_FILES", MetricType.METRIC_TYPE_NONE),
    ("PINNED_ICAPS", MetricType.METRIC_TYPE_NONE),
    ("OPENED_INODES", MetricType.METRIC_TYPE_NONE),
    ("READ_IO_SIZES", MetricType.METRIC_TYPE_SIZE),
    ("WRITE_IO_SIZES", MetricType.METRIC_TYPE_SIZE),
    ("AVG_READ_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("STDEV_READ_LATENCY", MetricType.METRIC_TYPE_STDEV),
    ("AVG_WRITE_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("STDEV_WRITE_LATENCY", MetricType.METRIC_TYPE_STDEV),
    ("AVG_METADATA_LATENCY", MetricType.METRIC_TYPE_LATENCY),
    ("STDEV_METADATA_LATENCY", MetricType.METRIC_TYPE_STDEV),
])
MGR_STATS_COUNTERS = list(MAIN_WINDOW_TOP_LINE_METRICS.keys())

CLIENT_METADATA_KEY = "client_metadata"
CLIENT_METADATA_MOUNT_POINT_KEY = "mount_point"
CLIENT_METADATA_MOUNT_ROOT_KEY = "root"
CLIENT_METADATA_IP_KEY = "IP"
CLIENT_METADATA_HOSTNAME_KEY = "hostname"
CLIENT_METADATA_VALID_METRICS_KEY = "valid_metrics"

GLOBAL_METRICS_KEY = "global_metrics"
GLOBAL_COUNTERS_KEY = "global_counters"


def calc_perc(c):
    if c[0] == 0 and c[1] == 0:
        return 0.0
    return round((c[0] / (c[0] + c[1])) * 100, 2)


def calc_lat(c):
    return round(c[0] * 1000 + c[1] / 1000000, 2)


def calc_stdev(c):
    stdev = 0.0
    if c[1] > 1:
        stdev = math.sqrt(c[0] / (c[1] - 1)) / 1000000
    return round(stdev, 2)


# in MB
def calc_size(c):
    return round(c[1] / (1024 * 1024), 2)


# in MB
def calc_avg_size(c):
    if c[0] == 0:
        return 0.0
    return round(c[1] / (c[0] * 1024 * 1024), 2)


# in MB/s
def calc_speed(size, duration):
    if duration == 0:
        return 0.0
    return round(size / (duration * 1024 * 1024), 2)


class FSTopBase(object):
    def __init__(self):
        self.last_time = time.time()
        self.last_read_size = {}
        self.last_write_size = {}
        self.dump_json = {}

    @staticmethod
    def has_metric(metadata, metrics_key):
        return metrics_key in metadata

    @staticmethod
    def has_metrics(metadata, metrics_keys):
        for key in metrics_keys:
            if not FSTopBase.has_metric(metadata, key):
                return False
        return True

    def __build_clients(self, fs, clients_json):
        fs_meta = clients_json.setdefault(fs, {})
        fs_key = self.stats_json[GLOBAL_METRICS_KEY].get(fs, {})
        clients = fs_key.keys()
        for client_id in clients:
            cur_time = time.time()
            duration = cur_time - self.last_time
            self.last_time = cur_time
            client_meta = self.stats_json[CLIENT_METADATA_KEY].get(fs, {}).get(client_id, {})
            for item in MAIN_WINDOW_TOP_LINE_ITEMS_START[1:]:
                if item == FS_TOP_MAIN_WINDOW_COL_CLIENT_ID:
                    client_id_meta = fs_meta.setdefault(client_id.split('.')[1], {})
                elif item == FS_TOP_MAIN_WINDOW_COL_MNT_ROOT:
                    client_id_meta.update({item:
                                           client_meta[CLIENT_METADATA_MOUNT_ROOT_KEY]})
            counters = [m.upper() for m in self.stats_json[GLOBAL_COUNTERS_KEY]]
            metrics = fs_key.get(client_id, {})
            cidx = 0
            for item in counters:
                if item in MAIN_WINDOW_TOP_LINE_METRICS_LEGACY:
                    cidx += 1
                    continue
                m = metrics[cidx]
                key = MGR_STATS_COUNTERS[cidx]
                typ = MAIN_WINDOW_TOP_LINE_METRICS[key]
                if item.lower() in client_meta.get(
                        CLIENT_METADATA_VALID_METRICS_KEY, []):
                    key_name = self.items(item)
                    if typ == MetricType.METRIC_TYPE_PERCENTAGE:
                        client_id_meta.update({f'{key_name}': calc_perc(m)})
                    elif typ == MetricType.METRIC_TYPE_LATENCY:
                        client_id_meta.update({f'{key_name}': calc_lat(m)})
                    elif typ == MetricType.METRIC_TYPE_STDEV:
                        client_id_meta.update({f'{key_name}': calc_stdev(m)})
                    elif typ == MetricType.METRIC_TYPE_SIZE:
                        client_id_meta.update({f'{key_name}': calc_size(m)})
                        # average io sizes
                        client_id_meta.update({f'{self.avg_items(item)}':
                                               calc_avg_size(m)})
                        # io speeds
                        size = 0
                        if key == "READ_IO_SIZES":
                            if m[1] > 0:
                                last_size = self.last_read_size.get(client_id, 0)
                                size = m[1] - last_size
                                self.last_read_size[client_id] = m[1]
                        if key == "WRITE_IO_SIZES":
                            if m[1] > 0:
                                last_size = self.last_write_size.get(client_id, 0)
                                size = m[1] - last_size
                                self.last_write_size[client_id] = m[1]
                        client_id_meta.update({f'{self.speed_items(item)}':
                                               calc_speed(abs(size), duration)})
                    else:
                        # display 0th element from metric tuple
                        client_id_meta.update({f'{key_name}': f'{m[0]}'})
                else:
                    client_id_meta.update({f'{self.items(item)}': "N/A"})
                cidx += 1

            for item in MAIN_WINDOW_TOP_LINE_ITEMS_END:
                if item == FS_TOP_MAIN_WINDOW_COL_MNTPT_HOST_ADDR:
                    if FSTopBase.has_metrics(client_meta,
                                             [CLIENT_METADATA_MOUNT_POINT_KEY,
                                              CLIENT_METADATA_HOSTNAME_KEY,
                                              CLIENT_METADATA_IP_KEY]):
                        mount_point = f'{client_meta[CLIENT_METADATA_MOUNT_POINT_KEY]}'\
                            f'@{client_meta[CLIENT_METADATA_HOSTNAME_KEY]}/'\
                            f'{client_meta[CLIENT_METADATA_IP_KEY]}'
                        client_id_meta.update({item: mount_point})
                    else:
                        client_id_meta.update({item: "N/A"})

    def dump_metrics_to_stdout(self, fs_name=None):
        fs_list = self.get_fs_names()
        if not fs_list:
            sys.stdout.write("No filesystem available\n")
        else:
            self.stats_json = self.perf_stats_query()
            if fs_name:  # --dumpfs
                if fs_name in fs_list:
                    self.__build_clients(fs_name, clients_json=self.dump_json)
                else:
                    sys.stdout.write(f"Filesystem {fs_name} not available\n")
                    return
            else:  # --dump
                num_clients = num_mounts = num_kclients = num_libs = 0
                for fs_name in fs_list:
                    client_metadata = self.stats_json[CLIENT_METADATA_KEY].get(fs_name, {})
                    client_cnt = len(client_metadata)
                    if client_cnt:
                        num_clients = num_clients + client_cnt
                        num_mounts = num_mounts + len(
                            [client for client, metadata in client_metadata.items() if
                             CLIENT_METADATA_MOUNT_POINT_KEY in metadata
                             and metadata[CLIENT_METADATA_MOUNT_POINT_KEY] != 'N/A'])
                        num_kclients = num_kclients + len(
                            [client for client, metadata in client_metadata.items() if
                             "kernel_version" in metadata])
                        num_libs = num_clients - (num_mounts + num_kclients)
                self.dump_json.update({'date': datetime.now().ctime()})
                client_count = self.dump_json.setdefault("client_count", {})
                client_count.update({'total_clients': num_clients, 'fuse': num_mounts,
                                     'kclient': num_kclients, 'libcephfs': num_libs})
                clients_json = self.dump_json.setdefault("filesystems", {})
                for fs in fs_list:
                    self.__build_clients(fs, clients_json)
            sys.stdout.write(json.dumps(self.dump_json))
            sys.stdout.write("\n")
