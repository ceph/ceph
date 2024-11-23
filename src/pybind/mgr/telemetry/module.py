"""
Telemetry module for ceph-mgr

Collect statistics from Ceph cluster and send this back to the Ceph project
when user has opted-in
"""
import logging
import numbers
import enum
import errno
import hashlib
import json
import rbd
import requests
import uuid
import time
from datetime import datetime, timedelta
from prettytable import PrettyTable
from threading import Event, Lock
from collections import defaultdict
from typing import cast, Any, DefaultDict, Dict, List, Optional, Tuple, TypeVar, TYPE_CHECKING, Union

from mgr_module import CLICommand, CLIReadCommand, MgrModule, Option, OptionValue, ServiceInfoT


ALL_CHANNELS = ['basic', 'ident', 'crash', 'device', 'perf']

LICENSE = 'sharing-1-0'
LICENSE_NAME = 'Community Data License Agreement - Sharing - Version 1.0'
LICENSE_URL = 'https://cdla.io/sharing-1-0/'
NO_SALT_CNT = 0

# Latest revision of the telemetry report.  Bump this each time we make
# *any* change.
REVISION = 3

# History of revisions
# --------------------
#
# Version 1:
#   Mimic and/or nautilus are lumped together here, since
#   we didn't track revisions yet.
#
# Version 2:
#   - added revision tracking, nagging, etc.
#   - added config option changes
#   - added channels
#   - added explicit license acknowledgement to the opt-in process
#
# Version 3:
#   - added device health metrics (i.e., SMART data, minus serial number)
#   - remove crush_rule
#   - added CephFS metadata (how many MDSs, fs features, how many data pools,
#     how much metadata is cached, rfiles, rbytes, rsnapshots)
#   - added more pool metadata (rep vs ec, cache tiering mode, ec profile)
#   - added host count, and counts for hosts with each of (mon, osd, mds, mgr)
#   - whether an OSD cluster network is in use
#   - rbd pool and image count, and rbd mirror mode (pool-level)
#   - rgw daemons, zones, zonegroups; which rgw frontends
#   - crush map stats

class Collection(str, enum.Enum):
    basic_base = 'basic_base'
    device_base = 'device_base'
    crash_base = 'crash_base'
    ident_base = 'ident_base'
    perf_perf = 'perf_perf'
    basic_mds_metadata = 'basic_mds_metadata'
    basic_pool_usage = 'basic_pool_usage'
    basic_usage_by_class = 'basic_usage_by_class'
    basic_rook_v01 = 'basic_rook_v01'
    perf_memory_metrics = 'perf_memory_metrics'
    basic_pool_options_bluestore = 'basic_pool_options_bluestore'
    basic_pool_flags = 'basic_pool_flags'

MODULE_COLLECTION : List[Dict] = [
    {
        "name": Collection.basic_base,
        "description": "Basic information about the cluster (capacity, number and type of daemons, version, etc.)",
        "channel": "basic",
        "nag": False
    },
    {
        "name": Collection.device_base,
        "description": "Information about device health metrics",
        "channel": "device",
        "nag": False
    },
    {
        "name": Collection.crash_base,
        "description": "Information about daemon crashes (daemon type and version, backtrace, etc.)",
        "channel": "crash",
        "nag": False
    },
    {
        "name": Collection.ident_base,
        "description": "User-provided identifying information about the cluster",
        "channel": "ident",
        "nag": False
    },
    {
        "name": Collection.perf_perf,
        "description": "Information about performance counters of the cluster",
        "channel": "perf",
        "nag": True
    },
    {
        "name": Collection.basic_mds_metadata,
        "description": "MDS metadata",
        "channel": "basic",
        "nag": False
    },
    {
        "name": Collection.basic_pool_usage,
        "description": "Default pool application and usage statistics",
        "channel": "basic",
        "nag": False
    },
    {
        "name": Collection.basic_usage_by_class,
        "description": "Default device class usage statistics",
        "channel": "basic",
        "nag": False
    },
    {
        "name": Collection.basic_rook_v01,
        "description": "Basic Rook deployment data",
        "channel": "basic",
        "nag": True
    },
    {
        "name": Collection.perf_memory_metrics,
        "description": "Heap stats and mempools for mon and mds",
        "channel": "perf",
        "nag": False
    },
    {
        "name": Collection.basic_pool_options_bluestore,
        "description": "Per-pool bluestore config options",
        "channel": "basic",
        "nag": False
    },
    {
        "name": Collection.basic_pool_flags,
        "description": "Per-pool flags",
        "channel": "basic",
        "nag": False
    },
]

ROOK_KEYS_BY_COLLECTION : List[Tuple[str, Collection]] = [
        # Note: a key cannot be both a node and a leaf, e.g.
        # "rook/a/b"
        # "rook/a/b/c"
        ("rook/version", Collection.basic_rook_v01),
        ("rook/kubernetes/version", Collection.basic_rook_v01),
        ("rook/csi/version", Collection.basic_rook_v01),
        ("rook/node/count/kubernetes-total", Collection.basic_rook_v01),
        ("rook/node/count/with-ceph-daemons", Collection.basic_rook_v01),
        ("rook/node/count/with-csi-rbd-plugin", Collection.basic_rook_v01),
        ("rook/node/count/with-csi-cephfs-plugin", Collection.basic_rook_v01),
        ("rook/node/count/with-csi-nfs-plugin", Collection.basic_rook_v01),
        ("rook/usage/storage-class/count/total", Collection.basic_rook_v01),
        ("rook/usage/storage-class/count/rbd", Collection.basic_rook_v01),
        ("rook/usage/storage-class/count/cephfs", Collection.basic_rook_v01),
        ("rook/usage/storage-class/count/nfs", Collection.basic_rook_v01),
        ("rook/usage/storage-class/count/bucket", Collection.basic_rook_v01),
        ("rook/cluster/storage/device-set/count/total", Collection.basic_rook_v01),
        ("rook/cluster/storage/device-set/count/portable", Collection.basic_rook_v01),
        ("rook/cluster/storage/device-set/count/non-portable", Collection.basic_rook_v01),
        ("rook/cluster/mon/count", Collection.basic_rook_v01),
        ("rook/cluster/mon/allow-multiple-per-node", Collection.basic_rook_v01),
        ("rook/cluster/mon/max-id", Collection.basic_rook_v01),
        ("rook/cluster/mon/pvc/enabled", Collection.basic_rook_v01),
        ("rook/cluster/mon/stretch/enabled", Collection.basic_rook_v01),
        ("rook/cluster/network/provider", Collection.basic_rook_v01),
        ("rook/cluster/external-mode", Collection.basic_rook_v01),
]

class Module(MgrModule):
    metadata_keys = [
        "arch",
        "ceph_version",
        "os",
        "cpu",
        "kernel_description",
        "kernel_version",
        "distro_description",
        "distro"
    ]

    MODULE_OPTIONS = [
        Option(name='url',
               type='str',
               default='https://telemetry.ceph.com/report'),
        Option(name='device_url',
               type='str',
               default='https://telemetry.ceph.com/device'),
        Option(name='enabled',
               type='bool',
               default=False),
        Option(name='last_opt_revision',
               type='int',
               default=1),
        Option(name='leaderboard',
               type='bool',
               default=False),
        Option(name='leaderboard_description',
               type='str',
               default=None),
        Option(name='description',
               type='str',
               default=None),
        Option(name='contact',
               type='str',
               default=None),
        Option(name='organization',
               type='str',
               default=None),
        Option(name='proxy',
               type='str',
               default=None),
        Option(name='interval',
               type='int',
               default=24,
               min=8),
        Option(name='channel_basic',
               type='bool',
               default=True,
               desc='Share basic cluster information (size, version)'),
        Option(name='channel_ident',
               type='bool',
               default=False,
               desc='Share a user-provided description and/or contact email for the cluster'),
        Option(name='channel_crash',
               type='bool',
               default=True,
               desc='Share metadata about Ceph daemon crashes (version, stack straces, etc)'),
        Option(name='channel_device',
               type='bool',
               default=True,
               desc=('Share device health metrics '
                     '(e.g., SMART data, minus potentially identifying info like serial numbers)')),
        Option(name='channel_perf',
               type='bool',
               default=False,
               desc='Share various performance metrics of a cluster'),
    ]

    @property
    def config_keys(self) -> Dict[str, OptionValue]:
        return dict((o['name'], o.get('default', None)) for o in self.MODULE_OPTIONS)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = False
        self.db_collection: Optional[List[str]] = None
        self.last_opted_in_ceph_version: Optional[int] = None
        self.last_opted_out_ceph_version: Optional[int] = None
        self.last_upload: Optional[int] = None
        self.last_report: Dict[str, Any] = dict()
        self.report_id: Optional[str] = None
        self.salt: Optional[str] = None
        self.get_report_lock = Lock()
        self.config_update_module_option()
        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.url = ''
            self.device_url = ''
            self.enabled = False
            self.last_opt_revision = 0
            self.leaderboard = ''
            self.leaderboard_description = ''
            self.interval = 0
            self.proxy = ''
            self.channel_basic = True
            self.channel_ident = False
            self.channel_crash = True
            self.channel_device = True
            self.channel_perf = False
            self.db_collection = ['basic_base', 'device_base']
            self.last_opted_in_ceph_version = 17
            self.last_opted_out_ceph_version = 0

    def config_update_module_option(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def config_notify(self) -> None:
        self.config_update_module_option()
        # wake up serve() thread
        self.event.set()

    def load(self) -> None:
        last_upload = self.get_store('last_upload', None)
        if last_upload is None:
            self.last_upload = None
        else:
            self.last_upload = int(last_upload)

        report_id = self.get_store('report_id', None)
        if report_id is None:
            self.report_id = str(uuid.uuid4())
            self.set_store('report_id', self.report_id)
        else:
            self.report_id = report_id

        salt = self.get_store('salt', None)
        if salt is None:
            self.salt = str(uuid.uuid4())
            self.set_store('salt', self.salt)
        else:
            self.salt = salt

        self.init_collection()

        last_opted_in_ceph_version = self.get_store('last_opted_in_ceph_version', None)
        if last_opted_in_ceph_version is None:
            self.last_opted_in_ceph_version = None
        else:
            self.last_opted_in_ceph_version = int(last_opted_in_ceph_version)

        last_opted_out_ceph_version = self.get_store('last_opted_out_ceph_version', None)
        if last_opted_out_ceph_version is None:
            self.last_opted_out_ceph_version = None
        else:
            self.last_opted_out_ceph_version = int(last_opted_out_ceph_version)

    def gather_osd_metadata(self,
                            osd_map: Dict[str, List[Dict[str, int]]]) -> Dict[str, Dict[str, int]]:
        keys = ["osd_objectstore", "rotational"]
        keys += self.metadata_keys

        metadata: Dict[str, Dict[str, int]] = dict()
        for key in keys:
            metadata[key] = defaultdict(int)

        for osd in osd_map['osds']:
            res = self.get_metadata('osd', str(osd['osd']))
            if res is None:
                self.log.debug('Could not get metadata for osd.%s' % str(osd['osd']))
                continue
            for k, v in res.items():
                if k not in keys:
                    continue

                metadata[k][v] += 1

        return metadata

    def gather_mon_metadata(self,
                            mon_map: Dict[str, List[Dict[str, str]]]) -> Dict[str, Dict[str, int]]:
        keys = list()
        keys += self.metadata_keys

        metadata: Dict[str, Dict[str, int]] = dict()
        for key in keys:
            metadata[key] = defaultdict(int)

        for mon in mon_map['mons']:
            res = self.get_metadata('mon', mon['name'])
            if res is None:
                self.log.debug('Could not get metadata for mon.%s' % (mon['name']))
                continue
            for k, v in res.items():
                if k not in keys:
                    continue

                metadata[k][v] += 1

        return metadata

    def gather_mds_metadata(self) -> Dict[str, Dict[str, int]]:
        metadata: Dict[str, Dict[str, int]] = dict()

        res = self.get('mds_metadata')  # metadata of *all* mds daemons
        if res is None or not res:
            self.log.debug('Could not get metadata for mds daemons')
            return metadata

        keys = list()
        keys += self.metadata_keys

        for key in keys:
            metadata[key] = defaultdict(int)

        for mds in res.values():
            for k, v in mds.items():
                if k not in keys:
                    continue

                metadata[k][v] += 1

        return metadata

    def gather_crush_info(self) -> Dict[str, Union[int,
                                                   bool,
                                                   List[int],
                                                   Dict[str, int],
                                                   Dict[int, int]]]:
        osdmap = self.get_osdmap()
        crush_raw = osdmap.get_crush()
        crush = crush_raw.dump()

        BucketKeyT = TypeVar('BucketKeyT', int, str)

        def inc(d: Dict[BucketKeyT, int], k: BucketKeyT) -> None:
            if k in d:
                d[k] += 1
            else:
                d[k] = 1

        device_classes: Dict[str, int] = {}
        for dev in crush['devices']:
            inc(device_classes, dev.get('class', ''))

        bucket_algs: Dict[str, int] = {}
        bucket_types: Dict[str, int] = {}
        bucket_sizes: Dict[int, int] = {}
        for bucket in crush['buckets']:
            if '~' in bucket['name']:  # ignore shadow buckets
                continue
            inc(bucket_algs, bucket['alg'])
            inc(bucket_types, bucket['type_id'])
            inc(bucket_sizes, len(bucket['items']))

        return {
            'num_devices': len(crush['devices']),
            'num_types': len(crush['types']),
            'num_buckets': len(crush['buckets']),
            'num_rules': len(crush['rules']),
            'device_classes': list(device_classes.values()),
            'tunables': crush['tunables'],
            'compat_weight_set': '-1' in crush['choose_args'],
            'num_weight_sets': len(crush['choose_args']),
            'bucket_algs': bucket_algs,
            'bucket_sizes': bucket_sizes,
            'bucket_types': bucket_types,
        }

    def gather_configs(self) -> Dict[str, List[str]]:
        # cluster config options
        cluster = set()
        r, outb, outs = self.mon_command({
            'prefix': 'config dump',
            'format': 'json'
        })
        if r != 0:
            return {}
        try:
            dump = json.loads(outb)
        except json.decoder.JSONDecodeError:
            return {}
        for opt in dump:
            name = opt.get('name')
            if name:
                cluster.add(name)
        # daemon-reported options (which may include ceph.conf)
        active = set()
        ls = self.get("modified_config_options")
        for opt in ls.get('options', {}):
            active.add(opt)
        return {
            'cluster_changed': sorted(list(cluster)),
            'active_changed': sorted(list(active)),
        }

    def anonymize_entity_name(self, entity_name:str) -> str:
        if '.' not in entity_name:
            self.log.debug(f"Cannot split entity name ({entity_name}), no '.' is found")
            return entity_name

        (etype, eid) = entity_name.split('.', 1)
        m = hashlib.sha1()
        salt = ''
        if self.salt is not None:
            salt = self.salt
        # avoid asserting that salt exists
        if not self.salt:
            # do not set self.salt to a temp value
            salt = f"no_salt_found_{NO_SALT_CNT}"
            NO_SALT_CNT += 1
            self.log.debug(f"No salt found, created a temp one: {salt}")
        m.update(salt.encode('utf-8'))
        m.update(eid.encode('utf-8'))
        m.update(salt.encode('utf-8'))

        return  etype + '.' + m.hexdigest()

    def get_heap_stats(self) -> Dict[str, dict]:
        result: Dict[str, dict] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        anonymized_daemons = {}
        osd_map = self.get('osd_map')

        # Combine available daemons
        daemons = []
        for osd in osd_map['osds']:
            daemons.append('osd'+'.'+str(osd['osd']))
        # perf_memory_metrics collection (1/2)
        if self.is_enabled_collection(Collection.perf_memory_metrics):
            mon_map = self.get('mon_map')
            mds_metadata = self.get('mds_metadata')
            for mon in mon_map['mons']:
                daemons.append('mon'+'.'+mon['name'])
            for mds in mds_metadata:
                daemons.append('mds'+'.'+mds)

        # Grab output from the "daemon.x heap stats" command
        for daemon in daemons:
            daemon_type, daemon_id = daemon.split('.', 1)
            heap_stats = self.parse_heap_stats(daemon_type, daemon_id)
            if heap_stats:
                if (daemon_type != 'osd'):
                    # Anonymize mon and mds
                    anonymized_daemons[daemon] = self.anonymize_entity_name(daemon)
                    daemon = anonymized_daemons[daemon]
                result[daemon_type][daemon] = heap_stats
            else:
                continue

        if anonymized_daemons:
            # for debugging purposes only, this data is never reported
            self.log.debug('Anonymized daemon mapping for telemetry heap_stats (anonymized: real): {}'.format(anonymized_daemons))
        return result

    def parse_heap_stats(self, daemon_type: str, daemon_id: Any) -> Dict[str, int]:
        parsed_output = {}

        cmd_dict = {
            'prefix': 'heap',
            'heapcmd': 'stats'
        }
        r, outb, outs = self.tell_command(daemon_type, str(daemon_id), cmd_dict)

        if r != 0:
            self.log.error("Invalid command dictionary: {}".format(cmd_dict))
        else:
            if 'tcmalloc heap stats' in outb:
                values = [int(i) for i in outb.split() if i.isdigit()]
                # `categories` must be ordered this way for the correct output to be parsed
                categories = ['use_by_application',
                              'page_heap_freelist',
                              'central_cache_freelist',
                              'transfer_cache_freelist',
                              'thread_cache_freelists',
                              'malloc_metadata',
                              'actual_memory_used',
                              'released_to_os',
                              'virtual_address_space_used',
                              'spans_in_use',
                              'thread_heaps_in_use',
                              'tcmalloc_page_size']
                if len(values) != len(categories):
                    self.log.error('Received unexpected output from {}.{}; ' \
                                   'number of values should match the number' \
                                   'of expected categories:\n values: len={} {} '\
                                   '~ categories: len={} {} ~ outs: {}'.format(daemon_type, daemon_id, len(values), values, len(categories), categories, outs))
                else:
                    parsed_output = dict(zip(categories, values))
            else:
                self.log.error('No heap stats available on {}.{}: {}'.format(daemon_type, daemon_id, outs))
        
        return parsed_output

    def get_mempool(self, mode: str = 'separated') -> Dict[str, dict]:
        result: Dict[str, dict] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        anonymized_daemons = {}
        osd_map = self.get('osd_map')

        # Combine available daemons
        daemons = []
        for osd in osd_map['osds']:
            daemons.append('osd'+'.'+str(osd['osd']))
        # perf_memory_metrics collection (2/2)
        if self.is_enabled_collection(Collection.perf_memory_metrics):
            mon_map = self.get('mon_map')
            mds_metadata = self.get('mds_metadata')
            for mon in mon_map['mons']:
                daemons.append('mon'+'.'+mon['name'])
            for mds in mds_metadata:
                daemons.append('mds'+'.'+mds)

        # Grab output from the "dump_mempools" command
        for daemon in daemons:
            daemon_type, daemon_id = daemon.split('.', 1)
            cmd_dict = {
                'prefix': 'dump_mempools',
                'format': 'json'
            }
            r, outb, outs = self.tell_command(daemon_type, daemon_id, cmd_dict)
            if r != 0:
                self.log.error("Invalid command dictionary: {}".format(cmd_dict))
                continue
            else:
                try:
                    # This is where the mempool will land.
                    dump = json.loads(outb)
                    if mode == 'separated':
                        # Anonymize mon and mds
                        if daemon_type != 'osd':
                            anonymized_daemons[daemon] = self.anonymize_entity_name(daemon)
                            daemon = anonymized_daemons[daemon]
                        result[daemon_type][daemon] = dump['mempool']['by_pool']
                    elif mode == 'aggregated':
                        for mem_type in dump['mempool']['by_pool']:
                            result[daemon_type][mem_type]['bytes'] += dump['mempool']['by_pool'][mem_type]['bytes']
                            result[daemon_type][mem_type]['items'] += dump['mempool']['by_pool'][mem_type]['items']
                    else:
                        self.log.error("Incorrect mode specified in get_mempool: {}".format(mode))
                except (json.decoder.JSONDecodeError, KeyError) as e:
                    self.log.exception("Error caught on {}.{}: {}".format(daemon_type, daemon_id, e))
                    continue

        if anonymized_daemons:
            # for debugging purposes only, this data is never reported
            self.log.debug('Anonymized daemon mapping for telemetry mempool (anonymized: real): {}'.format(anonymized_daemons))

        return result

    def get_osd_histograms(self, mode: str = 'separated') -> List[Dict[str, dict]]:
        # Initialize result dict
        result: Dict[str, dict] = defaultdict(lambda: defaultdict(
                                              lambda: defaultdict(
                                              lambda: defaultdict(
                                              lambda: defaultdict(
                                              lambda: defaultdict(int))))))

        # Get list of osd ids from the metadata
        osd_metadata = self.get('osd_metadata')

        # Grab output from the "osd.x perf histogram dump" command
        for osd_id in osd_metadata:
            cmd_dict = {
                'prefix': 'perf histogram dump',
                'id': str(osd_id),
                'format': 'json'
            }
            r, outb, outs = self.osd_command(cmd_dict)
            # Check for invalid calls
            if r != 0:
                self.log.error("Invalid command dictionary: {}".format(cmd_dict))
                continue
            else:
                try:
                    # This is where the histograms will land if there are any.
                    dump = json.loads(outb)

                    for histogram in dump['osd']:
                        # Log axis information. There are two axes, each represented
                        # as a dictionary. Both dictionaries are contained inside a
                        # list called 'axes'.
                        axes = []
                        for axis in dump['osd'][histogram]['axes']:

                            # This is the dict that contains information for an individual
                            # axis. It will be appended to the 'axes' list at the end.
                            axis_dict: Dict[str, Any] = defaultdict()

                            # Collecting information for buckets, min, name, etc.
                            axis_dict['buckets'] = axis['buckets']
                            axis_dict['min'] = axis['min']
                            axis_dict['name'] = axis['name']
                            axis_dict['quant_size'] = axis['quant_size']
                            axis_dict['scale_type'] = axis['scale_type']

                            # Collecting ranges; placing them in lists to
                            # improve readability later on.
                            ranges = []
                            for _range in axis['ranges']:
                                _max, _min = None, None
                                if 'max' in _range:
                                    _max = _range['max']
                                if 'min' in _range:
                                    _min = _range['min']
                                ranges.append([_min, _max])
                            axis_dict['ranges'] = ranges

                            # Now that 'axis_dict' contains all the appropriate
                            # information for the current axis, append it to the 'axes' list.
                            # There will end up being two axes in the 'axes' list, since the
                            # histograms are 2D.
                            axes.append(axis_dict)

                        # Add the 'axes' list, containing both axes, to result.
                        # At this point, you will see that the name of the key is the string
                        # form of our axes list (str(axes)). This is there so that histograms
                        # with different axis configs will not be combined.
                        # These key names are later dropped when only the values are returned.
                        result[str(axes)][histogram]['axes'] = axes

                        # Collect current values and make sure they are in
                        # integer form.
                        values = []
                        for value_list in dump['osd'][histogram]['values']:
                            values.append([int(v) for v in value_list])

                        if mode == 'separated':
                            if 'osds' not in result[str(axes)][histogram]:
                                result[str(axes)][histogram]['osds'] = []
                            result[str(axes)][histogram]['osds'].append({'osd_id': int(osd_id), 'values': values})

                        elif mode == 'aggregated':
                            # Aggregate values. If 'values' have already been initialized,
                            # we can safely add.
                            if 'values' in result[str(axes)][histogram]:
                                for i in range (0, len(values)):
                                    for j in range (0, len(values[i])):
                                        values[i][j] += result[str(axes)][histogram]['values'][i][j]

                            # Add the values to result.
                            result[str(axes)][histogram]['values'] = values

                            # Update num_combined_osds
                            if 'num_combined_osds' not in result[str(axes)][histogram]:
                                result[str(axes)][histogram]['num_combined_osds'] = 1
                            else:
                                result[str(axes)][histogram]['num_combined_osds'] += 1
                        else:
                            self.log.error('Incorrect mode specified in get_osd_histograms: {}'.format(mode))
                            return list()

                # Sometimes, json errors occur if you give it an empty string.
                # I am also putting in a catch for a KeyError since it could
                # happen where the code is assuming that a key exists in the
                # schema when it doesn't. In either case, we'll handle that
                # by continuing and collecting what we can from other osds.
                except (json.decoder.JSONDecodeError, KeyError) as e:
                    self.log.exception("Error caught on osd.{}: {}".format(osd_id, e))
                    continue

        return list(result.values())

    def get_io_rate(self) -> dict:
        return self.get('io_rate')

    def get_stats_per_pool(self) -> dict:
        result = self.get('pg_dump')['pool_stats']

        # collect application metadata from osd_map
        osd_map = self.get('osd_map')
        application_metadata = {pool['pool']: pool['application_metadata'] for pool in osd_map['pools']}

        # add application to each pool from pg_dump
        for pool in result:
            pool['application'] = []
            # Only include default applications
            for application in application_metadata[pool['poolid']]:
                if application in ['cephfs', 'mgr', 'rbd', 'rgw']:
                    pool['application'].append(application)

        return result

    def get_stats_per_pg(self) -> dict:
        return self.get('pg_dump')['pg_stats']

    def get_rocksdb_stats(self) -> Dict[str, str]:
        # Initalizers
        result: Dict[str, str] = defaultdict()
        version = self.get_rocksdb_version()

        # Update result
        result['version'] = version

        return result

    def gather_crashinfo(self) -> List[Dict[str, str]]:
        crashlist: List[Dict[str, str]] = list()
        errno, crashids, err = self.remote('crash', 'ls')
        if errno:
            return crashlist
        for crashid in crashids.split():
            errno, crashinfo, err = self.remote('crash', 'do_info', crashid)
            if errno:
                continue
            c = json.loads(crashinfo)

            # redact hostname
            del c['utsname_hostname']

            # entity_name might have more than one '.', beware
            (etype, eid) = c.get('entity_name', '').split('.', 1)
            m = hashlib.sha1()
            assert self.salt
            m.update(self.salt.encode('utf-8'))
            m.update(eid.encode('utf-8'))
            m.update(self.salt.encode('utf-8'))
            c['entity_name'] = etype + '.' + m.hexdigest()

            # redact final line of python tracebacks, as the exception
            # payload may contain identifying information
            if 'mgr_module' in c and 'backtrace' in c:
                # backtrace might be empty
                if len(c['backtrace']) > 0:
                    c['backtrace'][-1] = '<redacted>'

            crashlist.append(c)
        return crashlist

    def gather_perf_counters(self, mode: str = 'separated') -> Dict[str, dict]:
        # Extract perf counter data with get_unlabeled_perf_counters(), a method
        # from mgr/mgr_module.py. This method returns a nested dictionary that
        # looks a lot like perf schema, except with some additional fields.
        #
        # Example of output, a snapshot of a mon daemon:
        #   "mon.b": {
        #       "bluestore.kv_flush_lat": {
        #           "count": 2431,
        #           "description": "Average kv_thread flush latency",
        #           "nick": "fl_l",
        #           "priority": 8,
        #           "type": 5,
        #           "units": 1,
        #           "value": 88814109
        #       },
        #   },
        perf_counters = self.get_unlabeled_perf_counters()

        # Initialize 'result' dict
        result: Dict[str, dict] = defaultdict(lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))))

        # 'separated' mode
        anonymized_daemon_dict = {}

        for daemon, perf_counters_by_daemon in perf_counters.items():
            daemon_type = daemon[0:3] # i.e. 'mds', 'osd', 'rgw'

            if mode == 'separated':
                # anonymize individual daemon names except osds
                if (daemon_type != 'osd'):
                    anonymized_daemon = self.anonymize_entity_name(daemon)
                    anonymized_daemon_dict[anonymized_daemon] = daemon
                    daemon = anonymized_daemon

            # Calculate num combined daemon types if in aggregated mode
            if mode == 'aggregated':
                if 'num_combined_daemons' not in result[daemon_type]:
                    result[daemon_type]['num_combined_daemons'] = 1
                else:
                    result[daemon_type]['num_combined_daemons'] += 1

            for collection in perf_counters_by_daemon:
                # Split the collection to avoid redundancy in final report; i.e.:
                #   bluestore.kv_flush_lat, bluestore.kv_final_lat -->
                #   bluestore: kv_flush_lat, kv_final_lat
                col_0, col_1 = collection.split('.')

                # Debug log for empty keys. This initially was a problem for prioritycache
                # perf counters, where the col_0 was empty for certain mon counters:
                #
                # "mon.a": {                  instead of    "mon.a": {
                #      "": {                                     "prioritycache": {
                #        "cache_bytes": {...},                          "cache_bytes": {...},
                #
                # This log is here to detect any future instances of a similar issue.
                if (daemon == "") or (col_0 == "") or (col_1 == ""):
                    self.log.debug("Instance of an empty key: {}{}".format(daemon, collection))

                if mode == 'separated':
                    # Add value to result
                    result[daemon][col_0][col_1]['value'] = \
                            perf_counters_by_daemon[collection]['value']

                    # Check that 'count' exists, as not all counters have a count field.
                    if 'count' in perf_counters_by_daemon[collection]:
                        result[daemon][col_0][col_1]['count'] = \
                                perf_counters_by_daemon[collection]['count']
                elif mode == 'aggregated':
                    # Not every rgw daemon has the same schema. Specifically, each rgw daemon
                    # has a uniquely-named collection that starts off identically (i.e.
                    # "objecter-0x...") then diverges (i.e. "...55f4e778e140.op_rmw").
                    # This bit of code combines these unique counters all under one rgw instance.
                    # Without this check, the schema would remain separeted out in the final report.
                    if col_0[0:11] == "objecter-0x":
                        col_0 = "objecter-0x"

                    # Check that the value can be incremented. In some cases,
                    # the files are of type 'pair' (real-integer-pair, integer-integer pair).
                    # In those cases, the value is a dictionary, and not a number.
                    #   i.e. throttle-msgr_dispatch_throttler-hbserver["wait"]
                    if isinstance(perf_counters_by_daemon[collection]['value'], numbers.Number):
                        result[daemon_type][col_0][col_1]['value'] += \
                                perf_counters_by_daemon[collection]['value']

                    # Check that 'count' exists, as not all counters have a count field.
                    if 'count' in perf_counters_by_daemon[collection]:
                        result[daemon_type][col_0][col_1]['count'] += \
                                perf_counters_by_daemon[collection]['count']
                else:
                    self.log.error('Incorrect mode specified in gather_perf_counters: {}'.format(mode))
                    return {}

        if mode == 'separated':
            # for debugging purposes only, this data is never reported
            self.log.debug('Anonymized daemon mapping for telemetry perf_counters (anonymized: real): {}'.format(anonymized_daemon_dict))

        return result

    def get_active_channels(self) -> List[str]:
        r = []
        if self.channel_basic:
            r.append('basic')
        if self.channel_crash:
            r.append('crash')
        if self.channel_device:
            r.append('device')
        if self.channel_ident:
            r.append('ident')
        if self.channel_perf:
            r.append('perf')
        return r

    def gather_device_report(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        try:
            time_format = self.remote('devicehealth', 'get_time_format')
        except Exception as e:
            self.log.debug('Unable to format time: {}'.format(e))
            return {}
        cutoff = datetime.utcnow() - timedelta(hours=self.interval * 2)
        min_sample = cutoff.strftime(time_format)

        devices = self.get('devices')['devices']
        if not devices:
            self.log.debug('Unable to get device info from the mgr.')
            return {}

        # anon-host-id -> anon-devid -> { timestamp -> record }
        res: Dict[str, Dict[str, Dict[str, str]]] = {}
        for d in devices:
            devid = d['devid']
            try:
                # this is a map of stamp -> {device info}
                m = self.remote('devicehealth', 'get_recent_device_metrics',
                                devid, min_sample)
            except Exception as e:
                self.log.error('Unable to get recent metrics from device with id "{}": {}'.format(devid, e))
                continue

            # anonymize host id
            try:
                host = d['location'][0]['host']
            except (KeyError, IndexError) as e:
                self.log.exception('Unable to get host from device with id "{}": {}'.format(devid, e))
                continue
            anon_host = self.get_store('host-id/%s' % host)
            if not anon_host:
                anon_host = str(uuid.uuid1())
                self.set_store('host-id/%s' % host, anon_host)
            serial = None
            for dev, rep in m.items():
                rep['host_id'] = anon_host
                if serial is None and 'serial_number' in rep:
                    serial = rep['serial_number']

            # anonymize device id
            anon_devid = self.get_store('devid-id/%s' % devid)
            if not anon_devid:
                # ideally devid is 'vendor_model_serial',
                # but can also be 'model_serial', 'serial'
                if '_' in devid:
                    anon_devid = f"{devid.rsplit('_', 1)[0]}_{uuid.uuid1()}"
                else:
                    anon_devid = str(uuid.uuid1())
                self.set_store('devid-id/%s' % devid, anon_devid)
            self.log.info('devid %s / %s, host %s / %s' % (devid, anon_devid,
                                                           host, anon_host))

            # anonymize the smartctl report itself
            if serial:
                m_str = json.dumps(m)
                m = json.loads(m_str.replace(serial, 'deleted'))

            if anon_host not in res:
                res[anon_host] = {}
            res[anon_host][anon_devid] = m
        return res

    def get_latest(self, daemon_type: str, daemon_name: str, stat: str) -> int:
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
            return 0

    def compile_report(self, channels: Optional[List[str]] = None) -> Dict[str, Any]:
        if not channels:
            channels = self.get_active_channels()
        report = {
            'leaderboard': self.leaderboard,
            'leaderboard_description': self.leaderboard_description,
            'report_version': 1,
            'report_timestamp': datetime.utcnow().isoformat(),
            'report_id': self.report_id,
            'channels': channels,
            'channels_available': ALL_CHANNELS,
            'license': LICENSE,
            'collections_available': [c['name'].name for c in MODULE_COLLECTION],
            'collections_opted_in': [c['name'].name for c in MODULE_COLLECTION if self.is_enabled_collection(c['name'])],
        }

        if 'ident' in channels:
            for option in ['description', 'contact', 'organization']:
                report[option] = getattr(self, option)

        if 'basic' in channels:
            mon_map = self.get('mon_map')
            osd_map = self.get('osd_map')
            service_map = self.get('service_map')
            fs_map = self.get('fs_map')
            df = self.get('df')
            df_pools = {pool['id']: pool for pool in df['pools']}

            report['created'] = mon_map['created']

            # mons
            v1_mons = 0
            v2_mons = 0
            ipv4_mons = 0
            ipv6_mons = 0
            for mon in mon_map['mons']:
                for a in mon['public_addrs']['addrvec']:
                    if a['type'] == 'v2':
                        v2_mons += 1
                    elif a['type'] == 'v1':
                        v1_mons += 1
                    if a['addr'].startswith('['):
                        ipv6_mons += 1
                    else:
                        ipv4_mons += 1
            report['mon'] = {
                'count': len(mon_map['mons']),
                'features': mon_map['features'],
                'min_mon_release': mon_map['min_mon_release'],
                'v1_addr_mons': v1_mons,
                'v2_addr_mons': v2_mons,
                'ipv4_addr_mons': ipv4_mons,
                'ipv6_addr_mons': ipv6_mons,
            }

            report['config'] = self.gather_configs()

            # pools

            rbd_num_pools = 0
            rbd_num_images_by_pool = []
            rbd_mirroring_by_pool = []
            num_pg = 0
            report['pools'] = list()
            for pool in osd_map['pools']:
                num_pg += pool['pg_num']
                ec_profile = {}
                if pool['erasure_code_profile']:
                    orig = osd_map['erasure_code_profiles'].get(
                        pool['erasure_code_profile'], {})
                    ec_profile = {
                        k: orig[k] for k in orig.keys()
                        if k in ['k', 'm', 'plugin', 'technique',
                                 'crush-failure-domain', 'l']
                    }
                pool_data = {
                        'pool': pool['pool'],
                        'pg_num': pool['pg_num'],
                        'pgp_num': pool['pg_placement_num'],
                        'size': pool['size'],
                        'min_size': pool['min_size'],
                        'pg_autoscale_mode': pool['pg_autoscale_mode'],
                        'target_max_bytes': pool['target_max_bytes'],
                        'target_max_objects': pool['target_max_objects'],
                        'type': ['', 'replicated', '', 'erasure'][pool['type']],
                        'erasure_code_profile': ec_profile,
                        'cache_mode': pool['cache_mode'],
                    }

                # basic_pool_usage collection
                if self.is_enabled_collection(Collection.basic_pool_usage):
                    pool_data['application'] = []
                    for application in pool['application_metadata']:
                        # Only include default applications
                        if application in ['cephfs', 'mgr', 'rbd', 'rgw']:
                            pool_data['application'].append(application)
                    pool_stats = df_pools[pool['pool']]['stats']
                    pool_data['stats'] = { # filter out kb_used
                                            'avail_raw': pool_stats['avail_raw'],
                                            'bytes_used': pool_stats['bytes_used'],
                                            'compress_bytes_used': pool_stats['compress_bytes_used'],
                                            'compress_under_bytes': pool_stats['compress_under_bytes'],
                                            'data_bytes_used': pool_stats['data_bytes_used'],
                                            'dirty': pool_stats['dirty'],
                                            'max_avail': pool_stats['max_avail'],
                                            'objects': pool_stats['objects'],
                                            'omap_bytes_used': pool_stats['omap_bytes_used'],
                                            'percent_used': pool_stats['percent_used'],
                                            'quota_bytes': pool_stats['quota_bytes'],
                                            'quota_objects': pool_stats['quota_objects'],
                                            'rd': pool_stats['rd'],
                                            'rd_bytes': pool_stats['rd_bytes'],
                                            'stored': pool_stats['stored'],
                                            'stored_data': pool_stats['stored_data'],
                                            'stored_omap': pool_stats['stored_omap'],
                                            'stored_raw': pool_stats['stored_raw'],
                                            'wr': pool_stats['wr'],
                                            'wr_bytes': pool_stats['wr_bytes']
                        }
                    pool_data['options'] = {}
                    # basic_pool_options_bluestore collection
                    if self.is_enabled_collection(Collection.basic_pool_options_bluestore):
                        bluestore_options = ['compression_algorithm',
                                             'compression_mode',
                                             'compression_required_ratio',
                                             'compression_min_blob_size',
                                             'compression_max_blob_size']
                        for option in bluestore_options:
                            if option in pool['options']:
                                pool_data['options'][option] = pool['options'][option]

                # basic_pool_flags collection
                if self.is_enabled_collection(Collection.basic_pool_flags):
                    if 'flags_names' in pool and pool['flags_names'] is not None:
                        # flags are defined in pg_pool_t (src/osd/osd_types.h)
                        flags_to_report = [
                            'hashpspool',
                            'full',
                            'ec_overwrites',
                            'incomplete_clones',
                            'nodelete',
                            'nopgchange',
                            'nosizechange',
                            'write_fadvise_dontneed',
                            'noscrub',
                            'nodeep-scrub',
                            'full_quota',
                            'nearfull',
                            'backfillfull',
                            'selfmanaged_snaps',
                            'pool_snaps',
                            'creating',
                            'eio',
                            'bulk',
                            'crimson',
                            ]

                        pool_data['flags_names'] = [flag for flag in pool['flags_names'].split(',') if flag in flags_to_report]

                cast(List[Dict[str, Any]], report['pools']).append(pool_data)

                if 'rbd' in pool['application_metadata']:
                    rbd_num_pools += 1
                    ioctx = self.rados.open_ioctx(pool['pool_name'])
                    rbd_num_images_by_pool.append(
                        sum(1 for _ in rbd.RBD().list2(ioctx)))
                    rbd_mirroring_by_pool.append(
                        rbd.RBD().mirror_mode_get(ioctx) != rbd.RBD_MIRROR_MODE_DISABLED)
            report['rbd'] = {
                'num_pools': rbd_num_pools,
                'num_images_by_pool': rbd_num_images_by_pool,
                'mirroring_by_pool': rbd_mirroring_by_pool}

            # osds
            cluster_network = False
            for osd in osd_map['osds']:
                if osd['up'] and not cluster_network:
                    front_ip = osd['public_addrs']['addrvec'][0]['addr'].split(':')[0]
                    back_ip = osd['cluster_addrs']['addrvec'][0]['addr'].split(':')[0]
                    if front_ip != back_ip:
                        cluster_network = True
            report['osd'] = {
                'count': len(osd_map['osds']),
                'require_osd_release': osd_map['require_osd_release'],
                'require_min_compat_client': osd_map['require_min_compat_client'],
                'cluster_network': cluster_network,
            }

            # crush
            report['crush'] = self.gather_crush_info()

            # cephfs
            report['fs'] = {
                'count': len(fs_map['filesystems']),
                'feature_flags': fs_map['feature_flags'],
                'num_standby_mds': len(fs_map['standbys']),
                'filesystems': [],
            }
            num_mds = len(fs_map['standbys'])
            for fsm in fs_map['filesystems']:
                fs = fsm['mdsmap']
                num_sessions = 0
                cached_ino = 0
                cached_dn = 0
                cached_cap = 0
                subtrees = 0
                rfiles = 0
                rbytes = 0
                rsnaps = 0
                for gid, mds in fs['info'].items():
                    num_sessions += self.get_latest('mds', mds['name'],
                                                    'mds_sessions.session_count')
                    cached_ino += self.get_latest('mds', mds['name'],
                                                  'mds_mem.ino')
                    cached_dn += self.get_latest('mds', mds['name'],
                                                 'mds_mem.dn')
                    cached_cap += self.get_latest('mds', mds['name'],
                                                  'mds_mem.cap')
                    subtrees += self.get_latest('mds', mds['name'],
                                                'mds.subtrees')
                    if mds['rank'] == 0:
                        rfiles = self.get_latest('mds', mds['name'],
                                                 'mds.root_rfiles')
                        rbytes = self.get_latest('mds', mds['name'],
                                                 'mds.root_rbytes')
                        rsnaps = self.get_latest('mds', mds['name'],
                                                 'mds.root_rsnaps')
                report['fs']['filesystems'].append({  # type: ignore
                    'max_mds': fs['max_mds'],
                    'ever_allowed_features': fs['ever_allowed_features'],
                    'explicitly_allowed_features': fs['explicitly_allowed_features'],
                    'num_in': len(fs['in']),
                    'num_up': len(fs['up']),
                    'num_standby_replay': len(
                        [mds for gid, mds in fs['info'].items()
                         if mds['state'] == 'up:standby-replay']),
                    'num_mds': len(fs['info']),
                    'num_sessions': num_sessions,
                    'cached_inos': cached_ino,
                    'cached_dns': cached_dn,
                    'cached_caps': cached_cap,
                    'cached_subtrees': subtrees,
                    'balancer_enabled': len(fs['balancer']) > 0,
                    'num_data_pools': len(fs['data_pools']),
                    'standby_count_wanted': fs['standby_count_wanted'],
                    'approx_ctime': fs['created'][0:7],
                    'files': rfiles,
                    'bytes': rbytes,
                    'snaps': rsnaps,
                })
                num_mds += len(fs['info'])
            report['fs']['total_num_mds'] = num_mds  # type: ignore

            # daemons
            report['metadata'] = dict(osd=self.gather_osd_metadata(osd_map),
                                      mon=self.gather_mon_metadata(mon_map))

            if self.is_enabled_collection(Collection.basic_mds_metadata):
                report['metadata']['mds'] = self.gather_mds_metadata()  # type: ignore

            # host counts
            servers = self.list_servers()
            self.log.debug('servers %s' % servers)
            hosts = {
                'num': len([h for h in servers if h['hostname']]),
            }
            for t in ['mon', 'mds', 'osd', 'mgr']:
                nr_services = sum(1 for host in servers if
                                  any(service for service in cast(List[ServiceInfoT],
                                                                  host['services'])
                                      if service['type'] == t))
                hosts['num_with_' + t] = nr_services
            report['hosts'] = hosts

            report['usage'] = {
                'pools': len(df['pools']),
                'pg_num': num_pg,
                'total_used_bytes': df['stats']['total_used_bytes'],
                'total_bytes': df['stats']['total_bytes'],
                'total_avail_bytes': df['stats']['total_avail_bytes']
            }
            # basic_usage_by_class collection
            if self.is_enabled_collection(Collection.basic_usage_by_class):
                report['usage']['stats_by_class'] = {} # type: ignore
                for device_class in df['stats_by_class']:
                    if device_class in ['hdd', 'ssd', 'nvme']:
                        report['usage']['stats_by_class'][device_class] = df['stats_by_class'][device_class] # type: ignore

            services: DefaultDict[str, int] = defaultdict(int)
            for key, value in service_map['services'].items():
                services[key] += 1
                if key == 'rgw':
                    rgw = {}
                    zones = set()
                    zonegroups = set()
                    frontends = set()
                    count = 0
                    d = value.get('daemons', dict())
                    for k, v in d.items():
                        if k == 'summary' and v:
                            rgw[k] = v
                        elif isinstance(v, dict) and 'metadata' in v:
                            count += 1
                            zones.add(v['metadata']['zone_id'])
                            zonegroups.add(v['metadata']['zonegroup_id'])
                            frontends.add(v['metadata']['frontend_type#0'])

                            # we could actually iterate over all the keys of
                            # the dict and check for how many frontends there
                            # are, but it is unlikely that one would be running
                            # more than 2 supported ones
                            f2 = v['metadata'].get('frontend_type#1', None)
                            if f2:
                                frontends.add(f2)

                    rgw['count'] = count
                    rgw['zones'] = len(zones)
                    rgw['zonegroups'] = len(zonegroups)
                    rgw['frontends'] = list(frontends)  # sets aren't json-serializable
                    report['rgw'] = rgw
            report['services'] = services

            try:
                report['balancer'] = self.remote('balancer', 'gather_telemetry')
            except ImportError:
                report['balancer'] = {
                    'active': False
                }

            # Rook
            self.get_rook_data(report)

        if 'crash' in channels:
            report['crashes'] = self.gather_crashinfo()

        if 'perf' in channels:
            if self.is_enabled_collection(Collection.perf_perf):
                report['perf_counters'] = self.gather_perf_counters('separated')
                report['stats_per_pool'] = self.get_stats_per_pool()
                report['stats_per_pg'] = self.get_stats_per_pg()
                report['io_rate'] = self.get_io_rate()
                report['osd_perf_histograms'] = self.get_osd_histograms('separated')
                report['mempool'] = self.get_mempool('separated')
                report['heap_stats'] = self.get_heap_stats()
                report['rocksdb_stats'] = self.get_rocksdb_stats()

        # NOTE: We do not include the 'device' channel in this report; it is
        # sent to a different endpoint.

        return report

    def get_rook_data(self, report: Dict[str, object]) -> None:
        r, outb, outs = self.mon_command({
            'prefix': 'config-key dump',
            'format': 'json'
        })
        if r != 0:
            return
        try:
            config_kv_dump = json.loads(outb)
        except json.decoder.JSONDecodeError:
            return

        for elem in ROOK_KEYS_BY_COLLECTION:
            # elem[0] is the full key path (e.g. "rook/node/count/with-csi-nfs-plugin")
            # elem[1] is the Collection this key belongs to
            if self.is_enabled_collection(elem[1]):
                self.add_kv_to_report(report, elem[0], config_kv_dump.get(elem[0]))

    def add_kv_to_report(self, report: Dict[str, object], key_path: str, value: Any) -> None:
        last_node = key_path.split('/')[-1]
        for node in key_path.split('/')[0:-1]:
            if node not in report:
                report[node] = {}
            report = report[node]  # type: ignore

            # sanity check of keys correctness
            if not isinstance(report, dict):
                self.log.error(f"'{key_path}' is an invalid key, expected type 'dict' but got {type(report)}")
                return

        if last_node in report:
            self.log.error(f"'{key_path}' is an invalid key, last part must not exist at this point")
            return

        report[last_node] = value

    def _try_post(self, what: str, url: str, report: Dict[str, Dict[str, str]]) -> Optional[str]:
        self.log.info('Sending %s to: %s' % (what, url))
        proxies = dict()
        if self.proxy:
            self.log.info('Send using HTTP(S) proxy: %s', self.proxy)
            proxies['http'] = self.proxy
            proxies['https'] = self.proxy
        try:
            resp = requests.put(url=url, json=report, proxies=proxies)
            resp.raise_for_status()
        except Exception as e:
            fail_reason = 'Failed to send %s to %s: %s' % (what, url, str(e))
            self.log.error(fail_reason)
            return fail_reason
        return None

    class EndPoint(enum.Enum):
        ceph = 'ceph'
        device = 'device'

    def collection_delta(self, channels: Optional[List[str]] = None) -> Optional[List[Collection]]:
        '''
        Find collections that are available in the module, but are not in the db
        '''
        if self.db_collection is None:
            return None

        if not channels:
            channels = ALL_CHANNELS
        else:
            for ch in channels:
                if ch not in ALL_CHANNELS:
                    self.log.debug(f"invalid channel name: {ch}")
                    return None

        new_collection : List[Collection] = []

        for c in MODULE_COLLECTION:
            if c['name'].name not in self.db_collection:
                if c['channel'] in channels:
                    new_collection.append(c['name'])

        return new_collection

    def is_major_upgrade(self) -> bool:
        '''
        Returns True only if the user last opted-in to an older major
        '''
        if self.last_opted_in_ceph_version is None or self.last_opted_in_ceph_version == 0:
            # we do not know what Ceph version was when the user last opted-in,
            # thus we do not wish to nag in case of a major upgrade
            return False

        mon_map = self.get('mon_map')
        mon_min = mon_map.get("min_mon_release", 0)

        if mon_min - self.last_opted_in_ceph_version > 0:
            self.log.debug(f"major upgrade: mon_min is: {mon_min} and user last opted-in in {self.last_opted_in_ceph_version}")
            return True

        return False

    def is_opted_in(self) -> bool:
        # If len is 0 it means that the user is either opted-out (never
        # opted-in, or invoked `telemetry off`), or they upgraded from a
        # telemetry revision 1 or 2, which required to re-opt in to revision 3,
        # regardless, hence is considered as opted-out
        if self.db_collection is None:
            return False
        return len(self.db_collection) > 0

    def should_nag(self) -> bool:
        # Find delta between opted-in collections and module collections;
        # nag only if module has a collection which is not in db, and nag == True.

        # We currently do not nag if the user is opted-out (or never opted-in).
        # If we wish to do this in the future, we need to have a tri-mode state
        # (opted in, opted out, no action yet), and it needs to be guarded by a
        # config option (so that nagging can be turned off via config).
        # We also need to add a last_opted_out_ceph_version variable, for the
        # major upgrade check.

        # check if there are collections the user is not opt-in to
        # that we should nag about
        if self.db_collection is not None:
            for c in MODULE_COLLECTION:
                if c['name'].name not in self.db_collection:
                    if c['nag'] == True:
                        self.log.debug(f"The collection: {c['name']} is not reported")
                        return True

        # user might be opted-in to the most recent collection, or there is no
        # new collection which requires nagging about; thus nag in case it's a
        # major upgrade and there are new collections
        # (which their own nag == False):
        new_collections = False
        col_delta = self.collection_delta()
        if col_delta is not None and len(col_delta) > 0:
            new_collections = True

        return self.is_major_upgrade() and new_collections

    def init_collection(self) -> None:
        # We fetch from db the collections the user had already opted-in to.
        # During the transition the results will be empty, but the user might
        # be opted-in to an older version (e.g. revision = 3)

        collection = self.get_store('collection')

        if collection is not None:
            self.db_collection = json.loads(collection)

        if self.db_collection is None:
            # happens once on upgrade
            if not self.enabled:
                # user is not opted-in
                self.set_store('collection', json.dumps([]))
                self.log.debug("user is not opted-in")
            else:
                # user is opted-in, verify the revision:
                if self.last_opt_revision == REVISION:
                    self.log.debug(f"telemetry revision is {REVISION}")
                    base_collection = [Collection.basic_base.name, Collection.device_base.name, Collection.crash_base.name, Collection.ident_base.name]
                    self.set_store('collection', json.dumps(base_collection))
                else:
                    # user is opted-in to an older version, meaning they need
                    # to re-opt in regardless
                    self.set_store('collection', json.dumps([]))
                    self.log.debug(f"user is opted-in but revision is old ({self.last_opt_revision}), needs to re-opt-in")

            # reload collection after setting
            collection = self.get_store('collection')
            if collection is not None:
                self.db_collection = json.loads(collection)
            else:
                raise RuntimeError('collection is None after initial setting')
        else:
            # user has already upgraded
            self.log.debug(f"user has upgraded already: collection: {self.db_collection}")

    def is_enabled_collection(self, collection: Collection) -> bool:
        if self.db_collection is None:
            return False
        return collection.name in self.db_collection

    def opt_in_all_collections(self) -> None:
        """
        Opt-in to all collections; Update db with the currently available collections in the module
        """
        if self.db_collection is None:
            raise RuntimeError('db_collection is None after initial setting')

        for c in MODULE_COLLECTION:
            if c['name'].name not in self.db_collection:
                self.db_collection.append(c['name'])

        self.set_store('collection', json.dumps(self.db_collection))

    def send(self,
             report: Dict[str, Dict[str, str]],
             endpoint: Optional[List[EndPoint]] = None) -> Tuple[int, str, str]:
        if not endpoint:
            endpoint = [self.EndPoint.ceph, self.EndPoint.device]
        failed = []
        success = []
        self.log.debug('Send endpoints %s' % endpoint)
        for e in endpoint:
            if e == self.EndPoint.ceph:
                fail_reason = self._try_post('ceph report', self.url, report)
                if fail_reason:
                    failed.append(fail_reason)
                else:
                    now = int(time.time())
                    self.last_upload = now
                    self.set_store('last_upload', str(now))
                    success.append('Ceph report sent to {0}'.format(self.url))
                    self.log.info('Sent report to {0}'.format(self.url))
            elif e == self.EndPoint.device:
                if 'device' in self.get_active_channels():
                    devices = self.gather_device_report()
                    if devices:
                        num_devs = 0
                        num_hosts = 0
                        for host, ls in devices.items():
                            self.log.debug('host %s devices %s' % (host, ls))
                            if not len(ls):
                                continue
                            fail_reason = self._try_post('devices', self.device_url,
                                                         ls)
                            if fail_reason:
                                failed.append(fail_reason)
                            else:
                                num_devs += len(ls)
                                num_hosts += 1
                        if num_devs:
                            success.append('Reported %d devices from %d hosts across a total of %d hosts' % (
                                num_devs, num_hosts, len(devices)))
                    else:
                        fail_reason = 'Unable to send device report: Device channel is on, but the generated report was empty.'
                        failed.append(fail_reason)
                        self.log.error(fail_reason)
        if failed:
            return 1, '', '\n'.join(success + failed)
        return 0, '', '\n'.join(success)

    def format_perf_histogram(self, report: Dict[str, Any]) -> None:
        # Formatting the perf histograms so they are human-readable. This will change the
        # ranges and values, which are currently in list form, into strings so that
        # they are displayed horizontally instead of vertically.
        if 'report' in report:
            report = report['report']
        try:
            # Formatting ranges and values in osd_perf_histograms
            mode = 'osd_perf_histograms'
            for config in report[mode]:
                for histogram in config:
                    # Adjust ranges by converting lists into strings
                    for axis in config[histogram]['axes']:
                        for i in range(0, len(axis['ranges'])):
                            axis['ranges'][i] = str(axis['ranges'][i])

                    for osd in config[histogram]['osds']:
                        for i in range(0, len(osd['values'])):
                            osd['values'][i] = str(osd['values'][i])
        except KeyError:
            # If the perf channel is not enabled, there should be a KeyError since
            # 'osd_perf_histograms' would not be present in the report. In that case,
            # the show function should pass as usual without trying to format the
            # histograms.
            pass

    def toggle_channel(self, action: str, channels: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Enable or disable a list of channels
        '''
        if not self.enabled:
            # telemetry should be on for channels to be toggled
            msg = 'Telemetry is off. Please consider opting-in with `ceph telemetry on`.\n' \
                  'Preview sample reports with `ceph telemetry preview`.'
            return 0, msg, ''

        if channels is None:
            msg = f'Please provide a channel name. Available channels: {ALL_CHANNELS}.'
            return 0, msg, ''

        state = action == 'enable'
        msg = ''
        for c in channels:
            if c not in ALL_CHANNELS:
                msg = f"{msg}{c} is not a valid channel name. "\
                        f"Available channels: {ALL_CHANNELS}.\n"
            else:
                self.set_module_option(f"channel_{c}", state)
                setattr(self,
                        f"channel_{c}",
                        state)
                msg = f"{msg}channel_{c} is {action}d\n"

        return 0, msg, ''

    @CLIReadCommand('telemetry status')
    def status(self) -> Tuple[int, str, str]:
        '''
        Show current configuration
        '''
        r = {}
        for opt in self.MODULE_OPTIONS:
            r[opt['name']] = getattr(self, opt['name'])
        r['last_upload'] = (time.ctime(self.last_upload)
                            if self.last_upload else self.last_upload)
        return 0, json.dumps(r, indent=4, sort_keys=True), ''

    @CLIReadCommand('telemetry diff')
    def diff(self) -> Tuple[int, str, str]:
        '''
        Show the diff between opted-in collection and available collection
        '''
        diff = []
        keys = ['nag']

        for c in MODULE_COLLECTION:
            if not self.is_enabled_collection(c['name']):
                diff.append({key: val for key, val in c.items() if key not in keys})

        r = None
        if diff == []:
            r = "Telemetry is up to date"
        else:
            r = json.dumps(diff, indent=4, sort_keys=True)

        return 0, r, ''

    @CLICommand('telemetry on')
    def on(self, license: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Enable telemetry reports from this cluster
        '''
        if license != LICENSE:
            return -errno.EPERM, '', f'''Telemetry data is licensed under the {LICENSE_NAME} ({LICENSE_URL}).
To enable, add '--license {LICENSE}' to the 'ceph telemetry on' command.'''
        else:
            self.set_module_option('enabled', True)
            self.enabled = True
            self.opt_in_all_collections()

            # for major releases upgrade nagging
            mon_map = self.get('mon_map')
            mon_min = mon_map.get("min_mon_release", 0)
            self.set_store('last_opted_in_ceph_version', str(mon_min))
            self.last_opted_in_ceph_version = mon_min

            msg = 'Telemetry is on.'
            disabled_channels = ''
            active_channels = self.get_active_channels()
            for c in ALL_CHANNELS:
                if c not in active_channels and c != 'ident':
                    disabled_channels = f"{disabled_channels} {c}"

            if len(disabled_channels) > 0:
                msg = f"{msg}\nSome channels are disabled, please enable with:\n"\
                        f"`ceph telemetry enable channel{disabled_channels}`"

            # wake up serve() to reset health warning
            self.event.set()

            return 0, msg, ''

    @CLICommand('telemetry off')
    def off(self) -> Tuple[int, str, str]:
        '''
        Disable telemetry reports from this cluster
        '''
        if not self.enabled:
            # telemetry is already off
            msg = 'Telemetry is currently not enabled, nothing to turn off. '\
                    'Please consider opting-in with `ceph telemetry on`.\n' \
                  'Preview sample reports with `ceph telemetry preview`.'
            return 0, msg, ''

        self.set_module_option('enabled', False)
        self.enabled = False
        self.set_store('collection', json.dumps([]))
        self.db_collection = []

        # we might need this info in the future, in case
        # of nagging when user is opted-out
        mon_map = self.get('mon_map')
        mon_min = mon_map.get("min_mon_release", 0)
        self.set_store('last_opted_out_ceph_version', str(mon_min))
        self.last_opted_out_ceph_version = mon_min

        msg = 'Telemetry is now disabled.'
        return 0, msg, ''

    @CLIReadCommand('telemetry enable channel all')
    def enable_channel_all(self, channels: List[str] = ALL_CHANNELS) -> Tuple[int, str, str]:
        '''
        Enable all channels
        '''
        return self.toggle_channel('enable', channels)

    @CLIReadCommand('telemetry enable channel')
    def enable_channel(self, channels: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Enable a list of channels
        '''
        return self.toggle_channel('enable', channels)

    @CLIReadCommand('telemetry disable channel all')
    def disable_channel_all(self, channels: List[str] = ALL_CHANNELS) -> Tuple[int, str, str]:
        '''
        Disable all channels
        '''
        return self.toggle_channel('disable', channels)

    @CLIReadCommand('telemetry disable channel')
    def disable_channel(self, channels: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Disable a list of channels
        '''
        return self.toggle_channel('disable', channels)

    @CLIReadCommand('telemetry channel ls')
    def channel_ls(self) -> Tuple[int, str, str]:
        '''
        List all channels
        '''
        table = PrettyTable(
            [
                'NAME', 'ENABLED', 'DEFAULT', 'DESC',
            ],
            border=False)
        table.align['NAME'] = 'l'
        table.align['ENABLED'] = 'l'
        table.align['DEFAULT'] = 'l'
        table.align['DESC'] = 'l'
        table.left_padding_width = 0
        table.right_padding_width = 4

        for c in ALL_CHANNELS:
            enabled = "ON" if getattr(self, f"channel_{c}") else "OFF"
            for o in self.MODULE_OPTIONS:
                if o['name'] == f"channel_{c}":
                    default = "ON" if o.get('default', None) else "OFF"
                    desc = o.get('desc', None)

            table.add_row((
                c,
                enabled,
                default,
                desc,
            ))

        return 0, table.get_string(sortby="NAME"), ''

    @CLIReadCommand('telemetry collection ls')
    def collection_ls(self) -> Tuple[int, str, str]:
        '''
        List all collections
        '''
        col_delta = self.collection_delta()
        msg = ''
        if col_delta is not None and len(col_delta) > 0:
            msg = f"New collections are available:\n" \
                  f"{sorted([c.name for c in col_delta])}\n" \
                  f"Run `ceph telemetry on` to opt-in to these collections.\n"

        table = PrettyTable(
            [
                'NAME', 'STATUS', 'DESC',
            ],
            border=False)
        table.align['NAME'] = 'l'
        table.align['STATUS'] = 'l'
        table.align['DESC'] = 'l'
        table.left_padding_width = 0
        table.right_padding_width = 4

        for c in MODULE_COLLECTION:
            name = c['name']
            opted_in = self.is_enabled_collection(name)
            channel_enabled = getattr(self, f"channel_{c['channel']}")

            status = ''
            if channel_enabled and opted_in:
                status = "REPORTING"
            else:
                why = ''
                delimiter = ''

                if not opted_in:
                    why += "NOT OPTED-IN"
                    delimiter = ', '
                if not channel_enabled:
                    why += f"{delimiter}CHANNEL {c['channel']} IS OFF"

                status = f"NOT REPORTING: {why}"

            desc = c['description']

            table.add_row((
                name,
                status,
                desc,
            ))

        if len(msg):
            # add a new line between message and table output
            msg = f"{msg} \n"

        return 0, f'{msg}{table.get_string(sortby="NAME")}', ''

    @CLICommand('telemetry send')
    def do_send(self,
                endpoint: Optional[List[EndPoint]] = None,
                license: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Send a sample report
        '''
        if not self.is_opted_in() and license != LICENSE:
            self.log.debug(('A telemetry send attempt while opted-out. '
                            'Asking for license agreement'))
            return -errno.EPERM, '', f'''Telemetry data is licensed under the {LICENSE_NAME} ({LICENSE_URL}).
To manually send telemetry data, add '--license {LICENSE}' to the 'ceph telemetry send' command.
Please consider enabling the telemetry module with 'ceph telemetry on'.'''
        else:
            self.last_report = self.compile_report()
            return self.send(self.last_report, endpoint)

    @CLIReadCommand('telemetry show')
    def show(self, channels: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Show a sample report of opted-in collections (except for 'device')
        '''
        if not self.enabled:
            # if telemetry is off, no report is being sent, hence nothing to show
            msg = 'Telemetry is off. Please consider opting-in with `ceph telemetry on`.\n' \
                  'Preview sample reports with `ceph telemetry preview`.'
            return 0, msg, ''

        report = self.get_report_locked(channels=channels)
        self.format_perf_histogram(report)
        report = json.dumps(report, indent=4, sort_keys=True)

        if self.channel_device:
            report += '''\nDevice report is generated separately. To see it run 'ceph telemetry show-device'.'''

        return 0, report, ''

    @CLIReadCommand('telemetry preview')
    def preview(self, channels: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Preview a sample report of the most recent collections available (except for 'device')
        '''
        report = {}

        # We use a lock to prevent a scenario where the user wishes to preview
        # the report, and at the same time the module hits the interval of
        # sending a report with the opted-in collection, which has less data
        # than in the preview report.
        col_delta = self.collection_delta()
        with self.get_report_lock:
            if col_delta is not None and len(col_delta) == 0:
                # user is already opted-in to the most recent collection
                msg = 'Telemetry is up to date, see report with `ceph telemetry show`.'
                return 0, msg, ''
            else:
                # there are collections the user is not opted-in to
                next_collection = []

                for c in MODULE_COLLECTION:
                    next_collection.append(c['name'].name)

                opted_in_collection = self.db_collection
                self.db_collection = next_collection
                report = self.get_report(channels=channels)
                self.db_collection = opted_in_collection

        self.format_perf_histogram(report)
        report = json.dumps(report, indent=4, sort_keys=True)

        if self.channel_device:
            report += '''\nDevice report is generated separately. To see it run 'ceph telemetry preview-device'.'''

        return 0, report, ''

    @CLIReadCommand('telemetry show-device')
    def show_device(self) -> Tuple[int, str, str]:
        '''
        Show a sample device report
        '''
        if not self.enabled:
            # if telemetry is off, no report is being sent, hence nothing to show
            msg = 'Telemetry is off. Please consider opting-in with `ceph telemetry on`.\n' \
                  'Preview sample device reports with `ceph telemetry preview-device`.'
            return 0, msg, ''

        if not self.channel_device:
            # if device channel is off, device report is not being sent, hence nothing to show
            msg = 'device channel is off. Please enable with `ceph telemetry enable channel device`.\n' \
                  'Preview sample device reports with `ceph telemetry preview-device`.'
            return 0, msg, ''

        return 0, json.dumps(self.get_report_locked('device'), indent=4, sort_keys=True), ''

    @CLIReadCommand('telemetry preview-device')
    def preview_device(self) -> Tuple[int, str, str]:
        '''
        Preview a sample device report of the most recent device collection
        '''
        report = {}

        device_col_delta = self.collection_delta(['device'])
        with self.get_report_lock:
            if device_col_delta is not None and len(device_col_delta) == 0 and self.channel_device:
                # user is already opted-in to the most recent device collection,
                # and device channel is on, thus `show-device` should be called
                msg = 'device channel is on and up to date, see report with `ceph telemetry show-device`.'
                return 0, msg, ''

            # either the user is not opted-in at all, or there are collections
            # they are not opted-in to
            next_collection = []

            for c in MODULE_COLLECTION:
                next_collection.append(c['name'].name)

            opted_in_collection = self.db_collection
            self.db_collection = next_collection
            report = self.get_report('device')
            self.db_collection = opted_in_collection

        report = json.dumps(report, indent=4, sort_keys=True)
        return 0, report, ''

    @CLIReadCommand('telemetry show-all')
    def show_all(self) -> Tuple[int, str, str]:
        '''
        Show a sample report of all enabled channels (including 'device' channel)
        '''
        if not self.enabled:
            # if telemetry is off, no report is being sent, hence nothing to show
            msg = 'Telemetry is off. Please consider opting-in with `ceph telemetry on`.\n' \
                  'Preview sample reports with `ceph telemetry preview`.'
            return 0, msg, ''

        if not self.channel_device:
            # device channel is off, no need to display its report
            report = self.get_report_locked('default')
        else:
            # telemetry is on and device channel is enabled, show both
            report = self.get_report_locked('all')

        self.format_perf_histogram(report)
        return 0, json.dumps(report, indent=4, sort_keys=True), ''

    @CLIReadCommand('telemetry preview-all')
    def preview_all(self) -> Tuple[int, str, str]:
        '''
        Preview a sample report of the most recent collections available of all channels (including 'device')
        '''
        report = {}

        col_delta = self.collection_delta()
        with self.get_report_lock:
            if col_delta is not None and len(col_delta) == 0:
                # user is already opted-in to the most recent collection
                msg = 'Telemetry is up to date, see report with `ceph telemetry show`.'
                return 0, msg, ''

            # there are collections the user is not opted-in to
            next_collection = []

            for c in MODULE_COLLECTION:
                next_collection.append(c['name'].name)

            opted_in_collection = self.db_collection
            self.db_collection = next_collection
            report = self.get_report('all')
            self.db_collection = opted_in_collection

        self.format_perf_histogram(report)
        report = json.dumps(report, indent=4, sort_keys=True)

        return 0, report, ''

    def get_report_locked(self,
                          report_type: str = 'default',
                          channels: Optional[List[str]] = None) -> Dict[str, Any]:
        '''
        A wrapper around get_report to allow for compiling a report of the most recent module collections
        '''
        with self.get_report_lock:
            return self.get_report(report_type, channels)

    def get_report(self,
                   report_type: str = 'default',
                   channels: Optional[List[str]] = None) -> Dict[str, Any]:
        if report_type == 'default':
            return self.compile_report(channels=channels)
        elif report_type == 'device':
            return self.gather_device_report()
        elif report_type == 'all':
            return {'report': self.compile_report(channels=channels),
                    'device_report': self.gather_device_report()}
        return {}

    def self_test(self) -> None:
        self.opt_in_all_collections()
        report = self.compile_report(channels=ALL_CHANNELS)
        if len(report) == 0:
            raise RuntimeError('Report is empty')

        if 'report_id' not in report:
            raise RuntimeError('report_id not found in report')

    def shutdown(self) -> None:
        self.run = False
        self.event.set()

    def refresh_health_checks(self) -> None:
        health_checks = {}
        # TODO do we want to nag also in case the user is not opted-in?
        if self.enabled and self.should_nag():
            health_checks['TELEMETRY_CHANGED'] = {
                'severity': 'warning',
                'summary': 'Telemetry requires re-opt-in',
                'detail': [
                    'telemetry module includes new collections; please re-opt-in to new collections with `ceph telemetry on`'
                ]
            }
        self.set_health_checks(health_checks)

    def serve(self) -> None:
        self.load()
        self.run = True

        self.log.debug('Waiting for mgr to warm up')
        time.sleep(10)

        while self.run:
            self.event.clear()

            self.refresh_health_checks()

            if not self.is_opted_in():
                self.log.debug('Not sending report until user re-opts-in')
                self.event.wait(1800)
                continue
            if not self.enabled:
                self.log.debug('Not sending report until configured to do so')
                self.event.wait(1800)
                continue

            now = int(time.time())
            if not self.last_upload or \
               (now - self.last_upload) > self.interval * 3600:
                self.log.info('Compiling and sending report to %s',
                              self.url)

                try:
                    self.last_report = self.compile_report()
                except Exception:
                    self.log.exception('Exception while compiling report:')

                self.send(self.last_report)
            else:
                self.log.debug('Interval for sending new report has not expired')

            sleep = 3600
            self.log.debug('Sleeping for %d seconds', sleep)
            self.event.wait(sleep)

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        return True, ''
