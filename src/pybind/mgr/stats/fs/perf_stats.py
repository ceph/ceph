import re
import json
import time
import uuid
import errno
import traceback
import logging
from collections import OrderedDict
from typing import List, Dict, Set

from mgr_module import CommandResult

from datetime import datetime, timedelta
from threading import Lock, Condition, Thread, Timer
from ipaddress import ip_address

PERF_STATS_VERSION = 2

QUERY_IDS = "query_ids"
GLOBAL_QUERY_ID = "global_query_id"
QUERY_LAST_REQUEST = "last_time_stamp"
QUERY_RAW_COUNTERS = "query_raw_counters"
QUERY_RAW_COUNTERS_GLOBAL = "query_raw_counters_global"

MDS_RANK_ALL = (-1,)
CLIENT_ID_ALL = r"\d*"
CLIENT_IP_ALL = ".*"

fs_list = [] # type: List[str]

MDS_PERF_QUERY_REGEX_MATCH_ALL_RANKS = '^(.*)$'
MDS_PERF_QUERY_REGEX_MATCH_CLIENTS = r'^(client.{0}\s+{1}):.*'
MDS_PERF_QUERY_COUNTERS_MAP = OrderedDict({'cap_hit': 0,
                                           'read_latency': 1,
                                           'write_latency': 2,
                                           'metadata_latency': 3,
                                           'dentry_lease': 4,
                                           'opened_files': 5,
                                           'pinned_icaps': 6,
                                           'opened_inodes': 7,
                                           'read_io_sizes': 8,
                                           'write_io_sizes': 9,
                                           'avg_read_latency': 10,
                                           'stdev_read_latency': 11,
                                           'avg_write_latency': 12,
                                           'stdev_write_latency': 13,
                                           'avg_metadata_latency': 14,
                                           'stdev_metadata_latency': 15})
MDS_PERF_QUERY_COUNTERS = [] # type: List[str]
MDS_GLOBAL_PERF_QUERY_COUNTERS = list(MDS_PERF_QUERY_COUNTERS_MAP.keys())

QUERY_EXPIRE_INTERVAL = timedelta(minutes=1)
REREGISTER_TIMER_INTERVAL = 1

CLIENT_METADATA_KEY = "client_metadata"
CLIENT_METADATA_SUBKEYS = ["hostname", "root"]
CLIENT_METADATA_SUBKEYS_OPTIONAL = ["mount_point"]

NON_EXISTENT_KEY_STR = "N/A"

logger = logging.getLogger(__name__)

class FilterSpec(object):
    """
    query filters encapsulated and used as key for query map
    """
    def __init__(self, mds_ranks, client_id, client_ip):
        self.mds_ranks = mds_ranks
        self.client_id = client_id
        self.client_ip = client_ip

    def __hash__(self):
        return hash((self.mds_ranks, self.client_id, self.client_ip))

    def __eq__(self, other):
        return (self.mds_ranks, self.client_id, self.client_ip) == (other.mds_ranks, other.client_id, self.client_ip)

    def __ne__(self, other):
        return not(self == other)

def extract_mds_ranks_from_spec(mds_rank_spec):
    if not mds_rank_spec:
        return MDS_RANK_ALL
    match = re.match(r'^\d+(,\d+)*$', mds_rank_spec)
    if not match:
        raise ValueError("invalid mds filter spec: {}".format(mds_rank_spec))
    return tuple(int(mds_rank) for mds_rank in match.group(0).split(','))

def extract_client_id_from_spec(client_id_spec):
    if not client_id_spec:
        return CLIENT_ID_ALL
    # the client id is the spec itself since it'll be a part
    # of client filter regex.
    if not client_id_spec.isdigit():
        raise ValueError('invalid client_id filter spec: {}'.format(client_id_spec))
    return client_id_spec

def extract_client_ip_from_spec(client_ip_spec):
    if not client_ip_spec:
        return CLIENT_IP_ALL

    client_ip = client_ip_spec
    if client_ip.startswith('v1:'):
        client_ip = client_ip.replace('v1:', '')
    elif client_ip.startswith('v2:'):
        client_ip = client_ip.replace('v2:', '')

    try:
        ip_address(client_ip)
        return client_ip_spec
    except ValueError:
        raise ValueError('invalid client_ip filter spec: {}'.format(client_ip_spec))

def extract_mds_ranks_from_report(mds_ranks_str):
    if not mds_ranks_str:
        return []
    return [int(x) for x in mds_ranks_str.split(',')]

def extract_client_id_and_ip(client):
    match = re.match(r'^(client\.\d+)\s(.*)', client)
    if match:
        return match.group(1), match.group(2)
    return None, None

class FSPerfStats(object):
    lock = Lock()
    q_cv = Condition(lock)
    r_cv = Condition(lock)

    user_queries = {} # type: Dict[str, Dict]

    meta_lock = Lock()
    rqtimer = None
    client_metadata = {
        'metadata' : {},
        'to_purge' : set(),
        'in_progress' : {},
    } # type: Dict

    def __init__(self, module):
        self.module = module
        self.log = module.log
        self.prev_rank0_gid = None
        self.mx_last_updated = 0.0
        # report processor thread
        self.report_processor = Thread(target=self.run)
        self.report_processor.start()

    def set_client_metadata(self, fs_name, client_id, key, meta):
        result = (self.client_metadata['metadata'].setdefault(
                            fs_name, {})).setdefault(client_id, {})
        if not key in result or not result[key] == meta:
            result[key] = meta

    def notify_cmd(self, cmdtag):
        self.log.debug("cmdtag={0}".format(cmdtag))
        with self.meta_lock:
            try:
                result = self.client_metadata['in_progress'].pop(cmdtag)
            except KeyError:
                self.log.warn(f"cmdtag {cmdtag} not found in client metadata")
                return
            fs_name = result[0]
            client_meta = result[2].wait()
            if client_meta[0] != 0:
                self.log.warn("failed to fetch client metadata from gid {0}, err={1}".format(
                    result[1], client_meta[2]))
                return
            self.log.debug("notify: client metadata={0}".format(json.loads(client_meta[1])))
            for metadata in json.loads(client_meta[1]):
                client_id = "client.{0}".format(metadata['id'])
                result = (self.client_metadata['metadata'].setdefault(fs_name, {})).setdefault(client_id, {})
                for subkey in CLIENT_METADATA_SUBKEYS:
                    self.set_client_metadata(fs_name, client_id, subkey, metadata[CLIENT_METADATA_KEY][subkey])
                for subkey in CLIENT_METADATA_SUBKEYS_OPTIONAL:
                    self.set_client_metadata(fs_name, client_id, subkey,
                                             metadata[CLIENT_METADATA_KEY].get(subkey, NON_EXISTENT_KEY_STR))
                metric_features = int(metadata[CLIENT_METADATA_KEY]["metric_spec"]["metric_flags"]["feature_bits"], 16)
                supported_metrics = [metric for metric, bit in MDS_PERF_QUERY_COUNTERS_MAP.items() if metric_features & (1 << bit)]
                self.set_client_metadata(fs_name, client_id, "valid_metrics", supported_metrics)
                kver = metadata[CLIENT_METADATA_KEY].get("kernel_version", None)
                if kver:
                    self.set_client_metadata(fs_name, client_id, "kernel_version", kver)
            # when all async requests are done, purge clients metadata if any.
            if not self.client_metadata['in_progress']:
                global fs_list
                for fs_name in fs_list:
                    for client in self.client_metadata['to_purge']:
                        try:
                            if client in self.client_metadata['metadata'][fs_name]:
                                self.log.info("purge client metadata for {0}".format(client))
                                self.client_metadata['metadata'][fs_name].pop(client)
                        except:
                            pass
                    if fs_name in self.client_metadata['metadata'] and not bool(self.client_metadata['metadata'][fs_name]):
                        self.client_metadata['metadata'].pop(fs_name)
                self.client_metadata['to_purge'].clear()
            self.log.debug("client_metadata={0}, to_purge={1}".format(
                self.client_metadata['metadata'], self.client_metadata['to_purge']))

    def notify_fsmap(self):
        #Reregister the user queries when there is a new rank0 mds
        with self.lock:
            gid_state = FSPerfStats.get_rank0_mds_gid_state(self.module.get('fs_map'))
            if not gid_state:
                return
            for value in gid_state:
                rank0_gid, state = value
                if (rank0_gid and rank0_gid != self.prev_rank0_gid and state == 'up:active'):
                    #the new rank0 MDS is up:active
                    ua_last_updated = time.monotonic()
                    if (self.rqtimer and self.rqtimer.is_alive()):
                        self.rqtimer.cancel()
                    self.rqtimer = Timer(REREGISTER_TIMER_INTERVAL,
                                         self.re_register_queries,
                                         args=(rank0_gid, ua_last_updated,))
                    self.rqtimer.start()

    def re_register_queries(self, rank0_gid, ua_last_updated):
        #reregister queries if the metrics are the latest. Otherwise reschedule the timer and
        #wait for the empty metrics
        with self.lock:
            if self.mx_last_updated >= ua_last_updated:
                self.log.debug("reregistering queries...")
                self.module.reregister_mds_perf_queries()
                self.prev_rank0_gid = rank0_gid
            else:
                #reschedule the timer
                self.rqtimer = Timer(REREGISTER_TIMER_INTERVAL,
                                     self.re_register_queries, args=(rank0_gid, ua_last_updated,))
                self.rqtimer.start()

    @staticmethod
    def get_rank0_mds_gid_state(fsmap):
        gid_state = []
        for fs in fsmap['filesystems']:
            mds_map = fs['mdsmap']
            if mds_map is not None:
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['rank'] == 0:
                        gid_state.append([mds_status['gid'], mds_status['state']])
        if gid_state:
            return gid_state
        logger.warn("No rank0 mds in the fsmap")

    def update_client_meta(self):
        new_updates = {}
        pending_updates = [v[0] for v in self.client_metadata['in_progress'].values()]
        global fs_list
        fs_list.clear()
        with self.meta_lock:
            fsmap = self.module.get('fs_map')
            for fs in fsmap['filesystems']:
                mds_map = fs['mdsmap']
                if mds_map is not None:
                    fsname = mds_map['fs_name']
                    for mds_id, mds_status in mds_map['info'].items():
                        if mds_status['rank'] == 0:
                            fs_list.append(fsname)
                            rank0_gid = mds_status['gid']
                            tag = str(uuid.uuid4())
                            result = CommandResult(tag)
                            new_updates[tag] = (fsname, rank0_gid, result)
                    self.client_metadata['in_progress'].update(new_updates)

        self.log.debug(f"updating client metadata from {new_updates}")

        cmd_dict = {'prefix': 'client ls'}
        for tag,val in new_updates.items():
            self.module.send_command(val[2], "mds", str(val[1]), json.dumps(cmd_dict), tag)

    def run(self):
        try:
            self.log.info("FSPerfStats::report_processor starting...")
            while True:
                with self.lock:
                    self.scrub_expired_queries()
                    self.process_mds_reports()
                    self.r_cv.notify()

                    stats_period = int(self.module.get_ceph_option("mgr_stats_period"))
                    self.q_cv.wait(stats_period)
                self.log.debug("FSPerfStats::tick")
        except Exception as e:
            self.log.fatal("fatal error: {}".format(traceback.format_exc()))

    def cull_mds_entries(self, raw_perf_counters, incoming_metrics, missing_clients):
        # this is pretty straight forward -- find what MDSs are missing from
        # what is tracked vs what we received in incoming report and purge
        # the whole bunch.
        tracked_ranks = raw_perf_counters.keys()
        available_ranks = [int(counter['k'][0][0]) for counter in incoming_metrics]
        for rank in set(tracked_ranks) - set(available_ranks):
            culled = raw_perf_counters.pop(rank)
            self.log.info("culled {0} client entries from rank {1} (laggy: {2})".format(
                len(culled[1]), rank, "yes" if culled[0] else "no"))
            missing_clients.update(list(culled[1].keys()))

    def cull_client_entries(self, raw_perf_counters, incoming_metrics, missing_clients):
        # this is a bit more involved -- for each rank figure out what clients
        # are missing in incoming report and purge them from our tracked map.
        # but, if this is invoked after cull_mds_entries(), the rank set
        # is same, so we can loop based on that assumption.
        ranks = raw_perf_counters.keys()
        for rank in ranks:
            tracked_clients = raw_perf_counters[rank][1].keys()
            available_clients = [extract_client_id_and_ip(counter['k'][1][0]) for counter in incoming_metrics]
            for client in set(tracked_clients) - set([c[0] for c in available_clients if c[0] is not None]):
                raw_perf_counters[rank][1].pop(client)
                self.log.info("culled {0} from rank {1}".format(client, rank))
                missing_clients.add(client)

    def cull_missing_entries(self, raw_perf_counters, incoming_metrics):
        missing_clients = set() # type: Set[str]
        self.cull_mds_entries(raw_perf_counters, incoming_metrics, missing_clients)
        self.cull_client_entries(raw_perf_counters, incoming_metrics, missing_clients)

        self.log.debug("missing_clients={0}".format(missing_clients))
        with self.meta_lock:
            if self.client_metadata['in_progress']:
                self.client_metadata['to_purge'].update(missing_clients)
                self.log.info("deferring client metadata purge (now {0} client(s))".format(
                    len(self.client_metadata['to_purge'])))
            else:
                global fs_list
                for fs_name in fs_list:
                    for client in missing_clients:
                        try:
                            self.log.info("purge client metadata for {0}".format(client))
                            if client in self.client_metadata['metadata'][fs_name]:
                                self.client_metadata['metadata'][fs_name].pop(client)
                        except KeyError:
                            pass
                    self.log.debug("client_metadata={0}".format(self.client_metadata['metadata']))

    def cull_global_metrics(self, raw_perf_counters, incoming_metrics):
        tracked_clients = raw_perf_counters.keys()
        available_clients = [counter['k'][0][0] for counter in incoming_metrics]
        for client in set(tracked_clients) - set(available_clients):
            raw_perf_counters.pop(client)

    def get_raw_perf_counters(self, query):
        raw_perf_counters = query.setdefault(QUERY_RAW_COUNTERS, {})

        for query_id in query[QUERY_IDS]:
            result = self.module.get_mds_perf_counters(query_id)
            self.log.debug("raw_perf_counters={}".format(raw_perf_counters))
            self.log.debug("get_raw_perf_counters={}".format(result))

            # extract passed in delayed ranks. metrics for delayed ranks are tagged
            # as stale.
            delayed_ranks = extract_mds_ranks_from_report(result['metrics'][0][0])

            # what's received from MDS
            incoming_metrics = result['metrics'][1]

            # metrics updated (monotonic) time
            self.mx_last_updated = result['metrics'][2][0]

            # cull missing MDSs and clients
            self.cull_missing_entries(raw_perf_counters, incoming_metrics)

            # iterate over metrics list and update our copy (note that we have
            # already culled the differences).
            global fs_list
            for fs_name in fs_list:
                for counter in incoming_metrics:
                    mds_rank = int(counter['k'][0][0])
                    client_id, client_ip = extract_client_id_and_ip(counter['k'][1][0])
                    if self.client_metadata['metadata'].get(fs_name):
                        if (client_id is not None or not client_ip) and\
                             self.client_metadata["metadata"][fs_name].get(client_id): # client_id _could_ be 0
                            with self.meta_lock:
                                self.set_client_metadata(fs_name, client_id, "IP", client_ip)
                        else:
                            self.log.warn(f"client metadata for client_id={client_id} might be unavailable")
                    else:
                        self.log.warn(f"client metadata for filesystem={fs_name} might be unavailable")

                    raw_counters = raw_perf_counters.setdefault(mds_rank, [False, {}])
                    raw_counters[0] = True if mds_rank in delayed_ranks else False
                    raw_client_counters = raw_counters[1].setdefault(client_id, [])

                    del raw_client_counters[:]
                    raw_client_counters.extend(counter['c'])
        # send an asynchronous client metadata refresh
        self.update_client_meta()

    def get_raw_perf_counters_global(self, query):
        raw_perf_counters = query.setdefault(QUERY_RAW_COUNTERS_GLOBAL, {})
        result = self.module.get_mds_perf_counters(query[GLOBAL_QUERY_ID])

        self.log.debug("raw_perf_counters_global={}".format(raw_perf_counters))
        self.log.debug("get_raw_perf_counters_global={}".format(result))

        global_metrics = result['metrics'][1]
        self.cull_global_metrics(raw_perf_counters, global_metrics)
        for counter in global_metrics:
            client_id, _ = extract_client_id_and_ip(counter['k'][0][0])
            raw_client_counters = raw_perf_counters.setdefault(client_id, [])
            del raw_client_counters[:]
            raw_client_counters.extend(counter['c'])

    def process_mds_reports(self):
        for query in self.user_queries.values():
            self.get_raw_perf_counters(query)
            self.get_raw_perf_counters_global(query)

    def scrub_expired_queries(self):
        expire_time = datetime.now() - QUERY_EXPIRE_INTERVAL
        for filter_spec in list(self.user_queries.keys()):
            user_query = self.user_queries[filter_spec]
            self.log.debug("scrubbing query={}".format(user_query))
            if user_query[QUERY_LAST_REQUEST] < expire_time:
                expired_query_ids = user_query[QUERY_IDS].copy()
                expired_query_ids.append(user_query[GLOBAL_QUERY_ID])
                self.log.debug("unregistering query={} ids={}".format(user_query, expired_query_ids))
                self.unregister_mds_perf_queries(filter_spec, expired_query_ids)
                del self.user_queries[filter_spec]

    def prepare_mds_perf_query(self, rank, client_id, client_ip):
        mds_rank_regex = MDS_PERF_QUERY_REGEX_MATCH_ALL_RANKS
        if not rank == -1:
            mds_rank_regex = '^({})$'.format(rank)
        client_regex = MDS_PERF_QUERY_REGEX_MATCH_CLIENTS.format(client_id, client_ip)
        return {
            'key_descriptor' : [
                {'type' : 'mds_rank', 'regex' : mds_rank_regex},
                {'type' : 'client_id', 'regex' : client_regex},
                ],
            'performance_counter_descriptors' : MDS_PERF_QUERY_COUNTERS,
            }

    def prepare_global_perf_query(self, client_id, client_ip):
        client_regex = MDS_PERF_QUERY_REGEX_MATCH_CLIENTS.format(client_id, client_ip)
        return {
            'key_descriptor' : [
                {'type' : 'client_id', 'regex' : client_regex},
                ],
            'performance_counter_descriptors' : MDS_GLOBAL_PERF_QUERY_COUNTERS,
            }

    def unregister_mds_perf_queries(self, filter_spec, query_ids):
        self.log.info("unregister_mds_perf_queries: filter_spec={0}, query_id={1}".format(
            filter_spec, query_ids))
        for query_id in query_ids:
            self.module.remove_mds_perf_query(query_id)

    def register_mds_perf_query(self, filter_spec):
        mds_ranks = filter_spec.mds_ranks
        client_id = filter_spec.client_id
        client_ip = filter_spec.client_ip

        query_ids = []
        try:
            # register per-mds perf query
            for rank in mds_ranks:
                query = self.prepare_mds_perf_query(rank, client_id, client_ip)
                self.log.info("register_mds_perf_query: {}".format(query))

                query_id = self.module.add_mds_perf_query(query)
                if query_id is None: # query id can be 0
                    raise RuntimeError("failed to add MDS perf query: {}".format(query))
                query_ids.append(query_id)
        except Exception:
            for query_id in query_ids:
                self.module.remove_mds_perf_query(query_id)
            raise
        return query_ids

    def register_global_perf_query(self, filter_spec):
        client_id = filter_spec.client_id
        client_ip = filter_spec.client_ip

        # register a global perf query for metrics
        query = self.prepare_global_perf_query(client_id, client_ip)
        self.log.info("register_global_perf_query: {}".format(query))

        query_id = self.module.add_mds_perf_query(query)
        if query_id is None: # query id can be 0
            raise RuntimeError("failed to add global perf query: {}".format(query))
        return query_id

    def register_query(self, filter_spec):
        user_query = self.user_queries.get(filter_spec, None)
        if not user_query:
            user_query = {
                QUERY_IDS : self.register_mds_perf_query(filter_spec),
                GLOBAL_QUERY_ID : self.register_global_perf_query(filter_spec),
                QUERY_LAST_REQUEST : datetime.now(),
                }
            self.user_queries[filter_spec] = user_query

            self.q_cv.notify()
            self.r_cv.wait(5)
        else:
            user_query[QUERY_LAST_REQUEST] = datetime.now()
        return user_query

    def generate_report(self, user_query):
        result = {} # type: Dict
        global fs_list
        # start with counter info -- metrics that are global and per mds
        result["version"] = PERF_STATS_VERSION
        result["global_counters"] = MDS_GLOBAL_PERF_QUERY_COUNTERS
        result["counters"] = MDS_PERF_QUERY_COUNTERS

        # fill in client metadata
        raw_perfs_global = user_query.setdefault(QUERY_RAW_COUNTERS_GLOBAL, {})
        raw_perfs = user_query.setdefault(QUERY_RAW_COUNTERS, {})
        with self.meta_lock:
            raw_counters_clients = []
            for val in raw_perfs.values():
                raw_counters_clients.extend(list(val[1]))
            result_meta = result.setdefault("client_metadata", {})
            for fs_name in fs_list:
                meta = self.client_metadata["metadata"]
                if fs_name in meta and len(meta[fs_name]):
                    for client_id in raw_perfs_global.keys():
                        if client_id in meta[fs_name] and client_id in raw_counters_clients:
                            client_meta = (result_meta.setdefault(fs_name, {})).setdefault(client_id, {})
                            client_meta.update(meta[fs_name][client_id])

            # start populating global perf metrics w/ client metadata
            metrics = result.setdefault("global_metrics", {})
            for fs_name in fs_list:
                if fs_name in meta and len(meta[fs_name]):
                    for client_id, counters in raw_perfs_global.items():
                        if client_id in meta[fs_name] and client_id in raw_counters_clients:
                            global_client_metrics = (metrics.setdefault(fs_name, {})).setdefault(client_id, [])
                            del global_client_metrics[:]
                            global_client_metrics.extend(counters)

            # and, now per-mds metrics keyed by mds rank along with delayed ranks
            metrics = result.setdefault("metrics", {})

            metrics["delayed_ranks"] = [rank for rank, counters in raw_perfs.items() if counters[0]]
            for rank, counters in raw_perfs.items():
                mds_key = "mds.{}".format(rank)
                mds_metrics = metrics.setdefault(mds_key, {})
                mds_metrics.update(counters[1])
        return result

    def extract_query_filters(self, cmd):
        mds_rank_spec = cmd.get('mds_rank', None)
        client_id_spec = cmd.get('client_id', None)
        client_ip_spec = cmd.get('client_ip', None)

        self.log.debug("mds_rank_spec={0}, client_id_spec={1}, client_ip_spec={2}".format(
            mds_rank_spec, client_id_spec, client_ip_spec))

        mds_ranks = extract_mds_ranks_from_spec(mds_rank_spec)
        client_id = extract_client_id_from_spec(client_id_spec)
        client_ip = extract_client_ip_from_spec(client_ip_spec)

        return FilterSpec(mds_ranks, client_id, client_ip)

    def get_perf_data(self, cmd):
        try:
            filter_spec = self.extract_query_filters(cmd)
        except ValueError as e:
            return -errno.EINVAL, "", str(e)

        counters = {}
        with self.lock:
            user_query = self.register_query(filter_spec)
            result = self.generate_report(user_query)
        return 0, json.dumps(result), ""
