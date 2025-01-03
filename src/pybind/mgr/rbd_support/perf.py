import errno
import json
import rados
import rbd
import time
import traceback

from datetime import datetime, timedelta
from threading import Condition, Lock, Thread
from typing import cast, Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .common import (GLOBAL_POOL_KEY, authorize_request, extract_pool_key,
                     get_rbd_pools, PoolKeyT)

QUERY_POOL_ID = "pool_id"
QUERY_POOL_ID_MAP = "pool_id_map"
QUERY_IDS = "query_ids"
QUERY_SUM_POOL_COUNTERS = "pool_counters"
QUERY_RAW_POOL_COUNTERS = "raw_pool_counters"
QUERY_LAST_REQUEST = "last_request"

OSD_PERF_QUERY_REGEX_MATCH_ALL = '^(.*)$'
OSD_PERF_QUERY_COUNTERS = ['write_ops',
                           'read_ops',
                           'write_bytes',
                           'read_bytes',
                           'write_latency',
                           'read_latency']
OSD_PERF_QUERY_COUNTERS_INDICES = {
    OSD_PERF_QUERY_COUNTERS[i]: i for i in range(len(OSD_PERF_QUERY_COUNTERS))}

OSD_PERF_QUERY_LATENCY_COUNTER_INDICES = [4, 5]
OSD_PERF_QUERY_MAX_RESULTS = 256

POOL_REFRESH_INTERVAL = timedelta(minutes=5)
QUERY_EXPIRE_INTERVAL = timedelta(minutes=1)
STATS_RATE_INTERVAL = timedelta(minutes=1)

REPORT_MAX_RESULTS = 64


# {(pool_id, namespace)...}
ResolveImageNamesT = Set[Tuple[int, str]]

# (time, [value,...])
PerfCounterT = Tuple[int, List[int]]
# current, previous
RawImageCounterT = Tuple[PerfCounterT, Optional[PerfCounterT]]
# image_id => perf_counter
RawImagesCounterT = Dict[str, RawImageCounterT]
# namespace_counters => raw_images
RawNamespacesCountersT = Dict[str, RawImagesCounterT]
# pool_id => namespaces_counters
RawPoolCountersT = Dict[int, RawNamespacesCountersT]

SumImageCounterT = List[int]
# image_id => sum_image
SumImagesCounterT = Dict[str, SumImageCounterT]
# namespace => sum_images
SumNamespacesCountersT = Dict[str, SumImagesCounterT]
# pool_id, sum_namespaces
SumPoolCountersT = Dict[int, SumNamespacesCountersT]

ExtractDataFuncT = Callable[[int, Optional[RawImageCounterT], SumImageCounterT], float]


class PerfHandler:

    @classmethod
    def prepare_regex(cls, value: Any) -> str:
        return '^({})$'.format(value)

    @classmethod
    def prepare_osd_perf_query(cls,
                               pool_id: Optional[int],
                               namespace: Optional[str],
                               counter_type: str) -> Dict[str, Any]:
        pool_id_regex = OSD_PERF_QUERY_REGEX_MATCH_ALL
        namespace_regex = OSD_PERF_QUERY_REGEX_MATCH_ALL
        if pool_id:
            pool_id_regex = cls.prepare_regex(pool_id)
            if namespace:
                namespace_regex = cls.prepare_regex(namespace)

        return {
            'key_descriptor': [
                {'type': 'pool_id', 'regex': pool_id_regex},
                {'type': 'namespace', 'regex': namespace_regex},
                {'type': 'object_name',
                 'regex': '^(?:rbd|journal)_data\\.(?:([0-9]+)\\.)?([^.]+)\\.'},
            ],
            'performance_counter_descriptors': OSD_PERF_QUERY_COUNTERS,
            'limit': {'order_by': counter_type,
                      'max_count': OSD_PERF_QUERY_MAX_RESULTS},
        }

    @classmethod
    def pool_spec_search_keys(cls, pool_key: str) -> List[str]:
        return [pool_key[0:len(pool_key) - x]
                for x in range(0, len(pool_key) + 1)]

    @classmethod
    def submatch_pool_key(cls, pool_key: PoolKeyT, search_key: str) -> bool:
        return ((pool_key[1] == search_key[1] or not search_key[1])
                and (pool_key[0] == search_key[0] or not search_key[0]))

    def __init__(self, module: Any) -> None:
        self.user_queries: Dict[PoolKeyT, Dict[str, Any]] = {}
        self.image_cache: Dict[str, str] = {}

        self.lock = Lock()
        self.query_condition = Condition(self.lock)
        self.refresh_condition = Condition(self.lock)

        self.image_name_cache: Dict[Tuple[int, str], Dict[str, str]] = {}
        self.image_name_refresh_time = datetime.fromtimestamp(0)

        self.module = module
        self.log = module.log

        self.stop_thread = False
        self.thread = Thread(target=self.run)

    def setup(self) -> None:
        self.thread.start()

    def shutdown(self) -> None:
        self.log.info("PerfHandler: shutting down")
        self.stop_thread = True
        if self.thread.is_alive():
            self.log.debug("PerfHandler: joining thread")
            self.thread.join()
        self.log.info("PerfHandler: shut down")

    def run(self) -> None:
        try:
            self.log.info("PerfHandler: starting")
            while not self.stop_thread:
                with self.lock:
                    self.scrub_expired_queries()
                    self.process_raw_osd_perf_counters()
                    self.refresh_condition.notify()

                    stats_period = self.module.get_ceph_option("mgr_stats_period")
                    self.query_condition.wait(stats_period)

                self.log.debug("PerfHandler: tick")

        except (rados.ConnectionShutdown, rbd.ConnectionShutdown):
            self.log.exception("PerfHandler: client blocklisted")
            self.module.client_blocklisted.set()
        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def merge_raw_osd_perf_counters(self,
                                    pool_key: PoolKeyT,
                                    query: Dict[str, Any],
                                    now_ts: int,
                                    resolve_image_names: ResolveImageNamesT) -> RawPoolCountersT:
        pool_id_map = query[QUERY_POOL_ID_MAP]

        # collect and combine the raw counters from all sort orders
        raw_pool_counters: Dict[int, Dict[str, Dict[str, Any]]] = query.setdefault(QUERY_RAW_POOL_COUNTERS, {})
        for query_id in query[QUERY_IDS]:
            res = self.module.get_osd_perf_counters(query_id)
            for counter in res['counters']:
                # replace pool id from object name if it exists
                k = counter['k']
                pool_id = int(k[2][0]) if k[2][0] else int(k[0][0])
                namespace = k[1][0]
                image_id = k[2][1]

                # ignore metrics from non-matching pools/namespaces
                if pool_id not in pool_id_map:
                    continue
                if pool_key[1] is not None and pool_key[1] != namespace:
                    continue

                # flag the pool (and namespace) for refresh if we cannot find
                # image name in the cache
                resolve_image_key = (pool_id, namespace)
                if image_id not in self.image_name_cache.get(resolve_image_key, {}):
                    resolve_image_names.add(resolve_image_key)

                # copy the 'sum' counter values for each image (ignore count)
                # if we haven't already processed it for this round
                raw_namespaces = raw_pool_counters.setdefault(pool_id, {})
                raw_images = raw_namespaces.setdefault(namespace, {})
                raw_image = raw_images.get(image_id)
                # save the last two perf counters for each image
                new_current = (now_ts, [int(x[0]) for x in counter['c']])
                if raw_image:
                    old_current, _ = raw_image
                    if old_current[0] < now_ts:
                        raw_images[image_id] = (new_current, old_current)
                else:
                    raw_images[image_id] = (new_current, None)

        self.log.debug("merge_raw_osd_perf_counters: {}".format(raw_pool_counters))
        return raw_pool_counters

    def sum_osd_perf_counters(self,
                              query: Dict[str, dict],
                              raw_pool_counters: RawPoolCountersT,
                              now_ts: int) -> SumPoolCountersT:
        # update the cumulative counters for each image
        sum_pool_counters = query.setdefault(QUERY_SUM_POOL_COUNTERS, {})
        for pool_id, raw_namespaces in raw_pool_counters.items():
            sum_namespaces = sum_pool_counters.setdefault(pool_id, {})
            for namespace, raw_images in raw_namespaces.items():
                sum_namespace = sum_namespaces.setdefault(namespace, {})
                for image_id, raw_image in raw_images.items():
                    # zero-out non-updated raw counters
                    if not raw_image[0]:
                        continue
                    old_current, _ = raw_image
                    if old_current[0] < now_ts:
                        new_current = (now_ts, [0] * len(old_current[1]))
                        raw_images[image_id] = (new_current, old_current)
                        continue

                    counters = old_current[1]

                    # copy raw counters if this is a newly discovered image or
                    # increment existing counters
                    sum_image = sum_namespace.setdefault(image_id, None)
                    if sum_image:
                        for i in range(len(counters)):
                            sum_image[i] += counters[i]
                    else:
                        sum_namespace[image_id] = [x for x in counters]

        self.log.debug("sum_osd_perf_counters: {}".format(sum_pool_counters))
        return sum_pool_counters

    def refresh_image_names(self, resolve_image_names: ResolveImageNamesT) -> None:
        for pool_id, namespace in resolve_image_names:
            image_key = (pool_id, namespace)
            images = self.image_name_cache.setdefault(image_key, {})
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                for image_meta in rbd.RBD().list2(ioctx):
                    images[image_meta['id']] = image_meta['name']
            self.log.debug("resolve_image_names: {}={}".format(image_key, images))

    def scrub_missing_images(self) -> None:
        for pool_key, query in self.user_queries.items():
            raw_pool_counters = query.get(QUERY_RAW_POOL_COUNTERS, {})
            sum_pool_counters = query.get(QUERY_SUM_POOL_COUNTERS, {})
            for pool_id, sum_namespaces in sum_pool_counters.items():
                raw_namespaces = raw_pool_counters.get(pool_id, {})
                for namespace, sum_images in sum_namespaces.items():
                    raw_images = raw_namespaces.get(namespace, {})

                    image_key = (pool_id, namespace)
                    image_names = self.image_name_cache.get(image_key, {})
                    for image_id in list(sum_images.keys()):
                        # scrub image counters if we failed to resolve image name
                        if image_id not in image_names:
                            self.log.debug("scrub_missing_images: dropping {}/{}".format(
                                image_key, image_id))
                            del sum_images[image_id]
                            if image_id in raw_images:
                                del raw_images[image_id]

    def process_raw_osd_perf_counters(self) -> None:
        now = datetime.now()
        now_ts = int(now.strftime("%s"))

        # clear the image name cache if we need to refresh all active pools
        if self.image_name_cache and \
                self.image_name_refresh_time + POOL_REFRESH_INTERVAL < now:
            self.log.debug("process_raw_osd_perf_counters: expiring image name cache")
            self.image_name_cache = {}

        resolve_image_names: Set[Tuple[int, str]] = set()
        for pool_key, query in self.user_queries.items():
            if not query[QUERY_IDS]:
                continue

            raw_pool_counters = self.merge_raw_osd_perf_counters(
                pool_key, query, now_ts, resolve_image_names)
            self.sum_osd_perf_counters(query, raw_pool_counters, now_ts)

        if resolve_image_names:
            self.image_name_refresh_time = now
            self.refresh_image_names(resolve_image_names)
            self.scrub_missing_images()
        elif not self.image_name_cache:
            self.scrub_missing_images()

    def resolve_pool_id(self, pool_name: str) -> int:
        pool_id = self.module.rados.pool_lookup(pool_name)
        if not pool_id:
            raise rados.ObjectNotFound("Pool '{}' not found".format(pool_name),
                                       errno.ENOENT)
        return pool_id

    def scrub_expired_queries(self) -> None:
        # perf counters need to be periodically refreshed to continue
        # to be registered
        expire_time = datetime.now() - QUERY_EXPIRE_INTERVAL
        for pool_key in list(self.user_queries.keys()):
            user_query = self.user_queries[pool_key]
            if user_query[QUERY_LAST_REQUEST] < expire_time:
                self.unregister_osd_perf_queries(pool_key, user_query[QUERY_IDS])
                del self.user_queries[pool_key]

    def register_osd_perf_queries(self,
                                  pool_id: Optional[int],
                                  namespace: Optional[str]) -> List[int]:
        query_ids = []
        try:
            for counter in OSD_PERF_QUERY_COUNTERS:
                query = self.prepare_osd_perf_query(pool_id, namespace, counter)
                self.log.debug("register_osd_perf_queries: {}".format(query))

                query_id = self.module.add_osd_perf_query(query)
                if query_id is None:
                    raise RuntimeError('Failed to add OSD perf query: {}'.format(query))
                query_ids.append(query_id)

        except Exception:
            for query_id in query_ids:
                self.module.remove_osd_perf_query(query_id)
            raise

        return query_ids

    def unregister_osd_perf_queries(self, pool_key: PoolKeyT, query_ids: List[int]) -> None:
        self.log.info("unregister_osd_perf_queries: pool_key={}, query_ids={}".format(
            pool_key, query_ids))
        for query_id in query_ids:
            self.module.remove_osd_perf_query(query_id)
        query_ids[:] = []

    def register_query(self, pool_key: PoolKeyT) -> Dict[str, Any]:
        if pool_key not in self.user_queries:
            pool_name, namespace = pool_key
            pool_id = None
            if pool_name:
                pool_id = self.resolve_pool_id(cast(str, pool_name))

            user_query = {
                QUERY_POOL_ID: pool_id,
                QUERY_POOL_ID_MAP: {pool_id: pool_name},
                QUERY_IDS: self.register_osd_perf_queries(pool_id, namespace),
                QUERY_LAST_REQUEST: datetime.now()
            }

            self.user_queries[pool_key] = user_query

            # force an immediate stat pull if this is a new query
            self.query_condition.notify()
            self.refresh_condition.wait(5)

        else:
            user_query = self.user_queries[pool_key]

            # ensure query doesn't expire
            user_query[QUERY_LAST_REQUEST] = datetime.now()

            if pool_key == GLOBAL_POOL_KEY:
                # refresh the global pool id -> name map upon each
                # processing period
                user_query[QUERY_POOL_ID_MAP] = {
                    pool_id: pool_name for pool_id, pool_name
                    in get_rbd_pools(self.module).items()}

        self.log.debug("register_query: pool_key={}, query_ids={}".format(
            pool_key, user_query[QUERY_IDS]))

        return user_query

    def extract_stat(self,
                     index: int,
                     raw_image: Optional[RawImageCounterT],
                     sum_image: Any) -> float:
        # require two raw counters between a fixed time window
        if not raw_image or not raw_image[0] or not raw_image[1]:
            return 0

        current_counter, previous_counter = cast(Tuple[PerfCounterT, PerfCounterT], raw_image)
        current_time = current_counter[0]
        previous_time = previous_counter[0]
        if current_time <= previous_time or \
                current_time - previous_time > STATS_RATE_INTERVAL.total_seconds():
            return 0

        current_value = current_counter[1][index]
        instant_rate = float(current_value) / (current_time - previous_time)

        # convert latencies from sum to average per op
        ops_index = None
        if OSD_PERF_QUERY_COUNTERS[index] == 'write_latency':
            ops_index = OSD_PERF_QUERY_COUNTERS_INDICES['write_ops']
        elif OSD_PERF_QUERY_COUNTERS[index] == 'read_latency':
            ops_index = OSD_PERF_QUERY_COUNTERS_INDICES['read_ops']

        if ops_index is not None:
            ops = max(1, self.extract_stat(ops_index, raw_image, sum_image))
            instant_rate /= ops

        return instant_rate

    def extract_counter(self,
                        index: int,
                        raw_image: Optional[RawImageCounterT],
                        sum_image: List[int]) -> int:
        if sum_image:
            return sum_image[index]
        return 0

    def generate_report(self,
                        query: Dict[str, Union[Dict[str, str],
                                               Dict[int, Dict[str, dict]]]],
                        sort_by: str,
                        extract_data: ExtractDataFuncT) -> Tuple[Dict[int, str],
                                                                 List[Dict[str, List[float]]]]:
        pool_id_map = cast(Dict[int, str], query[QUERY_POOL_ID_MAP])
        sum_pool_counters = cast(SumPoolCountersT,
                                 query.setdefault(QUERY_SUM_POOL_COUNTERS,
                                                  cast(SumPoolCountersT, {})))
        # pool_id => {namespace => {image_id => [counter..] }
        raw_pool_counters = cast(RawPoolCountersT,
                                 query.setdefault(QUERY_RAW_POOL_COUNTERS,
                                                  cast(RawPoolCountersT, {})))

        sort_by_index = OSD_PERF_QUERY_COUNTERS.index(sort_by)

        # pre-sort and limit the response
        results = []
        for pool_id, sum_namespaces in sum_pool_counters.items():
            if pool_id not in pool_id_map:
                continue
            raw_namespaces: RawNamespacesCountersT = raw_pool_counters.get(pool_id, {})
            for namespace, sum_images in sum_namespaces.items():
                raw_images = raw_namespaces.get(namespace, {})
                for image_id, sum_image in sum_images.items():
                    raw_image = raw_images.get(image_id)

                    # always sort by recent IO activity
                    results.append(((pool_id, namespace, image_id),
                                    self.extract_stat(sort_by_index, raw_image,
                                                      sum_image)))
        results = sorted(results, key=lambda x: x[1], reverse=True)[:REPORT_MAX_RESULTS]

        # build the report in sorted order
        pool_descriptors: Dict[str, int] = {}
        counters = []
        for key, _ in results:
            pool_id = key[0]
            pool_name = pool_id_map[pool_id]

            namespace = key[1]
            image_id = key[2]
            image_names = self.image_name_cache.get((pool_id, namespace), {})
            image_name = image_names[image_id]

            raw_namespaces = raw_pool_counters.get(pool_id, {})
            raw_images = raw_namespaces.get(namespace, {})
            raw_image = raw_images.get(image_id)

            sum_namespaces = sum_pool_counters[pool_id]
            sum_images = sum_namespaces[namespace]
            sum_image = sum_images.get(image_id, [])

            pool_descriptor = pool_name
            if namespace:
                pool_descriptor += "/{}".format(namespace)
            pool_index = pool_descriptors.setdefault(pool_descriptor,
                                                     len(pool_descriptors))
            image_descriptor = "{}/{}".format(pool_index, image_name)
            data = [extract_data(i, raw_image, sum_image)
                    for i in range(len(OSD_PERF_QUERY_COUNTERS))]

            # skip if no data to report
            if data == [0 for i in range(len(OSD_PERF_QUERY_COUNTERS))]:
                continue

            counters.append({image_descriptor: data})

        return {idx: descriptor for descriptor, idx
                in pool_descriptors.items()}, \
            counters

    def get_perf_data(self,
                      report: str,
                      pool_spec: Optional[str],
                      sort_by: str,
                      extract_data: ExtractDataFuncT) -> Tuple[int, str, str]:
        self.log.debug("get_perf_{}s: pool_spec={}, sort_by={}".format(
            report, pool_spec, sort_by))
        self.scrub_expired_queries()

        pool_key = extract_pool_key(pool_spec)
        authorize_request(self.module, pool_key[0], pool_key[1])
        user_query = self.register_query(pool_key)

        now = datetime.now()
        pool_descriptors, counters = self.generate_report(
            user_query, sort_by, extract_data)

        report = {
            'timestamp': time.mktime(now.timetuple()),
            '{}_descriptors'.format(report): OSD_PERF_QUERY_COUNTERS,
            'pool_descriptors': pool_descriptors,
            '{}s'.format(report): counters
        }

        return 0, json.dumps(report), ""

    def get_perf_stats(self,
                       pool_spec: Optional[str],
                       sort_by: str) -> Tuple[int, str, str]:
        return self.get_perf_data(
            "stat", pool_spec, sort_by, self.extract_stat)

    def get_perf_counters(self,
                          pool_spec: Optional[str],
                          sort_by: str) -> Tuple[int, str, str]:
        return self.get_perf_data(
            "counter", pool_spec, sort_by, self.extract_counter)
