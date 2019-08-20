"""
RBD support module
"""

import errno
import json
import rados
import rbd
import re
import time
import traceback
import uuid

from mgr_module import MgrModule

from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial, wraps
from threading import Condition, Lock, Thread


GLOBAL_POOL_KEY = (None, None)

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

RBD_TASK_OID = "rbd_task"

TASK_SEQUENCE = "sequence"
TASK_ID = "id"
TASK_REFS = "refs"
TASK_MESSAGE = "message"
TASK_RETRY_TIME = "retry_time"
TASK_IN_PROGRESS = "in_progress"
TASK_PROGRESS = "progress"
TASK_CANCELED = "canceled"

TASK_REF_POOL_NAME = "pool_name"
TASK_REF_POOL_NAMESPACE = "pool_namespace"
TASK_REF_IMAGE_NAME = "image_name"
TASK_REF_IMAGE_ID = "image_id"
TASK_REF_ACTION = "action"

TASK_REF_ACTION_FLATTEN = "flatten"
TASK_REF_ACTION_REMOVE = "remove"
TASK_REF_ACTION_TRASH_REMOVE = "trash remove"
TASK_REF_ACTION_MIGRATION_EXECUTE = "migrate execute"
TASK_REF_ACTION_MIGRATION_COMMIT = "migrate commit"
TASK_REF_ACTION_MIGRATION_ABORT = "migrate abort"

VALID_TASK_ACTIONS = [TASK_REF_ACTION_FLATTEN,
                      TASK_REF_ACTION_REMOVE,
                      TASK_REF_ACTION_TRASH_REMOVE,
                      TASK_REF_ACTION_MIGRATION_EXECUTE,
                      TASK_REF_ACTION_MIGRATION_COMMIT,
                      TASK_REF_ACTION_MIGRATION_ABORT]

TASK_RETRY_INTERVAL = timedelta(seconds=30)
MAX_COMPLETED_TASKS = 50


def extract_pool_key(pool_spec):
    if not pool_spec:
        return GLOBAL_POOL_KEY

    match = re.match(r'^([^/]+)(?:/([^/]+))?$', pool_spec)
    if not match:
        raise ValueError("Invalid pool spec: {}".format(pool_spec))
    return (match.group(1), match.group(2) or '')


def get_rbd_pools(module):
    osd_map = module.get('osd_map')
    return {pool['pool']: pool['pool_name'] for pool in osd_map['pools']
            if 'rbd' in pool.get('application_metadata', {})}


class PerfHandler:
    user_queries = {}
    image_cache = {}

    lock = Lock()
    query_condition = Condition(lock)
    refresh_condition = Condition(lock)
    thread = None

    image_name_cache = {}
    image_name_refresh_time = datetime.fromtimestamp(0)

    @classmethod
    def prepare_regex(cls, value):
        return '^({})$'.format(value)

    @classmethod
    def prepare_osd_perf_query(cls, pool_id, namespace, counter_type):
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
    def pool_spec_search_keys(cls, pool_key):
        return [pool_key[0:len(pool_key) - x]
                for x in range(0, len(pool_key) + 1)]

    @classmethod
    def submatch_pool_key(cls, pool_key, search_key):
        return ((pool_key[1] == search_key[1] or not search_key[1])
                and (pool_key[0] == search_key[0] or not search_key[0]))

    def __init__(self, module):
        self.module = module
        self.log = module.log

        self.thread = Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            self.log.info("PerfHandler: starting")
            while True:
                with self.lock:
                    self.scrub_expired_queries()
                    self.process_raw_osd_perf_counters()
                    self.refresh_condition.notify()

                    stats_period = int(self.module.get_ceph_option("mgr_stats_period"))
                    self.query_condition.wait(stats_period)

                self.log.debug("PerfHandler: tick")

        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def merge_raw_osd_perf_counters(self, pool_key, query, now_ts,
                                    resolve_image_names):
        pool_id_map = query[QUERY_POOL_ID_MAP]

        # collect and combine the raw counters from all sort orders
        raw_pool_counters = query.setdefault(QUERY_RAW_POOL_COUNTERS, {})
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
                raw_image = raw_images.setdefault(image_id, [None, None])

                # save the last two perf counters for each image
                if raw_image[0] and raw_image[0][0] < now_ts:
                    raw_image[1] = raw_image[0]
                    raw_image[0] = None
                if not raw_image[0]:
                    raw_image[0] = [now_ts, [int(x[0]) for x in counter['c']]]

        self.log.debug("merge_raw_osd_perf_counters: {}".format(raw_pool_counters))
        return raw_pool_counters

    def sum_osd_perf_counters(self, query, raw_pool_counters, now_ts):
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
                    elif raw_image[0][0] < now_ts:
                        raw_image[1] = raw_image[0]
                        raw_image[0] = [now_ts, [0 for x in raw_image[1][1]]]
                        continue

                    counters = raw_image[0][1]

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

    def refresh_image_names(self, resolve_image_names):
        for pool_id, namespace in resolve_image_names:
            image_key = (pool_id, namespace)
            images = self.image_name_cache.setdefault(image_key, {})
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                for image_meta in rbd.RBD().list2(ioctx):
                    images[image_meta['id']] = image_meta['name']
            self.log.debug("resolve_image_names: {}={}".format(image_key, images))

    def scrub_missing_images(self):
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

    def process_raw_osd_perf_counters(self):
        now = datetime.now()
        now_ts = int(now.strftime("%s"))

        # clear the image name cache if we need to refresh all active pools
        if self.image_name_cache and \
                self.image_name_refresh_time + POOL_REFRESH_INTERVAL < now:
            self.log.debug("process_raw_osd_perf_counters: expiring image name cache")
            self.image_name_cache = {}

        resolve_image_names = set()
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

    def resolve_pool_id(self, pool_name):
        pool_id = self.module.rados.pool_lookup(pool_name)
        if not pool_id:
            raise rados.ObjectNotFound("Pool '{}' not found".format(pool_name))
        return pool_id

    def scrub_expired_queries(self):
        # perf counters need to be periodically refreshed to continue
        # to be registered
        expire_time = datetime.now() - QUERY_EXPIRE_INTERVAL
        for pool_key in list(self.user_queries.keys()):
            user_query = self.user_queries[pool_key]
            if user_query[QUERY_LAST_REQUEST] < expire_time:
                self.unregister_osd_perf_queries(pool_key, user_query[QUERY_IDS])
                del self.user_queries[pool_key]

    def register_osd_perf_queries(self, pool_id, namespace):
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

    def unregister_osd_perf_queries(self, pool_key, query_ids):
        self.log.info("unregister_osd_perf_queries: pool_key={}, query_ids={}".format(
            pool_key, query_ids))
        for query_id in query_ids:
            self.module.remove_osd_perf_query(query_id)
        query_ids[:] = []

    def register_query(self, pool_key):
        if pool_key not in self.user_queries:
            pool_id = None
            if pool_key[0]:
                pool_id = self.resolve_pool_id(pool_key[0])

            user_query = {
                QUERY_POOL_ID: pool_id,
                QUERY_POOL_ID_MAP: {pool_id: pool_key[0]},
                QUERY_IDS: self.register_osd_perf_queries(pool_id, pool_key[1]),
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

    def extract_stat(self, index, raw_image, sum_image):
        # require two raw counters between a fixed time window
        if not raw_image or not raw_image[0] or not raw_image[1]:
            return 0

        current_time = raw_image[0][0]
        previous_time = raw_image[1][0]
        if current_time <= previous_time or \
                current_time - previous_time > STATS_RATE_INTERVAL.total_seconds():
            return 0

        current_value = raw_image[0][1][index]
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

    def extract_counter(self, index, raw_image, sum_image):
        if sum_image:
            return sum_image[index]
        return 0

    def generate_report(self, query, sort_by, extract_data):
        pool_id_map = query[QUERY_POOL_ID_MAP]
        sum_pool_counters = query.setdefault(QUERY_SUM_POOL_COUNTERS, {})
        raw_pool_counters = query.setdefault(QUERY_RAW_POOL_COUNTERS, {})

        sort_by_index = OSD_PERF_QUERY_COUNTERS.index(sort_by)

        # pre-sort and limit the response
        results = []
        for pool_id, sum_namespaces in sum_pool_counters.items():
            if pool_id not in pool_id_map:
                continue
            raw_namespaces = raw_pool_counters.get(pool_id, {})
            for namespace, sum_images in sum_namespaces.items():
                raw_images = raw_namespaces.get(namespace, {})
                for image_id, sum_image in sum_images.items():
                    raw_image = raw_images.get(image_id, [])

                    # always sort by recent IO activity
                    results.append([(pool_id, namespace, image_id),
                                    self.extract_stat(sort_by_index, raw_image,
                                                      sum_image)])
        results = sorted(results, key=lambda x: x[1], reverse=True)[:REPORT_MAX_RESULTS]

        # build the report in sorted order
        pool_descriptors = {}
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
            raw_image = raw_images.get(image_id, [])

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

    def get_perf_data(self, report, pool_spec, sort_by, extract_data):
        self.log.debug("get_perf_{}s: pool_spec={}, sort_by={}".format(
            report, pool_spec, sort_by))
        self.scrub_expired_queries()

        pool_key = extract_pool_key(pool_spec)
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

    def get_perf_stats(self, pool_spec, sort_by):
        return self.get_perf_data(
            "stat", pool_spec, sort_by, self.extract_stat)

    def get_perf_counters(self, pool_spec, sort_by):
        return self.get_perf_data(
            "counter", pool_spec, sort_by, self.extract_counter)

    def handle_command(self, inbuf, prefix, cmd):
        with self.lock:
            if prefix == 'image stats':
                return self.get_perf_stats(cmd.get('pool_spec', None),
                                           cmd.get('sort_by', OSD_PERF_QUERY_COUNTERS[0]))
            elif prefix == 'image counters':
                return self.get_perf_counters(cmd.get('pool_spec', None),
                                              cmd.get('sort_by', OSD_PERF_QUERY_COUNTERS[0]))

        raise NotImplementedError(cmd['prefix'])


class Throttle:
    def __init__(self, throttle_period):
        self.throttle_period = throttle_period
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            if self.time_of_last_call + self.throttle_period <= now:
                self.time_of_last_call = now
                return fn(*args, **kwargs)
        return wrapper


class Task:
    def __init__(self, sequence, task_id, message, refs):
        self.sequence = sequence
        self.task_id = task_id
        self.message = message
        self.refs = refs
        self.retry_time = None
        self.in_progress = False
        self.progress = 0.0
        self.canceled = False
        self.failed = False

    def __str__(self):
        return self.to_json()

    @property
    def sequence_key(self):
        return "{0:016X}".format(self.sequence)

    def cancel(self):
        self.canceled = True
        self.fail("Operation canceled")

    def fail(self, message):
        self.failed = True
        self.failure_message = message

    def to_dict(self):
        d = {TASK_SEQUENCE: self.sequence,
             TASK_ID: self.task_id,
             TASK_MESSAGE: self.message,
             TASK_REFS: self.refs
             }
        if self.retry_time:
            d[TASK_RETRY_TIME] = self.retry_time.isoformat()
        if self.in_progress:
            d[TASK_IN_PROGRESS] = True
            d[TASK_PROGRESS] = self.progress
        if self.canceled:
            d[TASK_CANCELED] = True
        return d

    def to_json(self):
        return str(json.dumps(self.to_dict()))

    @classmethod
    def from_json(cls, val):
        try:
            d = json.loads(val)
            action = d.get(TASK_REFS, {}).get(TASK_REF_ACTION)
            if action not in VALID_TASK_ACTIONS:
                raise ValueError("Invalid task action: {}".format(action))

            return Task(d[TASK_SEQUENCE], d[TASK_ID], d[TASK_MESSAGE], d[TASK_REFS])
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON ({})".format(str(e)))
        except KeyError as e:
            raise ValueError("Invalid task format (missing key {})".format(str(e)))


class TaskHandler:
    lock = Lock()
    condition = Condition(lock)
    thread = None

    in_progress_task = None
    tasks_by_sequence = dict()
    tasks_by_id = dict()

    completed_tasks = []

    sequence = 0

    def __init__(self, module):
        self.module = module
        self.log = module.log

        with self.lock:
            self.init_task_queue()

        self.thread = Thread(target=self.run)
        self.thread.start()

    @property
    def default_pool_name(self):
        return self.module.get_ceph_option("rbd_default_pool")

    def extract_pool_spec(self, pool_spec):
        pool_spec = extract_pool_key(pool_spec)
        if pool_spec == GLOBAL_POOL_KEY:
            pool_spec = (self.default_pool_name, '')
        return pool_spec

    def extract_image_spec(self, image_spec):
        match = re.match(r'^(?:([^/]+)/(?:([^/]+)/)?)?([^/@]+)$',
                         image_spec or '')
        if not match:
            raise ValueError("Invalid image spec: {}".format(image_spec))
        return (match.group(1) or self.default_pool_name, match.group(2) or '',
                match.group(3))

    def run(self):
        try:
            self.log.info("TaskHandler: starting")
            while True:
                with self.lock:
                    now = datetime.now()
                    for sequence in sorted([sequence for sequence, task
                                            in self.tasks_by_sequence.items()
                                            if not task.retry_time or task.retry_time <= now]):
                        self.execute_task(sequence)

                    self.condition.wait(5)
                    self.log.debug("TaskHandler: tick")

        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    @contextmanager
    def open_ioctx(self, spec):
        try:
            with self.module.rados.open_ioctx(spec[0]) as ioctx:
                ioctx.set_namespace(spec[1])
                yield ioctx
        except rados.ObjectNotFound:
            self.log.error("Failed to locate pool {}".format(spec[0]))
            raise

    @classmethod
    def format_image_spec(cls, image_spec):
        image = image_spec[2]
        if image_spec[1]:
            image = "{}/{}".format(image_spec[1], image)
        if image_spec[0]:
            image = "{}/{}".format(image_spec[0], image)
        return image

    def init_task_queue(self):
        for pool_id, pool_name in get_rbd_pools(self.module).items():
            try:
                with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                    self.load_task_queue(ioctx, pool_name)

                    try:
                        namespaces = rbd.RBD().namespace_list(ioctx)
                    except rbd.OperationNotSupported:
                        self.log.debug("Namespaces not supported")
                        continue

                    for namespace in namespaces:
                        ioctx.set_namespace(namespace)
                        self.load_task_queue(ioctx, pool_name)

            except rados.ObjectNotFound:
                # pool DNE
                pass

        if self.tasks_by_sequence:
            self.sequence = list(sorted(self.tasks_by_sequence.keys()))[-1]

        self.log.debug("sequence={}, tasks_by_sequence={}, tasks_by_id={}".format(
            self.sequence, str(self.tasks_by_sequence), str(self.tasks_by_id)))

    def load_task_queue(self, ioctx, pool_name):
        pool_spec = pool_name
        if ioctx.nspace:
            pool_spec += "/{}".format(ioctx.nspace)

        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.log.info("load_task_task: {}, start_after={}".format(
                        pool_spec, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, RBD_TASK_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        v = v.decode()
                        self.log.info("load_task_task: task={}".format(v))

                        try:
                            task = Task.from_json(v)
                            self.append_task(task)
                        except ValueError:
                            self.log.error("Failed to decode task: pool_spec={}, task={}".format(pool_spec, v))

                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            # rbd_task DNE
            pass

    def append_task(self, task):
        self.tasks_by_sequence[task.sequence] = task
        self.tasks_by_id[task.task_id] = task

    def task_refs_match(self, task_refs, refs):
        if TASK_REF_IMAGE_ID not in refs and TASK_REF_IMAGE_ID in task_refs:
            task_refs = task_refs.copy()
            del task_refs[TASK_REF_IMAGE_ID]

        self.log.debug("task_refs_match: ref1={}, ref2={}".format(task_refs, refs))
        return task_refs == refs

    def find_task(self, refs):
        self.log.debug("find_task: refs={}".format(refs))

        # search for dups and return the original
        for task_id in reversed(sorted(self.tasks_by_id.keys())):
            task = self.tasks_by_id[task_id]
            if self.task_refs_match(task.refs, refs):
                return task

        # search for a completed task (message replay)
        for task in reversed(self.completed_tasks):
            if self.task_refs_match(task.refs, refs):
                return task

    def add_task(self, ioctx, message, refs):
        self.log.debug("add_task: message={}, refs={}".format(message, refs))

        # ensure unique uuid across all pools
        while True:
            task_id = str(uuid.uuid4())
            if task_id not in self.tasks_by_id:
                break

        self.sequence += 1
        task = Task(self.sequence, task_id, message, refs)

        # add the task to the rbd_task omap
        task_json = task.to_json()
        omap_keys = (task.sequence_key, )
        omap_vals = (str.encode(task_json), )
        self.log.info("adding task: {} {}".format(omap_keys[0], omap_vals[0]))

        with rados.WriteOpCtx() as write_op:
            ioctx.set_omap(write_op, omap_keys, omap_vals)
            ioctx.operate_write_op(write_op, RBD_TASK_OID)
        self.append_task(task)

        self.condition.notify()
        return task_json

    def remove_task(self, ioctx, task, remove_in_memory=True):
        self.log.info("remove_task: task={}".format(str(task)))
        omap_keys = (task.sequence_key, )
        try:
            with rados.WriteOpCtx() as write_op:
                ioctx.remove_omap_keys(write_op, omap_keys)
                ioctx.operate_write_op(write_op, RBD_TASK_OID)
        except rados.ObjectNotFound:
            pass

        if remove_in_memory:
            try:
                del self.tasks_by_id[task.task_id]
                del self.tasks_by_sequence[task.sequence]

                # keep a record of the last N tasks to help avoid command replay
                # races
                if not task.failed and not task.canceled:
                    self.log.debug("remove_task: moving to completed tasks")
                    self.completed_tasks.append(task)
                    self.completed_tasks = self.completed_tasks[-MAX_COMPLETED_TASKS:]

            except KeyError:
                pass

    def execute_task(self, sequence):
        task = self.tasks_by_sequence[sequence]
        self.log.info("execute_task: task={}".format(str(task)))

        pool_valid = False
        try:
            with self.open_ioctx((task.refs[TASK_REF_POOL_NAME],
                                  task.refs[TASK_REF_POOL_NAMESPACE])) as ioctx:
                pool_valid = True

                action = task.refs[TASK_REF_ACTION]
                execute_fn = {TASK_REF_ACTION_FLATTEN: self.execute_flatten,
                              TASK_REF_ACTION_REMOVE: self.execute_remove,
                              TASK_REF_ACTION_TRASH_REMOVE: self.execute_trash_remove,
                              TASK_REF_ACTION_MIGRATION_EXECUTE: self.execute_migration_execute,
                              TASK_REF_ACTION_MIGRATION_COMMIT: self.execute_migration_commit,
                              TASK_REF_ACTION_MIGRATION_ABORT: self.execute_migration_abort
                              }.get(action)
                if not execute_fn:
                    self.log.error("Invalid task action: {}".format(action))
                else:
                    task.in_progress = True
                    self.in_progress_task = task
                    self.update_progress(task, 0)

                    self.lock.release()
                    try:
                        execute_fn(ioctx, task)

                    except rbd.OperationCanceled:
                        self.log.info("Operation canceled: task={}".format(
                            str(task)))

                    finally:
                        self.lock.acquire()

                        task.in_progress = False
                        self.in_progress_task = None

                    self.complete_progress(task)
                    self.remove_task(ioctx, task)

        except rados.ObjectNotFound as e:
            self.log.error("execute_task: {}".format(e))
            if pool_valid:
                self.update_progress(task, 0)
            else:
                # pool DNE -- remove the task
                self.complete_progress(task)
                self.remove_task(ioctx, task)

        except (rados.Error, rbd.Error) as e:
            self.log.error("execute_task: {}".format(e))
            self.update_progress(task, 0)

        finally:
            task.in_progress = False
            task.retry_time = datetime.now() + TASK_RETRY_INTERVAL

    def progress_callback(self, task, current, total):
        progress = float(current) / float(total)
        self.log.debug("progress_callback: task={}, progress={}".format(
            str(task), progress))

        # avoid deadlocking when a new command comes in during a progress callback
        if not self.lock.acquire(False):
            return 0

        try:
            if not self.in_progress_task or self.in_progress_task.canceled:
                return -rbd.ECANCELED
            self.in_progress_task.progress = progress
        finally:
            self.lock.release()

        self.throttled_update_progress(task, progress)
        return 0

    def execute_flatten(self, ioctx, task):
        self.log.info("execute_flatten: task={}".format(str(task)))

        try:
            with rbd.Image(ioctx, task.refs[TASK_REF_IMAGE_NAME]) as image:
                image.flatten(on_progress=partial(self.progress_callback, task))
        except rbd.InvalidArgument:
            task.fail("Image does not have parent")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_remove(self, ioctx, task):
        self.log.info("execute_remove: task={}".format(str(task)))

        try:
            rbd.RBD().remove(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                             on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_trash_remove(self, ioctx, task):
        self.log.info("execute_trash_remove: task={}".format(str(task)))

        try:
            rbd.RBD().trash_remove(ioctx, task.refs[TASK_REF_IMAGE_ID],
                                   on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_execute(self, ioctx, task):
        self.log.info("execute_migration_execute: task={}".format(str(task)))

        try:
            rbd.RBD().migration_execute(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                        on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_commit(self, ioctx, task):
        self.log.info("execute_migration_commit: task={}".format(str(task)))

        try:
            rbd.RBD().migration_commit(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                       on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating or migration not executed")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_abort(self, ioctx, task):
        self.log.info("execute_migration_abort: task={}".format(str(task)))

        try:
            rbd.RBD().migration_abort(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                      on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def complete_progress(self, task):
        self.log.debug("complete_progress: task={}".format(str(task)))
        try:
            if task.failed:
                self.module.remote("progress", "fail", task.task_id,
                                   task.failure_message)
            else:
                self.module.remote("progress", "complete", task.task_id)
        except ImportError:
            # progress module is disabled
            pass

    def update_progress(self, task, progress):
        self.log.debug("update_progress: task={}, progress={}".format(str(task), progress))
        try:
            refs = {"origin": "rbd_support"}
            refs.update(task.refs)

            self.module.remote("progress", "update", task.task_id,
                               task.message, progress, refs)
        except ImportError:
            # progress module is disabled
            pass

    @Throttle(timedelta(seconds=1))
    def throttled_update_progress(self, task, progress):
        self.update_progress(task, progress)

    def queue_flatten(self, image_spec):
        image_spec = self.extract_image_spec(image_spec)
        self.log.info("queue_flatten: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_FLATTEN,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec) as ioctx:
            try:
                with rbd.Image(ioctx, image_spec[2]) as image:
                    refs[TASK_REF_IMAGE_ID] = image.id()

                    try:
                        parent_image_id = image.parent_id()
                    except rbd.ImageNotFound:
                        parent_image_id = None

            except rbd.ImageNotFound:
                pass

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            if TASK_REF_IMAGE_ID not in refs:
                raise rbd.ImageNotFound("Image {} does not exist".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)
            if not parent_image_id:
                raise rbd.ImageNotFound("Image {} does not have a parent".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)

            return 0, self.add_task(ioctx,
                                    "Flattening image {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ""

    def queue_remove(self, image_spec):
        image_spec = self.extract_image_spec(image_spec)
        self.log.info("queue_remove: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_REMOVE,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec) as ioctx:
            try:
                with rbd.Image(ioctx, image_spec[2]) as image:
                    refs[TASK_REF_IMAGE_ID] = image.id()
                    snaps = list(image.list_snaps())

            except rbd.ImageNotFound:
                pass

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            if TASK_REF_IMAGE_ID not in refs:
                raise rbd.ImageNotFound("Image {} does not exist".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)
            if snaps:
                raise rbd.ImageBusy("Image {} has snapshots".format(
                    self.format_image_spec(image_spec)), errno=errno.EBUSY)

            return 0, self.add_task(ioctx,
                                    "Removing image {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def queue_trash_remove(self, image_id_spec):
        image_id_spec = self.extract_image_spec(image_id_spec)
        self.log.info("queue_trash_remove: {}".format(image_id_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_TRASH_REMOVE,
                TASK_REF_POOL_NAME: image_id_spec[0],
                TASK_REF_POOL_NAMESPACE: image_id_spec[1],
                TASK_REF_IMAGE_ID: image_id_spec[2]}
        task = self.find_task(refs)
        if task:
            return 0, task.to_json(), ''

        # verify that image exists in trash
        with self.open_ioctx(image_id_spec) as ioctx:
            rbd.RBD().trash_get(ioctx, image_id_spec[2])

            return 0, self.add_task(ioctx,
                                    "Removing image {} from trash".format(
                                        self.format_image_spec(image_id_spec)),
                                    refs), ''

    def get_migration_status(self, ioctx, image_spec):
        try:
            return rbd.RBD().migration_status(ioctx, image_spec[2])
        except (rbd.InvalidArgument, rbd.ImageNotFound):
            return None

    def validate_image_migrating(self, image_spec, migration_status):
        if not migration_status:
            raise rbd.InvalidArgument("Image {} is not migrating".format(
                self.format_image_spec(image_spec)), errno=errno.EINVAL)

    def resolve_pool_name(self, pool_id):
        osd_map = self.module.get('osd_map')
        for pool in osd_map['pools']:
            if pool['pool'] == pool_id:
                return pool['pool_name']
        return '<unknown>'

    def queue_migration_execute(self, image_spec):
        image_spec = self.extract_image_spec(image_spec)
        self.log.info("queue_migration_execute: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_EXECUTE,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            if status['state'] not in [rbd.RBD_IMAGE_MIGRATION_STATE_PREPARED,
                                       rbd.RBD_IMAGE_MIGRATION_STATE_EXECUTING]:
                raise rbd.InvalidArgument("Image {} is not in ready state".format(
                    self.format_image_spec(image_spec)), errno=errno.EINVAL)

            source_pool = self.resolve_pool_name(status['source_pool_id'])
            dest_pool = self.resolve_pool_name(status['dest_pool_id'])
            return 0, self.add_task(ioctx,
                                    "Migrating image {} to {}".format(
                                        self.format_image_spec((source_pool,
                                                                status['source_pool_namespace'],
                                                                status['source_image_name'])),
                                        self.format_image_spec((dest_pool,
                                                                status['dest_pool_namespace'],
                                                                status['dest_image_name']))),
                                    refs), ''

    def queue_migration_commit(self, image_spec):
        image_spec = self.extract_image_spec(image_spec)
        self.log.info("queue_migration_commit: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_COMMIT,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            if status['state'] != rbd.RBD_IMAGE_MIGRATION_STATE_EXECUTED:
                raise rbd.InvalidArgument("Image {} has not completed migration".format(
                    self.format_image_spec(image_spec)), errno=errno.EINVAL)

            return 0, self.add_task(ioctx,
                                    "Committing image migration for {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def queue_migration_abort(self, image_spec):
        image_spec = self.extract_image_spec(image_spec)
        self.log.info("queue_migration_abort: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_ABORT,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            return 0, self.add_task(ioctx,
                                    "Aborting image migration for {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def task_cancel(self, task_id):
        self.log.info("task_cancel: {}".format(task_id))

        if task_id not in self.tasks_by_id:
            return -errno.ENOENT, '', "No such task {}".format(task_id)

        task = self.tasks_by_id[task_id]
        task.cancel()

        remove_in_memory = True
        if self.in_progress_task and self.in_progress_task.task_id == task_id:
            self.log.info("Attempting to cancel in-progress task: {}".format(str(self.in_progress_task)))
            remove_in_memory = False

        # complete any associated event in the progress module
        self.complete_progress(task)

        # remove from rbd_task omap
        with self.open_ioctx((task.refs[TASK_REF_POOL_NAME],
                              task.refs[TASK_REF_POOL_NAMESPACE])) as ioctx:
            self.remove_task(ioctx, task, remove_in_memory)

        return 0, "", ""

    def task_list(self, task_id):
        self.log.info("task_list: {}".format(task_id))

        if task_id:
            if task_id not in self.tasks_by_id:
                return -errno.ENOENT, '', "No such task {}".format(task_id)

            result = self.tasks_by_id[task_id].to_dict()
        else:
            result = []
            for sequence in sorted(self.tasks_by_sequence.keys()):
                task = self.tasks_by_sequence[sequence]
                result.append(task.to_dict())

        return 0, json.dumps(result), ""

    def handle_command(self, inbuf, prefix, cmd):
        with self.lock:
            if prefix == 'add flatten':
                return self.queue_flatten(cmd['image_spec'])
            elif prefix == 'add remove':
                return self.queue_remove(cmd['image_spec'])
            elif prefix == 'add trash remove':
                return self.queue_trash_remove(cmd['image_id_spec'])
            elif prefix == 'add migration execute':
                return self.queue_migration_execute(cmd['image_spec'])
            elif prefix == 'add migration commit':
                return self.queue_migration_commit(cmd['image_spec'])
            elif prefix == 'add migration abort':
                return self.queue_migration_abort(cmd['image_spec'])
            elif prefix == 'cancel':
                return self.task_cancel(cmd['task_id'])
            elif prefix == 'list':
                return self.task_list(cmd.get('task_id'))

        raise NotImplementedError(cmd['prefix'])


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "rbd perf image stats "
                   "name=pool_spec,type=CephString,req=false "
                   "name=sort_by,type=CephChoices,strings="
                   "write_ops|write_bytes|write_latency|"
                   "read_ops|read_bytes|read_latency,"
                   "req=false ",
            "desc": "Retrieve current RBD IO performance stats",
            "perm": "r"
        },
        {
            "cmd": "rbd perf image counters "
                   "name=pool_spec,type=CephString,req=false "
                   "name=sort_by,type=CephChoices,strings="
                   "write_ops|write_bytes|write_latency|"
                   "read_ops|read_bytes|read_latency,"
                   "req=false ",
            "desc": "Retrieve current RBD IO performance counters",
            "perm": "r"
        },
        {
            "cmd": "rbd task add flatten "
                   "name=image_spec,type=CephString",
            "desc": "Flatten a cloned image asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add remove "
                   "name=image_spec,type=CephString",
            "desc": "Remove an image asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add trash remove "
                   "name=image_id_spec,type=CephString",
            "desc": "Remove an image from the trash asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration execute "
                   "name=image_spec,type=CephString",
            "desc": "Execute an image migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration commit "
                   "name=image_spec,type=CephString",
            "desc": "Commit an executed migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration abort "
                   "name=image_spec,type=CephString",
            "desc": "Abort a prepared migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task cancel "
                   "name=task_id,type=CephString ",
            "desc": "Cancel a pending or running asynchronous task",
            "perm": "r"
        },
        {
            "cmd": "rbd task list "
                   "name=task_id,type=CephString,req=false ",
            "desc": "List pending or running asynchronous tasks",
            "perm": "r"
        }
    ]
    MODULE_OPTIONS = []

    perf = None
    task = None

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.perf = PerfHandler(self)
        self.task = TaskHandler(self)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        try:
            try:
                if prefix.startswith('rbd perf '):
                    return self.perf.handle_command(inbuf, prefix[9:], cmd)
                elif prefix.startswith('rbd task '):
                    return self.task.handle_command(inbuf, prefix[9:], cmd)

            except Exception as ex:
                # log the full traceback but don't send it to the CLI user
                self.log.fatal("Fatal runtime error: {}\n{}".format(
                    ex, traceback.format_exc()))
                raise

        except rados.Error as ex:
            return -ex.errno, "", str(ex)
        except rbd.OSError as ex:
            return -ex.errno, "", str(ex)
        except rbd.Error as ex:
            return -errno.EINVAL, "", str(ex)
        except KeyError as ex:
            return -errno.ENOENT, "", str(ex)
        except ValueError as ex:
            return -errno.EINVAL, "", str(ex)

        raise NotImplementedError(cmd['prefix'])
