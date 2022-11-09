from contextlib import contextmanager
from datetime import datetime
from threading import Event, Thread, Lock
from itertools import chain
import queue
import json
import errno
import time
from typing import cast, Any, Dict, Iterator, List, Optional, Tuple, Union

from mgr_module import CLICommand, CLIReadCommand, CLIWriteCommand, MgrModule, Option, OptionValue
from mgr_util import profile_method
from rbd import RBD

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import RequestException
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(name='hostname',
               default=None,
               desc='InfluxDB server hostname'),
        Option(name='port',
               type='int',
               default=8086,
               desc='InfluxDB server port'),
        Option(name='database',
               default='ceph',
               desc=('InfluxDB database name. You will need to create this '
                     'database and grant write privileges to the configured '
                     'username or the username must have admin privileges to '
                     'create it.')),
        Option(name='username',
               default=None,
               desc='username of InfluxDB server user'),
        Option(name='password',
               default=None,
               desc='password of InfluxDB server user'),
        Option(name='interval',
               type='secs',
               min=5,
               default=30,
               desc='Time between reports to InfluxDB.  Default 30 seconds.'),
        Option(name='ssl',
               default='false',
               desc='Use https connection for InfluxDB server. Use "true" or "false".'),
        Option(name='verify_ssl',
               default='true',
               desc='Verify https cert for InfluxDB server. Use "true" or "false".'),
        Option(name='threads',
               type='int',
               min=1,
               max=32,
               default=5,
               desc='How many worker threads should be spawned for sending data to InfluxDB.'),
        Option(name='batch_size',
               type='int',
               default=5000,
               desc='How big batches of data points should be when sending to InfluxDB.'),
        Option(name='rbd_stats_pools',
               default=''),
        Option(name='rbd_stats_pools_scrape_interval',
               type='int',
               default=15),
        Option(name='rbd_stats_pools_refresh_interval',
               type='int',
               default=300),
    ]

    @property
    def config_keys(self) -> Dict[str, OptionValue]:
        return dict((o['name'], o.get('default', None))
                for o in self.MODULE_OPTIONS)

    COMMANDS = [
        {
            "cmd": "influx config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "influx config-show",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "influx send",
            "desc": "Force sending data to Influx",
            "perm": "rw"
        }
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config: Dict[str, OptionValue] = dict()
        self.workers: List[Thread] = list()
        self.queue: 'queue.Queue[Optional[List[Dict[str, str]]]]' = queue.Queue(maxsize=100)
        self.health_checks: Dict[str, Dict[str, Any]] = dict()

        self.rbd_stats_event = Event()
        self.rbd_stats_worker = None
        self.rbd_stats_collect_lock = Lock()
        self.rbd_stats_collect_cache: Optional[str] = None
        self.rbd_stats = {
            'pools': {},
            'pools_refresh_time': 0,
            'counters_info': {
                'write_ops': {'type': self.PERFCOUNTER_COUNTER},
                'read_ops': {'type': self.PERFCOUNTER_COUNTER},
                'write_bytes': {'type': self.PERFCOUNTER_COUNTER},
                'read_bytes': {'type': self.PERFCOUNTER_COUNTER},
                'write_latency': {'type': self.PERFCOUNTER_LONGRUNAVG},
                'read_latency': {'type': self.PERFCOUNTER_LONGRUNAVG},
            },
        }  # type: Dict[str, Any]

    def get_fsid(self) -> str:
        return self.get('mon_map')['fsid']

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        if InfluxDBClient is not None:
            return True, ""
        else:
            return False, "influxdb python module not found"

    @staticmethod
    def get_timestamp() -> str:
        return datetime.utcnow().isoformat() + 'Z'

    @staticmethod
    def chunk(l: Iterator[Dict[str, str]], n: int) -> Iterator[List[Dict[str, str]]]:
        try:
            while True:
                xs = []
                for _ in range(n):
                    xs.append(next(l))
                yield xs
        except StopIteration:
            yield xs

    def queue_worker(self) -> None:
        while True:
            try:
                points = self.queue.get()
                if not points:
                    self.log.debug('Worker shutting down')
                    break

                start = time.time()
                with self.get_influx_client() as client:
                    client.write_points(points, time_precision='ms')
                runtime = time.time() - start
                self.log.debug('Writing points %d to Influx took %.3f seconds',
                               len(points), runtime)
            except RequestException as e:
                hostname = self.config['hostname']
                port = self.config['port']
                self.log.exception(f"Failed to connect to Influx host {hostname}:{port}")
                self.health_checks.update({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB server '
                                   f'at {hostname}:{port} due to an connection error',
                        'detail': [str(e)]
                    }
                })
            except InfluxDBClientError as e:
                self.health_checks.update({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB',
                        'detail': [str(e)]
                    }
                })
                self.log.exception('Failed to send data to InfluxDB')
            except queue.Empty:
                continue
            except:
                self.log.exception('Unhandled Exception while sending to Influx')
            finally:
                self.queue.task_done()

    def refresh_rbd_stats_pools(self, pools: Dict[str, Set[str]]) -> None:
        self.log.debug('refreshing rbd pools %s' % (pools))

        rbd = RBD()
        counters_info = self.rbd_stats['counters_info']
        for pool_name, cfg_ns_names in pools.items():
            try:
                pool_id = self.rados.pool_lookup(pool_name)
                with self.rados.open_ioctx(pool_name) as ioctx:
                    if pool_id not in self.rbd_stats['pools']:
                        self.rbd_stats['pools'][pool_id] = {'images': {}}
                    pool = self.rbd_stats['pools'][pool_id]
                    pool['name'] = pool_name
                    pool['ns_names'] = cfg_ns_names
                    if cfg_ns_names:
                        nspace_names = list(cfg_ns_names)
                    else:
                        nspace_names = [''] + rbd.namespace_list(ioctx)
                    for nspace_name in pool['images']:
                        if nspace_name not in nspace_names:
                            del pool['images'][nspace_name]
                    for nspace_name in nspace_names:
                        if nspace_name and\
                           not rbd.namespace_exists(ioctx, nspace_name):
                            self.log.debug('unknown namespace %s for pool %s' %
                                           (nspace_name, pool_name))
                            continue
                        ioctx.set_namespace(nspace_name)
                        if nspace_name not in pool['images']:
                            pool['images'][nspace_name] = {}
                        namespace = pool['images'][nspace_name]
                        images = {}
                        for image_meta in RBD().list2(ioctx):
                            image = {'n': image_meta['name']}
                            image_id = image_meta['id']
                            if image_id in namespace:
                                image['c'] = namespace[image_id]['c']
                            else:
                                image['c'] = [[0, 0] for x in counters_info]
                            images[image_id] = image
                        pool['images'][nspace_name] = images
            except Exception as e:
                self.log.error('failed listing pool %s: %s' % (pool_name, e))
        self.rbd_stats['pools_refresh_time'] = time.time()

    @profile_method()
    def collect_rbd_stats(self) -> Iterator[Dict[str, Any]]:
        # Per RBD image stats is collected by registering a dynamic osd perf
        # stats query that tells OSDs to group stats for requests associated
        # with RBD objects by pool, namespace, and image id, which are
        # extracted from the request object names or other attributes.
        # The RBD object names have the following prefixes:
        #   - rbd_data.{image_id}. (data stored in the same pool as metadata)
        #   - rbd_data.{pool_id}.{image_id}. (data stored in a dedicated data pool)
        #   - journal_data.{pool_id}.{image_id}. (journal if journaling is enabled)
        # The pool_id in the object name is the id of the pool with the image
        # metdata, and should be used in the image spec. If there is no pool_id
        # in the object name, the image pool is the pool where the object is
        # located.

        # Parse rbd_stats_pools option, which is a comma or space separated
        # list of pool[/namespace] entries. If no namespace is specifed the
        # stats are collected for every namespace in the pool. The wildcard
        # '*' can be used to indicate all pools or namespaces
        pools_string = cast(str, self.get_localized_module_option('rbd_stats_pools'))
        pool_keys = set()
        osd_map = self.get('osd_map')
        rbd_pools = [pool['pool_name'] for pool in osd_map['pools']
                     if 'rbd' in pool.get('application_metadata', {})]
        for x in re.split(r'[\s,]+', pools_string):
            if not x:
                continue

            s = x.split('/', 2)
            pool_name = s[0]
            namespace_name = None
            if len(s) == 2:
                namespace_name = s[1]

            if pool_name == "*":
                # collect for all pools
                for pool in rbd_pools:
                    pool_keys.add((pool, namespace_name))
            else:
                if pool_name in rbd_pools:
                    pool_keys.add((pool_name, namespace_name))  # avoids adding deleted pool

        pools = {}  # type: Dict[str, Set[str]]
        for pool_key in pool_keys:
            pool_name = pool_key[0]
            namespace_name = pool_key[1]
            if not namespace_name or namespace_name == "*":
                # empty set means collect for all namespaces
                pools[pool_name] = set()
                continue

            if pool_name not in pools:
                pools[pool_name] = set()
            elif not pools[pool_name]:
                continue
            pools[pool_name].add(namespace_name)

        rbd_stats_pools = {}
        for pool_id in self.rbd_stats['pools'].keys():
            name = self.rbd_stats['pools'][pool_id]['name']
            if name not in pools:
                del self.rbd_stats['pools'][pool_id]
            else:
                rbd_stats_pools[name] = \
                    self.rbd_stats['pools'][pool_id]['ns_names']

        pools_refreshed = False
        if pools:
            next_refresh = self.rbd_stats['pools_refresh_time'] + \
                self.get_localized_module_option(
                'rbd_stats_pools_refresh_interval', 300)
            if rbd_stats_pools != pools or time.time() >= next_refresh:
                self.refresh_rbd_stats_pools(pools)
                pools_refreshed = True

        pool_ids = list(self.rbd_stats['pools'])
        pool_ids.sort()
        pool_id_regex = '^(' + '|'.join([str(x) for x in pool_ids]) + ')$'

        nspace_names = []
        for pool_id, pool in self.rbd_stats['pools'].items():
            if pool['ns_names']:
                nspace_names.extend(pool['ns_names'])
            else:
                nspace_names = []
                break
        if nspace_names:
            namespace_regex = '^(' + \
                              "|".join([re.escape(x)
                                        for x in set(nspace_names)]) + ')$'
        else:
            namespace_regex = '^(.*)$'

        if ('query' in self.rbd_stats
            and (pool_id_regex != self.rbd_stats['query']['key_descriptor'][0]['regex']
                 or namespace_regex != self.rbd_stats['query']['key_descriptor'][1]['regex'])):
            self.remove_osd_perf_query(self.rbd_stats['query_id'])
            del self.rbd_stats['query_id']
            del self.rbd_stats['query']

        if not self.rbd_stats['pools']:
            return

        counters_info = self.rbd_stats['counters_info']

        if 'query_id' not in self.rbd_stats:
            query = {
                'key_descriptor': [
                    {'type': 'pool_id', 'regex': pool_id_regex},
                    {'type': 'namespace', 'regex': namespace_regex},
                    {'type': 'object_name',
                     'regex': r'^(?:rbd|journal)_data\.(?:([0-9]+)\.)?([^.]+)\.'},
                ],
                'performance_counter_descriptors': list(counters_info),
            }
            query_id = self.add_osd_perf_query(query)
            if query_id is None:
                self.log.error('failed to add query %s' % query)
                return
            self.rbd_stats['query'] = query
            self.rbd_stats['query_id'] = query_id

        res = self.get_osd_perf_counters(self.rbd_stats['query_id'])
        assert res
        now = self.get_timestamp()
        for c in res['counters']:
            # if the pool id is not found in the object name use id of the
            # pool where the object is located
            if c['k'][2][0]:
                pool_id = int(c['k'][2][0])
            else:
                pool_id = int(c['k'][0][0])
            if pool_id not in self.rbd_stats['pools'] and not pools_refreshed:
                self.refresh_rbd_stats_pools(pools)
                pools_refreshed = True
            if pool_id not in self.rbd_stats['pools']:
                continue
            pool = self.rbd_stats['pools'][pool_id]
            nspace_name = c['k'][1][0]
            if nspace_name not in pool['images']:
                continue
            image_id = c['k'][2][1]
            if image_id not in pool['images'][nspace_name] and \
               not pools_refreshed:
                self.refresh_rbd_stats_pools(pools)
                pool = self.rbd_stats['pools'][pool_id]
                pools_refreshed = True
            if image_id not in pool['images'][nspace_name]:
                continue
            counters = pool['images'][nspace_name][image_id]['c']
            for i in range(len(c['c'])):
                counters[i][0] += c['c'][i][0]
                counters[i][1] += c['c'][i][1]

        label_names = ("pool", "namespace", "image")
        for pool_id, pool in self.rbd_stats['pools'].items():
            pool_name = pool['name']
            for nspace_name, images in pool['images'].items():
                for image_id in images:
                    image_name = images[image_id]['n']
                    counters = images[image_id]['c']
                    i = 0
                    for key in counters_info:
                        counter_info = counters_info[key]
                        if counter_info['type'] == self.PERFCOUNTER_COUNTER:
                            yield {
                                "measurement": "ceph_rbd_stats",
                                "tags": {
                                    "type_instance": 'rbd.' + key,
                                    "pool": pool_name,
                                    "namespace": nspace_name,
                                    "image": image_name,
                                    "fsid": self.get_fsid()
                                },
                                "time": now,
                                "fields": {
                                    "value": counters[i][0],
                                }
                            }
                        elif counter_info['type'] == self.PERFCOUNTER_LONGRUNAVG:
                            yield {
                                "measurement": "ceph_rbd_stats",
                                "tags": {
                                    "type_instance": 'rbd.' + key + '_sum',
                                    "pool": pool_name,
                                    "namespace": nspace_name,
                                    "image": image_name,
                                    "fsid": self.get_fsid()
                                },
                                "time": now,
                                "fields": {
                                    "value": counters[i][0],
                                }
                            }
                            yield {
                                "measurement": "ceph_rbd_stats",
                                "tags": {
                                    "type_instance": 'rbd.' + key + '_count',
                                    "pool": pool_name,
                                    "namespace": nspace_name,
                                    "image": image_name,
                                    "fsid": self.get_fsid()
                                },
                                "time": now,
                                "fields": {
                                    "value": counters[i][1],
                                }
                            }
                        i += 1

    def rbd_stats_worker_thread(self) -> None:
        self.log.info('starting rbd stats thread')
        while not self.rbd_stats_event.is_set():
            self.log.debug('collecting rbd stats in thread')
            if self.have_mon_connection():
                start_time = time.time()

                try:
                    data = self.collect_rbd_stats()
                except Exception:
                    # Log any issues encountered during the data collection and continue
                    self.log.exception("failed to collect metrics:")
                    self.rbd_stats_event.wait(self.config['rbd_stats_pools_scrape_interval'])
                    continue

                duration = time.time() - start_time
                self.log.debug('collecting rbd stats in thread done')

                sleep_time = self.config['rbd_stats_pools_scrape_interval'] - duration
                if sleep_time < 0:
                    self.log.warning(
                        'Collecting data took more time than configured scrape interval. '
                        'This possibly results in stale data. '
                        'Collecting data took {:.2f} seconds but scrape interval is configured '
                        'to be {:.0f} seconds.'.format(
                            duration,
                            self.config['rbd_stats_pools_scrape_interval'],
                        )
                    )
                    sleep_time = 0

                with self.rbd_stats_collect_lock:
                    self.rbd_stats_collect_cache = data

                self.rbd_stats_event.wait(sleep_time)
            else:
                self.log.error('No MON connection')
                self.rbd_stats_event.wait(self.config['rbd_stats_pools_scrape_interval'])

    def get_latest(self, daemon_type: str, daemon_name: str, stat: str) -> int:
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self, now) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        df = self.get("df")
        data = []
        pool_info = {}

        df_types = [
            'stored',
            'kb_used',
            'dirty',
            'rd',
            'rd_bytes',
            'stored_raw',
            'wr',
            'wr_bytes',
            'objects',
            'max_avail',
            'quota_objects',
            'quota_bytes'
        ]

        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name": pool['name'],
                        "pool_id": pool['id'],
                        "type_instance": df_type,
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": pool['stats'][df_type],
                    }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        return data, pool_info

    def get_pg_summary_osd(self, pool_info: Dict[str, str], now: str) -> Iterator[Dict[str, Any]]:
        pg_sum = self.get('pg_summary')
        osd_sum = pg_sum['by_osd']
        for osd_id, stats in osd_sum.items():
            metadata = self.get_metadata('osd', "%s" % osd_id)
            if not metadata:
                continue

            for stat in stats:
                yield {
                    "measurement": "ceph_pg_summary_osd",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": stat,
                        "host": metadata['hostname']
                    },
                    "time" : now,
                    "fields" : {
                        "value": stats[stat]
                    }
                }

    def get_pg_summary_pool(self, pool_info: Dict[str, str], now: str) -> Iterator[Dict[str, Any]]:
        pool_sum = self.get('pg_summary')['by_pool']
        for pool_id, stats in pool_sum.items():
            try:
                pool_name = pool_info[pool_id]
            except KeyError:
                self.log.error('Unable to find pool name for pool {}'.format(pool_id))
                continue
            for stat in stats:
                yield {
                    "measurement": "ceph_pg_summary_pool",
                    "tags": {
                        "pool_name" : pool_name,
                        "pool_id" : pool_id,
                        "type_instance" : stat,
                    },
                    "time" : now,
                    "fields": {
                        "value" : stats[stat],
                    }
                }

    def get_daemon_stats(self, now: str) -> Iterator[Dict[str, Any]]:
        for daemon, counters in self.get_all_perf_counters().items():
            svc_type, svc_id = daemon.split(".", 1)
            metadata = self.get_metadata(svc_type, svc_id)
            if metadata is not None:
                hostname = metadata['hostname']
            else:
                hostname = 'N/A'

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                yield {
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": daemon,
                        "type_instance": path,
                        "host": hostname,
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": value
                    }
                }

    def init_module_config(self) -> None:
        self.config['hostname'] = \
            self.get_module_option("hostname", default=self.config_keys['hostname'])
        self.config['port'] = \
            cast(int, self.get_module_option("port", default=self.config_keys['port']))
        self.config['database'] = \
            self.get_module_option("database", default=self.config_keys['database'])
        self.config['username'] = \
            self.get_module_option("username", default=self.config_keys['username'])
        self.config['password'] = \
            self.get_module_option("password", default=self.config_keys['password'])
        self.config['interval'] = \
            cast(int, self.get_module_option("interval",
                                             default=self.config_keys['interval']))
        self.config['threads'] = \
            cast(int, self.get_module_option("threads",
                                             default=self.config_keys['threads']))
        self.config['batch_size'] = \
            cast(int, self.get_module_option("batch_size",
                                             default=self.config_keys['batch_size']))
        ssl = cast(str, self.get_module_option("ssl", default=self.config_keys['ssl']))
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            cast(str, self.get_module_option("verify_ssl", default=self.config_keys['verify_ssl']))
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'
        self.config['rbd_stats_pools'] = \
            cast(str, self.get_module_option("rbd_stats_pools", default=''))
        self.config['rbd_stats_pools_scrape_interval'] = \
            cast(int, self.get_module_option("rbd_stats_pools_scrape_interval", default=15))
        self.config['rbd_stats_pools_refresh_interval'] = \
            cast(int, self.get_module_option("rbd_stats_pools_refresh_interval", default=300))

    def gather_statistics(self) -> Iterator[Dict[str, str]]:
        now = self.get_timestamp()
        df_stats, pools = self.get_df_stats(now)
        result = chain(df_stats, self.get_daemon_stats(now),
                       self.get_pg_summary_osd(pools, now),
                       self.get_pg_summary_pool(pools, now))
        if self.rbd_stats_collect_cache is not None:
            return chain(result, self.rbd_stats_collect_cache)
        return result

    @contextmanager
    def get_influx_client(self) -> Iterator['InfluxDBClient']:
        client = InfluxDBClient(self.config['hostname'],
                                self.config['port'],
                                self.config['username'],
                                self.config['password'],
                                self.config['database'],
                                self.config['ssl'],
                                self.config['verify_ssl'])
        try:
            yield client
        finally:
            try:
                client.close()
            except AttributeError:
                # influxdb older than v5.0.0
                pass

    def send_to_influx(self) -> bool:
        if not self.config['hostname']:
            self.log.error("No Influx server configured, please set one using: "
                           "ceph influx config-set hostname <hostname>")

            self.set_health_checks({
                'MGR_INFLUX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No InfluxDB server configured',
                    'detail': ['Configuration option hostname not set']
                }
            })
            return False

        self.health_checks = dict()

        self.log.debug("Sending data to Influx host: %s",
                       self.config['hostname'])
        try:
            with self.get_influx_client() as client:
                databases = client.get_list_database()
                if {'name': self.config['database']} not in databases:
                    self.log.info("Database '%s' not found, trying to create "
                                  "(requires admin privs). You can also create "
                                  "manually and grant write privs to user "
                                  "'%s'", self.config['database'],
                                  self.config['database'])
                    client.create_database(self.config['database'])
                    client.create_retention_policy(name='8_weeks',
                                                   duration='8w',
                                                   replication='1',
                                                   default=True,
                                                   database=self.config['database'])

            self.log.debug('Gathering statistics')
            points = self.gather_statistics()
            for chunk in self.chunk(points, cast(int, self.config['batch_size'])):
                self.queue.put(chunk, block=False)

            self.log.debug('Queue currently contains %d items',
                           self.queue.qsize())
            return True
        except queue.Full:
            self.health_checks.update({
                'MGR_INFLUX_QUEUE_FULL': {
                    'severity': 'warning',
                    'summary': 'Failed to chunk to InfluxDB Queue',
                    'detail': ['Queue is full. InfluxDB might be slow with '
                               'processing data']
                }
            })
            self.log.error('Queue is full, failed to add chunk')
            return False
        except (RequestException, InfluxDBClientError) as e:
            self.health_checks.update({
                'MGR_INFLUX_DB_LIST_FAILED': {
                    'severity': 'warning',
                    'summary': 'Failed to list/create InfluxDB database',
                    'detail': [str(e)]
                }
            })
            self.log.exception('Failed to list/create InfluxDB database')
            return False
        finally:
            self.set_health_checks(self.health_checks)

    def shutdown(self) -> None:
        self.log.info('Stopping influx module')
        self.run = False

        if self.rbd_stats_worker:
            self.rbd_stats_event.set()
            self.log.info('Stopping rbd stats worker')
            self.rbd_stats_worker.join()

        self.event.set()
        self.log.debug('Shutting down queue workers')

        for _ in self.workers:
            self.queue.put([])

        self.queue.join()

        for worker in self.workers:
            worker.join()

    def self_test(self) -> Optional[str]:
        now = self.get_timestamp()
        daemon_stats = list(self.get_daemon_stats(now))
        assert len(daemon_stats)
        df_stats, pools = self.get_df_stats(now)

        result = {
            'daemon_stats': daemon_stats,
            'df_stats': df_stats
        }

        return json.dumps(result, indent=2, sort_keys=True)

    @CLIReadCommand('influx config-show')
    def config_show(self) -> Tuple[int, str, str]:
        """
        Show current configuration
        """
        return 0, json.dumps(self.config, sort_keys=True), ''

    @CLIWriteCommand('influx config-set')
    def config_set(self, key: str, value: str) -> Tuple[int, str, str]:
        if not value:
            return -errno.EINVAL, '', 'Value should not be empty'

        self.log.debug('Setting configuration option %s to %s', key, value)
        try:
            self.set_module_option(key, value)
            self.config[key] = self.get_module_option(key)
            return 0, 'Configuration option {0} updated'.format(key), ''
        except ValueError as e:
            return -errno.EINVAL, '', str(e)

    @CLICommand('influx send')
    def send(self) -> Tuple[int, str, str]:
        """
        Force sending data to Influx
        """
        self.send_to_influx()
        return 0, 'Sending data to Influx', ''

    def serve(self) -> None:
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return

        self.log.info('Starting influx module')
        self.init_module_config()
        self.run = True

        self.log.debug('Starting %d queue worker threads',
                       self.config['threads'])
        for i in range(cast(int, self.config['threads'])):
            worker = Thread(target=self.queue_worker, args=())
            worker.setDaemon(True)
            worker.start()
            self.workers.append(worker)

        if len(self.config['rbd_stats_pools']) > 0:
            self.rbd_stats_worker = Thread(target=self.rbd_stats_worker_thread, args=())
            self.rbd_stats_worker.setDaemon(True)
            self.rbd_stats_worker.start()

        while self.run:
            start = time.time()
            self.send_to_influx()
            runtime = time.time() - start
            self.log.debug('Finished sending data to Influx in %.3f seconds',
                           runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(cast(float, self.config['interval']))
