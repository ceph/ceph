from contextlib import contextmanager
from datetime import datetime
from threading import Event, Thread
from itertools import chain
import queue
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import RequestException
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
    MODULE_OPTIONS = [
            {
                'name': 'hostname',
                'default': None
            },
            {
                'name': 'port',
                'default': 8086
            },
            {
                'name': 'database',
                'default': 'ceph'
            },
            {
                'name': 'username',
                'default': None
            },
            {
                'name': 'password',
                'default': None
            },
            {
                'name': 'interval',
                'default': 30
            },
            {
                'name': 'ssl',
                'default': 'false'
            },
            {
                'name': 'verify_ssl',
                'default': 'true'
            },
            {
                'name': 'threads',
                'default': 5
            },
            {
                'name': 'batch_size',
                'default': 5000
            }
    ]

    @property
    def config_keys(self):
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

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()
        self.workers = list()
        self.queue = queue.Queue(maxsize=100)
        self.health_checks = dict()

    def get_fsid(self):
        return self.get('mon_map')['fsid']

    @staticmethod
    def can_run():
        if InfluxDBClient is not None:
            return True, ""
        else:
            return False, "influxdb python module not found"

    @staticmethod
    def get_timestamp():
        return datetime.utcnow().isoformat() + 'Z'

    @staticmethod
    def chunk(l, n):
        try:
            while True:
                xs = []
                for _ in range(n):
                    xs.append(next(l))
                yield xs
        except StopIteration:
            yield xs

    def queue_worker(self):
        while True:
            try:
                points = self.queue.get()
                if points is None:
                    self.log.debug('Worker shutting down')
                    break

                start = time.time()
                with self.get_influx_client() as client:
                    client.write_points(points, time_precision='ms')
                runtime = time.time() - start
                self.log.debug('Writing points %d to Influx took %.3f seconds',
                               len(points), runtime)
            except RequestException as e:
                self.log.exception("Failed to connect to Influx host %s:%d",
                                   self.config['hostname'], self.config['port'])
                self.health_checks.update({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB server '
                                   'at %s:%d due to an connection error'
                                   % (self.config['hostname'],
                                      self.config['port']),
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

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self, now):
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

    def get_pg_summary_osd(self, pool_info, now):
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

    def get_pg_summary_pool(self, pool_info, now):
        pool_sum = self.get('pg_summary')['by_pool']
        for pool_id, stats in pool_sum.items():
            for stat in stats:
                yield {
                    "measurement": "ceph_pg_summary_pool",
                    "tags": {
                        "pool_name" : pool_info[pool_id],
                        "pool_id" : pool_id,
                        "type_instance" : stat,
                    },
                    "time" : now,
                    "fields": {
                        "value" : stats[stat],
                    }
                }

    def get_daemon_stats(self, now):
        for daemon, counters in self.get_all_perf_counters().items():
            svc_type, svc_id = daemon.split(".", 1)
            metadata = self.get_metadata(svc_type, svc_id)

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                yield {
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": daemon,
                        "type_instance": path,
                        "host": metadata['hostname'],
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": value
                    }
                }

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['port', 'interval', 'threads', 'batch_size']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        if option in ['ssl', 'verify_ssl']:
            value = value.lower() == 'true'

        if option == 'threads':
            if 1 > value > 32:
                raise RuntimeError('threads should be in range 1-32')

        self.config[option] = value

    def init_module_config(self):
        self.config['hostname'] = \
            self.get_module_option("hostname", default=self.config_keys['hostname'])
        self.config['port'] = \
            int(self.get_module_option("port", default=self.config_keys['port']))
        self.config['database'] = \
            self.get_module_option("database", default=self.config_keys['database'])
        self.config['username'] = \
            self.get_module_option("username", default=self.config_keys['username'])
        self.config['password'] = \
            self.get_module_option("password", default=self.config_keys['password'])
        self.config['interval'] = \
            int(self.get_module_option("interval",
                                default=self.config_keys['interval']))
        self.config['threads'] = \
            int(self.get_module_option("threads",
                                default=self.config_keys['threads']))
        self.config['batch_size'] = \
            int(self.get_module_option("batch_size",
                                default=self.config_keys['batch_size']))
        ssl = self.get_module_option("ssl", default=self.config_keys['ssl'])
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            self.get_module_option("verify_ssl", default=self.config_keys['verify_ssl'])
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'

    def gather_statistics(self):
        now = self.get_timestamp()
        df_stats, pools = self.get_df_stats(now)
        return chain(df_stats, self.get_daemon_stats(now),
                     self.get_pg_summary_osd(pools, now),
                     self.get_pg_summary_pool(pools, now))

    @contextmanager
    def get_influx_client(self):
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

    def send_to_influx(self):
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
            for chunk in self.chunk(points, self.config['batch_size']):
                self.queue.put(chunk, block=False)

            self.log.debug('Queue currently contains %d items',
                           self.queue.qsize())
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

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()
        self.log.debug('Shutting down queue workers')

        for _ in self.workers:
            self.queue.put(None)

        self.queue.join()

        for worker in self.workers:
            worker.join()

    def self_test(self):
        now = self.get_timestamp()
        daemon_stats = list(self.get_daemon_stats(now))
        assert len(daemon_stats)
        df_stats, pools = self.get_df_stats(now)

        result = {
            'daemon_stats': daemon_stats,
            'df_stats': df_stats
        }

        return json.dumps(result, indent=2, sort_keys=True)

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == 'influx config-show':
            return 0, json.dumps(self.config, sort_keys=True), ''
        elif cmd['prefix'] == 'influx config-set':
            key = cmd['key']
            value = cmd['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_config_option(key, value)
            self.set_module_option(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif cmd['prefix'] == 'influx send':
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))

    def serve(self):
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return

        self.log.info('Starting influx module')
        self.init_module_config()
        self.run = True

        self.log.debug('Starting %d queue worker threads',
                       self.config['threads'])
        for i in range(self.config['threads']):
            worker = Thread(target=self.queue_worker, args=())
            worker.setDaemon(True)
            worker.start()
            self.workers.append(worker)

        while self.run:
            start = time.time()
            self.send_to_influx()
            runtime = time.time() - start
            self.log.debug('Finished sending data to Influx in %.3f seconds',
                           runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(self.config['interval'])
