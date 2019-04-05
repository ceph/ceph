from datetime import datetime
from threading import Event
import json
import errno
import six
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import ConnectionError
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
    OPTIONS = [
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
    ]

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None))
                for o in self.OPTIONS)

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
        },
        {
            "cmd": "influx self-test",
            "desc": "debug the module",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()

    def get_fsid(self):
        return self.get('mon_map')['fsid']

    @staticmethod
    def can_run():
        if InfluxDBClient is not None:
            return True, ""
        else:
            return False, "influxdb python module not found"

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self):
        df = self.get("df")
        data = []
        pool_info = {}

        now = datetime.utcnow().isoformat() + 'Z'

        df_types = [
            'bytes_used',
            'kb_used',
            'dirty',
            'rd',
            'rd_bytes',
            'raw_bytes_used',
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

    def get_pg_summary(self, pool_info):
        time = datetime.utcnow().isoformat() + 'Z'
        pg_sum = self.get('pg_summary')
        osd_sum = pg_sum['by_osd']
        pool_sum = pg_sum['by_pool']
        data = []
        for osd_id, stats in six.iteritems(osd_sum):
            metadata = self.get_metadata('osd', "%s" % osd_id)
            if not metadata:
                continue

            for stat in stats:
                point_1 = {
                    "measurement": "ceph_pg_summary_osd",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": stat,
                        "host": metadata['hostname']
                    },
                    "time" : time, 
                    "fields" : {
                        "value": stats[stat]
                    }
                }
                data.append(point_1)
        for pool_id, stats in six.iteritems(pool_sum):
            for stat in stats:
                point_2 = {
                    "measurement": "ceph_pg_summary_pool",
                    "tags": {
                        "pool_name" : pool_info[pool_id],
                        "pool_id" : pool_id,
                        "type_instance" : stat,
                    },
                    "time" : time,
                    "fields": {
                        "value" : stats[stat],
                    }
                }
                data.append(point_2)
        return data 


    def get_daemon_stats(self):
        data = []

        now = datetime.utcnow().isoformat() + 'Z'

        for daemon, counters in six.iteritems(self.get_all_perf_counters()):
            svc_type, svc_id = daemon.split(".", 1)
            metadata = self.get_metadata(svc_type, svc_id)

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                data.append({
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
                })

        return data

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['port', 'interval']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        if option in ['ssl', 'verify_ssl']:
            value = value.lower() == 'true'

        self.config[option] = value

    def init_module_config(self):
        self.config['hostname'] = \
            self.get_config("hostname", default=self.config_keys['hostname'])
        self.config['port'] = \
            int(self.get_config("port", default=self.config_keys['port']))
        self.config['database'] = \
            self.get_config("database", default=self.config_keys['database'])
        self.config['username'] = \
            self.get_config("username", default=self.config_keys['username'])
        self.config['password'] = \
            self.get_config("password", default=self.config_keys['password'])
        self.config['interval'] = \
            int(self.get_config("interval",
                                default=self.config_keys['interval']))
        ssl = self.get_config("ssl", default=self.config_keys['ssl'])
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            self.get_config("verify_ssl", default=self.config_keys['verify_ssl'])
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'

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
            return

        # If influx server has authentication turned off then
        # missing username/password is valid.
        self.log.debug("Sending data to Influx host: %s",
                       self.config['hostname'])
        client = InfluxDBClient(self.config['hostname'], self.config['port'],
                                self.config['username'],
                                self.config['password'],
                                self.config['database'],
                                self.config['ssl'],
                                self.config['verify_ssl'])

        # using influx client get_list_database requires admin privs,
        # instead we'll catch the not found exception and inform the user if
        # db can not be created
        try:
            df_stats, pools = self.get_df_stats()
            client.write_points(df_stats, 'ms')
            client.write_points(self.get_daemon_stats(), 'ms')
            client.write_points(self.get_pg_summary(pools))
            self.set_health_checks(dict())
        except ConnectionError as e:
            self.log.exception("Failed to connect to Influx host %s:%d",
                               self.config['hostname'], self.config['port'])
            self.set_health_checks({
                'MGR_INFLUX_SEND_FAILED': {
                    'severity': 'warning',
                    'summary': 'Failed to send data to InfluxDB server at %s:%d'
                               ' due to an connection error'
                               % (self.config['hostname'], self.config['port']),
                    'detail': [str(e)]
                }
            })
        except InfluxDBClientError as e:
            if e.code == 404:
                self.log.info("Database '%s' not found, trying to create "
                              "(requires admin privs).  You can also create "
                              "manually and grant write privs to user "
                              "'%s'", self.config['database'],
                              self.config['username'])
                client.create_database(self.config['database'])
                client.create_retention_policy(name='8_weeks', duration='8w',
                                               replication='1', default=True,
                                               database=self.config['database'])
            else:
                self.set_health_checks({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB',
                        'detail': [str(e)]
                    }
                })
                raise

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == 'influx config-show':
            return 0, json.dumps(self.config), ''
        elif cmd['prefix'] == 'influx config-set':
            key = cmd['key']
            value = cmd['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_config_option(key, value)
            self.set_config(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif cmd['prefix'] == 'influx send':
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats, pools = self.get_df_stats()

            result = {
                'daemon_stats': daemon_stats,
                'df_stats': df_stats
            }

            return 0, json.dumps(result, indent=2), 'Self-test OK'

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

        while self.run:
            start = time.time()
            self.send_to_influx()
            runtime = time.time() - start
            self.log.debug('Finished sending data in Influx in %.3f seconds',
                           runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(self.config['interval'])
