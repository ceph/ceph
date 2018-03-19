from datetime import datetime
from threading import Event
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import ConnectionError, ReadTimeout
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
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

    config_keys = {
        'hostname': None,
        'port': 8086,
        'database': 'ceph',
        'username': None,
        'password': None,
        'interval': 5,
        'ssl': 'false',
        'verify_ssl': 'true',
        'timeout': 4,   
        'retries': 1,
        'destinations': None
    }

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
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name": pool['name'],
                        "pool_id": pool['id'],
                        "type_instance": df_type,
                        "fsid": self.get_fsid()
                    },
                    "time" : timestamp,
                    "fields": {
                        "value" : pool['stats'][df_type],
                    }
                }
                data.append(point)

    def get_pg_summary(self, pool_info):
        osd_sum = self.get('pg_summary')['by_osd']
        pool_sum = self.get('pg_summary')['by_pool']
        data = []
        timestamp = datetime.utcnow().isoformat() + 'Z'

        for osd_id, stats in osd_sum.iteritems():
            metadata = self.get_metadata('osd', "%s" % osd_id)
            
            # guard against OSD being removed while in this loop
            if metadata == None:
                continue

            for stat in stats:
                point_1 = {
                    "measurement": "ceph_pg_summary_osd",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": stat,
                        "host": metadata['hostname']
                    },
                    "time" : timestamp, 
                    "fields" : {
                        "value": stats[stat]
                    }
                }
                data.append(point_1)
        for pool_id, stats in pool_sum.iteritems():
            for stat in stats:
                point_2 = {
                    "measurement": "ceph_pg_summary_pool",
                    "tags": {
                        "pool_name" : pool_info[pool_id],
                        "pool_id" : pool_id,
                        "type_instance" : stat,
                    },
                    "time" : timestamp,
                    "fields": {
                        "value" : stats[stat],
                    }
                }
                data.append(point_2)
        return data 


    def get_daemon_stats(self):
        data = []

        for daemon, counters in self.get_all_perf_counters().iteritems():
            svc_type, svc_id = daemon.split(".")
            metadata = self.get_metadata(svc_type, svc_id)

            # guard against OSD being removed while in this loop
            if metadata == None:
                continue

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
                    "time": timestamp,
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
        self.config['timeout'] = \
            int(self.get_config("timeout",
                                default=self.config_keys['timeout']))
        self.config['retries'] = \
            int(self.get_config("retries",
                                default=self.config_keys['retries']))
        
        # get_config_json returns None if key is not set, does not accept default arg 
        self.config['destinations'] = \
                self.get_config_json("destinations")

        self.init_influx_clients()

    def init_influx_clients(self):

        self.clients = []
        
        if not self.config['destinations']:
            destinations = [ 
                { 
                    'hostname':   self.config['hostname'],
                    'username':   self.config['username'],
                    'password':   self.config['password'],
                    'database':   self.config['database'],
                    'ssl':        self.config['ssl'],
                    'verify_ssl': self.config['verify_ssl'],
                    'timeout'   : self.config['timeout'],
                    'retries'   : self.config['retries']
                },
            ]
        else: 
            destinations = self.config['destinations']
        
        for dest in destinations:
            # use global settings if these keys not set in destinations object 
            merge_configs = [ 'port', 'database', 'username', 'password', 'ssl', 'verify_ssl', 'timeout', 'retries']
            conf = dict()

            for key in merge_configs:
                conf[key] = dest[key] if key in dest else self.config[key]
                # make sure this is an int or may encounter type errors later
                if key in [ 'port', 'timeout','retries' ]:
                    conf[key] = int(conf[key])

            # if not cast to string set_health_check will complain when var is used in error summary string format
            # everything else seems to consider it a string already (?)
            conf['hostname'] = str(dest['hostname'])

            self.log.debug("Sending data to Influx host: %s",
                conf['hostname'])

            client = InfluxDBClient(
                host=conf['hostname'], 
                port=conf['port'],
                username=conf['username'], 
                password=conf['password'], 
                database=conf['database'],
                ssl=conf['ssl'],
                verify_ssl=conf['verify_ssl'],
                timeout=conf['timeout'],
                retries=conf['retries'])

            self.clients.append([client,conf])

    def send_to_influx(self):
        if not self.config['hostname'] and not self.config['destinations']:
            self.log.error("No Influx server configured, please set using: "
                           "ceph influx config-set mgr/influx/hostname <hostname> or ceph influx config-set mgr/influx/destinations '<json array>'")
            self.set_health_checks({
                'MGR_INFLUX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No InfluxDB server configured',
                    'detail': ['Configuration option hostname not set']
                }
            })
            return

        df_stats = self.get_df_stats()
        daemon_stats = self.get_daemon_stats()
        pg_summary = self.get_pg_summary(df_stats[1])

        for client,conf in self.clients:
            # using influx client get_list_database requires admin privs,
            # instead we'll catch the not found exception and inform the user if
            # db can not be created
            try:
                client.write_points(df_stats[0], 'ms')
                client.write_points(daemon_stats, 'ms')
                client.write_points(self.get_pg_summary(df_stats[1]))
                self.set_health_checks(dict())
            except ConnectionError as e:
                # InfluxDBClient also has get_host and get_port but since we have the config here anyways...
                self.log.exception("Failed to connect to Influx host %s:%d",
                                   conf['hostname'], conf['port'])
                self.set_health_checks({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB server at %s:%d'
                                   ' due to a connection error'
                                   %(conf['hostname'], conf['port']),
                        'detail': [str(e)]
                    }
                })
            except ReadTimeout as e:
                self.log.exception("Timeout waiting for response from influx host %s:%d",
                                   conf['hostname'], conf['port'])
                self.set_health_checks({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB server at %s:%d'
                                   ' due to a response timeout from server'
                                   %(conf['hostname'], conf['port']),
                        'detail': [str(e)]
                    }
                })
            except InfluxDBClientError as e:
                if e.code == 404:
                    self.log.info("Database '%s' not found, trying to create "
                                  "(requires admin privs).  You can also create "
                                  "manually and grant write privs to user "
                                  "'%s'", conf['database'],
                                  conf['username'])
                    client.create_database(conf['database'])
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

    def handle_command(self, cmd):
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
        elif cmd['prefix'] == 'influx send':
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats = self.get_df_stats()

            result = {
                'daemon_stats': daemon_stats,
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
