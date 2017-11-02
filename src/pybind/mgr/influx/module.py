
from datetime import datetime
from threading import Event
import json
import errno

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
except ImportError:
    InfluxDBClient = None

class Module(MgrModule):
    COMMANDS = [
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


    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
            return 0


    def get_df_stats(self):
        df = self.get("df")
        data = []

        df_types = [
            'bytes_used',
            'dirty',
            'rd_bytes',
            'raw_bytes_used',
            'wr_bytes',
            'objects',
            'max_avail'
        ]

        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name" : pool['name'],
                        "pool_id" : pool['id'],
                        "type_instance" : df_type,
                        "mgr_id" : self.get_mgr_id(),
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields": {
                            "value" : pool['stats'][df_type],
                        }
                }
                data.append(point)
        return data

    def get_daemon_stats(self):
        data = []

        for daemon, counters in self.get_all_perf_counters().iteritems():
            svc_type, svc_id = daemon.split(".")
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
                        "host": metadata['hostname']
                    },
                    "time": datetime.utcnow().isoformat() + 'Z',
                    "fields": {
                        "value": value
                    }
                })

        return data

    def send_to_influx(self):
        host = self.get_config("hostname")
        if not host:
            self.log.error("No InfluxDB server configured, please set"
                           "`hostname` configuration key.")
            return

        port = int(self.get_config("port", default="8086"))
        database = self.get_config("database", default="ceph")

        # If influx server has authentication turned off then
        # missing username/password is valid.
        username = self.get_config("username", default="")
        password = self.get_config("password", default="")

        client = InfluxDBClient(host, port, username, password, database)

        # using influx client get_list_database requires admin privs, instead we'll catch the not found exception and inform the user if db can't be created
        try:
            client.write_points(self.get_df_stats(), 'ms')
            client.write_points(self.get_daemon_stats(), 'ms')
        except InfluxDBClientError as e:
            if e.code == 404:
                self.log.info("Database '{0}' not found, trying to create (requires admin privs).  You can also create manually and grant write privs to user '{1}'".format(database,username))
                client.create_database(database)
            else:
                raise

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats = self.get_df_stats()
            result = {
                'daemon_stats': daemon_stats,
                'df_stats': df_stats
            }
            return 0, json.dumps(result, indent=2), 'Self-test OK'
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(cmd['prefix']))

    def serve(self):
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return

        self.log.info('Starting influx module')
        self.run = True
        while self.run:
            self.send_to_influx()
            self.log.debug("Running interval loop")
            interval = self.get_config("interval")
            if interval is None:
                interval = 5
            self.log.debug("sleeping for %d seconds",interval)
            self.event.wait(interval)
            
