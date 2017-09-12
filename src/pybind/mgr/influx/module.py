
from datetime import datetime
from threading import Event
from ConfigParser import SafeConfigParser
from influxdb import InfluxDBClient
from mgr_module import MgrModule
from mgr_module import PERFCOUNTER_HISTOGRAM

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
                if counter_info['type'] & PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                data.append({
                    "measurement": "ceph_osd_stats",
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
        config = SafeConfigParser()
        config.read('/etc/ceph/influx.conf')
        host = config.get('influx','hostname')
        username = config.get('influx', 'username')
        password = config.get('influx', 'password')
        database = config.get('influx', 'database')
        port = int(config.get('influx','port'))
        stats = config.get('influx', 'stats').replace(' ', '').split(',')
        client = InfluxDBClient(host, port, username, password, database) 
        databases_avail = client.get_list_database()
        daemon_stats = self.get_daemon_stats()
        for database_avail in databases_avail:
            if database_avail == database: 
                break
            else: 
                client.create_database(database)

        for stat in stats:
            if stat == "pool": 
                client.write_points(self.get_df_stats(), 'ms')

            elif stat == "osd":
                client.write_points(daemon_stats, 'ms')
                self.log.debug("wrote osd stats")

            elif stat == "cluster": 
                self.log.debug("wrote cluster stats")
            else:
                self.log.error("invalid stat")

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx self-test':
            self.send_to_influx()
            return 0,' ', 'debugging module'
        else:
            print('not found')
            raise NotImplementedError(cmd['prefix'])

    def serve(self):
        self.log.info('Starting influx module')
        self.run = True
        config = SafeConfigParser()
        config.read('/etc/ceph/influx.conf')
        while self.run:
            self.send_to_influx()
            self.log.debug("Running interval loop")
            interval = int(config.get('influx','interval'))
            self.log.debug("sleeping for %d seconds",interval)
            self.event.wait(interval)
            