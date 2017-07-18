from collections import defaultdict
from datetime import datetime
import json
import sys
from threading import Event
import time
from ConfigParser import SafeConfigParser
from influxdb import InfluxDBClient
from mgr_module import MgrModule

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


    def get_default_stat(self):
        defaults= [ 
            "op_w",
            "op_in_bytes",
            "op_r",
            "op_out_bytes"
        ]

        osd_data = []
        cluster_data = []
        for default in defaults:
            osdmap = self.get("osd_map")['osds']
            value = 0
            for osd in osdmap:
                osd_id = osd['osd']
                metadata = self.get_metadata('osd', "%s" % osd_id)
                value += self.get_latest("osd", str(osd_id), "osd."+ str(default))
                point = {
                    "measurement": "ceph_osd_stats",
                    "tags": {
                        "mgr_id": self.get_mgr_id(),
                        "osd_id": osd_id,
                        "type_instance": default,
                        "host": metadata['hostname']
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z', 
                        "fields" : {
                            "value": self.get_latest("osd", osd_id.__str__(), "osd."+ default.__str__())
                        }
                }
                osd_data.append(point)
            point2 = {
                "measurement": "ceph_cluster_stats",
                "tags": {
                    "mgr_id": self.get_mgr_id(),
                    "type_instance": default,
                },
                    "time" : datetime.utcnow().isoformat() + 'Z',
                    "fields" : {
                        "value": value 
                    }
            }
            cluster_data.append(point2)
        return osd_data, cluster_data

        

    def get_extended(self, counter_type, type_inst):
        path = "osd." + type_inst.__str__()
        osdmap = self.get("osd_map")
        data = []
        value = 0
        for osd in osdmap['osds']: 
            osd_id = osd['osd']
            metadata = self.get_metadata('osd', "%s" % osd_id)
            value += self.get_latest("osd", osd_id.__str__(), path.__str__())
            point = {
                "measurement": "ceph_osd_stats",
                "tags": {
                    "mgr_id": self.get_mgr_id(),
                    "osd_id": osd_id,
                    "type_instance": type_inst,
                    "host": metadata['hostname']
                },
                    "time" : datetime.utcnow().isoformat() + 'Z', 
                    "fields" : {
                        "value": self.get_latest("osd", osd_id.__str__(), path.__str__())
                    }
            }
            data.append(point)
        if counter_type == "cluster":
            point = [{
                "measurement": "ceph_cluster_stats",
                "tags": {
                    "mgr_id": self.get_mgr_id(),
                    "type_instance": type_inst,
                },
                    "time" : datetime.utcnow().isoformat() + 'Z',
                    "fields" : {
                        "value": value 
                    }
            }]
            return point 
        else:
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
        default_stats = self.get_default_stat()
        for database_avail in databases_avail:
            if database_avail == database: 
                break
            else: 
                client.create_database(database)

        

        for stat in stats:
            if stat == "pool": 
                client.write_points(self.get_df_stats(), 'ms')

            elif stat == "osd":
                client.write_points(default_stats[0], 'ms')
                if config.has_option('extended', 'osd'):
                    osds = config.get('extended', 'osd').replace(' ', '').split(',')
                    for osd in osds:
                        client.write_points(self.get_extended("osd", osd), 'ms')
                self.log.debug("wrote osd stats")

            elif stat == "cluster": 
                client.write_points(default_stats[-1], 'ms')
                if config.has_option('extended', 'cluster'):
                    clusters = config.get('extended', 'cluster').replace(' ', '').split(',')
                    for cluster in clusters:
                        client.write_points(self.get_extended("cluster", cluster), 'ms')
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
            