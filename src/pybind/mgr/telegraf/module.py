import errno
import json
import itertools
import socket
import time
from threading import Event

from telegraf.basesocket import BaseSocket
from telegraf.protocol import Line
from mgr_module import MgrModule, PG_STATES

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "telegraf config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "telegraf config-show",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "telegraf send",
            "desc": "Force sending data to Telegraf",
            "perm": "rw"
        },
    ]

    MODULE_OPTIONS = [
        {
            'name': 'address',
            'default': 'unixgram:///tmp/telegraf.sock',
        },
        {
            'name': 'interval',
            'type': 'secs',
            'default': 15
        }
    ]

    ceph_health_mapping = {'HEALTH_OK': 0, 'HEALTH_WARN': 1, 'HEALTH_ERR': 2}

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None)) for o in self.MODULE_OPTIONS)

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.fsid = None
        self.config = dict()

    def get_fsid(self):
        if not self.fsid:
            self.fsid = self.get('mon_map')['fsid']

        return self.fsid

    def get_pool_stats(self):
        df = self.get('df')

        df_types = [
            'bytes_used',
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
                yield {
                    'measurement': 'ceph_pool_stats',
                    'tags': {
                        'pool_name': pool['name'],
                        'pool_id': pool['id'],
                        'type_instance': df_type,
                        'fsid': self.get_fsid()
                    },
                    'value': pool['stats'][df_type],
                }

    def get_daemon_stats(self):
        for daemon, counters in self.get_all_perf_counters().items():
            svc_type, svc_id = daemon.split('.', 1)
            metadata = self.get_metadata(svc_type, svc_id)
            if not metadata:
                continue

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                yield {
                    'measurement': 'ceph_daemon_stats',
                    'tags': {
                        'ceph_daemon': daemon,
                        'type_instance': path,
                        'host': metadata['hostname'],
                        'fsid': self.get_fsid()
                    },
                    'value': counter_info['value']
                }

    def get_pg_stats(self):
        stats = dict()

        pg_status = self.get('pg_status')
        for key in ['bytes_total', 'data_bytes', 'bytes_used', 'bytes_avail',
                    'num_pgs', 'num_objects', 'num_pools']:
            stats[key] = pg_status[key]

        for state in PG_STATES:
            stats['num_pgs_{0}'.format(state)] = 0

        stats['num_pgs'] = pg_status['num_pgs']
        for state in pg_status['pgs_by_state']:
            states = state['state_name'].split('+')
            for s in PG_STATES:
                key = 'num_pgs_{0}'.format(s)
                if s in states:
                    stats[key] += state['count']

        return stats

    def get_cluster_stats(self):
        stats = dict()

        health = json.loads(self.get('health')['json'])
        stats['health'] = self.ceph_health_mapping.get(health['status'])

        mon_status = json.loads(self.get('mon_status')['json'])
        stats['num_mon'] = len(mon_status['monmap']['mons'])

        stats['mon_election_epoch'] = mon_status['election_epoch']
        stats['mon_outside_quorum'] = len(mon_status['outside_quorum'])
        stats['mon_quorum'] = len(mon_status['quorum'])

        osd_map = self.get('osd_map')
        stats['num_osd'] = len(osd_map['osds'])
        stats['num_pg_temp'] = len(osd_map['pg_temp'])
        stats['osd_epoch'] = osd_map['epoch']

        mgr_map = self.get('mgr_map')
        stats['mgr_available'] = int(mgr_map['available'])
        stats['num_mgr_standby'] = len(mgr_map['standbys'])
        stats['mgr_epoch'] = mgr_map['epoch']

        num_up = 0
        num_in = 0
        for osd in osd_map['osds']:
            if osd['up'] == 1:
                num_up += 1

            if osd['in'] == 1:
                num_in += 1

        stats['num_osd_up'] = num_up
        stats['num_osd_in'] = num_in

        fs_map = self.get('fs_map')
        stats['num_mds_standby'] = len(fs_map['standbys'])
        stats['num_fs'] = len(fs_map['filesystems'])
        stats['mds_epoch'] = fs_map['epoch']

        num_mds_up = 0
        for fs in fs_map['filesystems']:
            num_mds_up += len(fs['mdsmap']['up'])

        stats['num_mds_up'] = num_mds_up
        stats['num_mds'] = num_mds_up + stats['num_mds_standby']

        stats.update(self.get_pg_stats())

        for key, value in stats.items():
            yield {
                'measurement': 'ceph_cluster_stats',
                'tags': {
                    'type_instance': key,
                    'fsid': self.get_fsid()
                },
                'value': int(value)
            }

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['interval']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        self.config[option] = value

    def init_module_config(self):
        self.config['address'] = \
            self.get_module_option("address", default=self.config_keys['address'])
        self.config['interval'] = \
            int(self.get_module_option("interval",
                                default=self.config_keys['interval']))

    def now(self):
        return int(round(time.time() * 1000000000))

    def gather_measurements(self):
        return itertools.chain(
            self.get_pool_stats(),
            self.get_daemon_stats(),
            self.get_cluster_stats()
        )

    def send_to_telegraf(self):
        url = urlparse(self.config['address'])

        sock = BaseSocket(url)
        self.log.debug('Sending data to Telegraf at %s', sock.address)
        now = self.now()
        try:
            with sock as s:
                for measurement in self.gather_measurements():
                    self.log.debug(measurement)
                    line = Line(measurement['measurement'],
                                measurement['value'],
                                measurement['tags'], now)
                    self.log.debug(line.to_line_protocol())
                    s.send(line.to_line_protocol())
        except (socket.error, RuntimeError, IOError, OSError):
            self.log.exception('Failed to send statistics to Telegraf:')
        except FileNotFoundError:
            self.log.exception('Failed to open Telegraf at: %s', url.geturl())

    def shutdown(self):
        self.log.info('Stopping Telegraf module')
        self.run = False
        self.event.set()

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == 'telegraf config-show':
            return 0, json.dumps(self.config), ''
        elif cmd['prefix'] == 'telegraf config-set':
            key = cmd['key']
            value = cmd['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_config_option(key, value)
            self.set_module_option(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif cmd['prefix'] == 'telegraf send':
            self.send_to_telegraf()
            return 0, 'Sending data to Telegraf', ''

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))

    def self_test(self):
        measurements = list(self.gather_measurements())
        if len(measurements) == 0:
            raise RuntimeError('No measurements found')

    def serve(self):
        self.log.info('Starting Telegraf module')
        self.init_module_config()
        self.run = True

        self.log.debug('Waiting 10 seconds before starting')
        self.event.wait(10)

        while self.run:
            start = self.now()
            self.send_to_telegraf()
            runtime = (self.now() - start) / 1000000
            self.log.debug('Sending data to Telegraf took %d ms', runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(self.config['interval'])
