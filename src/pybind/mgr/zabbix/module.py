"""
Zabbix module for ceph-mgr

Collect statistics from Ceph cluster and every X seconds send data to a Zabbix
server using the zabbix_sender executable.
"""
import json
import errno
from subprocess import Popen, PIPE
from threading import Event
from mgr_module import MgrModule


def avg(data):
    if len(data):
        return sum(data) / float(len(data))
    else:
        return 0


class ZabbixSender(object):
    def __init__(self, sender, host, port, log):
        self.sender = sender
        self.host = host
        self.port = port
        self.log = log

    def send(self, hostname, data):
        if len(data) == 0:
            return

        cmd = [self.sender, '-z', self.host, '-p', str(self.port), '-s',
               hostname, '-vv', '-i', '-']

        self.log.debug('Executing: %s', cmd)

        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        for key, value in data.items():
            proc.stdin.write('{0} ceph.{1} {2}\n'.format(hostname, key, value))

        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError('%s exited non-zero: %s' % (self.sender,
                                                           stderr))

        self.log.debug('Zabbix Sender: %s', stdout.rstrip())


class Module(MgrModule):
    run = False
    config = dict()
    ceph_health_mapping = {'HEALTH_OK': 0, 'HEALTH_WARN': 1, 'HEALTH_ERR': 2}

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None))
                for o in self.MODULE_OPTIONS)

    MODULE_OPTIONS = [
            {
                'name': 'zabbix_sender',
                'default': '/usr/bin/zabbix_sender'
            },
            {
                'name': 'zabbix_host',
                'default': None
            },
            {
                'name': 'zabbix_port',
                'type': 'int',
                'default': 10051
            },
            {
                'name': 'identifier',
                'default': ""
            },
            {
                'name': 'interval',
                'type': 'secs',
                'default': 60
            }
    ]

    COMMANDS = [
        {
            "cmd": "zabbix config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "zabbix config-show",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "zabbix send",
            "desc": "Force sending data to Zabbix",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def init_module_config(self):
        self.fsid = self.get('mon_map')['fsid']
        self.log.debug('Found Ceph fsid %s', self.fsid)

        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_module_option(key, default))

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['zabbix_port', 'interval']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 10:
            raise RuntimeError('interval should be set to at least 10 seconds')

        self.log.debug('Setting in-memory config option %s to: %s', option,
                       value)
        self.config[option] = value
        return True

    def get_pg_stats(self):
        stats = dict()

        pg_states = ['active', 'peering', 'clean', 'scrubbing', 'undersized',
                     'backfilling', 'recovering', 'degraded', 'inconsistent',
                     'remapped', 'backfill_toofull', 'wait_backfill',
                     'recovery_wait']

        for state in pg_states:
            stats['num_pg_{0}'.format(state)] = 0

        pg_status = self.get('pg_status')

        stats['num_pg'] = pg_status['num_pgs']

        for state in pg_status['pgs_by_state']:
            states = state['state_name'].split('+')
            for s in pg_states:
                key = 'num_pg_{0}'.format(s)
                if s in states:
                    stats[key] += state['count']

        return stats

    def get_data(self):
        data = dict()

        health = json.loads(self.get('health')['json'])
        # 'status' is luminous+, 'overall_status' is legacy mode.
        data['overall_status'] = health.get('status',
                                            health.get('overall_status'))
        data['overall_status_int'] = \
            self.ceph_health_mapping.get(data['overall_status'])

        mon_status = json.loads(self.get('mon_status')['json'])
        data['num_mon'] = len(mon_status['monmap']['mons'])

        df = self.get('df')
        data['num_pools'] = len(df['pools'])
        data['total_used_bytes'] = df['stats']['total_used_bytes']
        data['total_bytes'] = df['stats']['total_bytes']
        data['total_avail_bytes'] = df['stats']['total_avail_bytes']

        wr_ops = 0
        rd_ops = 0
        wr_bytes = 0
        rd_bytes = 0

        for pool in df['pools']:
            wr_ops += pool['stats']['wr']
            rd_ops += pool['stats']['rd']
            wr_bytes += pool['stats']['wr_bytes']
            rd_bytes += pool['stats']['rd_bytes']

        data['wr_ops'] = wr_ops
        data['rd_ops'] = rd_ops
        data['wr_bytes'] = wr_bytes
        data['rd_bytes'] = rd_bytes

        osd_map = self.get('osd_map')
        data['num_osd'] = len(osd_map['osds'])
        data['osd_nearfull_ratio'] = osd_map['nearfull_ratio']
        data['osd_full_ratio'] = osd_map['full_ratio']
        data['osd_backfillfull_ratio'] = osd_map['backfillfull_ratio']

        data['num_pg_temp'] = len(osd_map['pg_temp'])

        num_up = 0
        num_in = 0
        for osd in osd_map['osds']:
            if osd['up'] == 1:
                num_up += 1

            if osd['in'] == 1:
                num_in += 1

        data['num_osd_up'] = num_up
        data['num_osd_in'] = num_in

        osd_fill = list()
        osd_pgs = list()
        osd_apply_latency_ns = list()
        osd_commit_latency_ns = list()

        osd_stats = self.get('osd_stats')
        for osd in osd_stats['osd_stats']:
            if osd['kb'] == 0:
                continue
            osd_fill.append((float(osd['kb_used']) / float(osd['kb'])) * 100)
            osd_pgs.append(osd['num_pgs'])
            osd_apply_latency_ns.append(osd['perf_stat']['apply_latency_ns'])
            osd_commit_latency_ns.append(osd['perf_stat']['commit_latency_ns'])

        try:
            data['osd_max_fill'] = max(osd_fill)
            data['osd_min_fill'] = min(osd_fill)
            data['osd_avg_fill'] = avg(osd_fill)
            data['osd_max_pgs'] = max(osd_pgs)
            data['osd_min_pgs'] = min(osd_pgs)
            data['osd_avg_pgs'] = avg(osd_pgs)
        except ValueError:
            pass

        try:
            data['osd_latency_apply_max'] = max(osd_apply_latency_ns) / 1000000.0 # ns -> ms
            data['osd_latency_apply_min'] = min(osd_apply_latency_ns) / 1000000.0 # ns -> ms
            data['osd_latency_apply_avg'] = avg(osd_apply_latency_ns) / 1000000.0 # ns -> ms

            data['osd_latency_commit_max'] = max(osd_commit_latency_ns) / 1000000.0 # ns -> ms
            data['osd_latency_commit_min'] = min(osd_commit_latency_ns) / 1000000.0 # ns -> ms
            data['osd_latency_commit_avg'] = avg(osd_commit_latency_ns) / 1000000.0 # ns -> ms
        except ValueError:
            pass

        data.update(self.get_pg_stats())

        return data

    def send(self):
        data = self.get_data()

        identifier = self.config['identifier']
        if identifier is None or len(identifier) == 0:
            identifier = 'ceph-{0}'.format(self.fsid)

        if not self.config['zabbix_host']:
            self.log.error('Zabbix server not set, please configure using: '
                           'ceph zabbix config-set zabbix_host <zabbix_host>')
            self.set_health_checks({
                'MGR_ZABBIX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No Zabbix server configured',
                    'detail': ['Configuration value zabbix_host not configured']
                }
            })
            return

        try:
            self.log.info(
                'Sending data to Zabbix server %s as host/identifier %s',
                self.config['zabbix_host'], identifier)
            self.log.debug(data)

            zabbix = ZabbixSender(self.config['zabbix_sender'],
                                  self.config['zabbix_host'],
                                  self.config['zabbix_port'], self.log)

            zabbix.send(identifier, data)
            self.set_health_checks(dict())
            return True
        except Exception as exc:
            self.log.error('Exception when sending: %s', exc)
            self.set_health_checks({
                'MGR_ZABBIX_SEND_FAILED': {
                    'severity': 'warning',
                    'summary': 'Failed to send data to Zabbix',
                    'detail': [str(exc)]
                }
            })

        return False

    def handle_command(self, inbuf, command):
        if command['prefix'] == 'zabbix config-show':
            return 0, json.dumps(self.config), ''
        elif command['prefix'] == 'zabbix config-set':
            key = command['key']
            value = command['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            if self.set_config_option(key, value):
                self.set_module_option(key, value)
                return 0, 'Configuration option {0} updated'.format(key), ''

            return 1,\
                'Failed to update configuration option {0}'.format(key), ''

        elif command['prefix'] == 'zabbix send':
            if self.send():
                return 0, 'Sending data to Zabbix', ''

            return 1, 'Failed to send data to Zabbix', ''
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping zabbix')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.info('Zabbix module starting up')
        self.run = True

        self.init_module_config()

        while self.run:
            self.log.debug('Waking up for new iteration')

            try:
                self.send()
            except Exception as exc:
                # Shouldn't happen, but let's log it and retry next interval,
                # rather than dying completely.
                self.log.exception("Unexpected error during send():")

            interval = self.config['interval']
            self.log.debug('Sleeping for %d seconds', interval)
            self.event.wait(interval)

    def self_test(self):
        data = self.get_data()

        if data['overall_status'] not in self.ceph_health_mapping:
            raise RuntimeError('No valid overall_status found in data')

        int(data['overall_status_int'])

        if data['num_mon'] < 1:
            raise RuntimeError('num_mon is smaller than 1')
