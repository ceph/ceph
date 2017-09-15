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
    return sum(data) / float(len(data))


class ZabbixSender(object):
    def __init__(self, sender, host, port, hostname,log):
        self.sender = sender
        self.host = host
        self.port = port
	self.hostname = hostname
        self.log = log

    def send(self, data):
        if len(data) == 0:
            return

        cmd = [self.sender, '-z', self.host, '-p', str(self.port), '-s',
               self.hostname, '-vv', '-i', '-']      

        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        for key, value in data.items():
            proc.stdin.write('{0} ceph.{1} {2}\n'.format(self.hostname, key, value))

        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError('%s exited non-zero: %s' % (self.sender,
                                                           stderr))

        self.log.debug('Zabbix Sender: %s', stdout.rstrip())


class Module(MgrModule):
    run = False
    config = dict()
    ceph_health_mapping = {'HEALTH_OK': 0, 'HEALTH_WARN': 1, 'HEALTH_ERR': 2}

    config_keys = {
        'zabbix_sender': '/usr/bin/zabbix_sender',
        'zabbix_host': None,
        'zabbix_port': 10051,
        'identifier': None, 'interval': 60
    }

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
            "desc": "Force sending data to Zabbux",
            "perm": "rw"
        },
        {
            "cmd": "zabbix self-test",
            "desc": "Run a self-test on the Zabbix module",
            "perm": "r"
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def init_module_config(self):
        for key, default in self.config_keys.items():
            value = self.get_localized_config(key, default)
            if value is None:
                raise RuntimeError('Configuration key {0} not set; "ceph '
                                   'config-key set mgr/zabbix/{0} '
                                   '<value>"'.format(key))

            self.set_config_option(key, value)

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

        self.config[option] = value

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
        data['total_objects'] = df['stats']['total_objects']
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
        osd_apply_latency = list()
        osd_commit_latency = list()

        osd_stats = self.get('osd_stats')
        for osd in osd_stats['osd_stats']:
            osd_fill.append((float(osd['kb_used']) / float(osd['kb'])) * 100)
            osd_apply_latency.append(osd['perf_stat']['apply_latency_ms'])
            osd_commit_latency.append(osd['perf_stat']['commit_latency_ms'])

        try:
            data['osd_max_fill'] = max(osd_fill)
            data['osd_min_fill'] = min(osd_fill)
            data['osd_avg_fill'] = avg(osd_fill)
        except ValueError:
            pass

        try:
            data['osd_latency_apply_max'] = max(osd_apply_latency)
            data['osd_latency_apply_min'] = min(osd_apply_latency)
            data['osd_latency_apply_avg'] = avg(osd_apply_latency)

            data['osd_latency_commit_max'] = max(osd_commit_latency)
            data['osd_latency_commit_min'] = min(osd_commit_latency)
            data['osd_latency_commit_avg'] = avg(osd_commit_latency)
        except ValueError:
            pass

        pg_summary = self.get('pg_summary')
        num_pg = 0
        for state, num in pg_summary['all'].items():
            num_pg += num

        data['num_pg'] = num_pg

        return data

    def send(self):
        data = self.get_data()

        self.log.debug('Sending data to Zabbix server %s',
                       self.config['zabbix_host'])
        self.log.debug(data)

        try:
            zabbix = ZabbixSender(self.config['zabbix_sender'],
                                  self.config['zabbix_host'],
                                  self.config['zabbix_port'],
				  self.config['identifier'],
				  self.log)
            zabbix.send(data)
        except Exception as exc:
            self.log.error('Exception when sending: %s', exc)

    def handle_command(self, command):
        if command['prefix'] == 'zabbix config-show':
            return 0, json.dumps(self.config), ''
        elif command['prefix'] == 'zabbix config-set':
            key = command['key']
            value = command['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_config_option(key, value)
            self.set_localized_config(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif command['prefix'] == 'zabbix send':
            self.send()
            return 0, 'Sending data to Zabbix', ''
        elif command['prefix'] == 'zabbix self-test':
            self.self_test()
            return 0, 'Self-test succeeded', ''
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping zabbix')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.debug('Zabbix module starting up')
        self.run = True

        self.init_module_config()

        for key, value in self.config.items():
            self.log.debug('%s: %s', key, value)

        while self.run:
            self.log.debug('Waking up for new iteration')

            # Sometimes fetching data fails, should be fixed by PR #16020
            try:
                self.send()
            except Exception as exc:
                self.log.error(exc)

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
