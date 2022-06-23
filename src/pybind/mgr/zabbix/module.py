"""
Zabbix module for ceph-mgr

Collect statistics from Ceph cluster and every X seconds send data to a Zabbix
server using the zabbix_sender executable.
"""
import logging
import json
import errno
import re
from subprocess import Popen, PIPE
from threading import Event
from mgr_module import CLIReadCommand, CLIWriteCommand, MgrModule, Option, OptionValue
from typing import cast, Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union


def avg(data: Sequence[Union[int, float]]) -> float:
    if len(data):
        return sum(data) / float(len(data))
    else:
        return 0


class ZabbixSender(object):
    def __init__(self, sender: str, host: str, port: int, log: logging.Logger) -> None:
        self.sender = sender
        self.host = host
        self.port = port
        self.log = log

    def send(self, hostname: str, data: Mapping[str, Union[int, float, str]]) -> None:
        if len(data) == 0:
            return

        cmd = [self.sender, '-z', self.host, '-p', str(self.port), '-s',
               hostname, '-vv', '-i', '-']

        self.log.debug('Executing: %s', cmd)

        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, encoding='utf-8')

        for key, value in data.items():
            assert proc.stdin
            proc.stdin.write('{0} ceph.{1} {2}\n'.format(hostname, key, value))

        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError('%s exited non-zero: %s' % (self.sender,
                                                           stderr))

        self.log.debug('Zabbix Sender: %s', stdout.rstrip())


class Module(MgrModule):
    run = False
    config: Dict[str, OptionValue] = {}
    ceph_health_mapping = {'HEALTH_OK': 0, 'HEALTH_WARN': 1, 'HEALTH_ERR': 2}
    _zabbix_hosts: List[Dict[str, Union[str, int]]] = list()

    @property
    def config_keys(self) -> Dict[str, OptionValue]:
        return dict((o['name'], o.get('default', None))
                for o in self.MODULE_OPTIONS)

    MODULE_OPTIONS = [
        Option(
            name='zabbix_sender',
            default='/usr/bin/zabbix_sender'),
        Option(
            name='zabbix_host',
            type='str',
            default=None),
        Option(
            name='zabbix_port',
            type='int',
            default=10051),
        Option(
            name='identifier',
            default=""),
        Option(
            name='interval',
            type='secs',
            default=60),
        Option(
            name='discovery_interval',
            type='uint',
            default=100)
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def init_module_config(self) -> None:
        self.fsid = self.get('mon_map')['fsid']
        self.log.debug('Found Ceph fsid %s', self.fsid)

        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_module_option(key, default))

        if self.config['zabbix_host']:
            self._parse_zabbix_hosts()

    def set_config_option(self, option: str, value: OptionValue) -> bool:
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['zabbix_port', 'interval', 'discovery_interval']:
            try:
                int_value = int(value)  # type: ignore
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and int_value < 10:
            raise RuntimeError('interval should be set to at least 10 seconds')

        if option == 'discovery_interval' and int_value < 10:
            raise RuntimeError(
                "discovery_interval should not be more frequent "
                "than once in 10 regular data collection"
            )

        self.log.debug('Setting in-memory config option %s to: %s', option,
                       value)
        self.config[option] = value
        return True

    def _parse_zabbix_hosts(self) -> None:
        self._zabbix_hosts = list()
        servers = cast(str, self.config['zabbix_host']).split(",")
        for server in servers:
            uri = re.match("(?:(?:\[?)([a-z0-9-\.]+|[a-f0-9:\.]+)(?:\]?))(?:((?::))([0-9]{1,5}))?$", server)
            if uri:
                zabbix_host, sep, opt_zabbix_port = uri.groups()
                if sep == ':':
                    zabbix_port = int(opt_zabbix_port)
                else:
                    zabbix_port = cast(int, self.config['zabbix_port'])
                self._zabbix_hosts.append({'zabbix_host': zabbix_host, 'zabbix_port': zabbix_port})
            else:
                self.log.error('Zabbix host "%s" is not valid', server)

        self.log.error('Parsed Zabbix hosts: %s', self._zabbix_hosts)

    def get_pg_stats(self) -> Dict[str, int]:
        stats = dict()

        pg_states = ['active', 'peering', 'clean', 'scrubbing', 'undersized',
                     'backfilling', 'recovering', 'degraded', 'inconsistent',
                     'remapped', 'backfill_toofull', 'backfill_wait',
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

    def get_data(self) -> Dict[str, Union[int, float]]:
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
            data['[{0},rd_bytes]'.format(pool['name'])] = pool['stats']['rd_bytes']
            data['[{0},wr_bytes]'.format(pool['name'])] = pool['stats']['wr_bytes']
            data['[{0},rd_ops]'.format(pool['name'])] = pool['stats']['rd']
            data['[{0},wr_ops]'.format(pool['name'])] = pool['stats']['wr']
            data['[{0},bytes_used]'.format(pool['name'])] = pool['stats']['bytes_used']
            data['[{0},stored_raw]'.format(pool['name'])] = pool['stats']['stored_raw']
            data['[{0},percent_used]'.format(pool['name'])] = pool['stats']['percent_used'] * 100

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
            data['[osd.{0},up]'.format(int(osd['osd']))] = osd['up']
            if osd['up'] == 1:
                num_up += 1

            data['[osd.{0},in]'.format(int(osd['osd']))] = osd['in']
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
            try:
                osd_fill.append((float(osd['kb_used']) / float(osd['kb'])) * 100)
                data['[osd.{0},osd_fill]'.format(osd['osd'])] = (
                    float(osd['kb_used']) / float(osd['kb'])) * 100
            except ZeroDivisionError:
                continue
            osd_pgs.append(osd['num_pgs'])
            osd_apply_latency_ns.append(osd['perf_stat']['apply_latency_ns'])
            osd_commit_latency_ns.append(osd['perf_stat']['commit_latency_ns'])
            data['[osd.{0},num_pgs]'.format(osd['osd'])] = osd['num_pgs']
            data[
                '[osd.{0},osd_latency_apply]'.format(osd['osd'])
            ] = osd['perf_stat']['apply_latency_ns']  / 1000000.0 # ns -> ms
            data[
                '[osd.{0},osd_latency_commit]'.format(osd['osd'])
            ] = osd['perf_stat']['commit_latency_ns']  / 1000000.0 # ns -> ms

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

    def send(self, data: Mapping[str, Union[int, float, str]]) -> bool:
        identifier = cast(Optional[str], self.config['identifier'])
        if identifier is None or len(identifier) == 0:
            identifier = 'ceph-{0}'.format(self.fsid)

        if not self.config['zabbix_host'] or not self._zabbix_hosts:
            self.log.error('Zabbix server not set, please configure using: '
                           'ceph zabbix config-set zabbix_host <zabbix_host>')
            self.set_health_checks({
                'MGR_ZABBIX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No Zabbix server configured',
                    'detail': ['Configuration value zabbix_host not configured']
                }
            })
            return False

        result = True

        for server in self._zabbix_hosts:
            self.log.info(
                'Sending data to Zabbix server %s, port %s as host/identifier %s',
                server['zabbix_host'], server['zabbix_port'], identifier)
            self.log.debug(data)

            try:
                zabbix = ZabbixSender(cast(str, self.config['zabbix_sender']),
                                      cast(str, server['zabbix_host']),
                                      cast(int, server['zabbix_port']), self.log)
                zabbix.send(identifier, data)
            except Exception as exc:
                self.log.exception('Failed to send.')
                self.set_health_checks({
                    'MGR_ZABBIX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to Zabbix',
                        'detail': [str(exc)]
                    }
                })
                result = False

        self.set_health_checks(dict())
        return result

    def discovery(self) -> bool:
        osd_map = self.get('osd_map')
        osd_map_crush = self.get('osd_map_crush')

        # Discovering ceph pools
        pool_discovery = {
            pool['pool_name']: step['item_name']
            for pool in osd_map['pools']
            for rule in osd_map_crush['rules'] if rule['rule_id'] == pool['crush_rule']
            for step in rule['steps'] if step['op'] == "take"
        }
        pools_discovery_data = {"data": [
            {
                "{#POOL}": pool,
                "{#CRUSH_RULE}": rule
            }
            for pool, rule in pool_discovery.items()
        ]}

        # Discovering OSDs
        # Getting hosts for found crush rules
        osd_roots = {
            step['item_name']: [
                item['id']
                for item in root_bucket['items']
            ]
            for rule in osd_map_crush['rules']
            for step in rule['steps'] if step['op'] == "take"
            for root_bucket in osd_map_crush['buckets']
            if root_bucket['id'] == step['item']
        }
        # Getting osds for hosts with map to crush_rule
        osd_discovery = {
            item['id']: crush_rule
            for crush_rule, roots in osd_roots.items()
            for root in roots
            for bucket in osd_map_crush['buckets']
            if bucket['id'] == root
            for item in bucket['items']
        }
        osd_discovery_data = {"data": [
            {
                "{#OSD}": osd,
                "{#CRUSH_RULE}": rule
            }
            for osd, rule in osd_discovery.items()
        ]}
        # Preparing received data for sending
        data = {
            "zabbix.pool.discovery": json.dumps(pools_discovery_data),
            "zabbix.osd.discovery": json.dumps(osd_discovery_data)
        }
        return bool(self.send(data))

    @CLIReadCommand('zabbix config-show')
    def config_show(self) -> Tuple[int, str, str]:
        """
        Show current configuration
        """
        return 0, json.dumps(self.config, indent=4, sort_keys=True), ''

    @CLIWriteCommand('zabbix config-set')
    def config_set(self, key: str, value: str) -> Tuple[int, str, str]:
        """
        Set a configuration value
        """
        if not value:
            return -errno.EINVAL, '', 'Value should not be empty or None'

        self.log.debug('Setting configuration option %s to %s', key, value)
        if self.set_config_option(key, value):
            self.set_module_option(key, value)
            if key == 'zabbix_host' or key == 'zabbix_port':
                self._parse_zabbix_hosts()
                return 0, 'Configuration option {0} updated'.format(key), ''
        return 1,\
            'Failed to update configuration option {0}'.format(key), ''

    @CLIReadCommand('zabbix send')
    def do_send(self) -> Tuple[int, str, str]:
        """
        Force sending data to Zabbix
        """
        data = self.get_data()
        if self.send(data):
            return 0, 'Sending data to Zabbix', ''

        return 1, 'Failed to send data to Zabbix', ''

    @CLIReadCommand('zabbix discovery')
    def do_discovery(self) -> Tuple[int, str, str]:
        """
        Discovering Zabbix data
        """
        if self.discovery():
            return 0, 'Sending discovery data to Zabbix', ''

        return 1, 'Failed to send discovery data to Zabbix', ''

    def shutdown(self) -> None:
        self.log.info('Stopping zabbix')
        self.run = False
        self.event.set()

    def serve(self) -> None:
        self.log.info('Zabbix module starting up')
        self.run = True

        self.init_module_config()

        discovery_interval = self.config['discovery_interval']
        # We are sending discovery once plugin is loaded
        discovery_counter = cast(int, discovery_interval)
        while self.run:
            self.log.debug('Waking up for new iteration')

            if discovery_counter == discovery_interval:
                try:
                    self.discovery()
                except Exception:
                    # Shouldn't happen, but let's log it and retry next interval,
                    # rather than dying completely.
                    self.log.exception("Unexpected error during discovery():")
                finally:
                    discovery_counter = 0

            try:
                data = self.get_data()
                self.send(data)
            except Exception:
                # Shouldn't happen, but let's log it and retry next interval,
                # rather than dying completely.
                self.log.exception("Unexpected error during send():")

            interval = cast(float, self.config['interval'])
            self.log.debug('Sleeping for %d seconds', interval)
            discovery_counter += 1
            self.event.wait(interval)

    def self_test(self) -> None:
        data = self.get_data()

        if data['overall_status'] not in self.ceph_health_mapping:
            raise RuntimeError('No valid overall_status found in data')

        int(data['overall_status_int'])

        if data['num_mon'] < 1:
            raise RuntimeError('num_mon is smaller than 1')
