"""
Telemetry module for ceph-mgr

Collect statistics from Ceph cluster and send this back to the Ceph project
when user has opted-in
"""
import errno
import json
import re
import requests
import uuid
import time
from datetime import datetime
from threading import Event
from collections import defaultdict

from mgr_module import MgrModule


class Module(MgrModule):
    config = dict()

    metadata_keys = [
            "arch",
            "ceph_version",
            "os",
            "cpu",
            "kernel_description",
            "kernel_version",
            "distro_description",
            "distro"
    ]

    MODULE_OPTIONS = [
        {
            'name': 'url',
            'type': 'str',
            'default': 'https://telemetry.ceph.com/report'
        },
        {
            'name': 'enabled',
            'type': 'bool',
            'default': False
        },
        {
            'name': 'leaderboard',
            'type': 'bool',
            'default': False
        },
        {
            'name': 'description',
            'type': 'str',
            'default': None
        },
        {
            'name': 'contact',
            'type': 'str',
            'default': None
        },
        {
            'name': 'organization',
            'type': 'str',
            'default': None
        },
        {
            'name': 'proxy',
            'type': 'str',
            'default': None
        },
        {
            'name': 'interval',
            'type': 'int',
            'default': 72,
            'min': 24
        }
    ]

    COMMANDS = [
        {
            "cmd": "telemetry status",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "telemetry send",
            "desc": "Force sending data to Ceph telemetry",
            "perm": "rw"
        },
        {
            "cmd": "telemetry show",
            "desc": "Show last report or report to be sent",
            "perm": "r"
        },
        {
            "cmd": "telemetry on",
            "desc": "Enable telemetry reports from this cluster",
            "perm": "rw",
        },
        {
            "cmd": "telemetry off",
            "desc": "Disable telemetry reports from this cluster",
            "perm": "rw",
        },
    ]

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None)) for o in self.MODULE_OPTIONS)

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = False
        self.last_upload = None
        self.last_report = dict()
        self.report_id = None

    def config_notify(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    @staticmethod
    def parse_timestamp(timestamp):
        return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

    def load(self):
        self.last_upload = self.get_store('last_upload', None)
        if self.last_upload is not None:
            self.last_upload = int(self.last_upload)

        self.report_id = self.get_store('report_id', None)
        if self.report_id is None:
            self.report_id = str(uuid.uuid4())
            self.set_store('report_id', self.report_id)

    def gather_osd_metadata(self, osd_map):
        keys = ["osd_objectstore", "rotational"]
        keys += self.metadata_keys

        metadata = dict()
        for key in keys:
            metadata[key] = defaultdict(int)

        for osd in osd_map['osds']:
            for k, v in self.get_metadata('osd', str(osd['osd'])).items():
                if k not in keys:
                    continue

                metadata[k][v] += 1

        return metadata

    def gather_mon_metadata(self, mon_map):
        keys = list()
        keys += self.metadata_keys

        metadata = dict()
        for key in keys:
            metadata[key] = defaultdict(int)

        for mon in mon_map['mons']:
            for k, v in self.get_metadata('mon', mon['name']).items():
                if k not in keys:
                    continue

                metadata[k][v] += 1

        return metadata

    def gather_crashinfo(self):
        crashdict = dict()
        errno, crashids, err = self.remote('crash', 'do_ls', '', '')
        if errno:
            return ''
        for crashid in crashids.split():
            cmd = {'id': crashid}
            errno, crashinfo, err = self.remote('crash', 'do_info', cmd, '')
            if errno:
                continue
            crashdict[crashid] = json.loads(crashinfo)
        return crashdict

    def compile_report(self):
        report = {
            'leaderboard': False,
            'report_version': 1
        }

        if self.leaderboard:
            report['leaderboard'] = True

        for option in ['description', 'contact', 'organization']:
            report[option] = getattr(self, option)

        mon_map = self.get('mon_map')
        osd_map = self.get('osd_map')
        service_map = self.get('service_map')
        fs_map = self.get('fs_map')
        df = self.get('df')

        report['report_id'] = self.report_id
        report['created'] = self.parse_timestamp(mon_map['created']).isoformat()

        report['mon'] = {
            'count': len(mon_map['mons']),
            'features': mon_map['features']
        }

        num_pg = 0
        report['pools'] = list()
        for pool in osd_map['pools']:
            num_pg += pool['pg_num']
            report['pools'].append(
                {
                    'pool': pool['pool'],
                    'type': pool['type'],
                    'pg_num': pool['pg_num'],
                    'pgp_num': pool['pg_placement_num'],
                    'size': pool['size'],
                    'min_size': pool['min_size'],
                    'crush_rule': pool['crush_rule']
                }
            )

        report['osd'] = {
            'count': len(osd_map['osds']),
            'require_osd_release': osd_map['require_osd_release'],
            'require_min_compat_client': osd_map['require_min_compat_client']
        }

        report['fs'] = {
            'count': len(fs_map['filesystems'])
        }

        report['metadata'] = dict()
        report['metadata']['osd'] = self.gather_osd_metadata(osd_map)
        report['metadata']['mon'] = self.gather_mon_metadata(mon_map)

        report['usage'] = {
            'pools': len(df['pools']),
            'pg_num:': num_pg,
            'total_used_bytes': df['stats']['total_used_bytes'],
            'total_bytes': df['stats']['total_bytes'],
            'total_avail_bytes': df['stats']['total_avail_bytes']
        }

        report['services'] = defaultdict(int)
        for key, value in service_map['services'].items():
            report['services'][key] += 1

        report['crashes'] = self.gather_crashinfo()

        return report

    def send(self, report):
        self.log.info('Upload report to: %s', self.url)
        proxies = dict()
        if self.proxy:
            self.log.info('Using HTTP(S) proxy: %s', self.proxy)
            proxies['http'] = self.proxy
            proxies['https'] = self.proxy

        resp = requests.put(url=self.url, json=report, proxies=proxies)
        if not resp.ok:
            self.log.error("Report send failed: %d %s %s" %
                           (resp.status_code, resp.reason, resp.text))
        return resp

    def handle_command(self, inbuf, command):
        if command['prefix'] == 'telemetry status':
            r = {}
            for opt in self.MODULE_OPTIONS:
                r[opt['name']] = getattr(self, opt['name'])
            return 0, json.dumps(r, indent=4), ''
        elif command['prefix'] == 'telemetry on':
            self.set_module_option('enabled', True)
            return 0, '', ''
        elif command['prefix'] == 'telemetry off':
            self.set_module_option('enabled', False)
            return 0, '', ''
        elif command['prefix'] == 'telemetry send':
            self.last_report = self.compile_report()
            resp = self.send(self.last_report)
            if resp.ok:
                return 0, 'Report sent to {0}'.format(self.url), ''
            return 1, '', 'Failed to send report to %s: %d %s %s' % (
                self.url,
                resp.status_code,
                resp.reason,
                resp.text
            )

        elif command['prefix'] == 'telemetry show':
            report = self.last_report
            if not report:
                report = self.compile_report()
            return 0, json.dumps(report, indent=4), ''
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def self_test(self):
        report = self.compile_report()
        if len(report) == 0:
            raise RuntimeError('Report is empty')

        if 'report_id' not in report:
            raise RuntimeError('report_id not found in report')

    def shutdown(self):
        self.run = False
        self.event.set()

    def serve(self):
        self.load()
        self.config_notify()
        self.run = True

        self.log.debug('Waiting for mgr to warm up')
        self.event.wait(10)

        while self.run:
            if not self.enabled:
                self.log.info('Not sending report until configured to do so')
                self.event.wait(1800)
                continue

            now = int(time.time())
            if not self.last_upload or (now - self.last_upload) > \
                            self.interval * 3600:
                self.log.info('Compiling and sending report to %s',
                              self.url)

                try:
                    self.last_report = self.compile_report()
                except:
                    self.log.exception('Exception while compiling report:')

                try:
                    resp = self.send(self.last_report)
                    # self.send logs on failure; only update last_upload
                    # if we succeed
                    if resp.ok:
                        self.last_upload = now
                        self.set_store('last_upload', str(now))
                except:
                    self.log.exception('Exception while sending report:')
            else:
                self.log.info('Interval for sending new report has not expired')

            sleep = 3600
            self.log.debug('Sleeping for %d seconds', sleep)
            self.event.wait(sleep)

    def self_test(self):
        self.compile_report()
        return True

    @staticmethod
    def can_run():
        return True, ''
