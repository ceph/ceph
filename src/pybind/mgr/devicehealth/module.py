
"""
Device health monitoring
"""

import errno
import json
from mgr_module import MgrModule, CommandResult
import rados
from threading import Event
from datetime import datetime, timedelta, date, time
from six import iteritems

TIME_FORMAT = '%Y%m%d-%H%M%S'

DEFAULTS = {
    'enable_monitoring': True,
    'scrape_frequency': str(86400),
    'retention_period': str(86400*14),
    'pool_name': 'device_health_metrics',
}

class Module(MgrModule):
    OPTIONS = [
        { 'name': 'enable_monitoring' },
        { 'name': 'scrape_frequency' },
        { 'name': 'pool_name' },
        { 'name': 'retention_period' },
    ]

    COMMANDS = [
        {
            "cmd": "device query-daemon-health-metrics "
                   "name=who,type=CephString",
            "desc": "Get device health metrics for a given daemon (OSD)",
            "perm": "r"
        },
        {
            "cmd": "device scrape-daemon-health-metrics "
                   "name=who,type=CephString",
            "desc": "Scrape and store device health metrics for a given daemon",
            "perm": "r"
        },
        {
            "cmd": "device scrape-health-metrics name=devid,type=CephString,req=False",
            "desc": "Scrape and store health metrics",
            "perm": "r"
        },
        {
            "cmd": "device show-health-metrics name=devid,type=CephString name=sample,type=CephString,req=False",
            "desc": "Show stored device metrics for the device",
            "perm": "r"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        # options
        self.enable_monitoring = DEFAULTS['enable_monitoring']
        self.scrape_frequency = DEFAULTS['scrape_frequency']
        self.retention_period = DEFAULTS['retention_period']
        self.pool_name = DEFAULTS['pool_name']

        # other
        self.run = True
        self.event = Event()

    def handle_command(self, inbuf, cmd):
        self.log.error("handle_command")

        if cmd['prefix'] == 'device query-daemon-health-metrics':
            who = cmd.get('who', '')
            if who[0:4] != 'osd.':
                return (-errno.EINVAL, '', 'not a valid <osd.NNN> id')
            osd_id = who[4:]
            result = CommandResult('')
            self.send_command(result, 'osd', osd_id, json.dumps({
                'prefix': 'smart',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            return (r, outb, outs)
        elif cmd['prefix'] == 'device scrape-daemon-health-metrics':
            who = cmd.get('who', '')
            if who[0:4] != 'osd.':
                return (-errno.EINVAL, '', 'not a valid <osd.NNN> id')
            id = int(who[4:])
            return self.scrape_osd(id)
        elif cmd['prefix'] == 'device scrape-health-metrics':
            if 'devid' in cmd:
                return self.scrape_device(cmd['devid'])
            return self.scrape_all();
        elif cmd['prefix'] == 'device show-health-metrics':
            return self.show_device_metrics(cmd['devid'], cmd.get('sample'))
        else:
            # mgr should respect our self.COMMANDS and not call us for
            # any prefix we don't advertise
            raise NotImplementedError(cmd['prefix'])

    def self_test(self):
        self.refresh_config()
        osdmap = self.get('osd_map')
        osd_id = osdmap['osds'][0]['osd']
        osdmeta = self.get('osd_metadata')
        devs = osdmeta.get(str(osd_id), {}).get('device_ids')
        if devs:
            devid = devs.split()[0].split('=')[1]
            (r, before, err) = self.show_device_metrics(devid, '')
            assert r == 0
            (r, out, err) = self.scrape_device(devid)
            assert r == 0
            (r, after, err) = self.show_device_metrics(devid, '')
            assert r == 0
            assert before != after

    def refresh_config(self):
        self.enable_monitoring = self.get_config('enable_monitoring', '') is not '' or 'false'
        for opt, value in iteritems(DEFAULTS):
            setattr(self, opt, self.get_config(opt) or value)

    def serve(self):
        self.log.info("Starting")
        while self.run:
            self.refresh_config()

            # TODO normalize/align sleep interval
            sleep_interval = int(self.scrape_frequency)

            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

            # in case 'wait' was interrupted, it could mean config was changed
            # by the user; go back and read config vars
            if ret:
                continue

            self.log.debug('Waking up [%s]',
                           "active" if self.enable_monitoring else "inactive")
            if not self.enable_monitoring:
                continue
            self.log.debug('Running')

            # WRITE ME

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def open_connection(self):
        pools = self.rados.list_pools()
        is_pool = False
        for pool in pools:
            if pool == self.pool_name:
                is_pool = True
                break
        if not is_pool:
            self.log.debug('create %s pool' % self.pool_name)
            # create pool
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pool create',
                'format': 'json',
                'pool': self.pool_name,
                'pg_num': 1,
            }), '')
            r, outb, outs = result.wait()
            assert r == 0

            # set pool application
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pool application enable',
                'format': 'json',
                'pool': self.pool_name,
                'app': 'mgr_devicehealth',
            }), '')
            r, outb, outs = result.wait()
            assert r == 0

        ioctx = self.rados.open_ioctx(self.pool_name)
        return (ioctx)

    def scrape_osd(self, osd_id):
        ioctx = self.open_connection()
        raw_smart_data = self.do_scrape_osd(osd_id, ioctx)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                self.put_device_metrics(ioctx, device, data)
        ioctx.close()
        return (0, "", "")

    def scrape_all(self):
        osdmap = self.get("osd_map")
        assert osdmap is not None
        ioctx = self.open_connection()
        did_device = {}
        for osd in osdmap['osds']:
            osd_id = osd['osd']
            raw_smart_data = self.do_scrape_osd(osd_id, ioctx)
            if not raw_smart_data:
                continue
            for device, raw_data in raw_smart_data.items():
                if device in did_device:
                    self.log.debug('skipping duplicate %s' % device)
                    continue
                did_device[device] = 1
                data = self.extract_smart_features(raw_data)
                self.put_device_metrics(ioctx, device, data)

        ioctx.close()
        return (0, "", "")

    def scrape_device(self, devid):
        r = self.get("device " + devid)
        if not r or 'device' not in r.keys():
            return (-errno.ENOENT, '', 'device ' + devid + ' not found')
        daemons = r['device'].get('daemons', [])
        osds = [int(r[4:]) for r in daemons if r.startswith('osd.')]
        if not osds:
            return (-errno.EAGAIN, '',
                    'device ' + devid + ' not claimed by any active OSD daemons')
        osd_id = osds[0]
        ioctx = self.open_connection()
        raw_smart_data = self.do_scrape_osd(osd_id, ioctx, devid=devid)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                self.put_device_metrics(ioctx, device, data)
        ioctx.close()
        return (0, "", "")

    def do_scrape_osd(self, osd_id, ioctx, devid=''):
        self.log.debug('do_scrape_osd osd.%d' % osd_id)

        # scrape from osd
        result = CommandResult('')
        self.send_command(result, 'osd', str(osd_id), json.dumps({
            'prefix': 'smart',
            'format': 'json',
            'devid': devid,
        }), '')
        r, outb, outs = result.wait()

        try:
            return json.loads(outb)
        except:
            self.log.debug('Fail to parse JSON result from "%s"' % outb)

    def put_device_metrics(self, ioctx, devid, data):
        old_key = datetime.now() - timedelta(
            seconds=int(self.retention_period))
        prune = old_key.strftime(TIME_FORMAT)
        self.log.debug('put_device_metrics device %s prune %s' %
                       (devid, prune))
        erase = []
        try:
            with rados.ReadOpCtx() as op:
                iter, ret = ioctx.get_omap_keys(op, "", 500) # fixme
                assert ret == 0
                ioctx.operate_read_op(op, devid)
                for key, _ in list(iter):
                    if key >= prune:
                        break
                    erase.append(key)
        except:
            pass
        key = datetime.now().strftime(TIME_FORMAT)
        self.log.debug('put_device_metrics device %s key %s = %s, erase %s' %
                       (devid, key, data, erase))
        with rados.WriteOpCtx() as op:
            ioctx.set_omap(op, (key,), (str(json.dumps(data)),))
            if len(erase):
                ioctx.remove_omap_keys(op, tuple(erase))
            ioctx.operate_write_op(op, devid)

    def show_device_metrics(self, devid, sample):
        # verify device exists
        r = self.get("device " + devid)
        if not r or 'device' not in r.keys():
            return (-errno.ENOENT, '', 'device ' + devid + ' not found')
        # fetch metrics
        ioctx = self.open_connection()
        res = {}
        with rados.ReadOpCtx() as op:
            iter, ret = ioctx.get_omap_vals(op, "", sample or '', 500) # fixme
            assert ret == 0
            try:
                ioctx.operate_read_op(op, devid)
                for key, value in list(iter):
                    if sample and key != sample:
                        break
                    try:
                        v = json.loads(value)
                    except:
                        self.log.debug('unable to parse value for %s: "%s"' %
                                       (key, value))
                        pass
                    res[key] = v
            except:
                pass
        return (0, json.dumps(res, indent=4), '')

    def extract_smart_features(self, raw):
        # FIXME: extract and normalize raw smartctl --json output and
        # generate a dict of the fields we care about.
        return raw
