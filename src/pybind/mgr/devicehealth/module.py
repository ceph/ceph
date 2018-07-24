"""
Device health monitoring
"""

import errno
import json
from mgr_module import MgrModule, CommandResult
import operator
import rados
from threading import Event
from datetime import datetime, timedelta, date, time
from six import iteritems

TIME_FORMAT = '%Y%m%d-%H%M%S'

DEFAULTS = {
    'enable_monitoring': str(True),
    'scrape_frequency': str(86400),
    'retention_period': str(86400 * 14),
    'pool_name': 'device_health_metrics',
    'mark_out_threshold': str(86400*14),
    'warn_threshold': str(86400*14*2),
    'self_heal': str(True),
}

DEVICE_HEALTH = 'DEVICE_HEALTH'
DEVICE_HEALTH_IN_USE = 'DEVICE_HEALTH_IN_USE'
DEVICE_HEALTH_TOOMANY = 'DEVICE_HEALTH_TOOMANY'
HEALTH_MESSAGES = {
    DEVICE_HEALTH: '%d device(s) expected to fail soon',
    DEVICE_HEALTH_IN_USE: '%d daemons(s) expected to fail soon and still contain data',
    DEVICE_HEALTH_TOOMANY: 'Too many daemons are expected to fail soon',
}


class Module(MgrModule):
    OPTIONS = [
        {'name': 'enable_monitoring'},
        {'name': 'scrape_frequency'},
        {'name': 'pool_name'},
        {'name': 'retention_period'},
        {'name': 'mark_out_threshold'},
        {'name': 'warn_threshold'},
        {'name': 'self_heal'},
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
            "desc": "Scrape and store device health metrics "
                    "for a given daemon",
            "perm": "r"
        },
        {
            "cmd": "device scrape-health-metrics "
                   "name=devid,type=CephString,req=False",
            "desc": "Scrape and store health metrics",
            "perm": "r"
        },
        {
            "cmd": "device show-health-metrics "
                   "name=devid,type=CephString "
                   "name=sample,type=CephString,req=False",
            "desc": "Show stored device metrics for the device",
            "perm": "r"
        },
        {
            "cmd": "device check-health",
            "desc": "Check life expectancy of devices",
            "perm": "rw",
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        # options
        for k, v in DEFAULTS.iteritems():
            setattr(self, k, v)

        # other
        self.run = True
        self.event = Event()

    def handle_command(self, _, cmd):
        self.log.error("handle_command")

        if cmd['prefix'] == 'device query-daemon-health-metrics':
            who = cmd.get('who', '')
            if who[0:4] != 'osd.':
                return -errno.EINVAL, '', 'not a valid <osd.NNN> id'
            osd_id = who[4:]
            result = CommandResult('')
            self.send_command(result, 'osd', osd_id, json.dumps({
                'prefix': 'smart',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            return r, outb, outs
        elif cmd['prefix'] == 'device scrape-daemon-health-metrics':
            who = cmd.get('who', '')
            if who[0:4] != 'osd.':
                return -errno.EINVAL, '', 'not a valid <osd.NNN> id'
            osd_id = int(who[4:])
            return self.scrape_osd(osd_id)
        elif cmd['prefix'] == 'device scrape-health-metrics':
            if 'devid' in cmd:
                return self.scrape_device(cmd['devid'])
            return self.scrape_all()
        elif cmd['prefix'] == 'device show-health-metrics':
            return self.show_device_metrics(cmd['devid'], cmd.get('sample'))
        elif cmd['prefix'] == 'device check-health':
            return self.check_health()
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
        self.enable_monitoring = self.get_config('enable_monitoring',
                                                 '') is not '' or 'false'
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
            self.check_health()

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
        return ioctx

    def scrape_osd(self, osd_id):
        ioctx = self.open_connection()
        raw_smart_data = self.do_scrape_osd(osd_id)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                self.put_device_metrics(ioctx, device, data)
        ioctx.close()
        return 0, "", ""

    def scrape_all(self):
        osdmap = self.get("osd_map")
        assert osdmap is not None
        ioctx = self.open_connection()
        did_device = {}
        for osd in osdmap['osds']:
            osd_id = osd['osd']
            raw_smart_data = self.do_scrape_osd(osd_id)
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
        return 0, "", ""

    def scrape_device(self, devid):
        r = self.get("device " + devid)
        if not r or 'device' not in r.keys():
            return -errno.ENOENT, '', 'device ' + devid + ' not found'
        daemons = r['device'].get('daemons', [])
        osds = [int(r[4:]) for r in daemons if r.startswith('osd.')]
        if not osds:
            return (-errno.EAGAIN, '',
                    'device ' + devid + ' not claimed by any active '
                                        'OSD daemons')
        osd_id = osds[0]
        ioctx = self.open_connection()
        raw_smart_data = self.do_scrape_osd(osd_id, devid=devid)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                self.put_device_metrics(ioctx, device, data)
        ioctx.close()
        return 0, "", ""

    def do_scrape_osd(self, osd_id, devid=''):
        """
        :return: a dict, or None if the scrape failed.
        """
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
        except (IndexError, ValueError):
            self.log.error(
                "Fail to parse JSON result from OSD {0} ({1})".format(
                    osd_id, outb))

    def put_device_metrics(self, ioctx, devid, data):
        old_key = datetime.now() - timedelta(
            seconds=int(self.retention_period))
        prune = old_key.strftime(TIME_FORMAT)
        self.log.debug('put_device_metrics device %s prune %s' %
                       (devid, prune))
        erase = []
        try:
            with rados.ReadOpCtx() as op:
                omap_iter, ret = ioctx.get_omap_keys(op, "", 500)  # fixme
                assert ret == 0
                ioctx.operate_read_op(op, devid)
                for key, _ in list(omap_iter):
                    if key >= prune:
                        break
                    erase.append(key)
        except rados.ObjectNotFound:
            # The object doesn't already exist, no problem.
            pass
        except rados.Error as e:
            # Do not proceed with writes if something unexpected
            # went wrong with the reads.
            log.exception("Error reading OMAP: {0}".format(e))
            return

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
            return -errno.ENOENT, '', 'device ' + devid + ' not found'
        # fetch metrics
        ioctx = self.open_connection()
        res = {}
        with rados.ReadOpCtx() as op:
            omap_iter, ret = ioctx.get_omap_vals(op, "", sample or '', 500)  # fixme
            assert ret == 0
            try:
                ioctx.operate_read_op(op, devid)
                for key, value in list(omap_iter):
                    if sample and key != sample:
                        break
                    try:
                        v = json.loads(value)
                    except (ValueError, IndexError):
                        self.log.debug('unable to parse value for %s: "%s"' %
                                       (key, value))
                        pass
                    else:
                        res[key] = v
            except rados.ObjectNotFound:
                pass
            except rados.Error as e:
                log.exception("RADOS error reading omap: {0}".format(e))
                raise

        return 0, json.dumps(res, indent=4), ''

    def check_health(self):
        self.log.info('Check health')
        config = self.get('config')
        min_in_ratio = float(config.get('mon_osd_min_in_ratio'))
        mark_out_threshold_td = timedelta(seconds=int(self.mark_out_threshold))
        warn_threshold_td = timedelta(seconds=int(self.warn_threshold))
        checks = {}
        health_warnings = {
            DEVICE_HEALTH: [],
            DEVICE_HEALTH_IN_USE: [],
            }
        devs = self.get("devices")
        osds_in = {}
        osds_out = {}
        now = datetime.now()
        osdmap = self.get("osd_map")
        assert osdmap is not None
        for dev in devs['devices']:
            devid = dev['devid']
            if 'life_expectancy_min' not in dev:
                continue
            # life_expectancy_(min/max) is in the format of:
            # '%Y-%m-%d %H:%M:%S.%f', e.g.:
            # '2019-01-20 21:12:12.000000'
            life_expectancy_min = datetime.strptime(
                dev['life_expectancy_min'],
                '%Y-%m-%d %H:%M:%S.%f')
            self.log.debug('device %s expectancy min %s', dev,
                           life_expectancy_min)

            if life_expectancy_min - now <= mark_out_threshold_td:
                if self.self_heal:
                    # dev['daemons'] == ["osd.0","osd.1","osd.2"]
                    if dev['daemons']:
                        osds = [x for x in dev['daemons']
                                if x.startswith('osd.')]
                        osd_ids = map(lambda x: x[4:], osds)
                        for _id in osd_ids:
                            if self.is_osd_in(osdmap, _id):
                                osds_in[_id] = life_expectancy_min
                            else:
                                osds_out[_id] = 1

            if life_expectancy_min - now <= warn_threshold_td:
                # device can appear in more than one location in case
                # of SCSI multipath
                device_locations = map(lambda x: x['host'] + ':' + x['dev'],
                                       dev['location'])
                health_warnings[DEVICE_HEALTH].append(
                    '%s (%s); daemons %s; life expectancy between %s and %s'
                    % (dev['devid'],
                       ','.join(device_locations),
                       ','.join(dev.get('daemons', ['none'])),
                       dev['life_expectancy_min'],
                       dev.get('life_expectancy_max', 'unknown')))
                # TODO: by default, dev['life_expectancy_max'] == '0.000000',
                # so dev.get('life_expectancy_max', 'unknown')
                # above should be altered.

        # OSD might be marked 'out' (which means it has no
        # data), however PGs are still attached to it.
        for _id in osds_out.iterkeys():
            num_pgs = self.get_osd_num_pgs(_id)
            if num_pgs > 0:
                health_warnings[DEVICE_HEALTH_IN_USE].append(
                    'osd.%s is marked out '
                    'but still has %s PG(s)' %
                    (_id, num_pgs))
        if osds_in:
            self.log.debug('osds_in %s' % osds_in)
            # calculate target in ratio
            num_osds = len(osdmap['osds'])
            num_in = len([x for x in osdmap['osds'] if x['in']])
            num_bad = len(osds_in)
            # sort with next-to-fail first
            bad_osds = sorted(osds_in.items(), key=operator.itemgetter(1))
            did = 0
            to_mark_out = []
            for osd_id, when in bad_osds:
                ratio = float(num_in - did - 1) / float(num_osds)
                if ratio < min_in_ratio:
                    final_ratio = float(num_in - num_bad) / float(num_osds)
                    checks[DEVICE_HEALTH_TOOMANY] = {
                        'severity': 'warning',
                        'summary': HEALTH_MESSAGES[DEVICE_HEALTH_TOOMANY],
                        'detail': [
                            '%d OSDs with failing device(s) would bring "in" ratio to %f < mon_osd_min_in_ratio %f' % (num_bad - did, final_ratio, min_in_ratio)
                        ]
                    }
                    break
                to_mark_out.append(osd_id)
                did += 1
            if to_mark_out:
                self.mark_out_etc(to_mark_out)
        for warning, ls in health_warnings.iteritems():
            n = len(ls)
            if n:
                checks[warning] = {
                    'severity': 'warning',
                    'summary': HEALTH_MESSAGES[warning] % n,
                    'detail': ls,
                }
        self.set_health_checks(checks)
        return 0, "", ""

    def is_osd_in(self, osdmap, osd_id):
        for osd in osdmap['osds']:
            if str(osd_id) == str(osd['osd']):
                return bool(osd['in'])
        return False

    def get_osd_num_pgs(self, osd_id):
        stats = self.get('osd_stats')
        assert stats is not None
        for stat in stats['osd_stats']:
            if str(osd_id) == str(stat['osd']):
                return stat['num_pgs']
        return -1

    def mark_out_etc(self, osd_ids):
        self.log.info('Marking out OSDs: %s' % osd_ids)
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd out',
            'format': 'json',
            'ids': osd_ids,
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.warn('Could not mark OSD %s out. r: [%s], outb: [%s], outs: [%s]' % (osd_ids, r, outb, outs))
        for osd_id in osd_ids:
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd primary-affinity',
                'format': 'json',
                'id': int(osd_id),
                'weight': 0.0,
            }), '')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.warn('Could not set osd.%s primary-affinity, r: [%s], outs: [%s]' % (osd_id, r, outb, outs))

    def extract_smart_features(self, raw):
        # FIXME: extract and normalize raw smartctl --json output and
        # generate a dict of the fields we care about.
        return raw
