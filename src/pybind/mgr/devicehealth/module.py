"""
Device health monitoring
"""

import errno
import json
from mgr_module import MgrModule, CommandResult, MgrModuleRecoverDB, CLIRequiresDB, CLICommand, CLIReadCommand, Option, MgrDBNotReady
import operator
import rados
import re
from threading import Event
from datetime import datetime, timedelta, timezone
from typing import cast, Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING, Union

TIME_FORMAT = '%Y%m%d-%H%M%S'

DEVICE_HEALTH = 'DEVICE_HEALTH'
DEVICE_HEALTH_IN_USE = 'DEVICE_HEALTH_IN_USE'
DEVICE_HEALTH_TOOMANY = 'DEVICE_HEALTH_TOOMANY'
HEALTH_MESSAGES = {
    DEVICE_HEALTH: '%d device(s) expected to fail soon',
    DEVICE_HEALTH_IN_USE: '%d daemon(s) expected to fail soon and still contain data',
    DEVICE_HEALTH_TOOMANY: 'Too many daemons are expected to fail soon',
}


def get_ata_wear_level(data: Dict[Any, Any]) -> Optional[float]:
    """
    Extract wear level (as float) from smartctl -x --json output for SATA SSD
    """
    for page in data.get("ata_device_statistics", {}).get("pages", []):
        if page is None or page.get("number") != 7:
            continue
        for item in page.get("table", []):
            if item["offset"] == 8:
                return item["value"] / 100.0
    return None


def get_nvme_wear_level(data: Dict[Any, Any]) -> Optional[float]:
    """
    Extract wear level (as float) from smartctl -x --json output for NVME SSD
    """
    pct_used = data.get("nvme_smart_health_information_log", {}).get("percentage_used")
    if pct_used is None:
        return None
    return pct_used / 100.0


class Module(MgrModule):

    # latest (if db does not exist)
    SCHEMA = """
CREATE TABLE Device (
  devid TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE DeviceHealthMetrics (
  time DATETIME DEFAULT (strftime('%s', 'now')),
  devid TEXT NOT NULL REFERENCES Device (devid),
  raw_smart TEXT NOT NULL,
  PRIMARY KEY (time, devid)
);
"""

    SCHEMA_VERSIONED = [
        # v1
        """
CREATE TABLE Device (
  devid TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE DeviceHealthMetrics (
  time DATETIME DEFAULT (strftime('%s', 'now')),
  devid TEXT NOT NULL REFERENCES Device (devid),
  raw_smart TEXT NOT NULL,
  PRIMARY KEY (time, devid)
);
"""
    ]

    MODULE_OPTIONS = [
        Option(
            name='enable_monitoring',
            default=True,
            type='bool',
            desc='monitor device health metrics',
            runtime=True,
        ),
        Option(
            name='scrape_frequency',
            default=86400,
            type='secs',
            desc='how frequently to scrape device health metrics',
            runtime=True,
        ),
        Option(
            name='pool_name',
            default='device_health_metrics',
            type='str',
            desc='name of pool in which to store device health metrics',
            runtime=True,
        ),
        Option(
            name='retention_period',
            default=(86400 * 180),
            type='secs',
            desc='how long to retain device health metrics',
            runtime=True,
        ),
        Option(
            name='mark_out_threshold',
            default=(86400 * 14 * 2),
            type='secs',
            desc='automatically mark OSD if it may fail before this long',
            runtime=True,
        ),
        Option(
            name='warn_threshold',
            default=(86400 * 14 * 6),
            type='secs',
            desc='raise health warning if OSD may fail before this long',
            runtime=True,
        ),
        Option(
            name='self_heal',
            default=True,
            type='bool',
            desc='preemptively heal cluster around devices that may fail',
            runtime=True,
        ),
        Option(
            name='sleep_interval',
            default=600,
            type='secs',
            desc='how frequently to wake up and check device health',
            runtime=True,
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)

        # populate options (just until serve() runs)
        for opt in self.MODULE_OPTIONS:
            setattr(self, opt['name'], opt['default'])

        # other
        self.run = True
        self.event = Event()

        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.enable_monitoring = True
            self.scrape_frequency = 0.0
            self.pool_name = ''
            self.device_health_metrics = ''
            self.retention_period = 0.0
            self.mark_out_threshold = 0.0
            self.warn_threshold = 0.0
            self.self_heal = True
            self.sleep_interval = 0.0

    def is_valid_daemon_name(self, who: str) -> bool:
        parts = who.split('.', 1)
        if len(parts) != 2:
            return False
        return parts[0] in ('osd', 'mon')

    @CLIReadCommand('device query-daemon-health-metrics')
    def do_query_daemon_health_metrics(self, who: str) -> Tuple[int, str, str]:
        '''
        Get device health metrics for a given daemon
        '''
        if not self.is_valid_daemon_name(who):
            return -errno.EINVAL, '', 'not a valid mon or osd daemon name'
        (daemon_type, daemon_id) = who.split('.')
        result = CommandResult('')
        self.send_command(result, daemon_type, daemon_id, json.dumps({
            'prefix': 'smart',
            'format': 'json',
        }), '')
        return result.wait()

    @CLIRequiresDB
    @CLIReadCommand('device scrape-daemon-health-metrics')
    @MgrModuleRecoverDB
    def do_scrape_daemon_health_metrics(self, who: str) -> Tuple[int, str, str]:
        '''
        Scrape and store device health metrics for a given daemon
        '''
        if not self.is_valid_daemon_name(who):
            return -errno.EINVAL, '', 'not a valid mon or osd daemon name'
        (daemon_type, daemon_id) = who.split('.')
        return self.scrape_daemon(daemon_type, daemon_id)

    @CLIRequiresDB
    @CLIReadCommand('device scrape-health-metrics')
    @MgrModuleRecoverDB
    def do_scrape_health_metrics(self, devid: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Scrape and store device health metrics
        '''
        if devid is None:
            return self.scrape_all()
        else:
            return self.scrape_device(devid)

    @CLIRequiresDB
    @CLIReadCommand('device get-health-metrics')
    @MgrModuleRecoverDB
    def do_get_health_metrics(self, devid: str, sample: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Show stored device metrics for the device
        '''
        return self.show_device_metrics(devid, sample)

    @CLIRequiresDB
    @CLICommand('device check-health')
    @MgrModuleRecoverDB
    def do_check_health(self) -> Tuple[int, str, str]:
        '''
        Check life expectancy of devices
        '''
        return self.check_health()

    @CLICommand('device monitoring on')
    def do_monitoring_on(self) -> Tuple[int, str, str]:
        '''
        Enable device health monitoring
        '''
        self.set_module_option('enable_monitoring', True)
        self.event.set()
        return 0, '', ''

    @CLICommand('device monitoring off')
    def do_monitoring_off(self) -> Tuple[int, str, str]:
        '''
        Disable device health monitoring
        '''
        self.set_module_option('enable_monitoring', False)
        self.set_health_checks({})  # avoid stuck health alerts
        return 0, '', ''

    @CLIRequiresDB
    @CLIReadCommand('device predict-life-expectancy')
    @MgrModuleRecoverDB
    def do_predict_life_expectancy(self, devid: str) -> Tuple[int, str, str]:
        '''
        Predict life expectancy with local predictor
        '''
        return self.predict_lift_expectancy(devid)

    def self_test(self) -> None:
        assert self.db_ready()
        self.config_notify()
        osdmap = self.get('osd_map')
        osd_id = osdmap['osds'][0]['osd']
        osdmeta = self.get('osd_metadata')
        devs = osdmeta.get(str(osd_id), {}).get('device_ids')
        if devs:
            devid = devs.split()[0].split('=')[1]
            self.log.debug(f"getting devid {devid}")
            (r, before, err) = self.show_device_metrics(devid, None)
            assert r == 0
            self.log.debug(f"before: {before}")
            (r, out, err) = self.scrape_device(devid)
            assert r == 0
            (r, after, err) = self.show_device_metrics(devid, None)
            assert r == 0
            self.log.debug(f"after: {after}")
            assert before != after

    def config_notify(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def _legacy_put_device_metrics(self, t: str, devid: str, data: str) -> None:
        SQL = """
        INSERT OR IGNORE INTO DeviceHealthMetrics (time, devid, raw_smart)
            VALUES (?, ?, ?);
        """

        self._create_device(devid)
        epoch = self._t2epoch(t)
        json.loads(data)  # valid?
        self.db.execute(SQL, (epoch, devid, data))

    devre = r"[a-zA-Z0-9-]+[_-][a-zA-Z0-9-]+[_-][a-zA-Z0-9-]+"

    def _load_legacy_object(self, ioctx: rados.Ioctx, oid: str) -> bool:
        MAX_OMAP = 10000
        self.log.debug(f"loading object {oid}")
        if re.search(self.devre, oid) is None:
            return False
        with rados.ReadOpCtx() as op:
            it, rc = ioctx.get_omap_vals(op, None, None, MAX_OMAP)
            if rc == 0:
                ioctx.operate_read_op(op, oid)
                count = 0
                for t, raw_smart in it:
                    self.log.debug(f"putting {oid} {t}")
                    self._legacy_put_device_metrics(t, oid, raw_smart)
                    count += 1
                assert count < MAX_OMAP
        self.log.debug(f"removing object {oid}")
        ioctx.remove_object(oid)
        return True

    def check_legacy_pool(self) -> bool:
        try:
            # 'device_health_metrics' is automatically renamed '.mgr' in
            # create_mgr_pool
            ioctx = self.rados.open_ioctx(self.MGR_POOL_NAME)
        except rados.ObjectNotFound:
            return True
        if not ioctx:
            return True

        done = False
        with ioctx, self._db_lock, self.db:
            count = 0
            for obj in ioctx.list_objects():
                try:
                    if self._load_legacy_object(ioctx, obj.key):
                        count += 1
                except json.decoder.JSONDecodeError:
                    pass
                except rados.ObjectNotFound:
                    # https://tracker.ceph.com/issues/63882
                    # Sometimes an object appears in the pool listing but cannot be interacted with?
                    self.log.debug(f"object {obj} does not exist because it is deleted in HEAD")
                    pass
                if count >= 10:
                    break
            done = count < 10
        self.log.debug(f"finished reading legacy pool, complete = {done}")
        return done

    @MgrModuleRecoverDB
    def _do_serve(self) -> None:
        last_scrape = None
        finished_loading_legacy = False

        while self.run:
            # sleep first, in case of exceptions causing retry:
            sleep_interval = self.sleep_interval or 60
            if not finished_loading_legacy:
                sleep_interval = 2
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

            if self.db_ready() and self.enable_monitoring:
                self.log.debug('Running')

                if not finished_loading_legacy:
                    finished_loading_legacy = self.check_legacy_pool()

                if last_scrape is None:
                    ls = self.get_kv('last_scrape')
                    if ls:
                        try:
                            last_scrape = datetime.strptime(ls, TIME_FORMAT)
                        except ValueError:
                            pass
                    self.log.debug('Last scrape %s', last_scrape)

                self.check_health()

                now = datetime.utcnow()
                if not last_scrape:
                    next_scrape = now
                else:
                    # align to scrape interval
                    scrape_frequency = self.scrape_frequency or 86400
                    seconds = (last_scrape - datetime.utcfromtimestamp(0)).total_seconds()
                    seconds -= seconds % scrape_frequency
                    seconds += scrape_frequency
                    next_scrape = datetime.utcfromtimestamp(seconds)
                if last_scrape:
                    self.log.debug('Last scrape %s, next scrape due %s',
                                   last_scrape.strftime(TIME_FORMAT),
                                   next_scrape.strftime(TIME_FORMAT))
                else:
                    self.log.debug('Last scrape never, next scrape due %s',
                                   next_scrape.strftime(TIME_FORMAT))
                if now >= next_scrape:
                    self.scrape_all()
                    self.predict_all_devices()
                    last_scrape = now
                    self.set_kv('last_scrape', last_scrape.strftime(TIME_FORMAT))

    def serve(self) -> None:
        self.log.info("Starting")
        self.config_notify()

        self._do_serve()

    def shutdown(self) -> None:
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def scrape_daemon(self, daemon_type: str, daemon_id: str) -> Tuple[int, str, str]:
        if not self.db_ready():
            return -errno.EAGAIN, "", "mgr db not yet available"
        raw_smart_data = self.do_scrape_daemon(daemon_type, daemon_id)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                if device and data:
                    self.put_device_metrics(device, data)
        return 0, "", ""

    def scrape_all(self) -> Tuple[int, str, str]:
        if not self.db_ready():
            return -errno.EAGAIN, "", "mgr db not yet available"
        osdmap = self.get("osd_map")
        assert osdmap is not None
        did_device = {}
        ids = []
        for osd in osdmap['osds']:
            ids.append(('osd', str(osd['osd'])))
        monmap = self.get("mon_map")
        for mon in monmap['mons']:
            ids.append(('mon', mon['name']))
        for daemon_type, daemon_id in ids:
            raw_smart_data = self.do_scrape_daemon(daemon_type, daemon_id)
            if not raw_smart_data:
                continue
            for device, raw_data in raw_smart_data.items():
                if device in did_device:
                    self.log.debug('skipping duplicate %s' % device)
                    continue
                did_device[device] = 1
                data = self.extract_smart_features(raw_data)
                if device and data:
                    self.put_device_metrics(device, data)
        return 0, "", ""

    def scrape_device(self, devid: str) -> Tuple[int, str, str]:
        if not self.db_ready():
            return -errno.EAGAIN, "", "mgr db not yet available"
        r = self.get("device " + devid)
        if not r or 'device' not in r.keys():
            return -errno.ENOENT, '', 'device ' + devid + ' not found'
        daemons = r['device'].get('daemons', [])
        if not daemons:
            return (-errno.EAGAIN, '',
                    'device ' + devid + ' not claimed by any active daemons')
        (daemon_type, daemon_id) = daemons[0].split('.')
        raw_smart_data = self.do_scrape_daemon(daemon_type, daemon_id,
                                               devid=devid)
        if raw_smart_data:
            for device, raw_data in raw_smart_data.items():
                data = self.extract_smart_features(raw_data)
                if device and data:
                    self.put_device_metrics(device, data)
        return 0, "", ""

    def do_scrape_daemon(self,
                         daemon_type: str,
                         daemon_id: str,
                         devid: str = '') -> Optional[Dict[str, Any]]:
        """
        :return: a dict, or None if the scrape failed.
        """
        self.log.debug('do_scrape_daemon %s.%s' % (daemon_type, daemon_id))
        result = CommandResult('')
        self.send_command(result, daemon_type, daemon_id, json.dumps({
            'prefix': 'smart',
            'format': 'json',
            'devid': devid,
        }), '')
        r, outb, outs = result.wait()

        try:
            return json.loads(outb)
        except (IndexError, ValueError):
            self.log.error(
                "Fail to parse JSON result from daemon {0}.{1} ({2})".format(
                    daemon_type, daemon_id, outb))
            return None

    def _prune_device_metrics(self) -> None:
        SQL = """
        DELETE FROM DeviceHealthMetrics
            WHERE time < (strftime('%s', 'now') - ?);
        """

        cursor = self.db.execute(SQL, (self.retention_period,))
        if cursor.rowcount >= 1:
            self.log.info(f"pruned {cursor.rowcount} metrics")

    def _create_device(self, devid: str) -> None:
        SQL = """
        INSERT OR IGNORE INTO Device VALUES (?);
        """

        cursor = self.db.execute(SQL, (devid,))
        if cursor.rowcount >= 1:
            self.log.info(f"created device {devid}")
        else:
            self.log.debug(f"device {devid} already exists")

    def put_device_metrics(self, devid: str, data: Any) -> None:
        SQL = """
        INSERT OR REPLACE INTO DeviceHealthMetrics (devid, raw_smart, time)
            VALUES (?, ?, strftime('%s', 'now'));
        """

        with self._db_lock, self.db:
            self._create_device(devid)
            self.db.execute(SQL, (devid, json.dumps(data)))
            self._prune_device_metrics()

        # extract wear level?
        wear_level = get_ata_wear_level(data)
        if wear_level is None:
            wear_level = get_nvme_wear_level(data)
        dev_data = self.get(f"device {devid}") or {}
        if wear_level is not None:
            if dev_data.get(wear_level) != str(wear_level):
                dev_data["wear_level"] = str(wear_level)
                self.log.debug(f"updating {devid} wear level to {wear_level}")
                self.set_device_wear_level(devid, wear_level)
        else:
            if "wear_level" in dev_data:
                del dev_data["wear_level"]
                self.log.debug(f"removing {devid} wear level")
                self.set_device_wear_level(devid, -1.0)

    def _t2epoch(self, t: Optional[str]) -> int:
        if not t:
            return 0
        else:
            return int(datetime.strptime(t, TIME_FORMAT).strftime("%s"))

    def _get_device_metrics(self, devid: str,
                            sample: Optional[str] = None,
                            min_sample: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        res = {}

        SQL_EXACT = """
        SELECT time, raw_smart
            FROM DeviceHealthMetrics
            WHERE devid = ? AND time = ?
            ORDER BY time DESC;
        """
        SQL_MIN = """
        SELECT time, raw_smart
            FROM DeviceHealthMetrics
            WHERE devid = ? AND ? <= time
            ORDER BY time DESC;
        """

        isample = None
        imin_sample = None
        if sample:
            isample = self._t2epoch(sample)
        else:
            imin_sample = self._t2epoch(min_sample)

        self.log.debug(f"_get_device_metrics: {devid} {sample} {min_sample}")

        with self._db_lock, self.db:
            if isample:
                cursor = self.db.execute(SQL_EXACT, (devid, isample))
            else:
                cursor = self.db.execute(SQL_MIN, (devid, imin_sample))
            for row in cursor:
                t = row['time']
                dt = datetime.utcfromtimestamp(t).strftime(TIME_FORMAT)
                try:
                    res[dt] = json.loads(row['raw_smart'])
                except (ValueError, IndexError):
                    self.log.debug(f"unable to parse value for {devid}:{t}")
                    pass
        return res

    def show_device_metrics(self, devid: str, sample: Optional[str]) -> Tuple[int, str, str]:
        # verify device exists
        r = self.get("device " + devid)
        if not r or 'device' not in r.keys():
            return -errno.ENOENT, '', 'device ' + devid + ' not found'
        # fetch metrics
        res = self._get_device_metrics(devid, sample=sample)
        return 0, json.dumps(res, indent=4, sort_keys=True), ''

    def check_health(self) -> Tuple[int, str, str]:
        self.log.info('Check health')
        config = self.get('config')
        min_in_ratio = float(config.get('mon_osd_min_in_ratio'))
        mark_out_threshold_td = timedelta(seconds=self.mark_out_threshold)
        warn_threshold_td = timedelta(seconds=self.warn_threshold)
        checks: Dict[str, Dict[str, Union[int, str, Sequence[str]]]] = {}
        health_warnings: Dict[str, List[str]] = {
            DEVICE_HEALTH: [],
            DEVICE_HEALTH_IN_USE: [],
        }
        devs = self.get("devices")
        osds_in = {}
        osds_out = {}
        now = datetime.now(timezone.utc)  # e.g. '2021-09-22 13:18:45.021712+00:00'
        osdmap = self.get("osd_map")
        assert osdmap is not None
        for dev in devs['devices']:
            if 'life_expectancy_max' not in dev:
                continue
            # ignore devices that are not consumed by any daemons
            if not dev['daemons']:
                continue
            if not dev['life_expectancy_max'] or \
               dev['life_expectancy_max'] == '0.000000':
                continue
            # life_expectancy_(min/max) is in the format of:
            # '%Y-%m-%dT%H:%M:%S.%f%z', e.g.:
            # '2019-01-20 21:12:12.000000+00:00'
            life_expectancy_max = datetime.strptime(
                dev['life_expectancy_max'],
                '%Y-%m-%dT%H:%M:%S.%f%z')
            self.log.debug('device %s expectancy max %s', dev,
                           life_expectancy_max)

            if life_expectancy_max - now <= mark_out_threshold_td:
                if self.self_heal:
                    # dev['daemons'] == ["osd.0","osd.1","osd.2"]
                    if dev['daemons']:
                        osds = [x for x in dev['daemons']
                                if x.startswith('osd.')]
                        osd_ids = map(lambda x: x[4:], osds)
                        for _id in osd_ids:
                            if self.is_osd_in(osdmap, _id):
                                osds_in[_id] = life_expectancy_max
                            else:
                                osds_out[_id] = 1

            if life_expectancy_max - now <= warn_threshold_td:
                # device can appear in more than one location in case
                # of SCSI multipath
                device_locations = map(lambda x: x['host'] + ':' + x['dev'],
                                       dev['location'])
                health_warnings[DEVICE_HEALTH].append(
                    '%s (%s); daemons %s; life expectancy between %s and %s'
                    % (dev['devid'],
                       ','.join(device_locations),
                       ','.join(dev.get('daemons', ['none'])),
                       dev['life_expectancy_max'],
                       dev.get('life_expectancy_max', 'unknown')))

        # OSD might be marked 'out' (which means it has no
        # data), however PGs are still attached to it.
        for _id in osds_out:
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
                            '%d OSDs with failing device(s) would bring "in" ratio to %f < mon_osd_min_in_ratio %f' % (
                                num_bad - did, final_ratio, min_in_ratio)
                        ]
                    }
                    break
                to_mark_out.append(osd_id)
                did += 1
            if to_mark_out:
                self.mark_out_etc(to_mark_out)
        for warning, ls in health_warnings.items():
            n = len(ls)
            if n:
                checks[warning] = {
                    'severity': 'warning',
                    'summary': HEALTH_MESSAGES[warning] % n,
                    'count': len(ls),
                    'detail': ls,
                }
        self.set_health_checks(checks)
        return 0, "", ""

    def is_osd_in(self, osdmap: Dict[str, Any], osd_id: str) -> bool:
        for osd in osdmap['osds']:
            if osd_id == str(osd['osd']):
                return bool(osd['in'])
        return False

    def get_osd_num_pgs(self, osd_id: str) -> int:
        stats = self.get('osd_stats')
        assert stats is not None
        for stat in stats['osd_stats']:
            if osd_id == str(stat['osd']):
                return stat['num_pgs']
        return -1

    def mark_out_etc(self, osd_ids: List[str]) -> None:
        self.log.info('Marking out OSDs: %s' % osd_ids)
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd out',
            'format': 'json',
            'ids': osd_ids,
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.warning('Could not mark OSD %s out. r: [%s], outb: [%s], outs: [%s]',
                             osd_ids, r, outb, outs)
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
                self.log.warning('Could not set osd.%s primary-affinity, '
                                 'r: [%s], outb: [%s], outs: [%s]',
                                 osd_id, r, outb, outs)

    def extract_smart_features(self, raw: Any) -> Any:
        # FIXME: extract and normalize raw smartctl --json output and
        # generate a dict of the fields we care about.
        return raw

    def predict_lift_expectancy(self, devid: str) -> Tuple[int, str, str]:
        plugin_name = ''
        model = self.get_ceph_option('device_failure_prediction_mode')
        if cast(str, model).lower() == 'local':
            plugin_name = 'diskprediction_local'
        else:
            return -1, '', 'unable to enable any disk prediction model[local/cloud]'
        try:
            can_run, _ = self.remote(plugin_name, 'can_run')
            if can_run:
                return self.remote(plugin_name, 'predict_life_expectancy', devid=devid)
            else:
                return -1, '', f'{plugin_name} is not available'
        except Exception:
            return -1, '', 'unable to invoke diskprediction local or remote plugin'

    def predict_all_devices(self) -> Tuple[int, str, str]:
        plugin_name = ''
        model = self.get_ceph_option('device_failure_prediction_mode')
        if cast(str, model).lower() == 'local':
            plugin_name = 'diskprediction_local'
        else:
            return -1, '', 'unable to enable any disk prediction model[local/cloud]'
        try:
            can_run, _ = self.remote(plugin_name, 'can_run')
            if can_run:
                return self.remote(plugin_name, 'predict_all_devices')
            else:
                return -1, '', f'{plugin_name} is not available'
        except Exception:
            return -1, '', 'unable to invoke diskprediction local or remote plugin'

    def get_recent_device_metrics(self, devid: str, min_sample: str) -> Dict[str, Dict[str, Any]]:
        try:
            return self._get_device_metrics(devid, min_sample=min_sample)
        except MgrDBNotReady:
            return dict()

    def get_time_format(self) -> str:
        return TIME_FORMAT
