
"""
Pulling SMART data from OSD
"""

import json
from mgr_module import MgrModule, CommandResult
import rados
from threading import Event
from datetime import datetime, timedelta, date, time

TIME_FORMAT = '%Y%m%d-%H%M%S'
SCRAPE_FREQUENCY = 86400  # seconds

# TODO maybe have a 'refresh' command to run after config-key set
# to wake the sleep interval
# TODO document
# TODO check all r return value

# TODO maybe change the design a bit:
# currently we scrape some devices multiple times, since:
# each OSD has about 4 devices;
# some devices (SSD) are shared by several OSDs;
# so when running the main loop, in a single iteration several omap key:val
# (date:smart_dump) entries will be created for the same object (host:device).
# we should hold an host_and_device to OSD map, and scrape this device only
# once (maybe add a 'device' parameter to the 'tell' command). if something is
# wrong with this device - action will be taken on all OSDs sharing it.


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "smart status",
            "desc": "Show smart status",
            "perm": "r",
        },
        {
            "cmd": "smart on",
            "desc": "Enable automatic SMART scraping",
            "perm": "rw",
        },
        {
            "cmd": "smart off",
            "desc": "Disable automatic SMART scraping",
            "perm": "rw",
        },
        {
            "cmd": "smart osd scrape "
                   "name=osd_id,type=CephString,req=true",
            "desc": "Scraping SMART data of osd<osd_id>",
            "perm": "r"
        },
        {
            "cmd": "smart osd scrape-all",
            "desc": "Scraping SMART data of all OSDs",
            "perm": "r"
        },
        {
            "cmd": "smart osd dump "
                   "name=obj_name,type=CephString,req=true",
            "desc": "Get SMART data of osd_id, which was scraped and stored in a rados object",
            "perm": "r"
        },
        {
            "cmd": "smart osd dump-all",
            "desc": "Get SMART data of all OSDs, which was scraped and stored in a rados object",
            "perm": "r"
        },
        {
            "cmd": "smart predict-failure",
            "desc": "Run a failure prediction model based on the latest SMART stats",
            "perm": "rw",
        },
        {
            "cmd": "smart warn",
            "desc": "Test warning.",
            "perm": "rw",
        },
        {
            "cmd": "osd devices "
                   "name=osd_id,type=CephString,req=true",
            "desc": "Get list of devices used by osd<osd_id>",
            "perm": "r"
        },
        {
            "cmd": "device osds "
                   "name=host_device,type=CephString,req=true",
            "desc": "Get list of OSDs that use this <host:device>.",
            "perm": "rw",
        },
        {
            "cmd": "test-out "
                   "name=osd_id,type=CephString,req=true",
            "desc": "TEST out.",
            "perm": "rw",
        },
    ]
    active = False
    run = True
    event = Event()
    last_scrape_time = ""

    def open_connection(self):
        # TODO check that pool name is actually configurable
        pool_name = self.get_config('pool_name') or 'smart_data'
        # looking for the right pool
        pools = self.rados.list_pools()
        is_pool = False
        for pool in pools:
            self.log.debug('pool name is %s' % pool)
            if pool == pool_name:
                is_pool = True
                break
        if not is_pool:
            self.rados.create_pool(pool_name)
            self.log.debug('create %s pool' % pool_name)
        ioctx = self.rados.open_ioctx(pool_name)
        return (ioctx)

    def handle_osd_smart_scrape(self, osd_id):
        ioctx = self.open_connection()
        self.do_osd_smart_scrape(osd_id, ioctx)
        ioctx.close()
        return (0, "", "")

    # TODO maybe have one function that returns state tuple (in, up, ...)
    def is_osd_in(self, osd_id):
        osdmap = self.get("osd_map")
        assert osdmap is not None
        for osd in osdmap['osds']:
            if str(osd_id) == str(osd['osd']) and osd['in']:
                return True
        return False

    def is_osd_up(self, osd_id):
        osdmap = self.get("osd_map")
        assert osdmap is not None
        for osd in osdmap['osds']:
            if str(osd_id) == str(osd['osd']) and osd['up']:
                return True
        return False

    # TODO remove
    def test_osd_out(self, osd_id):
        result = CommandResult('')
        # 'ids': ["%s" % osd_id],
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd out',
            'format': 'json',
            'ids': [osd_id],
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error('Error calling tell cmd')
            return

        self.log.debug("tell r ***: %s" % r)
        self.log.debug("tell outb***: %s" % outb)
        self.log.debug("tell outs***: %s" % outs)
        """
        try:
            data = json.loads(outb)
        except:
            self.log.debug('Probably invalid or empty JSON')
        """
        return (0, "test osd out", outs)

    def do_osd_smart_scrape(self, osd_id, ioctx):
        # TODO is there a better way to check?
        if not self.is_osd_in(osd_id):
            self.log.debug("osd %s is out, cannot scrape smart data" % osd_id)
            return
        if not self.is_osd_up(osd_id):
            self.log.debug("osd %s is down, cannot scrape smart data" % osd_id)
            return
        # running 'tell' command on OSD, retrieving smartctl results
        result = CommandResult('')
        self.send_command(result, 'osd', osd_id, json.dumps({
            'prefix': 'smart',
            'format': 'json',
        }), '')
        r, outb, outs = result.wait()
        # TODO check r val!
        self.log.debug("tell outs: %s" % outs)
        try:
            smart_data = json.loads(outb)
        except:
            # TODO get the full error message
            self.log.debug('Probably invalid or empty JSON')

        # TODO can we have more than one hostname per OSD?
        osd_host = self.get_metadata('osd', osd_id)['hostname']
        if not osd_host:
            self.log.debug('Cannot get osd hostname of osd_id: %s' % osd_id)
            return
        self.log.debug('osd %s hostname is %s' % (osd_id, osd_host))

        # The output of 'tell' command:
        # smart_data is a dictionary:
        # its keys are the OSD's devices (sda,sdb,...)
        # its values are smartctl output of that device
        # Now we write it to rados:
        # object names are "osd_host:device" (e.g. 'my_host:sdd')
        # and each scrape is stored as omap key-val:
        # keys are the date and time of scrape
        # values are smartctl output (of that time for that OSD device)
        for device, device_smart_data in smart_data.items():
            object_name = osd_host + ":" + device
            with rados.WriteOpCtx() as op:
                # TODO should we change second arg to be byte?
                # TODO what about the flag in ioctx.operate_write_op()?
                now = datetime.now().strftime(TIME_FORMAT)
                ioctx.set_omap(op, (now,), (str(json.dumps(device_smart_data)),))
                ioctx.operate_write_op(op, object_name)
                self.log.debug('writing object %s, key: %s, value: %s' % (object_name, now, str(json.dumps(device_smart_data))))

    def handle_osd_smart_scrape_all(self):
        osdmap = self.get("osd_map")
        assert osdmap is not None
        ioctx = self.open_connection()
        for osd in osdmap['osds']:
            osd_id = osd['osd']
            self.log.debug('scraping osd %s for smart data' % str(osd_id))
            self.do_osd_smart_scrape(str(osd_id), ioctx)
        ioctx.close()
        return (0, "", "")

    # Returns the latest smart data of host_and_device saved in RADOS.
    # Will search only entries after 'start_after'. Will fail if there is none.
    def get_latest_osd_smart_dump(self, ioctx, host_and_device):
        smart_data = self.do_osd_smart_dump(ioctx, host_and_device)
        # TODO - check what happens when smart_data is empty
        # In case there are several keys after last_scrape_time, take latest:
        return smart_data[max(smart_data.iterkeys())]

    # Currently, if id does not exist, it's not considered a failure.
    def table_id_exists_and_greater(self, table, id, value):
        if self.smart_table_get_by_id(table, id) and self.smart_table_get_by_id(table, id) > 0:
            return True
        return False

    def predict_failure_single_device(self, ioctx, model, host_and_device):
        if model != "trivial":
            # TODO raise exception "not supported yet";
            # remove the return below
            return
        smart_data = self.get_latest_osd_smart_dump(ioctx, host_and_device)
        # TODO take care of smart_data == None which can happen also in case of
        # an attemp to read data from a none existing device - since its
        # host:device object name still exists in smart pool.
        # For now, it can also happen in case of none SATA device.

        # TODO - Make sure that ata_smart_attributes and table exist
        ata_smart_attr = smart_data['ata_smart_attributes']
        ata_table = ata_smart_attr['table']
        failure = False

        # 5 == Reallocated Sectors Count
        if self.table_id_exists_and_greater(ata_table, 5, 0):
            failure = True
            self.log.debug('device %s has a high Reallocated Sectors Count value' % host_and_device)
        # 187 == Reported Uncorrectable Errors
        if self.table_id_exists_and_greater(ata_table, 187, 0):
            failure = True
            self.log.debug('device %s has a high Reported Uncorrectable Errors value' % host_and_device)
        # 188 == Command Timeout
        if self.table_id_exists_and_greater(ata_table, 188, 0):
            failure = True
            self.log.debug('device %s has a high Command Timeout value' % host_and_device)
        # 197 == Current Pending Sector Count
        if self.table_id_exists_and_greater(ata_table, 197, 0):
            failure = True
            self.log.debug('device %s has a high Current Pending Sector Count value' % host_and_device)
        # 198 == Uncorrectable Sector Count
        if self.table_id_exists_and_greater(ata_table, 198, 0):
            failure = True
            self.log.debug('device %s has a high Uncorrectable Sector Count value' % host_and_device)

        return failure

    # returns a dictionary of ret["my_host:sda"] = [17,42]
    # where 17 and 42 are OSDs which share my_host:sda
    def get_device_to_osd_map(self):
        ret = {}
        osds = self.get("osd_metadata")
        for osd, data in osds.items():
            for device in data['devices']:
                name = data['hostname'] + ':' + device
                if name not in ret:
                    ret[name] = []
                ret[name] += osd
        return ret

    # osds_out == osds which are out / warned
    # this is becasue some OSDs share the same device and we don't want
    # to mark them out in case they're already out.
    def act_on_failure(self, action, host_and_device, dev_to_osd_map, osds_out):
        if action == "warn":
            self.log.info('Setting health check warning')
            self.set_health_checks({
                'MGR_SMART_FAILURE_WARNING': {
                    'severity': 'warning',
                    'summary': 'mgr smart predicts disk failure',
                    'detail': ['Immenent failure predicted on %s. Affected OSDs: %s'
                               % (host_and_device, ', '.join(dev_to_osd_map[host_and_device]))]
                }
            })
        elif action == "mark_out":
            self.log.info('Marking out')
            for osd in dev_to_osd_map[host_and_device]:
                if not osds_out[osd]:
                    self.do_osd_out(osd)
                    osds_out[osd] = True
                    # TODO what happens if an OSD was recreated and used the
                    # same ID of an already marked out OSD, in the same
                    # predict_failure_and_act iteration? This current code will
                    # not mark it as 'out'. Is it likely to happen?

    def do_osd_out(self, osd_id):
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd out',
            'format': 'json',
            'ids': [osd_id],
        }), '')
        r, outb, outs = result.wait()
        # TODO what else should we do in case marking out failed?
        if r != 0:
            self.log.error('Error: Could not mark OSD %s out. r: [%s], outb: [%s], outs: [%s]' % (osd_id, r, outb, outs))
            return

        # TODO change primary affinity to zero

    def predict_failure_and_act(self, model, action):
        # dev_to_osd_map is recreated here,
        # so it's updated with each iteration of the main loop
        dev_to_osd_map = self.get_device_to_osd_map()
        osds_out = {}
        ioctx = self.open_connection()
        object_iterator = ioctx.list_objects()
        while True:
            try:
                rados_object = object_iterator.next()
                self.log.debug('object name = %s' % rados_object.key)
                # host_and_device is the rados object name
                # TODO some object names will turn obsolete when devices are
                # removed - need to decide whether to delete them.
                host_and_device = rados_object.key
                if host_and_device not in dev_to_osd_map:
                    self.log.debug("device %s no longer exists, no need to predict." % host_and_device)
                    # TODO in case a device was removed and replaced within a
                    # week with a new one - need to pay attention in case the
                    # prediction model uses history - the object name would be
                    # the same, and probably would have the old omap keys with
                    # the 'about-to-fail' data.
                    continue
                failure = self.predict_failure_single_device(ioctx, model, host_and_device)
                # if a disk is about to fail:
                # act according to model on all OSDs sharing it
                if failure:
                    self.log.info('Device %s is about to fail. Affected OSDs: %s'
                                  % (host_and_device, ', '.join(dev_to_osd_map[host_and_device])))
                    self.act_on_failure(action, host_and_device, dev_to_osd_map, osds_out)
            except StopIteration:
                self.log.debug('Prediction iteration stopped')
                break
        ioctx.close()
        return

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    # TODO maybe change 'begin' to be datetime object instead of str
    def seconds_to_next_run(self, begin, interval):
        now = datetime.now()
        # creating datetime object for today's first run
        today = date.today()
        begin_time = time(int(begin[0:2]), int(begin[2:4]))
        begin_time_today = datetime.combine(today, begin_time)
        # In case we missed today's first run - rewinding the clock a day,
        # and calculating the next run by adding the seconds interval.
        # This assumes a minimum of one daily run.
        if begin_time_today > now:
            begin_time_today -= timedelta(days=1)
        while begin_time_today < now:
            begin_time_today += timedelta(seconds=interval)
        delta = begin_time_today - now
        return int(delta.total_seconds())

    def serve(self):
        self.log.info("Starting")
        while self.run:
            # TODO see if there might be a bug if active == false
            self.active = self.get_config('active', '') is not ''
            # TODO convert begin string to a datetime object
            begin_time = self.get_config('begin_time') or '0000'
            scrape_frequency = self.get_config('scrape_frequency') or SCRAPE_FREQUENCY
            failure_prediction_model = self.get_config('failure_prediction_model') or 'trivial'
            failure_prediction_action = self.get_config('failure_prediction_action') or 'warn'
            timeofday = time.strftime('%H%M%S', time.localtime())

            sleep_interval = self.seconds_to_next_run(begin_time, scrape_frequency)
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

            # in case 'wait' was interrupted, it could mean config was changed
            # by the user; go back and read config vars
            if ret:
                continue
            # in case we waited the entire interval
            else:
                # TODO specify the exact time of next run
                self.log.debug('Waking up [%s, scheduled for %s, now %s]',
                               "active" if self.active else "inactive",
                               begin_time, timeofday)
                if not self.active:
                    continue
                self.log.debug('Running')
                self.handle_osd_smart_scrape_all()
                self.predict_failure_and_act(failure_prediction_model, failure_prediction_action)
                self.last_scrape_time = datetime.now().strftime(TIME_FORMAT)

    # Find an ID in the SMART attribute table and return that item (returns a
    # whole dictionary)
    def smart_table_get_by_id(self, table, t_id):
        for item in table:
            self.log.debug('desired table id is %s and curr val is: %s' % (t_id, item['id']))
            self.log.debug('item type is: %s' % type(item))
            self.log.debug('table type is: %s' % type(table))
            if item['id'] == t_id:
                return item
        return None

    # TODO maybe add a 'date' param, for 'start key' in get_omap_vals
    def do_osd_smart_dump(self, ioctx, obj_name):
        smart_dump = {}
        with rados.ReadOpCtx() as op:
            iter, ret = ioctx.get_omap_vals(op, self.last_scrape_time, "", -1)
            assert(ret == 0)
            ioctx.operate_read_op(op, obj_name)
            # k == date; v == json_blob
            for (k, v) in list(iter):
                self.log.debug('Dumping SMART data of object: %s, key: %s, val %s' % (obj_name, k, v))
                try:
                    smart_dump[k] = json.loads(str(v))
                except:
                    # TODO get the full error message
                    self.log.debug('Probably invalid or empty JSON')
        return smart_dump

    def handle_osd_smart_dump(self, cmd):
        ioctx = self.open_connection()
        smart_dump = self.do_osd_smart_dump(ioctx, cmd['obj_name'])
        ioctx.close()
        return (0, "", str(json.dumps(smart_dump)))

    def handle_osd_smart_dump_all(self):
        ioctx = self.open_connection()
        object_iterator = ioctx.list_objects()
        # TODO see whether to change return val
        smart_dump = {}
        while True:
            try:
                rados_object = object_iterator.next()
                obj_name = rados_object.key
                self.log.debug('object name = %s' % rados_object.key)
                smart_dump[obj_name] = self.do_osd_smart_dump(ioctx, obj_name)
            except StopIteration:
                self.log.debug('iteration stopped')
                break
        ioctx.close()
        # TODO maybe have a wrapper for the cmd call
        # and return 0 or 1 here (and json obj)
        return (0, "", str(json.dumps(smart_dump)))

    # TODO implement
    # what is the best way to ask for n samples without reading all omap keys?
    # def get_recent_smart_data(device, num_samples):

    def handle_command(self, cmd):
        self.log.info("Handling command: '%s'" % str(cmd))

        if cmd['prefix'] == 'smart status':
            s = {
                'active': self.active,
            }
            return (0, json.dumps(s, indent=4), '')

        elif cmd['prefix'] == 'smart on':
            if not self.active:
                self.set_config('active', '1')
                self.active = True
            self.event.set()
            return (0, '', '')

        elif cmd['prefix'] == 'smart off':
            if self.active:
                self.set_config('active', '')
                self.active = False
            self.event.set()
            return (0, '', '')

        elif cmd['prefix'] == "smart osd scrape":
            # TODO check if cmd['osd_id'] is valid
            return self.handle_osd_smart_scrape(cmd['osd_id'])

        elif cmd['prefix'] == "smart osd scrape-all":
            return self.handle_osd_smart_scrape_all()

        elif cmd['prefix'] == "smart osd dump":
            # TODO check that cmd['obj_name'] is specified and valid
            return self.handle_osd_smart_dump(cmd)

        elif cmd['prefix'] == "smart osd dump-all":
            # TODO there might be an issue when running this cmd from cli since
            # self.last_scrape_time is updated via serve() and
            # do_osd_smart_dump() might not return results
            return self.handle_osd_smart_dump_all()

        elif cmd['prefix'] == "osd devices":
            if "osd_id" not in cmd:
                return (0, "", "Please specify <osd-id>")
            osd = self.get_metadata('osd', cmd['osd_id'])
            if not osd:
                return ("0", "", "osd %s is not found!" % cmd['osd_id'])
            return (0, "", osd['devices'])

        elif cmd['prefix'] == "device osds":
            if "host_device" not in cmd:
                return (0, "", "Please specify <hostname:device>")
            osds = self.get("osd_metadata")
            host, device = cmd['host_device'].split(":")
            device_osds = []
            for osd, data in osds.items():
                if data['hostname'] == host:
                    # TODO it seems like val here is a string and not a list
                    # TODO check case of 'host:' (empty set, all osds have it)
                    if device in data['devices']:
                        device_osds += osd
            return (0, "", str(device_osds))

        elif cmd['prefix'] == "smart warn":
            # TODO check why it's not in 'ceph health' output
            self.set_health_checks({
                'MGR_TEST_WARNING': {
                    'severity': 'warning',
                    'summary': 'test warning',
                    'detail': ['details about the warning']
                }
            })
            return (0, "warn", "test")

        elif cmd['prefix'] == "test-out":
            return self.test_osd_out(cmd['osd_id'])

        else:
            # mgr should respect our self.COMMANDS and not call us for
            # any prefix we don't advertise
            raise NotImplementedError(cmd['prefix'])
