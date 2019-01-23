import json
from ceph_volume.util.prepare import osd_id_available

class Strategy(object):

    def __init__(self, args, data_devs, db_or_journal_devs=[], wal_devs=[]):
        '''
        Note that this ctor is used by both bluestore and filestore strategies
        to reduce code duplication. A filestore strategy will always pass an
        empty list for wal_devs.
        '''
        self.args = args
        self.osd_ids = args.osd_ids
        self.osds_per_device = args.osds_per_device
        self.devices = data_devs + wal_devs + db_or_journal_devs
        self.data_devs = data_devs
        self.db_or_journal_devs = db_or_journal_devs
        self.wal_devs = wal_devs
        self.computed = {'osds': [], 'vgs': []}

    @staticmethod
    def split_devices_rotational(devices):
        data_devs = [device for device in devices if device.sys_api['rotational'] == '1']
        db_or_journal_devs = [device for device in devices if device.sys_api['rotational'] == '0']
        return data_devs, db_or_journal_devs


    def validate_compute(self):
        if self.devices:
            self.validate()
            self.compute()
        else:
            self.computed["changed"] = False

    def report_json(self, filtered_devices):
        # add filtered devices to report
        report = self.computed.copy()
        report['filtered_devices'] = filtered_devices
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def _validate_osd_ids(self):
        unavailable_ids = [id_ for id_ in self.osd_ids if
                           not osd_id_available(id_)]
        if unavailable_ids:
            msg = ("Not all specfied OSD ids are available: {}"
                   "unavailable").format(",".join(unavailable_ids))
            raise RuntimeError(msg)

    @property
    def total_osds(self):
        return len(self.data_devs) * self.osds_per_device

    # protect against base class instantiation and incomplete implementations.
    # We could also use the abc module and implement this as an
    # AbstractBaseClass
    def compute(self):
        raise NotImplementedError('compute() must be implemented in a child class')

    def execute(self):
        raise NotImplementedError('execute() must be implemented in a child class')

class MixedStrategy(Strategy):

    def get_common_vg(self, devs):
        # find all the vgs associated with the current device
        for dev in devs:
            for pv in dev.pvs_api:
                vg = self.system_vgs.get(vg_name=pv.vg_name)
                if not vg:
                    continue
                # this should give us just one VG, it would've been caught by
                # the validator otherwise
                return vg
