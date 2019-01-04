import json

class Strategy(object):

    def __init__(self, data_devs, db_or_journal_devs, wal_devs, args):
        '''
        Note that this ctor is used by both bluestore and filestore strategies
        to reduce code duplication. A filestore strategy will always pass an
        empty list for wal_devs.
        '''
        self.args = args
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

    def get_common_vg(self):
        # find all the vgs associated with the current device
        for ssd in self.db_or_journal_devs:
            for pv in ssd.pvs_api:
                vg = self.system_vgs.get(vg_name=pv.vg_name)
                if not vg:
                    continue
                # this should give us just one VG, it would've been caught by
                # the validator otherwise
                return vg
