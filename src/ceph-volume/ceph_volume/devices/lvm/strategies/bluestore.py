from __future__ import print_function
from ceph_volume.util import disk, prepare, str_to_int
from ceph_volume.api import lvm
from . import validators
from .strategies import Strategy
from .strategies import MixedStrategy
from ceph_volume.devices.lvm.create import Create
from ceph_volume.devices.lvm.prepare import Prepare
from ceph_volume.util import templates
from ceph_volume.exceptions import SizeAllocationError


class SingleType(Strategy):
    """
    Support for all SSDs, or all HDDS
    """

    def __init__(self, args, data_devs):
        super(SingleType, self).__init__(args, data_devs)
        self.validate_compute()

    @classmethod
    def with_auto_devices(cls, args, devices):
        #SingleType only deploys standalone OSDs
        return cls(args, devices)

    @staticmethod
    def type():
        return "bluestore.SingleType"

    def report_pretty(self, filtered_devices):
        string = ""
        if filtered_devices:
            string += templates.filtered_devices(filtered_devices)
        string += templates.total_osds.format(
            total_osds=self.total_osds,
        )
        string += templates.osd_component_titles

        for osd in self.computed['osds']:
            string += templates.osd_header
            if 'osd_id' in osd:
                string += templates.osd_reused_id.format(
                    id_=osd['osd_id'])
            string += templates.osd_component.format(
                _type='[data]',
                path=osd['data']['path'],
                size=osd['data']['human_readable_size'],
                percent=osd['data']['percentage'],
            )

        print(string)

    def validate(self):
        """
        Ensure that the minimum requirements for this type of scenario is
        met, raise an error if the provided devices would not work
        """
        # validate minimum size for all devices
        validators.minimum_device_size(
            self.data_devs, osds_per_device=self.osds_per_device
        )

        # make sure that data devices do not have any LVs
        validators.no_lvm_membership(self.data_devs)

        if self.osd_ids:
            self._validate_osd_ids()

    def compute(self):
        """
        Go through the rules needed to properly size the lvs, return
        a dictionary with the result
        """
        osds = self.computed['osds']
        for device in self.data_devs:
            extents = lvm.sizing(device.lvm_size.b, parts=self.osds_per_device)
            for _i in range(self.osds_per_device):
                osd = {'data': {}, 'block.db': {}}
                osd['data']['path'] = device.abspath
                osd['data']['size'] = extents['sizes']
                osd['data']['parts'] = extents['parts']
                osd['data']['percentage'] = 100 / self.osds_per_device
                osd['data']['human_readable_size'] = str(disk.Size(b=extents['sizes']))
                osds.append(osd)

        self.computed['changed'] = len(osds) > 0

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (block, block.db, block.wal, etc..) and offload the OSD creation to
        ``lvm create``
        """
        osd_vgs = dict([(osd['data']['path'], None) for osd in self.computed['osds']])

        # create the vgs first, mapping them to the device path
        for osd in self.computed['osds']:
            vg = osd_vgs.get(osd['data']['path'])
            if not vg:
                vg = lvm.create_vg(osd['data']['path'])
                osd_vgs[osd['data']['path']] = {'vg': vg, 'parts': osd['data']['parts']}

        # create the lvs from the vgs captured in the beginning
        for create in osd_vgs.values():
            lvs = lvm.create_lvs(create['vg'], parts=create['parts'], name_prefix='osd-data')
            vg_name = create['vg'].name
            for lv in lvs:
                command = ['--bluestore', '--data']
                command.append('%s/%s' % (vg_name, lv.name))
                if self.args.dmcrypt:
                    command.append('--dmcrypt')
                if self.args.no_systemd:
                    command.append('--no-systemd')
                if self.args.crush_device_class:
                    command.extend(['--crush-device-class', self.args.crush_device_class])

                if self.osd_ids:
                    command.extend(['--osd-id', self.osd_ids.pop(0)])

                if self.args.prepare:
                    Prepare(command).main()
                else:
                    Create(command).main()


class MixedType(MixedStrategy):

    def __init__(self, args, data_devs, db_devs, wal_devs=[]):
        super(MixedType, self).__init__(args, data_devs, db_devs, wal_devs)
        self.block_db_size = self.get_block_db_size()
        self.block_wal_size = self.get_block_wal_size()
        self.system_vgs = lvm.VolumeGroups()
        self.common_vg = None
        self.common_wal_vg = None
        self.dbs_needed = len(self.data_devs) * self.osds_per_device
        self.wals_needed = self.dbs_needed
        self.use_large_block_db = self.use_large_block_wal = False
        self.validate_compute()

    @classmethod
    def with_auto_devices(cls, args, devices):
        data_devs, db_devs = cls.split_devices_rotational(devices)
        return cls(args, data_devs, db_devs)

    @staticmethod
    def type():
        return "bluestore.MixedType"

    def get_block_db_size(self):
        if self.args.block_db_size:
            return disk.Size(b=self.args.block_db_size)
        else:
            return prepare.get_block_db_size(lv_format=False) or disk.Size(b=0)

    def get_block_wal_size(self):
        if self.args.block_wal_size:
            return disk.Size(b=self.args.block_wal_size)
        else:
            return prepare.get_block_wal_size(lv_format=False) or disk.Size(b=0)

    def report_pretty(self, filtered_devices):
        string = ""
        if filtered_devices:
            string += templates.filtered_devices(filtered_devices)
        string += templates.total_osds.format(
            total_osds=len(self.data_devs) * self.osds_per_device
        )

        if self.db_or_journal_devs:
            vg_extents = lvm.sizing(self.total_available_db_space.b, parts=self.dbs_needed)
            db_size = str(disk.Size(b=(vg_extents['sizes'])))

            string += templates.ssd_volume_group.format(
                target='block.db',
                total_lv_size=str(self.total_available_db_space),
                total_lvs=vg_extents['parts'] * self.osds_per_device,
                block_lv_size=db_size,
                block_db_devices=', '.join([ssd.abspath for ssd in
                                            self.db_or_journal_devs]),
                lv_size=self.block_db_size or str(disk.Size(b=(vg_extents['sizes']))),
                total_osds=len(self.data_devs)
            )

        if self.wal_devs:
            wal_vg_extents = lvm.sizing(self.total_available_wal_space.b,
                                        parts=self.wals_needed)
            wal_size = str(disk.Size(b=(wal_vg_extents['sizes'])))
            string += templates.ssd_volume_group.format(
                target='block.wal',
                total_lv_size=str(self.total_available_wal_space),
                total_lvs=wal_vg_extents['parts'] * self.osds_per_device,
                block_lv_size=wal_size,
                block_db_devices=', '.join([dev.abspath for dev in
                                            self.wal_devs]),
                lv_size=self.block_wal_size or str(disk.Size(b=(wal_vg_extents['sizes']))),
                total_osds=len(self.data_devs)
            )

        string += templates.osd_component_titles
        for osd in self.computed['osds']:
            string += templates.osd_header
            if 'osd_id' in osd:
                string += templates.osd_reused_id.format(
                    id_=osd['osd_id'])
            string += templates.osd_component.format(
                _type='[data]',
                path=osd['data']['path'],
                size=osd['data']['human_readable_size'],
                percent=osd['data']['percentage'])

            if 'block.db' in osd:
                string += templates.osd_component.format(
                    _type='[block.db]',
                    path=osd['block.db']['path'],
                    size=osd['block.db']['human_readable_size'],
                    percent=osd['block.db']['percentage'])

            if 'block.wal' in osd:
                string += templates.osd_component.format(
                    _type='[block.wal]',
                    path=osd['block.wal']['path'],
                    size=osd['block.wal']['human_readable_size'],
                    percent=osd['block.wal']['percentage'])

        print(string)

    def compute(self):
        osds = self.computed['osds']

        if self.data_devs and self.db_or_journal_devs:
            if not self.common_vg:
                # there isn't a common vg, so a new one must be created with all
                # the blank db devs
                self.computed['vg'] = {
                    'devices': ", ".join([ssd.abspath for ssd in self.blank_db_devs]),
                    'parts': self.dbs_needed,
                    'percentages': self.vg_extents['percentages'],
                    'sizes': self.block_db_size.b.as_int(),
                    'size': self.total_blank_db_dev_size.b.as_int(),
                    'human_readable_sizes': str(self.block_db_size),
                    'human_readable_size': str(self.total_available_db_space),
                }
                vg_name = 'vg/lv'
            else:
                vg_name = self.common_vg.name

        if self.data_devs and self.wal_devs:
            if not self.common_wal_vg:
                # there isn't a common vg, so a new one must be created with all
                # the blank wal devs
                self.computed['wal_vg'] = {
                    'devices': ", ".join([dev.abspath for dev in self.blank_wal_devs]),
                    'parts': self.wals_needed,
                    'percentages': self.wal_vg_extents['percentages'],
                    'sizes': self.block_wal_size.b.as_int(),
                    'size': self.total_blank_wal_dev_size.b.as_int(),
                    'human_readable_sizes': str(self.block_wal_size),
                    'human_readable_size': str(self.total_available_wal_space),
                }
                wal_vg_name = 'vg/lv'
            else:
                wal_vg_name = self.common_wal_vg.name

        for device in self.data_devs:
            for hdd in range(self.osds_per_device):
                osd = {'data': {}, 'block.db': {}}
                osd['data']['path'] = device.abspath
                osd['data']['size'] = device.lvm_size.b / self.osds_per_device
                osd['data']['percentage'] = 100 / self.osds_per_device
                osd['data']['human_readable_size'] = str(
                    disk.Size(b=device.lvm_size.b) / self.osds_per_device
                )

                if self.db_or_journal_devs:
                    osd['block.db']['path'] = 'vg: %s' % vg_name
                    osd['block.db']['size'] = int(self.block_db_size.b)
                    osd['block.db']['human_readable_size'] = str(self.block_db_size)
                    osd['block.db']['percentage'] = self.vg_extents['percentages']

                if self.wal_devs:
                    osd['block.wal'] = {}
                    osd['block.wal']['path'] = 'vg: %s' % wal_vg_name
                    osd['block.wal']['size'] = int(self.block_wal_size.b)
                    osd['block.wal']['human_readable_size'] = str(self.block_wal_size)
                    osd['block.wal']['percentage'] = self.wal_vg_extents['percentages']

                if self.osd_ids:
                    osd['osd_id'] = self.osd_ids.pop(0)

                osds.append(osd)

        self.computed['changed'] = len(osds) > 0

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (block, block.db, block.wal, etc..) and offload the OSD creation to
        ``lvm create``
        """
        data_vgs = dict([(osd['data']['path'], None) for osd in self.computed['osds']])

        # create 1 vg per data device first, mapping them to the device path,
        # when the lv gets created later, it can create as many as needed (or
        # even just 1)
        for osd in self.computed['osds']:
            vg = data_vgs.get(osd['data']['path'])
            if not vg:
                vg = lvm.create_vg(osd['data']['path'], name_prefix='ceph-block')
                data_vgs[osd['data']['path']] = vg

        if self.data_devs and self.db_or_journal_devs:
            blank_db_dev_paths = [d.abspath for d in self.blank_db_devs]

            # no common vg is found, create one with all the blank SSDs
            if not self.common_vg:
                db_vg = lvm.create_vg(blank_db_dev_paths, name_prefix='ceph-block-dbs')
            elif self.common_vg and blank_db_dev_paths:
                # if a common vg exists then extend it with any blank ssds
                db_vg = lvm.extend_vg(self.common_vg, blank_db_dev_paths)
            else:
                # one common vg with nothing else to extend can be used directly,
                # either this is one device with one vg, or multiple devices with the
                # same vg
                db_vg = self.common_vg

            if self.use_large_block_db:
                # make the block.db lvs as large as possible
                vg_free_count = str_to_int(db_vg.vg_free_count)
                db_lv_extents = int(vg_free_count / self.dbs_needed)
            else:
                db_lv_extents = db_vg.sizing(size=self.block_db_size.gb.as_int())['extents']

        if self.data_devs and self.wal_devs:
            blank_wal_dev_paths = [d.abspath for d in self.blank_wal_devs]

            if not self.common_wal_vg:
                wal_vg = lvm.create_vg(blank_wal_dev_paths,
                                      name_prefix='ceph-block-wals')
            elif self.common_wal_vg and blank_wal_dev_paths:
                wal_vg = lvm.extend_vg(self.common_wal_vg, blank_wal_dev_paths)
            else:
                wal_vg = self.common_wal_vg

            if self.use_large_block_wal:
                # make the block.db lvs as large as possible
                vg_free_count = str_to_int(wal_vg.vg_free_count)
                wal_lv_extents = int(vg_free_count / self.wals_needed)
            else:
                wal_lv_extents = wal_vg.sizing(size=self.block_wal_size.gb.as_int())['extents']

        # create the data lvs, and create the OSD with an lv from the common
        # block.db vg from before
        for osd in self.computed['osds']:
            data_path = osd['data']['path']
            data_vg = data_vgs[data_path]
            data_lv_extents = data_vg.sizing(parts=1)['extents']
            data_lv = lvm.create_lv(
                'osd-block', data_vg.name, extents=data_lv_extents, uuid_name=True
            )
            command = [
                '--bluestore',
                '--data', "%s/%s" % (data_lv.vg_name, data_lv.name),
            ]
            if 'block.db' in osd:
                db_lv = lvm.create_lv(
                    'osd-block-db', db_vg.name, extents=db_lv_extents, uuid_name=True
                )
                command.extend([ '--block.db',
                                '{}/{}'.format(db_lv.vg_name, db_lv.name)])
            if 'block.wal' in osd:
                wal_lv = lvm.create_lv(
                    'osd-block-wal', wal_vg.name, extents=wal_lv_extents, uuid_name=True
                )
                command.extend(
                    ['--block.wal',
                     '{}/{}'.format(wal_lv.vg_name, wal_lv.name)
                    ])
            if self.args.dmcrypt:
                command.append('--dmcrypt')
            if self.args.no_systemd:
                command.append('--no-systemd')
            if self.args.crush_device_class:
                command.extend(['--crush-device-class', self.args.crush_device_class])
            if 'osd_id' in osd:
                command.extend(['--osd-id', osd['osd_id']])

            if self.args.prepare:
                Prepare(command).main()
            else:
                Create(command).main()

    def validate(self):
        """
        HDDs represent data devices, and solid state devices are for block.db,
        make sure that the number of data devices would have enough LVs and
        those LVs would be large enough to accommodate a block.db
        """
        # validate minimum size for all devices
        validators.minimum_device_size(self.data_devs + self.db_or_journal_devs,
                                       osds_per_device=self.osds_per_device)
        validators.minimum_device_size(self.wal_devs,
                                      osds_per_device=self.osds_per_device,
                                      min_size=1)

        # make sure that data devices do not have any LVs
        validators.no_lvm_membership(self.data_devs)

        if self.data_devs and self.db_or_journal_devs:
            self._validate_db_devs()

        if self.data_devs and self.wal_devs:
            self._validate_wal_devs()

        if self.osd_ids:
            self._validate_osd_ids()

    def _validate_db_devs(self):
        # do not allow non-common VG to continue
        validators.has_common_vg(self.db_or_journal_devs)

        # find the common VG to calculate how much is available
        self.common_vg = self.get_common_vg(self.db_or_journal_devs)

        # find how many block.db LVs are possible from the common VG
        if self.common_vg:
            common_vg_size = disk.Size(gb=self.common_vg.free)
        else:
            common_vg_size = disk.Size(gb=0)

        # non-VG SSDs
        vg_members = set([d for d in self.db_or_journal_devs if d.is_lvm_member])
        self.blank_db_devs = set(self.db_or_journal_devs).difference(vg_members)
        self.total_blank_db_dev_size = disk.Size(b=0)
        for blank_db_dev in self.blank_db_devs:
            self.total_blank_db_dev_size += disk.Size(b=blank_db_dev.lvm_size.b)

        self.total_available_db_space = self.total_blank_db_dev_size + common_vg_size

        # If not configured, we default to 0, which is really "use as much as
        # possible" captured by the `else` condition
        if self.block_db_size.gb > 0:
            try:
                self.vg_extents = lvm.sizing(
                    self.total_available_db_space.b, size=self.block_db_size.b * self.osds_per_device
                )
            except SizeAllocationError:
                msg = "Not enough space in fast devices (%s) to create %s x %s block.db LV"
                raise RuntimeError(
                    msg % (self.total_available_db_space, self.osds_per_device, self.block_db_size)
                )
        else:
            self.vg_extents = lvm.sizing(
                self.total_available_db_space.b, parts=self.dbs_needed
            )

        # validate that number of block.db LVs possible are enough for number of
        # OSDs proposed
        if self.total_available_db_space.b == 0:
            msg = "No space left in fast devices to create block.db LVs"
            raise RuntimeError(msg)

        # bluestore_block_db_size was unset, so we must set this to whatever
        # size we get by dividing the total available space for block.db LVs
        # into the number of block.db LVs needed (i.e. "as large as possible")
        if self.block_db_size.b == 0:
            self.block_db_size = self.total_available_db_space / self.dbs_needed
            self.use_large_block_db = True

        total_dbs_possible = self.total_available_db_space / self.block_db_size

        if self.dbs_needed > total_dbs_possible:
            msg = "Not enough space (%s) to create %s x %s block.db LVs" % (
                self.total_available_db_space, self.dbs_needed, self.block_db_size,
            )
            raise RuntimeError(msg)

    def _validate_wal_devs(self):
        # do not allow non-common VG to continue
        validators.has_common_vg(self.wal_devs)

        # find the common VG to calculate how much is available
        self.common_wal_vg = self.get_common_vg(self.wal_devs)

        # find how many block.wal LVs are possible from the common VG
        if self.common_wal_vg:
            common_vg_size = disk.Size(gb=self.common_wal_vg.free)
        else:
            common_vg_size = disk.Size(gb=0)

        # non-VG SSDs
        vg_members = set([d for d in self.wal_devs if d.is_lvm_member])
        self.blank_wal_devs = set(self.wal_devs).difference(vg_members)
        self.total_blank_wal_dev_size = disk.Size(b=0)
        for blank_wal_dev in self.blank_wal_devs:
            self.total_blank_wal_dev_size += disk.Size(b=blank_wal_dev.lvm_size.b)

        self.total_available_wal_space = self.total_blank_wal_dev_size + common_vg_size

        # If not configured, we default to 0, which is really "use as much as
        # possible" captured by the `else` condition
        if self.block_wal_size.gb > 0:
            try:
                self.vg_extents = lvm.sizing(
                    self.total_available_wal_space.b, size=self.block_wal_size.b * self.osds_per_device
                )
            except SizeAllocationError:
                msg = "Not enough space in fast devices (%s) to create %s x %s block.wal LV"
                raise RuntimeError(
                    msg % (self.total_available_wal_space,
                           self.osds_per_device, self.block_wal_size)
                )
        else:
            self.wal_vg_extents = lvm.sizing(
                self.total_available_wal_space.b, parts=self.wals_needed
            )

        # validate that number of block.wal LVs possible are enough for number of
        # OSDs proposed
        if self.total_available_wal_space.b == 0:
            msg = "No space left in fast devices to create block.wal LVs"
            raise RuntimeError(msg)

        # bluestore_block_wal_size was unset, so we must set this to whatever
        # size we get by dividing the total available space for block.wal LVs
        # into the number of block.wal LVs needed (i.e. "as large as possible")
        if self.block_wal_size.b == 0:
            self.block_wal_size = self.total_available_wal_space / self.wals_needed
            self.use_large_block_wal = True

        total_wals_possible = self.total_available_wal_space / self.block_wal_size

        if self.wals_needed > total_wals_possible:
            msg = "Not enough space (%s) to create %s x %s block.wal LVs" % (
                self.total_available_wal_space, self.wals_needed,
                self.block_wal_size,
            )
            raise RuntimeError(msg)

