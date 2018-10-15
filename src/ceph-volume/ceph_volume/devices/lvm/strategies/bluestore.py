from __future__ import print_function
import json
from ceph_volume.util import disk, prepare
from ceph_volume.api import lvm
from . import validators
from ceph_volume.devices.lvm.create import Create
from ceph_volume.devices.lvm.prepare import Prepare
from ceph_volume.util import templates
from ceph_volume.exceptions import SizeAllocationError


class SingleType(object):
    """
    Support for all SSDs, or all HDDS
    """

    def __init__(self, devices, args):
        self.args = args
        self.osds_per_device = args.osds_per_device
        self.devices = devices
        # TODO: add --fast-devices and --slow-devices so these can be customized
        self.hdds = [device for device in devices if device.sys_api['rotational'] == '1']
        self.ssds = [device for device in devices if device.sys_api['rotational'] == '0']
        self.computed = {'osds': [], 'vgs': [], 'filtered_devices': args.filtered_devices}
        if self.devices:
            self.validate()
            self.compute()
        else:
            self.computed["changed"] = False

    @staticmethod
    def type():
        return "bluestore.SingleType"

    @property
    def total_osds(self):
        if self.hdds:
            return len(self.hdds) * self.osds_per_device
        else:
            return len(self.ssds) * self.osds_per_device

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def report_pretty(self):
        string = ""
        if self.args.filtered_devices:
            string += templates.filtered_devices(self.args.filtered_devices)
        string += templates.total_osds.format(
            total_osds=self.total_osds,
        )
        string += templates.osd_component_titles

        for osd in self.computed['osds']:
            string += templates.osd_header
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
            self.devices, osds_per_device=self.osds_per_device
        )

        # make sure that data devices do not have any LVs
        validators.no_lvm_membership(self.hdds)

    def compute(self):
        """
        Go through the rules needed to properly size the lvs, return
        a dictionary with the result
        """
        osds = self.computed['osds']
        for device in self.hdds:
            for hdd in range(self.osds_per_device):
                osd = {'data': {}, 'block.db': {}}
                osd['data']['path'] = device.abspath
                osd['data']['size'] = device.sys_api['size'] / self.osds_per_device
                osd['data']['parts'] = self.osds_per_device
                osd['data']['percentage'] = 100 / self.osds_per_device
                osd['data']['human_readable_size'] = str(
                    disk.Size(b=device.sys_api['size']) / self.osds_per_device
                )
                osds.append(osd)

        for device in self.ssds:
            extents = lvm.sizing(device.sys_api['size'], parts=self.osds_per_device)
            for ssd in range(self.osds_per_device):
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

                if self.args.prepare:
                    Prepare(command).main()
                else:
                    Create(command).main()


class MixedType(object):

    def __init__(self, devices, args):
        self.args = args
        self.devices = devices
        self.osds_per_device = args.osds_per_device
        # TODO: add --fast-devices and --slow-devices so these can be customized
        self.hdds = [device for device in devices if device.sys_api['rotational'] == '1']
        self.ssds = [device for device in devices if device.sys_api['rotational'] == '0']
        self.computed = {'osds': [], 'filtered_devices': args.filtered_devices}
        self.block_db_size = self.get_block_size()
        self.system_vgs = lvm.VolumeGroups()
        self.dbs_needed = len(self.hdds) * self.osds_per_device
        if self.devices:
            self.validate()
            self.compute()
        else:
            self.computed["changed"] = False

    @staticmethod
    def type():
        return "bluestore.MixedType"

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def get_block_size(self):
        if self.args.block_db_size:
            return disk.Size(b=self.args.block_db_size)
        else:
            return prepare.get_block_db_size(lv_format=False) or disk.Size(b=0)

    def report_pretty(self):
        vg_extents = lvm.sizing(self.total_available_db_space.b, parts=self.dbs_needed)
        db_size = str(disk.Size(b=(vg_extents['sizes'])))

        string = ""
        if self.args.filtered_devices:
            string += templates.filtered_devices(self.args.filtered_devices)
        string += templates.total_osds.format(
            total_osds=len(self.hdds) * self.osds_per_device
        )

        string += templates.ssd_volume_group.format(
            target='block.db',
            total_lv_size=str(self.total_available_db_space),
            total_lvs=vg_extents['parts'] * self.osds_per_device,
            block_lv_size=db_size,
            block_db_devices=', '.join([ssd.abspath for ssd in self.ssds]),
            lv_size=self.block_db_size or str(disk.Size(b=(vg_extents['sizes']))),
            total_osds=len(self.hdds)
        )

        string += templates.osd_component_titles
        for osd in self.computed['osds']:
            string += templates.osd_header
            string += templates.osd_component.format(
                _type='[data]',
                path=osd['data']['path'],
                size=osd['data']['human_readable_size'],
                percent=osd['data']['percentage'])

            string += templates.osd_component.format(
                _type='[block.db]',
                path=osd['block.db']['path'],
                size=osd['block.db']['human_readable_size'],
                percent=osd['block.db']['percentage'])

        print(string)

    def compute(self):
        osds = self.computed['osds']

        # unconfigured block db size will be 0, so set it back to using as much
        # as possible from looking at extents
        if self.block_db_size.b == 0:
            self.block_db_size = disk.Size(b=self.vg_extents['sizes'])

        if not self.common_vg:
            # there isn't a common vg, so a new one must be created with all
            # the blank SSDs
            self.computed['vg'] = {
                'devices': ", ".join([ssd.abspath for ssd in self.blank_ssds]),
                'parts': self.dbs_needed,
                'percentages': self.vg_extents['percentages'],
                'sizes': self.block_db_size.b.as_int(),
                'size': self.total_blank_ssd_size.b.as_int(),
                'human_readable_sizes': str(self.block_db_size),
                'human_readable_size': str(self.total_available_db_space),
            }
            vg_name = 'vg/lv'
        else:
            vg_name = self.common_vg.name

        for device in self.hdds:
            for hdd in range(self.osds_per_device):
                osd = {'data': {}, 'block.db': {}}
                osd['data']['path'] = device.abspath
                osd['data']['size'] = device.sys_api['size'] / self.osds_per_device
                osd['data']['percentage'] = 100 / self.osds_per_device
                osd['data']['human_readable_size'] = str(
                    disk.Size(b=(device.sys_api['size'])) / self.osds_per_device
                )
                osd['block.db']['path'] = 'vg: %s' % vg_name
                osd['block.db']['size'] = int(self.block_db_size.b)
                osd['block.db']['human_readable_size'] = str(self.block_db_size)
                osd['block.db']['percentage'] = self.vg_extents['percentages']
                osds.append(osd)

        self.computed['changed'] = len(osds) > 0

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (block, block.db, block.wal, etc..) and offload the OSD creation to
        ``lvm create``
        """
        blank_ssd_paths = [d.abspath for d in self.blank_ssds]
        data_vgs = dict([(osd['data']['path'], None) for osd in self.computed['osds']])

        # no common vg is found, create one with all the blank SSDs
        if not self.common_vg:
            db_vg = lvm.create_vg(blank_ssd_paths, name_prefix='ceph-block-dbs')

        # if a common vg exists then extend it with any blank ssds
        elif self.common_vg and blank_ssd_paths:
            db_vg = lvm.extend_vg(self.common_vg, blank_ssd_paths)

        # one common vg with nothing else to extend can be used directly,
        # either this is one device with one vg, or multiple devices with the
        # same vg
        else:
            db_vg = self.common_vg

        # since we are falling back to a block_db_size that might be "as large
        # as possible" we can't fully rely on LV format coming from the helper
        # function that looks up this value
        block_db_size = "%sG" % self.block_db_size.gb.as_int()

        # create 1 vg per data device first, mapping them to the device path,
        # when the lv gets created later, it can create as many as needed (or
        # even just 1)
        for osd in self.computed['osds']:
            vg = data_vgs.get(osd['data']['path'])
            if not vg:
                vg = lvm.create_vg(osd['data']['path'], name_prefix='ceph-block')
                data_vgs[osd['data']['path']] = vg

        # create the data lvs, and create the OSD with an lv from the common
        # block.db vg from before
        for osd in self.computed['osds']:
            data_path = osd['data']['path']
            data_lv_size = disk.Size(b=osd['data']['size']).gb.as_int()
            data_vg = data_vgs[data_path]
            data_lv_extents = data_vg.sizing(size=data_lv_size)['extents']
            data_lv = lvm.create_lv(
                'osd-block', data_vg.name, extents=data_lv_extents, uuid_name=True
            )
            db_lv = lvm.create_lv(
                'osd-block-db', db_vg.name, size=block_db_size, uuid_name=True
            )
            command = [
                '--bluestore',
                '--data', "%s/%s" % (data_lv.vg_name, data_lv.name),
                '--block.db', '%s/%s' % (db_lv.vg_name, db_lv.name)
            ]
            if self.args.dmcrypt:
                command.append('--dmcrypt')
            if self.args.no_systemd:
                command.append('--no-systemd')
            if self.args.crush_device_class:
                command.extend(['--crush-device-class', self.args.crush_device_class])

            if self.args.prepare:
                Prepare(command).main()
            else:
                Create(command).main()

    def get_common_vg(self):
        # find all the vgs associated with the current device
        for ssd in self.ssds:
            for pv in ssd.pvs_api:
                vg = self.system_vgs.get(vg_name=pv.vg_name)
                if not vg:
                    continue
                # this should give us just one VG, it would've been caught by
                # the validator otherwise
                return vg

    def validate(self):
        """
        HDDs represent data devices, and solid state devices are for block.db,
        make sure that the number of data devices would have enough LVs and
        those LVs would be large enough to accommodate a block.db
        """
        # validate minimum size for all devices
        validators.minimum_device_size(self.devices, osds_per_device=self.osds_per_device)

        # make sure that data devices do not have any LVs
        validators.no_lvm_membership(self.hdds)

        # do not allow non-common VG to continue
        validators.has_common_vg(self.ssds)

        # find the common VG to calculate how much is available
        self.common_vg = self.get_common_vg()

        # find how many block.db LVs are possible from the common VG
        if self.common_vg:
            common_vg_size = disk.Size(gb=self.common_vg.free)
        else:
            common_vg_size = disk.Size(gb=0)

        # non-VG SSDs
        self.vg_ssds = set([d for d in self.ssds if d.is_lvm_member])
        self.blank_ssds = set(self.ssds).difference(self.vg_ssds)
        self.total_blank_ssd_size = disk.Size(b=0)
        for blank_ssd in self.blank_ssds:
            self.total_blank_ssd_size += disk.Size(b=blank_ssd.sys_api['size'])

        self.total_available_db_space = self.total_blank_ssd_size + common_vg_size

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

        total_dbs_possible = self.total_available_db_space / self.block_db_size

        if self.dbs_needed > total_dbs_possible:
            msg = "Not enough space (%s) to create %s x %s block.db LVs" % (
                self.total_available_db_space, self.dbs_needed, self.block_db_size,
            )
            raise RuntimeError(msg)
