from __future__ import print_function
import json
from ceph_volume.util import disk, prepare
from ceph_volume.api import lvm
from . import validators
from ceph_volume.devices.lvm.create import Create
from ceph_volume.util import templates


class SingleType(object):
    """
    Support for all SSDs, or all HDDs, data and journal LVs will be colocated
    in the same device
    """

    def __init__(self, devices, args):
        self.args = args
        self.devices = devices
        self.hdds = [device for device in devices if device.sys_api['rotational'] == '1']
        self.ssds = [device for device in devices if device.sys_api['rotational'] == '0']
        self.computed = {'osds': [], 'vgs': []}
        self.validate()
        self.compute()

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def report_pretty(self):
        string = ""
        string += templates.total_osds.format(
            total_osds=len(self.hdds) or len(self.ssds) * 2
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
            string += templates.osd_component.format(
                _type='[journal]',
                path=osd['journal']['path'],
                size=osd['journal']['human_readable_size'],
                percent=osd['journal']['percentage'],
            )

        print(string)

    def validate(self):
        """
        Ensure that the minimum requirements for this type of scenario is
        met, raise an error if the provided devices would not work
        """
        # validate minimum size for all devices
        validators.minimum_device_size(self.devices)

    def compute(self):
        """
        Go through the rules needed to properly size the lvs, return
        a dictionary with the result
        """
        # chose whichever is the one group we have to compute against
        devices = self.hdds or self.ssds
        osds = self.computed['osds']
        vgs = self.computed['vgs']
        for device in devices:
            device_size = disk.Size(b=device.sys_api['size'])
            journal_size = prepare.get_journal_size(lv_format=False)
            data_size = device_size - journal_size
            data_percentage = data_size * 100 / device_size
            vgs.append({'devices': [device.abspath], 'parts': 2})
            osd = {'data': {}, 'journal': {}}
            osd['data']['path'] = device.abspath
            osd['data']['size'] = data_size.b
            osd['data']['percentage'] = int(data_percentage)
            osd['data']['human_readable_size'] = str(data_size)
            osd['journal']['path'] = device.abspath
            osd['journal']['size'] = journal_size.b
            osd['journal']['percentage'] = int(100 - data_percentage)
            osd['journal']['human_readable_size'] = str(journal_size)
            osds.append(osd)

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (data, journal) and offload the OSD creation to ``lvm create``
        """
        osd_vgs = []

        # create the vgs first, one per device (since this is colocating, it
        # picks the 'data' path)
        for osd in self.computed['osds']:
            vg = lvm.create_vg(osd['data']['path'])
            osd_vgs.append(vg)

        journal_size = prepare.get_journal_size()

        # create the lvs from the vgs captured in the beginning
        for vg in osd_vgs:
            # this is called again, getting us the LVM formatted string
            journal_lv = lvm.create_lv(
                'osd-journal', vg.name, size=journal_size, uuid_name=True
            )
            # no extents or size means it will use 100%FREE
            data_lv = lvm.create_lv('osd-data', vg.name)

            command = ['--filestore', '--data']
            command.append('%s/%s' % (vg.name, data_lv.name))
            command.extend(['--journal', '%s/%s' % (vg.name, journal_lv.name)])
            if self.args.dmcrypt:
                command.append('--dmcrypt')
            if self.args.no_systemd:
                command.append('--no-systemd')
            if self.args.crush_device_class:
                command.extend(['--crush-device-class', self.args.crush_device_class])

            Create(command).main()


class MixedType(object):
    """
    Supports HDDs with SSDs, journals will be placed on SSDs, while HDDs will
    be used fully for data.

    If an existing common VG is detected on SSDs, it will be extended if blank
    SSDs are used, otherwise it will be used directly.
    """

    def __init__(self, devices, args):
        self.args = args
        self.devices = devices
        self.hdds = [device for device in devices if device.sys_api['rotational'] == '1']
        self.ssds = [device for device in devices if device.sys_api['rotational'] == '0']
        self.computed = {'osds': [], 'vg': None}
        self.blank_ssds = []
        self.journals_needed = len(self.hdds)
        self.journal_size = prepare.get_journal_size(lv_format=False)
        self.system_vgs = lvm.VolumeGroups()
        self.validate()
        self.compute()

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def report_pretty(self):
        string = ""
        string += templates.total_osds.format(
            total_osds=len(self.hdds) or len(self.ssds) * 2
        )

        string += templates.ssd_volume_group.format(
            target='journal',
            total_lv_size=str(self.total_available_journal_space),
            total_lvs=self.journals_needed,
            block_db_devices=', '.join([d.path for d in self.ssds]),
            lv_size=str(self.journal_size),
            total_osds=self.journals_needed
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
            string += templates.osd_component.format(
                _type='[journal]',
                path=osd['journal']['path'],
                size=osd['journal']['human_readable_size'],
                percent=osd['journal']['percentage'],
            )

        print(string)

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
        Ensure that the minimum requirements for this type of scenario is
        met, raise an error if the provided devices would not work
        """
        # validate minimum size for all devices
        validators.minimum_device_size(self.devices)

        # make sure that data devices do not have any LVs
        validators.no_lvm_membership(self.hdds)

        # do not allow non-common VG to continue
        validators.has_common_vg(self.ssds)

        # find the common VG to calculate how much is available
        self.common_vg = self.get_common_vg()

        # find how many journals are possible from the common VG
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

        self.total_available_journal_space = self.total_blank_ssd_size + common_vg_size

        try:
            self.vg_extents = lvm.sizing(
                self.total_available_journal_space.b, size=self.journal_size.b
            )
        # FIXME with real exception catching from sizing that happens when the
        # journal space is not enough
        except Exception:
            self.vg_extents = {'parts': 0, 'percentages': 0, 'sizes': 0}

        # validate that number of journals possible are enough for number of
        # OSDs proposed
        total_journals_possible = self.total_available_journal_space / self.journal_size
        if len(self.hdds) > total_journals_possible:
            msg = "Not enough %s journals (%s) can be created for %s OSDs" % (
                self.journal_size, total_journals_possible, len(self.hdds)
            )
            raise RuntimeError(msg)

    def compute(self):
        """
        Go through the rules needed to properly size the lvs, return
        a dictionary with the result
        """
        osds = self.computed['osds']

        vg_free = int(self.total_available_journal_space.gb)
        if not self.common_vg:
            # there isn't a common vg, so a new one must be created with all
            # the blank SSDs
            self.computed['vg'] = {
                'devices': self.blank_ssds,
                'parts': self.journals_needed,
                'percentages': self.vg_extents['percentages'],
                'sizes': self.journal_size.b,
                'size': int(self.total_blank_ssd_size.b),
                'human_readable_sizes': str(self.journal_size),
                'human_readable_size': str(self.total_available_journal_space),
            }
            vg_name = 'lv/vg'
        else:
            vg_name = self.common_vg.name

        for device in self.hdds:
            device_size = disk.Size(b=device.sys_api['size'])
            data_size = device_size - self.journal_size
            osd = {'data': {}, 'journal': {}}
            osd['data']['path'] = device.path
            osd['data']['size'] = data_size.b
            osd['data']['percentage'] = 100
            osd['data']['human_readable_size'] = str(device_size)
            osd['journal']['path'] = 'vg: %s' % vg_name
            osd['journal']['size'] = self.journal_size.b
            osd['journal']['percentage'] = int(self.journal_size.gb * 100 / vg_free)
            osd['journal']['human_readable_size'] = str(self.journal_size)
            osds.append(osd)

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (data, journal) and offload the OSD creation to ``lvm create``
        """
        ssd_paths = [d.abspath for d in self.blank_ssds]

        # no common vg is found, create one with all the blank SSDs
        if not self.common_vg:
            journal_vg = lvm.create_vg(ssd_paths, name_prefix='ceph-journals')
        # a vg exists that can be extended
        elif self.common_vg and ssd_paths:
            journal_vg = lvm.extend_vg(self.common_vg, ssd_paths)
        # one common vg with nothing else to extend can be used directly
        else:
            journal_vg = self.common_vg

        journal_size = prepare.get_journal_size(lv_format=True)

        for osd in self.computed['osds']:
            data_vg = lvm.create_vg(osd['data']['path'], name_prefix='ceph-data')
            # no extents or size means it will use 100%FREE
            data_lv = lvm.create_lv('osd-data', data_vg.name)
            journal_lv = lvm.create_lv(
                'osd-journal', journal_vg.name, size=journal_size, uuid_name=True
            )

            command = ['--filestore', '--data']
            command.append('%s/%s' % (data_vg.name, data_lv.name))
            command.extend(['--journal', '%s/%s' % (journal_vg.name, journal_lv.name)])
            if self.args.dmcrypt:
                command.append('--dmcrypt')
            if self.args.no_systemd:
                command.append('--no-systemd')
            if self.args.crush_device_class:
                command.extend(['--crush-device-class', self.args.crush_device_class])

            Create(command).main()
