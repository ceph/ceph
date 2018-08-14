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
        self.hdds = [device for device in devices if device['rotational'] == '1']
        self.ssds = [device for device in devices if device['rotational'] == '0']
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
            device_size = disk.Size(b=device['size'])
            journal_size = prepare.get_journal_size(lv_format=False)
            data_size = device_size - journal_size
            data_percentage = data_size * 100 / device_size
            vgs.append({'devices': [device['path']], 'parts': 2})
            osd = {'data': {}, 'journal': {}}
            osd['data']['path'] = device['path']
            osd['data']['size'] = data_size.b
            osd['data']['percentage'] = int(data_percentage)
            osd['data']['human_readable_size'] = str(data_size)
            osd['journal']['path'] = device['path']
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

        # create the lvs from the vgs captured in the beginning
        for vg in osd_vgs:
            # this is called again, getting us the LVM formatted string
            journal_size = prepare.get_journal_size()
            journal_lv = lvm.create_lv('osd-journal', vg.name, size=journal_size)
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
    pass
