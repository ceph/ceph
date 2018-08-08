from __future__ import print_function
import json
from uuid import uuid4
from ceph_volume.util import disk
from ceph_volume.api import lvm
from . import validators
from ceph_volume.devices.lvm.create import Create
from ceph_volume.util import templates


class SingleType(object):
    """
    Support for all SSDs, or all HDDS
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
        osds = self.computed['osds']
        vgs = self.computed['vgs']
        for device in self.hdds:
            vgs.append({'devices': [device['path']], 'parts': 1})
            osd = {'data': {}, 'block.db': {}}
            osd['data']['path'] = device['path']
            osd['data']['size'] = device['size']
            osd['data']['parts'] = 1
            osd['data']['percentage'] = 100
            osd['data']['human_readable_size'] = str(disk.Size(b=device['size']))
            osds.append(osd)

        for device in self.ssds:
            # TODO: creates 2 OSDs per device, make this configurable (env var?)
            extents = lvm.sizing(device['size'], parts=2)
            vgs.append({'devices': [device['path']], 'parts': 2})
            for ssd in range(2):
                osd = {'data': {}, 'block.db': {}}
                osd['data']['path'] = device['path']
                osd['data']['size'] = extents['sizes']
                osd['data']['parts'] = extents['parts']
                osd['data']['percentage'] = 50
                osd['data']['human_readable_size'] = str(disk.Size(b=extents['sizes']))
                osds.append(osd)

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

                Create(command).main()


class MixedType(object):

    def __init__(self, devices, args):
        self.args = args
        self.devices = devices
        self.hdds = [device for device in devices if device['rotational'] == '1']
        self.ssds = [device for device in devices if device['rotational'] == '0']
        self.computed = {'osds': [], 'vgs': []}
        self.block_db_size = None
        # For every HDD we get 1 block.db
        self.db_lvs = len(self.hdds)
        self.validate()
        self.compute()

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    def report_pretty(self):
        vg_extents = lvm.sizing(self.total_ssd_size.b, parts=self.db_lvs)
        db_size = str(disk.Size(b=(vg_extents['sizes'])))

        string = ""
        string += templates.total_osds.format(
            total_osds=len(self.hdds)
        )

        string += templates.ssd_volume_group.format(
            target='block.db',
            total_lv_size=str(self.total_ssd_size),
            total_lvs=vg_extents['parts'],
            block_lv_size=db_size,
            block_db_devices=', '.join([ssd['path'] for ssd in self.ssds]),
            lv_size=str(disk.Size(b=(vg_extents['sizes']))),
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
                path='(volume-group/lv)',
                size=osd['block.db']['human_readable_size'],
                percent=osd['block.db']['percentage'])

        print(string)

    def compute(self):
        osds = self.computed['osds']
        for device in self.hdds:
            osd = {'data': {}, 'block.db': {}}
            osd['data']['path'] = device['path']
            osd['data']['size'] = device['size']
            osd['data']['percentage'] = 100
            osd['data']['human_readable_size'] = str(disk.Size(b=(device['size'])))
            osd['block.db']['path'] = None
            osd['block.db']['size'] = int(self.block_db_size.b)
            osd['block.db']['human_readable_size'] = str(self.block_db_size)
            osd['block.db']['percentage'] = self.vg_extents['percentages']
            osds.append(osd)

        self.computed['vgs'] = [{
            'devices': [d['path'] for d in self.ssds],
            'parts': self.db_lvs,
            'percentages': self.vg_extents['percentages'],
            'sizes': self.vg_extents['sizes'],
            'size': int(self.total_ssd_size.b),
            'human_readable_sizes': str(disk.Size(b=self.vg_extents['sizes'])),
            'human_readable_size': str(self.total_ssd_size),
        }]

    def execute(self):
        """
        Create vgs/lvs from the incoming set of devices, assign their roles
        (block, block.db, block.wal, etc..) and offload the OSD creation to
        ``lvm create``
        """
        # create the single vg for all block.db lv's first
        vg_info = self.computed['vgs'][0]
        vg = lvm.create_vg(vg_info['devices'])

        # now produce all the block.db lvs needed from that single vg
        db_lvs = lvm.create_lvs(vg, parts=vg_info['parts'], name_prefix='osd-block-db')

        # create the data lvs, and create the OSD with the matching block.db lvs from before
        for osd in self.computed['osds']:
            vg = lvm.create_vg(osd['data']['path'])
            data_lv = lvm.create_lv('osd-data-%s' % str(uuid4()), vg.name)
            db_lv = db_lvs.pop()
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

            Create(command).main()

    def validate(self):
        """
        HDDs represent data devices, and solid state devices are for block.db,
        make sure that the number of data devices would have enough LVs and
        those LVs would be large enough to accommodate a block.db
        """
        # validate minimum size for all devices
        validators.minimum_device_size(self.devices)

        # add all the size available in solid drives and divide it by the
        # expected number of osds, the expected output should be larger than
        # the minimum alllowed for block.db
        self.total_ssd_size = disk.Size(b=0)
        for ssd in self.ssds:
            self.total_ssd_size += disk.Size(b=ssd['size'])

        self.block_db_size = self.total_ssd_size / self.db_lvs
        self.vg_extents = lvm.sizing(self.total_ssd_size.b, parts=self.db_lvs)

        # min 2GB of block.db is allowed
        msg = 'Total solid size (%s) is not enough for block.db LVs larger than 2 GB'
        if self.block_db_size < disk.Size(gb=2):
            # use ad-hoc exception here
            raise RuntimeError(msg % self.total_ssd_size)
