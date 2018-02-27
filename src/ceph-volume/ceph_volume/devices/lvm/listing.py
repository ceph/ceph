from __future__ import print_function
import argparse
import json
import logging
from textwrap import dedent
from ceph_volume import decorators
from ceph_volume.util import disk
from ceph_volume.api import lvm as api
from ceph_volume.exceptions import MultipleLVsError

logger = logging.getLogger(__name__)


osd_list_header_template = """\n
{osd_id:=^20}"""


osd_device_header_template = """

  [{type: >4}]    {path}
"""

device_metadata_item_template = """
      {tag_name: <25} {value}"""


def readable_tag(tag):
    actual_name = tag.split('.')[-1]
    return actual_name.replace('_', ' ')


def pretty_report(report):
    output = []
    for _id, devices in report.items():
        output.append(
            osd_list_header_template.format(osd_id=" osd.%s " % _id)
        )
        for device in devices:
            output.append(
                osd_device_header_template.format(
                    type=device['type'],
                    path=device['path']
                )
            )
            for tag_name, value in device.get('tags', {}).items():
                output.append(
                    device_metadata_item_template.format(
                        tag_name=readable_tag(tag_name),
                        value=value
                    )
                )
    print(''.join(output))


class List(object):

    help = 'list logical volumes and devices associated with Ceph'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def list(self, args):
        # ensure everything is up to date before calling out
        # to list lv's
        self.update()
        report = self.generate(args)
        if args.format == 'json':
            # If the report is empty, we don't return a non-zero exit status
            # because it is assumed this is going to be consumed by automated
            # systems like ceph-ansible which would be forced to ignore the
            # non-zero exit status if all they need is the information in the
            # JSON object
            print(json.dumps(report, indent=4, sort_keys=True))
        else:
            if not report:
                raise SystemExit('No valid Ceph devices found')
            pretty_report(report)

    def update(self):
        """
        Ensure all journal devices are up to date if they aren't a logical
        volume
        """
        lvs = api.Volumes()
        for lv in lvs:
            try:
                lv.tags['ceph.osd_id']
            except KeyError:
                # only consider ceph-based logical volumes, everything else
                # will get ignored
                continue

            for device_type in ['journal', 'block', 'wal', 'db']:
                device_name = 'ceph.%s_device' % device_type
                device_uuid = lv.tags.get('ceph.%s_uuid' % device_type)
                if not device_uuid:
                    # bluestore will not have a journal, filestore will not have
                    # a block/wal/db, so we must skip if not present
                    continue
                disk_device = disk.get_device_from_partuuid(device_uuid)
                if disk_device:
                    if lv.tags[device_name] != disk_device:
                        # this means that the device has changed, so it must be updated
                        # on the API to reflect this
                        lv.set_tags({device_name: disk_device})

    def generate(self, args):
        """
        Generate reports for an individual device or for all Ceph-related
        devices, logical or physical, as long as they have been prepared by
        this tool before and contain enough metadata.
        """
        if args.device:
            return self.single_report(args.device)
        else:
            return self.full_report()

    def single_report(self, device):
        """
        Generate a report for a single device. This can be either a logical
        volume in the form of vg/lv or a device with an absolute path like
        /dev/sda1 or /dev/sda
        """
        lvs = api.Volumes()
        report = {}
        lv = api.get_lv_from_argument(device)

        # check if there was a pv created with the
        # name of device
        pv = api.get_pv(pv_name=device)
        if pv and not lv:
            try:
                lv = api.get_lv(vg_name=pv.vg_name)
            except MultipleLVsError:
                lvs.filter(vg_name=pv.vg_name)
                return self.full_report(lvs=lvs)

        if lv:
            try:
                _id = lv.tags['ceph.osd_id']
            except KeyError:
                logger.warning('device is not part of ceph: %s', device)
                return report

            report.setdefault(_id, [])
            report[_id].append(
                lv.as_dict()
            )

        else:
            # this has to be a journal/wal/db device (not a logical volume) so try
            # to find the PARTUUID that should be stored in the OSD logical
            # volume
            for device_type in ['journal', 'block', 'wal', 'db']:
                device_tag_name = 'ceph.%s_device' % device_type
                device_tag_uuid = 'ceph.%s_uuid' % device_type
                associated_lv = lvs.get(lv_tags={device_tag_name: device})
                if associated_lv:
                    _id = associated_lv.tags['ceph.osd_id']
                    uuid = associated_lv.tags[device_tag_uuid]

                    report.setdefault(_id, [])
                    report[_id].append(
                        {
                            'tags': {'PARTUUID': uuid},
                            'type': device_type,
                            'path': device,
                        }
                    )
        return report

    def full_report(self, lvs=None):
        """
        Generate a report for all the logical volumes and associated devices
        that have been previously prepared by Ceph
        """
        if lvs is None:
            lvs = api.Volumes()
        report = {}
        for lv in lvs:
            try:
                _id = lv.tags['ceph.osd_id']
            except KeyError:
                # only consider ceph-based logical volumes, everything else
                # will get ignored
                continue

            report.setdefault(_id, [])
            report[_id].append(
                lv.as_dict()
            )

            for device_type in ['journal', 'block', 'wal', 'db']:
                device_uuid = lv.tags.get('ceph.%s_uuid' % device_type)
                if not device_uuid:
                    # bluestore will not have a journal, filestore will not have
                    # a block/wal/db, so we must skip if not present
                    continue
                if not api.get_lv(lv_uuid=device_uuid):
                    # means we have a regular device, so query blkid
                    disk_device = disk.get_device_from_partuuid(device_uuid)
                    if disk_device:
                        report[_id].append(
                            {
                                'tags': {'PARTUUID': device_uuid},
                                'type': device_type,
                                'path': disk_device,
                            }
                        )

        return report

    def main(self):
        sub_command_help = dedent("""
        List devices or logical volumes associated with Ceph. An association is
        determined if a device has information relating to an OSD. This is
        verified by querying LVM's metadata and correlating it with devices.

        The lvs associated with the OSD need to have been prepared previously,
        so that all needed tags and metadata exist.

        Full listing of all system devices associated with a cluster::

            ceph-volume lvm list

        List a particular device, reporting all metadata about it::

            ceph-volume lvm list /dev/sda1

        List a logical volume, along with all its metadata (vg is a volume
        group, and lv the logical volume name)::

            ceph-volume lvm list {vg/lv}
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm list',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'device',
            metavar='DEVICE',
            nargs='?',
            help='Path to an lv (as vg/lv) or to a device like /dev/sda1'
        )

        parser.add_argument(
            '--format',
            help='output format, defaults to "pretty"',
            default='pretty',
            choices=['json', 'pretty'],
        )

        args = parser.parse_args(self.argv)
        self.list(args)
