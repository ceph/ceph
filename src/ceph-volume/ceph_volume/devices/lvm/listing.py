from __future__ import print_function
import argparse
import json
import logging
from textwrap import dedent
from ceph_volume import decorators
from ceph_volume.api import lvm as api

logger = logging.getLogger(__name__)


osd_list_header_template = """\n
{osd_id:=^20}"""


osd_device_header_template = """

  {type: <13} {path}
"""

device_metadata_item_template = """
      {tag_name: <25} {value}"""


def readable_tag(tag):
    actual_name = tag.split('.')[-1]
    return actual_name.replace('_', ' ')


def pretty_report(report):
    output = []
    for osd_id, devices in sorted(report.items()):
        output.append(
            osd_list_header_template.format(osd_id=" osd.%s " % osd_id)
        )
        for device in devices:
            output.append(
                osd_device_header_template.format(
                    type='[%s]' % device['type'],
                    path=device['path']
                )
            )
            for tag_name, value in sorted(device.get('tags', {}).items()):
                output.append(
                    device_metadata_item_template.format(
                        tag_name=readable_tag(tag_name),
                        value=value
                    )
                )
            if not device.get('devices'):
                continue
            else:
                output.append(
                    device_metadata_item_template.format(
                        tag_name='devices',
                        value=','.join(device['devices'])
                    )
                )

    print(''.join(output))


def direct_report():
    """
    Other non-cli consumers of listing information will want to consume the
    report without the need to parse arguments or other flags. This helper
    bypasses the need to deal with the class interface which is meant for cli
    handling.
    """
    return List([]).full_report()


# TODO: Perhaps, get rid of this class and simplify this module further?
class List(object):

    help = 'list logical volumes and devices associated with Ceph'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def list(self, args):
        report = self.single_report(args.device) if args.device else \
                 self.full_report()
        if args.format == 'json':
            # If the report is empty, we don't return a non-zero exit status
            # because it is assumed this is going to be consumed by automated
            # systems like ceph-ansible which would be forced to ignore the
            # non-zero exit status if all they need is the information in the
            # JSON object
            print(json.dumps(report, indent=4, sort_keys=True))
        else:
            if not report:
                raise SystemExit('No valid Ceph lvm devices found')
            pretty_report(report)

    def create_report(self, lvs):
        """
        Create a report for LVM dev(s) passed. Returns '{}' to denote failure.
        """

        report = {}

        pvs = api.get_pvs()

        for lv in lvs:
            if not api.is_ceph_device(lv):
                continue

            osd_id = lv.tags['ceph.osd_id']
            report.setdefault(osd_id, [])
            lv_report = lv.as_dict()

            lv_report['devices'] = [pv.name for pv in pvs if pv.lv_uuid == lv.lv_uuid] if pvs else []
            report[osd_id].append(lv_report)

            phys_devs = self.create_report_non_lv_device(lv)
            if phys_devs:
                report[osd_id].append(phys_devs)

        return report

    def create_report_non_lv_device(self, lv):
        report = {}
        if lv.tags.get('ceph.type', '') in ['data', 'block']:
            for dev_type in ['journal', 'wal', 'db']:
                dev = lv.tags.get('ceph.{}_device'.format(dev_type), '')
                # counting / in the device name seems brittle but should work,
                # lvs will have 3
                if dev and dev.count('/') == 2:
                    device_uuid = lv.tags.get('ceph.{}_uuid'.format(dev_type))
                    report = {'tags': {'PARTUUID': device_uuid},
                              'type': dev_type,
                              'path': dev}
        return report

    def full_report(self):
        """
        Create a report of all Ceph LVs. Returns '{}' to denote failure.
        """
        return self.create_report(api.get_lvs())

    def single_report(self, arg):
        """
        Generate a report for a single device. This can be either a logical
        volume in the form of vg/lv, a device with an absolute path like
        /dev/sda1 or /dev/sda, or a list of devices under same OSD ID.

        Return value '{}' denotes failure.
        """
        if isinstance(arg, int) or arg.isdigit():
            lv = api.get_lvs_from_osd_id(arg)
        elif arg[0] == '/':
            lv = api.get_lvs_from_path(arg)
        else:
            vg_name, lv_name = arg.split('/')
            lv = [api.get_single_lv(filters={'lv_name': lv_name,
                                             'vg_name': vg_name})]

        report = self.create_report(lv)

        if not report:
            # check if device is a non-lvm journals or wal/db
            for dev_type in ['journal', 'wal', 'db']:
                lvs = api.get_lvs(tags={
                    'ceph.{}_device'.format(dev_type): arg})
                if lvs:
                    # just taking the first lv here should work
                    lv = lvs[0]
                    phys_dev = self.create_report_non_lv_device(lv)
                    osd_id = lv.tags.get('ceph.osd_id')
                    if osd_id:
                        report[osd_id] = [phys_dev]


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

        List devices under same OSD ID::

            ceph-volume lvm list <OSD-ID>

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
