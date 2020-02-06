from __future__ import print_function
import argparse
import json
import logging
import os.path
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
                raise SystemExit('No valid Ceph devices found')
            pretty_report(report)

    def create_report(self, lvs, full_report=True, arg_is_vg=False):
        """
        Create a report for LVM dev(s) passed. Returns '{}' to denote failure.
        """
        if not lvs:
            return {}

        def create_report_for_nonlv_device():
            # bluestore will not have a journal, filestore will not have
            # a block/wal/db, so we must skip if not present
            if dev_type == 'data':
                return

            device_uuid = lv.tags.get('ceph.%s_uuid' % dev_type)
            pv = api.get_first_pv(filters={'vg_name': lv.vg_name})
            if device_uuid and pv:
                report[osd_id].append({'tags': {'PARTUUID': device_uuid},
                                       'type': dev_type,
                                       'path': pv.pv_name})

        report = {}

        lvs = [lvs] if isinstance(lvs, api.Volume) else lvs
        for lv in lvs:
            if not api.is_ceph_device(lv):
                continue

            osd_id = lv.tags['ceph.osd_id']
            report.setdefault(osd_id, [])
            lv_report = lv.as_dict()

            dev_type = lv.tags.get('ceph.type', None)
            if dev_type != 'journal' or (dev_type == 'journal' and
               full_report):
                pvs = api.get_pvs(filters={'lv_uuid': lv.lv_uuid})
                lv_report['devices'] = [pv.name for pv in pvs] if pvs else []
                report[osd_id].append(lv_report)

            if arg_is_vg or dev_type in ['journal', 'wal']:
                create_report_for_nonlv_device()

        return report

    def full_report(self):
        """
        Create a report of all Ceph LVs. Returns '{}' to denote failure.
        """
        return self.create_report(api.get_lvs())

    def single_report(self, device):
        """
        Generate a report for a single device. This can be either a logical
        volume in the form of vg/lv or a device with an absolute path like
        /dev/sda1 or /dev/sda. Returns '{}' to denote failure.
        """

        # The `device` argument can be a logical volume name or a device path.
        # If it's a path that exists, use the canonical path (in particular,
        # dereference symlinks); otherwise, assume it's a logical volume name
        # and use it as-is.
        if os.path.exists(device):
            device = os.path.realpath(device)

        lv = api.get_lvs(filters={'lv_full_name': device})
        if not lv:
            # if device at given path is not LV, it might be PV...
            pv = api.get_first_pv(filters={'pv_name': device})
            if pv:
                lv = api.get_lvs(filters={'vg_name': pv.vg_name})

        if not lv:
            return {}

        return self.create_report(lv, full_report=False)

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
