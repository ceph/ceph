# -*- coding: utf-8 -*-

import argparse

from ceph_volume import terminal
from ceph_volume.api import lvm as api
from ceph_volume.objectstore.lvm import Lvm as LVMActivate
from ceph_volume.objectstore.raw import Raw as RAWActivate
from ceph_volume.devices.simple.activate import Activate as SimpleActivate
from ceph_volume.util.lvm_osd_mappers import OsdLvmMappers

class Activate(object):

    help = "Activate an OSD"

    def __init__(self, argv):
        self.argv = argv

    def _has_osd_lvs(self):
        """
        Return True if an LV is tagged with the OSD fsid (and the OSD id, if
        given), i.e. the OSD is an LVM OSD. A failed lookup returns False.
        """
        if self.args.osd_fsid is None:
            return False
        tags = {'ceph.osd_fsid': self.args.osd_fsid}
        if self.args.osd_id is not None:
            tags['ceph.osd_id'] = self.args.osd_id
        try:
            lvs = api.get_lvs(tags=tags)
        except Exception as e:
            terminal.info(f'Failed to look up the LVs of the OSD: {e}')
            return False
        if lvs:
            terminal.info(f'Found LVM OSD {self.args.osd_fsid}, '
                          'skipping raw activation')
        return bool(lvs)

    def main(self):
        parser = argparse.ArgumentParser(
            prog='ceph-volume activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.help,
        )
        parser.add_argument(
            '--osd-id',
            help='OSD ID to activate'
        )
        parser.add_argument(
            '--osd-uuid',
            help='OSD UUID to activate',
            dest='osd_fsid'
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services'
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            help='Do not use a tmpfs mount for OSD data dir'
        )
        self.args = parser.parse_args(self.argv)

        # Close the LVM mappers to force a 'refresh'
        # Avoid that raw activates a LVM osd that is already activated
        # Only do this when both filters are available, because the mapper
        # lookup builds LVM tag strings and will fail if osd_fsid is None.
        if self.args.osd_id is not None and self.args.osd_fsid is not None:
            OsdLvmMappers(self.args.osd_id, self.args.osd_fsid).close()

        # first try raw, unless an LV carries this OSD's fsid: raw skips
        # LVM-backed OSDs anyway, and probing every block device of the
        # host for them is slow on hosts with many devices.
        if not self._has_osd_lvs():
            try:
                raw_activate = RAWActivate(self.args)
                raw_activate.activate()
                return
            except Exception as e:
                terminal.info(f'Failed to activate via raw: {e}')

        # then try lvm
        try:
            lvm_activate = LVMActivate(argparse.Namespace(
                no_tmpfs=self.args.no_tmpfs,
                no_systemd=self.args.no_systemd,
                osd_fsid=self.args.osd_fsid))
            lvm_activate.activate(None,
                                  self.args.osd_id,
                                  self.args.osd_fsid)
            return
        except Exception as e:
            terminal.info(f'Failed to activate via LVM: {e}')

        # then try simple
        try:
            SimpleActivate([]).activate(
                argparse.Namespace(
                    osd_id=self.args.osd_id,
                    osd_fsid=self.args.osd_fsid,
                    no_systemd=self.args.no_systemd,
                )
            )
            return
        except Exception as e:
            terminal.info(f'Failed to activate via simple: {e}')

        terminal.error('Failed to activate any OSD(s)')
