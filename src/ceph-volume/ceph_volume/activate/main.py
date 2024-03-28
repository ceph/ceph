# -*- coding: utf-8 -*-

import argparse

from ceph_volume import terminal
from ceph_volume.objectstore.lvmbluestore import LvmBlueStore as LVMActivate
from ceph_volume.objectstore.rawbluestore import RawBlueStore as RAWActivate
from ceph_volume.devices.simple.activate import Activate as SimpleActivate


class Activate(object):

    help = "Activate an OSD"

    def __init__(self, argv):
        self.argv = argv

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
            help='OSD UUID to activate'
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

        # first try raw
        try:
            raw_activate = RAWActivate([])
            raw_activate.activate(None,
                                  self.args.osd_id,
                                  self.args.osd_uuid,
                                  not self.args.no_tmpfs)
            return
        except Exception as e:
            terminal.info(f'Failed to activate via raw: {e}')

        # then try lvm
        try:
            lvm_activate = LVMActivate(argparse.Namespace(
                no_tmpfs=self.args.no_tmpfs,
                no_systemd=self.args.no_systemd,
                osd_fsid=self.args.osd_uuid))
            lvm_activate.activate(None,
                                  self.args.osd_id,
                                  self.args.osd_uuid)
            return
        except Exception as e:
            terminal.info(f'Failed to activate via LVM: {e}')

        # then try simple
        try:
            SimpleActivate([]).activate(
                argparse.Namespace(
                    osd_id=self.args.osd_id,
                    osd_fsid=self.args.osd_uuid,
                    no_systemd=self.args.no_systemd,
                )
            )
            return
        except Exception as e:
            terminal.info(f'Failed to activate via simple: {e}')

        terminal.error('Failed to activate any OSD(s)')
