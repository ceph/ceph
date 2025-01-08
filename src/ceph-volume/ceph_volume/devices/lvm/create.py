from __future__ import print_function
from textwrap import dedent
import logging
import argparse
from ceph_volume.util import system
from ceph_volume.util.arg_validators import exclude_group_options
from ceph_volume import decorators, terminal, objectstore
from .common import create_parser, rollback_osd
from typing import List, Optional

logger = logging.getLogger(__name__)


class Create(object):

    help = 'Create a new OSD from an LVM device'

    def __init__(self, argv: Optional[List[str]] = None,
                 args: Optional[argparse.Namespace] = None) -> None:
        self.objectstore: Optional[objectstore.baseobjectstore.BaseObjectStore] = None
        self.argv = argv
        self.args = args

    @decorators.needs_root
    def create(self) -> None:
        if self.args is not None:
            if not self.args.osd_fsid:
                self.args.osd_fsid = system.generate_uuid()
            self.objectstore = objectstore.mapping['LVM'][self.args.objectstore](args=self.args)
            if self.objectstore is not None:
                self.objectstore.safe_prepare()
                osd_id = self.objectstore.osd_id
                try:
                    # we try this for activate only when 'creating' an OSD, because a rollback should not
                    # happen when doing normal activation. For example when starting an OSD, systemd will call
                    # activate, which would never need to be rolled back.
                    self.objectstore.activate()
                except Exception:
                    logger.exception('lvm activate was unable to complete, while creating the OSD')
                    logger.info('will rollback OSD ID creation')
                    rollback_osd(osd_id)
                    raise
                terminal.success("ceph-volume lvm create successful for: %s" % self.args.data)

    def main(self) -> None:
        sub_command_help = dedent("""
        Create an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, adding
        all the metadata to the logical volumes using LVM tags, and starting
        the OSD daemon. This is a convenience command that combines the prepare
        and activate steps.

        Encryption is supported via dmcrypt and the --dmcrypt flag.

        Existing logical volume (lv):

            ceph-volume lvm create --data {vg/lv}

        Existing block device (a logical volume will be created):

            ceph-volume lvm create --data /path/to/device

        Optionally, can consume db and wal block devices, partitions or logical
        volumes. A device will get a logical volume, partitions and existing
        logical volumes will be used as is:

            ceph-volume lvm create --data {vg/lv} --block.wal {partition} --block.db {/path/to/device}
        """)
        parser = create_parser(
            prog='ceph-volume lvm create',
            description=sub_command_help,
        )
        if self.argv is None:
            self.argv = []
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        exclude_group_options(parser, groups=['bluestore'], argv=self.argv)
        if self.args is None:
            self.args = parser.parse_args(self.argv)
        if self.args.bluestore:
            self.args.objectstore = 'bluestore'
        self.objectstore = objectstore.mapping['LVM'][self.args.objectstore]
        self.create()
