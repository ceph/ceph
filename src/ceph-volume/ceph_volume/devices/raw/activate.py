from __future__ import print_function
import argparse
import logging
import os
from textwrap import dedent
from ceph_volume import process, conf, decorators, terminal
from ceph_volume.util import system
from ceph_volume.util import prepare as prepare_utils
from .list import direct_report


logger = logging.getLogger(__name__)

def activate_bluestore(meta, tmpfs, systemd):
    # find the osd
    osd_id = meta['osd_id']
    osd_uuid = meta['osd_uuid']

    # mount on tmpfs the osd directory
    osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    if not system.path_is_mounted(osd_path):
        # mkdir -p and mount as tmpfs
        prepare_utils.create_osd_path(osd_id, tmpfs=tmpfs)

    # XXX This needs to be removed once ceph-bluestore-tool can deal with
    # symlinks that exist in the osd dir
    for link_name in ['block', 'block.db', 'block.wal']:
        link_path = os.path.join(osd_path, link_name)
        if os.path.exists(link_path):
            os.unlink(os.path.join(osd_path, link_name))

    # Once symlinks are removed, the osd dir can be 'primed again. chown first,
    # regardless of what currently exists so that ``prime-osd-dir`` can succeed
    # even if permissions are somehow messed up
    system.chown(osd_path)
    prime_command = [
        'ceph-bluestore-tool',
        'prime-osd-dir',
        '--path', osd_path,
        '--no-mon-config',
        '--dev', meta['device'],
    ]
    process.run(prime_command)

    # always re-do the symlink regardless if it exists, so that the block,
    # block.wal, and block.db devices that may have changed can be mapped
    # correctly every time
    prepare_utils.link_block(meta['device'], osd_id)

    if 'device_db' in meta:
        prepare_utils.link_db(meta['device_db'], osd_id, osd_uuid)

    if 'device_wal' in meta:
        prepare_utils.link_wal(meta['device_wal'], osd_id, osd_uuid)

    system.chown(osd_path)
    terminal.success("ceph-volume raw activate successful for osd ID: %s" % osd_id)


class Activate(object):

    help = 'Discover and prepare a data directory for a (BlueStore) OSD on a raw device'

    def __init__(self, argv):
        self.argv = argv
        self.args = None

    @decorators.needs_root
    def activate(self, devs, start_osd_id, start_osd_uuid,
                 tmpfs, systemd):
        """
        :param args: The parsed arguments coming from the CLI
        """
        assert devs or start_osd_id or start_osd_uuid
        found = direct_report(devs)

        activated_any = False
        for osd_uuid, meta in found.items():
            osd_id = meta['osd_id']
            if start_osd_id is not None and str(osd_id) != str(start_osd_id):
                continue
            if start_osd_uuid is not None and osd_uuid != start_osd_uuid:
                continue
            logger.info('Activating osd.%s uuid %s cluster %s' % (
                        osd_id, osd_uuid, meta['ceph_fsid']))
            activate_bluestore(meta,
                               tmpfs=tmpfs,
                               systemd=systemd)
            activated_any = True

        if not activated_any:
            raise RuntimeError('did not find any matching OSD to activate')

    def main(self):
        sub_command_help = dedent("""
        Activate (BlueStore) OSD on a raw block device(s) based on the
        device label (normally the first block of the device).

            ceph-volume raw activate [/dev/sdb2 ...]

        or

            ceph-volume raw activate --osd-id NUM --osd-uuid UUID

        The device(s) associated with the OSD need to have been prepared
        previously, so that all needed tags and metadata exist.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume raw activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            '--device',
            help='The device for the OSD to start'
        )
        parser.add_argument(
            '--osd-id',
            help='OSD ID to activate'
        )
        parser.add_argument(
            '--osd-uuid',
            help='OSD UUID to active'
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services'
        )
        parser.add_argument(
            '--block.db',
            dest='block_db',
            help='Path to bluestore block.db block device'
        )
        parser.add_argument(
            '--block.wal',
            dest='block_wal',
            help='Path to bluestore block.wal block device'
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            help='Do not use a tmpfs mount for OSD data dir'
            )

        if not self.argv:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.args = args
        if not args.no_systemd:
            terminal.error('systemd support not yet implemented')
            raise SystemExit(1)

        devs = [args.device]
        if args.block_wal:
            devs.append(args.block_wal)
        if args.block_db:
            devs.append(args.block_db)

        self.activate(devs=devs,
                      start_osd_id=args.osd_id,
                      start_osd_uuid=args.osd_uuid,
                      tmpfs=not args.no_tmpfs,
                      systemd=not self.args.no_systemd)
