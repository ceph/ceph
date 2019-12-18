from __future__ import print_function
import argparse
import logging
import os
from textwrap import dedent
from ceph_volume import process, conf, decorators, terminal, __release__, configuration
from ceph_volume.util import system, disk
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.systemd import systemctl
from ceph_volume.api import lvm as api
from .list import direct_report


logger = logging.getLogger(__name__)

def activate_bluestore(meta, tmpfs, systemd):
    # find the osd
    osd_id = meta['osd_id']
    osd_fsid = meta['osd_uuid']

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
        'prime-osd-dir', '--dev', meta['device'],
        '--path', osd_path,
        '--no-mon-config']
    process.run(prime_command)

    # always re-do the symlink regardless if it exists, so that the block,
    # block.wal, and block.db devices that may have changed can be mapped
    # correctly every time
    process.run(['ln', '-snf', meta['device'], os.path.join(osd_path, 'block')])
    system.chown(os.path.join(osd_path, 'block'))
    system.chown(osd_path)

#    if systemd:
        # write me
        # enable the OSD
        #systemctl.enable_osd(osd_id)

        # start the OSD
        #systemctl.start_osd(osd_id)

    terminal.success("ceph-volume raw activate successful for osd ID: %s" % osd_id)


class Activate(object):

    help = 'Discover and prepare a data directory for a (BlueStore) OSD on a raw device'

    def __init__(self, argv):
        self.argv = argv
        self.args = None

    @decorators.needs_root
    def activate(self, devices, tmpfs, systemd,
                 only_osd_id=None,
                 only_osd_fsid=None):
        """
        :param args: The parsed arguments coming from the CLI
        """
        if devices:
            found = direct_report(devices)
        else:
            found = direct_report(None)

        for osd_id, meta in found.items():
            if only_osd_id and only_osd_fsid and \
               (only_osd_id != osd_id or only_osd_fsid != meta['osd_uuid']):
                logger.debug('ignoring osd.%d uuid %s cluster %s' % (
                    osd_id, meta['osd_uuid'], meta['ceph_fsid']))
                continue

            logger.info('Activating osd.%s uuid %s cluster %s' % (
                    osd_id, meta['osd_uuid'], meta['ceph_fsid']))
            activate_bluestore(meta,
                               tmpfs=tmpfs,
                               systemd=systemd)

    def main(self):
        sub_command_help = dedent("""
        Activate (BlueStore) OSD on a raw block device based on the 
        device label (normally the first block of the device).

            ceph-volume raw activate --device /dev/sdb
            ceph-volume raw activate --osd-id 1 --osd-fsid f0327efd-c28e-40bb-9199-f2e61e54c12a

        The device(s) associated with the OSD needs to have been prepared
        previously, so that all needed tags and metadata exist.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume raw activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            '--osd-id',
            help='The ID of the OSD, usually an integer, like 0'
        )
        parser.add_argument(
            '--osd-fsid',
            help='The UUID of the OSD'
        )
        parser.add_argument(
            '--device',
            nargs='*',
            help='The device(s) for the OSD to start')
        parser.add_argument(
            '--all',
            dest='activate_all',
            action='store_true',
            help='Activate all OSDs found in the system',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            help='Do not use a tmpfs mount for OSD data dir')
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.args = args
        if not args.device and not args.activate_all and not (args.osd_id and args.osd_fsid):
            terminal.error('must specify one of --device, --activate-all, or --osd-id and --osd-fsid')
            raise SystemExit(1)
        if not args.no_systemd:
            terminal.error('systemd support not yet implemented')
            raise SystemExit(1)
        self.activate(args.device,
                      tmpfs=not args.no_tmpfs,
                      systemd=not self.args.no_systemd,
                      only_osd_id=args.osd_id,
                      only_osd_fsid=args.osd_fsid)
