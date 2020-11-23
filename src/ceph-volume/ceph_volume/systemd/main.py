#!/usr/bin/python3
"""
This script is used by systemd to activate/deactivating a specific OSD by id
All it does is figure out the complete osd uuid and then runs the correct ceph-volume command
to mount the osd state directory
"""
import sys
import argparse
from ceph_volume import terminal, process
from ceph_volume.api import lvm

# NOTE: everything is just logged to stdout/stderr to let systemd take care of logging

def main():
    parser = argparse.ArgumentParser(prog='ceph-volume-systemd2')
    parser.add_argument(
        'action',
        default = 'activate',
        choices = [ 'activate', 'deactivate' ],
    )
    parser.add_argument(
        '--cluster',
        default = 'ceph',
        help = 'Cluster name (defaults to "ceph")',
    )
    parser.add_argument(
        '--osd-id',
        help = 'OSD ID',
        required = True,
    )
    args = parser.parse_args()

    lvs = lvm.get_lvs(tags={'ceph.osd_id': args.osd_id, 'ceph.cluster_name': args.cluster})
    if len(lvs) == 0:
        terminal.error('No LVS found for OSD ID {}'.format(args.osd_id))
        sys.exit(1)
    elif len(lvs) > 1:
        terminal.error('Multiple LVS found for OSD ID {}'.format(args.osd_id))
        sys.exit(1)

    osd_uuid = lvs[0].tags['ceph.osd_fsid']

    if args.action == 'activate':
        command = ['ceph-volume', '--cluster', args.cluster, 'lvm', 'activate', '--no-systemd', args.osd_id, osd_uuid]
        terminal.info("Activating OSD {} {}: {}".format(args.osd_id, osd_uuid, " ".join(command)))
        process.run(command, terminal_logging=True)
    elif args.action == 'deactivate':
        command = ['ceph-volume', '--cluster', args.cluster, 'lvm', 'deactivate', args.osd_id, osd_uuid]
        terminal.info("Deactivating OSD {} {}: {}".format(args.osd_id, osd_uuid, " ".join(command)))
        process.run(command, terminal_logging=True)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()

