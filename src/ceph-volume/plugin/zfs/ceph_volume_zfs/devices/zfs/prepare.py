import argparse
from textwrap import dedent

from ceph_volume_zfs.objectstore import Zfs


class Prepare(object):

    help = 'Prepare a device'

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
        Prepare a device to be used as a ZFS-backed Ceph OSD: creates a
        zpool on the device, a zvol inside it to serve as the raw
        block device, then registers a new OSD ID/FSID with the
        cluster (via "ceph osd new") and runs "ceph-osd --mkfs".

        The target device must be empty (no partitions, not mounted,
        no active swap, not an imported zpool member) -- use
        "ceph-volume zfs zap" first if it isn't.

        -n/--dry-run: prints the full plan (osd id/fsid, pool/zvol
        names, every command that would run) without touching
        anything or contacting a mon. Safe to run repeatedly.

        --test: a ZFS-only run. Creates the real zpool and zvol on the
        target device and tags them with the ceph:* properties, but
        skips every step that needs a Ceph binary or a live mon
        ("ceph osd new", the monmap, the keyring, "ceph-osd --mkfs").
        Useful for exercising the ZFS side standalone. It writes real
        data to the device but does NOT produce a usable OSD.

        Examples:
          ceph-volume zfs prepare --data /dev/ada0 -n
          ceph-volume zfs prepare --data /dev/ada0 --test
          ceph-volume zfs prepare --data /dev/ada0 --osd-id 5 --osd-fsid <uuid>
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs prepare',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            '--data',
            required=True,
            help='Path to the device to prepare, e.g. /dev/ada0',
        )
        parser.add_argument(
            '--osd-id',
            default=None,
            help='Reuse an existing OSD ID (skips allocating a new one)',
        )
        parser.add_argument(
            '--osd-fsid',
            default=None,
            help='Specify an OSD FSID (UUID) instead of generating one',
        )
        parser.add_argument(
            '-n', '--dry-run',
            action='store_true',
            default=False,
            help='Print the plan and every command that would run -- nothing is executed, no mon contact.',
        )
        parser.add_argument(
            '--test',
            action='store_true',
            default=False,
            help=(
                'ZFS-only run: creates the real zpool/zvol on --data '
                'and tags them, but skips everything requiring Ceph '
                'binaries or a mon ("ceph osd new", monmap, keyring, '
                '"ceph-osd --mkfs"). Writes real data to the device, '
                'but does NOT produce a usable OSD.'
            ),
        )
        parser.add_argument(
            '--block.db',
            dest='block_db',
            default=None,
            help=(
                'Put bluestore\'s RocksDB metadata on a separate device: '
                'either a physical device (/dev/nda0, gets its own zpool), '
                'or the literal "same-pool" to carve it from the block '
                'pool (allowed, but gains nothing over bluestore\'s own '
                'single-device layout).'
            ),
        )
        parser.add_argument(
            '--block.wal',
            dest='block_wal',
            default=None,
            help=(
                'Put bluestore\'s write-ahead log on a separate device. '
                'Same forms as --block.db.'
            ),
        )
        parser.add_argument(
            '--block-db-size',
            default=None,
            help=(
                'Size of the db zvol (e.g. 50G). Required with '
                '"--block.db same-pool"; defaults to 95%% of a dedicated '
                'db device\'s pool otherwise.'
            ),
        )
        parser.add_argument(
            '--block-wal-size',
            default=None,
            help=(
                'Size of the wal zvol (e.g. 10G). Required with '
                '"--block.wal same-pool"; defaults to 95%% of a dedicated '
                'wal device\'s pool otherwise.'
            ),
        )
        parser.add_argument(
            '--thin',
            action='store_true',
            default=False,
            help=(
                'Create sparse (thin-provisioned) zvols. By default zvols '
                'are space-reserved, so the pool cannot be oversubscribed. '
                'Thin zvols let you overcommit, but a full pool then '
                'surfaces to bluestore as write errors.'
            ),
        )
        parser.add_argument(
            '--zvol-size',
            default=None,
            help=(
                'Explicit size for the zvol, in any form "zfs create -V" '
                'accepts (e.g. 100G, 1T). Defaults to 95%% of the pool\'s '
                'available space. Percentages are NOT accepted by zfs.'
            ),
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            default=False,
            help='Do not use tmpfs for the OSD data directory',
        )
        parser.add_argument(
            '--crush-device-class',
            default=None,
            help='Crush device class to assign to this OSD',
        )
        parser.add_argument(
            '--cluster-fsid',
            default=None,
            help='Specify the cluster FSID, overriding the value in ceph.conf',
        )
        parser.add_argument(
            '--dmcrypt',
            action='store_true',
            default=False,
            help='(not yet supported by the zfs backend)',
        )
        self.args = parser.parse_args(self.argv)

        objectstore = Zfs(args=self.args)
        objectstore.safe_prepare()

