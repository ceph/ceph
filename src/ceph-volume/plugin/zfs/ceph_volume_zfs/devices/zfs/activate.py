import argparse
from textwrap import dedent

from ceph_volume_zfs.objectstore import Zfs


class Activate(object):

    help = 'Activate a prepared ZFS-backed OSD'

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
        Rebuild a prepared OSD's runtime directory so ceph-osd can run
        against it.

        The OSD directory (/var/lib/ceph/osd/<cluster>-<id>) is tmpfs
        and does not survive a reboot. That is by design: the durable
        state lives on the block zvol, and
        "ceph-bluestore-tool prime-osd-dir" reads it back out. So this
        has to run once per boot for every OSD -- that is what
        --all is for.

        Discovery is entirely local: OSDs are found by scanning zpools
        for the ceph:* properties that "ceph-volume zfs prepare" set.
        No monitor contact is needed.

        NOTE: this does not start the daemon by default. There is no
        systemd on FreeBSD and no rc.d integration yet, so --start
        just execs ceph-osd unsupervised.

        Examples:
          ceph-volume zfs activate --osd-id 5
          ceph-volume zfs activate --osd-id 5 --osd-fsid <uuid>
          ceph-volume zfs activate --all
          ceph-volume zfs activate --all --start
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            '--osd-id',
            default=None,
            help='OSD id to activate',
        )
        parser.add_argument(
            '--osd-fsid',
            default=None,
            help='OSD fsid to activate',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            default=False,
            help='Activate every ZFS-backed OSD found on this host',
        )
        parser.add_argument(
            '--start',
            action='store_true',
            default=False,
            help=(
                'Also start ceph-osd. Unsupervised -- it will not restart '
                'on its own if it dies (no rc.d integration yet).'
            ),
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            default=False,
            help='Do not use tmpfs for the OSD data directory',
        )
        self.args = parser.parse_args(self.argv)

        if not self.args.all and not self.args.osd_id and not self.args.osd_fsid:
            parser.print_help()
            return

        objectstore = Zfs(args=self.args)
        if self.args.all:
            objectstore.activate_all()
        else:
            objectstore.activate()

