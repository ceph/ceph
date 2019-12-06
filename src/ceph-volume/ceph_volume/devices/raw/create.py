from __future__ import print_function
from textwrap import dedent
import logging
from ceph_volume.util import system
from ceph_volume.util.arg_validators import exclude_group_options
from ceph_volume import decorators, terminal
from .prepare import Prepare
from .activate import Activate
from .common import create_parser
from ceph_volume.devices.lvm.common import rollback_osd

logger = logging.getLogger(__name__)


class Create(object):

    help = 'Create a new (BlueStore) OSD on a raw device'

    def __init__(self, argv):
        self.argv = argv
        self.args = None

    @decorators.needs_root
    def create(self, args):
        if not args.osd_fsid:
            args.osd_fsid = system.generate_uuid()
        prepare_step = Prepare([])
        prepare_step.safe_prepare(args)
        osd_id = prepare_step.osd_id
        try:
            # we try this for activate only when 'creating' an OSD,
            # because a rollback should not happen when doing normal
            # activation. For example when starting an OSD, systemd
            # will call activate, which would never need to be rolled
            # back.
            a = Activate([])
            a.args = self.args
            a.activate([args.data])
        except Exception:
            logger.exception('raw activate was unable to complete, while creating the OSD')
            logger.info('will rollback OSD ID creation')
            rollback_osd(args, osd_id)
            raise
        terminal.success("ceph-volume raw create successful for: %s" % args.data)

    def main(self):
        sub_command_help = dedent("""
        Create an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, and
        starting the OSD daemon. This is a convinience command that combines
        the prepare and activate steps.

        Encryption is not supported.

        Separate DB and WAL devices are not supported.

            ceph-volume raw create --data /dev/sdb

        """)
        parser = create_parser(
            prog='ceph-volume raw create',
            description=sub_command_help,
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        if not self.args.bluestore:
            terminal.error('must specify --bluestore (currently the only supported backend)')
            raise SystemExit(1)
        if not args.no_systemd:
            terminal.error('systemd support not yet implemented')
            raise SystemExit(1)
        self.create(self.args)
