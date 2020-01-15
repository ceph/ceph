from __future__ import print_function
import json
import logging
from textwrap import dedent
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system
from ceph_volume import conf, decorators, terminal
from ceph_volume.devices.lvm.common import rollback_osd
from .common import create_parser

logger = logging.getLogger(__name__)


def prepare_bluestore(block, secrets, osd_id, fsid, tmpfs):
    """
    :param block: The name of the logical volume for the bluestore data
    :param wal: a regular/plain disk or logical volume, to be used for block.wal
    :param db: a regular/plain disk or logical volume, to be used for block.db
    :param secrets: A dict with the secrets needed to create the osd (e.g. cephx)
    :param id_: The OSD id
    :param fsid: The OSD fsid, also known as the OSD UUID
    """
    cephx_secret = secrets.get('cephx_secret', prepare_utils.create_key())

    # create the directory
    prepare_utils.create_osd_path(osd_id, tmpfs=tmpfs)
    # symlink the block
    prepare_utils.link_block(block, osd_id)
    # get the latest monmap
    prepare_utils.get_monmap(osd_id)
    # write the OSD keyring if it doesn't exist already
    prepare_utils.write_keyring(osd_id, cephx_secret)
    # prepare the osd filesystem
    prepare_utils.osd_mkfs_bluestore(
        osd_id, fsid,
        keyring=cephx_secret,
    )


class Prepare(object):

    help = 'Format a raw device and associate it with a (BlueStore) OSD'

    def __init__(self, argv):
        self.argv = argv
        self.osd_id = None

    def safe_prepare(self, args=None):
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback

        :param args: Injected args, usually from `raw create` which compounds
                     both `prepare` and `create`
        """
        if args is not None:
            self.args = args
        try:
            self.prepare()
        except Exception:
            logger.exception('raw prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        terminal.success("ceph-volume raw prepare successful for: %s" % self.args.data)

    def get_cluster_fsid(self):
        """
        Allows using --cluster-fsid as an argument, but can fallback to reading
        from ceph.conf if that is unset (the default behavior).
        """
        if self.args.cluster_fsid:
            return self.args.cluster_fsid
        else:
            return conf.ceph.get('global', 'fsid')

    @decorators.needs_root
    def prepare(self):
        secrets = {'cephx_secret': prepare_utils.create_key()}
        osd_fsid = self.args.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            secrets['crush_device_class'] = crush_device_class
        tmpfs = not self.args.no_tmpfs
        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(
            osd_fsid, json.dumps(secrets), osd_id=self.args.osd_id)
        prepare_bluestore(
            self.args.data,
            secrets,
            self.osd_id,
            osd_fsid,
            tmpfs,
        )

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting the volume.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

        Encryption is not supported.

        DB and WAL devices are not supported.

            ceph-volume raw prepare --bluestore --data {device}

        """)
        parser = create_parser(
            prog='ceph-volume raw prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        if not self.args.bluestore:
            terminal.error('must specify --bluestore (currently the only supported backend)')
            raise SystemExit(1)

        self.safe_prepare(self.args)
