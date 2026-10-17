import logging
from textwrap import dedent
from ceph_volume import decorators
from ceph_volume.util import dmcrypt_rotation
from typing import List, Optional

logger = logging.getLogger(__name__)


class RotateDmcryptKey(object):

    help = "Rotate the dmcrypt (LUKS) passphrase of an encrypted OSD"

    sub_command_help = dedent("""
    Rotate the dmcrypt (LUKS) passphrase of an OSD deployed with
    `ceph-volume lvm prepare --dmcrypt`, in place and while the OSD is
    running. All LUKS devices of the OSD (block/db/wal) are rotated as a
    set.

    `lvm prepare` generates the passphrase itself and stores it on the
    monitors at dm-crypt/osd/{fsid}/luks, so one invocation performs the
    whole rotation: a new passphrase is generated, installed and stored
    back. Storing it requires credentials with "config-key set" caps on that
    key, which the OSD's lockbox entity does not have, so pass privileged
    ones:

        ceph-volume lvm rotate-dmcrypt-key --osd-id 0 \\
            --name client.admin --keyring /etc/ceph/ceph.client.admin.keyring

    If the lockbox cephx secret was also rotated (ceph auth rotate
    client.osd-lockbox.{fsid}), pass the new secret via
    CEPH_VOLUME_CEPHX_LOCKBOX_SECRET so the LV tags and the lockbox
    keyring are updated as well:

        export CEPH_VOLUME_CEPHX_LOCKBOX_SECRET=<new lockbox secret>
        ceph-volume lvm rotate-dmcrypt-key --osd-id 0 \\
            --name client.admin --keyring /etc/ceph/ceph.client.admin.keyring

    The OSD is named with either --osd-id or --osd-fsid. For OSDs whose
    passphrase is owned by an external system, see
    `ceph-volume raw rotate-dmcrypt-key`.
    """)

    def __init__(self, argv: Optional[List[str]] = None) -> None:
        self.argv = argv

    @decorators.needs_root
    def rotate(self) -> None:
        dmcrypt_rotation.rotate_from_args(self.args, 'lvm')

    def main(self) -> None:
        parser = dmcrypt_rotation.make_parser(
            'ceph-volume lvm rotate-dmcrypt-key',
            self.sub_command_help,
            'lvm',
        )
        if self.argv is None:
            self.argv = []
        if len(self.argv) == 0:
            print(self.sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        self.rotate()
