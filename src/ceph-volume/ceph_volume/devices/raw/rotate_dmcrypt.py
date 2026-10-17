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
    `ceph-volume raw prepare --dmcrypt`, in place and while the OSD is
    running. All LUKS devices of the OSD (block/db/wal) are rotated as a
    set.

    `raw prepare --dmcrypt` takes the passphrase from the environment, so
    the caller owns it (e.g. Rook, in a Kubernetes Secret or a KMS) and
    supplies it here the same way. ceph-volume performs only the LUKS
    keyslot operations and never writes to the monitor config-key store.

    The rotation runs as two invocations, so that the caller can store the
    new passphrase while both passphrases still open the devices. Which one
    this is follows from the environment: setting
    CEPH_VOLUME_NEW_DMCRYPT_SECRET installs that passphrase and keeps the
    current one valid; leaving it unset removes the previous one.

        export CEPH_VOLUME_DMCRYPT_SECRET=<passphrase in your store>
        export CEPH_VOLUME_NEW_DMCRYPT_SECRET=<new passphrase>
        ceph-volume raw rotate-dmcrypt-key --osd-fsid aaaa-bbb-ccc

        <store the new passphrase in your key store and verify it>

        export CEPH_VOLUME_DMCRYPT_SECRET=<new passphrase>
        unset CEPH_VOLUME_NEW_DMCRYPT_SECRET
        ceph-volume raw rotate-dmcrypt-key --osd-fsid aaaa-bbb-ccc

    CEPH_VOLUME_DMCRYPT_SECRET is always the passphrase your key store holds
    right now. A crash between the two invocations is safe: both passphrases
    open every device until the second one completes.

    If the lockbox cephx secret was also rotated (ceph auth rotate
    client.osd-lockbox.{fsid}), pass the new secret via
    CEPH_VOLUME_CEPHX_LOCKBOX_SECRET in the first invocation so the lockbox
    keyring on the OSD data directory is updated as well.

    For OSDs deployed with `lvm prepare --dmcrypt`, whose passphrase lives
    on the monitors, see `ceph-volume lvm rotate-dmcrypt-key`.
    """)

    def __init__(self, argv: Optional[List[str]] = None) -> None:
        self.argv = argv

    @decorators.needs_root
    def rotate(self) -> None:
        dmcrypt_rotation.rotate_from_args(self.args, 'raw')

    def main(self) -> None:
        parser = dmcrypt_rotation.make_parser(
            'ceph-volume raw rotate-dmcrypt-key',
            self.sub_command_help,
            'raw',
        )
        if self.argv is None:
            self.argv = []
        if len(self.argv) == 0:
            print(self.sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        self.rotate()
