import logging
import os
from .baseobjectstore import BaseObjectStore
from ceph_volume.util import system
from ceph_volume.util.encryption import CephLuks2
from ceph_volume import process
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse
    from ceph_volume.api.lvm import Volume

logger = logging.getLogger(__name__)


class BlueStore(BaseObjectStore):
    def __init__(self, args: "argparse.Namespace") -> None:
        super().__init__(args)
        self.args: "argparse.Namespace" = args
        self.objectstore = 'bluestore'
        self.osd_id: str = ''
        self.osd_fsid: str = ''
        self.osd_path: str = ''
        self.key: Optional[str] = None
        self.block_device_path: str = ''
        self.wal_device_path: str = ''
        self.db_device_path: str = ''
        self.block_lv: Volume

    def add_objectstore_opts(self) -> None:
        """
        Create the files for the OSD to function. A normal call will look like:

            ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                    --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                    --osd-data /var/lib/ceph/osd/ceph-0 \
                    --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                    --keyring /var/lib/ceph/osd/ceph-0/keyring \
                    --setuser ceph --setgroup ceph

        In some cases it is required to use the keyring, when it is passed
        in as a keyword argument it is used as part of the ceph-osd command
        """

        if self.wal_device_path:
            self.osd_mkfs_cmd.extend(
                ['--bluestore-block-wal-path', self.wal_device_path]
            )
            system.chown(self.wal_device_path)

        if self.db_device_path:
            self.osd_mkfs_cmd.extend(
                ['--bluestore-block-db-path', self.db_device_path]
            )
            system.chown(self.db_device_path)

        if self.get_osdspec_affinity():
            self.osd_mkfs_cmd.extend(['--osdspec-affinity',
                                      self.get_osdspec_affinity()])

    def unlink_bs_symlinks(self) -> None:
        for link_name in ['block', 'block.db', 'block.wal']:
            link_path = os.path.join(self.osd_path, link_name)
            if os.path.exists(link_path):
                os.unlink(os.path.join(self.osd_path, link_name))


    def add_label(self, key: str,
                  value: str,
                  device: str) -> None:
        """Add a label to a BlueStore device.
        Args:
            key (str): The name of the label being added.
            value (str): Value of the label being added.
            device (str): The path of the BlueStore device.
        Raises:
            RuntimeError: If `ceph-bluestore-tool` command doesn't success.
        """

        command: List[str] = ['ceph-bluestore-tool',
                              'set-label-key',
                              '-k',
                              key,
                              '-v',
                              value,
                              '--dev',
                              device]

        _, err, rc = process.call(command,
                                  terminal_verbose=True,
                                  show_command=True)
        if rc:
            raise RuntimeError(f"Can't add BlueStore label '{key}' to device {device}: {err}")

    def osd_mkfs(self) -> None:
        super().osd_mkfs()
        mapping: Dict[str, Any] = {'raw': ['data', 'block_db', 'block_wal'],
                                   'lvm': ['ceph.block_device', 'ceph.db_device', 'ceph.wal_device']}
        if self.args.dmcrypt:
            for dev_type in mapping[self.method]:
                if self.method == 'raw':
                    path = self.args.__dict__.get(dev_type, None)
                else:
                    path = self.block_lv.tags.get(dev_type, None)
                if path is not None:
                    CephLuks2(path).config_luks2({'subsystem': f'ceph_fsid={self.osd_fsid}'})
