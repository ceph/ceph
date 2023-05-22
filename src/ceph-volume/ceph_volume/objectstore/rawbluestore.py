import logging
import json
import os
from .bluestore import BlueStore
from ceph_volume import terminal, decorators, conf, process
from ceph_volume.util import system, disk
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.devices.lvm.common import rollback_osd
from ceph_volume.devices.raw.list import direct_report
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


class RawBlueStore(BlueStore):
    def __init__(self, args: "argparse.Namespace") -> None:
        super().__init__(args)
        if hasattr(self.args, 'data'):
            self.block_device_path = self.args.data
        if hasattr(self.args, 'block_db'):
            self.db_device_path = self.args.block_db
        if hasattr(self.args, 'block_wal'):
            self.wal_device_path = self.args.block_wal

    def prepare_dmcrypt(self) -> None:
        """
        Helper for devices that are encrypted. The operations needed for
        block, db, wal, devices are all the same
        """
        key = self.secrets['dmcrypt_key']

        for device, device_type in [(self.block_device_path, 'block'),
                                    (self.db_device_path, 'db'),
                                    (self.wal_device_path, 'wal')]:

            if device:
                kname = disk.lsblk(device)['KNAME']
                mapping = 'ceph-{}-{}-{}-dmcrypt'.format(self.osd_fsid,
                                                         kname,
                                                         device_type)
                # format data device
                encryption_utils.luks_format(
                    key,
                    device
                )
                encryption_utils.luks_open(
                    key,
                    device,
                    mapping
                )
                self.__dict__[f'{device_type}_device_path'] = \
                    '/dev/mapper/{}'.format(mapping)

    def safe_prepare(self,
                     args: Optional["argparse.Namespace"] = None) -> None:
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback

        :param args: Injected args, usually from `raw create` which compounds
                     both `prepare` and `create`
        """
        if args is not None:
            self.args = args  # This should be moved (to __init__ ?)
        try:
            self.prepare()
        except Exception:
            logger.exception('raw prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        dmcrypt_log = 'dmcrypt' if hasattr(args, 'dmcrypt') else 'clear'
        terminal.success("ceph-volume raw {} prepare "
                         "successful for: {}".format(dmcrypt_log,
                                                     self.args.data))

    @decorators.needs_root
    def prepare(self) -> None:
        if self.encrypted:
            self.secrets['dmcrypt_key'] = \
                os.getenv('CEPH_VOLUME_DMCRYPT_SECRET')
        self.osd_fsid = system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class

        tmpfs = not self.args.no_tmpfs
        if self.args.block_wal:
            self.wal = self.args.block_wal
        if self.args.block_db:
            self.db = self.args.block_db

        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(
            self.osd_fsid, json.dumps(self.secrets))

        if self.secrets.get('dmcrypt_key'):
            self.prepare_dmcrypt()

        self.prepare_osd_req(tmpfs=tmpfs)

        # prepare the osd filesystem
        self.osd_mkfs()

    def _activate(self,
                  meta: Dict[str, Any],
                  tmpfs: bool) -> None:
        # find the osd
        osd_id = meta['osd_id']
        osd_uuid = meta['osd_uuid']

        # mount on tmpfs the osd directory
        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
        if not system.path_is_mounted(self.osd_path):
            # mkdir -p and mount as tmpfs
            prepare_utils.create_osd_path(osd_id, tmpfs=tmpfs)

        # XXX This needs to be removed once ceph-bluestore-tool can deal with
        # symlinks that exist in the osd dir

        self.unlink_bs_symlinks()

        # Once symlinks are removed, the osd dir can be 'primed again. chown
        # first, regardless of what currently exists so that ``prime-osd-dir``
        # can succeed even if permissions are somehow messed up
        system.chown(self.osd_path)
        prime_command = [
            'ceph-bluestore-tool',
            'prime-osd-dir',
            '--path', self.osd_path,
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

        system.chown(self.osd_path)
        terminal.success("ceph-volume raw activate "
                         "successful for osd ID: %s" % osd_id)

    @decorators.needs_root
    def activate(self,
                 devs: List[str],
                 start_osd_id: str,
                 start_osd_uuid: str,
                 tmpfs: bool) -> None:
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
            self._activate(meta,
                           tmpfs=tmpfs)
            activated_any = True

        if not activated_any:
            raise RuntimeError('did not find any matching OSD to activate')
