import logging

from ceph_volume import terminal, conf, decorators
from ceph_volume.util import system
from ceph_volume.util import prepare as prepare_utils
from .raw import Raw
from .seastore import SeastoreMixin

logger = logging.getLogger(__name__)


class SeastoreRaw(SeastoreMixin, Raw):
    """
    Raw-device seastore OSD.  Inherits the full prepare/activate machinery
    from Raw but overrides the parts that are BlueStore-specific via
    SeastoreMixin.
    """

    @decorators.needs_root
    def activate(self) -> None:
        """Activate a SeaStore OSD on raw devices.

        Unlike ``Raw.activate`` which discovers OSDs via
        ``ceph-bluestore-tool show-label``, SeaStore devices have a
        different on-disk format and cannot be discovered that way.
        Until ``crimson-objectstore-tool`` gains a ``dump-superblock``
        command (tracker #68106), the user must supply ``--device``,
        ``--osd-id``, and ``--osd-uuid`` explicitly on every activate.
        ``--all`` is not supported for SeaStore on the raw backend.

        Secondary device symlinks must also be re-supplied via
        ``--seastore-secondary`` on each activate because the secondary
        device paths are not persisted outside the primary device's
        superblock.
        """
        if not self.devices:
            raise RuntimeError(
                'ceph-volume raw activate --objectstore seastore requires '
                '--device (discovery via --all is not yet supported for '
                'SeaStore). See doc/ceph-volume/seastore.rst.'
            )
        if not self.osd_id:
            raise RuntimeError(
                'ceph-volume raw activate --objectstore seastore requires '
                '--osd-id. SeaStore OSDs cannot be discovered from the '
                'device label; the OSD ID must be supplied explicitly.'
            )
        if not self.osd_fsid:
            raise RuntimeError(
                'ceph-volume raw activate --objectstore seastore requires '
                '--osd-uuid. SeaStore OSDs cannot be discovered from the '
                'device label; the OSD FSID must be supplied explicitly.'
            )

        # ``Raw.__init__`` populates ``block_device_path`` from
        # ``args.data``, which doesn't exist on the raw activate command
        # line.  Use the first entry in ``self.devices`` (which folds in
        # both ``--device`` and ``--devices`` from the parser).
        self.block_device_path = self.devices[0]
        logger.info(
            f'Activating osd.{self.osd_id} uuid {self.osd_fsid} '
            f'on {self.block_device_path}'
        )
        self._activate(self.osd_id, self.osd_fsid)

    def _activate(self, osd_id: str, osd_fsid: str) -> None:
        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
        if not system.path_is_mounted(self.osd_path):
            prepare_utils.create_osd_path(osd_id, tmpfs=not self.args.no_tmpfs)

        self.unlink_bs_symlinks()
        prepare_utils.link_block(self.block_device_path, osd_id)

        for i, (device, dtype) in enumerate(
                getattr(self.args, 'seastore_secondary', [])):
            prepare_utils.link_seastore_secondary(device, dtype, i + 1, osd_id)

        system.chown(self.osd_path)
        terminal.success("ceph-volume raw activate successful for osd ID: %s" % osd_id)
