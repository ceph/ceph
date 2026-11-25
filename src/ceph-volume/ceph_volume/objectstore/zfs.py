import json
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ceph_volume import conf, terminal, decorators, process
from ceph_volume.objectstore.baseobjectstore import BaseObjectStore
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system

from ceph_volume.util.zfs.gpart import get_gpart_info
from ceph_volume.util.zfs.mount import get_mount_info, get_swap_info
from ceph_volume.util.zfs import zvol as zfs_zvol
from ceph_volume.util.zfs import zpool as zfs_zpool
from ceph_volume.util.zfs.zpool import get_zpool_membership, list_ceph_zpools

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


class Zfs(BaseObjectStore):
    """
    ZFS-backed objectstore: creates a new zpool on the target whole
    disk, then a zvol inside that pool to serve as the raw block
    device bluestore writes to. This mirrors the LVM backend's
    VG-then-LV split (zpool ~ VG, zvol ~ LV) rather than the Raw
    backend's direct-partition approach, since a bare disk handed to
    `prepare` is expected to become ZFS-managed storage.
    """

    def __init__(self, args: "argparse.Namespace") -> None:
        self._dry_run = getattr(args, 'dry_run', False)
        # activate never needs a cephx secret -- the OSD already has
        # its keyring on disk -- so it takes the same lightweight
        # path, avoiding a pointless ceph-authtool dependency.
        self._activating = getattr(args, 'all', False) or (
            not hasattr(args, 'data') and (
                getattr(args, 'osd_id', None) or getattr(args, 'osd_fsid', None)))
        if self._dry_run or self._activating:
            # Neither a dry run nor activate should depend on the
            # real Ceph binaries being installed -- but
            # BaseObjectStore.__init__ eagerly runs "ceph-authtool
            # --gen-print-key" for a cephx secret that neither of them
            # needs, so skip the base class entirely.
            #
            # FRAGILE: this duplicates BaseObjectStore's attribute
            # set by hand. If upstream adds new attributes other code
            # paths rely on, this will need updating to match.
            self.args = args
            self.secrets = {}
            self.cephx_secret = ''
            self.encrypted = 0
            self.tags = {}
            self.osd_id = getattr(args, 'osd_id', '') or ''
            self.osd_fsid = getattr(args, 'osd_fsid', '') or ''
            self.cephx_lockbox_secret = ''
            self.osd_mkfs_cmd = []
            self.osd_type = getattr(args, 'osd_type', '')
            self.block_device_path = ''
            self.dmcrypt_key = ''
            self.with_tpm = int(getattr(args, 'with_tpm', False))
            self.tpm2_pcrs = getattr(args, 'tpm2_pcrs', '7')
            self.osd_path = ''
            self.key = None
            self.wal_device_path = ''
            self.db_device_path = ''
            self.block_lv = None
            self.skip_mkfs_discard = False
        else:
            super().__init__(args)
            self.osd_id = self.args.osd_id or ''
            self.osd_fsid = self.args.osd_fsid or ''

        self.method = 'zfs'
        # BaseObjectStore.__init__ defaults self.objectstore from
        # getattr(args, 'objectstore', '') -- this backend only
        # supports bluestore for now, so force it regardless of what
        # (if anything) was passed in.
        self.objectstore = 'bluestore'
        self.pool_name = ''
        self.zvol_name = ''
        self._zvol_path_name = ''
        self._data_device_commands: List[List[str]] = []
        # db/wal datasets created this run, for cross-linking properties
        self._metadata_datasets: Dict[str, str] = {}

    def _diskname(self) -> str:
        return self.args.data.replace('/dev/', '')

    def precondition_all_devices(self) -> None:
        """
        Validates EVERY device this run will touch -- the data device
        plus any dedicated --block.db / --block.wal devices -- before
        creating anything. Checking upfront matters: if a db device
        turned out to be unusable only when we got to it, the block
        pool would already exist and we'd leave a half-built OSD
        behind.
        """
        devices = [self.args.data]
        for device_type in ('db', 'wal'):
            device = getattr(self.args, 'block_{}'.format(device_type), None)
            # 'same-pool' isn't a device -- it reuses the block pool,
            # which is validated via self.args.data above.
            if device and device != 'same-pool':
                devices.append(device)
        for device in devices:
            self.precondition_device(device)

    def precondition_device(self, device: Optional[str] = None) -> None:
        """
        Refuse to touch a device that fails the same safety checks
        `zap` uses -- mounted, active swap, or already an imported
        zpool member/partitioned. prepare should never silently
        destroy live data; if the disk isn't pristine, the person
        needs to zap it first, deliberately.
        """
        if device is None:
            device = self.args.data
        diskname = device.replace('/dev/', '')
        gpart = get_gpart_info(diskname)
        zpool = get_zpool_membership(diskname)
        mount = get_mount_info(diskname, partitions=gpart.get('partitions'))
        swap = get_swap_info(diskname, partitions=gpart.get('partitions'))

        if mount.get('mounted'):
            raise RuntimeError(
                'Refusing to prepare /dev/{}: has mounted filesystem(s) at {}'.format(
                    diskname, ', '.join(mount.get('mountpoints', []))
                )
            )
        if swap.get('active'):
            raise RuntimeError(
                'Refusing to prepare /dev/{}: has active swap on {}'.format(
                    diskname, ', '.join(swap.get('devices', []))
                )
            )
        if zpool.get('in_pool'):
            raise RuntimeError(
                'Refusing to prepare /dev/{}: already a member of imported '
                'zpool "{}". Zap it first.'.format(diskname, zpool.get('pool_name'))
            )
        if not gpart.get('empty'):
            raise RuntimeError(
                'Refusing to prepare /dev/{}: has an existing partition '
                'table. Zap it first.'.format(diskname)
            )

    def plan_data_device(self, osd_uuid: str) -> None:
        """
        Computes (but does not execute) the pool/zvol names and the
        zpool/zfs commands that would create them. Populates
        self.pool_name and self.block_device_path so both the
        dry-run printer and the real executor use the exact same
        plan -- no drift between what's shown and what's run.
        """
        diskname = self._diskname()
        if not self.pool_name:
            # 'ceph-osd-<id>' rather than upstream's 'ceph-<uuid4>' VG
            # convention: our design is strictly one pool per OSD, so
            # the OSD id is both unique and far more readable at the
            # CLI than a full UUID (which also appears in the zvol
            # name below, making the combined path unwieldy).
            #
            # Note this means the pool name is only unique per OSD id
            # -- preparing a new OSD that reuses a destroyed OSD's id
            # would collide with a leftover pool of the same name.
            # zpool create will refuse rather than clobber it, which
            # is the safe failure.
            self.pool_name = 'ceph-osd-{}'.format(self.osd_id)
        # Matches the naming convention ceph_volume.api.lvm.create_lv()
        # uses for LVs: '{name_prefix}-{uuid}', with lvm.py passing
        # 'osd-{device_type}' as that prefix -- i.e. a real LV ends up
        # named 'osd-block-<osd_fsid>'. Same pattern here for the zvol.
        self.zvol_name = 'osd-block-{}'.format(osd_uuid)
        zvol_path_name = '{}/{}'.format(self.pool_name, self.zvol_name)
        self.block_device_path = '/dev/zvol/{}'.format(zvol_path_name)
        self._zvol_path_name = zvol_path_name
        # NOTE: only the zpool command is fully resolved here. The
        # zvol's -V size can't be known until the pool actually
        # exists (it's derived from the pool's reported available
        # bytes), so the real command is built at execution time in
        # prepare_data_device(); this placeholder is for display in
        # the dry-run plan only.
        self._data_device_commands = [
            # -m none: the pool's root dataset is never used (only the
            # zvol below is), so don't give it a mountpoint at
            # /<poolname> cluttering the root filesystem.
            ['/sbin/zpool', 'create', '-m', 'none',
             self.pool_name, '/dev/{}'.format(diskname)],
            ['/sbin/zfs', 'create', '-V', '<remaining pool space, computed after creation>',
             zvol_path_name],
        ]

    # BaseObjectStore types this as returning an LVM Volume; the zfs
    # backend has no Volume object and returns the zvol device path.
    def prepare_data_device(self, device_type: str, osd_uuid: str) -> str:  # type: ignore[override]
        """
        Executes the plan built by plan_data_device(): creates the
        zpool on the target whole device, then a zvol inside it to
        serve as the raw block device. Returns the device path
        bluestore should be pointed at.

        Ordering matters: any same-pool db/wal zvols are created
        BEFORE the block zvol, so the block zvol can be sized against
        what's genuinely left rather than oversubscribing the pool.

        ASSUMPTION (unverified on this system): FreeBSD exposes zvols
        at /dev/zvol/<pool>/<name>. Confirm with `zfs list -t volume`
        and `ls /dev/zvol/` after this runs, and correct this if the
        actual path differs.
        """
        self.plan_data_device(osd_uuid)
        logger.debug('Creating zpool {}'.format(self.pool_name))
        process.run(self._data_device_commands[0])
        # db/wal first -- see docstring.
        self.setup_metadata_devices(osd_uuid)
        # Size is resolved only now: the pool must exist to report its
        # available space, and any same-pool db/wal zvols must already
        # have taken their reservation out of it.
        size = self._zvol_size()
        zvol_cmd = self._zvol_create_cmd(size, self._zvol_path_name)
        self._data_device_commands[1] = zvol_cmd
        logger.debug('Creating zvol {} ({} bytes)'.format(self.block_device_path, size))
        process.run(zvol_cmd)
        zfs_zvol.tag_zvol(
            dataset='{}/{}'.format(self.pool_name, self.zvol_name),
            osd_id=self.osd_id, osd_fsid=self.osd_fsid,
            cluster_name=conf.cluster or 'ceph', cluster_fsid=self._cluster_fsid(),
            objectstore=self.objectstore,
            crush_device_class=self.args.crush_device_class or '',
            metadata_devices=self._metadata_datasets,
            block_zvol_path=None)
        zfs_zpool.tag_pool(
            pool_name=self.pool_name,
            osd_id=self.osd_id, osd_fsid=self.osd_fsid,
            zvol_name=self.zvol_name)
        return self.block_device_path

    def _zvol_create_cmd(self, size: str, dataset: str) -> List[str]:
        """Delegates to :func:`zfs_zvol.create_cmd`."""
        return zfs_zvol.create_cmd(size, dataset, thin=self.args.thin)

    def setup_metadata_devices(self, osd_uuid: str) -> None:
        """
        Creates the optional block.db / block.wal devices as zvols,
        mirroring Lvm.setup_metadata_devices()'s role for LVs.

        Two forms are accepted for --block.db / --block.wal:

          * a physical device (/dev/nda0): a dedicated zpool is
            created on it, and the db/wal zvol lives there. This is
            the setup that actually makes sense -- fast metadata
            device separate from bulk data.

          * the literal string 'same-pool': the zvol is carved out of
            the SAME pool as the block device. This is supported
            because you asked for it, but be clear-eyed that it buys
            nothing: db/wal on the same physical spindles as block is
            what bluestore already does internally when given a single
            device, only with more moving parts. The sizes then also
            compete with the block zvol for the pool's space.

        Sizes come from --block-db-size / --block-wal-size; they are
        required for the same-pool form (there's no sensible default
        when sharing) and optional for a dedicated device (defaults to
        95% of that pool).
        """
        for device_type in ('db', 'wal'):
            device = getattr(self.args, 'block_{}'.format(device_type), None)
            if not device:
                continue
            size = getattr(self.args, 'block_{}_size'.format(device_type), None)

            if device == 'same-pool':
                if not size:
                    raise RuntimeError(
                        '--block.{} same-pool requires --block-{}-size '
                        '(no default when sharing the block pool)'.format(
                            device_type, device_type)
                    )
                pool = self.pool_name
                dataset = '{}/osd-{}-{}'.format(pool, device_type, osd_uuid)
                logger.debug('Creating {} zvol {} in the block pool'.format(device_type, dataset))
                process.run(self._zvol_create_cmd(size, dataset))
            else:
                diskname = device.replace('/dev/', '')
                pool = '{}-{}'.format(self.pool_name, device_type)
                logger.debug('Creating {} zpool {} on /dev/{}'.format(device_type, pool, diskname))
                process.run(['/sbin/zpool', 'create', '-m', 'none',
                             pool, '/dev/{}'.format(diskname)])
                dataset = '{}/osd-{}-{}'.format(pool, device_type, osd_uuid)
                if not size:
                    size = zfs_zpool.pool_fraction_size(pool)
                logger.debug('Creating {} zvol {} ({})'.format(device_type, dataset, size))
                process.run(self._zvol_create_cmd(size, dataset))
                zfs_zpool.tag_pool(
                    pool_name=pool,
                    osd_id=self.osd_id, osd_fsid=self.osd_fsid,
                    device_type=device_type)

            self._metadata_datasets[device_type] = dataset
            zfs_zvol.tag_zvol(
                dataset=dataset,
                osd_id=self.osd_id, osd_fsid=self.osd_fsid,
                cluster_name=conf.cluster or 'ceph', cluster_fsid=self._cluster_fsid(),
                objectstore=self.objectstore,
                crush_device_class=self.args.crush_device_class or '',
                device_type=device_type,
                block_zvol_path='/dev/zvol/{}/{}'.format(
                    self.pool_name, self.zvol_name))
            setattr(self, '{}_device_path'.format(device_type),
                    '/dev/zvol/{}'.format(dataset))

    def _cluster_fsid(self) -> str:
        """
        The cluster FSID, which unlike the cluster *name* is actually
        unique (two clusters can both be called "ceph"). Falls back to
        empty rather than raising, so a run that does not need it
        (e.g. --dry-run without a loaded ceph.conf) is not aborted.
        """
        try:
            return self.get_cluster_fsid() or ''
        except Exception:
            logger.debug('could not determine cluster fsid, leaving it unset')
            return ''

    def _zvol_size(self) -> str:
        """
        Returns an absolute size string for ``zfs create -V``.
        Uses an explicit size when given, otherwise takes 95% of
        the pool's available space via
        :func:`zfs_zpool.pool_fraction_size`.

        Must be called AFTER the zpool exists.
        """
        if self.args.zvol_size:
            return self.args.zvol_size
        return zfs_zpool.pool_fraction_size(self.pool_name)

    @decorators.needs_root
    def activate(self, osd_id: Optional[str] = None,
                 osd_fsid: Optional[str] = None) -> None:
        """
        Rebuilds a prepared OSD's runtime directory so ceph-osd can
        start against it. Safe to re-run: it is exactly what has to
        happen after every reboot, since the OSD directory is tmpfs
        and does not survive one.

        The durable state lives on the block zvol itself --
        `ceph-bluestore-tool prime-osd-dir` reads bluestore's own
        labels back out of it and repopulates the directory. That is
        why losing the tmpfs is not a problem.

        Starting the daemon is deliberately NOT done here: upstream
        uses systemd for that, which does not exist on FreeBSD. Use
        --start once an rc.d integration exists, or start ceph-osd
        yourself.
        """
        osd_id = osd_id if osd_id is not None else self.args.osd_id
        osd_fsid = osd_fsid if osd_fsid is not None else self.args.osd_fsid
        if osd_id is None and osd_fsid is None:
            raise RuntimeError('activate requires --osd-id and/or --osd-fsid')

        osd = zfs_zpool.find_osd(osd_id=osd_id, osd_fsid=osd_fsid)
        if not osd:
            raise RuntimeError(
                'could not find a ZFS-backed OSD matching osd_id={} osd_fsid={}'.format(
                    osd_id, osd_fsid)
            )

        self.osd_id = osd['osd_id']
        self.osd_fsid = osd['osd_fsid']
        conf.cluster = osd['cluster_name']
        self.block_device_path = osd['block']
        self.db_device_path = osd['db'] or ''
        self.wal_device_path = osd['wal'] or ''

        self.osd_path = '/var/lib/ceph/osd/{}-{}'.format(conf.cluster, self.osd_id)
        if not system.path_is_mounted(self.osd_path):
            prepare_utils.create_osd_path(
                self.osd_id, tmpfs=not self.args.no_tmpfs)

        # ceph-bluestore-tool cannot deal with pre-existing symlinks
        # in the osd dir, so clear them before priming.
        self.unlink_bs_symlinks()

        system.chown(self.osd_path)
        process.run([
            'ceph-bluestore-tool',
            '--cluster={}'.format(conf.cluster),
            'prime-osd-dir',
            '--dev', self.block_device_path,
            '--path', self.osd_path,
            '--no-mon-config',
        ])

        prepare_utils.link_block(self.block_device_path, self.osd_id)
        if self.db_device_path:
            prepare_utils.link_db(self.db_device_path, self.osd_id, self.osd_fsid)
        if self.wal_device_path:
            prepare_utils.link_wal(self.wal_device_path, self.osd_id, self.osd_fsid)
        system.chown(self.osd_path)

        terminal.success(
            'ceph-volume zfs activate successful for osd ID: {}'.format(self.osd_id))
        if self.args.start:
            self.start_osd()
        else:
            terminal.info(
                'not starting the daemon; run: ceph-osd -i {} --setuser ceph '
                '--setgroup ceph'.format(self.osd_id))

    def start_osd(self) -> None:
        """
        Start the OSD daemon via the rc.d script.  Uses 'onestart'
        so it works whether or not ceph_osd is enabled in rc.conf.
        The rc.d script handles daemon(1), pid files, limits and
        aio_unsafe.
        """
        process.run([
            'service', 'ceph_osd', 'onestart', str(self.osd_id),
        ])
        terminal.info(
            'To start osd.{id} automatically at boot, run:\n'
            '  sysrc ceph_osd_enable=YES\n'
            '  sysrc ceph_osd_list+="{id}"'.format(id=self.osd_id))

    @decorators.needs_root
    def activate_all(self) -> None:
        """
        Activates every ZFS-backed OSD found on this host. This is
        what a boot-time hook would call.
        """
        entries = list_ceph_zpools()
        osd_ids: List[str] = []
        for entry in entries:
            osd_id = entry['properties'].get('osd_id')
            if osd_id and osd_id not in osd_ids:
                osd_ids.append(osd_id)
        if not osd_ids:
            terminal.warning('no ZFS-backed Ceph OSDs found to activate')
            return
        for osd_id in osd_ids:
            terminal.info('activating osd.{}'.format(osd_id))
            try:
                self.activate(osd_id=osd_id)
            except Exception as error:
                # one bad OSD should not stop the rest from coming up
                terminal.error('failed to activate osd.{}: {}'.format(osd_id, error))

    def prepare_dmcrypt(self) -> None:
        raise NotImplementedError('dmcrypt is not yet supported by the zfs backend')

    def _rollback_prepare(self) -> None:
        """
        Best-effort cleanup after a failed prepare: purge the OSD ID
        from the monitor, destroy any zpools, and labelclear the
        devices so they are returned completely clean.
        """
        try:
            if self.osd_id:
                terminal.error('Was unable to complete a new OSD, will rollback changes')
                bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/{}.keyring'.format(conf.cluster)
                cmd = [
                    'ceph',
                    '--cluster', conf.cluster,
                    '--name', 'client.bootstrap-osd',
                    '--keyring', bootstrap_keyring,
                    'osd', 'purge-new', 'osd.{}'.format(self.osd_id),
                    '--yes-i-really-mean-it',
                ]
                process.run(cmd)
        except Exception:
            logger.warning('failed to purge OSD %s during rollback', self.osd_id)

        # Destroy zpools that may have been created.  Metadata pools
        # (db/wal on a dedicated device) are named <pool>-db / <pool>-wal.
        if self.pool_name:
            for suffix in ['-db', '-wal', '']:
                pool = self.pool_name + suffix
                out, err, rc = process.call(
                    ['/sbin/zpool', 'destroy', '-f', pool])
                if rc == 0:
                    terminal.warning('rollback: destroyed zpool {}'.format(pool))

        # Labelclear whole devices so they come back completely clean.
        devices = set()  # type: ignore[var-annotated]
        data_dev = getattr(self.args, 'data', None)
        if data_dev:
            devices.add(data_dev)
        for dt in ('db', 'wal'):
            dev = getattr(self.args, 'block_{}'.format(dt), None)
            if dev and dev != 'same-pool':
                devices.add(dev)
        for dev in devices:
            process.call(['/sbin/zpool', 'labelclear', '-f', '/dev/{}'.format(dev)])

    def safe_prepare(self, args: Optional["argparse.Namespace"] = None) -> None:
        """
        An intermediate step between `main()` and `prepare()` so we
        can capture self.osd_id in case we need to roll back on
        failure.
        """
        if args is not None:
            self.args = args
        try:
            self.prepare()
        except Exception:
            logger.exception('zfs prepare was unable to complete')
            terminal.error('zfs prepare failed for {}'.format(self.args.data))
            self._rollback_prepare()
            raise
        if getattr(self.args, 'dry_run', False):
            return
        terminal.success(
            'ceph-volume zfs prepare successful for: {} (osd.{})'.format(
                self.args.data, self.osd_id
            )
        )

    @decorators.needs_root
    def prepare(self) -> None:
        if self.args.dmcrypt:
            raise RuntimeError('--dmcrypt is not yet supported by the zfs backend')

        self.precondition_all_devices()

        self.osd_fsid = self.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class

        tmpfs = not self.args.no_tmpfs
        dry_run = self.args.dry_run

        if dry_run:
            # A true dry run never contacts a mon or executes anything;
            # it only computes and prints the plan. If --osd-id wasn't
            # given, osd_id stays a placeholder since real allocation is
            # itself a live cluster action we're not performing here.
            self.osd_id = self.osd_id or '99999'
            self.plan_data_device(self.osd_fsid)
            terminal.warning('Dry run (-n/--dry-run): nothing will be created. Plan:')
            terminal.warning('  osd_id: {}'.format(self.osd_id))
            terminal.warning('  osd_fsid: {}'.format(self.osd_fsid))
            terminal.warning('  block device: {}'.format(self.block_device_path))
            terminal.warning('  osd data dir: /var/lib/ceph/osd/{}-{}'.format(conf.cluster, self.osd_id))
            terminal.warning('  commands:')
            for cmd in self._data_device_commands:
                terminal.warning('    ' + ' '.join(cmd))
            dataset = '{}/{}'.format(self.pool_name, self.zvol_name)
            for key, value in [
                ('ceph:osd_id', self.osd_id),
                ('ceph:osd_fsid', self.osd_fsid),
                ('ceph:cluster_name', conf.cluster),
                ('ceph:type', 'block'),
                ('ceph:objectstore', self.objectstore),
            ]:
                terminal.warning(
                    '    /sbin/zfs set {}={} {}'.format(key, value, dataset)
                )
            for key, value in [
                ('ceph:osd_id', self.osd_id),
                ('ceph:osd_fsid', self.osd_fsid),
                ('ceph:managed_by', 'ceph-volume-zfs'),
                ('ceph:zvol_name', self.zvol_name),
            ]:
                terminal.warning(
                    '    /sbin/zpool set {}={} {}'.format(key, value, self.pool_name)
                )
            for device_type in ('db', 'wal'):
                device = getattr(self.args, 'block_{}'.format(device_type), None)
                if not device:
                    continue
                size = getattr(self.args, 'block_{}_size'.format(device_type), None)
                if device == 'same-pool':
                    pool = self.pool_name
                    terminal.warning('  block.{}: carved from the block pool {}'.format(
                        device_type, pool))
                else:
                    pool = '{}-{}'.format(self.pool_name, device_type)
                    terminal.warning('  block.{}: dedicated device {} -> pool {}'.format(
                        device_type, device, pool))
                    terminal.warning('    /sbin/zpool create -m none {} {}'.format(
                        pool, device))
                ds = '{}/osd-{}-{}'.format(pool, device_type, self.osd_fsid)
                terminal.warning('    /sbin/zfs create {}-V {} {}'.format(
                    '-s ' if self.args.thin else '',
                    size or '<95% of pool, computed after creation>', ds))
                terminal.warning('    (plus ceph:* properties on {})'.format(ds))
                terminal.warning('  block.{} device: /dev/zvol/{}'.format(device_type, ds))
            terminal.warning('    ceph osd new {}  (mon round-trip)'.format(self.osd_fsid))
            terminal.warning('    ceph mon getmap  (mon round-trip)')
            terminal.warning('    ceph-osd --mkfs ...  (local)')
            return

        self.osd_id = prepare_utils.create_id(
            self.osd_fsid, json.dumps(self.secrets), self.osd_id)

        # prepare_data_device() creates the pool, then the db/wal
        # zvols, then the block zvol sized against what remains.
        self.block_device_path = self.prepare_data_device('block', self.osd_fsid)

        self.prepare_osd_req(tmpfs=tmpfs)

        self.osd_mkfs()

