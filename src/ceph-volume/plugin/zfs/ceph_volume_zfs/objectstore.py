import base64
import json
import logging
import os
import time
from typing import Optional, TYPE_CHECKING

from ceph_volume import conf, terminal, decorators, process
from ceph_volume.objectstore.baseobjectstore import BaseObjectStore
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system

from ceph_volume_zfs.util.disk import (
    get_gpart_info,
    get_zpool_membership,
    get_mount_info,
    get_swap_info,
    list_ceph_zpools,
)

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
        self._test = getattr(args, 'test', False)
        # activate never needs a cephx secret -- the OSD already has
        # its keyring on disk -- so it takes the same lightweight
        # path, avoiding a pointless ceph-authtool dependency.
        self._activating = getattr(args, 'all', False) or (
            not hasattr(args, 'data') and (
                getattr(args, 'osd_id', None) or getattr(args, 'osd_fsid', None)))
        if self._dry_run or self._test or self._activating:
            # Neither a dry run nor a --test run should depend on the
            # real Ceph binaries being installed -- but
            # BaseObjectStore.__init__ eagerly runs `ceph-authtool
            # --gen-print-key` for the cephx secret. Under --test the
            # OSD is never registered with a mon, so that secret is an
            # unused token; we generate an equivalent locally instead
            # (same shape: base64 of random bytes) and skip the base
            # class entirely.
            #
            # FRAGILE: this duplicates BaseObjectStore's attribute
            # set by hand. If upstream adds new attributes other code
            # paths rely on, this will need updating to match.
            self.args = args
            if self._test:
                # Same shape as `ceph-authtool --gen-print-key` output
                # (base64-encoded random bytes) but generated locally,
                # since under --test it's never presented to a mon.
                self.cephx_secret = base64.b64encode(os.urandom(40)).decode('utf-8')
                self.secrets = {'cephx_secret': self.cephx_secret}
            else:
                self.secrets = {}
                self.cephx_secret = None
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
            self.osd_id = getattr(self.args, 'osd_id', '') or ''
            self.osd_fsid = getattr(self.args, 'osd_fsid', '') or ''

        self.method = 'zfs'
        # BaseObjectStore.__init__ defaults self.objectstore from
        # getattr(args, 'objectstore', '') -- this backend only
        # supports bluestore for now, so force it regardless of what
        # (if anything) was passed in.
        self.objectstore = 'bluestore'
        self.pool_name = ''
        self.zvol_name = ''
        self._zvol_path_name = ''
        self._data_device_commands = []
        # db/wal datasets created this run, for cross-linking properties
        self._metadata_datasets = {}

    def _diskname(self) -> str:
        return self.args.data.replace('/dev/', '')

    def _vlog(self, msg: str) -> None:
        """
        Verbose step-by-step logging. Active under --test (so a fake
        run shows exactly what it's doing/faking at each step) or the
        global -v flag (conf.verbose, set by `ceph-volume zfs -v ...`).
        """
        if getattr(self.args, 'test', False) or getattr(conf, 'verbose', False):
            terminal.info(msg)

    def _fake_osd_id(self) -> str:
        """
        Only used under --test when no --osd-id was given. This is
        NOT a real cluster-allocated ID -- purely local bookkeeping so
        repeated test runs get distinguishable IDs without touching a
        mon. Do not use this ID as if it were registered; it isn't.
        """
        return str(int(time.time()) % 100000)

    def _fake_monmap(self, osd_id: str) -> None:
        """
        Generates a local, empty monmap via `monmaptool --create`
        instead of fetching one from a live mon (`ceph mon getmap`).
        monmaptool is designed for exactly this -- it's the same tool
        used to bootstrap a cluster before any mon exists -- so this
        works fully offline. `ceph-osd --mkfs` only needs *a* monmap
        file to embed at mkfs time; it doesn't need to contact
        anything live to do so.
        """
        path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
        monmap_destination = os.path.join(path, 'activate.monmap')
        terminal.warning(
            '--test: generating a local, empty monmap instead of running '
            '"ceph mon getmap" against a live mon'
        )
        process.run(['monmaptool', '--create', '--clobber', monmap_destination])

    def prepare_osd_req(self, tmpfs: bool = True) -> None:
        """
        Overrides BaseObjectStore.prepare_osd_req().

        Under --test this deliberately does only the parts that need
        no Ceph binaries: creates the OSD directory (optionally tmpfs)
        and symlinks the block device. The monmap (monmaptool) and
        keyring (ceph-authtool) are skipped, since --test also skips
        `ceph-osd --mkfs` -- nothing would consume either of them, and
        writing them would reintroduce the binary dependency --test
        exists to avoid.
        """
        prepare_utils.create_osd_path(self.osd_id, tmpfs=tmpfs)
        prepare_utils.link_block(self.block_device_path, self.osd_id)
        if self._test:
            self._vlog('--test: skipping monmap creation (no mkfs to consume it)')
            self._vlog('--test: skipping keyring write (needs ceph-authtool)')
            return
        prepare_utils.get_monmap(self.osd_id)
        prepare_utils.write_keyring(self.osd_id, self.cephx_secret)

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
            ['/sbin/zpool', 'create', '-f', '-m', 'none',
             self.pool_name, '/dev/{}'.format(diskname)],
            ['/sbin/zfs', 'create', '-V', '<remaining pool space, computed after creation>',
             zvol_path_name],
        ]

    def prepare_data_device(self, device_type: str, osd_uuid: str) -> str:
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
        self._vlog('Creating zpool {}'.format(self.pool_name))
        process.run(self._data_device_commands[0])
        # db/wal first -- see docstring.
        self.setup_metadata_devices(osd_uuid)
        # Size is resolved only now: the pool must exist to report its
        # available space, and any same-pool db/wal zvols must already
        # have taken their reservation out of it.
        size = self._zvol_size()
        zvol_cmd = self._zvol_create_cmd(size, self._zvol_path_name)
        self._data_device_commands[1] = zvol_cmd
        self._vlog('Creating zvol {} ({} bytes)'.format(self.block_device_path, size))
        process.run(zvol_cmd)
        self._tag_zvol()
        self._tag_zpool()
        return self.block_device_path

    def _zvol_create_cmd(self, size: str, dataset: str) -> list:
        """
        Builds a `zfs create -V` command. By default the zvol is
        space-reserved (thick): ZFS sets a refreservation covering the
        full volsize, so the pool cannot be oversubscribed and
        bluestore never sees a write fail because the pool ran out
        underneath it.

        --thin opts into sparse (-s) zvols instead. That allows
        overcommitting the pool, which is fine for testing but risky
        for a real OSD: a full pool surfaces as write errors to
        bluestore.
        """
        cmd = ['/sbin/zfs', 'create']
        if getattr(self.args, 'thin', False):
            cmd.append('-s')
        cmd.extend(['-V', size, dataset])
        return cmd

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
                self._vlog('Creating {} zvol {} in the block pool'.format(device_type, dataset))
                process.run(self._zvol_create_cmd(size, dataset))
            else:
                diskname = device.replace('/dev/', '')
                pool = '{}-{}'.format(self.pool_name, device_type)
                self._vlog('Creating {} zpool {} on /dev/{}'.format(device_type, pool, diskname))
                process.run(['/sbin/zpool', 'create', '-f', '-m', 'none',
                             pool, '/dev/{}'.format(diskname)])
                dataset = '{}/osd-{}-{}'.format(pool, device_type, osd_uuid)
                if not size:
                    size = self._pool_fraction_size(pool)
                self._vlog('Creating {} zvol {} ({})'.format(device_type, dataset, size))
                process.run(self._zvol_create_cmd(size, dataset))
                self._tag_zpool(pool_name=pool, device_type=device_type)

            self._metadata_datasets[device_type] = dataset
            self._tag_zvol(dataset=dataset, device_type=device_type)
            setattr(self, '{}_device_path'.format(device_type),
                    '/dev/zvol/{}'.format(dataset))

    def _tag_zvol(self, dataset: Optional[str] = None,
                  device_type: str = 'block') -> None:
        """
        Sets ZFS user properties on a zvol -- the metadata store
        equivalent to LVM tags, mirroring how lvm.py tags the LV
        (self.block_lv.set_tags(...)) rather than the VG.

        Defaults to the main block zvol; pass dataset/device_type to
        tag a db or wal zvol instead.

        Custom property names must contain a colon (the "ceph:" here
        is the namespace prefix ZFS requires for user properties, not
        a stylistic choice).
        """
        if dataset is None:
            dataset = '{}/{}'.format(self.pool_name, self.zvol_name)
        properties = {
            'ceph:osd_id': self.osd_id,
            'ceph:osd_fsid': self.osd_fsid,
            'ceph:cluster_name': conf.cluster,
            'ceph:cluster_fsid': self._cluster_fsid(),
            'ceph:type': device_type,
            'ceph:objectstore': self.objectstore,
            'ceph:crush_device_class': getattr(self.args, 'crush_device_class', None) or '',
        }
        # Cross-links, so any one zvol names the others belonging to
        # this OSD. Not derivable from ZFS itself, and needed once
        # db/wal live in a separate pool from block (where scanning a
        # single pool no longer finds the whole set).
        if device_type == 'block':
            for other in ('db', 'wal'):
                other_dataset = self._metadata_datasets.get(other)
                if other_dataset:
                    properties['ceph:{}_device'.format(other)] = \
                        '/dev/zvol/{}'.format(other_dataset)
        else:
            properties['ceph:block_device'] = '/dev/zvol/{}/{}'.format(
                self.pool_name, self.zvol_name)
        for key, value in properties.items():
            if not value:
                continue
            self._vlog('Setting {}={} on {}'.format(key, value, dataset))
            process.run(['/sbin/zfs', 'set', '{}={}'.format(key, value), dataset])

    def _tag_zpool(self, pool_name: Optional[str] = None,
                   device_type: str = 'block') -> None:
        """
        Sets a minimal set of ZFS user properties on the zpool itself
        (confirmed supported by `zpool set` on this system), so
        `ceph-volume zfs list`/`activate` can find Ceph-managed pools
        directly via `zpool get` without first needing to know the
        zvol name to look inside -- i.e. this links the pool to its
        zvol/OSD, deliberately not a full duplicate of every property
        set on the zvol in _tag_zvol().

        Defaults to the main block pool; pass pool_name/device_type
        for a dedicated db or wal pool.
        """
        if pool_name is None:
            pool_name = self.pool_name
        properties = {
            'ceph:osd_id': self.osd_id,
            'ceph:osd_fsid': self.osd_fsid,
            'ceph:managed_by': 'ceph-volume-zfs',
            'ceph:type': device_type,
        }
        if device_type == 'block':
            properties['ceph:zvol_name'] = self.zvol_name
        for key, value in properties.items():
            if not value:
                continue
            self._vlog('Setting {}={} on zpool {}'.format(key, value, pool_name))
            process.run(['/sbin/zpool', 'set', '{}={}'.format(key, value), pool_name])

    def _cluster_fsid(self) -> str:
        """
        The cluster FSID, which unlike the cluster *name* is actually
        unique (two clusters can both be called "ceph"). Falls back to
        empty rather than raising: under --test there may be no
        ceph.conf loaded at all, and a missing fsid shouldn't abort a
        ZFS-only run.
        """
        try:
            return self.get_cluster_fsid() or ''
        except Exception:
            logger.debug('could not determine cluster fsid, leaving it unset')
            return ''

    def _pool_fraction_size(self, pool_name: str) -> str:
        """
        Returns 95% of the named pool's available bytes as an absolute
        size string for `zfs create -V` (which rejects percentages).
        Must be called after the pool exists.
        """
        out, err, rc = process.call(
            ['/sbin/zfs', 'get', '-Hp', '-o', 'value', 'available', pool_name]
        )
        if rc != 0 or not out:
            raise RuntimeError(
                'Could not determine available space in zpool {}'.format(pool_name)
            )
        return str(int(int(out[0].strip()) * 0.95))

    def _zvol_size(self) -> str:
        """
        Returns an absolute size string for `zfs create -V`, which
        rejects percentages ("bad volume size '95%': invalid numeric
        suffix"). Queries the pool's actual available bytes and takes
        the configured fraction of that.

        Must be called AFTER the zpool exists -- the pool has to be
        there to report its size.
        """
        if getattr(self.args, 'zvol_size', None):
            return self.args.zvol_size
        out, err, rc = process.call(
            ['/sbin/zfs', 'get', '-Hp', '-o', 'value', 'available', self.pool_name]
        )
        if rc != 0 or not out:
            raise RuntimeError(
                'Could not determine available space in zpool {}'.format(self.pool_name)
            )
        available = int(out[0].strip())
        # Leave headroom rather than consuming the whole pool: ZFS
        # needs free space to operate well, and a zvol sized to 100%
        # of a pool tends to cause trouble as it fills.
        return str(int(available * 0.95))


    def _find_osd(self, osd_id=None, osd_fsid=None):
        """
        Locates a prepared OSD by scanning Ceph-managed zpools for
        matching ceph:osd_id / ceph:osd_fsid properties. Returns a
        dict with the block/db/wal device paths, or None.

        This is the ZFS counterpart to lvm's tag lookup: everything
        needed to reconstruct the OSD directory lives in the ZFS
        properties prepare() wrote, so no external state is required.
        """
        for entry in list_ceph_zpools():
            props = entry['properties']
            if osd_id is not None and str(props.get('osd_id')) != str(osd_id):
                continue
            if osd_fsid is not None and props.get('osd_fsid') != osd_fsid:
                continue
            found = {
                'osd_id': props.get('osd_id'),
                'osd_fsid': props.get('osd_fsid'),
                'cluster_name': props.get('cluster_name') or conf.cluster,
                'block': None,
                'db': None,
                'wal': None,
            }
            for zvol in entry['zvols']:
                ztype = zvol['properties'].get('type')
                if ztype in ('block', 'db', 'wal'):
                    found[ztype] = zvol['device']
                # cross-links let us pick up db/wal living in another
                # pool, which scanning this pool alone would miss
                for other in ('db', 'wal'):
                    linked = zvol['properties'].get('{}_device'.format(other))
                    if linked and not found[other]:
                        found[other] = linked
            if found['block']:
                return found
        return None

    @decorators.needs_root
    def activate(self, osd_id=None, osd_fsid=None) -> None:
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
        osd_id = osd_id if osd_id is not None else getattr(self.args, 'osd_id', None)
        osd_fsid = osd_fsid if osd_fsid is not None else getattr(self.args, 'osd_fsid', None)
        if osd_id is None and osd_fsid is None:
            raise RuntimeError('activate requires --osd-id and/or --osd-fsid')

        osd = self._find_osd(osd_id=osd_id, osd_fsid=osd_fsid)
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
                self.osd_id, tmpfs=not getattr(self.args, 'no_tmpfs', False))

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
        if getattr(self.args, 'start', False):
            self.start_osd()
        else:
            terminal.info(
                'not starting the daemon; run: ceph-osd -i {} --setuser ceph '
                '--setgroup ceph'.format(self.osd_id))

    def start_osd(self) -> None:
        """
        Starts the OSD daemon directly. There is no systemd on
        FreeBSD, and no rc.d integration for ceph-volume-zfs yet, so
        this simply execs ceph-osd -- the process is not supervised
        and will not come back on its own if it dies.
        """
        terminal.warning(
            'starting ceph-osd directly; it is NOT supervised and will not '
            'restart automatically (no rc.d integration yet)')
        process.run([
            'ceph-osd',
            '--cluster', conf.cluster,
            '-i', str(self.osd_id),
            '--setuser', 'ceph',
            '--setgroup', 'ceph',
        ])

    @decorators.needs_root
    def activate_all(self) -> None:
        """
        Activates every ZFS-backed OSD found on this host. This is
        what a boot-time hook would call.
        """
        entries = list_ceph_zpools()
        osd_ids = []
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
        if getattr(self.args, 'dmcrypt', False):
            raise RuntimeError('--dmcrypt is not yet supported by the zfs backend')

        self.precondition_all_devices()

        self.osd_fsid = self.osd_fsid or system.generate_uuid()
        crush_device_class = getattr(self.args, 'crush_device_class', None)
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class

        tmpfs = not getattr(self.args, 'no_tmpfs', False)
        dry_run = getattr(self.args, 'dry_run', False)
        test = getattr(self.args, 'test', False)

        if dry_run:
            # A true dry run never contacts a mon or executes anything,
            # regardless of --test -- it only computes and prints the
            # plan. If --osd-id wasn't given, osd_id stays a
            # placeholder since real allocation is itself a live
            # cluster action we're not performing here.
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
                    terminal.warning('    /sbin/zpool create -f -m none {} {}'.format(
                        pool, device))
                ds = '{}/osd-{}-{}'.format(pool, device_type, self.osd_fsid)
                terminal.warning('    /sbin/zfs create {}-V {} {}'.format(
                    '-s ' if getattr(self.args, 'thin', False) else '',
                    size or '<95% of pool, computed after creation>', ds))
                terminal.warning('    (plus ceph:* properties on {})'.format(ds))
                terminal.warning('  block.{} device: /dev/zvol/{}'.format(device_type, ds))
            if test:
                terminal.warning('    (--test also set: osd_id/fsid allocation and monmap fetch would be faked, not real)')
            else:
                terminal.warning('    ceph osd new {}  (mon round-trip)'.format(self.osd_fsid))
                terminal.warning('    ceph mon getmap  (mon round-trip)')
            terminal.warning('    ceph-osd --mkfs ...  (local)')
            return

        if test:
            self.osd_id = self.osd_id or self._fake_osd_id()
            terminal.warning(
                '--test: ZFS-only run -- creates the real zpool/zvol and '
                'tags them, but skips everything needing Ceph binaries or '
                'a mon ("ceph osd new", monmap, keyring, "ceph-osd --mkfs"). '
                'Using osd_id={} osd_fsid={}. This is NOT a usable OSD.'.format(
                    self.osd_id, self.osd_fsid
                )
            )
        else:
            self.osd_id = prepare_utils.create_id(
                self.osd_fsid, json.dumps(self.secrets), self.osd_id)

        # prepare_data_device() creates the pool, then the db/wal
        # zvols, then the block zvol sized against what remains.
        self.block_device_path = self.prepare_data_device('block', self.osd_fsid)

        self.prepare_osd_req(tmpfs=tmpfs)

        if test:
            self._vlog('--test: skipping "ceph-osd --mkfs"')
            return

        self.osd_mkfs()

