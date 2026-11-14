import argparse
import json
import logging
import os
import re
import time
from textwrap import dedent
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ceph_volume import conf, decorators, process, terminal
from ceph_volume.api import lvm as api
from ceph_volume.systemd import systemctl
from ceph_volume.util import disk, system
from ceph_volume.util.arg_validators import valid_osd_id
from . import zap

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

MAINTENANCE_FLAGS = ('noout', 'norebalance', 'nobackfill')

DD_BLOCK_SIZE = '4M'
# headroom required at the backup path on top of the sum of the sizes of the
# devices being backed up
BACKUP_SPACE_MARGIN = 0.01
OSD_STOP_TIMEOUT = 60  # seconds
OSD_UP_TIMEOUT = 120  # seconds
OSD_POLL_INTERVAL = 5  # seconds

# label descriptions reported by ceph-bluestore-tool for raw (non-LVM)
# BlueFS db/wal partitions
BLUEFS_LABEL_DESCRIPTIONS = ('bluefs db', 'bluefs wal')


def is_lvm_prefix_layout(kinds: Sequence[Optional[str]]) -> bool:
    """
    Given a sequence of per-partition classifications in partition order,
    where each entry is ``'lvm'``, ``'raw'`` or ``None`` (not a db/wal
    device), check that the LVM-backed db/wal devices form a prefix: no raw
    db/wal partition may exist below (before) an LVM-backed one.

    A layout like::

        [raw, raw, lvm, lvm]

    is rejected, while both of these are accepted::

        [lvm, lvm, raw, raw]
        [raw, raw, raw, raw]
    """
    seen_raw = False
    for kind in kinds:
        if kind == 'raw':
            seen_raw = True
        elif kind == 'lvm' and seen_raw:
            return False
    return True


def parse_what(value: str) -> List[str]:
    """
    Parse the ``--what`` argument: a comma-separated list of roles, either
    ``db`` or ``wal``. Duplicates are removed while preserving order.
    """
    roles: List[str] = []
    for token in (value or '').split(','):
        token = token.strip()
        if not token:
            continue
        if token not in ('db', 'wal'):
            raise RuntimeError(
                "invalid value for --what: '%s' (valid values are: db, wal)" % token)
        if token not in roles:
            roles.append(token)
    if not roles:
        raise RuntimeError('--what cannot be empty')
    return roles


def restore_size(image_size: int, target_size: int) -> Tuple[int, bool]:
    """
    Determine how many bytes of a backup image can be restored into a
    (possibly smaller) logical volume. Returns a ``(size, trimmed)`` tuple
    where ``trimmed`` indicates the image did not fit entirely.
    """
    if image_size <= target_size:
        return image_size, False
    return target_size, True


class MigrateBlockDevices(object):

    help = ('In-place migrate hybrid OSD block.db/block.wal partitions '
            'to LVM volumes')

    def __init__(self, argv: List[str]) -> None:
        self.argv = argv
        self.args: Optional[argparse.Namespace] = None
        self.roles: List[str] = []
        # roles with a partition pending migration: role -> partition path
        self.partitions: Dict[str, str] = {}
        # roles already backed by an LV: role -> api.Volume
        self.existing_lvs: Dict[str, api.Volume] = {}
        # roles successfully converted: role -> api.Volume
        self.new_lvs: Dict[str, api.Volume] = {}
        # role -> backup image path
        self.backup_images: Dict[str, str] = {}
        self.roles_pending: List[str] = []
        self.data_lv: Optional[api.Volume] = None
        self.osd_fsid: Optional[str] = None
        self.cluster_name: Optional[str] = None
        self.flags_set: List[str] = []
        # systemd units that were running when the OSD was stopped
        self.active_units: List[str] = []

    def make_parser(self, prog: str,
                    sub_command_help: str) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            prog=prog,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            '--osd-id',
            required=True,
            help='OSD ID to migrate',
            type=valid_osd_id,
        )
        parser.add_argument(
            '--what',
            default='db,wal',
            help=('comma-separated list of devices to migrate: db, wal '
                  '(defaults to both)'),
        )
        parser.add_argument(
            '--strategy',
            default='in-place',
            choices=['in-place', 'recreate-osd'],
            help='migration strategy (defaults to in-place)',
        )
        parser.add_argument(
            '--backup-path',
            help=('existing writable directory to store backup images in, '
                  'required for the in-place strategy'),
        )
        parser.add_argument(
            '--fsck',
            dest='fsck',
            default='shallow',
            choices=['shallow', 'deep', 'none'],
            help=('run ceph-bluestore-tool fsck before starting the OSD '
                  '(defaults to shallow)'),
        )
        parser.add_argument(
            '--no-maintenance-flags',
            dest='maintenance_flags',
            action='store_false',
            help=('do not set noout/norebalance/nobackfill during the '
                  'migration (they are set by default)'),
        )
        parser.add_argument(
            '--force-unhealthy',
            dest='force_unhealthy',
            action='store_true',
            help='proceed even if cluster health is not HEALTH_OK',
        )
        parser.add_argument(
            '--dry-run',
            dest='dry_run',
            action='store_true',
            help='run preflight checks and print the migration plan only',
        )
        parser.add_argument(
            '--confirm-recreate',
            dest='confirm_recreate',
            action='store_true',
            help=('acknowledge the destructive recreate-osd strategy '
                  '(not implemented yet)'),
        )
        return parser

    # preflight
    #

    def find_osd_devices(self) -> None:
        """
        Resolve the data LV for this OSD along with the current db/wal
        devices, classifying each requested role as either LVM-backed
        (already migrated) or partition-backed (pending migration).
        """
        assert self.args is not None
        osd_id = self.args.osd_id
        lvs = api.get_lvs(tags={'ceph.osd_id': osd_id})
        if not lvs:
            raise SystemExit(
                'Unable to find any LV for OSD ID: {} (only LVM-based OSDs '
                'are supported by this command)'.format(osd_id))

        for lv in lvs:
            if lv.tags.get('ceph.type') == 'block':
                self.data_lv = lv
                break

        if not self.data_lv:
            raise SystemExit(
                'Unable to find the data LV for OSD ID: {}'.format(osd_id))

        self.osd_fsid = self.data_lv.tags.get('ceph.osd_fsid')
        self.cluster_name = self.data_lv.tags.get(
            'ceph.cluster_name', conf.cluster)

        for role in ('db', 'wal'):
            role_lvs = api.get_lvs(
                tags={'ceph.osd_id': osd_id, 'ceph.type': role})
            # only consider LVs matching our fsid, stale LVs from failed
            # deployments carrying the same id are ignored (with a warning)
            role_lvs = [lv for lv in role_lvs
                        if lv.tags.get('ceph.osd_fsid') == self.osd_fsid]
            if role_lvs:
                if len(role_lvs) > 1:
                    mlogger.warning(
                        'found multiple %s LVs for OSD %s, using %s',
                        role, osd_id, role_lvs[0].lv_path)
                self.existing_lvs[role] = role_lvs[0]
                continue

            # no LV for this role: resolve the device through the PARTUUID
            # stored in the data LV tags
            partuuid = self.data_lv.tags.get('ceph.{}_uuid'.format(role))
            if not partuuid:
                continue
            device = disk.get_device_from_partuuid(partuuid)
            if device:
                self.partitions[role] = device
            elif role in self.roles:
                raise SystemExit(
                    'block.{} of OSD {} is neither an LV nor a resolvable '
                    'partition (uuid tag {} is broken), unable to '
                    'migrate'.format(role, osd_id, partuuid))

    def preflight(self) -> bool:
        """
        Run all pre-migration checks, populating the state needed to execute
        (or plan) the migration. Returns False when there is nothing to do.
        """
        self.find_osd_devices()
        assert self.data_lv is not None
        assert self.args is not None

        if self.data_lv.tags.get('ceph.objectstore', 'bluestore') != 'bluestore':
            raise SystemExit(
                'OSD {} is not a bluestore OSD, unable to migrate'.format(
                    self.args.osd_id))

        if self.data_lv.encrypted:
            raise SystemExit(
                'OSD {} is encrypted, migrating encrypted OSDs is not '
                'supported'.format(self.args.osd_id))

        for role in self.roles:
            if role not in self.partitions and role not in self.existing_lvs:
                raise SystemExit(
                    'OSD {} does not have a block.{} device'.format(
                        self.args.osd_id, role))

        for role in self.roles:
            if role in self.existing_lvs:
                mlogger.info(
                    'block.%s of OSD %s is already LVM-backed (%s), skipping',
                    role, self.args.osd_id, self.existing_lvs[role].lv_path)

        self.roles_pending = [role for role in self.roles
                              if role in self.partitions]
        if not self.roles_pending:
            terminal.success(
                'OSD {}: db/wal devices are already LVM-backed, '
                'nothing to do'.format(self.args.osd_id))
            return False

        # every device to migrate must be a partition
        for role in self.roles_pending:
            partition = self.partitions[role]
            if not disk.is_partition(partition):
                raise SystemExit(
                    '{} (block.{}) is not a partition, unable to '
                    'migrate'.format(partition, role))
            if not self.partition_size(partition):
                raise SystemExit(
                    'unable to determine the size of {}'.format(partition))

        self.check_layout_prefix()
        self.check_backup_path()
        self.check_cluster()
        return True

    def check_backup_path(self) -> None:
        assert self.args is not None
        path = self.args.backup_path
        if not path:
            raise SystemExit(
                '--backup-path is required for the in-place strategy')
        if not os.path.isdir(path):
            raise SystemExit(
                '--backup-path {} is not an existing directory'.format(path))
        if not os.access(path, os.W_OK):
            raise SystemExit('--backup-path {} is not writable'.format(path))
        required = self.backup_space_required()
        statvfs = os.statvfs(path)
        free = statvfs.f_bavail * statvfs.f_frsize
        if free < required:
            raise SystemExit(
                'not enough free space at {}: {} bytes are required, '
                '{} bytes are available'.format(path, required, free))

    def backup_space_required(self) -> int:
        total = sum(self.partition_size(partition)
                    for partition in self.partitions.values())
        return int(total * (1 + BACKUP_SPACE_MARGIN))

    def check_cluster(self) -> None:
        assert self.args is not None
        # the OSD must be registered and the cluster reachable
        stdout, stderr, returncode = process.call(self.ceph_cmd(
            ['osd', 'info', self.args.osd_id, '--format', 'json']))
        if returncode != 0:
            raise SystemExit(
                'unable to query the cluster for osd.{}: {}'.format(
                    self.args.osd_id, ' '.join(stderr).strip()))
        stdout, stderr, returncode = process.call(self.ceph_cmd(['health']))
        if returncode != 0:
            raise SystemExit('unable to query cluster health: {}'.format(
                ' '.join(stderr).strip()))
        status = ''.join(stdout).strip()
        if status != 'HEALTH_OK' and not self.args.force_unhealthy:
            raise SystemExit(
                'cluster health is "{}", not "HEALTH_OK". Use '
                '--force-unhealthy to proceed anyway'.format(status))

    def check_layout_prefix(self) -> None:
        """
        On the disk(s) hosting the partitions to migrate, LVM-backed db/wal
        devices must form a prefix in partition order: reject when a raw
        db/wal partition exists below an LVM-backed one (common after a
        single OSD was recreated with newer tooling on the same disk).
        """
        parents = set()
        for partition in self.partitions.values():
            parent = self.parent_device(partition)
            if not parent:
                raise SystemExit(
                    'unable to determine the parent device of {}'.format(
                        partition))
            parents.add(parent)

        for parent in sorted(parents):
            kinds = self.classify_disk_partitions(parent)
            if not is_lvm_prefix_layout(kinds):
                raise SystemExit(
                    'refusing to migrate: LVM-backed db/wal devices on {} '
                    'must form a prefix in partition order, but a raw '
                    'db/wal partition exists below an LVM-backed one (this '
                    'is common when a single OSD was recreated on this '
                    'disk). Either restore the original partition layout '
                    'or rebuild the OSDs involved'.format(parent))

    def parent_device(self, partition: str) -> Optional[str]:
        parts = disk.get_partitions()
        parent = parts.get(os.path.basename(os.path.realpath(partition)))
        if parent:
            return '/dev/{}'.format(parent)
        return None

    def disk_partitions(self, parent: str) -> List[str]:
        """
        Return the partitions of ``parent`` ordered by partition number.
        """
        parent_name = os.path.basename(os.path.realpath(parent))
        parts = disk.get_partitions()
        paths = ['/dev/{}'.format(name) for name, device in parts.items()
                 if device == parent_name]

        def partition_number(path: str) -> int:
            match = re.search(r'(\d+)$', os.path.basename(path))
            return int(match.group(1)) if match else 0

        return sorted(paths, key=partition_number)

    def classify_disk_partitions(self, parent: str) -> List[Optional[str]]:
        """
        Classify every partition of ``parent`` as 'lvm', 'raw' or ``None``
        (not a db/wal device). Partitions pending migration are classified
        as 'lvm' since that is the layout being validated.
        """
        migrating = set(self.partitions.values())
        return [self.classify_partition(partition, partition in migrating)
                for partition in self.disk_partitions(parent)]

    def classify_partition(self, partition: str,
                           will_migrate: bool) -> Optional[str]:
        if will_migrate:
            return 'lvm'
        for lv in api.get_device_lvs(partition):
            if lv.tags.get('ceph.type') in ('db', 'wal'):
                return 'lvm'
        label = disk.get_bluestore_header(partition).get(partition, {})
        if label.get('description') in BLUEFS_LABEL_DESCRIPTIONS:
            return 'raw'
        return None

    @staticmethod
    def partition_size(partition: str) -> int:
        """
        Size of a block device in bytes, as reported by sysfs.
        """
        name = os.path.basename(os.path.realpath(partition))
        sectors = disk.get_file_contents(
            os.path.join('/sys/class/block', name, 'size'), '0')
        return int(sectors or 0) * 512

    # migration
    #

    def ceph_cmd(self, args: List[str]) -> List[str]:
        assert self.cluster_name is not None
        return ['ceph', '--cluster', self.cluster_name] + args

    def set_maintenance_flags(self) -> None:
        for flag in MAINTENANCE_FLAGS:
            mlogger.info('setting cluster flag: %s', flag)
            process.run(self.ceph_cmd(['osd', 'set', flag]))
            # keep track of what was actually set, so that a failure halfway
            # through reports the correct unset instructions
            self.flags_set.append(flag)

    def unset_maintenance_flags(self) -> None:
        for flag in reversed(self.flags_set):
            mlogger.info('unsetting cluster flag: %s', flag)
            process.run(self.ceph_cmd(['osd', 'unset', flag]))
        self.flags_set = []

    def osd_units(self) -> List[str]:
        """
        Systemd units that may run this OSD: the classic unit and the
        cephadm one, when the cluster fsid is known.
        """
        assert self.data_lv is not None
        assert self.args is not None
        units = ['ceph-osd@{}'.format(self.args.osd_id)]
        cluster_fsid = self.data_lv.tags.get('ceph.cluster_fsid')
        if cluster_fsid:
            units.append('ceph-{}@osd.{}'.format(cluster_fsid,
                                                 self.args.osd_id))
        return units

    def stop_osd(self) -> None:
        assert self.args is not None
        for unit in self.osd_units():
            if systemctl.is_active(unit):
                mlogger.info('stopping %s', unit)
                systemctl.stop(unit)
                self.active_units.append(unit)
        if not self.active_units:
            mlogger.info('OSD %s is not running', self.args.osd_id)
            return
        deadline = time.time() + OSD_STOP_TIMEOUT
        while time.time() < deadline:
            if not any(systemctl.is_active(unit) for unit in self.active_units):
                return
            time.sleep(1)
        raise RuntimeError(
            'ceph-osd@{} did not stop within {} seconds'.format(
                self.args.osd_id, OSD_STOP_TIMEOUT))

    def backup_devices(self) -> None:
        assert self.args is not None
        metadata: Dict[str, Any] = {
            'osd_id': self.args.osd_id,
            'osd_fsid': self.osd_fsid,
            'cluster_name': self.cluster_name,
            'created': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'devices': [],
        }
        for role in self.roles_pending:
            partition = self.partitions[role]
            size = self.partition_size(partition)
            image = os.path.join(
                self.args.backup_path,
                'osd-{}-{}.img'.format(self.args.osd_id, role))
            mlogger.info('backing up %s (block.%s, %d bytes) to %s',
                         partition, role, size, image)
            process.run([
                'dd',
                'if={}'.format(partition),
                'of={}'.format(image),
                'bs={}'.format(DD_BLOCK_SIZE),
                'conv=fsync',
            ])
            if os.path.getsize(image) != size:
                raise RuntimeError(
                    'backup of {} is incomplete, expected {} bytes, '
                    'got {}'.format(partition, size,
                                    os.path.getsize(image)))
            self.backup_images[role] = image
            metadata['devices'].append({
                'role': role,
                'device': partition,
                'partuuid': disk.get_partuuid(partition),
                'size': size,
                'image': image,
            })
        metadata_file = os.path.join(
            self.args.backup_path,
            'ceph-volume-migrate-{}.json'.format(self.args.osd_id))
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        mlogger.info('backup metadata written to %s', metadata_file)

    def lv_tags_for_role(self, role: str) -> Dict[str, str]:
        """
        Tags to apply to the new LV, mirroring what ``ceph-volume lvm
        prepare`` sets on db/wal volumes.
        """
        assert self.data_lv is not None
        data_tags = self.data_lv.tags
        return {
            'ceph.osd_fsid': data_tags['ceph.osd_fsid'],
            'ceph.osd_id': data_tags['ceph.osd_id'],
            'ceph.cluster_fsid': data_tags['ceph.cluster_fsid'],
            'ceph.cluster_name': data_tags.get(
                'ceph.cluster_name', conf.cluster),
            'ceph.crush_device_class': data_tags.get(
                'ceph.crush_device_class', ''),
            'ceph.osdspec_affinity': data_tags.get(
                'ceph.osdspec_affinity', ''),
            'ceph.objectstore': data_tags.get('ceph.objectstore',
                                              'bluestore'),
            'ceph.encrypted': data_tags.get('ceph.encrypted', '0'),
            'ceph.with_tpm': data_tags.get('ceph.with_tpm', '0'),
            'ceph.type': role,
        }

    def convert_partition(self, role: str) -> api.Volume:
        """
        Wipe the partition backing ``role`` and replace it with a
        ceph-tagged PV/VG/LV using all the available space.
        """
        partition = self.partitions[role]
        mlogger.info('converting %s (block.%s) to LVM', partition, role)
        zap.zap_device(partition)
        api.create_pv(partition)
        vg = api.create_vg(partition, name_prefix='ceph')
        if not vg:
            raise RuntimeError(
                'unable to create a volume group on {}'.format(partition))
        lv = api.create_lv(
            name_prefix='osd-{}'.format(role),
            uuid=system.generate_uuid(),
            vg=vg,
            tags=self.lv_tags_for_role(role),
        )
        if not lv:
            raise RuntimeError(
                'unable to create a logical volume on {}'.format(vg.vg_name))
        # the uuid tag can only be set once the LV exists, mirroring what
        # prepare does for db/wal volumes
        lv.set_tags({
            'ceph.{}_uuid'.format(role): lv.lv_uuid,
            'ceph.vdo': api.is_vdo(lv.lv_path),
        })
        mlogger.info('created %s for block.%s', lv.lv_path, role)
        return lv

    def restore_device(self, role: str) -> None:
        lv = self.new_lvs[role]
        image = self.backup_images[role]
        image_size = os.path.getsize(image)
        lv_size = int(lv.lv_size)
        to_restore, trimmed = restore_size(image_size, lv_size)
        if trimmed:
            mlogger.warning(
                'backup image %s (%d bytes) is larger than %s (%d bytes): '
                'restoring the first %d bytes only. LVM metadata makes the '
                'new LV slightly smaller than the original partition; if '
                'BlueFS had allocated the tail of the partition the OSD '
                'will fail fsck/startup and must be restored from the '
                'backup', image, image_size, lv.lv_path, lv_size, to_restore)
        mlogger.info('restoring %s into %s', image, lv.lv_path)
        process.run([
            'dd',
            'if={}'.format(image),
            'of={}'.format(lv.lv_path),
            'bs={}'.format(DD_BLOCK_SIZE),
            'iflag=count_bytes',
            'count={}'.format(to_restore),
            'conv=fsync',
        ])

    def update_osd_metadata(self, role: str) -> None:
        """
        Point the data LV tags and the OSD directory symlinks at the new
        LV for ``role``.
        """
        assert self.data_lv is not None
        lv = self.new_lvs[role]
        mlogger.info('updating data LV tags for block.%s', role)
        self.data_lv.set_tags({
            'ceph.{}_uuid'.format(role): lv.lv_uuid,
            'ceph.{}_device'.format(role): lv.lv_path,
        })

        link = os.path.join(self.osd_path, 'block.{}'.format(role))
        if os.path.lexists(link):
            os.unlink(link)
        system.chown(lv.lv_path)
        process.run(['ln', '-s', lv.lv_path, link])
        mlogger.info('%s now points to %s', link, lv.lv_path)

    @property
    def osd_path(self) -> str:
        assert self.cluster_name is not None
        assert self.args is not None
        return '/var/lib/ceph/osd/{}-{}'.format(self.cluster_name,
                                                self.args.osd_id)

    def run_fsck(self) -> None:
        assert self.args is not None
        if self.args.fsck == 'none':
            mlogger.info('skipping fsck as requested')
            return
        command = ['ceph-bluestore-tool',
                   '--path', self.osd_path,
                   '--command', 'fsck']
        if self.args.fsck == 'deep':
            command.append('--deep')
        mlogger.info('running %s fsck on %s', self.args.fsck, self.osd_path)
        process.run(command)

    def osd_is_up(self) -> bool:
        assert self.args is not None
        stdout, stderr, returncode = process.call(self.ceph_cmd(
            ['osd', 'info', self.args.osd_id, '--format', 'json']))
        if returncode != 0:
            return False
        try:
            info = json.loads(''.join(stdout))
        except ValueError:
            return False
        return bool(info.get('up'))

    def start_osd_and_wait(self) -> None:
        assert self.args is not None
        if not self.active_units:
            mlogger.warning(
                'OSD %s was not running before the migration, not '
                'starting it', self.args.osd_id)
            return
        for unit in self.active_units:
            mlogger.info('starting %s', unit)
            systemctl.start(unit)
        deadline = time.time() + OSD_UP_TIMEOUT
        while time.time() < deadline:
            if self.osd_is_up():
                mlogger.info('OSD %s is up', self.args.osd_id)
                return
            time.sleep(OSD_POLL_INTERVAL)
        raise RuntimeError('OSD {} did not come up within {} seconds'.format(
            self.args.osd_id, OSD_UP_TIMEOUT))

    def report_failure(self) -> None:
        assert self.args is not None
        mlogger.error('migration of OSD %s failed', self.args.osd_id)
        mlogger.error('the OSD is stopped and the cluster maintenance '
                      'flags may still be set')
        if self.flags_set:
            mlogger.error('unset the maintenance flags with:')
            for flag in self.flags_set:
                mlogger.error('  ceph --cluster %s osd unset %s',
                              self.cluster_name, flag)
        if self.backup_images:
            mlogger.error('backup images are available at %s and can be '
                          'used to restore the original partitions',
                          self.args.backup_path)

    def print_plan(self) -> None:
        assert self.args is not None
        assert self.data_lv is not None
        print('OSD ID: {}'.format(self.args.osd_id))
        print('OSD FSID: {}'.format(self.osd_fsid))
        print('cluster: {}'.format(self.cluster_name))
        print('data LV: {}'.format(self.data_lv.lv_path))
        for role in self.roles_pending:
            partition = self.partitions[role]
            print('{}: {} ({} bytes) -> PV/VG/LV with ceph tags'.format(
                role, partition, self.partition_size(partition)))
        print('backup path: {} ({} bytes required)'.format(
            self.args.backup_path, self.backup_space_required()))
        print('fsck: {}'.format(self.args.fsck))
        if self.args.maintenance_flags:
            print('maintenance flags to set: {}'.format(
                ', '.join(MAINTENANCE_FLAGS)))
        else:
            print('maintenance flags: none')

    @decorators.needs_root
    def migrate_in_place(self) -> None:
        if not self.preflight():
            return
        if self.args.dry_run:
            self.print_plan()
            return
        try:
            if self.args.maintenance_flags:
                self.set_maintenance_flags()
            self.stop_osd()
            self.backup_devices()
            for role in self.roles_pending:
                self.new_lvs[role] = self.convert_partition(role)
                self.restore_device(role)
            for role in self.roles_pending:
                self.update_osd_metadata(role)
            self.run_fsck()
            self.start_osd_and_wait()
        except (Exception, KeyboardInterrupt):
            self.report_failure()
            raise
        if self.args.maintenance_flags:
            self.unset_maintenance_flags()
        terminal.success(
            'Migration successful. OSD {} now uses LVM for: {}'.format(
                self.args.osd_id, ', '.join(self.roles_pending)))

    def main(self) -> None:
        sub_command_help = dedent("""
        Converts partition-backed block.db/block.wal devices of a single
        hybrid (bluestore) OSD to LVM volumes, in-place: the OSD id and the
        data device are preserved and no rebalancing happens.

        The devices to convert are backed up to a directory first, the
        partition is then wiped and replaced by a ceph-tagged PV/VG/LV, the
        bytes are restored into the new LV, the OSD symlinks and LVM tags
        are updated, and a fsck runs before the OSD is started again.

        Moving BlueFS data between existing LVs is handled by
        "ceph-volume lvm migrate" instead.

        One OSD per invocation; loop over the command to migrate several
        OSDs.

        Example:

            ceph-volume lvm migrate-block-devices --osd-id 0 --what db,wal \\
                --backup-path /var/lib/ceph/migrate-backups

        Dry run (preflight and plan only):

            ceph-volume lvm migrate-block-devices --osd-id 0 --dry-run \\
                --backup-path /var/lib/ceph/migrate-backups
        """)

        parser = self.make_parser(
            'ceph-volume lvm migrate-block-devices', sub_command_help)

        if len(self.argv) == 0:
            print(sub_command_help)
            return

        self.args = parser.parse_args(self.argv)
        self.roles = parse_what(self.args.what)

        if self.args.strategy == 'recreate-osd':
            raise SystemExit(
                "strategy 'recreate-osd' is not implemented yet. Recreate "
                'the OSD manually with "ceph osd out", "ceph-volume lvm '
                'zap --destroy" and "ceph-volume lvm create" (data will '
                'be rebalanced)')

        self.migrate_in_place()
