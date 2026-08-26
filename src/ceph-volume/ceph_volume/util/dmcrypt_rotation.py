"""
In-place rotation of the dmcrypt (LUKS) passphrase of an encrypted OSD.

The passphrase (KEK) only wraps the LUKS volume key in a header keyslot, so
rotation works while the OSD is up: keyslot operations never disturb the open
dm-crypt mapping. All LUKS devices of an OSD (block/db/wal) share one
passphrase and are rotated as a set.

Two key-custody models are supported behind one interface:

* config-key custody (default): the passphrase lives on the monitors at
  ``dm-crypt/osd/<fsid>/luks``. ceph-volume generates the new passphrase
  and stores it back itself, which requires credentials with ``config-key
  set`` caps on that entry (the stock lockbox entity is get-only).
* external custody (``--key-store external``): the caller owns the passphrase
  (e.g. Rook in a Kubernetes Secret or KMS) and supplies it via environment
  variables. ceph-volume only performs the keyslot operations; the caller
  stores the new passphrase between the two phases (``--phase stage`` then
  ``--phase finish``).

Rotation state machine (slot 0 is the canonical slot written by
``luksFormat``, slot 1 is the staging slot; identical to Rook's key
rotation so mixed recovery works). Slots 0 and 1 are owned by this
protocol and may be overwritten by any flow, including ``--phase finish``;
foreign keyslots (2+) are never touched:

  S0 PRECHECK  the stored passphrase must open every device (read-only)
  S1 STAGE     ensure the current passphrase is valid in slot 1
               [crash: the stored passphrase still opens via slot 0]
  S2 INSTALL   wipe slot 0, enroll the new passphrase; both now valid
               [crash: the stored passphrase (still the old one) opens
               via slot 1]
               --phase stage exits here
  S3 PERSIST   write the new passphrase to the key store, read back and
               verify
               [crash after: the stored passphrase (now the new one)
               opens via slot 0]
  S4 CLEANUP   verify the stored passphrase opens everything, then wipe
               slot 1
               --phase finish entry point (reduced S0, then S4/S5)
  S5 REPORT    per-device keyslot summary

The invariant at every instant is that the passphrase held by the key
store (monitor config-key store, or an external store such as a
Kubernetes Secret/KMS) opens every device. Each state stores its result
durably in the LUKS header itself (a keyslot change is the state
transition), so a re-run probes the header with
``cryptsetup --test-passphrase`` and converges from any interruption; no
state files are used. The previous passphrase is wiped only once the
LUKS header and the key store hold the same passphrase. See
doc/ceph-volume/lvm/rotate-dmcrypt-key.rst for the full crash-window
table.
"""
import argparse
import fcntl
import logging
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Iterator, List, Optional

from ceph_volume import conf, configuration, terminal
from ceph_volume.api import lvm as api
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

CURRENT_KEY_ENV = 'CEPH_VOLUME_DMCRYPT_SECRET'
NEW_KEY_ENV = 'CEPH_VOLUME_NEW_DMCRYPT_SECRET'
LOCKBOX_SECRET_ENV = 'CEPH_VOLUME_CEPHX_LOCKBOX_SECRET'

SLOT_CANONICAL = 0
SLOT_STAGING = 1

LOCK_DIR = '/run/ceph-volume'


def _open_lock_dir() -> int:
    """
    Open the directory holding the rotation locks, creating it if absent.

    It lives under /run, which only root can write to, so an unprivileged
    user can neither pre-create it nor replace it with a symlink. The
    world-writable /run/lock would allow both. This is the scheme cryptsetup
    uses for its own LUKS2 header locks (/run/cryptsetup, 0700, O_NOFOLLOW;
    lib/utils_device_locking.c in the cryptsetup sources).
    """
    try:
        os.mkdir(LOCK_DIR, 0o700)
    except FileExistsError:
        pass
    except OSError as error:
        raise RuntimeError(f'unable to create the lock directory '
                           f'{LOCK_DIR}: {error}')
    try:
        return os.open(LOCK_DIR,
                       os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as error:
        raise RuntimeError(f'unable to open the lock directory '
                           f'{LOCK_DIR}: {error}')


@contextmanager
def _osd_rotation_lock(osd_fsid: str) -> Iterator[None]:
    """
    Serialize rotations per OSD with an advisory flock. The kernel releases
    the lock when the process exits, crash included, so a stale lock cannot
    occur; a held lock always means another rotation is running right now.
    """
    name = f'rotate-dmcrypt-{osd_fsid}.lock'
    path = os.path.join(LOCK_DIR, name)
    dir_fd = _open_lock_dir()
    try:
        # relative to the directory and O_NOFOLLOW, so a symlink planted at
        # the lock path fails instead of redirecting the open
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600,
                     dir_fd=dir_fd)
    except OSError as error:
        raise RuntimeError(f'unable to open the rotation lock {path}: '
                           f'{error}')
    finally:
        os.close(dir_fd)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError(
                f'another rotate-dmcrypt-key for OSD {osd_fsid} is already '
                f'running (lock {path})')
        yield
    finally:
        os.close(fd)


class KeyCustody(ABC):
    """Where the dmcrypt passphrase of an OSD lives."""

    @abstractmethod
    def get_current_key(self) -> str:
        pass

    @abstractmethod
    def get_new_key(self) -> str:
        pass

    @abstractmethod
    def persist_new_key(self, key: str) -> None:
        pass

    @abstractmethod
    def verify_persisted(self, key: str) -> None:
        pass


class ConfigKeyCustody(KeyCustody):
    """
    The passphrase lives in the mon config-key store at
    ``dm-crypt/osd/<fsid>/luks`` (cephadm and bare-metal deployments).
    """

    def __init__(self,
                 osd_id: str,
                 osd_fsid: str,
                 name: Optional[str] = None,
                 keyring: Optional[str] = None) -> None:
        self.osd_id = osd_id
        self.osd_fsid = osd_fsid
        self.name = name
        self.keyring = keyring

    def get_current_key(self) -> str:
        return encryption_utils.get_dmcrypt_key(self.osd_id,
                                                self.osd_fsid,
                                                lockbox_keyring=self.keyring,
                                                name=self.name)

    def get_new_key(self) -> str:
        return encryption_utils.create_dmcrypt_key()

    def persist_new_key(self, key: str) -> None:
        encryption_utils.set_dmcrypt_key(self.osd_id,
                                         self.osd_fsid,
                                         key,
                                         lockbox_keyring=self.keyring,
                                         name=self.name)

    def verify_persisted(self, key: str) -> None:
        stored = self.get_current_key()
        if stored != key:
            raise RuntimeError(
                'the dmcrypt key read back from the config-key store does '
                'not match the new key that was just stored')


class ExternalKeyCustody(KeyCustody):
    """
    The passphrase is owned by the caller (e.g. Rook: Kubernetes Secret or
    KMS) and supplied via environment variables. Persisting the new key is
    the caller's job, done between the two phases of the rotation.
    """

    def __init__(self, current_key: str, new_key: Optional[str]) -> None:
        self.current_key = current_key
        self.new_key = new_key

    def get_current_key(self) -> str:
        return self.current_key

    def get_new_key(self) -> str:
        if not self.new_key:
            raise RuntimeError(f'{NEW_KEY_ENV} is not set')
        return self.new_key

    def persist_new_key(self, key: str) -> None:
        pass

    def verify_persisted(self, key: str) -> None:
        pass


class RotationTarget(ABC):
    """Resolves an OSD to its set of backing LUKS devices."""

    osd_id: str = ''
    osd_fsid: str = ''
    encrypted: bool = False
    with_tpm: bool = False
    devices: List[str] = []

    @abstractmethod
    def update_lockbox_secret(self, secret: str) -> None:
        pass

    def _write_lockbox_keyring(self, secret: str) -> None:
        osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, self.osd_id)
        if not system.path_is_mounted(osd_path):
            prepare_utils.create_osd_path(self.osd_id, tmpfs=True)
        encryption_utils.write_lockbox_keyring(self.osd_id,
                                               self.osd_fsid,
                                               secret,
                                               force=True)


class LvmRotationTarget(RotationTarget):
    """An OSD deployed with ``ceph-volume lvm`` (LV tag driven)."""

    def __init__(self,
                 osd_id: Optional[str] = None,
                 osd_fsid: Optional[str] = None) -> None:
        tags = {}
        if osd_id is not None:
            tags['ceph.osd_id'] = str(osd_id)
        if osd_fsid is not None:
            tags['ceph.osd_fsid'] = osd_fsid
        lvs = api.get_lvs(tags=tags)
        self.lvs = [lv for lv in lvs
                    if lv.tags.get('ceph.type') in ('block', 'db', 'wal')]
        osd_desc = osd_fsid or osd_id
        if not self.lvs:
            raise RuntimeError(f'could not find any LVs for OSD {osd_desc}')
        try:
            block_lv = next(lv for lv in self.lvs
                            if lv.tags.get('ceph.type') == 'block')
        except StopIteration:
            raise RuntimeError(f'could not find a block LV for OSD {osd_desc}')
        self.osd_id = block_lv.tags['ceph.osd_id']
        self.osd_fsid = block_lv.tags['ceph.osd_fsid']
        cluster_name = block_lv.tags.get('ceph.cluster_name', 'ceph')
        conf.cluster = cluster_name
        configuration.load_ceph_conf_path(cluster_name)
        configuration.load()
        self.encrypted = block_lv.tags.get('ceph.encrypted', '0') == '1'
        self.with_tpm = block_lv.tags.get('ceph.with_tpm', '0') == '1'
        # in lvm mode, the LV itself is the LUKS container
        self.devices = [lv.lv_path for lv in self.lvs
                        if lv.tags.get('ceph.encrypted', '0') == '1']

    def update_lockbox_secret(self, secret: str) -> None:
        for lv in self.lvs:
            lv.set_tags({'ceph.cephx_lockbox_secret': secret})
        self._write_lockbox_keyring(secret)


class RawRotationTarget(RotationTarget):
    """An OSD deployed with ``ceph-volume raw`` (LUKS2 header driven)."""

    def __init__(self, osd_fsid: str) -> None:
        # deferred import: devices.raw.list pulls in the CLI layer
        from ceph_volume.devices.raw.list import direct_report
        from ceph_volume.util.raw_osd_crypt_mappers import RawOsdCryptMappers
        report = direct_report()
        meta = report.get(osd_fsid)
        if meta is None:
            raise RuntimeError(f'could not find a raw OSD with fsid {osd_fsid}')
        self.osd_id = str(meta['osd_id'])
        self.osd_fsid = osd_fsid
        self.devices = []
        for key in ('device', 'device_db', 'device_wal'):
            path = meta.get(key, '')
            if not path:
                continue
            backing = RawOsdCryptMappers.backing_device_path(path)
            if not backing:
                raise RuntimeError(f'could not resolve backing device of {path}')
            self.devices.append(backing)
        block_luks = encryption_utils.CephLuks2(self.devices[0])
        self.encrypted = block_luks.is_ceph_encrypted
        self.with_tpm = self.encrypted and block_luks.is_tpm2_enrolled

    def update_lockbox_secret(self, secret: str) -> None:
        # raw OSDs keep no LV tags; only the on-disk lockbox keyring (used as
        # config-key fallback when no env secret is passed) needs a refresh
        self._write_lockbox_keyring(secret)


class DmcryptRotator:
    """The rotation state machine. See the module docstring."""

    def __init__(self,
                 target: RotationTarget,
                 custody: KeyCustody,
                 phase: Optional[str] = None,
                 force: bool = False,
                 new_lockbox_secret: Optional[str] = None) -> None:
        self.target = target
        self.custody = custody
        self.phase = phase
        self.force = force
        self.new_lockbox_secret = new_lockbox_secret

    def rotate(self) -> None:
        with _osd_rotation_lock(self.target.osd_fsid):
            self._rotate()

    def _rotate(self) -> None:
        self._refuse_unsupported()
        # apply a rotated lockbox cephx secret before anything reads the
        # config-key store: after `ceph auth rotate client.osd-lockbox.<fsid>`
        # the on-disk lockbox keyring and LV tag are stale
        if self.new_lockbox_secret:
            mlogger.info('Updating lockbox cephx secret (keyring and tags)')
            self.target.update_lockbox_secret(self.new_lockbox_secret)

        if self.phase == 'finish':
            # reduced S0, then straight to S4/S5 (skips S1-S3)
            key = self.custody.get_current_key()
            self._assert_key_opens_all(key)
            self._cleanup(key)
            self._report()
            return

        current_key = self._resolve_current_key()   # S0
        self._check_unexpected_slots()              # S0
        self._stage_current(current_key)            # S1
        new_key = self.custody.get_new_key()
        self._install_new(current_key, new_key)     # S2
        if self.phase == 'stage':
            mlogger.info(
                'Rotation staged: both the previous and the new passphrase '
                'open all devices. Store the new passphrase, then run '
                '--phase finish with it.')
            self._report()
            return
        self.custody.persist_new_key(new_key)       # S3
        self.custody.verify_persisted(new_key)      # S3
        self._cleanup(new_key)                      # S4
        self._report()                              # S5

    def _refuse_unsupported(self) -> None:
        if self.target.with_tpm:
            raise RuntimeError(
                f'OSD {self.target.osd_fsid} is TPM2-enrolled: its passphrase '
                'is never stored on the monitors, so it does not need this '
                'rotation. Use systemd-cryptenroll to re-enroll if desired.')
        if not self.target.encrypted:
            raise RuntimeError(f'OSD {self.target.osd_fsid} is not encrypted')
        if not self.target.devices:
            raise RuntimeError(
                f'no encrypted devices found for OSD {self.target.osd_fsid}')
        for device in self.target.devices:
            # raises on non-LUKS devices (e.g. ceph-disk plain mode dmcrypt,
            # which has no keyslots and cannot be rotated)
            info = encryption_utils.get_luks_keyslots(device)
            logger.info('device %s: LUKS%s, active keyslots: %s',
                        device, info['version'], info['slots'])

    def _resolve_current_key(self) -> str:
        key = self.custody.get_current_key()
        if not key:
            raise RuntimeError(
                f'no current dmcrypt key available (is {CURRENT_KEY_ENV} set?)')
        for device in self.target.devices:
            if encryption_utils.luks_test_passphrase(key, device):
                continue
            new_key = os.environ.get(NEW_KEY_ENV, '')
            if new_key and encryption_utils.luks_test_passphrase(new_key,
                                                                 device):
                raise RuntimeError(
                    f'the current key does not open {device} but the new key '
                    'does: a previous rotation was interrupted after the key '
                    'was persisted. Run --phase finish with the stored '
                    'passphrase.')
            raise RuntimeError(
                f'the current dmcrypt key does not open {device}; refusing '
                'to rotate')
        return key

    def _check_unexpected_slots(self) -> None:
        expected = {SLOT_CANONICAL, SLOT_STAGING}
        for device in self.target.devices:
            info = encryption_utils.get_luks_keyslots(device)
            unexpected = [s for s in info['slots'] if s not in expected]
            if unexpected and not self.force:
                raise RuntimeError(
                    f'device {device} has unexpected active LUKS keyslots '
                    f'{unexpected} (expected only {sorted(expected)}). These '
                    'were not created by ceph-volume; pass --force to rotate '
                    'anyway (the extra slots are left untouched).')

    def _stage_current(self, current_key: str) -> None:
        for device in self.target.devices:
            if encryption_utils.luks_test_passphrase(current_key, device,
                                                     slot=SLOT_STAGING):
                continue
            mlogger.info(f'Staging current key in slot {SLOT_STAGING} of {device}')
            encryption_utils.luks_kill_slot(current_key, device, SLOT_STAGING)
            encryption_utils.luks_add_key(current_key, current_key, device,
                                          slot=SLOT_STAGING)

    def _install_new(self, current_key: str, new_key: str) -> None:
        for device in self.target.devices:
            if encryption_utils.luks_test_passphrase(new_key, device,
                                                     slot=SLOT_CANONICAL):
                continue
            mlogger.info(f'Installing new key in slot {SLOT_CANONICAL} of {device}')
            encryption_utils.luks_kill_slot(current_key, device, SLOT_CANONICAL)
            encryption_utils.luks_add_key(current_key, new_key, device,
                                          slot=SLOT_CANONICAL)

    def _assert_key_opens_all(self, key: str) -> None:
        for device in self.target.devices:
            if not encryption_utils.luks_test_passphrase(key, device):
                raise RuntimeError(
                    f'the persisted dmcrypt key does not open {device}; '
                    'refusing to remove the previous key')

    def _cleanup(self, key: str) -> None:
        self._assert_key_opens_all(key)
        for device in self.target.devices:
            mlogger.info(f'Removing previous key from slot {SLOT_STAGING} of {device}')
            encryption_utils.luks_kill_slot(key, device, SLOT_STAGING)

    def _report(self) -> None:
        for device in self.target.devices:
            info = encryption_utils.get_luks_keyslots(device)
            terminal.success(
                f'{device}: LUKS{info["version"]}, '
                f'active keyslots: {info["slots"]}')


def make_parser(prog: str, description: str, mode: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )
    if mode == 'lvm':
        parser.add_argument(
            '--osd-id',
            help='OSD id to rotate the dmcrypt passphrase for',
        )
    parser.add_argument(
        '--osd-fsid',
        required=(mode == 'raw'),
        help='OSD fsid to rotate the dmcrypt passphrase for',
    )
    parser.add_argument(
        '--key-store',
        choices=['mon', 'external'],
        default='mon',
        help='where the passphrase is stored: "mon" (default) is the monitor '
             'config-key store and ceph-volume generates and stores the new '
             'passphrase itself; "external" means the caller owns it and '
             f'passes it via {CURRENT_KEY_ENV} and {NEW_KEY_ENV}, which '
             'requires the two-phase flow',
    )
    parser.add_argument(
        '--phase',
        choices=['stage', 'finish'],
        help='run one phase of the external two-phase flow instead of the '
             'whole rotation: "stage" installs the new passphrase and keeps '
             'the previous one valid, "finish" removes the previous '
             'passphrase once the new one is stored (slot 1 is always '
             'cleared, it is owned by the rotation protocol)',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='rotate even if keyslots other than 0 and 1 are active',
    )
    parser.add_argument(
        '--name',
        help='cephx entity used against the config-key store '
             '(default: the OSD lockbox entity)',
    )
    parser.add_argument(
        '--keyring',
        help='keyring used against the config-key store '
             '(default: the OSD lockbox keyring)',
    )
    return parser


def _custody_from_args(args: argparse.Namespace,
                       target: RotationTarget) -> KeyCustody:
    """Build the key store named by --key-store and validate its inputs."""
    current_key = os.environ.get(CURRENT_KEY_ENV, '')
    new_key = os.environ.get(NEW_KEY_ENV, '')

    if args.key_store == 'external':
        if not args.phase:
            raise RuntimeError(
                '--key-store external requires the two-phase flow: run '
                '--phase stage, store the new passphrase, then run --phase '
                'finish with it. A single pass would remove the previous '
                'passphrase before the new one is safely stored.')
        if not current_key:
            raise RuntimeError(
                f'--key-store external requires {CURRENT_KEY_ENV} to hold the '
                'passphrase your key store currently has')
        if args.phase == 'stage' and not new_key:
            raise RuntimeError(
                f'--phase stage requires {NEW_KEY_ENV} to hold the new '
                'passphrase')
        return ExternalKeyCustody(current_key, new_key or None)

    if current_key or new_key:
        raise RuntimeError(
            f'{CURRENT_KEY_ENV}/{NEW_KEY_ENV} are set but the monitor '
            'config-key store was selected; pass --key-store external to '
            'rotate with the passphrases from the environment, or unset them')
    if args.phase == 'stage':
        raise RuntimeError(
            '--phase stage is only meaningful with --key-store external: the '
            'generated passphrase would never be stored anywhere. With the '
            'passphrase stored on the monitors, omit --phase — one invocation '
            'performs the whole rotation.')
    return ConfigKeyCustody(target.osd_id,
                            target.osd_fsid,
                            name=args.name,
                            keyring=args.keyring)


def rotate_from_args(args: argparse.Namespace, mode: str) -> None:
    if mode == 'lvm':
        if not args.osd_id and not args.osd_fsid:
            raise RuntimeError('pass at least one of --osd-id and --osd-fsid')
        target: RotationTarget = LvmRotationTarget(osd_id=args.osd_id,
                                                   osd_fsid=args.osd_fsid)
    else:
        target = RawRotationTarget(args.osd_fsid)

    custody = _custody_from_args(args, target)

    DmcryptRotator(
        target,
        custody,
        phase=args.phase,
        force=args.force,
        new_lockbox_secret=os.environ.get(LOCKBOX_SECRET_ENV) or None,
    ).rotate()
