import argparse
import pytest
from typing import Dict, List, Optional

from ceph_volume.util import dmcrypt_rotation
from ceph_volume.util import encryption
from ceph_volume.util.dmcrypt_rotation import (
    ConfigKeyCustody,
    DmcryptRotator,
    ExternalKeyCustody,
    KeyCustody,
    RotationTarget,
    SLOT_CANONICAL,
    SLOT_STAGING,
    rotate_from_args,
)


class FakeLuks:
    """
    In-memory simulation of LUKS keyslot semantics, faithful to cryptsetup:

    - a passphrase test succeeds if the key matches any (or the given) slot
    - luksAddKey authenticates with an existing key and refuses a full slot
    - luksKillSlot authenticates against the *other* slots only, and an
      inactive slot is not an error
    """

    def __init__(self, devices: Dict[str, Dict[int, str]]) -> None:
        self.devices = devices

    def test_passphrase(self, key: str, device: str, slot: Optional[int] = None) -> bool:
        slots = self.devices[device]
        if slot is not None:
            return slots.get(slot) == key
        return key in slots.values()

    def add_key(self, current_key: str, new_key: str, device: str, slot: Optional[int] = None) -> None:
        slots = self.devices[device]
        if current_key not in slots.values():
            raise RuntimeError(f'No key available with this passphrase on {device}')
        if slot in slots:
            raise RuntimeError(f'Key slot {slot} is full on {device}')
        slots[slot] = new_key

    def kill_slot(self, auth_key: str, device: str, slot: int) -> None:
        slots = self.devices[device]
        if slot not in slots:
            return
        remaining = {s: k for s, k in slots.items() if s != slot}
        if auth_key not in remaining.values():
            raise RuntimeError(f'No key available with this passphrase on {device}')
        del slots[slot]

    def keyslots(self, device: str) -> Dict[str, object]:
        return {'version': 2, 'slots': sorted(self.devices[device])}

    def patch_into(self, monkeypatch) -> None:
        monkeypatch.setattr(encryption, 'luks_test_passphrase', self.test_passphrase)
        monkeypatch.setattr(encryption, 'luks_add_key', self.add_key)
        monkeypatch.setattr(encryption, 'luks_kill_slot', self.kill_slot)
        monkeypatch.setattr(encryption, 'get_luks_keyslots', self.keyslots)


class FakeTarget(RotationTarget):
    def __init__(self, devices: List[str], encrypted: bool = True, with_tpm: bool = False) -> None:
        self.osd_id = '0'
        self.osd_fsid = 'aaaa-bbbb'
        self.devices = devices
        self.encrypted = encrypted
        self.with_tpm = with_tpm
        self.lockbox_updates: List[str] = []

    def update_lockbox_secret(self, secret: str) -> None:
        self.lockbox_updates.append(secret)


class FakeCustody(KeyCustody):
    def __init__(self, stored: str, new: str = 'NEW') -> None:
        self.stored = stored
        self.new = new
        self.persist_calls: List[str] = []
        self.events: List[str] = []

    def get_current_key(self) -> str:
        self.events.append('get_current')
        return self.stored

    def get_new_key(self) -> str:
        return self.new

    def persist_new_key(self, key: str) -> None:
        self.persist_calls.append(key)
        self.stored = key

    def verify_persisted(self, key: str) -> None:
        assert self.stored == key


class TestRotationFullPass:
    def test_single_device(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        assert luks.devices['/dev/foo'] == {0: 'NEW'}
        assert custody.stored == 'NEW'
        assert custody.persist_calls == ['NEW']

    def test_multi_device(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}, '/dev/bar': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo', '/dev/bar']), custody).rotate()
        assert luks.devices['/dev/foo'] == {0: 'NEW'}
        assert luks.devices['/dev/bar'] == {0: 'NEW'}
        assert custody.stored == 'NEW'

    def test_persist_not_reached_when_a_device_fails(self, monkeypatch):
        # second device rejects the current key: nothing may be persisted
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}, '/dev/bar': {0: 'SOMETHING-ELSE'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        with pytest.raises(RuntimeError):
            DmcryptRotator(FakeTarget(['/dev/foo', '/dev/bar']), custody).rotate()
        assert custody.persist_calls == []
        # the invariant holds: the stored key still opens the untouched device
        assert luks.test_passphrase('OLD', '/dev/foo')

    def test_rotation_works_on_luks1(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        original_keyslots = luks.keyslots
        def luks1_keyslots(device):
            result = original_keyslots(device)
            result['version'] = 1
            return result
        luks.patch_into(monkeypatch)
        monkeypatch.setattr(encryption, 'get_luks_keyslots', luks1_keyslots)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        assert luks.devices['/dev/foo'] == {0: 'NEW'}


class TestRotationPhases:
    def test_stage_keeps_both_passphrases(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody, phase='stage').rotate()
        assert luks.devices['/dev/foo'] == {SLOT_CANONICAL: 'NEW', SLOT_STAGING: 'OLD'}
        assert custody.persist_calls == []

    def test_finish_removes_staging_slot(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {SLOT_CANONICAL: 'NEW', SLOT_STAGING: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('NEW')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody, phase='finish').rotate()
        assert luks.devices['/dev/foo'] == {SLOT_CANONICAL: 'NEW'}

    def test_finish_refuses_non_opening_passphrase(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {SLOT_CANONICAL: 'OTHER', SLOT_STAGING: 'ALSO-OTHER'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('NEW')
        with pytest.raises(RuntimeError):
            DmcryptRotator(FakeTarget(['/dev/foo']), custody, phase='finish').rotate()
        assert SLOT_STAGING in luks.devices['/dev/foo']


class TestCrashRecovery:
    def test_resume_after_staging(self, monkeypatch):
        # crashed after S1: current key in both slots
        luks = FakeLuks({'/dev/foo': {0: 'OLD', 1: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        assert luks.devices['/dev/foo'] == {0: 'NEW'}
        assert custody.stored == 'NEW'

    def test_resume_after_install_before_persist(self, monkeypatch):
        # crashed after S2: an unpersisted new key sits in slot 0, custody
        # still holds the old key; the re-run generates a different new key
        luks = FakeLuks({'/dev/foo': {0: 'LOST-NEW', 1: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD', new='NEW-2')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        assert luks.devices['/dev/foo'] == {0: 'NEW-2'}
        assert custody.stored == 'NEW-2'

    def test_resume_after_persist_before_cleanup(self, monkeypatch):
        # crashed after S3: new key persisted, old key still staged
        luks = FakeLuks({'/dev/foo': {0: 'NEW', 1: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('NEW', new='NEW-2')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        # a full re-run rotates again, which must converge and end clean
        assert luks.devices['/dev/foo'] == {0: 'NEW-2'}
        assert custody.stored == 'NEW-2'

    def test_external_crash_after_caller_persisted(self, monkeypatch):
        # external flow: caller stored the new passphrase but --phase finish
        # never ran; a fresh stage attempt with the *new* passphrase as
        # current must point the operator at --phase finish instead of
        # failing obscurely
        luks = FakeLuks({'/dev/foo': {0: 'NEW', 1: 'OLD'}})
        luks.patch_into(monkeypatch)
        monkeypatch.setenv(dmcrypt_rotation.NEW_KEY_ENV, 'NEW')
        custody = ExternalKeyCustody('OLD-GONE', 'NEW')
        # OLD-GONE opens nothing; NEW opens -> targeted error message
        luks.devices['/dev/foo'] = {0: 'NEW', 1: 'ALSO-GONE'}
        with pytest.raises(RuntimeError) as error:
            DmcryptRotator(FakeTarget(['/dev/foo']), custody, phase='stage').rotate()
        assert '--phase finish' in str(error.value)


class TestPrechecks:
    def test_refuses_tpm2(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        with pytest.raises(RuntimeError) as error:
            DmcryptRotator(FakeTarget(['/dev/foo'], with_tpm=True), custody).rotate()
        assert 'TPM2' in str(error.value)

    def test_refuses_unencrypted(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        with pytest.raises(RuntimeError):
            DmcryptRotator(FakeTarget(['/dev/foo'], encrypted=False), custody).rotate()

    def test_refuses_non_opening_current_key(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'SOMETHING-ELSE'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        with pytest.raises(RuntimeError):
            DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()

    def test_refuses_unexpected_slots(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD', 5: 'MYSTERY'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        with pytest.raises(RuntimeError) as error:
            DmcryptRotator(FakeTarget(['/dev/foo']), custody).rotate()
        assert '5' in str(error.value)

    def test_force_rotates_despite_unexpected_slots(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD', 5: 'MYSTERY'}})
        luks.patch_into(monkeypatch)
        custody = FakeCustody('OLD')
        DmcryptRotator(FakeTarget(['/dev/foo']), custody, force=True).rotate()
        # rotated, and the foreign slot is left untouched
        assert luks.devices['/dev/foo'] == {0: 'NEW', 5: 'MYSTERY'}


class TestLockboxSecretUpdate:
    def test_lockbox_updated_before_custody_read(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        target = FakeTarget(['/dev/foo'])
        events: List[str] = []

        class OrderedCustody(FakeCustody):
            def get_current_key(self) -> str:
                events.append('custody_read')
                return super().get_current_key()

        original_update = target.update_lockbox_secret
        def recording_update(secret: str) -> None:
            events.append('lockbox_update')
            original_update(secret)
        target.update_lockbox_secret = recording_update  # type: ignore[method-assign]

        custody = OrderedCustody('OLD')
        DmcryptRotator(target, custody,
                       new_lockbox_secret='new-lockbox-secret').rotate()
        assert target.lockbox_updates == ['new-lockbox-secret']
        assert events.index('lockbox_update') < events.index('custody_read')

    def test_no_lockbox_update_without_secret(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        target = FakeTarget(['/dev/foo'])
        DmcryptRotator(target, FakeCustody('OLD')).rotate()
        assert target.lockbox_updates == []

    def test_lockbox_update_with_external_custody_phase_one(self, monkeypatch):
        # the documented combination: auth-rotated lockbox secret passed
        # into phase one of the external two-phase flow
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        target = FakeTarget(['/dev/foo'])
        DmcryptRotator(target, ExternalKeyCustody('OLD', 'NEW'),
                       phase='stage',
                       new_lockbox_secret='new-lockbox-secret').rotate()
        assert target.lockbox_updates == ['new-lockbox-secret']
        assert luks.devices['/dev/foo'] == {SLOT_CANONICAL: 'NEW',
                                            SLOT_STAGING: 'OLD'}


class TestConfigKeyCustody:
    def test_round_trip(self, monkeypatch):
        store = {}
        monkeypatch.setattr(encryption, 'get_dmcrypt_key',
                            lambda osd_id, osd_fsid, lockbox_keyring=None, name=None: store['key'])
        def fake_set(osd_id, osd_fsid, key, lockbox_keyring=None, name=None):
            store['key'] = key
        monkeypatch.setattr(encryption, 'set_dmcrypt_key', fake_set)
        monkeypatch.setattr(encryption, 'create_dmcrypt_key', lambda: 'GENERATED')
        store['key'] = 'OLD'
        custody = ConfigKeyCustody('0', 'aaaa')
        assert custody.get_current_key() == 'OLD'
        new = custody.get_new_key()
        assert new == 'GENERATED'
        custody.persist_new_key(new)
        custody.verify_persisted(new)
        assert store['key'] == 'GENERATED'

    def test_verify_detects_mismatch(self, monkeypatch):
        monkeypatch.setattr(encryption, 'get_dmcrypt_key',
                            lambda osd_id, osd_fsid, lockbox_keyring=None, name=None: 'TAMPERED')
        custody = ConfigKeyCustody('0', 'aaaa')
        with pytest.raises(RuntimeError):
            custody.verify_persisted('NEW')


class TestRotateFromArgs:
    def make_args(self, **kw) -> argparse.Namespace:
        defaults = dict(osd_id=None, osd_fsid='aaaa', key_store='mon',
                        phase=None, force=False, name=None, keyring=None)
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    def test_external_key_store_requires_two_phase(self, monkeypatch):
        monkeypatch.setenv(dmcrypt_rotation.CURRENT_KEY_ENV, 'OLD')
        monkeypatch.setenv(dmcrypt_rotation.NEW_KEY_ENV, 'NEW')
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        with pytest.raises(RuntimeError) as error:
            rotate_from_args(self.make_args(key_store='external'), 'raw')
        assert 'two-phase' in str(error.value)

    def test_key_store_external_uses_env_passphrases(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        monkeypatch.setenv(dmcrypt_rotation.CURRENT_KEY_ENV, 'OLD')
        monkeypatch.setenv(dmcrypt_rotation.NEW_KEY_ENV, 'NEW')
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        rotate_from_args(
            self.make_args(key_store='external', phase='stage'), 'raw')
        assert luks.devices['/dev/foo'] == {0: 'NEW', 1: 'OLD'}

    def test_env_passphrases_refused_with_mon_key_store(self, monkeypatch):
        monkeypatch.setenv(dmcrypt_rotation.CURRENT_KEY_ENV, 'OLD')
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        with pytest.raises(RuntimeError) as error:
            rotate_from_args(self.make_args(), 'raw')
        assert '--key-store external' in str(error.value)

    def test_external_key_store_requires_current_passphrase(self, monkeypatch):
        monkeypatch.delenv(dmcrypt_rotation.CURRENT_KEY_ENV, raising=False)
        monkeypatch.delenv(dmcrypt_rotation.NEW_KEY_ENV, raising=False)
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        with pytest.raises(RuntimeError) as error:
            rotate_from_args(
                self.make_args(key_store='external', phase='finish'), 'raw')
        assert dmcrypt_rotation.CURRENT_KEY_ENV in str(error.value)

    def test_stage_requires_the_new_passphrase(self, monkeypatch):
        monkeypatch.setenv(dmcrypt_rotation.CURRENT_KEY_ENV, 'OLD')
        monkeypatch.delenv(dmcrypt_rotation.NEW_KEY_ENV, raising=False)
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        with pytest.raises(RuntimeError) as error:
            rotate_from_args(
                self.make_args(key_store='external', phase='stage'), 'raw')
        assert dmcrypt_rotation.NEW_KEY_ENV in str(error.value)

    def test_key_store_mon_uses_config_key_store(self, monkeypatch):
        luks = FakeLuks({'/dev/foo': {0: 'OLD'}})
        luks.patch_into(monkeypatch)
        monkeypatch.delenv(dmcrypt_rotation.CURRENT_KEY_ENV, raising=False)
        monkeypatch.delenv(dmcrypt_rotation.NEW_KEY_ENV, raising=False)
        store = {'key': 'OLD'}
        monkeypatch.setattr(encryption, 'get_dmcrypt_key',
                            lambda osd_id, osd_fsid, lockbox_keyring=None, name=None: store['key'])
        def fake_set(osd_id, osd_fsid, key, lockbox_keyring=None, name=None):
            store['key'] = key
        monkeypatch.setattr(encryption, 'set_dmcrypt_key', fake_set)
        monkeypatch.setattr(encryption, 'create_dmcrypt_key', lambda: 'GENERATED')
        monkeypatch.setattr(dmcrypt_rotation, 'LvmRotationTarget',
                            lambda osd_id=None, osd_fsid=None: FakeTarget(['/dev/foo']))
        rotate_from_args(self.make_args(), 'lvm')
        assert luks.devices['/dev/foo'] == {0: 'GENERATED'}
        assert store['key'] == 'GENERATED'

    def test_lvm_requires_an_identifier(self):
        with pytest.raises(RuntimeError):
            rotate_from_args(self.make_args(osd_fsid=None), 'lvm')

    def test_parser_rejects_unknown_phase(self):
        parser = dmcrypt_rotation.make_parser('prog', 'desc', 'raw')
        with pytest.raises(SystemExit):
            parser.parse_args(['--osd-fsid', 'aaaa', '--phase', 'bogus'])

    def test_stage_requires_external_key_store(self, monkeypatch):
        monkeypatch.delenv(dmcrypt_rotation.CURRENT_KEY_ENV, raising=False)
        monkeypatch.delenv(dmcrypt_rotation.NEW_KEY_ENV, raising=False)
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        with pytest.raises(RuntimeError) as error:
            rotate_from_args(self.make_args(phase='stage'), 'raw')
        assert '--key-store external' in str(error.value)

    def test_finish_stays_legal_with_config_key_store(
            self, monkeypatch):
        # the documented recovery path after an interrupted rotation
        luks = FakeLuks({'/dev/foo': {0: 'NEW', 1: 'OLD'}})
        luks.patch_into(monkeypatch)
        monkeypatch.delenv(dmcrypt_rotation.CURRENT_KEY_ENV, raising=False)
        monkeypatch.delenv(dmcrypt_rotation.NEW_KEY_ENV, raising=False)
        monkeypatch.setattr(encryption, 'get_dmcrypt_key',
                            lambda osd_id, osd_fsid, **kw: 'NEW')
        monkeypatch.setattr(dmcrypt_rotation, 'RawRotationTarget',
                            lambda fsid: FakeTarget(['/dev/foo']))
        rotate_from_args(self.make_args(phase='finish'), 'raw')
        assert luks.devices['/dev/foo'] == {0: 'NEW'}
