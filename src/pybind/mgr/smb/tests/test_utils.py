import pytest

import smb.utils


def test_one():
    assert smb.utils.one(['a']) == 'a'
    with pytest.raises(ValueError):
        smb.utils.one([])
    with pytest.raises(ValueError):
        smb.utils.one(['a', 'b'])


def test_rand_name():
    name = smb.utils.rand_name('bob')
    assert name.startswith('bob')
    assert len(name) == 11
    name = smb.utils.rand_name('carla')
    assert name.startswith('carla')
    assert len(name) == 13
    name = smb.utils.rand_name('dangeresque')
    assert name.startswith('dangeresqu')
    assert len(name) == 18
    name = smb.utils.rand_name('fhqwhgadsfhqwhgadsfhqwhgads')
    assert name.startswith('fhqwhgadsf')
    assert len(name) == 18
    name = smb.utils.rand_name('')
    assert len(name) == 8


def test_checked():
    assert smb.utils.checked('foo') == 'foo'
    assert smb.utils.checked(77) == 77
    assert smb.utils.checked(0) == 0
    with pytest.raises(smb.utils.IsNoneError):
        smb.utils.checked(None)


def test_ynbool():
    assert smb.utils.ynbool(True) == 'Yes'
    assert smb.utils.ynbool(False) == 'No'
    # for giggles
    assert smb.utils.ynbool(0) == 'No'


def test_compute_credential_hash():
    # Test determinism: same inputs produce same hash
    hash1 = smb.utils.compute_credential_hash('cluster1', 'alice')
    hash2 = smb.utils.compute_credential_hash('cluster1', 'alice')
    assert hash1 == hash2

    # Test format: should start with "cred-" and have 12 hex chars
    assert hash1.startswith('cred-')
    assert len(hash1) == len('cred-') + 12
    assert all(c in '0123456789abcdef' for c in hash1[5:])

    # Test different users produce different hashes
    hash_different_user = smb.utils.compute_credential_hash('cluster1', 'bob')
    assert hash1 != hash_different_user

    # Test different clusters produce different hashes
    hash_different_cluster = smb.utils.compute_credential_hash(
        'cluster2', 'alice'
    )
    assert hash1 != hash_different_cluster

    # Test tenant-aware users produce different hashes
    hash_tenant1 = smb.utils.compute_credential_hash(
        'cluster1', 'tenant1$user'
    )
    hash_tenant2 = smb.utils.compute_credential_hash(
        'cluster1', 'tenant2$user'
    )
    assert hash_tenant1 != hash_tenant2

    # Test with actual RGW-like format
    hash_rgw = smb.utils.compute_credential_hash('rgwtest', 'exouser')
    assert hash_rgw.startswith('cred-')
    assert len(hash_rgw) == 17
