import logging
import sqlite3

import pytest

import smb


class _FakeDBAccessor:
    def __init__(self) -> None:
        self.db = sqlite3.connect(':memory:', isolation_level=None)


def _plain_store():
    return smb.sqlite_store.mgr_sqlite3_db(_FakeDBAccessor())


def _mirroring_store(opts=None):
    mirror_target = smb.config_store.MemConfigStore()
    store = smb.sqlite_store.mgr_sqlite3_db_with_mirroring(
        _FakeDBAccessor(), mirror_target, opts
    )
    return store, mirror_target


def test_basic_crud():
    store = _plain_store()
    key = ('clusters', 'smb1')

    store.set_object(key, {'cluster_id': 'smb1', 'auth_mode': 'user'})
    assert store.get_object(key) == {
        'cluster_id': 'smb1',
        'auth_mode': 'user',
    }

    with pytest.raises(KeyError):
        store.get_object(('clusters', 'missing'))
    assert store.remove(('clusters', 'missing')) is False

    assert store.remove(key) is True
    with pytest.raises(KeyError):
        store.get_object(key)


def test_listing_and_iteration():
    store = _plain_store()
    store.set_object(('clusters', 'smb1'), {'cluster_id': 'smb1'})
    store.set_object(('clusters', 'smb2'), {'cluster_id': 'smb2'})
    store.set_object(('join_auths', 'auth1'), {'auth': {}})

    assert store.contents('clusters') == {'smb1', 'smb2'}
    assert 'clusters' in store.namespaces()
    assert set(store) == {
        ('clusters', 'smb1'),
        ('clusters', 'smb2'),
        ('join_auths', 'auth1'),
    }


def test_share_find():
    store = _plain_store()
    store.set_object(
        ('shares', 'smb1/share1'),
        {'cluster_id': 'smb1', 'share_id': 'share1', 'name': 'share1'},
    )
    store.set_object(
        ('shares', 'smb1/share2'),
        {'cluster_id': 'smb1', 'share_id': 'share2', 'name': 'share2'},
    )
    found = store.find_entries(
        'shares', {'cluster_id': 'smb1', 'name': 'share2'}
    )
    assert len(found) == 1
    assert found[0].get()['share_id'] == 'share2'


def test_mirror_write_and_get():
    store, mirror_target = _mirroring_store()
    key = ('join_auths', 'auth1')
    store.set_object(key, {'auth': {'username': 'foo', 'password': 'secret'}})

    assert mirror_target[key].get()['auth']['password'] == 'secret'
    raw = smb.sqlite_store.SqliteStore.get_object(store, key)
    assert 'password' not in raw['auth']
    assert store.get_object(key)['auth']['password'] == 'secret'


def test_users_and_groups_mirror_write_and_get():
    store, _ = _mirroring_store()
    key = ('users_and_groups', 'ug1')
    obj = {
        'values': {
            'users': [{'name': 'u1', 'password': 'secret'}],
            'groups': [],
        }
    }
    store.set_object(key, obj)

    raw = smb.sqlite_store.SqliteStore.get_object(store, key)
    assert raw['values']['users'][0]['password'] == ''
    assert store.get_object(key)['values']['users'][0]['password'] == 'secret'


def test_mirror_opt_out():
    store, mirror_target = _mirroring_store(opts={'mirror_join_auths': 'no'})
    key = ('join_auths', 'auth1')
    store.set_object(key, {'auth': {'username': 'foo', 'password': 'secret'}})

    assert list(mirror_target) == []
    assert store.get_object(key)['auth']['password'] == 'secret'


def test_remove_mirrors_deletion():
    store, mirror_target = _mirroring_store()
    mirrored_key = ('join_auths', 'auth1')
    unmirrored_key = ('clusters', 'smb1')
    store.set_object(
        mirrored_key, {'auth': {'username': 'foo', 'password': 'secret'}}
    )
    store.set_object(unmirrored_key, {'cluster_id': 'smb1'})
    assert mirrored_key in list(mirror_target)

    assert store.remove(mirrored_key) is True
    assert mirrored_key not in list(mirror_target)
    assert store.remove(mirrored_key) is False

    assert store.remove(unmirrored_key) is True
    assert list(mirror_target) == []


def test_remove_in_transaction_commits():
    store, mirror_target = _mirroring_store()
    key = ('join_auths', 'auth1')
    store.set_object(key, {'auth': {'username': 'foo', 'password': 'secret'}})

    with store.transaction():
        assert store.remove(key) is True
        assert key in list(mirror_target)

    assert key not in list(mirror_target)


def test_remove_in_transaction_rollback():
    store, mirror_target = _mirroring_store()
    key = ('join_auths', 'auth1')
    store.set_object(key, {'auth': {'username': 'foo', 'password': 'secret'}})

    with pytest.raises(RuntimeError):
        with store.transaction():
            assert store.remove(key) is True
            raise RuntimeError('later step in the same batch failed')

    assert store.get_object(key)['auth']['password'] == 'secret'
    assert key in list(mirror_target)


def test_remove_then_recreate_keeps_mirror():
    store, mirror_target = _mirroring_store()
    key = ('join_auths', 'auth1')
    store.set_object(key, {'auth': {'username': 'foo', 'password': 'old'}})

    with store.transaction():
        assert store.remove(key) is True
        store.set_object(
            key, {'auth': {'username': 'foo', 'password': 'new'}}
        )

    assert key in list(mirror_target)
    assert mirror_target[key].get()['auth']['password'] == 'new'
    assert store.get_object(key)['auth']['password'] == 'new'


def test_reconcile_fixes_drift():
    store, mirror_target = _mirroring_store()
    good_key = ('join_auths', 'auth1')
    store.set_object(good_key, {'auth': {'username': 'foo', 'password': 's'}})
    orphan_auth = ('join_auths', 'orphan-auth')
    orphan_ug = ('users_and_groups', 'orphan-ug')
    mirror_target[orphan_auth].set({'auth': {'username': 'y'}})
    mirror_target[orphan_ug].set({'values': {'users': [], 'groups': []}})
    unrecoverable_key = ('join_auths', 'auth2')
    smb.sqlite_store.SqliteStore.set_object(
        store, unrecoverable_key, {'auth': {'username': 'bar'}}
    )

    store.reconcile()

    assert set(mirror_target) == {good_key}
    assert store.get_object(good_key)['auth']['password'] == 's'
    assert smb.sqlite_store.SqliteStore.get_object(
        store, unrecoverable_key
    ) == {'auth': {'username': 'bar'}}


def test_reconcile_logs_unrecoverable_entries(caplog):
    store, _ = _mirroring_store()
    key = ('join_auths', 'auth1')
    smb.sqlite_store.SqliteStore.set_object(store, key, {'auth': {}})

    with caplog.at_level(logging.WARNING):
        store.reconcile()

    assert 'no mirror entry' in caplog.text


def test_reconcile_no_mirrors():
    store, mirror_target = _mirroring_store(
        opts={
            'mirror_join_auths': 'no',
            'mirror_users_and_groups': 'no',
            'mirror_tls_credentials': 'no',
            'mirror_external_ceph_clusters': 'no',
            'mirror_rgw_credentials': 'no',
        }
    )
    store.reconcile()
    assert list(mirror_target) == []
