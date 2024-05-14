from unittest import mock

import smb.rados_store


def rados_mock():
    import rados

    m = mock.MagicMock()
    m._fake_store = {}
    ioctx = m.open_ioctx.return_value

    def _read(key, *args):
        return m._fake_store[ioctx._current_ns, key].encode('utf8')

    def _set_ns(ns):
        ioctx._current_ns = ns

    def _write_full(key, data):
        m._fake_store[ioctx._current_ns, key] = data

    def _list(*args):
        return [mock.MagicMock(nspace=a, key=b) for a, b in m._fake_store]

    def _stat(key):
        if (ioctx._current_ns, key) not in m._fake_store:
            raise rados.ObjectNotFound

    def _remove_object(key):
        del m._fake_store[ioctx._current_ns, key]

    ioctx.__enter__.return_value = ioctx
    ioctx.list_objects.side_effect = _list
    ioctx.set_namespace.side_effect = _set_ns
    ioctx.read.side_effect = _read
    ioctx.write_full.side_effect = _write_full
    ioctx.stat.side_effect = _stat
    ioctx.remove_object.side_effect = _remove_object

    m._fake_store[
        'foo', 'one'
    ] = """
        {"mocked": true, "silly": "very", "name": "one"}
    """
    m._fake_store[
        'bar', 'two'
    ] = """
        {"mocked": true, "silly": "very", "name": "two"}
    """
    m._fake_store[
        'bar', 'three'
    ] = """
        {"mocked": true, "silly": "very", "name": "three"}
    """

    return m


def test_mocked_rados_store_iteration():
    r = rados_mock()
    store = smb.rados_store.RADOSConfigStore(r)
    ekeys = list(store)
    assert ekeys == [('foo', 'one'), ('bar', 'two'), ('bar', 'three')]
    assert sorted(store.namespaces()) == ['bar', 'foo']
    assert list(store.contents('foo')) == ['one']
    assert list(store.contents('bar')) == ['two', 'three']


def test_mocked_rados_store_get_entry():
    r = rados_mock()
    store = smb.rados_store.RADOSConfigStore(r)
    entry = store['foo', 'one']
    assert entry.uri == 'rados://.smb/foo/one'
    assert entry.full_key == ('foo', 'one')
    data = entry.get()
    assert isinstance(data, dict)
    assert data['mocked']
    assert data['silly'] == 'very'
    assert data['name'] == 'one'


def test_mocked_rados_store_set_get_entry():
    r = rados_mock()
    store = smb.rados_store.RADOSConfigStore(r)
    entry = store['foo', 'four']
    assert entry.uri == 'rados://.smb/foo/four'
    assert entry.full_key == ('foo', 'four')
    entry.set(
        dict(
            mocked=False,
            silly='walks',
            name='four',
        )
    )
    assert list(store.contents('foo')) == ['one', 'four']


def test_mocked_rados_store_entry_exists():
    r = rados_mock()
    store = smb.rados_store.RADOSConfigStore(r)
    entry = store['bar', 'two']
    assert entry.uri == 'rados://.smb/bar/two'
    assert entry.full_key == ('bar', 'two')
    assert entry.exists()

    entry = store['bar', 'seven']
    assert entry.uri == 'rados://.smb/bar/seven'
    assert entry.full_key == ('bar', 'seven')
    assert not entry.exists()


def test_mocked_rados_store_entry_remove():
    r = rados_mock()
    store = smb.rados_store.RADOSConfigStore(r)
    store.remove(('bar', 'three'))
    assert list(store) == [('foo', 'one'), ('bar', 'two')]
