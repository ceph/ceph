from unittest import mock

from ..common import get_rbd_pools


def _pool(pool_id, name, pool_type, flags_names='', apps=('rbd',)):
    return {
        'pool': pool_id,
        'pool_name': name,
        'type': pool_type,
        'flags_names': flags_names,
        'application_metadata': {app: {} for app in apps},
    }


def test_get_rbd_pools():
    module = mock.Mock()
    module.get.return_value = {'pools': [
        _pool(1, 'rbd', 1, 'hashpspool,selfmanaged_snaps'),
        _pool(2, 'rbd-data', 3, 'hashpspool,ec_overwrites'),
        _pool(3, 'rbd-fast-ec', 3,
              'hashpspool,ec_overwrites,ec_optimizations,supports_omap'),
        _pool(4, 'cephfs', 1, 'hashpspool', apps=('cephfs',)),
        _pool(5, 'untagged', 1, 'hashpspool', apps=()),
    ]}

    assert get_rbd_pools(module) == {1: 'rbd', 3: 'rbd-fast-ec'}
    assert get_rbd_pools(module, include_data_pools=True) == {
        1: 'rbd', 2: 'rbd-data', 3: 'rbd-fast-ec'}
    module.get.assert_called_with('osd_map')
