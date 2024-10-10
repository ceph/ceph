import json
import pytest
from .ceph_volume_data import data
from cephadm.serve import CephadmServe
from mock import patch
from .fixtures import _run_cephadm, with_host


class TestCephVolumeList:
    def test_get_data(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.data == json.loads(data)

    def test_devices_by_type_block(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.devices_by_type('block')) == set(['/dev/vdb',
                                                                                                 '/dev/vdc',
                                                                                                 '/dev/vdg',
                                                                                                 '/dev/vde',
                                                                                                 '/dev/vdf',
                                                                                                 '/dev/vdh'])

    def test_devices_by_type_db(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.devices_by_type('db')) == set(['/dev/vdi',
                                                                                              '/dev/vdk'])

    def test_devices_by_type_wal(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.devices_by_type('wal') == ['/dev/vdj']

    def test_block_devices(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.block_devices()) == set(['/dev/vdb',
                                                                                        '/dev/vdc',
                                                                                        '/dev/vdg',
                                                                                        '/dev/vde',
                                                                                        '/dev/vdf',
                                                                                        '/dev/vdh'])

    def test_db_devices(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.db_devices()) == set(['/dev/vdk',
                                                                                     '/dev/vdi'])

    def test_wal_devices(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.wal_devices()) == set(['/dev/vdj'])

    def test_all_devices(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.all_devices()) == set(['/dev/vdg',
                                                                                      '/dev/vdj',
                                                                                      '/dev/vdh',
                                                                                      '/dev/vdi',
                                                                                      '/dev/vdc',
                                                                                      '/dev/vde',
                                                                                      '/dev/vdf',
                                                                                      '/dev/vdb',
                                                                                      '/dev/vdk'])

    def test_device_osd_mapping(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.device_osd_mapping() == {'/dev/vdb': {'osd_ids': ['0']},
                                                                                    '/dev/vdk': {'osd_ids': ['0', '1']},
                                                                                    '/dev/vdc': {'osd_ids': ['1']},
                                                                                    '/dev/vdf': {'osd_ids': ['2']},
                                                                                    '/dev/vde': {'osd_ids': ['3']},
                                                                                    '/dev/vdg': {'osd_ids': ['4']},
                                                                                    '/dev/vdj': {'osd_ids': ['4', '5']},
                                                                                    '/dev/vdi': {'osd_ids': ['4', '5']},
                                                                                    '/dev/vdh': {'osd_ids': ['5']}}

    def test_block_device_osd_mapping(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.block_device_osd_mapping() == {'/dev/vdb': {'osd_ids': ['0']},
                                                                                          '/dev/vdc': {'osd_ids': ['1']},
                                                                                          '/dev/vdf': {'osd_ids': ['2']},
                                                                                          '/dev/vde': {'osd_ids': ['3']},
                                                                                          '/dev/vdg': {'osd_ids': ['4']},
                                                                                          '/dev/vdh': {'osd_ids': ['5']}}

    def test_db_device_osd_mapping(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.db_device_osd_mapping() == {'/dev/vdk': {'osd_ids': ['0', '1']},
                                                                                       '/dev/vdi': {'osd_ids': ['4', '5']}}

    def test_wal_device_osd_mapping(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.wal_device_osd_mapping() == {'/dev/vdj': {'osd_ids': ['4', '5']}}

    def test_is_shared_device(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.is_shared_device('/dev/vdj')

    def test_is_shared_device_with_invalid_device(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    with pytest.raises(RuntimeError) as e:
                        assert cephadm_module.ceph_volume_list.is_shared_device('/dev/invalid-device')
                    assert str(e.value) == 'Not a valid device path.'

    def test_is_block_device(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.is_block_device('/dev/vdb')

    def test_is_db_device(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.is_db_device('/dev/vdk')

    def test_is_wal_device(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.is_wal_device('/dev/vdj')

    def test_get_block_devices_from_osd_id(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert cephadm_module.ceph_volume_list.get_block_devices_from_osd_id('0') == ['/dev/vdb']

    def test_osd_ids(self, cephadm_module) -> None:  # type: ignore
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    cephadm_module.ceph_volume_list.get_data('test')
                    assert set(cephadm_module.ceph_volume_list.osd_ids()) == set(['0', '1', '2', '3', '4', '5'])
