# type: ignore
import os
import pytest
from copy import deepcopy
from unittest.mock import patch, call, Mock
from ceph_volume import process
from ceph_volume.api import lvm as api
from ceph_volume.devices.lvm import zap
from . import data_zap
from typing import Tuple, List


def process_call(command, **kw):
    result: Tuple[List[str], List[str], int] = ''
    if 'udevadm' in command:
        result = data_zap.udevadm_property, [], 0
    if 'ceph-bluestore-tool' in command:
        result = data_zap.ceph_bluestore_tool_output, [], 0
    if 'is-active' in command:
        result = [], [], 1
    if 'lsblk' in command:
        result = data_zap.lsblk_all, [], 0
    if 'blkid' in command:
        result = data_zap.blkid_output, [], 0
    if 'pvs' in command:
        result = [], [], 0
    return result


class TestZap:
    def test_invalid_osd_id_passed(self) -> None:
        with pytest.raises(SystemExit):
            zap.Zap(argv=['--osd-id', 'foo']).main()

    @patch('ceph_volume.util.disk._dd_write', Mock())
    @patch('ceph_volume.util.arg_validators.Device')
    def test_clear_replace_header_is_being_replaced(self, m_device: Mock) -> None:
        m_dev = m_device.return_value
        m_dev.is_being_replaced = True
        with pytest.raises(SystemExit) as e:
            zap.Zap(argv=['--clear', '/dev/foo']).main()
        assert e.value.code == 0

    @patch('ceph_volume.util.disk._dd_write', Mock())
    @patch('ceph_volume.util.arg_validators.Device')
    def test_clear_replace_header_is_not_being_replaced(self, m_device: Mock) -> None:
        m_dev = m_device.return_value
        m_dev.is_being_replaced = False
        with pytest.raises(SystemExit) as e:
            zap.Zap(argv=['--clear', '/dev/foo']).main()
        assert e.value.code == 1

    @patch('ceph_volume.devices.lvm.zap.direct_report', Mock(return_value={}))
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_no_lvs_and_raw_found_that_match_id(self, is_root, monkeypatch, device_info):
        tags = 'ceph.osd_id=9,ceph.journal_uuid=x,ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_tags=tags, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kwargs: {})

        z = zap.Zap(['--osd-id', '10'])

        with pytest.raises(SystemExit):
            z.main()

    @patch('ceph_volume.devices.lvm.zap.direct_report', Mock(return_value={}))
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_no_lvs_and_raw_found_that_match_fsid(self, is_root, monkeypatch):
        tags = 'ceph.osd_id=9,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,'+\
               'ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags=tags,
                         vg_name='vg', lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kwargs: {})

        z = zap.Zap(['--osd-fsid', 'aaaa-lkjh'])

        with pytest.raises(SystemExit):
            z.main()

    @patch('ceph_volume.devices.lvm.zap.direct_report', Mock(return_value={}))
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_no_lvs_and_raw_found_that_match_id_fsid(self, is_root, monkeypatch):
        tags = 'ceph.osd_id=9,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,'+\
               'ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_tags=tags, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kwargs: {})

        z = zap.Zap(['--osd-id', '9', '--osd-fsid', 'aaaa-lkjh'])

        with pytest.raises(SystemExit):
            z.main()

    @patch('ceph_volume.devices.lvm.zap.direct_report', Mock(return_value={}))
    def test_no_ceph_lvs_and_no_ceph_raw_found(self, is_root, monkeypatch):
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags='',
                         lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kwargs: {})

        z = zap.Zap(['--osd-id', '100'])

        with pytest.raises(SystemExit):
            z.main()

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_lv_is_matched_id(self, mock_zap, monkeypatch, is_root):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='',
                         lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes = [osd]
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)

        z = zap.Zap(['--osd-id', '0'])
        z.main()
        assert z.args.devices[0].path == '/dev/VolGroup/lv'
        mock_zap.assert_called_once()

    # @patch('ceph_volume.devices.lvm.zap.disk.has_bluestore_label', Mock(return_value=True))
    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_raw_is_matched_id(self, mock_zap, monkeypatch, is_root):
        volumes = []
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)

        z = zap.Zap(['--osd-id', '0'])
        z.main()
        assert z.args.devices[0].path == '/dev/sdb'
        mock_zap.assert_called_once()

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    def test_lv_is_matched_fsid(self, mock_zap, monkeypatch, is_root):
        tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,' +\
               'ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='',
                         lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes = [osd]
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: deepcopy(volumes))
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        z = zap.Zap(['--osd-fsid', 'asdf-lkjh'])
        z.main()

        assert z.args.devices[0].path == '/dev/VolGroup/lv'
        mock_zap.assert_called_once

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_raw_is_matched_fsid(self, mock_zap, monkeypatch, is_root):
        volumes = []
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)

        z = zap.Zap(['--osd-fsid', 'd5a496bc-dcb9-4ad0-a12c-393d3200d2b6'])
        z.main()

        assert z.args.devices[0].path == '/dev/sdb'
        mock_zap.assert_called_once

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    def test_lv_is_matched_id_fsid(self, mock_zap, monkeypatch, is_root):
        tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,' +\
               'ceph.type=data'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='',
                         lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        z = zap.Zap(['--osd-id', '0', '--osd-fsid', 'asdf-lkjh', '--no-systemd'])
        z.main()

        assert z.args.devices[0].path == '/dev/VolGroup/lv'
        mock_zap.assert_called_once

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(return_value='/dev/sdb'))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_raw_is_matched_id_fsid(self, mock_zap, monkeypatch, is_root):
        volumes = []
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)

        z = zap.Zap(['--osd-id', '0', '--osd-fsid', 'd5a496bc-dcb9-4ad0-a12c-393d3200d2b6'])
        z.main()

        assert z.args.devices[0].path == '/dev/sdb'
        mock_zap.assert_called_once

    @patch('ceph_volume.devices.lvm.zap.Zap.zap')
    @patch('ceph_volume.devices.raw.list.List.filter_lvm_osd_devices', Mock(side_effect=['/dev/vdx', '/dev/vdy', '/dev/vdz', None]))
    @patch('ceph_volume.process.call', Mock(side_effect=process_call))
    def test_raw_multiple_devices(self, mock_zap, monkeypatch, is_root):
        volumes = []
        monkeypatch.setattr(zap.api, 'get_lvs', lambda **kw: volumes)
        z = zap.Zap(['--osd-id', '5'])
        z.main()

        set([device.path for device in z.args.devices]) == {'/dev/vdx', '/dev/vdy', '/dev/vdz'}
        mock_zap.assert_called_once

    @patch('ceph_volume.devices.lvm.zap.direct_report', Mock(return_value={}))
    @patch('ceph_volume.devices.lvm.zap.api.get_lvs', Mock(return_value=[]))
    def test_nothing_is_found(self, is_root):
        z = zap.Zap(['--osd-id', '0'])
        with pytest.raises(SystemExit):
            z.main()

    def test_block_is_found(self, fake_call):
        tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,ceph.type=block'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', vg_name='', lv_path='/dev/VolGroup/block', lv_tags=tags)
        volumes = []
        volumes.append(osd)
        result = zap.Zap([]).ensure_associated_lvs(volumes)
        assert result == ['/dev/VolGroup/block']

    def test_success_message_for_fsid(self, factory, is_root, capsys):
        cli_zap = zap.Zap([])
        args = factory(devices=[], osd_id=None, osd_fsid='asdf-lkjh')
        cli_zap.args = args
        cli_zap.zap()
        out, err = capsys.readouterr()
        assert "Zapping successful for OSD: asdf-lkjh" in err

    def test_success_message_for_id(self, factory, is_root, capsys):
        cli_zap = zap.Zap([])
        args = factory(devices=[], osd_id='1', osd_fsid=None)
        cli_zap.args = args
        cli_zap.zap()
        out, err = capsys.readouterr()
        assert "Zapping successful for OSD: 1" in err

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_multiple_dbs_are_found(self):
        tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.journal_uuid=x,ceph.type=db'
        volumes = []
        for i in range(3):
            osd = api.Volume(
                lv_name='volume%s' % i, lv_uuid='y', vg_name='', lv_path='/dev/VolGroup/lv%s' % i, lv_tags=tags)
            volumes.append(osd)
        result = zap.Zap([]).ensure_associated_lvs(volumes)
        assert '/dev/VolGroup/lv0' in result
        assert '/dev/VolGroup/lv1' in result
        assert '/dev/VolGroup/lv2' in result

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_multiple_wals_are_found(self):
        tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.wal_uuid=x,ceph.type=wal'
        volumes = []
        for i in range(3):
            osd = api.Volume(
                lv_name='volume%s' % i, lv_uuid='y', vg_name='', lv_path='/dev/VolGroup/lv%s' % i, lv_tags=tags)
            volumes.append(osd)
        result = zap.Zap([]).ensure_associated_lvs(volumes)
        assert '/dev/VolGroup/lv0' in result
        assert '/dev/VolGroup/lv1' in result
        assert '/dev/VolGroup/lv2' in result

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_multiple_backing_devs_are_found(self):
        volumes = []
        for _type in ['journal', 'db', 'wal']:
            tags = 'ceph.osd_id=0,ceph.osd_fsid=asdf-lkjh,ceph.wal_uuid=x,ceph.type=%s' % _type
            osd = api.Volume(
                lv_name='volume%s' % _type, lv_uuid='y', vg_name='', lv_path='/dev/VolGroup/lv%s' % _type, lv_tags=tags)
            volumes.append(osd)
        result = zap.Zap([]).ensure_associated_lvs(volumes)
        assert '/dev/VolGroup/lvjournal' in result
        assert '/dev/VolGroup/lvwal' in result
        assert '/dev/VolGroup/lvdb' in result

    @patch('ceph_volume.devices.lvm.zap.api.get_lvs')
    def test_ensure_associated_lvs(self, m_get_lvs):
        zap.Zap([]).ensure_associated_lvs([], lv_tags={'ceph.osd_id': '1'})
        calls = [
            call(tags={'ceph.type': 'db', 'ceph.osd_id': '1'}),
            call(tags={'ceph.type': 'wal', 'ceph.osd_id': '1'})
        ]
        m_get_lvs.assert_has_calls(calls, any_order=True)


class TestWipeFs(object):

    def setup_method(self):
        os.environ['CEPH_VOLUME_WIPEFS_INTERVAL'] = '0'

    def test_works_on_second_try(self, stub_call):
        os.environ['CEPH_VOLUME_WIPEFS_TRIES'] = '2'
        stub_call([('wiping /dev/sda', '', 1), ('', '', 0)])
        result = zap.wipefs('/dev/sda')
        assert result is None

    def test_does_not_work_after_several_tries(self, stub_call):
        os.environ['CEPH_VOLUME_WIPEFS_TRIES'] = '2'
        stub_call([('wiping /dev/sda', '', 1), ('', '', 1)])
        with pytest.raises(RuntimeError):
            zap.wipefs('/dev/sda')

    def test_does_not_work_default_tries(self, stub_call):
        stub_call([('wiping /dev/sda', '', 1)]*8)
        with pytest.raises(RuntimeError):
            zap.wipefs('/dev/sda')
