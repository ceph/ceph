import pytest
from ceph_volume.devices import lvm
from ceph_volume.api import lvm as api
from mock import patch, Mock

# TODO: add tests for following commands -
# ceph-volume list
# ceph-volume list <path-to-pv>
# ceph-volume list <path-to-vg>
# ceph-volume list <path-to-lv>

class TestReadableTag(object):

    def test_dots_get_replaced(self):
        result = lvm.listing.readable_tag('ceph.foo')
        assert result == 'foo'

    def test_underscores_are_replaced_with_spaces(self):
        result = lvm.listing.readable_tag('ceph.long_tag')
        assert result == 'long tag'


class TestPrettyReport(object):

    def test_is_empty(self, capsys):
        lvm.listing.pretty_report({})
        stdout, stderr = capsys.readouterr()
        assert stdout == '\n'

    def test_type_and_path_are_reported(self, capsys):
        lvm.listing.pretty_report({0: [
            {'type': 'data', 'path': '/dev/sda1', 'devices': ['/dev/sda']}
        ]})
        stdout, stderr = capsys.readouterr()
        assert '[data]        /dev/sda1' in stdout

    def test_osd_id_header_is_reported(self, capsys):
        lvm.listing.pretty_report({0: [
            {'type': 'data', 'path': '/dev/sda1', 'devices': ['/dev/sda']}
        ]})
        stdout, stderr = capsys.readouterr()
        assert '====== osd.0 =======' in stdout

    def test_tags_are_included(self, capsys):
        lvm.listing.pretty_report(
            {0: [{
                'type': 'data',
                'path': '/dev/sda1',
                'tags': {'ceph.osd_id': '0'},
                'devices': ['/dev/sda'],
            }]}
        )
        stdout, stderr = capsys.readouterr()
        assert 'osd id' in stdout

    def test_devices_are_comma_separated(self, capsys):
        lvm.listing.pretty_report({0: [
            {'type': 'data', 'path': '/dev/sda1', 'devices': ['/dev/sda', '/dev/sdb1']}
        ]})
        stdout, stderr = capsys.readouterr()
        assert '/dev/sda,/dev/sdb1' in stdout


class TestList(object):

    def test_empty_full_json_zero_exit_status(self, fake_call, is_root, factory, capsys):
        args = factory(format='json', device=None)
        lvm.listing.List([]).list(args)
        stdout, stderr = capsys.readouterr()
        assert stdout == '{}\n'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_empty_device_json_zero_exit_status(self, is_root,factory,capsys):
        args = factory(format='json', device='/dev/sda1')
        lvm.listing.List([]).list(args)
        stdout, stderr = capsys.readouterr()
        assert stdout == '{}\n'

    def test_empty_full_zero_exit_status(self, fake_call, is_root, factory):
        args = factory(format='pretty', device=None)
        with pytest.raises(SystemExit):
            lvm.listing.List([]).list(args)

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_empty_device_zero_exit_status(self, is_root, factory):
        args = factory(format='pretty', device='/dev/sda1')
        with pytest.raises(SystemExit):
            lvm.listing.List([]).list(args)

class TestFullReport(object):

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_no_ceph_lvs(self, monkeypatch):
        # ceph lvs are detected by looking into its tags
        osd = api.Volume(lv_name='volume1', lv_path='/dev/VolGroup/lv',
                         lv_tags={})
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        result = lvm.listing.List([]).full_report()
        assert result == {}

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_ceph_data_lv_reported(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        pv = api.PVolume(pv_name='/dev/sda1', pv_tags={}, pv_uuid="0000",
                         vg_name='VolGroup', lv_uuid="aaaa")
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags=tags,
                         lv_path='/dev/VolGroup/lv', vg_name='VolGroup')
        volumes = []
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'get_single_pv', lambda **kwargs: pv)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_ceph_journal_lv_reported(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        journal_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=journal'
        pv = api.PVolume(pv_name='/dev/sda1', pv_tags={}, pv_uuid="0000",
                         vg_name="VolGroup", lv_uuid="aaaa")
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags=tags,
                         lv_path='/dev/VolGroup/lv', vg_name='VolGroup')
        journal = api.Volume(
            lv_name='journal', lv_uuid='x', lv_tags=journal_tags,
            lv_path='/dev/VolGroup/journal', vg_name='VolGroup')
        volumes = []
        volumes.append(osd)
        volumes.append(journal)
        monkeypatch.setattr(lvm.listing.api,'get_single_pv',lambda **kwargs:pv)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'
        assert result['0'][1]['name'] == 'journal'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_ceph_wal_lv_reported(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.wal_uuid=x,ceph.type=data'
        wal_tags = 'ceph.osd_id=0,ceph.wal_uuid=x,ceph.type=wal'
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags=tags,
                         lv_path='/dev/VolGroup/lv', vg_name='VolGroup')
        wal = api.Volume(lv_name='wal', lv_uuid='x', lv_tags=wal_tags,
                         lv_path='/dev/VolGroup/wal', vg_name='VolGroup')
        volumes = []
        volumes.append(osd)
        volumes.append(wal)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'
        assert result['0'][1]['name'] == 'wal'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    @pytest.mark.parametrize('type_', ['journal', 'db', 'wal'])
    def test_physical_2nd_device_gets_reported(self, type_, monkeypatch):
        tags = ('ceph.osd_id=0,ceph.{t}_uuid=x,ceph.type=data,'
                'ceph.{t}_device=/dev/sda1').format(t=type_)
        osd = api.Volume(lv_name='volume1', lv_uuid='y', lv_tags=tags,
                         vg_name='VolGroup', lv_path='/dev/VolGroup/lv')
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            [osd])

        result = lvm.listing.List([]).full_report()
        assert result['0'][1]['path'] == '/dev/sda1'
        assert result['0'][1]['tags'] == {'PARTUUID': 'x'}
        assert result['0'][1]['type'] == type_


class TestSingleReport(object):

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_not_a_ceph_lv(self, monkeypatch):
        # ceph lvs are detected by looking into its tags
        lv = api.Volume(lv_name='lv', lv_tags={}, lv_path='/dev/VolGroup/lv',
                        vg_name='VolGroup')
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            [lv])

        result = lvm.listing.List([]).single_report('VolGroup/lv')
        assert result == {}

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_a_ceph_lv(self, monkeypatch):
        # ceph lvs are detected by looking into its tags
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lv = api.Volume(lv_name='lv', vg_name='VolGroup', lv_uuid='aaaa',
                        lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes = []
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        result = lvm.listing.List([]).single_report('VolGroup/lv')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert result['0'][0]['devices'] == []

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_a_ceph_journal_device(self, monkeypatch):
        # ceph lvs are detected by looking into its tags
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,' + \
               'ceph.journal_device=/dev/sda1'
        lv = api.Volume(lv_name='lv', lv_uuid='aaa', lv_tags=tags,
                        lv_path='/dev/VolGroup/lv', vg_name='VolGroup')
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            [lv] if 'tags' in kwargs else [])

        result = lvm.listing.List([]).single_report('/dev/sda1')
        assert result['0'][0]['tags'] == {'PARTUUID': 'x'}
        assert result['0'][0]['type'] == 'journal'
        assert result['0'][0]['path'] == '/dev/sda1'

    def test_report_a_ceph_lv_with_devices(self, monkeypatch):
        pvolumes = []

        tags = 'ceph.osd_id=0,ceph.type=data'
        pv1 = api.PVolume(vg_name="VolGroup", pv_name='/dev/sda1',
                          pv_uuid='', pv_tags={}, lv_uuid="aaaa")
        pv2 = api.PVolume(vg_name="VolGroup", pv_name='/dev/sdb1',
                          pv_uuid='', pv_tags={}, lv_uuid="aaaa")
        pvolumes.append(pv1)
        pvolumes.append(pv2)


        volumes = []
        lv = api.Volume(lv_name='lv', vg_name='VolGroup',lv_uuid='aaaa',
                        lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes.append(lv)

        monkeypatch.setattr(lvm.listing.api, 'get_pvs', lambda **kwargs:
                            pvolumes)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        listing = lvm.listing.List([])
        listing._pvs = [
            {'lv_uuid': 'aaaa', 'pv_name': '/dev/sda1', 'pv_tags': '', 'pv_uuid': ''},
            {'lv_uuid': 'aaaa', 'pv_name': '/dev/sdb1', 'pv_tags': '', 'pv_uuid': ''},
        ]

        result = listing.single_report('VolGroup/lv')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert result['0'][0]['devices'] == ['/dev/sda1', '/dev/sdb1']

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_by_osd_id_for_just_block_dev(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=block'
        lvs = [ api.Volume(lv_name='lv1', lv_tags=tags, lv_path='/dev/vg/lv1',
                           lv_uuid='aaaa', vg_name='vg')
              ]
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs: lvs)

        listing = lvm.listing.List([])
        result = listing.single_report(0)
        assert result['0'][0]['name'] == 'lv1'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['lv_path'] == '/dev/vg/lv1'
        assert result['0'][0]['vg_name'] == 'vg'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_by_osd_id_for_just_data_dev(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lvs = [ api.Volume(lv_name='lv1', lv_tags=tags, lv_path='/dev/vg/lv1',
                           lv_uuid='bbbb', vg_name='vg'),
                ]
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs: lvs)

        listing = lvm.listing.List([])
        result = listing.single_report(0)
        assert result['0'][0]['name'] == 'lv1'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['lv_path'] == '/dev/vg/lv1'
        assert result['0'][0]['vg_name'] == 'vg'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_by_osd_id_for_just_block_wal_and_db_dev(self, monkeypatch):
        tags1 = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=block'
        tags2 = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=wal'
        tags3 = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=db'
        lvs = [ api.Volume(lv_name='lv1', lv_tags=tags1, lv_path='/dev/vg/lv1',
                           lv_uuid='aaaa', vg_name='vg'),
                api.Volume(lv_name='lv2', lv_tags=tags2, lv_path='/dev/vg/lv2',
                           lv_uuid='bbbb', vg_name='vg'),
                api.Volume(lv_name='lv3', lv_tags=tags3, lv_path='/dev/vg/lv3',
                           lv_uuid='cccc', vg_name='vg'),
              ]
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs: lvs)

        listing = lvm.listing.List([])
        result = listing.single_report(0)
        assert result['0'][0]['name'] == 'lv1'
        assert result['0'][0]['lv_tags'] == tags1
        assert result['0'][0]['lv_path'] == '/dev/vg/lv1'
        assert result['0'][0]['vg_name'] == 'vg'
        assert result['0'][1]['name'] == 'lv2'
        assert result['0'][1]['lv_tags'] == tags2
        assert result['0'][1]['lv_path'] == '/dev/vg/lv2'
        assert result['0'][1]['vg_name'] == 'vg'
        assert result['0'][2]['name'] == 'lv3'
        assert result['0'][2]['lv_tags'] == tags3
        assert result['0'][2]['lv_path'] == '/dev/vg/lv3'
        assert result['0'][2]['vg_name'] == 'vg'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_by_osd_id_for_data_and_journal_dev(self, monkeypatch):
        tags1 = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        tags2 = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=journal'
        lvs = [ api.Volume(lv_name='lv1', lv_tags=tags1, lv_path='/dev/vg/lv1',
                    lv_uuid='aaaa', vg_name='vg'),
                api.Volume(lv_name='lv2', lv_tags=tags2, lv_path='/dev/vg/lv2',
                    lv_uuid='bbbb', vg_name='vg'),
              ]
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs: lvs)

        listing = lvm.listing.List([])
        result = listing.single_report(0)
        assert result['0'][0]['name'] == 'lv1'
        assert result['0'][0]['lv_tags'] == tags1
        assert result['0'][0]['lv_path'] == '/dev/vg/lv1'
        assert result['0'][0]['vg_name'] == 'vg'
        assert result['0'][1]['name'] == 'lv2'
        assert result['0'][1]['lv_tags'] == tags2
        assert result['0'][1]['lv_path'] == '/dev/vg/lv2'
        assert result['0'][1]['vg_name'] == 'vg'

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_by_nonexistent_osd_id(self, monkeypatch):
        lv = api.Volume(lv_name='lv', lv_tags={}, lv_path='/dev/VolGroup/lv',
                        vg_name='VolGroup')
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            [lv])

        result = lvm.listing.List([]).single_report('1')
        assert result == {}

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    def test_report_a_ceph_lv_with_no_matching_devices(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.type=data'
        lv = api.Volume(lv_name='lv', vg_name='VolGroup', lv_uuid='aaaa',
                        lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes = []
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'get_lvs', lambda **kwargs:
                            volumes)

        listing = lvm.listing.List([])
        listing._pvs = [
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sda1', 'pv_tags': '',
             'pv_uuid': ''},
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sdb1', 'pv_tags': '',
             'pv_uuid': ''}]

        result = listing.single_report('VolGroup/lv')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert result['0'][0]['devices'] == []
