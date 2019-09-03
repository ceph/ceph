import pytest
from ceph_volume.devices import lvm
from ceph_volume.api import lvm as api


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

    def test_empty_full_json_zero_exit_status(self, is_root, volumes, factory, capsys):
        args = factory(format='json', device=None)
        lvm.listing.List([]).list(args)
        stdout, stderr = capsys.readouterr()
        assert stdout == '{}\n'

    def test_empty_device_json_zero_exit_status(self, is_root, volumes, factory, capsys):
        args = factory(format='json', device='/dev/sda1')
        lvm.listing.List([]).list(args)
        stdout, stderr = capsys.readouterr()
        assert stdout == '{}\n'

    def test_empty_full_zero_exit_status(self, is_root, volumes, factory):
        args = factory(format='pretty', device=None)
        with pytest.raises(SystemExit):
            lvm.listing.List([]).list(args)

    def test_empty_device_zero_exit_status(self, is_root, volumes, factory):
        args = factory(format='pretty', device='/dev/sda1')
        with pytest.raises(SystemExit):
            lvm.listing.List([]).list(args)

    def test_lvs_list_is_created_just_once(self, monkeypatch, is_root, volumes, factory):
        api.volumes_obj_create_count = 0

        def monkey_populate(self):
            api.volumes_obj_create_count += 1
            for lv_item in api.get_api_lvs():
                self.append(api.Volume(**lv_item))
        monkeypatch.setattr(api.Volumes, '_populate', monkey_populate)

        args = factory(format='pretty', device='/dev/sda1')
        with pytest.raises(SystemExit):
            lvm.listing.List([]).list(args)

        # XXX: Ideally, the count should be just 1. Volumes._populate() is
        # being called thrice out of which only twice is moneky_populate.
        assert api.volumes_obj_create_count == 2


class TestFullReport(object):

    def test_no_ceph_lvs(self, volumes, monkeypatch):
        # ceph lvs are detected by looking into its tags
        osd = api.Volume(lv_name='volume1', lv_path='/dev/VolGroup/lv', lv_tags={})
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).full_report()
        assert result == {}

    def test_ceph_data_lv_reported(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'

    def test_ceph_journal_lv_reported(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        journal_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=journal'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', lv_path='/dev/VolGroup/lv', lv_tags=tags)
        journal = api.Volume(
            lv_name='journal', lv_uuid='x', lv_path='/dev/VolGroup/journal', lv_tags=journal_tags)
        volumes.append(osd)
        volumes.append(journal)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'
        assert result['0'][1]['name'] == 'journal'

    def test_ceph_wal_lv_reported(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.wal_uuid=x,ceph.type=data'
        wal_tags = 'ceph.osd_id=0,ceph.wal_uuid=x,ceph.type=wal'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', lv_path='/dev/VolGroup/lv', lv_tags=tags)
        wal = api.Volume(
            lv_name='wal', lv_uuid='x', lv_path='/dev/VolGroup/wal', lv_tags=wal_tags)
        volumes.append(osd)
        volumes.append(wal)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).full_report()
        assert result['0'][0]['name'] == 'volume1'
        assert result['0'][1]['name'] == 'wal'

    def test_physical_journal_gets_reported(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        monkeypatch.setattr(lvm.listing.disk, 'get_device_from_partuuid', lambda x: '/dev/sda1')
        result = lvm.listing.List([]).full_report()
        assert result['0'][1]['path'] == '/dev/sda1'
        assert result['0'][1]['tags'] == {'PARTUUID': 'x'}
        assert result['0'][1]['type'] == 'journal'

    def test_physical_wal_gets_reported(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.wal_uuid=x,ceph.type=data'
        osd = api.Volume(
            lv_name='volume1', lv_uuid='y', lv_path='/dev/VolGroup/lv', lv_tags=tags)
        volumes.append(osd)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        monkeypatch.setattr(lvm.listing.disk, 'get_device_from_partuuid', lambda x: '/dev/sda1')
        result = lvm.listing.List([]).full_report()
        assert result['0'][1]['path'] == '/dev/sda1'
        assert result['0'][1]['tags'] == {'PARTUUID': 'x'}
        assert result['0'][1]['type'] == 'wal'


class TestSingleReport(object):

    def test_not_a_ceph_lv(self, volumes, monkeypatch):
        # ceph lvs are detected by looking into its tags
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup', lv_path='/dev/VolGroup/lv', lv_tags={})
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).single_report('VolGroup/lv')
        assert result == {}

    def test_report_a_ceph_lv(self, volumes, monkeypatch):
        # ceph lvs are detected by looking into its tags
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup',
            lv_uuid='aaaa', lv_path='/dev/VolGroup/lv', lv_tags=tags
        )
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).single_report('VolGroup/lv')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert result['0'][0]['devices'] == []

    def test_report_a_ceph_journal_device(self, volumes, monkeypatch):
        # ceph lvs are detected by looking into its tags
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.journal_device=/dev/sda1'
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup', lv_path='/dev/VolGroup/lv',
            lv_uuid='aaa', lv_tags=tags)
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        result = lvm.listing.List([]).single_report('/dev/sda1')
        assert result['0'][0]['tags'] == {'PARTUUID': 'x'}
        assert result['0'][0]['type'] == 'journal'
        assert result['0'][0]['path'] == '/dev/sda1'

    def test_report_a_ceph_lv_with_devices(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup',
            lv_uuid='aaaa', lv_path='/dev/VolGroup/lv', lv_tags=tags
        )
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
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

    def test_report_a_ceph_lv_with_multiple_pvs_of_same_name(self, pvolumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup',
            lv_uuid='aaaa', lv_path='/dev/VolGroup/lv', lv_tags=tags
        )
        monkeypatch.setattr(api, 'get_lv_from_argument', lambda device: None)
        monkeypatch.setattr(api, 'get_lv', lambda vg_name: lv)
        FooPVolume = api.PVolume(vg_name="vg", pv_name='/dev/sda', pv_uuid="0000", pv_tags={}, lv_uuid="aaaa")
        BarPVolume = api.PVolume(vg_name="vg", pv_name='/dev/sda', pv_uuid="0000", pv_tags={})
        pvolumes.append(FooPVolume)
        pvolumes.append(BarPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        listing = lvm.listing.List([])
        result = listing.single_report('/dev/sda')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert len(result) == 1

    def test_report_a_ceph_lv_with_no_matching_devices(self, volumes, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data'
        lv = api.Volume(
            lv_name='lv', vg_name='VolGroup',
            lv_uuid='aaaa', lv_path='/dev/VolGroup/lv', lv_tags=tags
        )
        volumes.append(lv)
        monkeypatch.setattr(lvm.listing.api, 'Volumes', lambda: volumes)
        listing = lvm.listing.List([])
        listing._pvs = [
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sda1', 'pv_tags': '', 'pv_uuid': ''},
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sdb1', 'pv_tags': '', 'pv_uuid': ''},
        ]
        result = listing.single_report('VolGroup/lv')
        assert result['0'][0]['name'] == 'lv'
        assert result['0'][0]['lv_tags'] == tags
        assert result['0'][0]['path'] == '/dev/VolGroup/lv'
        assert result['0'][0]['devices'] == []


class TestListingPVs(object):

    def setup(self):
        self.default_pvs = [
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sda1', 'pv_tags': '', 'pv_uuid': ''},
            {'lv_uuid': 'ffff', 'pv_name': '/dev/sdb1', 'pv_tags': '', 'pv_uuid': ''},
        ]

    def test_pvs_is_unset(self, monkeypatch):
        monkeypatch.setattr(lvm.listing.api, 'get_api_pvs', lambda: self.default_pvs)
        listing = lvm.listing.List([])
        assert listing.pvs == self.default_pvs

    def test_pvs_is_set(self, monkeypatch):
        # keep it patched so that we can fail if this gets returned
        monkeypatch.setattr(lvm.listing.api, 'get_api_pvs', lambda: self.default_pvs)
        listing = lvm.listing.List([])
        listing._pvs = []
        assert listing.pvs == []
