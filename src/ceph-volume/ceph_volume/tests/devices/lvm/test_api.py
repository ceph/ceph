import pytest
from ceph_volume import process, exceptions
from ceph_volume.devices.lvm import api


class TestParseTags(object):

    def test_no_tags_means_empty_dict(self):
        result = api.parse_tags('')
        assert result == {}

    def test_single_tag_gets_parsed(self):
        result = api.parse_tags('ceph.osd_something=1')
        assert result == {'ceph.osd_something': '1'}

    def test_multiple_csv_expands_in_dict(self):
        result = api.parse_tags('ceph.osd_something=1,ceph.foo=2,ceph.fsid=0000')
        # assert them piecemeal to avoid the un-ordered dict nature
        assert result['ceph.osd_something'] == '1'
        assert result['ceph.foo'] == '2'
        assert result['ceph.fsid'] == '0000'


class TestGetAPIVgs(object):

    def test_report_is_emtpy(self, monkeypatch):
        monkeypatch.setattr(api.process, 'call', lambda x: ('{}', '', 0))
        assert api.get_api_vgs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = '{"report":[{"vg":[{"vg_name":"VolGroup00"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_vgs() == [{'vg_name': 'VolGroup00'}]

    def test_report_has_multiple_items(self, monkeypatch):
        report = '{"report":[{"vg":[{"vg_name":"VolGroup00"},{"vg_name":"ceph_vg"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_vgs() == [{'vg_name': 'VolGroup00'}, {'vg_name': 'ceph_vg'}]

    def test_does_not_get_poluted_with_non_vg_items(self, monkeypatch):
        report = '{"report":[{"vg":[{"vg_name":"VolGroup00"}],"lv":[{"lv":"1"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_vgs() == [{'vg_name': 'VolGroup00'}]


class TestGetAPILvs(object):

    def test_report_is_emtpy(self, monkeypatch):
        monkeypatch.setattr(api.process, 'call', lambda x: ('{}', '', 0))
        assert api.get_api_lvs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = '{"report":[{"lv":[{"lv_name":"VolGroup00"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_lvs() == [{'lv_name': 'VolGroup00'}]

    def test_report_has_multiple_items(self, monkeypatch):
        report = '{"report":[{"lv":[{"lv_name":"VolName"},{"lv_name":"ceph_lv"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_lvs() == [{'lv_name': 'VolName'}, {'lv_name': 'ceph_lv'}]

    def test_does_not_get_poluted_with_non_lv_items(self, monkeypatch):
        report = '{"report":[{"lv":[{"lv_name":"VolName"}],"vg":[{"vg":"1"}]}]}'
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_lvs() == [{'lv_name': 'VolName'}]


@pytest.fixture
def volumes(monkeypatch):
    monkeypatch.setattr(process, 'call', lambda x: ('{}', '', 0))
    volumes = api.Volumes()
    volumes._purge()
    return volumes


class TestGetLV(object):

    def test_nothing_is_passed_in(self):
        # so we return a None
        assert api.get_lv() is None

    def test_single_lv_is_matched(self, volumes, monkeypatch):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo', lv_tags="ceph.type=data")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        assert api.get_lv(lv_name='foo') == FooVolume


class TestVolumes(object):

    def test_volume_get_has_no_volumes(self, volumes):
        assert volumes.get() is None

    def test_volume_has_multiple_matches(self, volumes):
        volume1 = volume2 = api.Volume(lv_name='foo', lv_path='/dev/vg/lv', lv_tags='')
        volumes.append(volume1)
        volumes.append(volume2)
        with pytest.raises(exceptions.MultipleLVsError):
            volumes.get(lv_name='foo')

    def test_find_the_correct_one(self, volumes):
        volume1 = api.Volume(lv_name='volume1', lv_path='/dev/vg/lv', lv_tags='')
        volume2 = api.Volume(lv_name='volume2', lv_path='/dev/vg/lv', lv_tags='')
        volumes.append(volume1)
        volumes.append(volume2)
        assert volumes.get(lv_name='volume1') == volume1
