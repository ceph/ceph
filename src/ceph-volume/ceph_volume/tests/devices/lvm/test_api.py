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


@pytest.fixture
def volume_groups(monkeypatch):
    monkeypatch.setattr(process, 'call', lambda x: ('{}', '', 0))
    vgs = api.VolumeGroups()
    vgs._purge()
    return vgs


class TestGetLV(object):

    def test_nothing_is_passed_in(self):
        # so we return a None
        assert api.get_lv() is None

    def test_single_lv_is_matched(self, volumes, monkeypatch):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo', lv_tags="ceph.type=data")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        assert api.get_lv(lv_name='foo') == FooVolume


class TestGetVG(object):

    def test_nothing_is_passed_in(self):
        # so we return a None
        assert api.get_vg() is None

    def test_single_vg_is_matched(self, volume_groups, monkeypatch):
        FooVG = api.VolumeGroup(vg_name='foo')
        volume_groups.append(FooVG)
        monkeypatch.setattr(api, 'VolumeGroups', lambda: volume_groups)
        assert api.get_vg(vg_name='foo') == FooVG


class TestVolumes(object):

    def test_volume_get_has_no_volumes(self, volumes):
        assert volumes.get() is None

    def test_volume_get_filtered_has_no_volumes(self, volumes):
        assert volumes.get(lv_name='ceph') is None

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

    def test_filter_by_tag(self, volumes):
        lv_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.Volume(lv_name='volume1', lv_path='/dev/vg/lv', lv_tags=lv_tags)
        journal = api.Volume(lv_name='volume2', lv_path='/dev/vg/lv', lv_tags='ceph.type=journal')
        volumes.append(osd)
        volumes.append(journal)
        volumes.filter(lv_tags={'ceph.type': 'data'})
        assert len(volumes) == 1
        assert volumes[0].lv_name == 'volume1'

    def test_filter_by_vg_name(self, volumes):
        lv_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.Volume(lv_name='volume1', vg_name='ceph_vg', lv_tags=lv_tags)
        journal = api.Volume(lv_name='volume2', vg_name='system_vg', lv_tags='ceph.type=journal')
        volumes.append(osd)
        volumes.append(journal)
        volumes.filter(vg_name='ceph_vg')
        assert len(volumes) == 1
        assert volumes[0].lv_name == 'volume1'

    def test_filter_by_lv_path(self, volumes):
        osd = api.Volume(lv_name='volume1', lv_path='/dev/volume1', lv_tags='')
        journal = api.Volume(lv_name='volume2', lv_path='/dev/volume2', lv_tags='')
        volumes.append(osd)
        volumes.append(journal)
        volumes.filter(lv_path='/dev/volume1')
        assert len(volumes) == 1
        assert volumes[0].lv_name == 'volume1'

    def test_filter_requires_params(self, volumes):
        with pytest.raises(TypeError):
            volumes.filter()


class TestVolumeGroups(object):

    def test_volume_get_has_no_volume_groups(self, volume_groups):
        assert volume_groups.get() is None

    def test_volume_get_filtered_has_no_volumes(self, volume_groups):
        assert volume_groups.get(vg_name='ceph') is None

    def test_volume_has_multiple_matches(self, volume_groups):
        volume1 = volume2 = api.VolumeGroup(vg_name='foo', lv_path='/dev/vg/lv', lv_tags='')
        volume_groups.append(volume1)
        volume_groups.append(volume2)
        with pytest.raises(exceptions.MultipleVGsError):
            volume_groups.get(vg_name='foo')

    def test_find_the_correct_one(self, volume_groups):
        volume1 = api.VolumeGroup(vg_name='volume1', lv_tags='')
        volume2 = api.VolumeGroup(vg_name='volume2', lv_tags='')
        volume_groups.append(volume1)
        volume_groups.append(volume2)
        assert volume_groups.get(vg_name='volume1') == volume1

    def test_filter_by_tag(self, volume_groups):
        vg_tags = "ceph.group=dmcache"
        osd = api.VolumeGroup(vg_name='volume1', vg_tags=vg_tags)
        journal = api.VolumeGroup(vg_name='volume2', vg_tags='ceph.group=plain')
        volume_groups.append(osd)
        volume_groups.append(journal)
        volume_groups.filter(vg_tags={'ceph.group': 'dmcache'})
        assert len(volume_groups) == 1
        assert volume_groups[0].vg_name == 'volume1'

    def test_filter_by_vg_name(self, volume_groups):
        vg_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.VolumeGroup(vg_name='ceph_vg', vg_tags=vg_tags)
        journal = api.VolumeGroup(vg_name='volume2', vg_tags='ceph.type=journal')
        volume_groups.append(osd)
        volume_groups.append(journal)
        volume_groups.filter(vg_name='ceph_vg')
        assert len(volume_groups) == 1
        assert volume_groups[0].vg_name == 'ceph_vg'

    def test_filter_requires_params(self, volume_groups):
        with pytest.raises(TypeError):
            volume_groups.filter()


class TestCreateLV(object):

    def setup(self):
        self.foo_volume = api.Volume(lv_name='foo', lv_path='/path', vg_name='foo_group', lv_tags='')

    def test_uses_size(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size=5, type='data')
        expected = ['sudo', 'lvcreate', '--yes', '-L', '5G', '-n', 'foo', 'foo_group']
        assert capture.calls[0]['args'][0] == expected

    def test_calls_to_set_type_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size=5, type='data')
        ceph_tag = ['sudo', 'lvchange', '--addtag', 'ceph.type=data', '/path']
        assert capture.calls[1]['args'][0] == ceph_tag

    def test_calls_to_set_data_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size=5, type='data')
        data_tag = ['sudo', 'lvchange', '--addtag', 'ceph.data_device=/path', '/path']
        assert capture.calls[2]['args'][0] == data_tag
