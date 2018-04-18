import os
import pytest
from ceph_volume import process, exceptions
from ceph_volume.api import lvm as api


class TestParseTags(object):

    def test_no_tags_means_empty_dict(self):
        result = api.parse_tags('')
        assert result == {}

    def test_single_tag_gets_parsed(self):
        result = api.parse_tags('ceph.osd_something=1')
        assert result == {'ceph.osd_something': '1'}

    def test_non_ceph_tags_are_skipped(self):
        result = api.parse_tags('foo')
        assert result == {}

    def test_mixed_non_ceph_tags(self):
        result = api.parse_tags('foo,ceph.bar=1')
        assert result == {'ceph.bar': '1'}

    def test_multiple_csv_expands_in_dict(self):
        result = api.parse_tags('ceph.osd_something=1,ceph.foo=2,ceph.fsid=0000')
        # assert them piecemeal to avoid the un-ordered dict nature
        assert result['ceph.osd_something'] == '1'
        assert result['ceph.foo'] == '2'
        assert result['ceph.fsid'] == '0000'


class TestGetAPIVgs(object):

    def test_report_is_emtpy(self, monkeypatch):
        monkeypatch.setattr(api.process, 'call', lambda x: ('\n\n', '', 0))
        assert api.get_api_vgs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = ['  VolGroup00']
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        assert api.get_api_vgs() == [{'vg_name': 'VolGroup00'}]

    def test_report_has_stuff_with_empty_attrs(self, monkeypatch):
        report = ['  VolGroup00 ;;;;;;9g']
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        result = api.get_api_vgs()[0]
        assert len(result.keys()) == 7
        assert result['vg_name'] == 'VolGroup00'
        assert result['vg_free'] == '9g'

    def test_report_has_multiple_items(self, monkeypatch):
        report = ['   VolGroup00;;;;;;;', '    ceph_vg;;;;;;;']
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        result = api.get_api_vgs()
        assert result[0]['vg_name'] == 'VolGroup00'
        assert result[1]['vg_name'] == 'ceph_vg'


class TestGetAPILvs(object):

    def test_report_is_emtpy(self, monkeypatch):
        monkeypatch.setattr(api.process, 'call', lambda x: ('', '', 0))
        assert api.get_api_lvs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = ['  ;/path;VolGroup00;root']
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        result = api.get_api_lvs()
        assert result[0]['lv_name'] == 'VolGroup00'

    def test_report_has_multiple_items(self, monkeypatch):
        report = ['  ;/path;VolName;root', ';/dev/path;ceph_lv;ceph_vg']
        monkeypatch.setattr(api.process, 'call', lambda x: (report, '', 0))
        result = api.get_api_lvs()
        assert result[0]['lv_name'] == 'VolName'
        assert result[1]['lv_name'] == 'ceph_lv'


@pytest.fixture
def volumes(monkeypatch):
    monkeypatch.setattr(process, 'call', lambda x: ('', '', 0))
    volumes = api.Volumes()
    volumes._purge()
    # also patch api.Volumes so that when it is called, it will use the newly
    # created fixture, with whatever the test method wants to append to it
    monkeypatch.setattr(api, 'Volumes', lambda: volumes)
    return volumes


@pytest.fixture
def pvolumes(monkeypatch):
    monkeypatch.setattr(process, 'call', lambda x: ('', '', 0))
    pvolumes = api.PVolumes()
    pvolumes._purge()
    return pvolumes


@pytest.fixture
def volume_groups(monkeypatch):
    monkeypatch.setattr(process, 'call', lambda x: ('', '', 0))
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

    def test_single_lv_is_matched_by_uuid(self, volumes, monkeypatch):
        FooVolume = api.Volume(
            lv_name='foo', lv_path='/dev/vg/foo',
            lv_uuid='1111', lv_tags="ceph.type=data")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        assert api.get_lv(lv_uuid='1111') == FooVolume


class TestGetPV(object):

    def test_nothing_is_passed_in(self):
        # so we return a None
        assert api.get_pv() is None

    def test_single_pv_is_not_matched(self, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        assert api.get_pv(pv_uuid='foo') is None

    def test_single_pv_is_matched(self, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(vg_name="vg", pv_name='/dev/sda', pv_uuid="0000", pv_tags={})
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        assert api.get_pv(pv_uuid='0000') == FooPVolume

    def test_single_pv_is_matched_by_uuid(self, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(
            pv_name='/dev/vg/foo',
            pv_uuid='1111', pv_tags="ceph.type=data", vg_name="vg")
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        assert api.get_pv(pv_uuid='1111') == FooPVolume

    def test_vg_name_is_set(self, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(
            pv_name='/dev/vg/foo',
            pv_uuid='1111', pv_tags="ceph.type=data", vg_name="vg")
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        pv = api.get_pv(pv_name="/dev/vg/foo")
        assert pv.vg_name == "vg"


class TestPVolumes(object):

    def test_filter_by_tag_does_not_match_one(self, pvolumes, monkeypatch):
        pv_tags = "ceph.type=journal,ceph.osd_id=1,ceph.fsid=000-aaa"
        FooPVolume = api.PVolume(
            pv_name='/dev/vg/foo',
            pv_uuid='1111', pv_tags=pv_tags, vg_name='vg')
        pvolumes.append(FooPVolume)
        pvolumes.filter(pv_tags={'ceph.type': 'journal', 'ceph.osd_id': '2'})
        assert pvolumes == []

    def test_filter_by_tags_matches(self, pvolumes, monkeypatch):
        pv_tags = "ceph.type=journal,ceph.osd_id=1"
        FooPVolume = api.PVolume(
            pv_name='/dev/vg/foo',
            pv_uuid='1111', pv_tags=pv_tags, vg_name="vg")
        pvolumes.append(FooPVolume)
        pvolumes.filter(pv_tags={'ceph.type': 'journal', 'ceph.osd_id': '1'})
        assert pvolumes == [FooPVolume]


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

    def test_as_dict_infers_type_from_tags(self, volumes):
        lv_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.Volume(lv_name='volume1', lv_path='/dev/vg/lv', lv_tags=lv_tags)
        volumes.append(osd)
        result = volumes.get(lv_tags={'ceph.type': 'data'}).as_dict()
        assert result['type'] == 'data'

    def test_as_dict_populates_path_from_lv_api(self, volumes):
        lv_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.Volume(lv_name='volume1', lv_path='/dev/vg/lv', lv_tags=lv_tags)
        volumes.append(osd)
        result = volumes.get(lv_tags={'ceph.type': 'data'}).as_dict()
        assert result['path'] == '/dev/vg/lv'

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

    def test_filter_by_tag_does_not_match_one(self, volumes):
        lv_tags = "ceph.type=data,ceph.fsid=000-aaa"
        osd = api.Volume(lv_name='volume1', lv_path='/dev/vg/lv', lv_tags=lv_tags)
        journal = api.Volume(lv_name='volume2', lv_path='/dev/vg/lv', lv_tags='ceph.osd_id=1,ceph.type=journal')
        volumes.append(osd)
        volumes.append(journal)
        # note the different osd_id!
        volumes.filter(lv_tags={'ceph.type': 'data', 'ceph.osd_id': '2'})
        assert volumes == []

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

    def test_filter_by_lv_uuid(self, volumes):
        osd = api.Volume(lv_name='volume1', lv_path='/dev/volume1', lv_uuid='1111', lv_tags='')
        journal = api.Volume(lv_name='volume2', lv_path='/dev/volume2', lv_uuid='', lv_tags='')
        volumes.append(osd)
        volumes.append(journal)
        volumes.filter(lv_uuid='1111')
        assert len(volumes) == 1
        assert volumes[0].lv_name == 'volume1'

    def test_filter_by_lv_uuid_nothing_found(self, volumes):
        osd = api.Volume(lv_name='volume1', lv_path='/dev/volume1', lv_uuid='1111', lv_tags='')
        journal = api.Volume(lv_name='volume2', lv_path='/dev/volume2', lv_uuid='', lv_tags='')
        volumes.append(osd)
        volumes.append(journal)
        volumes.filter(lv_uuid='22222')
        assert volumes == []

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

    def test_filter_by_tag_does_not_match_one(self, volume_groups):
        vg_tags = "ceph.group=dmcache,ceph.disk_type=ssd"
        osd = api.VolumeGroup(vg_name='volume1', vg_path='/dev/vg/lv', vg_tags=vg_tags)
        volume_groups.append(osd)
        volume_groups.filter(vg_tags={'ceph.group': 'data', 'ceph.disk_type': 'ssd'})
        assert volume_groups == []

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


class TestGetLVFromArgument(object):

    def setup(self):
        self.foo_volume = api.Volume(
            lv_name='foo', lv_path='/path/to/lv',
            vg_name='foo_group', lv_tags=''
        )

    def test_non_absolute_path_is_not_valid(self, volumes):
        volumes.append(self.foo_volume)
        assert api.get_lv_from_argument('foo') is None

    def test_too_many_slashes_is_invalid(self, volumes):
        volumes.append(self.foo_volume)
        assert api.get_lv_from_argument('path/to/lv') is None

    def test_absolute_path_is_not_lv(self, volumes):
        volumes.append(self.foo_volume)
        assert api.get_lv_from_argument('/path') is None

    def test_absolute_path_is_lv(self, volumes):
        volumes.append(self.foo_volume)
        assert api.get_lv_from_argument('/path/to/lv') == self.foo_volume


class TestRemoveLV(object):

    def test_removes_lv(self, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 0)
        monkeypatch.setattr(process, 'call', mock_call)
        assert api.remove_lv("vg/lv")

    def test_fails_to_remove_lv(self, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 1)
        monkeypatch.setattr(process, 'call', mock_call)
        with pytest.raises(RuntimeError):
            api.remove_lv("vg/lv")


class TestCreateLV(object):

    def setup(self):
        self.foo_volume = api.Volume(lv_name='foo', lv_path='/path', vg_name='foo_group', lv_tags='')

    def test_uses_size(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size='5G', tags={'ceph.type': 'data'})
        expected = ['lvcreate', '--yes', '-L', '5G', '-n', 'foo', 'foo_group']
        assert capture.calls[0]['args'][0] == expected

    def test_calls_to_set_type_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size='5G', tags={'ceph.type': 'data'})
        ceph_tag = ['lvchange', '--addtag', 'ceph.type=data', '/path']
        assert capture.calls[1]['args'][0] == ceph_tag

    def test_calls_to_set_data_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', lambda *a, **kw: self.foo_volume)
        api.create_lv('foo', 'foo_group', size='5G', tags={'ceph.type': 'data'})
        data_tag = ['lvchange', '--addtag', 'ceph.data_device=/path', '/path']
        assert capture.calls[2]['args'][0] == data_tag


#
# The following tests are pretty gnarly. VDO detection is very convoluted and
# involves correlating information from device mappers, realpaths, slaves of
# those mappers, and parents or related mappers.  This makes it very hard to
# patch nicely or keep tests short and readable. These tests are trying to
# ensure correctness, the better approach will be to do some functional testing
# with VDO.
#


@pytest.fixture
def disable_kvdo_path(monkeypatch):
    monkeypatch.setattr('os.path.isdir', lambda x: False)


@pytest.fixture
def enable_kvdo_path(monkeypatch):
    monkeypatch.setattr('os.path.isdir', lambda x: True)


# Stub for os.listdir


class ListDir(object):

    def __init__(self, paths):
        self.paths = paths
        self._normalize_paths()
        self.listdir = os.listdir

    def _normalize_paths(self):
        for k, v in self.paths.items():
            self.paths[k.rstrip('/')] = v.rstrip('/')

    def add(self, original, fake):
        self.paths[original.rstrip('/')] = fake.rstrip('/')

    def __call__(self, path):
        return self.listdir(self.paths[path.rstrip('/')])


@pytest.fixture(scope='function')
def listdir(monkeypatch):
    def apply(paths=None, stub=None):
        if not stub:
            stub = ListDir(paths)
        if paths:
            for original, fake in paths.items():
                stub.add(original, fake)

        monkeypatch.setattr('os.listdir', stub)
    return apply


@pytest.fixture(scope='function')
def makedirs(tmpdir):
    def create(directory):
        path = os.path.join(str(tmpdir), directory)
        os.makedirs(path)
        return path
    create.base = str(tmpdir)
    return create


class TestIsVdo(object):

    def test_no_vdo_dir(self, disable_kvdo_path):
        assert api._is_vdo('/path') is False

    def test_exceptions_return_false(self, monkeypatch):
        def throw():
            raise Exception()
        monkeypatch.setattr('ceph_volume.api.lvm._is_vdo', throw)
        assert api.is_vdo('/path') == '0'

    def test_is_vdo_returns_a_string(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm._is_vdo', lambda x: True)
        assert api.is_vdo('/path') == '1'

    def test_kvdo_dir_no_devices(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x: [])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x: [])
        assert api._is_vdo('/dev/mapper/vdo0') is False

    def test_vdo_slaves_found_and_matched(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x: ['/dev/dm-3'])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x: [])
        assert api._is_vdo('/dev/dm-3') is True

    def test_vdo_parents_found_and_matched(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x: [])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x: ['/dev/dm-4'])
        assert api._is_vdo('/dev/dm-4') is True


class TestVdoSlaves(object):

    def test_slaves_are_not_found(self, makedirs, listdir, monkeypatch):
        slaves_path = makedirs('sys/block/vdo0/slaves')
        listdir(paths={'/sys/block/vdo0/slaves': slaves_path})
        monkeypatch.setattr('ceph_volume.api.lvm.os.path.exists', lambda x: True)
        result = sorted(api._vdo_slaves(['vdo0']))
        assert '/dev/mapper/vdo0' in result
        assert 'vdo0' in result

    def test_slaves_are_found(self, makedirs, listdir, monkeypatch):
        slaves_path = makedirs('sys/block/vdo0/slaves')
        makedirs('sys/block/vdo0/slaves/dm-4')
        makedirs('dev/mapper/vdo0')
        listdir(paths={'/sys/block/vdo0/slaves': slaves_path})
        monkeypatch.setattr('ceph_volume.api.lvm.os.path.exists', lambda x: True)
        result = sorted(api._vdo_slaves(['vdo0']))
        assert '/dev/dm-4' in result
        assert 'dm-4' in result


class TestVDOParents(object):

    def test_parents_are_found(self, makedirs, listdir):
        block_path = makedirs('sys/block')
        slaves_path = makedirs('sys/block/dm-4/slaves')
        makedirs('sys/block/dm-4/slaves/dm-3')
        listdir(paths={
            '/sys/block/dm-4/slaves': slaves_path,
            '/sys/block': block_path})
        result = api._vdo_parents(['dm-3'])
        assert '/dev/dm-4' in result
        assert 'dm-4' in result

    def test_parents_are_not_found(self, makedirs, listdir):
        block_path = makedirs('sys/block')
        slaves_path = makedirs('sys/block/dm-4/slaves')
        makedirs('sys/block/dm-4/slaves/dm-5')
        listdir(paths={
            '/sys/block/dm-4/slaves': slaves_path,
            '/sys/block': block_path})
        result = api._vdo_parents(['dm-3'])
        assert result == []
