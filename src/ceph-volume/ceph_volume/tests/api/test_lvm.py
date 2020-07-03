import os
import pytest
from mock.mock import patch
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
        monkeypatch.setattr(api.process, 'call', lambda x,**kw: ('\n\n', '', 0))
        assert api.get_api_vgs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = ['  VolGroup00']
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: (report, '', 0))
        assert api.get_api_vgs() == [{'vg_name': 'VolGroup00'}]

    def test_report_has_stuff_with_empty_attrs(self, monkeypatch):
        report = ['  VolGroup00 ;;;;;;4194304']
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: (report, '', 0))
        result = api.get_api_vgs()[0]
        assert len(result.keys()) == 7
        assert result['vg_name'] == 'VolGroup00'
        assert result['vg_extent_size'] == '4194304'

    def test_report_has_multiple_items(self, monkeypatch):
        report = ['   VolGroup00;;;;;;;', '    ceph_vg;;;;;;;']
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: (report, '', 0))
        result = api.get_api_vgs()
        assert result[0]['vg_name'] == 'VolGroup00'
        assert result[1]['vg_name'] == 'ceph_vg'


class TestGetAPILvs(object):

    def test_report_is_emtpy(self, monkeypatch):
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: ('', '', 0))
        assert api.get_api_lvs() == []

    def test_report_has_stuff(self, monkeypatch):
        report = ['  ;/path;VolGroup00;root']
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: (report, '', 0))
        result = api.get_api_lvs()
        assert result[0]['lv_name'] == 'VolGroup00'

    def test_report_has_multiple_items(self, monkeypatch):
        report = ['  ;/path;VolName;root', ';/dev/path;ceph_lv;ceph_vg']
        monkeypatch.setattr(api.process, 'call', lambda x, **kw: (report, '', 0))
        result = api.get_api_lvs()
        assert result[0]['lv_name'] == 'VolName'
        assert result[1]['lv_name'] == 'ceph_lv'


class TestVolume(object):

    def test_is_ceph_device(self):
        lv_tags = "ceph.type=data,ceph.osd_id=0"
        osd = api.Volume(lv_name='osd/volume', lv_tags=lv_tags)
        assert api.is_ceph_device(osd)

    @pytest.mark.parametrize('dev',[
        '/dev/sdb',
        api.VolumeGroup(vg_name='foo'),
        api.Volume(lv_name='vg/no_osd', lv_tags='', lv_path='lv/path'),
        api.Volume(lv_name='vg/no_osd', lv_tags='ceph.osd_id=null', lv_path='lv/path'),
        None,
    ])
    def test_is_not_ceph_device(self, dev):
        assert not api.is_ceph_device(dev)

    def test_no_empty_lv_name(self):
        with pytest.raises(ValueError):
            api.Volume(lv_name='', lv_tags='')


class TestVolumeGroup(object):

    def test_volume_group_no_empty_name(self):
        with pytest.raises(ValueError):
            api.VolumeGroup(vg_name='')


class TestVolumeGroupFree(object):

    def test_integer_gets_produced(self):
        vg = api.VolumeGroup(vg_name='nosize', vg_free_count=100, vg_extent_size=4194304)
        assert vg.free == 100 * 4194304


class TestCreateLVs(object):

    def setup(self):
        self.vg = api.VolumeGroup(vg_name='ceph',
                                         vg_extent_size=1073741824,
                                         vg_extent_count=99999999,
                                         vg_free_count=999)

    def test_creates_correct_lv_number_from_parts(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: (a, kw))
        lvs = api.create_lvs(self.vg, parts=4)
        assert len(lvs) == 4

    def test_suffixes_the_size_arg(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: (a, kw))
        lvs = api.create_lvs(self.vg, parts=4)
        assert lvs[0][1]['extents'] == 249

    def test_only_uses_free_size(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: (a, kw))
        vg = api.VolumeGroup(vg_name='ceph',
                             vg_extent_size=1073741824,
                             vg_extent_count=99999999,
                             vg_free_count=1000)
        lvs = api.create_lvs(vg, parts=4)
        assert lvs[0][1]['extents'] == 250

    def test_null_tags_are_set_by_default(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: (a, kw))
        kwargs = api.create_lvs(self.vg, parts=4)[0][1]
        assert list(kwargs['tags'].values()) == ['null', 'null', 'null', 'null']

    def test_fallback_to_one_part(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: (a, kw))
        lvs = api.create_lvs(self.vg)
        assert len(lvs) == 1


class TestVolumeGroupSizing(object):

    def setup(self):
        self.vg = api.VolumeGroup(vg_name='ceph',
                                         vg_extent_size=1073741824,
                                         vg_free_count=1024)

    def test_parts_and_size_errors(self):
        with pytest.raises(ValueError) as error:
            self.vg.sizing(parts=4, size=10)
        assert "Cannot process sizing" in str(error.value)

    def test_zero_parts_produces_100_percent(self):
        result = self.vg.sizing(parts=0)
        assert result['percentages'] == 100

    def test_two_parts_produces_50_percent(self):
        result = self.vg.sizing(parts=2)
        assert result['percentages'] == 50

    def test_two_parts_produces_half_size(self):
        result = self.vg.sizing(parts=2)
        assert result['sizes'] == 512

    def test_half_size_produces_round_sizes(self):
        result = self.vg.sizing(size=512)
        assert result['sizes'] == 512
        assert result['percentages'] == 50
        assert result['parts'] == 2

    def test_bit_more_than_half_size_allocates_full_size(self):
        # 513 can't allocate more than 1, so it just fallsback to using the
        # whole device
        result = self.vg.sizing(size=513)
        assert result['sizes'] == 1024
        assert result['percentages'] == 100
        assert result['parts'] == 1

    def test_extents_are_halfed_rounded_down(self):
        result = self.vg.sizing(size=512)
        assert result['extents'] == 512

    def test_bit_less_size_rounds_down(self):
        result = self.vg.sizing(size=129)
        assert result['sizes'] == 146
        assert result['percentages'] == 14
        assert result['parts'] == 7

    def test_unable_to_allocate_past_free_size(self):
        with pytest.raises(exceptions.SizeAllocationError):
            self.vg.sizing(size=2048)


class TestRemoveLV(object):

    def test_removes_lv(self, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 0)
        monkeypatch.setattr(process, 'call', mock_call)
        assert api.remove_lv("vg/lv")

    def test_removes_lv_object(self, fake_call):
        foo_volume = api.Volume(lv_name='foo', lv_path='/path', vg_name='foo_group', lv_tags='')
        api.remove_lv(foo_volume)
        # last argument from the list passed to process.call
        assert fake_call.calls[0]['args'][0][-1] == '/path'

    def test_fails_to_remove_lv(self, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 1)
        monkeypatch.setattr(process, 'call', mock_call)
        with pytest.raises(RuntimeError):
            api.remove_lv("vg/lv")


class TestCreateLV(object):

    def setup(self):
        self.foo_volume = api.Volume(lv_name='foo', lv_path='/path', vg_name='foo_group', lv_tags='')
        self.foo_group = api.VolumeGroup(vg_name='foo_group',
                                         vg_extent_size=4194304,
                                         vg_free_count=100)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_uses_size(self, m_get_first_lv, m_call, m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group, size=5368709120, tags={'ceph.type': 'data'})
        expected = ['lvcreate', '--yes', '-l', '1280', '-n', 'foo-0', 'foo_group']
        m_run.assert_called_with(expected)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_uses_extents(self, m_get_first_lv, m_call, m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group, extents='50', tags={'ceph.type': 'data'})
        expected = ['lvcreate', '--yes', '-l', '50', '-n', 'foo-0', 'foo_group']
        m_run.assert_called_with(expected)

    @pytest.mark.parametrize("test_input,expected",
                             [(2, 50),
                              (3, 33),])
    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_uses_slots(self, m_get_first_lv, m_call, m_run, monkeypatch, test_input, expected):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group, slots=test_input, tags={'ceph.type': 'data'})
        expected = ['lvcreate', '--yes', '-l', str(expected), '-n', 'foo-0', 'foo_group']
        m_run.assert_called_with(expected)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_uses_all(self, m_get_first_lv, m_call, m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group, tags={'ceph.type': 'data'})
        expected = ['lvcreate', '--yes', '-l', '100%FREE', '-n', 'foo-0', 'foo_group']
        m_run.assert_called_with(expected)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_calls_to_set_tags_default(self, m_get_first_lv, m_set_tags, m_call, m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group)
        tags = {
            "ceph.osd_id": "null",
            "ceph.type": "null",
            "ceph.cluster_fsid": "null",
            "ceph.osd_fsid": "null",
        }
        m_set_tags.assert_called_with(tags)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_calls_to_set_tags_arg(self, m_get_first_lv, m_set_tags, m_call, m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        api.create_lv('foo', 0, vg=self.foo_group, tags={'ceph.type': 'data'})
        tags = {
            "ceph.type": "data",
            "ceph.data_device": "/path"
        }
        m_set_tags.assert_called_with(tags)

    @patch('ceph_volume.api.lvm.process.run')
    @patch('ceph_volume.api.lvm.process.call')
    @patch('ceph_volume.api.lvm.get_device_vgs')
    @patch('ceph_volume.api.lvm.create_vg')
    @patch('ceph_volume.api.lvm.get_first_lv')
    def test_create_vg(self, m_get_first_lv, m_create_vg, m_get_device_vgs, m_call,
                       m_run, monkeypatch):
        m_get_first_lv.return_value = self.foo_volume
        m_get_device_vgs.return_value = []
        api.create_lv('foo', 0, device='dev/foo', size='5G', tags={'ceph.type': 'data'})
        m_create_vg.assert_called_with('dev/foo', name_prefix='ceph')


class TestTags(object):

    def setup(self):
        self.foo_volume_clean = api.Volume(lv_name='foo_clean', lv_path='/pathclean',
            vg_name='foo_group',
            lv_tags='')
        self.foo_volume = api.Volume(lv_name='foo', lv_path='/path',
            vg_name='foo_group',
            lv_tags='ceph.foo0=bar0,ceph.foo1=bar1,ceph.foo2=bar2')

    def test_set_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        self.foo_volume_clean.set_tag('foo', 'bar')
        expected = ['lvchange', '--addtag', 'foo=bar', '/pathclean']
        assert capture.calls[0]['args'][0] == expected
        assert self.foo_volume_clean.tags == {'foo': 'bar'}

    def test_set_clear_tag(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        self.foo_volume_clean.set_tag('foo', 'bar')
        assert self.foo_volume_clean.tags == {'foo': 'bar'}
        self.foo_volume_clean.clear_tag('foo')
        expected = ['lvchange', '--deltag', 'foo=bar', '/pathclean']
        assert self.foo_volume_clean.tags == {}
        assert capture.calls[1]['args'][0] == expected

    def test_set_tags(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        tags = {'ceph.foo0': 'bar0', 'ceph.foo1': 'bar1', 'ceph.foo2': 'bar2'}
        assert self.foo_volume.tags == tags

        tags = {'ceph.foo0': 'bar0', 'ceph.foo1': 'baz1', 'ceph.foo2': 'baz2'}
        self.foo_volume.set_tags(tags)
        assert self.foo_volume.tags == tags

        self.foo_volume.set_tag('ceph.foo1', 'other1')
        tags['ceph.foo1'] = 'other1'
        assert self.foo_volume.tags == tags

        expected = [
            sorted(['lvchange', '--deltag', 'ceph.foo0=bar0', '--deltag',
                    'ceph.foo1=bar1', '--deltag', 'ceph.foo2=bar2', '/path']),
            sorted(['lvchange', '--deltag', 'ceph.foo1=baz1', '/path']),
            sorted(['lvchange', '--addtag', 'ceph.foo0=bar0', '--addtag',
                    'ceph.foo1=baz1', '--addtag', 'ceph.foo2=baz2', '/path']),
            sorted(['lvchange', '--addtag', 'ceph.foo1=other1', '/path']),
        ]
        # The order isn't guaranted
        for call in capture.calls:
            assert sorted(call['args'][0]) in expected
        assert len(capture.calls) == len(expected)

    def test_clear_tags(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        tags = {'ceph.foo0': 'bar0', 'ceph.foo1': 'bar1', 'ceph.foo2': 'bar2'}

        self.foo_volume_clean.set_tags(tags)
        assert self.foo_volume_clean.tags == tags
        self.foo_volume_clean.clear_tags()
        assert self.foo_volume_clean.tags == {}

        expected = [
            sorted(['lvchange', '--addtag', 'ceph.foo0=bar0', '--addtag',
                    'ceph.foo1=bar1', '--addtag', 'ceph.foo2=bar2',
                    '/pathclean']),
            sorted(['lvchange', '--deltag', 'ceph.foo0=bar0', '--deltag',
                    'ceph.foo1=bar1', '--deltag', 'ceph.foo2=bar2',
                    '/pathclean']),
        ]
        # The order isn't guaranted
        for call in capture.calls:
            assert sorted(call['args'][0]) in expected
        assert len(capture.calls) == len(expected)


class TestExtendVG(object):

    def setup(self):
        self.foo_volume = api.VolumeGroup(vg_name='foo', lv_tags='')

    def test_uses_single_device_in_list(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.extend_vg(self.foo_volume, ['/dev/sda'])
        expected = ['vgextend', '--force', '--yes', 'foo', '/dev/sda']
        assert fake_run.calls[0]['args'][0] == expected

    def test_uses_single_device(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.extend_vg(self.foo_volume, '/dev/sda')
        expected = ['vgextend', '--force', '--yes', 'foo', '/dev/sda']
        assert fake_run.calls[0]['args'][0] == expected

    def test_uses_multiple_devices(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.extend_vg(self.foo_volume, ['/dev/sda', '/dev/sdb'])
        expected = ['vgextend', '--force', '--yes', 'foo', '/dev/sda', '/dev/sdb']
        assert fake_run.calls[0]['args'][0] == expected


class TestReduceVG(object):

    def setup(self):
        self.foo_volume = api.VolumeGroup(vg_name='foo', lv_tags='')

    def test_uses_single_device_in_list(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.reduce_vg(self.foo_volume, ['/dev/sda'])
        expected = ['vgreduce', '--force', '--yes', 'foo', '/dev/sda']
        assert fake_run.calls[0]['args'][0] == expected

    def test_uses_single_device(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.reduce_vg(self.foo_volume, '/dev/sda')
        expected = ['vgreduce', '--force', '--yes', 'foo', '/dev/sda']
        assert fake_run.calls[0]['args'][0] == expected

    def test_uses_multiple_devices(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.reduce_vg(self.foo_volume, ['/dev/sda', '/dev/sdb'])
        expected = ['vgreduce', '--force', '--yes', 'foo', '/dev/sda', '/dev/sdb']
        assert fake_run.calls[0]['args'][0] == expected


class TestCreateVG(object):

    def setup(self):
        self.foo_volume = api.VolumeGroup(vg_name='foo', lv_tags='')

    def test_no_name(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.create_vg('/dev/sda')
        result = fake_run.calls[0]['args'][0]
        assert '/dev/sda' in result
        assert result[-2].startswith('ceph-')

    def test_devices_list(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.create_vg(['/dev/sda', '/dev/sdb'], name='ceph')
        result = fake_run.calls[0]['args'][0]
        expected = ['vgcreate', '--force', '--yes', 'ceph', '/dev/sda', '/dev/sdb']
        assert result == expected

    def test_name_prefix(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.create_vg('/dev/sda', name_prefix='master')
        result = fake_run.calls[0]['args'][0]
        assert '/dev/sda' in result
        assert result[-2].startswith('master-')

    def test_specific_name(self, monkeypatch, fake_run):
        monkeypatch.setattr(api, 'get_first_vg', lambda **kw: True)
        api.create_vg('/dev/sda', name='master')
        result = fake_run.calls[0]['args'][0]
        assert '/dev/sda' in result
        assert result[-2] == 'master'

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
    monkeypatch.setattr('os.path.isdir', lambda x, **kw: False)


@pytest.fixture
def enable_kvdo_path(monkeypatch):
    monkeypatch.setattr('os.path.isdir', lambda x, **kw: True)


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
        monkeypatch.setattr('ceph_volume.api.lvm._is_vdo', lambda x, **kw: True)
        assert api.is_vdo('/path') == '1'

    def test_kvdo_dir_no_devices(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x, **kw: [])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x, **kw: [])
        assert api._is_vdo('/dev/mapper/vdo0') is False

    def test_vdo_slaves_found_and_matched(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x, **kw: ['/dev/dm-3'])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x, **kw: [])
        assert api._is_vdo('/dev/dm-3') is True

    def test_vdo_parents_found_and_matched(self, makedirs, enable_kvdo_path, listdir, monkeypatch):
        kvdo_path = makedirs('sys/kvdo')
        listdir(paths={'/sys/kvdo': kvdo_path})
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_slaves', lambda x, **kw: [])
        monkeypatch.setattr('ceph_volume.api.lvm._vdo_parents', lambda x, **kw: ['/dev/dm-4'])
        assert api._is_vdo('/dev/dm-4') is True


class TestVdoSlaves(object):

    def test_slaves_are_not_found(self, makedirs, listdir, monkeypatch):
        slaves_path = makedirs('sys/block/vdo0/slaves')
        listdir(paths={'/sys/block/vdo0/slaves': slaves_path})
        monkeypatch.setattr('ceph_volume.api.lvm.os.path.exists', lambda x, **kw: True)
        result = sorted(api._vdo_slaves(['vdo0']))
        assert '/dev/mapper/vdo0' in result
        assert 'vdo0' in result

    def test_slaves_are_found(self, makedirs, listdir, monkeypatch):
        slaves_path = makedirs('sys/block/vdo0/slaves')
        makedirs('sys/block/vdo0/slaves/dm-4')
        makedirs('dev/mapper/vdo0')
        listdir(paths={'/sys/block/vdo0/slaves': slaves_path})
        monkeypatch.setattr('ceph_volume.api.lvm.os.path.exists', lambda x, **kw: True)
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


class TestSplitNameParser(object):

    def test_keys_are_parsed_without_prefix(self):
        line = ["DM_VG_NAME='/dev/mapper/vg';DM_LV_NAME='lv';DM_LV_LAYER=''"]
        result = api._splitname_parser(line)
        assert result['VG_NAME'] == 'vg'
        assert result['LV_NAME'] == 'lv'
        assert result['LV_LAYER'] == ''

    def test_vg_name_sans_mapper(self):
        line = ["DM_VG_NAME='/dev/mapper/vg';DM_LV_NAME='lv';DM_LV_LAYER=''"]
        result = api._splitname_parser(line)
        assert '/dev/mapper' not in result['VG_NAME']


class TestGetDeviceVgs(object):

    @patch('ceph_volume.process.call')
    @patch('ceph_volume.api.lvm._output_parser')
    def test_get_device_vgs_with_empty_pv(self, patched_output_parser, pcall):
        patched_output_parser.return_value = [{'vg_name': ''}]
        pcall.return_value = ('', '', '')
        vgs = api.get_device_vgs('/dev/foo')
        assert vgs == []

class TestGetDeviceLvs(object):

    @patch('ceph_volume.process.call')
    @patch('ceph_volume.api.lvm._output_parser')
    def test_get_device_lvs_with_empty_vg(self, patched_output_parser, pcall):
        patched_output_parser.return_value = [{'lv_name': ''}]
        pcall.return_value = ('', '', '')
        vgs = api.get_device_lvs('/dev/foo')
        assert vgs == []
