import pytest
from mock.mock import patch
from ceph_volume import process
from ceph_volume.api import lvm as api
from ceph_volume.devices.lvm import migrate
from ceph_volume.util.device import Device
from ceph_volume.util import system
from ceph_volume.util import encryption as encryption_utils

class TestGetClusterName(object):

    mock_volumes = []
    def mock_get_lvs(self, *args, **kwargs):
        return self.mock_volumes.pop(0)

    def test_cluster_found(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234,ceph.cluster_name=name_of_the_cluster'
        vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='',
                         lv_path='/dev/VolGroup/lv1', lv_tags=tags)
        self.mock_volumes = []
        self.mock_volumes.append([vol])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        result = migrate.get_cluster_name(osd_id='0', osd_fsid='1234')
        assert "name_of_the_cluster" == result

    def test_cluster_not_found(self, monkeypatch, capsys):
        self.mock_volumes = []
        self.mock_volumes.append([])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        with pytest.raises(SystemExit) as error:
            migrate.get_cluster_name(osd_id='0', osd_fsid='1234')
        stdout, stderr = capsys.readouterr()
        expected = 'Unexpected error, terminating'
        assert expected in str(error.value)
        expected = 'Unable to find any LV for source OSD: id:0 fsid:1234'
        assert expected in stderr

class TestFindAssociatedDevices(object):

    mock_volumes = []
    def mock_get_lvs(self, *args, **kwargs):
        return self.mock_volumes.pop(0)

    mock_single_volumes = {}
    def mock_get_single_lv(self, *args, **kwargs):
        p = kwargs['filters']['lv_path']
        return self.mock_single_volumes[p]

    def test_lv_is_matched_id(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234'
        vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='',
                         lv_path='/dev/VolGroup/lv1', lv_tags=tags)
        self.mock_volumes = []
        self.mock_volumes.append([vol])
        self.mock_volumes.append([vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([])

        self.mock_single_volumes = {'/dev/VolGroup/lv1': vol}

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        result = migrate.find_associated_devices(osd_id='0', osd_fsid='1234')
        assert len(result) == 1
        assert result[0][0].path == '/dev/VolGroup/lv1'
        assert result[0][0].lvs == [vol]
        assert result[0][1] == 'block'

    def test_lv_is_matched_id2(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234'
        vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=tags)
        tags2 = 'ceph.osd_id=0,ceph.journal_uuid=xx,ceph.type=wal,ceph.osd_fsid=1234'
        vol2 = api.Volume(lv_name='volume2', lv_uuid='z', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=tags2)
        self.mock_volumes = []
        self.mock_volumes.append([vol])
        self.mock_volumes.append([vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([vol2])

        self.mock_single_volumes = {'/dev/VolGroup/lv1': vol, '/dev/VolGroup/lv2': vol2}

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        result = migrate.find_associated_devices(osd_id='0', osd_fsid='1234')
        assert len(result) == 2
        for d in result:
          if d[1] == 'block':
            assert d[0].path == '/dev/VolGroup/lv1'
            assert d[0].lvs == [vol]
          elif d[1] == 'wal':
            assert d[0].path == '/dev/VolGroup/lv2'
            assert d[0].lvs == [vol2]
          else:
            assert False

    def test_lv_is_matched_id3(self, monkeypatch):
        tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234'
        vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=tags)
        tags2 = 'ceph.osd_id=0,ceph.journal_uuid=xx,ceph.type=wal,ceph.osd_fsid=1234'
        vol2 = api.Volume(lv_name='volume2', lv_uuid='z', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=tags2)
        tags3 = 'ceph.osd_id=0,ceph.journal_uuid=xx,ceph.type=db,ceph.osd_fsid=1234'
        vol3 = api.Volume(lv_name='volume3', lv_uuid='z', vg_name='vg',
                         lv_path='/dev/VolGroup/lv3', lv_tags=tags3)

        self.mock_volumes = []
        self.mock_volumes.append([vol])
        self.mock_volumes.append([vol])
        self.mock_volumes.append([vol3])
        self.mock_volumes.append([vol2])

        self.mock_single_volumes = {'/dev/VolGroup/lv1': vol,
                                    '/dev/VolGroup/lv2': vol2,
                                    '/dev/VolGroup/lv3': vol3}

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        result = migrate.find_associated_devices(osd_id='0', osd_fsid='1234')
        assert len(result) == 3
        for d in result:
          if d[1] == 'block':
            assert d[0].path == '/dev/VolGroup/lv1'
            assert d[0].lvs == [vol]
          elif d[1] == 'wal':
            assert d[0].path == '/dev/VolGroup/lv2'
            assert d[0].lvs == [vol2]
          elif d[1] == 'db':
            assert d[0].path == '/dev/VolGroup/lv3'
            assert d[0].lvs == [vol3]
          else:
            assert False

    def test_lv_is_not_matched(self, monkeypatch, capsys):
        self.mock_volumes = [None]
        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)
        monkeypatch.setattr(process, 'call', lambda x, **kw: ('', '', 0))

        with pytest.raises(SystemExit) as error:
            migrate.find_associated_devices(osd_id='1', osd_fsid='1234')
        stdout, stderr = capsys.readouterr()
        expected = 'Unexpected error, terminating'
        assert expected in str(error.value)
        expected = 'Unable to find any LV for source OSD: id:1 fsid:1234'
        assert expected in stderr

class TestVolumeTagTracker(object):
    mock_single_volumes = {}
    def mock_get_single_lv(self, *args, **kwargs):
        p = kwargs['filters']['lv_path']
        return self.mock_single_volumes[p]

    mock_process_input = []
    def mock_process(self, *args, **kwargs):
        self.mock_process_input.append(args[0]);
        return ('', '', 0)

    def test_init(self, monkeypatch):
        source_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234'
        source_db_tags = 'ceph.osd_id=0,journal_uuid=x,ceph.type=db, osd_fsid=1234'
        source_wal_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=wal'
        target_tags="ceph.a=1,ceph.b=2,c=3,ceph.d=4" # 'c' to be bypassed
        devices=[]

        data_vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=source_db_tags)
        wal_vol = api.Volume(lv_name='volume3', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv3', lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv2': db_vol,
                                    '/dev/VolGroup/lv3': wal_vol}
        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        data_device = Device(path = '/dev/VolGroup/lv1')
        db_device = Device(path = '/dev/VolGroup/lv2')
        wal_device = Device(path = '/dev/VolGroup/lv3')
        devices.append([data_device, 'block'])
        devices.append([db_device, 'db'])
        devices.append([wal_device, 'wal'])

        target = api.Volume(lv_name='target_name', lv_tags=target_tags,
            lv_path='/dev/VolGroup/lv_target')
        t = migrate.VolumeTagTracker(devices, target);

        assert 3 == len(t.old_target_tags)

        assert data_device == t.data_device
        assert 4 == len(t.old_data_tags)
        assert 'data' == t.old_data_tags['ceph.type']

        assert db_device == t.db_device
        assert 2 == len(t.old_db_tags)
        assert 'db' == t.old_db_tags['ceph.type']

        assert wal_device == t.wal_device
        assert 3 == len(t.old_wal_tags)
        assert 'wal' == t.old_wal_tags['ceph.type']

    def test_update_tags_when_lv_create(self, monkeypatch):
        source_tags = \
        'ceph.osd_id=0,ceph.journal_uuid=x,' \
        'ceph.type=data,ceph.osd_fsid=1234'
        source_db_tags = \
        'ceph.osd_id=0,journal_uuid=x,ceph.type=db,' \
        'osd_fsid=1234'

        devices=[]

        data_vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=source_db_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv2': db_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        data_device = Device(path = '/dev/VolGroup/lv1')
        db_device = Device(path = '/dev/VolGroup/lv2')
        devices.append([data_device, 'block'])
        devices.append([db_device, 'db'])

        target = api.Volume(lv_name='target_name', lv_tags='',
            lv_uuid='wal_uuid',
            lv_path='/dev/VolGroup/lv_target')
        t = migrate.VolumeTagTracker(devices, target);

        self.mock_process_input = []
        t.update_tags_when_lv_create('wal')

        assert 3 == len(self.mock_process_input)

        assert ['lvchange',
                '--addtag', 'ceph.wal_uuid=wal_uuid',
                '--addtag', 'ceph.wal_device=/dev/VolGroup/lv_target',
                '/dev/VolGroup/lv1'] == self.mock_process_input[0]

        assert  self.mock_process_input[1].sort() == [
                'lvchange',
                '--addtag', 'ceph.osd_id=0',
                '--addtag', 'ceph.journal_uuid=x',
                '--addtag', 'ceph.type=wal',
                '--addtag', 'ceph.osd_fsid=1234',
                '--addtag', 'ceph.wal_uuid=wal_uuid',
                '--addtag', 'ceph.wal_device=/dev/VolGroup/lv_target',
                '/dev/VolGroup/lv_target'].sort()

        assert ['lvchange',
                '--addtag', 'ceph.wal_uuid=wal_uuid',
                '--addtag', 'ceph.wal_device=/dev/VolGroup/lv_target',
                '/dev/VolGroup/lv2'] == self.mock_process_input[2]

    def test_remove_lvs(self, monkeypatch):
        source_tags = \
        'ceph.osd_id=0,ceph.journal_uuid=x,' \
        'ceph.type=data,ceph.osd_fsid=1234,ceph.wal_uuid=aaaaa'
        source_db_tags = \
        'ceph.osd_id=0,journal_uuid=x,ceph.type=db,' \
        'osd_fsid=1234,ceph.wal_device=aaaaa'
        source_wal_tags = \
        'ceph.wal_uuid=uuid,ceph.wal_device=device,' \
        'ceph.osd_id=0,ceph.type=wal'

        devices=[]

        data_vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=source_db_tags)
        wal_vol = api.Volume(lv_name='volume3', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv3', lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv2': db_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        data_device = Device(path = '/dev/VolGroup/lv1')
        db_device = Device(path = '/dev/VolGroup/lv2')
        wal_device = Device(path = '/dev/VolGroup/lv3')
        devices.append([data_device, 'block'])
        devices.append([db_device, 'db'])
        devices.append([wal_device, 'wal'])

        target = api.Volume(lv_name='target_name', lv_tags='',
            lv_path='/dev/VolGroup/lv_target')
        t = migrate.VolumeTagTracker(devices, target);

        device_to_remove = devices.copy()

        self.mock_process_input = []
        t.remove_lvs(device_to_remove, 'db')

        assert 3 == len(self.mock_process_input)
        assert ['lvchange',
                '--deltag', 'ceph.wal_uuid=uuid',
                '--deltag', 'ceph.wal_device=device',
                '--deltag', 'ceph.osd_id=0',
                '--deltag', 'ceph.type=wal',
                '/dev/VolGroup/lv3'] == self.mock_process_input[0]
        assert ['lvchange',
                '--deltag', 'ceph.wal_uuid=aaaaa',
                '/dev/VolGroup/lv1'] == self.mock_process_input[1]
        assert ['lvchange',
                '--deltag', 'ceph.wal_device=aaaaa',
                '/dev/VolGroup/lv2'] == self.mock_process_input[2]

    def test_replace_lvs(self, monkeypatch):
        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234,'\
        'ceph.wal_uuid=wal_uuid,ceph.db_device=/dbdevice'
        source_db_tags = \
        'ceph.osd_id=0,ceph.type=db,ceph.osd_fsid=1234'
        source_wal_tags = \
        'ceph.wal_uuid=uuid,ceph.wal_device=device,' \
        'ceph.osd_id=0,ceph.type=wal'

        devices=[]

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2', lv_uuid='dbuuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=source_db_tags)
        wal_vol = api.Volume(lv_name='volume3', lv_uuid='waluuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv3', lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv2': db_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        data_device = Device(path = '/dev/VolGroup/lv1')
        db_device = Device(path = '/dev/VolGroup/lv2')
        wal_device = Device(path = '/dev/VolGroup/lv3')
        devices.append([data_device, 'block'])
        devices.append([db_device, 'db'])
        devices.append([wal_device, 'wal'])

        target = api.Volume(lv_name='target_name',
            lv_uuid='ttt',
            lv_tags='ceph.tag_to_remove=aaa',
            lv_path='/dev/VolGroup/lv_target')
        t = migrate.VolumeTagTracker(devices, target);

        self.mock_process_input = []
        t.replace_lvs(devices, 'db')

        assert 5 == len(self.mock_process_input)

        assert ['lvchange',
                '--deltag', 'ceph.osd_id=0',
                '--deltag', 'ceph.type=db',
                '--deltag', 'ceph.osd_fsid=1234',
                '/dev/VolGroup/lv2'] == self.mock_process_input[0]
        assert ['lvchange',
                '--deltag', 'ceph.wal_uuid=uuid',
                '--deltag', 'ceph.wal_device=device',
                '--deltag', 'ceph.osd_id=0',
                '--deltag', 'ceph.type=wal',
                '/dev/VolGroup/lv3'] == self.mock_process_input[1]
        assert ['lvchange',
                '--deltag', 'ceph.db_device=/dbdevice',
                '--deltag', 'ceph.wal_uuid=wal_uuid',
                '/dev/VolGroup/lv1'] == self.mock_process_input[2]

        assert ['lvchange',
                '--addtag', 'ceph.db_uuid=ttt',
                '--addtag', 'ceph.db_device=/dev/VolGroup/lv_target',
                '/dev/VolGroup/lv1'] == self.mock_process_input[3]

        assert self.mock_process_input[4].sort() == [
            'lvchange',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.db_uuid=ttt',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv_target',
            '/dev/VolGroup/lv_target'].sort()

    def test_undo(self, monkeypatch):
        source_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=data,ceph.osd_fsid=1234'
        source_db_tags = 'ceph.osd_id=0,journal_uuid=x,ceph.type=db, osd_fsid=1234'
        source_wal_tags = 'ceph.osd_id=0,ceph.journal_uuid=x,ceph.type=wal'
        target_tags=""
        devices=[]

        data_vol = api.Volume(lv_name='volume1', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv2', lv_tags=source_db_tags)
        wal_vol = api.Volume(lv_name='volume3', lv_uuid='y', vg_name='vg',
                         lv_path='/dev/VolGroup/lv3', lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv2': db_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        data_device = Device(path = '/dev/VolGroup/lv1')
        db_device = Device(path = '/dev/VolGroup/lv2')
        wal_device = Device(path = '/dev/VolGroup/lv3')
        devices.append([data_device, 'block'])
        devices.append([db_device, 'db'])
        devices.append([wal_device, 'wal'])

        target = api.Volume(lv_name='target_name', lv_tags=target_tags,
            lv_path='/dev/VolGroup/lv_target')
        t = migrate.VolumeTagTracker(devices, target);

        target.tags['ceph.a'] = 'aa';
        target.tags['ceph.b'] = 'bb';

        data_vol.tags['ceph.journal_uuid'] = 'z';

        db_vol.tags.pop('ceph.type')

        wal_vol.tags.clear()

        assert 2 == len(target.tags)
        assert 4 == len(data_vol.tags)
        assert 1 == len(db_vol.tags)

        self.mock_process_input = []
        t.undo()

        assert 0 == len(target.tags)
        assert 4 == len(data_vol.tags)
        assert 'x' == data_vol.tags['ceph.journal_uuid']

        assert 2 == len(db_vol.tags)
        assert 'db' == db_vol.tags['ceph.type']

        assert 3 == len(wal_vol.tags)
        assert 'wal' == wal_vol.tags['ceph.type']

        assert 6 == len(self.mock_process_input)
        assert 'lvchange' in self.mock_process_input[0]
        assert '--deltag' in self.mock_process_input[0]
        assert 'ceph.journal_uuid=z' in self.mock_process_input[0]
        assert '/dev/VolGroup/lv1' in self.mock_process_input[0]

        assert 'lvchange' in self.mock_process_input[1]
        assert '--addtag' in self.mock_process_input[1]
        assert 'ceph.journal_uuid=x' in self.mock_process_input[1]
        assert '/dev/VolGroup/lv1' in self.mock_process_input[1]

        assert 'lvchange' in self.mock_process_input[2]
        assert '--deltag' in self.mock_process_input[2]
        assert 'ceph.osd_id=0' in self.mock_process_input[2]
        assert '/dev/VolGroup/lv2' in self.mock_process_input[2]

        assert 'lvchange' in self.mock_process_input[3]
        assert '--addtag' in self.mock_process_input[3]
        assert 'ceph.type=db' in self.mock_process_input[3]
        assert '/dev/VolGroup/lv2' in self.mock_process_input[3]

        assert 'lvchange' in self.mock_process_input[4]
        assert '--addtag' in self.mock_process_input[4]
        assert 'ceph.type=wal' in self.mock_process_input[4]
        assert '/dev/VolGroup/lv3' in self.mock_process_input[4]

        assert 'lvchange' in self.mock_process_input[5]
        assert '--deltag' in self.mock_process_input[5]
        assert 'ceph.a=aa' in self.mock_process_input[5]
        assert 'ceph.b=bb' in self.mock_process_input[5]
        assert '/dev/VolGroup/lv_target' in self.mock_process_input[5]

class TestNew(object):

    mock_volume = None
    def mock_get_lv_by_fullname(self, *args, **kwargs):
        return self.mock_volume

    mock_process_input = []
    def mock_process(self, *args, **kwargs):
        self.mock_process_input.append(args[0]);
        return ('', '', 0)

    mock_single_volumes = {}
    def mock_get_single_lv(self, *args, **kwargs):
        p = kwargs['filters']['lv_path']
        return self.mock_single_volumes[p]

    mock_volumes = []
    def mock_get_lvs(self, *args, **kwargs):
        return self.mock_volumes.pop(0)

    def mock_prepare_dmcrypt(self, *args, **kwargs):
        return '/dev/mapper/' + kwargs['mapping']

    def test_newdb_non_root(self):
        with pytest.raises(Exception) as error:
            migrate.NewDB(argv=[
                '--osd-id', '1',
                '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
                '--target', 'vgname/new_db']).main()
        expected = 'This command needs to be executed with sudo or as root'
        assert expected in str(error.value)

    @patch('os.getuid')
    def test_newdb_not_target_lvm(self, m_getuid, capsys):
        m_getuid.return_value = 0
        with pytest.raises(SystemExit) as error:
            migrate.NewDB(argv=[
                '--osd-id', '1',
                '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
                '--target', 'vgname/new_db']).main()
        stdout, stderr = capsys.readouterr()
        expected = 'Unable to attach new volume : vgname/new_db'
        assert expected in str(error.value)
        expected = 'Target path vgname/new_db is not a Logical Volume'
        assert expected in stderr


    @patch('os.getuid')
    def test_newdb_already_in_use(self, m_getuid, monkeypatch, capsys):
        m_getuid.return_value = 0

        self.mock_volume = api.Volume(lv_name='volume1',
                                      lv_uuid='y',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv1',
                                      lv_tags='ceph.osd_id=5') # this results in set used_by_ceph
        monkeypatch.setattr(api, 'get_lv_by_fullname', self.mock_get_lv_by_fullname)

        with pytest.raises(SystemExit) as error:
            migrate.NewDB(argv=[
                '--osd-id', '1',
                '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
                '--target', 'vgname/new_db']).main()
        stdout, stderr = capsys.readouterr()
        expected = 'Unable to attach new volume : vgname/new_db'
        assert expected in str(error.value)
        expected = 'Target Logical Volume is already used by ceph: vgname/new_db'
        assert expected in stderr

    @patch('os.getuid')
    def test_newdb(self, m_getuid, monkeypatch, capsys):
        m_getuid.return_value = 0

        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234,'\
        'ceph.wal_uuid=wal_uuid,ceph.db_device=/dbdevice'
        source_wal_tags = \
        'ceph.wal_uuid=uuid,ceph.wal_device=device,' \
        'ceph.osd_id=0,ceph.type=wal'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol, wal_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([wal_vol])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph_cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        migrate.NewDB(argv=[
            '--osd-id', '1',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_db']).main()

        n = len(self.mock_process_input)
        assert n >= 5

        assert self.mock_process_input[n - 5] == [
            'lvchange',
            '--deltag', 'ceph.db_device=/dbdevice',
            '/dev/VolGroup/lv1']
        assert self.mock_process_input[n - 4] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv1']

        assert self.mock_process_input[n - 3].sort() == [
            'lvchange',
            '--addtag', 'ceph.wal_uuid=uuid',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/target_volume'].sort()

        assert self.mock_process_input[n - 2] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv3']

        assert self.mock_process_input[n - 1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph_cluster-1',
            '--dev-target', '/dev/VolGroup/target_volume',
            '--command', 'bluefs-bdev-new-db']

    def test_newdb_active_systemd(self, is_root, monkeypatch, capsys):
        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234,'\
        'ceph.wal_uuid=wal_uuid,ceph.db_device=/dbdevice'
        source_wal_tags = \
        'ceph.wal_uuid=uuid,ceph.wal_device=device,' \
        'ceph.osd_id=0,ceph.type=wal'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: True)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol, wal_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([wal_vol])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph_cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        m = migrate.NewDB(argv=[
            '--osd-id', '1',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_db'])

        with pytest.raises(SystemExit) as error:
            m.main()

        stdout, stderr = capsys.readouterr()

        assert 'Unable to attach new volume for OSD: 1' == str(error.value)
        assert '--> OSD ID is running, stop it with: systemctl stop ceph-osd@1' == stderr.rstrip()
        assert not stdout

    def test_newdb_no_systemd(self, is_root, monkeypatch):
        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234,'\
        'ceph.wal_uuid=wal_uuid,ceph.db_device=/dbdevice'
        source_wal_tags = \
        'ceph.wal_uuid=uuid,ceph.wal_device=device,' \
        'ceph.osd_id=0,ceph.type=wal'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol,
                                    '/dev/VolGroup/lv3': wal_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol, wal_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([wal_vol])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph_cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        migrate.NewDB(argv=[
            '--osd-id', '1',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_db',
            '--no-systemd']).main()

        n = len(self.mock_process_input)
        assert n >= 5

        assert self.mock_process_input[n - 5] == [
            'lvchange',
            '--deltag', 'ceph.db_device=/dbdevice',
            '/dev/VolGroup/lv1']
        assert self.mock_process_input[n - 4] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv1']

        assert self.mock_process_input[n - 3].sort() == [
            'lvchange',
            '--addtag', 'ceph.wal_uuid=uuid',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/target_volume'].sort()

        assert self.mock_process_input[n - 2] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=y',
            '--addtag', 'ceph.db_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv3']

        assert self.mock_process_input[n - 1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph_cluster-1',
            '--dev-target', '/dev/VolGroup/target_volume',
            '--command', 'bluefs-bdev-new-db']

    @patch('os.getuid')
    def test_newwal(self, m_getuid, monkeypatch, capsys):
        m_getuid.return_value = 0

        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y', vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname', self.mock_get_lv_by_fullname)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active", lambda id: False)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name', lambda osd_id, osd_fsid: 'cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        migrate.NewWAL(argv=[
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_wal']).main()

        n = len(self.mock_process_input)
        assert n >= 3

        assert self.mock_process_input[n - 3] == [
            'lvchange',
            '--addtag', 'ceph.wal_uuid=y',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv1']

        assert self.mock_process_input[n - 2].sort() == [
            'lvchange',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.type=wal',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.wal_uuid=y',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/target_volume'].sort()

        assert self.mock_process_input[n - 1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/cluster-2',
            '--dev-target', '/dev/VolGroup/target_volume',
            '--command', 'bluefs-bdev-new-wal']

    def test_newwal_active_systemd(self, is_root, monkeypatch, capsys):
        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y', vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname', self.mock_get_lv_by_fullname)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active", lambda id: True)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name', lambda osd_id, osd_fsid: 'cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        m = migrate.NewWAL(argv=[
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_wal'])

        with pytest.raises(SystemExit) as error:
            m.main()

        stdout, stderr = capsys.readouterr()

        assert 'Unable to attach new volume for OSD: 2' == str(error.value)
        assert '--> OSD ID is running, stop it with: systemctl stop ceph-osd@2' == stderr.rstrip()
        assert not stdout

    def test_newwal_no_systemd(self, is_root, monkeypatch):
        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='y', vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname', self.mock_get_lv_by_fullname)

        #find_associated_devices will call get_lvs() 4 times
        # and it this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name', lambda osd_id, osd_fsid: 'cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        migrate.NewWAL(argv=[
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_wal',
            '--no-systemd']).main()

        n = len(self.mock_process_input)
        assert n >= 3

        assert self.mock_process_input[n - 3] == [
            'lvchange',
            '--addtag', 'ceph.wal_uuid=y',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv1']

        assert self.mock_process_input[n - 2].sort() == [
            'lvchange',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.type=wal',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.wal_uuid=y',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/target_volume'].sort()

        assert self.mock_process_input[n - 1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/cluster-2',
            '--dev-target', '/dev/VolGroup/target_volume',
            '--command', 'bluefs-bdev-new-wal']

    @patch('os.getuid')
    def test_newwal_encrypted(self, m_getuid, monkeypatch, capsys):
        m_getuid.return_value = 0

        source_tags = \
        'ceph.osd_id=0,ceph.type=data,ceph.osd_fsid=1234,ceph.encrypted=1'

        data_vol = api.Volume(lv_name='volume1', lv_uuid='datauuid', vg_name='vg',
                         lv_path='/dev/VolGroup/lv1', lv_tags=source_tags)

        self.mock_single_volumes = {'/dev/VolGroup/lv1': data_vol}

        monkeypatch.setattr(migrate.api, 'get_single_lv', self.mock_get_single_lv)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        self.mock_volume = api.Volume(lv_name='target_volume1', lv_uuid='target_uuid', vg_name='vg',
                                      lv_path='/dev/VolGroup/target_volume',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname', self.mock_get_lv_by_fullname)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active", lambda id: False)

        #find_associated_devices will call get_lvs() 4 times
        # and this needs results to be arranged that way
        self.mock_volumes = []
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([data_vol])
        self.mock_volumes.append([])
        self.mock_volumes.append([])

        monkeypatch.setattr(migrate.api, 'get_lvs', self.mock_get_lvs)

        monkeypatch.setattr(migrate, 'get_cluster_name', lambda osd_id, osd_fsid: 'cluster')
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        monkeypatch.setattr(encryption_utils, 'prepare_dmcrypt', self.mock_prepare_dmcrypt)

        migrate.NewWAL(argv=[
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--target', 'vgname/new_wal']).main()

        n = len(self.mock_process_input)
        assert n >= 3

        assert self.mock_process_input[n - 3] == [
            'lvchange',
            '--addtag', 'ceph.wal_uuid=target_uuid',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/lv1']

        assert self.mock_process_input[n - 2].sort() == [
            'lvchange',
            '--addtag', 'ceph.osd_id=0',
            '--addtag', 'ceph.type=wal',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.wal_uuid=target_uuid',
            '--addtag', 'ceph.wal_device=/dev/VolGroup/target_volume',
            '/dev/VolGroup/target_volume'].sort()

        assert self.mock_process_input[n - 1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/cluster-2',
            '--dev-target', '/dev/mapper/target_uuid',
            '--command', 'bluefs-bdev-new-wal']

class TestMigrate(object):

    def test_invalid_osd_id_passed(self, is_root):
        with pytest.raises(SystemExit):
            migrate.Migrate(argv=['--osd-fsid', '123', '--from', 'data', '--target', 'foo', '--osd-id', 'foo']).main()

    mock_volume = None
    def mock_get_lv_by_fullname(self, *args, **kwargs):
        return self.mock_volume

    mock_process_input = []
    def mock_process(self, *args, **kwargs):
        self.mock_process_input.append(args[0])
        return ('', '', 0)

    mock_single_volumes = {}
    def mock_get_single_lv(self, *args, **kwargs):
        p = kwargs['filters']['lv_path']
        return self.mock_single_volumes[p]

    mock_volumes = []
    def mock_get_lvs(self, *args, **kwargs):
        return self.mock_volumes.pop(0)

    mock_prepare_dmcrypt_uuid = ''
    def mock_prepare_dmcrypt(self, *args, **kwargs):
        self.mock_prepare_dmcrypt_uuid = kwargs['mapping']
        return '/dev/mapper/' + kwargs['mapping']

    mock_dmcrypt_close_uuid = []
    def mock_dmcrypt_close(self, *args, **kwargs):
        self.mock_dmcrypt_close_uuid.append(kwargs['mapping'])

    def test_get_source_devices(self, monkeypatch):

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='datauuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='datauuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2', lv_uuid='y',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2',
                                      lv_tags='ceph.osd_id=5,ceph.osd_type=db')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)


        argv = [
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--from', 'data', 'wal',
            '--target', 'vgname/new_wal'
        ]
        m = migrate.Migrate(argv=argv)
        m.args = m.make_parser('ceph-volume lvm migation', 'help').parse_args(argv)
        res_devices = m.get_source_devices(devices)

        assert 2 == len(res_devices)
        assert devices[0] == res_devices[0]
        assert devices[2] == res_devices[1]

        argv = [
            '--osd-id', '2',
            '--osd-fsid', '55BD4219-16A7-4037-BC20-0F158EFCC83D',
            '--from', 'db', 'wal', 'data',
            '--target', 'vgname/new_wal'
        ]
        m = migrate.Migrate(argv=argv)
        m.args = m.make_parser('ceph-volume lvm migation', 'help').parse_args(argv)
        res_devices = m.get_source_devices(devices)

        assert 3 == len(res_devices)
        assert devices[0] == res_devices[0]
        assert devices[1] == res_devices[1]
        assert devices[2] == res_devices[2]


    def test_migrate_without_args(self, capsys):
        help_msg = """
Moves BlueFS data from source volume(s) to the target one, source
volumes (except the main (i.e. data or block) one) are removed on
success. LVM volumes are permitted for Target only, both already
attached or new logical one. In the latter case it is attached to OSD
replacing one of the source devices. Following replacement rules apply
(in the order of precedence, stop on the first match):
* if source list has DB volume - target device replaces it.
* if source list has WAL volume - target device replace it.
* if source list has slow volume only - operation is not permitted,
  requires explicit allocation via new-db/new-wal command.

Example calls for supported scenarios:

  Moves BlueFS data from main device to LV already attached as DB:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/db

  Moves BlueFS data from shared main device to LV which will be attached
   as a new DB:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/new_db

  Moves BlueFS data from DB device to new LV, DB is replaced:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db --target vgname/new_db

  Moves BlueFS data from main and DB devices to new LV, DB is replaced:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db --target vgname/new_db

  Moves BlueFS data from main, DB and WAL devices to new LV, WAL is
   removed and DB is replaced:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db wal --target vgname/new_db

  Moves BlueFS data from main, DB and WAL devices to main device, WAL
   and DB are removed:

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db wal --target vgname/data

"""
        m = migrate.Migrate(argv=[])
        m.main()
        stdout, stderr = capsys.readouterr()
        assert help_msg in stdout
        assert not stderr


    @patch('os.getuid')
    def test_migrate_data_db_to_new_db(self, m_getuid, monkeypatch):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)


        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal'])
        m.main()

        n = len(self.mock_process_input)
        assert n >= 5

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/VolGroup/lv2_new',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db']

    @patch('os.getuid')
    def test_migrate_data_db_to_new_db_encrypted(self, m_getuid, monkeypatch):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,'\
        'ceph.encrypted=1'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,'\
        'ceph.encrypted=1'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)


        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr(encryption_utils, 'prepare_dmcrypt', self.mock_prepare_dmcrypt)
        monkeypatch.setattr(encryption_utils, 'dmcrypt_close', self.mock_dmcrypt_close)

        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal'])
        m.main()

        assert self.mock_prepare_dmcrypt_uuid == self.mock_volume.lv_uuid

        n = len(self.mock_dmcrypt_close_uuid)
        assert n >= 1
        assert self.mock_dmcrypt_close_uuid[n-1] == db_vol.lv_uuid

        n = len(self.mock_process_input)
        assert n >= 5

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.encrypted=1',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.encrypted=1',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/mapper/new-db-uuid',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db']

    def test_migrate_data_db_to_new_db_active_systemd(self, is_root, monkeypatch, capsys):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)


        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: True)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal'])

        with pytest.raises(SystemExit) as error:
            m.main()

        stdout, stderr = capsys.readouterr()

        assert 'Unable to migrate devices associated with OSD ID: 2' == str(error.value)
        assert '--> OSD is running, stop it with: systemctl stop ceph-osd@2' == stderr.rstrip()
        assert not stdout

    def test_migrate_data_db_to_new_db_no_systemd(self, is_root, monkeypatch):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)


        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal',
            '--no-systemd'])
        m.main()

        n = len(self.mock_process_input)
        assert n >= 5

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/VolGroup/lv2_new',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db']

    @patch('os.getuid')
    def test_migrate_data_db_to_new_db_skip_wal(self, m_getuid, monkeypatch):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='datauuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db',
            '--target', 'vgname/new_wal'])
        m.main()

        n = len(self.mock_process_input)
        assert n >= 7

        assert self. mock_process_input[n-7] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-6] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv3']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv3']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/VolGroup/lv2_new',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db']

    @patch('os.getuid')
    def test_migrate_data_db_wal_to_new_db(self, m_getuid, monkeypatch):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=0,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal'])
        m.main()

        n = len(self.mock_process_input)
        assert n >= 6

        assert self. mock_process_input[n-6] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=0',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv3']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/VolGroup/lv2_new',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']

    @patch('os.getuid')
    def test_migrate_data_db_wal_to_new_db_encrypted(self, m_getuid, monkeypatch):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev,ceph.encrypted=1'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.encrypted=1'
        source_wal_tags = 'ceph.osd_id=0,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev,ceph.encrypted=1'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr(encryption_utils, 'prepare_dmcrypt', self.mock_prepare_dmcrypt)
        monkeypatch.setattr(encryption_utils, 'dmcrypt_close', self.mock_dmcrypt_close)

        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data', 'db', 'wal',
            '--target', 'vgname/new_wal'])
        m.main()

        assert self.mock_prepare_dmcrypt_uuid == self.mock_volume.lv_uuid

        n = len(self.mock_dmcrypt_close_uuid)
        assert n >= 2
        assert self.mock_dmcrypt_close_uuid[n-2] == db_vol.lv_uuid
        assert self.mock_dmcrypt_close_uuid[n-1] == wal_vol.lv_uuid

        n = len(self.mock_process_input)
        assert n >= 6

        assert self. mock_process_input[n-6] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=db',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.encrypted=1',
            '/dev/VolGroup/lv2']

        assert self. mock_process_input[n-5] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=0',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '--deltag', 'ceph.encrypted=1',
            '/dev/VolGroup/lv3']

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv1']

        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--addtag', 'ceph.osd_id=2',
            '--addtag', 'ceph.type=db',
            '--addtag', 'ceph.osd_fsid=1234',
            '--addtag', 'ceph.cluster_name=ceph',
            '--addtag', 'ceph.encrypted=1',
            '--addtag', 'ceph.db_uuid=new-db-uuid',
            '--addtag', 'ceph.db_device=/dev/VolGroup/lv2_new',
            '/dev/VolGroup/lv2_new']

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/dev/mapper/new-db-uuid',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.db',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']

    @patch('os.getuid')
    def test_dont_migrate_data_db_wal_to_new_data(self,
                                                  m_getuid,
                                                  monkeypatch,
                                                  capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = api.Volume(lv_name='volume2_new', lv_uuid='new-db-uuid',
                                      vg_name='vg',
                                      lv_path='/dev/VolGroup/lv2_new',
                                      lv_tags='')
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'data',
            '--target', 'vgname/new_data'])

        with pytest.raises(SystemExit) as error:
            m.main()
        stdout, stderr = capsys.readouterr()
        expected = 'Unable to migrate to : vgname/new_data'
        assert expected in str(error.value)
        expected = 'Unable to determine new volume type,'
        ' please use new-db or new-wal command before.'
        assert expected in stderr

    @patch('os.getuid')
    def test_dont_migrate_db_to_wal(self,
                                    m_getuid,
                                    monkeypatch,
                                    capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = wal_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db',
            '--target', 'vgname/wal'])

        with pytest.raises(SystemExit) as error:
            m.main()
        stdout, stderr = capsys.readouterr()
        expected = 'Unable to migrate to : vgname/wal'
        assert expected in str(error.value)
        expected = 'Migrate to WAL is not supported'
        assert expected in stderr

    @patch('os.getuid')
    def test_migrate_data_db_to_db(self,
                                    m_getuid,
                                    monkeypatch,
                                    capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data',
            '--target', 'vgname/db'])

        m.main()

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block.db',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block']

    def test_migrate_data_db_to_db_active_systemd(self, is_root, monkeypatch, capsys):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: True)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data',
            '--target', 'vgname/db'])

        with pytest.raises(SystemExit) as error:
            m.main()

        stdout, stderr = capsys.readouterr()

        assert 'Unable to migrate devices associated with OSD ID: 2' == str(error.value)
        assert '--> OSD is running, stop it with: systemctl stop ceph-osd@2' == stderr.rstrip()
        assert not stdout

    def test_migrate_data_db_to_db_no_systemd(self, is_root, monkeypatch):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data',
            '--target', 'vgname/db',
            '--no-systemd'])

        m.main()

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block.db',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block']

    @patch('os.getuid')
    def test_migrate_data_wal_to_db(self,
                                    m_getuid,
                                    monkeypatch,
                                    capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data', 'wal',
            '--target', 'vgname/db'])

        m.main()

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv3']
        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']
        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv2']
        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block.db',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']

    @patch('os.getuid')
    def test_migrate_wal_to_db(self,
                                m_getuid,
                                monkeypatch,
                                capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = data_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'wal',
            '--target', 'vgname/data'])

        m.main()

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv3']
        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']
        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']

    @patch('os.getuid')
    def test_migrate_data_wal_to_db_encrypted(self,
                                              m_getuid,
                                              monkeypatch,
                                              capsys):
        m_getuid.return_value = 0

        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev,ceph.encrypted=1'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev,ceph.encrypted=1'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev,ceph.encrypted=1'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: False)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr(encryption_utils, 'prepare_dmcrypt', self.mock_prepare_dmcrypt)
        monkeypatch.setattr(encryption_utils, 'dmcrypt_close', self.mock_dmcrypt_close)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data', 'wal',
            '--target', 'vgname/db'])

        m.main()

        assert self.mock_prepare_dmcrypt_uuid == ''

        n = len(self.mock_dmcrypt_close_uuid)
        assert n >= 1
        assert self.mock_dmcrypt_close_uuid[n-1] == wal_vol.lv_uuid

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '--deltag', 'ceph.encrypted=1',
            '/dev/VolGroup/lv3']
        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']
        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv2']
        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block.db',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']

    def test_migrate_data_wal_to_db_active_systemd(self, is_root, monkeypatch, capsys):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr("ceph_volume.systemd.systemctl.osd_is_active",
            lambda id: True)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data', 'wal',
            '--target', 'vgname/db'])

        with pytest.raises(SystemExit) as error:
            m.main()

        stdout, stderr = capsys.readouterr()

        assert 'Unable to migrate devices associated with OSD ID: 2' == str(error.value)
        assert '--> OSD is running, stop it with: systemctl stop ceph-osd@2' == stderr.rstrip()
        assert not stdout

    def test_migrate_data_wal_to_db_no_systemd(self, is_root, monkeypatch):
        source_tags = 'ceph.osd_id=2,ceph.type=data,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_db_tags = 'ceph.osd_id=2,ceph.type=db,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'
        source_wal_tags = 'ceph.osd_id=2,ceph.type=wal,ceph.osd_fsid=1234,' \
        'ceph.cluster_name=ceph,ceph.db_uuid=dbuuid,ceph.db_device=db_dev,' \
        'ceph.wal_uuid=waluuid,ceph.wal_device=wal_dev'

        data_vol = api.Volume(lv_name='volume1',
                              lv_uuid='datauuid',
                              vg_name='vg',
                              lv_path='/dev/VolGroup/lv1',
                              lv_tags=source_tags)
        db_vol = api.Volume(lv_name='volume2',
                            lv_uuid='dbuuid',
                            vg_name='vg',
                            lv_path='/dev/VolGroup/lv2',
                            lv_tags=source_db_tags)

        wal_vol = api.Volume(lv_name='volume3',
                             lv_uuid='waluuid',
                             vg_name='vg',
                             lv_path='/dev/VolGroup/lv3',
                             lv_tags=source_wal_tags)

        self.mock_single_volumes = {
            '/dev/VolGroup/lv1': data_vol,
            '/dev/VolGroup/lv2': db_vol,
            '/dev/VolGroup/lv3': wal_vol,
        }
        monkeypatch.setattr(migrate.api, 'get_single_lv',
            self.mock_get_single_lv)

        self.mock_volume = db_vol
        monkeypatch.setattr(api, 'get_lv_by_fullname',
            self.mock_get_lv_by_fullname)

        self.mock_process_input = []
        monkeypatch.setattr(process, 'call', self.mock_process)

        devices = []
        devices.append([Device('/dev/VolGroup/lv1'), 'block'])
        devices.append([Device('/dev/VolGroup/lv2'), 'db'])
        devices.append([Device('/dev/VolGroup/lv3'), 'wal'])

        monkeypatch.setattr(migrate, 'find_associated_devices',
            lambda osd_id, osd_fsid: devices)

        monkeypatch.setattr(migrate, 'get_cluster_name',
            lambda osd_id, osd_fsid: 'ceph')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        m = migrate.Migrate(argv=[
            '--osd-id', '2',
            '--osd-fsid', '1234',
            '--from', 'db', 'data', 'wal',
            '--target', 'vgname/db',
            '--no-systemd'])

        m.main()

        n = len(self.mock_process_input)
        assert n >= 1
        for s in self.mock_process_input:
            print(s)

        assert self. mock_process_input[n-4] == [
            'lvchange',
            '--deltag', 'ceph.osd_id=2',
            '--deltag', 'ceph.type=wal',
            '--deltag', 'ceph.osd_fsid=1234',
            '--deltag', 'ceph.cluster_name=ceph',
            '--deltag', 'ceph.db_uuid=dbuuid',
            '--deltag', 'ceph.db_device=db_dev',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv3']
        assert self. mock_process_input[n-3] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv1']
        assert self. mock_process_input[n-2] == [
            'lvchange',
            '--deltag', 'ceph.wal_uuid=waluuid',
            '--deltag', 'ceph.wal_device=wal_dev',
            '/dev/VolGroup/lv2']
        assert self. mock_process_input[n-1] == [
            'ceph-bluestore-tool',
            '--path', '/var/lib/ceph/osd/ceph-2',
            '--dev-target', '/var/lib/ceph/osd/ceph-2/block.db',
            '--command', 'bluefs-bdev-migrate',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block',
            '--devs-source', '/var/lib/ceph/osd/ceph-2/block.wal']
