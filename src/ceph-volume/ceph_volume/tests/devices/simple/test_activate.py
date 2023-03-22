import os
import pytest
from ceph_volume.devices.simple import activate


class TestActivate(object):

    def test_no_data_uuid(self, factory, is_root, monkeypatch, capture, fake_filesystem):
        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        args = factory(osd_id='0', osd_fsid='1234', json_config='/tmp/json-config')
        with pytest.raises(RuntimeError):
            activate.Activate([]).activate(args)

    def test_invalid_json_path(self):
        os.environ['CEPH_VOLUME_SIMPLE_JSON_DIR'] = '/non/existing/path'
        with pytest.raises(RuntimeError) as error:
            activate.Activate(['1', 'asdf']).main()
        assert 'Expected JSON config path not found' in str(error.value)

    def test_main_spits_help_with_no_arguments(self, capsys):
        activate.Activate([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Activate OSDs by mounting devices previously configured' in stdout

    def test_activate_all(self, is_root, monkeypatch):
        '''
        make sure Activate calls activate for each file returned by glob
        '''
        mocked_glob = []
        def mock_glob(glob):
            path = os.path.dirname(glob)
            mocked_glob.extend(['{}/{}.json'.format(path, file_) for file_ in
                                ['1', '2', '3']])
            return mocked_glob
        activate_files = []
        def mock_activate(self, args):
            activate_files.append(args.json_config)
        monkeypatch.setattr('glob.glob', mock_glob)
        monkeypatch.setattr(activate.Activate, 'activate', mock_activate)
        activate.Activate(['--all']).main()
        assert activate_files == mocked_glob




class TestEnableSystemdUnits(object):

    def test_nothing_is_activated(self, is_root, capsys, fake_filesystem):
        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--no-systemd', '--file', '/tmp/json-config', '0', '1234'], from_trigger=True)
        activation.activate = lambda x: True
        activation.main()
        activation.enable_systemd_units('0', '1234')
        stdout, stderr = capsys.readouterr()
        assert 'Skipping enabling of `simple`' in stderr
        assert 'Skipping masking of ceph-disk' in stderr
        assert 'Skipping enabling and starting OSD simple' in stderr

    def test_no_systemd_flag_is_true(self, is_root, fake_filesystem):
        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--no-systemd', '--file', '/tmp/json-config', '0', '1234'], from_trigger=True)
        activation.activate = lambda x: True
        activation.main()
        assert activation.skip_systemd is True

    def test_no_systemd_flag_is_false(self, is_root, fake_filesystem):
        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--file', '/tmp/json-config', '0', '1234'], from_trigger=True)
        activation.activate = lambda x: True
        activation.main()
        assert activation.skip_systemd is False

    def test_masks_ceph_disk(self, is_root, monkeypatch, capture, fake_filesystem):
        monkeypatch.setattr('ceph_volume.systemd.systemctl.mask_ceph_disk', capture)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_volume', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_osd', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.start_osd', lambda *a: True)

        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--file', '/tmp/json-config', '0', '1234'], from_trigger=False)
        activation.activate = lambda x: True
        activation.main()
        activation.enable_systemd_units('0', '1234')
        assert len(capture.calls) == 1

    def test_enables_simple_unit(self, is_root, monkeypatch, capture, fake_filesystem):
        monkeypatch.setattr('ceph_volume.systemd.systemctl.mask_ceph_disk', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_volume', capture)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_osd', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.start_osd', lambda *a: True)

        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--file', '/tmp/json-config', '0', '1234'], from_trigger=False)
        activation.activate = lambda x: True
        activation.main()
        activation.enable_systemd_units('0', '1234')
        assert len(capture.calls) == 1
        assert capture.calls[0]['args'] == ('0', '1234', 'simple')

    def test_enables_osd_unit(self, is_root, monkeypatch, capture, fake_filesystem):
        monkeypatch.setattr('ceph_volume.systemd.systemctl.mask_ceph_disk', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_volume', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_osd', capture)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.start_osd', lambda *a: True)

        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--file', '/tmp/json-config', '0', '1234'], from_trigger=False)
        activation.activate = lambda x: True
        activation.main()
        activation.enable_systemd_units('0', '1234')
        assert len(capture.calls) == 1
        assert capture.calls[0]['args'] == ('0',)

    def test_starts_osd_unit(self, is_root, monkeypatch, capture, fake_filesystem):
        monkeypatch.setattr('ceph_volume.systemd.systemctl.mask_ceph_disk', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_volume', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.enable_osd', lambda *a: True)
        monkeypatch.setattr('ceph_volume.systemd.systemctl.start_osd', capture)

        fake_filesystem.create_file('/tmp/json-config', contents='{}')
        activation = activate.Activate(['--file', '/tmp/json-config', '0', '1234'], from_trigger=False)
        activation.activate = lambda x: True
        activation.main()
        activation.enable_systemd_units('0', '1234')
        assert len(capture.calls) == 1
        assert capture.calls[0]['args'] == ('0',)


class TestValidateDevices(object):

    def test_bluestore_with_all_devices(self):
        activation = activate.Activate([])
        result = activation.validate_devices({'type': 'bluestore', 'data': {}, 'block': {}})
        assert result is True

    def test_bluestore_without_type(self):
        activation = activate.Activate([])
        result = activation.validate_devices({'data': {}, 'block': {}})
        assert result is True

    def test_bluestore_is_default(self):
        activation = activate.Activate([])
        result = activation.validate_devices({'data': {}, 'block': {}})
        assert result is True

    def test_bluestore_data_device_found(self, capsys):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError):
            activation.validate_devices({'data': {}})
        stdout, stderr = capsys.readouterr()
        assert "devices found: ['data']" in stderr

    def test_bluestore_missing_data(self):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError) as error:
            activation.validate_devices({'type': 'bluestore', 'block': {}})
        assert 'Unable to activate bluestore OSD due to missing devices' in str(error.value)

    def test_bluestore_block_device_found(self, capsys):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError):
            activation.validate_devices({'block': {}})
        stdout, stderr = capsys.readouterr()
        assert "devices found: ['block']" in stderr
