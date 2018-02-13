import os
import pytest
from ceph_volume.devices.simple import activate


class TestActivate(object):

    def test_no_data_uuid(self, factory, tmpfile, is_root, monkeypatch, capture):
        json_config = tmpfile(contents='{}')
        args = factory(osd_id='0', osd_fsid='1234', json_config=json_config)
        with pytest.raises(RuntimeError):
            activate.Activate([]).activate(args)

    def test_invalid_json_path(self):
        os.environ['CEPH_VOLUME_SIMPLE_JSON_DIR'] = '/non/existing/path'
        with pytest.raises(RuntimeError) as error:
            activate.Activate(['1', 'asdf']).main()
        assert 'RuntimeError: Expected JSON config path not found' in str(error)

    def test_main_spits_help_with_no_arguments(self, capsys):
        activate.Activate([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Activate OSDs by mounting devices previously configured' in stdout


class TestValidateDevices(object):

    def test_filestore_missing_journal(self):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError) as error:
            activation.validate_devices({'type': 'filestore', 'data': {}})
        assert 'Unable to activate filestore OSD due to missing devices' in str(error)

    def test_filestore_missing_data(self):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError) as error:
            activation.validate_devices({'type': 'filestore', 'journal': {}})
        assert 'Unable to activate filestore OSD due to missing devices' in str(error)

    def test_filestore_journal_device_found(self, capsys):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError):
            activation.validate_devices({'type': 'filestore', 'journal': {}})
        stdout, stderr = capsys.readouterr()
        assert "devices found: ['journal']" in stdout

    def test_filestore_data_device_found(self, capsys):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError):
            activation.validate_devices({'type': 'filestore', 'data': {}})
        stdout, stderr = capsys.readouterr()
        assert "devices found: ['data']" in stdout

    def test_filestore_with_all_devices(self):
        activation = activate.Activate([])
        result = activation.validate_devices({'type': 'filestore', 'journal': {}, 'data': {}})
        assert result is True

    def test_bluestore_with_all_devices(self):
        activation = activate.Activate([])
        result = activation.validate_devices({'type': 'bluestore', 'data': {}, 'block': {}})
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
        assert "devices found: ['data']" in stdout

    def test_bluestore_missing_data(self):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError) as error:
            activation.validate_devices({'type': 'bluestore', 'block': {}})
        assert 'Unable to activate bluestore OSD due to missing devices' in str(error)

    def test_bluestore_block_device_found(self, capsys):
        activation = activate.Activate([])
        with pytest.raises(RuntimeError):
            activation.validate_devices({'block': {}})
        stdout, stderr = capsys.readouterr()
        assert "devices found: ['block']" in stdout
