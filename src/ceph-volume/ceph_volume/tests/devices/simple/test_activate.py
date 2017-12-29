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
