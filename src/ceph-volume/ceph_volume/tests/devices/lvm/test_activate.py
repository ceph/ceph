import pytest
from ceph_volume.devices.lvm import activate
from ceph_volume.api import lvm as api


class Args(object):

    def __init__(self, **kw):
        # default flags
        self.bluestore = False
        self.filestore = False
        self.auto_detect_objectstore = None
        for k, v in kw.items():
            setattr(self, k, v)


class TestActivate(object):

    # these tests are very functional, hence the heavy patching, it is hard to
    # test the negative side effect with an actual functional run, so we must
    # setup a perfect scenario for this test to check it can really work
    # with/without osd_id
    def test_no_osd_id_matches_fsid(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo', lv_tags="ceph.osd_fsid=1234")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        monkeypatch.setattr(activate, 'activate_filestore', capture)
        args = Args(osd_id=None, osd_fsid='1234', filestore=True)
        activate.Activate([]).activate(args)
        assert capture.calls[0]['args'][0] == [FooVolume]

    def test_no_osd_id_matches_fsid_bluestore(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo', lv_tags="ceph.osd_fsid=1234")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        monkeypatch.setattr(activate, 'activate_bluestore', capture)
        args = Args(osd_id=None, osd_fsid='1234', bluestore=True)
        activate.Activate([]).activate(args)
        assert capture.calls[0]['args'][0] == [FooVolume]

    def test_no_osd_id_no_matching_fsid(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo', lv_tags="ceph.osd_fsid=11234")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'Volumes', lambda: volumes)
        monkeypatch.setattr(activate, 'activate_filestore', capture)
        args = Args(osd_id=None, osd_fsid='1234')
        with pytest.raises(RuntimeError):
            activate.Activate([]).activate(args)


class TestActivateFlags(object):

    def test_default_objectstore(self, capture):
        args = ['0', 'asdf-ljh-asdf']
        activation = activate.Activate(args)
        activation.activate = capture
        activation.main()
        parsed_args = capture.calls[0]['args'][0]
        assert parsed_args.filestore is False
        assert parsed_args.bluestore is True

    def test_uses_filestore(self, capture):
        args = ['--filestore', '0', 'asdf-ljh-asdf']
        activation = activate.Activate(args)
        activation.activate = capture
        activation.main()
        parsed_args = capture.calls[0]['args'][0]
        assert parsed_args.filestore is True
        assert parsed_args.bluestore is False

    def test_uses_bluestore(self, capture):
        args = ['--bluestore', '0', 'asdf-ljh-asdf']
        activation = activate.Activate(args)
        activation.activate = capture
        activation.main()
        parsed_args = capture.calls[0]['args'][0]
        assert parsed_args.filestore is False
        assert parsed_args.bluestore is True
