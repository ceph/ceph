import pytest
from ceph_volume.devices.lvm import activate, api


class Args(object):

    def __init__(self, **kw):
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
        args = Args(osd_id=None, osd_fsid='1234')
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
