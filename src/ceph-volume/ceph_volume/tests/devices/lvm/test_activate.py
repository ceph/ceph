import pytest
from copy import deepcopy
from ceph_volume.devices.lvm import activate
from ceph_volume.api import lvm as api
from ceph_volume.tests.conftest import Capture


class Args(object):

    def __init__(self, **kw):
        # default flags
        self.bluestore = False
        self.filestore = False
        self.no_systemd = False
        self.auto_detect_objectstore = None
        for k, v in kw.items():
            setattr(self, k, v)


class TestActivate(object):

    # these tests are very functional, hence the heavy patching, it is hard to
    # test the negative side effect with an actual functional run, so we must
    # setup a perfect scenario for this test to check it can really work
    # with/without osd_id
    def test_no_osd_id_matches_fsid(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo',
                               lv_tags="ceph.osd_fsid=1234")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: volumes)
        monkeypatch.setattr(activate, 'activate_filestore', capture)
        args = Args(osd_id=None, osd_fsid='1234', filestore=True)
        activate.Activate([]).activate(args)
        assert capture.calls[0]['args'][0] == [FooVolume]

    def test_no_osd_id_matches_fsid_bluestore(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo',
                               lv_tags="ceph.osd_fsid=1234")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: volumes)
        monkeypatch.setattr(activate, 'activate_bluestore', capture)
        args = Args(osd_id=None, osd_fsid='1234', bluestore=True)
        activate.Activate([]).activate(args)
        assert capture.calls[0]['args'][0] == [FooVolume]

    def test_no_osd_id_no_matching_fsid(self, is_root, volumes, monkeypatch, capture):
        FooVolume = api.Volume(lv_name='foo', lv_path='/dev/vg/foo',
                               lv_tags="ceph.osd_fsid=1111")
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: [])
        monkeypatch.setattr(api, 'get_first_lv', lambda **kwargs: [])
        monkeypatch.setattr(activate, 'activate_filestore', capture)

        args = Args(osd_id=None, osd_fsid='2222')
        with pytest.raises(RuntimeError):
            activate.Activate([]).activate(args)

    def test_filestore_no_systemd(self, is_root, volumes, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.device_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        JournalVolume = api.Volume(
            lv_name='journal',
            lv_path='/dev/vg/journal',
            lv_uuid='000',
            lv_tags=','.join([
                "ceph.cluster_name=ceph", "ceph.journal_device=/dev/vg/journal",
                "ceph.journal_uuid=000", "ceph.type=journal",
                "ceph.osd_id=0", "ceph.osd_fsid=1234"])
        )
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_uuid='001',
            lv_tags="ceph.cluster_name=ceph,ceph.journal_device=/dev/vg/" + \
                    "journal,ceph.journal_uuid=000,ceph.type=data," + \
                    "ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        volumes.append(JournalVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True, filestore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_filestore_no_systemd_autodetect(self, is_root, volumes, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.device_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        JournalVolume = api.Volume(
            lv_name='journal',
            lv_path='/dev/vg/journal',
            lv_uuid='000',
            lv_tags=','.join([
                "ceph.cluster_name=ceph", "ceph.journal_device=/dev/vg/journal",
                "ceph.journal_uuid=000", "ceph.type=journal",
                "ceph.osd_id=0", "ceph.osd_fsid=1234"])
        )
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_uuid='001',
            lv_tags="ceph.cluster_name=ceph,ceph.journal_device=/dev/vg/" + \
                    "journal,ceph.journal_uuid=000,ceph.type=data," + \
                    "ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        volumes.append(JournalVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True,
                    filestore=True, auto_detect_objectstore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_filestore_systemd_autodetect(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.device_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        JournalVolume = api.Volume(
            lv_name='journal',
            lv_path='/dev/vg/journal',
            lv_uuid='000',
            lv_tags=','.join([
                "ceph.cluster_name=ceph", "ceph.journal_device=/dev/vg/journal",
                "ceph.journal_uuid=000", "ceph.type=journal",
                "ceph.osd_id=0","ceph.osd_fsid=1234"])
            )
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_uuid='001',
            lv_tags="ceph.cluster_name=ceph,ceph.journal_device=/dev/vg/" + \
                    "journal,ceph.journal_uuid=000,ceph.type=data," + \
                    "ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        volumes.append(JournalVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    filestore=True, auto_detect_objectstore=False)
        activate.Activate([]).activate(args)
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []

    def test_filestore_systemd(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.device_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        JournalVolume = api.Volume(
            lv_name='journal',
            lv_path='/dev/vg/journal',
            lv_uuid='000',
            lv_tags=','.join([
                "ceph.cluster_name=ceph", "ceph.journal_device=/dev/vg/journal",
                "ceph.journal_uuid=000", "ceph.type=journal",
                "ceph.osd_id=0","ceph.osd_fsid=1234"])
            )
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_uuid='001',
            lv_tags="ceph.cluster_name=ceph,ceph.journal_device=/dev/vg/" + \
                    "journal,ceph.journal_uuid=000,ceph.type=data," + \
                    "ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        volumes.append(JournalVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    filestore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []

    def test_bluestore_no_systemd(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True, bluestore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_bluestore_systemd(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    bluestore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []

    def test_bluestore_no_systemd_autodetect(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.block_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True,
                    bluestore=True, auto_detect_objectstore=True)
        activate.Activate([]).activate(args)
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_bluestore_systemd_autodetect(self, is_root, volumes, monkeypatch, capture):
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted',
                            lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw:
                            True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(activate.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(activate.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    bluestore=True, auto_detect_objectstore=False)
        activate.Activate([]).activate(args)
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []

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


class TestActivateAll(object):

    def test_does_not_detect_osds(self, capsys, is_root, capture, monkeypatch):
        monkeypatch.setattr('ceph_volume.devices.lvm.activate.direct_report', lambda: {})
        args = ['--all']
        activation = activate.Activate(args)
        activation.main()
        out, err = capsys.readouterr()
        assert 'Was unable to find any OSDs to activate' in err
        assert 'Verify OSDs are present with ' in err

    def test_detects_running_osds(self, capsys, is_root, capture, monkeypatch):
        monkeypatch.setattr('ceph_volume.devices.lvm.activate.direct_report', lambda: direct_report)
        monkeypatch.setattr('ceph_volume.devices.lvm.activate.systemctl.osd_is_active', lambda x: True)
        args = ['--all']
        activation = activate.Activate(args)
        activation.main()
        out, err = capsys.readouterr()
        assert 'a8789a96ce8b process is active. Skipping activation' in err
        assert 'b8218eaa1634 process is active. Skipping activation' in err

    def test_detects_osds_to_activate(self, is_root, capture, monkeypatch):
        monkeypatch.setattr('ceph_volume.devices.lvm.activate.direct_report', lambda: direct_report)
        monkeypatch.setattr('ceph_volume.devices.lvm.activate.systemctl.osd_is_active', lambda x: False)
        args = ['--all']
        activation = activate.Activate(args)
        activation.activate = capture
        activation.main()
        calls = sorted(capture.calls, key=lambda x: x['kwargs']['osd_id'])
        assert calls[0]['kwargs']['osd_id'] == '0'
        assert calls[0]['kwargs']['osd_fsid'] == '957d22b7-24ce-466a-9883-b8218eaa1634'
        assert calls[1]['kwargs']['osd_id'] == '1'
        assert calls[1]['kwargs']['osd_fsid'] == 'd0f3e4ad-e52a-4520-afc0-a8789a96ce8b'

#
# Activate All fixture
#

direct_report = {
    "0": [
        {
            "lv_name": "osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "lv_path": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "lv_tags": "ceph.block_device=/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634,ceph.block_uuid=6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4,ceph.cephx_lockbox_secret=,ceph.cluster_fsid=d4962338-46ff-4cd5-8ea6-c033dbdc5b44,ceph.cluster_name=ceph,ceph.crush_device_class=None,ceph.encrypted=0,ceph.osd_fsid=957d22b7-24ce-466a-9883-b8218eaa1634,ceph.osd_id=0,ceph.type=block",
            "lv_uuid": "6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4",
            "name": "osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "path": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "tags": {
                "ceph.block_device": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
                "ceph.block_uuid": "6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4",
                "ceph.cephx_lockbox_secret": "",
                "ceph.cluster_fsid": "d4962338-46ff-4cd5-8ea6-c033dbdc5b44",
                "ceph.cluster_name": "ceph",
                "ceph.crush_device_class": "None",
                "ceph.encrypted": "0",
                "ceph.osd_fsid": "957d22b7-24ce-466a-9883-b8218eaa1634",
                "ceph.osd_id": "0",
                "ceph.type": "block"
            },
            "type": "block",
            "vg_name": "ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44"
        }
    ],
    "1": [
        {
            "lv_name": "osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "lv_path": "/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "lv_tags": "ceph.block_device=/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b,ceph.block_uuid=1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW,ceph.cephx_lockbox_secret=,ceph.cluster_fsid=d4962338-46ff-4cd5-8ea6-c033dbdc5b44,ceph.cluster_name=ceph,ceph.crush_device_class=None,ceph.encrypted=0,ceph.osd_fsid=d0f3e4ad-e52a-4520-afc0-a8789a96ce8b,ceph.osd_id=1,ceph.type=block",
            "lv_uuid": "1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW",
            "name": "osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "path": "/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "tags": {
                "ceph.block_device": "/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
                "ceph.block_uuid": "1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW",
                "ceph.cephx_lockbox_secret": "",
                "ceph.cluster_fsid": "d4962338-46ff-4cd5-8ea6-c033dbdc5b44",
                "ceph.cluster_name": "ceph",
                "ceph.crush_device_class": "None",
                "ceph.encrypted": "0",
                "ceph.osd_fsid": "d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
                "ceph.osd_id": "1",
                "ceph.type": "block"
            },
            "type": "block",
            "vg_name": "ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532"
        }
    ]
}
