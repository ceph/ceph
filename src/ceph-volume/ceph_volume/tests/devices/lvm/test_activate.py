import pytest
from copy import deepcopy
from ceph_volume.devices.lvm import activate
from ceph_volume.api import lvm as api
from ceph_volume.tests.conftest import Capture
from ceph_volume import objectstore
#from ceph_volume.util.prepare import create_key
from mock import patch, call
from argparse import Namespace

class Args(object):

    def __init__(self, **kw):
        # default flags
        self.bluestore = False
        self.no_systemd = False
        self.auto_detect_objectstore = None
        for k, v in kw.items():
            setattr(self, k, v)


@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestActivate(object):

    # these tests are very functional, hence the heavy patching, it is hard to
    # test the negative side effect with an actual functional run, so we must
    # setup a perfect scenario for this test to check it can really work
    # with/without osd_id
    def test_no_osd_id_matches_fsid_bluestore(self,
                                              m_create_key,
                                              is_root,
                                              monkeypatch,
                                              capture):
        FooVolume = api.Volume(lv_name='foo',
                            lv_path='/dev/vg/foo',
                            lv_tags="ceph.osd_fsid=1234")
        volumes = []
        volumes.append(FooVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: volumes)
        monkeypatch.setattr(objectstore.lvmbluestore.LvmBlueStore,
                            '_activate',
                            capture)

        args = Args(osd_id=None, osd_fsid='1234', bluestore=True)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        a.objectstore.activate()
        assert capture.calls[0]['args'][0] == [FooVolume]

    def test_osd_id_no_osd_fsid(self, m_create_key, is_root):
        args = Args(osd_id=42, osd_fsid=None)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        with pytest.raises(RuntimeError) as result:
            a.objectstore.activate()
        assert result.value.args[0] == 'could not activate osd.42, please provide the osd_fsid too'

    def test_no_osd_id_no_osd_fsid(self, m_create_key, is_root):
        args = Args(osd_id=None, osd_fsid=None)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        with pytest.raises(RuntimeError) as result:
            a.objectstore.activate()
        assert result.value.args[0] == 'Please provide both osd_id and osd_fsid'

    def test_bluestore_no_systemd(self, m_create_key, is_root, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes = []
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True, bluestore=True)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        a.objectstore.activate()
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_bluestore_systemd(self, m_create_key, is_root, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes = []
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    bluestore=True)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        a.objectstore.activate()
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []

    def test_bluestore_no_systemd_autodetect(self, m_create_key, is_root, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.block_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes = []
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=True,
                    bluestore=True, auto_detect_objectstore=True)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        a.objectstore.activate()
        assert fake_enable.calls == []
        assert fake_start_osd.calls == []

    def test_bluestore_systemd_autodetect(self, m_create_key, is_root, monkeypatch, capture):
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        fake_enable = Capture()
        fake_start_osd = Capture()
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted',
                            lambda *a, **kw: True)
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda *a, **kw:
                            True)
        monkeypatch.setattr('ceph_volume.process.run', lambda *a, **kw: True)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'enable_volume', fake_enable)
        monkeypatch.setattr(objectstore.lvmbluestore.systemctl, 'start_osd', fake_start_osd)
        DataVolume = api.Volume(
            lv_name='data',
            lv_path='/dev/vg/data',
            lv_tags="ceph.cluster_name=ceph,,ceph.journal_uuid=000," + \
                    "ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=1234")
        volumes = []
        volumes.append(DataVolume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs: deepcopy(volumes))

        args = Args(osd_id=None, osd_fsid='1234', no_systemd=False,
                    bluestore=True, auto_detect_objectstore=False)
        a = activate.Activate([])
        a.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
        a.objectstore.activate()
        assert fake_enable.calls != []
        assert fake_start_osd.calls != []


@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
@patch('ceph_volume.objectstore.lvmbluestore.LvmBlueStore.activate_all')
@patch('ceph_volume.objectstore.lvmbluestore.LvmBlueStore.activate')
class TestActivateFlags(object):

    def test_default_objectstore(self, m_activate, m_activate_all, m_create_key, capture):
        args = ['0', 'asdf-ljh-asdf']

        a = activate.Activate(args)
        a.main()
        assert a.args.objectstore == 'bluestore'

    def test_bluestore_backward_compatibility(self, m_activate, m_activate_all, m_create_key, capture):
        args = ['--bluestore', '0', 'asdf-ljh-asdf']
        a = activate.Activate(args)
        a.main()
        assert a.args.objectstore == 'bluestore'


@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestActivateAll(object):

    def test_does_not_detect_osds(self, m_create_key, capsys, is_root, monkeypatch):
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.direct_report', lambda: {})
        args = ['--all']
        activation = activate.Activate(args)
        activation.main()
        out, err = capsys.readouterr()
        assert 'Was unable to find any OSDs to activate' in err
        assert 'Verify OSDs are present with ' in err

    def test_detects_running_osds(self, m_create_key, capsys, is_root, capture, monkeypatch):
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.direct_report', lambda: direct_report)
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.systemctl.osd_is_active', lambda x: True)
        args = ['--all']
        activation = activate.Activate(args)
        activation.main()
        out, err = capsys.readouterr()
        assert 'a8789a96ce8b process is active. Skipping activation' in err
        assert 'b8218eaa1634 process is active. Skipping activation' in err

    @patch('ceph_volume.objectstore.lvmbluestore.LvmBlueStore.activate')
    def test_detects_osds_to_activate_systemd(self, m_activate, m_create_key, is_root, monkeypatch):
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.direct_report', lambda: direct_report)
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.systemctl.osd_is_active', lambda x: False)
        args = ['--all', '--bluestore']
        a = activate.Activate(args)
        a.main()
        calls = [
            call(Namespace(activate_all=True,
                           auto_detect_objectstore=False,
                           bluestore=True,
                           no_systemd=False,
                           no_tmpfs=False,
                           objectstore='bluestore',
                           osd_fsid=None,
                           osd_id=None),
                           osd_id='0',
                           osd_fsid='957d22b7-24ce-466a-9883-b8218eaa1634'),
            call(Namespace(activate_all=True,
                           auto_detect_objectstore=False,
                           bluestore=True,
                           no_systemd=False,
                           no_tmpfs=False,
                           objectstore='bluestore',
                           osd_fsid=None,
                           osd_id=None),
                           osd_id='1',
                           osd_fsid='d0f3e4ad-e52a-4520-afc0-a8789a96ce8b')
        ]
        m_activate.assert_has_calls(calls)

    @patch('ceph_volume.objectstore.lvmbluestore.LvmBlueStore.activate')
    def test_detects_osds_to_activate_no_systemd(self, m_activate, m_create_key, is_root, monkeypatch):
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.direct_report', lambda: direct_report)
        args = ['--all', '--no-systemd', '--bluestore']
        a = activate.Activate(args)
        a.main()
        calls = [
            call(Namespace(activate_all=True,
                           auto_detect_objectstore=False,
                           bluestore=True,
                           no_systemd=True,
                           no_tmpfs=False,
                           objectstore='bluestore',
                           osd_fsid=None,
                           osd_id=None),
                           osd_id='0',
                           osd_fsid='957d22b7-24ce-466a-9883-b8218eaa1634'),
            call(Namespace(activate_all=True,
                           auto_detect_objectstore=False,
                           bluestore=True,
                           no_systemd=True,
                           no_tmpfs=False,
                           objectstore='bluestore',
                           osd_fsid=None,
                           osd_id=None),
                           osd_id='1',
                           osd_fsid='d0f3e4ad-e52a-4520-afc0-a8789a96ce8b')
        ]
        m_activate.assert_has_calls(calls)

#
# Activate All fixture
#

direct_report = {
    "0": [
        {
            "lv_name": "osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "lv_path": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "lv_tags": "ceph.block_device=/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634,ceph.block_uuid=6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4,ceph.cephx_lockbox_secret=,ceph.cluster_fsid=d4962338-46ff-4cd5-8ea6-c033dbdc5b44,ceph.cluster_name=ceph,ceph.crush_device_class=,ceph.encrypted=0,ceph.osd_fsid=957d22b7-24ce-466a-9883-b8218eaa1634,ceph.osd_id=0,ceph.type=block",
            "lv_uuid": "6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4",
            "name": "osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "path": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
            "tags": {
                "ceph.block_device": "/dev/ceph-d4962338-46ff-4cd5-8ea6-c033dbdc5b44/osd-block-957d22b7-24ce-466a-9883-b8218eaa1634",
                "ceph.block_uuid": "6MixOd-2Q1I-f8K3-PPOq-UJGV-L3A0-0XwUm4",
                "ceph.cephx_lockbox_secret": "",
                "ceph.cluster_fsid": "d4962338-46ff-4cd5-8ea6-c033dbdc5b44",
                "ceph.cluster_name": "ceph",
                "ceph.crush_device_class": "",
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
            "lv_tags": "ceph.block_device=/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b,ceph.block_uuid=1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW,ceph.cephx_lockbox_secret=,ceph.cluster_fsid=d4962338-46ff-4cd5-8ea6-c033dbdc5b44,ceph.cluster_name=ceph,ceph.crush_device_class=,ceph.encrypted=0,ceph.osd_fsid=d0f3e4ad-e52a-4520-afc0-a8789a96ce8b,ceph.osd_id=1,ceph.type=block",
            "lv_uuid": "1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW",
            "name": "osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "path": "/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
            "tags": {
                "ceph.block_device": "/dev/ceph-7538bcf0-f155-4d3f-a9fd-d8b15905e532/osd-block-d0f3e4ad-e52a-4520-afc0-a8789a96ce8b",
                "ceph.block_uuid": "1igwLb-ZlmV-eLgp-hapx-c1Hr-M5gz-sHjnyW",
                "ceph.cephx_lockbox_secret": "",
                "ceph.cluster_fsid": "d4962338-46ff-4cd5-8ea6-c033dbdc5b44",
                "ceph.cluster_name": "ceph",
                "ceph.crush_device_class": "",
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
