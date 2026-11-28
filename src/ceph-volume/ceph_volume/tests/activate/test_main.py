from ceph_volume.activate.main import Activate
from ceph_volume.api.lvm import Volume
from ceph_volume.util.lvm_osd_mappers import OsdLvmMappers


def make_lv(name, kind):
    return Volume(
        lv_name=name,
        lv_path='/dev/vg/%s' % name,
        lv_tags='ceph.osd_id=1,ceph.osd_fsid=fsid,ceph.type=%s,ceph.cluster_name=ceph' % kind,
    )


class ActivationCalls(object):

    def __init__(self):
        self.raw = 0
        self.lvm = []


def patch_activation(monkeypatch, lvs):
    calls = ActivationCalls()

    def mappers_factory(osd_id, osd_fsid):
        mappers = OsdLvmMappers(osd_id, osd_fsid, lvs=lvs)
        mappers.close = lambda: None
        return mappers

    class RawActivate(object):
        def __init__(self, args):
            self.args = args

        def activate(self):
            calls.raw += 1

    class LvmActivate(object):
        def __init__(self, args):
            self.args = args

        def activate(self, args, osd_id, osd_fsid):
            calls.lvm.append((osd_id, osd_fsid))

    monkeypatch.setattr(
        'ceph_volume.util.osd_luks_credentials.OsdLuksCredentials.apply_cluster_context',
        lambda self, cluster_name: None,
    )
    monkeypatch.setattr('ceph_volume.activate.main.OsdLvmMappers', mappers_factory)
    monkeypatch.setattr('ceph_volume.activate.main.RAWActivate', RawActivate)
    monkeypatch.setattr('ceph_volume.activate.main.LVMActivate', LvmActivate)
    return calls


def test_block_lv_activates_lvm_without_raw_scan(monkeypatch):
    calls = patch_activation(monkeypatch, [make_lv('block', 'block')])

    Activate(['--osd-id', '1', '--osd-uuid', 'fsid', '--no-systemd']).main()

    assert calls.raw == 0
    assert calls.lvm == [('1', 'fsid')]


def test_db_or_wal_lv_without_block_keeps_raw_scan(monkeypatch):
    calls = patch_activation(
        monkeypatch,
        [make_lv('db', 'db'), make_lv('wal', 'wal')],
    )

    Activate(['--osd-id', '1', '--osd-uuid', 'fsid', '--no-systemd']).main()

    assert calls.raw == 1
    assert calls.lvm == []
