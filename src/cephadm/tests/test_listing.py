import pathlib
import json

import pytest

from .fixtures import funkypatch, with_cephadm_ctx, import_cephadm, cephadm_fs



def test_list_daemons_no_detail_nothing(funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call
    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        dl = _cephadm.list_daemons(ctx, detail=False)
    assert dl == []


class _EntryHelper:
    def __init__(self, dl):
        self.dl = dl
        self.found = set()

    def get(self, name):
        matches = [v for v in self.dl if v['name'] == name]
        assert len(matches) != 0, "no matching entries"
        assert len(matches) < 2, "too many matches"
        m = matches[0]
        self.found.add(name)
        return m

    def assert_checked_all(self):
        assert len(self.dl) == len(self.found), "did not check all entries"


def test_list_daemons_no_detail_minimal(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False)
    assert len(dl) == 2
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    edl.assert_checked_all()


def test_list_daemons_no_detail_invalid_entry(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    invalid_dir = cluster_dir / 'foo-bar-baz'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)
    invalid_dir.mkdir(parents=True)

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False)
    assert len(dl) == 2
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    edl.assert_checked_all()


def test_list_daemons_no_detail_match_type(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False, type_of_daemon='mgr')
    assert len(dl) == 1
    edl = _EntryHelper(dl)
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    edl.assert_checked_all()


def test_list_daemons_no_detail_match_name(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False, daemon_name='mon.ceph0')
    assert len(dl) == 1
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    edl.assert_checked_all()


def test_list_daemons_detail_minimal(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    # container command fakery
    img = 'quay.io/fake/ceph:ci'
    img_id = 'fd6b0fb89677f907edf0f5dbec41b2d09850d58ff860a8a0671ad24fafa1e889'
    img_sha = 'sha256:c217e3d06df0334fba3f33242e76548a4f71cec619dfa29f64dec9321bd518f3'
    ctr1 = 'cd9ceec3fc3aa59901e3ced4f4eab8557d067cf05cbc24fa2521962d4bef3b92'
    ctr2 = '7d067cf05cbc24fa2521962d4bef3b92cd9ceec3fc3aa59901e3ced4f4eab855'
    date = '2025-01-31 08:13:30.148338962 -0500 EST'
    vers = ''

    def _fake_call(ctx, cmd, *args, **kwargs):
        out = ''
        if 'stats' in cmd and any('MemUsage' in a for a in cmd):
            out = '\n'.join(['bob,500 / 1000', 'kit,100 / 1000'])
        elif 'inspect' in cmd and any('RepoDigests' in a for a in cmd):
            out = f'[{img}@{img_sha}]'
        elif 'is-active' in cmd:
            out = 'active'
        elif 'inspect' in cmd and 'mon' in cmd[-1]:
            out = f'{ctr1},{img},{img_id},{date},{vers}'
        elif 'inspect' in cmd and 'mgr' in cmd[-1]:
            out = f'{ctr2},{img},{img_id},{date},{vers}'
        return out, '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/tmp/_lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)

    # add meta file for the mon service
    with (mon_dir / 'unit.meta').open('w') as fh:
        json.dump({'meta_foo': 'yes', 'meta_bar': 'no'}, fh)

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx)
    assert len(dl) == 2
    edl = _EntryHelper(dl)
    # mon check
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    assert mon_entry['enabled'] == True
    assert mon_entry['state'] == 'running'
    assert mon_entry['container_id'] == ctr1
    assert mon_entry['container_image_name'] == img
    assert mon_entry['container_image_id'] == img_id
    assert mon_entry['container_image_digests'] == [f'{img}@{img_sha}']
    # some of the meta values for the mon
    assert mon_entry['meta_foo'] == 'yes'
    assert mon_entry['meta_bar'] == 'no'
    # mgr check
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    assert mgr_entry['enabled'] == True
    assert mgr_entry['state'] == 'running'
    assert mgr_entry['container_id'] == ctr2
    assert mgr_entry['container_image_name'] == img
    assert mgr_entry['container_image_id'] == img_id
    assert mgr_entry['container_image_digests'] == [f'{img}@{img_sha}']
    edl.assert_checked_all()


def test_list_daemons_detail_mgrnotrunning(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    # container command fakery
    img = 'quay.io/fake/ceph:ci'
    img_id = 'fd6b0fb89677f907edf0f5dbec41b2d09850d58ff860a8a0671ad24fafa1e889'
    img_sha = 'sha256:c217e3d06df0334fba3f33242e76548a4f71cec619dfa29f64dec9321bd518f3'
    ctr1 = 'cd9ceec3fc3aa59901e3ced4f4eab8557d067cf05cbc24fa2521962d4bef3b92'
    ctr2 = '7d067cf05cbc24fa2521962d4bef3b92cd9ceec3fc3aa59901e3ced4f4eab855'
    date = '2025-01-31 08:13:30.148338962 -0500 EST'
    vers = ''

    def _fake_call(ctx, cmd, *args, **kwargs):
        out = ''
        code = 0
        if 'stats' in cmd and any('MemUsage' in a for a in cmd):
            out = '\n'.join(['bob,500 / 1000', 'kit,100 / 1000'])
        elif 'inspect' in cmd and any('RepoDigests' in a for a in cmd):
            out = f'[{img}@{img_sha}]'
        elif 'is-active' in cmd and 'mgr' in cmd[-1]:
            out = 'inactive'
        elif 'is-active' in cmd:
            out = 'active'
        elif 'inspect' in cmd and 'mon' in cmd[-1]:
            out = f'{ctr1},{img},{img_id},{date},{vers}'
        elif 'inspect' in cmd and 'mgr' in cmd[-1]:
            code = 2
        return out, '', code

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/tmp/_lib/fake/ceph')
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)

    # add meta file for the mon service
    with (mon_dir / 'unit.meta').open('w') as fh:
        json.dump({'meta_foo': 'yes', 'meta_bar': 'no'}, fh)
    # since the container is not running it will fall back to
    # reading a unit.image file. create that
    with (mgr_dir / 'unit.image').open('w') as fh:
        fh.write(f'{img}\n')

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx)
    assert len(dl) == 2
    edl = _EntryHelper(dl)
    # mon check
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    assert mon_entry['enabled'] == True
    assert mon_entry['state'] == 'running'
    assert mon_entry['container_id'] == ctr1
    assert mon_entry['container_image_name'] == img
    assert mon_entry['container_image_id'] == img_id
    assert mon_entry['container_image_digests'] == [f'{img}@{img_sha}']
    # some of the meta values for the mon
    assert mon_entry['meta_foo'] == 'yes'
    assert mon_entry['meta_bar'] == 'no'
    # mgr check
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    assert mgr_entry['enabled'] == True
    assert mgr_entry['state'] == 'stopped'
    assert mgr_entry['container_id'] is None
    assert mgr_entry['container_image_name'] == img
    assert mgr_entry['container_image_id'] is None
    assert mgr_entry['container_image_digests'] is None
    edl.assert_checked_all()


def test_list_daemons_no_detail_legacy(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    # cephadm style
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)
    # legacy style
    lmon_dir = fake_ceph / 'mon' / f'ycagel-fred'
    losd_dir = fake_ceph / 'osd' / f'ycagel-wilma'
    lmon_dir.mkdir(parents=True)
    losd_dir.mkdir(parents=True)
    legacy_etc_conf = pathlib.Path('/etc/ceph/ycagel.conf')
    legacy_etc_conf.parent.mkdir(parents=True)
    legacy_etc_conf.write_text(f"""
[global]
fsid = {fsid}
""")

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False)
    assert len(dl) == 4
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    # legacy
    lmon_entry = edl.get('mon.fred')
    assert lmon_entry['fsid'] == fsid
    assert lmon_entry['systemd_unit'] == f'ceph-mon@fred'
    losd_entry = edl.get('osd.wilma')
    assert losd_entry['fsid'] == fsid
    assert losd_entry['systemd_unit'] == f'ceph-osd@wilma'
    edl.assert_checked_all()


def test_list_daemons_no_detail_legacy_invalid_entry(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    # cephadm style
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)
    # legacy style
    lmon_dir = fake_ceph / 'mon' / f'ycagel-fred'
    losd_dir = fake_ceph / 'osd' / f'ycagel-wilma'
    lmon_dir.mkdir(parents=True)
    losd_dir.mkdir(parents=True)
    invalid_dir = fake_ceph / 'osd' / f'bambam'
    invalid_dir.mkdir(parents=True)
    legacy_etc_conf = pathlib.Path('/etc/ceph/ycagel.conf')
    legacy_etc_conf.parent.mkdir(parents=True)
    legacy_etc_conf.write_text(f"""
[global]
fsid = {fsid}
""")

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False)
    assert len(dl) == 4
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    # legacy
    lmon_entry = edl.get('mon.fred')
    assert lmon_entry['fsid'] == fsid
    assert lmon_entry['systemd_unit'] == f'ceph-mon@fred'
    losd_entry = edl.get('osd.wilma')
    assert losd_entry['fsid'] == fsid
    assert losd_entry['systemd_unit'] == f'ceph-osd@wilma'
    edl.assert_checked_all()


def test_list_daemons_no_detail_legacy_match_type(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    def _fake_call(ctx, cmd, *args, **kwargs):
        return '', '', 0

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    # cephadm style
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)
    # legacy style
    lmon_dir = fake_ceph / 'mon' / f'ycagel-fred'
    losd_dir = fake_ceph / 'osd' / f'ycagel-wilma'
    lmon_dir.mkdir(parents=True)
    losd_dir.mkdir(parents=True)
    legacy_etc_conf = pathlib.Path('/etc/ceph/ycagel.conf')
    legacy_etc_conf.parent.mkdir(parents=True)
    legacy_etc_conf.write_text(f"""
[global]
fsid = {fsid}
""")

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx, detail=False, type_of_daemon='mon')
    assert len(dl) == 2
    edl = _EntryHelper(dl)
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    # legacy
    lmon_entry = edl.get('mon.fred')
    assert lmon_entry['fsid'] == fsid
    assert lmon_entry['systemd_unit'] == f'ceph-mon@fred'
    edl.assert_checked_all()


def test_list_daemons_detail_legacy(cephadm_fs, funkypatch):
    _cephadm = import_cephadm()
    _call = funkypatch.patch('cephadmlib.call_wrappers.call')

    # container command fakery
    img = 'quay.io/fake/ceph:ci'
    img_id = 'fd6b0fb89677f907edf0f5dbec41b2d09850d58ff860a8a0671ad24fafa1e889'
    img_sha = 'sha256:c217e3d06df0334fba3f33242e76548a4f71cec619dfa29f64dec9321bd518f3'
    ctr1 = 'cd9ceec3fc3aa59901e3ced4f4eab8557d067cf05cbc24fa2521962d4bef3b92'
    ctr2 = '7d067cf05cbc24fa2521962d4bef3b92cd9ceec3fc3aa59901e3ced4f4eab855'
    date = '2025-01-31 08:13:30.148338962 -0500 EST'
    vers = ''

    def _fake_call(ctx, cmd, *args, **kwargs):
        out = ''
        code = 0
        if 'stats' in cmd and any('MemUsage' in a for a in cmd):
            out = '\n'.join(['bob,500 / 1000', 'kit,100 / 1000'])
        elif 'inspect' in cmd and any('RepoDigests' in a for a in cmd):
            out = f'[{img}@{img_sha}]'
        elif 'is-active' in cmd:
            out = 'active'
        elif 'inspect' in cmd and 'mon' in cmd[-1]:
            out = f'{ctr1},{img},{img_id},{date},{vers}'
        elif 'inspect' in cmd and 'mgr' in cmd[-1]:
            out = f'{ctr2},{img},{img_id},{date},{vers}'
        elif 'ceph' in cmd and '-v' in cmd:
            out = 'ceph version v1.2.3 phony-version'
        return out, '', code

    _call.side_effect = _fake_call

    fsid = 'dc93cfee-ddc5-11ef-a056-525400220000'
    fake_ceph = pathlib.Path('/var/lib/fake/ceph')
    # cephadm style
    cluster_dir = fake_ceph / fsid
    mon_dir = cluster_dir / 'mon.ceph0'
    mgr_dir = cluster_dir / 'mgr.ceph0.zzzabc'
    mon_dir.mkdir(parents=True)
    mgr_dir.mkdir(parents=True)
    # legacy style
    lmon_dir = fake_ceph / 'mon' / f'ycagel-fred'
    losd_dir = fake_ceph / 'osd' / f'ycagel-wilma'
    lmon_dir.mkdir(parents=True)
    losd_dir.mkdir(parents=True)
    legacy_etc_conf = pathlib.Path('/etc/ceph/ycagel.conf')
    legacy_etc_conf.parent.mkdir(parents=True)
    legacy_etc_conf.write_text(f"""
[global]
fsid = {fsid}
""")

    with with_cephadm_ctx([], mock_cephadm_call_fn=False) as ctx:
        ctx.data_dir = str(fake_ceph)
        dl = _cephadm.list_daemons(ctx)
    assert len(dl) == 4
    edl = _EntryHelper(dl)
    # basic check for non-legacy mon
    mon_entry = edl.get('mon.ceph0')
    assert mon_entry['fsid'] == fsid
    assert mon_entry['systemd_unit'] == f'ceph-{fsid}@mon.ceph0'
    assert mon_entry['container_id'] == ctr1
    # basic check for non-legacy mgr
    mgr_entry = edl.get('mgr.ceph0.zzzabc')
    assert mgr_entry['fsid'] == fsid
    assert mgr_entry['systemd_unit'] == f'ceph-{fsid}@mgr.ceph0.zzzabc'
    assert mgr_entry['container_id'] == ctr2
    # legacy mon
    lmon_entry = edl.get('mon.fred')
    assert lmon_entry['fsid'] == fsid
    assert lmon_entry['systemd_unit'] == f'ceph-mon@fred'
    assert lmon_entry['enabled'] == True
    assert lmon_entry['state'] == 'running'
    assert lmon_entry['host_version'] == 'v1.2.3'
    # legacy osd
    losd_entry = edl.get('osd.wilma')
    assert losd_entry['fsid'] == fsid
    assert losd_entry['systemd_unit'] == f'ceph-osd@wilma'
    assert losd_entry['enabled'] == True
    assert losd_entry['state'] == 'running'
    assert losd_entry['host_version'] == 'v1.2.3'
    edl.assert_checked_all()
