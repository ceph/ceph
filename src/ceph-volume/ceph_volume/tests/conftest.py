import os
import pytest
from mock.mock import patch, PropertyMock
from ceph_volume.util import disk
from ceph_volume.util.constants import ceph_disk_guids
from ceph_volume.api import lvm as lvm_api
from ceph_volume import conf, configuration


class Capture(object):

    def __init__(self, *a, **kw):
        self.a = a
        self.kw = kw
        self.calls = []
        self.return_values = kw.get('return_values', False)
        self.always_returns = kw.get('always_returns', False)

    def __call__(self, *a, **kw):
        self.calls.append({'args': a, 'kwargs': kw})
        if self.always_returns:
            return self.always_returns
        if self.return_values:
            return self.return_values.pop()


class Factory(object):

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


@pytest.fixture
def factory():
    return Factory


@pytest.fixture
def capture():
    return Capture()


@pytest.fixture
def fake_run(monkeypatch):
    fake_run = Capture()
    monkeypatch.setattr('ceph_volume.process.run', fake_run)
    return fake_run


@pytest.fixture
def fake_call(monkeypatch):
    fake_call = Capture(always_returns=([], [], 0))
    monkeypatch.setattr('ceph_volume.process.call', fake_call)
    return fake_call


@pytest.fixture
def fakedevice(factory):
    def apply(**kw):
        params = dict(
            path='/dev/sda',
            abspath='/dev/sda',
            lv_api=None,
            pvs_api=[],
            disk_api={},
            sys_api={},
            exists=True,
            is_lvm_member=True,
        )
        params.update(dict(kw))
        params['lvm_size'] = disk.Size(b=params['sys_api'].get("size", 0))
        return factory(**params)
    return apply


@pytest.fixture
def stub_call(monkeypatch):
    """
    Monkeypatches process.call, so that a caller can add behavior to the response
    """
    def apply(return_values):
        if isinstance(return_values, tuple):
            return_values = [return_values]
        stubbed_call = Capture(return_values=return_values)
        monkeypatch.setattr('ceph_volume.process.call', stubbed_call)
        return stubbed_call

    return apply


@pytest.fixture(autouse=True)
def reset_cluster_name(request, monkeypatch):
    """
    The globally available ``ceph_volume.conf.cluster`` might get mangled in
    tests, make sure that after evert test, it gets reset, preventing pollution
    going into other tests later.
    """
    def fin():
        conf.cluster = None
        try:
            os.environ.pop('CEPH_CONF')
        except KeyError:
            pass
    request.addfinalizer(fin)


@pytest.fixture
def conf_ceph(monkeypatch):
    """
    Monkeypatches ceph_volume.conf.ceph, which is meant to parse/read
    a ceph.conf. The patching is naive, it allows one to set return values for
    specific method calls.
    """
    def apply(**kw):
        stub = Factory(**kw)
        monkeypatch.setattr(conf, 'ceph', stub)
        return stub
    return apply


@pytest.fixture
def conf_ceph_stub(monkeypatch, tmpfile):
    """
    Monkeypatches ceph_volume.conf.ceph with contents from a string that are
    written to a temporary file and then is fed through the same ceph.conf
    loading mechanisms for testing.  Unlike ``conf_ceph`` which is just a fake,
    we are actually loading values as seen on a ceph.conf file

    This is useful when more complex ceph.conf's are needed. In the case of
    just trying to validate a key/value behavior ``conf_ceph`` is better
    suited.
    """
    def apply(contents):
        conf_path = tmpfile(contents=contents)
        parser = configuration.load(conf_path)
        monkeypatch.setattr(conf, 'ceph', parser)
        return parser
    return apply


@pytest.fixture
def volumes(monkeypatch):
    monkeypatch.setattr('ceph_volume.process.call', lambda x, **kw: ('', '', 0))
    volumes = lvm_api.Volumes()
    volumes._purge()
    return volumes


@pytest.fixture
def volume_groups(monkeypatch):
    monkeypatch.setattr('ceph_volume.process.call', lambda x, **kw: ('', '', 0))
    vgs = lvm_api.VolumeGroups()
    vgs._purge()
    return vgs

def volume_groups_empty(monkeypatch):
    monkeypatch.setattr('ceph_volume.process.call', lambda x, **kw: ('', '', 0))
    vgs = lvm_api.VolumeGroups(populate=False)
    return vgs

@pytest.fixture
def stub_vgs(monkeypatch, volume_groups):
    def apply(vgs):
        monkeypatch.setattr(lvm_api, 'get_api_vgs', lambda: vgs)
    return apply


# TODO: allow init-ing pvolumes to list we want
@pytest.fixture
def pvolumes(monkeypatch):
    monkeypatch.setattr('ceph_volume.process.call', lambda x, **kw: ('', '', 0))
    pvolumes = lvm_api.PVolumes()
    pvolumes._purge()
    return pvolumes

@pytest.fixture
def pvolumes_empty(monkeypatch):
    monkeypatch.setattr('ceph_volume.process.call', lambda x, **kw: ('', '', 0))
    pvolumes = lvm_api.PVolumes(populate=False)
    return pvolumes



@pytest.fixture
def is_root(monkeypatch):
    """
    Patch ``os.getuid()`` so that ceph-volume's decorators that ensure a user
    is root (or is sudoing to superuser) can continue as-is
    """
    monkeypatch.setattr('os.getuid', lambda: 0)


@pytest.fixture
def tmpfile(tmpdir):
    """
    Create a temporary file, optionally filling it with contents, returns an
    absolute path to the file when called
    """
    def generate_file(name='file', contents='', directory=None):
        directory = directory or str(tmpdir)
        path = os.path.join(directory, name)
        with open(path, 'w') as fp:
            fp.write(contents)
        return path
    return generate_file


@pytest.fixture
def disable_kernel_queries(monkeypatch):
    '''
    This speeds up calls to Device and Disk
    '''
    monkeypatch.setattr("ceph_volume.util.device.disk.get_devices", lambda: {})
    monkeypatch.setattr("ceph_volume.util.disk.udevadm_property", lambda *a, **kw: {})


@pytest.fixture
def disable_lvm_queries(monkeypatch):
    '''
    This speeds up calls to Device and Disk
    '''
    monkeypatch.setattr("ceph_volume.util.device.lvm.get_lv_from_argument", lambda path: None)
    monkeypatch.setattr("ceph_volume.util.device.lvm.get_lv", lambda vg_name, lv_uuid: None)


@pytest.fixture(params=[
    '', 'ceph data', 'ceph journal', 'ceph block',
    'ceph block.wal', 'ceph block.db', 'ceph lockbox'])
def ceph_partlabel(request):
    return request.param


@pytest.fixture(params=list(ceph_disk_guids.keys()))
def ceph_parttype(request):
    return request.param


@pytest.fixture
def lsblk_ceph_disk_member(monkeypatch, request, ceph_partlabel, ceph_parttype):
    monkeypatch.setattr("ceph_volume.util.device.disk.lsblk",
                        lambda path: {'TYPE': 'disk', 'PARTLABEL': ceph_partlabel})
    # setting blkid here too in order to be able to fall back to PARTTYPE based
    # membership
    monkeypatch.setattr("ceph_volume.util.device.disk.blkid",
                        lambda path: {'TYPE': 'disk',
                                      'PARTLABEL': '',
                                      'PARTTYPE': ceph_parttype})


@pytest.fixture
def blkid_ceph_disk_member(monkeypatch, request, ceph_partlabel, ceph_parttype):
    monkeypatch.setattr("ceph_volume.util.device.disk.blkid",
                        lambda path: {'TYPE': 'disk',
                                      'PARTLABEL': ceph_partlabel,
                                      'PARTTYPE': ceph_parttype})


@pytest.fixture(params=[
    ('gluster partition', 'gluster partition'),
    # falls back to blkid
    ('', 'gluster partition'),
    ('gluster partition', ''),
])
def device_info_not_ceph_disk_member(monkeypatch, request):
    monkeypatch.setattr("ceph_volume.util.device.disk.lsblk",
                        lambda path: {'TYPE': 'disk',
                                      'PARTLABEL': request.param[0]})
    monkeypatch.setattr("ceph_volume.util.device.disk.blkid",
                        lambda path: {'TYPE': 'disk',
                                      'PARTLABEL': request.param[1]})

@pytest.fixture
def patched_get_block_devs_lsblk():
    with patch('ceph_volume.util.disk.get_block_devs_lsblk') as p:
        yield p

@pytest.fixture
def patch_bluestore_label():
    with patch('ceph_volume.util.device.Device.has_bluestore_label',
               new_callable=PropertyMock) as p:
        p.return_value = False
        yield p

@pytest.fixture
def device_info(monkeypatch, patch_bluestore_label):
    def apply(devices=None, lsblk=None, lv=None, blkid=None, udevadm=None,
              has_bluestore_label=False):
        devices = devices if devices else {}
        lsblk = lsblk if lsblk else {}
        blkid = blkid if blkid else {}
        udevadm = udevadm if udevadm else {}
        lv = Factory(**lv) if lv else None
        monkeypatch.setattr("ceph_volume.sys_info.devices", {})
        monkeypatch.setattr("ceph_volume.util.device.disk.get_devices", lambda: devices)
        if not devices:
            monkeypatch.setattr("ceph_volume.util.device.lvm.get_first_lv", lambda filters: lv)
        else:
            monkeypatch.setattr("ceph_volume.util.device.lvm.get_lv_from_argument", lambda path: None)
            monkeypatch.setattr("ceph_volume.util.device.lvm.get_device_lvs",
                                lambda path: [lv])
        monkeypatch.setattr("ceph_volume.util.device.lvm.get_lv", lambda vg_name, lv_uuid: lv)
        monkeypatch.setattr("ceph_volume.util.device.disk.lsblk", lambda path: lsblk)
        monkeypatch.setattr("ceph_volume.util.device.disk.blkid", lambda path: blkid)
        monkeypatch.setattr("ceph_volume.util.disk.udevadm_property", lambda *a, **kw: udevadm)
    return apply
