import os
import pytest
from mock.mock import patch, PropertyMock, create_autospec
from ceph_volume.api import lvm
from ceph_volume.util import disk
from ceph_volume.util import device
from ceph_volume.util.constants import ceph_disk_guids
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
def mock_lv_device_generator():
    def mock_lv():
        size = 21474836480
        dev = create_autospec(device.Device)
        dev.lv_name = 'lv'
        dev.vg_name = 'vg'
        dev.path = '{}/{}'.format(dev.vg_name, dev.lv_name)
        dev.used_by_ceph = False
        dev.vg_size = [size]
        dev.vg_free = dev.vg_size
        dev.available_lvm = True
        dev.is_device = False
        dev.lvs = [lvm.Volume(vg_name=dev.vg_name, lv_name=dev.lv_name, lv_size=size, lv_tags='')]
        return dev
    return mock_lv

def mock_device():
    dev = create_autospec(device.Device)
    dev.path = '/dev/foo'
    dev.vg_name = 'vg_foo'
    dev.lv_name = 'lv_foo'
    dev.symlink = None
    dev.vgs = [lvm.VolumeGroup(vg_name=dev.vg_name, lv_name=dev.lv_name)]
    dev.available_lvm = True
    dev.vg_size = [21474836480]
    dev.vg_free = dev.vg_size
    dev.lvs = []
    return dev

@pytest.fixture(params=range(1,4))
def mock_devices_available(request):
    ret = []
    for n in range(request.param):
        dev = mock_device()
        # after v15.2.8, a single VG is created for each PV
        dev.vg_name = f'vg_foo_{n}'
        dev.vgs = [lvm.VolumeGroup(vg_name=dev.vg_name, lv_name=dev.lv_name)]
        ret.append(dev)
    return ret

@pytest.fixture
def mock_device_generator():
    return mock_device


@pytest.fixture(params=range(1,11))
def osds_per_device(request):
    return request.param


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
    monkeypatch.setattr("ceph_volume.util.device.disk.get_devices", lambda device='': {})
    monkeypatch.setattr("ceph_volume.util.disk.udevadm_property", lambda *a, **kw: {})


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
                        lambda path: {'TYPE': 'disk',
                                      'NAME': 'sda',
                                      'PARTLABEL': ceph_partlabel,
                                      'PARTTYPE': ceph_parttype})
    monkeypatch.setattr("ceph_volume.util.device.disk.lsblk_all",
                        lambda: [{'TYPE': 'disk',
                                  'NAME': 'sda',
                                  'PARTLABEL': ceph_partlabel,
                                  'PARTTYPE': ceph_parttype}])

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
                                      'NAME': 'sda',
                                      'PARTLABEL': request.param[0]})
    monkeypatch.setattr("ceph_volume.util.device.disk.lsblk_all",
                        lambda: [{'TYPE': 'disk',
                                  'NAME': 'sda',
                                  'PARTLABEL': request.param[0]}])
    monkeypatch.setattr("ceph_volume.util.device.disk.blkid",
                        lambda path: {'TYPE': 'disk',
                                      'PARTLABEL': request.param[1]})

@pytest.fixture
def patched_get_block_devs_sysfs():
    with patch('ceph_volume.util.disk.get_block_devs_sysfs') as p:
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
        if devices:
            for dev in devices.keys():
                devices[dev]['device_nodes'] = os.path.basename(dev)
        else:
            devices = {}
        lsblk = lsblk if lsblk else {}
        blkid = blkid if blkid else {}
        udevadm = udevadm if udevadm else {}
        lv = Factory(**lv) if lv else None
        monkeypatch.setattr("ceph_volume.sys_info.devices", {})
        monkeypatch.setattr("ceph_volume.util.device.disk.get_devices", lambda device='': devices)
        if not devices:
            monkeypatch.setattr("ceph_volume.util.device.lvm.get_single_lv", lambda filters: lv)
        else:
            monkeypatch.setattr("ceph_volume.util.device.lvm.get_device_lvs",
                                lambda path: [lv])
        monkeypatch.setattr("ceph_volume.util.device.disk.lsblk", lambda path: lsblk)
        monkeypatch.setattr("ceph_volume.util.device.disk.blkid", lambda path: blkid)
        monkeypatch.setattr("ceph_volume.util.disk.udevadm_property", lambda *a, **kw: udevadm)
    return apply

@pytest.fixture(params=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.999, 1.0])
def data_allocate_fraction(request):
    return request.param

@pytest.fixture
def fake_filesystem(fs):

    fs.create_dir('/sys/block/sda/slaves')
    fs.create_dir('/sys/block/sda/queue')
    fs.create_dir('/sys/block/rbd0')
    yield fs
