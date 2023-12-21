import mock
import os
import pytest
import time

from contextlib import contextmanager
from pyfakefs import fake_filesystem

from typing import Dict, List, Optional, Any


def import_cephadm():
    """Import cephadm as a module."""
    import cephadm as _cephadm

    return _cephadm


def mock_docker():
    from cephadmlib.container_engines import Docker

    docker = mock.Mock(Docker)
    docker.path = '/usr/bin/docker'
    type(docker).unlimited_pids_option = Docker.unlimited_pids_option
    return docker


def mock_podman():
    from cephadmlib.container_engines import Podman

    podman = mock.Mock(Podman)
    podman.path = '/usr/bin/podman'
    podman.version = (2, 1, 0)
    # This next little bit of black magic was adapated from the mock docs for
    # PropertyMock. We don't use a PropertyMock but the suggestion to call
    # type(...) from the doc allows us to "borrow" the real
    # supports_split_cgroups attribute:
    # https://docs.python.org/3/library/unittest.mock.html#unittest.mock.Mock
    type(podman).supports_split_cgroups = Podman.supports_split_cgroups
    type(podman).service_args = Podman.service_args
    type(podman).unlimited_pids_option = Podman.unlimited_pids_option
    return podman


def _daemon_path():
    return os.getcwd()


def mock_bad_firewalld():
    def raise_bad_firewalld():
        raise Exception('Called bad firewalld')

    _cephadm = import_cephadm()
    f = mock.Mock(_cephadm.Firewalld)
    f.enable_service_for = lambda _: raise_bad_firewalld()
    f.apply_rules = lambda: raise_bad_firewalld()
    f.open_ports = lambda _: raise_bad_firewalld()


def _mock_scrape_host(obj, interval):
    try:
        raise ValueError("wah")
    except Exception as e:
        obj._handle_thread_exception(e, 'host')


def _mock_run(obj):
    t = obj._create_thread(obj._scrape_host_facts, 'host', 5)
    time.sleep(1)
    if not t.is_alive():
        obj.cephadm_cache.update_health('host', "inactive", "host thread stopped")


@pytest.fixture()
def cephadm_fs(
    fs: fake_filesystem.FakeFilesystem,
):
    """
    use pyfakefs to stub filesystem calls
    """
    from cephadmlib import constants

    # the following is a workaround for the fakefs interfering with jinja2's
    # package loader when run in the pytest suite when this fixture is used.
    # it effectively maps what is `src/cephadm` as a real fs into the fake fs.`
    # See: https://pytest-pyfakefs.readthedocs.io/en/stable/usage.html#access-to-files-in-the-real-file-system
    srcdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fs.add_real_directory(srcdir)

    uid = os.getuid()
    gid = os.getgid()

    def fchown(fd, _uid, _gid):
        """pyfakefs doesn't provide a working fchown or fchmod.
        In order to get permissions working generally across renames
        we need to provide our own implemenation.
        """
        file_obj = fs.get_open_file(fd).get_object()
        file_obj.st_uid = _uid
        file_obj.st_gid = _gid

    _cephadm = import_cephadm()
    with mock.patch('os.fchown', side_effect=fchown), \
         mock.patch('os.fchmod'), \
         mock.patch('platform.processor', return_value='x86_64'), \
         mock.patch('cephadm.extract_uid_gid', return_value=(uid, gid)):

        try:
            if not fake_filesystem.is_root():
                fake_filesystem.set_uid(0)
        except AttributeError:
            pass

        fs.create_dir(constants.DATA_DIR)
        fs.create_dir(constants.LOG_DIR)
        fs.create_dir(constants.LOCK_DIR)
        fs.create_dir(constants.LOGROTATE_DIR)
        fs.create_dir(constants.UNIT_DIR)
        fs.create_dir('/sys/block')

        yield fs


@pytest.fixture()
def host_sysfs(fs: fake_filesystem.FakeFilesystem):
    """Create a fake filesystem to represent sysfs"""
    enc_path = '/sys/class/scsi_generic/sg2/device/enclosure/0:0:1:0'
    dev_path = '/sys/class/scsi_generic/sg2/device'
    slot_count = 12
    fs.create_dir(dev_path)
    fs.create_file(os.path.join(dev_path, 'vendor'), contents="EnclosuresInc")
    fs.create_file(os.path.join(dev_path, 'model'), contents="D12")
    fs.create_file(os.path.join(enc_path, 'id'), contents='1')
    fs.create_file(os.path.join(enc_path, 'components'), contents=str(slot_count))
    for slot_num in range(slot_count):
        slot_dir = os.path.join(enc_path, str(slot_num))
        fs.create_file(os.path.join(slot_dir, 'locate'), contents='0')
        fs.create_file(os.path.join(slot_dir, 'fault'), contents='0')
        fs.create_file(os.path.join(slot_dir, 'slot'), contents=str(slot_num))
        if slot_num < 6:
            fs.create_file(os.path.join(slot_dir, 'status'), contents='Ok')
            slot_dev = os.path.join(slot_dir, 'device')
            fs.create_dir(slot_dev)
            fs.create_file(os.path.join(slot_dev, 'vpd_pg80'), contents=f'fake{slot_num:0>3}')
        else:
            fs.create_file(os.path.join(slot_dir, 'status'), contents='not installed')

    yield fs


@contextmanager
def with_cephadm_ctx(
    cmd: List[str],
    list_networks: Optional[Dict[str, Dict[str, List[str]]]] = None,
    hostname: Optional[str] = None,
):
    """
    :param cmd: cephadm command argv
    :param list_networks: mock 'list-networks' return
    :param hostname: mock 'socket.gethostname' return
    """
    if not hostname:
        hostname = 'host1'

    _cephadm = import_cephadm()
    with mock.patch('cephadmlib.net_utils.attempt_bind'), \
         mock.patch('cephadmlib.call_wrappers.call', return_value=('', '', 0)), \
         mock.patch('cephadmlib.call_wrappers.call_timeout', return_value=0), \
         mock.patch('cephadm.call', return_value=('', '', 0)), \
         mock.patch('cephadm.call_timeout', return_value=0), \
         mock.patch('cephadmlib.exe_utils.find_executable', return_value='foo'), \
         mock.patch('cephadm.get_container_info', return_value=None), \
         mock.patch('cephadm.is_available', return_value=True), \
         mock.patch('cephadm.json_loads_retry', return_value={'epoch' : 1}), \
         mock.patch('cephadm.logger'), \
         mock.patch('cephadm.FileLock'), \
         mock.patch('socket.gethostname', return_value=hostname):
        ctx: _cephadm.CephadmContext = _cephadm.cephadm_init_ctx(cmd)
        ctx.container_engine = mock_podman()
        if list_networks is not None:
            with mock.patch('cephadm.list_networks', return_value=list_networks):
                yield ctx
        else:
            yield ctx


@pytest.fixture()
def funkypatch(monkeypatch):
    """Defines the funkypatch fixtures that acts like a mixture between
    mock.patch and pytest's monkeypatch fixture.
    """
    fp = FunkyPatcher(monkeypatch)
    yield fp


class FunkyPatcher:
    """FunkyPatcher monkeypatches all imported instances of an object.

    Use `patch` to patch the canonical location of an object and FunkyPatcher
    will automatically replace other imports of that object.
    """

    def __init__(self, monkeypatcher):
        self._mp = monkeypatcher
        # keep track of objects we've already patched. this dictionary
        # maps a (module-name, object-name) tuple to the original object
        # before patching. This could be used to determine if a name has
        # already been patched or compare a patched object to the original.
        self._originals: Dict[Tuple[str, str], Any] = {}

    def patch(
        self,
        mod: str,
        name: str = '',
        *,
        dest: Any = None,
        force: bool = False,
    ) -> Any:
        """Patch an object and all existing imports of that object.
        Specify mod as `my.mod.name.obj` where obj is name of the object to be
        patched or as `my.mod.name` and specify `name` as the name of the
        object to be patched.
        If the object to be patched is not imported as the same name in `mod`
        it will *not* be automatically patched. In other words, `from
        my.mod.name import foo` will work, but `from my.mod.name import foo as
        _foo` will not.
        Use the keyword-only argument `dest` to specify the new object to be
        used. A MagicMock will be created and used if dest is None.
        Use the keyword-only argument `force` to override checks that a mocked
        objects are the same across modules. This can be used in the case that
        some other code already patched an object and you want funkypatch to
        override that patch (use with caution).
        Returns the patched object (the MagicMock or supplied dest).
        """
        import sys
        import importlib

        if not name:
            mod, name = mod.rsplit('.', 1)
        modname = (mod, name)
        # We don't strictly need the check but patching already patched objs is
        # confusing to think about. It's better to block it for now and perhaps
        # later we can relax these restrictions or be clever in some way.
        if modname in self._originals:
            raise KeyError(f'{modname} already patched')

        if dest is None:
            dest = mock.MagicMock()

        imod = importlib.import_module(mod)
        self._originals[modname] = getattr(imod, name)

        for mname, imod in sys.modules.items():
            try:
                obj = getattr(imod, name)
            except AttributeError:
                # no matching name in module
                continue
            # make sure that the module imported the same object as the
            # one we want to patch out, and not just some naming collision.
            # ensure the original object and the one in the module are the
            # same object
            if obj is self._originals[modname] or force:
                self._mp.setattr(imod, name, dest)
        return dest
