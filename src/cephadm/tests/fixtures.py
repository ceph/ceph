from importlib.resources import contents
import mock
import os
import pytest
import time

from contextlib import contextmanager
from pyfakefs import fake_filesystem

from typing import Callable, Dict, List, Optional


with mock.patch('builtins.open', create=True):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm.py').load_module()


def mock_docker():
    docker = mock.Mock(cd.Docker)
    docker.path = '/usr/bin/docker'
    return docker


def mock_podman():
    podman = mock.Mock(cd.Podman)
    podman.path = '/usr/bin/podman'
    podman.version = (2, 1, 0)
    return podman


def _daemon_path():
    return os.getcwd()


def mock_bad_firewalld():
    def raise_bad_firewalld():
        raise Exception('Called bad firewalld')
    f = mock.Mock(cd.Firewalld)
    f.enable_service_for = lambda _ : raise_bad_firewalld()
    f.apply_rules = lambda : raise_bad_firewalld()
    f.open_ports = lambda _ : raise_bad_firewalld()

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
    uid = os.getuid()
    gid = os.getgid()

    with mock.patch('os.fchown'), \
         mock.patch('os.fchmod'), \
         mock.patch('platform.processor', return_value='x86_64'), \
         mock.patch('cephadm.extract_uid_gid', return_value=(uid, gid)):

            fs.create_dir(cd.DATA_DIR)
            fs.create_dir(cd.LOG_DIR)
            fs.create_dir(cd.LOCK_DIR)
            fs.create_dir(cd.LOGROTATE_DIR)
            fs.create_dir(cd.UNIT_DIR)
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
    container_engine: Callable = mock_podman(),
    list_networks: Optional[Dict[str,Dict[str,List[str]]]] = None,
    hostname: Optional[str] = None,
):
    """
    :param cmd: cephadm command argv
    :param container_engine: container engine mock (podman or docker)
    :param list_networks: mock 'list-networks' return
    :param hostname: mock 'socket.gethostname' return
    """
    if not hostname:
        hostname = 'host1'

    with mock.patch('cephadm.attempt_bind'), \
         mock.patch('cephadm.call', return_value=('', '', 0)), \
         mock.patch('cephadm.call_timeout', return_value=0), \
         mock.patch('cephadm.find_executable', return_value='foo'), \
         mock.patch('cephadm.is_available', return_value=True), \
         mock.patch('cephadm.get_container_info', return_value=None), \
         mock.patch('cephadm.json_loads_retry', return_value={'epoch' : 1}), \
         mock.patch('socket.gethostname', return_value=hostname):
        ctx: cd.CephadmContext = cd.cephadm_init_ctx(cmd)
        ctx.container_engine = container_engine
        if list_networks is not None:
            with mock.patch('cephadm.list_networks', return_value=list_networks):
                yield ctx
        else:
            yield ctx

