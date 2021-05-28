import mock
import os
import pytest
import time

from contextlib import contextmanager
from pyfakefs import fake_filesystem

from typing import Callable, Dict, List, Optional


with mock.patch('builtins.open', create=True):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()


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
    

@pytest.fixture
def exporter():
    with mock.patch('cephadm.CephadmDaemon.daemon_path', _daemon_path()), \
       mock.patch('cephadm.CephadmDaemon.can_run', return_value=True), \
       mock.patch('cephadm.CephadmDaemon.run', _mock_run), \
       mock.patch('cephadm.CephadmDaemon._scrape_host_facts', _mock_scrape_host):

        ctx = cd.CephadmContext()
        exporter = cd.CephadmDaemon(ctx, fsid='foobar', daemon_id='test')
        assert exporter.token == 'MyAccessToken' 
        yield exporter


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
         mock.patch('cephadm.extract_uid_gid', return_value=(uid, gid)):

            fs.create_dir(cd.DATA_DIR)
            fs.create_dir(cd.LOG_DIR)
            fs.create_dir(cd.LOCK_DIR)
            fs.create_dir(cd.LOGROTATE_DIR)
            fs.create_dir(cd.UNIT_DIR)

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
    if not list_networks:
        list_networks = {}
    if not hostname:
        hostname = 'host1'

    with mock.patch('cephadm.get_parm'), \
         mock.patch('cephadm.attempt_bind'), \
         mock.patch('cephadm.call', return_value=('', '', 0)), \
         mock.patch('cephadm.find_executable', return_value='foo'), \
         mock.patch('cephadm.is_available', return_value=True), \
         mock.patch('cephadm.json_loads_retry', return_value={'epoch' : 1}), \
         mock.patch('cephadm.list_networks', return_value=list_networks), \
         mock.patch('socket.gethostname', return_value=hostname):
             ctx: cd.CephadmContext = cd.cephadm_init_ctx(cmd)
             ctx.container_engine = container_engine
             yield ctx
