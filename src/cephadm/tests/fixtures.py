import mock
import os
import pytest
import time

from pyfakefs import fake_filesystem


with mock.patch('builtins.open', create=True):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()


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
