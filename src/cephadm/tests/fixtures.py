
import mock
import pytest

import os
import time

import cephadm as cd


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
