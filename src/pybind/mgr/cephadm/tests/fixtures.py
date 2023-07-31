import fnmatch
import asyncio
import sys
from tempfile import NamedTemporaryFile
from contextlib import contextmanager

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec
from ceph.utils import datetime_to_str, datetime_now
from cephadm.serve import CephadmServe, cephadmNoImage

try:
    from typing import Any, Iterator, List, Callable, Dict
except ImportError:
    pass

from cephadm import CephadmOrchestrator
from orchestrator import raise_if_exception, OrchResult, HostSpec, DaemonDescriptionStatus
from tests import mock


def async_side_effect(result):
    async def side_effect(*args, **kwargs):
        return result
    return side_effect


def get_ceph_option(_, key):
    return __file__


def get_module_option_ex(_, module, key, default=None):
    if module == 'prometheus':
        if key == 'server_port':
            return 9283
    return None


def _run_cephadm(ret):
    async def foo(s, host, entity, cmd, e, **kwargs):
        if cmd == 'gather-facts':
            return '{}', '', 0
        return [ret], '', 0
    return foo


def match_glob(val, pat):
    ok = fnmatch.fnmatchcase(val, pat)
    if not ok:
        assert pat in val


class MockEventLoopThread:
    def get_result(self, coro, timeout):
        if sys.version_info >= (3, 7):
            return asyncio.run(coro)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
            asyncio.set_event_loop(None)


def receive_agent_metadata(m: CephadmOrchestrator, host: str, ops: List[str] = None) -> None:
    to_update: Dict[str, Callable[[str, Any], None]] = {
        'ls': m._process_ls_output,
        'gather-facts': m.cache.update_host_facts,
        'list-networks': m.cache.update_host_networks,
    }
    if ops:
        for op in ops:
            out = m.wait_async(CephadmServe(m)._run_cephadm_json(host, cephadmNoImage, op, []))
            to_update[op](host, out)
    m.cache.last_daemon_update[host] = datetime_now()
    m.cache.last_facts_update[host] = datetime_now()
    m.cache.last_network_update[host] = datetime_now()
    m.cache.metadata_up_to_date[host] = True


def receive_agent_metadata_all_hosts(m: CephadmOrchestrator) -> None:
    for host in m.cache.get_hosts():
        receive_agent_metadata(m, host)


@contextmanager
def with_cephadm_module(module_options=None, store=None):
    """
    :param module_options: Set opts as if they were set before module.__init__ is called
    :param store: Set the store before module.__init__ is called
    """
    with mock.patch("cephadm.module.CephadmOrchestrator.get_ceph_option", get_ceph_option), \
            mock.patch("cephadm.services.osd.RemoveUtil._run_mon_cmd"), \
            mock.patch('cephadm.module.CephadmOrchestrator.get_module_option_ex', get_module_option_ex), \
            mock.patch("cephadm.module.CephadmOrchestrator.get_osdmap"), \
            mock.patch("cephadm.module.CephadmOrchestrator.remote"), \
            mock.patch("cephadm.agent.CephadmAgentHelpers._request_agent_acks"), \
            mock.patch("cephadm.agent.CephadmAgentHelpers._apply_agent", return_value=False), \
            mock.patch("cephadm.agent.CephadmAgentHelpers._agent_down", return_value=False), \
            mock.patch('cephadm.offline_watcher.OfflineHostWatcher.run'), \
            mock.patch('cephadm.tuned_profiles.TunedProfileUtils._remove_stray_tuned_profiles'), \
            mock.patch('cephadm.offline_watcher.OfflineHostWatcher.run'), \
            mock.patch('cephadm.http_server.CephadmHttpServer.run'):

        m = CephadmOrchestrator.__new__(CephadmOrchestrator)
        if module_options is not None:
            for k, v in module_options.items():
                m._ceph_set_module_option('cephadm', k, v)
        if store is None:
            store = {}
        if '_ceph_get/mon_map' not in store:
            m.mock_store_set('_ceph_get', 'mon_map', {
                'modified': datetime_to_str(datetime_now()),
                'fsid': 'foobar',
            })
        if '_ceph_get/mgr_map' not in store:
            m.mock_store_set('_ceph_get', 'mgr_map', {
                'services': {
                    'dashboard': 'http://[::1]:8080',
                    'prometheus': 'http://[::1]:8081'
                },
                'modules': ['dashboard', 'prometheus'],
            })
        for k, v in store.items():
            m._ceph_set_store(k, v)

        m.__init__('cephadm', 0, 0)
        m._cluster_fsid = "fsid"

        m.event_loop = MockEventLoopThread()
        m.tkey = NamedTemporaryFile(prefix='test-cephadm-identity-')

        yield m


def wait(m: CephadmOrchestrator, c: OrchResult) -> Any:
    return raise_if_exception(c)


@contextmanager
def with_host(m: CephadmOrchestrator, name, addr='1::4', refresh_hosts=True, rm_with_force=True):
    with mock.patch("cephadm.utils.resolve_ip", return_value=addr):
        wait(m, m.add_host(HostSpec(hostname=name)))
        if refresh_hosts:
            CephadmServe(m)._refresh_hosts_and_daemons()
            receive_agent_metadata(m, name)
        yield
        wait(m, m.remove_host(name, force=rm_with_force))


def assert_rm_service(cephadm: CephadmOrchestrator, srv_name):
    mon_or_mgr = cephadm.spec_store[srv_name].spec.service_type in ('mon', 'mgr')
    if mon_or_mgr:
        assert 'Unable' in wait(cephadm, cephadm.remove_service(srv_name))
        return
    assert wait(cephadm, cephadm.remove_service(srv_name)) == f'Removed service {srv_name}'
    assert cephadm.spec_store[srv_name].deleted is not None
    CephadmServe(cephadm)._check_daemons()
    CephadmServe(cephadm)._apply_all_services()
    assert cephadm.spec_store[srv_name].deleted
    unmanaged = cephadm.spec_store[srv_name].spec.unmanaged
    CephadmServe(cephadm)._purge_deleted_services()
    if not unmanaged:  # cause then we're not deleting daemons
        assert srv_name not in cephadm.spec_store, f'{cephadm.spec_store[srv_name]!r}'


@contextmanager
def with_service(cephadm_module: CephadmOrchestrator, spec: ServiceSpec, meth=None, host: str = '', status_running=False) -> Iterator[List[str]]:
    if spec.placement.is_empty() and host:
        spec.placement = PlacementSpec(hosts=[host], count=1)
    if meth is not None:
        c = meth(cephadm_module, spec)
        assert wait(cephadm_module, c) == f'Scheduled {spec.service_name()} update...'
    else:
        c = cephadm_module.apply([spec])
        assert wait(cephadm_module, c) == [f'Scheduled {spec.service_name()} update...']

    specs = [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())]
    assert spec in specs

    CephadmServe(cephadm_module)._apply_all_services()

    if status_running:
        make_daemons_running(cephadm_module, spec.service_name())

    dds = wait(cephadm_module, cephadm_module.list_daemons())
    own_dds = [dd for dd in dds if dd.service_name() == spec.service_name()]
    if host and spec.service_type != 'osd':
        assert own_dds

    yield [dd.name() for dd in own_dds]

    assert_rm_service(cephadm_module, spec.service_name())


def make_daemons_running(cephadm_module, service_name):
    own_dds = cephadm_module.cache.get_daemons_by_service(service_name)
    for dd in own_dds:
        dd.status = DaemonDescriptionStatus.running  # We're changing the reference
