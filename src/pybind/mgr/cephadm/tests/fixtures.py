import fnmatch
from contextlib import contextmanager

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec
from ceph.utils import datetime_to_str, datetime_now
from cephadm.serve import CephadmServe

try:
    from typing import Any, Iterator, List
except ImportError:
    pass

from cephadm import CephadmOrchestrator
from orchestrator import raise_if_exception, OrchResult, HostSpec
from tests import mock


def get_ceph_option(_, key):
    return __file__


def _run_cephadm(ret):
    def foo(*args, **kwargs):
        return [ret], '', 0
    return foo


def match_glob(val, pat):
    ok = fnmatch.fnmatchcase(val, pat)
    if not ok:
        assert pat in val


@contextmanager
def with_cephadm_module(module_options=None, store=None):
    """
    :param module_options: Set opts as if they were set before module.__init__ is called
    :param store: Set the store before module.__init__ is called
    """
    with mock.patch("cephadm.module.CephadmOrchestrator.get_ceph_option", get_ceph_option),\
            mock.patch("cephadm.services.osd.RemoveUtil._run_mon_cmd"), \
            mock.patch("cephadm.module.CephadmOrchestrator.get_osdmap"), \
            mock.patch("cephadm.services.osd.OSDService.get_osdspec_affinity", return_value='test_spec'), \
            mock.patch("cephadm.module.CephadmOrchestrator.remote"):

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
        for k, v in store.items():
            m._ceph_set_store(k, v)

        m.__init__('cephadm', 0, 0)
        m._cluster_fsid = "fsid"
        yield m


def wait(m, c):
    # type: (CephadmOrchestrator, OrchResult) -> Any
    return raise_if_exception(c)


@contextmanager
def with_host(m: CephadmOrchestrator, name, refresh_hosts=True):
    # type: (CephadmOrchestrator, str) -> None
    wait(m, m.add_host(HostSpec(hostname=name)))
    if refresh_hosts:
        CephadmServe(m)._refresh_hosts_and_daemons()
    yield
    wait(m, m.remove_host(name))


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
def with_service(cephadm_module: CephadmOrchestrator, spec: ServiceSpec, meth=None, host: str = '') -> Iterator[List[str]]:
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

    dds = wait(cephadm_module, cephadm_module.list_daemons())
    own_dds = [dd for dd in dds if dd.service_name() == spec.service_name()]
    if host:
        assert own_dds

    yield [dd.name() for dd in own_dds]

    assert_rm_service(cephadm_module, spec.service_name())


def _deploy_cephadm_binary(host):
    def foo(*args, **kwargs):
        return True
    return foo
