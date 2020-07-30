from datetime import datetime

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, HostPlacementSpec
from cephadm import CephadmOrchestrator
from cephadm.tests.fixtures import _run_cephadm, cephadm_module, wait, match_glob, with_host
from orchestrator import ServiceDescription
from tests import mock

@mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
@mock.patch("cephadm.services.cephadmservice.RgwService.create_realm_zonegroup_zone", lambda _,__,___: None)
def test_service_ls(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        with with_host(cephadm_module, 'host2'):

            # emulate the old scheduler:
            c = cephadm_module.apply_rgw(
                ServiceSpec('rgw', 'r.z', placement=PlacementSpec(host_pattern='*', count=2))
            )
            assert wait(cephadm_module, c) == 'Scheduled rgw.r.z update...'

            cephadm_module._apply_all_services()
            out = {o.hostname for o in wait(cephadm_module, cephadm_module.list_daemons())}
            assert out == {'host1', 'host2'}

            c = cephadm_module.apply_rgw(
                ServiceSpec('rgw', 'r.z', placement=PlacementSpec(host_pattern='host1', count=2))
            )
            assert wait(cephadm_module, c) == 'Scheduled rgw.r.z update...'

            cephadm_module.migration_current = 0
            cephadm_module.migration.migrate()

            # assert we need all daemons.
            assert cephadm_module.migration_current == 0

            # Sorry, for this hack, but I need to make sure, Migration thinks,
            # we have updated all daemons already.
            cephadm_module.cache.last_daemon_update['host1'] = datetime.now()
            cephadm_module.cache.last_daemon_update['host2'] = datetime.now()

            cephadm_module.migration.migrate()
            assert cephadm_module.migration_current == 1

            out = [o.spec.placement for o in wait(cephadm_module, cephadm_module.describe_service())]
            assert out == [PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])]



#            assert_rm_service(cephadm_module, 'rgw.r.z')
#            assert_rm_daemon(cephadm_module, 'mds.name', 'test')