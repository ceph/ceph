import json

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, HostPlacementSpec
from ceph.utils import datetime_to_str, datetime_now
from cephadm import CephadmOrchestrator
from cephadm.inventory import SPEC_STORE_PREFIX
from cephadm.tests.fixtures import _run_cephadm, wait, with_host
from cephadm.serve import CephadmServe
from tests import mock


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_scheduler(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1', refresh_hosts=False):
        with with_host(cephadm_module, 'host2', refresh_hosts=False):

            # emulate the old scheduler:
            c = cephadm_module.apply_rgw(
                ServiceSpec('rgw', 'r.z', placement=PlacementSpec(host_pattern='*', count=2))
            )
            assert wait(cephadm_module, c) == 'Scheduled rgw.r.z update...'

            # with pytest.raises(OrchestratorError, match="cephadm migration still ongoing. Please wait, until the migration is complete."):
            CephadmServe(cephadm_module)._apply_all_services()

            cephadm_module.migration_current = 0
            cephadm_module.migration.migrate()
            # assert we need all daemons.
            assert cephadm_module.migration_current == 0

            CephadmServe(cephadm_module)._refresh_hosts_and_daemons()
            cephadm_module.migration.migrate()

            CephadmServe(cephadm_module)._apply_all_services()

            out = {o.hostname for o in wait(cephadm_module, cephadm_module.list_daemons())}
            assert out == {'host1', 'host2'}

            c = cephadm_module.apply_rgw(
                ServiceSpec('rgw', 'r.z', placement=PlacementSpec(host_pattern='host1', count=2))
            )
            assert wait(cephadm_module, c) == 'Scheduled rgw.r.z update...'

            # Sorry, for this hack, but I need to make sure, Migration thinks,
            # we have updated all daemons already.
            cephadm_module.cache.last_daemon_update['host1'] = datetime_now()
            cephadm_module.cache.last_daemon_update['host2'] = datetime_now()

            cephadm_module.migration_current = 0
            cephadm_module.migration.migrate()
            assert cephadm_module.migration_current == 2

            out = [o.spec.placement for o in wait(
                cephadm_module, cephadm_module.describe_service())]
            assert out == [PlacementSpec(count=2, hosts=[HostPlacementSpec(
                hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])]


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_service_id_mon_one(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(SPEC_STORE_PREFIX + 'mon.wrong', json.dumps({
            'spec': {
                'service_type': 'mon',
                'service_id': 'wrong',
                'placement': {
                    'hosts': ['host1']
                }
            },
            'created': datetime_to_str(datetime_now()),
        }, sort_keys=True),
        )

        cephadm_module.spec_store.load()

        assert len(cephadm_module.spec_store.all_specs) == 1
        assert cephadm_module.spec_store.all_specs['mon.wrong'].service_name() == 'mon'

        cephadm_module.migration_current = 1
        cephadm_module.migration.migrate()
        assert cephadm_module.migration_current == 2

        assert len(cephadm_module.spec_store.all_specs) == 1
        assert cephadm_module.spec_store.all_specs['mon'] == ServiceSpec(
            service_type='mon',
            unmanaged=True,
            placement=PlacementSpec(hosts=['host1'])
        )


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_service_id_mon_two(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(SPEC_STORE_PREFIX + 'mon', json.dumps({
            'spec': {
                'service_type': 'mon',
                'placement': {
                    'count': 5,
                }
            },
            'created': datetime_to_str(datetime_now()),
        }, sort_keys=True),
        )
        cephadm_module.set_store(SPEC_STORE_PREFIX + 'mon.wrong', json.dumps({
            'spec': {
                'service_type': 'mon',
                'service_id': 'wrong',
                'placement': {
                    'hosts': ['host1']
                }
            },
            'created': datetime_to_str(datetime_now()),
        }, sort_keys=True),
        )

        cephadm_module.spec_store.load()

        assert len(cephadm_module.spec_store.all_specs) == 2
        assert cephadm_module.spec_store.all_specs['mon.wrong'].service_name() == 'mon'
        assert cephadm_module.spec_store.all_specs['mon'].service_name() == 'mon'

        cephadm_module.migration_current = 1
        cephadm_module.migration.migrate()
        assert cephadm_module.migration_current == 2

        assert len(cephadm_module.spec_store.all_specs) == 1
        assert cephadm_module.spec_store.all_specs['mon'] == ServiceSpec(
            service_type='mon',
            unmanaged=True,
            placement=PlacementSpec(count=5)
        )


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_service_id_mds_one(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(SPEC_STORE_PREFIX + 'mds', json.dumps({
            'spec': {
                'service_type': 'mds',
                'placement': {
                    'hosts': ['host1']
                }
            },
            'created': datetime_to_str(datetime_now()),
        }, sort_keys=True),
        )

        cephadm_module.spec_store.load()

        # there is nothing to migrate, as the spec is gone now.
        assert len(cephadm_module.spec_store.all_specs) == 0
