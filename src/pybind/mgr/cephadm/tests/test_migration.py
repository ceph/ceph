import json
import pytest

from ceph.deployment.service_spec import (
    PlacementSpec,
    ServiceSpec,
    HostPlacementSpec,
    RGWSpec,
    IngressSpec,
    IscsiServiceSpec
)
from ceph.utils import datetime_to_str, datetime_now
from cephadm import CephadmOrchestrator
from cephadm.inventory import SPEC_STORE_PREFIX
from cephadm.migrations import LAST_MIGRATION
from cephadm.tests.fixtures import _run_cephadm, wait, with_host, receive_agent_metadata_all_hosts
from cephadm.serve import CephadmServe
from orchestrator import DaemonDescription
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
            receive_agent_metadata_all_hosts(cephadm_module)
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
            assert cephadm_module.migration_current >= 2

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
        assert cephadm_module.migration_current >= 2

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
        assert cephadm_module.migration_current >= 2

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


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_nfs_initial(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(
            SPEC_STORE_PREFIX + 'mds',
            json.dumps({
                'spec': {
                    'service_type': 'nfs',
                    'service_id': 'foo',
                    'placement': {
                        'hosts': ['host1']
                    },
                    'spec': {
                        'pool': 'mypool',
                        'namespace': 'foons',
                    },
                },
                'created': datetime_to_str(datetime_now()),
            }, sort_keys=True),
        )
        cephadm_module.migration_current = 1
        cephadm_module.spec_store.load()

        ls = json.loads(cephadm_module.get_store('nfs_migration_queue'))
        assert ls == [['foo', 'mypool', 'foons']]

        cephadm_module.migration.migrate(True)
        assert cephadm_module.migration_current == 2

        cephadm_module.migration.migrate()
        assert cephadm_module.migration_current == LAST_MIGRATION


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_nfs_initial_octopus(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(
            SPEC_STORE_PREFIX + 'mds',
            json.dumps({
                'spec': {
                    'service_type': 'nfs',
                    'service_id': 'ganesha-foo',
                    'placement': {
                        'hosts': ['host1']
                    },
                    'spec': {
                        'pool': 'mypool',
                        'namespace': 'foons',
                    },
                },
                'created': datetime_to_str(datetime_now()),
            }, sort_keys=True),
        )
        cephadm_module.migration_current = 1
        cephadm_module.spec_store.load()

        ls = json.loads(cephadm_module.get_store('nfs_migration_queue'))
        assert ls == [['ganesha-foo', 'mypool', 'foons']]

        cephadm_module.migration.migrate(True)
        assert cephadm_module.migration_current == 2

        cephadm_module.migration.migrate()
        assert cephadm_module.migration_current == LAST_MIGRATION


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_admin_client_keyring(cephadm_module: CephadmOrchestrator):
    assert 'client.admin' not in cephadm_module.keys.keys

    cephadm_module.migration_current = 3
    cephadm_module.migration.migrate()
    assert cephadm_module.migration_current == LAST_MIGRATION

    assert cephadm_module.keys.keys['client.admin'].placement.label == '_admin'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_set_sane_value(cephadm_module: CephadmOrchestrator):
    cephadm_module.migration_current = 0
    cephadm_module.migration.set_sane_migration_current()
    assert cephadm_module.migration_current == 0

    cephadm_module.migration_current = LAST_MIGRATION
    cephadm_module.migration.set_sane_migration_current()
    assert cephadm_module.migration_current == LAST_MIGRATION

    cephadm_module.migration_current = None
    cephadm_module.migration.set_sane_migration_current()
    assert cephadm_module.migration_current == LAST_MIGRATION

    cephadm_module.migration_current = LAST_MIGRATION + 1
    cephadm_module.migration.set_sane_migration_current()
    assert cephadm_module.migration_current == 0

    cephadm_module.migration_current = None
    ongoing = cephadm_module.migration.is_migration_ongoing()
    assert not ongoing
    assert cephadm_module.migration_current == LAST_MIGRATION

    cephadm_module.migration_current = LAST_MIGRATION + 1
    ongoing = cephadm_module.migration.is_migration_ongoing()
    assert ongoing
    assert cephadm_module.migration_current == 0


@pytest.mark.parametrize(
    "rgw_spec_store_entry, should_migrate",
    [
        ({
            'spec': {
                'service_type': 'rgw',
                'service_name': 'rgw.foo',
                'service_id': 'foo',
                'placement': {
                    'hosts': ['host1']
                },
                'spec': {
                    'rgw_frontend_type': 'beast  tcp_nodelay=1    request_timeout_ms=65000   rgw_thread_pool_size=512',
                    'rgw_frontend_port': '5000',
                },
            },
            'created': datetime_to_str(datetime_now()),
        }, True),
        ({
            'spec': {
                'service_type': 'rgw',
                'service_name': 'rgw.foo',
                'service_id': 'foo',
                'placement': {
                    'hosts': ['host1']
                },
            },
            'created': datetime_to_str(datetime_now()),
        }, False),
    ]
)
@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
def test_migrate_rgw_spec(cephadm_module: CephadmOrchestrator, rgw_spec_store_entry, should_migrate):
    with with_host(cephadm_module, 'host1'):
        cephadm_module.set_store(
            SPEC_STORE_PREFIX + 'rgw',
            json.dumps(rgw_spec_store_entry, sort_keys=True),
        )

        # make sure rgw_migration_queue is populated accordingly
        cephadm_module.migration_current = 1
        cephadm_module.spec_store.load()
        ls = json.loads(cephadm_module.get_store('rgw_migration_queue'))
        assert 'rgw' == ls[0]['spec']['service_type']

        # shortcut rgw_migration_queue loading by directly assigning
        # ls output to rgw_migration_queue list
        cephadm_module.migration.rgw_migration_queue = ls

        # skip other migrations and go directly to 5_6 migration (RGW spec)
        cephadm_module.migration_current = 5
        cephadm_module.migration.migrate()
        assert cephadm_module.migration_current == LAST_MIGRATION

        if should_migrate:
            # make sure the spec has been migrated and the the param=value entries
            # that were part of the rgw_frontend_type are now in the new
            # 'rgw_frontend_extra_args' list
            assert 'rgw.foo' in cephadm_module.spec_store.all_specs
            rgw_spec = cephadm_module.spec_store.all_specs['rgw.foo']
            assert dict(rgw_spec.to_json()) == {'service_type': 'rgw',
                                                'service_id': 'foo',
                                                'service_name': 'rgw.foo',
                                                'placement': {'hosts': ['host1']},
                                                'spec': {
                                                    'rgw_frontend_extra_args': ['tcp_nodelay=1',
                                                                                'request_timeout_ms=65000',
                                                                                'rgw_thread_pool_size=512'],
                                                    'rgw_frontend_port': '5000',
                                                    'rgw_frontend_type': 'beast',
                                                }}
        else:
            # in a real environment, we still expect the spec to be there,
            # just untouched by the migration. For this test specifically
            # though, the spec will only have ended up in the spec store
            # if it was migrated, so we can use this to test the spec
            # was untouched
            assert 'rgw.foo' not in cephadm_module.spec_store.all_specs


def test_migrate_cert_store(cephadm_module: CephadmOrchestrator):
    rgw_spec = RGWSpec(service_id='foo', rgw_frontend_ssl_certificate='rgw_cert', ssl=True)
    iscsi_spec = IscsiServiceSpec(service_id='foo', pool='foo', ssl_cert='iscsi_cert', ssl_key='iscsi_key')
    ingress_spec = IngressSpec(service_id='rgw.foo', ssl_cert='ingress_cert', ssl_key='ingress_key', ssl=True)
    cephadm_module.spec_store._specs = {
        'rgw.foo': rgw_spec,
        'iscsi.foo': iscsi_spec,
        'ingress.rgw.foo': ingress_spec
    }

    cephadm_module.set_store('cephadm_agent/root/cert', 'agent_cert')
    cephadm_module.set_store('cephadm_agent/root/key', 'agent_key')
    cephadm_module.set_store('service_discovery/root/cert', 'service_discovery_cert')
    cephadm_module.set_store('service_discovery/root/key', 'service_discovery_key')

    cephadm_module.set_store('host1/grafana_crt', 'grafana_cert1')
    cephadm_module.set_store('host1/grafana_key', 'grafana_key1')
    cephadm_module.set_store('host2/grafana_crt', 'grafana_cert2')
    cephadm_module.set_store('host2/grafana_key', 'grafana_key2')
    cephadm_module.cache.daemons = {'host1': {'grafana.host1': DaemonDescription('grafana', 'host1', 'host1')},
                                    'host2': {'grafana.host2': DaemonDescription('grafana', 'host2', 'host2')}}

    cephadm_module.migration.migrate_6_7()

    assert cephadm_module.cert_key_store.get_cert('rgw_frontend_ssl_cert', service_name='rgw.foo')
    assert cephadm_module.cert_key_store.get_cert('iscsi_ssl_cert', service_name='iscsi.foo')
    assert cephadm_module.cert_key_store.get_key('iscsi_ssl_key', service_name='iscsi.foo')
    assert cephadm_module.cert_key_store.get_cert('ingress_ssl_cert', service_name='ingress.rgw.foo')
    assert cephadm_module.cert_key_store.get_key('ingress_ssl_key', service_name='ingress.rgw.foo')

    assert cephadm_module.cert_key_store.get_cert('grafana_cert', host='host1')
    assert cephadm_module.cert_key_store.get_cert('grafana_cert', host='host2')
    assert cephadm_module.cert_key_store.get_key('grafana_key', host='host1')
    assert cephadm_module.cert_key_store.get_key('grafana_key', host='host2')
