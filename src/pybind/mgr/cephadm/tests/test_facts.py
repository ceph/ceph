from ..import CephadmOrchestrator

from .fixtures import wait

from tests import mock


def test_facts(cephadm_module: CephadmOrchestrator):
    facts = {'node-1.ceph.com': {'bios_version': 'F2', 'cpu_cores': 16}}
    cephadm_module.cache.facts = facts
    ret_facts = cephadm_module.get_facts('node-1.ceph.com')
    assert wait(cephadm_module, ret_facts) == [{'bios_version': 'F2', 'cpu_cores': 16}]


@mock.patch("cephadm.inventory.Inventory.update_known_hostnames")
def test_known_hostnames(_update_known_hostnames, cephadm_module: CephadmOrchestrator):
    host_facts = {'hostname': 'host1.domain',
                  'shortname': 'host1',
                  'fqdn': 'host1.domain',
                  'memory_free_kb': 37383384,
                  'memory_total_kb': 40980612,
                  'nic_count': 2}
    cephadm_module.cache.update_host_facts('host1', host_facts)
    _update_known_hostnames.assert_called_with('host1.domain', 'host1', 'host1.domain')

    host_facts = {'hostname': 'host1.domain',
                  'memory_free_kb': 37383384,
                  'memory_total_kb': 40980612,
                  'nic_count': 2}
    cephadm_module.cache.update_host_facts('host1', host_facts)
    _update_known_hostnames.assert_called_with('host1.domain', '', '')


def test_host_cache_load_normalizes_uppercase_keys(cephadm_module: CephadmOrchestrator):
    """HostCache.load() normalizes uppercase persisted keys to lowercase."""
    import json
    from cephadm.inventory import HOST_CACHE_PREFIX

    cache = cephadm_module.cache
    host_data = json.dumps({
        'daemons': {},
        'last_device_update': '2024-01-01T00:00:00.000000',
    })
    cephadm_module.inventory._inventory['ceph-node-00'] = {
        'hostname': 'ceph-node-00', 'addr': '10.0.0.1', 'labels': [], 'status': '',
    }
    cephadm_module.set_store(HOST_CACHE_PREFIX + 'CEPH-NODE-00', host_data)

    cache.load()

    assert 'ceph-node-00' in cache.daemons
    assert 'CEPH-NODE-00' not in cache.daemons
    assert 'ceph-node-00' in cache.last_device_update


def test_inventory_load_normalizes_keys(cephadm_module: CephadmOrchestrator):
    """Inventory.__init__ normalizes uppercase persisted keys on load and persists them."""
    import json

    raw_data = {
        'MULTI80-SITE1': {
            'hostname': 'MULTI80-SITE1',
            'addr': '10.245.0.5',
            'labels': ['_admin'],
            'status': '',
        },
        'MULTI80-SITE2': {
            'hostname': 'MULTI80-SITE2',
            'addr': '10.245.0.6',
            'labels': [],
            'status': '',
        },
    }
    cephadm_module.set_store('inventory', json.dumps(raw_data))

    from cephadm.inventory import Inventory
    inv = Inventory(cephadm_module)

    assert 'multi80-site1' in inv
    assert 'MULTI80-SITE1' in inv
    assert list(inv._inventory.keys()) == ['multi80-site1', 'multi80-site2']
    assert inv._inventory['multi80-site1']['hostname'] == 'multi80-site1'

    saved = json.loads(cephadm_module.get_store('inventory'))
    assert 'multi80-site1' in saved
    assert 'MULTI80-SITE1' not in saved


def test_inventory_case_insensitive_operations(cephadm_module: CephadmOrchestrator):
    """Verify that inventory operations (rm, label, addr) accept any hostname case."""
    inv = cephadm_module.inventory
    inv._inventory = {
        'myhost-01': {
            'hostname': 'myhost-01',
            'addr': '10.0.0.1',
            'labels': ['_admin'],
        },
    }
    assert inv.get_addr('MYHOST-01') == '10.0.0.1'
    assert inv.get_addr('MyHost-01') == '10.0.0.1'
    assert inv.has_label('MYHOST-01', '_admin') is True

    inv.add_label('MYHOST-01', 'mon')
    assert inv.has_label('myhost-01', 'mon') is True

    inv.rm_label('MYHOST-01', 'mon')
    assert inv.has_label('myhost-01', 'mon') is False
