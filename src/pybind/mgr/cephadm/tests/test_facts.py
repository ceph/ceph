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
