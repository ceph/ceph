from ..import CephadmOrchestrator

from .fixtures import wait


def test_facts(cephadm_module: CephadmOrchestrator):
    facts = {'node-1.ceph.com': {'bios_version': 'F2', 'cpu_cores': 16}}
    cephadm_module.cache.facts = facts
    ret_facts = cephadm_module.get_facts('node-1.ceph.com')
    assert wait(cephadm_module, ret_facts) == [{'bios_version': 'F2', 'cpu_cores': 16}]
