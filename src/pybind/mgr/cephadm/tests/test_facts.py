import pytest

from ..inventory import HostCache
from ..import CephadmOrchestrator


@pytest.fixture()
def test_facts():
    facts = {'node-1.ceph.com', {'bios_version': 'F2', 'cpu_cores': 16}}
    HostCache.facts = facts
    ret_facts = CephadmOrchestrator.get_facts('node-1.ceph.com')
    assert ret_facts == [{'bios_version': 'F2', 'cpu_cores': 16}]
