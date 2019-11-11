from orchestrator import ServiceDescription
from ..module import SSHOrchestrator
from tests import mock


@mock.patch("ssh.module.SSHOrchestrator.get_ceph_option", lambda _,key: __file__)
def test_get_unique_name():
    o = SSHOrchestrator('module_name', 0, 0)
    existing = [
        ServiceDescription(service_instance='mon.a')
    ]
    new_mon = o.get_unique_name(existing, 'mon')
    assert new_mon.startswith('mon.')
    assert new_mon != 'mon.a'

