import pytest
import testinfra
from typing import Any, Dict, List, TYPE_CHECKING


@pytest.fixture()
def node(host: testinfra.host, request: Any) -> Dict[str, Any]:
    """ This fixture represents a single node in the ceph cluster. Using the
    host.ansible fixture provided by testinfra it can access all the ansible
    variables provided to it by the specific test scenario being ran.

    You must include this fixture on any tests that operate on specific type
    of node because it contains the logic to manage which tests a node
    should run.
    """
    ansible_vars = host.ansible.get_variables()
    num_osd_ports: int = 4
    cluster_address: str = ''
    # I can assume eth1 because I know all the vagrant
    # boxes we test with use that interface
    address: str = host.interface("eth1").addresses[0]
    subnet: str = ".".join(ansible_vars["public_network"].split(".")[0:-1])
    dmcrypt: bool = ansible_vars.get('dmcrypt', False)
    # Can't check the length of `devices` directly which is `"{{ hdd + nvme }}"`
    # It would return the length of the string "{{ hdd + nvme }}"
    # num_osds: "{{ osd_devices | map(attribute='data_devices') | flatten | length | int }}"
    osd_devices: List[Dict[str, Any]] = ansible_vars.get('osd_devices', [])

    data_devices: List[str] = []
    for osd_device in osd_devices:
        if isinstance(osd_device, dict) and 'data_devices' in osd_device:
            data_devices.extend(osd_device['data_devices'])

    all_devices: List[str] = []
    for osd_device in osd_devices:
        for v in osd_device.values():
            all_devices.extend(v)

    num_osds: int = len(data_devices)
    osds_per_device: int = ansible_vars.get('osds_per_device', 1)
    num_osds = num_osds * osds_per_device

    # If number of devices doesn't map to number of OSDs, allow tests to define
    # that custom number, defaulting it to ``num_devices``
    #num_osds = ansible_vars.get('num_osds', num_osds)
    cluster_address = host.interface('eth2').addresses[0]
    cluster_fsid = host.run('sudo ls /var/lib/ceph')

    # if (request.node.get_closest_marker('unencrypted') and dmcrypt) or \
    #    (request.node.get_closest_marker('dmcrypt') and not dmcrypt):
    #     pytest.skip('Not a valid test.')

    if dmcrypt != bool(request.node.get_closest_marker('dmcrypt')) or \
       request.node.get_closest_marker('unencrypted'):
        pytest.skip('Not a valid test.')

    data: Dict[str, Any] = dict(
        address=address,
        subnet=subnet,
        cluster_fsid=cluster_fsid.stdout.strip(),
        osd_ids=range(0, num_osds),
        num_osds=num_osds,
        num_osd_ports=num_osd_ports,
        cluster_address=cluster_address,
        all_devices=all_devices
    )
    return data

def pytest_collection_modifyitems(items: List[pytest.Item]) -> None:
    for item in items:
        test_path = item.location[0]
        if "unencrypted" in test_path:
            item.add_marker(pytest.mark.unencrypted)
        if "dmcrypt" in test_path:
            item.add_marker(pytest.mark.dmcrypt)
        # else:
        #     item.add_marker(pytest.mark.all)
