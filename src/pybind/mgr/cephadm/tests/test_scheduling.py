from typing import NamedTuple, List
import pytest

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, ServiceSpecValidationError

from cephadm.module import HostAssignment
from orchestrator import DaemonDescription, OrchestratorValidationError


class NodeAssignmentTest(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: List[str]

@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected",
    [
        # just hosts
        NodeAssignmentTest(
            'mon',
            PlacementSpec(hosts=['smithi060:[v2:172.21.15.60:3301,v1:172.21.15.60:6790]=c']),
            ['smithi060'],
            [],
            ['smithi060']
        ),
        # zero count
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=0),
            ['smithi060'],
            [],
            []
        ),

        # all_hosts
        NodeAssignmentTest(
            'mon',
            PlacementSpec(all_hosts=True),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
                DaemonDescription('mon', 'b', 'host2'),
            ],
            ['host1', 'host2', 'host3']
        ),
        # count that is bigger than the amount of hosts. Truncate to len(hosts)
        # RGWs should not be co-located to each other.
        NodeAssignmentTest(
            'rgw',
            PlacementSpec(count=4),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3']
        ),
        # count + partial host list
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=3, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
                DaemonDescription('mon', 'b', 'host2'),
            ],
            ['host1', 'host2', 'host3']
        ),
        # zero count + partial host list
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=0, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
                DaemonDescription('mon', 'b', 'host2'),
            ],
            []
        ),
        # count + partial host list + existing
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
            ],
            ['host1', 'host3']
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
            ],
            ['host1', 'host2']
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mon',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mon', 'a', 'host2'),
            ],
            ['host1', 'host2']
        ),
        # label only
        NodeAssignmentTest(
            'mon',
            PlacementSpec(label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3']
        ),
        # host_pattern
        NodeAssignmentTest(
            'mon',
            PlacementSpec(host_pattern='mon*'),
            'monhost1 monhost2 datahost'.split(),
            [],
            ['monhost1', 'monhost2']
        ),
    ])
def test_node_assignment(service_type, placement, hosts, daemons, expected):
    hosts = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        get_hosts_func=lambda _: hosts,
        get_daemons_func=lambda _: daemons).place()
    assert sorted([h.hostname for h in hosts]) == sorted(expected)

class NodeAssignmentTest2(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected_len: int
    in_set: List[str]

@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,in_set",
    [
        # empty
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host1', 'host2', 'host3'],
        ),

        # just count
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=1),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host1', 'host2', 'host3'],
        ),

        # hosts + (smaller) count
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=1, hosts='host1 host2'.split()),
            'host1 host2'.split(),
            [],
            1,
            ['host1', 'host2'],
        ),
        # hosts + (smaller) count, existing
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=1, hosts='host1 host2 host3'.split()),
            'host1 host2 host3'.split(),
            [DaemonDescription('mon', 'mon.a', 'host1'),],
            1,
            ['host1', 'host2', 'host3'],
        ),
        # hosts + (smaller) count, (more) existing
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=1, hosts='host1 host2 host3'.split()),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mon', 'a', 'host1'),
                DaemonDescription('mon', 'b', 'host2'),
            ],
            1,
            ['host1', 'host2']
        ),
        # count + partial host list
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [],
            2,
            ['host1', 'host2', 'host3']
        ),
        # label + count
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=1, label='foo'),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host1', 'host2', 'host3']
        ),
    ])
def test_node_assignment2(service_type, placement, hosts,
                          daemons, expected_len, in_set):
    hosts = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        get_hosts_func=lambda _: hosts,
        get_daemons_func=lambda _: daemons).place()
    assert len(hosts) == expected_len
    for h in [h.hostname for h in hosts]:
        assert h in in_set

@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,must_have",
    [
        # hosts + (smaller) count, (more) existing
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=3, hosts='host3'.split()),
            'host1 host2 host3'.split(),
            [],
            3,
            ['host3']
        ),
        # count + partial host list
        NodeAssignmentTest2(
            'mon',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [],
            2,
            ['host3']
        ),
    ])
def test_node_assignment3(service_type, placement, hosts,
                          daemons, expected_len, must_have):
    hosts = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        get_hosts_func=lambda _: hosts,
        get_daemons_func=lambda _: daemons).place()
    assert len(hosts) == expected_len
    for h in must_have:
        assert h in [h.hostname for h in hosts]


@pytest.mark.parametrize("placement",
    [
        ('1 all:true'),
        ('all:true label:foo'),
        ('all:true host1 host2'),
    ])
def test_bad_placements(placement):
    try:
        s = PlacementSpec.from_string(placement.split(' '))
        assert False
    except ServiceSpecValidationError as e:
        pass
