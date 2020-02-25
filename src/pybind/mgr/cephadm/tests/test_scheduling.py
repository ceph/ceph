from typing import NamedTuple, List
import pytest

from cephadm.module import HostAssignment
from orchestrator import ServiceSpec, PlacementSpec, DaemonDescription, OrchestratorValidationError


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
        # label only
        NodeAssignmentTest(
            'mon',
            PlacementSpec(label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3']
        ),
    ])
def test_node_assignment(service_type, placement, hosts, daemons, expected):
    s = HostAssignment(spec=ServiceSpec(service_type, placement=placement),
                       get_hosts_func=lambda _: hosts,
                       get_daemons_func=lambda _: daemons).load()
    assert sorted([h.hostname for h in s.placement.hosts]) == sorted(expected)

class NodeAssignmentTest2(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected_len: int
    in_set: List[str]

@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,in_set",
    [
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
    s = HostAssignment(spec=ServiceSpec(service_type, placement=placement),
                       get_hosts_func=lambda _: hosts,
                       get_daemons_func=lambda _: daemons).load()
    assert len(s.placement.hosts) == expected_len
    for h in [h.hostname for h in s.placement.hosts]:
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
    s = HostAssignment(spec=ServiceSpec(service_type, placement=placement),
                       get_hosts_func=lambda _: hosts,
                       get_daemons_func=lambda _: daemons).load()
    assert len(s.placement.hosts) == expected_len
    for h in must_have:
        assert h in [h.hostname for h in s.placement.hosts]


@pytest.mark.parametrize("placement",
    [
        ('1 all:true'),
        ('all:true label:foo'),
        ('all:true host1 host2'),
    ])
def test_bad_placements(placement):
    try:
        s = PlacementSpec.from_strings(placement.split(' '))
        assert False
    except OrchestratorValidationError as e:
        pass
