# Disable autopep8 for this file:

# fmt: off

from typing import NamedTuple, List
import pytest

from ceph.deployment.hostspec import HostSpec
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, ServiceSpecValidationError

from cephadm.module import HostAssignment
from orchestrator import DaemonDescription, OrchestratorValidationError, OrchestratorError, HostSpec


def wrapper(func):
    # some odd thingy to revert the order or arguments
    def inner(*args):
        def inner2(expected):
            func(expected, *args)
        return inner2
    return inner


@wrapper
def none(expected):
    assert expected == []


@wrapper
def one_of(expected, *hosts):
    if not isinstance(expected, list):
        assert False, str(expected)
    assert len(expected) == 1, f'one_of failed len({expected}) != 1'
    assert expected[0] in hosts


@wrapper
def two_of(expected, *hosts):
    if not isinstance(expected, list):
        assert False, str(expected)
    assert len(expected) == 2, f'one_of failed len({expected}) != 2'
    matches = 0
    for h in hosts:
        matches += int(h in expected)
    if matches != 2:
        assert False, f'two of {hosts} not in {expected}'


@wrapper
def exactly(expected, *hosts):
    assert expected == list(hosts)


@wrapper
def error(expected, kind, match):
    assert isinstance(expected, kind), (str(expected), match)
    assert str(expected) == match, (str(expected), match)


@wrapper
def _or(expected, *inners):
    def catch(inner):
        try:
            inner(expected)
        except AssertionError as e:
            return e
    result = [catch(i) for i in inners]
    if None not in result:
        assert False, f"_or failed: {expected}"


def _always_true(_): pass


def k(s):
    return [e for e in s.split(' ') if e]


def get_result(key, results):
    def match(one):
        for o, k in zip(one, key):
            if o != k and o != '*':
                return False
        return True
    return [v for k, v in results
     if match(k)][0]


def mk_spec_and_host(spec_section, hosts, explicit_key, explicit, count):

    if spec_section == 'hosts':
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(
                    hosts=explicit,
                    count=count,
                ))
    elif spec_section == 'label':
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(
            label='mylabel',
            count=count,
        ))
    elif spec_section == 'host_pattern':
        pattern = {
            'e': 'notfound',
            '1': '1',
            '12': '[1-2]',
            '123': '*',
        }[explicit_key]
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(
                    host_pattern=pattern,
                    count=count,
                ))
    else:
        assert False

    hosts = [
            HostSpec(h, labels=['mylabel']) if h in explicit else HostSpec(h)
            for h in hosts
        ]

    return mk_spec, hosts


def run_scheduler_test(results, mk_spec, hosts, get_daemons_func, key_elems):
    key = ' '.join('N' if e is None else str(e) for e in key_elems)
    try:
        assert_res = get_result(k(key), results)
    except IndexError:
        try:
            spec = mk_spec()
            host_res = HostAssignment(
                spec=spec,
                hosts=hosts,
                get_daemons_func=get_daemons_func).place()
            if isinstance(host_res, list):
                e = ', '.join(repr(h.hostname) for h in host_res)
                assert False, f'`(k("{key}"), exactly({e})),` not found'
            assert False, f'`(k("{key}"), ...),` not found'
        except OrchestratorError as e:
            assert False, f'`(k("{key}"), error({type(e).__name__}, {repr(str(e))})),` not found'

    for _ in range(10):  # scheduler has a random component
        try:
            spec = mk_spec()
            host_res = HostAssignment(
                spec=spec,
                hosts=hosts,
                get_daemons_func=get_daemons_func).place()

            assert_res(sorted([h.hostname for h in host_res]))
        except Exception as e:
            assert_res(e)


# * first match from the top wins
# * where e=[], *=any
#
#       + list of known hosts available for scheduling (host_key)
#       |   + hosts used for explict placement (explicit_key)
#       |   |   + count
#       |   |   | + section (host, label, pattern)
#       |   |   | |     + expected result
#       |   |   | |     |
test_explicit_scheduler_results = [
    (k("*   *   0 *"), error(ServiceSpecValidationError, 'num/count must be > 1')),
    (k("*   e   N l"), error(OrchestratorValidationError, 'Cannot place <ServiceSpec for service_name=mgr>: No matching hosts for label mylabel')),
    (k("*   e   N p"), error(OrchestratorValidationError, 'Cannot place <ServiceSpec for service_name=mgr>: No matching hosts')),
    (k("*   e   N h"), error(OrchestratorValidationError, 'placement spec is empty: no hosts, no label, no pattern, no count')),
    (k("*   e   * *"), none),
    (k("1   12  * h"), error(OrchestratorValidationError, "Cannot place <ServiceSpec for service_name=mgr> on 2: Unknown hosts")),
    (k("1   123 * h"), error(OrchestratorValidationError, "Cannot place <ServiceSpec for service_name=mgr> on 2, 3: Unknown hosts")),
    (k("1   *   * *"), exactly('1')),
    (k("12  1   * *"), exactly('1')),
    (k("12  12  1 *"), one_of('1', '2')),
    (k("12  12  * *"), exactly('1', '2')),
    (k("12  123 * h"), error(OrchestratorValidationError, "Cannot place <ServiceSpec for service_name=mgr> on 3: Unknown hosts")),
    (k("12  123 1 *"), one_of('1', '2', '3')),
    (k("12  123 * *"), two_of('1', '2', '3')),
    (k("123 1   * *"), exactly('1')),
    (k("123 12  1 *"), one_of('1', '2')),
    (k("123 12  * *"), exactly('1', '2')),
    (k("123 123 1 *"), one_of('1', '2', '3')),
    (k("123 123 2 *"), two_of('1', '2', '3')),
    (k("123 123 * *"), exactly('1', '2', '3')),
]

@pytest.mark.parametrize("spec_section_key,spec_section",
    [
        ('h', 'hosts'),
        ('l', 'label'),
        ('p', 'host_pattern'),
    ])
@pytest.mark.parametrize("count",
    [
        None,
        0,
        1,
        2,
        3,
    ])
@pytest.mark.parametrize("explicit_key, explicit",
    [
        ('e', []),
        ('1', ['1']),
        ('12', ['1', '2']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("host_key, hosts",
    [
        ('1', ['1']),
        ('12', ['1', '2']),
        ('123', ['1', '2', '3']),
    ])
def test_explicit_scheduler(host_key, hosts,
                            explicit_key, explicit,
                            count,
                            spec_section_key, spec_section):

    mk_spec, hosts = mk_spec_and_host(spec_section, hosts, explicit_key, explicit, count)
    run_scheduler_test(
        results=test_explicit_scheduler_results,
        mk_spec=mk_spec,
        hosts=hosts,
        get_daemons_func=lambda _: [],
        key_elems=(host_key, explicit_key, count, spec_section_key)
    )


# * first match from the top wins
# * where e=[], *=any
#
#       + list of known hosts available for scheduling (host_key)
#       |   + hosts used for explict placement (explicit_key)
#       |   |   + count
#       |   |   | + existing daemons
#       |   |   | |     + section (host, label, pattern)
#       |   |   | |     |   + expected result
#       |   |   | |     |   |
test_scheduler_daemons_results = [
    (k("*   1   * *   *"), exactly('1')),
    (k("1   123 * *   h"), error(OrchestratorValidationError, 'Cannot place <ServiceSpec for service_name=mgr> on 2, 3: Unknown hosts')),
    (k("1   123 * *   *"), exactly('1')),
    (k("12  123 * *   h"), error(OrchestratorValidationError, 'Cannot place <ServiceSpec for service_name=mgr> on 3: Unknown hosts')),
    (k("12  123 N *   *"), exactly('1', '2')),
    (k("12  123 1 *   *"), one_of('1', '2')),
    (k("12  123 2 *   *"), exactly('1', '2')),
    (k("12  123 3 *   *"), exactly('1', '2')),
    (k("123 123 N *   *"), exactly('1', '2', '3')),
    (k("123 123 1 e   *"), one_of('1', '2', '3')),
    (k("123 123 1 1   *"), exactly('1')),
    (k("123 123 1 3   *"), exactly('3')),
    (k("123 123 1 12  *"), one_of('1', '2')),
    (k("123 123 1 112 *"), one_of('1', '2')),
    (k("123 123 1 23  *"), one_of('2', '3')),
    (k("123 123 1 123 *"), one_of('1', '2', '3')),
    (k("123 123 2 e   *"), two_of('1', '2', '3')),
    (k("123 123 2 1   *"), _or(exactly('1', '2'), exactly('1', '3'))),
    (k("123 123 2 3   *"), _or(exactly('1', '3'), exactly('2', '3'))),
    (k("123 123 2 12  *"), exactly('1', '2')),
    (k("123 123 2 112 *"), exactly('1', '2')),
    (k("123 123 2 23  *"), exactly('2', '3')),
    (k("123 123 2 123 *"), two_of('1', '2', '3')),
    (k("123 123 3 *   *"), exactly('1', '2', '3')),
]


@pytest.mark.parametrize("spec_section_key,spec_section",
    [
        ('h', 'hosts'),
        ('l', 'label'),
        ('p', 'host_pattern'),
    ])
@pytest.mark.parametrize("daemons_key, daemons",
    [
        ('e', []),
        ('1', ['1']),
        ('3', ['3']),
        ('12', ['1', '2']),
        ('112', ['1', '1', '2']),  # deal with existing co-located daemons
        ('23', ['2', '3']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("count",
    [
        None,
        1,
        2,
        3,
    ])
@pytest.mark.parametrize("explicit_key, explicit",
    [
        ('1', ['1']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("host_key, hosts",
    [
        ('1', ['1']),
        ('12', ['1', '2']),
        ('123', ['1', '2', '3']),
    ])
def test_scheduler_daemons(host_key, hosts,
                           explicit_key, explicit,
                           count,
                           daemons_key, daemons,
                           spec_section_key, spec_section):
    mk_spec, hosts = mk_spec_and_host(spec_section, hosts, explicit_key, explicit, count)
    dds = [
        DaemonDescription('mgr', d, d)
        for d in daemons
    ]
    run_scheduler_test(
        results=test_scheduler_daemons_results,
        mk_spec=mk_spec,
        hosts=hosts,
        get_daemons_func=lambda _: dds,
        key_elems=(host_key, explicit_key, count, daemons_key, spec_section_key)
    )


# =========================


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
            'mgr',
            PlacementSpec(hosts=['smithi060:[v2:172.21.15.60:3301,v1:172.21.15.60:6790]=c']),
            ['smithi060'],
            [],
            ['smithi060']
        ),
        # all_hosts
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(host_pattern='*'),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mgr', 'b', 'host2'),
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
            'mgr',
            PlacementSpec(count=3, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mgr', 'b', 'host2'),
            ],
            ['host3']
        ),
        # count 1 + partial host list
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=1, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mgr', 'b', 'host2'),
            ],
            ['host3']
        ),
        # count + partial host list + existing
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
            ],
            ['host3']
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
            ],
            ['host1']
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mgr', 'a', 'host2'),
            ],
            ['host1']
        ),
        # label only
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3']
        ),
        # host_pattern
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(host_pattern='mgr*'),
            'mgrhost1 mgrhost2 datahost'.split(),
            [],
            ['mgrhost1', 'mgrhost2']
        ),
    ])
def test_node_assignment(service_type, placement, hosts, daemons, expected):
    service_id = None
    if service_type == 'rgw':
        service_id = 'realm.zone'

    spec = ServiceSpec(service_type=service_type,
                       service_id=service_id,
                       placement=placement)

    hosts = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h, labels=['foo']) for h in hosts],
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
        # just count
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=1),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host1', 'host2', 'host3'],
        ),

        # hosts + (smaller) count
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=1, hosts='host1 host2'.split()),
            'host1 host2'.split(),
            [],
            1,
            ['host1', 'host2'],
        ),
        # hosts + (smaller) count, existing
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=1, hosts='host1 host2 host3'.split()),
            'host1 host2 host3'.split(),
            [DaemonDescription('mgr', 'mgr.a', 'host1'),],
            1,
            ['host1', 'host2', 'host3'],
        ),
        # hosts + (smaller) count, (more) existing
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=1, hosts='host1 host2 host3'.split()),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mgr', 'b', 'host2'),
            ],
            1,
            ['host1', 'host2']
        ),
        # count + partial host list
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host1', 'host2', 'host3']
        ),
        # label + count
        NodeAssignmentTest2(
            'mgr',
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
        hosts=[HostSpec(h, labels=['foo']) for h in hosts],
        get_daemons_func=lambda _: daemons).place()
    assert len(hosts) == expected_len
    for h in [h.hostname for h in hosts]:
        assert h in in_set

@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,must_have",
    [
        # hosts + (smaller) count, (more) existing
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=3, hosts='host3'.split()),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host3']
        ),
        # count + partial host list
        NodeAssignmentTest2(
            'mgr',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [],
            1,
            ['host3']
        ),
    ])
def test_node_assignment3(service_type, placement, hosts,
                          daemons, expected_len, must_have):
    hosts = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        hosts=[HostSpec(h) for h in hosts],
        get_daemons_func=lambda _: daemons).place()
    assert len(hosts) == expected_len
    for h in must_have:
        assert h in [h.hostname for h in hosts]


@pytest.mark.parametrize("placement",
    [
        ('1 *'),
        ('* label:foo'),
        ('* host1 host2'),
        ('hostname12hostname12hostname12hostname12hostname12hostname12hostname12'),  # > 63 chars
    ])
def test_bad_placements(placement):
    try:
        s = PlacementSpec.from_string(placement.split(' '))
        assert False
    except ServiceSpecValidationError as e:
        pass


class NodeAssignmentTestBadSpec(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: str
@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected",
    [
        # unknown host
        NodeAssignmentTestBadSpec(
            'mgr',
            PlacementSpec(hosts=['unknownhost']),
            ['knownhost'],
            [],
            "Cannot place <ServiceSpec for service_name=mgr> on unknownhost: Unknown hosts"
        ),
        # unknown host pattern
        NodeAssignmentTestBadSpec(
            'mgr',
            PlacementSpec(host_pattern='unknownhost'),
            ['knownhost'],
            [],
            "Cannot place <ServiceSpec for service_name=mgr>: No matching hosts"
        ),
        # unknown label
        NodeAssignmentTestBadSpec(
            'mgr',
            PlacementSpec(label='unknownlabel'),
            [],
            [],
            "Cannot place <ServiceSpec for service_name=mgr>: No matching hosts for label unknownlabel"
        ),
    ])
def test_bad_specs(service_type, placement, hosts, daemons, expected):
    with pytest.raises(OrchestratorValidationError) as e:
        hosts = HostAssignment(
            spec=ServiceSpec(service_type, placement=placement),
            hosts=[HostSpec(h) for h in hosts],
            get_daemons_func=lambda _: daemons).place()
    assert str(e.value) == expected

class ActiveAssignmentTest(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: List[List[str]]


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected",
                         [
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=2),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3'),
                                 ],
                                 [['host1', 'host2'], ['host1', 'host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=2),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host1', 'host3'], ['host2', 'host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2', is_active=True),
                                     DaemonDescription('mgr', 'c', 'host3'),
                                 ],
                                 [['host2']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host1'], ['host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=2),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2', is_active=True),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host2', 'host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'b', 'host2', is_active=True),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host1'], ['host2'], ['host3']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'a2', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3'),
                                 ],
                                 [['host1']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'a2', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3'),
                                 ],
                                 [['host1']]
                             ),
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=2),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1', is_active=True),
                                     DaemonDescription('mgr', 'a2', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host1', 'host3']]
                             ),
                             # Explicit placement should override preference for active daemon
                             ActiveAssignmentTest(
                                 'mgr',
                                 PlacementSpec(count=1, hosts=['host1']),
                                 'host1 host2 host3'.split(),
                                 [
                                     DaemonDescription('mgr', 'a', 'host1'),
                                     DaemonDescription('mgr', 'b', 'host2'),
                                     DaemonDescription('mgr', 'c', 'host3', is_active=True),
                                 ],
                                 [['host1']]
                             ),

                         ])
def test_active_assignment(service_type, placement, hosts, daemons, expected):

    spec = ServiceSpec(service_type=service_type,
                       service_id=None,
                       placement=placement)

    hosts = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h) for h in hosts],
        get_daemons_func=lambda _: daemons).place()
    assert sorted([h.hostname for h in hosts]) in expected

class OddMonsTest(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected_count: int


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_count",
                         [
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=5),
                                 'host1 host2 host3 host4'.split(),
                                 [],
                                 3
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=4),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 3
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=5),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 5
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(hosts='host1 host2 host3 host4'.split()),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 3
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(hosts='host1 host2 host3 host4 host5'.split()),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 5
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(host_pattern='*'),
                                 'host1 host2 host3 host4'.split(),
                                 [],
                                 3
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=5, hosts='host1 host2 host3 host4'.split()),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 3
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=2, hosts='host1 host2 host3'.split()),
                                 'host1 host2 host3 host4 host5'.split(),
                                 [],
                                 1
                             ),
                             OddMonsTest(
                                 'mon',
                                 PlacementSpec(count=5),
                                 'host1 host2 host3 host4'.split(),
                                 [
                                     DaemonDescription('mon', 'a', 'host1'),
                                     DaemonDescription('mon', 'b', 'host2'),
                                     DaemonDescription('mon', 'c', 'host3'),
                                 ],
                                 3
                             ),
                            OddMonsTest(
                                'mon',
                                PlacementSpec(count=5),
                                'host1 host2 host3 host4'.split(),
                                [
                                   DaemonDescription('mon', 'a', 'host1'),
                                   DaemonDescription('mon', 'b', 'host2'),
                                ],
                                3
                            ),
                            OddMonsTest(
                                'mon',
                                PlacementSpec(hosts='host1 host2 host3 host4'.split()),
                                'host1 host2 host3 host4 host5'.split(),
                                [
                                   DaemonDescription('mon', 'a', 'host1'),
                                   DaemonDescription('mon', 'b', 'host2'),
                                   DaemonDescription('mon', 'c', 'host3'),
                                ],
                                3
                            ),

                         ])
def test_odd_mons(service_type, placement, hosts, daemons, expected_count):

    spec = ServiceSpec(service_type=service_type,
                       service_id=None,
                       placement=placement)

    hosts = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h) for h in hosts],
        get_daemons_func=lambda _: daemons).place()
    assert len(hosts) == expected_count
