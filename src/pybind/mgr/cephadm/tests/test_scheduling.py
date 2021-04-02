# Disable autopep8 for this file:

# fmt: off

from typing import NamedTuple, List, Dict
import pytest

from ceph.deployment.hostspec import HostSpec
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, ServiceSpecValidationError

from cephadm.module import HostAssignment
from cephadm.schedule import DaemonPlacement
from orchestrator import DaemonDescription, OrchestratorValidationError, OrchestratorError


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


def _always_true(_):
    pass


def k(s):
    return [e for e in s.split(' ') if e]


def get_result(key, results):
    def match(one):
        for o, k in zip(one, key):
            if o != k and o != '*':
                return False
        return True
    return [v for k, v in results if match(k)][0]


def mk_spec_and_host(spec_section, hosts, explicit_key, explicit, count):

    if spec_section == 'hosts':
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(  # noqa: E731
            hosts=explicit,
            count=count,
        ))
    elif spec_section == 'label':
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(  # noqa: E731
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
        mk_spec = lambda: ServiceSpec('mgr', placement=PlacementSpec(  # noqa: E731
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


def run_scheduler_test(results, mk_spec, hosts, daemons, key_elems):
    key = ' '.join('N' if e is None else str(e) for e in key_elems)
    try:
        assert_res = get_result(k(key), results)
    except IndexError:
        try:
            spec = mk_spec()
            host_res, to_add, to_remove = HostAssignment(
                spec=spec,
                hosts=hosts,
                daemons=daemons,
            ).place()
            if isinstance(host_res, list):
                e = ', '.join(repr(h.hostname) for h in host_res)
                assert False, f'`(k("{key}"), exactly({e})),` not found'
            assert False, f'`(k("{key}"), ...),` not found'
        except OrchestratorError as e:
            assert False, f'`(k("{key}"), error({type(e).__name__}, {repr(str(e))})),` not found'

    for _ in range(10):  # scheduler has a random component
        try:
            spec = mk_spec()
            host_res, to_add, to_remove = HostAssignment(
                spec=spec,
                hosts=hosts,
                daemons=daemons
            ).place()

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


@pytest.mark.parametrize(
    'dp,dd,result',
    [
        (
            DaemonPlacement(hostname='host1'),
            DaemonDescription('mgr', 'a', 'host1'),
            True
        ),
        (
            DaemonPlacement(hostname='host1', name='a'),
            DaemonDescription('mgr', 'a', 'host1'),
            True
        ),
        (
            DaemonPlacement(hostname='host1', name='a'),
            DaemonDescription('mgr', 'b', 'host1'),
            False
        ),
    ])
def test_daemon_placement_match(dp, dd, result):
    assert dp.matches_daemon(dd) == result


@pytest.mark.parametrize("spec_section_key,spec_section",
    [   # noqa: E128
        ('h', 'hosts'),
        ('l', 'label'),
        ('p', 'host_pattern'),
    ])
@pytest.mark.parametrize("count",
    [   # noqa: E128
        None,
        0,
        1,
        2,
        3,
    ])
@pytest.mark.parametrize("explicit_key, explicit",
    [   # noqa: E128
        ('e', []),
        ('1', ['1']),
        ('12', ['1', '2']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("host_key, hosts",
    [   # noqa: E128
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
        daemons=[],
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
    [   # noqa: E128
        ('h', 'hosts'),
        ('l', 'label'),
        ('p', 'host_pattern'),
    ])
@pytest.mark.parametrize("daemons_key, daemons",
    [   # noqa: E128
        ('e', []),
        ('1', ['1']),
        ('3', ['3']),
        ('12', ['1', '2']),
        ('112', ['1', '1', '2']),  # deal with existing co-located daemons
        ('23', ['2', '3']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("count",
    [   # noqa: E128
        None,
        1,
        2,
        3,
    ])
@pytest.mark.parametrize("explicit_key, explicit",
    [   # noqa: E128
        ('1', ['1']),
        ('123', ['1', '2', '3']),
    ])
@pytest.mark.parametrize("host_key, hosts",
    [   # noqa: E128
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
        daemons=dds,
        key_elems=(host_key, explicit_key, count, daemons_key, spec_section_key)
    )


# =========================


class NodeAssignmentTest(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: List[str]
    expected_add: List[str]
    expected_remove: List[DaemonDescription]


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected,expected_add,expected_remove",
    [   # noqa: E128
        # just hosts
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(hosts=['smithi060']),
            ['smithi060'],
            [],
            ['smithi060'], ['smithi060'], []
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
            ['host1', 'host2', 'host3'], ['host3'], []
        ),
        # all_hosts + count_per_host
        NodeAssignmentTest(
            'mds',
            PlacementSpec(host_pattern='*', count_per_host=2),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mds', 'a', 'host1'),
                DaemonDescription('mds', 'b', 'host2'),
            ],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3'],
            ['host3', 'host1', 'host2', 'host3'],
            []
        ),
        # count that is bigger than the amount of hosts. Truncate to len(hosts)
        # mgr should not be co-located to each other.
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=4),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3'], ['host1', 'host2', 'host3'], []
        ),
        # count that is bigger than the amount of hosts; wrap around.
        NodeAssignmentTest(
            'mds',
            PlacementSpec(count=6),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3'],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3'],
            []
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
            ['host3'], ['host3'], ['mgr.a', 'mgr.b']
        ),
        # count + partial host list (with colo)
        NodeAssignmentTest(
            'mds',
            PlacementSpec(count=3, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mgr', 'b', 'host2'),
            ],
            ['host3', 'host3', 'host3'], ['host3', 'host3', 'host3'], ['mgr.a', 'mgr.b']
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
            ['host3'], ['host3'], ['mgr.a', 'mgr.b']
        ),
        # count + partial host list + existing
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host3']),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
            ],
            ['host3'], ['host3'], ['mgr.a']
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mgr', 'a', 'host1'),
            ],
            ['host1'], [], []
        ),
        # count + partial host list + existing (deterministic)
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=2, hosts=['host1']),
            'host1 host2'.split(),
            [
                DaemonDescription('mgr', 'a', 'host2'),
            ],
            ['host1'], ['host1'], ['mgr.a']
        ),
        # label only
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3'], ['host1', 'host2', 'host3'], []
        ),
        # label + count (truncate to host list)
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=4, label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3'], ['host1', 'host2', 'host3'], []
        ),
        # label + count (with colo)
        NodeAssignmentTest(
            'mds',
            PlacementSpec(count=6, label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3'],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3'],
            []
        ),
        # label only + count_per_hst
        NodeAssignmentTest(
            'mds',
            PlacementSpec(label='foo', count_per_host=3),
            'host1 host2 host3'.split(),
            [],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3',
             'host1', 'host2', 'host3'],
            ['host1', 'host2', 'host3', 'host1', 'host2', 'host3',
             'host1', 'host2', 'host3'],
            []
        ),
        # host_pattern
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(host_pattern='mgr*'),
            'mgrhost1 mgrhost2 datahost'.split(),
            [],
            ['mgrhost1', 'mgrhost2'], ['mgrhost1', 'mgrhost2'], []
        ),
        # host_pattern + count_per_host
        NodeAssignmentTest(
            'mds',
            PlacementSpec(host_pattern='mds*', count_per_host=3),
            'mdshost1 mdshost2 datahost'.split(),
            [],
            ['mdshost1', 'mdshost2', 'mdshost1', 'mdshost2', 'mdshost1', 'mdshost2'],
            ['mdshost1', 'mdshost2', 'mdshost1', 'mdshost2', 'mdshost1', 'mdshost2'],
            []
        ),
        # label + count_per_host + ports
        NodeAssignmentTest(
            'rgw',
            PlacementSpec(count=6, label='foo'),
            'host1 host2 host3'.split(),
            [],
            ['host1(*:80)', 'host2(*:80)', 'host3(*:80)',
             'host1(*:81)', 'host2(*:81)', 'host3(*:81)'],
            ['host1(*:80)', 'host2(*:80)', 'host3(*:80)',
             'host1(*:81)', 'host2(*:81)', 'host3(*:81)'],
            []
        ),
        # label + count_per_host + ports (+ xisting)
        NodeAssignmentTest(
            'rgw',
            PlacementSpec(count=6, label='foo'),
            'host1 host2 host3'.split(),
            [
                DaemonDescription('rgw', 'a', 'host1', ports=[81]),
                DaemonDescription('rgw', 'b', 'host2', ports=[80]),
                DaemonDescription('rgw', 'c', 'host1', ports=[82]),
            ],
            ['host1(*:80)', 'host2(*:80)', 'host3(*:80)',
             'host1(*:81)', 'host2(*:81)', 'host3(*:81)'],
            ['host1(*:80)', 'host3(*:80)',
             'host2(*:81)', 'host3(*:81)'],
            ['rgw.c']
        ),
        # cephadm.py teuth case
        NodeAssignmentTest(
            'mgr',
            PlacementSpec(count=3, hosts=['host1=y', 'host2=x']),
            'host1 host2'.split(),
            [
                DaemonDescription('mgr', 'y', 'host1'),
                DaemonDescription('mgr', 'x', 'host2'),
            ],
            ['host1(name=y)', 'host2(name=x)'],
            [], []
        ),
    ])
def test_node_assignment(service_type, placement, hosts, daemons,
                         expected, expected_add, expected_remove):
    service_id = None
    allow_colo = False
    if service_type == 'rgw':
        service_id = 'realm.zone'
        allow_colo = True
    elif service_type == 'mds':
        service_id = 'myfs'
        allow_colo = True

    spec = ServiceSpec(service_type=service_type,
                       service_id=service_id,
                       placement=placement)

    all_slots, to_add, to_remove = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h, labels=['foo']) for h in hosts],
        daemons=daemons,
        allow_colo=allow_colo,
    ).place()

    got = [str(p) for p in all_slots]
    num_wildcard = 0
    for i in expected:
        if i == '*':
            num_wildcard += 1
        else:
            assert i in got
            got.remove(i)
    assert num_wildcard == len(got)

    got = [str(p) for p in to_add]
    num_wildcard = 0
    for i in expected_add:
        if i == '*':
            num_wildcard += 1
        else:
            assert i in got
            got.remove(i)
    assert num_wildcard == len(got)

    assert sorted([d.name() for d in to_remove]) == sorted(expected_remove)


class NodeAssignmentTest2(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected_len: int
    in_set: List[str]


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,in_set",
    [   # noqa: E128
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
            [DaemonDescription('mgr', 'mgr.a', 'host1')],
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
    hosts, to_add, to_remove = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        hosts=[HostSpec(h, labels=['foo']) for h in hosts],
        daemons=daemons,
    ).place()
    assert len(hosts) == expected_len
    for h in [h.hostname for h in hosts]:
        assert h in in_set


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected_len,must_have",
    [   # noqa: E128
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
    hosts, to_add, to_remove = HostAssignment(
        spec=ServiceSpec(service_type, placement=placement),
        hosts=[HostSpec(h) for h in hosts],
        daemons=daemons,
    ).place()
    assert len(hosts) == expected_len
    for h in must_have:
        assert h in [h.hostname for h in hosts]


class NodeAssignmentTest4(NamedTuple):
    spec: ServiceSpec
    networks: Dict[str, Dict[str, List[str]]]
    daemons: List[DaemonDescription]
    expected: List[str]
    expected_add: List[str]
    expected_remove: List[DaemonDescription]


@pytest.mark.parametrize("spec,networks,daemons,expected,expected_add,expected_remove",
    [   # noqa: E128
        NodeAssignmentTest4(
            ServiceSpec(
                service_type='rgw',
                service_id='foo',
                placement=PlacementSpec(count=6, label='foo'),
                networks=['10.0.0.0/8'],
            ),
            {
                'host1': {'10.0.0.0/8': ['10.0.0.1']},
                'host2': {'10.0.0.0/8': ['10.0.0.2']},
                'host3': {'192.168.0.0/16': ['192.168.0.1']},
            },
            [],
            ['host1(10.0.0.1:80)', 'host2(10.0.0.2:80)',
             'host1(10.0.0.1:81)', 'host2(10.0.0.2:81)',
             'host1(10.0.0.1:82)', 'host2(10.0.0.2:82)'],
            ['host1(10.0.0.1:80)', 'host2(10.0.0.2:80)',
             'host1(10.0.0.1:81)', 'host2(10.0.0.2:81)',
             'host1(10.0.0.1:82)', 'host2(10.0.0.2:82)'],
            []
        ),
    ])
def test_node_assignment4(spec, networks, daemons,
                          expected, expected_add, expected_remove):
    all_slots, to_add, to_remove = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h, labels=['foo']) for h in networks.keys()],
        daemons=daemons,
        allow_colo=True,
        networks=networks,
    ).place()

    got = [str(p) for p in all_slots]
    num_wildcard = 0
    for i in expected:
        if i == '*':
            num_wildcard += 1
        else:
            assert i in got
            got.remove(i)
    assert num_wildcard == len(got)

    got = [str(p) for p in to_add]
    num_wildcard = 0
    for i in expected_add:
        if i == '*':
            num_wildcard += 1
        else:
            assert i in got
            got.remove(i)
    assert num_wildcard == len(got)

    assert sorted([d.name() for d in to_remove]) == sorted(expected_remove)


@pytest.mark.parametrize("placement",
    [   # noqa: E128
        ('1 *'),
        ('* label:foo'),
        ('* host1 host2'),
        ('hostname12hostname12hostname12hostname12hostname12hostname12hostname12'),  # > 63 chars
    ])
def test_bad_placements(placement):
    try:
        PlacementSpec.from_string(placement.split(' '))
        assert False
    except ServiceSpecValidationError:
        pass


class NodeAssignmentTestBadSpec(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: str


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected",
    [   # noqa: E128
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
        hosts, to_add, to_remove = HostAssignment(
            spec=ServiceSpec(service_type, placement=placement),
            hosts=[HostSpec(h) for h in hosts],
            daemons=daemons,
        ).place()
    assert str(e.value) == expected


class ActiveAssignmentTest(NamedTuple):
    service_type: str
    placement: PlacementSpec
    hosts: List[str]
    daemons: List[DaemonDescription]
    expected: List[List[str]]
    expected_add: List[List[str]]
    expected_remove: List[List[str]]


@pytest.mark.parametrize("service_type,placement,hosts,daemons,expected,expected_add,expected_remove",
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
                                 [['host1', 'host2'], ['host1', 'host3']],
                                 [[]],
                                 [['mgr.b'], ['mgr.c']]
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
                                 [['host1', 'host3'], ['host2', 'host3']],
                                 [[]],
                                 [['mgr.a'], ['mgr.b']]
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
                                 [['host2']],
                                 [[]],
                                 [['mgr.a', 'mgr.c']]
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
                                 [['host3']],
                                 [[]],
                                 [['mgr.a', 'mgr.b']]
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
                                 [['host1'], ['host3']],
                                 [[]],
                                 [['mgr.a', 'mgr.b'], ['mgr.b', 'mgr.c']]
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
                                 [['host2', 'host3']],
                                 [[]],
                                 [['mgr.a']]
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
                                 [['host1'], ['host2'], ['host3']],
                                 [[]],
                                 [['mgr.a', 'mgr.b'], ['mgr.b', 'mgr.c'], ['mgr.a', 'mgr.c']]
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
                                 [['host1']],
                                 [[]],
                                 [['mgr.a2', 'mgr.b', 'mgr.c']]
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
                                 [['host1']],
                                 [[]],
                                 [['mgr.a', 'mgr.b', 'mgr.c'], ['mgr.a2', 'mgr.b', 'mgr.c']]
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
                                 [['host1', 'host3']],
                                 [[]],
                                 [['mgr.a2', 'mgr.b']]
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
                                 [['host1']],
                                 [[]],
                                 [['mgr.b', 'mgr.c']]
                             ),

                         ])
def test_active_assignment(service_type, placement, hosts, daemons, expected, expected_add, expected_remove):

    spec = ServiceSpec(service_type=service_type,
                       service_id=None,
                       placement=placement)

    hosts, to_add, to_remove = HostAssignment(
        spec=spec,
        hosts=[HostSpec(h) for h in hosts],
        daemons=daemons,
    ).place()
    assert sorted([h.hostname for h in hosts]) in expected
    assert sorted([h.hostname for h in to_add]) in expected_add
    assert sorted([h.name() for h in to_remove]) in expected_remove
