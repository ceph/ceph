from typing import Any, Dict, List, Optional

from balancer import weight_sets


def crush_dump(host_osds: List[List[float]], rule_op: str = 'chooseleaf_firstn',
               rule_type: str = 'host', shadow: bool = False,
               choose_args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Hosts with the given OSD weights in two racks under one root"""
    buckets = []
    racks: List[List[Dict[str, int]]] = [[], []]
    osd = 0
    for h, osds in enumerate(host_osds):
        items = []
        for w in osds:
            items.append({'id': osd, 'weight': int(w * 0x10000), 'pos': len(items)})
            osd += 1
        buckets.append({'id': -10 - h, 'name': 'host%d' % h, 'type_id': 1,
                        'type_name': 'host', 'items': items})
        racks[h % 2].append({'id': -10 - h, 'weight': int(sum(osds) * 0x10000),
                             'pos': len(racks[h % 2])})
    root_items = []
    for r, items in enumerate(racks):
        buckets.append({'id': -2 - r, 'name': 'rack%d' % r, 'type_id': 3,
                        'type_name': 'rack', 'items': items})
        root_items.append({'id': -2 - r, 'pos': r,
                           'weight': sum(i['weight'] for i in items)})
    buckets.append({'id': -1, 'name': 'default~hdd' if shadow else 'default',
                    'type_id': 11, 'type_name': 'root', 'items': root_items})
    return {
        'types': [{'type_id': 0, 'name': 'osd'}, {'type_id': 1, 'name': 'host'},
                  {'type_id': 3, 'name': 'rack'}, {'type_id': 11, 'name': 'root'}],
        'buckets': buckets,
        'rules': [{'rule_id': 0, 'rule_name': 'r', 'steps': [
            {'op': 'set_chooseleaf_tries', 'num': 5},
            {'op': 'take', 'item': -1, 'item_name': 'default'},
            {'op': rule_op, 'num': 0, 'type': rule_type},
            {'op': 'emit'}]}],
        'choose_args': choose_args or {},
    }


POOL = {'pool': 7, 'pool_name': 'p', 'size': 3, 'crush_rule': 0}


def fake_flat(weights: List[float], picks: int) -> Optional[List[float]]:
    # halve the first item, whatever the truth is
    return [w / 2 if i == 0 else w for i, w in enumerate(weights)]


def fake_positional(weights: List[float], picks: int) -> Optional[List[List[float]]]:
    return [[w * (1 + k) if i == 0 else w for i, w in enumerate(weights)]
            for k in range(picks)]


def no_solution(weights: List[float], picks: int) -> None:
    return None


def test_rule_picks() -> None:
    rule = crush_dump([[1]])['rules'][0]
    assert weight_sets.rule_picks(rule, 3) == (-1, 'firstn', 'host', 3)
    rule['steps'][2] = {'op': 'chooseleaf_indep', 'num': 4, 'type': 'rack'}
    assert weight_sets.rule_picks(rule, 6) == (-1, 'indep', 'rack', 4)
    rule['steps'][2]['num'] = -1
    assert weight_sets.rule_picks(rule, 6) == (-1, 'indep', 'rack', 5)
    # two choose steps: not handled
    rule['steps'].insert(2, {'op': 'choose_firstn', 'num': 2, 'type': 'rack'})
    assert weight_sets.rule_picks(rule, 6) is None


def test_failure_domains() -> None:
    dump = crush_dump([[1, 1], [0.5], [2]])
    hosts = weight_sets.failure_domains(dump, -1, 'host')
    assert hosts is not None
    # tree order: rack0 (host0, host2), rack1 (host1)
    assert [(d.id, d.weight, d.osds) for d in hosts] == [
        (-10, 2.0, {0: 1.0, 1: 1.0}), (-12, 2.0, {3: 2.0}), (-11, 0.5, {2: 0.5})]
    racks = weight_sets.failure_domains(dump, -1, 'rack')
    assert racks is not None
    assert [(d.id, d.weight, sorted(d.osds)) for d in racks] == [
        (-2, 4.0, [0, 1, 3]), (-3, 0.5, [2])]
    osds = weight_sets.failure_domains(dump, -1, 'osd')
    assert osds is not None
    assert [d.id for d in osds] == [0, 1, 3, 2]
    # no such type between the root and the devices
    assert weight_sets.failure_domains(dump, -1, 'datacenter') is None


def test_desired_positional() -> None:
    dump = crush_dump([[1, 1], [0.5], [2], [1]])
    want, why = weight_sets.desired(dump, POOL, fake_flat, fake_positional)
    assert want is not None, why
    assert want.mode == weight_sets.POSITIONAL
    assert want.positions == 3
    # host0 is the first domain: its devices are scaled, the others are not
    assert want.weights[0] == [1.0, 2.0, 3.0]
    assert want.weights[1] == [1.0, 2.0, 3.0]
    assert want.weights[2] == [0.5, 0.5, 0.5]
    assert want.weights[3] == [2.0, 2.0, 2.0]


def test_desired_flat() -> None:
    hosts = [[1, 1], [0.5], [2], [1]]
    # erasure coded: flat
    want, _ = weight_sets.desired(crush_dump(hosts, 'chooseleaf_indep'),
                                  dict(POOL, size=4), fake_flat, fake_positional)
    assert want is not None
    assert (want.mode, want.positions) == (weight_sets.FLAT, 1)
    assert want.weights[0] == [0.5] and want.weights[3] == [2.0]
    # a device class rule, positional turned off, no positional solution
    for dump, positional, solver in (
            (crush_dump(hosts, shadow=True), True, fake_positional),
            (crush_dump(hosts), False, fake_positional),
            (crush_dump(hosts), True, no_solution)):
        want, _ = weight_sets.desired(dump, POOL, fake_flat, solver,
                                      allow_positional=positional)
        assert want is not None and want.mode == weight_sets.FLAT


def test_desired_none() -> None:
    hosts = [[1, 1], [0.5], [2], [1]]
    for dump, pool, flat in (
            (crush_dump([[1], [1], [1], [1]]), POOL, fake_flat),   # equal weights
            (crush_dump(hosts), dict(POOL, size=1), fake_flat),   # one pick
            (crush_dump(hosts), dict(POOL, crush_rule=5), fake_flat),
            (crush_dump(hosts, rule_type='datacenter'), POOL, fake_flat),
            (crush_dump(hosts, 'chooseleaf_indep'), POOL, no_solution)):
        want, why = weight_sets.desired(dump, pool, flat, fake_positional)
        assert want is None and why


def test_change() -> None:
    hosts = [[1, 1], [0.5], [2], [1]]
    want, _ = weight_sets.desired(crush_dump(hosts), POOL, fake_flat, fake_positional)
    assert want is not None

    # no weight-set yet
    change = weight_sets.change(crush_dump(hosts), 7, want)
    assert change is not None
    assert (change.remove, change.create, change.mode) == (False, True, 'positional')
    assert sorted(change.weights) == [0, 1, 2, 3, 4]
    cmds = change.commands('p')
    assert cmds[0] == {'prefix': 'osd crush weight-set create', 'pool': 'p',
                       'mode': 'positional'}
    assert cmds[1] == {'prefix': 'osd crush weight-set reweight', 'pool': 'p',
                       'item': 'osd.0', 'weight': [1.0, 2.0, 3.0]}
    assert change.show('p')[1] == \
        'ceph osd crush weight-set reweight p osd.0 1.000000 2.000000 3.000000'

    def args(host0: List[List[float]], positions: int = 3) -> Dict[str, Any]:
        return {'7': [
            {'bucket_id': -10, 'weight_set': host0},
            {'bucket_id': -11, 'weight_set': [[0.5]] * positions},
            {'bucket_id': -12, 'weight_set': [[2.0]] * positions},
            {'bucket_id': -13, 'weight_set': [[1.0]] * positions},
            {'bucket_id': -1}]}

    # up to date, within the precision of a weight
    current = args([[1.0, 1.0], [2.0, 2.00001], [3.0, 3.0]])
    assert weight_sets.change(crush_dump(hosts, choose_args=current), 7, want) is None

    # one device differs
    current = args([[1.0, 1.0], [2.0, 1.5], [3.0, 3.0]])
    change = weight_sets.change(crush_dump(hosts, choose_args=current), 7, want)
    assert change is not None
    assert (change.remove, change.create) == (False, False)
    assert change.weights == {1: [1.0, 2.0, 3.0]}

    # a flat weight-set where a positional one is wanted: start again
    current = args([[1.0, 1.0]], positions=1)
    change = weight_sets.change(crush_dump(hosts, choose_args=current), 7, want)
    assert change is not None
    assert (change.remove, change.create) == (True, True)
    assert len(change.weights) == 5
