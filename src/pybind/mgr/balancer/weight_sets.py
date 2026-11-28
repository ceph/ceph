"""
Per-pool CRUSH weight-sets that correct the multi-pick anomaly.

When a rule picks several items from a bucket, CRUSH gives low-weight items
more than their share (https://tracker.ceph.com/issues/15653). The weights to
draw with instead follow from the weights of the items the rule picks between
(its failure domains) and the number of picks; the solver is in
src/crush/CrushWeightSolver.h. This module works out, from a CRUSH dump, what
the weight-set of a pool should be and how it differs from the one it has.

A weight-set is set per device (``osd crush weight-set reweight``); the
weights of buckets are the sums of what is below them. So, to give a failure
domain a weight that is ``f`` times its CRUSH weight at one position, every
device below it gets ``f`` times its CRUSH weight at that position.
"""

from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

FLAT = 'flat'
POSITIONAL = 'positional'

# weight-set weights are 16.16 fixed point, and are dumped as floats
WEIGHT_TOLERANCE = 2.0 / 0x10000

FlatSolver = Callable[[List[float], int], Optional[List[float]]]
PositionalSolver = Callable[[List[float], int], Optional[List[List[float]]]]


class Domain(NamedTuple):
    """An item a rule picks: a bucket of the failure domain type, or a device"""
    id: int
    weight: float
    osds: Dict[int, float]     # devices below it and their CRUSH weights


class PoolWeightSet(NamedTuple):
    mode: str                          # FLAT or POSITIONAL
    positions: int                     # weights per device
    weights: Dict[int, List[float]]    # osd -> weight at each position


class Change(NamedTuple):
    """What to do to give a pool its weight-set"""
    remove: bool                       # it has one of the other mode
    create: bool
    mode: str
    weights: Dict[int, List[float]]    # only the devices that change

    def commands(self, pool_name: str) -> List[Dict[str, Any]]:
        cmds: List[Dict[str, Any]] = []
        if self.remove:
            cmds.append({'prefix': 'osd crush weight-set rm',
                         'pool': pool_name})
        if self.create:
            cmds.append({'prefix': 'osd crush weight-set create',
                         'pool': pool_name, 'mode': self.mode})
        for osd, weights in sorted(self.weights.items()):
            cmds.append({'prefix': 'osd crush weight-set reweight',
                         'pool': pool_name, 'item': 'osd.%d' % osd,
                         'weight': weights})
        return cmds

    def show(self, pool_name: str) -> List[str]:
        lines = []
        for cmd in self.commands(pool_name):
            words = ['ceph', cmd['prefix'], pool_name]
            if 'mode' in cmd:
                words.append(cmd['mode'])
            if 'item' in cmd:
                words.append(cmd['item'])
                words += ['%f' % w for w in cmd['weight']]
            lines.append(' '.join(words))
        return lines


def rule_picks(rule: Dict[str, Any], pool_size: int) -> Optional[Tuple[int, str, str, int]]:
    """
    (take item, 'firstn' or 'indep', failure domain type, picks) for a rule
    of the form take, choose[leaf] ... type T, emit. None for other rules:
    with several choose steps the picks of one bucket are not one sample.
    """
    steps = [s for s in rule.get('steps', []) if not s['op'].startswith('set_')]
    if len(steps) != 3 or steps[0]['op'] != 'take' or steps[2]['op'] != 'emit':
        return None
    choose = steps[1]
    if choose['op'] not in ('choose_firstn', 'chooseleaf_firstn',
                            'choose_indep', 'chooseleaf_indep'):
        return None
    picks = choose['num'] if choose['num'] > 0 else pool_size + choose['num']
    picks = min(picks, pool_size)
    if picks < 1:
        return None
    return (steps[0]['item'], choose['op'].split('_')[1], choose['type'], picks)


def _buckets(crush_dump: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    return {b['id']: b for b in crush_dump.get('buckets', [])}


def _osds_below(buckets: Dict[int, Dict[str, Any]], item: int,
                weight: float) -> Dict[int, float]:
    if item >= 0:
        return {item: weight}
    osds: Dict[int, float] = {}
    for child in buckets[item]['items']:
        osds.update(_osds_below(buckets, child['id'],
                                child['weight'] / 0x10000))
    return osds


def failure_domains(crush_dump: Dict[str, Any], take: int,
                    domain_type: str) -> Optional[List[Domain]]:
    """
    The items of type domain_type below the take item, in tree order. None
    if a device is found above that type (the rule cannot map there).
    """
    buckets = _buckets(crush_dump)
    types = {t['type_id']: t['name'] for t in crush_dump.get('types', [])}
    device_type = types.get(0, 'osd')
    if take not in buckets:
        return None
    domains: List[Domain] = []

    def walk(bucket_id: int) -> bool:
        for child in buckets[bucket_id]['items']:
            cid = child['id']
            weight = child['weight'] / 0x10000
            if cid >= 0:
                if domain_type != device_type:
                    return False
                domains.append(Domain(cid, weight, {cid: weight}))
            elif buckets[cid]['type_name'] == domain_type:
                domains.append(Domain(cid, weight,
                                      _osds_below(buckets, cid, weight)))
            elif not walk(cid):
                return False
        return True

    if not walk(take):
        return None
    return domains


def desired(crush_dump: Dict[str, Any],
            pool: Dict[str, Any],
            solve_flat: FlatSolver,
            solve_positional: PositionalSolver,
            allow_positional: bool = True) -> Tuple[Optional[PoolWeightSet], str]:
    """
    The weight-set a pool should have, and why if it should have none.

    :param pool: the pool's entry in the osdmap dump
    """
    rules = {r['rule_id']: r for r in crush_dump.get('rules', [])}
    rule = rules.get(pool['crush_rule'])
    if rule is None:
        return None, 'no rule %d' % pool['crush_rule']
    shape = rule_picks(rule, pool['size'])
    if shape is None:
        return None, 'rule %s is not take, choose, emit' % rule['rule_name']
    take, how, domain_type, picks = shape
    if picks < 2:
        return None, 'one pick'
    domains = failure_domains(crush_dump, take, domain_type)
    if not domains:
        return None, 'no items of type %s below the take item' % domain_type
    weights = [d.weight for d in domains]
    if max(weights) - min(weights) <= WEIGHT_TOLERANCE:
        return None, 'equal weights'

    # device classes: positional weight-sets do not survive in shadow buckets,
    # https://tracker.ceph.com/issues/82900
    shadow = '~' in _buckets(crush_dump)[take]['name']
    solved: Optional[List[List[float]]] = None
    mode = FLAT
    if how == 'firstn' and allow_positional and not shadow:
        solved = solve_positional(weights, picks)
        if solved is not None:
            mode = POSITIONAL
    if solved is None:
        flat = solve_flat(weights, picks)
        if flat is None:
            return None, ('no solution (an item of type %s has more than 1/%d '
                          'of the weight), or too many items of different '
                          'weights to solve' % (domain_type, picks))
        solved = [flat]

    positions = 1
    if mode == POSITIONAL:
        # a positional weight-set has one position per replica of the pool
        positions = pool['size']
        solved += [solved[-1]] * (positions - len(solved))

    osd_weights: Dict[int, List[float]] = {}
    for i, domain in enumerate(domains):
        for osd, crush_weight in domain.osds.items():
            if osd in osd_weights:
                return None, 'osd.%d is below the take item twice' % osd
            osd_weights[osd] = [
                crush_weight * position[i] / domain.weight
                if domain.weight > 0 else 0.0
                for position in solved]
    return PoolWeightSet(mode, positions, osd_weights), ''


def current(crush_dump: Dict[str, Any],
            pool_id: int) -> Optional[Dict[int, List[float]]]:
    """The weights of each device in the weight-set of a pool, if it has one"""
    args = crush_dump.get('choose_args', {}).get(str(pool_id))
    if args is None:
        return None
    buckets = _buckets(crush_dump)
    weights: Dict[int, List[float]] = {}
    for arg in args:
        bucket = buckets.get(arg['bucket_id'])
        weight_set = arg.get('weight_set')
        if bucket is None or not weight_set:
            continue
        for pos, item in enumerate(bucket['items']):
            if item['id'] >= 0 and item['id'] not in weights:
                weights[item['id']] = [position[pos] for position in weight_set]
    return weights


def change(crush_dump: Dict[str, Any], pool_id: int,
           want: PoolWeightSet) -> Optional[Change]:
    """What has to change so that the pool has this weight-set; None if nothing"""
    have = current(crush_dump, pool_id)
    if have is not None:
        have_positions = max((len(w) for w in have.values()), default=0)
        if have_positions != want.positions:
            # a new weight-set starts from the CRUSH weights
            return Change(True, True, want.mode, dict(want.weights))
    changed = {}
    for osd, weights in want.weights.items():
        old = have.get(osd) if have is not None else None
        if old is None or any(abs(a - b) > WEIGHT_TOLERANCE
                              for a, b in zip(old, weights)):
            changed[osd] = weights
    if have is not None and not changed:
        return None
    return Change(False, have is None, want.mode, changed)
