"""
Put the pool of a zone thrash job on a hand-written CRUSH rule over the zones
of crush_map_config, as an operator's existing rule would be, and check at the
end that the pool kept its rule and that no rule went. With the num_zones
flip thrasher, the pool may instead end on a rule generated with the same
placement, and the rule written here may go.
"""
import contextlib
import json
import logging

from teuthology import misc as teuthology
from tasks.stretch_num_zones_flip import NumZonesFlipper

log = logging.getLogger(__name__)

POOL = 'rulepool'


def _rule_text(name, kind, ec, zones, zone_type, osd_type, per_zone,
               device_class, rule_id):
    mode = 'indep' if ec else 'firstn'
    # chooseleaf of type osd can return OSDs that are out
    leaf = 'choose' if osd_type == 'osd' else 'chooseleaf'
    lines = [f'rule {name} {{', f'        id {rule_id}',
             f'        type {"erasure" if ec else "replicated"}']
    if ec:
        lines += ['        step set_chooseleaf_tries 5',
                  '        step set_choose_tries 100']
    if kind == 'take':
        for zone in zones:
            lines += [f'        step take {zone}',
                      f'        step {leaf} {mode} {per_zone} type {osd_type}',
                      '        step emit']
    else:
        take = 'default' if kind == 'choose' else f'default class {device_class}'
        lines += [f'        step take {take}',
                  f'        step choose {mode} {len(zones)} type {zone_type}',
                  f'        step {leaf} {mode} {per_zone} type {osd_type}',
                  '        step emit']
    return '\n'.join(lines + ['}', ''])


def _check_placement(manager, rule, root, zones, zone_type, osd_type):
    dump = json.loads(manager.raw_cluster_cmd(
        'osd', 'crush', 'rule', 'dump', rule, '--format=json'))
    steps = dump['steps']
    log.info('pool rule %s: %s', rule, steps)
    takes = [s['item_name'] for s in steps if s['op'] == 'take']
    chooses = [(s['num'], s['type']) for s in steps
               if s['op'].startswith('choose')]
    assert takes == [root], (rule, steps)
    assert chooses[0] == (len(zones), zone_type), (rule, steps)
    assert chooses[-1][1] == osd_type, (rule, steps)


@contextlib.contextmanager
def task(ctx, config):
    """
    Create the pool rulepool over the zones of
    overrides.ceph.pool-config.crush_map_config on a rule written here, and
    give it to the rados task through overrides.rados.pools::

        tasks:
        - stretch_rules:
            rule: take      # take each zone; choose: choose them;
                            # class: choose them in the class of osd.0
            enable_stretch_mode: false  # true: enable global stretch
                                        # mode first, with the rule for a
                                        # replicated pool

    The pool is erasure coded with pool-config's erasure_code_profile when
    pool-config's ec_pool is true, and replicated otherwise, with the
    replicas per zone of the pool creation defaults.
    """
    config = config or {}
    kind = config.get('rule', 'take')
    assert kind in ('take', 'choose', 'class'), kind
    manager = ctx.managers['ceph']
    pool_config = ctx.config.get('overrides', {}).get('ceph', {}).get(
        'pool-config', {})
    crush = pool_config['crush_map_config']
    zones = [z['name'] for z in crush['zones']]
    zone_type = crush.get('zone_type', 'datacenter')
    osd_type = crush.get('failure_domain', 'osd')
    ec = pool_config.get('ec_pool', False)
    name = f'{kind}_rule'

    if ec:
        # the profile as the rados task sets it again, name included
        profile = pool_config['erasure_code_profile']
        profile_name = profile.get('name', 'teuthologyprofile')
        manager.create_erasure_code_profile(profile_name, profile)
        per_zone = int(profile['k']) + int(profile['m'])
    elif config.get('enable_stretch_mode', False):
        # global stretch mode gives every replicated pool 2 per zone
        per_zone = 2
    else:
        defaults = json.loads(manager.raw_cluster_cmd(
            'osd', 'pool', 'default', 'get', '--format=json'))
        per_zone = int(defaults['replica']['value'])
    device_class = manager.raw_cluster_cmd(
        'osd', 'crush', 'get-device-class', 'osd.0').strip()
    dump = json.loads(manager.raw_cluster_cmd('osd', 'crush', 'dump',
                                              '--format=json'))
    rule_id = max(r['rule_id'] for r in dump['rules']) + 1
    text = _rule_text(name, kind, ec, zones, zone_type, osd_type, per_zone,
                      device_class, rule_id)
    log.info('adding rule %s:\n%s', name, text)
    manager.controller.sh(
        'tmp=$(mktemp -d) && trap "rm -rf $tmp" EXIT && '
        'sudo ceph osd getcrushmap -o $tmp/cm && '
        'crushtool --decompile $tmp/cm -o $tmp/cm.txt && '
        f"printf '%s' '{text}' >> $tmp/cm.txt && "
        'crushtool --compile $tmp/cm.txt -o $tmp/cm.bin && '
        'sudo ceph osd setcrushmap -i $tmp/cm.bin')

    if config.get('enable_stretch_mode', False):
        # a replicated rule is global stretch mode's rule for every pool
        rule_arg = [] if ec else ['--new_crush_rule', name]
        manager.raw_cluster_cmd('mon', 'enable_stretch_mode', *rule_arg,
                                '--dividing_bucket', zone_type)
    if ec:
        manager.raw_cluster_cmd('osd', 'pool', 'create', POOL, '16', '16',
                                'erasure', profile_name, name,
                                '--num_zones', str(len(zones)))
    else:
        # a named rule takes no CRUSH option: the zone type is the rule's
        manager.raw_cluster_cmd('osd', 'pool', 'create', POOL, '16', '16',
                                'replicated', name,
                                '--num_zones', str(len(zones)),
                                '--replica', str(per_zone))
    if 'min_size' in pool_config:
        manager.raw_cluster_cmd('osd', 'pool', 'set', POOL, 'min_size',
                                str(pool_config['min_size']))
    manager.raw_cluster_cmd('osd', 'pool', 'application', 'enable', POOL,
                            'rados')
    manager.pools[POOL] = manager.get_pool_int_property(POOL, 'pg_num')
    teuthology.deep_merge(ctx.config.setdefault('overrides', {}),
                          {'rados': {'pools': [POOL]}})
    rules = set(manager.raw_cluster_cmd('osd', 'crush', 'rule', 'ls').split())
    try:
        yield
    finally:
        now = set(manager.raw_cluster_cmd('osd', 'crush', 'rule',
                                          'ls').split())
        rule = json.loads(manager.raw_cluster_cmd(
            'osd', 'pool', 'get', POOL, 'crush_rule', '--format=json'))
        gone = rules - now
        if rule['crush_rule'] != name:
            # a num_zones change gives the pool a rule of the same placement
            # and removes the one it replaces, even if written by hand
            assert any(isinstance(t, NumZonesFlipper)
                       for t in ctx.ceph['ceph'].thrashers), rule
            root = f'default~{device_class}' if kind == 'class' else 'default'
            _check_placement(manager, rule['crush_rule'], root, zones,
                             zone_type, osd_type)
            gone -= {name}
        assert not gone, f'rules removed: {sorted(gone)}'
        log.info('rules added during the job: %s', sorted(now - rules))
