"""
Automatically scale pg_num based on how much data is stored in each pool.
"""

import errno
import json
import mgr_util
import threading
import uuid
from six import itervalues, iteritems
from collections import defaultdict
from prettytable import PrettyTable

from mgr_module import MgrModule

"""
Some terminology is made up for the purposes of this module:

 - "raw pgs": pg count after applying replication, i.e. the real resource
              consumption of a pool.
 - "grow/shrink" - increase/decrease the pg_num in a pool
 - "crush subtree" - non-overlapping domains in crush hierarchy: used as
                     units of resource management.
"""

INTERVAL = 5

PG_NUM_MIN = 4  # unless specified on a per-pool basis

def nearest_power_of_two(n):
    v = int(n)

    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16

    # High bound power of two
    v += 1

    # Low bound power of tow
    x = v >> 1

    return x if (v - n) > (n - x) else v


class PgAutoscaler(MgrModule):
    """
    PG autoscaler.
    """
    COMMANDS = [
        {
            "cmd": "osd pool autoscale-status",
            "desc": "report on pool pg_num sizing recommendation and intent",
            "perm": "r"
        },
    ]

    NATIVE_OPTIONS = [
        'mon_target_pg_per_osd',
        'mon_max_pg_per_osd',
    ]

    MODULE_OPTIONS = [
        {
            'name': 'sleep_interval',
            'default': str(60),
        },
    ]

    def __init__(self, *args, **kwargs):
        super(PgAutoscaler, self).__init__(*args, **kwargs)
        self._shutdown = threading.Event()

        # So much of what we do peeks at the osdmap that it's easiest
        # to just keep a copy of the pythonized version.
        self._osd_map = None

    def config_notify(self):
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))


    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "osd pool autoscale-status":
            retval = self._command_autoscale_status(cmd)
        else:
            assert False  # ceph-mgr should never pass us unknown cmds
        return retval

    def _command_autoscale_status(self, cmd):
        osdmap = self.get_osdmap()
        pools = osdmap.get_pools_by_name()
        ps, root_map, pool_root = self._get_pool_status(osdmap, pools)

        if cmd.get('format') == 'json' or cmd.get('format') == 'json-pretty':
            return 0, json.dumps(ps, indent=2), ''
        else:
            table = PrettyTable(['POOL', 'SIZE', 'TARGET SIZE',
                                 'RATE', 'RAW CAPACITY',
                                 'RATIO', 'TARGET RATIO',
                                 'BIAS',
                                 'PG_NUM',
#                                 'IDEAL',
                                 'NEW PG_NUM', 'AUTOSCALE'],
                                border=False)
            table.align['POOL'] = 'l'
            table.align['SIZE'] = 'r'
            table.align['TARGET SIZE'] = 'r'
            table.align['RATE'] = 'r'
            table.align['RAW CAPACITY'] = 'r'
            table.align['RATIO'] = 'r'
            table.align['TARGET RATIO'] = 'r'
            table.align['BIAS'] = 'r'
            table.align['PG_NUM'] = 'r'
#            table.align['IDEAL'] = 'r'
            table.align['NEW PG_NUM'] = 'r'
            table.align['AUTOSCALE'] = 'l'
            for p in ps:
                if p['would_adjust']:
                    final = str(p['pg_num_final'])
                else:
                    final = ''
                if p['target_bytes'] > 0:
                    ts = mgr_util.format_bytes(p['target_bytes'], 6)
                else:
                    ts = ''
                if p['target_ratio'] > 0.0:
                    tr = '%.4f' % p['target_ratio']
                else:
                    tr = ''
                table.add_row([
                    p['pool_name'],
                    mgr_util.format_bytes(p['logical_used'], 6),
                    ts,
                    p['raw_used_rate'],
                    mgr_util.format_bytes(p['subtree_capacity'], 6),
                    '%.4f' % p['capacity_ratio'],
                    tr,
                    p['bias'],
                    p['pg_num_target'],
#                    p['pg_num_ideal'],
                    final,
                    p['pg_autoscale_mode'],
                ])
            return 0, table.get_string(), ''

    def serve(self):
        self.config_notify()
        while not self._shutdown.is_set():
            self._maybe_adjust()
            self._shutdown.wait(timeout=int(self.sleep_interval))

    def get_subtree_resource_status(self, osdmap, crush):
        """
        For each CRUSH subtree of interest (i.e. the roots under which
        we have pools), calculate the current resource usages and targets,
        such as how many PGs there are, vs. how many PGs we would
        like there to be.
        """
        result = {}
        pool_root = {}
        roots = []

        class CrushSubtreeResourceStatus(object):
            def __init__(self):
                self.root_ids = []
                self.osds = set()
                self.osd_count = None  # Number of OSDs
                self.pg_target = None  # Ideal full-capacity PG count?
                self.pg_current = 0  # How many PGs already?
                self.capacity = None  # Total capacity of OSDs in subtree
                self.pool_ids = []
                self.pool_names = []

        # identify subtrees (note that they may overlap!)
        for pool_id, pool in osdmap.get_pools().items():
            cr_name = crush.get_rule_by_id(pool['crush_rule'])['rule_name']
            root_id = int(crush.get_rule_root(cr_name))
            pool_root[pool_id] = root_id
            osds = set(crush.get_osds_under(root_id))

            # do we intersect an existing root?
            s = None
            for prev in itervalues(result):
                if osds & prev.osds:
                    s = prev
                    break
            if not s:
                s = CrushSubtreeResourceStatus()
                roots.append(s)
            result[root_id] = s
            s.root_ids.append(root_id)
            s.osds |= osds
            s.pool_ids.append(int(pool_id))
            s.pool_names.append(pool['pool_name'])
            s.pg_current += pool['pg_num_target'] * pool['size']


        # finish subtrees
        all_stats = self.get('osd_stats')
        for s in roots:
            s.osd_count = len(s.osds)
            s.pg_target = s.osd_count * int(self.mon_target_pg_per_osd)

            capacity = 0.0
            for osd_stats in all_stats['osd_stats']:
                if osd_stats['osd'] in s.osds:
                    # Intentionally do not apply the OSD's reweight to
                    # this, because we want to calculate PG counts based
                    # on the physical storage available, not how it is
                    # reweighted right now.
                    capacity += osd_stats['kb'] * 1024

            s.capacity = capacity

            self.log.debug('root_ids %s pools %s with %d osds, pg_target %d',
                           s.root_ids,
                           s.pool_ids,
                           s.osd_count,
                           s.pg_target)

        return result, pool_root


    def _get_pool_status(
            self,
            osdmap,
            pools,
            threshold=3.0,
    ):
        assert threshold >= 2.0

        crush_map = osdmap.get_crush()

        root_map, pool_root = self.get_subtree_resource_status(osdmap, crush_map)

        df = self.get('df')
        pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])

        ret = []

        # iterate over all pools to determine how they should be sized
        for pool_name, p in iteritems(pools):
            pool_id = p['pool']

            # FIXME: we assume there is only one take per pool, but that
            # may not be true.
            cr_name = crush_map.get_rule_by_id(p['crush_rule'])['rule_name']
            root_id = int(crush_map.get_rule_root(cr_name))
            pool_root[pool_name] = root_id

            capacity = root_map[root_id].capacity
            if capacity == 0:
                self.log.debug('skipping empty subtree %s', cr_name)
                continue

            raw_used_rate = osdmap.pool_raw_used_rate(pool_id)

            pool_logical_used = pool_stats[pool_id]['bytes_used']
            bias = p['options'].get('pg_autoscale_bias', 1.0)
            target_bytes = p['options'].get('target_size_bytes', 0)

            # What proportion of space are we using?
            actual_raw_used = pool_logical_used * raw_used_rate
            actual_capacity_ratio = float(actual_raw_used) / capacity

            pool_raw_used = max(pool_logical_used, target_bytes) * raw_used_rate
            capacity_ratio = float(pool_raw_used) / capacity

            target_ratio = p['options'].get('target_size_ratio', 0.0)
            final_ratio = max(capacity_ratio, target_ratio)

            # So what proportion of pg allowance should we be using?
            pool_pg_target = (final_ratio * root_map[root_id].pg_target) / raw_used_rate * bias

            final_pg_target = max(p['options'].get('pg_num_min', PG_NUM_MIN),
                                  nearest_power_of_two(pool_pg_target))

            self.log.info("Pool '{0}' root_id {1} using {2} of space, bias {3}, "
                          "pg target {4} quantized to {5} (current {6})".format(
                              p['pool_name'],
                              root_id,
                              final_ratio,
                              bias,
                              pool_pg_target,
                              final_pg_target,
                              p['pg_num_target']
                          ))

            adjust = False
            if (final_pg_target > p['pg_num_target'] * threshold or \
                final_pg_target <= p['pg_num_target'] / threshold) and \
                final_ratio >= 0.0 and \
                final_ratio <= 1.0:
                adjust = True

            ret.append({
                'pool_id': pool_id,
                'pool_name': p['pool_name'],
                'crush_root_id': root_id,
                'pg_autoscale_mode': p['pg_autoscale_mode'],
                'pg_num_target': p['pg_num_target'],
                'logical_used': pool_logical_used,
                'target_bytes': target_bytes,
                'raw_used_rate': raw_used_rate,
                'subtree_capacity': capacity,
                'actual_raw_used': actual_raw_used,
                'raw_used': pool_raw_used,
                'actual_capacity_ratio': actual_capacity_ratio,
                'capacity_ratio': capacity_ratio,
                'target_ratio': target_ratio,
                'pg_num_ideal': int(pool_pg_target),
                'pg_num_final': final_pg_target,
                'would_adjust': adjust,
                'bias': p.get('options', {}).get('pg_autoscale_bias', 1.0),
                });

        return (ret, root_map, pool_root)


    def _maybe_adjust(self):
        self.log.info('_maybe_adjust')
        osdmap = self.get_osdmap()
        pools = osdmap.get_pools_by_name()
        ps, root_map, pool_root = self._get_pool_status(osdmap, pools)

        # Anyone in 'warn', set the health message for them and then
        # drop them from consideration.
        too_few = []
        too_many = []
        health_checks = {}

        total_ratio = dict([(r, 0.0) for r in iter(root_map)])
        total_target_ratio = dict([(r, 0.0) for r in iter(root_map)])
        target_ratio_pools = dict([(r, []) for r in iter(root_map)])

        total_bytes = dict([(r, 0) for r in iter(root_map)])
        total_target_bytes = dict([(r, 0.0) for r in iter(root_map)])
        target_bytes_pools = dict([(r, []) for r in iter(root_map)])

        for p in ps:
            total_ratio[p['crush_root_id']] += max(p['actual_capacity_ratio'],
                                                   p['target_ratio'])
            if p['target_ratio'] > 0:
                total_target_ratio[p['crush_root_id']] += p['target_ratio']
                target_ratio_pools[p['crush_root_id']].append(p['pool_name'])
            total_bytes[p['crush_root_id']] += max(
                p['actual_raw_used'],
                p['target_bytes'] * p['raw_used_rate'])
            if p['target_bytes'] > 0:
                total_target_bytes[p['crush_root_id']] += p['target_bytes'] * p['raw_used_rate']
                target_bytes_pools[p['crush_root_id']].append(p['pool_name'])
            if not p['would_adjust']:
                continue
            if p['pg_autoscale_mode'] == 'warn':
                msg = 'Pool %s has %d placement groups, should have %d' % (
                    p['pool_name'],
                    p['pg_num_target'],
                    p['pg_num_final'])
                if p['pg_num_final'] > p['pg_num_target']:
                    too_few.append(msg)
                else:
                    too_many.append(msg)

            if p['pg_autoscale_mode'] == 'on':
                # Note that setting pg_num actually sets pg_num_target (see
                # OSDMonitor.cc)
                r = self.mon_command({
                    'prefix': 'osd pool set',
                    'pool': p['pool_name'],
                    'var': 'pg_num',
                    'val': str(p['pg_num_final'])
                })

                if r[0] != 0:
                    # FIXME: this is a serious and unexpected thing,
                    # we should expose it as a cluster log error once
                    # the hook for doing that from ceph-mgr modules is
                    # in.
                    self.log.error("pg_num adjustment on {0} to {1} failed: {2}"
                                   .format(p['pool_name'],
                                           p['pg_num_final'], r))

        if too_few:
            summary = "{0} pools have too few placement groups".format(
                len(too_few))
            health_checks['POOL_TOO_FEW_PGS'] = {
                'severity': 'warning',
                'summary': summary,
                'detail': too_few
            }
        if too_many:
            summary = "{0} pools have too many placement groups".format(
                len(too_many))
            health_checks['POOL_TOO_MANY_PGS'] = {
                'severity': 'warning',
                'summary': summary,
                'detail': too_many
            }

        too_much_target_ratio = []
        for root_id, total in iteritems(total_ratio):
            total_target = total_target_ratio[root_id]
            if total > 1.0:
                too_much_target_ratio.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'target_size_ratio %.03f on pools %s' % (
                        root_map[root_id].pool_names,
                        total,
                        total_target,
                        target_ratio_pools[root_id]
                    )
                )
            elif total_target > 1.0:
                too_much_target_ratio.append(
                    'Pools %s have collective target_size_ratio %.03f > 1.0' % (
                        root_map[root_id].pool_names,
                        total_target
                    )
                )
        if too_much_target_ratio:
            health_checks['POOL_TARGET_SIZE_RATIO_OVERCOMMITTED'] = {
                'severity': 'warning',
                'summary': "%d subtrees have overcommitted pool target_size_ratio" % len(too_much_target_ratio),
                'detail': too_much_target_ratio,
            }

        too_much_target_bytes = []
        for root_id, total in iteritems(total_bytes):
            total_target = total_target_bytes[root_id]
            if total > root_map[root_id].capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'target_size_bytes %s on pools %s' % (
                        root_map[root_id].pool_names,
                        total / root_map[root_id].capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                        target_bytes_pools[root_id]
                    )
                )
            elif total_target > root_map[root_id].capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'collective target_size_bytes of %s' % (
                        root_map[root_id].pool_names,
                        total / root_map[root_id].capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                    )
                )
        if too_much_target_bytes:
            health_checks['POOL_TARGET_SIZE_BYTES_OVERCOMMITTED'] = {
                'severity': 'warning',
                'summary': "%d subtrees have overcommitted pool target_size_bytes" % len(too_much_target_bytes),
                'detail': too_much_target_bytes,
            }


        self.set_health_checks(health_checks)
