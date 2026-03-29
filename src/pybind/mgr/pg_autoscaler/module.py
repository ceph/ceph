"""
Automatically scale pg_num based on how much data is stored in each pool.
"""

from collections import defaultdict
import json
import mgr_util
import threading
from typing import Any, Dict, NamedTuple, List, Optional, Set, Tuple, TYPE_CHECKING, Union
import uuid
from prettytable import PrettyTable
from mgr_module import HealthChecksT, CRUSHMap, MgrModule, Option, OSDMap

from .cli import PGAutoscalerCLICommand

"""
Some terminology is made up for the purposes of this module:

 - "raw pgs": pg count after applying replication, i.e. the real resource
              consumption of a pool.
 - "grow/shrink" - increase/decrease the pg_num in a pool
 - "crush subtree" - non-overlapping domains in crush hierarchy: used as
                     units of resource management.
"""

INTERVAL = 5

if TYPE_CHECKING:
    import sys
    if sys.version_info >= (3, 8):
        from typing import Literal
    else:
        from typing_extensions import Literal

    PassT = Literal['first', 'second', 'third', 'fourth']

def is_power_of_two(v: int) -> bool:
    return v >= 0 and (v & (v - 1)) == 0

def nearest_power_of_two(n: int, direction: str = "nearest") -> int:
    """
    Direction is up, down, or nearest.
    If n is a power of two and down is specified, round to the power of two below n,
    else return n
    """
    v = int(n)
    if is_power_of_two(v):
        if direction == "down":
            return v >> 1 if v >= 1 else 0
        return v

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

    if direction == "up":
        return v
    elif direction == "down":
        return x
    return x if (v - n) > (n - x) else v


def effective_target_ratio(target_ratio: float,
                           total_target_ratio: float,
                           total_target_bytes: int,
                           capacity: int) -> float:
    """
    Returns the target ratio after normalizing for ratios across pools and
    adjusting for capacity reserved by pools that have target_size_bytes set.
    """
    target_ratio = float(target_ratio)
    if total_target_ratio:
        target_ratio = target_ratio / total_target_ratio

    if total_target_bytes and capacity:
        fraction_available = 1.0 - min(1.0, float(total_target_bytes) / capacity)
        target_ratio *= fraction_available

    return target_ratio


class PgAdjustmentProgress(object):
    """
    Keeps the initial and target pg_num values
    """

    def __init__(self, pool_id: int, pg_num: int, pg_num_target: int) -> None:
        self.ev_id = str(uuid.uuid4())
        self.pool_id = pool_id
        self.reset(pg_num, pg_num_target)

    def reset(self, pg_num: int, pg_num_target: int) -> None:
        self.pg_num = pg_num
        self.pg_num_target = pg_num_target

    def update(self, module: MgrModule, progress: float) -> None:
        desc = 'increasing' if self.pg_num < self.pg_num_target else 'decreasing'
        module.remote('progress', 'update', self.ev_id,
                      ev_msg="PG autoscaler %s pool %d PGs from %d to %d" %
                      (desc, self.pool_id, self.pg_num, self.pg_num_target),
                      ev_progress=progress,
                      refs=[("pool", self.pool_id)])


class CrushSubtreeResourceStatus:
    def __init__(self) -> None:
        self.root_ids: List[int] = []
        self.osds: Set[int] = set()
        self.osd_count: Optional[int] = None  # Number of OSDs
        self.pg_target: Optional[int] = None  # Ideal full-capacity PG count?
        self.pg_current = 0  # How many PGs already?
        self.pg_left = 0
        self.capacity: Optional[int] = None  # Total capacity of OSDs in subtree
        self.pool_ids: List[int] = []
        self.pool_names: List[str] = []
        self.pool_count: Optional[int] = None
        self.pool_used = 0
        self.total_target_ratio = 0.0
        self.total_target_bytes = 0  # including replication / EC overhead


class BacktrackNode(NamedTuple):
    current_pg_sum: int # cummulative pgs added at current node
    total_cost: int #cummulative cost at current node
    prev_pg_sum: int #cummulative PGs added at previous node

class GroupKey(NamedTuple):
    pg_target: int
    size: int
    bias: int
    bulk: bool
    autoscale: bool

class PoolGroup:
    # Treat similar pools as a group to prefer fairness over greedy. All pools
    # with the same GroupKey get the same PGs, even if a subset of them
    # could be rounded up to have more
    def __init__(self, pg_target: int, size: int, bias: float, autoscale: bool) -> None:
        self.pools: Dict[str, Dict[str, Any]] = {} #Group together pools with the same
        # (pg_target, replication size, bias, bulk, autoscale) so they can be rounded the same direction
        self.pg_target_total = 0
        self.rd_down_total = 0
        self.rd_up_total = 0
        self.deviation_cost_down_total = 0
        self.deviation_cost_up_total = 0
        self.update(pg_target, size, bias, autoscale)

    def update(self, pg_target: int, size: int, bias: float, autoscale: bool) -> None:
        self.pg_target = pg_target * bias
        pg_target_per_replica = self.pg_target / size
        self.rd_down_val = nearest_power_of_two(pg_target_per_replica, "down") * size
        self.rd_up_val = nearest_power_of_two(pg_target_per_replica, "up") * size
        self.cost_down_val = abs(self.rd_down_val - pg_target)
        self.cost_up_val = abs(self.rd_up_val - pg_target)
        if not autoscale: # If autoscaler is not enabled then use current value without rounding
            self.rd_down_val = self.pg_target
            self.rd_up_val = self.pg_target
            self.cost_down_val = 0
            self.cost_up_val = 0

        self.refresh_totals()

    def refresh_totals(self) -> None:
        self.pg_target_total = self.pg_target * self.size()
        self.rd_down_total = self.rd_down_val * self.size()
        self.rd_up_total = self.rd_up_val * self.size()
        self.deviation_cost_down_total = self.cost_down_val * self.size()
        self.deviation_cost_up_total = self.cost_up_val * self.size()

    def add(self, pool_name: str, p: Dict[str, Any]) -> None:
        self.pools[pool_name] = p
        self.refresh_totals()

    def size(self) -> int:
        return len(self.pools)

class PgAutoscaler(MgrModule):
    CLICommand = PGAutoscalerCLICommand
    """
    PG autoscaler.
    """
    NATIVE_OPTIONS = [
        'mon_target_pg_per_osd',
        'mon_max_pg_per_osd',
        'mon_min_pool_pg_num',
    ]

    MODULE_OPTIONS = [
        Option(
            name='sleep_interval',
            type='secs',
            default=60),

        Option(
            name='threshold',
            type='float',
            desc='scaling threshold',
            long_desc=('The factor by which the `NEW PG_NUM` must vary from the current'
                       '`PG_NUM` before being accepted. Cannot be less than 1.0'),
            default=3.0,
            min=1.0),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(PgAutoscaler, self).__init__(*args, **kwargs)
        self._shutdown = threading.Event()
        self._event: Dict[int, PgAdjustmentProgress] = {}

        # So much of what we do peeks at the osdmap that it's easiest
        # to just keep a copy of the pythonized version.
        self._osd_map = None
        if TYPE_CHECKING:
            self.sleep_interval = 60
            self.mon_target_pg_per_osd = 0
            self.threshold = 3.0

    def config_notify(self) -> None:
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))

    @PGAutoscalerCLICommand.Read('osd pool autoscale-status')
    def _command_autoscale_status(self, format: str = 'plain') -> Tuple[int, str, str]:
        """
        report on pool pg_num sizing recommendation and intent
        """
        osdmap = self.get_osdmap()
        pools = osdmap.get_pools_by_name()
        ps, root_map = self._get_pool_status(osdmap, pools)

        if format in ('json', 'json-pretty'):
            return 0, json.dumps(ps, indent=4, sort_keys=True), ''
        else:
            table = PrettyTable(['POOL', 'SIZE', 'TARGET SIZE',
                                 'RATE', 'RAW CAPACITY',
                                 'RATIO', 'FINAL RATIO', 'TARGET RATIO',
                                 'EFFECTIVE RATIO',
                                 'BIAS',
                                 'PG_NUM',
#                                 'IDEAL',
                                 'NEW PG_NUM', 'AUTOSCALE',
                                 'BULK'],
                                border=False)
            table.left_padding_width = 0
            table.right_padding_width = 2
            table.align['POOL'] = 'l'
            table.align['SIZE'] = 'r'
            table.align['TARGET SIZE'] = 'r'
            table.align['RATE'] = 'r'
            table.align['RAW CAPACITY'] = 'r'
            table.align['RATIO'] = 'r'
            table.align['FINAL RATIO'] = 'r'
            table.align['TARGET RATIO'] = 'r'
            table.align['EFFECTIVE RATIO'] = 'r'
            table.align['BIAS'] = 'r'
            table.align['PG_NUM'] = 'r'
#            table.align['IDEAL'] = 'r'
            table.align['NEW PG_NUM'] = 'r'
            table.align['AUTOSCALE'] = 'l'
            table.align['BULK'] = 'l'
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
                if p['effective_target_ratio'] > 0.0:
                    etr = '%.4f' % p['effective_target_ratio']
                else:
                    etr = ''
                table.add_row([
                    p['pool_name'],
                    mgr_util.format_bytes(p['logical_used'], 6),
                    ts,
                    p['raw_used_rate'],
                    mgr_util.format_bytes(p['subtree_capacity'], 6),
                    '%.4f' % p['capacity_ratio'],
                    '%.4f' % p['final_ratio'],
                    tr,
                    etr,
                    p['bias'],
                    p['pg_num_target'],
#                    p['pg_num_ideal'],
                    final,
                    str(p['pg_autoscale_mode']),
                    str(p['bulk'])
                ])
            return 0, table.get_string(), ''

    @PGAutoscalerCLICommand.Write("osd pool set threshold")
    def set_scaling_threshold(self, num: float) -> Tuple[int, str, str]:
        """
        set the autoscaler threshold
        A.K.A. the factor by which the new pg_num must vary
        from the existing pg_num before action is taken
        """
        if num < 1.0:
            return 22, "", "threshold cannot be set less than 1.0"
        self.set_module_option("threshold", num)
        return 0, "threshold updated", ""

    @PGAutoscalerCLICommand.Read("osd pool get threshold")
    def get_scaling_threshold(self) -> Tuple[int, str, str]:
        """
        return the autoscaler threshold value
        A.K.A. the factor by which the new pg_num must vary
        from the existing pg_num before action is taken
        """
        return 0, str(self.get_module_option('threshold')), ''

    def complete_all_progress_events(self) -> None:
        for pool_id in list(self._event):
            ev = self._event[pool_id]
            self.remote('progress', 'complete', ev.ev_id)
            del self._event[pool_id]

    def has_noautoscale_flag(self) -> bool:
        flags = self.get_osdmap().dump().get('flags', '')
        if 'noautoscale' in flags:
            return True
        else:
            return False

    def has_norecover_flag(self) -> bool:
        flags = self.get_osdmap().dump().get('flags', '')
        if 'norecover' in flags:
            return True
        else:
            return False

    @PGAutoscalerCLICommand.Write("osd pool get noautoscale")
    def get_noautoscale(self) -> Tuple[int, str, str]:
        """
        Get the noautoscale flag to see if all pools
        are setting the autoscaler on or off as well
        as newly created pools in the future.
        """
        if self.has_noautoscale_flag():
            return 0, "", "noautoscale is on"
        else:
            return 0, "", "noautoscale is off"

    @PGAutoscalerCLICommand.Write("osd pool unset noautoscale")
    def unset_noautoscale(self) -> Tuple[int, str, str]:
        """
        Unset the noautoscale flag so all pools will
        go back to its previous mode. Newly created
        pools in the future will autoscaler on by default.
        """
        if not self.has_noautoscale_flag():
            return 0, "", "noautoscale is already unset!"
        else:
            self.mon_command({
                'prefix': 'config set',
                'who': 'global',
                'name': 'osd_pool_default_pg_autoscale_mode',
                'value': 'on'
            })
            self.mon_command({
                'prefix': 'osd unset',
                'key': 'noautoscale'
            })
            return 0, "", "noautoscale is unset, all pools now back to its previous mode"

    @PGAutoscalerCLICommand.Write("osd pool set noautoscale")
    def set_noautoscale(self) -> Tuple[int, str, str]:
        """
        set the noautoscale for all pools (including
        newly created pools in the future)
        and complete all on-going progress events
        regarding PG-autoscaling.
        """
        if self.has_noautoscale_flag():
            return 0, "", "noautoscale is already set!"
        else:
            self.mon_command({
                'prefix': 'config set',
                'who': 'global',
                'name': 'osd_pool_default_pg_autoscale_mode',
                'value': 'off'
            })
            self.mon_command({
                'prefix': 'osd set',
                'key': 'noautoscale'
            })
            self.complete_all_progress_events()
            return 0, "", "noautoscale is set, all pools now have autoscale off"

    def serve(self) -> None:
        self.config_notify()
        while not self._shutdown.is_set():
            if not self.has_noautoscale_flag() and not self.has_norecover_flag():
                osdmap = self.get_osdmap()
                pools = osdmap.get_pools_by_name()
                self._maybe_adjust(osdmap, pools)
                self._update_progress_events(osdmap, pools)
            self._shutdown.wait(timeout=self.sleep_interval)

    def shutdown(self) -> None:
        self.log.info('Stopping pg_autoscaler')
        self._shutdown.set()

    def identify_subtrees_and_overlaps(self,
                                       osdmap: OSDMap,
                                       pools: Dict[str, Dict[str, Any]],
                                       crush: CRUSHMap,
                                       result: Dict[int, CrushSubtreeResourceStatus],
                                       overlapped_roots: Set[int],
                                       roots: List[CrushSubtreeResourceStatus]) -> \
        Tuple[List[CrushSubtreeResourceStatus],
              Set[int]]:

        # We identify subtrees and overlapping roots from osdmap
        for pool_name, pool in pools.items():
            crush_rule = crush.get_rule_by_id(pool['crush_rule'])
            assert crush_rule is not None
            cr_name = crush_rule['rule_name']
            root_id = crush.get_rule_root(cr_name)
            assert root_id is not None
            osds = set(crush.get_osds_under(root_id))

            # Are there overlapping roots?
            s = None
            for prev_root_id, prev in result.items():
                if osds & prev.osds:
                    s = prev
                    if prev_root_id != root_id:
                        overlapped_roots.add(prev_root_id)
                        overlapped_roots.add(root_id)
                        self.log.warning("pool %s won't scale due to overlapping roots: %s",
                                      pool_name, overlapped_roots)
                        self.log.warning("Please See: https://docs.ceph.com/en/"
                                         "latest/rados/operations/placement-groups"
                                         "/#automated-scaling")
                    break
            if not s:
                s = CrushSubtreeResourceStatus()
                roots.append(s)
            result[root_id] = s
            s.root_ids.append(root_id)
            s.osds |= osds
            s.pool_ids.append(pool['pool'])
            s.pool_names.append(pool_name)
            s.pg_current += pool['pg_num_target'] * pool['size']
            target_ratio = pool['options'].get('target_size_ratio', 0.0)
            if target_ratio:
                s.total_target_ratio += target_ratio
            else:
                target_bytes = pool['options'].get('target_size_bytes', 0)
                if target_bytes:
                    s.total_target_bytes += target_bytes * osdmap.pool_raw_used_rate(pool['pool'])
        return roots, overlapped_roots

    def get_subtree_resource_status(self,
                                    osdmap: OSDMap,
                                    pools: Dict[str, Dict[str, Any]],
                                    crush: CRUSHMap) -> Tuple[Dict[int, CrushSubtreeResourceStatus],
                                                              Set[int]]:
        """
        For each CRUSH subtree of interest (i.e. the roots under which
        we have pools), calculate the current resource usages and targets,
        such as how many PGs there are, vs. how many PGs we would
        like there to be.
        """
        result: Dict[int, CrushSubtreeResourceStatus] = {}
        roots: List[CrushSubtreeResourceStatus] = []
        overlapped_roots: Set[int] = set()
        # identify subtrees and overlapping roots
        roots, overlapped_roots = self.identify_subtrees_and_overlaps(
            osdmap, pools, crush, result, overlapped_roots, roots
        )
        # finish subtrees
        all_stats = self.get('osd_stats')
        for s in roots:
            assert s.osds is not None
            s.osd_count = len(s.osds)
            s.pg_target = s.osd_count * self.mon_target_pg_per_osd
            s.pg_left = s.pg_target
            s.pool_count = len(s.pool_ids)
            capacity = 0
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

        return result, overlapped_roots

    def _append_result (
            self,
            pool_groups: List[PoolGroup],
            backtrack: List[BacktrackNode],
            final_ratios: List[int],
            pool_pg_targets: List[int],
            final_pool_pg_targets: List[int],
            pools: List[Dict[str, Any]],
            pg_total: int
    ) -> None:
        """
        Calculate the final_pool_pg_target based on the backtracked optimal allocation
        """
        for i, group in enumerate(pool_groups):
            for pool_name, p in group.pools.items():
                min_pg = p.get('options', {}).get('pg_num_min', self.mon_min_pool_pg_num)
                max_pg = p.get('options', {}).get('pg_num_max')
                pool_pg_target = int(group.pg_target_total / group.size())
                final_pool_pg_target_total = backtrack[i].current_pg_sum - backtrack[i].prev_pg_sum + group.rd_down_total
                final_pool_pg_target = final_pool_pg_target_total / group.size()
                final_pool_pg_target_per_replica = int(final_pool_pg_target / p['size'])
                if min_pg > final_pool_pg_target_per_replica:
                    final_pool_pg_target_per_replica = min_pg
                    final_pool_pg_target = min_pg * p['size']
                if max_pg and max_pg < final_pool_pg_target:
                    final_pool_pg_target_per_replica = max_pg
                    final_pool_pg_target = max_pg * p['size']
                self.log.info("{} final_pool_pg_target_per_replica {}".format(p['pool_name'], final_pool_pg_target_per_replica))

                final_ratio = final_pool_pg_target / pg_total
                final_ratios.append(final_ratio)
                pool_pg_targets.append(pool_pg_target)
                final_pool_pg_targets.append(final_pool_pg_target_per_replica)
                pools.append(p)
                self.log.info("Pool '{0}' using {1} of space, pg target {2} quantized to {3}".format(
                    pool_name,
                    final_ratio,
                    pool_pg_target,
                    final_pool_pg_target,
                ))
                if group.size() > 0:
                    self.log.info("{} pools share the same target_ratio and pg_target. Rounding them in the same direction".format(group.size()))
                if p['pg_autoscale_mode'] == 'on':
                    assert is_power_of_two(final_pool_pg_target_per_replica)

    def _find_optimal_pg_distribution (
            self,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            root_id: int,
            base: int,
            cost: int,
            pool_groups: List[PoolGroup],
            backtrack: List[BacktrackNode]
    ) -> None:
        """
        Determine which pools to round up per root_id using knapsack problem dynamic programming.
        The cost associated with a rounding decision is the absolute difference
        between the target PG value and the rounded PG value: cost = abs(pg_target - rounded_pg).
        If multiple solutions have the same total cost, the solution with the higher cumulative PG count is selected.
        The time complexity of this problem is O(nlog2(budget)) where n = number of pools and budget = target_pg_num
        of the cluster.
        """
        pg_left = root_map[root_id].pg_left
        budget =  pg_left - base
        assert pg_left is not None

        rd_up_cost_dict: List[Tuple[int, int]] = [] # list of (rd_up_cost, pool_index)
        for i in range(len(pool_groups)):
            rd_up_cost = pool_groups[i].rd_up_total - pool_groups[i].rd_down_total
            rd_up_cost_dict.append((rd_up_cost, i))
        # parent lets us reconstruct which pools were upgraded:
        # parent[new pg total] = (cummulative cost, current pg total)
        parent: List[Dict[int, Tuple[int, int]]] = [defaultdict(lambda: (0, 0)) for _ in range(len(pool_groups)+1)]
        parent[0][0] = (cost, 0)
        for (rd_up_cost, pool_index) in rd_up_cost_dict:
            parent[pool_index+1] = {k: (v[0], k) for k, v in parent[pool_index].items()}
            for current_pg_sum, (cost, prev_pg_sum) in parent[pool_index].items():
                next_rd_up_cost = current_pg_sum + rd_up_cost
                next_rd_up_weight = cost - pool_groups[pool_index].deviation_cost_down_total + pool_groups[pool_index].deviation_cost_up_total
                if base + next_rd_up_cost <= budget:
                    if next_rd_up_cost not in parent[pool_index+1] or next_rd_up_weight < parent[pool_index+1][next_rd_up_cost][0]:
                        parent[pool_index+1][next_rd_up_cost] = (next_rd_up_weight, current_pg_sum)
        # Select solution with the smallest cost. With tiebreaker, choose solution that allocates the most total pgs
        opt = None
        for current_pg_sum, (cost, prev_pg_sum) in parent[-1].items():
            if opt is None or cost < opt[1] or (cost == opt[1] and current_pg_sum > opt[0]):
                opt = BacktrackNode(current_pg_sum, cost, prev_pg_sum)
        assert opt != None
        backtrack.append(opt)
        for i in range(len(parent)-2, 0, -1):
            (current_pg_sum, cost, prev_pg_sum) = backtrack[-1]
            backtrack.append(BacktrackNode(prev_pg_sum, parent[i][prev_pg_sum][0], parent[i][prev_pg_sum][1]))
        backtrack.reverse()

    def _calculate_final_pool_pg_target(
            self,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            root_id: int,
            func_pass: 'PassT',
            pool_group: Dict[GroupKey, PoolGroup],
    ) -> Tuple[List[float], List[int], List[int], List[Dict[str, Any]]]:
        """
        Finds the optimal distribution of PGs among all pools for the current root_id.
        First pass (Non-autoscale pools): Subtract num_pg_target from the budget without rounding to a power of two
        for non-autoscale pools.
        Second pass (Non-bulk pools): Calculate the target PG for non-bulk pools based off
        capacity_ratio = max(acutal data, or target size).
        Third pass (Bulk pools): Calculate the target PG for pools with capacity_ratio > even_ratio where
        even_ratio = pg_left / # bulk pools
        Fourth pass (Remaining bulk pools): Distribute remaining PGs to even pools where final_ratio = 1 / (# pools remaining)
        """
        backtrack: List[BacktrackNode] = []
        # (cummulative pgs added at pool i, cummulative cost at pool i,  cummulative pgs added at pool i-1)

        final_ratios = []
        pool_pg_targets = []
        final_pool_pg_targets = []
        pools = []

        base = 0
        cost = 0
        pg_left = root_map[root_id].pg_left
        victims = [] # GroupKey to remove

        self.log.debug("root {} starting {} pass. {} PG budget remaining".format(root_id, func_pass, pg_left))
        if not pool_group:
            return final_ratios, pool_pg_targets, final_pool_pg_targets, pools
        if func_pass == 'first':
            non_autoscale_groups = []
            # first pass to deal with pools without autoscale enabled
            # subtracting the current pg_num_target regardless of
            # if it is a power of two
            for group_key, group in pool_group.items():
                (pg_target, size, bias, bulk, autoscale) = group_key
                if not autoscale:
                    for pool_name, p in group.pools.items():
                        group.update(p['pg_num_target'] * size, size, bias, autoscale)
                    self.log.debug("updating group with target {} size {} bias {} bulk {} autoscale {}".format(p['pg_num_target'] * size, size, bias, bulk, autoscale))
                    base += group.rd_down_total
                    cost += group.deviation_cost_down_total
                    root_map[root_id].pool_used += group.size()
                    non_autoscale_groups.append(group)
                    victims.append(group_key)
            self._find_optimal_pg_distribution(root_map, root_id, base, cost, non_autoscale_groups, backtrack)
            self._append_result(non_autoscale_groups, backtrack, final_ratios, pool_pg_targets, final_pool_pg_targets, pools, root_map[root_id].pg_target)
        elif func_pass == 'second':
            # second pass to deal with small pools (no bulk flag)
            # calculating final_pool_pg_target based on capacity ratio
            # we also keep track of bulk_pools to be used in second pass
            non_bulk_groups = []
            for group_key, group in pool_group.items():
                (pg_target, size, bias, bulk, autoscale) = group_key
                if not bulk:
                    base += group.rd_down_total
                    cost += group.deviation_cost_down_total
                    root_map[root_id].pool_used += group.size()
                    non_bulk_groups.append(group)
                    victims.append(group_key)
            self._find_optimal_pg_distribution(root_map, root_id, base, cost, non_bulk_groups, backtrack)
            self._append_result(non_bulk_groups, backtrack, final_ratios, pool_pg_targets, final_pool_pg_targets, pools, root_map[root_id].pg_target)
        elif func_pass == 'third':
            # third pass we calculate the final_pool_pg_target
            # for pools that have used_ratio > even_ratio
            # and we keep track of even pools to be used in third pass
            pool_count = root_map[root_id].pool_count
            assert pool_count is not None
            even_ratio = 1 / (pool_count - root_map[root_id].pool_used)
            non_even_groups = []
            assert pg_left is not None
            for group_key, group in pool_group.items():
                (pg_target, size, bias, bulk, autoscale) = group_key
                used_ratio = group.pg_target_total / group.size() / pg_left
                if used_ratio > even_ratio and bulk:
                    base += group.rd_down_total
                    cost +=group.deviation_cost_down_total
                    root_map[root_id].pool_used += group.size()
                    non_even_groups.append(group)
                    victims.append(group_key)
            self._find_optimal_pg_distribution(root_map, root_id, base, cost, non_even_groups, backtrack)
            self._append_result(non_even_groups, backtrack, final_ratios, pool_pg_targets, final_pool_pg_targets, pools, root_map[root_id].pg_target)
        else:
            # fourth pass all pools are even and are assigned the same final_pool_pg_target
            pool_count = root_map[root_id].pool_count
            assert pool_count is not None
            final_ratio = 1 / (pool_count - root_map[root_id].pool_used)
            for (pg_target, size, bias, bulk, autoscale), group in pool_group.items():
                pg_target = final_ratio * root_map[root_id].pg_left
                self.log.debug("updating group with target {} size {} bias {} bulk {} autoscale {}".format(pg_target, size, bias, bulk, autoscale))
                group.update(pg_target, size, bias, autoscale)
                base += group.rd_down_total
                cost += group.deviation_cost_down_total
                root_map[root_id].pool_used += group.size()
            remainder_groups = list(pool_group.values())
            self._find_optimal_pg_distribution(root_map, root_id, base, cost, remainder_groups, backtrack)
            self._append_result(remainder_groups, backtrack, final_ratios, pool_pg_targets, final_pool_pg_targets, pools, root_map[root_id].pg_target)

        for key in victims:
            del pool_group[key]
        return final_ratios, pool_pg_targets, final_pool_pg_targets, pools

    def get_dynamic_threshold(
            self,
            final_pg_num: int,
            default_threshold: float,
    ) -> float:
        if final_pg_num in (512, 1024):
            return 1.0
        elif final_pg_num == 2048:
            return 2.0
        return default_threshold

    def _calculate_pool_metrics(
            self,
            osdmap: OSDMap,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            root_id: int,
            pool_id: int,
            pool_stats: Dict[int, Dict[str, int]],
            capacity: float,
            bulk: bool,
            p: Dict[str, Any],
            pool_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        raw_used_rate = osdmap.pool_raw_used_rate(pool_id)
        target_bytes = 0
        # ratio takes precedence if both are set
        if p['options'].get('target_size_ratio', 0.0) == 0.0:
            target_bytes = p['options'].get('target_size_bytes', 0)

        # What proportion of space are we using?
        actual_raw_used = pool_stats[pool_id]['bytes_used']
        actual_capacity_ratio = float(actual_raw_used) / capacity
        pool_raw_used = max(actual_raw_used, target_bytes * raw_used_rate)
        capacity_ratio = float(pool_raw_used) / capacity

        self.log.info("effective_target_ratio {0} {1} {2} {3}".format(
            p['options'].get('target_size_ratio', 0.0),
            root_map[root_id].total_target_ratio,
            root_map[root_id].total_target_bytes,
            capacity))

        target_ratio = effective_target_ratio(p['options'].get('target_size_ratio', 0.0),
                                            root_map[root_id].total_target_ratio,
                                            root_map[root_id].total_target_bytes,
                                            capacity)
        pool_id = p['pool']
        pool_metrics[pool_id] = {
            'target_bytes': target_bytes,
            'raw_used_rate': raw_used_rate,
            'actual_raw_used': actual_raw_used,
            'actual_capacity_ratio': actual_capacity_ratio,
            'pool_raw_used': pool_raw_used,
            'capacity_ratio': capacity_ratio,
            'target_ratio': target_ratio,
            'bulk': bulk,
        }

    def _get_pool_pg_targets(
            self,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            ret: List[Dict[str, Any]],
            threshold: float,
            func_pass: 'PassT',
            pool_groups_by_root: Dict[int, Dict[GroupKey, PoolGroup]],
            pool_metrics: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Calculate final_pool_pg_target for each CRUSH root and return
        the autoscaler output along with pool metrics.

        For each root, this method:
        - calculates final PG targets for pools in that root,
        - determines whether each pool would be adjusted in the current pass,
        - records the derived metrics used by the autoscaler output.

        Pools grouped together for rounding are handled by _calculate_final_pool_pg_target().

        If a root runs out of PG budget during a pass, scaling decisions for pools in
        that pass may be suppressed.

        All final scaling decisions get checked against dynamic_threshold
        """
        ret_copy = []
        for root_id in pool_groups_by_root:
            pgs_used_total = 0
            final_ratios, pool_pg_targets, final_pool_pg_targets, pools = self._calculate_final_pool_pg_target(
            root_map, root_id, func_pass, pool_groups_by_root[root_id])

            fail_all = False
            for final_ratio, pool_pg_target, final_pool_pg_target, p in zip(final_ratios, pool_pg_targets, final_pool_pg_targets, pools):
                if final_ratio is None:
                    continue
                pgs_used = final_pool_pg_target * p['size']
                root_map[root_id].pg_left -= pgs_used
                adjust = False

                if root_map[root_id].pg_left <= 0:
                    fail_all = True
                # Dynamic threshold only applies to scaling UP, otherwise use the default threshold.
                if final_pool_pg_target is not None and \
                   final_pool_pg_target > p['pg_num_target']:
                    dynamic_threshold = self.get_dynamic_threshold(final_pool_pg_target, threshold)
                    adjust = final_pool_pg_target > p['pg_num_target'] * dynamic_threshold
                else:
                    adjust = final_pool_pg_target < p['pg_num_target'] / threshold

                if adjust and \
                   final_ratio >= 0.0 and \
                   final_ratio <= 1.0 and \
                   p['pg_autoscale_mode'] == 'on':
                    adjust = True
                else:
                    if final_pool_pg_target != p['pg_num_target']:
                        self.log.warning("pool %s won't scale because recommended PG_NUM target"
                                         " value %d varies from current PG_NUM value %d by"
                                         " more than '%f' scaling threshold",
                                         p['pool_name'], p['pg_num_target'], final_pool_pg_target,
                                         dynamic_threshold if final_pool_pg_target > p['pg_num_target'] else threshold)
                pool_id = p['pool']
                capacity = root_map[root_id].capacity
                metrics = pool_metrics[pool_id]
                assert pool_pg_target is not None
                ret_copy.append({
                    'pool_id': pool_id,
                    'pool_name': p['pool_name'],
                    'crush_root_id': root_id,
                    'pg_autoscale_mode': p['pg_autoscale_mode'],
                    'pg_num_target': p['pg_num_target'],
                    'logical_used': float(metrics['actual_raw_used'])/metrics['raw_used_rate'],
                    'target_bytes': metrics['target_bytes'],
                    'raw_used_rate': metrics['raw_used_rate'],
                    'subtree_capacity': capacity,
                    'actual_raw_used': metrics['actual_raw_used'],
                    'raw_used': metrics['pool_raw_used'],
                    'actual_capacity_ratio': metrics['actual_capacity_ratio'],
                    'capacity_ratio': metrics['capacity_ratio'],
                    'final_ratio': final_ratio,
                    'target_ratio': p['options'].get('target_size_ratio', 0.0),
                    'effective_target_ratio': metrics['target_ratio'],
                    # 'pg_num_ideal': int(pool_pg_target),
                    'pg_num_final': final_pool_pg_target,
                    'would_adjust': adjust,
                    'bias': p.get('options', {}).get('pg_autoscale_bias', 1.0),
                    'bulk': metrics['bulk'],
                })

            for copy in ret_copy:
                if fail_all:
                    copy['would_adjust'] = False
                ret.append(copy)
        return ret

    def _compute_pool_group_metrics(
        self,
        osdmap: OSDMap,
        pools: Dict[str, Dict[str, Any]],
        crush_map: CRUSHMap,
        root_map: Dict[int, CrushSubtreeResourceStatus],
        pool_stats: Dict[int, Dict[str, int]],
        overlapped_roots: Set[int],
        pool_groups_by_root: Dict[int, Dict[GroupKey, PoolGroup]],
        pool_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        """
        Add pools to PoolGroups where all pools with the same GroupKey are
        allocated the same number of PGs
        """
        for pool_name, p in pools.items():
            pool_id = p['pool']
            if pool_id not in pool_stats:
                # race with pool deletion; skip
                continue
            # FIXME: we assume there is only one take per pool, but that
            # may not be true.
            crush_rule = crush_map.get_rule_by_id(p['crush_rule'])
            assert crush_rule is not None
            cr_name = crush_rule['rule_name']
            root_id = crush_map.get_rule_root(cr_name)
            assert root_id is not None
            if root_id in overlapped_roots:
                # skip pools
                # with overlapping roots
                self.log.warn("pool %d contains an overlapping root %d"
                              "... skipping scaling", pool_id, root_id)
                continue
            capacity = root_map[root_id].capacity
            assert capacity is not None
            if capacity == 0:
                self.log.debug("skipping empty subtree {0}".format(cr_name))
                continue

            bias = p['options'].get('pg_autoscale_bias', 1.0)
            # determine if the pool is a bulk
            bulk = False
            flags = p['flags_names'].split(",")
            if "bulk" in flags:
                bulk = True
            self._calculate_pool_metrics(osdmap, root_map, root_id, pool_id, pool_stats, capacity, bulk, p, pool_metrics)
            metrics = pool_metrics[pool_id]
            pg_left = root_map[root_id].pg_left
            self.log.debug("{} metrics: {}".format(p['pool_name'], metrics))

            capacity_ratio = max(metrics['capacity_ratio'], metrics['target_ratio'])
            pg_target = int(capacity_ratio * pg_left)
            autoscale = p['pg_autoscale_mode'] != 'off'
            self.log.debug("pool {} capacity ratio: {} target ratio {} pg_num_target {}".format(pool_name, metrics['capacity_ratio'], metrics['target_ratio'], p['pg_num_target']))

            #group similar pools so they are all rounded the same direction
            group_key = GroupKey(pg_target, p['size'], bias, bulk, autoscale)
            if group_key not in pool_groups_by_root[root_id]:
                pool_groups_by_root[root_id][group_key] = PoolGroup(pg_target, p['size'], bias, autoscale)
            pool_groups_by_root[root_id][group_key].add(pool_name, p)

            self.log.debug("adding pool {} with target {} size {} bias {} bulk {} autoscale {}".format(pool_name, pg_target, p['size'], bias, bulk, autoscale))

    def  _get_pool_status(
            self,
            osdmap: OSDMap,
            pools: Dict[str, Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]],
               Dict[int, CrushSubtreeResourceStatus]]:
        threshold = self.threshold
        assert threshold >= 1.0

        crush_map = osdmap.get_crush()
        root_map, overlapped_roots = self.get_subtree_resource_status(osdmap, pools, crush_map)
        df = self.get('df')
        pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])

        ret: List[Dict[str, Any]] = []

        # Iterate over all pools to determine how they should be sized.
        # First call _get_pool_pg_targets() is to remove the PG num of non-autsocale pools from the budget
        # Second call of get_pool_pg_targets() is to adjust non-bulk pools.
        # Third call of get_pool_pg_targets() is to find/adjust bulk pools that use more capacity than
        # the even_ratio of other pools and we adjust those first.
        # Fourth call, make use of the even_pools and iterate over those and give them 1/pool_count of the
        # total pgs.

        pool_groups_by_root: Dict[int, Dict[GroupKey, PoolGroup]] = defaultdict(dict)
        # { root_id: { GroupKey(pg_target, replication size, bias, bulk): PoolGroup }}

        pool_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        # {
        #     'target_bytes': int
        #     'raw_used_rate': float
        #     'actual_raw_used': int
        #     'actual_capacity_ratio': float
        #     'pool_raw_used': float
        #     'capacity_ratio': float
        #     'target_ratio': float
        #     'bulk': bool
        # }
        self._compute_pool_group_metrics(osdmap, pools, crush_map, root_map, pool_stats, overlapped_roots, pool_groups_by_root, pool_metrics)

        ret = self._get_pool_pg_targets(root_map, ret, threshold, 'first', pool_groups_by_root, pool_metrics)
        ret = self._get_pool_pg_targets(root_map, ret, threshold, 'second', pool_groups_by_root, pool_metrics)
        ret = self._get_pool_pg_targets(root_map, ret, threshold, 'third', pool_groups_by_root, pool_metrics)
        ret = self._get_pool_pg_targets(root_map, ret, threshold, 'fourth', pool_groups_by_root, pool_metrics)


        # If noautoscale flag is set, we set pg_autoscale_mode to off
        if self.has_noautoscale_flag():
            for p in ret:
                p['pg_autoscale_mode'] = 'off'

        return (ret, root_map)

    def _get_pool_by_id(self,
                     pools: Dict[str, Dict[str, Any]],
                     pool_id: int) -> Optional[Dict[str, Any]]:
        # Helper for getting pool data by pool_id
        for pool_name, p in pools.items():
            if p['pool'] == pool_id:
                return p
        self.log.debug('pool not found')
        return None

    def _update_progress_events(self,
                                osdmap: OSDMap,
                                pools: Dict[str, Dict[str, Any]]) -> None:
        # Update progress events if necessary
        if self.has_noautoscale_flag():
            self.log.debug("noautoscale_flag is set.")
            return
        for pool_id in list(self._event):
            ev = self._event[pool_id]
            pool_data = self._get_pool_by_id(pools, pool_id)
            if (
                pool_data is None
                or pool_data["pg_num"] == pool_data["pg_num_target"]
                or ev.pg_num == ev.pg_num_target
            ):
                # pool is gone or we've reached our target
                self.remote('progress', 'complete', ev.ev_id)
                del self._event[pool_id]
                continue
            ev.update(self, (ev.pg_num - pool_data['pg_num']) / (ev.pg_num - ev.pg_num_target))

    def _maybe_adjust(self,
                      osdmap: OSDMap,
                      pools: Dict[str, Dict[str, Any]]) -> None:
        # Figure out which pool needs pg adjustments
        self.log.info('_maybe_adjust')
        if self.has_noautoscale_flag():
            self.log.debug("noautoscale_flag is set.")
            return
        if osdmap.get_require_osd_release() < 'nautilus':
            return

        self.log.debug("pool: {0}".format(json.dumps(pools, indent=4,
                                sort_keys=True)))

        ps, root_map = self._get_pool_status(osdmap, pools)

        # Anyone in 'warn', set the health message for them and then
        # drop them from consideration.
        too_few = []
        too_many = []
        bytes_and_ratio = []
        health_checks: Dict[str, Dict[str, Union[int, str, List[str]]]] = {}

        total_bytes = dict([(r, 0) for r in iter(root_map)])
        total_target_bytes = dict([(r, 0.0) for r in iter(root_map)])
        target_bytes_pools: Dict[int, List[int]] = dict([(r, []) for r in iter(root_map)])

        for p in ps:
            pool_id = p['pool_id']
            pool_opts = pools[p['pool_name']]['options']
            if pool_opts.get('target_size_ratio', 0) > 0 and pool_opts.get('target_size_bytes', 0) > 0:
                bytes_and_ratio.append(
                    'Pool %s has target_size_bytes and target_size_ratio set' % p['pool_name'])
            total_bytes[p['crush_root_id']] += max(
                p['actual_raw_used'],
                p['target_bytes'] * p['raw_used_rate'])
            if p['target_bytes'] > 0:
                total_target_bytes[p['crush_root_id']] += p['target_bytes'] * p['raw_used_rate']
                target_bytes_pools[p['crush_root_id']].append(p['pool_name'])
            if p['pg_autoscale_mode'] == 'warn':
                msg = 'Pool %s has %d placement groups, should have %d' % (
                    p['pool_name'],
                    p['pg_num_target'],
                    p['pg_num_final'])
                if p['pg_num_final'] > p['pg_num_target']:
                    too_few.append(msg)
                elif p['pg_num_final'] < p['pg_num_target']:
                    too_many.append(msg)
            if not p['would_adjust']:
                continue
            if p['pg_autoscale_mode'] == 'on':
                # Note that setting pg_num actually sets pg_num_target (see
                # OSDMonitor.cc)
                r = self.mon_command({
                    'prefix': 'osd pool set',
                    'pool': p['pool_name'],
                    'var': 'pg_num',
                    'val': str(p['pg_num_final'])
                })

                # create new event or update existing one to reflect
                # progress from current state to the new pg_num_target
                pool_data = pools[p['pool_name']]
                pg_num = pool_data['pg_num']
                new_target = p['pg_num_final']
                if pool_id in self._event:
                    self._event[pool_id].reset(pg_num, new_target)
                else:
                    self._event[pool_id] = PgAdjustmentProgress(pool_id, pg_num, new_target)
                self._event[pool_id].update(self, 0.0)

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
                'count': len(too_few),
                'detail': too_few
            }
        if too_many:
            summary = "{0} pools have too many placement groups".format(
                len(too_many))
            health_checks['POOL_TOO_MANY_PGS'] = {
                'severity': 'warning',
                'summary': summary,
                'count': len(too_many),
                'detail': too_many
            }

        too_much_target_bytes = []
        for root_id, total in total_bytes.items():
            total_target = int(total_target_bytes[root_id])
            capacity = root_map[root_id].capacity
            assert capacity is not None
            if total_target > 0 and total > capacity and capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'target_size_bytes %s on pools %s' % (
                        root_map[root_id].pool_names,
                        total / capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                        target_bytes_pools[root_id]
                    )
                )
            elif total_target > capacity and capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'collective target_size_bytes of %s' % (
                        root_map[root_id].pool_names,
                        total / capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                    )
                )
        if too_much_target_bytes:
            health_checks['POOL_TARGET_SIZE_BYTES_OVERCOMMITTED'] = {
                'severity': 'warning',
                'summary': "%d subtrees have overcommitted pool target_size_bytes" % len(too_much_target_bytes),
                'count': len(too_much_target_bytes),
                'detail': too_much_target_bytes,
            }

        if bytes_and_ratio:
            health_checks['POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO'] = {
                'severity': 'warning',
                'summary': "%d pools have both target_size_bytes and target_size_ratio set" % len(bytes_and_ratio),
                'count': len(bytes_and_ratio),
                'detail': bytes_and_ratio,
            }

        self.set_health_checks(health_checks)
