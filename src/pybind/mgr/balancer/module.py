"""
Balance PG distribution across OSDs.
"""

import copy
import enum
import errno
import json
import math
import random
import time
from mgr_module import CLIReadCommand, CLICommand, CommandResult, MgrModule, Option, OSDMap
from threading import Event
from typing import cast, Any, Dict, List, Optional, Sequence, Tuple, Union
from mgr_module import CRUSHMap
import datetime

TIME_FORMAT = '%Y-%m-%d_%H:%M:%S'


class MappingState:
    def __init__(self, osdmap, raw_pg_stats, raw_pool_stats, desc=''):
        self.desc = desc
        self.osdmap = osdmap
        self.osdmap_dump = self.osdmap.dump()
        self.crush = osdmap.get_crush()
        self.crush_dump = self.crush.dump()
        self.raw_pg_stats = raw_pg_stats
        self.raw_pool_stats = raw_pool_stats
        self.pg_stat = {
            i['pgid']: i['stat_sum'] for i in raw_pg_stats.get('pg_stats', [])
        }
        osd_poolids = [p['pool'] for p in self.osdmap_dump.get('pools', [])]
        pg_poolids = [p['poolid'] for p in raw_pool_stats.get('pool_stats', [])]
        self.poolids = set(osd_poolids) & set(pg_poolids)
        self.pg_up = {}
        self.pg_up_by_poolid = {}
        for poolid in self.poolids:
            self.pg_up_by_poolid[poolid] = osdmap.map_pool_pgs_up(poolid)
            for a, b in self.pg_up_by_poolid[poolid].items():
                self.pg_up[a] = b

    def calc_misplaced_from(self, other_ms):
        num = len(other_ms.pg_up)
        misplaced = 0
        for pgid, before in other_ms.pg_up.items():
            if before != self.pg_up.get(pgid, []):
                misplaced += 1
        if num > 0:
            return float(misplaced) / float(num)
        return 0.0


class Mode(enum.Enum):
    none = 'none'
    crush_compat = 'crush-compat'
    upmap = 'upmap'


class Plan(object):
    def __init__(self, name, mode, osdmap, pools):
        self.name = name
        self.mode = mode
        self.osdmap = osdmap
        self.osdmap_dump = osdmap.dump()
        self.pools = pools
        self.osd_weights = {}
        self.compat_ws = {}
        self.inc = osdmap.new_incremental()
        self.pg_status = {}

    def dump(self) -> str:
        return json.dumps(self.inc.dump(), indent=4, sort_keys=True)

    def show(self) -> str:
        return 'upmap plan'


class MsPlan(Plan):
    """
    Plan with a preloaded MappingState member.
    """

    def __init__(self, name: str, mode: str, ms: MappingState, pools: List[str]) -> None:
        super(MsPlan, self).__init__(name, mode, ms.osdmap, pools)
        self.initial = ms

    def final_state(self) -> MappingState:
        self.inc.set_osd_reweights(self.osd_weights)
        self.inc.set_crush_compat_weight_set_weights(self.compat_ws)
        return MappingState(self.initial.osdmap.apply_incremental(self.inc),
                            self.initial.raw_pg_stats,
                            self.initial.raw_pool_stats,
                            'plan %s final' % self.name)

    def show(self) -> str:
        ls = []
        ls.append('# starting osdmap epoch %d' % self.initial.osdmap.get_epoch())
        ls.append('# starting crush version %d' %
                  self.initial.osdmap.get_crush_version())
        ls.append('# mode %s' % self.mode)
        if len(self.compat_ws) and \
           not CRUSHMap.have_default_choose_args(self.initial.crush_dump):
            ls.append('ceph osd crush weight-set create-compat')
        for osd, weight in self.compat_ws.items():
            ls.append('ceph osd crush weight-set reweight-compat %s %f' %
                      (osd, weight))
        for osd, weight in self.osd_weights.items():
            ls.append('ceph osd reweight osd.%d %f' % (osd, weight))
        incdump = self.inc.dump()
        for pgid in incdump.get('old_pg_upmap_items', []):
            ls.append('ceph osd rm-pg-upmap-items %s' % pgid)
        for item in incdump.get('new_pg_upmap_items', []):
            osdlist = []
            for m in item['mappings']:
                osdlist += [m['from'], m['to']]
            ls.append('ceph osd pg-upmap-items %s %s' %
                      (item['pgid'], ' '.join([str(a) for a in osdlist])))
        return '\n'.join(ls)


class Eval:
    def __init__(self, ms: MappingState):
        self.ms = ms
        self.root_ids: Dict[str, int] = {}        # root name -> id
        self.pool_name: Dict[str, str] = {}       # pool id -> pool name
        self.pool_id: Dict[str, int] = {}         # pool name -> id
        self.pool_roots: Dict[str, List[str]] = {}      # pool name -> root name
        self.root_pools: Dict[str, List[str]] = {}      # root name -> pools
        self.target_by_root: Dict[str, Dict[int, float]] = {}  # root name -> target weight map
        self.count_by_pool: Dict[str, dict] = {}
        self.count_by_root: Dict[str, dict] = {}
        self.actual_by_pool: Dict[str, dict] = {}  # pool -> by_* -> actual weight map
        self.actual_by_root: Dict[str, dict] = {}  # pool -> by_* -> actual weight map
        self.total_by_pool: Dict[str, dict] = {}   # pool -> by_* -> total
        self.total_by_root: Dict[str, dict] = {}   # root -> by_* -> total
        self.stats_by_pool: Dict[str, dict] = {}   # pool -> by_* -> stddev or avg -> value
        self.stats_by_root: Dict[str, dict] = {}   # root -> by_* -> stddev or avg -> value

        self.score_by_pool: Dict[str, float] = {}
        self.score_by_root: Dict[str, Dict[str, float]] = {}

        self.score = 0.0

    def show(self, verbose: bool = False) -> str:
        if verbose:
            r = self.ms.desc + '\n'
            r += 'target_by_root %s\n' % self.target_by_root
            r += 'actual_by_pool %s\n' % self.actual_by_pool
            r += 'actual_by_root %s\n' % self.actual_by_root
            r += 'count_by_pool %s\n' % self.count_by_pool
            r += 'count_by_root %s\n' % self.count_by_root
            r += 'total_by_pool %s\n' % self.total_by_pool
            r += 'total_by_root %s\n' % self.total_by_root
            r += 'stats_by_root %s\n' % self.stats_by_root
            r += 'score_by_pool %s\n' % self.score_by_pool
            r += 'score_by_root %s\n' % self.score_by_root
        else:
            r = self.ms.desc + ' '
        r += 'score %f (lower is better)\n' % self.score
        return r

    def calc_stats(self, count, target, total):
        num = max(len(target), 1)
        r: Dict[str, Dict[str, Union[int, float]]] = {}
        for t in ('pgs', 'objects', 'bytes'):
            if total[t] == 0:
                r[t] = {
                    'max': 0,
                    'min': 0,
                    'avg': 0,
                    'stddev': 0,
                    'sum_weight': 0,
                    'score': 0,
                }
                continue

            avg = float(total[t]) / float(num)
            dev = 0.0

            # score is a measure of how uneven the data distribution is.
            # score lies between [0, 1), 0 means perfect distribution.
            score = 0.0
            sum_weight = 0.0

            for k, v in count[t].items():
                # adjust/normalize by weight
                if target[k]:
                    adjusted = float(v) / target[k] / float(num)
                else:
                    adjusted = 0.0

                # Overweighted devices and their weights are factors to calculate reweight_urgency.
                # One 10% underfilled device with 5 2% overfilled devices, is arguably a better
                # situation than one 10% overfilled with 5 2% underfilled devices
                if adjusted > avg:
                    '''
                    F(x) = 2*phi(x) - 1, where phi(x) = cdf of standard normal distribution
                    x = (adjusted - avg)/avg.
                    Since, we're considering only over-weighted devices, x >= 0, and so phi(x) lies in [0.5, 1).
                    To bring range of F(x) in range [0, 1), we need to make the above modification.

                    In general, we need to use a function F(x), where x = (adjusted - avg)/avg
                    1. which is bounded between 0 and 1, so that ultimately reweight_urgency will also be bounded.
                    2. A larger value of x, should imply more urgency to reweight.
                    3. Also, the difference between F(x) when x is large, should be minimal.
                    4. The value of F(x) should get close to 1 (highest urgency to reweight) with steeply.

                    Could have used F(x) = (1 - e^(-x)). But that had slower convergence to 1, compared to the one currently in use.

                    cdf of standard normal distribution: https://stackoverflow.com/a/29273201
                    '''
                    score += target[k] * (math.erf(((adjusted - avg) / avg) / math.sqrt(2.0)))
                    sum_weight += target[k]
                dev += (avg - adjusted) * (avg - adjusted)
            stddev = math.sqrt(dev / float(max(num - 1, 1)))
            score = score / max(sum_weight, 1)
            r[t] = {
                'max': max(count[t].values()),
                'min': min(count[t].values()),
                'avg': avg,
                'stddev': stddev,
                'sum_weight': sum_weight,
                'score': score,
            }
        return r


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(name='active',
               type='bool',
               default=True,
               desc='automatically balance PGs across cluster',
               runtime=True),
        Option(name='begin_time',
               type='str',
               default='0000',
               desc='beginning time of day to automatically balance',
               long_desc='This is a time of day in the format HHMM.',
               runtime=True),
        Option(name='end_time',
               type='str',
               default='2359',
               desc='ending time of day to automatically balance',
               long_desc='This is a time of day in the format HHMM.',
               runtime=True),
        Option(name='begin_weekday',
               type='uint',
               default=0,
               min=0,
               max=6,
               desc='Restrict automatic balancing to this day of the week or later',
               long_desc='0 = Sunday, 1 = Monday, etc.',
               runtime=True),
        Option(name='end_weekday',
               type='uint',
               default=0,
               min=0,
               max=6,
               desc='Restrict automatic balancing to days of the week earlier than this',
               long_desc='0 = Sunday, 1 = Monday, etc.',
               runtime=True),
        Option(name='crush_compat_max_iterations',
               type='uint',
               default=25,
               min=1,
               max=250,
               desc='maximum number of iterations to attempt optimization',
               runtime=True),
        Option(name='crush_compat_metrics',
               type='str',
               default='pgs,objects,bytes',
               desc='metrics with which to calculate OSD utilization',
               long_desc='Value is a list of one or more of "pgs", "objects", or "bytes", and indicates which metrics to use to balance utilization.',
               runtime=True),
        Option(name='crush_compat_step',
               type='float',
               default=.5,
               min=.001,
               max=.999,
               desc='aggressiveness of optimization',
               long_desc='.99 is very aggressive, .01 is less aggressive',
               runtime=True),
        Option(name='min_score',
               type='float',
               default=0,
               desc='minimum score, below which no optimization is attempted',
               runtime=True),
        Option(name='mode',
               desc='Balancer mode',
               default='upmap',
               enum_allowed=['none', 'crush-compat', 'upmap'],
               runtime=True),
        Option(name='sleep_interval',
               type='secs',
               default=60,
               desc='how frequently to wake up and attempt optimization',
               runtime=True),
        Option(name='upmap_max_optimizations',
               type='uint',
               default=10,
               desc='maximum upmap optimizations to make per attempt',
               runtime=True),
        Option(name='upmap_max_deviation',
               type='int',
               default=5,
               min=1,
               desc='deviation below which no optimization is attempted',
               long_desc='If the number of PGs are within this count then no optimization is attempted',
               runtime=True),
        Option(name='pool_ids',
               type='str',
               default='',
               desc='pools which the automatic balancing will be limited to',
               runtime=True)
    ]

    active = False
    run = True
    plans: Dict[str, Plan] = {}
    mode = ''
    optimizing = False
    last_optimize_started = ''
    last_optimize_duration = ''
    optimize_result = ''
    no_optimization_needed = False
    success_string = 'Optimization plan created successfully'
    in_progress_string = 'in progress'
    last_pg_upmap: List[Dict[str, Any]] = []
    added_pg_upmap_items: List[Dict[str, Any]] = []
    removed_pg_upmap_items: List[Dict[str, Any]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    @CLIReadCommand('balancer status')
    def show_status(self) -> Tuple[int, str, str]:
        """
        Show balancer status
        """
        s = {
            'plans': list(self.plans.keys()),
            'active': self.active,
            'last_optimize_started': self.last_optimize_started,
            'last_optimize_duration': self.last_optimize_duration,
            'optimize_result': self.optimize_result,
            'no_optimization_needed': self.no_optimization_needed,
            'mode': self.get_module_option('mode'),
        }
        return (0, json.dumps(s, indent=4, sort_keys=True), '')

    @CLIReadCommand('balancer status detail')
    def show_status_detail(self) -> Tuple[int, str, str]:
        """
        Show balancer status (detailed)
        """
        s = {
            'plans': list(self.plans.keys()),
            'active': self.active,
            'last_optimize_started': self.last_optimize_started,
            'last_optimize_duration': self.last_optimize_duration,
            'optimize_result': self.optimize_result,
            'no_optimization_needed': self.no_optimization_needed,
            'mode': self.get_module_option('mode'),
            'added_pg_upmap_items': self.added_pg_upmap_items,
            'removed_pg_upmap_items': self.removed_pg_upmap_items,
        }
        return (0, json.dumps(s, indent=4), '')

    @CLICommand('balancer mode')
    def set_mode(self, mode: Mode) -> Tuple[int, str, str]:
        """
        Set balancer mode
        """
        min_compat_client = self.get_osdmap().dump().get('require_min_compat_client', '')
        if mode == Mode.upmap:
            try:
                release = CephReleases[min_compat_client]
                if release.value < CephReleases.luminous.value:
                    warn = ('min_compat_client "%s" '
                            '< "luminous", which is required for pg-upmap. '
                            'Try "ceph osd set-require-min-compat-client luminous" '
                            'before enabling this mode' % min_compat_client)
                    return (-errno.EPERM, '', warn)
            except KeyError:
                self.log.error('Unable to apply mode {} due to unknown min_compat_client {}'.format(mode, min_compat_client))
                warn = ('Unable to apply mode {} due to unknown min_compat_client {}.'.format(mode, min_compat_client))
                return (-errno.EPERM, '', warn)
        elif mode == Mode.crush_compat:
            ms = MappingState(self.get_osdmap(),
                              self.get("pg_stats"),
                              self.get("pool_stats"),
                              'initialize compat weight-set')
            self.get_compat_weight_set_weights(ms)  # ignore error
        self.set_module_option('mode', mode.value)
        return (0, '', '')

    @CLICommand('balancer on')
    def on(self) -> Tuple[int, str, str]:
        """
        Enable automatic balancing
        """
        if not self.active:
            self.set_module_option('active', 'true')
            self.active = True
        self.event.set()
        return (0, '', '')

    @CLICommand('balancer off')
    def off(self) -> Tuple[int, str, str]:
        """
        Disable automatic balancing
        """
        if self.active:
            self.set_module_option('active', 'false')
            self.active = False
        self.event.set()
        return (0, '', '')

    @CLIReadCommand('balancer pool ls')
    def pool_ls(self) -> Tuple[int, str, str]:
        """
        List automatic balancing pools

        Note that empty list means all existing pools will be automatic balancing targets,
        which is the default behaviour of balancer.
        """
        pool_ids = cast(str, self.get_module_option('pool_ids'))
        if pool_ids == '':
            return (0, '', '')
        pool_ids = [int(p) for p in pool_ids.split(',')]
        pool_name_by_id = dict((p['pool'], p['pool_name'])
                               for p in self.get_osdmap().dump().get('pools', []))
        should_prune = False
        final_ids: List[int] = []
        final_names = []
        for p in pool_ids:
            if p in pool_name_by_id:
                final_ids.append(p)
                final_names.append(pool_name_by_id[p])
            else:
                should_prune = True
        if should_prune:  # some pools were gone, prune
            self.set_module_option('pool_ids', ','.join(str(p) for p in final_ids))
        return (0, json.dumps(sorted(final_names), indent=4, sort_keys=True), '')

    @CLICommand('balancer pool add')
    def pool_add(self, pools: Sequence[str]) -> Tuple[int, str, str]:
        """
        Enable automatic balancing for specific pools
        """
        raw_names = pools
        pool_id_by_name = dict((p['pool_name'], p['pool'])
                               for p in self.get_osdmap().dump().get('pools', []))
        invalid_names = [p for p in raw_names if p not in pool_id_by_name]
        if invalid_names:
            return (-errno.EINVAL, '', 'pool(s) %s not found' % invalid_names)
        to_add = set(str(pool_id_by_name[p]) for p in raw_names if p in pool_id_by_name)
        pool_ids = cast(str, self.get_module_option('pool_ids'))
        existing = set(pool_ids.split(',') if pool_ids else [])
        final = to_add | existing
        self.set_module_option('pool_ids', ','.join(final))
        return (0, '', '')

    @CLICommand('balancer pool rm')
    def pool_rm(self, pools: Sequence[str]) -> Tuple[int, str, str]:
        """
        Disable automatic balancing for specific pools
        """
        raw_names = pools
        existing = cast(str, self.get_module_option('pool_ids'))
        if existing == '':  # for idempotence
            return (0, '', '')
        existing = existing.split(',')
        osdmap = self.get_osdmap()
        pool_ids = [str(p['pool']) for p in osdmap.dump().get('pools', [])]
        pool_id_by_name = dict((p['pool_name'], p['pool']) for p in osdmap.dump().get('pools', []))
        final = [p for p in existing if p in pool_ids]
        to_delete = [str(pool_id_by_name[p]) for p in raw_names if p in pool_id_by_name]
        final = set(final) - set(to_delete)
        self.set_module_option('pool_ids', ','.join(final))
        return (0, '', '')

    def _state_from_option(self, option: Optional[str] = None) -> Tuple[MappingState, List[str]]:
        pools = []
        if option is None:
            ms = MappingState(self.get_osdmap(),
                              self.get("pg_stats"),
                              self.get("pool_stats"),
                              'current cluster')
        elif option in self.plans:
            plan = self.plans.get(option)
            assert plan
            pools = plan.pools
            if plan.mode == 'upmap':
                # Note that for upmap, to improve the efficiency,
                # we use a basic version of Plan without keeping the obvious
                # *redundant* MS member.
                # Hence ms might not be accurate here since we are basically
                # using an old snapshotted osdmap vs a fresh copy of pg_stats.
                # It should not be a big deal though..
                ms = MappingState(plan.osdmap,
                                  self.get("pg_stats"),
                                  self.get("pool_stats"),
                                  f'plan "{plan.name}"')
            else:
                ms = cast(MsPlan, plan).final_state()
        else:
            # not a plan, does it look like a pool?
            osdmap = self.get_osdmap()
            valid_pool_names = [p['pool_name'] for p in osdmap.dump().get('pools', [])]
            if option not in valid_pool_names:
                raise ValueError(f'option "{option}" not a plan or a pool')
            pools.append(option)
            ms = MappingState(osdmap,
                              self.get("pg_stats"),
                              self.get("pool_stats"),
                              f'pool "{option}"')
        return ms, pools

    @CLIReadCommand('balancer eval-verbose')
    def plan_eval_verbose(self, option: Optional[str] = None):
        """
        Evaluate data distribution for the current cluster or specific pool or specific
        plan (verbosely)
        """
        try:
            ms, pools = self._state_from_option(option)
            return (0, self.evaluate(ms, pools, verbose=True), '')
        except ValueError as e:
            return (-errno.EINVAL, '', str(e))

    @CLIReadCommand('balancer eval')
    def plan_eval_brief(self, option: Optional[str] = None):
        """
        Evaluate data distribution for the current cluster or specific pool or specific plan
        """
        try:
            ms, pools = self._state_from_option(option)
            return (0, self.evaluate(ms, pools, verbose=False), '')
        except ValueError as e:
            return (-errno.EINVAL, '', str(e))

    @CLIReadCommand('balancer optimize')
    def plan_optimize(self, plan: str, pools: List[str] = []) -> Tuple[int, str, str]:
        """
        Run optimizer to create a new plan
        """
        # The GIL can be release by the active balancer, so disallow when active
        if self.active:
            return (-errno.EINVAL, '', 'Balancer enabled, disable to optimize manually')
        if self.optimizing:
            return (-errno.EINVAL, '', 'Balancer finishing up....try again')
        osdmap = self.get_osdmap()
        valid_pool_names = [p['pool_name'] for p in osdmap.dump().get('pools', [])]
        invalid_pool_names = []
        for p in pools:
            if p not in valid_pool_names:
                invalid_pool_names.append(p)
        if len(invalid_pool_names):
            return (-errno.EINVAL, '', 'pools %s not found' % invalid_pool_names)
        plan_ = self.plan_create(plan, osdmap, pools)
        self.last_optimize_started = time.asctime(time.localtime())
        self.optimize_result = self.in_progress_string
        start = time.time()
        r, detail = self.optimize(plan_)
        end = time.time()
        self.last_optimize_duration = str(datetime.timedelta(seconds=(end - start)))
        if r == 0:
            # Add plan if an optimization was created
            self.optimize_result = self.success_string
            self.plans[plan] = plan_
        else:
            self.optimize_result = detail
        return (r, '', detail)

    @CLIReadCommand('balancer show')
    def plan_show(self, plan: str) -> Tuple[int, str, str]:
        """
        Show details of an optimization plan
        """
        plan_ = self.plans.get(plan)
        if not plan_:
            return (-errno.ENOENT, '', f'plan {plan} not found')
        return (0, plan_.show(), '')

    @CLICommand('balancer rm')
    def plan_rm(self, plan: str) -> Tuple[int, str, str]:
        """
        Discard an optimization plan
        """
        if plan in self.plans:
            del self.plans[plan]
        return (0, '', '')

    @CLICommand('balancer reset')
    def plan_reset(self) -> Tuple[int, str, str]:
        """
        Discard all optimization plans
        """
        self.plans = {}
        return (0, '', '')

    @CLIReadCommand('balancer dump')
    def plan_dump(self, plan: str) -> Tuple[int, str, str]:
        """
        Show an optimization plan
        """
        plan_ = self.plans.get(plan)
        if not plan_:
            return -errno.ENOENT, '', f'plan {plan} not found'
        else:
            return (0, plan_.dump(), '')

    @CLIReadCommand('balancer ls')
    def plan_ls(self) -> Tuple[int, str, str]:
        """
        List all plans
        """
        return (0, json.dumps([p for p in self.plans], indent=4, sort_keys=True), '')

    @CLIReadCommand('balancer execute')
    def plan_execute(self, plan: str) -> Tuple[int, str, str]:
        """
        Execute an optimization plan
        """
        # The GIL can be release by the active balancer, so disallow when active
        if self.active:
            return (-errno.EINVAL, '', 'Balancer enabled, disable to execute a plan')
        if self.optimizing:
            return (-errno.EINVAL, '', 'Balancer finishing up....try again')
        plan_ = self.plans.get(plan)
        if not plan_:
            return (-errno.ENOENT, '', f'plan {plan} not found')
        r, detail = self.execute(plan_)
        self.plan_rm(plan)
        return (r, '', detail)

    def shutdown(self) -> None:
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def time_permit(self) -> bool:
        local_time = time.localtime()
        time_of_day = time.strftime('%H%M', local_time)
        weekday = (local_time.tm_wday + 1) % 7  # be compatible with C
        permit = False

        def check_time(time: str, option: str):
            if len(time) != 4:
                self.log.error('invalid time for %s - expected HHMM format', option)
            try:
                datetime.time(int(time[:2]), int(time[2:]))
            except ValueError as err:
                self.log.error('invalid time for %s - %s', option, err)

        begin_time = cast(str, self.get_module_option('begin_time'))
        check_time(begin_time, 'begin_time')
        end_time = cast(str, self.get_module_option('end_time'))
        check_time(end_time, 'end_time')
        if begin_time < end_time:
            permit = begin_time <= time_of_day < end_time
        elif begin_time == end_time:
            permit = True
        else:
            permit = time_of_day >= begin_time or time_of_day < end_time
        if not permit:
            self.log.debug("should run between %s - %s, now %s, skipping",
                           begin_time, end_time, time_of_day)
            return False

        begin_weekday = cast(int, self.get_module_option('begin_weekday'))
        end_weekday = cast(int, self.get_module_option('end_weekday'))
        if begin_weekday < end_weekday:
            permit = begin_weekday <= weekday <= end_weekday
        elif begin_weekday == end_weekday:
            permit = True
        else:
            permit = weekday >= begin_weekday or weekday < end_weekday
        if not permit:
            self.log.debug("should run between weekday %d - %d, now %d, skipping",
                           begin_weekday, end_weekday, weekday)
            return False

        return True

    def serve(self) -> None:
        self.log.info('Starting')
        while self.run:
            self.active = cast(bool, self.get_module_option('active'))
            sleep_interval = cast(float, self.get_module_option('sleep_interval'))
            self.log.debug('Waking up [%s, now %s]',
                           "active" if self.active else "inactive",
                           time.strftime(TIME_FORMAT, time.localtime()))
            if self.active and self.time_permit():
                self.log.debug('Running')
                name = 'auto_%s' % time.strftime(TIME_FORMAT, time.gmtime())
                osdmap = self.get_osdmap()
                pool_ids = cast(str, self.get_module_option('pool_ids'))
                if pool_ids:
                    allow = [int(p) for p in pool_ids.split(',')]
                else:
                    allow = []
                final: List[str] = []
                if allow:
                    pools = osdmap.dump().get('pools', [])
                    valid = [p['pool'] for p in pools]
                    ids = set(allow) & set(valid)
                    if set(allow) - set(valid):  # some pools were gone, prune
                        self.set_module_option('pool_ids', ','.join(str(p) for p in ids))
                    pool_name_by_id = dict((p['pool'], p['pool_name']) for p in pools)
                    final = [pool_name_by_id[p] for p in ids if p in pool_name_by_id]
                plan = self.plan_create(name, osdmap, final)
                self.optimizing = True
                self.last_optimize_started = time.asctime(time.localtime())
                self.optimize_result = self.in_progress_string
                start = time.time()
                r, detail = self.optimize(plan)
                end = time.time()
                self.added_pg_upmap_items = [pg for pg in osdmap.dump().get('pg_upmap_items', '') if pg not in self.last_pg_upmap]
                self.removed_pg_upmap_items = [pg for pg in self.last_pg_upmap if pg not in osdmap.dump().get('pg_upmap_items', '')]
                self.last_pg_upmap = osdmap.dump().get('pg_upmap_items', '')
                self.last_optimize_duration = str(datetime.timedelta(seconds=(end - start)))
                if r == 0:
                    self.optimize_result = self.success_string
                    self.execute(plan)
                else:
                    self.optimize_result = detail
                self.optimizing = False
            self.log.debug('Sleeping for %d', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def plan_create(self, name: str, osdmap: OSDMap, pools: List[str]) -> Plan:
        mode = cast(str, self.get_module_option('mode'))
        if mode == 'upmap':
            # drop unnecessary MS member for upmap mode.
            # this way we could effectively eliminate the usage of a
            # complete pg_stats, which can become horribly inefficient
            # as pg_num grows..
            plan = Plan(name, mode, osdmap, pools)
        else:
            plan = MsPlan(name,
                          mode,
                          MappingState(osdmap,
                                       self.get("pg_stats"),
                                       self.get("pool_stats"),
                                       'plan %s initial' % name),
                          pools)
        return plan

    def calc_eval(self, ms: MappingState, pools: List[str]) -> Eval:
        pe = Eval(ms)
        pool_rule = {}
        pool_info = {}
        for p in ms.osdmap_dump.get('pools', []):
            if len(pools) and p['pool_name'] not in pools:
                continue
            # skip dead or not-yet-ready pools too
            if p['pool'] not in ms.poolids:
                continue
            pe.pool_name[p['pool']] = p['pool_name']
            pe.pool_id[p['pool_name']] = p['pool']
            pool_rule[p['pool_name']] = p['crush_rule']
            pe.pool_roots[p['pool_name']] = []
            pool_info[p['pool_name']] = p
        if len(pool_info) == 0:
            return pe
        self.log.debug('pool_name %s' % pe.pool_name)
        self.log.debug('pool_id %s' % pe.pool_id)
        self.log.debug('pools %s' % pools)
        self.log.debug('pool_rule %s' % pool_rule)

        osd_weight = {a['osd']: a['weight']
                      for a in ms.osdmap_dump.get('osds', []) if a['weight'] > 0}

        # get expected distributions by root
        actual_by_root: Dict[str, Dict[str, dict]] = {}
        rootids = ms.crush.find_takes()
        roots = []
        for rootid in rootids:
            ls = ms.osdmap.get_pools_by_take(rootid)
            want = []
            # find out roots associating with pools we are passed in
            for candidate in ls:
                if candidate in pe.pool_name:
                    want.append(candidate)
            if len(want) == 0:
                continue
            root = ms.crush.get_item_name(rootid)
            pe.root_pools[root] = []
            for poolid in want:
                pe.pool_roots[pe.pool_name[poolid]].append(root)
                pe.root_pools[root].append(pe.pool_name[poolid])
            pe.root_ids[root] = rootid
            roots.append(root)
            weight_map = ms.crush.get_take_weight_osd_map(rootid)
            adjusted_map = {
                osd: cw * osd_weight[osd]
                for osd, cw in weight_map.items() if osd in osd_weight and cw > 0
            }
            sum_w = sum(adjusted_map.values())
            assert len(adjusted_map) == 0 or sum_w > 0
            pe.target_by_root[root] = {osd: w / sum_w
                                       for osd, w in adjusted_map.items()}
            actual_by_root[root] = {
                'pgs': {},
                'objects': {},
                'bytes': {},
            }
            for osd in pe.target_by_root[root]:
                actual_by_root[root]['pgs'][osd] = 0
                actual_by_root[root]['objects'][osd] = 0
                actual_by_root[root]['bytes'][osd] = 0
            pe.total_by_root[root] = {
                'pgs': 0,
                'objects': 0,
                'bytes': 0,
            }
        self.log.debug('pool_roots %s' % pe.pool_roots)
        self.log.debug('root_pools %s' % pe.root_pools)
        self.log.debug('target_by_root %s' % pe.target_by_root)

        # pool and root actual
        for pool, pi in pool_info.items():
            poolid = pi['pool']
            pm = ms.pg_up_by_poolid[poolid]
            pgs = 0
            objects = 0
            bytes = 0
            pgs_by_osd = {}
            objects_by_osd = {}
            bytes_by_osd = {}
            for pgid, up in pm.items():
                for osd in [int(osd) for osd in up]:
                    if osd == CRUSHMap.ITEM_NONE:
                        continue
                    if osd not in pgs_by_osd:
                        pgs_by_osd[osd] = 0
                        objects_by_osd[osd] = 0
                        bytes_by_osd[osd] = 0
                    pgs_by_osd[osd] += 1
                    objects_by_osd[osd] += ms.pg_stat[pgid]['num_objects']
                    bytes_by_osd[osd] += ms.pg_stat[pgid]['num_bytes']
                    # pick a root to associate this pg instance with.
                    # note that this is imprecise if the roots have
                    # overlapping children.
                    # FIXME: divide bytes by k for EC pools.
                    for root in pe.pool_roots[pool]:
                        if osd in pe.target_by_root[root]:
                            actual_by_root[root]['pgs'][osd] += 1
                            actual_by_root[root]['objects'][osd] += ms.pg_stat[pgid]['num_objects']
                            actual_by_root[root]['bytes'][osd] += ms.pg_stat[pgid]['num_bytes']
                            pgs += 1
                            objects += ms.pg_stat[pgid]['num_objects']
                            bytes += ms.pg_stat[pgid]['num_bytes']
                            pe.total_by_root[root]['pgs'] += 1
                            pe.total_by_root[root]['objects'] += ms.pg_stat[pgid]['num_objects']
                            pe.total_by_root[root]['bytes'] += ms.pg_stat[pgid]['num_bytes']
                            break
            pe.count_by_pool[pool] = {
                'pgs': {
                    k: v
                    for k, v in pgs_by_osd.items()
                },
                'objects': {
                    k: v
                    for k, v in objects_by_osd.items()
                },
                'bytes': {
                    k: v
                    for k, v in bytes_by_osd.items()
                },
            }
            pe.actual_by_pool[pool] = {
                'pgs': {
                    k: float(v) / float(max(pgs, 1))
                    for k, v in pgs_by_osd.items()
                },
                'objects': {
                    k: float(v) / float(max(objects, 1))
                    for k, v in objects_by_osd.items()
                },
                'bytes': {
                    k: float(v) / float(max(bytes, 1))
                    for k, v in bytes_by_osd.items()
                },
            }
            pe.total_by_pool[pool] = {
                'pgs': pgs,
                'objects': objects,
                'bytes': bytes,
            }
        for root in pe.total_by_root:
            pe.count_by_root[root] = {
                'pgs': {
                    k: float(v)
                    for k, v in actual_by_root[root]['pgs'].items()
                },
                'objects': {
                    k: float(v)
                    for k, v in actual_by_root[root]['objects'].items()
                },
                'bytes': {
                    k: float(v)
                    for k, v in actual_by_root[root]['bytes'].items()
                },
            }
            pe.actual_by_root[root] = {
                'pgs': {
                    k: float(v) / float(max(pe.total_by_root[root]['pgs'], 1))
                    for k, v in actual_by_root[root]['pgs'].items()
                },
                'objects': {
                    k: float(v) / float(max(pe.total_by_root[root]['objects'], 1))
                    for k, v in actual_by_root[root]['objects'].items()
                },
                'bytes': {
                    k: float(v) / float(max(pe.total_by_root[root]['bytes'], 1))
                    for k, v in actual_by_root[root]['bytes'].items()
                },
            }
        self.log.debug('actual_by_pool %s' % pe.actual_by_pool)
        self.log.debug('actual_by_root %s' % pe.actual_by_root)

        # average and stddev and score
        pe.stats_by_root = {
            a: pe.calc_stats(
                b,
                pe.target_by_root[a],
                pe.total_by_root[a]
            ) for a, b in pe.count_by_root.items()
        }
        self.log.debug('stats_by_root %s' % pe.stats_by_root)

        # the scores are already normalized
        pe.score_by_root = {
            r: {
                'pgs': pe.stats_by_root[r]['pgs']['score'],
                'objects': pe.stats_by_root[r]['objects']['score'],
                'bytes': pe.stats_by_root[r]['bytes']['score'],
            } for r in pe.total_by_root.keys()
        }
        self.log.debug('score_by_root %s' % pe.score_by_root)

        # get the list of score metrics, comma separated
        metrics = cast(str, self.get_module_option('crush_compat_metrics')).split(',')

        # total score is just average of normalized stddevs
        pe.score = 0.0
        for r, vs in pe.score_by_root.items():
            for k, v in vs.items():
                if k in metrics:
                    pe.score += v
        pe.score /= len(metrics) * len(roots)
        return pe

    def evaluate(self, ms: MappingState, pools: List[str], verbose: bool = False) -> str:
        pe = self.calc_eval(ms, pools)
        return pe.show(verbose=verbose)

    def optimize(self, plan: Plan) -> Tuple[int, str]:
        self.log.info('Optimize plan %s' % plan.name)
        max_misplaced = cast(float, self.get_ceph_option('target_max_misplaced_ratio'))
        self.log.info('Mode %s, max misplaced %f' %
                      (plan.mode, max_misplaced))

        info = self.get('pg_status')
        unknown = info.get('unknown_pgs_ratio', 0.0)
        degraded = info.get('degraded_ratio', 0.0)
        inactive = info.get('inactive_pgs_ratio', 0.0)
        misplaced = info.get('misplaced_ratio', 0.0)
        plan.pg_status = info
        self.log.debug('unknown %f degraded %f inactive %f misplaced %g',
                       unknown, degraded, inactive, misplaced)
        if unknown > 0.0:
            detail = 'Some PGs (%f) are unknown; try again later' % unknown
            self.log.info(detail)
            return -errno.EAGAIN, detail
        elif degraded > 0.0:
            detail = 'Some objects (%f) are degraded; try again later' % degraded
            self.log.info(detail)
            return -errno.EAGAIN, detail
        elif inactive > 0.0:
            detail = 'Some PGs (%f) are inactive; try again later' % inactive
            self.log.info(detail)
            return -errno.EAGAIN, detail
        elif misplaced >= max_misplaced:
            detail = 'Too many objects (%f > %f) are misplaced; ' \
                     'try again later' % (misplaced, max_misplaced)
            self.log.info(detail)
            return -errno.EAGAIN, detail
        else:
            if plan.mode == 'upmap':
                return self.do_upmap(plan)
            elif plan.mode == 'crush-compat':
                return self.do_crush_compat(cast(MsPlan, plan))
            elif plan.mode == 'none':
                detail = 'Please do "ceph balancer mode" to choose a valid mode first'
                self.log.info('Idle')
                return -errno.ENOEXEC, detail
            else:
                detail = 'Unrecognized mode %s' % plan.mode
                self.log.info(detail)
                return -errno.EINVAL, detail

    def do_upmap(self, plan: Plan) -> Tuple[int, str]:
        self.log.info('do_upmap')
        max_optimizations = cast(float, self.get_module_option('upmap_max_optimizations'))
        max_deviation = cast(int, self.get_module_option('upmap_max_deviation'))
        osdmap_dump = plan.osdmap_dump

        if len(plan.pools):
            pools = plan.pools
        else:  # all
            pools = [str(i['pool_name']) for i in osdmap_dump.get('pools', [])]
        if len(pools) == 0:
            detail = 'No pools available'
            self.log.info(detail)
            return -errno.ENOENT, detail
        # shuffle pool list so they all get equal (in)attention
        random.shuffle(pools)
        self.log.info('pools %s' % pools)

        adjusted_pools = []
        inc = plan.inc
        total_did = 0
        left = max_optimizations
        pools_with_pg_merge = [p['pool_name'] for p in osdmap_dump.get('pools', [])
                               if p['pg_num'] > p['pg_num_target']]
        crush_rule_by_pool_name = dict((p['pool_name'], p['crush_rule'])
                                       for p in osdmap_dump.get('pools', []))
        for pool in pools:
            if pool not in crush_rule_by_pool_name:
                self.log.info('pool %s does not exist' % pool)
                continue
            if pool in pools_with_pg_merge:
                self.log.info('pool %s has pending PG(s) for merging, skipping for now' % pool)
                continue
            adjusted_pools.append(pool)
        # shuffle so all pools get equal (in)attention
        random.shuffle(adjusted_pools)
        pool_dump = osdmap_dump.get('pools', [])
        for pool in adjusted_pools:
            for p in pool_dump:
                if p['pool_name'] == pool:
                    pool_id = p['pool']
                    break

            # note that here we deliberately exclude any scrubbing pgs too
            # since scrubbing activities have significant impacts on performance
            num_pg_active_clean = 0
            for p in plan.pg_status.get('pgs_by_pool_state', []):
                pgs_pool_id = p['pool_id']
                if pgs_pool_id != pool_id:
                    continue
                for s in p['pg_state_counts']:
                    if s['state_name'] == 'active+clean':
                        num_pg_active_clean += s['count']
                        break
            available = min(left, num_pg_active_clean)
            did = plan.osdmap.calc_pg_upmaps(inc, max_deviation, available, [pool])
            total_did += did
            left -= did
            if left <= 0:
                break
        self.log.info('prepared %d/%d changes' % (total_did, max_optimizations))
        if total_did == 0:
            self.no_optimization_needed = True
            return -errno.EALREADY, 'Unable to find further optimization, ' \
                                    'or pool(s) pg_num is decreasing, ' \
                                    'or distribution is already perfect'
        return 0, ''

    def do_crush_compat(self, plan: MsPlan) -> Tuple[int, str]:
        self.log.info('do_crush_compat')
        max_iterations = cast(int, self.get_module_option('crush_compat_max_iterations'))
        if max_iterations < 1:
            return -errno.EINVAL, '"crush_compat_max_iterations" must be >= 1'
        step = cast(float, self.get_module_option('crush_compat_step'))
        if step <= 0 or step >= 1.0:
            return -errno.EINVAL, '"crush_compat_step" must be in (0, 1)'
        max_misplaced = cast(float, self.get_ceph_option('target_max_misplaced_ratio'))
        min_pg_per_osd = 2

        ms = plan.initial
        osdmap = ms.osdmap
        crush = osdmap.get_crush()
        pe = self.calc_eval(ms, plan.pools)
        min_score_to_optimize = cast(float, self.get_module_option('min_score'))
        if pe.score <= min_score_to_optimize:
            if pe.score == 0:
                detail = 'Distribution is already perfect'
            else:
                detail = 'score %f <= min_score %f, will not optimize' \
                         % (pe.score, min_score_to_optimize)
            self.log.info(detail)
            return -errno.EALREADY, detail

        # get current osd reweights
        orig_osd_weight = {a['osd']: a['weight']
                           for a in ms.osdmap_dump.get('osds', [])}

        # get current compat weight-set weights
        orig_ws = self.get_compat_weight_set_weights(ms)
        if not orig_ws:
            return -errno.EAGAIN, 'compat weight-set not available'
        orig_ws = {a: b for a, b in orig_ws.items() if a >= 0}

        # Make sure roots don't overlap their devices.  If so, we
        # can't proceed.
        roots = list(pe.target_by_root.keys())
        self.log.debug('roots %s', roots)
        visited: Dict[int, str] = {}
        overlap: Dict[int, List[str]] = {}
        for root, wm in pe.target_by_root.items():
            for osd in wm:
                if osd in visited:
                    if osd not in overlap:
                        overlap[osd] = [visited[osd]]
                    overlap[osd].append(root)
                visited[osd] = root
        if len(overlap) > 0:
            detail = 'Some osds belong to multiple subtrees: %s' % \
                     overlap
            self.log.error(detail)
            return -errno.EOPNOTSUPP, detail

        # rebalance by pgs, objects, or bytes
        metrics = cast(str, self.get_module_option('crush_compat_metrics')).split(',')
        key = metrics[0]  # balancing using the first score metric
        if key not in ['pgs', 'bytes', 'objects']:
            self.log.warning("Invalid crush_compat balancing key %s. Using 'pgs'." % key)
            key = 'pgs'

        # go
        best_ws = copy.deepcopy(orig_ws)
        best_ow = copy.deepcopy(orig_osd_weight)
        best_pe = pe
        left = max_iterations
        bad_steps = 0
        next_ws = copy.deepcopy(best_ws)
        next_ow = copy.deepcopy(best_ow)
        while left > 0:
            # adjust
            self.log.debug('best_ws %s' % best_ws)
            random.shuffle(roots)
            for root in roots:
                pools = best_pe.root_pools[root]
                osds = len(best_pe.target_by_root[root])
                min_pgs = osds * min_pg_per_osd
                if best_pe.total_by_root[root][key] < min_pgs:
                    self.log.info('Skipping root %s (pools %s), total pgs %d '
                                  '< minimum %d (%d per osd)',
                                  root, pools,
                                  best_pe.total_by_root[root][key],
                                  min_pgs, min_pg_per_osd)
                    continue
                self.log.info('Balancing root %s (pools %s) by %s' %
                              (root, pools, key))
                target = best_pe.target_by_root[root]
                actual = best_pe.actual_by_root[root][key]
                queue = sorted(actual.keys(),
                               key=lambda osd: -abs(target[osd] - actual[osd]))
                for osd in queue:
                    if orig_osd_weight[osd] == 0:
                        self.log.debug('skipping out osd.%d', osd)
                    else:
                        deviation = target[osd] - actual[osd]
                        if deviation == 0:
                            break
                        self.log.debug('osd.%d deviation %f', osd, deviation)
                        weight = best_ws[osd]
                        ow = orig_osd_weight[osd]
                        if actual[osd] > 0:
                            calc_weight = target[osd] / actual[osd] * weight * ow
                        else:
                            # for newly created osds, reset calc_weight at target value
                            # this way weight-set will end up absorbing *step* of its
                            # target (final) value at the very beginning and slowly catch up later.
                            # note that if this turns out causing too many misplaced
                            # pgs, then we'll reduce step and retry
                            calc_weight = target[osd]
                        new_weight = weight * (1.0 - step) + calc_weight * step
                        self.log.debug('Reweight osd.%d %f -> %f', osd, weight,
                                       new_weight)
                        next_ws[osd] = new_weight
                        if ow < 1.0:
                            new_ow = min(1.0, max(step + (1.0 - step) * ow,
                                                  ow + .005))
                            self.log.debug('Reweight osd.%d reweight %f -> %f',
                                           osd, ow, new_ow)
                            next_ow[osd] = new_ow

                # normalize weights under this root
                root_weight = crush.get_item_weight(pe.root_ids[root])
                root_sum = sum(b for a, b in next_ws.items()
                               if a in target.keys())
                if root_sum > 0 and root_weight > 0:
                    factor = root_sum / root_weight
                    self.log.debug('normalizing root %s %d, weight %f, '
                                   'ws sum %f, factor %f',
                                   root, pe.root_ids[root], root_weight,
                                   root_sum, factor)
                    for osd in actual.keys():
                        next_ws[osd] = next_ws[osd] / factor

            # recalc
            plan.compat_ws = copy.deepcopy(next_ws)
            next_ms = plan.final_state()
            next_pe = self.calc_eval(next_ms, plan.pools)
            next_misplaced = next_ms.calc_misplaced_from(ms)
            self.log.debug('Step result score %f -> %f, misplacing %f',
                           best_pe.score, next_pe.score, next_misplaced)

            if next_misplaced > max_misplaced:
                if best_pe.score < pe.score:
                    self.log.debug('Step misplaced %f > max %f, stopping',
                                   next_misplaced, max_misplaced)
                    break
                step /= 2.0
                next_ws = copy.deepcopy(best_ws)
                next_ow = copy.deepcopy(best_ow)
                self.log.debug('Step misplaced %f > max %f, reducing step to %f',
                               next_misplaced, max_misplaced, step)
            else:
                if next_pe.score > best_pe.score * 1.0001:
                    bad_steps += 1
                    if bad_steps < 5 and random.randint(0, 100) < 70:
                        self.log.debug('Score got worse, taking another step')
                    else:
                        step /= 2.0
                        next_ws = copy.deepcopy(best_ws)
                        next_ow = copy.deepcopy(best_ow)
                        self.log.debug('Score got worse, trying smaller step %f',
                                       step)
                else:
                    bad_steps = 0
                    best_pe = next_pe
                    best_ws = copy.deepcopy(next_ws)
                    best_ow = copy.deepcopy(next_ow)
                    if best_pe.score == 0:
                        break
            left -= 1

        # allow a small regression if we are phasing out osd weights
        fudge = 0.0
        if best_ow != orig_osd_weight:
            fudge = .001

        if best_pe.score < pe.score + fudge:
            self.log.info('Success, score %f -> %f', pe.score, best_pe.score)
            plan.compat_ws = best_ws
            for osd, w in best_ow.items():
                if w != orig_osd_weight[osd]:
                    self.log.debug('osd.%d reweight %f', osd, w)
                    plan.osd_weights[osd] = w
            return 0, ''
        else:
            self.log.info('Failed to find further optimization, score %f',
                          pe.score)
            plan.compat_ws = {}
            return -errno.EDOM, 'Unable to find further optimization, ' \
                                'change balancer mode and retry might help'

    def get_compat_weight_set_weights(self, ms: MappingState):
        have_choose_args = CRUSHMap.have_default_choose_args(ms.crush_dump)
        if have_choose_args:
            # get number of buckets in choose_args
            choose_args_len = len(CRUSHMap.get_default_choose_args(ms.crush_dump))
        if not have_choose_args or choose_args_len != len(ms.crush_dump['buckets']):
            # enable compat weight-set first
            self.log.debug('no choose_args or all buckets do not have weight-sets')
            self.log.debug('ceph osd crush weight-set create-compat')
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush weight-set create-compat',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error creating compat weight-set')
                return

            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush dump',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error dumping crush map')
                return
            try:
                crushmap = json.loads(outb)
            except json.JSONDecodeError:
                raise RuntimeError('unable to parse crush map')
        else:
            crushmap = ms.crush_dump

        raw = CRUSHMap.get_default_choose_args(crushmap)
        weight_set = {}
        for b in raw:
            bucket = None
            for t in crushmap['buckets']:
                if t['id'] == b['bucket_id']:
                    bucket = t
                    break
            if not bucket:
                raise RuntimeError('could not find bucket %s' % b['bucket_id'])
            self.log.debug('bucket items %s' % bucket['items'])
            self.log.debug('weight set %s' % b['weight_set'][0])
            if len(bucket['items']) != len(b['weight_set'][0]):
                raise RuntimeError('weight-set size does not match bucket items')
            for pos in range(len(bucket['items'])):
                weight_set[bucket['items'][pos]['id']] = b['weight_set'][0][pos]

        self.log.debug('weight_set weights %s' % weight_set)
        return weight_set

    def do_crush(self) -> None:
        self.log.info('do_crush (not yet implemented)')

    def do_osd_weight(self) -> None:
        self.log.info('do_osd_weight (not yet implemented)')

    def execute(self, plan: Plan) -> Tuple[int, str]:
        self.log.info('Executing plan %s' % plan.name)

        commands = []

        # compat weight-set
        if len(plan.compat_ws):
            ms_plan = cast(MsPlan, plan)
            if not CRUSHMap.have_default_choose_args(ms_plan.initial.crush_dump):
                self.log.debug('ceph osd crush weight-set create-compat')
                result = CommandResult('')
                self.send_command(result, 'mon', '', json.dumps({
                    'prefix': 'osd crush weight-set create-compat',
                    'format': 'json',
                }), '')
                r, outb, outs = result.wait()
                if r != 0:
                    self.log.error('Error creating compat weight-set')
                    return r, outs

        for osd, weight in plan.compat_ws.items():
            self.log.info('ceph osd crush weight-set reweight-compat osd.%d %f',
                          osd, weight)
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush weight-set reweight-compat',
                'format': 'json',
                'item': 'osd.%d' % osd,
                'weight': [weight],
            }), '')
            commands.append(result)

        # new_weight
        reweightn = {}
        for osd, weight in plan.osd_weights.items():
            reweightn[str(osd)] = str(int(weight * float(0x10000)))
        if len(reweightn):
            self.log.info('ceph osd reweightn %s', reweightn)
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd reweightn',
                'format': 'json',
                'weights': json.dumps(reweightn),
            }), '')
            commands.append(result)

        # upmap
        incdump = plan.inc.dump()
        for item in incdump.get('new_pg_upmap', []):
            self.log.info('ceph osd pg-upmap %s mappings %s', item['pgid'],
                          item['osds'])
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pg-upmap',
                'format': 'json',
                'pgid': item['pgid'],
                'id': item['osds'],
            }), 'foo')
            commands.append(result)

        for pgid in incdump.get('old_pg_upmap', []):
            self.log.info('ceph osd rm-pg-upmap %s', pgid)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd rm-pg-upmap',
                'format': 'json',
                'pgid': pgid,
            }), 'foo')
            commands.append(result)

        for item in incdump.get('new_pg_upmap_items', []):
            self.log.info('ceph osd pg-upmap-items %s mappings %s', item['pgid'],
                          item['mappings'])
            osdlist = []
            for m in item['mappings']:
                osdlist += [m['from'], m['to']]
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pg-upmap-items',
                'format': 'json',
                'pgid': item['pgid'],
                'id': osdlist,
            }), 'foo')
            commands.append(result)

        for pgid in incdump.get('old_pg_upmap_items', []):
            self.log.info('ceph osd rm-pg-upmap-items %s', pgid)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd rm-pg-upmap-items',
                'format': 'json',
                'pgid': pgid,
            }), 'foo')
            commands.append(result)

        # wait for commands
        self.log.debug('commands %s' % commands)
        for result in commands:
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('execute error: r = %d, detail = %s' % (r, outs))
                return r, outs
        self.log.debug('done')
        return 0, ''

    def gather_telemetry(self) -> Dict[str, Any]:
        return {
            'active': self.active,
            'mode': self.mode,
        }
