
"""
Balance PG distribution across OSDs.
"""

import copy
import errno
import json
import math
import random
import six
import time
from mgr_module import MgrModule, CommandResult
from threading import Event
from mgr_module import CRUSHMap

TIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

class MappingState:
    def __init__(self, osdmap, pg_dump, desc=''):
        self.desc = desc
        self.osdmap = osdmap
        self.osdmap_dump = self.osdmap.dump()
        self.crush = osdmap.get_crush()
        self.crush_dump = self.crush.dump()
        self.pg_dump = pg_dump
        self.pg_stat = {
            i['pgid']: i['stat_sum'] for i in pg_dump.get('pg_stats', [])
        }
        osd_poolids = [p['pool'] for p in self.osdmap_dump.get('pools', [])]
        pg_poolids = [p['poolid'] for p in pg_dump.get('pool_stats', [])]
        self.poolids = set(osd_poolids) & set(pg_poolids)
        self.pg_up = {}
        self.pg_up_by_poolid = {}
        for poolid in self.poolids:
            self.pg_up_by_poolid[poolid] = osdmap.map_pool_pgs_up(poolid)
            for a,b in six.iteritems(self.pg_up_by_poolid[poolid]):
                self.pg_up[a] = b

    def calc_misplaced_from(self, other_ms):
        num = len(other_ms.pg_up)
        misplaced = 0
        for pgid, before in six.iteritems(other_ms.pg_up):
            if before != self.pg_up.get(pgid, []):
                misplaced += 1
        if num > 0:
            return float(misplaced) / float(num)
        return 0.0

class Plan:
    def __init__(self, name, ms, pools):
        self.mode = 'unknown'
        self.name = name
        self.initial = ms
        self.pools = pools

        self.osd_weights = {}
        self.compat_ws = {}
        self.inc = ms.osdmap.new_incremental()

    def final_state(self):
        self.inc.set_osd_reweights(self.osd_weights)
        self.inc.set_crush_compat_weight_set_weights(self.compat_ws)
        return MappingState(self.initial.osdmap.apply_incremental(self.inc),
                            self.initial.pg_dump,
                            'plan %s final' % self.name)

    def dump(self):
        return json.dumps(self.inc.dump(), indent=4)

    def show(self):
        ls = []
        ls.append('# starting osdmap epoch %d' % self.initial.osdmap.get_epoch())
        ls.append('# starting crush version %d' %
                  self.initial.osdmap.get_crush_version())
        ls.append('# mode %s' % self.mode)
        if len(self.compat_ws) and \
           not CRUSHMap.have_default_choose_args(self.initial.crush_dump):
            ls.append('ceph osd crush weight-set create-compat')
        for osd, weight in six.iteritems(self.compat_ws):
            ls.append('ceph osd crush weight-set reweight-compat %s %f' %
                      (osd, weight))
        for osd, weight in six.iteritems(self.osd_weights):
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
    def __init__(self, ms):
        self.ms = ms
        self.root_ids = {}        # root name -> id
        self.pool_name = {}       # pool id -> pool name
        self.pool_id = {}         # pool name -> id
        self.pool_roots = {}      # pool name -> root name
        self.root_pools = {}      # root name -> pools
        self.target_by_root = {}  # root name -> target weight map
        self.count_by_pool = {}
        self.count_by_root = {}
        self.actual_by_pool = {}  # pool -> by_* -> actual weight map
        self.actual_by_root = {}  # pool -> by_* -> actual weight map
        self.total_by_pool = {}   # pool -> by_* -> total
        self.total_by_root = {}   # root -> by_* -> total
        self.stats_by_pool = {}   # pool -> by_* -> stddev or avg -> value
        self.stats_by_root = {}   # root -> by_* -> stddev or avg -> value

        self.score_by_pool = {}
        self.score_by_root = {}

        self.score = 0.0

    def show(self, verbose=False):
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
        r = {}
        for t in ('pgs', 'objects', 'bytes'):
            if total[t] == 0:
                r[t] = {
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

            for k, v in six.iteritems(count[t]):
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
                    score += target[k] * (math.erf(((adjusted - avg)/avg) / math.sqrt(2.0)))
                    sum_weight += target[k]
                dev += (avg - adjusted) * (avg - adjusted)
            stddev = math.sqrt(dev / float(max(num - 1, 1)))
            score = score / max(sum_weight, 1)
            r[t] = {
                'avg': avg,
                'stddev': stddev,
                'sum_weight': sum_weight,
                'score': score,
            }
        return r

class Module(MgrModule):
    MODULE_OPTIONS = [
        {
            'name': 'active',
            'type': 'bool',
            'default': False,
            'desc': 'automatically balance PGs across cluster',
            'runtime': True,
        },
        {
            'name': 'begin_time',
            'type': 'str',
            'default': '0000',
            'desc': 'beginning time of day to automatically balance',
            'long_desc': 'This is a time of day in the format HHMM.',
            'runtime': True,
        },
        {
            'name': 'end_time',
            'type': 'str',
            'default': '2400',
            'desc': 'ending time of day to automatically balance',
            'long_desc': 'This is a time of day in the format HHMM.',
            'runtime': True,
        },
        {
            'name': 'begin_weekday',
            'type': 'uint',
            'default': 0,
            'desc': 'Restrict automatic balancing to this day of the week or later',
            'long_desc': '0 or 7 = Sunday, 1 = Monday, etc.',
            'runtime': True,
        },
        {
            'name': 'end_weekday',
            'type': 'uint',
            'default': 7,
            'desc': 'Restrict automatic balancing to days of the week earlier than this',
            'long_desc': '0 or 7 = Sunday, 1 = Monday, etc.',
            'runtime': True,
        },
        {
            'name': 'crush_compat_max_iterations',
            'type': 'uint',
            'default': 25,
            'min': '1',
            'max': '250',
            'desc': 'maximum number of iterations to attempt optimization',
            'runtime': True,
        },
        {
            'name': 'crush_compat_metrics',
            'type': 'str',
            'default': 'pgs,objects,bytes',
            'desc': 'metrics with which to calculate OSD utilization',
            'long_desc': 'Value is a list of one or more of "pgs", "objects", or "bytes", and indicates which metrics to use to balance utilization.',
            'runtime': True,
        },
        {
            'name': 'crush_compat_step',
            'type': 'float',
            'default': .5,
            'min': '.001',
            'max': '.999',
            'desc': 'aggressiveness of optimization',
            'long_desc': '.99 is very aggressive, .01 is less aggressive',
            'runtime': True,
        },
        {
            'name': 'min_score',
            'type': 'float',
            'default': 0,
            'desc': 'minimum score, below which no optimization is attempted',
            'runtime': True,
        },
        {
            'name': 'mode',
            'desc': 'Balancer mode',
            'default': 'none',
            'enum_allowed': ['none', 'crush-compat', 'upmap'],
            'runtime': True,
        },
        {
            'name': 'sleep_interval',
            'type': 'secs',
            'default': 60,
            'desc': 'how frequently to wake up and attempt optimization',
            'runtime': True,
        },
        {
            'name': 'upmap_max_iterations',
            'type': 'uint',
            'default': 10,
            'desc': 'maximum upmap optimization iterations',
            'runtime': True,
        },
        {
            'name': 'upmap_max_deviation',
            'type': 'float',
            'default': .01,
            'min': 0,
            'max': 1,
            'desc': 'deviation below which no optimization is attempted',
            'long_desc': 'If the ratio between the fullest and least-full OSD is below this value then we stop trying to optimize placement.',
            'runtime': True,
        },
        {
            'name': 'pool_ids',
            'type': 'str',
            'default': '',
            'desc': 'pools which the automatic balancing will be limited to',
            'runtime': True,
        },
    ]

    COMMANDS = [
        {
            "cmd": "balancer status",
            "desc": "Show balancer status",
            "perm": "r",
        },
        {
            "cmd": "balancer mode name=mode,type=CephChoices,strings=none|crush-compat|upmap",
            "desc": "Set balancer mode",
            "perm": "rw",
        },
        {
            "cmd": "balancer on",
            "desc": "Enable automatic balancing",
            "perm": "rw",
        },
        {
            "cmd": "balancer off",
            "desc": "Disable automatic balancing",
            "perm": "rw",
        },
        {
            "cmd": "balancer pool ls",
            "desc": "List automatic balancing pools. "
                    "Note that empty list means all existing pools will be automatic balancing targets, "
                    "which is the default behaviour of balancer.",
            "perm": "r",
        },
        {
            "cmd": "balancer pool add name=pools,type=CephString,n=N",
            "desc": "Enable automatic balancing for specific pools",
            "perm": "rw",
        },
        {
            "cmd": "balancer pool rm name=pools,type=CephString,n=N",
            "desc": "Disable automatic balancing for specific pools",
            "perm": "rw",
        },
        {
            "cmd": "balancer eval name=option,type=CephString,req=false",
            "desc": "Evaluate data distribution for the current cluster or specific pool or specific plan",
            "perm": "r",
        },
        {
            "cmd": "balancer eval-verbose name=option,type=CephString,req=false",
            "desc": "Evaluate data distribution for the current cluster or specific pool or specific plan (verbosely)",
            "perm": "r",
        },
        {
            "cmd": "balancer optimize name=plan,type=CephString name=pools,type=CephString,n=N,req=false",
            "desc": "Run optimizer to create a new plan",
            "perm": "rw",
        },
        {
            "cmd": "balancer show name=plan,type=CephString",
            "desc": "Show details of an optimization plan",
            "perm": "r",
        },
        {
            "cmd": "balancer rm name=plan,type=CephString",
            "desc": "Discard an optimization plan",
            "perm": "rw",
        },
        {
            "cmd": "balancer reset",
            "desc": "Discard all optimization plans",
            "perm": "rw",
        },
        {
            "cmd": "balancer dump name=plan,type=CephString",
            "desc": "Show an optimization plan",
            "perm": "r",
        },
        {
            "cmd": "balancer ls",
            "desc": "List all plans",
            "perm": "r",
        },
        {
            "cmd": "balancer execute name=plan,type=CephString",
            "desc": "Execute an optimization plan",
            "perm": "rw",
        },
    ]
    active = False
    run = True
    plans = {}
    mode = ''

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def handle_command(self, inbuf, command):
        self.log.warn("Handling command: '%s'" % str(command))
        if command['prefix'] == 'balancer status':
            s = {
                'plans': list(self.plans.keys()),
                'active': self.active,
                'mode': self.get_module_option('mode'),
            }
            return (0, json.dumps(s, indent=4), '')
        elif command['prefix'] == 'balancer mode':
            if command['mode'] == 'upmap':
                min_compat_client = self.get_osdmap().dump().get('require_min_compat_client', '')
                if min_compat_client < 'luminous': # works well because version is alphabetized..
                    warn = 'min_compat_client "%s" ' \
                           '< "luminous", which is required for pg-upmap. ' \
                           'Try "ceph osd set-require-min-compat-client luminous" ' \
                           'before enabling this mode' % min_compat_client
                    return (-errno.EPERM, '', warn)
            self.set_module_option('mode', command['mode'])
            return (0, '', '')
        elif command['prefix'] == 'balancer on':
            if not self.active:
                self.set_module_option('active', 'true')
                self.active = True
            self.event.set()
            return (0, '', '')
        elif command['prefix'] == 'balancer off':
            if self.active:
                self.set_module_option('active', 'false')
                self.active = False
            self.event.set()
            return (0, '', '')
        elif command['prefix'] == 'balancer pool ls':
            pool_ids = self.get_module_option('pool_ids')
            if pool_ids is '':
                return (0, '', '')
            pool_ids = pool_ids.split(',')
            pool_ids = [int(p) for p in pool_ids]
            pool_name_by_id = dict((p['pool'], p['pool_name']) for p in self.get_osdmap().dump().get('pools', []))
            should_prune = False
            final_ids = []
            final_names = []
            for p in pool_ids:
                if p in pool_name_by_id:
                    final_ids.append(p)
                    final_names.append(pool_name_by_id[p])
                else:
                    should_prune = True
            if should_prune: # some pools were gone, prune
                self.set_module_option('pool_ids', ','.join(final_ids))
            return (0, json.dumps(final_names, indent=4), '')
        elif command['prefix'] == 'balancer pool add':
            raw_names = command['pools']
            pool_id_by_name = dict((p['pool_name'], p['pool']) for p in self.get_osdmap().dump().get('pools', []))
            invalid_names = [p for p in raw_names if p not in pool_id_by_name]
            if invalid_names:
                return (-errno.EINVAL, '', 'pool(s) %s not found' % invalid_names)
            to_add = [str(pool_id_by_name[p]) for p in raw_names if p in pool_id_by_name]
            existing = self.get_module_option('pool_ids')
            final = to_add
            if existing is not '':
                existing = existing.split(',')
                final = set(to_add) | set(existing)
            self.set_module_option('pool_ids', ','.join(final))
            return (0, '', '')
        elif command['prefix'] == 'balancer pool rm':
            raw_names = command['pools']
            existing = self.get_module_option('pool_ids')
            if existing is '': # for idempotence
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
        elif command['prefix'] == 'balancer eval' or command['prefix'] == 'balancer eval-verbose':
            verbose = command['prefix'] == 'balancer eval-verbose'
            pools = []
            if 'option' in command:
                plan = self.plans.get(command['option'])
                if not plan:
                    # not a plan, does it look like a pool?
                    osdmap = self.get_osdmap()
                    valid_pool_names = [p['pool_name'] for p in osdmap.dump().get('pools', [])]
                    option = command['option']
                    if option not in valid_pool_names:
                         return (-errno.EINVAL, '', 'option "%s" not a plan or a pool' % option)
                    pools.append(option)
                    ms = MappingState(osdmap, self.get("pg_dump"), 'pool "%s"' % option)
                else:
                    pools = plan.pools
                    ms = plan.final_state()
            else:
                ms = MappingState(self.get_osdmap(),
                                  self.get("pg_dump"),
                                  'current cluster')
            return (0, self.evaluate(ms, pools, verbose=verbose), '')
        elif command['prefix'] == 'balancer optimize':
            pools = []
            if 'pools' in command:
                pools = command['pools']
            osdmap = self.get_osdmap()
            valid_pool_names = [p['pool_name'] for p in osdmap.dump().get('pools', [])]
            invalid_pool_names = []
            for p in pools:
                if p not in valid_pool_names:
                    invalid_pool_names.append(p)
            if len(invalid_pool_names):
                return (-errno.EINVAL, '', 'pools %s not found' % invalid_pool_names)
            plan = self.plan_create(command['plan'], osdmap, pools)
            r, detail = self.optimize(plan)
            # remove plan if we are currently unable to find an optimization
            # or distribution is already perfect
            if r:
                self.plan_rm(command['plan'])
            return (r, '', detail)
        elif command['prefix'] == 'balancer rm':
            self.plan_rm(command['plan'])
            return (0, '', '')
        elif command['prefix'] == 'balancer reset':
            self.plans = {}
            return (0, '', '')
        elif command['prefix'] == 'balancer ls':
            return (0, json.dumps([p for p in self.plans], indent=4), '')
        elif command['prefix'] == 'balancer dump':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            return (0, plan.dump(), '')
        elif command['prefix'] == 'balancer show':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            return (0, plan.show(), '')
        elif command['prefix'] == 'balancer execute':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            r, detail = self.execute(plan)
            self.plan_rm(command['plan'])
            return (r, '', detail)
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def time_permit(self):
        local_time = time.localtime()
        time_of_day = time.strftime('%H%M', local_time)
        weekday = local_time.tm_wday
        permit = False

        begin_time = self.get_module_option('begin_time')
        end_time = self.get_module_option('end_time')
        if begin_time <= end_time:
            permit = begin_time <= time_of_day < end_time
        else:
            permit = time_of_day >= begin_time or time_of_day < end_time
        if not permit:
            self.log.debug("should run between %s - %s, now %s, skipping",
                           begin_time, end_time, time_of_day)
            return False

        begin_weekday = self.get_module_option('begin_weekday')
        end_weekday = self.get_module_option('end_weekday')
        if begin_weekday <= end_weekday:
            permit = begin_weekday <= weekday < end_weekday
        else:
            permit = weekday >= begin_weekday or weekday < end_weekday
        if not permit:
            self.log.debug("should run between weekday %d - %d, now %d, skipping",
                           begin_weekday, end_weekday, weekday)
            return False

        return True

    def serve(self):
        self.log.info('Starting')
        while self.run:
            self.active = self.get_module_option('active')
            sleep_interval = self.get_module_option('sleep_interval')
            self.log.debug('Waking up [%s, now %s]',
                           "active" if self.active else "inactive",
                           time.strftime(TIME_FORMAT, time.localtime()))
            if self.active and self.time_permit():
                self.log.debug('Running')
                name = 'auto_%s' % time.strftime(TIME_FORMAT, time.gmtime())
                osdmap = self.get_osdmap()
                allow = self.get_module_option('pool_ids')
                final = []
                if allow is not '':
                    allow = allow.split(',')
                    valid = [str(p['pool']) for p in osdmap.dump().get('pools', [])]
                    final = set(allow) & set(valid)
                    if set(allow) - set(valid): # some pools were gone, prune
                        self.set_module_option('pool_ids', ','.join(final))
                    pool_name_by_id = dict((p['pool'], p['pool_name']) for p in osdmap.dump().get('pools', []))
                    final = [int(p) for p in final]
                    final = [pool_name_by_id[p] for p in final if p in pool_name_by_id]
                plan = self.plan_create(name, osdmap, final)
                r, detail = self.optimize(plan)
                if r == 0:
                    self.execute(plan)
                self.plan_rm(name)
            self.log.debug('Sleeping for %d', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def plan_create(self, name, osdmap, pools):
        plan = Plan(name,
                    MappingState(osdmap,
                                 self.get("pg_dump"),
                                 'plan %s initial' % name),
                    pools)
        self.plans[name] = plan
        return plan

    def plan_rm(self, name):
        if name in self.plans:
            del self.plans[name]

    def calc_eval(self, ms, pools):
        pe = Eval(ms)
        pool_rule = {}
        pool_info = {}
        for p in ms.osdmap_dump.get('pools',[]):
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

        osd_weight = { a['osd']: a['weight']
                       for a in ms.osdmap_dump.get('osds',[]) if a['weight'] > 0 }

        # get expected distributions by root
        actual_by_root = {}
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
                for osd,cw in six.iteritems(weight_map) if osd in osd_weight and cw > 0
            }
            sum_w = sum(adjusted_map.values())
            assert len(adjusted_map) == 0 or sum_w > 0
            pe.target_by_root[root] = { osd: w / sum_w
                                        for osd,w in six.iteritems(adjusted_map) }
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
        for pool, pi in six.iteritems(pool_info):
            poolid = pi['pool']
            pm = ms.pg_up_by_poolid[poolid]
            pgs = 0
            objects = 0
            bytes = 0
            pgs_by_osd = {}
            objects_by_osd = {}
            bytes_by_osd = {}
            for root in pe.pool_roots[pool]:
                for osd in pe.target_by_root[root]:
                    pgs_by_osd[osd] = 0
                    objects_by_osd[osd] = 0
                    bytes_by_osd[osd] = 0
            for pgid, up in six.iteritems(pm):
                for osd in [int(osd) for osd in up]:
                    if osd == CRUSHMap.ITEM_NONE:
                        continue
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
                    for k, v in six.iteritems(pgs_by_osd)
                },
                'objects': {
                    k: v
                    for k, v in six.iteritems(objects_by_osd)
                },
                'bytes': {
                    k: v
                    for k, v in six.iteritems(bytes_by_osd)
                },
            }
            pe.actual_by_pool[pool] = {
                'pgs': {
                    k: float(v) / float(max(pgs, 1))
                    for k, v in six.iteritems(pgs_by_osd)
                },
                'objects': {
                    k: float(v) / float(max(objects, 1))
                    for k, v in six.iteritems(objects_by_osd)
                },
                'bytes': {
                    k: float(v) / float(max(bytes, 1))
                    for k, v in six.iteritems(bytes_by_osd)
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
                    for k, v in six.iteritems(actual_by_root[root]['pgs'])
                },
                'objects': {
                    k: float(v)
                    for k, v in six.iteritems(actual_by_root[root]['objects'])
                },
                'bytes': {
                    k: float(v)
                    for k, v in six.iteritems(actual_by_root[root]['bytes'])
                },
            }
            pe.actual_by_root[root] = {
                'pgs': {
                    k: float(v) / float(max(pe.total_by_root[root]['pgs'], 1))
                    for k, v in six.iteritems(actual_by_root[root]['pgs'])
                },
                'objects': {
                    k: float(v) / float(max(pe.total_by_root[root]['objects'], 1))
                    for k, v in six.iteritems(actual_by_root[root]['objects'])
                },
                'bytes': {
                    k: float(v) / float(max(pe.total_by_root[root]['bytes'], 1))
                    for k, v in six.iteritems(actual_by_root[root]['bytes'])
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
            ) for a, b in six.iteritems(pe.count_by_root)
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
        metrics = self.get_module_option('crush_compat_metrics').split(',')

        # total score is just average of normalized stddevs
        pe.score = 0.0
        for r, vs in six.iteritems(pe.score_by_root):
            for k, v in six.iteritems(vs):
                if k in metrics:
                    pe.score += v
        pe.score /= len(metrics) * len(roots)
        return pe

    def evaluate(self, ms, pools, verbose=False):
        pe = self.calc_eval(ms, pools)
        return pe.show(verbose=verbose)

    def optimize(self, plan):
        self.log.info('Optimize plan %s' % plan.name)
        plan.mode = self.get_module_option('mode')
        max_misplaced = float(self.get_ceph_option('target_max_misplaced_ratio'))
        self.log.info('Mode %s, max misplaced %f' %
                      (plan.mode, max_misplaced))

        info = self.get('pg_status')
        unknown = info.get('unknown_pgs_ratio', 0.0)
        degraded = info.get('degraded_ratio', 0.0)
        inactive = info.get('inactive_pgs_ratio', 0.0)
        misplaced = info.get('misplaced_ratio', 0.0)
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
                return self.do_crush_compat(plan)
            elif plan.mode == 'none':
                detail = 'Please do "ceph balancer mode" to choose a valid mode first'
                self.log.info('Idle')
                return -errno.ENOEXEC, detail
            else:
                detail = 'Unrecognized mode %s' % plan.mode
                self.log.info(detail)
                return -errno.EINVAL, detail
        ##

    def do_upmap(self, plan):
        self.log.info('do_upmap')
        max_iterations = self.get_module_option('upmap_max_iterations')
        max_deviation = self.get_module_option('upmap_max_deviation')

        ms = plan.initial
        if len(plan.pools):
            pools = plan.pools
        else: # all
            pools = [str(i['pool_name']) for i in ms.osdmap_dump.get('pools',[])]
        if len(pools) == 0:
            detail = 'No pools available'
            self.log.info(detail)
            return -errno.ENOENT, detail

        inc = plan.inc
        total_did = 0
        left = max_iterations
        osdmap_dump = self.get_osdmap().dump()
        pools_with_pg_merge = [p['pool_name'] for p in osdmap_dump.get('pools', [])
                               if p['pg_num'] > p['pg_num_target']]
        crush_rule_by_pool_name = dict((p['pool_name'], p['crush_rule']) for p in osdmap_dump.get('pools', []))
        pools_by_crush_rule = {} # group pools by crush_rule
        for pool in pools:
            if pool not in crush_rule_by_pool_name:
                self.log.info('pool %s does not exist' % pool)
                continue
            if pool in pools_with_pg_merge:
                self.log.info('pool %s has pending PG(s) for merging, skipping for now' % pool)
                continue
            crush_rule = crush_rule_by_pool_name[pool]
            if crush_rule not in pools_by_crush_rule:
                pools_by_crush_rule[crush_rule] = []
            pools_by_crush_rule[crush_rule].append(pool)
        classified_pools = pools_by_crush_rule.values()
        # shuffle so all pools get equal (in)attention
        random.shuffle(classified_pools)
        for it in classified_pools:
            did = ms.osdmap.calc_pg_upmaps(inc, max_deviation, left, it)
            total_did += did
            left -= did
            if left <= 0:
                break
        self.log.info('prepared %d/%d changes' % (total_did, max_iterations))
        if total_did == 0:
            return -errno.EALREADY, 'Unable to find further optimization, ' \
                                    'or pool(s)\' pg_num is decreasing, ' \
                                    'or distribution is already perfect'
        return 0, ''

    def do_crush_compat(self, plan):
        self.log.info('do_crush_compat')
        max_iterations = self.get_module_option('crush_compat_max_iterations')
        if max_iterations < 1:
            return -errno.EINVAL, '"crush_compat_max_iterations" must be >= 1'
        step = self.get_module_option('crush_compat_step')
        if step <= 0 or step >= 1.0:
            return -errno.EINVAL, '"crush_compat_step" must be in (0, 1)'
        max_misplaced = float(self.get_ceph_option('target_max_misplaced_ratio'))
        min_pg_per_osd = 2

        ms = plan.initial
        osdmap = ms.osdmap
        crush = osdmap.get_crush()
        pe = self.calc_eval(ms, plan.pools)
        min_score_to_optimize = self.get_module_option('min_score')
        if pe.score <= min_score_to_optimize:
            if pe.score == 0:
                detail = 'Distribution is already perfect'
            else:
                detail = 'score %f <= min_score %f, will not optimize' \
                         % (pe.score, min_score_to_optimize)
            self.log.info(detail)
            return -errno.EALREADY, detail

        # get current osd reweights
        orig_osd_weight = { a['osd']: a['weight']
                            for a in ms.osdmap_dump.get('osds',[]) }
        reweighted_osds = [ a for a,b in six.iteritems(orig_osd_weight)
                            if b < 1.0 and b > 0.0 ]

        # get current compat weight-set weights
        orig_ws = self.get_compat_weight_set_weights(ms)
        if not orig_ws:
            return -errno.EAGAIN, 'compat weight-set not available'
        orig_ws = { a: b for a, b in six.iteritems(orig_ws) if a >= 0 }

        # Make sure roots don't overlap their devices.  If so, we
        # can't proceed.
        roots = pe.target_by_root.keys()
        self.log.debug('roots %s', roots)
        visited = {}
        overlap = {}
        root_ids = {}
        for root, wm in six.iteritems(pe.target_by_root):
            for osd in wm:
                if osd in visited:
                    if osd not in overlap:
                        overlap[osd] = [ visited[osd] ]
                    overlap[osd].append(root)
                visited[osd] = root
        if len(overlap) > 0:
            detail = 'Some osds belong to multiple subtrees: %s' % \
                     overlap
            self.log.error(detail)
            return -errno.EOPNOTSUPP, detail

        # rebalance by pgs, objects, or bytes
        metrics = self.get_module_option('crush_compat_metrics').split(',')
        key = metrics[0] # balancing using the first score metric
        if key not in ['pgs', 'bytes', 'objects']:
            self.log.warn("Invalid crush_compat balancing key %s. Using 'pgs'." % key)
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
                            # not enough to go on here... keep orig weight
                            calc_weight = weight / orig_osd_weight[osd]
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
                root_sum = sum(b for a,b in six.iteritems(next_ws)
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
        fudge = 0
        if next_ow != orig_osd_weight:
            fudge = .001

        if best_pe.score < pe.score + fudge:
            self.log.info('Success, score %f -> %f', pe.score, best_pe.score)
            plan.compat_ws = best_ws
            for osd, w in six.iteritems(best_ow):
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

    def get_compat_weight_set_weights(self, ms):
        if not CRUSHMap.have_default_choose_args(ms.crush_dump):
            # enable compat weight-set first
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
            except:
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

    def do_crush(self):
        self.log.info('do_crush (not yet implemented)')

    def do_osd_weight(self):
        self.log.info('do_osd_weight (not yet implemented)')

    def execute(self, plan):
        self.log.info('Executing plan %s' % plan.name)

        commands = []

        # compat weight-set
        if len(plan.compat_ws) and \
           not CRUSHMap.have_default_choose_args(plan.initial.crush_dump):
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

        for osd, weight in six.iteritems(plan.compat_ws):
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
        for osd, weight in six.iteritems(plan.osd_weights):
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
        for pgid in incdump.get('old_pg_upmap_items', []):
            self.log.info('ceph osd rm-pg-upmap-items %s', pgid)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd rm-pg-upmap-items',
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

        # wait for commands
        self.log.debug('commands %s' % commands)
        for result in commands:
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('execute error: r = %d, detail = %s' % (r, outs))
                return r, outs
        self.log.debug('done')
        return 0, ''
