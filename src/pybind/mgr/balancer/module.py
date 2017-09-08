
"""
Balance PG distribution across OSDs.
"""

import errno
import json
import math
import random
import time
from mgr_module import MgrModule, CommandResult
from threading import Event

# available modes: 'none', 'crush', 'crush-compat', 'upmap', 'osd_weight'
default_mode = 'none'
default_sleep_interval = 60   # seconds
default_max_misplaced = .03   # max ratio of pgs replaced at a time

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


class Plan:
    def __init__(self, name, ms):
        self.mode = 'unknown'
        self.name = name
        self.initial = ms

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
           '-1' not in self.initial.crush_dump.get('choose_args', {}):
            ls.append('ceph osd crush weight-set create-compat')
        for osd, weight in self.compat_ws.iteritems():
            ls.append('ceph osd crush weight-set reweight-compat %s %f' %
                      (osd, weight))
        for osd, weight in self.osd_weights.iteritems():
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
    pool_name = {}       # pool id -> pool name
    pool_id = {}         # pool name -> id
    pool_roots = {}      # pool name -> root name
    root_pools = {}      # root name -> pools
    target_by_root = {}  # root name -> target weight map
    count_by_pool = {}
    count_by_root = {}
    actual_by_pool = {}  # pool -> by_* -> actual weight map
    actual_by_root = {}  # pool -> by_* -> actual weight map
    total_by_pool = {}   # pool -> by_* -> total
    total_by_root = {}   # root -> by_* -> total
    stats_by_pool = {}   # pool -> by_* -> stddev or avg -> value
    stats_by_root = {}   # root -> by_* -> stddev or avg -> value

    score_by_pool = {}
    score_by_root = {}

    score = 0.0

    def __init__(self, ms):
        self.ms = ms

    def show(self):
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
        r += 'score %f (lower is better)\n' % self.score
        return r

    def calc_stats(self, count, target, total):
        num = max(len(target), 1)
        r = {}
        for t in ('pgs', 'objects', 'bytes'):
            avg = float(total[t]) / float(num)
            dev = 0.0

            # score is a measure of how uneven the data distribution is.
            # score lies between [0, 1), 0 means perfect distribution.
            score = 0.0
            sum_weight = 0.0

            for k, v in count[t].iteritems():
                # adjust/normalize by weight
                adjusted = float(v) / target[k] / float(num)

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
                'score': sum_weight,
            }
        return r

class Module(MgrModule):
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
            "cmd": "balancer eval name=plan,type=CephString,req=false",
            "desc": "Evaluate data distribution for the current cluster or specific plan",
            "perm": "r",
        },
        {
            "cmd": "balancer optimize name=plan,type=CephString",
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
            "cmd": "balancer execute name=plan,type=CephString",
            "desc": "Execute an optimization plan",
            "perm": "r",
        },
    ]
    active = False
    run = True
    plans = {}
    mode = ''

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def handle_command(self, command):
        self.log.warn("Handling command: '%s'" % str(command))
        if command['prefix'] == 'balancer status':
            s = {
                'plans': self.plans.keys(),
                'active': self.active,
                'mode': self.get_config('mode', default_mode),
            }
            return (0, json.dumps(s, indent=4), '')
        elif command['prefix'] == 'balancer mode':
            self.set_config('mode', command['mode'])
            return (0, '', '')
        elif command['prefix'] == 'balancer on':
            if not self.active:
                self.set_config('active', '1')
                self.active = True
            self.event.set()
            return (0, '', '')
        elif command['prefix'] == 'balancer off':
            if self.active:
                self.set_config('active', '')
                self.active = False
            self.event.set()
            return (0, '', '')
        elif command['prefix'] == 'balancer eval':
            if 'plan' in command:
                plan = self.plans.get(command['plan'])
                if not plan:
                    return (-errno.ENOENT, '', 'plan %s not found' %
                            command['plan'])
                ms = plan.final_state()
            else:
                ms = MappingState(self.get_osdmap(),
                                  self.get("pg_dump"),
                                  'current cluster')
            return (0, self.evaluate(ms), '')
        elif command['prefix'] == 'balancer optimize':
            plan = self.plan_create(command['plan'])
            self.optimize(plan)
            return (0, '', '')
        elif command['prefix'] == 'balancer rm':
            self.plan_rm(command['name'])
            return (0, '', '')
        elif command['prefix'] == 'balancer reset':
            self.plans = {}
            return (0, '', '')
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
            self.execute(plan)
            self.plan_rm(plan)
            return (0, '', '')
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.info('Starting')
        while self.run:
            self.log.debug('Waking up')
            self.active = self.get_config('active', '') is not ''
            sleep_interval = float(self.get_config('sleep_interval',
                                                   default_sleep_interval))
            if self.active:
                self.log.debug('Running')
                name = 'auto_%s' % time.strftime(TIME_FORMAT, time.gmtime())
                plan = self.plan_create(name)
                if self.optimize(plan):
                    self.execute(plan)
                self.plan_rm(name)
            self.log.debug('Sleeping for %d', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def plan_create(self, name):
        plan = Plan(name, MappingState(self.get_osdmap(),
                                       self.get("pg_dump"),
                                       'plan %s initial' % name))
        self.plans[name] = plan
        return plan

    def plan_rm(self, name):
        if name in self.plans:
            del self.plans[name]

    def calc_eval(self, ms):
        pe = Eval(ms)
        pool_rule = {}
        for p in ms.osdmap_dump.get('pools',[]):
            pe.pool_name[p['pool']] = p['pool_name']
            pe.pool_id[p['pool_name']] = p['pool']
            pool_rule[p['pool_name']] = p['crush_rule']
            pe.pool_roots[p['pool_name']] = []
        pools = pe.pool_id.keys()
        if len(pools) == 0:
            return pe
        self.log.debug('pool_name %s' % pe.pool_name)
        self.log.debug('pool_id %s' % pe.pool_id)
        self.log.debug('pools %s' % pools)
        self.log.debug('pool_rule %s' % pool_rule)

        # get expected distributions by root
        actual_by_root = {}
        rootids = ms.crush.find_takes()
        roots = []
        for rootid in rootids:
            root = ms.crush.get_item_name(rootid)
            roots.append(root)
            ls = ms.osdmap.get_pools_by_take(rootid)
            pe.root_pools[root] = []
            for poolid in ls:
                pe.pool_roots[pe.pool_name[poolid]].append(root)
                pe.root_pools[root].append(pe.pool_name[poolid])
            pe.target_by_root[root] = ms.crush.get_take_weight_osd_map(rootid)
            actual_by_root[root] = {
                'pgs': {},
                'objects': {},
                'bytes': {},
            }
            for osd in pe.target_by_root[root].iterkeys():
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
        for pool in pools:
            pi = [p for p in ms.osdmap_dump.get('pools',[])
                  if p['pool_name'] == pool][0]
            poolid = pi['pool']
            pm = ms.osdmap.map_pool_pgs_up(poolid)
            pgs = 0
            objects = 0
            bytes = 0
            pgs_by_osd = {}
            objects_by_osd = {}
            bytes_by_osd = {}
            for root in pe.pool_roots[pool]:
                for osd in pe.target_by_root[root].iterkeys():
                    pgs_by_osd[osd] = 0
                    objects_by_osd[osd] = 0
                    bytes_by_osd[osd] = 0
            for pgid, up in pm.iteritems():
                for osd in [int(osd) for osd in up]:
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
                    for k, v in pgs_by_osd.iteritems()
                },
                'objects': {
                    k: v
                    for k, v in objects_by_osd.iteritems()
                },
                'bytes': {
                    k: v
                    for k, v in bytes_by_osd.iteritems()
                },
            }
            pe.actual_by_pool[pool] = {
                'pgs': {
                    k: float(v) / float(max(pgs, 1))
                    for k, v in pgs_by_osd.iteritems()
                },
                'objects': {
                    k: float(v) / float(max(objects, 1))
                    for k, v in objects_by_osd.iteritems()
                },
                'bytes': {
                    k: float(v) / float(max(bytes, 1))
                    for k, v in bytes_by_osd.iteritems()
                },
            }
            pe.total_by_pool[pool] = {
                'pgs': pgs,
                'objects': objects,
                'bytes': bytes,
            }
        for root, m in pe.total_by_root.iteritems():
            pe.count_by_root[root] = {
                'pgs': {
                    k: float(v)
                    for k, v in actual_by_root[root]['pgs'].iteritems()
                },
                'objects': {
                    k: float(v)
                    for k, v in actual_by_root[root]['objects'].iteritems()
                },
                'bytes': {
                    k: float(v)
                    for k, v in actual_by_root[root]['bytes'].iteritems()
                },
            }
            pe.actual_by_root[root] = {
                'pgs': {
                    k: float(v) / float(max(pe.total_by_root[root]['pgs'], 1))
                    for k, v in actual_by_root[root]['pgs'].iteritems()
                },
                'objects': {
                    k: float(v) / float(max(pe.total_by_root[root]['objects'], 1))
                    for k, v in actual_by_root[root]['objects'].iteritems()
                },
                'bytes': {
                    k: float(v) / float(max(pe.total_by_root[root]['bytes'], 1))
                    for k, v in actual_by_root[root]['bytes'].iteritems()
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
            ) for a, b in pe.count_by_root.iteritems()
        }

	# the scores are already normalized
        pe.score_by_root = {
            r: {
                'pgs': pe.stats_by_root[r]['pgs']['score'],
                'objects': pe.stats_by_root[r]['objects']['score'],
                'bytes': pe.stats_by_root[r]['bytes']['score'],
            } for r in pe.total_by_root.keys()
        }

        # total score is just average of normalized stddevs
        pe.score = 0.0
        for r, vs in pe.score_by_root.iteritems():
            for k, v in vs.iteritems():
                pe.score += v
        pe.score /= 3 * len(roots)
        return pe

    def evaluate(self, ms):
        pe = self.calc_eval(ms)
        return pe.show()

    def optimize(self, plan):
        self.log.info('Optimize plan %s' % plan.name)
        plan.mode = self.get_config('mode', default_mode)
        max_misplaced = float(self.get_config('max_misplaced',
                                              default_max_misplaced))
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
            self.log.info('Some PGs (%f) are unknown; waiting', unknown)
        elif degraded > 0.0:
            self.log.info('Some PGs (%f) are degraded; waiting', degraded)
        elif inactive > 0.0:
            self.log.info('Some PGs (%f) are inactive; waiting', inactive)
        elif misplaced >= max_misplaced:
            self.log.info('Too many PGs (%f > %f) are misplaced; waiting',
                          misplaced, max_misplaced)
        else:
            if plan.mode == 'upmap':
                return self.do_upmap(plan)
            elif plan.mode == 'crush-compat':
                return self.do_crush_compat(plan)
            elif plan.mode == 'none':
                self.log.info('Idle')
            else:
                self.log.info('Unrecognized mode %s' % plan.mode)
        return False

        ##

    def do_upmap(self, plan):
        self.log.info('do_upmap')
        max_iterations = self.get_config('upmap_max_iterations', 10)
        max_deviation = self.get_config('upmap_max_deviation', .01)

        ms = plan.initial
        pools = [str(i['pool_name']) for i in ms.osdmap_dump.get('pools',[])]
        if len(pools) == 0:
            self.log.info('no pools, nothing to do')
            return False
        # shuffle pool list so they all get equal (in)attention
        random.shuffle(pools)
        self.log.info('pools %s' % pools)

        inc = plan.inc
        total_did = 0
        left = max_iterations
        for pool in pools:
            did = ms.osdmap.calc_pg_upmaps(inc, max_deviation, left, [pool])
            total_did += did
            left -= did
            if left <= 0:
                break
        self.log.info('prepared %d/%d changes' % (total_did, max_iterations))
        return True

    def do_crush_compat(self, plan):
        self.log.info('do_crush_compat')
        osdmap = self.get_osdmap()
        crush = osdmap.get_crush()

        # get current compat weight-set weights
        old_ws = self.get_compat_weight_set_weights()

        ms = plan.initial
        pe = self.calc_eval(ms)

        # Make sure roots don't overlap their devices.  If so, we
        # can't proceed.
        roots = pe.target_by_root.keys()
        self.log.debug('roots %s', roots)
        visited = {}
        overlap = {}
        root_ids = {}
        for root, wm in pe.target_by_root.iteritems():
            for osd in wm.iterkeys():
                if osd in visited:
                    overlap[osd] = 1
                visited[osd] = 1
        if len(overlap) > 0:
            self.log.err('error: some osds belong to multiple subtrees: %s' %
                         overlap)
            return False

        key = 'pgs'  # pgs objects or bytes

        # go
        random.shuffle(roots)
        for root in roots:
            pools = pe.root_pools[root]
            self.log.info('Balancing root %s (pools %s) by %s' %
                          (root, pools, key))
            target = pe.target_by_root[root]
            actual = pe.actual_by_root[root][key]
            queue = sorted(actual.keys(),
                           key=lambda osd: -abs(target[osd] - actual[osd]))
            self.log.debug('queue %s' % queue)
            for osd in queue:
                deviation = target[osd] - actual[osd]
                if deviation == 0:
                    break
                self.log.debug('osd.%d deviation %f', osd, deviation)
                weight = old_ws[osd]
                calc_weight = target[osd] / actual[osd] * weight
                new_weight = weight * .7 + calc_weight * .3
                self.log.debug('Reweight osd.%d %f -> %f', osd, weight,
                               new_weight)
                plan.compat_ws[osd] = new_weight
        return True

    def compat_weight_set_reweight(self, osd, new_weight):
        self.log.debug('ceph osd crush weight-set reweight-compat')
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd crush weight-set reweight-compat',
            'format': 'json',
            'item': 'osd.%d' % osd,
            'weight': [new_weight],
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error('Error setting compat weight-set osd.%d to %f' %
            (osd, new_weight))
            return

    def get_compat_weight_set_weights(self):
        # enable compat weight-set
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

        raw = crushmap.get('choose_args',{}).get('-1', [])
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

    def get_util(self, cost_mode, pools, osds):
        if cost_mode == 'pg' or \
           cost_mode == 'pg_bytes' or \
           cost_mode == 'pg_objects':
            util_map = {}
            for osd in osds:
                util_map[osd] = 0
            dump = self.get('pg_dump')
            #self.log.info('dump %s' % dump)
            self.log.info('osds %s' % osds)
            for pg in dump['pg_stats']:
                inpool = False
                for pool in pools:
                    if pg['pgid'].startswith(str(pool) + '.'):
                        inpool = True
                        break
                if not inpool:
                    self.log.info('skipping %s' % pg['pgid'])
                    continue
                self.log.info('pg %s osds %s' % (pg['pgid'], pg['up']))
                for osd in [int(a) for a in pg['up']]:
                    if osd in osds:
                        if cost_mode == 'pg':
                            util_map[osd] += 1
                        elif cost_mode == 'pg_bytes':
                            util_map[osd] += pg['stat_sum']['num_bytes']
                        elif cost_mode == 'pg_objects':
                            util_map[osd] += pg['stat_sum']['num_objects']
            return util_map
        else:
            raise RuntimeError('unsupported cost mode %s' % cost_mode)

    def get_target(self, util_map):
        total = 0
        count = 0
        for k, v in util_map.iteritems():
            total += v;
            count += 1
        return total / count

    def do_crush(self):
        self.log.info('do_crush (not yet implemented)')

    def do_osd_weight(self):
        self.log.info('do_osd_weight (not yet implemented)')

    def execute(self, plan):
        self.log.info('Executing plan %s' % plan.name)

        commands = []

        # compat weight-set
        if len(plan.compat_ws) and \
           '-1' not in plan.initial.crush_dump.get('choose_args', {}):
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

        for osd, weight in plan.compat_ws.iteritems():
            self.log.info('ceph osd crush weight-set reweight-compat osd.%d %f',
                          osd, weight)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush weight-set reweight-compat',
                'format': 'json',
                'item': 'osd.%d' % osd,
                'weight': [weight],
            }), 'foo')
            commands.append(result)

        # new_weight
        reweightn = {}
        for osd, weight in plan.osd_weights.iteritems():
            reweightn[int(osd)] = float(weight) / float(0x10000)
        if len(reweightn):
            self.log.info('ceph osd reweightn %s', reweightn)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd reweightn',
                'format': 'json',
                'weights': json.dumps(reweightn),
            }), 'foo')
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
                self.log.error('Error on command')
                return
        self.log.debug('done')
