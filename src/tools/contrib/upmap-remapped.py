#!/usr/bin/env python3
#
# DISCLAIMER: THIS SCRIPT COMES WITH NO WARRANTY OR GUARANTEE
# OF ANY KIND.
#
# DISCLAIMER 2: THIS TOOL USES A CEPH FEATURE MARKED "(developers only)"
# YOU SHOULD NOT RUN THIS UNLESS YOU KNOW EXACTLY HOW THOSE
# FUNCTIONALITIES WORK.
#
# upmap-remapped.py
#
# Usage (print only): ./upmap-remapped.py
# Usage (production): ./upmap-remapped.py | sh
#
# Optional to ignore PGs that are backfilling and not backfill+wait:
# Usage: ./upmap-remapped.py --ignore-backfilling
#
# This tool will use ceph's pg-upmap-items functionality to
# quickly modify all PGs which are currently remapped to become
# active+clean. I use it in combination with the ceph-mgr upmap
# balancer and the norebalance state for these use-cases:
#
# - Change crush rules or tunables.
# - Adding capacity (add new host, rack, ...).
# - Change the failure domain a crush rule separates its pgs over.
#
# In general, the correct procedure for using this script is:
#
# 1. Backup your osdmaps, crush maps, ...
# 2. Set the norebalance flag.
# 3. Make your change (tunables, add osds, etc...)
# 4. Run this script a few times. (Remember to | sh)
# 5. Cluster should now be 100% active+clean.
# 6. Unset the norebalance flag.
# 7. The ceph-mgr balancer in upmap mode should now gradually
#    remove the upmap-items entries which were created by this
#    tool.
#
# Changing the failure domain of a crush rule - from host to rack,
# say - is the one case where the acting set of a pg is not a
# placement the pg is allowed to keep: some of its osds now sit in
# the same failure domain, and the mon refuses to map the pg onto
# them.  The script then keeps the acting osds the new rule still
# allows and replaces only the ones which violate it, so a pool
# moves the shards it has to move and leaves the others alone.  The
# procedure is the same one; step 3 is the 'ceph osd pool set
# <pool> crush_rule <rule>'.
#
# A pg whose pool is placed by a rule which chooses buckets and then
# chooses osds inside them - two shards per rack, say - is left to
# the mon, since one osd per failure domain does not describe it.
#
# Hacked by: Dan van der Ster <daniel.vanderster@cern.ch>


import argparse, atexit, json, subprocess, sys

# How long to wait for a mon command.  'pg ls' and 'osd dump' can take a while
# on a large cluster with many remapped pgs.
MON_TIMEOUT = 300

# How 'pg ls' shows a shard of an erasure-coded pg which has no osd: crush could
# not place it, or the osd holding it is gone.
CRUSH_ITEM_NONE = 2147483647

# The cluster state every function below works from.  main() fills it in; the
# tests set it directly.
OSDS = set()      # the osds which exist, from 'osd ls'
WEIGHT = {}       # osd id -> the weight it effectively has, from 'osd df'
UP = None         # the osds which are up, from 'osd dump' (None: do not check)

# the crush tree and the rules, from 'osd crush dump'
PARENT = {}       # bucket or osd id -> the id of the bucket holding it
BUCKET_TYPE = {}  # bucket id -> its crush type id
CHILDREN = {}     # bucket id -> the ids it holds
RULE_DOMAIN = {}  # crush rule id -> the crush type id it separates pgs over
RULE_ROOT = {}    # crush rule id -> the bucket its take step starts from
POOL_RULE = {}    # pool id (as a string) -> the crush rule it uses
_domain_of = {}   # (osd id, crush type id) -> the bucket of that type above it
_domain_osds = {} # (crush type id, root) -> ([bucket ids], {bucket id: [usable osds]})
_root_osds = {}   # take root -> the set of osds under it
cluster = None    # the librados connection, or None when using the shell
use_shell = True

def get_command_output(command):
  result = subprocess.run(command, capture_output=True, universal_newlines=True, check=True, shell=True)
  return result.stdout

def eprint(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)

def parse_args():
  parser = argparse.ArgumentParser(
    description='Print the ceph commands which make every remapped pg '
                'active+clean again.  Pipe the output into sh to run them.')
  parser.add_argument('--ignore-backfilling', action='store_true',
                      help='leave the pgs which are already backfilling alone, '
                           'instead of interrupting them')
  return parser.parse_args()

def connect():
  """Talk to the cluster through librados when it is available, and fall back to
  running the ceph cli in a shell when it is not."""
  global cluster, use_shell
  try:
    import rados
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
  except Exception:
    use_shell = True
  else:
    use_shell = False
    # every exit from here on should close the connection, not just the last one
    atexit.register(cluster.shutdown)

def get_cluster_output(shell_command, mon_command):
  """Run a command through librados if it is available, else through the shell,
  and return its output.  Exits if the command fails."""
  try:
    if use_shell:
      return get_command_output(shell_command)
    ret, output, errs = cluster.mon_command(json.dumps(mon_command), b'',
                                            timeout=MON_TIMEOUT)
  except Exception as e:
    eprint('Error running "%s": %s' % (shell_command, e))
    sys.exit(1)
  if ret != 0:
    eprint('Error running "%s": %s'
           % (shell_command, errs.strip() or 'returned %d' % ret))
    sys.exit(1)
  return output.decode('utf-8').strip()

def load_osds():
  """Read the osds which exist and the weight each of them effectively has."""
  global OSDS, WEIGHT
  try:
    OSDS = set(json.loads(get_cluster_output('ceph osd ls -f json',
                                             {"prefix": "osd ls", "format": "json"})))
    DF = json.loads(get_cluster_output('ceph osd df -f json',
                                       {"prefix": "osd df", "format": "json"}))['nodes']
  except ValueError:
    eprint('Error loading OSD IDs')
    sys.exit(1)
  # gen_upmap() asks about this for every shard of every remapped pg
  WEIGHT = dict((o['id'], o['crush_weight'] * o['reweight']) for o in DF)

def crush_weight(id):
  return WEIGHT.get(id, 0)

def is_up(id):
  return UP is None or id in UP

def usable(id):
  """Whether a pg may be mapped onto this osd.  An osd which is out has nowhere
  to put the data, and one which is down would leave the pg degraded until it
  comes back, which is worse than leaving the pg remapped."""
  return crush_weight(id) > 0 and is_up(id)

def build_topology(crush, osd_dump):
  """Take the crush tree, the failure domain of every rule and the rule every
  pool uses out of 'osd crush dump' and 'osd dump'."""
  type_id = dict((t['name'], t['type_id']) for t in crush.get('types', []))
  for bucket in crush.get('buckets', []):
    CHILDREN[bucket['id']] = [item['id'] for item in bucket.get('items', [])]
    # A shadow bucket of the per-device-class tree is only walked down from the
    # take step of a 'class' rule, to find the osds the rule can use; the failure
    # domain of an osd is read from the real tree.
    if '~' in bucket['name']:
      continue
    BUCKET_TYPE[bucket['id']] = bucket['type_id']
    for item in bucket.get('items', []):
      PARENT[item['id']] = bucket['id']
  for rule in crush.get('rules', []):
    # Only a rule which separates the pg over one type of bucket - the usual
    # 'chooseleaf firstn 0 type host' - is modelled here.  A rule which chooses
    # several buckets and then several osds inside each of them constrains the
    # pg in a way this cannot express, so leave its domain unknown.
    steps = [step.get('type') for step in rule.get('steps', [])
             if str(step.get('op', '')).startswith('choose')]
    domain = type_id.get(steps[0]) if len(steps) == 1 else None
    # A rule which separates the pg over osds (type 0) has no buckets to keep
    # the shards apart in, and any set of distinct osds satisfies it, so leave
    # its domain unknown as well and handle its pgs the way master does.
    RULE_DOMAIN[rule['rule_id']] = domain if domain else None
    # The mon only maps a pg onto osds under the bucket the rule takes - for a
    # 'class hdd' rule, the hdd shadow tree.  A rule with several take steps is
    # left unrestricted, as is one whose take step does not name a bucket.
    takes = [step.get('item') for step in rule.get('steps', [])
             if step.get('op') == 'take']
    RULE_ROOT[rule['rule_id']] = takes[0] if len(takes) == 1 else None
  for pool in osd_dump.get('pools', []):
    POOL_RULE[str(pool['pool'])] = pool['crush_rule']

def domain_of(osd, domain_type):
  """The bucket of the failure domain type which holds this osd, or None."""
  key = (osd, domain_type)
  if key not in _domain_of:
    bucket = PARENT.get(osd)
    while bucket is not None and BUCKET_TYPE.get(bucket) != domain_type:
      bucket = PARENT.get(bucket)
    _domain_of[key] = bucket
  return _domain_of[key]

def osds_under(bucket, found):
  for child in CHILDREN.get(bucket, []):
    if child >= 0:
      found.append(child)
    else:
      osds_under(child, found)
  return found

def under_root(osd, root):
  """Whether the rule which takes this bucket can map a pg onto this osd."""
  if root is None:
    return True
  if root not in _root_osds:
    _root_osds[root] = set([root] if root >= 0 else osds_under(root, []))
  return osd in _root_osds[root]

def domains(domain_type, root=None):
  """Every bucket of the failure domain type, and the osds a pg of a rule taking
  this root can be mapped onto inside each of them."""
  key = (domain_type, root)
  if key not in _domain_osds:
    buckets = sorted(b for b, t in BUCKET_TYPE.items() if t == domain_type)
    _domain_osds[key] = (
      buckets,
      dict((b, sorted(o for o in osds_under(b, [])
                      if usable(o) and under_root(o, root))) for b in buckets))
  return _domain_osds[key]

def spread(pgid, position, salt=0):
  """A number which depends only on the pg and the position in it.  The osds a
  pg is forced to move to are picked with this, so that they are spread over the
  cluster instead of piling up on the first free bucket, and so that two runs of
  the script make the same choice."""
  try:
    seed = int(str(pgid).split('.')[1], 16)
  except (IndexError, ValueError):
    seed = 0
  return (seed * 2654435761 + position * 40503 + salt * 7) & 0xffffffff

def rotations(up, target):
  """The positions of an erasure-coded pg whose mapping is part of a rotation.

  The mon applies pg-upmap-items one pair after the other and ignores a pair
  whose destination is still in the pg, so the pair which vacates a position has
  to come before the pair which fills it again.  When those dependencies form a
  cycle - two osds trading places, or a longer rotation - no order of the pairs
  expresses it and gen_upmap() has to drop them."""
  moving = [i for i in range(len(up)) if up[i] != target[i]]
  position_of = dict((up[i], i) for i in moving)
  # the mapping at position i can only be applied after the one at needs[i]
  needs = dict((i, position_of.get(target[i])) for i in moving)
  done = {}
  cycled = set()
  for start in moving:
    path = []
    i = start
    while i is not None and i not in done:
      done[i] = False
      path.append(i)
      i = needs.get(i)
    if i is not None and done[i] is False:
      cycled.update(path[path.index(i):])
    for j in path:
      done[j] = True
  return sorted(cycled)

def allowed(osds, domain_type, root=None):
  """Whether the mon would map a pg onto these osds: each of them usable, under
  the root of the rule and in a failure domain of its own.  A shard without an
  osd does not count against it."""
  seen = set()
  for osd in osds:
    if osd == CRUSH_ITEM_NONE:
      continue
    bucket = domain_of(osd, domain_type)
    if (not usable(osd) or not under_root(osd, root) or bucket is None
        or bucket in seen):
      return False
    seen.add(bucket)
  return True

def pick_target(pgid, up, acting, domain_type, replicated=False, root=None):
  """The osds to map this pg onto, in the positions to map them into.

  This is the acting set whenever the acting set is something the pg is allowed
  to keep, which is the case when osds are added or removed, when the pgs of a
  pool are split, and when the tunables change.  It is not the case when the
  crush rule of a pool starts separating its pgs over a different type of
  bucket: some of the acting osds then sit in the same failure domain, the mon
  refuses to map the pg onto them, and the pg has to move.

  So keep every acting osd the new failure domain still allows - one per bucket,
  and those shards do not move - and replace only the others, preferring the osd
  crush itself picked for the position and otherwise spreading the moves over
  the buckets which are still free, so that the pgs a rule change forces to move
  do not all land on the same osds.

  Every osd it picks is under root, the bucket the pool's rule takes, since the
  mon refuses a mapping onto any other osd.

  Returns the acting set unchanged when the failure domain of the rule is not
  known, and None when this pg has a shard crush placed which cannot be put in
  any free failure domain."""
  if domain_type is None or len(up) != len(acting):
    return list(acting)

  # The acting set is a placement the pg may keep: map it back exactly as master
  # does.  This is what every pg gets after hardware is added or removed, even
  # when a failure domain is down and a pool has no spare one.  An erasure-coded
  # pg whose acting set rotates shards between positions goes on below, so that
  # the rotation can be broken.
  if (allowed(acting, domain_type, root)
      and (replicated or not rotations(up, acting))):
    return list(acting)

  buckets, bucket_osds = domains(domain_type, root)

  target = [None] * len(up)
  used = set()

  # keep the acting osds whose failure domain the pg can still have
  for i, osd in enumerate(acting):
    bucket = domain_of(osd, domain_type)
    if (usable(osd) and under_root(osd, root) and bucket is not None
        and bucket not in used):
      target[i] = osd
      used.add(bucket)

  # take crush's own choice wherever the pg is allowed to have it, before
  # anything else takes the failure domain it sits in
  for i, osd in enumerate(up):
    if target[i] is not None:
      continue
    bucket = domain_of(osd, domain_type)
    if (osd in OSDS and usable(osd) and under_root(osd, root)
        and bucket is not None and bucket not in used):
      target[i] = osd
      used.add(bucket)

  # and spread whatever is still unplaced over the free failure domains
  for i in range(len(up)):
    if target[i] is not None:
      continue
    free = [b for b in buckets if b not in used and bucket_osds.get(b)]
    if not free:
      if up[i] == CRUSH_ITEM_NONE:
        # crush could not place this shard either, with a failure domain down:
        # leave it unplaced rather than give up on the shards which can move
        target[i] = up[i]
        continue
      return None
    bucket = free[spread(pgid, i) % len(free)]
    used.add(bucket)
    osds = bucket_osds[bucket]
    wanted = [o for o in osds if o in up]   # a shard crush wanted here anyway
    target[i] = wanted[0] if wanted else osds[spread(pgid, i, 1) % len(osds)]

  # Only the set of osds matters on a replicated pool, so its mappings never
  # form a rotation.
  if replicated:
    return target

  # Keeping the acting osds in the positions they are already in can leave an
  # erasure-coded pg wanting to rotate two of its shards, which pg-upmap-items
  # cannot express and gen_upmap() drops - sending those shards back to crush.
  # Moving one member of the rotation onto an osd which is not in the pg at all
  # breaks the rotation, cannot create another one, and leaves the rest of the
  # shards where they are.
  for _ in range(len(up)):
    cycle = rotations(up, target)
    if not cycle:
      break
    i = cycle[0]
    used.discard(domain_of(target[i], domain_type))
    free = [b for b in buckets
            if b not in used and [o for o in bucket_osds.get(b, []) if o not in up]]
    if not free:
      used.add(domain_of(target[i], domain_type))
      for j in cycle:
        target[j] = up[j]    # nowhere to move it: leave those shards to crush
      continue
    bucket = free[spread(pgid, i, 2) % len(free)]
    used.add(bucket)
    osds = [o for o in bucket_osds[bucket] if o not in up]
    target[i] = osds[spread(pgid, i, 3) % len(osds)]

  return target

def gen_upmap(up, acting, replicated=False):
  # a pg which is degraded as well as remapped can report an acting set of a
  # different length, and there is nothing useful to do with those
  if len(up) != len(acting):
    return []

  # On replicated pools only the set of osds matters, so vacate the osds which do
  # not belong in the pg and fill it with the ones which are missing from it.
  # This never maps onto an osd which is already in the up set, which the mon
  # would ignore.
  # e.g. ceph osd pg-upmap-items 4.5fd 603 383 499 804
  if replicated:
    sources = [u for u in up if u not in acting and u in OSDS]
    dests = [a for a in acting if a not in up and usable(a)]
    return list(zip(sources, dests))

  # On erasure-coded pools every position in the up set matters, so the mappings
  # have to be positional.  Only keep the ones we are allowed to make.
  mappings = [(u, a) for u, a in zip(up, acting) if u != a and u in OSDS and usable(a)]

  # Dropping a mapping above leaves its osd in the up set, and mapping onto an
  # osd which is staying in the up set asks for the same osd twice, which the mon
  # ignores.  Drop those mappings too, repeating until nothing changes.
  while True:
    staying = set(up) - set(u for u, a in mappings)
    keep = [(u, a) for u, a in mappings if a not in staying]
    if len(keep) == len(mappings):
      break
    mappings = keep

  # Order the mappings on erasure-coded pools so that data is moved off an osd
  # before it is moved on to it.
  # e.g. ceph osd pg-upmap-items 15.c9 714 803 929 714
  # Each osd is used at most once as a source and once as a destination, so the
  # mappings form chains and cycles.  Emit each chain in order.  A cycle, such as
  # (314, 272) & (272, 314) or 1 -> 2 -> 3 -> 1, has no valid order, so leave
  # those mappings out and let the pg stay remapped.
  by_source = dict((u, (u, a)) for u, a in mappings)
  ordered = []
  placed = set()
  for m in mappings:
    if m in placed:
      continue
    # walk back over the mappings which have to be done before this one
    chain = []
    n = m
    while n is not None and n not in placed and n not in chain:
      chain.append(n)
      n = by_source.get(n[1])
    placed.update(chain)
    if n in chain:
      continue
    chain.reverse()
    ordered.extend(chain)

  return ordered

def reverse_upmap(up, mappings):
  """Return the up set crush would have given this pg without its pg-upmap-items.

  'pg ls' reports the up set the mon has already applied the pg's upmap items
  to, while pg-upmap-items is expressed against the mapping crush produces.  The
  mon applied each item by putting 'to' where 'from' was, so undo them in
  reverse order.

  The mon skips an item whose 'to' is already in the pg - crush picked that osd
  itself - or is out, and then 'from' is still in the up set.  Such an item was
  never applied, so there is nothing to undo."""
  raw = list(up)
  for mapping in reversed(mappings):
    if mapping['from'] in raw:
      continue
    for i, osd in enumerate(raw):
      if osd == mapping['to']:
        raw[i] = mapping['from']
        break
  return raw

def upmap_pg_items(pgid, mapping):
  if len(mapping):
    print('ceph osd pg-upmap-items %s ' % pgid, end='')
    for pair in mapping:
      print('%s %s ' % pair, end='')
    print('&')

def rm_upmap_pg_items(pgid):
  print('ceph osd rm-pg-upmap-items %s &' % pgid)

def main():
  options = parse_args()
  if options.ignore_backfilling:
    eprint('All actively backfilling PGs will be ignored.')

  connect()
  load_osds()

  # discover remapped pgs
  try:
    remapped_json = get_cluster_output('ceph pg ls remapped -f json',
                                       {"prefix": "pg ls", "states": ["remapped"], "format": "json"})
    try:
      remapped = json.loads(remapped_json)['pg_stats']
    except KeyError:
      eprint("There are no remapped PGs")
      sys.exit(0)
  except ValueError:
    eprint('Error loading remapped pgs')
    sys.exit(1)

  # discover existing upmaps and which osds are up
  global UP
  try:
    osd_dump = json.loads(get_cluster_output('ceph osd dump -f json',
                                             {"prefix": "osd dump", "format": "json"}))
    upmaps = osd_dump['pg_upmap_items']
  except (ValueError, KeyError):
    eprint('Error loading existing upmaps')
    sys.exit(1)
  UP = set(o['osd'] for o in osd_dump.get('osds', []) if o.get('up'))

  # discover the crush tree, the failure domain of each rule and the rule each
  # pool uses
  try:
    crush = json.loads(get_cluster_output('ceph osd crush dump -f json',
                                          {"prefix": "osd crush dump", "format": "json"}))
  except ValueError:
    eprint('Error loading the crush map')
    sys.exit(1)
  build_topology(crush, osd_dump)

  # discover pools replicated or erasure
  pool_type = {}
  try:
    osd_pool_ls_detail = get_cluster_output('ceph osd pool ls detail',
                                            {"prefix": "osd pool ls", "detail": "detail", "format": "plain"})
    for line in osd_pool_ls_detail.split('\n'):
      if line.startswith('pool '):
        x = line.split(' ')
        pool_type[x[1]] = x[3]
  except IndexError:
    eprint('Error parsing pool types')
    sys.exit(1)

  # the pg-upmap-items each pg already carries, if any
  upmap_by_pgid = dict((str(u['pgid']), u.get('mappings', [])) for u in upmaps)

  # handle each remapped pg
  print(r'while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')
  num = 0
  for pg in remapped:
    if num == 50:
      print(r'wait; sleep 4; while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')
      num = 0

    if options.ignore_backfilling and "backfilling" in pg['state']:
      continue

    pgid = str(pg['pgid'])
    pool = pgid.split('.')[0]
    if pool not in pool_type:
      # the pool was deleted between reading the pgs and reading the pools
      eprint('Skipping pg %s of unknown pool %s' % (pgid, pool))
      continue
    if pool_type[pool] not in ('replicated', 'erasure'):
      eprint('Unknown pool type for %s' % pool)
      sys.exit(1)

    # work from the mapping crush produces, not from the one the pg already
    # carries, because that is what pg-upmap-items is expressed against
    items = upmap_by_pgid.get(pgid, [])
    raw = reverse_upmap(pg['up'], items) if items else pg['up']

    target = pick_target(pgid, raw, pg['acting'],
                         RULE_DOMAIN.get(POOL_RULE.get(pool)),
                         replicated=(pool_type[pool] == 'replicated'),
                         root=RULE_ROOT.get(POOL_RULE.get(pool)))
    if target is None:
      eprint('Skipping pg %s: a shard of it has no free failure domain with a '
             'usable osd to go to' % pgid)
      continue

    pairs = gen_upmap(raw, target,
                      replicated=(pool_type[pool] == 'replicated'))

    # leave a pg which already carries exactly these items alone
    if items and pairs == [(m['from'], m['to']) for m in items]:
      continue

    # crush already maps the pg where it should be, so the items it carries are
    # the only thing making it remapped
    if not pairs:
      if items:
        rm_upmap_pg_items(pgid)
        num += 1
      continue

    # pg-upmap-items replaces the pg's items, so there is nothing to remove first
    upmap_pg_items(pgid, pairs)
    num += 1

  print(r'wait; sleep 4; while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')

if __name__ == '__main__':
  main()
