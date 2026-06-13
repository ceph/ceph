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
# Hacked by: Dan van der Ster <daniel.vanderster@cern.ch>


import json, subprocess, sys

def get_command_output(command):
  result = subprocess.run(command, capture_output=True, universal_newlines=True, check=True, shell=True)
  return result.stdout

try:
  import rados
  cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
  cluster.connect()
except:
  use_shell = True
else:
  use_shell = False

def eprint(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)

try:
  if use_shell:
    OSDS = json.loads(get_command_output('ceph osd ls -f json | jq -r .'))
    DF = json.loads(get_command_output('ceph osd df -f json | jq -r .nodes'))
  else:
    cmd = {"prefix": "osd ls", "format": "json"}
    ret, output, errs = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
    output = output.decode('utf-8').strip()
    OSDS = json.loads(output)
    cmd = {"prefix": "osd df", "format": "json"}
    ret, output, errs = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
    output = output.decode('utf-8').strip()
    DF = json.loads(output)['nodes']
except ValueError:
  eprint('Error loading OSD IDs')
  sys.exit(1)

ignore_backfilling = False
for arg in sys.argv[1:]:
  if arg == "--ignore-backfilling":
    eprint ("All actively backfilling PGs will be ignored.")
    ignore_backfilling = True

def crush_weight(id):
  for o in DF:
    if o['id'] == id:
      return o['crush_weight'] * o['reweight']
  return 0

def gen_upmap(up, acting, replicated=False):
  assert(len(up) == len(acting))

  # Create mappings needed to make the PG clean
  mappings = [(u, a) for u, a in zip(up, acting) if u != a and u in OSDS and crush_weight(a) > 0]

  # Remove indirect mappings on replicated pools
  # e.g. ceph osd pg-upmap-items 4.5fd 603 383 499 804 804 530 &
  if replicated:
    p = list(mappings)
    u = set([x[0] for x in p])
    a = set([x[1] for x in p])
    mappings = list(zip(u-a, a-u))
  # Order the mappings on erasure-coded pools so that data is moved off an osd
  # before it is moved on to it.
  # e.g. ceph osd pg-upmap-items 15.c9 714 803 929 714
  else:
    # Handle the situation where the src and dst of one mapping matches the dst
    # and src of another.  Example: (314, 272) & (272, 314)
    for (x, y) in mappings:
      if (y, x) in mappings:
        mappings.remove((x, y))
        mappings.remove((y, x))

    # Do multiple passes of a modified bubble sort to order the mappings so that
    # data is moved off an OSD before it is moved on to it.  Stop when no
    # mappings are swapped.
    while True:
      swapped = False
      for i in range(len(mappings)-1):
        for j in range(i+1, len(mappings)):
          if mappings[j][0] == mappings[i][1] and mappings[j][1] != mappings[i][0]:
            mappings[i], mappings[j] = mappings[j], mappings[i]
            swapped = True

      if not swapped:
        break

  return mappings

def upmap_pg_items(pgid, mapping):
  if len(mapping):
    print('ceph osd pg-upmap-items %s ' % pgid, end='')
    for pair in mapping:
      print('%s %s ' % pair, end='')
    print('&')

def rm_upmap_pg_items(pgid):
  print('ceph osd rm-pg-upmap-items %s &' % pgid)


# start here

# discover remapped pgs
try:
  if use_shell:
    remapped_json = get_command_output('ceph pg ls remapped -f json | jq -r .')
  else:
    cmd = {"prefix": "pg ls", "states": ["remapped"], "format": "json"}
    ret, output, err = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
    remapped_json = output.decode('utf-8').strip()
  try:
    remapped = json.loads(remapped_json)['pg_stats']
  except KeyError:
    eprint("There are no remapped PGs")
    sys.exit(0)
except ValueError:
  eprint('Error loading remapped pgs')
  sys.exit(1)

# discover existing upmaps
try:
  if use_shell:
    osd_dump_json = get_command_output('ceph osd dump -f json | jq -r .')
  else:
    cmd = {"prefix": "osd dump", "format": "json"}
    ret, output, errs = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
    osd_dump_json = output.decode('utf-8').strip()
  upmaps = json.loads(osd_dump_json)['pg_upmap_items']
except ValueError:
  eprint('Error loading existing upmaps')
  sys.exit(1)

# discover pools replicated or erasure
pool_type = {}
try:
  if use_shell:
    osd_pool_ls_detail = get_command_output('ceph osd pool ls detail')
  else:
    cmd = {"prefix": "osd pool ls", "detail": "detail", "format": "plain"}
    ret, output, errs = cluster.mon_command(json.dumps(cmd), b'', timeout=5)
    osd_pool_ls_detail = output.decode('utf-8').strip()
  for line in osd_pool_ls_detail.split('\n'):
    if 'pool' in line:
      x = line.split(' ')
      pool_type[x[1]] = x[3]
except:
  eprint('Error parsing pool types')
  sys.exit(1)

# discover if each pg is already upmapped
has_upmap = {}
for pg in upmaps:
  pgid = str(pg['pgid'])
  has_upmap[pgid] = True

# handle each remapped pg
print(r'while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')
num = 0
for pg in remapped:
  if num == 50:
    print(r'wait; sleep 4; while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')
    num = 0

  if ignore_backfilling:
    if "backfilling" in pg['state']:
      continue

  pgid = pg['pgid']

  try:
    if has_upmap[pgid]:
      rm_upmap_pg_items(pgid)
      num += 1
      continue
  except KeyError:
    pass

  up = pg['up']
  acting = pg['acting']
  pool = pgid.split('.')[0]
  if pool_type[pool] == 'replicated':
    try:
      pairs = gen_upmap(up, acting, replicated=True)
    except:
      continue
  elif pool_type[pool] == 'erasure':
    try:
      pairs = gen_upmap(up, acting)
    except:
      continue
  else:
    eprint('Unknown pool type for %s' % pool)
    sys.exit(1)
  upmap_pg_items(pgid, pairs)
  num += 1

print(r'wait; sleep 4; while ceph status | grep -q "peering\|activating\|laggy"; do sleep 2; done')

if not use_shell:
  cluster.shutdown()
