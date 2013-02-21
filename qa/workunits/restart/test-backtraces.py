#!/usr/bin/env python

import subprocess as sub
from cStringIO import StringIO
import json
import os
import time
import sys
import thread

import rados as rados
import cephfs as cephfs

prefix='testbt'

def get_name(b, i, j):
    c = '{pre}.{pid}.{i}.{j}'.format(pre=prefix, pid=os.getpid(), i=i, j=j)
    return c, b + '/' + c

def mkdir(ceph, d):
    print >>sys.stderr, "mkdir {d}".format(d=d)
    ceph.mkdir(d, 0755)
    return ceph.stat(d)['st_ino']

def create(ceph, f):
    print >>sys.stderr, "creating {f}".format(f=f)
    fd = ceph.open(f, os.O_CREAT|os.O_RDWR, 0644)
    ceph.close(fd)
    return ceph.stat(f)['st_ino']

def set_mds_config_param(ceph, param):
    with file('/dev/null', 'rb') as devnull:
        confarg = ''
        if conf != '':
            confarg = '-c {c}'.format(c=conf)
        r = sub.call("ceph {ca} mds tell a injectargs '{p}'".format(ca=confarg, p=param), shell=True, stdout=devnull)
        if (r != 0):
            raise

import ConfigParser
import contextlib

class _TrimIndentFile(object):
    def __init__(self, fp):
        self.fp = fp

    def readline(self):
        line = self.fp.readline()
        return line.lstrip(' \t')

def _optionxform(s):
    s = s.replace('_', ' ')
    s = '_'.join(s.split())
    return s

def conf_set_kill_mds(location, killnum):
    print >>sys.stderr, 'setting mds kill config option for {l}.{k}'.format(l=location, k=killnum)
    print "restart mds a mds_kill_{l}_at {k}".format(l=location, k=killnum)
    sys.stdout.flush()
    for l in sys.stdin.readline():
        if l == 'restarted':
            break

def flush(ceph, testnum):
    print >>sys.stderr, 'flushing {t}'.format(t=testnum)
    set_mds_config_param(ceph, '--mds_log_max_segments 1')

    for i in range(1, 500):
        f = '{p}.{pid}.{t}.{i}'.format(p=prefix, pid=os.getpid(), t=testnum, i=i)
        print >>sys.stderr, 'flushing with create {f}'.format(f=f)
        fd = ceph.open(f, os.O_CREAT | os.O_RDWR, 0644)
        ceph.close(fd)
        ceph.unlink(f)

    print >> sys.stderr, 'flush doing shutdown'
    ceph.shutdown()
    print >> sys.stderr, 'flush reinitializing ceph'
    ceph = cephfs.LibCephFS(conffile=conf)
    print >> sys.stderr, 'flush doing mount'
    ceph.mount()
    return ceph

def kill_mds(ceph, location, killnum):
    print >>sys.stderr, 'killing mds: {l}.{k}'.format(l=location, k=killnum)
    set_mds_config_param(ceph, '--mds_kill_{l}_at {k}'.format(l=location, k=killnum))

def wait_for_mds(ceph):
    # wait for restart
    while True:
        confarg = ''
        if conf != '':
            confarg = '-c {c}'.format(c=conf)
        r = sub.check_output("ceph {ca} mds stat".format(ca=confarg), shell=True)
        if r.find('a=up:active'):
            break
        time.sleep(1)

def decode(value):

    tmpfile = '/tmp/{p}.{pid}'.format(p=prefix, pid=os.getpid())
    with open(tmpfile, 'w+') as f:
      f.write(value)

    p = sub.Popen(
        [
            'ceph-dencoder',
            'import',
            tmpfile,
            'type',
            'inode_backtrace_t',
            'decode',
            'dump_json',
        ],
        stdin=sub.PIPE,
        stdout=sub.PIPE,
      )
    (stdout, _) = p.communicate(input=value)
    p.stdin.close()
    if (p.returncode != 0):
        raise
    os.remove(tmpfile)
    return json.loads(stdout)

class VerifyFailure(Exception):
    pass

def verify(rados_ioctx, ino, values, pool):
    print >>sys.stderr, 'getting parent attr for ino: %lx.00000000' % ino
    savede = None
    for i in range(1, 20):
        try:
            savede = None
            binbt = rados_ioctx.get_xattr('%lx.00000000' % ino, 'parent')
        except rados.ObjectNotFound as e:
            # wait for a bit to let segments get flushed out
            savede = e
            time.sleep(10)
    if savede:
        raise savede

    bt = decode(binbt)

    if bt['ino'] != ino:
        raise VerifyFailure('inode mismatch: {bi} != {ino}\n\tbacktrace:\n\t\t{bt}\n\tfailed verify against:\n\t\t{i}, {v}'.format(
                    bi=bt['ancestors'][ind]['dname'], ino=ino, bt=bt, i=ino, v=values))
    ind = 0
    for (n, i) in values:
        if bt['ancestors'][ind]['dirino'] != i:
            raise VerifyFailure('ancestor dirino mismatch: {b} != {ind}\n\tbacktrace:\n\t\t{bt}\n\tfailed verify against:\n\t\t{i}, {v}'.format(
                    b=bt['ancestors'][ind]['dirino'], ind=i, bt=bt, i=ino, v=values))
        if bt['ancestors'][ind]['dname'] != n:
            raise VerifyFailure('ancestor dname mismatch: {b} != {n}\n\tbacktrace:\n\t\t{bt}\n\tfailed verify against:\n\t\t{i}, {v}'.format(
                    b=bt['ancestors'][ind]['dname'], n=n, bt=bt, i=ino, v=values))
        ind += 1

    if bt['pool'] != pool:
        raise VerifyFailure('pool mismatch: {btp} != {p}\n\tbacktrace:\n\t\t{bt}\n\tfailed verify against:\n\t\t{i}, {v}'.format(
                    btp=bt['pool'], p=pool, bt=bt, i=ino, v=values))

def make_abc(ceph, rooti, i):
    expected_bt = []
    c, d = get_name("/", i, 0)
    expected_bt = [(c, rooti)] + expected_bt
    di = mkdir(ceph, d)
    c, d = get_name(d, i, 1)
    expected_bt = [(c, di)] + expected_bt
    di = mkdir(ceph, d)
    c, f = get_name(d, i, 2)
    fi = create(ceph, f)
    expected_bt = [(c, di)] + expected_bt
    return fi, expected_bt

test = -1
if len(sys.argv) > 1:
    test = int(sys.argv[1])

conf = ''
if len(sys.argv) > 2:
    conf = sys.argv[2]

radosobj = rados.Rados(conffile=conf)
radosobj.connect()
ioctx = radosobj.open_ioctx('data')

ceph = cephfs.LibCephFS(conffile=conf)
ceph.mount()

rooti = ceph.stat('/')['st_ino']

test = -1
if len(sys.argv) > 1:
    test = int(sys.argv[1])

conf = '/etc/ceph/ceph.conf'
if len(sys.argv) > 2:
    conf = sys.argv[2]

# create /a/b/c
# flush
# verify

i = 0
if test < 0 or test == i:
  print >>sys.stderr, 'Running test %d: basic verify' % i
  ino, expected_bt = make_abc(ceph, rooti, i)
  ceph = flush(ceph, i)
  verify(ioctx, ino, expected_bt, 0)

i += 1

# kill-mds-at-openc-1
# create /a/b/c
# restart-mds
# flush
# verify

if test < 0 or test == i:
  print >>sys.stderr, 'Running test %d: kill openc' % i
  print "restart mds a"
  sys.stdout.flush()
  kill_mds(ceph, 'openc', 1)
  ino, expected_bt = make_abc(ceph, rooti, i)
  ceph = flush(ceph, i)
  verify(ioctx, ino, expected_bt, 0)

i += 1

# kill-mds-at-openc-1
# create /a/b/c
# restart-mds with kill-mds-at-replay-1
# restart-mds
# flush
# verify
if test < 0 or test == i:
  print >>sys.stderr, 'Running test %d: kill openc/replay' % i
  # these are reversed because we want to prepare the config
  conf_set_kill_mds('journal_replay', 1)
  kill_mds(ceph, 'openc', 1)
  print "restart mds a"
  sys.stdout.flush()
  ino, expected_bt = make_abc(ceph, rooti, i)
  ceph = flush(ceph, i)
  verify(ioctx, ino, expected_bt, 0)

i += 1

ioctx.close()
radosobj.shutdown()
ceph.shutdown()

print "done"
sys.stdout.flush()
