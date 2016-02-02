#!/usr/bin/python

import json
import subprocess
import shlex
from StringIO import StringIO
import errno
import sys
import os
import io
import re


import rados
from ceph_argparse import *

keyring_base = '/tmp/cephtest-caps.keyring'

class UnexpectedReturn(Exception):
  def __init__(self, cmd, ret, expected, msg):
    if isinstance(cmd, list):
      self.cmd = ' '.join(cmd)
    else:
      assert isinstance(cmd, str) or isinstance(cmd, unicode), \
          'cmd needs to be either a list or a str'
      self.cmd = cmd
    self.cmd = str(self.cmd)
    self.ret = int(ret)
    self.expected = int(expected)
    self.msg = str(msg)

  def __str__(self):
    return repr('{c}: expected return {e}, got {r} ({o})'.format(
        c=self.cmd, e=self.expected, r=self.ret, o=self.msg))

def call(cmd):
  if isinstance(cmd, list):
    args = cmd
  elif isinstance(cmd, str) or isinstance(cmd, unicode):
    args = shlex.split(cmd)
  else:
    assert False, 'cmd is not a string/unicode nor a list!'

  print 'call: {0}'.format(args)
  proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  ret = proc.wait()

  return (ret, proc)

def expect(cmd, expected_ret):

  try:
    (r, p) = call(cmd)
  except ValueError as e:
    print >> sys.stderr, \
             'unable to run {c}: {err}'.format(c=repr(cmd), err=e.message)
    return errno.EINVAL

  assert r == p.returncode, \
      'wth? r was supposed to match returncode!'

  if r != expected_ret:
    raise UnexpectedReturn(repr(cmd), r, expected_ret, str(p.stderr.read()))

  return p

def expect_to_file(cmd, expected_ret, out_file, mode='a'):

  # Let the exception be propagated to the caller
  p = expect(cmd, expected_ret)
  assert p.returncode == expected_ret, \
      'expected result doesn\'t match and no exception was thrown!'

  with io.open(out_file, mode) as file:
    file.write(unicode(p.stdout.read()))

  return p

class Command:
  def __init__(self, cid, j):
    self.cid = cid[3:]
    self.perms = j['perm']
    self.module = j['module']

    self.sig = ''
    self.args = []
    for s in j['sig']:
      if not isinstance(s, dict):
        assert isinstance(s, str) or isinstance(s,unicode), \
            'malformatted signature cid {0}: {1}\n{2}'.format(cid,s,j)
        if len(self.sig) > 0:
          self.sig += ' '
        self.sig += s
      else:
        self.args.append(s)

  def __str__(self):
    return repr('command {0}: {1} (requires \'{2}\')'.format(self.cid,\
          self.sig, self.perms))


def destroy_keyring(path):
  if not os.path.exists(path):
    raise Exception('oops! cannot remove inexistent keyring {0}'.format(path))

  # grab all client entities from the keyring
  entities = [m.group(1) for m in [re.match(r'\[client\.(.*)\]', l)
                for l in [str(line.strip())
                  for line in io.open(path,'r')]] if m is not None]

  # clean up and make sure each entity is gone
  for e in entities:
    expect('ceph auth del client.{0}'.format(e), 0)
    expect('ceph auth get client.{0}'.format(e), errno.ENOENT)

  # remove keyring
  os.unlink(path)

  return True

def test_basic_auth():
  # make sure we can successfully add/del entities, change their caps
  # and import/export keyrings.

  expect('ceph auth add client.basicauth', 0)
  expect('ceph auth caps client.basicauth mon \'allow *\'', 0)
  # entity exists and caps do not match
  expect('ceph auth add client.basicauth', errno.EINVAL)
  # this command attempts to change an existing state and will fail
  expect('ceph auth add client.basicauth mon \'allow w\'', errno.EINVAL)
  expect('ceph auth get-or-create client.basicauth', 0)
  expect('ceph auth get-key client.basicauth', 0)
  expect('ceph auth get-or-create client.basicauth2', 0)
  # cleanup
  expect('ceph auth del client.basicauth', 0)
  expect('ceph auth del client.basicauth2', 0)

  return True

def gen_module_keyring(module):
  module_caps = [
      ('all', '{t} \'allow service {s} rwx\'', 0),
      ('none', '', errno.EACCES),
      ('wrong', '{t} \'allow service foobar rwx\'', errno.EACCES),
      ('right', '{t} \'allow service {s} {p}\'', 0),
      ('no-execute', '{t} \'allow service {s} x\'', errno.EACCES)
      ]

  keyring = '{0}.service-{1}'.format(keyring_base,module)
  for perms in 'r rw x'.split():
    for (n,p,r) in module_caps:
      c = p.format(t='mon', s=module, p=perms)
      expect_to_file(
          'ceph auth get-or-create client.{cn}-{cp} {caps}'.format(
          cn=n,cp=perms,caps=c), 0, keyring)

  return keyring


def test_all():


  perms = {
      'good': {
        'broad':[
          ('rwx', 'allow *'),
          ('r', 'allow r'),
          ('rw', 'allow rw'),
          ('x', 'allow x'),
          ],
        'service':[
          ('rwx', 'allow service {s} rwx'),
          ('r', 'allow service {s} r'),
          ('rw', 'allow service {s} rw'),
          ('x', 'allow service {s} x'),
          ],
        'command':[
          ('rwx', 'allow command "{c}"'),
          ],
        'command-with':[
          ('rwx', 'allow command "{c}" with {kv}')
          ],
        'command-with-prefix':[
          ('rwx', 'allow command "{c}" with {key} prefix {val}')
          ]
        },
      'bad': {
        'broad':[
          ('none', ''),
          ],
        'service':[
          ('none1', 'allow service foo rwx'),
          ('none2', 'allow service foo r'),
          ('none3', 'allow service foo rw'),
          ('none4', 'allow service foo x'),
          ],
        'command':[
          ('none', 'allow command foo'),
          ],
        'command-with':[
          ('none', 'allow command "{c}" with foo=bar'),
          ],
        'command-with-prefix':[
          ('none', 'allow command "{c}" with foo prefix bar'),
          ],
        }
      }

  cmds = {
      '':[
        {
          'cmd':('status', '', 'r')
          },
        {
          'pre':'heap start_profiler',
          'cmd':('heap', 'heapcmd=stats', 'rw'),
          'post':'heap stop_profiler'
          }
        ],
      'auth':[
        {
          'pre':'',
          'cmd':('auth list', '', 'r'),
          'post':''
          },
        {
          'pre':'auth get-or-create client.foo mon \'allow *\'',
          'cmd':('auth caps', 'entity="client.foo"', 'rw'),
          'post':'auth del client.foo'
          }
        ],
      'pg':[
        {
          'cmd':('pg getmap', '', 'r'),
          },
        ],
      'mds':[
        {
          'cmd':('mds getmap', '', 'r'),
          },
        {
          'cmd':('mds cluster_down', '', 'rw'),
          'post':'mds cluster_up'
          },
        ],
      'mon':[
        {
          'cmd':('mon getmap', '', 'r')
          },
        {
          'cmd':('mon remove', 'name=a', 'rw')
          }
        ],
      'osd':[
        {
          'cmd':('osd getmap', '', 'r'),
          },
        {
          'cmd':('osd pause', '', 'rw'),
          'post':'osd unpause'
          },
        {
          'cmd':('osd crush dump', '', 'r')
          },
        ],
      'config-key':[
          {
            'pre':'config-key put foo bar',
            'cmd':('config-key get', 'key=foo', 'r')
            },
          {
            'pre':'config-key put foo bar',
            'cmd':('config-key del', 'key=foo', 'rw')
            }
          ]
      }

  for (module,cmd_lst) in cmds.iteritems():
    k = keyring_base + '.' + module
    for cmd in cmd_lst:

      (cmd_cmd, cmd_args, cmd_perm) = cmd['cmd']
      cmd_args_key = ''
      cmd_args_val = ''
      if len(cmd_args) > 0:
        (cmd_args_key, cmd_args_val) = cmd_args.split('=')

      print 'generating keyring for {m}/{c}'.format(m=module,c=cmd_cmd)
      # gen keyring
      for (good_or_bad,kind_map) in perms.iteritems():
        for (kind,lst) in kind_map.iteritems():
          for (perm, cap) in lst:
            cap_formatted = cap.format(
                s=module,
                c=cmd_cmd,
                kv=cmd_args,
                key=cmd_args_key,
                val=cmd_args_val)

            if len(cap_formatted) == 0:
              run_cap = ''
            else:
              run_cap = 'mon \'{fc}\''.format(fc=cap_formatted)

            cname = 'client.{gb}-{kind}-{p}'.format(
                gb=good_or_bad,kind=kind,p=perm)
            expect_to_file(
                'ceph auth get-or-create {n} {c}'.format(
                  n=cname,c=run_cap), 0, k)
      # keyring generated
      print 'testing {m}/{c}'.format(m=module,c=cmd_cmd)

      # test
      for good_bad in perms.iterkeys():
        for (kind,lst) in perms[good_bad].iteritems():
          for (perm,_) in lst:
            cname = 'client.{gb}-{k}-{p}'.format(gb=good_bad,k=kind,p=perm)

          if good_bad == 'good':
            expect_ret = 0
          else:
            expect_ret = errno.EACCES

          if ( cmd_perm not in perm ):
            expect_ret = errno.EACCES
          if 'with' in kind and len(cmd_args) == 0:
            expect_ret = errno.EACCES
          if 'service' in kind and len(module) == 0:
            expect_ret = errno.EACCES

          if 'pre' in cmd and len(cmd['pre']) > 0:
            expect('ceph {0}'.format(cmd['pre']), 0)
          expect('ceph -n {cn} -k {k} {c} {arg_val}'.format(
            cn=cname,k=k,c=cmd_cmd,arg_val=cmd_args_val), expect_ret)
          if 'post' in cmd and len(cmd['post']) > 0:
            expect('ceph {0}'.format(cmd['post']), 0)
      # finish testing
      destroy_keyring(k)


  return True


def test_misc():

  k = keyring_base + '.misc'
  expect_to_file(
      'ceph auth get-or-create client.caps mon \'allow command "auth caps"' \
          ' with entity="client.caps"\'', 0, k)
  expect('ceph -n client.caps -k {kf} mon_status'.format(kf=k), errno.EACCES)
  expect('ceph -n client.caps -k {kf} auth caps client.caps mon \'allow *\''.format(kf=k), 0)
  expect('ceph -n client.caps -k {kf} mon_status'.format(kf=k), 0)
  destroy_keyring(k)

def main():

  test_basic_auth()
  test_all()
  test_misc()

  print 'OK'

  return 0

if __name__ == '__main__':
  main()

