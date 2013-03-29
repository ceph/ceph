#!/usr/bin/python
#
# test_mon_config_key - Test 'ceph config-key' interface
#
# Copyright (C) 2013 Inktank
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

import sys
import os
import base64
import time
import errno
import random
import subprocess
import string

sizes = [
    (0, 0),
    (10, 0),
    (25, 0),
    (50, 0),
    (100, 0),
    (1000, 0),
    (4096, 0),
    (4097, -errno.EFBIG),
    (8192, -errno.EFBIG)
    ]
ops = [ 'put', 'del', 'exists', 'get' ]

config_put = []       #list: keys
config_del = []       #list: keys
config_existing = {}  #map: key -> size

def run_cmd(cmd, expects=0):
  full_cmd = [ 'ceph', 'config-key' ] + cmd

  if expects < 0:
    expects = -expects

  print 'run_cmd >> {fc}'.format(fc=' '.join(full_cmd))

  try:
    subprocess.check_call(full_cmd)
  except subprocess.CalledProcessError as err:
    assert err.returncode == expects, \
        'command \'{c}\' failed with error \'{e}\' (expected \'{x}\')'.format(
            c=' '.join(full_cmd),e='({code}) {msg}'.format(
              code=err.returncode,msg=os.strerror(err.returncode)),x=expects)
#end run_cmd

def gen_data(size,rnd):
  chars = string.ascii_letters + string.digits
  return ''.join(rnd.choice(chars) for i in range(size))

def gen_key(rnd):
  return gen_data(20,rnd)

def gen_tmp_file_path(rnd):
  file_name = gen_data(20,rnd)
  file_path = os.path.join('/tmp', 'ceph-test.'+file_name)
  return file_path

def destroy_tmp_file(fpath):
  if os.path.exists(fpath) and os.path.isfile(fpath):
    os.unlink(fpath)

def write_data_file(data, rnd):
  file_path = gen_tmp_file_path(rnd)
  f = open(file_path, 'wr+')
  f.truncate()
  f.write(data)
  f.close()
  return file_path
#end write_data_file



if __name__ == "__main__":

  duration = 300 # 5 minutes
  if len(sys.argv) > 1:
    duration = int(sys.argv[1])

  seed = int(time.time())
  if len(sys.argv) > 2:
    seed = int(sys.argv[2])

  print 'seed: {s}'.format(s=seed)

  rnd = random.Random()
  rnd.seed(seed)

  start = time.time()

  while (time.time() - start) < duration:
    op = rnd.choice(ops)

    if op == 'put':

      via_file = (rnd.uniform(0,100) < 50.0)

      subops = [ 'existing', 'new' ]
      sop = rnd.choice(subops)

      expected = 0
      cmd = [ 'put' ]
      key = None

      if sop == 'existing':
        if len(config_existing) == 0:
          print '{o}::{s} >> no existing keys; continue'.format(o=op,s=sop)
          continue
        key = rnd.choice(config_put)
        assert key in config_existing, \
            'key \'{k_}\' not in config_existing'.format(k_=key)

        expected = 0 # the store just overrides the value if the key exists
      #end if sop == 'existing'
      elif sop == 'new':
        for x in xrange(0,10):
          key = gen_key(rnd)
          if key not in config_existing:
            break
          key = None
        if key is None:
          print '{o}::{s} >> unable to generate an unique key'\
                ' -- try again later.'.format(o=op,s=sop)
          continue
        assert key not in config_put and key not in config_existing,\
            'key {k} was not supposed to exist!'.format(k=key)

      assert key is not None, \
          'key must be != None'

      cmd += [ key ]

      (size,error) = rnd.choice(sizes)
      if size > 25:
        via_file = True

      data = gen_data(size,rnd)
      if error == 0: # only add if we expect the put to be successful
        if sop == 'new':
          config_put.append(key)
        config_existing[key] = size
      expected = error

      if via_file:
        data_file = write_data_file(data, rnd)
        cmd += [ '-i', data_file ]
      else:
        cmd += [ data ]

      print '{o}::{s} >> size: {sz}, via: {v}'.format(
          o=op,s=sop,
          sz=size,v='file: {f}'.format(f=data_file) if via_file == True else 'cli')
      run_cmd(cmd, expects=expected)
      if via_file:
        destroy_tmp_file(data_file)
      continue
    elif op == 'del':
      subops = [ 'existing', 'enoent' ]
      sop = rnd.choice(subops)

      expected = 0
      cmd = [ 'del' ]
      key = None

      if sop == 'existing':
        if len(config_existing) == 0:
          print '{o}::{s} >> no existing keys; continue'.format(o=op,s=sop)
          continue
        key = rnd.choice(config_put)
        assert key in config_existing, \
            'key \'{k_}\' not in config_existing'.format(k_=key)

      if sop == 'enoent':
        for x in xrange(0,10):
          key = base64.b64encode(os.urandom(20))
          if key not in config_existing:
            break
          key = None
        if key is None:
          print '{o}::{s} >> unable to generate an unique key'\
                ' -- try again later.'.format(o=op,s=sop)
          continue
        assert key not in config_put and key not in config_existing,\
            'key {k} was not supposed to exist!'.format(k=key)
        expected = 0  # deleting a non-existent key succeeds

      assert key is not None, \
          'key must be != None'

      cmd += [ key ]
      print '{o}::{s} >> key: {k}'.format(o=op,s=sop,k=key)
      run_cmd(cmd, expects=expected)
      if op == 'existing':
        config_del.append(key)
        config_put.remove(key)
        config_existing.erase(key)
      continue
    elif op == 'exists':
      subops = [ 'existing', 'enoent' ]
      sop = rnd.choice(subops)

      expected = 0
      cmd = [ 'exists' ]
      key = None

      if sop == 'existing':
        if len(config_existing) == 0:
          print '{o}::{s} >> no existing keys; continue'.format(o=op,s=sop)
          continue
        key = rnd.choice(config_put)
        assert key in config_existing, \
            'key \'{k_}\' not in config_existing'.format(k_=key)

      if sop == 'enoent':
        for x in xrange(0,10):
          key = base64.b64encode(os.urandom(20))
          if key not in config_existing:
            break
          key = None
        if key is None:
          print '{o}::{s} >> unable to generate an unique key'\
                ' -- try again later.'.format(o=op,s=sop)
          continue
        assert key not in config_put and key not in config_existing,\
            'key {k} was not supposed to exist!'.format(k=key)
        expected = -errno.ENOENT

      assert key is not None, \
          'key must be != None'

      cmd += [ key ]
      print '{o}::{s} >> key: {k}'.format(o=op,s=sop,k=key)
      run_cmd(cmd, expects=expected)
      continue
    elif op == 'get':
      subops = [ 'existing', 'enoent' ]
      sop = rnd.choice(subops)

      expected = 0
      cmd = [ 'get' ]
      key = None

      if sop == 'existing':
        if len(config_existing) == 0:
          print '{o}::{s} >> no existing keys; continue'.format(o=op,s=sop)
          continue
        key = rnd.choice(config_put)
        assert key in config_existing, \
            'key \'{k_}\' not in config_existing'.format(k_=key)

      if sop == 'enoent':
        for x in xrange(0,10):
          key = base64.b64encode(os.urandom(20))
          if key not in config_existing:
            break
          key = None
        if key is None:
          print '{o}::{s} >> unable to generate an unique key'\
                ' -- try again later.'.format(o=op,s=sop)
          continue
        assert key not in config_put and key not in config_existing,\
            'key {k} was not supposed to exist!'.format(k=key)
        expected = -errno.ENOENT

      assert key is not None, \
          'key must be != None'

      file_path = gen_tmp_file_path(rnd)
      cmd += [ key, '-o', file_path ]
      print '{o}::{s} >> key: {k}'.format(o=op,s=sop,k=key)
      run_cmd(cmd, expects=expected)
      if sop == 'existing':
        try:
          f = open(file_path, 'r+')
        except IOError as err: 
          if err == errno.ENOENT:
            assert config_existing[key] == 0, \
              'error opening \'{fp}\': {e}'.format(fp=file_path,e=err)
        cnt = 0
        while True:
          l = f.read()
          if l == '':
            break;
          cnt += len(l)
        assert cnt == config_existing[key], \
          'wrong size from store for key \'{k}\': {sz}, expected {es}'.format(
              k=key,sz=cnt,es=config_existing[key])
        destroy_tmp_file(file_path)
      continue
    else:
      assert False, 'unknown op {o}'.format(o=op)

  # check if all keys in 'config_put' exist and
  # if all keys on 'config_del' don't.
  # but first however, remove all keys in config_put that might
  # be in config_del as well.
  config_put_set = set(config_put)
  config_del_set = set(config_del).difference(config_put_set)

  for k in config_put_set:
    print 'check::puts >> key: {k_}'.format(k_=k)
    run_cmd(['exists', k], expects=0)
  for k in config_del_set:
    print 'check::dels >> key: {k_}'.format(k_=k)
    run_cmd(['exists', k], expects=-errno.ENOENT)
