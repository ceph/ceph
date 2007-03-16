// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __FILECACHE_H
#define __FILECACHE_H

#include <iostream>
using namespace std;

#include "common/Cond.h"
#include "mds/Capability.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;
#include "crypto/ExtCap.h"

class ObjectCacher;

class FileCache {
  ObjectCacher *oc;
  inode_t inode;
  
  // caps
  int latest_caps;
  map<int, list<Context*> > caps_callbacks;

  int num_reading;
  int num_writing;
  //int num_unsafe;

  // waiters
  set<Cond*> waitfor_read;
  set<Cond*> waitfor_write;

  bool waitfor_release;

 public:
  FileCache(ObjectCacher *_oc, inode_t _inode) : 
    oc(_oc), 
    inode(_inode),
    latest_caps(0),
    num_reading(0), num_writing(0),// num_unsafe(0),
    waitfor_release(false) {}
  ~FileCache() {
    tear_down();
  }

  // waiters/waiting
  bool can_read() { return latest_caps & CAP_FILE_RD; }
  bool can_write() { return latest_caps & CAP_FILE_WR; }
  bool all_safe();// { return num_unsafe == 0; }

  void add_safe_waiter(Context *c);

  // ...
  void flush_dirty(Context *onflush=0);
  off_t release_clean();
  void empty(Context *onempty=0);
  bool is_empty() { return !(is_cached() || is_dirty()); }
  bool is_cached();
  bool is_dirty();  

  void tear_down();

  int get_caps() { return latest_caps; }
  void set_caps(int caps, Context *onimplement=0);
  void check_caps();

  int read(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock,
	   ExtCap *read_ext_cap=0);  // may block.
  void write(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock,
	     ExtCap *write_ext_cap=0);  // may block.

};


#endif
