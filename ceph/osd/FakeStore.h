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


#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"
#include "common/ThreadPool.h"
#include "common/Mutex.h"

#include "Fake.h"
//#include "FakeStoreBDBCollections.h"


#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


// fake attributes in memory, if we need to.


class FakeStore : public ObjectStore, 
                  public FakeStoreAttrs,
                  public FakeStoreCollections {
  string basedir;
  int whoami;
  
  int unsync;

  Mutex lock;

  // fns
  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn);
  void wipe_dir(string mydir);


 public:
  FakeStore(char *base, int whoami) : FakeStoreAttrs(this), FakeStoreCollections(this)
  {
    this->basedir = base;
    this->whoami = whoami;
    unsync = 0;
  }


  int mount();
  int umount();
  int mkfs();

  int statfs(struct statfs *buf);

  // ------------------
  // objects
  int pick_object_revision(object_t& oid) {
    return 0;
  }
  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);
  int remove(object_t oid, Context *onsafe);
  int truncate(object_t oid, off_t size, Context *onsafe);
  int read(object_t oid, 
           off_t offset, size_t len,
           bufferlist& bl);
  int write(object_t oid, 
            off_t offset, size_t len,
            bufferlist& bl, 
            Context *onsafe);

  void sync(Context *onsafe);
};

#endif
