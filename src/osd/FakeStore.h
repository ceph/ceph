// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

class FakeStore : public ObjectStore {
  string basedir;

  Mutex synclock;
  Cond synccond;
  int unsync;

  // fake attrs?
  FakeStoreAttrs attrs;
  bool fake_attrs;

  // fake collections?
  FakeStoreCollections collections;
  bool fake_collections;
  
  // helper fns
  void get_oname(pobject_t oid, char *s);
  void get_cdir(coll_t cid, char *s);
  void get_coname(coll_t cid, pobject_t oid, char *s);
  pobject_t parse_object(char *s);
  coll_t parse_coll(char *s);

 public:
  FakeStore(const char *base) : 
    basedir(base),
    unsync(0),
    attrs(this), fake_attrs(false), 
    collections(this), fake_collections(false) { }

  int mount();
  int umount();
  int mkfs();

  int statfs(struct statfs *buf);

  // ------------------
  // objects
  int pick_object_revision_lt(pobject_t& oid) {
    return 0;
  }
  bool exists(pobject_t oid);
  int stat(pobject_t oid, struct stat *st);
  int remove(pobject_t oid, Context *onsafe);
  int truncate(pobject_t oid, off_t size, Context *onsafe);
  int read(pobject_t oid, off_t offset, size_t len, bufferlist& bl);
  int write(pobject_t oid, off_t offset, size_t len, const bufferlist& bl, Context *onsafe);

  void sync();
  void sync(Context *onsafe);

  int list_objects(list<pobject_t>& ls);

  // attrs
  int setattr(pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe=0);
  int setattrs(pobject_t oid, map<string,bufferptr>& aset);
  int getattr(pobject_t oid, const char *name, void *value, size_t size);
  int getattrs(pobject_t oid, map<string,bufferptr>& aset);
  int rmattr(pobject_t oid, const char *name, Context *onsafe=0);
  //int listattr(pobject_t oid, char *attrs, size_t size);
  int collection_setattr(coll_t c, const char *name, void *value, size_t size, Context *onsafe=0);
  int collection_rmattr(coll_t c, const char *name, Context *onsafe=0);
  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  //int collection_listattr(coll_t c, char *attrs, size_t size);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);
  int collection_setattrs(coll_t cid, map<string,bufferptr> &aset);

  // collections
  int list_collections(list<coll_t>& ls);
  int create_collection(coll_t c, Context *onsafe=0);
  int destroy_collection(coll_t c, Context *onsafe=0);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  int collection_add(coll_t c, pobject_t o, Context *onsafe=0);
  int collection_remove(coll_t c, pobject_t o, Context *onsafe=0);
  int collection_list(coll_t c, list<pobject_t>& o);



};

#endif
