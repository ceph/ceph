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


#ifndef __FILESTORE_H
#define __FILESTORE_H

#include "ObjectStore.h"
#include "JournalingObjectStore.h"
#include "common/ThreadPool.h"
#include "common/Mutex.h"

#include "Fake.h"
//#include "FakeStoreBDBCollections.h"


#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


// fake attributes in memory, if we need to.

class FileStore : public JournalingObjectStore {
  string basedir;
  __u64 fsid;
  
  int btrfs_fd;  // >= if btrfs

  // fake attrs?
  FakeAttrs attrs;
  bool fake_attrs;

  // fake collections?
  FakeCollections collections;
  bool fake_collections;
  
  // helper fns
  void append_oname(const pobject_t &oid, char *s);
  //void get_oname(pobject_t oid, char *s);
  void get_cdir(coll_t cid, char *s);
  void get_coname(coll_t cid, pobject_t oid, char *s);
  pobject_t parse_object(char *s);
  coll_t parse_coll(char *s);

  // sync thread
  Mutex lock;
  Cond sync_cond;
  bool stop;
  void sync_entry();
  struct SyncThread : public Thread {
    FileStore *fs;
    SyncThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->sync_entry();
      return 0;
    }
  } sync_thread;

  void sync_fs(); // actuall sync underlying fs

 public:
  FileStore(const char *base) : 
    basedir(base),
    btrfs_fd(-1),
    attrs(this), fake_attrs(false), 
    collections(this), fake_collections(false),
    stop(false), sync_thread(this) { }

  int mount();
  int umount();
  int mkfs();

  int transaction_start();
  void transaction_end(int id);

  int statfs(struct statfs *buf);

  // ------------------
  // objects
  int pick_object_revision_lt(pobject_t& oid) {
    return 0;
  }
  bool exists(coll_t cid, pobject_t oid);
  int stat(coll_t cid, pobject_t oid, struct stat *st);
  int remove(coll_t cid, pobject_t oid, Context *onsafe);
  int truncate(coll_t cid, pobject_t oid, off_t size, Context *onsafe);
  int read(coll_t cid, pobject_t oid, off_t offset, size_t len, bufferlist& bl);
  int write(coll_t cid, pobject_t oid, off_t offset, size_t len, const bufferlist& bl, Context *onsafe);
  int clone(coll_t cid, pobject_t oldoid, pobject_t newoid);

  void sync();
  void sync(Context *onsafe);

  int list_objects(list<pobject_t>& ls);

  // attrs
  int setattr(coll_t cid, pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe=0);
  int setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset);
  int getattr(coll_t cid, pobject_t oid, const char *name, void *value, size_t size);
  int getattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset);
  int rmattr(coll_t cid, pobject_t oid, const char *name, Context *onsafe=0);

  int collection_setattr(coll_t c, const char *name, const void *value, size_t size, Context *onsafe=0);
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
  int collection_add(coll_t c, coll_t ocid, pobject_t o, Context *onsafe=0);
  int collection_remove(coll_t c, pobject_t o, Context *onsafe=0);
  int collection_list(coll_t c, list<pobject_t>& o);

  int pick_object_revision_lt(coll_t cid, pobject_t& oid) { return -1; }
  void trim_from_cache(coll_t cid, pobject_t oid, off_t offset, size_t len) {}
  int is_cached(coll_t cid, pobject_t oid, off_t offset, size_t len) { return -1; }


};

#endif
