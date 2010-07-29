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


#ifndef CEPH_FAKESTOREBDBCOLLECTIONS_H
#define CEPH_FAKESTOREBDBCOLLECTIONS_H

#include "BDBMap.h"
#include "ObjectStore.h"
#include "common/Mutex.h"

#define BDBHASH_DIRS       128LL
#define BDBHASH_FUNC(x)    (((x) ^ ((x)>>30) ^ ((x)>>18) ^ ((x)>>45) ^ 0xdead1234) * 884811 % BDBHASH_DIRS)

class FakeStoreBDBCollections {
 private:
  int whoami;
  string basedir;

  Mutex bdblock;

  // collection dbs
  BDBMap<coll_t, int>                 collections;
  map<coll_t, BDBMap<object_t, int>*> collection_map;
  
  // dirs
  void get_dir(string& dir) {
    char s[30];
    sprintf(s, "%d", whoami);
    dir = basedir + "/" + s;
  }
  void get_collfn(coll_t c, string &fn) {
    char s[100];
    sprintf(s, "%d/%02llx/%016llx.co", whoami, BDBHASH_FUNC(c), c);
    fn = basedir + "/" + s;
  }

  void open_collections() {
    string cfn;
    get_dir(cfn);
    cfn += "/collections";
    collections.open(cfn.c_str());  
    list<coll_t> ls;
    collections.list_keys(ls);
  }
  void close_collections() {
    if (collections.is_open())
      collections.close();
    
    for (map<coll_t, BDBMap<object_t, int>*>::iterator it = collection_map.begin();
         it != collection_map.end();
         it++) {
      it->second->close();
    }
    collection_map.clear();
  }
  
  int open_collection(coll_t c) {
    if (collection_map.count(c))
      return 0;  // already open.
    
    string fn;
    get_collfn(c,fn);
    collection_map[c] = new BDBMap<coll_t,int>;
    int r = collection_map[c]->open(fn.c_str());
    if (r != 0)
      collection_map.erase(c);  // failed
    return r;
  }
  
 public:
  FakeStoreBDBCollections(int w, string& bd) : whoami(w), basedir(bd) {}
  ~FakeStoreBDBCollections() {
    close_collections();
  }

  int list_collections(list<coll_t>& ls) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    ls.clear();
    collections.list_keys(ls);
    bdblock.Unlock();
    return 0;
  }
  int create_collection(coll_t c) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    collections.put(c, 1);
    open_collection(c);
    bdblock.Unlock();
    return 0;
  }
  int destroy_collection(coll_t c) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    collections.del(c);
    
    open_collection(c);
    collection_map[c]->close();
    
    string fn;
    get_collfn(c,fn);
    collection_map[c]->remove(fn.c_str());
    delete collection_map[c];
    collection_map.erase(c);
    bdblock.Unlock();
    return 0;
  }
  int collection_stat(coll_t c, struct stat *st) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    string fn;
    get_collfn(c,fn);
    int r = ::stat(fn.c_str(), st);
    bdblock.Unlock();
    return r;
  }
  bool collection_exists(coll_t c) {
    bdblock.Lock();
    struct stat st;
    int r = collection_stat(c, &st) == 0;
    bdblock.Unlock();
    return r;
  }
  int collection_add(coll_t c, object_t o) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    open_collection(c);
    collection_map[c]->put(o,1);
    bdblock.Unlock();
    return 0;
  }
  int collection_remove(coll_t c, object_t o) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    open_collection(c);
    collection_map[c]->del(o);
    bdblock.Unlock();
    return 0;
  }
  int collection_list(coll_t c, list<object_t>& o) {
    bdblock.Lock();
    if (!collections.is_open()) open_collections();
    
    open_collection(c);
    collection_map[c]->list_keys(o);
    bdblock.Unlock();
    return 0;
  }
};

#endif
