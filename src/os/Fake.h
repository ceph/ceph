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


#ifndef __FAKE_H
#define __FAKE_H

#include "include/types.h"

#include <list>
#include <set>
#include <ext/hash_map>
using namespace std;
using namespace __gnu_cxx;

class FakeCollections {
 private:
  Mutex faker_lock;
  ObjectStore *store;
  hash_map<coll_t, set<sobject_t> > fakecollections;

 public:
  FakeCollections(ObjectStore *s) : faker_lock("FakeCollections::faker_lock"), store(s) {}

  // faked collections
  int list_collections(vector<coll_t>& ls) {
    faker_lock.Lock();
    int r = 0;
    for (hash_map< coll_t, set<sobject_t> >::iterator p = fakecollections.begin();
         p != fakecollections.end();
         p++) {
      r++;
      ls.push_back(p->first);
    }
    faker_lock.Unlock();
    return r;
  }

  int create_collection(coll_t c,
                        Context *onsafe=0) {
    faker_lock.Lock();
    fakecollections[c].size();
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return 0;
  }

  int destroy_collection(coll_t c,
                         Context *onsafe=0) {
    int r = 0;
    faker_lock.Lock();
    if (fakecollections.count(c)) {
      fakecollections.erase(c);
      //fakecattr.erase(c);
      if (onsafe) store->sync(onsafe);
    } else 
      r = -1;
    faker_lock.Unlock();
    return r;
  }

  int collection_stat(coll_t c, struct stat *st) {
    return collection_exists(c) ? 0:-1;
  }

  bool collection_exists(coll_t c) {
    faker_lock.Lock();
    int r = fakecollections.count(c);
    faker_lock.Unlock();
    return r;
  }

  int collection_add(coll_t c, sobject_t o,
                     Context *onsafe=0) {
    faker_lock.Lock();
    fakecollections[c].insert(o);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return 0;
  }

  int collection_remove(coll_t c, sobject_t o,
                        Context *onsafe=0) {
    faker_lock.Lock();
    fakecollections[c].erase(o);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return 0;
  }

  int collection_empty(coll_t c) {
    Mutex::Locker l(faker_lock);
    return fakecollections[c].empty();
  }

  int collection_list(coll_t c, vector<sobject_t>& o) {
    faker_lock.Lock();
    int r = 0;
    for (set<sobject_t>::iterator p = fakecollections[c].begin();
         p != fakecollections[c].end();
         p++) {
      o.push_back(*p);
      r++;
    }
    faker_lock.Unlock();
    return r;
  }

};

class FakeAttrs {
 private:
  
  class FakeAttrSet {
  public:
    map<string, bufferptr> attrs;
    
    int getattr(const char *name, void *value, size_t size) {
      string n = name;
      if (attrs.count(n)) {
        size_t l = MIN( attrs[n].length(), size );
        bufferlist bl;
        bl.append(attrs[n]);
        bl.copy(0, l, (char*)value);
        return l;
      }
      return -ENODATA;
    }
    int getattr(const char *name, bufferptr &bp) {
      string n = name;
      if (attrs.count(n)) {
	bp = attrs[n];
	return bp.length();
      }
      return -ENODATA;
    }
    int getattr(const char *name, bufferlist& bl) {
      string n = name;
      if (attrs.count(n)) {
        bl.append(attrs[n]);
        return bl.length();
      }
      return -1;
    }
    int getattrs(map<string,bufferptr>& aset) {
      aset = attrs;
      return 0;
    }
    int setattrs(map<string,bufferptr>& aset) {
      attrs = aset;
      return 0;
    }
    
    int setattr(const char *name, const void *value, size_t size) {
      string n = name;
      bufferptr bp = buffer::copy((char*)value, size);
      attrs[n] = bp;
      return 0;
    }
    
    int listattr(char *attrs, size_t size) {
      assert(0);
      return 0;
    }

    int rmattr(const char *name) {
      string n = name;
      attrs.erase(n);
      return 0;
    }
    
    bool empty() { return attrs.empty(); }
  };

  Mutex faker_lock;
  ObjectStore *store;
  hash_map<sobject_t, FakeAttrSet> fakeoattrs;
  hash_map<coll_t, FakeAttrSet> fakecattrs;

 public:
  FakeAttrs(ObjectStore *s) : faker_lock("FakeAttrs::faker_lock"), store(s) {}

  int setattr(coll_t cid, sobject_t oid, const char *name,
              const void *value, size_t size,
              Context *onsafe=0) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].setattr(name, value, size);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return r;
  }
  int setattrs(coll_t cid, sobject_t oid, map<string,bufferptr>& aset) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].setattrs(aset);
    faker_lock.Unlock();
    return r;
  }
  int getattr(coll_t cid, sobject_t oid, const char *name,
              void *value, size_t size) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].getattr(name, value, size);
    faker_lock.Unlock();
    return r;
  }
  int getattr(coll_t cid, sobject_t oid, const char *name, bufferptr& bp) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].getattr(name, bp);
    faker_lock.Unlock();
    return r;
  }
  int getattrs(coll_t cid, sobject_t oid, map<string,bufferptr>& aset) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].getattrs(aset);
    faker_lock.Unlock();
    return r;
  }
  int rmattr(coll_t cid, sobject_t oid, const char *name,
             Context *onsafe=0) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].rmattr(name);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return r;
  }

  int listattr(coll_t cid, sobject_t oid, char *attrs, size_t size) {
    faker_lock.Lock();
    int r = fakeoattrs[oid].listattr(attrs,size);
    faker_lock.Unlock();
    return r;
  }

  int collection_setattr(coll_t c, const char *name,
                         const void *value, size_t size,
                         Context *onsafe=0) {
    faker_lock.Lock();
    int r = fakecattrs[c].setattr(name, value, size);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return r;
  }
  int collection_setattrs(coll_t cid, map<string,bufferptr>& aset) {
    faker_lock.Lock();
    int r = fakecattrs[cid].setattrs(aset);
    faker_lock.Unlock();
    return r;
  }
  int collection_getattrs(coll_t cid, map<string,bufferptr>& aset) {
    faker_lock.Lock();
    int r = fakecattrs[cid].getattrs(aset);
    faker_lock.Unlock();
    return r;
  }
  int collection_rmattr(coll_t c, const char *name,
                        Context *onsafe=0) {
    faker_lock.Lock();
    int r = fakecattrs[c].rmattr(name);
    if (onsafe) store->sync(onsafe);
    faker_lock.Unlock();
    return r;
  }
  int collection_getattr(coll_t c, const char *name,
                         void *value, size_t size) {
    faker_lock.Lock();
    int r = fakecattrs[c].getattr(name, value, size);
    faker_lock.Unlock();
    return r;
  }
  int collection_getattr(coll_t c, const char *name, bufferlist& bl) {
    faker_lock.Lock();
    int r = fakecattrs[c].getattr(name, bl);
    faker_lock.Unlock();
    return r;
  }
  int collection_listattr(coll_t c, char *attrs, size_t size) {
    faker_lock.Lock();
    int r = fakecattrs[c].listattr(attrs,size);
    faker_lock.Unlock();
    return r;
  }

};

#endif
