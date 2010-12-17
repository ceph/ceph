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


#ifndef CEPH_BERKELEYDB_H
#define CEPH_BERKELEYDB_H

#include <db.h>
#include <list>
#include <unistd.h>

#include "common/debug.h"

using namespace std;

template<typename K, typename D>
class BDBMap {
 private:
  DB *dbp;
  
 public:
  BDBMap() : dbp(0) {}
  ~BDBMap() {
    close();
  }

  bool is_open() { return dbp ? true:false; }

  // open/close
  int open(const char *fn) {
    //cout << "open " << fn << endl;

    int r;
    if ((r = db_create(&dbp, NULL, 0)) != 0) {
      derr << "db_create: " << db_strerror(r) << dendl;
      assert(0);
    }

    dbp->set_errfile(dbp, stderr);
    dbp->set_errpfx(dbp, "bdbmap");

    r = dbp->open(dbp, NULL, fn, NULL, DB_BTREE, DB_CREATE, 0644);
    if (r != 0) {
      dbp->err(dbp, r, "%s", fn);
    }
    assert(r == 0);
    return 0;
  }
  void close() {
    if (dbp) {
      dbp->close(dbp,0);
      dbp = 0;
    }
  }
  void remove(const char *fn) {
    if (!dbp) open(fn);
    if (dbp) {
      dbp->remove(dbp, fn, 0, 0);
      dbp = 0;
    } else {
      ::unlink(fn);
    }
  }
  
  // accessors
  int put(K key,
          D data) {
    DBT k;
    memset(&k, 0, sizeof(k)); 
    k.data = &key;
    k.size = sizeof(K);
    DBT d;
    memset(&d, 0, sizeof(d));
    d.data = &data;
    d.size = sizeof(data);
    return dbp->put(dbp, NULL, &k, &d, 0);
  }

  int get(K key,
          D& data) {
    DBT k;
    memset(&k, 0, sizeof(k)); 
    k.data = &key;
    k.size = sizeof(key);
    DBT d;
    memset(&d, 0, sizeof(d));
    d.data = &data;
    d.size = sizeof(data);
    int r = dbp->get(dbp, NULL, &k, &d, 0);
    return r;
  }

  int del(K key) {
    DBT k;
    memset(&k, 0, sizeof(k)); 
    k.data = &key;
    k.size = sizeof(key);
    return dbp->del(dbp, NULL, &k, 0);
  }

  int list_keys(list<K>& ls) {
    DBC *cursor = 0;
    int r = dbp->cursor(dbp, NULL, &cursor, 0);
    assert(r == 0);

    DBT k,d;
    memset(&k, 0, sizeof(k));
    memset(&d, 0, sizeof(d));

    while ((r = cursor->c_get(cursor, &k, &d, DB_NEXT)) == 0) {
      K key;
      assert(k.size == sizeof(key));
      memcpy(&key, k.data, k.size);
      ls.push_back(key);
    }
    if (r != DB_NOTFOUND) {
      dbp->err(dbp, r, "DBcursor->get");
      assert(r == DB_NOTFOUND);
    }

    cursor->c_close(cursor);
    return 0;
  }
  
};

#endif
