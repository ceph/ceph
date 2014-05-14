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
#include <ctype.h>
#include <sstream>
#include "include/memory.h"
#include "ObjectStore.h"
#include "common/Formatter.h"
#include "FileStore.h"
#include "MemStore.h"
#include "KeyValueStore.h"
#include "common/safe_io.h"

ObjectStore *ObjectStore::create(CephContext *cct,
				 const string& type,
				 const string& data,
				 const string& journal,
			         osflagbits_t flags)
{
  if (type == "filestore") {
    return new FileStore(data, journal, flags);
  }
  if (type == "memstore") {
    return new MemStore(cct, data);
  }
  if (type == "keyvaluestore-dev") {
    return new KeyValueStore(data);
  }
  return NULL;
}

int ObjectStore::write_meta(const std::string& key,
			    const std::string& value)
{
  string v = value;
  v += "\n";
  int r = safe_write_file(path.c_str(), key.c_str(),
			  v.c_str(), v.length());
  if (r < 0)
    return r;
  return 0;
}

int ObjectStore::read_meta(const std::string& key,
			   std::string *value)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
			 buf, sizeof(buf));
  if (r <= 0)
    return r;
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = string(buf, r);
  return 0;
}




ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s)
{
  return out << "osr(" << s.get_name() << " " << &s << ")";
}

unsigned ObjectStore::apply_transactions(Sequencer *osr,
					 list<Transaction*> &tls,
					 Context *ondisk)
{
  // use op pool
  Cond my_cond;
  Mutex my_lock("ObjectStore::apply_transaction::my_lock");
  int r = 0;
  bool done;
  C_SafeCond *onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

  queue_transactions(osr, tls, onreadable, ondisk);

  my_lock.Lock();
  while (!done)
    my_cond.Wait(my_lock);
  my_lock.Unlock();
  return r;
}

int ObjectStore::queue_transactions(
  Sequencer *osr,
  list<Transaction*>& tls,
  Context *onreadable,
  Context *oncommit,
  Context *onreadable_sync,
  Context *oncomplete,
  TrackedOpRef op = TrackedOpRef())
{
  RunOnDeleteRef _complete(new RunOnDelete(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(
    onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(
    oncommit, _complete);
  return queue_transactions(osr, tls, _onreadable, _oncommit,
			    onreadable_sync, op);
}

int ObjectStore::collection_list(coll_t c, vector<hobject_t>& o)
{
  vector<ghobject_t> go;
  int ret = collection_list(c, go);
  if (ret == 0) {
    o.reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      o.push_back(i->hobj);
  }
  return ret;
}

int ObjectStore::collection_list_partial(coll_t c, hobject_t start,
			      int min, int max, snapid_t snap,
				      vector<hobject_t> *ls, hobject_t *next)
{
  vector<ghobject_t> go;
  ghobject_t gnext, gstart(start);
  int ret = collection_list_partial(c, gstart, min, max, snap, &go, &gnext);
  if (ret == 0) {
    *next = gnext.hobj;
    ls->reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(i->hobj);
  }
  return ret;
}

int ObjectStore::collection_list_range(coll_t c, hobject_t start, hobject_t end,
			    snapid_t seq, vector<hobject_t> *ls)
{
  vector<ghobject_t> go;
  // Starts with the smallest shard id and generation to
  // make sure the result list has the marker object
  ghobject_t gstart(start, 0, shard_id_t(0));
  // Exclusive end, choose the smallest end ghobject
  ghobject_t gend(end, 0, shard_id_t(0));
  int ret = collection_list_range(c, gstart, gend, seq, &go);
  if (ret == 0) {
    ls->reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(i->hobj);
  }
  return ret;
}
