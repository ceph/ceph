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
#include "common/safe_io.h"

#include "filestore/FileStore.h"
#include "memstore/MemStore.h"
#if defined(HAVE_LIBAIO)
#include "bluestore/BlueStore.h"
#endif
#include "kstore/KStore.h"

void decode_str_str_map_to_bl(bufferlist::iterator& p,
			      bufferlist *out)
{
  bufferlist::iterator start = p;
  __u32 n;
  ::decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    ::decode(l, p);
    p.advance(l);
    len += 4 + l;
    ::decode(l, p);
    p.advance(l);
    len += 4 + l;
  }
  start.copy(len, *out);
}

void decode_str_set_to_bl(bufferlist::iterator& p,
			  bufferlist *out)
{
  bufferlist::iterator start = p;
  __u32 n;
  ::decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    ::decode(l, p);
    p.advance(l);
    len += 4 + l;
  }
  start.copy(len, *out);
}

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
#if defined(HAVE_LIBAIO)
  if (type == "bluestore" &&
      cct->check_experimental_feature_enabled("bluestore")) {
    return new BlueStore(cct, data);
  }
#endif
  if (type == "kstore" &&
      cct->check_experimental_feature_enabled("kstore")) {
    return new KStore(cct, data);
  }
  return NULL;
}

int ObjectStore::probe_block_device_fsid(
  const string& path,
  uuid_d *fsid)
{
  int r;

#if defined(HAVE_LIBAIO)
  // first try bluestore -- it has a crc on its header and will fail
  // reliably.
  r = BlueStore::get_block_device_fsid(path, fsid);
  if (r == 0)
    return r;
#endif

  // okay, try FileStore (journal).
  r = FileStore::get_block_device_fsid(path, fsid);
  if (r == 0)
    return r;

  return -EINVAL;
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

ostream& operator<<(ostream& out, const ObjectStore::Transaction& tx) {

  return out << "Transaction(" << &tx << ")"; 
}

unsigned ObjectStore::apply_transactions(Sequencer *osr,
					 vector<Transaction>& tls,
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
  vector<Transaction>& tls,
  Context *onreadable,
  Context *oncommit,
  Context *onreadable_sync,
  Context *oncomplete,
  TrackedOpRef op = TrackedOpRef())
{
  RunOnDeleteRef _complete (std::make_shared<RunOnDelete>(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(
    onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(
    oncommit, _complete);
  return queue_transactions(osr, tls, _onreadable, _oncommit,
			    onreadable_sync, op);
}
