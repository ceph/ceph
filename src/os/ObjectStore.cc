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
#if defined(WITH_BLUESTORE)
#include "bluestore/BlueStore.h"
#endif
#include "kstore/KStore.h"

void decode_str_str_map_to_bl(bufferlist::iterator& p,
			      bufferlist *out)
{
  bufferlist::iterator start = p;
  __u32 n;
  decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    decode(l, p);
    p.advance(l);
    len += 4 + l;
    decode(l, p);
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
  decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    decode(l, p);
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
    return new FileStore(cct, data, journal, flags);
  }
  if (type == "memstore") {
    return new MemStore(cct, data);
  }
#if defined(WITH_BLUESTORE)
  if (type == "bluestore") {
    return new BlueStore(cct, data);
  }
  if (type == "random") {
    if (rand() % 2) {
      return new FileStore(cct, data, journal, flags);
    } else {
      return new BlueStore(cct, data);
    }
  }
#else
  if (type == "random") {
    return new FileStore(cct, data, journal, flags);
  }
#endif
  if (type == "kstore" &&
      cct->check_experimental_feature_enabled("kstore")) {
    return new KStore(cct, data);
  }
  return NULL;
}

int ObjectStore::probe_block_device_fsid(
  CephContext *cct,
  const string& path,
  uuid_d *fsid)
{
  int r;

#if defined(WITH_BLUESTORE)
  // first try bluestore -- it has a crc on its header and will fail
  // reliably.
  r = BlueStore::get_block_device_fsid(cct, path, fsid);
  if (r == 0) {
    lgeneric_dout(cct, 0) << __func__ << " " << path << " is bluestore, "
			  << *fsid << dendl;
    return r;
  }
#endif

  // okay, try FileStore (journal).
  r = FileStore::get_block_device_fsid(cct, path, fsid);
  if (r == 0) {
    lgeneric_dout(cct, 0) << __func__ << " " << path << " is filestore, "
			  << *fsid << dendl;
    return r;
  }

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




ostream& operator<<(ostream& out, const ObjectStore::Transaction& tx) {

  return out << "Transaction(" << &tx << ")"; 
}
