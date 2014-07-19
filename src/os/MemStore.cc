// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "MemStore.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "memstore(" << path << ") "

// for comparing collections for lock ordering
bool operator>(const MemStore::CollectionRef& l,
	       const MemStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}


int MemStore::peek_journal_fsid(uuid_d *fsid)
{
  *fsid = uuid_d();
  return 0;
}

int MemStore::mount()
{
  int r = _load();
  if (r < 0)
    return r;
  finisher.start();
  return 0;
}

int MemStore::umount()
{
  finisher.stop();
  return _save();
}

int MemStore::_save()
{
  dout(10) << __func__ << dendl;
  Mutex::Locker l(apply_lock); // block any writer
  dump_all();
  set<coll_t> collections;
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    bufferlist bl;
    assert(p->second);
    p->second->encode(bl);
    string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;
  }

  string fn = path + "/collections";
  bufferlist bl;
  ::encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  if (sharded) {
   string fn = path + "/sharded";
    bufferlist bl;
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;
  }

  return 0;
}

void MemStore::dump_all()
{
  Formatter *f = new_formatter("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void MemStore::dump(Formatter *f)
{
  f->open_array_section("collections");
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("xattrs");
    for (map<string,bufferptr>::iterator q = p->second->xattr.begin();
	 q != p->second->xattr.end();
	 ++q) {
      f->open_object_section("xattr");
      f->dump_string("name", q->first);
      f->dump_int("length", q->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("objects");
    for (map<ghobject_t,ObjectRef>::iterator q = p->second->object_map.begin();
	 q != p->second->object_map.end();
	 ++q) {
      f->open_object_section("object");
      f->dump_string("name", stringify(q->first));
      if (q->second)
	q->second->dump(f);
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int MemStore::_load()
{
  dout(10) << __func__ << dendl;
  bufferlist bl;
  string fn = path + "/collections";
  string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0)
    return r;

  set<coll_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);

  for (set<coll_t>::iterator q = collections.begin();
       q != collections.end();
       ++q) {
    string fn = path + "/" + stringify(*q);
    bufferlist cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0)
      return r;
    CollectionRef c(new Collection);
    bufferlist::iterator p = cbl.begin();
    c->decode(p);
    coll_map[*q] = c;
  }

  fn = path + "/sharded";
  struct stat st;
  if (::stat(fn.c_str(), &st) == 0)
    set_allow_sharded_objects();

  dump_all();

  return 0;  
}

void MemStore::set_fsid(uuid_d u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

uuid_d MemStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  assert(b);
  return uuid;
}

int MemStore::mkfs()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fs_fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else {
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  return 0;
}

int MemStore::statfs(struct statfs *st)
{
  dout(10) << __func__ << dendl;
  // make some shit up.  these are the only fields that matter.
  st->f_bsize = 1024;
  st->f_blocks = 1000000;
  st->f_bfree =  1000000;
  st->f_bavail = 1000000;
  return 0;
}

objectstore_perf_stat_t MemStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

MemStore::CollectionRef MemStore::get_collection(coll_t cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}


// ---------------
// read operations

bool MemStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return false;
  RWLock::RLocker l(c->lock);

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  return (bool)c->get_object(oid);
}

int MemStore::stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  st->st_size = o->data.length();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int MemStore::read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker lc(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (offset >= o->data.length())
    return 0;
  size_t l = len;
  if (l == 0)  // note: len == 0 means read the entire object
    l = o->data.length();
  else if (offset + l > o->data.length())
    l = o->data.length() - offset;
  bl.clear();
  bl.substr_of(o->data, offset, l);
  return bl.length();
}

int MemStore::fiemap(coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker lc(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (offset >= o->data.length())
    return 0;
  size_t l = len;
  if (offset + l > o->data.length())
    l = o->data.length() - offset;
  map<uint64_t, uint64_t> m;
  m[offset] = l;
  ::encode(m, bl);
  return 0;  
}

int MemStore::getattr(coll_t cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  string k(name);
  if (!o->xattr.count(k)) {
    return -ENODATA;
  }
  value = o->xattr[k];
  return 0;
}

int MemStore::getattrs(coll_t cid, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  aset = o->xattr;
  return 0;
}

int MemStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool MemStore::collection_exists(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);
  return coll_map.count(cid);
}

int MemStore::collection_getattr(coll_t cid, const char *name,
				 void *value, size_t size)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker lc(c->lock);

  if (!c->xattr.count(name))
    return -ENOENT;
  bufferlist bl;
  bl.append(c->xattr[name]);
  size_t l = MIN(size, bl.length());
  bl.copy(0, size, (char *)value);
  return l;
}

int MemStore::collection_getattr(coll_t cid, const char *name, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  if (!c->xattr.count(name))
    return -ENOENT;
  bl.clear();
  bl.append(c->xattr[name]);
  return bl.length();
}

int MemStore::collection_getattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  aset = c->xattr;
  return 0;
}

bool MemStore::collection_empty(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  return c->object_map.empty();
}

int MemStore::collection_list(coll_t cid, vector<ghobject_t>& o)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  for (map<ghobject_t,ObjectRef>::iterator p = c->object_map.begin();
       p != c->object_map.end();
       ++p)
    o.push_back(p->first);
  return 0;
}

int MemStore::collection_list_partial(coll_t cid, ghobject_t start,
				      int min, int max, snapid_t snap, 
				      vector<ghobject_t> *ls, ghobject_t *next)
{
  dout(10) << __func__ << " " << cid << " " << start << " " << min << "-"
	   << max << " " << snap << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  map<ghobject_t,ObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 ls->size() < (unsigned)max) {
    ls->push_back(p->first);
    ++p;
  }
  if (p == c->object_map.end())
    *next = ghobject_t::get_max();
  else
    *next = p->first;
  return 0;
}

int MemStore::collection_list_range(coll_t cid,
				    ghobject_t start, ghobject_t end,
				    snapid_t seq, vector<ghobject_t> *ls)
{
  dout(10) << __func__ << " " << cid << " " << start << " " << end
	   << " " << seq << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  map<ghobject_t,ObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 p->first < end) {
    ls->push_back(p->first);
    ++p;
  }
  return 0;
}

int MemStore::omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  *header = o->omap_header;
  *out = o->omap;
  return 0;
}

int MemStore::omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio ///< [in] don't assert on eio
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  *header = o->omap_header;
  return 0;
}

int MemStore::omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (map<string,bufferlist>::iterator p = o->omap.begin();
       p != o->omap.end();
       ++p)
    keys->insert(p->first);
  return 0;
}

int MemStore::omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*q);
  }
  return 0;
}

int MemStore::omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*p);
  }
  return 0;
}

ObjectMap::ObjectMapIterator MemStore::get_omap_iterator(coll_t cid,
							 const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return ObjectMap::ObjectMapIterator();
  RWLock::RLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return ObjectMap::ObjectMapIterator();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o));
}


// ---------------
// write operations

int MemStore::queue_transactions(Sequencer *osr,
				 list<Transaction*>& tls,
				 TrackedOpRef op,
				 ThreadPool::TPHandle *handle)
{
  // fixme: ignore the Sequencer and serialize everything.
  Mutex::Locker l(apply_lock);

  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle)
      handle->reset_tp_timeout();

    _do_transaction(**p);
  }

  Context *on_apply = NULL, *on_apply_sync = NULL, *on_commit = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit,
					     &on_apply_sync);
  if (on_apply_sync)
    on_apply_sync->complete(0);
  if (on_apply)
    finisher.queue(on_apply);
  if (on_commit)
    finisher.queue(on_commit);
  return 0;
}

void MemStore::_do_transaction(Transaction& t)
{
  Transaction::iterator i = t.begin();
  int pos = 0;

  while (i.have_op()) {
    int op = i.decode_op();
    int r = 0;

    switch (op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	r = _touch(cid, oid);
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	bool replica = i.get_replica();
	bufferlist bl;
	i.decode_bl(bl);
	r = _write(cid, oid, off, len, bl, replica);
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	r = _zero(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	i.decode_cid();
	i.decode_oid();
	i.decode_length();
	i.decode_length();
	// deprecated, no-op
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	r = _truncate(cid, oid, off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	r = _remove(cid, oid);
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	string name = i.decode_attrname();
	bufferlist bl;
	i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(cid, oid, to_set);
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	map<string, bufferptr> aset;
	i.decode_attrset(aset);
	r = _setattrs(cid, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	string name = i.decode_attrname();
	r = _rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	r = _rmattrs(cid, oid);
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	r = _clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	r = _clone_range(cid, oid, noid, off, len, off);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	uint64_t srcoff = i.decode_length();
	uint64_t len = i.decode_length();
	uint64_t dstoff = i.decode_length();
	r = _clone_range(cid, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.decode_cid();
	r = _create_collection(cid);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.decode_cid();
        uint32_t type = i.decode_u32();
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.decode_cid();
	r = _destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ncid = i.decode_cid();
	coll_t ocid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	r = _collection_add(ncid, ocid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	r = _remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	coll_t oldcid = i.decode_cid();
	ghobject_t oldoid = i.decode_oid();
	coll_t newcid = i.decode_cid();
	ghobject_t newoid = i.decode_oid();
	r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.decode_cid();
	string name = i.decode_attrname();
	bufferlist bl;
	i.decode_bl(bl);
	r = _collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.decode_cid();
	string name = i.decode_attrname();
	r = _collection_rmattr(cid, name.c_str());
      }
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.decode_cid());
	coll_t ncid(i.decode_cid());
	r = -EOPNOTSUPP;
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	r = _omap_clear(cid, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	map<string, bufferlist> aset;
	i.decode_attrset(aset);
	r = _omap_setkeys(cid, oid, aset);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	set<string> keys;
	i.decode_keyset(keys);
	r = _omap_rmkeys(cid, oid, keys);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	string first, last;
	first = i.decode_key();
	last = i.decode_key();
	r = _omap_rmkeyrange(cid, oid, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	bufferlist bl;
	i.decode_bl(bl);
	r = _omap_setheader(cid, oid, bl);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
	coll_t cid(i.decode_cid());
	uint32_t bits(i.decode_u32());
	uint32_t rem(i.decode_u32());
	coll_t dest(i.decode_cid());
	r = _split_collection(cid, bits, rem, dest);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        coll_t cid(i.decode_cid());
        ghobject_t oid = i.decode_oid();
        i.decode_length(); // uint64_t expected_object_size
        i.decode_length(); // uint64_t expected_write_size
      }
      break;

    default:
      derr << "bad op " << op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op == Transaction::OP_CLONERANGE ||
			    op == Transaction::OP_CLONE ||
			    op == Transaction::OP_CLONERANGE2 ||
			    op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op == Transaction::OP_CLONERANGE ||
			     op == Transaction::OP_CLONE ||
			     op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	  dump_all();
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }
}

int MemStore::_touch(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o) {
    o.reset(new Object);
    c->object_map[oid] = o;
    c->object_hash[oid] = o;
  }
  return 0;
}

int MemStore::_write(coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     bool replica)
{
  dout(10) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  assert(len == bl.length());

  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o) {
    // write implicitly creates a missing object
    o.reset(new Object);
    c->object_map[oid] = o;
    c->object_hash[oid] = o;
  }

  _write_into_bl(bl, offset, &o->data);
  return 0;
}

void MemStore::_write_into_bl(const bufferlist& src, unsigned offset,
			      bufferlist *dst)
{
  unsigned len = src.length();

  // before
  bufferlist newdata;
  if (dst->length() >= offset) {
    newdata.substr_of(*dst, 0, offset);
  } else {
    newdata.substr_of(*dst, 0, dst->length());
    bufferptr bp(offset - dst->length());
    bp.zero();
    newdata.append(bp);
  }

  newdata.append(src);

  // after
  if (dst->length() > offset + len) {
    bufferlist tail;
    tail.substr_of(*dst, offset + len, dst->length() - (offset + len));
    newdata.append(tail);
  }

  dst->claim(newdata);
}

int MemStore::_zero(coll_t cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  bufferptr bp(len);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  return _write(cid, oid, offset, len, bl);
}

int MemStore::_truncate(coll_t cid, const ghobject_t& oid, uint64_t size)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << size << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (o->data.length() > size) {
    bufferlist bl;
    bl.substr_of(o->data, 0, size);
    o->data.claim(bl);
  } else if (o->data.length() == size) {
    // do nothing
  } else {
    bufferptr bp(size - o->data.length());
    bp.zero();
    o->data.append(bp);
  }
  return 0;
}

int MemStore::_remove(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  c->object_map.erase(oid);
  c->object_hash.erase(oid);
  return 0;
}

int MemStore::_setattrs(coll_t cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (map<string,bufferptr>::const_iterator p = aset.begin(); p != aset.end(); ++p)
    o->xattr[p->first] = p->second;
  return 0;
}

int MemStore::_rmattr(coll_t cid, const ghobject_t& oid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (!o->xattr.count(name))
    return -ENODATA;
  o->xattr.erase(name);
  return 0;
}

int MemStore::_rmattrs(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  o->xattr.clear();
  return 0;
}

int MemStore::_clone(coll_t cid, const ghobject_t& oldoid,
		     const ghobject_t& newoid)
{
  dout(10) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_object(newoid);
  if (!no) {
    no.reset(new Object);
    c->object_map[newoid] = no;
    c->object_hash[newoid] = no;
  }
  no->data = oo->data;
  no->omap_header = oo->omap_header;
  no->omap = oo->omap;
  no->xattr = oo->xattr;
  return 0;
}

int MemStore::_clone_range(coll_t cid, const ghobject_t& oldoid,
			   const ghobject_t& newoid,
			   uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(10) << __func__ << " " << cid << " "
	   << oldoid << " " << srcoff << "~" << len << " -> "
	   << newoid << " " << dstoff << "~" << len
	   << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_object(newoid);
  if (!no) {
    no.reset(new Object);
    c->object_map[newoid] = no;
    c->object_hash[newoid] = no;
  }
  if (srcoff >= oo->data.length())
    return 0;
  if (srcoff + len >= oo->data.length())
    len = oo->data.length() - srcoff;
  bufferlist bl;
  bl.substr_of(oo->data, srcoff, len);
  _write_into_bl(bl, dstoff, &no->data);
  return len;
}

int MemStore::_omap_clear(coll_t cid, const ghobject_t &oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  o->omap.clear();
  return 0;
}

int MemStore::_omap_setkeys(coll_t cid, const ghobject_t &oid,
			    const map<string, bufferlist> &aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (map<string,bufferlist>::const_iterator p = aset.begin(); p != aset.end(); ++p)
    o->omap[p->first] = p->second;
  return 0;
}

int MemStore::_omap_rmkeys(coll_t cid, const ghobject_t &oid,
			   const set<string> &keys)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p)
    o->omap.erase(*p);
  return 0;
}

int MemStore::_omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
			       const string& first, const string& last)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  map<string,bufferlist>::iterator p = o->omap.upper_bound(first);
  map<string,bufferlist>::iterator e = o->omap.lower_bound(last);
  while (p != e)
    o->omap.erase(p++);
  return 0;
}

int MemStore::_omap_setheader(coll_t cid, const ghobject_t &oid,
			      const bufferlist &bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::WLocker l(c->lock);

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  o->omap_header = bl;
  return 0;
}

int MemStore::_create_collection(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp != coll_map.end())
    return -EEXIST;
  coll_map[cid].reset(new Collection);
  return 0;
}

int MemStore::_destroy_collection(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  {
    RWLock::RLocker l2(cp->second->lock);
    if (!cp->second->object_map.empty())
      return -ENOTEMPTY;
  }
  coll_map.erase(cp);
  return 0;
}

int MemStore::_collection_add(coll_t cid, coll_t ocid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << ocid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  CollectionRef oc = get_collection(ocid);
  if (!oc)
    return -ENOENT;
  RWLock::WLocker l1(MIN(&(*c), &(*oc))->lock);
  RWLock::WLocker l2(MAX(&(*c), &(*oc))->lock);

  if (c->object_hash.count(oid))
    return -EEXIST;
  if (oc->object_hash.count(oid) == 0)
    return -ENOENT;
  ObjectRef o = oc->object_hash[oid];
  c->object_map[oid] = o;
  c->object_hash[oid] = o;
  return 0;
}

int MemStore::_collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
				      coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << oldcid << " " << oldoid << " -> "
	   << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  CollectionRef oc = get_collection(oldcid);
  if (!oc)
    return -ENOENT;

  // note: c and oc may be the same
  if (&(*c) == &(*oc)) {
    c->lock.get_write();
  } else if (&(*c) < &(*oc)) {
    c->lock.get_write();
    oc->lock.get_write();
  } else if (&(*c) > &(*oc)) {
    oc->lock.get_write();
    c->lock.get_write();
  }

  int r = -EEXIST;
  if (c->object_hash.count(oid))
    goto out;
  r = -ENOENT;
  if (oc->object_hash.count(oldoid) == 0)
    goto out;
  {
    ObjectRef o = oc->object_hash[oldoid];
    c->object_map[oid] = o;
    c->object_hash[oid] = o;
    oc->object_map.erase(oldoid);
    oc->object_hash.erase(oldoid);
  }
  r = 0;
 out:
  c->lock.put_write();
  if (&(*c) != &(*oc))
    oc->lock.put_write();
  return r;
}

int MemStore::_collection_setattr(coll_t cid, const char *name,
				  const void *value, size_t size)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  RWLock::WLocker l(cp->second->lock);

  cp->second->xattr[name] = bufferptr((const char *)value, size);
  return 0;
}

int MemStore::_collection_setattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << cid << dendl;
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  RWLock::WLocker l(cp->second->lock);

  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end();
       ++p) {
    cp->second->xattr[p->first] = p->second;
  }
  return 0;
}

int MemStore::_collection_rmattr(coll_t cid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  RWLock::WLocker l(cp->second->lock);

  if (cp->second->xattr.count(name) == 0)
    return -ENODATA;
  cp->second->xattr.erase(name);
  return 0;
}

int MemStore::_split_collection(coll_t cid, uint32_t bits, uint32_t match,
				coll_t dest)
{
  dout(10) << __func__ << " " << cid << " " << bits << " " << match << " "
	   << dest << dendl;
  CollectionRef sc = get_collection(cid);
  if (!sc)
    return -ENOENT;
  CollectionRef dc = get_collection(dest);
  if (!dc)
    return -ENOENT;
  RWLock::WLocker l1(MIN(&(*sc), &(*dc))->lock);
  RWLock::WLocker l2(MAX(&(*sc), &(*dc))->lock);

  map<ghobject_t,ObjectRef>::iterator p = sc->object_map.begin();
  while (p != sc->object_map.end()) {
    if (p->first.match(bits, match)) {
      dout(20) << " moving " << p->first << dendl;
      dc->object_map.insert(make_pair(p->first, p->second));
      dc->object_hash.insert(make_pair(p->first, p->second));
      sc->object_hash.erase(p->first);
      sc->object_map.erase(p++);
    } else {
      ++p;
    }
  }

  return 0;
}
