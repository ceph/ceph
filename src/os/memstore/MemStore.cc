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
#include "common/errno.h"
#include "MemStore.h"
#include "include/compat.h"

#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "memstore(" << path << ") "

using ceph::decode;
using ceph::encode;

// for comparing collections for lock ordering
bool operator>(const MemStore::CollectionRef& l,
	       const MemStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
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
  finisher.wait_for_empty();
  finisher.stop();
  return _save();
}

int MemStore::_save()
{
  dout(10) << __func__ << dendl;
  dump_all();
  std::set<coll_t> collections;
  for (auto p = coll_map.begin(); p != coll_map.end(); ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    ceph::buffer::list bl;
    ceph_assert(p->second);
    p->second->encode(bl);
    std::string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;
  }

  std::string fn = path + "/collections";
  ceph::buffer::list bl;
  encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  return 0;
}

void MemStore::dump_all()
{
  auto f = ceph::Formatter::create("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void MemStore::dump(ceph::Formatter *f)
{
  f->open_array_section("collections");
  for (auto p = coll_map.begin(); p != coll_map.end(); ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("xattrs");
    for (auto q = p->second->xattr.begin();
	 q != p->second->xattr.end();
	 ++q) {
      f->open_object_section("xattr");
      f->dump_string("name", q->first);
      f->dump_int("length", q->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("objects");
    for (auto q = p->second->object_map.begin();
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
  ceph::buffer::list bl;
  std::string fn = path + "/collections";
  std::string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0)
    return r;

  std::set<coll_t> collections;
  auto p = bl.cbegin();
  decode(collections, p);

  for (auto q = collections.begin();
       q != collections.end();
       ++q) {
    std::string fn = path + "/" + stringify(*q);
    ceph::buffer::list cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0)
      return r;
    auto c = ceph::make_ref<Collection>(cct, *q);
    auto p = cbl.cbegin();
    c->decode(p);
    coll_map[*q] = c;
    used_bytes += c->used_bytes();
  }

  dump_all();

  return 0;
}

void MemStore::set_fsid(uuid_d u)
{
  int r = write_meta("fsid", stringify(u));
  ceph_assert(r >= 0);
}

uuid_d MemStore::get_fsid()
{
  std::string fsid_str;
  int r = read_meta("fsid", &fsid_str);
  ceph_assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  ceph_assert(b);
  return uuid;
}

int MemStore::mkfs()
{
  std::string fsid_str;
  int r = read_meta("fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else if (r < 0) {
    return r;
  } else {  
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  std::string fn = path + "/collections";
  derr << path << dendl;
  ceph::buffer::list bl;
  std::set<coll_t> collections;
  encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  r = write_meta("type", "memstore");
  if (r < 0)
    return r;

  return 0;
}

int MemStore::statfs(struct store_statfs_t *st, osd_alert_list_t* alerts)
{
  dout(10) << __func__ << dendl;
  if (alerts) {
    alerts->clear(); // returns nothing for now
  }
  st->reset();
  st->total = cct->_conf->memstore_device_bytes;
  st->available = std::max<int64_t>(st->total - used_bytes, 0);
  dout(10) << __func__ << ": used_bytes: " << used_bytes
	   << "/" << cct->_conf->memstore_device_bytes << dendl;
  return 0;
}

int MemStore::pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
			  bool *per_pool_omap)
{
  return -ENOTSUP;
}

objectstore_perf_stat_t MemStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

MemStore::CollectionRef MemStore::get_collection(const coll_t& cid)
{
  std::shared_lock l{coll_lock};
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

ObjectStore::CollectionHandle MemStore::create_new_collection(const coll_t& cid)
{
  std::lock_guard l{coll_lock};
  auto c = ceph::make_ref<Collection>(cct, cid);
  new_coll_map[cid] = c;
  return c;
}


// ---------------
// read operations

bool MemStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists)
    return false;

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  return (bool)c->get_object(oid);
}

int MemStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  st->st_size = o->get_size();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int MemStore::set_collection_opts(
  CollectionHandle& ch,
  const pool_opts_t& opts)
{
  return -EOPNOTSUPP;
}

int MemStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  ceph::buffer::list& bl,
  uint32_t op_flags)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (offset >= o->get_size())
    return 0;
  size_t l = len;
  if (l == 0 && offset == 0)  // note: len == 0 means read the entire object
    l = o->get_size();
  else if (offset + l > o->get_size())
    l = o->get_size() - offset;
  bl.clear();
  return o->read(offset, l, bl);
}

int MemStore::fiemap(CollectionHandle& ch, const ghobject_t& oid,
		     uint64_t offset, size_t len, ceph::buffer::list& bl)
{
  std::map<uint64_t, uint64_t> destmap;
  int r = fiemap(ch, oid, offset, len, destmap);
  if (r >= 0)
    encode(destmap, bl);
  return r;
}

int MemStore::fiemap(CollectionHandle& ch, const ghobject_t& oid,
		     uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap)
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  size_t l = len;
  if (offset + l > o->get_size())
    l = o->get_size() - offset;
  if (offset >= o->get_size())
    goto out;
  destmap[offset] = l;
 out:
  return 0;
}

int MemStore::getattr(CollectionHandle &c_, const ghobject_t& oid,
		      const char *name, ceph::buffer::ptr& value)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::string k(name);
  std::lock_guard lock{o->xattr_mutex};
  if (!o->xattr.count(k)) {
    return -ENODATA;
  }
  value = o->xattr[k];
  return 0;
}

int MemStore::getattrs(CollectionHandle &c_, const ghobject_t& oid,
		       std::map<std::string,ceph::buffer::ptr,std::less<>>& aset)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->xattr_mutex};
  aset = o->xattr;
  return 0;
}

int MemStore::list_collections(std::vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  std::shared_lock l{coll_lock};
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool MemStore::collection_exists(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  std::shared_lock l{coll_lock};
  return coll_map.count(cid);
}

int MemStore::collection_empty(CollectionHandle& ch, bool *empty)
{
  dout(10) << __func__ << " " << ch->cid << dendl;
  CollectionRef c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  *empty = c->object_map.empty();
  return 0;
}

int MemStore::collection_bits(CollectionHandle& ch)
{
  dout(10) << __func__ << " " << ch->cid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  return c->bits;
}

int MemStore::collection_list(CollectionHandle& ch,
			      const ghobject_t& start,
			      const ghobject_t& end,
			      int max,
			      std::vector<ghobject_t> *ls, ghobject_t *next)
{
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};

  dout(10) << __func__ << " cid " << ch->cid << " start " << start
	   << " end " << end << dendl;
  auto p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 ls->size() < (unsigned)max &&
	 p->first < end) {
    ls->push_back(p->first);
    ++p;
  }
  if (next != NULL) {
    if (p == c->object_map.end())
      *next = ghobject_t::get_max();
    else
      *next = p->first;
  }
  dout(10) << __func__ << " cid " << ch->cid << " got " << ls->size() << dendl;
  return 0;
}

int MemStore::omap_get(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  ceph::buffer::list *header,      ///< [out] omap header
  std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  *header = o->omap_header;
  *out = o->omap;
  return 0;
}

int MemStore::omap_get_header(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  ceph::buffer::list *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  *header = o->omap_header;
  return 0;
}

int MemStore::omap_get_keys(
  CollectionHandle& ch,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  std::set<std::string> *keys      ///< [out] Keys defined on oid
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  for (auto p = o->omap.begin(); p != o->omap.end(); ++p)
    keys->insert(p->first);
  return 0;
}

int MemStore::omap_get_values(
  CollectionHandle& ch,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const std::set<std::string> &keys,     ///< [in] Keys to get
  std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  for (auto p = keys.begin(); p != keys.end(); ++p) {
    auto q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*q);
  }
  return 0;
}

#ifdef WITH_SEASTAR
int MemStore::omap_get_values(
  CollectionHandle& ch,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const std::optional<std::string> &start_after,     ///< [in] Keys to get
  std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  assert(start_after);
  std::lock_guard lock{o->omap_mutex};
  for (auto it = o->omap.upper_bound(*start_after);
       it != std::end(o->omap);
       ++it) {
    out->insert(*it);
  }
  return 0;
}
#endif

int MemStore::omap_check_keys(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const std::set<std::string> &keys, ///< [in] Keys to check
  std::set<std::string> *out         ///< [out] Subset of keys defined on oid
  )
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  for (auto p = keys.begin(); p != keys.end(); ++p) {
    auto q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*p);
  }
  return 0;
}

class MemStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  CollectionRef c;
  ObjectRef o;
  std::map<std::string,ceph::buffer::list>::iterator it;
public:
  OmapIteratorImpl(CollectionRef c, ObjectRef o)
    : c(c), o(o), it(o->omap.begin()) {}

  int seek_to_first() override {
    std::lock_guard lock{o->omap_mutex};
    it = o->omap.begin();
    return 0;
  }
  int upper_bound(const std::string &after) override {
    std::lock_guard lock{o->omap_mutex};
    it = o->omap.upper_bound(after);
    return 0;
  }
  int lower_bound(const std::string &to) override {
    std::lock_guard lock{o->omap_mutex};
    it = o->omap.lower_bound(to);
    return 0;
  }
  bool valid() override {
    std::lock_guard lock{o->omap_mutex};
    return it != o->omap.end();
  }
  int next() override {
    std::lock_guard lock{o->omap_mutex};
    ++it;
    return 0;
  }
  std::string key() override {
    std::lock_guard lock{o->omap_mutex};
    return it->first;
  }
  ceph::buffer::list value() override {
    std::lock_guard lock{o->omap_mutex};
    return it->second;
  }
  int status() override {
    return 0;
  }
};

ObjectMap::ObjectMapIterator MemStore::get_omap_iterator(
  CollectionHandle& ch,
  const ghobject_t& oid)
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  ObjectRef o = c->get_object(oid);
  if (!o)
    return ObjectMap::ObjectMapIterator();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o));
}


// ---------------
// write operations

int MemStore::queue_transactions(
  CollectionHandle& ch,
  std::vector<Transaction>& tls,
  TrackedOpRef op,
  ThreadPool::TPHandle *handle)
{
  // because memstore operations are synchronous, we can implement the
  // Sequencer with a mutex. this guarantees ordering on a given sequencer,
  // while allowing operations on different sequencers to happen in parallel
  Collection *c = static_cast<Collection*>(ch.get());
  std::unique_lock lock{c->sequencer_mutex};

  for (auto p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle)
      handle->reset_tp_timeout();

    _do_transaction(*p);
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
    Transaction::Op *op = i.decode_op();
    int r = 0;

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
    case Transaction::OP_CREATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _touch(cid, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        ceph::buffer::list bl;
        i.decode_bl(bl);
	r = _write(cid, oid, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(cid, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
	r = _truncate(cid, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        std::string name = i.decode_string();
        ceph::buffer::list bl;
        i.decode_bl(bl);
	std::map<std::string, ceph::buffer::ptr> to_set;
	to_set[name] = ceph::buffer::ptr(bl.c_str(), bl.length());
	r = _setattrs(cid, oid, to_set);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	std::map<std::string, ceph::buffer::ptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(cid, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        std::string name = i.decode_string();
	r = _rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _rmattrs(cid, oid);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
	r = _clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _clone_range(cid, oid, noid, off, len, off);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(cid, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _create_collection(cid, op->split_bits);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint;
        ceph::buffer::list hint;
        i.decode_bl(hint);
        auto hiter = hint.cbegin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          decode(pg_num, hiter);
          decode(num_objs, hiter);
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _collection_add(ncid, ocid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
	if (r == -ENOENT)
	  r = 0;
      }
      break;

    case Transaction::OP_TRY_RENAME:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _collection_move_rename(cid, oldoid, cid, newoid);
	if (r == -ENOENT)
	  r = 0;
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	ceph_abort_msg("not implemented");
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	ceph_abort_msg("not implemented");
      }
      break;

    case Transaction::OP_COLL_RENAME:
      {
	ceph_abort_msg("not implemented");
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _omap_clear(cid, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ceph::buffer::list aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(cid, oid, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ceph::buffer::list keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(cid, oid, keys_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        std::string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkeyrange(cid, oid, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ceph::buffer::list bl;
        i.decode_bl(bl);
	r = _omap_setheader(cid, oid, bl);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
	r = _split_collection(cid, bits, rem, dest);
      }
      break;
    case Transaction::OP_MERGE_COLLECTION:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        coll_t dest = i.get_cid(op->dest_cid);
	r = _merge_collection(cid, bits, dest);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        r = 0;
      }
      break;

    case Transaction::OP_COLL_SET_BITS:
      {
        r = 0;
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from MemStore, misconfigured cluster or insufficient memory";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	  dump_all();
	}

	derr    << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	ceph::JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
        if (!g_conf().get_val<bool>("objectstore_debug_throw_on_failed_txc")) {
	  ceph_abort_msg("unexpected error");
        } else {
	  throw r;
        }
      }
    }

    ++pos;
  }
}

int MemStore::_touch(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  c->get_or_create_object(oid);
  return 0;
}

int MemStore::_write(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, const ceph::buffer::list& bl,
		     uint32_t fadvise_flags)
{
  dout(10) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  ceph_assert(len == bl.length());

  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_or_create_object(oid);
  if (len > 0 && !cct->_conf->memstore_debug_omit_block_device_write) {
    const ssize_t old_size = o->get_size();
    o->write(offset, bl);
    used_bytes += (o->get_size() - old_size);
  }

  return 0;
}

int MemStore::_zero(const coll_t& cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  ceph::buffer::list bl;
  bl.append_zero(len);
  return _write(cid, oid, offset, len, bl);
}

int MemStore::_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << size << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (cct->_conf->memstore_debug_omit_block_device_write)
    return 0;
  const ssize_t old_size = o->get_size();
  int r = o->truncate(size);
  used_bytes += (o->get_size() - old_size);
  return r;
}

int MemStore::_remove(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  std::lock_guard l{c->lock};

  auto i = c->object_hash.find(oid);
  if (i == c->object_hash.end())
    return -ENOENT;
  used_bytes -= i->second->get_size();
  c->object_hash.erase(i);
  c->object_map.erase(oid);

  return 0;
}

int MemStore::_setattrs(const coll_t& cid, const ghobject_t& oid,
			std::map<std::string,ceph::buffer::ptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->xattr_mutex};
  for (auto p = aset.begin(); p != aset.end(); ++p)
    o->xattr[p->first] = p->second;
  return 0;
}

int MemStore::_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->xattr_mutex};
  auto i = o->xattr.find(name);
  if (i == o->xattr.end())
    return -ENODATA;
  o->xattr.erase(i);
  return 0;
}

int MemStore::_rmattrs(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->xattr_mutex};
  o->xattr.clear();
  return 0;
}

int MemStore::_clone(const coll_t& cid, const ghobject_t& oldoid,
		     const ghobject_t& newoid)
{
  dout(10) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_or_create_object(newoid);
  used_bytes += oo->get_size() - no->get_size();
  no->clone(oo.get(), 0, oo->get_size(), 0);

  // take xattr and omap locks with std::lock()
  std::scoped_lock l{oo->xattr_mutex,
		     no->xattr_mutex,
		     oo->omap_mutex,
		     no->omap_mutex};

  no->omap_header = oo->omap_header;
  no->omap = oo->omap;
  no->xattr = oo->xattr;
  return 0;
}

int MemStore::_clone_range(const coll_t& cid, const ghobject_t& oldoid,
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

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_or_create_object(newoid);
  if (srcoff >= oo->get_size())
    return 0;
  if (srcoff + len >= oo->get_size())
    len = oo->get_size() - srcoff;

  const ssize_t old_size = no->get_size();
  no->clone(oo.get(), srcoff, len, dstoff);
  used_bytes += (no->get_size() - old_size);

  return len;
}

int MemStore::_omap_clear(const coll_t& cid, const ghobject_t &oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  o->omap.clear();
  o->omap_header.clear();
  return 0;
}

int MemStore::_omap_setkeys(const coll_t& cid, const ghobject_t &oid,
			    ceph::buffer::list& aset_bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  auto p = aset_bl.cbegin();
  __u32 num;
  decode(num, p);
  while (num--) {
    std::string key;
    decode(key, p);
    decode(o->omap[key], p);
  }
  return 0;
}

int MemStore::_omap_rmkeys(const coll_t& cid, const ghobject_t &oid,
			   ceph::buffer::list& keys_bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  auto p = keys_bl.cbegin();
  __u32 num;
  decode(num, p);
  while (num--) {
    std::string key;
    decode(key, p);
    o->omap.erase(key);
  }
  return 0;
}

int MemStore::_omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
			       const std::string& first, const std::string& last)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  auto p = o->omap.lower_bound(first);
  auto e = o->omap.lower_bound(last);
  o->omap.erase(p, e);
  return 0;
}

int MemStore::_omap_setheader(const coll_t& cid, const ghobject_t &oid,
			      const ceph::buffer::list &bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard lock{o->omap_mutex};
  o->omap_header = bl;
  return 0;
}

int MemStore::_create_collection(const coll_t& cid, int bits)
{
  dout(10) << __func__ << " " << cid << dendl;
  std::lock_guard l{coll_lock};
  auto result = coll_map.insert(std::make_pair(cid, CollectionRef()));
  if (!result.second)
    return -EEXIST;
  auto p = new_coll_map.find(cid);
  ceph_assert(p != new_coll_map.end());
  result.first->second = p->second;
  result.first->second->bits = bits;
  new_coll_map.erase(p);
  return 0;
}

int MemStore::_destroy_collection(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  std::lock_guard l{coll_lock};
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  {
    std::shared_lock l2{cp->second->lock};
    if (!cp->second->object_map.empty())
      return -ENOTEMPTY;
    cp->second->exists = false;
  }
  used_bytes -= cp->second->used_bytes();
  coll_map.erase(cp);
  return 0;
}

int MemStore::_collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << ocid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  CollectionRef oc = get_collection(ocid);
  if (!oc)
    return -ENOENT;

  std::scoped_lock l{std::min(&(*c), &(*oc))->lock,
		     std::max(&(*c), &(*oc))->lock};

  if (c->object_hash.count(oid))
    return -EEXIST;
  if (oc->object_hash.count(oid) == 0)
    return -ENOENT;
  ObjectRef o = oc->object_hash[oid];
  c->object_map[oid] = o;
  c->object_hash[oid] = o;
  return 0;
}

int MemStore::_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
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
  ceph_assert(&(*c) == &(*oc));

  std::lock_guard l{c->lock};
  if (c->object_hash.count(oid))
    return -EEXIST;
  if (oc->object_hash.count(oldoid) == 0)
    return -ENOENT;
  {
    ObjectRef o = oc->object_hash[oldoid];
    c->object_map[oid] = o;
    c->object_hash[oid] = o;
    oc->object_map.erase(oldoid);
    oc->object_hash.erase(oldoid);
  }
  return 0;
}

int MemStore::_split_collection(const coll_t& cid, uint32_t bits, uint32_t match,
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

  std::scoped_lock l{std::min(&(*sc), &(*dc))->lock,
                     std::max(&(*sc), &(*dc))->lock};

  auto p = sc->object_map.begin();
  while (p != sc->object_map.end()) {
    if (p->first.match(bits, match)) {
      dout(20) << " moving " << p->first << dendl;
      dc->object_map.insert(std::make_pair(p->first, p->second));
      dc->object_hash.insert(std::make_pair(p->first, p->second));
      sc->object_hash.erase(p->first);
      sc->object_map.erase(p++);
    } else {
      ++p;
    }
  }

  sc->bits = bits;
  ceph_assert(dc->bits == (int)bits);

  return 0;
}

int MemStore::_merge_collection(const coll_t& cid, uint32_t bits, coll_t dest)
{
  dout(10) << __func__ << " " << cid << " " << bits << " "
	   << dest << dendl;
  CollectionRef sc = get_collection(cid);
  if (!sc)
    return -ENOENT;
  CollectionRef dc = get_collection(dest);
  if (!dc)
    return -ENOENT;
  {
    std::scoped_lock l{std::min(&(*sc), &(*dc))->lock,
                       std::max(&(*sc), &(*dc))->lock};

    auto p = sc->object_map.begin();
    while (p != sc->object_map.end()) {
      dout(20) << " moving " << p->first << dendl;
      dc->object_map.insert(std::make_pair(p->first, p->second));
      dc->object_hash.insert(std::make_pair(p->first, p->second));
      sc->object_hash.erase(p->first);
      sc->object_map.erase(p++);
    }

    dc->bits = bits;
  }

  {
    std::lock_guard l{coll_lock};
    ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
    ceph_assert(cp != coll_map.end());
    used_bytes -= cp->second->used_bytes();
    coll_map.erase(cp);
  }

  return 0;
}

namespace {
struct BufferlistObject : public MemStore::Object {
  ceph::spinlock mutex;
  ceph::buffer::list data;

  size_t get_size() const override { return data.length(); }

  int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) override;
  int write(uint64_t offset, const ceph::buffer::list &bl) override;
  int clone(Object *src, uint64_t srcoff, uint64_t len,
            uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(ceph::buffer::list& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) override {
    DECODE_START(1, p);
    decode(data, p);
    decode_base(p);
    DECODE_FINISH(p);
  }
};
}
// BufferlistObject
int BufferlistObject::read(uint64_t offset, uint64_t len,
                                     ceph::buffer::list &bl)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  bl.substr_of(data, offset, len);
  return bl.length();
}

int BufferlistObject::write(uint64_t offset, const ceph::buffer::list &src)
{
  unsigned len = src.length();

  std::lock_guard<decltype(mutex)> lock(mutex);

  // before
  ceph::buffer::list newdata;
  if (get_size() >= offset) {
    newdata.substr_of(data, 0, offset);
  } else {
    if (get_size()) {
      newdata.substr_of(data, 0, get_size());
    }
    newdata.append_zero(offset - get_size());
  }

  newdata.append(src);

  // after
  if (get_size() > offset + len) {
    ceph::buffer::list tail;
    tail.substr_of(data, offset + len, get_size() - (offset + len));
    newdata.append(tail);
  }

  data = std::move(newdata);
  return 0;
}

int BufferlistObject::clone(Object *src, uint64_t srcoff,
                                      uint64_t len, uint64_t dstoff)
{
  auto srcbl = dynamic_cast<BufferlistObject*>(src);
  if (srcbl == nullptr)
    return -ENOTSUP;

  ceph::buffer::list bl;
  {
    std::lock_guard<decltype(srcbl->mutex)> lock(srcbl->mutex);
    if (srcoff == dstoff && len == src->get_size()) {
      data = srcbl->data;
      return 0;
    }
    bl.substr_of(srcbl->data, srcoff, len);
  }
  return write(dstoff, bl);
}

int BufferlistObject::truncate(uint64_t size)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  if (get_size() > size) {
    ceph::buffer::list bl;
    bl.substr_of(data, 0, size);
    data = std::move(bl);
  } else if (get_size() == size) {
    // do nothing
  } else {
    data.append_zero(size - get_size());
  }
  return 0;
}

// PageSetObject

struct MemStore::PageSetObject : public Object {
  PageSet data;
  uint64_t data_len;
#if defined(__GLIBCXX__)
  // use a thread-local vector for the pages returned by PageSet, so we
  // can avoid allocations in read/write()
  static thread_local PageSet::page_vector tls_pages;
#endif

  size_t get_size() const override { return data_len; }

  int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) override;
  int write(uint64_t offset, const ceph::buffer::list &bl) override;
  int clone(Object *src, uint64_t srcoff, uint64_t len,
            uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(ceph::buffer::list& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(data_len, bl);
    data.encode(bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) override {
    DECODE_START(1, p);
    decode(data_len, p);
    data.decode(p);
    decode_base(p);
    DECODE_FINISH(p);
  }

private:
  FRIEND_MAKE_REF(PageSetObject);
  explicit PageSetObject(size_t page_size) : data(page_size), data_len(0) {}
};

#if defined(__GLIBCXX__)
// use a thread-local vector for the pages returned by PageSet, so we
// can avoid allocations in read/write()
thread_local PageSet::page_vector MemStore::PageSetObject::tls_pages;
#define DEFINE_PAGE_VECTOR(name)
#else
#define DEFINE_PAGE_VECTOR(name) PageSet::page_vector name;
#endif

int MemStore::PageSetObject::read(uint64_t offset, uint64_t len, ceph::buffer::list& bl)
{
  const auto start = offset;
  const auto end = offset + len;
  auto remaining = len;

  DEFINE_PAGE_VECTOR(tls_pages);
  data.get_range(offset, len, tls_pages);

  // allocate a buffer for the data
  ceph::buffer::ptr buf(len);

  auto p = tls_pages.begin();
  while (remaining) {
    // no more pages in range
    if (p == tls_pages.end() || (*p)->offset >= end) {
      buf.zero(offset - start, remaining);
      break;
    }
    auto page = *p;

    // fill any holes between pages with zeroes
    if (page->offset > offset) {
      const auto count = std::min(remaining, page->offset - offset);
      buf.zero(offset - start, count);
      remaining -= count;
      offset = page->offset;
      if (!remaining)
        break;
    }

    // read from page
    const auto page_offset = offset - page->offset;
    const auto count = std::min(remaining, data.get_page_size() - page_offset);

    buf.copy_in(offset - start, count, page->data + page_offset);

    remaining -= count;
    offset += count;

    ++p;
  }

  tls_pages.clear(); // drop page refs

  bl.append(std::move(buf));
  return len;
}

int MemStore::PageSetObject::write(uint64_t offset, const ceph::buffer::list &src)
{
  unsigned len = src.length();

  DEFINE_PAGE_VECTOR(tls_pages);
  // make sure the page range is allocated
  data.alloc_range(offset, src.length(), tls_pages);

  auto page = tls_pages.begin();

  auto p = src.begin();
  while (len > 0) {
    unsigned page_offset = offset - (*page)->offset;
    unsigned pageoff = data.get_page_size() - page_offset;
    unsigned count = std::min(len, pageoff);
    p.copy(count, (*page)->data + page_offset);
    offset += count;
    len -= count;
    if (count == pageoff)
      ++page;
  }
  if (data_len < offset)
    data_len = offset;
  tls_pages.clear(); // drop page refs
  return 0;
}

int MemStore::PageSetObject::clone(Object *src, uint64_t srcoff,
                                   uint64_t len, uint64_t dstoff)
{
  const int64_t delta = dstoff - srcoff;

  auto &src_data = static_cast<PageSetObject*>(src)->data;
  const uint64_t src_page_size = src_data.get_page_size();

  auto &dst_data = data;
  const auto dst_page_size = dst_data.get_page_size();

  DEFINE_PAGE_VECTOR(tls_pages);
  PageSet::page_vector dst_pages;

  while (len) {
    // limit to 16 pages at a time so tls_pages doesn't balloon in size
    auto count = std::min(len, (uint64_t)src_page_size * 16);
    src_data.get_range(srcoff, count, tls_pages);

    // allocate the destination range
    // TODO: avoid allocating pages for holes in the source range
    dst_data.alloc_range(srcoff + delta, count, dst_pages);
    auto dst_iter = dst_pages.begin();

    for (auto &src_page : tls_pages) {
      auto sbegin = std::max(srcoff, src_page->offset);
      auto send = std::min(srcoff + count, src_page->offset + src_page_size);

      // zero-fill holes before src_page
      if (srcoff < sbegin) {
        while (dst_iter != dst_pages.end()) {
          auto &dst_page = *dst_iter;
          auto dbegin = std::max(srcoff + delta, dst_page->offset);
          auto dend = std::min(sbegin + delta, dst_page->offset + dst_page_size);
          std::fill(dst_page->data + dbegin - dst_page->offset,
                    dst_page->data + dend - dst_page->offset, 0);
          if (dend < dst_page->offset + dst_page_size)
            break;
          ++dst_iter;
        }
        const auto c = sbegin - srcoff;
        count -= c;
        len -= c;
      }

      // copy data from src page to dst pages
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(sbegin + delta, dst_page->offset);
        auto dend = std::min(send + delta, dst_page->offset + dst_page_size);

        std::copy(src_page->data + (dbegin - delta) - src_page->offset,
                  src_page->data + (dend - delta) - src_page->offset,
                  dst_page->data + dbegin - dst_page->offset);
        if (dend < dst_page->offset + dst_page_size)
          break;
        ++dst_iter;
      }

      const auto c = send - sbegin;
      count -= c;
      len -= c;
      srcoff = send;
      dstoff = send + delta;
    }
    tls_pages.clear(); // drop page refs

    // zero-fill holes after the last src_page
    if (count > 0) {
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(dstoff, dst_page->offset);
        auto dend = std::min(dstoff + count, dst_page->offset + dst_page_size);
        std::fill(dst_page->data + dbegin - dst_page->offset,
                  dst_page->data + dend - dst_page->offset, 0);
        ++dst_iter;
      }
      srcoff += count;
      dstoff += count;
      len -= count;
    }
    dst_pages.clear(); // drop page refs
  }

  // update object size
  if (data_len < dstoff)
    data_len = dstoff;
  return 0;
}

int MemStore::PageSetObject::truncate(uint64_t size)
{
  data.free_pages_after(size);
  data_len = size;

  const auto page_size = data.get_page_size();
  const auto page_offset = size & ~(page_size-1);
  if (page_offset == size)
    return 0;

  DEFINE_PAGE_VECTOR(tls_pages);
  // write zeroes to the rest of the last page
  data.get_range(page_offset, page_size, tls_pages);
  if (tls_pages.empty())
    return 0;

  auto page = tls_pages.begin();
  auto data = (*page)->data;
  std::fill(data + (size - page_offset), data + page_size, 0);
  tls_pages.clear(); // drop page ref
  return 0;
}


MemStore::ObjectRef MemStore::Collection::create_object() const {
  if (use_page_set)
    return ceph::make_ref<PageSetObject>(cct->_conf->memstore_page_size);
  return make_ref<BufferlistObject>();
}
