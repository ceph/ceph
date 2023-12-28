// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <limits.h>

#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "common/common_init.h"
#include "common/TracepointProvider.h"
#include "common/hobject.h"
#include "common/async/waiter.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include <include/stringify.h>

#include "librados/AioCompletionImpl.h"
#include "librados/IoCtxImpl.h"
#include "librados/ObjectOperationImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include "librados/RadosXattrIter.h"
#include "librados/ListObjectImpl.h"
#include "librados/librados_util.h"
#include "cls/lock/cls_lock_client.h"

#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <stdexcept>
#include <system_error>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/librados.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

using std::list;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

static TracepointProvider::Traits tracepoint_traits("librados_tp.so", "rados_tracing");

/*
 * Structure of this file
 *
 * RadosClient and the related classes are the internal implementation of librados.
 * Above that layer sits the C API, found in include/rados/librados.h, and
 * the C++ API, found in include/rados/librados.hpp
 *
 * The C++ API sometimes implements things in terms of the C API.
 * Both the C++ and C API rely on RadosClient.
 *
 * Visually:
 * +--------------------------------------+
 * |             C++ API                  |
 * +--------------------+                 |
 * |       C API        |                 |
 * +--------------------+-----------------+
 * |          RadosClient                 |
 * +--------------------------------------+
 */

size_t librados::ObjectOperation::size()
{
  ::ObjectOperation *o = &impl->o;
  if (o)
    return o->size();
  else
    return 0;
}

//deprcated
void librados::ObjectOperation::set_op_flags(ObjectOperationFlags flags)
{
  set_op_flags2((int)flags);
}

void librados::ObjectOperation::set_op_flags2(int flags)
{
  ceph_assert(impl);
  impl->o.set_last_op_flags(get_op_flags(flags));
}

void librados::ObjectOperation::cmpext(uint64_t off,
                                       const bufferlist &cmp_bl,
                                       int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = cmp_bl;
  o->cmpext(off, c, prval);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, const bufferlist& v)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_STRING, v);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, uint64_t v)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist bl;
  encode(v, bl);
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_U64, bl);
}

void librados::ObjectOperation::assert_version(uint64_t ver)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->assert_version(ver);
}

void librados::ObjectOperation::assert_exists()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->stat(nullptr, nullptr, nullptr);
}

void librados::ObjectOperation::exec(const char *cls, const char *method,
				     bufferlist& inbl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->call(cls, method, inbl);
}

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, bufferlist *outbl, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->call(cls, method, inbl, outbl, NULL, prval);
}

class ObjectOpCompletionCtx : public Context {
  librados::ObjectOperationCompletion *completion;
  bufferlist bl;
public:
  explicit ObjectOpCompletionCtx(librados::ObjectOperationCompletion *c) : completion(c) {}
  void finish(int r) override {
    completion->handle_completion(r, bl);
    delete completion;
  }

  bufferlist *outbl() {
    return &bl;
  }
};

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, librados::ObjectOperationCompletion *completion)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;

  ObjectOpCompletionCtx *ctx = new ObjectOpCompletionCtx(completion);

  o->call(cls, method, inbl, ctx->outbl(), ctx, NULL);
}

void librados::ObjectReadOperation::stat(uint64_t *psize, time_t *pmtime, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->stat(psize, pmtime, prval);
}

void librados::ObjectReadOperation::stat2(uint64_t *psize, struct timespec *pts, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->stat(psize, pts, prval);
}

void librados::ObjectReadOperation::read(size_t off, uint64_t len, bufferlist *pbl, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->read(off, len, pbl, prval, NULL);
}

void librados::ObjectReadOperation::sparse_read(uint64_t off, uint64_t len,
						std::map<uint64_t,uint64_t> *m,
						bufferlist *data_bl, int *prval,
						uint64_t truncate_size,
						uint32_t truncate_seq)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->sparse_read(off, len, m, data_bl, prval, truncate_size, truncate_seq);
}

void librados::ObjectReadOperation::checksum(rados_checksum_type_t type,
					     const bufferlist &init_value_bl,
					     uint64_t off, size_t len,
					     size_t chunk_size, bufferlist *pbl,
					     int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->checksum(get_checksum_op_type(type), init_value_bl, off, len, chunk_size,
	      pbl, prval, nullptr);
}

void librados::ObjectReadOperation::getxattr(const char *name, bufferlist *pbl, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->getxattr(name, pbl, prval);
}

void librados::ObjectReadOperation::omap_get_vals(
  const std::string &start_after,
  const std::string &filter_prefix,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, filter_prefix, max_return, out_vals, nullptr,
		   prval);
}

void librados::ObjectReadOperation::omap_get_vals2(
  const std::string &start_after,
  const std::string &filter_prefix,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  bool *pmore,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, filter_prefix, max_return, out_vals, pmore,
		   prval);
}

void librados::ObjectReadOperation::omap_get_vals(
  const std::string &start_after,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, "", max_return, out_vals, nullptr, prval);
}

void librados::ObjectReadOperation::omap_get_vals2(
  const std::string &start_after,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  bool *pmore,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, "", max_return, out_vals, pmore, prval);
}

void librados::ObjectReadOperation::omap_get_keys(
  const std::string &start_after,
  uint64_t max_return,
  std::set<std::string> *out_keys,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_keys(start_after, max_return, out_keys, nullptr, prval);
}

void librados::ObjectReadOperation::omap_get_keys2(
  const std::string &start_after,
  uint64_t max_return,
  std::set<std::string> *out_keys,
  bool *pmore,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_keys(start_after, max_return, out_keys, pmore, prval);
}

void librados::ObjectReadOperation::omap_get_header(bufferlist *bl, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_header(bl, prval);
}

void librados::ObjectReadOperation::omap_get_vals_by_keys(
  const std::set<std::string> &keys,
  std::map<std::string, bufferlist> *map,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals_by_keys(keys, map, prval);
}

void librados::ObjectOperation::omap_cmp(
  const std::map<std::string, pair<bufferlist, int> > &assertions,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_cmp(assertions, prval);
}

void librados::ObjectReadOperation::list_watchers(
  list<obj_watch_t> *out_watchers,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->list_watchers(out_watchers, prval);
}

void librados::ObjectReadOperation::list_snaps(
  snap_set_t *out_snaps,
  int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->list_snaps(out_snaps, prval);
}

void librados::ObjectReadOperation::is_dirty(bool *is_dirty, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->is_dirty(is_dirty, prval);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
                                   const std::string& orig_start_after,
                                   const std::string& filter_prefix,
                                   uint64_t max_return,
                                   std::map<std::string, bufferlist> *out_vals)
{
  bool first = true;
  string start_after = orig_start_after;
  bool more = true;
  while (max_return > 0 && more) {
    std::map<std::string,bufferlist> out;
    ObjectReadOperation op;
    op.omap_get_vals2(start_after, filter_prefix, max_return, &out, &more,
		      nullptr);
    bufferlist bl;
    int ret = operate(oid, &op, &bl);
    if (ret < 0) {
      return ret;
    }
    if (more) {
      if (out.empty()) {
	return -EINVAL;  // wth
      }
      start_after = out.rbegin()->first;
    }
    if (out.size() <= max_return) {
      max_return -= out.size();
    } else {
      max_return = 0;
    }
    if (first) {
      out_vals->swap(out);
      first = false;
    } else {
      out_vals->insert(out.begin(), out.end());
      out.clear();
    }
  }
  return 0;
}

int librados::IoCtx::omap_get_vals2(
  const std::string& oid,
  const std::string& start_after,
  const std::string& filter_prefix,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  bool *pmore)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals2(start_after, filter_prefix, max_return, out_vals, pmore, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;
  return r;
}

void librados::ObjectReadOperation::getxattrs(map<string, bufferlist> *pattrs, int *prval)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->getxattrs(pattrs, prval);
}

void librados::ObjectWriteOperation::mtime(time_t *pt)
{
  ceph_assert(impl);
  if (pt) {
    impl->rt = ceph::real_clock::from_time_t(*pt);
    impl->prt = &impl->rt;
  }
}

void librados::ObjectWriteOperation::mtime2(struct timespec *pts)
{
  ceph_assert(impl);
  if (pts) {
    impl->rt = ceph::real_clock::from_timespec(*pts);
    impl->prt = &impl->rt;
  }
}

void librados::ObjectWriteOperation::create(bool exclusive)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->create(exclusive);
}

void librados::ObjectWriteOperation::create(bool exclusive,
					    const std::string& category) // unused
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->create(exclusive);
}

void librados::ObjectWriteOperation::write(uint64_t off, const bufferlist& bl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->write(off, c);
}

void librados::ObjectWriteOperation::write_full(const bufferlist& bl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->write_full(c);
}

void librados::ObjectWriteOperation::writesame(uint64_t off, uint64_t write_len,
					       const bufferlist& bl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->writesame(off, write_len, c);
}

void librados::ObjectWriteOperation::append(const bufferlist& bl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->append(c);
}

void librados::ObjectWriteOperation::remove()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->remove();
}

void librados::ObjectWriteOperation::truncate(uint64_t off)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->truncate(off);
}

void librados::ObjectWriteOperation::zero(uint64_t off, uint64_t len)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->zero(off, len);
}

void librados::ObjectWriteOperation::rmxattr(const char *name)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->rmxattr(name);
}

void librados::ObjectWriteOperation::setxattr(const char *name, const bufferlist& v)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->setxattr(name, v);
}

void librados::ObjectWriteOperation::setxattr(const char *name,
					      const buffer::list&& v)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->setxattr(name, std::move(v));
}

void librados::ObjectWriteOperation::omap_set(
  const map<string, bufferlist> &map)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_set(map);
}

void librados::ObjectWriteOperation::omap_set_header(const bufferlist &bl)
{
  ceph_assert(impl);
  bufferlist c = bl;
  ::ObjectOperation *o = &impl->o;
  o->omap_set_header(c);
}

void librados::ObjectWriteOperation::omap_clear()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_clear();
}

void librados::ObjectWriteOperation::omap_rm_keys(
  const std::set<std::string> &to_rm)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->omap_rm_keys(to_rm);
}

void librados::ObjectWriteOperation::copy_from(const std::string& src,
					       const IoCtx& src_ioctx,
					       uint64_t src_version,
					       uint32_t src_fadvise_flags)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->copy_from(object_t(src), src_ioctx.io_ctx_impl->snap_seq,
	       src_ioctx.io_ctx_impl->oloc, src_version, 0, src_fadvise_flags);
}

void librados::ObjectWriteOperation::copy_from2(const std::string& src,
					        const IoCtx& src_ioctx,
					        uint64_t src_version,
						uint32_t truncate_seq,
						uint64_t truncate_size,
					        uint32_t src_fadvise_flags)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->copy_from2(object_t(src), src_ioctx.io_ctx_impl->snap_seq,
	        src_ioctx.io_ctx_impl->oloc, src_version, 0,
	        truncate_seq, truncate_size, src_fadvise_flags);
}

void librados::ObjectWriteOperation::undirty()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->undirty();
}

void librados::ObjectReadOperation::cache_flush()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cache_flush();
}

void librados::ObjectReadOperation::cache_try_flush()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cache_try_flush();
}

void librados::ObjectReadOperation::cache_evict()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cache_evict();
}

void librados::ObjectReadOperation::tier_flush()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->tier_flush();
}

void librados::ObjectReadOperation::tier_evict()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->tier_evict();
}

void librados::ObjectWriteOperation::set_redirect(const std::string& tgt_obj, 
						  const IoCtx& tgt_ioctx,
						  uint64_t tgt_version,
						  int flag)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->set_redirect(object_t(tgt_obj), tgt_ioctx.io_ctx_impl->snap_seq,
			  tgt_ioctx.io_ctx_impl->oloc, tgt_version, flag);
}

void librados::ObjectReadOperation::set_chunk(uint64_t src_offset,
					       uint64_t src_length,
					       const IoCtx& tgt_ioctx,
					       string tgt_oid,
					       uint64_t tgt_offset,
					       int flag)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->set_chunk(src_offset, src_length,
	       tgt_ioctx.io_ctx_impl->oloc, object_t(tgt_oid), tgt_offset, flag);
}

void librados::ObjectWriteOperation::tier_promote()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->tier_promote();
}

void librados::ObjectWriteOperation::unset_manifest()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->unset_manifest();
}

void librados::ObjectWriteOperation::tmap_update(const bufferlist& cmdbl)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  bufferlist c = cmdbl;
  o->tmap_update(c);
}

void librados::ObjectWriteOperation::selfmanaged_snap_rollback(snap_t snapid)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->rollback(snapid);
}

// You must specify the snapid not the name normally used with pool snapshots
void librados::ObjectWriteOperation::snap_rollback(snap_t snapid)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->rollback(snapid);
}

void librados::ObjectWriteOperation::set_alloc_hint(
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->set_alloc_hint(expected_object_size, expected_write_size, 0);
}
void librados::ObjectWriteOperation::set_alloc_hint2(
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size,
					    uint32_t flags)
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->set_alloc_hint(expected_object_size, expected_write_size, flags);
}

void librados::ObjectWriteOperation::cache_pin()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cache_pin();
}

void librados::ObjectWriteOperation::cache_unpin()
{
  ceph_assert(impl);
  ::ObjectOperation *o = &impl->o;
  o->cache_unpin();
}

librados::WatchCtx::
~WatchCtx()
{
}

librados::WatchCtx2::
~WatchCtx2()
{
}

///////////////////////////// NObjectIteratorImpl /////////////////////////////
librados::NObjectIteratorImpl::NObjectIteratorImpl(ObjListCtx *ctx_)
  : ctx(ctx_)
{
}

librados::NObjectIteratorImpl::~NObjectIteratorImpl()
{
  ctx.reset();
}

librados::NObjectIteratorImpl::NObjectIteratorImpl(const NObjectIteratorImpl &rhs)
{
  *this = rhs;
}

librados::NObjectIteratorImpl& librados::NObjectIteratorImpl::operator=(const librados::NObjectIteratorImpl &rhs)
{
  if (&rhs == this)
    return *this;
  if (rhs.ctx.get() == NULL) {
    ctx.reset();
    return *this;
  }
  Objecter::NListContext *list_ctx = new Objecter::NListContext(*rhs.ctx->nlc);
  ctx.reset(new ObjListCtx(rhs.ctx->ctx, list_ctx));
  cur_obj = rhs.cur_obj;
  return *this;
}

bool librados::NObjectIteratorImpl::operator==(const librados::NObjectIteratorImpl& rhs) const {

  if (ctx.get() == NULL) {
    if (rhs.ctx.get() == NULL)
      return true;
    return rhs.ctx->nlc->at_end();
  }
  if (rhs.ctx.get() == NULL) {
    // Redundant but same as ObjectIterator version
    if (ctx.get() == NULL)
      return true;
    return ctx->nlc->at_end();
  }
  return ctx.get() == rhs.ctx.get();
}

bool librados::NObjectIteratorImpl::operator!=(const librados::NObjectIteratorImpl& rhs) const {
  return !(*this == rhs);
}

const librados::ListObject& librados::NObjectIteratorImpl::operator*() const {
  return cur_obj;
}

const librados::ListObject* librados::NObjectIteratorImpl::operator->() const {
  return &cur_obj;
}

librados::NObjectIteratorImpl& librados::NObjectIteratorImpl::operator++()
{
  get_next();
  return *this;
}

librados::NObjectIteratorImpl librados::NObjectIteratorImpl::operator++(int)
{
  librados::NObjectIteratorImpl ret(*this);
  get_next();
  return ret;
}

uint32_t librados::NObjectIteratorImpl::seek(uint32_t pos)
{
  uint32_t r = rados_nobjects_list_seek(ctx.get(), pos);
  get_next();
  return r;
}

uint32_t librados::NObjectIteratorImpl::seek(const ObjectCursor& cursor)
{
  uint32_t r = rados_nobjects_list_seek_cursor(ctx.get(), (rados_object_list_cursor)cursor.c_cursor);
  get_next();
  return r;
}

librados::ObjectCursor librados::NObjectIteratorImpl::get_cursor()
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)ctx.get();
  librados::ObjectCursor oc;
  oc.set(lh->ctx->nlist_get_cursor(lh->nlc));
  return oc;
}

void librados::NObjectIteratorImpl::set_filter(const bufferlist &bl)
{
  ceph_assert(ctx);
  ctx->nlc->filter = bl;
}

void librados::NObjectIteratorImpl::get_next()
{
  const char *entry, *key, *nspace;
  size_t entry_size, key_size, nspace_size;
  if (ctx->nlc->at_end())
    return;
  int ret = rados_nobjects_list_next2(ctx.get(), &entry, &key, &nspace,
                                      &entry_size, &key_size, &nspace_size);
  if (ret == -ENOENT) {
    return;
  }
  else if (ret) {
    throw std::system_error(-ret, std::system_category(),
                            "rados_nobjects_list_next2");
  }

  if (cur_obj.impl == NULL)
    cur_obj.impl = new ListObjectImpl();
  cur_obj.impl->nspace = string{nspace, nspace_size};
  cur_obj.impl->oid = string{entry, entry_size};
  cur_obj.impl->locator = key ? string(key, key_size) : string();
}

uint32_t librados::NObjectIteratorImpl::get_pg_hash_position() const
{
  return ctx->nlc->get_pg_hash_position();
}

///////////////////////////// NObjectIterator /////////////////////////////
librados::NObjectIterator::NObjectIterator(ObjListCtx *ctx_)
{
  impl = new NObjectIteratorImpl(ctx_);
}

librados::NObjectIterator::~NObjectIterator()
{
  delete impl;
}

librados::NObjectIterator::NObjectIterator(const NObjectIterator &rhs)
{
  if (rhs.impl == NULL) {
    impl = NULL;
    return;
  }
  impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
}

librados::NObjectIterator& librados::NObjectIterator::operator=(const librados::NObjectIterator &rhs)
{
  if (rhs.impl == NULL) {
    delete impl;
    impl = NULL;
    return *this;
  }
  if (impl == NULL)
    impl = new NObjectIteratorImpl();
  *impl = *(rhs.impl);
  return *this;
}

bool librados::NObjectIterator::operator==(const librados::NObjectIterator& rhs) const 
{
  if (impl && rhs.impl) {
    return *impl == *(rhs.impl);
  } else {
    return impl == rhs.impl;
  }
}

bool librados::NObjectIterator::operator!=(const librados::NObjectIterator& rhs) const
{
  return !(*this == rhs);
}

const librados::ListObject& librados::NObjectIterator::operator*() const {
  ceph_assert(impl);
  return *(impl->get_listobjectp());
}

const librados::ListObject* librados::NObjectIterator::operator->() const {
  ceph_assert(impl);
  return impl->get_listobjectp();
}

librados::NObjectIterator& librados::NObjectIterator::operator++()
{
  ceph_assert(impl);
  impl->get_next();
  return *this;
}

librados::NObjectIterator librados::NObjectIterator::operator++(int)
{
  librados::NObjectIterator ret(*this);
  impl->get_next();
  return ret;
}

uint32_t librados::NObjectIterator::seek(uint32_t pos)
{
  ceph_assert(impl);
  return impl->seek(pos);
}

uint32_t librados::NObjectIterator::seek(const ObjectCursor& cursor)
{
  ceph_assert(impl);
  return impl->seek(cursor);
}

librados::ObjectCursor librados::NObjectIterator::get_cursor()
{
  ceph_assert(impl);
  return impl->get_cursor();
}

void librados::NObjectIterator::set_filter(const bufferlist &bl)
{
  impl->set_filter(bl);
}

void librados::NObjectIterator::get_next()
{
  ceph_assert(impl);
  impl->get_next();
}

uint32_t librados::NObjectIterator::get_pg_hash_position() const
{
  ceph_assert(impl);
  return impl->get_pg_hash_position();
}

const librados::NObjectIterator librados::NObjectIterator::__EndObjectIterator(NULL);

///////////////////////////// PoolAsyncCompletion //////////////////////////////
librados::PoolAsyncCompletion::PoolAsyncCompletion::~PoolAsyncCompletion()
{
  auto c = reinterpret_cast<PoolAsyncCompletionImpl *>(pc);
  c->release();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::set_callback(void *cb_arg,
								     rados_callback_t cb)
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->set_callback(cb_arg, cb);
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::wait()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->wait();
}

bool librados::PoolAsyncCompletion::PoolAsyncCompletion::is_complete()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->is_complete();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::get_return_value()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->get_return_value();
}

void librados::PoolAsyncCompletion::PoolAsyncCompletion::release()
{
  delete this;
}

///////////////////////////// AioCompletion //////////////////////////////
librados::AioCompletion::AioCompletion::~AioCompletion()
{
  auto c = reinterpret_cast<AioCompletionImpl *>(pc);
  c->release();
}

int librados::AioCompletion::AioCompletion::set_complete_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_complete_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::set_safe_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_safe_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::wait_for_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}

int librados::AioCompletion::AioCompletion::wait_for_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}

bool librados::AioCompletion::AioCompletion::is_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete();
}

bool librados::AioCompletion::AioCompletion::is_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_safe();
}

int librados::AioCompletion::AioCompletion::wait_for_complete_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete_and_cb();
}

int librados::AioCompletion::AioCompletion::wait_for_safe_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_safe_and_cb();
}

bool librados::AioCompletion::AioCompletion::is_complete_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete_and_cb();
}

bool librados::AioCompletion::AioCompletion::is_safe_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_safe_and_cb();
}

int librados::AioCompletion::AioCompletion::get_return_value()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_return_value();
}

int librados::AioCompletion::AioCompletion::get_version()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_version();
}

uint64_t librados::AioCompletion::AioCompletion::get_version64()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_version();
}

void librados::AioCompletion::AioCompletion::release()
{
  delete this;
}

///////////////////////////// IoCtx //////////////////////////////
librados::IoCtx::IoCtx() : io_ctx_impl(NULL)
{
}

void librados::IoCtx::from_rados_ioctx_t(rados_ioctx_t p, IoCtx &io)
{
  IoCtxImpl *io_ctx_impl = (IoCtxImpl*)p;

  io.io_ctx_impl = io_ctx_impl;
  if (io_ctx_impl) {
    io_ctx_impl->get();
  }
}

librados::IoCtx::IoCtx(const IoCtx& rhs)
{
  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    io_ctx_impl->get();
  }
}

librados::IoCtx& librados::IoCtx::operator=(const IoCtx& rhs)
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = rhs.io_ctx_impl;
  io_ctx_impl->get();
  return *this;
}

librados::IoCtx::IoCtx(IoCtx&& rhs) noexcept
  : io_ctx_impl(std::exchange(rhs.io_ctx_impl, nullptr))
{
}

librados::IoCtx& librados::IoCtx::operator=(IoCtx&& rhs) noexcept
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = std::exchange(rhs.io_ctx_impl, nullptr);
  return *this;
}

librados::IoCtx::~IoCtx()
{
  close();
}

bool librados::IoCtx::is_valid() const {
  return io_ctx_impl != nullptr;
}

void librados::IoCtx::close()
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = 0;
}

void librados::IoCtx::dup(const IoCtx& rhs)
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = new IoCtxImpl();
  io_ctx_impl->get();
  io_ctx_impl->dup(*rhs.io_ctx_impl);
}

int librados::IoCtx::set_auid(uint64_t auid_)
{
  return -EOPNOTSUPP;
}

int librados::IoCtx::set_auid_async(uint64_t auid_, PoolAsyncCompletion *c)
{
  return -EOPNOTSUPP;
}

int librados::IoCtx::get_auid(uint64_t *auid_)
{
  return -EOPNOTSUPP;
}

bool librados::IoCtx::pool_requires_alignment()
{
  return io_ctx_impl->client->pool_requires_alignment(get_id());
}

int librados::IoCtx::pool_requires_alignment2(bool *req)
{
  return io_ctx_impl->client->pool_requires_alignment2(get_id(), req);
}

uint64_t librados::IoCtx::pool_required_alignment()
{
  return io_ctx_impl->client->pool_required_alignment(get_id());
}

int librados::IoCtx::pool_required_alignment2(uint64_t *alignment)
{
  return io_ctx_impl->client->pool_required_alignment2(get_id(), alignment);
}

std::string librados::IoCtx::get_pool_name()
{
  std::string s;
  io_ctx_impl->client->pool_get_name(get_id(), &s);
  return s;
}

std::string librados::IoCtx::get_pool_name() const
{
  return io_ctx_impl->get_cached_pool_name();
}

uint64_t librados::IoCtx::get_instance_id() const
{
  return io_ctx_impl->client->get_instance_id();
}

int librados::IoCtx::create(const std::string& oid, bool exclusive)
{
  object_t obj(oid);
  return io_ctx_impl->create(obj, exclusive);
}

int librados::IoCtx::create(const std::string& oid, bool exclusive,
			    const std::string& category) // unused
{
  object_t obj(oid);
  return io_ctx_impl->create(obj, exclusive);
}

int librados::IoCtx::write(const std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->write(obj, bl, len, off);
}

int librados::IoCtx::append(const std::string& oid, bufferlist& bl, size_t len)
{
  object_t obj(oid);
  return io_ctx_impl->append(obj, bl, len);
}

int librados::IoCtx::write_full(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->write_full(obj, bl);
}

int librados::IoCtx::writesame(const std::string& oid, bufferlist& bl,
			       size_t write_len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->writesame(obj, bl, write_len, off);
}


int librados::IoCtx::read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->read(obj, bl, len, off);
}

int librados::IoCtx::checksum(const std::string& oid,
			      rados_checksum_type_t type,
			      const bufferlist &init_value_bl, size_t len,
			      uint64_t off, size_t chunk_size, bufferlist *pbl)
{
  object_t obj(oid);
  return io_ctx_impl->checksum(obj, get_checksum_op_type(type), init_value_bl,
			       len, off, chunk_size, pbl);
}

int librados::IoCtx::remove(const std::string& oid)
{
  object_t obj(oid);
  return io_ctx_impl->remove(obj);
}

int librados::IoCtx::remove(const std::string& oid, int flags)
{
  object_t obj(oid);
  return io_ctx_impl->remove(obj, flags); 
}

int librados::IoCtx::trunc(const std::string& oid, uint64_t size)
{
  object_t obj(oid);
  return io_ctx_impl->trunc(obj, size);
}

int librados::IoCtx::mapext(const std::string& oid, uint64_t off, size_t len,
			    std::map<uint64_t,uint64_t>& m)
{
  object_t obj(oid);
  return io_ctx_impl->mapext(obj, off, len, m);
}

int librados::IoCtx::cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl)
{
  object_t obj(oid);
  return io_ctx_impl->cmpext(obj, off, cmp_bl);
}

int librados::IoCtx::sparse_read(const std::string& oid, std::map<uint64_t,uint64_t>& m,
				 bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->sparse_read(obj, m, bl, len, off);
}

int librados::IoCtx::getxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->getxattr(obj, name, bl);
}

int librados::IoCtx::getxattrs(const std::string& oid, map<std::string, bufferlist>& attrset)
{
  object_t obj(oid);
  return io_ctx_impl->getxattrs(obj, attrset);
}

int librados::IoCtx::setxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->setxattr(obj, name, bl);
}

int librados::IoCtx::rmxattr(const std::string& oid, const char *name)
{
  object_t obj(oid);
  return io_ctx_impl->rmxattr(obj, name);
}

int librados::IoCtx::stat(const std::string& oid, uint64_t *psize, time_t *pmtime)
{
  object_t obj(oid);
  return io_ctx_impl->stat(obj, psize, pmtime);
}

int librados::IoCtx::stat2(const std::string& oid, uint64_t *psize, struct timespec *pts)
{
  object_t obj(oid);
  return io_ctx_impl->stat2(obj, psize, pts);
}

int librados::IoCtx::exec(const std::string& oid, const char *cls, const char *method,
			  bufferlist& inbl, bufferlist& outbl)
{
  object_t obj(oid);
  return io_ctx_impl->exec(obj, cls, method, inbl, outbl);
}

int librados::IoCtx::tmap_update(const std::string& oid, bufferlist& cmdbl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_update(obj, cmdbl);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
                                   const std::string& start_after,
                                   uint64_t max_return,
                                   std::map<std::string, bufferlist> *out_vals)
{
  return omap_get_vals(oid, start_after, string(), max_return, out_vals);
}

int librados::IoCtx::omap_get_vals2(
  const std::string& oid,
  const std::string& start_after,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  bool *pmore)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals2(start_after, max_return, out_vals, pmore, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;
  return r;
}

int librados::IoCtx::omap_get_keys(const std::string& oid,
                                   const std::string& orig_start_after,
                                   uint64_t max_return,
                                   std::set<std::string> *out_keys)
{
  bool first = true;
  string start_after = orig_start_after;
  bool more = true;
  while (max_return > 0 && more) {
    std::set<std::string> out;
    ObjectReadOperation op;
    op.omap_get_keys2(start_after, max_return, &out, &more, nullptr);
    bufferlist bl;
    int ret = operate(oid, &op, &bl);
    if (ret < 0) {
      return ret;
    }
    if (more) {
      if (out.empty()) {
	return -EINVAL;  // wth
      }
      start_after = *out.rbegin();
    }
    if (out.size() <= max_return) {
      max_return -= out.size();
    } else {
      max_return = 0;
    }
    if (first) {
      out_keys->swap(out);
      first = false;
    } else {
      out_keys->insert(out.begin(), out.end());
      out.clear();
    }
  }
  return 0;
}

int librados::IoCtx::omap_get_keys2(
  const std::string& oid,
  const std::string& start_after,
  uint64_t max_return,
  std::set<std::string> *out_keys,
  bool *pmore)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_keys2(start_after, max_return, out_keys, pmore, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;
  return r;
}

int librados::IoCtx::omap_get_header(const std::string& oid,
                                     bufferlist *bl)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_header(bl, &r);
  bufferlist b;
  int ret = operate(oid, &op, &b);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_get_vals_by_keys(const std::string& oid,
                                           const std::set<std::string>& keys,
                                           std::map<std::string, bufferlist> *vals)
{
  ObjectReadOperation op;
  int r;
  bufferlist bl;
  op.omap_get_vals_by_keys(keys, vals, &r);
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_set(const std::string& oid,
                              const map<string, bufferlist>& m)
{
  ObjectWriteOperation op;
  op.omap_set(m);
  return operate(oid, &op);
}

int librados::IoCtx::omap_set_header(const std::string& oid,
                                     const bufferlist& bl)
{
  ObjectWriteOperation op;
  op.omap_set_header(bl);
  return operate(oid, &op);
}

int librados::IoCtx::omap_clear(const std::string& oid)
{
  ObjectWriteOperation op;
  op.omap_clear();
  return operate(oid, &op);
}

int librados::IoCtx::omap_rm_keys(const std::string& oid,
                                  const std::set<std::string>& keys)
{
  ObjectWriteOperation op;
  op.omap_rm_keys(keys);
  return operate(oid, &op);
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->operate(obj, &o->impl->o, (ceph::real_time *)o->impl->prt);
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectWriteOperation *o, int flags)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->operate(obj, &o->impl->o, (ceph::real_time *)o->impl->prt, translate_flags(flags));
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectReadOperation *o, bufferlist *pbl)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->operate_read(obj, &o->impl->o, pbl);
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectReadOperation *o, bufferlist *pbl, int flags)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->operate_read(obj, &o->impl->o, pbl, translate_flags(flags));
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  io_ctx_impl->snapc, o->impl->prt, 0);
}
int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 ObjectWriteOperation *o, int flags)
{
  object_t obj(oid);
  if (unlikely(!o->impl))
    return -EINVAL;
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  io_ctx_impl->snapc, o->impl->prt,
				  translate_flags(flags));
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectWriteOperation *o,
				 snap_t snap_seq, std::vector<snap_t>& snaps)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(snap_seq, snv);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  snapc, o->impl->prt, 0);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
         librados::ObjectWriteOperation *o,
         snap_t snap_seq, std::vector<snap_t>& snaps,
         const blkin_trace_info *trace_info)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(snap_seq, snv);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
          snapc, o->impl->prt, 0, trace_info);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
         librados::ObjectWriteOperation *o,
         snap_t snap_seq, std::vector<snap_t>& snaps, int flags,
         const blkin_trace_info *trace_info)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(snap_seq, snv);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc, snapc, o->impl->prt,
                                  translate_flags(flags), trace_info);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o,
				 bufferlist *pbl)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, &o->impl->o, c->pc,
				       0, pbl);
}

// deprecated
int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o, 
				 snap_t snapid_unused_deprecated,
				 int flags, bufferlist *pbl)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  int op_flags = 0;
  if (flags & OPERATION_BALANCE_READS)
    op_flags |= CEPH_OSD_FLAG_BALANCE_READS;
  if (flags & OPERATION_LOCALIZE_READS)
    op_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
  if (flags & OPERATION_ORDER_READS_WRITES)
    op_flags |= CEPH_OSD_FLAG_RWORDERED;

  return io_ctx_impl->aio_operate_read(obj, &o->impl->o, c->pc,
				       op_flags, pbl);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o,
				 int flags, bufferlist *pbl)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, &o->impl->o, c->pc,
				       translate_flags(flags), pbl);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
         librados::ObjectReadOperation *o,
         int flags, bufferlist *pbl, const blkin_trace_info *trace_info)
{
  if (unlikely(!o->impl))
    return -EINVAL;
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, &o->impl->o, c->pc,
               translate_flags(flags), pbl, trace_info);
}

void librados::IoCtx::snap_set_read(snap_t seq)
{
  io_ctx_impl->set_snap_read(seq);
}

int librados::IoCtx::selfmanaged_snap_set_write_ctx(snap_t seq, vector<snap_t>& snaps)
{
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (unsigned i=0; i<snaps.size(); i++)
    snv[i] = snaps[i];
  return io_ctx_impl->set_snap_write_context(seq, snv);
}

int librados::IoCtx::snap_create(const char *snapname)
{
  return io_ctx_impl->snap_create(snapname);
}

int librados::IoCtx::snap_lookup(const char *name, snap_t *snapid)
{
  return io_ctx_impl->snap_lookup(name, snapid);
}

int librados::IoCtx::snap_get_stamp(snap_t snapid, time_t *t)
{
  return io_ctx_impl->snap_get_stamp(snapid, t);
}

int librados::IoCtx::snap_get_name(snap_t snapid, std::string *s)
{
  return io_ctx_impl->snap_get_name(snapid, s);
}

int librados::IoCtx::snap_remove(const char *snapname)
{
  return io_ctx_impl->snap_remove(snapname);
}

int librados::IoCtx::snap_list(std::vector<snap_t> *snaps)
{
  return io_ctx_impl->snap_list(snaps);
}

int librados::IoCtx::snap_rollback(const std::string& oid, const char *snapname)
{
  return io_ctx_impl->rollback(oid, snapname);
}

// Deprecated name kept for backward compatibility
int librados::IoCtx::rollback(const std::string& oid, const char *snapname)
{
  return snap_rollback(oid, snapname);
}

int librados::IoCtx::selfmanaged_snap_create(uint64_t *snapid)
{
  return io_ctx_impl->selfmanaged_snap_create(snapid);
}

void librados::IoCtx::aio_selfmanaged_snap_create(uint64_t *snapid,
                                                  AioCompletion *c)
{
  io_ctx_impl->aio_selfmanaged_snap_create(snapid, c->pc);
}

int librados::IoCtx::selfmanaged_snap_remove(uint64_t snapid)
{
  return io_ctx_impl->selfmanaged_snap_remove(snapid);
}

void librados::IoCtx::aio_selfmanaged_snap_remove(uint64_t snapid,
                                                  AioCompletion *c)
{
  io_ctx_impl->aio_selfmanaged_snap_remove(snapid, c->pc);
}

int librados::IoCtx::selfmanaged_snap_rollback(const std::string& oid, uint64_t snapid)
{
  return io_ctx_impl->selfmanaged_snap_rollback_object(oid,
						       io_ctx_impl->snapc,
						       snapid);
}

int librados::IoCtx::lock_exclusive(const std::string &oid, const std::string &name,
				    const std::string &cookie,
				    const std::string &description,
				    struct timeval * duration, uint8_t flags)
{
  utime_t dur = utime_t();
  if (duration)
    dur.set_from_timeval(duration);

  return rados::cls::lock::lock(this, oid, name, ClsLockType::EXCLUSIVE, cookie, "",
		  		description, dur, flags);
}

int librados::IoCtx::lock_shared(const std::string &oid, const std::string &name,
				 const std::string &cookie, const std::string &tag,
				 const std::string &description,
				 struct timeval * duration, uint8_t flags)
{
  utime_t dur = utime_t();
  if (duration)
    dur.set_from_timeval(duration);

  return rados::cls::lock::lock(this, oid, name, ClsLockType::SHARED, cookie, tag,
		  		description, dur, flags);
}

int librados::IoCtx::unlock(const std::string &oid, const std::string &name,
			    const std::string &cookie)
{
  return rados::cls::lock::unlock(this, oid, name, cookie);
}

struct AioUnlockCompletion : public librados::ObjectOperationCompletion {
  librados::AioCompletionImpl *completion;
  AioUnlockCompletion(librados::AioCompletion *c) : completion(c->pc) {
    completion->get();
  };
  void handle_completion(int r, bufferlist& outbl) override {
    rados_callback_t cb = completion->callback_complete;
    void *cb_arg = completion->callback_complete_arg;
    cb(completion, cb_arg);
    completion->lock.lock();
    completion->callback_complete = NULL;
    completion->cond.notify_all();
    completion->put_unlock();
  }
};

int librados::IoCtx::aio_unlock(const std::string &oid, const std::string &name,
			        const std::string &cookie, AioCompletion *c)
{
  return rados::cls::lock::aio_unlock(this, oid, name, cookie, c);
}

int librados::IoCtx::break_lock(const std::string &oid, const std::string &name,
				const std::string &client, const std::string &cookie)
{
  entity_name_t locker;
  if (!locker.parse(client))
    return -EINVAL;
  return rados::cls::lock::break_lock(this, oid, name, cookie, locker);
}

int librados::IoCtx::list_lockers(const std::string &oid, const std::string &name,
				  int *exclusive,
				  std::string *tag,
				  std::list<librados::locker_t> *lockers)
{
  std::list<librados::locker_t> tmp_lockers;
  map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> rados_lockers;
  std::string tmp_tag;
  ClsLockType tmp_type;
  int r = rados::cls::lock::get_lock_info(this, oid, name, &rados_lockers, &tmp_type, &tmp_tag);
  if (r < 0)
	  return r;

  map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t>::iterator map_it;
  for (map_it = rados_lockers.begin(); map_it != rados_lockers.end(); ++map_it) {
    librados::locker_t locker;
    locker.client = stringify(map_it->first.locker);
    locker.cookie = map_it->first.cookie;
    locker.address = stringify(map_it->second.addr);
    tmp_lockers.push_back(locker);
  }

  if (lockers)
    *lockers = tmp_lockers;
  if (tag)
    *tag = tmp_tag;
  if (exclusive) {
    if (tmp_type == ClsLockType::EXCLUSIVE)
      *exclusive = 1;
    else
      *exclusive = 0;
  }

  return tmp_lockers.size();
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
    const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.get_next();
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  uint32_t pos, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(pos);
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  const ObjectCursor& cursor, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(cursor);
  return iter;
}

const librados::NObjectIterator& librados::IoCtx::nobjects_end() const
{
  return NObjectIterator::__EndObjectIterator;
}

int librados::IoCtx::hit_set_list(uint32_t hash, AioCompletion *c,
				  std::list< std::pair<time_t, time_t> > *pls)
{
  return io_ctx_impl->hit_set_list(hash, c->pc, pls);
}

int librados::IoCtx::hit_set_get(uint32_t hash,  AioCompletion *c, time_t stamp,
				 bufferlist *pbl)
{
  return io_ctx_impl->hit_set_get(hash, c->pc, stamp, pbl);
}



uint64_t librados::IoCtx::get_last_version()
{
  return io_ctx_impl->last_version();
}

int librados::IoCtx::aio_read(const std::string& oid, librados::AioCompletion *c,
			      bufferlist *pbl, size_t len, uint64_t off)
{
  return io_ctx_impl->aio_read(oid, c->pc, pbl, len, off,
			       io_ctx_impl->snap_seq);
}

int librados::IoCtx::aio_read(const std::string& oid, librados::AioCompletion *c,
			      bufferlist *pbl, size_t len, uint64_t off,
			      uint64_t snapid)
{
  return io_ctx_impl->aio_read(oid, c->pc, pbl, len, off, snapid);
}

int librados::IoCtx::aio_exec(const std::string& oid,
			      librados::AioCompletion *c, const char *cls,
			      const char *method, bufferlist& inbl,
			      bufferlist *outbl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_exec(obj, c->pc, cls, method, inbl, outbl);
}

int librados::IoCtx::aio_cmpext(const std::string& oid,
				librados::AioCompletion *c,
				uint64_t off,
				bufferlist& cmp_bl)
{
  return io_ctx_impl->aio_cmpext(oid, c->pc, off, cmp_bl);
}

int librados::IoCtx::aio_sparse_read(const std::string& oid, librados::AioCompletion *c,
				     std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
				     size_t len, uint64_t off)
{
  return io_ctx_impl->aio_sparse_read(oid, c->pc,
				      m, data_bl, len, off,
				      io_ctx_impl->snap_seq);
}

int librados::IoCtx::aio_sparse_read(const std::string& oid, librados::AioCompletion *c,
				     std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
				     size_t len, uint64_t off, uint64_t snapid)
{
  return io_ctx_impl->aio_sparse_read(oid, c->pc,
				      m, data_bl, len, off, snapid);
}

int librados::IoCtx::aio_write(const std::string& oid, librados::AioCompletion *c,
			       const bufferlist& bl, size_t len, uint64_t off)
{
  return io_ctx_impl->aio_write(oid, c->pc, bl, len, off);
}

int librados::IoCtx::aio_append(const std::string& oid, librados::AioCompletion *c,
				const bufferlist& bl, size_t len)
{
  return io_ctx_impl->aio_append(oid, c->pc, bl, len);
}

int librados::IoCtx::aio_write_full(const std::string& oid, librados::AioCompletion *c,
				    const bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_write_full(obj, c->pc, bl);
}

int librados::IoCtx::aio_writesame(const std::string& oid, librados::AioCompletion *c,
				   const bufferlist& bl, size_t write_len,
				   uint64_t off)
{
  return io_ctx_impl->aio_writesame(oid, c->pc, bl, write_len, off);
}


int librados::IoCtx::aio_remove(const std::string& oid, librados::AioCompletion *c)
{
  return io_ctx_impl->aio_remove(oid, c->pc);
}

int librados::IoCtx::aio_remove(const std::string& oid, librados::AioCompletion *c, int flags)
{
  return io_ctx_impl->aio_remove(oid, c->pc, flags);
}

int librados::IoCtx::aio_flush_async(librados::AioCompletion *c)
{
  io_ctx_impl->flush_aio_writes_async(c->pc);
  return 0;
}

int librados::IoCtx::aio_flush()
{
  io_ctx_impl->flush_aio_writes();
  return 0;
}

struct AioGetxattrDataPP {
  AioGetxattrDataPP(librados::AioCompletionImpl *c, bufferlist *_bl) :
    bl(_bl), completion(c) {}
  bufferlist *bl;
  struct librados::CB_AioCompleteAndSafe completion;
};

static void rados_aio_getxattr_completepp(rados_completion_t c, void *arg) {
  AioGetxattrDataPP *cdata = reinterpret_cast<AioGetxattrDataPP*>(arg);
  int rc = rados_aio_get_return_value(c);
  if (rc >= 0) {
    rc = cdata->bl->length();
  }
  cdata->completion(rc);
  delete cdata;
}

int librados::IoCtx::aio_getxattr(const std::string& oid, librados::AioCompletion *c,
				  const char *name, bufferlist& bl)
{
  // create data object to be passed to async callback
  AioGetxattrDataPP *cdata = new AioGetxattrDataPP(c->pc, &bl);
  if (!cdata) {
    return -ENOMEM;
  }
  // create completion callback
  librados::AioCompletionImpl *comp = new librados::AioCompletionImpl;
  comp->set_complete_callback(cdata, rados_aio_getxattr_completepp);
  // call actual getxattr from IoCtxImpl
  object_t obj(oid);
  return io_ctx_impl->aio_getxattr(obj, comp, name, bl);
}

int librados::IoCtx::aio_getxattrs(const std::string& oid, AioCompletion *c,
				   map<std::string, bufferlist>& attrset)
{
  object_t obj(oid);
  return io_ctx_impl->aio_getxattrs(obj, c->pc, attrset);
}

int librados::IoCtx::aio_setxattr(const std::string& oid, AioCompletion *c,
				  const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_setxattr(obj, c->pc, name, bl);
}

int librados::IoCtx::aio_rmxattr(const std::string& oid, AioCompletion *c,
				 const char *name)
{
  object_t obj(oid);
  return io_ctx_impl->aio_rmxattr(obj, c->pc, name);
}

int librados::IoCtx::aio_stat(const std::string& oid, librados::AioCompletion *c,
			      uint64_t *psize, time_t *pmtime)
{
  object_t obj(oid);
  return io_ctx_impl->aio_stat(obj, c->pc, psize, pmtime);
}

int librados::IoCtx::aio_cancel(librados::AioCompletion *c)
{
  return io_ctx_impl->aio_cancel(c->pc);
}

int librados::IoCtx::watch(const string& oid, uint64_t ver, uint64_t *cookie,
			   librados::WatchCtx *ctx)
{
  object_t obj(oid);
  return io_ctx_impl->watch(obj, cookie, ctx, NULL);
}

int librados::IoCtx::watch2(const string& oid, uint64_t *cookie,
			    librados::WatchCtx2 *ctx2)
{
  object_t obj(oid);
  return io_ctx_impl->watch(obj, cookie, NULL, ctx2);
}

int librados::IoCtx::watch3(const string& oid, uint64_t *cookie,
          librados::WatchCtx2 *ctx2, uint32_t timeout)
{
  object_t obj(oid);
  return io_ctx_impl->watch(obj, cookie, NULL, ctx2, timeout);
}

int librados::IoCtx::aio_watch(const string& oid, AioCompletion *c,
                               uint64_t *cookie,
                               librados::WatchCtx2 *ctx2)
{
  object_t obj(oid);
  return io_ctx_impl->aio_watch(obj, c->pc, cookie, NULL, ctx2);
}

int librados::IoCtx::aio_watch2(const string& oid, AioCompletion *c,
                                uint64_t *cookie,
                                librados::WatchCtx2 *ctx2,
                                uint32_t timeout)
{
  object_t obj(oid);
  return io_ctx_impl->aio_watch(obj, c->pc, cookie, NULL, ctx2, timeout);
}

int librados::IoCtx::unwatch(const string& oid, uint64_t handle)
{
  return io_ctx_impl->unwatch(handle);
}

int librados::IoCtx::unwatch2(uint64_t handle)
{
  return io_ctx_impl->unwatch(handle);
}

int librados::IoCtx::aio_unwatch(uint64_t handle, AioCompletion *c)
{
  return io_ctx_impl->aio_unwatch(handle, c->pc);
}

int librados::IoCtx::watch_check(uint64_t handle)
{
  return io_ctx_impl->watch_check(handle);
}

int librados::IoCtx::notify(const string& oid, uint64_t ver, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->notify(obj, bl, 0, NULL, NULL, NULL);
}

int librados::IoCtx::notify2(const string& oid, bufferlist& bl,
			     uint64_t timeout_ms, bufferlist *preplybl)
{
  object_t obj(oid);
  return io_ctx_impl->notify(obj, bl, timeout_ms, preplybl, NULL, NULL);
}

int librados::IoCtx::aio_notify(const string& oid, AioCompletion *c,
                                bufferlist& bl, uint64_t timeout_ms,
                                bufferlist *preplybl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_notify(obj, c->pc, bl, timeout_ms, preplybl, NULL,
                                 NULL);
}

void librados::IoCtx::decode_notify_response(bufferlist &bl,
                                             std::vector<librados::notify_ack_t> *acks,
                                             std::vector<librados::notify_timeout_t> *timeouts)
{
  map<pair<uint64_t,uint64_t>,bufferlist> acked;
  set<pair<uint64_t,uint64_t>> missed;

  auto iter = bl.cbegin();
  decode(acked, iter);
  decode(missed, iter);

  for (auto &[who, payload] : acked) {
    acks->emplace_back(librados::notify_ack_t{who.first, who.second, payload});
  }
  for (auto &[notifier_id, cookie] : missed) {
    timeouts->emplace_back(librados::notify_timeout_t{notifier_id, cookie});
  }
}

void librados::IoCtx::notify_ack(const std::string& o,
				 uint64_t notify_id, uint64_t handle,
				 bufferlist& bl)
{
  io_ctx_impl->notify_ack(o, notify_id, handle, bl);
}

int librados::IoCtx::list_watchers(const std::string& oid,
                                   std::list<obj_watch_t> *out_watchers)
{
  ObjectReadOperation op;
  int r;
  op.list_watchers(out_watchers, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::list_snaps(const std::string& oid,
                                   snap_set_t *out_snaps)
{
  ObjectReadOperation op;
  int r;
  if (io_ctx_impl->snap_seq != CEPH_SNAPDIR)
    return -EINVAL;
  op.list_snaps(out_snaps, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

void librados::IoCtx::set_notify_timeout(uint32_t timeout)
{
  io_ctx_impl->set_notify_timeout(timeout);
}

int librados::IoCtx::set_alloc_hint(const std::string& o,
                                    uint64_t expected_object_size,
                                    uint64_t expected_write_size)
{
  object_t oid(o);
  return io_ctx_impl->set_alloc_hint(oid, expected_object_size,
                                     expected_write_size, 0);
}

int librados::IoCtx::set_alloc_hint2(const std::string& o,
				     uint64_t expected_object_size,
				     uint64_t expected_write_size,
				     uint32_t flags)
{
  object_t oid(o);
  return io_ctx_impl->set_alloc_hint(oid, expected_object_size,
                                     expected_write_size, flags);
}

void librados::IoCtx::set_assert_version(uint64_t ver)
{
  io_ctx_impl->set_assert_version(ver);
}

void librados::IoCtx::locator_set_key(const string& key)
{
  io_ctx_impl->oloc.key = key;
}

void librados::IoCtx::set_namespace(const string& nspace)
{
  io_ctx_impl->oloc.nspace = nspace;
}

std::string librados::IoCtx::get_namespace() const
{
  return io_ctx_impl->oloc.nspace;
}

int64_t librados::IoCtx::get_id()
{
  return io_ctx_impl->get_id();
}

uint32_t librados::IoCtx::get_object_hash_position(const std::string& oid)
{
  uint32_t hash;
  int r = io_ctx_impl->get_object_hash_position(oid, &hash);
  if (r < 0)
    hash = 0;
  return hash;
}

uint32_t librados::IoCtx::get_object_pg_hash_position(const std::string& oid)
{
  uint32_t hash;
  int r = io_ctx_impl->get_object_pg_hash_position(oid, &hash);
  if (r < 0)
    hash = 0;
  return hash;
}

int librados::IoCtx::get_object_hash_position2(
    const std::string& oid, uint32_t *hash_position)
{
  return io_ctx_impl->get_object_hash_position(oid, hash_position);
}

int librados::IoCtx::get_object_pg_hash_position2(
    const std::string& oid, uint32_t *pg_hash_position)
{
  return io_ctx_impl->get_object_pg_hash_position(oid, pg_hash_position);
}

librados::config_t librados::IoCtx::cct()
{
  return (config_t)io_ctx_impl->client->cct;
}

librados::IoCtx::IoCtx(IoCtxImpl *io_ctx_impl_)
  : io_ctx_impl(io_ctx_impl_)
{
}

void librados::IoCtx::set_osdmap_full_try()
{
  io_ctx_impl->extra_op_flags |= CEPH_OSD_FLAG_FULL_TRY;
}

void librados::IoCtx::unset_osdmap_full_try()
{
  io_ctx_impl->extra_op_flags &= ~CEPH_OSD_FLAG_FULL_TRY;
}

bool librados::IoCtx::get_pool_full_try()
{
  return (io_ctx_impl->extra_op_flags & CEPH_OSD_FLAG_FULL_TRY) != 0;
}

void librados::IoCtx::set_pool_full_try()
{
  io_ctx_impl->extra_op_flags |= CEPH_OSD_FLAG_FULL_TRY;
}

void librados::IoCtx::unset_pool_full_try()
{
  io_ctx_impl->extra_op_flags &= ~CEPH_OSD_FLAG_FULL_TRY;
}

///////////////////////////// Rados //////////////////////////////
void librados::Rados::version(int *major, int *minor, int *extra)
{
  rados_version(major, minor, extra);
}

librados::Rados::Rados() : client(NULL)
{
}

librados::Rados::Rados(IoCtx &ioctx)
{
  client = ioctx.io_ctx_impl->client;
  ceph_assert(client != NULL);
  client->get();
}

librados::Rados::~Rados()
{
  shutdown();
}

void librados::Rados::from_rados_t(rados_t cluster, Rados &rados) {
  if (rados.client) {
    rados.client->put();
  }
  rados.client = static_cast<RadosClient*>(cluster);
  if (rados.client) {
    rados.client->get();
  }
}

int librados::Rados::init(const char * const id)
{
  return rados_create((rados_t *)&client, id);
}

int librados::Rados::init2(const char * const name,
			   const char * const clustername, uint64_t flags)
{
  return rados_create2((rados_t *)&client, clustername, name, flags);
}

int librados::Rados::init_with_context(config_t cct_)
{
  return rados_create_with_context((rados_t *)&client, (rados_config_t)cct_);
}

int librados::Rados::connect()
{
  return client->connect();
}

librados::config_t librados::Rados::cct()
{
  return (config_t)client->cct;
}

int librados::Rados::watch_flush()
{
  if (!client)
    return -EINVAL;
  return client->watch_flush();
}

int librados::Rados::aio_watch_flush(AioCompletion *c)
{
  if (!client)
    return -EINVAL;
  return client->async_watch_flush(c->pc);
}

void librados::Rados::shutdown()
{
  if (!client)
    return;
  if (client->put()) {
    client->shutdown();
    delete client;
    client = NULL;
  }
}

uint64_t librados::Rados::get_instance_id()
{
  return client->get_instance_id();
}

int librados::Rados::get_min_compatible_osd(int8_t* require_osd_release)
{
  return client->get_min_compatible_osd(require_osd_release);
}

int librados::Rados::get_min_compatible_client(int8_t* min_compat_client,
                                               int8_t* require_min_compat_client)
{
  return client->get_min_compatible_client(min_compat_client,
                                           require_min_compat_client);
}

int librados::Rados::conf_read_file(const char * const path) const
{
  return rados_conf_read_file((rados_t)client, path);
}

int librados::Rados::conf_parse_argv(int argc, const char ** argv) const
{
  return rados_conf_parse_argv((rados_t)client, argc, argv);
}

int librados::Rados::conf_parse_argv_remainder(int argc, const char ** argv,
					       const char ** remargv) const
{
  return rados_conf_parse_argv_remainder((rados_t)client, argc, argv, remargv);
}

int librados::Rados::conf_parse_env(const char *name) const
{
  return rados_conf_parse_env((rados_t)client, name);
}

int librados::Rados::conf_set(const char *option, const char *value)
{
  return rados_conf_set((rados_t)client, option, value);
}

int librados::Rados::conf_get(const char *option, std::string &val)
{
  char *str = NULL;
  const auto& conf = client->cct->_conf;
  int ret = conf.get_val(option, &str, -1);
  if (ret) {
    free(str);
    return ret;
  }
  val = str;
  free(str);
  return 0;
}

int librados::Rados::service_daemon_register(
  const std::string& service,  ///< service name (e.g., 'rgw')
  const std::string& name,     ///< daemon name (e.g., 'gwfoo')
  const std::map<std::string,std::string>& metadata) ///< static metadata about daemon
{
  return client->service_daemon_register(service, name, metadata);
}

int librados::Rados::service_daemon_update_status(
  std::map<std::string,std::string>&& status)
{
  return client->service_daemon_update_status(std::move(status));
}

int librados::Rados::pool_create(const char *name)
{
  string str(name);
  return client->pool_create(str);
}

int librados::Rados::pool_create(const char *name, uint64_t auid)
{
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    return -EINVAL;
  }
  string str(name);
  return client->pool_create(str);
}

int librados::Rados::pool_create(const char *name, uint64_t auid, __u8 crush_rule)
{
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    return -EINVAL;
  }
  string str(name);
  return client->pool_create(str, crush_rule);
}

int librados::Rados::pool_create_with_rule(const char *name, __u8 crush_rule)
{
  string str(name);
  return client->pool_create(str, crush_rule);
}

int librados::Rados::pool_create_async(const char *name, PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, PoolAsyncCompletion *c)
{
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    return -EINVAL;
  }
  string str(name);
  return client->pool_create_async(str, c->pc);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, __u8 crush_rule,
				       PoolAsyncCompletion *c)
{
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    return -EINVAL;
  }
  string str(name);
  return client->pool_create_async(str, c->pc, crush_rule);
}

int librados::Rados::pool_create_with_rule_async(
  const char *name, __u8 crush_rule,
  PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc, crush_rule);
}

int librados::Rados::pool_get_base_tier(int64_t pool_id, int64_t* base_tier)
{
  tracepoint(librados, rados_pool_get_base_tier_enter, (rados_t)client, pool_id);
  int retval = client->pool_get_base_tier(pool_id, base_tier);
  tracepoint(librados, rados_pool_get_base_tier_exit, retval, *base_tier);
  return retval;
}

int librados::Rados::pool_delete(const char *name)
{
  return client->pool_delete(name);
}

int librados::Rados::pool_delete_async(const char *name, PoolAsyncCompletion *c)
{
  return client->pool_delete_async(name, c->pc);
}

int librados::Rados::pool_list(std::list<std::string>& v)
{
  std::list<std::pair<int64_t, std::string> > pools;
  int r = client->pool_list(pools);
  if (r < 0) {
    return r;
  }

  v.clear();
  for (std::list<std::pair<int64_t, std::string> >::iterator it = pools.begin();
       it != pools.end(); ++it) {
    v.push_back(it->second);
  }
  return 0;
}

int librados::Rados::pool_list2(std::list<std::pair<int64_t, std::string> >& v)
{
  return client->pool_list(v);
}

int64_t librados::Rados::pool_lookup(const char *name)
{
  return client->lookup_pool(name);
}

int librados::Rados::pool_reverse_lookup(int64_t id, std::string *name)
{
  return client->pool_get_name(id, name, true);
}

int librados::Rados::mon_command(string cmd, const bufferlist& inbl,
				 bufferlist *outbl, string *outs)
{
  vector<string> cmdvec;
  cmdvec.push_back(cmd);
  return client->mon_command(cmdvec, inbl, outbl, outs);
}

int librados::Rados::osd_command(int osdid, std::string cmd, const bufferlist& inbl,
                                 bufferlist *outbl, std::string *outs)
{
  vector<string> cmdvec;
  cmdvec.push_back(cmd);
  return client->osd_command(osdid, cmdvec, inbl, outbl, outs);
}

int librados::Rados::mgr_command(std::string cmd, const bufferlist& inbl,
                                 bufferlist *outbl, std::string *outs)
{
  vector<string> cmdvec;
  cmdvec.push_back(cmd);
  return client->mgr_command(cmdvec, inbl, outbl, outs);
}



int librados::Rados::pg_command(const char *pgstr, std::string cmd, const bufferlist& inbl,
                                bufferlist *outbl, std::string *outs)
{
  vector<string> cmdvec;
  cmdvec.push_back(cmd);

  pg_t pgid;
  if (!pgid.parse(pgstr))
    return -EINVAL;

  return client->pg_command(pgid, cmdvec, inbl, outbl, outs);
}

int librados::Rados::ioctx_create(const char *name, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create((rados_t)client, name, &p);
  if (ret)
    return ret;
  io.close();
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

int librados::Rados::ioctx_create2(int64_t pool_id, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create2((rados_t)client, pool_id, &p);
  if (ret)
    return ret;
  io.close();
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

void librados::Rados::test_blocklist_self(bool set)
{
  client->blocklist_self(set);
}

int librados::Rados::get_pool_stats(std::list<string>& v,
				    stats_map& result)
{
  map<string,::pool_stat_t> rawresult;
  bool per_pool = false;
  int r = client->get_pool_stats(v, &rawresult, &per_pool);
  for (map<string,::pool_stat_t>::iterator p = rawresult.begin();
       p != rawresult.end();
       ++p) {
    pool_stat_t& pv = result[p->first];
    auto& pstat = p->second;
    store_statfs_t &statfs = pstat.store_stats;
    uint64_t allocated_bytes = pstat.get_allocated_data_bytes(per_pool) +
      pstat.get_allocated_omap_bytes(per_pool);
    // FIXME: raw_used_rate is unknown hence use 1.0 here
    // meaning we keep net amount aggregated over all replicas
    // Not a big deal so far since this field isn't exposed
    uint64_t user_bytes = pstat.get_user_data_bytes(1.0, per_pool) +
      pstat.get_user_omap_bytes(1.0, per_pool);

    object_stat_sum_t *sum = &p->second.stats.sum;
    pv.num_kb = shift_round_up(allocated_bytes, 10);
    pv.num_bytes = allocated_bytes;
    pv.num_objects = sum->num_objects;
    pv.num_object_clones = sum->num_object_clones;
    pv.num_object_copies = sum->num_object_copies;
    pv.num_objects_missing_on_primary = sum->num_objects_missing_on_primary;
    pv.num_objects_unfound = sum->num_objects_unfound;
    pv.num_objects_degraded = sum->num_objects_degraded;
    pv.num_rd = sum->num_rd;
    pv.num_rd_kb = sum->num_rd_kb;
    pv.num_wr = sum->num_wr;
    pv.num_wr_kb = sum->num_wr_kb;
    pv.num_user_bytes = user_bytes;
    pv.compressed_bytes_orig = statfs.data_compressed_original;
    pv.compressed_bytes = statfs.data_compressed;
    pv.compressed_bytes_alloc = statfs.data_compressed_allocated;
  }
  return r;
}

int librados::Rados::get_pool_stats(std::list<string>& v,
				    std::map<string, stats_map>& result)
{
  stats_map m;
  int r = get_pool_stats(v, m);
  if (r < 0)
    return r;
  for (map<string,pool_stat_t>::iterator p = m.begin();
       p != m.end();
       ++p) {
    result[p->first][string()] = p->second;
  }
  return r;
}

int librados::Rados::get_pool_stats(std::list<string>& v,
				    string& category, // unused
				    std::map<string, stats_map>& result)
{
  return -EOPNOTSUPP;
}

// deprecated, use pool_is_in_selfmanaged_snaps_mode() instead
bool librados::Rados::get_pool_is_selfmanaged_snaps_mode(const std::string& pool)
{
  // errors are ignored, prone to false negative results
  return client->pool_is_in_selfmanaged_snaps_mode(pool) > 0;
}

int librados::Rados::pool_is_in_selfmanaged_snaps_mode(const std::string& pool)
{
  return client->pool_is_in_selfmanaged_snaps_mode(pool);
}

int librados::Rados::cluster_stat(cluster_stat_t& result)
{
  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result.kb = stats.kb;
  result.kb_used = stats.kb_used;
  result.kb_avail = stats.kb_avail;
  result.num_objects = stats.num_objects;
  return r;
}

int librados::Rados::cluster_fsid(string *fsid)
{
  return client->get_fsid(fsid);
}

namespace librados {
  struct PlacementGroupImpl {
    pg_t pgid;
  };

  PlacementGroup::PlacementGroup()
    : impl{new PlacementGroupImpl}
  {}

  PlacementGroup::PlacementGroup(const PlacementGroup& pg)
    : impl{new PlacementGroupImpl}
  {
    impl->pgid = pg.impl->pgid;
  }

  PlacementGroup::~PlacementGroup()
  {}

  bool PlacementGroup::parse(const char* s)
  {
    return impl->pgid.parse(s);
  }
}

std::ostream& librados::operator<<(std::ostream& out,
				   const librados::PlacementGroup& pg)
{
  return out << pg.impl->pgid;
}

int librados::Rados::get_inconsistent_pgs(int64_t pool_id,
					  std::vector<PlacementGroup>* pgs)
{
  std::vector<string> pgids;
  if (auto ret = client->get_inconsistent_pgs(pool_id, &pgids); ret) {
    return ret;
  }
  for (const auto& pgid : pgids) {
    librados::PlacementGroup pg;
    if (!pg.parse(pgid.c_str())) {
      return -EINVAL;
    }
    pgs->emplace_back(pg);
  }
  return 0;
}

int librados::Rados::get_inconsistent_objects(const PlacementGroup& pg,
					      const object_id_t &start_after,
					      unsigned max_return,
					      AioCompletion *c,
					      std::vector<inconsistent_obj_t>* objects,
					      uint32_t* interval)
{
  IoCtx ioctx;
  const pg_t pgid = pg.impl->pgid;
  int r = ioctx_create2(pgid.pool(), ioctx);
  if (r < 0) {
    return r;
  }

  return ioctx.io_ctx_impl->get_inconsistent_objects(pgid,
						     start_after,
						     max_return,
						     c->pc,
						     objects,
						     interval);
}

int librados::Rados::get_inconsistent_snapsets(const PlacementGroup& pg,
					       const object_id_t &start_after,
					       unsigned max_return,
					       AioCompletion *c,
					       std::vector<inconsistent_snapset_t>* snapsets,
					       uint32_t* interval)
{
  IoCtx ioctx;
  const pg_t pgid = pg.impl->pgid;
  int r = ioctx_create2(pgid.pool(), ioctx);
  if (r < 0) {
    return r;
  }

  return ioctx.io_ctx_impl->get_inconsistent_snapsets(pgid,
						      start_after,
						      max_return,
						      c->pc,
						      snapsets,
						      interval);
}

int librados::Rados::wait_for_latest_osdmap()
{
  return client->wait_for_latest_osdmap();
}

int librados::Rados::blocklist_add(const std::string& client_address,
				   uint32_t expire_seconds)
{
  return client->blocklist_add(client_address, expire_seconds);
}

std::string librados::Rados::get_addrs() const {
  return client->get_addrs();
}

librados::PoolAsyncCompletion *librados::Rados::pool_async_create_completion()
{
  PoolAsyncCompletionImpl *c = new PoolAsyncCompletionImpl;
  return new PoolAsyncCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion()
{
  AioCompletionImpl *c = new AioCompletionImpl;
  return new AioCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion(void *cb_arg,
								callback_t cb_complete,
								callback_t cb_safe)
{
  AioCompletionImpl *c;
  int r = rados_aio_create_completion(cb_arg, cb_complete, cb_safe, (void**)&c);
  ceph_assert(r == 0);
  return new AioCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion(void *cb_arg,
								callback_t cb_complete)
{
  AioCompletionImpl *c;
  int r = rados_aio_create_completion2(cb_arg, cb_complete, (void**)&c);
  ceph_assert(r == 0);
  return new AioCompletion(c);
}

librados::ObjectOperation::ObjectOperation() : impl(new ObjectOperationImpl) {}

librados::ObjectOperation::ObjectOperation(ObjectOperation&& rhs)
  : impl(rhs.impl) {
  rhs.impl = nullptr;
}

librados::ObjectOperation&
librados::ObjectOperation::operator =(ObjectOperation&& rhs) {
  delete impl;
  impl = rhs.impl;
  rhs.impl = nullptr;
  return *this;
}

librados::ObjectOperation::~ObjectOperation() {
  delete impl;
}

///////////////////////////// ListObject //////////////////////////////
librados::ListObject::ListObject() : impl(NULL)
{
}

librados::ListObject::ListObject(librados::ListObjectImpl *i): impl(i)
{
}

librados::ListObject::ListObject(const ListObject& rhs)
{
  if (rhs.impl == NULL) {
    impl = NULL;
    return;
  }
  impl = new ListObjectImpl();
  *impl = *(rhs.impl);
}

librados::ListObject& librados::ListObject::operator=(const ListObject& rhs)
{
  if (rhs.impl == NULL) {
    delete impl;
    impl = NULL;
    return *this;
  }
  if (impl == NULL)
    impl = new ListObjectImpl();
  *impl = *(rhs.impl);
  return *this;
}

librados::ListObject::~ListObject()
{
  if (impl)
    delete impl;
  impl = NULL;
}

const std::string& librados::ListObject::get_nspace() const
{
  return impl->get_nspace();
}

const std::string& librados::ListObject::get_oid() const
{
  return impl->get_oid();
}

const std::string& librados::ListObject::get_locator() const
{
  return impl->get_locator();
}

std::ostream& librados::operator<<(std::ostream& out, const librados::ListObject& lop)
{
  out << *(lop.impl);
  return out;
}

librados::ObjectCursor::ObjectCursor()
{
  c_cursor = (rados_object_list_cursor)new hobject_t();
}

librados::ObjectCursor::~ObjectCursor()
{
  hobject_t *h = (hobject_t *)c_cursor;
  delete h;
}

librados::ObjectCursor::ObjectCursor(rados_object_list_cursor c)
{
  if (!c) {
    c_cursor = nullptr;
  } else {
    c_cursor = (rados_object_list_cursor)new hobject_t(*(hobject_t *)c);
  }
}

librados::ObjectCursor& librados::ObjectCursor::operator=(const librados::ObjectCursor& rhs)
{
  if (rhs.c_cursor != nullptr) {
    hobject_t *h = (hobject_t*)rhs.c_cursor;
    c_cursor = (rados_object_list_cursor)(new hobject_t(*h));
  } else {
    c_cursor = nullptr;
  }
  return *this;
}

bool librados::ObjectCursor::operator<(const librados::ObjectCursor &rhs) const
{
  const hobject_t lhs_hobj = (c_cursor == nullptr) ? hobject_t() : *((hobject_t*)c_cursor);
  const hobject_t rhs_hobj = (rhs.c_cursor == nullptr) ? hobject_t() : *((hobject_t*)(rhs.c_cursor));
  return lhs_hobj < rhs_hobj;
}

bool librados::ObjectCursor::operator==(const librados::ObjectCursor &rhs) const
{
  const hobject_t lhs_hobj = (c_cursor == nullptr) ? hobject_t() : *((hobject_t*)c_cursor);
  const hobject_t rhs_hobj = (rhs.c_cursor == nullptr) ? hobject_t() : *((hobject_t*)(rhs.c_cursor));
  return cmp(lhs_hobj, rhs_hobj) == 0;
}
librados::ObjectCursor::ObjectCursor(const librados::ObjectCursor &rhs)
{
  *this = rhs;
}

librados::ObjectCursor librados::IoCtx::object_list_begin()
{
  hobject_t *h = new hobject_t(io_ctx_impl->objecter->enumerate_objects_begin());
  ObjectCursor oc;
  oc.set((rados_object_list_cursor)h);
  return oc;
}


librados::ObjectCursor librados::IoCtx::object_list_end()
{
  hobject_t *h = new hobject_t(io_ctx_impl->objecter->enumerate_objects_end());
  librados::ObjectCursor oc;
  oc.set((rados_object_list_cursor)h);
  return oc;
}


void librados::ObjectCursor::set(rados_object_list_cursor c)
{
  delete (hobject_t*)c_cursor;
  c_cursor = c;
}

string librados::ObjectCursor::to_str() const
{
  stringstream ss;
  ss << *(hobject_t *)c_cursor;
  return ss.str();
}

bool librados::ObjectCursor::from_str(const string& s)
{
  if (s.empty()) {
    *(hobject_t *)c_cursor = hobject_t();
    return true;
  }
  return ((hobject_t *)c_cursor)->parse(s);
}

CEPH_RADOS_API std::ostream& librados::operator<<(std::ostream& os, const librados::ObjectCursor& oc)
{
  if (oc.c_cursor) {
    os << *(hobject_t *)oc.c_cursor;
  } else {
    os << hobject_t();
  }
  return os;
}

bool librados::IoCtx::object_list_is_end(const ObjectCursor &oc)
{
  hobject_t *h = (hobject_t *)oc.c_cursor;
  return h->is_max();
}

int librados::IoCtx::object_list(const ObjectCursor &start,
                const ObjectCursor &finish,
                const size_t result_item_count,
                const bufferlist &filter,
                std::vector<ObjectItem> *result,
                ObjectCursor *next)
{
  ceph_assert(result != nullptr);
  ceph_assert(next != nullptr);
  result->clear();

  ceph::async::waiter<boost::system::error_code,
		      std::vector<librados::ListObjectImpl>,
		      hobject_t>  w;
  io_ctx_impl->objecter->enumerate_objects<librados::ListObjectImpl>(
      io_ctx_impl->poolid,
      io_ctx_impl->oloc.nspace,
      *((hobject_t*)start.c_cursor),
      *((hobject_t*)finish.c_cursor),
      result_item_count,
      filter,
      w);

  auto [ec, obj_result, next_hash] = w.wait();
  if (ec) {
    next->set((rados_object_list_cursor)(new hobject_t(hobject_t::get_max())));
    return ceph::from_error_code(ec);
  }

  next->set((rados_object_list_cursor)(new hobject_t(next_hash)));

  for (auto i = obj_result.begin();
       i != obj_result.end(); ++i) {
    ObjectItem oi;
    oi.oid = i->oid;
    oi.nspace = i->nspace;
    oi.locator = i->locator;
    result->push_back(oi);
  }

  return obj_result.size();
}

void librados::IoCtx::object_list_slice(
    const ObjectCursor start,
    const ObjectCursor finish,
    const size_t n,
    const size_t m,
    ObjectCursor *split_start,
    ObjectCursor *split_finish)
{
  ceph_assert(split_start != nullptr);
  ceph_assert(split_finish != nullptr);

  io_ctx_impl->object_list_slice(
      *((hobject_t*)(start.c_cursor)),
      *((hobject_t*)(finish.c_cursor)),
      n,
      m,
      (hobject_t*)(split_start->c_cursor),
      (hobject_t*)(split_finish->c_cursor));
}

int librados::IoCtx::application_enable(const std::string& app_name,
                                        bool force)
{
  return io_ctx_impl->application_enable(app_name, force);
}

int librados::IoCtx::application_enable_async(const std::string& app_name,
                                              bool force,
                                              PoolAsyncCompletion *c)
{
  io_ctx_impl->application_enable_async(app_name, force, c->pc);
  return 0;
}

int librados::IoCtx::application_list(std::set<std::string> *app_names)
{
  return io_ctx_impl->application_list(app_names);
}

int librados::IoCtx::application_metadata_get(const std::string& app_name,
                                              const std::string &key,
                                              std::string* value)
{
  return io_ctx_impl->application_metadata_get(app_name, key, value);
}

int librados::IoCtx::application_metadata_set(const std::string& app_name,
                                              const std::string &key,
                                              const std::string& value)
{
  return io_ctx_impl->application_metadata_set(app_name, key, value);
}

int librados::IoCtx::application_metadata_remove(const std::string& app_name,
                                                 const std::string &key)
{
  return io_ctx_impl->application_metadata_remove(app_name, key);
}

int librados::IoCtx::application_metadata_list(const std::string& app_name,
                                               std::map<std::string, std::string> *values)
{
  return io_ctx_impl->application_metadata_list(app_name, values);
}
