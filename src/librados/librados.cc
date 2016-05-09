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
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include <include/stringify.h>

#include "librados/AioCompletionImpl.h"
#include "librados/IoCtxImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include "librados/RadosXattrIter.h"
#include "librados/ListObjectImpl.h"
#include <cls/lock/cls_lock_client.h>

#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <stdexcept>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/librados.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

using std::string;
using std::map;
using std::set;
using std::vector;
using std::list;
using std::runtime_error;

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

#define RADOS_LIST_MAX_ENTRIES 1024

namespace {

TracepointProvider::Traits tracepoint_traits("librados_tp.so", "rados_tracing");

} // anonymous namespace

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

namespace librados {

struct ObjectOperationImpl {
  ::ObjectOperation o;
  real_time rt;
  real_time *prt;

  ObjectOperationImpl() : prt(NULL) {}
};

}

size_t librados::ObjectOperation::size()
{
  ::ObjectOperation *o = &impl->o;
  return o->size();
}

static void set_op_flags(::ObjectOperation *o, int flags)
{
  int rados_flags = 0;
  if (flags & LIBRADOS_OP_FLAG_EXCL)
    rados_flags |= CEPH_OSD_OP_FLAG_EXCL;
  if (flags & LIBRADOS_OP_FLAG_FAILOK)
    rados_flags |= CEPH_OSD_OP_FLAG_FAILOK;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_RANDOM;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_WILLNEED)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_WILLNEED;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_NOCACHE)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  o->set_last_op_flags(rados_flags);
}

//deprcated
void librados::ObjectOperation::set_op_flags(ObjectOperationFlags flags)
{
  ::set_op_flags(&impl->o, (int)flags);
}

void librados::ObjectOperation::set_op_flags2(int flags)
{
  ::ObjectOperation *o = &impl->o;
  ::set_op_flags(o, flags);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, const bufferlist& v)
{
  ::ObjectOperation *o = &impl->o;
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_STRING, v);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, uint64_t v)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist bl;
  ::encode(v, bl);
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_U64, bl);
}

void librados::ObjectOperation::src_cmpxattr(const std::string& src_oid,
					 const char *name, int op, const bufferlist& v)
{
  ::ObjectOperation *o = &impl->o;
  object_t oid(src_oid);
  o->src_cmpxattr(oid, CEPH_NOSNAP, name, v, op, CEPH_OSD_CMPXATTR_MODE_STRING);
}

void librados::ObjectOperation::src_cmpxattr(const std::string& src_oid,
					 const char *name, int op, uint64_t val)
{
  ::ObjectOperation *o = &impl->o;
  object_t oid(src_oid);
  bufferlist bl;
  ::encode(val, bl);
  o->src_cmpxattr(oid, CEPH_NOSNAP, name, bl, op, CEPH_OSD_CMPXATTR_MODE_U64);
}

void librados::ObjectOperation::assert_version(uint64_t ver)
{
  ::ObjectOperation *o = &impl->o;
  o->assert_version(ver);
}

void librados::ObjectOperation::assert_exists()
{
  ::ObjectOperation *o = &impl->o;
  o->stat(NULL, (ceph::real_time*) NULL, NULL);
}

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl)
{
  ::ObjectOperation *o = &impl->o;
  o->call(cls, method, inbl);
}

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, bufferlist *outbl, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->call(cls, method, inbl, outbl, NULL, prval);
}

class ObjectOpCompletionCtx : public Context {
  librados::ObjectOperationCompletion *completion;
  bufferlist bl;
public:
  explicit ObjectOpCompletionCtx(librados::ObjectOperationCompletion *c) : completion(c) {}
  void finish(int r) {
    completion->handle_completion(r, bl);
    delete completion;
  }

  bufferlist *outbl() {
    return &bl;
  }
};

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, librados::ObjectOperationCompletion *completion)
{
  ::ObjectOperation *o = &impl->o;

  ObjectOpCompletionCtx *ctx = new ObjectOpCompletionCtx(completion);

  o->call(cls, method, inbl, ctx->outbl(), ctx, NULL);
}

void librados::ObjectReadOperation::stat(uint64_t *psize, time_t *pmtime, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->stat(psize, pmtime, prval);
}

void librados::ObjectReadOperation::stat2(uint64_t *psize, struct timespec *pts, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->stat(psize, pts, prval);
}

void librados::ObjectReadOperation::read(size_t off, uint64_t len, bufferlist *pbl, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->read(off, len, pbl, prval, NULL);
}

void librados::ObjectReadOperation::sparse_read(uint64_t off, uint64_t len,
						std::map<uint64_t,uint64_t> *m,
						bufferlist *data_bl, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->sparse_read(off, len, m, data_bl, prval);
}

void librados::ObjectReadOperation::tmap_get(bufferlist *pbl, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->tmap_get(pbl, prval);
}

void librados::ObjectReadOperation::getxattr(const char *name, bufferlist *pbl, int *prval)
{
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
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, filter_prefix, max_return, out_vals, prval);
}

void librados::ObjectReadOperation::omap_get_vals(
  const std::string &start_after,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals(start_after, "", max_return, out_vals, prval);
}

void librados::ObjectReadOperation::omap_get_keys(
  const std::string &start_after,
  uint64_t max_return,
  std::set<std::string> *out_keys,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_get_keys(start_after, max_return, out_keys, prval);
}

void librados::ObjectReadOperation::omap_get_header(bufferlist *bl, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_get_header(bl, prval);
}

void librados::ObjectReadOperation::omap_get_vals_by_keys(
  const std::set<std::string> &keys,
  std::map<std::string, bufferlist> *map,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_get_vals_by_keys(keys, map, prval);
}

void librados::ObjectOperation::omap_cmp(
  const std::map<std::string, pair<bufferlist, int> > &assertions,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_cmp(assertions, prval);
}

void librados::ObjectReadOperation::list_watchers(
  list<obj_watch_t> *out_watchers,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->list_watchers(out_watchers, prval);
}

void librados::ObjectReadOperation::list_snaps(
  snap_set_t *out_snaps,
  int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->list_snaps(out_snaps, prval);
}

void librados::ObjectReadOperation::is_dirty(bool *is_dirty, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->is_dirty(is_dirty, prval);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
                                   const std::string& start_after,
                                   const std::string& filter_prefix,
                                   uint64_t max_return,
                                   std::map<std::string, bufferlist> *out_vals)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals(start_after, filter_prefix, max_return, out_vals, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

void librados::ObjectReadOperation::getxattrs(map<string, bufferlist> *pattrs, int *prval)
{
  ::ObjectOperation *o = &impl->o;
  o->getxattrs(pattrs, prval);
}

void librados::ObjectWriteOperation::mtime(time_t *pt)
{
  if (pt) {
    impl->rt = ceph::real_clock::from_time_t(*pt);
    impl->prt = &impl->rt;
  }
}

void librados::ObjectWriteOperation::mtime2(struct timespec *pts)
{
  if (pts) {
    impl->rt = ceph::real_clock::from_timespec(*pts);
    impl->prt = &impl->rt;
  }
}

void librados::ObjectWriteOperation::create(bool exclusive)
{
  ::ObjectOperation *o = &impl->o;
  o->create(exclusive);
}

void librados::ObjectWriteOperation::create(bool exclusive,
					    const std::string& category) // unused
{
  ::ObjectOperation *o = &impl->o;
  o->create(exclusive);
}

void librados::ObjectWriteOperation::write(uint64_t off, const bufferlist& bl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->write(off, c);
}

void librados::ObjectWriteOperation::write_full(const bufferlist& bl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->write_full(c);
}

void librados::ObjectWriteOperation::writesame(uint64_t off, uint64_t write_len,
					       const bufferlist& bl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->writesame(off, write_len, c);
}

void librados::ObjectWriteOperation::append(const bufferlist& bl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->append(c);
}

void librados::ObjectWriteOperation::remove()
{
  ::ObjectOperation *o = &impl->o;
  o->remove();
}

void librados::ObjectWriteOperation::truncate(uint64_t off)
{
  ::ObjectOperation *o = &impl->o;
  o->truncate(off);
}

void librados::ObjectWriteOperation::zero(uint64_t off, uint64_t len)
{
  ::ObjectOperation *o = &impl->o;
  o->zero(off, len);
}

void librados::ObjectWriteOperation::rmxattr(const char *name)
{
  ::ObjectOperation *o = &impl->o;
  o->rmxattr(name);
}

void librados::ObjectWriteOperation::setxattr(const char *name, const bufferlist& v)
{
  ::ObjectOperation *o = &impl->o;
  o->setxattr(name, v);
}

void librados::ObjectWriteOperation::omap_set(
  const map<string, bufferlist> &map)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_set(map);
}

void librados::ObjectWriteOperation::omap_set_header(const bufferlist &bl)
{
  bufferlist c = bl;
  ::ObjectOperation *o = &impl->o;
  o->omap_set_header(c);
}

void librados::ObjectWriteOperation::omap_clear()
{
  ::ObjectOperation *o = &impl->o;
  o->omap_clear();
}

void librados::ObjectWriteOperation::omap_rm_keys(
  const std::set<std::string> &to_rm)
{
  ::ObjectOperation *o = &impl->o;
  o->omap_rm_keys(to_rm);
}

void librados::ObjectWriteOperation::copy_from(const std::string& src,
                                               const IoCtx& src_ioctx,
                                               uint64_t src_version)
{
  copy_from2(src, src_ioctx, src_version, 0);
}

void librados::ObjectWriteOperation::copy_from2(const std::string& src,
					        const IoCtx& src_ioctx,
					        uint64_t src_version,
					        uint32_t src_fadvise_flags)
{
  ::ObjectOperation *o = &impl->o;
  o->copy_from(object_t(src), src_ioctx.io_ctx_impl->snap_seq,
	       src_ioctx.io_ctx_impl->oloc, src_version, 0, src_fadvise_flags);
}

void librados::ObjectWriteOperation::undirty()
{
  ::ObjectOperation *o = &impl->o;
  o->undirty();
}

void librados::ObjectReadOperation::cache_flush()
{
  ::ObjectOperation *o = &impl->o;
  o->cache_flush();
}

void librados::ObjectReadOperation::cache_try_flush()
{
  ::ObjectOperation *o = &impl->o;
  o->cache_try_flush();
}

void librados::ObjectReadOperation::cache_evict()
{
  ::ObjectOperation *o = &impl->o;
  o->cache_evict();
}

void librados::ObjectWriteOperation::tmap_put(const bufferlist &bl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = bl;
  o->tmap_put(c);
}

void librados::ObjectWriteOperation::tmap_update(const bufferlist& cmdbl)
{
  ::ObjectOperation *o = &impl->o;
  bufferlist c = cmdbl;
  o->tmap_update(c);
}

void librados::ObjectWriteOperation::clone_range(uint64_t dst_off,
                     const std::string& src_oid, uint64_t src_off,
                     size_t len)
{
  ::ObjectOperation *o = &impl->o;
  o->clone_range(src_oid, src_off, len, dst_off);
}

void librados::ObjectWriteOperation::selfmanaged_snap_rollback(snap_t snapid)
{
  ::ObjectOperation *o = &impl->o;
  o->rollback(snapid);
}

// You must specify the snapid not the name normally used with pool snapshots
void librados::ObjectWriteOperation::snap_rollback(snap_t snapid)
{
  ::ObjectOperation *o = &impl->o;
  o->rollback(snapid);
}

void librados::ObjectWriteOperation::set_alloc_hint(
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size)
{
  ::ObjectOperation *o = &impl->o;
  o->set_alloc_hint(expected_object_size, expected_write_size);
}

void librados::ObjectWriteOperation::cache_pin()
{
  ::ObjectOperation *o = &impl->o;
  o->cache_pin();
}

void librados::ObjectWriteOperation::cache_unpin()
{
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


struct librados::ObjListCtx {
  bool new_request;
  librados::IoCtxImpl dupctx;
  librados::IoCtxImpl *ctx;
  Objecter::ListContext *lc;
  Objecter::NListContext *nlc;

  ObjListCtx(IoCtxImpl *c, Objecter::ListContext *l) : new_request(false), lc(l), nlc(NULL) {
    // Get our own private IoCtxImpl so that namespace setting isn't changed by caller
    // between uses.
    ctx = &dupctx;
    dupctx.dup(*c);
  }
  ObjListCtx(IoCtxImpl *c, Objecter::NListContext *nl) : new_request(true), lc(NULL), nlc(nl) {
    // Get our own private IoCtxImpl so that namespace setting isn't changed by caller
    // between uses.
    ctx = &dupctx;
    dupctx.dup(*c);
  }
  ~ObjListCtx() {
    ctx = NULL;
    if (new_request)
      delete nlc;
    else
      delete lc;
  }
};

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
  if (rhs.ctx->new_request) {
    Objecter::NListContext *list_ctx = new Objecter::NListContext(*rhs.ctx->nlc);
    ctx.reset(new ObjListCtx(rhs.ctx->ctx, list_ctx));
    cur_obj = rhs.cur_obj;
  } else {
    Objecter::ListContext *list_ctx = new Objecter::ListContext(*rhs.ctx->lc);
    ctx.reset(new ObjListCtx(rhs.ctx->ctx, list_ctx));
    cur_obj = rhs.cur_obj;
  }
  return *this;
}

bool librados::NObjectIteratorImpl::operator==(const librados::NObjectIteratorImpl& rhs) const {

  if (ctx.get() == NULL) {
    if (rhs.ctx.get() == NULL)
      return true;
    if (rhs.ctx->new_request)
      return rhs.ctx->nlc->at_end();
    else
      return rhs.ctx->lc->at_end();
  }
  if (rhs.ctx.get() == NULL) {
    // Redundant but same as ObjectIterator version
    if (ctx.get() == NULL)
      return true;
    if (ctx->new_request)
      return ctx->nlc->at_end();
    else
      return ctx->lc->at_end();
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

void librados::NObjectIteratorImpl::set_filter(const bufferlist &bl)
{
  assert(ctx);
  if (ctx->nlc) {
    ctx->nlc->filter = bl;
  }

  if (ctx->lc) {
    ctx->lc->filter = bl;
  }
}

void librados::NObjectIteratorImpl::get_next()
{
  const char *entry, *key, *nspace;
  if (ctx->new_request) {
    if (ctx->nlc->at_end())
      return;
  } else {
    if (ctx->lc->at_end())
      return;
  }
  int ret = rados_nobjects_list_next(ctx.get(), &entry, &key, &nspace);
  if (ret == -ENOENT) {
    return;
  }
  else if (ret) {
    ostringstream oss;
    oss << "rados returned " << cpp_strerror(ret);
    throw std::runtime_error(oss.str());
  }

  if (cur_obj.impl == NULL)
    cur_obj.impl = new ListObjectImpl();
  cur_obj.impl->nspace = nspace;
  cur_obj.impl->oid = entry;
  cur_obj.impl->locator = key ? key : string();
}

uint32_t librados::NObjectIteratorImpl::get_pg_hash_position() const
{
  if (ctx->new_request)
    return ctx->nlc->get_pg_hash_position();
  else
    return ctx->lc->get_pg_hash_position();
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
  assert(impl);
  return *(impl->get_listobjectp());
}

const librados::ListObject* librados::NObjectIterator::operator->() const {
  assert(impl);
  return impl->get_listobjectp();
}

librados::NObjectIterator& librados::NObjectIterator::operator++()
{
  assert(impl);
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
  assert(impl);
  return impl->seek(pos);
}

void librados::NObjectIterator::set_filter(const bufferlist &bl)
{
  impl->set_filter(bl);
}

void librados::NObjectIterator::get_next()
{
  assert(impl);
  impl->get_next();
}

uint32_t librados::NObjectIterator::get_pg_hash_position() const
{
  assert(impl);
  return impl->get_pg_hash_position();
}

const librados::NObjectIterator librados::NObjectIterator::__EndObjectIterator(NULL);

// DEPRECATED; Use NObjectIterator instead
///////////////////////////// ObjectIterator /////////////////////////////
librados::ObjectIterator::ObjectIterator(ObjListCtx *ctx_)
  : ctx(ctx_)
{
}

librados::ObjectIterator::~ObjectIterator()
{
  ctx.reset();
}

librados::ObjectIterator::ObjectIterator(const ObjectIterator &rhs)
{
  *this = rhs;
}

librados::ObjectIterator& librados::ObjectIterator::operator=(const librados::ObjectIterator &rhs)
{
  if (&rhs == this)
    return *this;
  if (rhs.ctx.get() == NULL) {
    ctx.reset();
    return *this;
  }
  Objecter::ListContext *list_ctx = new Objecter::ListContext(*rhs.ctx->lc);
  ctx.reset(new ObjListCtx(rhs.ctx->ctx, list_ctx));
  cur_obj = rhs.cur_obj;
  return *this;
}

bool librados::ObjectIterator::operator==(const librados::ObjectIterator& rhs) const {
  if (ctx.get() == NULL)
    return rhs.ctx.get() == NULL || rhs.ctx->lc->at_end();
  if (rhs.ctx.get() == NULL)
    return ctx.get() == NULL || ctx->lc->at_end();
  return ctx.get() == rhs.ctx.get();
}

bool librados::ObjectIterator::operator!=(const librados::ObjectIterator& rhs) const {
  return !(*this == rhs);
}

const pair<std::string, std::string>& librados::ObjectIterator::operator*() const {
  return cur_obj;
}

const pair<std::string, std::string>* librados::ObjectIterator::operator->() const {
  return &cur_obj;
}

librados::ObjectIterator& librados::ObjectIterator::operator++()
{
  get_next();
  return *this;
}

librados::ObjectIterator librados::ObjectIterator::operator++(int)
{
  librados::ObjectIterator ret(*this);
  get_next();
  return ret;
}

uint32_t librados::ObjectIterator::seek(uint32_t pos)
{
  uint32_t r = rados_objects_list_seek(ctx.get(), pos);
  get_next();
  return r;
}

void librados::ObjectIterator::get_next()
{
  const char *entry, *key;
  if (ctx->lc->at_end())
    return;
  int ret = rados_objects_list_next(ctx.get(), &entry, &key);
  if (ret == -ENOENT) {
    return;
  }
  else if (ret) {
    ostringstream oss;
    oss << "rados returned " << cpp_strerror(ret);
    throw std::runtime_error(oss.str());
  }

  cur_obj = make_pair(entry, key ? key : string());
}

uint32_t librados::ObjectIterator::get_pg_hash_position() const
{
  return ctx->lc->get_pg_hash_position();
}

const librados::ObjectIterator librados::ObjectIterator::__EndObjectIterator(NULL);

///////////////////////////// PoolAsyncCompletion //////////////////////////////
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
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  c->release();
  delete this;
}

///////////////////////////// AioCompletion //////////////////////////////
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
  return c->wait_for_safe();
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
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  c->release();
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

librados::IoCtx::~IoCtx()
{
  close();
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
  return io_ctx_impl->pool_change_auid(auid_);
}

int librados::IoCtx::set_auid_async(uint64_t auid_, PoolAsyncCompletion *c)
{
  return io_ctx_impl->pool_change_auid_async(auid_, c->pc);
}

int librados::IoCtx::get_auid(uint64_t *auid_)
{
  return rados_ioctx_pool_get_auid(io_ctx_impl, auid_);
}

bool librados::IoCtx::pool_requires_alignment()
{
  return io_ctx_impl->client->pool_requires_alignment(get_id());
}

int librados::IoCtx::pool_requires_alignment2(bool *requires)
{
  return io_ctx_impl->client->pool_requires_alignment2(get_id(), requires);
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

const std::string& librados::IoCtx::get_pool_name() const
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

int librados::IoCtx::clone_range(const std::string& dst_oid, uint64_t dst_off,
				 const std::string& src_oid, uint64_t src_off,
				 size_t len)
{
  object_t src(src_oid), dst(dst_oid);
  return io_ctx_impl->clone_range(dst, dst_off, src, src_off, len);
}

int librados::IoCtx::read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->read(obj, bl, len, off);
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

int librados::IoCtx::tmap_put(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_put(obj, bl);
}

int librados::IoCtx::tmap_get(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_get(obj, bl);
}

int librados::IoCtx::tmap_to_omap(const std::string& oid, bool nullok)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_to_omap(obj, nullok);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
                                   const std::string& start_after,
                                   uint64_t max_return,
                                   std::map<std::string, bufferlist> *out_vals)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals(start_after, max_return, out_vals, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_get_keys(const std::string& oid,
                                   const std::string& start_after,
                                   uint64_t max_return,
                                   std::set<std::string> *out_keys)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_keys(start_after, max_return, out_keys, &r);
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



static int translate_flags(int flags)
{
  int op_flags = 0;
  if (flags & librados::OPERATION_BALANCE_READS)
    op_flags |= CEPH_OSD_FLAG_BALANCE_READS;
  if (flags & librados::OPERATION_LOCALIZE_READS)
    op_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
  if (flags & librados::OPERATION_ORDER_READS_WRITES)
    op_flags |= CEPH_OSD_FLAG_RWORDERED;
  if (flags & librados::OPERATION_IGNORE_CACHE)
    op_flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (flags & librados::OPERATION_SKIPRWLOCKS)
    op_flags |= CEPH_OSD_FLAG_SKIPRWLOCKS;
  if (flags & librados::OPERATION_IGNORE_OVERLAY)
    op_flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (flags & librados::OPERATION_FULL_TRY)
    op_flags |= CEPH_OSD_FLAG_FULL_TRY;

  return op_flags;
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  return io_ctx_impl->operate(obj, &o->impl->o, (ceph::real_time *)o->impl->prt);
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectReadOperation *o, bufferlist *pbl)
{
  object_t obj(oid);
  return io_ctx_impl->operate_read(obj, &o->impl->o, pbl);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  io_ctx_impl->snapc, 0);
}
int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 ObjectWriteOperation *o, int flags)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  io_ctx_impl->snapc,
				  translate_flags(flags));
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectWriteOperation *o,
				 snap_t snap_seq, std::vector<snap_t>& snaps)
{
  object_t obj(oid);
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(snap_seq, snv);
  return io_ctx_impl->aio_operate(obj, &o->impl->o, c->pc,
				  snapc, 0);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o,
				 bufferlist *pbl)
{
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
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, &o->impl->o, c->pc,
				       translate_flags(flags), pbl);
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

int librados::IoCtx::selfmanaged_snap_remove(uint64_t snapid)
{
  return io_ctx_impl->selfmanaged_snap_remove(snapid);
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

  return rados::cls::lock::lock(this, oid, name, LOCK_EXCLUSIVE, cookie, "",
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

  return rados::cls::lock::lock(this, oid, name, LOCK_SHARED, cookie, tag,
		  		description, dur, flags);
}

int librados::IoCtx::unlock(const std::string &oid, const std::string &name,
			    const std::string &cookie)
{
  return rados::cls::lock::unlock(this, oid, name, cookie);
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
    if (tmp_type == LOCK_EXCLUSIVE)
      *exclusive = 1;
    else
      *exclusive = 0;
  }

  return tmp_lockers.size();
}

librados::NObjectIterator librados::IoCtx::nobjects_begin()
{
  bufferlist bl;
  return nobjects_begin(bl);
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

librados::NObjectIterator librados::IoCtx::nobjects_begin(uint32_t pos)
{
  bufferlist bl;
  return nobjects_begin(pos, bl);
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

const librados::NObjectIterator& librados::IoCtx::nobjects_end() const
{
  return NObjectIterator::__EndObjectIterator;
}

// DEPRECATED; use n versions above
librados::ObjectIterator librados::IoCtx::objects_begin()
{
  rados_list_ctx_t listh;
  if (io_ctx_impl->oloc.nspace == librados::all_nspaces) {
    ostringstream oss;
    oss << "rados returned " << cpp_strerror(-EINVAL);
    throw std::runtime_error(oss.str());
  }
  rados_objects_list_open(io_ctx_impl, &listh);
  ObjectIterator iter((ObjListCtx*)listh);
  iter.get_next();
  return iter;
}

librados::ObjectIterator librados::IoCtx::objects_begin(uint32_t pos)
{
  rados_list_ctx_t listh;
  if (io_ctx_impl->oloc.nspace == librados::all_nspaces) {
    ostringstream oss;
    oss << "rados returned " << cpp_strerror(-EINVAL);
    throw std::runtime_error(oss.str());
  }
  rados_objects_list_open(io_ctx_impl, &listh);
  ObjectIterator iter((ObjListCtx*)listh);
  iter.seek(pos);
  return iter;
}

const librados::ObjectIterator& librados::IoCtx::objects_end() const
{
  return ObjectIterator::__EndObjectIterator;
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

int librados::IoCtx::aio_watch(const string& oid, AioCompletion *c,
                               uint64_t *cookie,
                               librados::WatchCtx2 *ctx2)
{
  object_t obj(oid);
  return io_ctx_impl->aio_watch(obj, c->pc, cookie, NULL, ctx2);
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
                                     expected_write_size);
}

void librados::IoCtx::set_assert_version(uint64_t ver)
{
  io_ctx_impl->set_assert_version(ver);
}

void librados::IoCtx::set_assert_src_version(const std::string& oid, uint64_t ver)
{
  object_t obj(oid);
  io_ctx_impl->set_assert_src_version(obj, ver);
}

int librados::IoCtx::cache_pin(const string& oid)
{
  object_t obj(oid);
  return io_ctx_impl->cache_pin(obj);
}

int librados::IoCtx::cache_unpin(const string& oid)
{
  object_t obj(oid);
  return io_ctx_impl->cache_unpin(obj);
}

void librados::IoCtx::locator_set_key(const string& key)
{
  io_ctx_impl->oloc.key = key;
}

void librados::IoCtx::set_namespace(const string& nspace)
{
  io_ctx_impl->oloc.nspace = nspace;
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
  assert(client != NULL);
  client->get();
}

librados::Rados::~Rados()
{
  shutdown();
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
  md_config_t *conf = client->cct->_conf;
  int ret = conf->get_val(option, &str, -1);
  if (ret) {
    free(str);
    return ret;
  }
  val = str;
  free(str);
  return 0;
}

int librados::Rados::pool_create(const char *name)
{
  string str(name);
  return client->pool_create(str);
}

int librados::Rados::pool_create(const char *name, uint64_t auid)
{
  string str(name);
  return client->pool_create(str, auid);
}

int librados::Rados::pool_create(const char *name, uint64_t auid, __u8 crush_rule)
{
  string str(name);
  return client->pool_create(str, auid, crush_rule);
}

int librados::Rados::pool_create_async(const char *name, PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc, auid);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, __u8 crush_rule,
				       PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc, auid, crush_rule);
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
  return client->pool_get_name(id, name);
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
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

int librados::Rados::ioctx_create2(int64_t pool_id, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create2((rados_t)client, pool_id, &p);
  if (ret)
    return ret;
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

void librados::Rados::test_blacklist_self(bool set)
{
  client->blacklist_self(set);
}

int librados::Rados::get_pool_stats(std::list<string>& v,
				    stats_map& result)
{
  map<string,::pool_stat_t> rawresult;
  int r = client->get_pool_stats(v, rawresult);
  for (map<string,::pool_stat_t>::iterator p = rawresult.begin();
       p != rawresult.end();
       ++p) {
    pool_stat_t& pv = result[p->first];
    object_stat_sum_t *sum = &p->second.stats.sum;
    pv.num_kb = SHIFT_ROUND_UP(sum->num_bytes, 10);
    pv.num_bytes = sum->num_bytes;
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

namespace {
  int decode_json(JSONObj *obj, pg_t& pg)
  {
    string pg_str;
    JSONDecoder::decode_json("pgid", pg_str, obj);
    if (pg.parse(pg_str.c_str())) {
      return 0;
    } else {
      return -EINVAL;
    }
  }

  int get_inconsistent_pgs(librados::RadosClient& client,
			   int64_t pool_id,
			   std::vector<librados::PlacementGroup>* pgs)
  {
    vector<string> cmd = {
      "{\"prefix\": \"pg ls\","
      "\"pool\": " + std::to_string(pool_id) + ","
      "\"states\": [\"inconsistent\"],"
      "\"format\": \"json\"}"
    };
    bufferlist inbl, outbl;
    string outstring;
    int ret = client.mon_command(cmd, inbl, &outbl, &outstring);
    if (ret) {
      return ret;
    }
    if (!outbl.length()) {
      // no pg returned
      return ret;
    }
    JSONParser parser;
    if (!parser.parse(outbl.c_str(), outbl.length())) {
      return -EINVAL;
    }
    if (!parser.is_array()) {
      return -EINVAL;
    }
    vector<string> v = parser.get_array_elements();
    for (auto i : v) {
      JSONParser pg_json;
      if (!pg_json.parse(i.c_str(), i.length())) {
	return -EINVAL;
      }
      librados::PlacementGroup pg;
      if (decode_json(&pg_json, pg.impl->pgid)) {
	return -EINVAL;
      }
      pgs->emplace_back(pg);
    }
    return 0;
  }
}

int librados::Rados::get_inconsistent_pgs(int64_t pool_id,
					  std::vector<PlacementGroup>* pgs)
{
  return ::get_inconsistent_pgs(*client, pool_id, pgs);
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

int librados::Rados::blacklist_add(const std::string& client_address,
				   uint32_t expire_seconds)
{
  return client->blacklist_add(client_address, expire_seconds);
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
  assert(r == 0);
  return new AioCompletion(c);
}

librados::ObjectOperation::ObjectOperation()
{
  impl = new ObjectOperationImpl;
}

librados::ObjectOperation::~ObjectOperation()
{
  delete impl;
}

///////////////////////////// C API //////////////////////////////

static CephContext *rados_create_cct(const char * const clustername,
                                     CephInitParameters *iparams)
{
  // missing things compared to global_init:
  // g_ceph_context, g_conf, g_lockdep, signal handlers
  CephContext *cct = common_preinit(*iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  if (clustername)
    cct->_conf->cluster = clustername;
  cct->_conf->parse_env(); // environment variables override
  cct->_conf->apply_changes(NULL);

  TracepointProvider::initialize<tracepoint_traits>(cct);
  return cct;
}

extern "C" int rados_create(rados_t *pcluster, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }
  CephContext *cct = rados_create_cct("", &iparams);

  tracepoint(librados, rados_create_enter, id);
  *pcluster = reinterpret_cast<rados_t>(new librados::RadosClient(cct));
  tracepoint(librados, rados_create_exit, 0, *pcluster);

  cct->put();
  return 0;
}

// as above, but
// 1) don't assume 'client.'; name is a full type.id namestr
// 2) allow setting clustername
// 3) flags is for future expansion (maybe some of the global_init()
//    behavior is appropriate for some consumers of librados, for instance)

extern "C" int rados_create2(rados_t *pcluster, const char *const clustername,
			     const char * const name, uint64_t flags)
{
  // client is assumed, but from_str will override
  int retval = 0;
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (!name || !iparams.name.from_str(name)) {
    retval = -EINVAL;
  }

  CephContext *cct = rados_create_cct(clustername, &iparams);
  tracepoint(librados, rados_create2_enter, clustername, name, flags);
  if (retval == 0) {
    *pcluster = reinterpret_cast<rados_t>(new librados::RadosClient(cct));
  }
  tracepoint(librados, rados_create2_exit, retval, *pcluster);

  cct->put();
  return retval;
}

/* This function is intended for use by Ceph daemons. These daemons have
 * already called global_init and want to use that particular configuration for
 * their cluster.
 */
extern "C" int rados_create_with_context(rados_t *pcluster, rados_config_t cct_)
{
  CephContext *cct = (CephContext *)cct_;
  TracepointProvider::initialize<tracepoint_traits>(cct);

  tracepoint(librados, rados_create_with_context_enter, cct_);
  librados::RadosClient *radosp = new librados::RadosClient(cct);
  *pcluster = (void *)radosp;
  tracepoint(librados, rados_create_with_context_exit, 0, *pcluster);
  return 0;
}

extern "C" rados_config_t rados_cct(rados_t cluster)
{
  tracepoint(librados, rados_cct_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  rados_config_t retval = (rados_config_t)client->cct;
  tracepoint(librados, rados_cct_exit, retval);
  return retval;
}

extern "C" int rados_connect(rados_t cluster)
{
  tracepoint(librados, rados_connect_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->connect();
  tracepoint(librados, rados_connect_exit, retval);
  return retval;
}

extern "C" void rados_shutdown(rados_t cluster)
{
  tracepoint(librados, rados_shutdown_enter, cluster);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  radosp->shutdown();
  delete radosp;
  tracepoint(librados, rados_shutdown_exit);
}

extern "C" uint64_t rados_get_instance_id(rados_t cluster)
{
  tracepoint(librados, rados_get_instance_id_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  uint64_t retval = client->get_instance_id();
  tracepoint(librados, rados_get_instance_id_exit, retval);
  return retval;
}

extern "C" void rados_version(int *major, int *minor, int *extra)
{
  tracepoint(librados, rados_version_enter, major, minor, extra);
  if (major)
    *major = LIBRADOS_VER_MAJOR;
  if (minor)
    *minor = LIBRADOS_VER_MINOR;
  if (extra)
    *extra = LIBRADOS_VER_EXTRA;
  tracepoint(librados, rados_version_exit, LIBRADOS_VER_MAJOR, LIBRADOS_VER_MINOR, LIBRADOS_VER_EXTRA);
}


// -- config --
extern "C" int rados_conf_read_file(rados_t cluster, const char *path_list)
{
  tracepoint(librados, rados_conf_read_file_enter, cluster, path_list);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  int ret = conf->parse_config_files(path_list, NULL, 0);
  if (ret) {
    tracepoint(librados, rados_conf_read_file_exit, ret);
    return ret;
  }
  conf->parse_env(); // environment variables override

  conf->apply_changes(NULL);
  client->cct->_conf->complain_about_parse_errors(client->cct);
  tracepoint(librados, rados_conf_read_file_exit, 0);
  return 0;
}

extern "C" int rados_conf_parse_argv(rados_t cluster, int argc, const char **argv)
{
  tracepoint(librados, rados_conf_parse_argv_enter, cluster, argc);
  int i;
  for(i = 0; i < argc; i++) {
    tracepoint(librados, rados_conf_parse_argv_arg, argv[i]);
  }
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  int ret = conf->parse_argv(args);
  if (ret) {
    tracepoint(librados, rados_conf_parse_argv_exit, ret);
    return ret;
  }
  conf->apply_changes(NULL);
  tracepoint(librados, rados_conf_parse_argv_exit, 0);
  return 0;
}

// like above, but return the remainder of argv to contain remaining
// unparsed args.  Must be allocated to at least argc by caller.
// remargv will contain n <= argc pointers to original argv[], the end
// of which may be NULL

extern "C" int rados_conf_parse_argv_remainder(rados_t cluster, int argc,
					       const char **argv,
					       const char **remargv)
{
  tracepoint(librados, rados_conf_parse_argv_remainder_enter, cluster, argc);
  unsigned int i;
  for(i = 0; i < (unsigned int) argc; i++) {
    tracepoint(librados, rados_conf_parse_argv_remainder_arg, argv[i]);
  }
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  for (int i=0; i<argc; i++)
    args.push_back(argv[i]);
  int ret = conf->parse_argv(args);
  if (ret) {
    tracepoint(librados, rados_conf_parse_argv_remainder_exit, ret);
    return ret;
  }
  conf->apply_changes(NULL);
  assert(args.size() <= (unsigned int)argc);
  for (i = 0; i < (unsigned int)argc; ++i) {
    if (i < args.size())
      remargv[i] = args[i];
    else
      remargv[i] = (const char *)NULL;
    tracepoint(librados, rados_conf_parse_argv_remainder_remarg, remargv[i]);
  }
  tracepoint(librados, rados_conf_parse_argv_remainder_exit, 0);
  return 0;
}

extern "C" int rados_conf_parse_env(rados_t cluster, const char *env)
{
  tracepoint(librados, rados_conf_parse_env_enter, cluster, env);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  env_to_vec(args, env);
  int ret = conf->parse_argv(args);
  if (ret) {
    tracepoint(librados, rados_conf_parse_env_exit, ret);
    return ret;
  }
  conf->apply_changes(NULL);
  tracepoint(librados, rados_conf_parse_env_exit, 0);
  return 0;
}

extern "C" int rados_conf_set(rados_t cluster, const char *option, const char *value)
{
  tracepoint(librados, rados_conf_set_enter, cluster, option, value);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  int ret = conf->set_val(option, value);
  if (ret) {
    tracepoint(librados, rados_conf_set_exit, ret);
    return ret;
  }
  conf->apply_changes(NULL);
  tracepoint(librados, rados_conf_set_exit, 0);
  return 0;
}

/* cluster info */
extern "C" int rados_cluster_stat(rados_t cluster, rados_cluster_stat_t *result)
{
  tracepoint(librados, rados_cluster_stat_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;

  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result->kb = stats.kb;
  result->kb_used = stats.kb_used;
  result->kb_avail = stats.kb_avail;
  result->num_objects = stats.num_objects;
  tracepoint(librados, rados_cluster_stat_exit, r, result->kb, result->kb_used, result->kb_avail, result->num_objects);
  return r;
}

extern "C" int rados_conf_get(rados_t cluster, const char *option, char *buf, size_t len)
{
  tracepoint(librados, rados_conf_get_enter, cluster, option, len);
  char *tmp = buf;
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  int retval = conf->get_val(option, &tmp, len);
  tracepoint(librados, rados_conf_get_exit, retval, retval ? "" : option);
  return retval;
}

extern "C" int64_t rados_pool_lookup(rados_t cluster, const char *name)
{
  tracepoint(librados, rados_pool_lookup_enter, cluster, name);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  int64_t retval = radosp->lookup_pool(name);
  tracepoint(librados, rados_pool_lookup_exit, retval);
  return retval;
}

extern "C" int rados_pool_reverse_lookup(rados_t cluster, int64_t id,
					 char *buf, size_t maxlen)
{
  tracepoint(librados, rados_pool_reverse_lookup_enter, cluster, id, maxlen);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string name;
  int r = radosp->pool_get_name(id, &name);
  if (r < 0) {
    tracepoint(librados, rados_pool_reverse_lookup_exit, r, "");
    return r;
  }
  if (name.length() >= maxlen) {
    tracepoint(librados, rados_pool_reverse_lookup_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(buf, name.c_str());
  int retval = name.length();
  tracepoint(librados, rados_pool_reverse_lookup_exit, retval, buf);
  return retval;
}

extern "C" int rados_cluster_fsid(rados_t cluster, char *buf,
				  size_t maxlen)
{
  tracepoint(librados, rados_cluster_fsid_enter, cluster, maxlen);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string fsid;
  radosp->get_fsid(&fsid);
  if (fsid.length() >= maxlen) {
    tracepoint(librados, rados_cluster_fsid_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(buf, fsid.c_str());
  int retval = fsid.length();
  tracepoint(librados, rados_cluster_fsid_exit, retval, buf);
  return retval;
}

extern "C" int rados_wait_for_latest_osdmap(rados_t cluster)
{
  tracepoint(librados, rados_wait_for_latest_osdmap_enter, cluster);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  int retval = radosp->wait_for_latest_osdmap();
  tracepoint(librados, rados_wait_for_latest_osdmap_exit, retval);
  return retval;
}

extern "C" int rados_blacklist_add(rados_t cluster, char *client_address,
				   uint32_t expire_seconds)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  return radosp->blacklist_add(client_address, expire_seconds);
}

extern "C" int rados_pool_list(rados_t cluster, char *buf, size_t len)
{
  tracepoint(librados, rados_pool_list_enter, cluster, len);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  std::list<std::pair<int64_t, std::string> > pools;
  int r = client->pool_list(pools);
  if (r < 0) {
    tracepoint(librados, rados_pool_list_exit, r);
    return r;
  }

  if (len > 0 && !buf) {
    tracepoint(librados, rados_pool_list_exit, -EINVAL);
    return -EINVAL;
  }

  char *b = buf;
  if (b)
    memset(b, 0, len);
  int needed = 0;
  std::list<std::pair<int64_t, std::string> >::const_iterator i = pools.begin();
  std::list<std::pair<int64_t, std::string> >::const_iterator p_end =
    pools.end();
  for (; i != p_end; ++i) {
    int rl = i->second.length() + 1;
    if (len < (unsigned)rl)
      break;
    const char* pool = i->second.c_str();
    tracepoint(librados, rados_pool_list_pool, pool);
    strncat(b, pool, rl);
    needed += rl;
    len -= rl;
    b += rl;
  }
  for (; i != p_end; ++i) {
    int rl = i->second.length() + 1;
    needed += rl;
  }
  int retval = needed + 1;
  tracepoint(librados, rados_pool_list_exit, retval);
  return retval;
}

CEPH_RADOS_API int rados_inconsistent_pg_list(rados_t cluster, int64_t pool_id,
					      char *buf, size_t len)
{
  tracepoint(librados, rados_inconsistent_pg_list_enter, cluster, pool_id, len);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  std::vector<librados::PlacementGroup> pgs;
  int r = ::get_inconsistent_pgs(*client, pool_id, &pgs);
  if (r < 0) {
    tracepoint(librados, rados_inconsistent_pg_list_exit, r);
    return r;
  }

  if (len > 0 && !buf) {
    tracepoint(librados, rados_inconsistent_pg_list_exit, -EINVAL);
    return -EINVAL;
  }

  char *b = buf;
  if (b)
    memset(b, 0, len);
  int needed = 0;
  for (const auto pg : pgs) {
    std::ostringstream ss;
    ss << pg;
    auto s = ss.str();
    unsigned rl = s.length() + 1;
    if (len >= rl) {
      tracepoint(librados, rados_inconsistent_pg_list_pg, s.c_str());
      strncat(b, s.c_str(), rl);
      b += rl;
      len -= rl;
    }
    needed += rl;
  }
  int retval = needed + 1;
  tracepoint(librados, rados_inconsistent_pg_list_exit, retval);
  return retval;
}

static void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

static void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

extern "C" int rados_ping_monitor(rados_t cluster, const char *mon_id,
                                  char **outstr, size_t *outstrlen)
{
  tracepoint(librados, rados_ping_monitor_enter, cluster, mon_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  string str;

  if (!mon_id) {
    tracepoint(librados, rados_ping_monitor_exit, -EINVAL, NULL, NULL);
    return -EINVAL;
  }

  int ret = client->ping_monitor(mon_id, &str);
  if (ret == 0) {
    do_out_buffer(str, outstr, outstrlen);
  }
  tracepoint(librados, rados_ping_monitor_exit, ret, ret < 0 ? NULL : outstr, ret < 0 ? NULL : outstrlen);
  return ret;
}

extern "C" int rados_mon_command(rados_t cluster, const char **cmd,
				 size_t cmdlen,
				 const char *inbuf, size_t inbuflen,
				 char **outbuf, size_t *outbuflen,
				 char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mon_command_enter, cluster, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mon_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->mon_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mon_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}

extern "C" int rados_mon_command_target(rados_t cluster, const char *name,
					const char **cmd,
					size_t cmdlen,
					const char *inbuf, size_t inbuflen,
					char **outbuf, size_t *outbuflen,
					char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mon_command_target_enter, cluster, name, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  // is this a numeric id?
  char *endptr;
  errno = 0;
  long rank = strtol(name, &endptr, 10);
  if ((errno == ERANGE && (rank == LONG_MAX || rank == LONG_MIN)) ||
      (errno != 0 && rank == 0) ||
      endptr == name ||    // no digits
      *endptr != '\0') {   // extra characters
    rank = -1;
  }

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mon_command_target_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret;
  if (rank >= 0)
    ret = client->mon_command(rank, cmdvec, inbl, &outbl, &outstring);
  else
    ret = client->mon_command(name, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mon_command_target_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}

extern "C" int rados_osd_command(rados_t cluster, int osdid, const char **cmd,
				 size_t cmdlen,
				 const char *inbuf, size_t inbuflen,
				 char **outbuf, size_t *outbuflen,
				 char **outs, size_t *outslen)
{
  tracepoint(librados, rados_osd_command_enter, cluster, osdid, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_osd_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->osd_command(osdid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_osd_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}



extern "C" int rados_pg_command(rados_t cluster, const char *pgstr,
				const char **cmd, size_t cmdlen,
				const char *inbuf, size_t inbuflen,
				char **outbuf, size_t *outbuflen,
				char **outs, size_t *outslen)
{
  tracepoint(librados, rados_pg_command_enter, cluster, pgstr, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  pg_t pgid;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_pg_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  if (!pgid.parse(pgstr))
    return -EINVAL;

  int ret = client->pg_command(pgid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_pg_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}

extern "C" void rados_buffer_free(char *buf)
{
  tracepoint(librados, rados_buffer_free_enter, buf);
  if (buf)
    free(buf);
  tracepoint(librados, rados_buffer_free_exit);
}

extern "C" int rados_monitor_log(rados_t cluster, const char *level, rados_log_callback_t cb, void *arg)
{
  tracepoint(librados, rados_monitor_log_enter, cluster, level, cb, arg);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->monitor_log(level, cb, arg);
  tracepoint(librados, rados_monitor_log_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_create(rados_t cluster, const char *name, rados_ioctx_t *io)
{
  tracepoint(librados, rados_ioctx_create_enter, cluster, name);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::IoCtxImpl *ctx;

  int r = client->create_ioctx(name, &ctx);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_create_exit, r, NULL);
    return r;
  }

  *io = ctx;
  ctx->get();
  tracepoint(librados, rados_ioctx_create_exit, 0, ctx);
  return 0;
}

extern "C" int rados_ioctx_create2(rados_t cluster, int64_t pool_id,
                                   rados_ioctx_t *io)
{
  tracepoint(librados, rados_ioctx_create2_enter, cluster, pool_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::IoCtxImpl *ctx;

  int r = client->create_ioctx(pool_id, &ctx);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_create2_exit, r, NULL);
    return r;
  }

  *io = ctx;
  ctx->get();
  tracepoint(librados, rados_ioctx_create2_exit, 0, ctx);
  return 0;
}

extern "C" void rados_ioctx_destroy(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_destroy_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->put();
  tracepoint(librados, rados_ioctx_destroy_exit);
}

extern "C" int rados_ioctx_pool_stat(rados_ioctx_t io, struct rados_pool_stat_t *stats)
{
  tracepoint(librados, rados_ioctx_pool_stat_enter, io);
  librados::IoCtxImpl *io_ctx_impl = (librados::IoCtxImpl *)io;
  list<string> ls;
  std::string pool_name;

  int err = io_ctx_impl->client->pool_get_name(io_ctx_impl->get_id(), &pool_name);
  if (err) {
    tracepoint(librados, rados_ioctx_pool_stat_exit, err, stats);
    return err;
  }
  ls.push_back(pool_name);

  map<string, ::pool_stat_t> rawresult;
  err = io_ctx_impl->client->get_pool_stats(ls, rawresult);
  if (err) {
    tracepoint(librados, rados_ioctx_pool_stat_exit, err, stats);
    return err;
  }

  ::pool_stat_t& r = rawresult[pool_name];
  stats->num_kb = SHIFT_ROUND_UP(r.stats.sum.num_bytes, 10);
  stats->num_bytes = r.stats.sum.num_bytes;
  stats->num_objects = r.stats.sum.num_objects;
  stats->num_object_clones = r.stats.sum.num_object_clones;
  stats->num_object_copies = r.stats.sum.num_object_copies;
  stats->num_objects_missing_on_primary = r.stats.sum.num_objects_missing_on_primary;
  stats->num_objects_unfound = r.stats.sum.num_objects_unfound;
  stats->num_objects_degraded =
    r.stats.sum.num_objects_degraded +
    r.stats.sum.num_objects_misplaced; // FIXME: this is imprecise
  stats->num_rd = r.stats.sum.num_rd;
  stats->num_rd_kb = r.stats.sum.num_rd_kb;
  stats->num_wr = r.stats.sum.num_wr;
  stats->num_wr_kb = r.stats.sum.num_wr_kb;
  tracepoint(librados, rados_ioctx_pool_stat_exit, 0, stats);
  return 0;
}

extern "C" rados_config_t rados_ioctx_cct(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_cct_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  rados_config_t retval = (rados_config_t)ctx->client->cct;
  tracepoint(librados, rados_ioctx_cct_exit, retval);
  return retval;
}

extern "C" void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t seq)
{
  tracepoint(librados, rados_ioctx_snap_set_read_enter, io, seq);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->set_snap_read((snapid_t)seq);
  tracepoint(librados, rados_ioctx_snap_set_read_exit);
}

extern "C" int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io,
	    rados_snap_t seq, rados_snap_t *snaps, int num_snaps)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_set_write_ctx_enter, io, seq, snaps, num_snaps);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  vector<snapid_t> snv;
  snv.resize(num_snaps);
  for (int i=0; i<num_snaps; i++) {
    snv[i] = (snapid_t)snaps[i];
  }
  int retval = ctx->set_snap_write_context((snapid_t)seq, snv);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_set_write_ctx_exit, retval);
  return retval;
}

extern "C" int rados_write(rados_ioctx_t io, const char *o, const char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_write_enter, io, o, buf, len, off);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->write(oid, bl, len, off);
  tracepoint(librados, rados_write_exit, retval);
  return retval;
}

extern "C" int rados_append(rados_ioctx_t io, const char *o, const char *buf, size_t len)
{
  tracepoint(librados, rados_append_enter, io, o, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->append(oid, bl, len);
  tracepoint(librados, rados_append_exit, retval);
  return retval;
}

extern "C" int rados_write_full(rados_ioctx_t io, const char *o, const char *buf, size_t len)
{
  tracepoint(librados, rados_write_full_enter, io, o, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->write_full(oid, bl);
  tracepoint(librados, rados_write_full_exit, retval);
  return retval;
}

extern "C" int rados_writesame(rados_ioctx_t io,
				const char *o,
				const char *buf,
				size_t data_len,
				size_t write_len,
				uint64_t off)
{
  tracepoint(librados, rados_writesame_enter, io, o, buf, data_len, write_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, data_len);
  int retval = ctx->writesame(oid, bl, write_len, off);
  tracepoint(librados, rados_writesame_exit, retval);
  return retval;
}

extern "C" int rados_clone_range(rados_ioctx_t io, const char *dst, uint64_t dst_off,
                                 const char *src, uint64_t src_off, size_t len)
{
  tracepoint(librados, rados_clone_range_enter, io, dst, dst_off, src, src_off, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t dst_oid(dst), src_oid(src);
  int retval = ctx->clone_range(dst_oid, dst_off, src_oid, src_off, len);
  tracepoint(librados, rados_clone_range_exit, retval);
  return retval;
}

extern "C" int rados_trunc(rados_ioctx_t io, const char *o, uint64_t size)
{
  tracepoint(librados, rados_trunc_enter, io, o, size);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->trunc(oid, size);
  tracepoint(librados, rados_trunc_exit, retval);
  return retval;
}

extern "C" int rados_remove(rados_ioctx_t io, const char *o)
{
  tracepoint(librados, rados_remove_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->remove(oid);
  tracepoint(librados, rados_remove_exit, retval);
  return retval;
}

extern "C" int rados_read(rados_ioctx_t io, const char *o, char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_read_enter, io, o, buf, len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);

  bufferlist bl;
  bufferptr bp = buffer::create_static(len, buf);
  bl.push_back(bp);

  ret = ctx->read(oid, bl, len, off);
  if (ret >= 0) {
    if (bl.length() > len) {
      tracepoint(librados, rados_read_exit, -ERANGE, NULL);
      return -ERANGE;
    }
    if (!bl.is_provided_buffer(buf))
      bl.copy(0, bl.length(), buf);
    ret = bl.length();    // hrm :/
  }

  tracepoint(librados, rados_read_exit, ret, buf);
  return ret;
}

extern "C" uint64_t rados_get_last_version(rados_ioctx_t io)
{
  tracepoint(librados, rados_get_last_version_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  uint64_t retval = ctx->last_version();
  tracepoint(librados, rados_get_last_version_exit, retval);
  return retval;
}

extern "C" int rados_pool_create(rados_t cluster, const char *name)
{
  tracepoint(librados, rados_pool_create_enter, cluster, name);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname);
  tracepoint(librados, rados_pool_create_exit, retval);
  return retval;
}

extern "C" int rados_pool_create_with_auid(rados_t cluster, const char *name,
					   uint64_t auid)
{
  tracepoint(librados, rados_pool_create_with_auid_enter, cluster, name, auid);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname, auid);
  tracepoint(librados, rados_pool_create_with_auid_exit, retval);
  return retval;
}

extern "C" int rados_pool_create_with_crush_rule(rados_t cluster, const char *name,
						 __u8 crush_rule_num)
{
  tracepoint(librados, rados_pool_create_with_crush_rule_enter, cluster, name, crush_rule_num);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname, 0, crush_rule_num);
  tracepoint(librados, rados_pool_create_with_crush_rule_exit, retval);
  return retval;
}

extern "C" int rados_pool_create_with_all(rados_t cluster, const char *name,
					  uint64_t auid, __u8 crush_rule_num)
{
  tracepoint(librados, rados_pool_create_with_all_enter, cluster, name, auid, crush_rule_num);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname, auid, crush_rule_num);
  tracepoint(librados, rados_pool_create_with_all_exit, retval);
  return retval;
}

extern "C" int rados_pool_get_base_tier(rados_t cluster, int64_t pool_id, int64_t* base_tier)
{
  tracepoint(librados, rados_pool_get_base_tier_enter, cluster, pool_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->pool_get_base_tier(pool_id, base_tier);
  tracepoint(librados, rados_pool_get_base_tier_exit, retval, *base_tier);
  return retval;
}

extern "C" int rados_pool_delete(rados_t cluster, const char *pool_name)
{
  tracepoint(librados, rados_pool_delete_enter, cluster, pool_name);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->pool_delete(pool_name);
  tracepoint(librados, rados_pool_delete_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid)
{
  tracepoint(librados, rados_ioctx_pool_set_auid_enter, io, auid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->pool_change_auid(auid);
  tracepoint(librados, rados_ioctx_pool_set_auid_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_pool_get_auid(rados_ioctx_t io, uint64_t *auid)
{
  tracepoint(librados, rados_ioctx_pool_get_auid_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->client->pool_get_auid(ctx->get_id(), (unsigned long long *)auid);
  tracepoint(librados, rados_ioctx_pool_get_auid_exit, retval, *auid);
  return retval;
}

extern "C" int rados_ioctx_pool_requires_alignment(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_pool_requires_alignment_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->client->pool_requires_alignment(ctx->get_id());
  tracepoint(librados, rados_ioctx_pool_requires_alignment_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_pool_requires_alignment2(rados_ioctx_t io,
	int *requires)
{
  tracepoint(librados, rados_ioctx_pool_requires_alignment_enter2, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  bool requires_alignment;
  int retval = ctx->client->pool_requires_alignment2(ctx->get_id(), 
  	&requires_alignment);
  tracepoint(librados, rados_ioctx_pool_requires_alignment_exit2, retval, 
  	requires_alignment);
  if (requires)
    *requires = requires_alignment;
  return retval;
}

extern "C" uint64_t rados_ioctx_pool_required_alignment(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_pool_required_alignment_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  uint64_t retval = ctx->client->pool_required_alignment(ctx->get_id());
  tracepoint(librados, rados_ioctx_pool_required_alignment_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_pool_required_alignment2(rados_ioctx_t io,
	uint64_t *alignment)
{
  tracepoint(librados, rados_ioctx_pool_required_alignment_enter2, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->client->pool_required_alignment2(ctx->get_id(),
  	alignment);
  tracepoint(librados, rados_ioctx_pool_required_alignment_exit2, retval, 
  	*alignment);
  return retval;
}

extern "C" void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key)
{
  tracepoint(librados, rados_ioctx_locator_set_key_enter, io, key);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (key)
    ctx->oloc.key = key;
  else
    ctx->oloc.key = "";
  tracepoint(librados, rados_ioctx_locator_set_key_exit);
}

extern "C" void rados_ioctx_set_namespace(rados_ioctx_t io, const char *nspace)
{
  tracepoint(librados, rados_ioctx_set_namespace_enter, io, nspace);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (nspace)
    ctx->oloc.nspace = nspace;
  else
    ctx->oloc.nspace = "";
  tracepoint(librados, rados_ioctx_set_namespace_exit);
}

extern "C" rados_t rados_ioctx_get_cluster(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_get_cluster_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  rados_t retval = (rados_t)ctx->client;
  tracepoint(librados, rados_ioctx_get_cluster_exit, retval);
  return retval;
}

extern "C" int64_t rados_ioctx_get_id(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_get_id_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int64_t retval = ctx->get_id();
  tracepoint(librados, rados_ioctx_get_id_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_get_pool_name(rados_ioctx_t io, char *s, unsigned maxlen)
{
  tracepoint(librados, rados_ioctx_get_pool_name_enter, io, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::string pool_name;

  int err = ctx->client->pool_get_name(ctx->get_id(), &pool_name);
  if (err) {
    tracepoint(librados, rados_ioctx_get_pool_name_exit, err, "");
    return err;
  }
  if (pool_name.length() >= maxlen) {
    tracepoint(librados, rados_ioctx_get_pool_name_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(s, pool_name.c_str());
  int retval = pool_name.length();
  tracepoint(librados, rados_ioctx_get_pool_name_exit, retval, s);
  return retval;
}

// snaps

extern "C" int rados_ioctx_snap_create(rados_ioctx_t io, const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_create_enter, io, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_create(snapname);
  tracepoint(librados, rados_ioctx_snap_create_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_snap_remove(rados_ioctx_t io, const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_remove_enter, io, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_remove(snapname);
  tracepoint(librados, rados_ioctx_snap_remove_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_snap_rollback(rados_ioctx_t io, const char *oid,
			      const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_rollback_enter, io, oid, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->rollback(oid, snapname);
  tracepoint(librados, rados_ioctx_snap_rollback_exit, retval);
  return retval;
}

// Deprecated name kept for backward compatibility
extern "C" int rados_rollback(rados_ioctx_t io, const char *oid,
			      const char *snapname)
{
  return rados_ioctx_snap_rollback(io, oid, snapname);
}

extern "C" int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io,
					     uint64_t *snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_create(snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_exit, retval, *snapid);
  return retval;
}

extern "C" int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io,
					     uint64_t snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_enter, io, snapid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_remove(snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io,
						     const char *oid,
						     uint64_t snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_rollback_enter, io, oid, snapid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_rollback_object(oid, ctx->snapc, snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_rollback_exit, retval);
  return retval;
}

extern "C" int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t *snaps,
				    int maxlen)
{
  tracepoint(librados, rados_ioctx_snap_list_enter, io, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  vector<uint64_t> snapvec;
  int r = ctx->snap_list(&snapvec);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_snap_list_exit, r, snaps, 0);
    return r;
  }
  if ((int)snapvec.size() <= maxlen) {
    for (unsigned i=0; i<snapvec.size(); i++) {
      snaps[i] = snapvec[i];
    }
    int retval = snapvec.size();
    tracepoint(librados, rados_ioctx_snap_list_exit, retval, snaps, retval);
    return retval;
  }
  int retval = -ERANGE;
  tracepoint(librados, rados_ioctx_snap_list_exit, retval, snaps, 0);
  return retval;
}

extern "C" int rados_ioctx_snap_lookup(rados_ioctx_t io, const char *name,
				      rados_snap_t *id)
{
  tracepoint(librados, rados_ioctx_snap_lookup_enter, io, name);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_lookup(name, (uint64_t *)id);
  tracepoint(librados, rados_ioctx_snap_lookup_exit, retval, *id);
  return retval;
}

extern "C" int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id,
					char *name, int maxlen)
{
  tracepoint(librados, rados_ioctx_snap_get_name_enter, io, id, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::string sname;
  int r = ctx->snap_get_name(id, &sname);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_snap_get_name_exit, r, "");
    return r;
  }
  if ((int)sname.length() >= maxlen) {
    int retval = -ERANGE;
    tracepoint(librados, rados_ioctx_snap_get_name_exit, retval, "");
    return retval;
  }
  strncpy(name, sname.c_str(), maxlen);
  tracepoint(librados, rados_ioctx_snap_get_name_exit, 0, name);
  return 0;
}

extern "C" int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t *t)
{
  tracepoint(librados, rados_ioctx_snap_get_stamp_enter, io, id);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_get_stamp(id, t);
  tracepoint(librados, rados_ioctx_snap_get_stamp_exit, retval, *t);
  return retval;
}

extern "C" int rados_getxattr(rados_ioctx_t io, const char *o, const char *name,
			      char *buf, size_t len)
{
  tracepoint(librados, rados_getxattr_enter, io, o, name, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);
  bufferlist bl;
  bl.push_back(buffer::create_static(len, buf));
  ret = ctx->getxattr(oid, name, bl);
  if (ret >= 0) {
    if (bl.length() > len) {
      tracepoint(librados, rados_getxattr_exit, -ERANGE, buf, 0);
      return -ERANGE;
    }
    if (!bl.is_provided_buffer(buf))
      bl.copy(0, bl.length(), buf);
    ret = bl.length();
  }

  tracepoint(librados, rados_getxattr_exit, ret, buf, ret);
  return ret;
}

extern "C" int rados_getxattrs(rados_ioctx_t io, const char *oid,
			       rados_xattrs_iter_t *iter)
{
  tracepoint(librados, rados_getxattrs_enter, io, oid);
  librados::RadosXattrsIter *it = new librados::RadosXattrsIter();
  if (!it) {
    tracepoint(librados, rados_getxattrs_exit, -ENOMEM, NULL);
    return -ENOMEM;
  }
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t obj(oid);
  int ret = ctx->getxattrs(obj, it->attrset);
  if (ret) {
    delete it;
    tracepoint(librados, rados_getxattrs_exit, ret, NULL);
    return ret;
  }
  it->i = it->attrset.begin();

  librados::RadosXattrsIter **iret = (librados::RadosXattrsIter**)iter;
  *iret = it;
  *iter = it;
  tracepoint(librados, rados_getxattrs_exit, 0, *iter);
  return 0;
}

extern "C" int rados_getxattrs_next(rados_xattrs_iter_t iter,
				    const char **name, const char **val, size_t *len)
{
  tracepoint(librados, rados_getxattrs_next_enter, iter);
  librados::RadosXattrsIter *it = static_cast<librados::RadosXattrsIter*>(iter);
  if (it->i == it->attrset.end()) {
    *name = NULL;
    *val = NULL;
    *len = 0;
    tracepoint(librados, rados_getxattrs_next_exit, 0, NULL, NULL, 0);
    return 0;
  }
  free(it->val);
  const std::string &s(it->i->first);
  *name = s.c_str();
  bufferlist &bl(it->i->second);
  size_t bl_len = bl.length();
  if (!bl_len) {
    // malloc(0) is not guaranteed to return a valid pointer
    *val = (char *)NULL;
  } else {
    it->val = (char*)malloc(bl_len);
    if (!it->val) {
      tracepoint(librados, rados_getxattrs_next_exit, -ENOMEM, *name, NULL, 0);
      return -ENOMEM;
    }
    memcpy(it->val, bl.c_str(), bl_len);
    *val = it->val;
  }
  *len = bl_len;
  ++it->i;
  tracepoint(librados, rados_getxattrs_next_exit, 0, *name, *val, *len);
  return 0;
}

extern "C" void rados_getxattrs_end(rados_xattrs_iter_t iter)
{
  tracepoint(librados, rados_getxattrs_end_enter, iter);
  librados::RadosXattrsIter *it = static_cast<librados::RadosXattrsIter*>(iter);
  delete it;
  tracepoint(librados, rados_getxattrs_end_exit);
}

extern "C" int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len)
{
  tracepoint(librados, rados_setxattr_enter, io, o, name, buf, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->setxattr(oid, name, bl);
  tracepoint(librados, rados_setxattr_exit, retval);
  return retval;
}

extern "C" int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name)
{
  tracepoint(librados, rados_rmxattr_enter, io, o, name);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->rmxattr(oid, name);
  tracepoint(librados, rados_rmxattr_exit, retval);
  return retval;
}

extern "C" int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime)
{
  tracepoint(librados, rados_stat_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->stat(oid, psize, pmtime);
  tracepoint(librados, rados_stat_exit, retval, psize, pmtime);
  return retval;
}

extern "C" int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen)
{
  tracepoint(librados, rados_tmap_update_enter, io, o, cmdbuf, cmdbuflen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist cmdbl;
  cmdbl.append(cmdbuf, cmdbuflen);
  int retval = ctx->tmap_update(oid, cmdbl);
  tracepoint(librados, rados_tmap_update_exit, retval);
  return retval;
}

extern "C" int rados_tmap_put(rados_ioctx_t io, const char *o, const char *buf, size_t buflen)
{
  tracepoint(librados, rados_tmap_put_enter, io, o, buf, buflen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, buflen);
  int retval = ctx->tmap_put(oid, bl);
  tracepoint(librados, rados_tmap_put_exit, retval);
  return retval;
}

extern "C" int rados_tmap_get(rados_ioctx_t io, const char *o, char *buf, size_t buflen)
{
  tracepoint(librados, rados_tmap_get_enter, io, o, buflen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  int r = ctx->tmap_get(oid, bl);
  if (r < 0) {
    tracepoint(librados, rados_tmap_get_exit, r, buf, 0);
    return r;
  }
  if (bl.length() > buflen) {
    tracepoint(librados, rados_tmap_get_exit, -ERANGE, buf, 0);
    return -ERANGE;
  }
  bl.copy(0, bl.length(), buf);
  int retval = bl.length();
  tracepoint(librados, rados_tmap_get_exit, retval, buf, retval);
  return retval;
}

extern "C" int rados_tmap_to_omap(rados_ioctx_t io, const char *o, bool nullok)
{
  tracepoint(librados, rados_tmap_to_omap_enter, io, o, nullok);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->tmap_to_omap(oid, nullok);
  tracepoint(librados, rados_tmap_to_omap_exit, retval);
  return retval;
}

extern "C" int rados_exec(rados_ioctx_t io, const char *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  tracepoint(librados, rados_exec_enter, io, o, cls, method, inbuf, in_len, out_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = ctx->exec(oid, cls, method, inbl, outbl);
  if (ret >= 0) {
    if (outbl.length()) {
      if (outbl.length() > out_len) {
	tracepoint(librados, rados_exec_exit, -ERANGE, buf, 0);
	return -ERANGE;
      }
      outbl.copy(0, outbl.length(), buf);
      ret = outbl.length();   // hrm :/
    }
  }
  tracepoint(librados, rados_exec_exit, ret, buf, ret);
  return ret;
}

extern "C" rados_object_list_cursor rados_object_list_begin(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  hobject_t *result = new hobject_t(ctx->objecter->enumerate_objects_begin());
  return (rados_object_list_cursor)result;
}

extern "C" rados_object_list_cursor rados_object_list_end(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  hobject_t *result = new hobject_t(ctx->objecter->enumerate_objects_end());
  return (rados_object_list_cursor)result;
}

extern "C" int rados_object_list_is_end(
    rados_ioctx_t io, rados_object_list_cursor cur)
{
  hobject_t *hobj = (hobject_t*)cur;
  return hobj->is_max();
}

extern "C" void rados_object_list_cursor_free(
    rados_ioctx_t io, rados_object_list_cursor cur)
{
  hobject_t *hobj = (hobject_t*)cur;
  delete hobj;
}

extern "C" int rados_object_list_cursor_cmp(
    rados_ioctx_t io,
    rados_object_list_cursor lhs_cur,
    rados_object_list_cursor rhs_cur)
{
  hobject_t *lhs = (hobject_t*)lhs_cur;
  hobject_t *rhs = (hobject_t*)rhs_cur;
  return cmp_bitwise(*lhs, *rhs);
}

extern "C" int rados_object_list(rados_ioctx_t io,
    const rados_object_list_cursor start,
    const rados_object_list_cursor finish,
    const size_t result_item_count,
    const char *filter_buf,
    const size_t filter_buf_len,
    rados_object_list_item *result_items,
    rados_object_list_cursor *next)
{
  assert(next);

  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  // Zero out items so that they will be safe to free later
  memset(result_items, 0, sizeof(rados_object_list_item) * result_item_count);

  std::list<librados::ListObjectImpl> result;
  hobject_t next_hash;

  bufferlist filter_bl;
  if (filter_buf != nullptr) {
    filter_bl.append(filter_buf, filter_buf_len);
  }

  C_SaferCond cond;
  ctx->objecter->enumerate_objects(
      ctx->poolid,
      ctx->oloc.nspace,
      *((hobject_t*)start),
      *((hobject_t*)finish),
      result_item_count,
      filter_bl,
      &result,
      &next_hash,
      &cond);

  hobject_t *next_hobj = (hobject_t*)(*next);
  assert(next_hobj);

  int r = cond.wait();
  if (r < 0) {
    *next_hobj = hobject_t::get_max();
    return r;
  }

  assert(result.size() <= result_item_count);  // Don't overflow!

  int k = 0;
  for (std::list<librados::ListObjectImpl>::iterator i = result.begin();
       i != result.end(); ++i) {
    rados_object_list_item &item = result_items[k++];
    do_out_buffer(i->oid, &item.oid, &item.oid_length);
    do_out_buffer(i->nspace, &item.nspace, &item.nspace_length);
    do_out_buffer(i->locator, &item.locator, &item.locator_length);
  }

  *next_hobj = next_hash;

  return result.size();
}

extern "C" void rados_object_list_free(
    const size_t result_size,
    rados_object_list_item *results)
{
  assert(results);

  for (unsigned int i = 0; i < result_size; ++i) {
    rados_buffer_free(results[i].oid);
    rados_buffer_free(results[i].locator);
    rados_buffer_free(results[i].nspace);
  }
}

/* list objects */

extern "C" int rados_nobjects_list_open(rados_ioctx_t io, rados_list_ctx_t *listh)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  // Let's do it the old way for backward compatbility if not using ANY_NSPACES
  if (ctx->oloc.nspace != librados::all_nspaces)
    return rados_objects_list_open(io, listh);

  tracepoint(librados, rados_nobjects_list_open_enter, io);

  Objecter::NListContext *h = new Objecter::NListContext;
  h->pool_id = ctx->poolid;
  h->pool_snap_seq = ctx->snap_seq;
  h->nspace = ctx->oloc.nspace;	// After dropping compatibility need nspace
  *listh = (void *)new librados::ObjListCtx(ctx, h);
  tracepoint(librados, rados_nobjects_list_open_exit, 0, *listh);
  return 0;
}

extern "C" void rados_nobjects_list_close(rados_list_ctx_t h)
{
  tracepoint(librados, rados_nobjects_list_close_enter, h);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)h;
  delete lh;
  tracepoint(librados, rados_nobjects_list_close_exit);
}

extern "C" uint32_t rados_nobjects_list_seek(rados_list_ctx_t listctx,
					    uint32_t pos)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;

  // Let's do it the old way for backward compatbility if not using ANY_NSPACES
  if (!lh->new_request)
    return rados_objects_list_seek(listctx, pos);

  tracepoint(librados, rados_nobjects_list_seek_enter, listctx, pos);
  uint32_t r = lh->ctx->nlist_seek(lh->nlc, pos);
  tracepoint(librados, rados_nobjects_list_seek_exit, r);
  return r;
}

extern "C" uint32_t rados_nobjects_list_get_pg_hash_position(
  rados_list_ctx_t listctx)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  if (!lh->new_request)
    return rados_objects_list_get_pg_hash_position(listctx);

  tracepoint(librados, rados_nobjects_list_get_pg_hash_position_enter, listctx);
  uint32_t retval = lh->nlc->get_pg_hash_position();
  tracepoint(librados, rados_nobjects_list_get_pg_hash_position_exit, retval);
  return retval;
}

// Deprecated, but using it for compatibility with older OSDs
extern "C" int rados_objects_list_open(rados_ioctx_t io, rados_list_ctx_t *listh)
{
  tracepoint(librados, rados_objects_list_open_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (ctx->oloc.nspace == librados::all_nspaces)
    return -EINVAL;
  Objecter::ListContext *h = new Objecter::ListContext;
  h->pool_id = ctx->poolid;
  h->pool_snap_seq = ctx->snap_seq;
  h->nspace = ctx->oloc.nspace;
  *listh = (void *)new librados::ObjListCtx(ctx, h);
  tracepoint(librados, rados_objects_list_open_exit, 0, *listh);
  return 0;
}

// Deprecated, but using it for compatibility with older OSDs
extern "C" void rados_objects_list_close(rados_list_ctx_t h)
{
  tracepoint(librados, rados_objects_list_close_enter, h);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)h;
  delete lh;
  tracepoint(librados, rados_objects_list_close_exit);
}

extern "C" uint32_t rados_objects_list_seek(rados_list_ctx_t listctx,
					    uint32_t pos)
{
  tracepoint(librados, rados_objects_list_seek_enter, listctx, pos);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  uint32_t r = lh->ctx->list_seek(lh->lc, pos);
  tracepoint(librados, rados_objects_list_seek_exit, r);
  return r;
}

extern "C" uint32_t rados_objects_list_get_pg_hash_position(
  rados_list_ctx_t listctx)
{
  tracepoint(librados, rados_objects_list_get_pg_hash_position_enter, listctx);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  uint32_t retval = lh->lc->get_pg_hash_position();
  tracepoint(librados, rados_objects_list_get_pg_hash_position_exit, retval);
  return retval;
}

extern "C" int rados_nobjects_list_next(rados_list_ctx_t listctx, const char **entry, const char **key, const char **nspace)
{
  tracepoint(librados, rados_nobjects_list_next_enter, listctx);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  if (!lh->new_request) {
    int retval = rados_objects_list_next(listctx, entry, key);
    // Let's return nspace as you would expect even when asking
    // for a specific one, since you know what it must be.
    if (retval == 0 && nspace)
      *nspace = lh->ctx->oloc.nspace.c_str();
    return retval;
  }
  Objecter::NListContext *h = lh->nlc;

  // if the list is non-empty, this method has been called before
  if (!h->list.empty())
    // so let's kill the previously-returned object
    h->list.pop_front();

  if (h->list.empty()) {
    int ret = lh->ctx->nlist(lh->nlc, RADOS_LIST_MAX_ENTRIES);
    if (ret < 0) {
      tracepoint(librados, rados_nobjects_list_next_exit, ret, NULL, NULL, NULL);
      return ret;
    }
    if (h->list.empty()) {
      tracepoint(librados, rados_nobjects_list_next_exit, -ENOENT, NULL, NULL, NULL);
      return -ENOENT;
    }
  }

  *entry = h->list.front().oid.c_str();

  if (key) {
    if (h->list.front().locator.size())
      *key = h->list.front().locator.c_str();
    else
      *key = NULL;
  }
  if (nspace)
    *nspace = h->list.front().nspace.c_str();
  tracepoint(librados, rados_nobjects_list_next_exit, 0, *entry, key, nspace);
  return 0;
}

// DEPRECATED
extern "C" int rados_objects_list_next(rados_list_ctx_t listctx, const char **entry, const char **key)
{
  tracepoint(librados, rados_objects_list_next_enter, listctx);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  Objecter::ListContext *h = lh->lc;

  // Calling wrong interface after rados_nobjects_list_open()
  if (lh->new_request)
    return -EINVAL;

  // if the list is non-empty, this method has been called before
  if (!h->list.empty())
    // so let's kill the previously-returned object
    h->list.pop_front();

  if (h->list.empty()) {
    int ret = lh->ctx->list(lh->lc, RADOS_LIST_MAX_ENTRIES);
    if (ret < 0) {
      tracepoint(librados, rados_objects_list_next_exit, ret, NULL, NULL);
      return ret;
    }
    if (h->list.empty()) {
      tracepoint(librados, rados_objects_list_next_exit, -ENOENT, NULL, NULL);
      return -ENOENT;
    }
  }

  *entry = h->list.front().first.name.c_str();

  if (key) {
    if (h->list.front().second.size())
      *key = h->list.front().second.c_str();
    else
      *key = NULL;
  }
  tracepoint(librados, rados_objects_list_next_exit, 0, *entry, key);
  return 0;
}



// -------------------------
// aio

extern "C" int rados_aio_create_completion(void *cb_arg,
					   rados_callback_t cb_complete,
					   rados_callback_t cb_safe,
					   rados_completion_t *pc)
{
  tracepoint(librados, rados_aio_create_completion_enter, cb_arg, cb_complete, cb_safe);
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete)
    c->set_complete_callback(cb_arg, cb_complete);
  if (cb_safe)
    c->set_safe_callback(cb_arg, cb_safe);
  *pc = c;
  tracepoint(librados, rados_aio_create_completion_exit, 0, *pc);
  return 0;
}

extern "C" int rados_aio_wait_for_complete(rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_complete_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_complete();
  tracepoint(librados, rados_aio_wait_for_complete_exit, retval);
  return retval;
}

extern "C" int rados_aio_wait_for_safe(rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_safe_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_safe();
  tracepoint(librados, rados_aio_wait_for_safe_exit, retval);
  return retval;
}

extern "C" int rados_aio_is_complete(rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_complete_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_complete();
  tracepoint(librados, rados_aio_is_complete_exit, retval);
  return retval;
}

extern "C" int rados_aio_is_safe(rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_safe_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_safe();
  tracepoint(librados, rados_aio_is_safe_exit, retval);
  return retval;
}

extern "C" int rados_aio_wait_for_complete_and_cb(rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_complete_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_complete_and_cb();
  tracepoint(librados, rados_aio_wait_for_complete_and_cb_exit, retval);
  return retval;
}

extern "C" int rados_aio_wait_for_safe_and_cb(rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_safe_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_safe_and_cb();
  tracepoint(librados, rados_aio_wait_for_safe_and_cb_exit, retval);
  return retval;
}

extern "C" int rados_aio_is_complete_and_cb(rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_complete_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_complete_and_cb();
  tracepoint(librados, rados_aio_is_complete_and_cb_exit, retval);
  return retval;
}

extern "C" int rados_aio_is_safe_and_cb(rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_safe_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_safe_and_cb();
  tracepoint(librados, rados_aio_is_safe_and_cb_exit, retval);
  return retval;
}

extern "C" int rados_aio_get_return_value(rados_completion_t c)
{
  tracepoint(librados, rados_aio_get_return_value_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->get_return_value();
  tracepoint(librados, rados_aio_get_return_value_exit, retval);
  return retval;
}

extern "C" uint64_t rados_aio_get_version(rados_completion_t c)
{
  tracepoint(librados, rados_aio_get_version_enter, c);
  uint64_t retval = ((librados::AioCompletionImpl*)c)->get_version();
  tracepoint(librados, rados_aio_get_version_exit, retval);
  return retval;
}

extern "C" void rados_aio_release(rados_completion_t c)
{
  tracepoint(librados, rados_aio_release_enter, c);
  ((librados::AioCompletionImpl*)c)->put();
  tracepoint(librados, rados_aio_release_exit);
}

extern "C" int rados_aio_read(rados_ioctx_t io, const char *o,
			       rados_completion_t completion,
			       char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_aio_read_enter, io, o, completion, len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_read(oid, (librados::AioCompletionImpl*)completion,
		       buf, len, off, ctx->snap_seq);
  tracepoint(librados, rados_aio_read_exit, retval);
  return retval;
}

extern "C" int rados_aio_write(rados_ioctx_t io, const char *o,
				rados_completion_t completion,
				const char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_aio_write_enter, io, o, completion, buf, len, off);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_write(oid, (librados::AioCompletionImpl*)completion,
			bl, len, off);
  tracepoint(librados, rados_aio_write_exit, retval);
  return retval;
}

extern "C" int rados_aio_append(rados_ioctx_t io, const char *o,
				rados_completion_t completion,
				const char *buf, size_t len)
{
  tracepoint(librados, rados_aio_append_enter, io, o, completion, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_append(oid, (librados::AioCompletionImpl*)completion,
			 bl, len);
  tracepoint(librados, rados_aio_append_exit, retval);
  return retval;
}

extern "C" int rados_aio_write_full(rados_ioctx_t io, const char *o,
				    rados_completion_t completion,
				    const char *buf, size_t len)
{
  tracepoint(librados, rados_aio_write_full_enter, io, o, completion, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_write_full(oid, (librados::AioCompletionImpl*)completion, bl);
  tracepoint(librados, rados_aio_write_full_exit, retval);
  return retval;
}

extern "C" int rados_aio_writesame(rados_ioctx_t io, const char *o,
				   rados_completion_t completion,
				   const char *buf, size_t data_len,
				   size_t write_len, uint64_t off)
{
  tracepoint(librados, rados_aio_writesame_enter, io, o, completion, buf,
						data_len, write_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, data_len);
  int retval = ctx->aio_writesame(o, (librados::AioCompletionImpl*)completion,
				  bl, write_len, off);
  tracepoint(librados, rados_aio_writesame_exit, retval);
  return retval;
}

extern "C" int rados_aio_remove(rados_ioctx_t io, const char *o,
				rados_completion_t completion)
{
  tracepoint(librados, rados_aio_remove_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_remove(oid, (librados::AioCompletionImpl*)completion);
  tracepoint(librados, rados_aio_remove_exit, retval);
  return retval;
}

extern "C" int rados_aio_flush_async(rados_ioctx_t io,
				     rados_completion_t completion)
{
  tracepoint(librados, rados_aio_flush_async_enter, io, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes_async((librados::AioCompletionImpl*)completion);
  tracepoint(librados, rados_aio_flush_async_exit, 0);
  return 0;
}

extern "C" int rados_aio_flush(rados_ioctx_t io)
{
  tracepoint(librados, rados_aio_flush_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes();
  tracepoint(librados, rados_aio_flush_exit, 0);
  return 0;
}

extern "C" int rados_aio_stat(rados_ioctx_t io, const char *o, 
			      rados_completion_t completion,
			      uint64_t *psize, time_t *pmtime)
{
  tracepoint(librados, rados_aio_stat_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_stat(oid, (librados::AioCompletionImpl*)completion,
		       psize, pmtime);
  tracepoint(librados, rados_aio_stat_exit, retval);
  return retval;
}

extern "C" int rados_aio_cancel(rados_ioctx_t io, rados_completion_t completion)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->aio_cancel((librados::AioCompletionImpl*)completion);
}

struct C_WatchCB : public librados::WatchCtx {
  rados_watchcb_t wcb;
  void *arg;
  C_WatchCB(rados_watchcb_t _wcb, void *_arg) : wcb(_wcb), arg(_arg) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    wcb(opcode, ver, arg);
  }
};

extern "C" int rados_watch(rados_ioctx_t io, const char *o, uint64_t ver,
			   uint64_t *handle,
			   rados_watchcb_t watchcb, void *arg)
{
  tracepoint(librados, rados_watch_enter, io, o, ver, watchcb, arg);
  uint64_t *cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  C_WatchCB *wc = new C_WatchCB(watchcb, arg);
  int retval = ctx->watch(oid, cookie, wc, NULL, true);
  tracepoint(librados, rados_watch_exit, retval, *handle);
  return retval;
}

struct C_WatchCB2 : public librados::WatchCtx2 {
  rados_watchcb2_t wcb;
  rados_watcherrcb_t errcb;
  void *arg;
  C_WatchCB2(rados_watchcb2_t _wcb,
	     rados_watcherrcb_t _errcb,
	     void *_arg) : wcb(_wcb), errcb(_errcb), arg(_arg) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_gid,
		     bufferlist& bl) {
    wcb(arg, notify_id, cookie, notifier_gid, bl.c_str(), bl.length());
  }
  void handle_error(uint64_t cookie, int err) {
    if (errcb)
      errcb(arg, cookie, err);
  }
};

extern "C" int rados_watch2(rados_ioctx_t io, const char *o, uint64_t *handle,
			    rados_watchcb2_t watchcb,
			    rados_watcherrcb_t watcherrcb,
			    void *arg)
{
  tracepoint(librados, rados_watch2_enter, io, o, handle, watchcb, arg);
  int ret;
  if (!watchcb || !o || !handle) {
    ret = -EINVAL;
  } else {
    uint64_t *cookie = handle;
    librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
    object_t oid(o);
    C_WatchCB2 *wc = new C_WatchCB2(watchcb, watcherrcb, arg);
    ret = ctx->watch(oid, cookie, NULL, wc, true);
  }
  tracepoint(librados, rados_watch_exit, ret, handle ? *handle : 0);
  return ret;
}

extern "C" int rados_aio_watch(rados_ioctx_t io, const char *o,
                               rados_completion_t completion,
                               uint64_t *handle,
                               rados_watchcb2_t watchcb,
                               rados_watcherrcb_t watcherrcb, void *arg)
{
  tracepoint(librados, rados_aio_watch_enter, io, o, completion, handle, watchcb, arg);
  int ret;
  if (!completion || !watchcb || !o || !handle) {
    ret = -EINVAL;
  } else {
    uint64_t *cookie = handle;
    librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
    object_t oid(o);
    librados::AioCompletionImpl *c =
      reinterpret_cast<librados::AioCompletionImpl*>(completion);
    C_WatchCB2 *wc = new C_WatchCB2(watchcb, watcherrcb, arg);
    ret = ctx->aio_watch(oid, c, cookie, NULL, wc, true);
  }
  tracepoint(librados, rados_watch_exit, ret, handle ? *handle : 0);
  return ret;
}


extern "C" int rados_unwatch(rados_ioctx_t io, const char *o, uint64_t handle)
{
  tracepoint(librados, rados_unwatch_enter, io, o, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->unwatch(cookie);
  tracepoint(librados, rados_unwatch_exit, retval);
  return retval;
}

extern "C" int rados_unwatch2(rados_ioctx_t io, uint64_t handle)
{
  tracepoint(librados, rados_unwatch2_enter, io, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->unwatch(cookie);
  tracepoint(librados, rados_unwatch2_exit, retval);
  return retval;
}

extern "C" int rados_aio_unwatch(rados_ioctx_t io, uint64_t handle,
                                 rados_completion_t completion)
{
  tracepoint(librados, rados_aio_unwatch_enter, io, handle, completion);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c =
    reinterpret_cast<librados::AioCompletionImpl*>(completion);
  int retval = ctx->aio_unwatch(cookie, c);
  tracepoint(librados, rados_aio_unwatch_exit, retval);
  return retval;
}

extern "C" int rados_watch_check(rados_ioctx_t io, uint64_t handle)
{
  tracepoint(librados, rados_watch_check_enter, io, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->watch_check(cookie);
  tracepoint(librados, rados_watch_check_exit, retval);
  return retval;
}

extern "C" int rados_notify(rados_ioctx_t io, const char *o,
			    uint64_t ver, const char *buf, int buf_len)
{
  tracepoint(librados, rados_notify_enter, io, o, ver, buf, buf_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  int retval = ctx->notify(oid, bl, 0, NULL, NULL, NULL);
  tracepoint(librados, rados_notify_exit, retval);
  return retval;
}

extern "C" int rados_notify2(rados_ioctx_t io, const char *o,
			     const char *buf, int buf_len,
			     uint64_t timeout_ms,
			     char **reply_buffer,
			     size_t *reply_buffer_len)
{
  tracepoint(librados, rados_notify2_enter, io, o, buf, buf_len, timeout_ms);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  int ret = ctx->notify(oid, bl, timeout_ms, NULL, reply_buffer, reply_buffer_len);
  tracepoint(librados, rados_notify2_exit, ret);
  return ret;
}

extern "C" int rados_aio_notify(rados_ioctx_t io, const char *o,
                                rados_completion_t completion,
                                const char *buf, int buf_len,
                                uint64_t timeout_ms, char **reply_buffer,
                                size_t *reply_buffer_len)
{
  tracepoint(librados, rados_aio_notify_enter, io, o, completion, buf, buf_len,
             timeout_ms);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bl.push_back(buffer::copy(buf, buf_len));
  }
  librados::AioCompletionImpl *c =
    reinterpret_cast<librados::AioCompletionImpl*>(completion);
  int ret = ctx->aio_notify(oid, c, bl, timeout_ms, NULL, reply_buffer,
                            reply_buffer_len);
  tracepoint(librados, rados_aio_notify_exit, ret);
  return ret;
}

extern "C" int rados_notify_ack(rados_ioctx_t io, const char *o,
				uint64_t notify_id, uint64_t handle,
				const char *buf, int buf_len)
{
  tracepoint(librados, rados_notify_ack_enter, io, o, notify_id, handle, buf, buf_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  ctx->notify_ack(oid, notify_id, handle, bl);
  tracepoint(librados, rados_notify_ack_exit, 0);
  return 0;
}

extern "C" int rados_watch_flush(rados_t cluster)
{
  tracepoint(librados, rados_watch_flush_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->watch_flush();
  tracepoint(librados, rados_watch_flush_exit, retval);
  return retval;
}

extern "C" int rados_aio_watch_flush(rados_t cluster, rados_completion_t completion)
{
  tracepoint(librados, rados_aio_watch_flush_enter, cluster, completion);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  int retval = client->async_watch_flush(c);
  tracepoint(librados, rados_aio_watch_flush_exit, retval);
  return retval;
}

extern "C" int rados_set_alloc_hint(rados_ioctx_t io, const char *o,
                                    uint64_t expected_object_size,
                                    uint64_t expected_write_size)
{
  tracepoint(librados, rados_set_alloc_hint_enter, io, o, expected_object_size, expected_write_size);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->set_alloc_hint(oid, expected_object_size, expected_write_size);
  tracepoint(librados, rados_set_alloc_hint_exit, retval);
  return retval;
}

extern "C" int rados_lock_exclusive(rados_ioctx_t io, const char * o,
			  const char * name, const char * cookie,
			  const char * desc, struct timeval * duration,
			  uint8_t flags)
{
  tracepoint(librados, rados_lock_exclusive_enter, io, o, name, cookie, desc, duration, flags);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.lock_exclusive(o, name, cookie, desc, duration, flags);
  tracepoint(librados, rados_lock_exclusive_exit, retval);
  return retval;
}

extern "C" int rados_lock_shared(rados_ioctx_t io, const char * o,
			  const char * name, const char * cookie,
			  const char * tag, const char * desc,
			  struct timeval * duration, uint8_t flags)
{
  tracepoint(librados, rados_lock_shared_enter, io, o, name, cookie, tag, desc, duration, flags);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.lock_shared(o, name, cookie, tag, desc, duration, flags);
  tracepoint(librados, rados_lock_shared_exit, retval);
  return retval;
}
extern "C" int rados_unlock(rados_ioctx_t io, const char *o, const char *name,
			    const char *cookie)
{
  tracepoint(librados, rados_unlock_enter, io, o, name, cookie);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.unlock(o, name, cookie);
  tracepoint(librados, rados_unlock_exit, retval);
  return retval;
}

extern "C" ssize_t rados_list_lockers(rados_ioctx_t io, const char *o,
				      const char *name, int *exclusive,
				      char *tag, size_t *tag_len,
				      char *clients, size_t *clients_len,
				      char *cookies, size_t *cookies_len,
				      char *addrs, size_t *addrs_len)
{
  tracepoint(librados, rados_list_lockers_enter, io, o, name, *tag_len, *clients_len, *cookies_len, *addrs_len);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);
  std::string name_str = name;
  std::string oid = o;
  std::string tag_str;
  int tmp_exclusive;
  std::list<librados::locker_t> lockers;
  int r = ctx.list_lockers(oid, name_str, &tmp_exclusive, &tag_str, &lockers);
  if (r < 0) {
    tracepoint(librados, rados_list_lockers_exit, r, *exclusive, "", *tag_len, *clients_len, *cookies_len, *addrs_len);
	  return r;
  }

  size_t clients_total = 0;
  size_t cookies_total = 0;
  size_t addrs_total = 0;
  list<librados::locker_t>::const_iterator it;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    clients_total += it->client.length() + 1;
    cookies_total += it->cookie.length() + 1;
    addrs_total += it->address.length() + 1;
  }

  bool too_short = ((clients_total > *clients_len) ||
                    (cookies_total > *cookies_len) ||
                    (addrs_total > *addrs_len) ||
                    (tag_str.length() + 1 > *tag_len));
  *clients_len = clients_total;
  *cookies_len = cookies_total;
  *addrs_len = addrs_total;
  *tag_len = tag_str.length() + 1;
  if (too_short) {
    tracepoint(librados, rados_list_lockers_exit, -ERANGE, *exclusive, "", *tag_len, *clients_len, *cookies_len, *addrs_len);
    return -ERANGE;
  }

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    strcpy(clients_p, it->client.c_str());
    strcpy(cookies_p, it->cookie.c_str());
    strcpy(addrs_p, it->address.c_str());
    tracepoint(librados, rados_list_lockers_locker, clients_p, cookies_p, addrs_p);
    clients_p += it->client.length() + 1;
    cookies_p += it->cookie.length() + 1;
    addrs_p += it->address.length() + 1;
  }
  if (tmp_exclusive)
    *exclusive = 1;
  else
    *exclusive = 0;

  int retval = lockers.size();
  tracepoint(librados, rados_list_lockers_exit, retval, *exclusive, tag, *tag_len, *clients_len, *cookies_len, *addrs_len);
  return retval;
}

extern "C" int rados_break_lock(rados_ioctx_t io, const char *o,
				const char *name, const char *client,
				const char *cookie)
{
  tracepoint(librados, rados_break_lock_enter, io, o, name, client, cookie);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.break_lock(o, name, client, cookie);
  tracepoint(librados, rados_break_lock_exit, retval);
  return retval;
}

extern "C" rados_write_op_t rados_create_write_op()
{
  tracepoint(librados, rados_create_write_op_enter);
  rados_write_op_t retval = new (std::nothrow)::ObjectOperation;
  tracepoint(librados, rados_create_write_op_exit, retval);
  return retval;
}

extern "C" void rados_release_write_op(rados_write_op_t write_op)
{
  tracepoint(librados, rados_release_write_op_enter, write_op);
  delete (::ObjectOperation*)write_op;
  tracepoint(librados, rados_release_write_op_exit);
}

extern "C" void rados_write_op_set_flags(rados_write_op_t write_op, int flags)
{
  tracepoint(librados, rados_write_op_set_flags_enter, write_op, flags);
  set_op_flags((::ObjectOperation *)write_op, flags);
  tracepoint(librados, rados_write_op_set_flags_exit);
}

extern "C" void rados_write_op_assert_version(rados_write_op_t write_op, uint64_t ver)
{
  tracepoint(librados, rados_write_op_assert_version_enter, write_op, ver);
  ((::ObjectOperation *)write_op)->assert_version(ver);
  tracepoint(librados, rados_write_op_assert_version_exit);
}

extern "C" void rados_write_op_assert_exists(rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_assert_exists_enter, write_op);
  ((::ObjectOperation *)write_op)->stat(NULL, (ceph::real_time *)NULL, NULL);
  tracepoint(librados, rados_write_op_assert_exists_exit);
}

extern "C" void rados_write_op_cmpxattr(rados_write_op_t write_op,
                                       const char *name,
				       uint8_t comparison_operator,
				       const char *value,
				       size_t value_len)
{
  tracepoint(librados, rados_write_op_cmpxattr_enter, write_op, name, comparison_operator, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)write_op)->cmpxattr(name,
					    comparison_operator,
					    CEPH_OSD_CMPXATTR_MODE_STRING,
					    bl);
  tracepoint(librados, rados_write_op_cmpxattr_exit);
}

static void rados_c_omap_cmp(ObjectOperation *op,
			     const char *key,
			     uint8_t comparison_operator,
			     const char *val,
			     size_t val_len,
			     int *prval)
{
  bufferlist bl;
  bl.append(val, val_len);
  std::map<std::string, pair<bufferlist, int> > assertions;
  assertions[key] = std::make_pair(bl, comparison_operator);
  op->omap_cmp(assertions, prval);
}

extern "C" void rados_write_op_omap_cmp(rados_write_op_t write_op,
					const char *key,
					uint8_t comparison_operator,
					const char *val,
					size_t val_len,
					int *prval)
{
  tracepoint(librados, rados_write_op_omap_cmp_enter, write_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp((::ObjectOperation *)write_op, key, comparison_operator,
		   val, val_len, prval);
  tracepoint(librados, rados_write_op_omap_cmp_exit);
}

extern "C" void rados_write_op_setxattr(rados_write_op_t write_op,
                                       const char *name,
				       const char *value,
				       size_t value_len)
{
  tracepoint(librados, rados_write_op_setxattr_enter, write_op, name, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)write_op)->setxattr(name, bl);
  tracepoint(librados, rados_write_op_setxattr_exit);
}

extern "C" void rados_write_op_rmxattr(rados_write_op_t write_op,
                                       const char *name)
{
  tracepoint(librados, rados_write_op_rmxattr_enter, write_op, name);
  bufferlist bl;
  ((::ObjectOperation *)write_op)->rmxattr(name);
  tracepoint(librados, rados_write_op_rmxattr_exit);
}

extern "C" void rados_write_op_create(rados_write_op_t write_op,
                                      int exclusive,
				      const char* category) // unused
{
  tracepoint(librados, rados_write_op_create_enter, write_op, exclusive);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  oo->create(!!exclusive);
  tracepoint(librados, rados_write_op_create_exit);
}

extern "C" void rados_write_op_write(rados_write_op_t write_op,
				     const char *buffer,
				     size_t len,
                                     uint64_t offset)
{
  tracepoint(librados, rados_write_op_write_enter, write_op, buffer, len, offset);
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->write(offset, bl);
  tracepoint(librados, rados_write_op_write_exit);
}

extern "C" void rados_write_op_write_full(rados_write_op_t write_op,
				          const char *buffer,
				          size_t len)
{
  tracepoint(librados, rados_write_op_write_full_enter, write_op, buffer, len);
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->write_full(bl);
  tracepoint(librados, rados_write_op_write_full_exit);
}

extern "C" void rados_write_op_writesame(rados_write_op_t write_op,
				         const char *buffer,
				         size_t data_len,
				         size_t write_len,
					 uint64_t offset)
{
  tracepoint(librados, rados_write_op_writesame_enter, write_op, buffer, data_len, write_len, offset);
  bufferlist bl;
  bl.append(buffer, data_len);
  ((::ObjectOperation *)write_op)->writesame(offset, write_len, bl);
  tracepoint(librados, rados_write_op_writesame_exit);
}

extern "C" void rados_write_op_append(rados_write_op_t write_op,
				      const char *buffer,
				      size_t len)
{
  tracepoint(librados, rados_write_op_append_enter, write_op, buffer, len);
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->append(bl);
  tracepoint(librados, rados_write_op_append_exit);
}

extern "C" void rados_write_op_remove(rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_remove_enter, write_op);
  ((::ObjectOperation *)write_op)->remove();
  tracepoint(librados, rados_write_op_remove_exit);
}

extern "C" void rados_write_op_truncate(rados_write_op_t write_op,
				        uint64_t offset)
{
  tracepoint(librados, rados_write_op_truncate_enter, write_op, offset);
  ((::ObjectOperation *)write_op)->truncate(offset);
  tracepoint(librados, rados_write_op_truncate_exit);
}

extern "C" void rados_write_op_zero(rados_write_op_t write_op,
				    uint64_t offset,
				    uint64_t len)
{
  tracepoint(librados, rados_write_op_zero_enter, write_op, offset, len);
  ((::ObjectOperation *)write_op)->zero(offset, len);
  tracepoint(librados, rados_write_op_zero_exit);
}

extern "C" void rados_write_op_exec(rados_write_op_t write_op,
				    const char *cls,
				    const char *method,
				    const char *in_buf,
				    size_t in_len,
				    int *prval)
{
  tracepoint(librados, rados_write_op_exec_enter, write_op, cls, method, in_buf, in_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  ((::ObjectOperation *)write_op)->call(cls, method, inbl, NULL, NULL, prval);
  tracepoint(librados, rados_write_op_exec_exit);
}

extern "C" void rados_write_op_omap_set(rados_write_op_t write_op,
					char const* const* keys,
					char const* const* vals,
					const size_t *lens,
					size_t num)
{
  tracepoint(librados, rados_write_op_omap_set_enter, write_op, num);
  std::map<std::string, bufferlist> entries;
  for (size_t i = 0; i < num; ++i) {
    tracepoint(librados, rados_write_op_omap_set_entry, keys[i], vals[i], lens[i]);
    bufferlist bl(lens[i]);
    bl.append(vals[i], lens[i]);
    entries[keys[i]] = bl;
  }
  ((::ObjectOperation *)write_op)->omap_set(entries);
  tracepoint(librados, rados_write_op_omap_set_exit);
}

extern "C" void rados_write_op_omap_rm_keys(rados_write_op_t write_op,
					    char const* const* keys,
					    size_t keys_len)
{
  tracepoint(librados, rados_write_op_omap_rm_keys_enter, write_op, keys_len);
  for(size_t i = 0; i < keys_len; i++) {
    tracepoint(librados, rados_write_op_omap_rm_keys_entry, keys[i]);
  }
  std::set<std::string> to_remove(keys, keys + keys_len);
  ((::ObjectOperation *)write_op)->omap_rm_keys(to_remove);
  tracepoint(librados, rados_write_op_omap_rm_keys_exit);
}

extern "C" void rados_write_op_omap_clear(rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_omap_clear_enter, write_op);
  ((::ObjectOperation *)write_op)->omap_clear();
  tracepoint(librados, rados_write_op_omap_clear_exit);
}

extern "C" void rados_write_op_set_alloc_hint(rados_write_op_t write_op,
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size)
{
  tracepoint(librados, rados_write_op_set_alloc_hint_enter, write_op, expected_object_size, expected_write_size);
  ((::ObjectOperation *)write_op)->set_alloc_hint(expected_object_size,
                                                  expected_write_size);
  tracepoint(librados, rados_write_op_set_alloc_hint_exit);
}

extern "C" int rados_write_op_operate(rados_write_op_t write_op,
                                      rados_ioctx_t io,
                                      const char *oid,
				      time_t *mtime,
				      int flags)
{
  tracepoint(librados, rados_write_op_operate_enter, write_op, io, oid, mtime, flags);
  object_t obj(oid);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  ceph::real_time *prt = NULL;
  ceph::real_time rt;

  if (mtime) {
    rt = ceph::real_clock::from_time_t(*mtime);
    prt = &rt;
  }

  int retval = ctx->operate(obj, oo, prt, translate_flags(flags));
  tracepoint(librados, rados_write_op_operate_exit, retval);
  return retval;
}

extern "C" int rados_write_op_operate2(rados_write_op_t write_op,
                                       rados_ioctx_t io,
                                       const char *oid,
                                       struct timespec *ts,
                                       int flags)
{
  tracepoint(librados, rados_write_op_operate2_enter, write_op, io, oid, ts, flags);
  object_t obj(oid);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  ceph::real_time *prt = NULL;
  ceph::real_time rt;

  if (ts) {
    rt = ceph::real_clock::from_timespec(*ts);
    prt = &rt;
  }

  int retval = ctx->operate(obj, oo, prt, translate_flags(flags));
  tracepoint(librados, rados_write_op_operate_exit, retval);
  return retval;
}

extern "C" int rados_aio_write_op_operate(rados_write_op_t write_op,
					  rados_ioctx_t io,
					  rados_completion_t completion,
					  const char *oid,
					  time_t *mtime,
					  int flags)
{
  tracepoint(librados, rados_aio_write_op_operate_enter, write_op, io, completion, oid, mtime, flags);
  object_t obj(oid);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  int retval = ctx->aio_operate(obj, oo, c, ctx->snapc, translate_flags(flags));
  tracepoint(librados, rados_aio_write_op_operate_exit, retval);
  return retval;
}

extern "C" rados_read_op_t rados_create_read_op()
{
  tracepoint(librados, rados_create_read_op_enter);
  rados_read_op_t retval = new (std::nothrow)::ObjectOperation;
  tracepoint(librados, rados_create_read_op_exit, retval);
  return retval;
}

extern "C" void rados_release_read_op(rados_read_op_t read_op)
{
  tracepoint(librados, rados_release_read_op_enter, read_op);
  delete (::ObjectOperation *)read_op;
  tracepoint(librados, rados_release_read_op_exit);
}

extern "C" void rados_read_op_set_flags(rados_read_op_t read_op, int flags)
{
  tracepoint(librados, rados_read_op_set_flags_enter, read_op, flags);
  set_op_flags((::ObjectOperation *)read_op, flags);
  tracepoint(librados, rados_read_op_set_flags_exit);
}

extern "C" void rados_read_op_assert_version(rados_read_op_t read_op, uint64_t ver)
{
  tracepoint(librados, rados_read_op_assert_version_enter, read_op, ver);
  ((::ObjectOperation *)read_op)->assert_version(ver);
  tracepoint(librados, rados_read_op_assert_version_exit);
}

extern "C" void rados_read_op_assert_exists(rados_read_op_t read_op)
{
  tracepoint(librados, rados_read_op_assert_exists_enter, read_op);
  ((::ObjectOperation *)read_op)->stat(NULL, (ceph::real_time *)NULL, NULL);
  tracepoint(librados, rados_read_op_assert_exists_exit);
}

extern "C" void rados_read_op_cmpxattr(rados_read_op_t read_op,
				       const char *name,
				       uint8_t comparison_operator,
				       const char *value,
				       size_t value_len)
{
  tracepoint(librados, rados_read_op_cmpxattr_enter, read_op, name, comparison_operator, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)read_op)->cmpxattr(name,
					   comparison_operator,
					   CEPH_OSD_CMPXATTR_MODE_STRING,
					   bl);
  tracepoint(librados, rados_read_op_cmpxattr_exit);
}

extern "C" void rados_read_op_omap_cmp(rados_read_op_t read_op,
				       const char *key,
				       uint8_t comparison_operator,
				       const char *val,
				       size_t val_len,
				       int *prval)
{
  tracepoint(librados, rados_read_op_omap_cmp_enter, read_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp((::ObjectOperation *)read_op, key, comparison_operator,
		   val, val_len, prval);
  tracepoint(librados, rados_read_op_omap_cmp_exit);
}

extern "C" void rados_read_op_stat(rados_read_op_t read_op,
				   uint64_t *psize,
				   time_t *pmtime,
				   int *prval)
{
  tracepoint(librados, rados_read_op_stat_enter, read_op, psize, pmtime, prval);
  ((::ObjectOperation *)read_op)->stat(psize, pmtime, prval);
  tracepoint(librados, rados_read_op_stat_exit);
}

class C_bl_to_buf : public Context {
  char *out_buf;
  size_t out_len;
  size_t *bytes_read;
  int *prval;
public:
  bufferlist out_bl;
  C_bl_to_buf(char *out_buf,
	      size_t out_len,
	      size_t *bytes_read,
	      int *prval) : out_buf(out_buf), out_len(out_len),
			    bytes_read(bytes_read), prval(prval) {}
  void finish(int r) {
    if (out_bl.length() > out_len) {
      if (prval)
	*prval = -ERANGE;
      if (bytes_read)
	*bytes_read = 0;
      return;
    }
    if (bytes_read)
      *bytes_read = out_bl.length();
    if (out_buf && !out_bl.is_provided_buffer(out_buf))
      out_bl.copy(0, out_bl.length(), out_buf);
  }
};

extern "C" void rados_read_op_read(rados_read_op_t read_op,
				   uint64_t offset,
				   size_t len,
				   char *buf,
				   size_t *bytes_read,
				   int *prval)
{
  tracepoint(librados, rados_read_op_read_enter, read_op, offset, len, buf, bytes_read, prval);
  C_bl_to_buf *ctx = new C_bl_to_buf(buf, len, bytes_read, prval);
  ctx->out_bl.push_back(buffer::create_static(len, buf));
  ((::ObjectOperation *)read_op)->read(offset, len, &ctx->out_bl, prval, ctx);
  tracepoint(librados, rados_read_op_read_exit);
}

class C_out_buffer : public Context {
  char **out_buf;
  size_t *out_len;
public:
  bufferlist out_bl;
  C_out_buffer(char **out_buf, size_t *out_len) : out_buf(out_buf),
						  out_len(out_len) {}
  void finish(int r) {
    // ignore r since we don't know the meaning of return values
    // from custom class methods
    do_out_buffer(out_bl, out_buf, out_len);
  }
};

extern "C" void rados_read_op_exec(rados_read_op_t read_op,
				   const char *cls,
				   const char *method,
				   const char *in_buf,
				   size_t in_len,
				   char **out_buf,
				   size_t *out_len,
				   int *prval)
{
  tracepoint(librados, rados_read_op_exec_enter, read_op, cls, method, in_buf, in_len, out_buf, out_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  C_out_buffer *ctx = new C_out_buffer(out_buf, out_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
  tracepoint(librados, rados_read_op_exec_exit);
}

extern "C" void rados_read_op_exec_user_buf(rados_read_op_t read_op,
					    const char *cls,
					    const char *method,
					    const char *in_buf,
					    size_t in_len,
					    char *out_buf,
					    size_t out_len,
					    size_t *used_len,
					    int *prval)
{
  tracepoint(librados, rados_read_op_exec_user_buf_enter, read_op, cls, method, in_buf, in_len, out_buf, out_len, used_len, prval);
  C_bl_to_buf *ctx = new C_bl_to_buf(out_buf, out_len, used_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
  tracepoint(librados, rados_read_op_exec_user_buf_exit);
}

struct RadosOmapIter {
  std::map<std::string, bufferlist> values;
  std::map<std::string, bufferlist>::iterator i;
};

class C_OmapIter : public Context {
  RadosOmapIter *iter;
public:
  explicit C_OmapIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) {
    iter->i = iter->values.begin();
  }
};

class C_XattrsIter : public Context {
  librados::RadosXattrsIter *iter;
public:
  explicit C_XattrsIter(librados::RadosXattrsIter *iter) : iter(iter) {}
  void finish(int r) {
    iter->i = iter->attrset.begin();
  }
};

extern "C" void rados_read_op_getxattrs(rados_read_op_t read_op,
					rados_xattrs_iter_t *iter,
					int *prval)
{
  tracepoint(librados, rados_read_op_getxattrs_enter, read_op, prval);
  librados::RadosXattrsIter *xattrs_iter = new librados::RadosXattrsIter;
  ((::ObjectOperation *)read_op)->getxattrs(&xattrs_iter->attrset, prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_XattrsIter(xattrs_iter));
  *iter = xattrs_iter;
  tracepoint(librados, rados_read_op_getxattrs_exit, *iter);
}

extern "C" void rados_read_op_omap_get_vals(rados_read_op_t read_op,
					    const char *start_after,
					    const char *filter_prefix,
					    uint64_t max_return,
					    rados_omap_iter_t *iter,
					    int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_enter, read_op, start_after, filter_prefix, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  const char *start = start_after ? start_after : "";
  const char *filter = filter_prefix ? filter_prefix : "";
  ((::ObjectOperation *)read_op)->omap_get_vals(start,
						filter,
						max_return,
						&omap_iter->values,
						prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_vals_exit, *iter);
}

struct C_OmapKeysIter : public Context {
  RadosOmapIter *iter;
  std::set<std::string> keys;
  explicit C_OmapKeysIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) {
    // map each key to an empty bl
    for (std::set<std::string>::const_iterator i = keys.begin();
	 i != keys.end(); ++i) {
      iter->values[*i];
    }
    iter->i = iter->values.begin();
  }
};

extern "C" void rados_read_op_omap_get_keys(rados_read_op_t read_op,
					    const char *start_after,
					    uint64_t max_return,
					    rados_omap_iter_t *iter,
					    int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_keys_enter, read_op, start_after, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  C_OmapKeysIter *ctx = new C_OmapKeysIter(omap_iter);
  ((::ObjectOperation *)read_op)->omap_get_keys(start_after ? start_after : "",
						max_return, &ctx->keys, prval);
  ((::ObjectOperation *)read_op)->add_handler(ctx);
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_keys_exit, *iter);
}

extern "C" void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op,
						    char const* const* keys,
						    size_t keys_len,
						    rados_omap_iter_t *iter,
						    int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_enter, read_op, keys, keys_len, iter, prval);
  std::set<std::string> to_get(keys, keys + keys_len);

  RadosOmapIter *omap_iter = new RadosOmapIter;
  ((::ObjectOperation *)read_op)->omap_get_vals_by_keys(to_get,
							&omap_iter->values,
							prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_exit, *iter);
}

extern "C" int rados_omap_get_next(rados_omap_iter_t iter,
				   char **key,
				   char **val,
				   size_t *len)
{
  tracepoint(librados, rados_omap_get_next_enter, iter);
  RadosOmapIter *it = static_cast<RadosOmapIter *>(iter);
  if (it->i == it->values.end()) {
    *key = NULL;
    *val = NULL;
    *len = 0;
    tracepoint(librados, rados_omap_get_next_exit, 0, key, val, len);
    return 0;
  }
  if (key)
    *key = (char*)it->i->first.c_str();
  if (val)
    *val = it->i->second.c_str();
  if (len)
    *len = it->i->second.length();
  ++it->i;
  tracepoint(librados, rados_omap_get_next_exit, 0, key, val, len);
  return 0;
}

extern "C" void rados_omap_get_end(rados_omap_iter_t iter)
{
  tracepoint(librados, rados_omap_get_end_enter, iter);
  RadosOmapIter *it = static_cast<RadosOmapIter *>(iter);
  delete it;
  tracepoint(librados, rados_omap_get_end_exit);
}

extern "C" int rados_read_op_operate(rados_read_op_t read_op,
				     rados_ioctx_t io,
				     const char *oid,
				     int flags)
{
  tracepoint(librados, rados_read_op_operate_enter, read_op, io, oid, flags);
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->operate_read(obj, (::ObjectOperation *)read_op, NULL,
				 translate_flags(flags));
  tracepoint(librados, rados_read_op_operate_exit, retval);
  return retval;
}

extern "C" int rados_aio_read_op_operate(rados_read_op_t read_op,
					 rados_ioctx_t io,
					 rados_completion_t completion,
					 const char *oid,
					 int flags)
{
  tracepoint(librados, rados_aio_read_op_operate_enter, read_op, io, completion, oid, flags);
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  int retval = ctx->aio_operate_read(obj, (::ObjectOperation *)read_op,
				     c, translate_flags(flags), NULL);
  tracepoint(librados, rados_aio_read_op_operate_exit, retval);
  return retval;
}

extern "C" int rados_cache_pin(rados_ioctx_t io, const char *o)
{
  tracepoint(librados, rados_cache_pin_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->cache_pin(oid);
  tracepoint(librados, rados_cache_pin_exit, retval);
  return retval;
}

extern "C" int rados_cache_unpin(rados_ioctx_t io, const char *o)
{
  tracepoint(librados, rados_cache_unpin_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->cache_unpin(oid);
  tracepoint(librados, rados_cache_unpin_exit, retval);
  return retval;
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

CEPH_RADOS_API void rados_object_list_slice(
    rados_ioctx_t io,
    const rados_object_list_cursor start,
    const rados_object_list_cursor finish,
    const size_t n,
    const size_t m,
    rados_object_list_cursor *split_start,
    rados_object_list_cursor *split_finish)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  assert(split_start);
  assert(split_finish);
  hobject_t *split_start_hobj = (hobject_t*)(*split_start);
  hobject_t *split_finish_hobj = (hobject_t*)(*split_finish);
  assert(split_start_hobj);
  assert(split_finish_hobj);
  hobject_t *start_hobj = (hobject_t*)(start);
  hobject_t *finish_hobj = (hobject_t*)(finish);

  ctx->object_list_slice(
      *start_hobj,
      *finish_hobj,
      n,
      m,
      split_start_hobj,
      split_finish_hobj);
}

librados::ObjectCursor::ObjectCursor()
{
  c_cursor = new hobject_t();
}

librados::ObjectCursor::~ObjectCursor()
{
  hobject_t *h = (hobject_t *)c_cursor;
  delete h;
}

bool librados::ObjectCursor::operator<(const librados::ObjectCursor &rhs)
{
  const hobject_t lhs_hobj = (c_cursor == nullptr) ? hobject_t() : *((hobject_t*)c_cursor);
  const hobject_t rhs_hobj = (rhs.c_cursor == nullptr) ? hobject_t() : *((hobject_t*)(rhs.c_cursor));
  return cmp_bitwise(lhs_hobj, rhs_hobj) == -1;
}

librados::ObjectCursor::ObjectCursor(const librados::ObjectCursor &rhs)
{
  if (rhs.c_cursor != nullptr) {
    hobject_t *h = (hobject_t*)rhs.c_cursor;
    c_cursor = (rados_object_list_cursor)(new hobject_t(*h));
  } else {
    c_cursor = nullptr;
  }
}

librados::ObjectCursor librados::IoCtx::object_list_begin()
{
  hobject_t *h = new hobject_t(io_ctx_impl->objecter->enumerate_objects_begin());
  ObjectCursor oc;
  oc.c_cursor = (rados_object_list_cursor)h;
  return oc;
}


librados::ObjectCursor librados::IoCtx::object_list_end()
{
  hobject_t *h = new hobject_t(io_ctx_impl->objecter->enumerate_objects_end());
  librados::ObjectCursor oc;
  oc.c_cursor = (rados_object_list_cursor)h;
  return oc;
}


void librados::ObjectCursor::set(rados_object_list_cursor c)
{
  delete (hobject_t*)c_cursor;
  c_cursor = c;
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
  assert(result != nullptr);
  assert(next != nullptr);
  result->clear();

  C_SaferCond cond;
  hobject_t next_hash;
  std::list<librados::ListObjectImpl> obj_result;
  io_ctx_impl->objecter->enumerate_objects(
      io_ctx_impl->poolid,
      io_ctx_impl->oloc.nspace,
      *((hobject_t*)start.c_cursor),
      *((hobject_t*)finish.c_cursor),
      result_item_count,
      filter,
      &obj_result,
      &next_hash,
      &cond);

  int r = cond.wait();
  if (r < 0) {
    next->set((rados_object_list_cursor)(new hobject_t(hobject_t::get_max())));
    return r;
  }

  next->set((rados_object_list_cursor)(new hobject_t(next_hash)));

  for (std::list<librados::ListObjectImpl>::iterator i = obj_result.begin();
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
  assert(split_start != nullptr);
  assert(split_finish != nullptr);

  io_ctx_impl->object_list_slice(
      *((hobject_t*)(start.c_cursor)),
      *((hobject_t*)(finish.c_cursor)),
      n,
      m,
      (hobject_t*)(split_start->c_cursor),
      (hobject_t*)(split_finish->c_cursor));
}

