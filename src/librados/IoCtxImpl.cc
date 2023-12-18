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

#include "IoCtxImpl.h"

#include "librados/librados_c.h"
#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include "include/ceph_assert.h"
#include "common/valgrind.h"
#include "common/EventTrace.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

using std::string;
using std::map;
using std::unique_lock;
using std::vector;

namespace bs = boost::system;
namespace ca = ceph::async;
namespace cb = ceph::buffer;

namespace librados {
namespace {

struct CB_notify_Finish {
  CephContext *cct;
  Context *ctx;
  Objecter *objecter;
  Objecter::LingerOp *linger_op;
  bufferlist *preply_bl;
  char **preply_buf;
  size_t *preply_buf_len;

  CB_notify_Finish(CephContext *_cct, Context *_ctx, Objecter *_objecter,
		   Objecter::LingerOp *_linger_op, bufferlist *_preply_bl,
		   char **_preply_buf, size_t *_preply_buf_len)
    : cct(_cct), ctx(_ctx), objecter(_objecter), linger_op(_linger_op),
      preply_bl(_preply_bl), preply_buf(_preply_buf),
      preply_buf_len(_preply_buf_len) {}


  // move-only
  CB_notify_Finish(const CB_notify_Finish&) = delete;
  CB_notify_Finish& operator =(const CB_notify_Finish&) = delete;
  CB_notify_Finish(CB_notify_Finish&&) = default;
  CB_notify_Finish& operator =(CB_notify_Finish&&) = default;

  void operator()(bs::error_code ec, bufferlist&& reply_bl) {
    ldout(cct, 10) << __func__ << " completed notify (linger op "
                   << linger_op << "), ec = " << ec << dendl;

    // pass result back to user
    // NOTE: we do this regardless of what error code we return
    if (preply_buf) {
      if (reply_bl.length()) {
        *preply_buf = (char*)malloc(reply_bl.length());
        memcpy(*preply_buf, reply_bl.c_str(), reply_bl.length());
      } else {
        *preply_buf = NULL;
      }
    }
    if (preply_buf_len)
      *preply_buf_len = reply_bl.length();
    if (preply_bl)
      *preply_bl = std::move(reply_bl);

    ctx->complete(ceph::from_error_code(ec));
  }
};

struct CB_aio_linger_cancel {
  Objecter *objecter;
  Objecter::LingerOp *linger_op;

  CB_aio_linger_cancel(Objecter *_objecter, Objecter::LingerOp *_linger_op)
    : objecter(_objecter), linger_op(_linger_op)
  {
  }

  void operator()() {
    objecter->linger_cancel(linger_op);
  }
};

struct C_aio_linger_Complete : public Context {
  AioCompletionImpl *c;
  Objecter::LingerOp *linger_op;
  bool cancel;

  C_aio_linger_Complete(AioCompletionImpl *_c, Objecter::LingerOp *_linger_op, bool _cancel)
    : c(_c), linger_op(_linger_op), cancel(_cancel)
  {
    c->get();
  }

  void finish(int r) override {
    if (cancel || r < 0)
      boost::asio::defer(c->io->client->finish_strand,
			 CB_aio_linger_cancel(c->io->objecter,
					      linger_op));

    c->lock.lock();
    c->rval = r;
    c->complete = true;
    c->cond.notify_all();

    if (c->callback_complete ||
	c->callback_safe) {
      boost::asio::defer(c->io->client->finish_strand, CB_AioComplete(c));
    }
    c->put_unlock();
  }
};

struct C_aio_notify_Complete : public C_aio_linger_Complete {
  ceph::mutex lock = ceph::make_mutex("C_aio_notify_Complete::lock");
  bool acked = false;
  bool finished = false;
  int ret_val = 0;

  C_aio_notify_Complete(AioCompletionImpl *_c, Objecter::LingerOp *_linger_op)
    : C_aio_linger_Complete(_c, _linger_op, false) {
  }

  void handle_ack(int r) {
    // invoked by C_aio_notify_Ack
    lock.lock();
    acked = true;
    complete_unlock(r);
  }

  void complete(int r) override {
    // invoked by C_notify_Finish
    lock.lock();
    finished = true;
    complete_unlock(r);
  }

  void complete_unlock(int r) {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (acked && finished) {
      lock.unlock();
      cancel = true;
      C_aio_linger_Complete::complete(ret_val);
    } else {
      lock.unlock();
    }
  }
};

struct C_aio_notify_Ack : public Context {
  CephContext *cct;
  C_aio_notify_Complete *oncomplete;

  C_aio_notify_Ack(CephContext *_cct,
                   C_aio_notify_Complete *_oncomplete)
    : cct(_cct), oncomplete(_oncomplete)
  {
  }

  void finish(int r) override
  {
    ldout(cct, 10) << __func__ << " linger op " << oncomplete->linger_op << " "
                   << "acked (" << r << ")" << dendl;
    oncomplete->handle_ack(r);
  }
};

struct C_aio_selfmanaged_snap_op_Complete : public Context {
  librados::RadosClient *client;
  librados::AioCompletionImpl *c;

  C_aio_selfmanaged_snap_op_Complete(librados::RadosClient *client,
                                     librados::AioCompletionImpl *c)
    : client(client), c(c) {
    c->get();
  }

  void finish(int r) override {
    c->lock.lock();
    c->rval = r;
    c->complete = true;
    c->cond.notify_all();

    if (c->callback_complete || c->callback_safe) {
      boost::asio::defer(client->finish_strand, librados::CB_AioComplete(c));
    }
    c->put_unlock();
  }
};

struct C_aio_selfmanaged_snap_create_Complete : public C_aio_selfmanaged_snap_op_Complete {
  snapid_t snapid;
  uint64_t *dest_snapid;

  C_aio_selfmanaged_snap_create_Complete(librados::RadosClient *client,
                                         librados::AioCompletionImpl *c,
                                         uint64_t *dest_snapid)
    : C_aio_selfmanaged_snap_op_Complete(client, c),
      dest_snapid(dest_snapid) {
  }

  void finish(int r) override {
    if (r >= 0) {
      *dest_snapid = snapid;
    }
    C_aio_selfmanaged_snap_op_Complete::finish(r);
  }
};

} // anonymous namespace
} // namespace librados

librados::IoCtxImpl::IoCtxImpl() = default;

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       int64_t poolid, snapid_t s)
  : client(c), poolid(poolid), snap_seq(s),
    notify_timeout(c->cct->_conf->client_notify_timeout),
    oloc(poolid),
    aio_write_seq(0), objecter(objecter)
{
}

void librados::IoCtxImpl::set_snap_read(snapid_t s)
{
  if (!s)
    s = CEPH_NOSNAP;
  ldout(client->cct, 10) << "set snap read " << snap_seq << " -> " << s << dendl;
  snap_seq = s;
}

int librados::IoCtxImpl::set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps)
{
  ::SnapContext n;
  ldout(client->cct, 10) << "set snap write context: seq = " << seq
			 << " and snaps = " << snaps << dendl;
  n.seq = seq;
  n.snaps = snaps;
  if (!n.is_valid())
    return -EINVAL;
  snapc = n;
  return 0;
}

int librados::IoCtxImpl::get_object_hash_position(
    const std::string& oid, uint32_t *hash_position)
{
  int64_t r = objecter->get_object_hash_position(poolid, oid, oloc.nspace);
  if (r < 0)
    return r;
  *hash_position = (uint32_t)r;
  return 0;
}

int librados::IoCtxImpl::get_object_pg_hash_position(
    const std::string& oid, uint32_t *pg_hash_position)
{
  int64_t r = objecter->get_object_pg_hash_position(poolid, oid, oloc.nspace);
  if (r < 0)
    return r;
  *pg_hash_position = (uint32_t)r;
  return 0;
}

void librados::IoCtxImpl::queue_aio_write(AioCompletionImpl *c)
{
  get();
  std::scoped_lock l{aio_write_list_lock};
  ceph_assert(c->io == this);
  c->aio_write_seq = ++aio_write_seq;
  ldout(client->cct, 20) << "queue_aio_write " << this << " completion " << c
			 << " write_seq " << aio_write_seq << dendl;
  aio_write_list.push_back(&c->aio_write_list_item);
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "complete_aio_write " << c << dendl;
  aio_write_list_lock.lock();
  ceph_assert(c->io == this);
  c->aio_write_list_item.remove_myself();

  map<ceph_tid_t, std::list<AioCompletionImpl*> >::iterator waiters = aio_write_waiters.begin();
  while (waiters != aio_write_waiters.end()) {
    if (!aio_write_list.empty() &&
	aio_write_list.front()->aio_write_seq <= waiters->first) {
      ldout(client->cct, 20) << " next outstanding write is " << aio_write_list.front()->aio_write_seq
			     << " <= waiter " << waiters->first
			     << ", stopping" << dendl;
      break;
    }
    ldout(client->cct, 20) << " waking waiters on seq " << waiters->first << dendl;
    for (std::list<AioCompletionImpl*>::iterator it = waiters->second.begin();
	 it != waiters->second.end(); ++it) {
      boost::asio::defer(client->finish_strand, CB_AioCompleteAndSafe(*it));
      (*it)->put();
    }
    aio_write_waiters.erase(waiters++);
  }

  aio_write_cond.notify_all();
  aio_write_list_lock.unlock();
  put();
}

void librados::IoCtxImpl::flush_aio_writes_async(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "flush_aio_writes_async " << this
			 << " completion " << c << dendl;
  std::lock_guard l(aio_write_list_lock);
  ceph_tid_t seq = aio_write_seq;
  if (aio_write_list.empty()) {
    ldout(client->cct, 20) << "flush_aio_writes_async no writes. (tid "
			   << seq << ")" << dendl;
    boost::asio::defer(client->finish_strand, CB_AioCompleteAndSafe(c));
  } else {
    ldout(client->cct, 20) << "flush_aio_writes_async " << aio_write_list.size()
			   << " writes in flight; waiting on tid " << seq << dendl;
    c->get();
    aio_write_waiters[seq].push_back(c);
  }
}

void librados::IoCtxImpl::flush_aio_writes()
{
  ldout(client->cct, 20) << "flush_aio_writes" << dendl;
  std::unique_lock l{aio_write_list_lock};
  aio_write_cond.wait(l, [seq=aio_write_seq, this] {
    return (aio_write_list.empty() ||
	    aio_write_list.front()->aio_write_seq > seq);
  });
}

string librados::IoCtxImpl::get_cached_pool_name()
{
  std::string pn;
  client->pool_get_name(get_id(), &pn);
  return pn;
}

// SNAPS

int librados::IoCtxImpl::snap_create(const char *snapName)
{
  int reply;
  string sName(snapName);

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::snap_create::mylock");
  ceph::condition_variable cond;
  bool done;
  Context *onfinish = new C_SafeCond(mylock, cond, &done, &reply);
  objecter->create_pool_snap(poolid, sName, onfinish);

  std::unique_lock l{mylock};
  cond.wait(l, [&done] { return done; });
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_create(uint64_t *psnapid)
{
  int reply;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::selfmanaged_snap_create::mylock");
  ceph::condition_variable cond;
  bool done;
  Context *onfinish = new C_SafeCond(mylock, cond, &done, &reply);
  snapid_t snapid;
  objecter->allocate_selfmanaged_snap(poolid, &snapid, onfinish);

  {
    std::unique_lock l{mylock};
    cond.wait(l, [&done] { return done; });
  }
  if (reply == 0)
    *psnapid = snapid;
  return reply;
}

void librados::IoCtxImpl::aio_selfmanaged_snap_create(uint64_t *snapid,
                                                      AioCompletionImpl *c)
{
  C_aio_selfmanaged_snap_create_Complete *onfinish =
    new C_aio_selfmanaged_snap_create_Complete(client, c, snapid);
  objecter->allocate_selfmanaged_snap(poolid, &onfinish->snapid,
				      onfinish);
}

int librados::IoCtxImpl::snap_remove(const char *snapName)
{
  int reply;
  string sName(snapName);

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::snap_remove::mylock");
  ceph::condition_variable cond;
  bool done;
  Context *onfinish = new C_SafeCond(mylock, cond, &done, &reply);
  objecter->delete_pool_snap(poolid, sName, onfinish);
  unique_lock l{mylock};
  cond.wait(l, [&done] { return done; });
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_rollback_object(const object_t& oid,
							  ::SnapContext& snapc,
							  uint64_t snapid)
{
  int reply;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::snap_rollback::mylock");
  ceph::condition_variable cond;
  bool done;
  Context *onack = new C_SafeCond(mylock, cond, &done, &reply);

  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.rollback(snapid);
  objecter->mutate(oid, oloc,
		   op, snapc, ceph::real_clock::now(),
		   extra_op_flags,
		   onack, NULL);

  std::unique_lock l{mylock};
  cond.wait(l, [&done] { return done; });
  return reply;
}

int librados::IoCtxImpl::rollback(const object_t& oid, const char *snapName)
{
  snapid_t snap;

  int r = objecter->pool_snap_by_name(poolid, snapName, &snap);
  if (r < 0) {
    return r;
  }

  return selfmanaged_snap_rollback_object(oid, snapc, snap);
}

int librados::IoCtxImpl::selfmanaged_snap_remove(uint64_t snapid)
{
  int reply;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::selfmanaged_snap_remove::mylock");
  ceph::condition_variable cond;
  bool done;
  objecter->delete_selfmanaged_snap(poolid, snapid_t(snapid),
				    new C_SafeCond(mylock, cond, &done, &reply));

  std::unique_lock l{mylock};
  cond.wait(l, [&done] { return done; });
  return (int)reply;
}

void librados::IoCtxImpl::aio_selfmanaged_snap_remove(uint64_t snapid,
                                                      AioCompletionImpl *c)
{
  Context *onfinish = new C_aio_selfmanaged_snap_op_Complete(client, c);
  objecter->delete_selfmanaged_snap(poolid, snapid, onfinish);
}

int librados::IoCtxImpl::snap_list(vector<uint64_t> *snaps)
{
  return objecter->pool_snap_list(poolid, snaps);
}

int librados::IoCtxImpl::snap_lookup(const char *name, uint64_t *snapid)
{
  return objecter->pool_snap_by_name(poolid, name, (snapid_t *)snapid);
}

int librados::IoCtxImpl::snap_get_name(uint64_t snapid, std::string *s)
{
  pool_snap_info_t info;
  int ret = objecter->pool_snap_get_info(poolid, snapid, &info);
  if (ret < 0) {
    return ret;
  }
  *s = info.name.c_str();
  return 0;
}

int librados::IoCtxImpl::snap_get_stamp(uint64_t snapid, time_t *t)
{
  pool_snap_info_t info;
  int ret = objecter->pool_snap_get_info(poolid, snapid, &info);
  if (ret < 0) {
    return ret;
  }
  *t = info.stamp.sec();
  return 0;
}


// IO

int librados::IoCtxImpl::nlist(Objecter::NListContext *context, int max_entries)
{
  bool done;
  int r = 0;
  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::nlist::mylock");
  ceph::condition_variable cond;

  if (context->at_end())
    return 0;

  context->max_entries = max_entries;
  context->nspace = oloc.nspace;

  objecter->list_nobjects(context, new C_SafeCond(mylock, cond, &done, &r));

  std::unique_lock l{mylock};
  cond.wait(l, [&done] { return done; });
  return r;
}

uint32_t librados::IoCtxImpl::nlist_seek(Objecter::NListContext *context,
					uint32_t pos)
{
  context->list.clear();
  return objecter->list_nobjects_seek(context, pos);
}

uint32_t librados::IoCtxImpl::nlist_seek(Objecter::NListContext *context,
                                    const rados_object_list_cursor& cursor)
{
  context->list.clear();
  return objecter->list_nobjects_seek(context, *(const hobject_t *)cursor);
}

rados_object_list_cursor librados::IoCtxImpl::nlist_get_cursor(Objecter::NListContext *context)
{
  hobject_t *c = new hobject_t;

  objecter->list_nobjects_get_cursor(context, c);
  return (rados_object_list_cursor)c;
}

int librados::IoCtxImpl::create(const object_t& oid, bool exclusive)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.create(exclusive);
  return operate(oid, &op, NULL);
}

/*
 * add any version assert operations that are appropriate given the
 * stat in the IoCtx, either the target version assert or any src
 * object asserts.  these affect a single ioctx operation, so clear
 * the ioctx state when we're doing.
 *
 * return a pointer to the ObjectOperation if we added any events;
 * this is convenient for passing the extra_ops argument into Objecter
 * methods.
 */
::ObjectOperation *librados::IoCtxImpl::prepare_assert_ops(::ObjectOperation *op)
{
  ::ObjectOperation *pop = NULL;
  if (assert_ver) {
    op->assert_version(assert_ver);
    assert_ver = 0;
    pop = op;
  }
  return pop;
}

int librados::IoCtxImpl::write(const object_t& oid, bufferlist& bl,
			       size_t len, uint64_t off)
{
  if (len > UINT_MAX/2)
    return -E2BIG;
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op.write(off, mybl);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::append(const object_t& oid, bufferlist& bl, size_t len)
{
  if (len > UINT_MAX/2)
    return -E2BIG;
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op.append(mybl);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::write_full(const object_t& oid, bufferlist& bl)
{
  if (bl.length() > UINT_MAX/2)
    return -E2BIG;
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.write_full(bl);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::writesame(const object_t& oid, bufferlist& bl,
				   size_t write_len, uint64_t off)
{
  if ((bl.length() > UINT_MAX/2) || (write_len > UINT_MAX/2))
    return -E2BIG;
  if ((bl.length() == 0) || (write_len % bl.length()))
    return -EINVAL;
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  bufferlist mybl;
  mybl.substr_of(bl, 0, bl.length());
  op.writesame(off, write_len, mybl);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::operate(const object_t& oid, ::ObjectOperation *o,
				 ceph::real_time *pmtime, int flags)
{
  ceph::real_time ut = (pmtime ? *pmtime :
    ceph::real_clock::now());

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  if (!o->size())
    return 0;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::operate::mylock");
  ceph::condition_variable cond;
  bool done;
  int r;
  version_t ver;

  Context *oncommit = new C_SafeCond(mylock, cond, &done, &r);

  int op = o->ops[0].op.op;
  ldout(client->cct, 10) << ceph_osd_op_name(op) << " oid=" << oid
			 << " nspace=" << oloc.nspace << dendl;
  Objecter::Op *objecter_op = objecter->prepare_mutate_op(
    oid, oloc,
    *o, snapc, ut,
    flags | extra_op_flags,
    oncommit, &ver);
  objecter->op_submit(objecter_op);

  {
    std::unique_lock l{mylock};
    cond.wait(l, [&done] { return done;});
  }
  ldout(client->cct, 10) << "Objecter returned from "
	<< ceph_osd_op_name(op) << " r=" << r << dendl;

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::operate_read(const object_t& oid,
				      ::ObjectOperation *o,
				      bufferlist *pbl,
				      int flags)
{
  if (!o->size())
    return 0;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::operate_read::mylock");
  ceph::condition_variable cond;
  bool done;
  int r;
  version_t ver;

  Context *onack = new C_SafeCond(mylock, cond, &done, &r);

  int op = o->ops[0].op.op;
  ldout(client->cct, 10) << ceph_osd_op_name(op) << " oid=" << oid << " nspace=" << oloc.nspace << dendl;
  Objecter::Op *objecter_op = objecter->prepare_read_op(
    oid, oloc,
    *o, snap_seq, pbl,
    flags | extra_op_flags,
    onack, &ver);
  objecter->op_submit(objecter_op);

  {
    std::unique_lock l{mylock};
    cond.wait(l, [&done] { return done; });
  }
  ldout(client->cct, 10) << "Objecter returned from "
	<< ceph_osd_op_name(op) << " r=" << r << dendl;

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_operate_read(const object_t &oid,
					  ::ObjectOperation *o,
					  AioCompletionImpl *c,
					  int flags,
					  bufferlist *pbl,
                                          const blkin_trace_info *trace_info)
{
  FUNCTRACE(client->cct);
  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;

  ZTracer::Trace trace;
  if (trace_info) {
    ZTracer::Trace parent_trace("", nullptr, trace_info);
    trace.init("rados operate read", &objecter->trace_endpoint, &parent_trace);
  }

  trace.event("init root span");
  Objecter::Op *objecter_op = objecter->prepare_read_op(
    oid, oloc,
    *o, snap_seq, pbl, flags | extra_op_flags,
    oncomplete, &c->objver, nullptr, 0, &trace);
  objecter->op_submit(objecter_op, &c->tid);
  trace.event("rados operate read submitted");

  return 0;
}

int librados::IoCtxImpl::aio_operate(const object_t& oid,
				     ::ObjectOperation *o, AioCompletionImpl *c,
				     const SnapContext& snap_context,
				     const ceph::real_time *pmtime, int flags,
                                     const blkin_trace_info *trace_info)
{
  FUNCTRACE(client->cct);
  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_WRITE_OP_BEGIN");
  const ceph::real_time ut = (pmtime ? *pmtime : ceph::real_clock::now());
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);
#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif

  c->io = this;
  queue_aio_write(c);

  ZTracer::Trace trace;
  if (trace_info) {
    ZTracer::Trace parent_trace("", nullptr, trace_info);
    trace.init("rados operate", &objecter->trace_endpoint, &parent_trace);
  }

  trace.event("init root span");
  Objecter::Op *op = objecter->prepare_mutate_op(
    oid, oloc, *o, snap_context, ut, flags | extra_op_flags,
    oncomplete, &c->objver, osd_reqid_t(), &trace);
  objecter->op_submit(op, &c->tid);
  trace.event("rados operate op submitted");

  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  bufferlist *pbl, size_t len, uint64_t off,
				  uint64_t snapid, const blkin_trace_info *info)
{
  FUNCTRACE(client->cct);
  if (len > (size_t) INT_MAX)
    return -EDOM;

  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_READ_OP_BEGIN");
  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;
  c->blp = pbl;

  ZTracer::Trace trace;
  if (info)
    trace.init("rados read", &objecter->trace_endpoint, info);

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    off, len, snapid, pbl, extra_op_flags,
    oncomplete, &c->objver, nullptr, 0, &trace);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  char *buf, size_t len, uint64_t off,
				  uint64_t snapid, const blkin_trace_info *info)
{
  FUNCTRACE(client->cct);
  if (len > (size_t) INT_MAX)
    return -EDOM;

  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_READ_OP_BEGIN");
  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;
  c->bl.clear();
  c->bl.push_back(buffer::create_static(len, buf));
  c->blp = &c->bl;
  c->out_buf = buf;

  ZTracer::Trace trace;
  if (info)
    trace.init("rados read", &objecter->trace_endpoint, info);

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    off, len, snapid, &c->bl, extra_op_flags,
    oncomplete, &c->objver, nullptr, 0, &trace);
  objecter->op_submit(o, &c->tid);
  return 0;
}

class C_ObjectOperation : public Context {
public:
  ::ObjectOperation m_ops;
  explicit C_ObjectOperation(Context *c) : m_ctx(c) {}
  void finish(int r) override {
    m_ctx->complete(r);
  }
private:
  Context *m_ctx;
};

int librados::IoCtxImpl::aio_sparse_read(const object_t oid,
					 AioCompletionImpl *c,
					 std::map<uint64_t,uint64_t> *m,
					 bufferlist *data_bl, size_t len,
					 uint64_t off, uint64_t snapid)
{
  FUNCTRACE(client->cct);
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *nested = new C_aio_Complete(c);
  C_ObjectOperation *onack = new C_ObjectOperation(nested);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) nested)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;

  onack->m_ops.sparse_read(off, len, m, data_bl, NULL);

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    onack->m_ops, snapid, NULL, extra_op_flags,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_cmpext(const object_t& oid,
				    AioCompletionImpl *c,
				    uint64_t off,
				    bufferlist& cmp_bl)
{
  if (cmp_bl.length() > UINT_MAX/2)
    return -E2BIG;

  Context *onack = new C_aio_Complete(c);

  c->is_read = true;
  c->io = this;

  Objecter::Op *o = objecter->prepare_cmpext_op(
    oid, oloc, off, cmp_bl, snap_seq, extra_op_flags,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

/* use m_ops.cmpext() + prepare_read_op() for non-bufferlist C API */
int librados::IoCtxImpl::aio_cmpext(const object_t& oid,
				    AioCompletionImpl *c,
				    const char *cmp_buf,
				    size_t cmp_len,
				    uint64_t off)
{
  if (cmp_len > UINT_MAX/2)
    return -E2BIG;

  bufferlist cmp_bl;
  cmp_bl.append(cmp_buf, cmp_len);

  Context *nested = new C_aio_Complete(c);
  C_ObjectOperation *onack = new C_ObjectOperation(nested);

  c->is_read = true;
  c->io = this;

  onack->m_ops.cmpext(off, cmp_len, cmp_buf, NULL);

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc, onack->m_ops, snap_seq, NULL, extra_op_flags, onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_write(const object_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off, const blkin_trace_info *info)
{
  FUNCTRACE(client->cct);
  auto ut = ceph::real_clock::now();
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~" << len << " snapc=" << snapc << " snap_seq=" << snap_seq << dendl;
  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_WRITE_OP_BEGIN");

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  ZTracer::Trace trace;
  if (info)
    trace.init("rados write", &objecter->trace_endpoint, info);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_write_op(
    oid, oloc,
    off, len, snapc, bl, ut, extra_op_flags,
    oncomplete, &c->objver, nullptr, 0, &trace);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_append(const object_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  FUNCTRACE(client->cct);
  auto ut = ceph::real_clock::now();

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);
#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_append_op(
    oid, oloc,
    len, snapc, bl, ut, extra_op_flags,
    oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const object_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  FUNCTRACE(client->cct);
  auto ut = ceph::real_clock::now();

  if (bl.length() > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);
#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_write_full_op(
    oid, oloc,
    snapc, bl, ut, extra_op_flags,
    oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_writesame(const object_t &oid,
				       AioCompletionImpl *c,
				       const bufferlist& bl,
				       size_t write_len,
				       uint64_t off)
{
  FUNCTRACE(client->cct);
  auto ut = ceph::real_clock::now();

  if ((bl.length() > UINT_MAX/2) || (write_len > UINT_MAX/2))
    return -E2BIG;
  if ((bl.length() == 0) || (write_len % bl.length()))
    return -EINVAL;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_writesame_op(
    oid, oloc,
    write_len, off,
    snapc, bl, ut, extra_op_flags,
    oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_remove(const object_t &oid, AioCompletionImpl *c, int flags)
{
  FUNCTRACE(client->cct);
  auto ut = ceph::real_clock::now();

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_remove_op(
    oid, oloc,
    snapc, ut, flags | extra_op_flags,
    oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}


int librados::IoCtxImpl::aio_stat(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, time_t *pmtime)
{
  C_aio_stat_Ack *onack = new C_aio_stat_Ack(c, pmtime);
  c->is_read = true;
  c->io = this;
  Objecter::Op *o = objecter->prepare_stat_op(
    oid, oloc,
    snap_seq, psize, &onack->mtime, extra_op_flags,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_stat2(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, struct timespec *pts)
{
  C_aio_stat2_Ack *onack = new C_aio_stat2_Ack(c, pts);
  c->is_read = true;
  c->io = this;
  Objecter::Op *o = objecter->prepare_stat_op(
    oid, oloc,
    snap_seq, psize, &onack->mtime, extra_op_flags,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_getxattr(const object_t& oid, AioCompletionImpl *c,
				      const char *name, bufferlist& bl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.getxattr(name, &bl, NULL);
  int r = aio_operate_read(oid, &rd, c, 0, &bl);
  return r;
}

int librados::IoCtxImpl::aio_rmxattr(const object_t& oid, AioCompletionImpl *c,
				     const char *name)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.rmxattr(name);
  return aio_operate(oid, &op, c, snapc, nullptr, 0);
}

int librados::IoCtxImpl::aio_setxattr(const object_t& oid, AioCompletionImpl *c,
				      const char *name, bufferlist& bl)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.setxattr(name, bl);
  return aio_operate(oid, &op, c, snapc, nullptr, 0);
}

namespace {
struct AioGetxattrsData {
  AioGetxattrsData(librados::AioCompletionImpl *c, map<string, bufferlist>* attrset,
		   librados::RadosClient *_client) :
    user_completion(c), user_attrset(attrset), client(_client) {}
  struct librados::CB_AioCompleteAndSafe user_completion;
  map<string, bufferlist> result_attrset;
  map<std::string, bufferlist>* user_attrset;
  librados::RadosClient *client;
};
}

static void aio_getxattrs_complete(rados_completion_t c, void *arg) {
  AioGetxattrsData *cdata = reinterpret_cast<AioGetxattrsData*>(arg);
  int rc = rados_aio_get_return_value(c);
  cdata->user_attrset->clear();
  if (rc >= 0) {
    for (map<string,bufferlist>::iterator p = cdata->result_attrset.begin();
	 p != cdata->result_attrset.end();
	 ++p) {
      ldout(cdata->client->cct, 10) << "IoCtxImpl::getxattrs: xattr=" << p->first << dendl;
      (*cdata->user_attrset)[p->first] = p->second;
    }
  }
  cdata->user_completion(rc);
  ((librados::AioCompletionImpl*)c)->put();
  delete cdata;
}

int librados::IoCtxImpl::aio_getxattrs(const object_t& oid, AioCompletionImpl *c,
				       map<std::string, bufferlist>& attrset)
{
  AioGetxattrsData *cdata = new AioGetxattrsData(c, &attrset, client);
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.getxattrs(&cdata->result_attrset, NULL);
  librados::AioCompletionImpl *comp = new librados::AioCompletionImpl;
  comp->set_complete_callback(cdata, aio_getxattrs_complete);
  return aio_operate_read(oid, &rd, comp, 0, NULL);
}

int librados::IoCtxImpl::aio_cancel(AioCompletionImpl *c)
{
  return objecter->op_cancel(c->tid, -ECANCELED);
}


int librados::IoCtxImpl::hit_set_list(uint32_t hash, AioCompletionImpl *c,
			      std::list< std::pair<time_t, time_t> > *pls)
{
  Context *oncomplete = new C_aio_Complete(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  rd.hit_set_ls(pls, NULL);
  object_locator_t oloc(poolid);
  Objecter::Op *o = objecter->prepare_pg_read_op(
    hash, oloc, rd, NULL, extra_op_flags, oncomplete, NULL, NULL);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::hit_set_get(uint32_t hash, AioCompletionImpl *c,
				     time_t stamp,
				     bufferlist *pbl)
{
  Context *oncomplete = new C_aio_Complete(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  rd.hit_set_get(ceph::real_clock::from_time_t(stamp), pbl, 0);
  object_locator_t oloc(poolid);
  Objecter::Op *o = objecter->prepare_pg_read_op(
    hash, oloc, rd, NULL, extra_op_flags, oncomplete, NULL, NULL);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::remove(const object_t& oid)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.remove();
  return operate(oid, &op, nullptr, librados::OPERATION_FULL_FORCE);
}

int librados::IoCtxImpl::remove(const object_t& oid, int flags)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.remove();
  return operate(oid, &op, NULL, flags);
}

int librados::IoCtxImpl::trunc(const object_t& oid, uint64_t size)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.truncate(size);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::get_inconsistent_objects(const pg_t& pg,
						  const librados::object_id_t& start_after,
						  uint64_t max_to_get,
						  AioCompletionImpl *c,
						  std::vector<inconsistent_obj_t>* objects,
						  uint32_t* interval)
{
  Context *oncomplete = new C_aio_Complete(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation op;
  op.scrub_ls(start_after, max_to_get, objects, interval, &c->rval);
  object_locator_t oloc{poolid, pg.ps()};
  Objecter::Op *o = objecter->prepare_pg_read_op(
    oloc.hash, oloc, op, nullptr, CEPH_OSD_FLAG_PGOP | extra_op_flags, oncomplete,
    nullptr, nullptr);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::get_inconsistent_snapsets(const pg_t& pg,
						   const librados::object_id_t& start_after,
						   uint64_t max_to_get,
						   AioCompletionImpl *c,
						   std::vector<inconsistent_snapset_t>* snapsets,
						   uint32_t* interval)
{
  Context *oncomplete = new C_aio_Complete(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation op;
  op.scrub_ls(start_after, max_to_get, snapsets, interval, &c->rval);
  object_locator_t oloc{poolid, pg.ps()};
  Objecter::Op *o = objecter->prepare_pg_read_op(
    oloc.hash, oloc, op, nullptr, CEPH_OSD_FLAG_PGOP | extra_op_flags, oncomplete,
    nullptr, nullptr);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::tmap_update(const object_t& oid, bufferlist& cmdbl)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.tmap_update(cmdbl);
  return operate(oid, &wr, NULL);
}

int librados::IoCtxImpl::exec(const object_t& oid,
			      const char *cls, const char *method,
			      bufferlist& inbl, bufferlist& outbl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  return operate_read(oid, &rd, &outbl);
}

int librados::IoCtxImpl::aio_exec(const object_t& oid, AioCompletionImpl *c,
				  const char *cls, const char *method,
				  bufferlist& inbl, bufferlist *outbl)
{
  FUNCTRACE(client->cct);
  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc, rd, snap_seq, outbl, extra_op_flags, oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_exec(const object_t& oid, AioCompletionImpl *c,
				  const char *cls, const char *method,
				  bufferlist& inbl, char *buf, size_t out_len)
{
  FUNCTRACE(client->cct);
  Context *oncomplete = new C_aio_Complete(c);

#if defined(WITH_EVENTTRACE)
  ((C_aio_Complete *) oncomplete)->oid = oid;
#endif
  c->is_read = true;
  c->io = this;
  c->bl.clear();
  c->bl.push_back(buffer::create_static(out_len, buf));
  c->blp = &c->bl;
  c->out_buf = buf;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc, rd, snap_seq, &c->bl, extra_op_flags, oncomplete, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::read(const object_t& oid,
			      bufferlist& bl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;
  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_READ_OP_BEGIN");

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.read(off, len, &bl, NULL, NULL);
  int r = operate_read(oid, &rd, &bl);
  if (r < 0)
    return r;

  if (bl.length() < len) {
    ldout(client->cct, 10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int librados::IoCtxImpl::cmpext(const object_t& oid, uint64_t off,
                                bufferlist& cmp_bl)
{
  if (cmp_bl.length() > UINT_MAX/2)
    return -E2BIG;

  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.cmpext(off, cmp_bl, NULL);
  return operate_read(oid, &op, NULL);
}

int librados::IoCtxImpl::mapext(const object_t& oid,
				uint64_t off, size_t len,
				std::map<uint64_t,uint64_t>& m)
{
  bufferlist bl;

  ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::read::mylock");
  ceph::condition_variable cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(mylock, cond, &done, &r);

  objecter->mapext(oid, oloc,
		   off, len, snap_seq, &bl, extra_op_flags,
		   onack);

  {
    unique_lock l{mylock};
    cond.wait(l, [&done] { return done;});
  }
  ldout(client->cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  auto iter = bl.cbegin();
  decode(m, iter);

  return m.size();
}

int librados::IoCtxImpl::sparse_read(const object_t& oid,
				     std::map<uint64_t,uint64_t>& m,
				     bufferlist& data_bl, size_t len,
				     uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.sparse_read(off, len, &m, &data_bl, NULL);

  int r = operate_read(oid, &rd, NULL);
  if (r < 0)
    return r;

  return m.size();
}

int librados::IoCtxImpl::checksum(const object_t& oid, uint8_t type,
				  const bufferlist &init_value, size_t len,
				  uint64_t off, size_t chunk_size,
				  bufferlist *pbl)
{
  if (len > (size_t) INT_MAX) {
    return -EDOM;
  }

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.checksum(type, init_value, off, len, chunk_size, pbl, nullptr, nullptr);

  int r = operate_read(oid, &rd, nullptr);
  if (r < 0) {
    return r;
  }

  return 0;
}

int librados::IoCtxImpl::stat(const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  uint64_t size;
  real_time mtime;

  if (!psize)
    psize = &size;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.stat(psize, &mtime, nullptr);
  int r = operate_read(oid, &rd, NULL);

  if (r >= 0 && pmtime) {
    *pmtime = real_clock::to_time_t(mtime);
  }

  return r;
}

int librados::IoCtxImpl::stat2(const object_t& oid, uint64_t *psize, struct timespec *pts)
{
  uint64_t size;
  ceph::real_time mtime;

  if (!psize)
    psize = &size;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.stat(psize, &mtime, nullptr);
  int r = operate_read(oid, &rd, NULL);
  if (r < 0) {
    return r;
  }

  if (pts) {
    *pts = ceph::real_clock::to_timespec(mtime);
  }

  return 0;
}

int librados::IoCtxImpl::getxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.getxattr(name, &bl, NULL);
  int r = operate_read(oid, &rd, &bl);
  if (r < 0)
    return r;

  return bl.length();
}

int librados::IoCtxImpl::rmxattr(const object_t& oid, const char *name)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.rmxattr(name);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::setxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.setxattr(name, bl);
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::getxattrs(const object_t& oid,
				     map<std::string, bufferlist>& attrset)
{
  map<string, bufferlist> aset;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.getxattrs(&aset, NULL);
  int r = operate_read(oid, &rd, NULL);

  attrset.clear();
  if (r >= 0) {
    for (map<string,bufferlist>::iterator p = aset.begin(); p != aset.end(); ++p) {
      ldout(client->cct, 10) << "IoCtxImpl::getxattrs: xattr=" << p->first << dendl;
      attrset[p->first.c_str()] = p->second;
    }
  }

  return r;
}

void librados::IoCtxImpl::set_sync_op_version(version_t ver)
{
  ANNOTATE_BENIGN_RACE_SIZED(&last_objver, sizeof(last_objver),
                             "IoCtxImpl last_objver");
  last_objver = ver;
}

namespace librados {
void intrusive_ptr_add_ref(IoCtxImpl *p) { p->get(); }
void intrusive_ptr_release(IoCtxImpl *p) { p->put(); }
}

struct WatchInfo {
  boost::intrusive_ptr<librados::IoCtxImpl> ioctx;
  object_t oid;
  librados::WatchCtx *ctx;
  librados::WatchCtx2 *ctx2;

  WatchInfo(librados::IoCtxImpl *io, object_t o,
	    librados::WatchCtx *c, librados::WatchCtx2 *c2)
    : ioctx(io), oid(o), ctx(c), ctx2(c2) {}

  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist& bl) {
    ldout(ioctx->client->cct, 10) << __func__ << " " << notify_id
				  << " cookie " << cookie
				  << " notifier_id " << notifier_id
				  << " len " << bl.length()
				  << dendl;

    if (ctx2)
      ctx2->handle_notify(notify_id, cookie, notifier_id, bl);
    if (ctx) {
      ctx->notify(0, 0, bl);

      // send ACK back to OSD if using legacy protocol
      bufferlist empty;
      ioctx->notify_ack(oid, notify_id, cookie, empty);
    }
  }
  void handle_error(uint64_t cookie, int err) {
    ldout(ioctx->client->cct, 10) << __func__ << " cookie " << cookie
				  << " err " << err
				  << dendl;
    if (ctx2)
      ctx2->handle_error(cookie, err);
  }

  void operator()(bs::error_code ec,
		  uint64_t notify_id,
		  uint64_t cookie,
		  uint64_t notifier_id,
		  bufferlist&& bl) {
    if (ec) {
      handle_error(cookie, ceph::from_error_code(ec));
    } else {
      handle_notify(notify_id, cookie, notifier_id, bl);
    }
  }
};

// internal WatchInfo that owns the context memory
struct InternalWatchInfo : public WatchInfo {
  std::unique_ptr<librados::WatchCtx> ctx;
  std::unique_ptr<librados::WatchCtx2> ctx2;

  InternalWatchInfo(librados::IoCtxImpl *io, object_t o,
                    librados::WatchCtx *c, librados::WatchCtx2 *c2)
    : WatchInfo(io, o, c, c2), ctx(c), ctx2(c2) {}
};

int librados::IoCtxImpl::watch(const object_t& oid, uint64_t *handle,
                               librados::WatchCtx *ctx,
                               librados::WatchCtx2 *ctx2,
                               bool internal)
{
  return watch(oid, handle, ctx, ctx2, 0, internal);
}

int librados::IoCtxImpl::watch(const object_t& oid, uint64_t *handle,
                               librados::WatchCtx *ctx,
                               librados::WatchCtx2 *ctx2,
                               uint32_t timeout,
                               bool internal)
{
  ::ObjectOperation wr;
  version_t objver;
  C_SaferCond onfinish;

  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc,
                                                            extra_op_flags);
  *handle = linger_op->get_cookie();
  if (internal) {
    linger_op->handle = InternalWatchInfo(this, oid, ctx, ctx2);
  } else {
    linger_op->handle = WatchInfo(this, oid, ctx, ctx2);
  }
  prepare_assert_ops(&wr);
  wr.watch(*handle, CEPH_OSD_WATCH_OP_WATCH, timeout);
  bufferlist bl;
  objecter->linger_watch(linger_op, wr,
			 snapc, ceph::real_clock::now(), bl,
			 &onfinish,
			 &objver);

  int r = onfinish.wait();

  set_sync_op_version(objver);

  if (r < 0) {
    objecter->linger_cancel(linger_op);
    *handle = 0;
  }

  return r;
}

int librados::IoCtxImpl::aio_watch(const object_t& oid,
                                   AioCompletionImpl *c,
                                   uint64_t *handle,
                                   librados::WatchCtx *ctx,
                                   librados::WatchCtx2 *ctx2,
                                   bool internal) {
  return aio_watch(oid, c, handle, ctx, ctx2, 0, internal);
}

int librados::IoCtxImpl::aio_watch(const object_t& oid,
                                   AioCompletionImpl *c,
                                   uint64_t *handle,
                                   librados::WatchCtx *ctx,
                                   librados::WatchCtx2 *ctx2,
                                   uint32_t timeout,
                                   bool internal)
{
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc,
                                                            extra_op_flags);
  c->io = this;
  Context *oncomplete = new C_aio_linger_Complete(c, linger_op, false);

  ::ObjectOperation wr;
  *handle = linger_op->get_cookie();
  if (internal) {
    linger_op->handle = InternalWatchInfo(this, oid, ctx, ctx2);
  } else {
    linger_op->handle = WatchInfo(this, oid, ctx, ctx2);
  }

  prepare_assert_ops(&wr);
  wr.watch(*handle, CEPH_OSD_WATCH_OP_WATCH, timeout);
  bufferlist bl;
  objecter->linger_watch(linger_op, wr,
                         snapc, ceph::real_clock::now(), bl,
                         oncomplete, &c->objver);

  return 0;
}


int librados::IoCtxImpl::notify_ack(
  const object_t& oid,
  uint64_t notify_id,
  uint64_t cookie,
  bufferlist& bl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.notify_ack(notify_id, cookie, bl);
  objecter->read(oid, oloc, rd, snap_seq, (bufferlist*)NULL, extra_op_flags, 0, 0);
  return 0;
}

int librados::IoCtxImpl::watch_check(uint64_t cookie)
{
  auto linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  auto r = objecter->linger_check(linger_op);
  if (r)
    return 1 + std::chrono::duration_cast<
      std::chrono::milliseconds>(*r).count();
  else
    return ceph::from_error_code(r.error());
}

int librados::IoCtxImpl::unwatch(uint64_t cookie)
{
  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  C_SaferCond onfinish;
  version_t ver = 0;

  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  objecter->mutate(linger_op->target.base_oid, oloc, wr,
		   snapc, ceph::real_clock::now(), extra_op_flags,
		   &onfinish, &ver);
  objecter->linger_cancel(linger_op);

  int r = onfinish.wait();
  set_sync_op_version(ver);
  return r;
}

int librados::IoCtxImpl::aio_unwatch(uint64_t cookie, AioCompletionImpl *c)
{
  c->io = this;
  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  Context *oncomplete = new C_aio_linger_Complete(c, linger_op, true);

  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.watch(cookie, CEPH_OSD_WATCH_OP_UNWATCH);
  objecter->mutate(linger_op->target.base_oid, oloc, wr,
		   snapc, ceph::real_clock::now(), extra_op_flags,
		   oncomplete, &c->objver);
  return 0;
}

int librados::IoCtxImpl::notify(const object_t& oid, bufferlist& bl,
				uint64_t timeout_ms,
				bufferlist *preply_bl,
				char **preply_buf, size_t *preply_buf_len)
{
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc,
                                                            extra_op_flags);

  C_SaferCond notify_finish_cond;
  auto e = boost::asio::prefer(
    objecter->service.get_executor(),
    boost::asio::execution::outstanding_work.tracked);
  linger_op->on_notify_finish =
    boost::asio::bind_executor(
      std::move(e),
      CB_notify_Finish(client->cct, &notify_finish_cond,
                       objecter, linger_op, preply_bl,
                       preply_buf, preply_buf_len));
  uint32_t timeout = notify_timeout;
  if (timeout_ms)
    timeout = timeout_ms / 1000;

  // Construct RADOS op
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  bufferlist inbl;
  rd.notify(linger_op->get_cookie(), 1, timeout, bl, &inbl);

  // Issue RADOS op
  C_SaferCond onack;
  version_t objver;
  objecter->linger_notify(linger_op,
			  rd, snap_seq, inbl, NULL,
			  &onack, &objver);

  ldout(client->cct, 10) << __func__ << " issued linger op " << linger_op << dendl;
  int r = onack.wait();
  ldout(client->cct, 10) << __func__ << " linger op " << linger_op
			 << " acked (" << r << ")" << dendl;

  if (r == 0) {
    ldout(client->cct, 10) << __func__ << " waiting for watch_notify finish "
			   << linger_op << dendl;
    r = notify_finish_cond.wait();

  } else {
    ldout(client->cct, 10) << __func__ << " failed to initiate notify, r = "
			   << r << dendl;
    notify_finish_cond.wait();
  }

  objecter->linger_cancel(linger_op);

  set_sync_op_version(objver);
  return r;
}

int librados::IoCtxImpl::aio_notify(const object_t& oid, AioCompletionImpl *c,
                                    bufferlist& bl, uint64_t timeout_ms,
                                    bufferlist *preply_bl, char **preply_buf,
                                    size_t *preply_buf_len)
{
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc,
                                                            extra_op_flags);

  c->io = this;

  C_aio_notify_Complete *oncomplete = new C_aio_notify_Complete(c, linger_op);
  auto e = boost::asio::prefer(
    objecter->service.get_executor(),
    boost::asio::execution::outstanding_work.tracked);
  linger_op->on_notify_finish =
    boost::asio::bind_executor(
      std::move(e),
      CB_notify_Finish(client->cct, oncomplete,
                       objecter, linger_op,
                       preply_bl, preply_buf,
                       preply_buf_len));
  Context *onack = new C_aio_notify_Ack(client->cct, oncomplete);

  uint32_t timeout = notify_timeout;
  if (timeout_ms)
    timeout = timeout_ms / 1000;

  // Construct RADOS op
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  bufferlist inbl;
  rd.notify(linger_op->get_cookie(), 1, timeout, bl, &inbl);

  // Issue RADOS op
  objecter->linger_notify(linger_op,
			  rd, snap_seq, inbl, NULL,
			  onack, &c->objver);
  return 0;
}

int librados::IoCtxImpl::set_alloc_hint(const object_t& oid,
                                        uint64_t expected_object_size,
                                        uint64_t expected_write_size,
					uint32_t flags)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.set_alloc_hint(expected_object_size, expected_write_size, flags);
  return operate(oid, &wr, NULL);
}

version_t librados::IoCtxImpl::last_version()
{
  return last_objver;
}

void librados::IoCtxImpl::set_assert_version(uint64_t ver)
{
  assert_ver = ver;
}

void librados::IoCtxImpl::set_notify_timeout(uint32_t timeout)
{
  notify_timeout = timeout;
}

int librados::IoCtxImpl::cache_pin(const object_t& oid)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.cache_pin();
  return operate(oid, &wr, NULL);
}

int librados::IoCtxImpl::cache_unpin(const object_t& oid)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.cache_unpin();
  return operate(oid, &wr, NULL);
}


///////////////////////////// C_aio_stat_Ack ////////////////////////////

librados::IoCtxImpl::C_aio_stat_Ack::C_aio_stat_Ack(AioCompletionImpl *_c,
						    time_t *pm)
   : c(_c), pmtime(pm)
{
  ceph_assert(!c->io);
  c->get();
}

void librados::IoCtxImpl::C_aio_stat_Ack::finish(int r)
{
  c->lock.lock();
  c->rval = r;
  c->complete = true;
  c->cond.notify_all();

  if (r >= 0 && pmtime) {
    *pmtime = real_clock::to_time_t(mtime);
  }

  if (c->callback_complete) {
    boost::asio::defer(c->io->client->finish_strand, CB_AioComplete(c));
  }

  c->put_unlock();
}

///////////////////////////// C_aio_stat2_Ack ////////////////////////////

librados::IoCtxImpl::C_aio_stat2_Ack::C_aio_stat2_Ack(AioCompletionImpl *_c,
						     struct timespec *pt)
   : c(_c), pts(pt)
{
  ceph_assert(!c->io);
  c->get();
}

void librados::IoCtxImpl::C_aio_stat2_Ack::finish(int r)
{
  c->lock.lock();
  c->rval = r;
  c->complete = true;
  c->cond.notify_all();

  if (r >= 0 && pts) {
    *pts = real_clock::to_timespec(mtime);
  }

  if (c->callback_complete) {
    boost::asio::defer(c->io->client->finish_strand, CB_AioComplete(c));
  }

  c->put_unlock();
}

//////////////////////////// C_aio_Complete ////////////////////////////////

librados::IoCtxImpl::C_aio_Complete::C_aio_Complete(AioCompletionImpl *_c)
  : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_Complete::finish(int r)
{
  c->lock.lock();
  // Leave an existing rval unless r != 0
  if (r)
    c->rval = r; // This clears the error set in C_ObjectOperation_scrub_ls::finish()
  c->complete = true;
  c->cond.notify_all();

  if (r == 0 && c->blp && c->blp->length() > 0) {
    if (c->out_buf && !c->blp->is_contiguous()) {
      c->rval = -ERANGE;
    } else {
      if (c->out_buf && !c->blp->is_provided_buffer(c->out_buf))
        c->blp->begin().copy(c->blp->length(), c->out_buf);

      c->rval = c->blp->length();
    }
  }

  if (c->callback_complete ||
      c->callback_safe) {
    boost::asio::defer(c->io->client->finish_strand, CB_AioComplete(c));
  }

  if (c->aio_write_seq) {
    c->io->complete_aio_write(c);
  }

#if defined(WITH_EVENTTRACE)
  OID_EVENT_TRACE(oid.name.c_str(), "RADOS_OP_COMPLETE");
#endif
  c->put_unlock();
}

void librados::IoCtxImpl::object_list_slice(
  const hobject_t start,
  const hobject_t finish,
  const size_t n,
  const size_t m,
  hobject_t *split_start,
  hobject_t *split_finish)
{
  if (start.is_max()) {
    *split_start = hobject_t::get_max();
    *split_finish = hobject_t::get_max();
    return;
  }

  uint64_t start_hash = hobject_t::_reverse_bits(start.get_hash());
  uint64_t finish_hash =
    finish.is_max() ? 0x100000000 :
    hobject_t::_reverse_bits(finish.get_hash());

  uint64_t diff = finish_hash - start_hash;
  uint64_t rev_start = start_hash + (diff * n / m);
  uint64_t rev_finish = start_hash + (diff * (n + 1) / m);
  if (n == 0) {
    *split_start = start;
  } else {
    *split_start = hobject_t(
      object_t(), string(), CEPH_NOSNAP,
      hobject_t::_reverse_bits(rev_start), poolid, string());
  }

  if (n == m - 1)
    *split_finish = finish;
  else if (rev_finish >= 0x100000000)
    *split_finish = hobject_t::get_max();
  else
    *split_finish = hobject_t(
      object_t(), string(), CEPH_NOSNAP,
      hobject_t::_reverse_bits(rev_finish), poolid, string());
}

int librados::IoCtxImpl::application_enable(const std::string& app_name,
                                            bool force)
{
  auto c = new PoolAsyncCompletionImpl();
  application_enable_async(app_name, force, c);

  int r = c->wait();
  ceph_assert(r == 0);

  r = c->get_return_value();
  c->release();
  c->put();
  if (r < 0) {
    return r;
  }

  return client->wait_for_latest_osdmap();
}

void librados::IoCtxImpl::application_enable_async(const std::string& app_name,
                                                   bool force,
                                                   PoolAsyncCompletionImpl *c)
{
  // pre-Luminous clusters will return -EINVAL and application won't be
  // preserved until Luminous is configured as minimim version.
  if (!client->get_required_monitor_features().contains_all(
        ceph::features::mon::FEATURE_LUMINOUS)) {
    boost::asio::defer(client->finish_strand,
		       [cb = CB_PoolAsync_Safe(c)]() mutable {
			 cb(-EOPNOTSUPP);
		       });
    return;
  }

  std::stringstream cmd;
  cmd << "{"
      << "\"prefix\": \"osd pool application enable\","
      << "\"pool\": \"" << get_cached_pool_name() << "\","
      << "\"app\": \"" << app_name << "\"";
  if (force) {
    cmd << ",\"yes_i_really_mean_it\": true";
  }
  cmd << "}";

  std::vector<std::string> cmds;
  cmds.push_back(cmd.str());
  bufferlist inbl;
  client->mon_command_async(cmds, inbl, nullptr, nullptr,
                            make_lambda_context(CB_PoolAsync_Safe(c)));
}

int librados::IoCtxImpl::application_list(std::set<std::string> *app_names)
{
  int r = 0;
  app_names->clear();
  objecter->with_osdmap([&](const OSDMap& o) {
      auto pg_pool = o.get_pg_pool(poolid);
      if (pg_pool == nullptr) {
	r = -ENOENT;
        return;
      }

      for (auto &pair : pg_pool->application_metadata) {
        app_names->insert(pair.first);
      }
    });
  return r;
}

int librados::IoCtxImpl::application_metadata_get(const std::string& app_name,
                                                  const std::string &key,
                                                  std::string* value)
{
  int r = 0;
  objecter->with_osdmap([&](const OSDMap& o) {
      auto pg_pool = o.get_pg_pool(poolid);
      if (pg_pool == nullptr) {
	r = -ENOENT;
        return;
      }

      auto app_it = pg_pool->application_metadata.find(app_name);
      if (app_it == pg_pool->application_metadata.end()) {
        r = -ENOENT;
        return;
      }

      auto it = app_it->second.find(key);
      if (it == app_it->second.end()) {
        r = -ENOENT;
        return;
      }

      *value = it->second;
    });
  return r;
}

int librados::IoCtxImpl::application_metadata_set(const std::string& app_name,
                                                  const std::string &key,
                                                  const std::string& value)
{
  std::stringstream cmd;
  cmd << "{"
      << "\"prefix\":\"osd pool application set\","
      << "\"pool\":\"" << get_cached_pool_name() << "\","
      << "\"app\":\"" << app_name << "\","
      << "\"key\":\"" << key << "\","
      << "\"value\":\"" << value << "\""
      << "}";

  std::vector<std::string> cmds;
  cmds.push_back(cmd.str());
  bufferlist inbl;
  int r = client->mon_command(cmds, inbl, nullptr, nullptr);
  if (r < 0) {
    return r;
  }

  // ensure we have the latest osd map epoch before proceeding
  return client->wait_for_latest_osdmap();
}

int librados::IoCtxImpl::application_metadata_remove(const std::string& app_name,
                                                     const std::string &key)
{
  std::stringstream cmd;
  cmd << "{"
      << "\"prefix\":\"osd pool application rm\","
      << "\"pool\":\"" << get_cached_pool_name() << "\","
      << "\"app\":\"" << app_name << "\","
      << "\"key\":\"" << key << "\""
      << "}";

  std::vector<std::string> cmds;
  cmds.push_back(cmd.str());
  bufferlist inbl;
  int r = client->mon_command(cmds, inbl, nullptr, nullptr);
  if (r < 0) {
    return r;
  }

  // ensure we have the latest osd map epoch before proceeding
  return client->wait_for_latest_osdmap();
}

int librados::IoCtxImpl::application_metadata_list(const std::string& app_name,
                                                   std::map<std::string, std::string> *values)
{
  int r = 0;
  values->clear();
  objecter->with_osdmap([&](const OSDMap& o) {
      auto pg_pool = o.get_pg_pool(poolid);
      if (pg_pool == nullptr) {
        r = -ENOENT;
        return;
      }

      auto it = pg_pool->application_metadata.find(app_name);
      if (it == pg_pool->application_metadata.end()) {
        r = -ENOENT;
        return;
      }

      *values = it->second;
    });
  return r;
}

