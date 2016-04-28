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

#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include "include/assert.h"
#include "common/valgrind.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

namespace librados {
namespace {

struct C_notify_Finish : public Context {
  CephContext *cct;
  Context *ctx;
  Objecter *objecter;
  Objecter::LingerOp *linger_op;
  bufferlist reply_bl;
  bufferlist *preply_bl;
  char **preply_buf;
  size_t *preply_buf_len;

  C_notify_Finish(CephContext *_cct, Context *_ctx, Objecter *_objecter,
                  Objecter::LingerOp *_linger_op, bufferlist *_preply_bl,
                  char **_preply_buf, size_t *_preply_buf_len)
    : cct(_cct), ctx(_ctx), objecter(_objecter), linger_op(_linger_op),
      preply_bl(_preply_bl), preply_buf(_preply_buf),
      preply_buf_len(_preply_buf_len)
  {
    linger_op->on_notify_finish = this;
    linger_op->notify_result_bl = &reply_bl;
  }

  virtual void finish(int r)
  {
    ldout(cct, 10) << __func__ << " completed notify (linger op "
                   << linger_op << "), r = " << r << dendl;

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
      preply_bl->claim(reply_bl);

    ctx->complete(r);
  }
};

struct C_aio_linger_cancel : public Context {
  Objecter *objecter;
  Objecter::LingerOp *linger_op;

  C_aio_linger_cancel(Objecter *_objecter, Objecter::LingerOp *_linger_op)
    : objecter(_objecter), linger_op(_linger_op)
  {
  }

  virtual void finish(int r)
  {
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

  virtual void finish(int r) {
    if (cancel || r < 0)
      c->io->client->finisher.queue(new C_aio_linger_cancel(c->io->objecter,
                                                            linger_op));

    c->lock.Lock();
    c->rval = r;
    c->ack = true;
    c->safe = true;
    c->cond.Signal();

    if (c->callback_complete) {
      c->io->client->finisher.queue(new C_AioComplete(c));
    }
    if (c->callback_safe) {
      c->io->client->finisher.queue(new C_AioSafe(c));
    }
    c->put_unlock();
  }
};

struct C_aio_notify_Complete : public C_aio_linger_Complete {
  Mutex lock;
  bool acked = false;
  bool finished = false;
  int ret_val = 0;

  C_aio_notify_Complete(AioCompletionImpl *_c, Objecter::LingerOp *_linger_op)
    : C_aio_linger_Complete(_c, _linger_op, false),
      lock("C_aio_notify_Complete::lock") {
  }

  void handle_ack(int r) {
    // invoked by C_aio_notify_Ack
    lock.Lock();
    acked = true;
    complete_unlock(r);
  }

  virtual void complete(int r) override {
    // invoked by C_notify_Finish (or C_aio_notify_Ack on failure)
    lock.Lock();
    finished = true;
    complete_unlock(r);
  }

  void complete_unlock(int r) {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (acked && finished) {
      lock.Unlock();
      cancel = true;
      C_aio_linger_Complete::complete(ret_val);
    } else {
      lock.Unlock();
    }
  }
};

struct C_aio_notify_Ack : public Context {
  CephContext *cct;
  C_notify_Finish *onfinish;
  C_aio_notify_Complete *oncomplete;

  C_aio_notify_Ack(CephContext *_cct, C_notify_Finish *_onfinish,
                   C_aio_notify_Complete *_oncomplete)
    : cct(_cct), onfinish(_onfinish), oncomplete(_oncomplete)
  {
  }

  virtual void finish(int r)
  {
    ldout(cct, 10) << __func__ << " linger op " << oncomplete->linger_op << " "
                   << "acked (" << r << ")" << dendl;
    oncomplete->handle_ack(r);
    if (r < 0) {
      // on failure, we won't expect to see a notify_finish callback
      onfinish->complete(r);
    }
  }
};

} // anonymous namespace
} // namespace librados

librados::IoCtxImpl::IoCtxImpl() :
  ref_cnt(0), client(NULL), poolid(0), assert_ver(0), last_objver(0),
  notify_timeout(30), aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
  aio_write_seq(0), cached_pool_names_lock("librados::IoCtxImpl::cached_pool_names_lock"),
  objecter(NULL)
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       int64_t poolid, snapid_t s)
  : ref_cnt(0), client(c), poolid(poolid), snap_seq(s),
    assert_ver(0), last_objver(0),
    notify_timeout(c->cct->_conf->client_notify_timeout),
    oloc(poolid), aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
    aio_write_seq(0), cached_pool_names_lock("librados::IoCtxImpl::cached_pool_names_lock"),
    objecter(objecter)
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
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->aio_write_seq = ++aio_write_seq;
  ldout(client->cct, 20) << "queue_aio_write " << this << " completion " << c
			 << " write_seq " << aio_write_seq << dendl;
  aio_write_list.push_back(&c->aio_write_list_item);
  aio_write_list_lock.Unlock();
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "complete_aio_write " << c << dendl;
  aio_write_list_lock.Lock();
  assert(c->io == this);
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
      client->finisher.queue(new C_AioCompleteAndSafe(*it));
      (*it)->put();
    }
    aio_write_waiters.erase(waiters++);
  }

  aio_write_cond.Signal();
  aio_write_list_lock.Unlock();
  put();
}

void librados::IoCtxImpl::flush_aio_writes_async(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "flush_aio_writes_async " << this
			 << " completion " << c << dendl;
  Mutex::Locker l(aio_write_list_lock);
  ceph_tid_t seq = aio_write_seq;
  if (aio_write_list.empty()) {
    ldout(client->cct, 20) << "flush_aio_writes_async no writes. (tid "
			   << seq << ")" << dendl;
    client->finisher.queue(new C_AioCompleteAndSafe(c));
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
  aio_write_list_lock.Lock();
  ceph_tid_t seq = aio_write_seq;
  while (!aio_write_list.empty() &&
	 aio_write_list.front()->aio_write_seq <= seq)
    aio_write_cond.Wait(aio_write_list_lock);
  aio_write_list_lock.Unlock();
}

const string& librados::IoCtxImpl::get_cached_pool_name()
{
  std::string pn;
  client->pool_get_name(get_id(), &pn);

  Mutex::Locker l(cached_pool_names_lock);

  if (cached_pool_names.empty() || cached_pool_names.back() != pn)
    cached_pool_names.push_back(pn);

  return cached_pool_names.back();
}

// SNAPS

int librados::IoCtxImpl::snap_create(const char *snapName)
{
  int reply;
  string sName(snapName);

  Mutex mylock ("IoCtxImpl::snap_create::mylock");
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  reply = objecter->create_pool_snap(poolid, sName, onfinish);

  if (reply < 0) {
    delete onfinish;
  } else {
    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();
  }
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_create(uint64_t *psnapid)
{
  int reply;

  Mutex mylock("IoCtxImpl::selfmanaged_snap_create::mylock");
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  snapid_t snapid;
  reply = objecter->allocate_selfmanaged_snap(poolid, &snapid, onfinish);

  if (reply < 0) {
    delete onfinish;
  } else {
    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();
    if (reply == 0)
      *psnapid = snapid;
  }
  return reply;
}

int librados::IoCtxImpl::snap_remove(const char *snapName)
{
  int reply;
  string sName(snapName);

  Mutex mylock ("IoCtxImpl::snap_remove::mylock");
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  reply = objecter->delete_pool_snap(poolid, sName, onfinish);

  if (reply < 0) {
    delete onfinish; 
  } else {
    mylock.Lock();
    while(!done)
      cond.Wait(mylock);
    mylock.Unlock();
  }
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_rollback_object(const object_t& oid,
							  ::SnapContext& snapc,
							  uint64_t snapid)
{
  int reply;

  Mutex mylock("IoCtxImpl::snap_rollback::mylock");
  Cond cond;
  bool done;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &reply);

  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.rollback(snapid);
  objecter->mutate(oid, oloc,
		   op, snapc, ceph::real_clock::now(client->cct), 0,
		   onack, NULL, NULL);

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
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

  Mutex mylock("IoCtxImpl::selfmanaged_snap_remove::mylock");
  Cond cond;
  bool done;
  objecter->delete_selfmanaged_snap(poolid, snapid_t(snapid),
				    new C_SafeCond(&mylock, &cond, &done, &reply));

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return (int)reply;
}

int librados::IoCtxImpl::pool_change_auid(unsigned long long auid)
{
  int reply;

  Mutex mylock("IoCtxImpl::pool_change_auid::mylock");
  Cond cond;
  bool done;
  objecter->change_pool_auid(poolid,
			     new C_SafeCond(&mylock, &cond, &done, &reply),
			     auid);

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::IoCtxImpl::pool_change_auid_async(unsigned long long auid,
						  PoolAsyncCompletionImpl *c)
{
  objecter->change_pool_auid(poolid,
			     new C_PoolAsync_Safe(c),
			     auid);
  return 0;
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
  Cond cond;
  bool done;
  int r = 0;
  Mutex mylock("IoCtxImpl::nlist::mylock");

  if (context->at_end())
    return 0;

  context->max_entries = max_entries;
  context->nspace = oloc.nspace;

  objecter->list_nobjects(context, new C_SafeCond(&mylock, &cond, &done, &r));

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

uint32_t librados::IoCtxImpl::nlist_seek(Objecter::NListContext *context,
					uint32_t pos)
{
  context->list.clear();
  return objecter->list_nobjects_seek(context, pos);
}

int librados::IoCtxImpl::list(Objecter::ListContext *context, int max_entries)
{
  Cond cond;
  bool done;
  int r = 0;
  Mutex mylock("IoCtxImpl::list::mylock");

  if (context->at_end())
    return 0;

  context->max_entries = max_entries;
  context->nspace = oloc.nspace;

  objecter->list_objects(context, new C_SafeCond(&mylock, &cond, &done, &r));

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

uint32_t librados::IoCtxImpl::list_seek(Objecter::ListContext *context,
					uint32_t pos)
{
  context->list.clear();
  return objecter->list_objects_seek(context, pos);
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
  while (!assert_src_version.empty()) {
    map<object_t,uint64_t>::iterator p = assert_src_version.begin();
    op->assert_src_version(p->first, CEPH_NOSNAP, p->second);
    assert_src_version.erase(p);
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

int librados::IoCtxImpl::clone_range(const object_t& dst_oid,
				     uint64_t dst_offset,
				     const object_t& src_oid,
				     uint64_t src_offset,
				     uint64_t len)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.clone_range(src_oid, src_offset, len, dst_offset);
  return operate(dst_oid, &wr, NULL);
}

int librados::IoCtxImpl::operate(const object_t& oid, ::ObjectOperation *o,
				 ceph::real_time *pmtime, int flags)
{
  ceph::real_time ut = (pmtime ? *pmtime :
    ceph::real_clock::now(client->cct));

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  if (!o->size())
    return 0;

  Mutex mylock("IoCtxImpl::operate::mylock");
  Cond cond;
  bool done;
  int r;
  version_t ver;

  Context *oncommit = new C_SafeCond(&mylock, &cond, &done, &r);

  int op = o->ops[0].op.op;
  ldout(client->cct, 10) << ceph_osd_op_name(op) << " oid=" << oid
			 << " nspace=" << oloc.nspace << dendl;
  Objecter::Op *objecter_op = objecter->prepare_mutate_op(oid, oloc,
							  *o, snapc, ut, flags,
							  NULL, oncommit, &ver);
  objecter->op_submit(objecter_op);

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
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

  Mutex mylock("IoCtxImpl::operate_read::mylock");
  Cond cond;
  bool done;
  int r;
  version_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  int op = o->ops[0].op.op;
  ldout(client->cct, 10) << ceph_osd_op_name(op) << " oid=" << oid << " nspace=" << oloc.nspace << dendl;
  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, oloc,
	                                      *o, snap_seq, pbl, flags,
	                                      onack, &ver);
  objecter->op_submit(objecter_op);

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from "
	<< ceph_osd_op_name(op) << " r=" << r << dendl;

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_operate_read(const object_t &oid,
					  ::ObjectOperation *o,
					  AioCompletionImpl *c,
					  int flags,
					  bufferlist *pbl)
{
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;

  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, oloc,
		 *o, snap_seq, pbl, flags,
		 onack, &c->objver);
  objecter->op_submit(objecter_op, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_operate(const object_t& oid,
				     ::ObjectOperation *o, AioCompletionImpl *c,
				     const SnapContext& snap_context, int flags)
{
  auto ut = ceph::real_clock::now(client->cct);
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *oncommit = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *op = objecter->prepare_mutate_op(
    oid, oloc, *o, snap_context, ut, flags, onack,
    oncommit, &c->objver);
  objecter->op_submit(op, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  bufferlist *pbl, size_t len, uint64_t off,
				  uint64_t snapid)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;
  c->blp = pbl;

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    off, len, snapid, pbl, 0,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  char *buf, size_t len, uint64_t off,
				  uint64_t snapid)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;
  c->bl.clear();
  c->bl.push_back(buffer::create_static(len, buf));
  c->blp = &c->bl;

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    off, len, snapid, &c->bl, 0,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

class C_ObjectOperation : public Context {
public:
  ::ObjectOperation m_ops;
  explicit C_ObjectOperation(Context *c) : m_ctx(c) {}
  virtual void finish(int r) {
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
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *nested = new C_aio_Ack(c);
  C_ObjectOperation *onack = new C_ObjectOperation(nested);

  c->is_read = true;
  c->io = this;

  onack->m_ops.sparse_read(off, len, m, data_bl, NULL);

  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc,
    onack->m_ops, snap_seq, NULL, 0,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::aio_write(const object_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off)
{
  auto ut = ceph::real_clock::now(client->cct);
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~" << len << " snapc=" << snapc << " snap_seq=" << snap_seq << dendl;

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_write_op(
    oid, oloc,
    off, len, snapc, bl, ut, 0,
    onack, onsafe, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_append(const object_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  auto ut = ceph::real_clock::now(client->cct);

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_append_op(
    oid, oloc,
    len, snapc, bl, ut, 0,
    onack, onsafe, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const object_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  auto ut = ceph::real_clock::now(client->cct);

  if (bl.length() > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_write_full_op(
    oid, oloc,
    snapc, bl, ut, 0,
    onack, onsafe, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_writesame(const object_t &oid,
				       AioCompletionImpl *c,
				       const bufferlist& bl,
				       size_t write_len,
				       uint64_t off)
{
  auto ut = ceph::real_clock::now(client->cct);

  if ((bl.length() > UINT_MAX/2) || (write_len > UINT_MAX/2))
    return -E2BIG;
  if ((bl.length() == 0) || (write_len % bl.length()))
    return -EINVAL;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_writesame_op(
    oid, oloc,
    write_len, off,
    snapc, bl, ut, 0,
    onack, onsafe, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_remove(const object_t &oid, AioCompletionImpl *c)
{
  auto ut = ceph::real_clock::now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Objecter::Op *o = objecter->prepare_remove_op(
    oid, oloc,
    snapc, ut, 0,
    onack, onsafe, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}


int librados::IoCtxImpl::aio_stat(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, time_t *pmtime)
{
  C_aio_stat_Ack *onack = new C_aio_stat_Ack(c, pmtime);

  c->io = this;
  Objecter::Op *o = objecter->prepare_stat_op(
    oid, oloc,
    snap_seq, psize, &onack->mtime, 0,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_stat2(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, struct timespec *pts)
{
  C_aio_stat2_Ack *onack = new C_aio_stat2_Ack(c, pts);

  c->io = this;
  Objecter::Op *o = objecter->prepare_stat_op(
    oid, oloc,
    snap_seq, psize, &onack->mtime, 0,
    onack, &c->objver);
  objecter->op_submit(o, &c->tid);

  return 0;
}

int librados::IoCtxImpl::aio_cancel(AioCompletionImpl *c)
{
  return objecter->op_cancel(c->tid, -ECANCELED);
}


int librados::IoCtxImpl::hit_set_list(uint32_t hash, AioCompletionImpl *c,
			      std::list< std::pair<time_t, time_t> > *pls)
{
  Context *onack = new C_aio_Ack(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  rd.hit_set_ls(pls, NULL);
  object_locator_t oloc(poolid);
  Objecter::Op *o = objecter->prepare_pg_read_op(
    hash, oloc, rd, NULL, 0, onack, NULL, NULL);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::hit_set_get(uint32_t hash, AioCompletionImpl *c,
				     time_t stamp,
				     bufferlist *pbl)
{
  Context *onack = new C_aio_Ack(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  rd.hit_set_get(ceph::real_clock::from_time_t(stamp), pbl, 0);
  object_locator_t oloc(poolid);
  Objecter::Op *o = objecter->prepare_pg_read_op(
    hash, oloc, rd, NULL, 0, onack, NULL, NULL);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::remove(const object_t& oid)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.remove();
  return operate(oid, &op, NULL);
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
  Context *onack = new C_aio_Ack(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation op;
  op.scrub_ls(start_after, max_to_get, objects, interval, nullptr);
  object_locator_t oloc{poolid, pg.ps()};
  Objecter::Op *o = objecter->prepare_pg_read_op(
    oloc.hash, oloc, op, nullptr, CEPH_OSD_FLAG_PGOP, onack,
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
  Context *onack = new C_aio_Ack(c);
  c->is_read = true;
  c->io = this;

  ::ObjectOperation op;
  op.scrub_ls(start_after, max_to_get, snapsets, interval, nullptr);
  object_locator_t oloc{poolid, pg.ps()};
  Objecter::Op *o = objecter->prepare_pg_read_op(
    oloc.hash, oloc, op, nullptr, CEPH_OSD_FLAG_PGOP, onack,
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

int librados::IoCtxImpl::tmap_put(const object_t& oid, bufferlist& bl)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.tmap_put(bl);
  return operate(oid, &wr, NULL);
}

int librados::IoCtxImpl::tmap_get(const object_t& oid, bufferlist& bl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.tmap_get(&bl, NULL);
  return operate_read(oid, &rd, NULL);
}

int librados::IoCtxImpl::tmap_to_omap(const object_t& oid, bool nullok)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.tmap_to_omap(nullok);
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
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  Objecter::Op *o = objecter->prepare_read_op(
    oid, oloc, rd, snap_seq, outbl, 0, onack, &c->objver);
  objecter->op_submit(o, &c->tid);
  return 0;
}

int librados::IoCtxImpl::read(const object_t& oid,
			      bufferlist& bl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

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

int librados::IoCtxImpl::mapext(const object_t& oid,
				uint64_t off, size_t len,
				std::map<uint64_t,uint64_t>& m)
{
  bufferlist bl;

  Mutex mylock("IoCtxImpl::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  objecter->mapext(oid, oloc,
		   off, len, snap_seq, &bl, 0,
		   onack);

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);

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

int librados::IoCtxImpl::stat(const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  uint64_t size;
  real_time mtime;

  if (!psize)
    psize = &size;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.stat(psize, &mtime, NULL);
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
  rd.stat(psize, &mtime, NULL);
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

struct WatchInfo : public Objecter::WatchContext {
  librados::IoCtxImpl *ioctx;
  object_t oid;
  librados::WatchCtx *ctx;
  librados::WatchCtx2 *ctx2;

  WatchInfo(librados::IoCtxImpl *io, object_t o,
	    librados::WatchCtx *c, librados::WatchCtx2 *c2)
    : ioctx(io), oid(o), ctx(c), ctx2(c2) {
    ioctx->get();
  }
  ~WatchInfo() {
    ioctx->put();
  }

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
};

int librados::IoCtxImpl::watch(const object_t& oid,
			       uint64_t *handle,
			       librados::WatchCtx *ctx,
			       librados::WatchCtx2 *ctx2)
{
  ::ObjectOperation wr;
  version_t objver;
  C_SaferCond onfinish;

  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc, 0);
  *handle = linger_op->get_cookie();
  linger_op->watch_context = new WatchInfo(this,
					   oid, ctx, ctx2);

  prepare_assert_ops(&wr);
  wr.watch(*handle, CEPH_OSD_WATCH_OP_WATCH);
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
                                   librados::WatchCtx2 *ctx2)
{
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc, 0);
  c->io = this;
  Context *oncomplete = new C_aio_linger_Complete(c, linger_op, false);

  ::ObjectOperation wr;
  *handle = linger_op->get_cookie();
  linger_op->watch_context = new WatchInfo(this, oid, ctx, ctx2);

  prepare_assert_ops(&wr);
  wr.watch(*handle, CEPH_OSD_WATCH_OP_WATCH);
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
  objecter->read(oid, oloc, rd, snap_seq, (bufferlist*)NULL, 0, 0, 0);
  return 0;
}

int librados::IoCtxImpl::watch_check(uint64_t cookie)
{
  Objecter::LingerOp *linger_op = reinterpret_cast<Objecter::LingerOp*>(cookie);
  return objecter->linger_check(linger_op);
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
		   snapc, ceph::real_clock::now(client->cct), 0, NULL,
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
		   snapc, ceph::real_clock::now(client->cct), 0, NULL,
		   oncomplete, &c->objver);
  return 0;
}

int librados::IoCtxImpl::notify(const object_t& oid, bufferlist& bl,
				uint64_t timeout_ms,
				bufferlist *preply_bl,
				char **preply_buf, size_t *preply_buf_len)
{
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc, 0);

  C_SaferCond notify_finish_cond;
  Context *notify_finish = new C_notify_Finish(client->cct, &notify_finish_cond,
                                               objecter, linger_op, preply_bl,
                                               preply_buf, preply_buf_len);

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
    notify_finish->complete(r);
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
  Objecter::LingerOp *linger_op = objecter->linger_register(oid, oloc, 0);

  c->io = this;

  C_aio_notify_Complete *oncomplete = new C_aio_notify_Complete(c, linger_op);
  C_notify_Finish *onnotify = new C_notify_Finish(client->cct, oncomplete,
                                                  objecter, linger_op,
                                                  preply_bl, preply_buf,
                                                  preply_buf_len);
  Context *onack = new C_aio_notify_Ack(client->cct, onnotify, oncomplete);

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
                                        uint64_t expected_write_size)
{
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.set_alloc_hint(expected_object_size, expected_write_size);
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
void librados::IoCtxImpl::set_assert_src_version(const object_t& oid,
						 uint64_t ver)
{
  assert_src_version[oid] = ver;
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


///////////////////////////// C_aio_Ack ////////////////////////////////

librados::IoCtxImpl::C_aio_Ack::C_aio_Ack(AioCompletionImpl *_c) : c(_c)
{
  assert(!c->io);
  c->get();
}

void librados::IoCtxImpl::C_aio_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  if (c->is_read)
    c->safe = true;
  c->cond.Signal();

  if (r == 0 && c->blp && c->blp->length() > 0) {
    c->rval = c->blp->length();
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }
  if (c->is_read && c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->put_unlock();
}

///////////////////////////// C_aio_stat_Ack ////////////////////////////

librados::IoCtxImpl::C_aio_stat_Ack::C_aio_stat_Ack(AioCompletionImpl *_c,
						    time_t *pm)
   : c(_c), pmtime(pm)
{
  assert(!c->io);
  c->get();
}

void librados::IoCtxImpl::C_aio_stat_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  if (r >= 0 && pmtime) {
    *pmtime = real_clock::to_time_t(mtime);
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }

  c->put_unlock();
}

///////////////////////////// C_aio_stat2_Ack ////////////////////////////

librados::IoCtxImpl::C_aio_stat2_Ack::C_aio_stat2_Ack(AioCompletionImpl *_c,
						     struct timespec *pt)
   : c(_c), pts(pt)
{
  assert(!c->io);
  c->get();
}

void librados::IoCtxImpl::C_aio_stat2_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  if (r >= 0 && pts) {
    *pts = real_clock::to_timespec(mtime);
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }

  c->put_unlock();
}

//////////////////////////// C_aio_Safe ////////////////////////////////

librados::IoCtxImpl::C_aio_Safe::C_aio_Safe(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_Safe::finish(int r)
{
  c->lock.Lock();
  if (!c->ack) {
    c->rval = r;
    c->ack = true;
  }
  c->safe = true;
  c->cond.Signal();

  if (c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->io->complete_aio_write(c);

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

