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

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

librados::IoCtxImpl::IoCtxImpl() :
  ref_cnt(0), client(NULL), poolid(0), assert_ver(0), last_objver(0),
  notify_timeout(30), aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
  aio_write_seq(0), lock(NULL), objecter(NULL)
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       Mutex *client_lock, int poolid,
			       const char *pool_name, snapid_t s)
  : ref_cnt(0), client(c), poolid(poolid), pool_name(pool_name), snap_seq(s),
    assert_ver(0), notify_timeout(c->cct->_conf->client_notify_timeout),
    oloc(poolid),
    aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
    aio_write_seq(0), lock(client_lock), objecter(objecter)
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

uint32_t librados::IoCtxImpl::get_object_hash_position(const std::string& oid)
{
  return objecter->get_object_hash_position(poolid, oid, oloc.nspace);
}

uint32_t librados::IoCtxImpl::get_object_pg_hash_position(const std::string& oid)
{
  return objecter->get_object_pg_hash_position(poolid, oid, oloc.nspace);
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
  utime_t ut = ceph_clock_now(client->cct);
  int reply;

  Mutex mylock("IoCtxImpl::snap_rollback::mylock");
  Cond cond;
  bool done;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &reply);

  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.rollback(snapid);
  objecter->mutate(oid, oloc,
	           op, snapc, ut, 0,
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
  string sName(snapName);

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
  object_t oid;
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
  object_t oid;
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
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.write_full(bl);
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
				 time_t *pmtime, int flags)
{
  utime_t ut;
  if (pmtime) {
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(client->cct);
  }

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
  ldout(client->cct, 10) << ceph_osd_op_name(op) << " oid=" << oid << " nspace=" << oloc.nspace << dendl;
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
  c->tid = objecter->op_submit(objecter_op);
  return 0;
}

int librados::IoCtxImpl::aio_operate(const object_t& oid,
				     ::ObjectOperation *o, AioCompletionImpl *c,
				     const SnapContext& snap_context, int flags)
{
  utime_t ut = ceph_clock_now(client->cct);
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *oncommit = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  c->tid = objecter->mutate(oid, oloc, *o, snap_context, ut, flags, onack, oncommit,
		            &c->objver);

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

  c->tid = objecter->read(oid, oloc,
		 off, len, snapid, pbl, 0,
		 onack, &c->objver);
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

  c->tid = objecter->read(oid, oloc,
		 off, len, snapid, &c->bl, 0,
		 onack, &c->objver);

  return 0;
}

class C_ObjectOperation : public Context {
public:
  ::ObjectOperation m_ops;
  C_ObjectOperation(Context *c) : m_ctx(c) {}
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

  c->tid = objecter->read(oid, oloc,
		 onack->m_ops, snap_seq, NULL, 0,
		 onack, &c->objver);
  return 0;
}

int librados::IoCtxImpl::aio_write(const object_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off)
{
  utime_t ut = ceph_clock_now(client->cct);
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~" << len << " snapc=" << snapc << " snap_seq=" << snap_seq << dendl;

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->tid = objecter->write(oid, oloc,
		  off, len, snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_append(const object_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(client->cct);

  if (len > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->tid = objecter->append(oid, oloc,
		   len, snapc, bl, ut, 0,
		   onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const object_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  if (bl.length() > UINT_MAX/2)
    return -E2BIG;
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->tid = objecter->write_full(oid, oloc,
		       snapc, bl, ut, 0,
		       onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_remove(const object_t &oid, AioCompletionImpl *c)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  c->tid = objecter->remove(oid, oloc,
		   snapc, ut, 0,
		   onack, onsafe, &c->objver);

  return 0;
}


int librados::IoCtxImpl::aio_stat(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, time_t *pmtime)
{
  c->io = this;
  C_aio_stat_Ack *onack = new C_aio_stat_Ack(c, pmtime);

  c->tid = objecter->stat(oid, oloc,
		 snap_seq, psize, &onack->mtime, 0,
		 onack, &c->objver);

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
  c->tid = objecter->pg_read(hash, oloc, rd, NULL, 0, onack, NULL, NULL);
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
  rd.hit_set_get(utime_t(stamp, 0), pbl, 0);
  object_locator_t oloc(poolid);
  c->tid = objecter->pg_read(hash, oloc, rd, NULL, 0, onack, NULL, NULL);
  return 0;
}

int librados::IoCtxImpl::remove(const object_t& oid)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.remove();
  return operate(oid, &op, NULL);
}

int librados::IoCtxImpl::trunc(const object_t& oid, uint64_t size)
{
  ::ObjectOperation op;
  prepare_assert_ops(&op);
  op.truncate(size);
  return operate(oid, &op, NULL);
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
  c->tid = objecter->read(oid, oloc, rd, snap_seq, outbl, 0, onack, &c->objver);

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
  utime_t mtime;

  if (!psize)
    psize = &size;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.stat(psize, &mtime, NULL);
  int r = operate_read(oid, &rd, NULL);

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  return r;
}

int librados::IoCtxImpl::getxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.getxattr(name, &bl, NULL);
  int r = operate_read(oid, &rd, NULL);
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
  last_objver = ver;
}

int librados::IoCtxImpl::watch(const object_t& oid, uint64_t ver,
			       uint64_t *cookie, librados::WatchCtx *ctx)
{
  ::ObjectOperation wr;
  Mutex mylock("IoCtxImpl::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  version_t objver;

  lock->Lock();

  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->watch_ctx = ctx;
  client->register_watch_notify_callback(wc, cookie);
  prepare_assert_ops(&wr);
  wr.watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger_mutate(oid, oloc, wr,
					  snapc, ceph_clock_now(NULL), bl,
					  0,
					  NULL, onfinish, &objver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(objver);

  if (r < 0) {
    lock->Lock();
    client->unregister_watch_notify_callback(*cookie); // destroys wc
    lock->Unlock();
  }

  return r;
}


/* this is called with IoCtxImpl::lock held */
int librados::IoCtxImpl::_notify_ack(
  const object_t& oid,
  uint64_t notify_id, uint64_t ver,
  uint64_t cookie)
{
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.notify_ack(notify_id, ver, cookie);
  objecter->read(oid, oloc, rd, snap_seq, (bufferlist*)NULL, 0, 0, 0);
  return 0;
}

int librados::IoCtxImpl::unwatch(const object_t& oid, uint64_t cookie)
{
  bufferlist inbl, outbl;

  Mutex mylock("IoCtxImpl::unwatch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *oncommit = new C_SafeCond(&mylock, &cond, &done, &r);
  version_t ver;
  lock->Lock();

  client->unregister_watch_notify_callback(cookie);

  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.watch(cookie, 0, 0);
  objecter->mutate(oid, oloc, wr, snapc, ceph_clock_now(client->cct), 0, NULL, oncommit, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::notify(const object_t& oid, uint64_t ver, bufferlist& bl)
{
  bufferlist inbl, outbl;

  // Construct WatchNotifyInfo
  Cond cond_all;
  Mutex mylock_all("IoCtxImpl::notify::mylock_all");
  bool done_all = false;
  int r_notify = 0;
  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->notify_done = &done_all;
  wc->notify_lock = &mylock_all;
  wc->notify_cond = &cond_all;
  wc->notify_rval = &r_notify;

  lock->Lock();

  // Acquire cookie
  uint64_t cookie;
  client->register_watch_notify_callback(wc, &cookie);
  uint32_t prot_ver = 1;
  uint32_t timeout = notify_timeout;
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  ::encode(bl, inbl);

  // Construct RADOS op
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.notify(cookie, ver, inbl);

  // Issue RADOS op
  C_SaferCond onack;
  version_t objver;
  wc->linger_id = objecter->linger_read(oid, oloc, rd, snap_seq, inbl, NULL, 0,
					&onack, &objver);
  lock->Unlock();

  ldout(client->cct, 10) << __func__ << " issued linger op " << wc->linger_id << dendl;
  int r_issue = onack.wait();
  ldout(client->cct, 10) << __func__ << " linger op " << wc->linger_id << " acked (" << r_issue << ")" << dendl;

  if (r_issue == 0) {
  ldout(client->cct, 10) << __func__ << "waiting for watch_notify message for linger op " << wc->linger_id << dendl;
    mylock_all.Lock();
    while (!done_all)
      cond_all.Wait(mylock_all);
    mylock_all.Unlock();
  }
  ldout(client->cct, 10) << __func__ << " completed notify (linger op " << wc->linger_id << "), unregistering" << dendl;

  lock->Lock();
  client->unregister_watch_notify_callback(cookie);   // destroys wc
  lock->Unlock();

  set_sync_op_version(objver);

  return r_issue == 0 ? r_notify : r_issue;
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

///////////////////////////// C_aio_Ack ////////////////////////////////

librados::IoCtxImpl::C_aio_Ack::C_aio_Ack(AioCompletionImpl *_c) : c(_c)
{
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
  c->get();
}

void librados::IoCtxImpl::C_aio_stat_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
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

