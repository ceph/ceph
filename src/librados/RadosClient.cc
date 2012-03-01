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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
#include <pthread.h>
#include <errno.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "include/buffer.h"

#include "messages/MWatchNotify.h"
#include "msg/SimpleMessenger.h"

#include "AioCompletionImpl.h"
#include "IoCtxImpl.h"
#include "PoolAsyncCompletionImpl.h"
#include "RadosClient.h"

#define DOUT_SUBSYS rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

static atomic_t rados_instance;

librados::PoolAsyncCompletionImpl *librados::RadosClient::pool_async_create_completion()
{
  return new librados::PoolAsyncCompletionImpl;
}

librados::PoolAsyncCompletionImpl *librados::RadosClient::pool_async_create_completion(void *cb_arg, rados_callback_t cb)
{
  librados::PoolAsyncCompletionImpl *c = new librados::PoolAsyncCompletionImpl;
  if (cb)
    c->set_callback(cb_arg, cb);
  return c;
}

librados::AioCompletionImpl *librados::RadosClient::aio_create_completion()
{
  return new librados::AioCompletionImpl;
}

librados::AioCompletionImpl *librados::RadosClient::aio_create_completion(void *cb_arg,
									  rados_callback_t cb_complete,
									  rados_callback_t cb_safe)
{
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete)
    c->set_complete_callback(cb_arg, cb_complete);
  if (cb_safe)
    c->set_safe_callback(cb_arg, cb_safe);
  return c;
}

bool librados::RadosClient::ms_get_authorizer(int dest_type,
					      AuthAuthorizer **authorizer,
					      bool force_new) {
  //ldout(cct, 0) << "RadosClient::ms_get_authorizer type=" << dest_type << dendl;
  /* monitor authorization is being handled on different layer */
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = monclient.auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

librados::RadosClient::RadosClient(CephContext *cct_) : Dispatcher(cct_),
							cct(cct_),
							conf(cct_->_conf),
							state(DISCONNECTED),
							monclient(cct_),
							messenger(NULL),
							objecter(NULL),
							lock("radosclient"),
							timer(cct, lock),
							max_watch_cookie(0)
{
}

int64_t librados::RadosClient::lookup_pool(const char *name) {
  int64_t ret = osdmap.lookup_pg_pool_name(name);
  if (ret < 0)
    return -ENOENT;
  return ret;
}

const char *librados::RadosClient::get_pool_name(int64_t poolid_)
{
  return osdmap.get_pool_name(poolid_);
}

int librados::RadosClient::connect()
{
  common_init_finish(cct);

  int err;
  uint64_t nonce;

  // already connected?
  if (state == CONNECTING)
    return -EINPROGRESS;
  if (state == CONNECTED)
    return -EISCONN;
  state = CONNECTING;

  // get monmap
  err = monclient.build_initial_monmap();
  if (err < 0)
    goto out;

  err = -ENOMEM;
  nonce = getpid() + (1000000 * (uint64_t)rados_instance.inc());
  messenger = new SimpleMessenger(cct, entity_name_t::CLIENT(-1), nonce);
  if (!messenger)
    goto out;

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::client(0, CEPH_FEATURE_OSDREPLYMUX));

  ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

  ldout(cct, 1) << "starting objecter" << dendl;

  err = -ENOMEM;
  objecter = new Objecter(cct, messenger, &monclient, &osdmap, lock, timer);
  if (!objecter)
    goto out;
  objecter->set_balanced_budget();

  monclient.set_messenger(messenger);

  messenger->add_dispatcher_head(this);

  messenger->start();

  ldout(cct, 1) << "setting wanted keys" << dendl;
  monclient.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  ldout(cct, 1) << "calling monclient init" << dendl;
  err = monclient.init();
  if (err) {
    ldout(cct, 0) << conf->name << " initialization error " << cpp_strerror(-err) << dendl;
    shutdown();
    goto out;
  }

  err = monclient.authenticate(conf->client_mount_timeout);
  if (err) {
    ldout(cct, 0) << conf->name << " authentication error " << cpp_strerror(-err) << dendl;
    shutdown();
    goto out;
  }
  messenger->set_myname(entity_name_t::CLIENT(monclient.get_global_id()));

  lock.Lock();

  timer.init();

  objecter->set_client_incarnation(0);
  objecter->init();
  monclient.renew_subs();

  while (osdmap.get_epoch() == 0) {
    ldout(cct, 1) << "waiting for osdmap" << dendl;
    cond.Wait(lock);
  }
  state = CONNECTED;
  lock.Unlock();

  ldout(cct, 1) << "init done" << dendl;
  err = 0;

 out:
  if (err)
    state = DISCONNECTED;
  return err;
}

void librados::RadosClient::shutdown()
{
  lock.Lock();
  if (state == DISCONNECTED) {
    lock.Unlock();
    return;
  }
  monclient.shutdown();
  if (objecter && state == CONNECTED)
    objecter->shutdown();
  state = DISCONNECTED;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
  ldout(cct, 1) << "shutdown" << dendl;
}

librados::RadosClient::~RadosClient()
{
  if (messenger)
    delete messenger;
  if (objecter)
    delete objecter;
  common_destroy_context(cct);
  cct = NULL;
}


bool librados::RadosClient::ms_dispatch(Message *m)
{
  bool ret;

  lock.Lock();
  if (state == DISCONNECTED) {
    ldout(cct, 10) << "disconnected, discarding " << *m << dendl;
    m->put();
    ret = true;
  } else {
    ret = _dispatch(m);
  }
  lock.Unlock();
  return ret;
}

void librados::RadosClient::ms_handle_connect(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_connect(con);
}

bool librados::RadosClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_reset(con);
  return false;
}

void librados::RadosClient::ms_handle_remote_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_remote_reset(con);
}


bool librados::RadosClient::_dispatch(Message *m)
{
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply((class MOSDOpReply*)m);
    break;
  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map((MOSDMap*)m);
    cond.Signal();
    break;
  case MSG_GETPOOLSTATSREPLY:
    objecter->handle_get_pool_stats_reply((MGetPoolStatsReply*)m);
    break;

  case CEPH_MSG_MDS_MAP:
    break;

  case CEPH_MSG_STATFS_REPLY:
    objecter->handle_fs_stats_reply((MStatfsReply*)m);
    break;

  case CEPH_MSG_POOLOP_REPLY:
    objecter->handle_pool_op_reply((MPoolOpReply*)m);
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    watch_notify((MWatchNotify *)m);
    break;
  default:
    return false;
  }

  return true;
}

int librados::RadosClient::pool_list(std::list<std::string>& v)
{
  Mutex::Locker l(lock);
  for (map<int64_t,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
       p != osdmap.get_pools().end();
       p++)
    v.push_back(osdmap.get_pool_name(p->first));
  return 0;
}

int librados::RadosClient::get_pool_stats(std::list<string>& pools,
					  map<string,::pool_stat_t>& result)
{
  Mutex mylock("RadosClient::get_pool_stats::mylock");
  Cond cond;
  bool done;

  lock.Lock();
  objecter->get_pool_stats(pools, &result, new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return 0;
}

int librados::RadosClient::get_fs_stats(ceph_statfs& stats)
{
  Mutex mylock ("RadosClient::get_fs_stats::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();

  return 0;
}


// SNAPS

int librados::RadosClient::snap_create(rados_ioctx_t io, const char *snapName)
{
  int reply;
  int64_t poolID = ((IoCtxImpl *)io)->poolid;
  string sName(snapName);

  Mutex mylock ("RadosClient::snap_create::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->create_pool_snap(poolID,
			     sName,
			     new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while(!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::selfmanaged_snap_create(rados_ioctx_t io, uint64_t *psnapid)
{
  int reply;
  int64_t poolID = ((IoCtxImpl *)io)->poolid;

  Mutex mylock("RadosClient::selfmanaged_snap_create::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  snapid_t snapid;
  objecter->allocate_selfmanaged_snap(poolID, &snapid,
				      new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  if (reply == 0)
    *psnapid = snapid;
  return reply;
}

int librados::RadosClient::snap_remove(rados_ioctx_t io, const char *snapName)
{
  int reply;
  int64_t poolID = ((IoCtxImpl *)io)->poolid;
  string sName(snapName);

  Mutex mylock ("RadosClient::snap_remove::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->delete_pool_snap(poolID,
			     sName,
			     new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while(!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::selfmanaged_snap_rollback_object(rados_ioctx_t io,
							    const object_t& oid,
							    ::SnapContext& snapc,
							    uint64_t snapid)
{
  int reply;
  IoCtxImpl* ctx = (IoCtxImpl *) io;

  Mutex mylock("RadosClient::snap_rollback::mylock");
  Cond cond;
  bool done;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &reply);

  lock.Lock();
  objecter->rollback_object(oid, ctx->oloc, snapc, snapid,
		     ceph_clock_now(cct), onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::rollback(rados_ioctx_t io_, const object_t& oid,
				    const char *snapName)
{
  IoCtxImpl* io = (IoCtxImpl *) io_;
  string sName(snapName);

  lock.Lock();
  snapid_t snap;
  const map<int64_t, pg_pool_t>& pools = objecter->osdmap->get_pools();
  const pg_pool_t& pg_pool = pools.find(io->poolid)->second;
  map<snapid_t, pool_snap_info_t>::const_iterator p;
  for (p = pg_pool.snaps.begin();
       p != pg_pool.snaps.end();
       ++p) {
    if (p->second.name == snapName) {
      snap = p->first;
      break;
    }
  }
  if (p == pg_pool.snaps.end()) {
    lock.Unlock();
    return -ENOENT;
  }
  lock.Unlock();

  return selfmanaged_snap_rollback_object(io_, oid, io->snapc, snap);
}

int librados::RadosClient::selfmanaged_snap_remove(rados_ioctx_t io, uint64_t snapid)
{
  int reply;
  int64_t poolID = ((IoCtxImpl *)io)->poolid;

  Mutex mylock("RadosClient::selfmanaged_snap_remove::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->delete_selfmanaged_snap(poolID, snapid_t(snapid),
				      new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return (int)reply;
}

int librados::RadosClient::pool_create(string& name, unsigned long long auid,
				       __u8 crush_rule)
{
  int reply;

  Mutex mylock ("RadosClient::pool_create::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->create_pool(name,
			new C_SafeCond(&mylock, &cond, &done, &reply),
			auid, crush_rule);
  lock.Unlock();

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::pool_create_async(string& name, PoolAsyncCompletionImpl *c,
					     unsigned long long auid,
					     __u8 crush_rule)
{
  Mutex::Locker l(lock);
  objecter->create_pool(name,
			new C_PoolAsync_Safe(c),
			auid, crush_rule);
  return 0;
}

int librados::RadosClient::pool_delete(const char *name)
{
  int tmp_pool_id = osdmap.lookup_pg_pool_name(name);
  if (tmp_pool_id < 0)
    return -ENOENT;

  Mutex mylock("RadosClient::pool_delete::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  int reply = 0;
  objecter->delete_pool(tmp_pool_id, new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::pool_delete_async(const char *name, PoolAsyncCompletionImpl *c)
{
  int tmp_pool_id = osdmap.lookup_pg_pool_name(name);
  if (tmp_pool_id < 0)
    return -ENOENT;

  Mutex::Locker l(lock);
  objecter->delete_pool(tmp_pool_id, new C_PoolAsync_Safe(c));

  return 0;
}

int librados::RadosClient::pool_change_auid(rados_ioctx_t io, unsigned long long auid)
{
  int reply;

  int64_t poolID = ((IoCtxImpl *)io)->poolid;

  Mutex mylock("RadosClient::pool_change_auid::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->change_pool_auid(poolID,
			     new C_SafeCond(&mylock, &cond, &done, &reply),
			     auid);
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::RadosClient::pool_change_auid_async(rados_ioctx_t io, unsigned long long auid,
						  PoolAsyncCompletionImpl *c)
{
  int64_t poolID = ((IoCtxImpl *)io)->poolid;

  Mutex::Locker l(lock);
  objecter->change_pool_auid(poolID,
			     new C_PoolAsync_Safe(c),
			     auid);
  return 0;
}

int librados::RadosClient::pool_get_auid(rados_ioctx_t io, unsigned long long *auid)
{
  Mutex::Locker l(lock);
  int64_t pool_id = ((IoCtxImpl *)io)->poolid;
  const pg_pool_t *pg = osdmap.get_pg_pool(pool_id);
  if (!pg)
    return -ENOENT;
  *auid = pg->auid;
  return 0;
}

int librados::RadosClient::snap_list(IoCtxImpl *io, vector<uint64_t> *snaps)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       p++)
    snaps->push_back(p->first);
  return 0;
}

int librados::RadosClient::snap_lookup(IoCtxImpl *io, const char *name, uint64_t *snapid)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       p++) {
    if (p->second.name == name) {
      *snapid = p->first;
      return 0;
    }
  }
  return -ENOENT;
}

int librados::RadosClient::snap_get_name(IoCtxImpl *io, uint64_t snapid, std::string *s)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.find(snapid);
  if (p == pi->snaps.end())
    return -ENOENT;
  *s = p->second.name.c_str();
  return 0;
}

int librados::RadosClient::snap_get_stamp(IoCtxImpl *io, uint64_t snapid, time_t *t)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.find(snapid);
  if (p == pi->snaps.end())
    return -ENOENT;
  *t = p->second.stamp.sec();
  return 0;
}


// IO

int librados::RadosClient::list(Objecter::ListContext *context, int max_entries)
{
  Cond cond;
  bool done;
  int r = 0;
  object_t oid;
  Mutex mylock("RadosClient::list::mylock");

  if (context->at_end)
    return 0;

  context->max_entries = max_entries;

  lock.Lock();
  objecter->list_objects(context, new C_SafeCond(&mylock, &cond, &done, &r));
  lock.Unlock();

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

int librados::RadosClient::create(IoCtxImpl& io, const object_t& oid, bool exclusive)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::create::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  objecter->create(oid, io.oloc,
		  io.snapc, ut, 0, (exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0),
		  onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::create(IoCtxImpl& io, const object_t& oid, bool exclusive,
				  const std::string& category)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::create::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  ::ObjectOperation o;
  o.create(exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0, category);

  lock.Lock();
  objecter->mutate(oid, io.oloc, o, io.snapc, ut, 0, onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
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
::ObjectOperation *librados::RadosClient::prepare_assert_ops(IoCtxImpl *io, ::ObjectOperation *op)
{
  ::ObjectOperation *pop = NULL;
  if (io->assert_ver) {
    op->assert_version(io->assert_ver);
    io->assert_ver = 0;
    pop = op;
  }
  while (!io->assert_src_version.empty()) {
    map<object_t,uint64_t>::iterator p = io->assert_src_version.begin();
    op->assert_src_version(p->first, CEPH_NOSNAP, p->second);
    io->assert_src_version.erase(p);
    pop = op;
  }
  return pop;
}

int librados::RadosClient::write(IoCtxImpl& io, const object_t& oid, bufferlist& bl,
				 size_t len, uint64_t off)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::write::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  // extra ops?
  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->write(oid, io.oloc,
		  off, len, io.snapc, bl, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return len;
}

int librados::RadosClient::append(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::append::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->append(oid, io.oloc,
		  len, io.snapc, bl, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return len;
}

int librados::RadosClient::write_full(IoCtxImpl& io, const object_t& oid, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->write_full(oid, io.oloc,
		  io.snapc, bl, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::clone_range(IoCtxImpl& io,
				       const object_t& dst_oid, uint64_t dst_offset,
				       const object_t& src_oid, uint64_t src_offset,
				       uint64_t len)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::clone_range::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock.Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&io, &wr);
  wr.clone_range(src_oid, src_offset, len, dst_offset);
  objecter->mutate(dst_oid, io.oloc, wr, io.snapc, ut, 0, onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::operate(IoCtxImpl& io, const object_t& oid,
				   ::ObjectOperation *o, time_t *pmtime)
{
  utime_t ut;
  if (pmtime) {
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(cct);
  }

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  if (!o->size())
    return 0;

  Mutex mylock("RadosClient::mutate::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  objecter->mutate(oid, io.oloc,
	           *o, io.snapc, ut, 0,
	           onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::operate_read(IoCtxImpl& io, const object_t& oid,
					::ObjectOperation *o, bufferlist *pbl)
{
  if (!o->size())
    return 0;

  Mutex mylock("RadosClient::mutate::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  objecter->read(oid, io.oloc,
	           *o, io.snap_seq, pbl, 0,
	           onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::aio_operate_read(IoCtxImpl& io, const object_t &oid,
					    ::ObjectOperation *o,
					    AioCompletionImpl *c, bufferlist *pbl)
{
  Context *onack = new C_aio_Ack(c);

  c->pbl = pbl;

  Mutex::Locker l(lock);
  objecter->read(oid, io.oloc,
		 *o, io.snap_seq, pbl, 0,
		 onack, 0);
  return 0;
}

int librados::RadosClient::aio_operate(IoCtxImpl& io, const object_t& oid,
				       ::ObjectOperation *o, AioCompletionImpl *c)
{
  utime_t ut = ceph_clock_now(cct);
  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *oncommit = new C_aio_Safe(c);

  io.queue_aio_write(c);

  Mutex::Locker l(lock);
  objecter->mutate(oid, io.oloc, *o, io.snapc, ut, 0, onack, oncommit, &c->objver);

  return 0;
}

int librados::RadosClient::aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
				    bufferlist *pbl, size_t len, uint64_t off)
{

  Context *onack = new C_aio_Ack(c);
  eversion_t ver;

  c->pbl = pbl;

  Mutex::Locker l(lock);
  objecter->read(oid, io.oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack, &c->objver);
  return 0;
}

int librados::RadosClient::aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
				    char *buf, size_t len, uint64_t off)
{
  Context *onack = new C_aio_Ack(c);

  c->buf = buf;
  c->maxlen = len;

  Mutex::Locker l(lock);
  objecter->read(oid, io.oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack, &c->objver);

  return 0;
}

int librados::RadosClient::aio_sparse_read(IoCtxImpl& io, const object_t oid,
					   AioCompletionImpl *c, std::map<uint64_t,uint64_t> *m,
					   bufferlist *data_bl, size_t len, uint64_t off)
{

  C_aio_sparse_read_Ack *onack = new C_aio_sparse_read_Ack(c);
  onack->m = m;
  onack->data_bl = data_bl;
  eversion_t ver;

  c->pbl = NULL;

  Mutex::Locker l(lock);
  objecter->sparse_read(oid, io.oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack);
  return 0;
}

int librados::RadosClient::aio_write(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
				     const bufferlist& bl, size_t len, uint64_t off)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  io.queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(lock);
  objecter->write(oid, io.oloc,
		  off, len, io.snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int librados::RadosClient::aio_append(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
				      const bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  io.queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(lock);
  objecter->append(oid, io.oloc,
		  len, io.snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int librados::RadosClient::aio_write_full(IoCtxImpl& io, const object_t &oid,
					  AioCompletionImpl *c, const bufferlist& bl)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  io.queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(lock);
  objecter->write_full(oid, io.oloc,
		  io.snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int librados::RadosClient::remove(IoCtxImpl& io, const object_t& oid)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::remove::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->remove(oid, io.oloc,
		  io.snapc, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::trunc(IoCtxImpl& io, const object_t& oid, uint64_t size)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->trunc(oid, io.oloc,
		  io.snapc, ut, 0,
		  size, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::tmap_update(IoCtxImpl& io, const object_t& oid, bufferlist& cmdbl)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::tmap_update::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock.Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&io, &wr);
  wr.tmap_update(cmdbl);
  objecter->mutate(oid, io.oloc, wr, io.snapc, ut, 0, onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::tmap_put(IoCtxImpl& io, const object_t& oid, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::tmap_put::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock.Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&io, &wr);
  wr.tmap_put(bl);
  objecter->mutate(oid, io.oloc, wr, io.snapc, ut, 0, onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::tmap_get(IoCtxImpl& io, const object_t& oid, bufferlist& bl)
{
  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::tmap_put::mylock");
  Cond cond;
  bool done;
  int r = 0;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock.Lock();
  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);
  rd.tmap_get(&bl, NULL);
  objecter->read(oid, io.oloc, rd, io.snap_seq, 0, 0, onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::exec(IoCtxImpl& io, const object_t& oid,
				const char *cls, const char *method,
				bufferlist& inbl, bufferlist& outbl)
{
  Mutex mylock("RadosClient::exec::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;


  lock.Lock();
  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);
  rd.call(cls, method, inbl);
  objecter->read(oid, io.oloc, rd, io.snap_seq, &outbl, 0, onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::aio_exec(IoCtxImpl& io, const object_t& oid, AioCompletionImpl *c,
				const char *cls, const char *method,
				bufferlist& inbl, bufferlist *outbl)
{
  Context *onack = new C_aio_Ack(c);

  Mutex::Locker l(lock);
  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);
  rd.call(cls, method, inbl);
  objecter->read(oid, io.oloc, rd, io.snap_seq, outbl, 0, onack, &c->objver);

  return 0;
}

int librados::RadosClient::read(IoCtxImpl& io, const object_t& oid,
				bufferlist& bl, size_t len, uint64_t off)
{
  CephContext *cct = io.client->cct;
  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->read(oid, io.oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(cct, 10) << "Objecter returned from read r=" << r << dendl;

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  if (bl.length() < len) {
    ldout(cct, 10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int librados::RadosClient::mapext(IoCtxImpl& io, const object_t& oid,
				  uint64_t off, size_t len, std::map<uint64_t,uint64_t>& m)
{
  CephContext *cct = io.client->cct;
  bufferlist bl;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  objecter->mapext(oid, io.oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);

  return m.size();
}

int librados::RadosClient::sparse_read(IoCtxImpl& io, const object_t& oid,
				       std::map<uint64_t,uint64_t>& m,
				       bufferlist& data_bl, size_t len, uint64_t off)
{
  CephContext *cct = io.client->cct;
  bufferlist bl;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  objecter->sparse_read(oid, io.oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);
  ::decode(data_bl, iter);

  return m.size();
}

int librados::RadosClient::stat(IoCtxImpl& io, const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  CephContext *cct = io.client->cct;
  Mutex mylock("RadosClient::stat::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  uint64_t size;
  utime_t mtime;
  eversion_t ver;

  if (!psize)
    psize = &size;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->stat(oid, io.oloc,
	      io.snap_seq, psize, &mtime, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(cct, 10) << "Objecter returned from stat" << dendl;

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::getxattr(IoCtxImpl& io, const object_t& oid,
				    const char *name, bufferlist& bl)
{
  CephContext *cct = io.client->cct;
  Mutex mylock("RadosClient::getxattr::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->getxattr(oid, io.oloc,
	      name, io.snap_seq, &bl, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(cct, 10) << "Objecter returned from getxattr" << dendl;

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return bl.length();
}

int librados::RadosClient::rmxattr(IoCtxImpl& io, const object_t& oid, const char *name)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::rmxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->removexattr(oid, io.oloc, name,
		  io.snapc, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return 0;
}

int librados::RadosClient::setxattr(IoCtxImpl& io, const object_t& oid,
				    const char *name, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(cct);

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("RadosClient::setxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  lock.Lock();
  objecter->setxattr(oid, io.oloc, name,
		  io.snapc, bl, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return 0;
}

int librados::RadosClient::getxattrs(IoCtxImpl& io, const object_t& oid,
				     map<std::string, bufferlist>& attrset)
{
  CephContext *cct = io.client->cct;
  Mutex mylock("RadosClient::getexattrs::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&io, &op);

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  map<string, bufferlist> aset;
  objecter->getxattrs(oid, io.oloc, io.snap_seq,
		      aset,
		      0, onack, &ver, pop);
  lock.Unlock();

  attrset.clear();


  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  for (map<string,bufferlist>::iterator p = aset.begin(); p != aset.end(); p++) {
    ldout(cct, 10) << "RadosClient::getxattrs: xattr=" << p->first << dendl;
    attrset[p->first.c_str()] = p->second;
  }

  set_sync_op_version(io, ver);

  return r;
}

void librados::RadosClient::watch_notify(MWatchNotify *m)
{
  assert(lock.is_locked());
  WatchContext *wc = NULL;
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(m->cookie);
  if (iter != watchers.end())
    wc = iter->second;

  if (!wc)
    return;

  wc->notify(this, m);

  m->put();
}

int librados::RadosClient::watch(IoCtxImpl& io, const object_t& oid, uint64_t ver,
				 uint64_t *cookie, librados::WatchCtx *ctx)
{
  ::ObjectOperation rd;
  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t objver;

  lock.Lock();

  WatchContext *wc;
  register_watcher(io, oid, ctx, cookie, &wc);
  prepare_assert_ops(&io, &rd);
  rd.watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger(oid, io.oloc, rd, io.snap_seq, bl, NULL, 0, NULL, onfinish, &objver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, objver);

  if (r < 0) {
    lock.Lock();
    unregister_watcher(*cookie);
    lock.Unlock();
  }

  return r;
}


/* this is called with RadosClient::lock held */
int librados::RadosClient::_notify_ack(IoCtxImpl& io, const object_t& oid,
				       uint64_t notify_id, uint64_t ver)
{
  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  eversion_t objver;

  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);
  rd.notify_ack(notify_id, ver);
  objecter->read(oid, io.oloc, rd, io.snap_seq, (bufferlist*)NULL, 0, 0, 0);

  return 0;
}

int librados::RadosClient::unwatch(IoCtxImpl& io, const object_t& oid, uint64_t cookie)
{
  bufferlist inbl, outbl;

  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;
  lock.Lock();

  unregister_watcher(cookie);

  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);
  rd.watch(cookie, 0, 0);
  objecter->read(oid, io.oloc, rd, io.snap_seq, &outbl, 0, onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int librados::RadosClient::notify(IoCtxImpl& io, const object_t& oid, uint64_t ver, bufferlist& bl)
{
  bufferlist inbl, outbl;

  Mutex mylock("RadosClient::notify::mylock");
  Mutex mylock_all("RadosClient::notify::mylock_all");
  Cond cond, cond_all;
  bool done, done_all;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t objver;
  uint64_t cookie;
  C_NotifyComplete *ctx = new C_NotifyComplete(&mylock_all, &cond_all, &done_all);

  ::ObjectOperation rd;
  prepare_assert_ops(&io, &rd);

  lock.Lock();
  WatchContext *wc;
  register_watcher(io, oid, ctx, &cookie, &wc);
  uint32_t prot_ver = 1;
  uint32_t timeout = io.notify_timeout;
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  ::encode(bl, inbl);
  rd.notify(cookie, ver, inbl);
  wc->linger_id = objecter->linger(oid, io.oloc, rd, io.snap_seq, inbl, NULL,
				   0, onack, NULL, &objver);
  lock.Unlock();

  mylock_all.Lock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r == 0) {
    while (!done_all)
      cond_all.Wait(mylock_all);
  }

  mylock_all.Unlock();

  lock.Lock();
  unregister_watcher(cookie);
  lock.Unlock();

  set_sync_op_version(io, objver);
  delete ctx;

  return r;
}

void librados::RadosClient::set_sync_op_version(IoCtxImpl& io, eversion_t& ver)
{
  io.last_objver = ver;
}

void librados::RadosClient::register_watcher(IoCtxImpl& io,
					     const object_t& oid,
					     librados::WatchCtx *ctx,
					     uint64_t *cookie,
					     WatchContext **pwc)
{
  assert(lock.is_locked());
  WatchContext *wc = new WatchContext(&io, oid, ctx);
  *cookie = ++max_watch_cookie;
  watchers[*cookie] = wc;
  if (pwc)
    *pwc = wc;
}

void librados::RadosClient::unregister_watcher(uint64_t cookie)
{
  assert(lock.is_locked());
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(cookie);
  if (iter != watchers.end()) {
    WatchContext *ctx = iter->second;
    if (ctx->linger_id)
      objecter->unregister_linger(ctx->linger_id);
    delete ctx;
    watchers.erase(iter);
  }
}

eversion_t librados::RadosClient::last_version(IoCtxImpl& io)
{
  return io.last_objver;
}

void librados::RadosClient::set_assert_version(IoCtxImpl& io, uint64_t ver)
{
  io.assert_ver = ver;
}
void librados::RadosClient::set_assert_src_version(IoCtxImpl& io,
						   const object_t& oid,
						   uint64_t ver)
{
  io.assert_src_version[oid] = ver;
}

void librados::RadosClient::set_notify_timeout(IoCtxImpl& io, uint32_t timeout)
{
  io.notify_timeout = timeout;
}

///////////////////////////// C_aio_Ack ////////////////////////////////

librados::RadosClient::C_aio_Ack::C_aio_Ack(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::RadosClient::C_aio_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  if (c->buf && c->bl.length() > 0) {
    unsigned l = MIN(c->bl.length(), c->maxlen);
    c->bl.copy(0, l, c->buf);
    c->rval = c->bl.length();
  }
  if (c->pbl) {
    *c->pbl = c->bl;
  }

  if (c->callback_complete) {
    rados_callback_t cb = c->callback_complete;
    void *cb_arg = c->callback_arg;
    c->lock.Unlock();
    cb(c, cb_arg);
    c->lock.Lock();
  }

  c->put_unlock();
}

/////////////////////// C_aio_sparse_read_Ack //////////////////////////

librados::RadosClient::C_aio_sparse_read_Ack::C_aio_sparse_read_Ack(AioCompletionImpl *_c)
  : c(_c)
{
  c->get();
}

void librados::RadosClient::C_aio_sparse_read_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  bufferlist::iterator iter = c->bl.begin();
  if (r >= 0) {
    ::decode(*m, iter);
    ::decode(*data_bl, iter);
  }

  if (c->callback_complete) {
    rados_callback_t cb = c->callback_complete;
    void *cb_arg = c->callback_arg;
    c->lock.Unlock();
    cb(c, cb_arg);
    c->lock.Lock();
  }

  c->put_unlock();
}

//////////////////////////// C_aio_Safe ////////////////////////////////

librados::RadosClient::C_aio_Safe::C_aio_Safe(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::RadosClient::C_aio_Safe::finish(int r)
{
  c->lock.Lock();
  if (!c->ack) {
    c->rval = r;
    c->ack = true;
  }
  c->safe = true;
  c->cond.Signal();

  if (c->callback_safe) {
    rados_callback_t cb = c->callback_safe;
    void *cb_arg = c->callback_arg;
    c->lock.Unlock();
    cb(c, cb_arg);
    c->lock.Lock();
  }

  c->io->complete_aio_write(c);

  c->put_unlock();
}

///////////////////////// C_PoolAsync_Safe /////////////////////////////

librados::RadosClient::C_PoolAsync_Safe::C_PoolAsync_Safe(PoolAsyncCompletionImpl *_c)
  : c(_c)
{
  c->get();
}

void librados::RadosClient::C_PoolAsync_Safe::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->done = true;
  c->cond.Signal();

  if (c->callback) {
    rados_callback_t cb = c->callback;
    void *cb_arg = c->callback_arg;
    c->lock.Unlock();
    cb(c, cb_arg);
    c->lock.Lock();
  }

  c->put_unlock();
}

///////////////////////// C_NotifyComplete /////////////////////////////

librados::RadosClient::C_NotifyComplete::C_NotifyComplete(Mutex *_l,
							  Cond *_c,
							  bool *_d)
  : lock(_l), cond(_c), done(_d)
{
  *done = false;
}

void librados::RadosClient::C_NotifyComplete::notify(uint8_t opcode,
						     uint64_t ver,
						     bufferlist& bl)
{
  *done = true;
  cond->Signal();
}

/////////////////////////// WatchContext ///////////////////////////////

librados::RadosClient::WatchContext::WatchContext(IoCtxImpl *io_ctx_impl_,
						  const object_t& _oc,
						  librados::WatchCtx *_ctx)
  : io_ctx_impl(io_ctx_impl_), oid(_oc), ctx(_ctx), linger_id(0)
{
  io_ctx_impl->get();
}

librados::RadosClient::WatchContext::~WatchContext()
{
  io_ctx_impl->put();
}

void librados::RadosClient::WatchContext::notify(RadosClient *client,
						 MWatchNotify *m)
{
  ctx->notify(m->opcode, m->ver, m->bl);
  if (m->opcode != WATCH_NOTIFY_COMPLETE) {
    client->_notify_ack(*io_ctx_impl, oid, m->notify_id, m->ver);
  }
}
