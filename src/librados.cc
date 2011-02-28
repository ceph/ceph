// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
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
using namespace std;

#include "common/config.h"

#include "mon/MonMap.h"
#include "mds/MDS.h"
#include "osd/OSDMap.h"
#include "osd/PGLS.h"

#include "msg/SimpleMessenger.h"

#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/common_init.h"

#include "mon/MonClient.h"

#include "osdc/Objecter.h"

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"

#include "messages/MWatchNotify.h"

#define RADOS_LIST_MAX_ENTRIES 1024
#define DOUT_SUBSYS rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "


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

///////////////////////////// RadosClient //////////////////////////////
struct IoCtxImpl {
  RadosClient *client;
  int poolid;
  string pool_name;
  snapid_t snap_seq;
  SnapContext snapc;
  uint64_t assert_ver;
  eversion_t last_objver;
  uint32_t notify_timeout;

  IoCtxImpl(RadosClient *c, int pid, const char *pool_name_, snapid_t s = CEPH_NOSNAP) :
    client(c), poolid(pid), pool_name(pool_name_), snap_seq(s), assert_ver(0),
    notify_timeout(g_conf.client_notify_timeout) {}

  void set_snap_read(snapid_t s) {
    if (!s)
      s = CEPH_NOSNAP;
    snap_seq = s;
  }

  int set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps) {
    SnapContext n;
    n.seq = seq;
    n.snaps = snaps;
    if (!n.is_valid())
      return -EINVAL;
    snapc = n;
    return 0;
  }
};


struct AioCompletionImpl {
  Mutex lock;
  Cond cond;
  int ref, rval;
  bool released;
  bool ack, safe;
  eversion_t objver;

  rados_callback_t callback_complete, callback_safe;
  void *callback_arg;

  // for read
  bufferlist bl, *pbl;
  char *buf;
  unsigned maxlen;

  AioCompletionImpl() : lock("AioCompletionImpl lock"),
		    ref(1), rval(0), released(false), ack(false), safe(false),
		    callback_complete(0), callback_safe(0), callback_arg(0),
		    pbl(0), buf(0), maxlen(0) { }

  int set_complete_callback(void *cb_arg, rados_callback_t cb) {
    lock.Lock();
    callback_complete = cb;
    callback_arg = cb_arg;
    lock.Unlock();
    return 0;
  }
  int set_safe_callback(void *cb_arg, rados_callback_t cb) {
    lock.Lock();
    callback_safe = cb;
    callback_arg = cb_arg;
    lock.Unlock();
    return 0;
  }
  int wait_for_complete() {
    lock.Lock();
    while (!ack)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int wait_for_safe() {
    lock.Lock();
    while (!safe)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int is_complete() {
    lock.Lock();
    int r = ack;
    lock.Unlock();
    return r;
  }
  int is_safe() {
    lock.Lock();
    int r = safe;
    lock.Unlock();
    return r;
  }
  int get_return_value() {
    lock.Lock();
    int r = rval;
    lock.Unlock();
    return r;
  }
  uint64_t get_version() {
    lock.Lock();
    eversion_t v = objver;
    lock.Unlock();
    return v.version;
  }

  void get() {
    lock.Lock();
    assert(ref > 0);
    ref++;
    lock.Unlock();
  }
  void release() {
    lock.Lock();
    assert(!released);
    released = true;
    put_unlock();
  }
  void put() {
    lock.Lock();
    put_unlock();
  }
  void put_unlock() {
    assert(ref > 0);
    int n = --ref;
    lock.Unlock();
    if (!n)
      delete this;
  }
};

struct ObjListCtx {
  IoCtxImpl *ctx;
  Objecter::ListContext *lc;

  ObjListCtx(IoCtxImpl *c, Objecter::ListContext *l) : ctx(c), lc(l) {}
  ~ObjListCtx() {
    delete lc;
  }
};

class RadosClient : public Dispatcher
{
  OSDMap osdmap;
  MonClient monclient;
  SimpleMessenger *messenger;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new) {
    //dout(0) << "RadosClient::ms_get_authorizer type=" << dest_type << dendl;
    /* monitor authorization is being handled on different layer */
    if (dest_type == CEPH_ENTITY_TYPE_MON)
      return true;
    *authorizer = monclient.auth->build_authorizer(dest_type);
    return *authorizer != NULL;
  }
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);


  Objecter *objecter;

  Mutex lock;
  Cond cond;
  SafeTimer timer;

public:
  RadosClient() : messenger(NULL), objecter(NULL),
	    lock("radosclient"), timer(lock), max_watch_cookie(0)
  {
  }

  ~RadosClient();
  int connect();
  void shutdown();

  int lookup_pool(const char *name) {
    int ret = osdmap.lookup_pg_pool_name(name);
    if (ret < 0)
      return -ENOENT;
    return ret;
  }

  const char *get_pool_name(int poolid_)
  {
    return osdmap.get_pool_name(poolid_);
  }

  // snaps
  int snap_list(IoCtxImpl *io, vector<uint64_t> *snaps);
  int snap_lookup(IoCtxImpl *io, const char *name, uint64_t *snapid);
  int snap_get_name(IoCtxImpl *io, uint64_t snapid, std::string *s);
  int snap_get_stamp(IoCtxImpl *io, uint64_t snapid, time_t *t);
  int snap_create(rados_ioctx_t io, const char* snapname);
  int selfmanaged_snap_create(rados_ioctx_t io, uint64_t *snapid);
  int snap_remove(rados_ioctx_t io, const char* snapname);
  int rollback(rados_ioctx_t io_, const object_t& oid, const char *snapName);
  int selfmanaged_snap_remove(rados_ioctx_t io, uint64_t snapid);
  int selfmanaged_snap_rollback_object(rados_ioctx_t io, const object_t& oid,
                                       SnapContext& snapc, uint64_t snapid);

  // io
  int create(IoCtxImpl& io, const object_t& oid, bool exclusive);
  int write(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len, off_t off);
  int write_full(IoCtxImpl& io, const object_t& oid, bufferlist& bl);
  int read(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len, off_t off);
  int mapext(IoCtxImpl& io, const object_t& oid, off_t off, size_t len, std::map<off_t,size_t>& m);
  int sparse_read(IoCtxImpl& io, const object_t& oid, std::map<off_t,size_t>& m, bufferlist& bl,
		  size_t len, off_t off);
  int remove(IoCtxImpl& io, const object_t& oid);
  int stat(IoCtxImpl& io, const object_t& oid, uint64_t *psize, time_t *pmtime);
  int trunc(IoCtxImpl& io, const object_t& oid, size_t size);

  int tmap_update(IoCtxImpl& io, const object_t& oid, bufferlist& cmdbl);
  int exec(IoCtxImpl& io, const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl);
  int getxattrs(IoCtxImpl& io, const object_t& oid, map<string, bufferlist>& attrset);
  int rmxattr(IoCtxImpl& io, const object_t& oid, const char *name);

  int pool_list(std::list<string>& ls);
  int get_pool_stats(std::list<string>& ls, map<string,pool_stat_t>& result);
  int get_fs_stats(ceph_statfs& result);

  int pool_create(string& name, unsigned long long auid=0, __u8 crush_rule=0);
  int pool_delete(const char *name);
  int pool_change_auid(rados_ioctx_t io, unsigned long long auid);

  int list(Objecter::ListContext *context, int max_entries);

  struct C_aio_Ack : public Context {
    AioCompletionImpl *c;
    void finish(int r) {
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
    C_aio_Ack(AioCompletionImpl *_c) : c(_c) {
      c->get();
    }
  };

  struct C_aio_sparse_read_Ack : public Context {
    AioCompletionImpl *c;
    bufferlist *data_bl;
    std::map<off_t,size_t> *m;

    void finish(int r) {
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
    C_aio_sparse_read_Ack(AioCompletionImpl *_c) : c(_c) {
      c->get();
    }
  };

  struct C_aio_Safe : public Context {
    AioCompletionImpl *c;
    void finish(int r) {
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

      c->put_unlock();
    }
    C_aio_Safe(AioCompletionImpl *_c) : c(_c) {
      c->get();
    }
  };

  int aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
			  bufferlist *pbl, size_t len, off_t off);
  int aio_read(IoCtxImpl& io, object_t oid, AioCompletionImpl *c,
	       char *buf, size_t len, off_t off);
  int aio_sparse_read(IoCtxImpl& io, const object_t oid,
		    AioCompletionImpl *c, std::map<off_t,size_t> *m,
		    bufferlist *data_bl, size_t len, off_t off);
  int aio_write(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
		const bufferlist& bl, size_t len, off_t off);
  int aio_write_full(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
		     const bufferlist& bl);

  static AioCompletionImpl *aio_create_completion() {
    return new AioCompletionImpl;
  }
  static AioCompletionImpl *aio_create_completion(void *cb_arg, rados_callback_t cb_complete, rados_callback_t cb_safe) {
    AioCompletionImpl *c = new AioCompletionImpl;
    if (cb_complete)
      c->set_complete_callback(cb_arg, cb_complete);
    if (cb_safe)
      c->set_safe_callback(cb_arg, cb_safe);
    return c;
  }

  // watch/notify
  struct WatchContext {
    IoCtxImpl io_ctx_impl;
    const object_t oid;
    uint64_t cookie;
    uint64_t ver;
    librados::WatchCtx *ctx;
    ObjectOperation *op;
    uint64_t linger_id;

    WatchContext(IoCtxImpl& io_ctx_impl_, const object_t& _oc, librados::WatchCtx *_ctx,
                 ObjectOperation *_op) : io_ctx_impl(io_ctx_impl_), oid(_oc), ctx(_ctx), op(_op), linger_id(0) {}
    ~WatchContext() {
    }
    void notify(RadosClient *client, MWatchNotify *m) {
      ctx->notify(m->opcode, m->ver);
      if (m->opcode != WATCH_NOTIFY_COMPLETE) {
        client->_notify_ack(io_ctx_impl, oid, m->notify_id, m->ver);
      }
    }
  };

  struct C_NotifyComplete : public librados::WatchCtx {
    Mutex *lock;
    Cond *cond;
    bool *done;

    C_NotifyComplete(Mutex *_l, Cond *_c, bool *_d) : lock(_l), cond(_c), done(_d) {}

    void notify(uint8_t opcode, uint64_t ver) {
      if (opcode != WATCH_NOTIFY_COMPLETE)
        derr << "WARNING: C_NotifyComplete got response: opcode="
	     << (int)opcode << " ver=" << ver << dendl;
      *done = true;
      cond->Signal();
    }
  };

  uint64_t max_watch_cookie;
  map<uint64_t, WatchContext *> watchers;

  void set_sync_op_version(IoCtxImpl& io, eversion_t& ver) {
      io.last_objver = ver;
  }

  void register_watcher(IoCtxImpl& io, const object_t& oid, librados::WatchCtx *ctx,
			ObjectOperation *op, uint64_t *cookie, WatchContext **pwc = NULL) {
    assert(lock.is_locked());
    WatchContext *wc = new WatchContext(io, oid, ctx, op);
    *cookie = ++max_watch_cookie;
    watchers[*cookie] = wc;
    if (pwc)
      *pwc = wc;
  }

  void unregister_watcher(uint64_t cookie) {
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

  void watch_notify(MWatchNotify *m);

  int watch(IoCtxImpl& io, const object_t& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(IoCtxImpl& io, const object_t& oid, uint64_t cookie);
  int notify(IoCtxImpl& io, const object_t& oid, uint64_t ver);
  int _notify_ack(IoCtxImpl& io, const object_t& oid, uint64_t notify_id, uint64_t ver);

  eversion_t last_version(IoCtxImpl& io) {
    return io.last_objver;
  }
  void set_assert_version(IoCtxImpl& io, uint64_t ver) {
    io.assert_ver = ver;
  }

  void set_notify_timeout(IoCtxImpl& io, uint32_t timeout) {
    io.notify_timeout = timeout;
  }
};

int RadosClient::connect()
{
  // get monmap
  int ret = monclient.build_initial_monmap();
  if (ret < 0)
    return ret;

  messenger = new SimpleMessenger();
  if (!messenger)
    return -ENOMEM;

  dout(1) << "starting msgr at " << messenger->get_ms_addr() << dendl;

  messenger->register_entity(entity_name_t::CLIENT(-1));
  dout(1) << "starting objecter" << dendl;

  objecter = new Objecter(messenger, &monclient, &osdmap, lock, timer);
  if (!objecter)
    return -ENOMEM;
  objecter->set_balanced_budget();

  monclient.set_messenger(messenger);

  messenger->add_dispatcher_head(this);

  messenger->start(1);
  messenger->add_dispatcher_head(this);

  dout(1) << "setting wanted keys" << dendl;
  monclient.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  dout(1) << "calling monclient init" << dendl;
  monclient.init();

  int err = monclient.authenticate(g_conf.client_mount_timeout);
  if (err) {
    dout(0) << *g_conf.entity_name << " authentication error " << strerror(-err) << dendl;
    shutdown();
    return err;
  }
  messenger->set_myname(entity_name_t::CLIENT(monclient.get_global_id()));

  lock.Lock();

  timer.init();

  objecter->set_client_incarnation(0);
  objecter->init();
  monclient.renew_subs();

  while (osdmap.get_epoch() == 0) {
    dout(1) << "waiting for osdmap" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();

  dout(1) << "init done" << dendl;

  return 0;
}

void RadosClient::shutdown()
{
  lock.Lock();
  monclient.shutdown();
  objecter->shutdown();
  timer.shutdown();
  lock.Unlock();
  messenger->shutdown();
  messenger->wait();
  dout(1) << "shutdown" << dendl;
}

RadosClient::~RadosClient()
{
  if (messenger)
    messenger->destroy();
}


bool RadosClient::ms_dispatch(Message *m)
{
  lock.Lock();
  bool ret = _dispatch(m);
  lock.Unlock();
  return ret;
}

void RadosClient::ms_handle_connect(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_connect(con);
}

bool RadosClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_reset(con);
  return false;
}

void RadosClient::ms_handle_remote_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_remote_reset(con);
}


bool RadosClient::_dispatch(Message *m)
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

int RadosClient::pool_list(std::list<std::string>& v)
{
  Mutex::Locker l(lock);
  for (map<int,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
       p != osdmap.get_pools().end();
       p++)
    v.push_back(osdmap.get_pool_name(p->first));
  return 0;
}

int RadosClient::get_pool_stats(std::list<string>& pools, map<string,pool_stat_t>& result)
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

int RadosClient::get_fs_stats(ceph_statfs& stats)
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

int RadosClient::snap_create(rados_ioctx_t io, const char *snapName)
{
  int reply;
  int poolID = ((IoCtxImpl *)io)->poolid;
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

int RadosClient::selfmanaged_snap_create(rados_ioctx_t io, uint64_t *psnapid)
{
  int reply;
  int poolID = ((IoCtxImpl *)io)->poolid;

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

int RadosClient::snap_remove(rados_ioctx_t io, const char *snapName)
{
  int reply;
  int poolID = ((IoCtxImpl *)io)->poolid;
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

int RadosClient::selfmanaged_snap_rollback_object(rados_ioctx_t io,
				      const object_t& oid, SnapContext& snapc,
                                      uint64_t snapid)
{
  int reply;
  IoCtxImpl* ctx = (IoCtxImpl *) io;

  object_locator_t oloc(ctx->poolid);

  Mutex mylock("RadosClient::snap_rollback::mylock");
  Cond cond;
  bool done;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &reply);

  lock.Lock();
  objecter->rollback_object(oid, oloc, snapc, snapid,
		     g_clock.now(), onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int RadosClient::rollback(rados_ioctx_t io_, const object_t& oid,
			  const char *snapName)
{
  IoCtxImpl* io = (IoCtxImpl *) io_;
  string sName(snapName);

  snapid_t snap;
  const map<int, pg_pool_t>& pools = objecter->osdmap->get_pools();
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
  if (p == pg_pool.snaps.end()) return -ENOENT;

  return selfmanaged_snap_rollback_object(io_, oid, io->snapc, snap);
}

int RadosClient::selfmanaged_snap_remove(rados_ioctx_t io, uint64_t snapid)
{
  int reply;
  int poolID = ((IoCtxImpl *)io)->poolid;

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

int RadosClient::pool_create(string& name, unsigned long long auid,
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

int RadosClient::pool_delete(const char *name)
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

/**
 * Attempt to change a io's associated auid "owner." Requires that you
 * have write permission on both the current and new auid.
 * io: reference to the io to change.
 * auid: the auid you wish the io to have.
 * Returns: 0 on success, or -ERROR# on failure.
 */
int RadosClient::pool_change_auid(rados_ioctx_t io, unsigned long long auid)
{
  int reply;

  int poolID = ((IoCtxImpl *)io)->poolid;

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

int RadosClient::snap_list(IoCtxImpl *io, vector<uint64_t> *snaps)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       p++)
    snaps->push_back(p->first);
  return 0;
}

int RadosClient::snap_lookup(IoCtxImpl *io, const char *name, uint64_t *snapid)
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

int RadosClient::snap_get_name(IoCtxImpl *io, uint64_t snapid, std::string *s)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(io->poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.find(snapid);
  if (p == pi->snaps.end())
    return -ENOENT;
  *s = p->second.name.c_str();
  return 0;
}

int RadosClient::snap_get_stamp(IoCtxImpl *io, uint64_t snapid, time_t *t)
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

int RadosClient::list(Objecter::ListContext *context, int max_entries)
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

int RadosClient::create(IoCtxImpl& io, const object_t& oid, bool exclusive)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::create::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->create(oid, oloc,
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

int RadosClient::write(IoCtxImpl& io, const object_t& oid,
		       bufferlist& bl, size_t len, off_t off)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::write::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->write(oid, oloc,
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

int RadosClient::write_full(IoCtxImpl& io, const object_t& oid, bufferlist& bl)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  eversion_t ver;
  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->write_full(oid, oloc,
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

int RadosClient::aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
			  bufferlist *pbl, size_t len, off_t off)
{

  Context *onack = new C_aio_Ack(c);
  eversion_t ver;

  c->pbl = pbl;

  Mutex::Locker l(lock);
  object_locator_t oloc(io.poolid);
  objecter->read(oid, oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack, &c->objver);
  return 0;
}

int RadosClient::aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
			  char *buf, size_t len, off_t off)
{
  Context *onack = new C_aio_Ack(c);

  c->buf = buf;
  c->maxlen = len;

  Mutex::Locker l(lock);
  object_locator_t oloc(io.poolid);
  objecter->read(oid, oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack, &c->objver);

  return 0;
}

int RadosClient::aio_sparse_read(IoCtxImpl& io, const object_t oid,
			  AioCompletionImpl *c, std::map<off_t,size_t> *m,
			  bufferlist *data_bl, size_t len, off_t off)
{

  C_aio_sparse_read_Ack *onack = new C_aio_sparse_read_Ack(c);
  onack->m = m;
  onack->data_bl = data_bl;
  eversion_t ver;

  c->pbl = NULL;

  Mutex::Locker l(lock);
  object_locator_t oloc(io.poolid);
  objecter->sparse_read(oid, oloc,
		 off, len, io.snap_seq, &c->bl, 0,
		 onack);
  return 0;
}

int RadosClient::aio_write(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
			   const bufferlist& bl, size_t len, off_t off)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(lock);
  object_locator_t oloc(io.poolid);
  objecter->write(oid, oloc,
		  off, len, io.snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int RadosClient::aio_write_full(IoCtxImpl& io, const object_t &oid,
		AioCompletionImpl *c, const bufferlist& bl)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(lock);
  object_locator_t oloc(io.poolid);
  objecter->write_full(oid, oloc,
		  io.snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int RadosClient::remove(IoCtxImpl& io, const object_t& oid)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::remove::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->remove(oid, oloc,
		  snapc, ut, 0,
		  onack, NULL, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int RadosClient::trunc(IoCtxImpl& io, const object_t& oid, size_t size)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->trunc(oid, oloc,
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

int RadosClient::tmap_update(IoCtxImpl& io, const object_t& oid, bufferlist& cmdbl)
{
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::tmap_update::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock.Lock();
  SnapContext snapc;
  object_locator_t oloc(io.poolid);
  ObjectOperation wr;
  if (io.assert_ver) {
    wr.assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  wr.tmap_update(cmdbl);
  objecter->mutate(oid, oloc, wr, snapc, ut, 0, onack, NULL, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}


int RadosClient::exec(IoCtxImpl& io, const object_t& oid, const char *cls, const char *method,
		      bufferlist& inbl, bufferlist& outbl)
{
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::exec::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;


  lock.Lock();
  object_locator_t oloc(io.poolid);
  ObjectOperation rd;
  if (io.assert_ver) {
    rd.assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  rd.call(cls, method, inbl);
  objecter->read(oid, oloc, rd, io.snap_seq, &outbl, 0, onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}

int RadosClient::read(IoCtxImpl& io, const object_t& oid,
		      bufferlist& bl, size_t len, off_t off)
{
  SnapContext snapc;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->read(oid, oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from read r=" << r << dendl;

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  if (bl.length() < len) {
    dout(10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int RadosClient::mapext(IoCtxImpl& io, const object_t& oid, off_t off, size_t len, std::map<off_t,size_t>& m)
{
  SnapContext snapc;
  bufferlist bl;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->mapext(oid, oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);

  return m.size();
}

int RadosClient::sparse_read(IoCtxImpl& io, const object_t& oid,
	  std::map<off_t,size_t>& m, bufferlist& data_bl, size_t len, off_t off)
{
  SnapContext snapc;
  bufferlist bl;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->sparse_read(oid, oloc,
	      off, len, io.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);
  ::decode(data_bl, iter);

  return m.size();
}

int RadosClient::stat(IoCtxImpl& io, const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  SnapContext snapc;

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

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->stat(oid, oloc,
	      io.snap_seq, psize, &mtime, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from stat" << dendl;

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  set_sync_op_version(io, ver);

  return r;
}

int RadosClient::getxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl)
{
  SnapContext snapc;

  Mutex mylock("RadosClient::getxattr::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->getxattr(oid, oloc,
	      name, io.snap_seq, &bl, 0,
              onack, &ver, pop);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from getxattr" << dendl;

  set_sync_op_version(io, ver);

  if (r < 0)
    return r;

  return bl.length();
}

int RadosClient::rmxattr(IoCtxImpl& io, const object_t& oid, const char *name)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::rmxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  object_locator_t oloc(io.poolid);

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  lock.Lock();
  objecter->removexattr(oid, oloc, name,
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

int RadosClient::setxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::setxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  lock.Lock();
  object_locator_t oloc(io.poolid);
  objecter->setxattr(oid, oloc, name,
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

  return bl.length();
}

int RadosClient::getxattrs(IoCtxImpl& io, const object_t& oid, map<std::string, bufferlist>& attrset)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (io.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::getexattrs::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  ObjectOperation op, *pop = NULL;
  if (io.assert_ver) {
    op.assert_version(io.assert_ver);
    io.assert_ver = 0;
    pop = &op;
  }
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  object_locator_t oloc(io.poolid);
  map<string, bufferlist> aset;
  objecter->getxattrs(oid, oloc, io.snap_seq,
		      aset,
		      0, onack, &ver, pop);
  lock.Unlock();

  attrset.clear();


  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  for (map<string,bufferlist>::iterator p = aset.begin(); p != aset.end(); p++) {
    dout(10) << "RadosClient::getxattrs: xattr=" << p->first << dendl;
    attrset[p->first.c_str()] = p->second;
  }

  set_sync_op_version(io, ver);

  return r;
}

void RadosClient::watch_notify(MWatchNotify *m)
{
  assert(lock.is_locked());
  WatchContext *wc = NULL;
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(m->cookie);
  if (iter != watchers.end())
    wc = iter->second;

  if (!wc)
    return;

  wc->notify(this, m);
}

int RadosClient::watch(IoCtxImpl& io, const object_t& oid, uint64_t ver,
		       uint64_t *cookie, librados::WatchCtx *ctx)
{
  utime_t ut = g_clock.now();

  ObjectOperation *rd = new ObjectOperation();
  if (!rd)
    return -ENOMEM;

  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t objver;

  lock.Lock();

  WatchContext *wc;
  register_watcher(io, oid, ctx, rd, cookie, &wc);

  object_locator_t oloc(io.poolid);

  if (io.assert_ver) {
    rd->assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  rd->watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger(oid, oloc, *rd, io.snap_seq, bl, NULL, 0, onack, NULL, &objver);
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
int RadosClient::_notify_ack(IoCtxImpl& io, const object_t& oid, uint64_t notify_id, uint64_t ver)
{
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  eversion_t objver;

  object_locator_t oloc(io.poolid);
  ObjectOperation rd;
  if (io.assert_ver) {
    rd.assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  rd.notify_ack(notify_id, ver);
  objecter->read(oid, oloc, rd, io.snap_seq, NULL, 0, 0, 0);

  return 0;
}

int RadosClient::unwatch(IoCtxImpl& io, const object_t& oid, uint64_t cookie)
{
  utime_t ut = g_clock.now();
  bufferlist inbl, outbl;

  Mutex mylock("RadosClient::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;
  lock.Lock();

  unregister_watcher(cookie);

  object_locator_t oloc(io.poolid);
  ObjectOperation rd;
  if (io.assert_ver) {
    rd.assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  rd.watch(cookie, 0, 0);
  objecter->read(oid, oloc, rd, io.snap_seq, &outbl, 0, onack, &ver);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(io, ver);

  return r;
}// ---------------------------------------------

int RadosClient::notify(IoCtxImpl& io, const object_t& oid, uint64_t ver)
{
  utime_t ut = g_clock.now();
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
  ObjectOperation rd;

  object_locator_t oloc(io.poolid);
  if (io.assert_ver) {
    rd.assert_version(io.assert_ver);
    io.assert_ver = 0;
  }
  lock.Lock();
  register_watcher(io, oid, ctx, &rd, &cookie);
  uint32_t prot_ver = 1;
  uint32_t timeout = io.notify_timeout;
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  rd.notify(cookie, ver, inbl);
  objecter->read(oid, oloc, rd, io.snap_seq, &outbl, 0, onack, &objver);
  lock.Unlock();

  mylock_all.Lock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  while (!done_all)
    cond_all.Wait(mylock_all);
  mylock_all.Unlock();

  lock.Lock();
  unregister_watcher(cookie);
  lock.Unlock();

  set_sync_op_version(io, objver);
  delete ctx;

  return r;
}

///////////////////////////// ObjectIterator /////////////////////////////
librados::ObjectIterator::
ObjectIterator(ObjListCtx *ctx_)
: ctx(ctx_)
{
}

librados::ObjectIterator::
~ObjectIterator()
{
  ctx.reset();
}

bool librados::ObjectIterator::
operator==(const librados::ObjectIterator& rhs) const {
  return (ctx.get() == rhs.ctx.get());
}

bool librados::ObjectIterator::
operator!=(const librados::ObjectIterator& rhs) const {
  return (ctx.get() != rhs.ctx.get());
}

const std::string& librados::ObjectIterator::
operator*() const {
  return cur_obj;
}

librados::ObjectIterator& librados::ObjectIterator::
operator++()
{
  get_next();
  return *this;
}

librados::ObjectIterator librados::ObjectIterator::
operator++(int)
{
  librados::ObjectIterator ret(*this);
  get_next();
  return ret;
}

void librados::ObjectIterator::
get_next()
{
  const char *entry;
  int ret = rados_objects_list_next(ctx.get(), &entry);
  if (ret == -ENOENT) {
    ctx.reset();
    *this = __EndObjectIterator;
    return;
  }
  else if (ret) {
    ostringstream oss;
    oss << "rados_objects_list_next returned " << ret;
    throw std::runtime_error(oss.str());
  }

  cur_obj = entry;
}

const librados::ObjectIterator librados::ObjectIterator::
__EndObjectIterator(NULL);

///////////////////////////// AioCompletion //////////////////////////////
int librados::AioCompletion::AioCompletion::
set_complete_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_complete_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::
set_safe_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_safe_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::
wait_for_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}

int librados::AioCompletion::AioCompletion::
wait_for_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_safe();
}

bool librados::AioCompletion::AioCompletion::
is_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete();
}

bool librados::AioCompletion::AioCompletion::
is_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_safe();
}

int librados::AioCompletion::AioCompletion::
get_return_value()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_return_value();
}

int librados::AioCompletion::AioCompletion::
get_version()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_version();
}

void librados::AioCompletion::AioCompletion::
release()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  c->release();
}

///////////////////////////// IoCtx //////////////////////////////
librados::IoCtx::
IoCtx() : io_ctx_impl(NULL)
{
}

void librados::IoCtx::
from_rados_ioctx_t(rados_ioctx_t p, IoCtx &io)
{
  IoCtxImpl *io_ctx_impl = (IoCtxImpl*)p;

  io.io_ctx_impl = io_ctx_impl;
}

librados::IoCtx::
~IoCtx()
{
  close();
}

void librados::IoCtx::
close()
{
  delete io_ctx_impl;
  io_ctx_impl = 0;
}

int librados::IoCtx::
set_auid(uint64_t auid_)
{
  return io_ctx_impl->client->pool_change_auid(io_ctx_impl, auid_);
}

int librados::IoCtx::
create(const std::string& oid, bool exclusive)
{
  object_t obj(oid);
  return io_ctx_impl->client->create(*io_ctx_impl, obj, exclusive);
}

int librados::IoCtx::
write(const std::string& oid, bufferlist& bl, size_t len, off_t off)
{
  object_t obj(oid);
  return io_ctx_impl->client->write(*io_ctx_impl, obj, bl, len, off);
}

int librados::IoCtx::
write_full(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->client->write_full(*io_ctx_impl, obj, bl);
}

int librados::IoCtx::
read(const std::string& oid, bufferlist& bl, size_t len, off_t off)
{
  object_t obj(oid);
  return io_ctx_impl->client->read(*io_ctx_impl, obj, bl, len, off);
}

int librados::IoCtx::
remove(const std::string& oid)
{
  object_t obj(oid);
  return io_ctx_impl->client->remove(*io_ctx_impl, obj);
}

int librados::IoCtx::
trunc(const std::string& oid, size_t size)
{
  object_t obj(oid);
  return io_ctx_impl->client->trunc(*io_ctx_impl, obj, size);
}

int librados::IoCtx::
mapext(const std::string& oid, off_t off, size_t len, std::map<off_t, size_t>& m)
{
  object_t obj(oid);
  return io_ctx_impl->client->mapext(*io_ctx_impl, oid, off, len, m);
}

int librados::IoCtx::
sparse_read(const std::string& oid, std::map<off_t, size_t>& m,
	    bufferlist& bl, size_t len, off_t off)
{
  object_t obj(oid);
  return io_ctx_impl->client->sparse_read(*io_ctx_impl, oid, m, bl, len, off);
}

int librados::IoCtx::
getxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->client->getxattr(*io_ctx_impl, obj, name, bl);
}

int librados::IoCtx::
getxattrs(const std::string& oid, map<std::string, bufferlist>& attrset)
{
  object_t obj(oid);
  return io_ctx_impl->client->getxattrs(*io_ctx_impl, obj, attrset);
}

int librados::IoCtx::
setxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->client->setxattr(*io_ctx_impl, obj, name, bl);
}

int librados::IoCtx::
rmxattr(const std::string& oid, const char *name)
{
  object_t obj(oid);
  return io_ctx_impl->client->rmxattr(*io_ctx_impl, obj, name);
}

int librados::IoCtx::
stat(const std::string& oid, uint64_t *psize, time_t *pmtime)
{
  object_t obj(oid);
  return io_ctx_impl->client->stat(*io_ctx_impl, oid, psize, pmtime);
}

int librados::IoCtx::
exec(const std::string& oid, const char *cls, const char *method,
    bufferlist& inbl, bufferlist& outbl)
{
  object_t obj(oid);
  return io_ctx_impl->client->exec(*io_ctx_impl, obj, cls, method, inbl, outbl);
}

int librados::IoCtx::
tmap_update(const std::string& oid, bufferlist& cmdbl)
{
  object_t obj(oid);
  return io_ctx_impl->client->tmap_update(*io_ctx_impl, obj, cmdbl);
}

void librados::IoCtx::
snap_set_read(snap_t seq)
{
  io_ctx_impl->set_snap_read(seq);
}

int librados::IoCtx::
selfmanaged_snap_set_write_ctx(snap_t seq, vector<snap_t>& snaps)
{
  vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (unsigned i=0; i<snaps.size(); i++)
    snv[i] = snaps[i];
  return io_ctx_impl->set_snap_write_context(seq, snv);
}

int librados::IoCtx::
snap_create(const char *snapname)
{
  return io_ctx_impl->client->snap_create(io_ctx_impl, snapname);
}

int librados::IoCtx::
snap_lookup(const char *name, snap_t *snapid)
{
  return io_ctx_impl->client->snap_lookup(io_ctx_impl, name, snapid);
}

int librados::IoCtx::
snap_get_stamp(snap_t snapid, time_t *t)
{
  return io_ctx_impl->client->snap_get_stamp(io_ctx_impl, snapid, t);
}

int librados::IoCtx::
snap_get_name(snap_t snapid, std::string *s)
{
  return io_ctx_impl->client->snap_get_name(io_ctx_impl, snapid, s);
}

int librados::IoCtx::
snap_remove(const char *snapname)
{
  return io_ctx_impl->client->snap_remove(io_ctx_impl, snapname);
}

int librados::IoCtx::
snap_list(std::vector<snap_t> *snaps)
{
  return io_ctx_impl->client->snap_list(io_ctx_impl, snaps);
}

int librados::IoCtx::
rollback(const std::string& oid, const char *snapname)
{
  return io_ctx_impl->client->rollback(io_ctx_impl, oid, snapname);
}

int librados::IoCtx::
selfmanaged_snap_create(uint64_t *snapid)
{
  return io_ctx_impl->client->selfmanaged_snap_create(io_ctx_impl, snapid);
}

int librados::IoCtx::
selfmanaged_snap_remove(uint64_t snapid)
{
  return io_ctx_impl->client->selfmanaged_snap_remove(io_ctx_impl, snapid);
}

librados::ObjectIterator librados::IoCtx::
objects_begin()
{
  rados_list_ctx_t listh;
  rados_objects_list_open(io_ctx_impl, &listh);
  ObjectIterator iter((ObjListCtx*)listh);
  iter.get_next();
  return iter;
}

const librados::ObjectIterator& librados::IoCtx::
objects_end() const
{
  return ObjectIterator::__EndObjectIterator;
}

uint64_t librados::IoCtx::
get_last_version()
{
  eversion_t ver = io_ctx_impl->client->last_version(*io_ctx_impl);
  return ver.version;
}

int librados::IoCtx::
aio_read(const std::string& oid, librados::AioCompletion *c,
	 bufferlist *pbl, size_t len, off_t off)
{
  return io_ctx_impl->client->aio_read(*io_ctx_impl, oid, c->pc, pbl, len, off);
}

int librados::IoCtx::
aio_sparse_read(const std::string& oid, librados::AioCompletion *c,
		std::map<off_t,size_t> *m, bufferlist *data_bl,
		size_t len, off_t off)
{
  return io_ctx_impl->client->aio_sparse_read(*io_ctx_impl, oid, c->pc,
					   m, data_bl, len, off);
}

int librados::IoCtx::
aio_write(const std::string& oid, librados::AioCompletion *c, const bufferlist& bl,
	  size_t len, off_t off)
{
  return io_ctx_impl->client->aio_write(*io_ctx_impl, oid, c->pc, bl, len, off );
}

int librados::IoCtx::
aio_write_full(const std::string& oid, librados::AioCompletion *c, const bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->client->aio_write_full(*io_ctx_impl, obj, c->pc, bl);
}

int librados::IoCtx::
watch(const string& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx)
{
  object_t obj(oid);
  return io_ctx_impl->client->watch(*io_ctx_impl, obj, ver, cookie, ctx);
}

int librados::IoCtx::
unwatch(const string& oid, uint64_t handle)
{
  uint64_t cookie = handle;
  object_t obj(oid);
  return io_ctx_impl->client->unwatch(*io_ctx_impl, obj, cookie);
}

int librados::IoCtx::
notify(const string& oid, uint64_t ver)
{
  object_t obj(oid);
  return io_ctx_impl->client->notify(*io_ctx_impl, obj, ver);
}

void librados::IoCtx::
set_notify_timeout(uint32_t timeout)
{
  io_ctx_impl->client->set_notify_timeout(*io_ctx_impl, timeout);
}

void librados::IoCtx::
set_assert_version(uint64_t ver)
{
  io_ctx_impl->client->set_assert_version(*io_ctx_impl, ver);
}

const std::string& librados::IoCtx::
get_pool_name() const
{
  return io_ctx_impl->pool_name;
}

librados::IoCtx::
IoCtx(IoCtxImpl *io_ctx_impl_)
  : io_ctx_impl(io_ctx_impl_)
{
}

///////////////////////////// Rados //////////////////////////////
void librados::Rados::
version(int *major, int *minor, int *extra)
{
  rados_version(major, minor, extra);
}

librados::Rados::
Rados() : client(NULL)
{
}

librados::Rados::
~Rados()
{
  shutdown();
}

int librados::Rados::
init(const char * const id)
{
  return rados_create((rados_t *)&client, id);
}

int librados::Rados::
connect()
{
  return client->connect();
}

void librados::Rados::
shutdown()
{
  if (!client)
    return;
  client->shutdown();
  delete client;
  client = NULL;
}

int librados::Rados::
conf_read_file(const char * const path) const
{
  return rados_conf_read_file((rados_t)client, path);
}

int librados::Rados::
conf_set(const char *option, const char *value)
{
  return rados_conf_set((rados_t)client, option, value);
}

void librados::Rados::
reopen_log()
{
  rados_reopen_log((rados_t)client);
}

int librados::Rados::
conf_get(const char *option, std::string &val)
{
  char *str;
  int ret = g_conf.get_val(option, &str, -1);
  if (ret)
    return ret;
  val = str;
  free(str);
  return 0;
}

int librados::Rados::
pool_create(const char *name)
{
  string str(name);
  return client->pool_create(str);
}

int librados::Rados::
pool_create(const char *name, uint64_t auid)
{
  string str(name);
  return client->pool_create(str, auid);
}

int librados::Rados::
pool_create(const char *name, uint64_t auid, __u8 crush_rule)
{
  string str(name);
  return client->pool_create(str, auid, crush_rule);
}

int librados::Rados::
pool_delete(const char *name)
{
  return client->pool_delete(name);
}

int librados::Rados::
pool_list(std::list<std::string>& v)
{
  return client->pool_list(v);
}

int librados::Rados::
pool_lookup(const char *name)
{
  return client->lookup_pool(name);
}

int librados::Rados::
ioctx_open(const char *name, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_open((rados_t)client, name, &p);
  if (ret)
    return ret;
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

int librados::Rados::
get_pool_stats(std::list<string>& v, std::map<string,pool_stat_t>& result)
{
  map<string,::pool_stat_t> rawresult;
  int r = client->get_pool_stats(v, rawresult);
  for (map<string,::pool_stat_t>::iterator p = rawresult.begin();
       p != rawresult.end();
       p++) {
    pool_stat_t& v = result[p->first];
    v.num_kb = p->second.num_kb;
    v.num_bytes = p->second.num_bytes;
    v.num_objects = p->second.num_objects;
    v.num_object_clones = p->second.num_object_clones;
    v.num_object_copies = p->second.num_object_copies;
    v.num_objects_missing_on_primary = p->second.num_objects_missing_on_primary;
    v.num_objects_unfound = p->second.num_objects_unfound;
    v.num_objects_degraded = p->second.num_objects_degraded;
    v.num_rd = p->second.num_rd;
    v.num_rd_kb = p->second.num_rd_kb;
    v.num_wr = p->second.num_wr;
    v.num_wr_kb = p->second.num_wr_kb;
  }
  return r;
}

int librados::Rados::
get_fs_stats(statfs_t& result)
{
  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result.kb = stats.kb;
  result.kb_used = stats.kb_used;
  result.kb_avail = stats.kb_avail;
  result.num_objects = stats.num_objects;
  return r;
}

librados::AioCompletion *librados::Rados::
aio_create_completion()
{
  AioCompletionImpl *c = RadosClient::aio_create_completion();
  return new AioCompletion(c);
}

librados::AioCompletion *librados::Rados::
aio_create_completion(void *cb_arg, callback_t cb_complete, callback_t cb_safe)
{
  AioCompletionImpl *c = RadosClient::aio_create_completion(cb_arg, cb_complete, cb_safe);
  return new AioCompletion(c);
}

///////////////////////////// C API //////////////////////////////
static Mutex rados_init_mutex("rados_init");
static int rados_initialized = 0;

static void rados_set_conf_defaults(md_config_t *conf)
{
  /* Default configuration values for rados clients.
   * These can be overridden by using rados_conf_set or rados_conf_read.
   */
  free((void*)conf->log_file);
  conf->log_file = NULL;

  free((void*)conf->log_dir);
  conf->log_dir = NULL;

  free((void*)conf->log_sym_dir);
  conf->log_sym_dir = NULL;

  conf->log_sym_history = 0;

  conf->log_to_stderr = LOG_TO_STDERR_SOME;

  conf->log_to_syslog = false;

  conf->log_per_instance = false;

  conf->log_to_file = false;
}

extern "C" int rados_create(rados_t *pcluster, const char * const id)
{
  rados_init_mutex.Lock();
  if (!rados_initialized) {
    // parse environment
    vector<const char*> args;
    env_to_vec(args);

    if (id)
      g_conf.id = strdup(id);
    common_init(args, "librados", STARTUP_FLAG_INIT_KEYS);

    ++rados_initialized;

    rados_set_conf_defaults(&g_conf);
  }
  rados_init_mutex.Unlock();
  RadosClient *radosp = new RadosClient;
  *pcluster = (void *)radosp;
  return 0;
}

extern "C" int rados_connect(rados_t cluster)
{
  RadosClient *radosp = (RadosClient *)cluster;
  return radosp->connect();
}

extern "C" void rados_shutdown(rados_t cluster)
{
  RadosClient *radosp = (RadosClient *)cluster;
  radosp->shutdown();
  delete radosp;
}

extern "C" void rados_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRADOS_VER_MAJOR;
  if (minor)
    *minor = LIBRADOS_VER_MINOR;
  if (extra)
    *extra = LIBRADOS_VER_EXTRA;
}


// -- config --
extern "C" int rados_conf_read_file(rados_t cluster, const char *path)
{
  // TODO: don't call common_init again.
  // Split out the config-reading part of common_init from the rest of it
  vector<const char*> args;
  args.push_back("-c");
  args.push_back(path);
  args.push_back("-i");
  args.push_back(g_conf.id);
  common_init(args, "librados", STARTUP_FLAG_INIT_KEYS);

  return 0;
}

extern "C" int rados_conf_set(rados_t cluster, const char *option, const char *value)
{
  return g_conf.set_val(option, value);
}

extern "C" void rados_reopen_log(rados_t cluster)
{
  sighup_handler(SIGHUP);
}

extern "C" int rados_conf_get(rados_t cluster, const char *option, char *buf, int len)
{
  char *tmp = buf;
  if (len <= 0)
    return -EINVAL;
  return g_conf.get_val(option, &tmp, len);
}

extern "C" int rados_ioctx_lookup(rados_t cluster, const char *name)
{
  RadosClient *radosp = (RadosClient *)cluster;
  return radosp->lookup_pool(name);
}

extern "C" int rados_pool_list(rados_t cluster, char *buf, int len)
{
  RadosClient *client = (RadosClient *)cluster;
  std::list<std::string> pools;
  client->pool_list(pools);

  char *b = buf;
  if (b)
    memset(b, 0, len);
  int needed = 0;
  std::list<std::string>::const_iterator i = pools.begin();
  std::list<std::string>::const_iterator p_end = pools.end();
  for (; i != p_end; ++i) {
    if (len <= 0)
      break;
    int rl = i->length() + 1;
    strncat(b, i->c_str(), len - 2); // leave space for two NULLs
    needed += rl;
    len -= rl;
    b += rl;
  }
  for (; i != p_end; ++i) {
    int rl = i->length() + 1;
    needed += rl;
  }
  return needed + 1;
}

extern "C" int rados_ioctx_open(rados_t cluster, const char *name, rados_ioctx_t *io)
{
  RadosClient *radosp = (RadosClient *)cluster;
  int poolid = radosp->lookup_pool(name);
  if (poolid >= 0) {
    IoCtxImpl *ctx = new IoCtxImpl(radosp, poolid, name, CEPH_NOSNAP);
    if (!ctx)
      return -ENOMEM;
    *io = ctx;
    return 0;
  }
  return poolid;
}

extern "C" void rados_ioctx_close(rados_ioctx_t io)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  delete ctx;
}

extern "C" int rados_ioctx_stat(rados_ioctx_t io, struct rados_ioctx_stat_t *stats)
{
  IoCtxImpl *io_ctx_impl = (IoCtxImpl *)io;
  list<string> ls;
  ls.push_back(io_ctx_impl->pool_name);
  map<string, ::pool_stat_t> rawresult;

  int err = io_ctx_impl->client->get_pool_stats(ls, rawresult);
  if (err)
    return err;

  ::pool_stat_t& r = rawresult[io_ctx_impl->pool_name];
  stats->num_kb = r.num_kb;
  stats->num_bytes = r.num_bytes;
  stats->num_objects = r.num_objects;
  stats->num_object_clones = r.num_object_clones;
  stats->num_object_copies = r.num_object_copies;
  stats->num_objects_missing_on_primary = r.num_objects_missing_on_primary;
  stats->num_objects_unfound = r.num_objects_unfound;
  stats->num_objects_degraded = r.num_objects_degraded;
  stats->num_rd = r.num_rd;
  stats->num_rd_kb = r.num_rd_kb;
  stats->num_wr = r.num_wr;
  stats->num_wr_kb = r.num_wr_kb;
  return 0;
}


extern "C" void rados_snap_set_read(rados_ioctx_t io, rados_snap_t seq)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  ctx->set_snap_read((snapid_t)seq);
}

extern "C" int rados_snap_set_write_context(rados_ioctx_t io, rados_snap_t seq,
				       rados_snap_t *snaps, int num_snaps)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  vector<snapid_t> snv;
  snv.resize(num_snaps);
  for (int i=0; i<num_snaps; i++)
    snv[i] = (snapid_t)snaps[i];
  return ctx->set_snap_write_context((snapid_t)seq, snv);
}

extern "C" int rados_write(rados_ioctx_t io, const char *o, const char *buf, size_t len, off_t off)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->client->write(*ctx, oid, bl, len, off);
}

extern "C" int rados_write_full(rados_ioctx_t io, const char *o, const char *buf, size_t len, off_t off)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->client->write_full(*ctx, oid, bl);
}

extern "C" int rados_trunc(rados_ioctx_t io, const char *o, size_t size)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->trunc(*ctx, oid, size);
}

extern "C" int rados_remove(rados_ioctx_t io, const char *o)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->remove(*ctx, oid);
}

extern "C" int rados_read(rados_ioctx_t io, const char *o, char *buf, size_t len, off_t off)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  int ret;
  object_t oid(o);

  bufferlist bl;
  bufferptr bp = buffer::create_static(len, buf);
  bl.push_back(bp);

  ret = ctx->client->read(*ctx, oid, bl, len, off);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    if (bl.c_str() != buf)
      bl.copy(0, bl.length(), buf);
    ret = bl.length();    // hrm :/
  }

  return ret;
}

extern "C" uint64_t rados_get_last_version(rados_ioctx_t io)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  eversion_t ver = ctx->client->last_version(*ctx);
  return ver.version;
}

extern "C" int rados_pool_create(rados_t cluster, const char *name)
{
  RadosClient *radosp = (RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname);
}

extern "C" int rados_pool_create_with_auid(rados_t cluster, const char *name,
					   uint64_t auid)
{
  RadosClient *radosp = (RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, auid);
}

extern "C" int rados_pool_create_with_crush_rule(rados_t cluster, const char *name,
						 __u8 crush_rule)
{
  RadosClient *radosp = (RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, 0, crush_rule);
}

extern "C" int rados_pool_create_with_all(rados_t cluster, const char *name,
					  uint64_t auid, __u8 crush_rule)
{
  RadosClient *radosp = (RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, auid, crush_rule);
}

extern "C" int rados_pool_delete(rados_t cluster, const char *pool_name)
{
  RadosClient *client = (RadosClient *)cluster;
  return client->pool_delete(pool_name);
}

extern "C" int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->pool_change_auid(ctx, auid);
}

// snaps

extern "C" int rados_ioctx_snap_create(rados_ioctx_t io, const char *snapname)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->snap_create(ctx, snapname);
}

extern "C" int rados_ioctx_snap_remove(rados_ioctx_t io, const char *snapname)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->snap_remove(ctx, snapname);
}

extern "C" int rados_rollback(rados_ioctx_t io, const char *oid,
			      const char *snapname)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->rollback(ctx, oid, snapname);
}

extern "C" int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io,
					     uint64_t *snapid)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->selfmanaged_snap_create(ctx, snapid);
}

extern "C" int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io,
					     uint64_t snapid)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->selfmanaged_snap_remove(ctx, snapid);
}

extern "C" int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t *snaps,
				    int maxlen)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  vector<uint64_t> snapvec;
  int r = ctx->client->snap_list(ctx, &snapvec);
  if (r < 0)
    return r;
  if ((int)snapvec.size() <= maxlen) {
    for (unsigned i=0; i<snapvec.size(); i++)
      snaps[i] = snapvec[i];
    return snapvec.size();
  }
  return -ERANGE;
}

extern "C" int rados_ioctx_snap_lookup(rados_ioctx_t io, const char *name,
				      rados_snap_t *id)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->snap_lookup(ctx, name, (uint64_t *)id);
}

extern "C" int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id,
					char *name, int maxlen)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  std::string sname;
  int r = ctx->client->snap_get_name(ctx, id, &sname);
  if (r < 0)
    return r;
  if ((int)sname.length() >= maxlen)
    return -ERANGE;
  strncpy(name, sname.c_str(), maxlen);
  return 0;
}

extern "C" int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t *t)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  return ctx->client->snap_get_stamp(ctx, id, t);
}

extern "C" int rados_getxattr(rados_ioctx_t io, const char *o, const char *name,
			      char *buf, size_t len)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  int ret;
  object_t oid(o);
  bufferlist bl;
  ret = ctx->client->getxattr(*ctx, oid, name, bl);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    bl.copy(0, bl.length(), buf);
    ret = bl.length();
  }

  return ret;
}

extern "C" int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->client->setxattr(*ctx, oid, name, bl);
}

extern "C" int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->rmxattr(*ctx, oid, name);
}

extern "C" int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->stat(*ctx, oid, psize, pmtime);
}

extern "C" int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist cmdbl;
  cmdbl.append(cmdbuf, cmdbuflen);
  return ctx->client->tmap_update(*ctx, oid, cmdbl);
}

extern "C" int rados_exec(rados_ioctx_t io, const char *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = ctx->client->exec(*ctx, oid, cls, method, inbl, outbl);
  if (ret >= 0) {
    if (outbl.length()) {
      if (outbl.length() > out_len)
	return -ERANGE;
      outbl.copy(0, outbl.length(), buf);
      ret = outbl.length();   // hrm :/
    }
  }
  return ret;
}

/* list objects */

extern "C" int rados_objects_list_open(rados_ioctx_t io, rados_list_ctx_t *listh)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  Objecter::ListContext *h = new Objecter::ListContext;
  h->pool_id = ctx->poolid;
  h->pool_snap_seq = ctx->snap_seq;
  *listh = (void *)new ObjListCtx(ctx, h);
  return 0;
}

extern "C" void rados_objects_list_close(rados_list_ctx_t h)
{
  ObjListCtx *lh = (ObjListCtx *)h;
  delete lh;
}

extern "C" int rados_objects_list_next(rados_list_ctx_t listctx, const char **entry)
{
  ObjListCtx *lh = (ObjListCtx *)listctx;
  Objecter::ListContext *h = lh->lc;
  int ret;

  // if the list is non-empty, this method has been called before
  if (!h->list.empty())
    // so let's kill the previously-returned object
    h->list.pop_front();

  if (h->list.empty()) {
    ret = lh->ctx->client->list(lh->lc, RADOS_LIST_MAX_ENTRIES);
    if (!h->list.size())
      return -ENOENT;
  }

  *entry = h->list.front().name.c_str();
  return 0;
}



// -------------------------
// aio

extern "C" int rados_aio_create_completion(void *cb_arg, rados_callback_t cb_complete, rados_callback_t cb_safe,
					   rados_completion_t *pc)
{
  *pc = RadosClient::aio_create_completion(cb_arg, cb_complete, cb_safe);
  return 0;
}

extern "C" int rados_aio_wait_for_complete(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->wait_for_complete();
}

extern "C" int rados_aio_wait_for_safe(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->wait_for_safe();
}

extern "C" int rados_aio_is_complete(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->is_complete();
}

extern "C" int rados_aio_is_safe(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->is_safe();
}

extern "C" int rados_aio_get_return_value(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->get_return_value();
}

extern "C" uint64_t rados_aio_get_version(rados_completion_t c)
{
  return ((AioCompletionImpl*)c)->get_version();
}

extern "C" void rados_aio_release(rados_completion_t c)
{
  ((AioCompletionImpl*)c)->put();
}

extern "C" int rados_aio_read(rados_ioctx_t io, const char *o,
			       rados_completion_t completion,
			       char *buf, size_t len, off_t off)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->aio_read(*ctx, oid,
	     (AioCompletionImpl*)completion, buf, len, off);
}

extern "C" int rados_aio_write(rados_ioctx_t io, const char *o,
				rados_completion_t completion,
				const char *buf, size_t len, off_t off)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->client->aio_write(*ctx, oid,
	      (AioCompletionImpl*)completion, bl, len, off);
}

extern "C" int rados_aio_write_full(rados_ioctx_t io, const char *o,
			 rados_completion_t completion,
			 const char *buf, size_t len)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->client->aio_write_full(*ctx, oid,
	      (AioCompletionImpl*)completion, bl);
}

struct C_WatchCB : public librados::WatchCtx {
  rados_watchcb_t wcb;
  void *arg;
  C_WatchCB(rados_watchcb_t _wcb, void *_arg) : wcb(_wcb), arg(_arg) {}
  void notify(uint8_t opcode, uint64_t ver) {
    wcb(opcode, ver, arg);
  }
};

int rados_watch(rados_ioctx_t io, const char *o, uint64_t ver, uint64_t *handle,
                rados_watchcb_t watchcb, void *arg)
{
  uint64_t *cookie = handle;
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  C_WatchCB *wc = new C_WatchCB(watchcb, arg);
  return ctx->client->watch(*ctx, oid, ver, cookie, wc);
}

int rados_unwatch(rados_ioctx_t io, const char *o, uint64_t handle)
{
  uint64_t cookie = handle;
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->unwatch(*ctx, oid, cookie);
}

int rados_notify(rados_ioctx_t io, const char *o, uint64_t ver)
{
  IoCtxImpl *ctx = (IoCtxImpl *)io;
  object_t oid(o);
  return ctx->client->notify(*ctx, oid, ver);
}
