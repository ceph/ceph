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

// needed for static_cast
#include "messages/PaxosServiceMessage.h"
#include "messages/MPoolOpReply.h"
#include "messages/MStatfsReply.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "AioCompletionImpl.h"
#include "IoCtxImpl.h"
#include "PoolAsyncCompletionImpl.h"
#include "RadosClient.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

static atomic_t rados_instance;

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

librados::RadosClient::RadosClient(CephContext *cct_)
  : Dispatcher(cct_),
    cct(cct_->get()),
    conf(cct_->_conf),
    state(DISCONNECTED),
    monclient(cct_),
    messenger(NULL),
    objecter(NULL),
    lock("librados::RadosClient::lock"),
    timer(cct, lock),
    refcnt(1),
    finisher(cct),
    max_watch_cookie(0)
{
}

int64_t librados::RadosClient::lookup_pool(const char *name)
{
  Mutex::Locker l(lock);
  int64_t ret = osdmap.lookup_pg_pool_name(name);
  if (ret < 0)
    return -ENOENT;
  return ret;
}

const char *librados::RadosClient::get_pool_name(int64_t pool_id)
{
  Mutex::Locker l(lock);
  return osdmap.get_pool_name(pool_id);
}

int librados::RadosClient::pool_get_auid(uint64_t pool_id, unsigned long long *auid)
{
  Mutex::Locker l(lock);
  const pg_pool_t *pg = osdmap.get_pg_pool(pool_id);
  if (!pg)
    return -ENOENT;
  *auid = pg->auid;
  return 0;
}

int librados::RadosClient::pool_get_name(uint64_t pool_id, std::string *s)
{
  Mutex::Locker l(lock);
  const char *str = osdmap.get_pool_name(pool_id);
  if (!str)
    return -ENOENT;
  *s = str;
  return 0;
}

int librados::RadosClient::get_fsid(std::string *s)
{
  if (!s)
    return -EINVAL;
  Mutex::Locker l(lock);
  ostringstream oss;
  oss << osdmap.get_fsid();
  *s = oss.str();
  return 0;
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
  messenger = new SimpleMessenger(cct, entity_name_t::CLIENT(-1), "radosclient", nonce);
  if (!messenger)
    goto out;

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

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

  objecter->init_unlocked();
  lock.Lock();

  timer.init();

  objecter->set_client_incarnation(0);
  objecter->init_locked();
  monclient.renew_subs();

  while (osdmap.get_epoch() == 0) {
    ldout(cct, 1) << "waiting for osdmap" << dendl;
    cond.Wait(lock);
  }

  finisher.start();

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
  if (state == CONNECTED) {
    finisher.stop();
  }
  monclient.shutdown();
  bool need_objecter = false;
  if (objecter && state == CONNECTED) {
    need_objecter = true;
    objecter->shutdown_locked();
  }
  state = DISCONNECTED;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  if (need_objecter)
    objecter->shutdown_unlocked();
  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
  ldout(cct, 1) << "shutdown" << dendl;
}

uint64_t librados::RadosClient::get_instance_id()
{
  Mutex::Locker l(lock);
  if (state == DISCONNECTED)
    return 0;
  uint64_t id = monclient.get_global_id();
  return id;
}

librados::RadosClient::~RadosClient()
{
  if (messenger)
    delete messenger;
  if (objecter)
    delete objecter;
  cct->put();
  cct = NULL;
}

int librados::RadosClient::create_ioctx(const char *name, IoCtxImpl **io)
{
  int64_t poolid = lookup_pool(name);
  if (poolid < 0)
    return (int)poolid;

  *io = new librados::IoCtxImpl(this, objecter, &lock, poolid, name,
				CEPH_NOSNAP);
  return 0;
}

bool librados::RadosClient::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  bool ret;

  if (state == DISCONNECTED) {
    ldout(cct, 10) << "disconnected, discarding " << *m << dendl;
    m->put();
    ret = true;
  } else {
    ret = _dispatch(m);
  }
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
    objecter->handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
    break;
  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map(static_cast<MOSDMap*>(m));
    cond.Signal();
    break;
  case MSG_GETPOOLSTATSREPLY:
    objecter->handle_get_pool_stats_reply(static_cast<MGetPoolStatsReply*>(m));
    break;

  case CEPH_MSG_MDS_MAP:
    break;

  case CEPH_MSG_STATFS_REPLY:
    objecter->handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
    break;

  case CEPH_MSG_POOLOP_REPLY:
    objecter->handle_pool_op_reply(static_cast<MPoolOpReply*>(m));
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    watch_notify(static_cast<MWatchNotify *>(m));
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
       ++p)
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

void librados::RadosClient::get() {
  Mutex::Locker l(lock);
  assert(refcnt > 0);
  refcnt++;
}

bool librados::RadosClient::put() {
  Mutex::Locker l(lock);
  assert(refcnt > 0);
  refcnt--;
  return (refcnt == 0);
}
 
int librados::RadosClient::pool_create(string& name, unsigned long long auid,
				       __u8 crush_rule)
{
  int reply;

  Mutex mylock ("RadosClient::pool_create::mylock");
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  lock.Lock();
  reply = objecter->create_pool(name, onfinish, auid, crush_rule);
  lock.Unlock();

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

int librados::RadosClient::pool_create_async(string& name, PoolAsyncCompletionImpl *c,
					     unsigned long long auid,
					     __u8 crush_rule)
{
  Mutex::Locker l(lock);
  Context *onfinish = new C_PoolAsync_Safe(c);
  int r = objecter->create_pool(name, onfinish, auid, crush_rule);
  if (r < 0) {
    delete c;
    delete onfinish;
  }
  return r;
}

int librados::RadosClient::pool_delete(const char *name)
{
  lock.Lock();
  int tmp_pool_id = osdmap.lookup_pg_pool_name(name);
  if (tmp_pool_id < 0) {
    lock.Unlock();
    return -ENOENT;
  }

  Mutex mylock("RadosClient::pool_delete::mylock");
  Cond cond;
  bool done;
  int ret;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &ret);
  ret = objecter->delete_pool(tmp_pool_id, onfinish);
  lock.Unlock();

  if (ret < 0) {
    delete onfinish;
  } else {
    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();
  }
  return ret;
}

int librados::RadosClient::pool_delete_async(const char *name, PoolAsyncCompletionImpl *c)
{
  Mutex::Locker l(lock);
  int tmp_pool_id = osdmap.lookup_pg_pool_name(name);
  if (tmp_pool_id < 0)
    return -ENOENT;

  Context *onfinish = new C_PoolAsync_Safe(c);
  int r = objecter->delete_pool(tmp_pool_id, onfinish);
  if (r < 0) {
    delete c;
    delete onfinish;
  }
  return r;
}

void librados::RadosClient::register_watcher(WatchContext *wc, uint64_t *cookie)
{
  assert(lock.is_locked());
  wc->cookie = *cookie = ++max_watch_cookie;
  watchers[wc->cookie] = wc;
}

void librados::RadosClient::unregister_watcher(uint64_t cookie)
{
  assert(lock.is_locked());
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(cookie);
  if (iter != watchers.end()) {
    WatchContext *ctx = iter->second;
    if (ctx->linger_id)
      objecter->unregister_linger(ctx->linger_id);

    watchers.erase(iter);
    lock.Unlock();
    ldout(cct, 10) << "unregister_watcher, dropping reference, waiting ctx=" << (void *)ctx << dendl;
    ctx->put_wait();
    ldout(cct, 10) << "unregister_watcher, done ctx=" << (void *)ctx << dendl;
    lock.Lock();
  }
}

void librados::RadosClient::blacklist_self(bool set) {
  Mutex::Locker l(lock);
  objecter->blacklist_self(set);
}

class C_WatchNotify : public Context {
  librados::WatchContext *ctx;
  Mutex *client_lock;
  uint8_t opcode;
  uint64_t ver;
  uint64_t notify_id;
  bufferlist bl;

public:
  C_WatchNotify(librados::WatchContext *_ctx, Mutex *_client_lock,
                uint8_t _o, uint64_t _v, uint64_t _n, bufferlist& _bl) : 
                ctx(_ctx), client_lock(_client_lock), opcode(_o), ver(_v), notify_id(_n), bl(_bl) {}

  void finish(int r) {
    ctx->notify(client_lock, opcode, ver, notify_id, bl);
    ctx->put();
  }
};

void librados::RadosClient::watch_notify(MWatchNotify *m)
{
  assert(lock.is_locked());
  WatchContext *wc = NULL;
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(m->cookie);
  if (iter != watchers.end())
    wc = iter->second;

  if (!wc)
    return;

  wc->get();
  finisher.queue(new C_WatchNotify(wc, &lock, m->opcode, m->ver, m->notify_id, m->bl));
  m->put();
}
