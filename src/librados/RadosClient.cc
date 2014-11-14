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
#include "include/stringify.h"

#include "messages/MWatchNotify.h"
#include "messages/MLog.h"
#include "msg/Messenger.h"

// needed for static_cast
#include "messages/PaxosServiceMessage.h"
#include "messages/MPoolOpReply.h"
#include "messages/MStatfsReply.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MCommandReply.h"

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
    instance_id(0),
    objecter(NULL),
    lock("librados::RadosClient::lock"),
    timer(cct, lock),
    refcnt(1),
    log_last_version(0), log_cb(NULL), log_cb_arg(NULL),
    finisher(cct)
{
}

int64_t librados::RadosClient::lookup_pool(const char *name)
{
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  const OSDMap *osdmap = objecter->get_osdmap_read();
  int64_t ret = osdmap->lookup_pg_pool_name(name);
  objecter->put_osdmap_read();
  return ret;
}

bool librados::RadosClient::pool_requires_alignment(int64_t pool_id)
{
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  const OSDMap *osdmap = objecter->get_osdmap_read();
  bool ret = osdmap->have_pg_pool(pool_id) &&
    osdmap->get_pg_pool(pool_id)->requires_aligned_append();
  objecter->put_osdmap_read();
  return ret;
}

uint64_t librados::RadosClient::pool_required_alignment(int64_t pool_id)
{
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  const OSDMap *osdmap = objecter->get_osdmap_read();
  uint64_t ret = osdmap->have_pg_pool(pool_id) ?
    osdmap->get_pg_pool(pool_id)->required_alignment() : 0;
  objecter->put_osdmap_read();
  return ret;
}

int librados::RadosClient::pool_get_auid(uint64_t pool_id, unsigned long long *auid)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  const OSDMap *osdmap = objecter->get_osdmap_read();
  const pg_pool_t *pg = osdmap->get_pg_pool(pool_id);
  if (!pg) {
    r = -ENOENT;
  } else {
    r = 0;
    *auid = pg->auid;
  }
  objecter->put_osdmap_read();
  return r;
}

int librados::RadosClient::pool_get_name(uint64_t pool_id, std::string *s)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  const OSDMap *osdmap = objecter->get_osdmap_read();
  if (!osdmap->have_pg_pool(pool_id)) {
    r = -ENOENT;
  } else {
    r = 0;
    *s = osdmap->get_pool_name(pool_id);
  }
  objecter->put_osdmap_read();
  return r;
}

int librados::RadosClient::get_fsid(std::string *s)
{
  if (!s)
    return -EINVAL;
  Mutex::Locker l(lock);
  ostringstream oss;
  oss << monclient.get_fsid();
  *s = oss.str();
  return 0;
}

int librados::RadosClient::ping_monitor(const string mon_id, string *result)
{
  int err = 0;
  /* If we haven't yet connected, we have no way of telling whether we
   * already built monc's initial monmap.  IF we are in CONNECTED state,
   * then it is safe to assume that we went through connect(), which does
   * build a monmap.
   */
  if (state != CONNECTED) {
    ldout(cct, 10) << __func__ << " build monmap" << dendl;
    err = monclient.build_initial_monmap();
  }
  if (err < 0) {
    return err;
  }

  err = monclient.ping_monitor(mon_id, result);
  return err;
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
  messenger = Messenger::create(cct, entity_name_t::CLIENT(-1),
				"radosclient", nonce);
  if (!messenger)
    goto out;

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

  ldout(cct, 1) << "starting objecter" << dendl;

  err = -ENOMEM;
  objecter = new Objecter(cct, messenger, &monclient,
			  cct->_conf->rados_mon_op_timeout,
			  cct->_conf->rados_osd_op_timeout);
  if (!objecter)
    goto out;
  objecter->set_balanced_budget();

  monclient.set_messenger(messenger);

  objecter->init();
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

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

  objecter->set_client_incarnation(0);
  objecter->start();
  lock.Lock();

  timer.init();

  monclient.renew_subs();

  finisher.start();

  state = CONNECTED;
  instance_id = monclient.get_global_id();

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
  bool need_objecter = false;
  if (objecter && objecter->initialized.read()) {
    need_objecter = true;
  }
  state = DISCONNECTED;
  instance_id = 0;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  if (need_objecter)
    objecter->shutdown();
  monclient.shutdown();
  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
  ldout(cct, 1) << "shutdown" << dendl;
}

uint64_t librados::RadosClient::get_instance_id()
{
  return instance_id;
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
  if (poolid < 0) {
    // Make sure we have the latest map
    int r = wait_for_latest_osdmap();
    if (r < 0)
      return r;

    poolid = lookup_pool(name);
    if (poolid < 0) {
      return (int)poolid;
    }
  }

  *io = new librados::IoCtxImpl(this, objecter, &lock, poolid, name,
				CEPH_NOSNAP);
  return 0;
}

bool librados::RadosClient::ms_dispatch(Message *m)
{
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
}

bool librados::RadosClient::ms_handle_reset(Connection *con)
{
  return false;
}

void librados::RadosClient::ms_handle_remote_reset(Connection *con)
{
}


bool librados::RadosClient::_dispatch(Message *m)
{
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_MAP:
    lock.Lock();
    cond.Signal();
    lock.Unlock();
    m->put();
    break;

  case CEPH_MSG_MDS_MAP:
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    handle_watch_notify(static_cast<MWatchNotify *>(m));
    break;

  case MSG_LOG:
    handle_log(static_cast<MLog *>(m));
    break;

  default:
    return false;
  }

  return true;
}


int librados::RadosClient::wait_for_osdmap()
{
  assert(!lock.is_locked_by_me());

  if (objecter == NULL) {
    return -ENOTCONN;
  }

  bool need_map = false;
  const OSDMap *osdmap = objecter->get_osdmap_read();
  if (osdmap->get_epoch() == 0) {
    need_map = true;
  }
  objecter->put_osdmap_read();

  if (need_map) {
    Mutex::Locker l(lock);

    utime_t timeout;
    if (cct->_conf->rados_mon_op_timeout > 0)
      timeout.set_from_double(cct->_conf->rados_mon_op_timeout);

    const OSDMap *osdmap = objecter->get_osdmap_read();
    if (osdmap->get_epoch() == 0) {
      ldout(cct, 10) << __func__ << " waiting" << dendl;
      utime_t start = ceph_clock_now(cct);
      while (osdmap->get_epoch() == 0) {
        objecter->put_osdmap_read();
        cond.WaitInterval(cct, lock, timeout);
        utime_t elapsed = ceph_clock_now(cct) - start;
        if (!timeout.is_zero() && elapsed > timeout) {
          lderr(cct) << "timed out waiting for first osdmap from monitors" << dendl;
          return -ETIMEDOUT;
        }
        osdmap = objecter->get_osdmap_read();
      }
      ldout(cct, 10) << __func__ << " done waiting" << dendl;
    }
    objecter->put_osdmap_read();
    return 0;
  } else {
    return 0;
  }
}


int librados::RadosClient::wait_for_latest_osdmap()
{
  Mutex mylock("RadosClient::wait_for_latest_osdmap");
  Cond cond;
  bool done;

  objecter->wait_for_latest_osdmap(new C_SafeCond(&mylock, &cond, &done));

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return 0;
}

int librados::RadosClient::pool_list(std::list<std::string>& v)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  const OSDMap *osdmap = objecter->get_osdmap_read();
  for (map<int64_t,pg_pool_t>::const_iterator p = osdmap->get_pools().begin();
       p != osdmap->get_pools().end();
       ++p)
    v.push_back(osdmap->get_pool_name(p->first));
  objecter->put_osdmap_read();
  return 0;
}

int librados::RadosClient::get_pool_stats(std::list<string>& pools,
					  map<string,::pool_stat_t>& result)
{
  Mutex mylock("RadosClient::get_pool_stats::mylock");
  Cond cond;
  bool done;
  int ret = 0;

  objecter->get_pool_stats(pools, &result, new C_SafeCond(&mylock, &cond, &done,
							  &ret));

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return ret;
}

int librados::RadosClient::get_fs_stats(ceph_statfs& stats)
{
  Mutex mylock ("RadosClient::get_fs_stats::mylock");
  Cond cond;
  bool done;
  int ret = 0;

  lock.Lock();
  objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done, &ret));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();

  return ret;
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
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  Mutex mylock ("RadosClient::pool_create::mylock");
  int reply;
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  reply = objecter->create_pool(name, onfinish, auid, crush_rule);

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
  int r = wait_for_osdmap();
  if (r < 0)
    return r;

  Context *onfinish = new C_PoolAsync_Safe(c);
  r = objecter->create_pool(name, onfinish, auid, crush_rule);
  if (r < 0) {
    delete onfinish;
  }
  return r;
}

int librados::RadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier)
{
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  const OSDMap *osdmap = objecter->get_osdmap_read();

  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  if (pool) {
    if (pool->tier_of < 0) {
      *base_tier = pool_id;
    } else {
      *base_tier = pool->tier_of;
    }
    r = 0;
  } else {
    r = -ENOENT;
  }

  objecter->put_osdmap_read();
  return r;
}

int librados::RadosClient::pool_delete(const char *name)
{
  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  Mutex mylock("RadosClient::pool_delete::mylock");
  Cond cond;
  bool done;
  int ret;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &ret);
  ret = objecter->delete_pool(name, onfinish);

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
  int r = wait_for_osdmap();
  if (r < 0)
    return r;

  Context *onfinish = new C_PoolAsync_Safe(c);
  r = objecter->delete_pool(name, onfinish);
  if (r < 0) {
    delete onfinish;
  }
  return r;
}

void librados::RadosClient::blacklist_self(bool set) {
  Mutex::Locker l(lock);
  objecter->blacklist_self(set);
}


// -----------
// watch/notify

void librados::RadosClient::register_watch_notify_callback(
  WatchNotifyInfo *wc,
  uint64_t cookie)
{
  assert(lock.is_locked_by_me());
  wc->cookie = cookie;
  ldout(cct,10) << __func__ << " cookie " << wc->cookie << dendl;
  watch_notify_info[wc->cookie] = wc;
}

int librados::RadosClient::unregister_watch_notify_callback(uint64_t cookie,
							    object_t *poid)
{
  ldout(cct,10) << __func__ << " cookie " << cookie << dendl;
  assert(lock.is_locked_by_me());
  map<uint64_t, WatchNotifyInfo *>::iterator iter = watch_notify_info.find(cookie);
  if (iter == watch_notify_info.end())
    return -EBADF;

  WatchNotifyInfo *ctx = iter->second;
  if (poid)
    *poid = ctx->oid;
  if (ctx->linger_op) {
    objecter->linger_cancel(ctx->linger_op);
    ctx->linger_op = NULL;
  }

  watch_notify_info.erase(iter);
  lock.Unlock();
  ldout(cct, 10) << __func__ << " dropping reference, waiting ctx="
		 << (void *)ctx << dendl;
  ctx->put_wait();
  ldout(cct, 10) << __func__ << " done ctx=" << (void *)ctx << dendl;
  lock.Lock();
  return 0;
}

int librados::RadosClient::watch_check(uint64_t cookie)
{
  ldout(cct,10) << __func__ << " cookie " << cookie << dendl;
  map<uint64_t, WatchNotifyInfo *>::iterator iter = watch_notify_info.find(cookie);
  if (iter == watch_notify_info.end())
    return -EBADF;
  WatchNotifyInfo *ctx = iter->second;
  if (ctx->err)
    return ctx->err;
  return objecter->linger_check(ctx->linger_op);
}

struct C_DoWatchNotify : public Context {
  librados::RadosClient *rados;
  MWatchNotify *m;
  C_DoWatchNotify(librados::RadosClient *r, MWatchNotify *m) : rados(r), m(m) {}
  void finish(int r) {
    rados->do_watch_notify(m);
  }
};

struct C_DoWatchError : public Context {
  librados::RadosClient *rados;
  uint64_t cookie;
  int err;
  C_DoWatchError(librados::RadosClient *r, uint64_t cookie, int err)
    : rados(r), cookie(cookie), err(err) {}
  void finish(int r) {
    rados->do_watch_error(cookie, err);
  }
};

void librados::WatchNotifyInfo::OnError::complete(int r)
{
  RadosClient *client = info->io_ctx_impl->client;
  client->finisher.queue(new C_DoWatchError(client, info->cookie, r));
}

void librados::RadosClient::do_watch_error(uint64_t cookie, int err)
{
  Mutex::Locker l(lock);
  map<uint64_t, WatchNotifyInfo *>::iterator iter =
    watch_notify_info.find(cookie);
  if (iter != watch_notify_info.end()) {
    WatchNotifyInfo *wc = iter->second;
    assert(wc);
    wc->err = err;
    if (wc->watch_ctx2) {
      wc->get();
      ldout(cct,10) << __func__ << " cookie " << cookie
		    << " handle_error " << err << dendl;
      lock.Unlock();
      wc->watch_ctx2->handle_error(cookie, err);
      lock.Lock();
      ldout(cct,10) << __func__ << " cookie " << cookie
		    << " handle_error " << err << " done" << dendl;
      wc->put();
    }
  } else {
    ldout(cct,10) << __func__ << " cookie " << cookie << " not found" << dendl;
  }
}

void librados::RadosClient::handle_watch_notify(MWatchNotify *m)
{
  Mutex::Locker l(lock);

  if (watch_notify_info.count(m->cookie)) {
    ldout(cct,10) << __func__ << " queueing async " << *m << dendl;
    // deliver this async via a finisher thread
    finisher.queue(new C_DoWatchNotify(this, m));
  } else {
    // drop it on the floor
    ldout(cct,10) << __func__ << " cookie " << m->cookie << " unknown" << dendl;
    m->put();
  }
}

void librados::RadosClient::do_watch_notify(MWatchNotify *m)
{
  Mutex::Locker l(lock);
  map<uint64_t, WatchNotifyInfo *>::iterator iter =
    watch_notify_info.find(m->cookie);
  if (iter != watch_notify_info.end()) {
    WatchNotifyInfo *wc = iter->second;
    assert(wc);
    if (wc->notify_lock) {
      // we sent a notify and it completed (or failed)
      // NOTE: opcode may be either NOTIFY (older OSDs) or NOTIFY_COMPLETE
      // (newer OSDs).  In practice it doesn't matter because completion is the
      // only kind of event we get on notify cookies.
      ldout(cct,10) << __func__ << " completed notify " << *m << dendl;
      wc->notify_lock->Lock();
      *wc->notify_done = true;
      *wc->notify_rval = m->return_code;
      if (wc->notify_reply_bl) {
	wc->notify_reply_bl->claim(m->get_data());
      }
      if (wc->notify_reply_buf) {
	*wc->notify_reply_buf = (char*)malloc(m->get_data().length());
	memcpy(*wc->notify_reply_buf, m->get_data().c_str(),
	       m->get_data().length());
      }
      if (wc->notify_reply_buf_len) {
	*wc->notify_reply_buf_len = m->get_data().length();
      }
      wc->notify_cond->Signal();
      wc->notify_lock->Unlock();
    } else if (m->opcode == CEPH_WATCH_EVENT_NOTIFY) {
      // we are watcher and got a notify
      ldout(cct,10) << __func__ << " got notify " << *m << dendl;
      wc->get();

      // trigger the callback
      assert(!!wc->watch_ctx ^ !!wc->watch_ctx2);  // only one is defined
      lock.Unlock();
      if (wc->watch_ctx) {
	wc->watch_ctx->notify(CEPH_WATCH_EVENT_NOTIFY, m->ver, m->bl);
	// send ACK back to the OSD
	bufferlist empty;
	wc->io_ctx_impl->notify_ack(wc->oid, m->notify_id, m->cookie, empty);
      } else if (wc->watch_ctx2) {
	wc->watch_ctx2->handle_notify(m->notify_id, m->cookie,
				      m->notifier_gid, m->bl);
	// user needs to explicitly ack (and may have already!)
      }
      lock.Lock();
      ldout(cct,10) << __func__ << " notify done" << dendl;
      wc->put();
    } else if (m->opcode == CEPH_WATCH_EVENT_FAILED_NOTIFY) {
      // we are watcher and failed to ack a notify in time, causing it to time
      // out.
      ldout(cct,10) << __func__ << " failed notify " << *m << dendl;
      wc->get();
      // trigger the callback
      assert(!!wc->watch_ctx ^ !!wc->watch_ctx2);  // only one is defined
      lock.Unlock();
      if (wc->watch_ctx2) {
	wc->watch_ctx2->handle_failed_notify(m->notify_id, m->cookie,
					     m->notifier_gid);
      }
      lock.Lock();
      ldout(cct,10) << __func__ << " failed notify done" << dendl;
      wc->put();
    } else if (m->opcode == CEPH_WATCH_EVENT_DISCONNECT) {
      // we failed to ping or reconnect and our watch was canceled.
      ldout(cct,10) << __func__ << " disconnect " << *m << dendl;
      wc->err = -ENOTCONN;
      if (wc->watch_ctx2) {
	wc->get();
	// trigger the callback
	lock.Unlock();
	wc->watch_ctx2->handle_error(m->cookie, -ENOTCONN);
	lock.Lock();
	ldout(cct,10) << __func__ << " disconnect done" << dendl;
	wc->put();
      } else {
	lderr(cct) << __func__ << " watch disconnect on "
		   << wc->oid << " on old API user, silently ignoring"
		   << dendl;
      }
    } else {
      lderr(cct) << __func__ << " got unknown event " << m->opcode
		 << " " << ceph_watch_event_name(m->opcode) << dendl;
    }
  } else {
    ldout(cct, 4) << __func__ << " unknown cookie " << m->cookie << dendl;
  }
  m->put();
}


int librados::RadosClient::mon_command(const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::mon_command(int rank, const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(rank, cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::mon_command(string name, const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(name, cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::osd_command(int osd, vector<string>& cmd,
				       const bufferlist& inbl,
				       bufferlist *poutbl, string *prs)
{
  Mutex mylock("RadosClient::osd_command::mylock");
  Cond cond;
  bool done;
  int ret;
  ceph_tid_t tid;

  if (osd < 0)
    return -EINVAL;

  lock.Lock();
  // XXX do anything with tid?
  int r = objecter->osd_command(osd, cmd, inbl, &tid, poutbl, prs,
			 new C_SafeCond(&mylock, &cond, &done, &ret));
  lock.Unlock();
  if (r != 0)
    return r;
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return ret;
}

int librados::RadosClient::pg_command(pg_t pgid, vector<string>& cmd,
				      const bufferlist& inbl,
				      bufferlist *poutbl, string *prs)
{
  Mutex mylock("RadosClient::pg_command::mylock");
  Cond cond;
  bool done;
  int ret;
  ceph_tid_t tid;
  lock.Lock();
  int r = objecter->pg_command(pgid, cmd, inbl, &tid, poutbl, prs,
		        new C_SafeCond(&mylock, &cond, &done, &ret));
  lock.Unlock();
  if (r != 0)
    return r;
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return ret;
}

int librados::RadosClient::monitor_log(const string& level, rados_log_callback_t cb, void *arg)
{
  if (cb == NULL) {
    // stop watch
    ldout(cct, 10) << __func__ << " removing cb " << (void*)log_cb << dendl;
    monclient.sub_unwant(log_watch);
    log_watch.clear();
    log_cb = NULL;
    log_cb_arg = NULL;
    return 0;
  }

  string watch_level;
  if (level == "debug") {
    watch_level = "log-debug";
  } else if (level == "info") {
    watch_level = "log-info";
  } else if (level == "warn" || level == "warning") {
    watch_level = "log-warn";
  } else if (level == "err" || level == "error") {
    watch_level = "log-error";
  } else if (level == "sec") {
    watch_level = "log-sec";
  } else {
    ldout(cct, 10) << __func__ << " invalid level " << level << dendl;
    return -EINVAL;
  }

  if (log_cb)
    monclient.sub_unwant(log_watch);

  // (re)start watch
  ldout(cct, 10) << __func__ << " add cb " << (void*)cb << " level " << level << dendl;
  monclient.sub_want(watch_level, 0, 0);
  monclient.renew_subs();
  log_cb = cb;
  log_cb_arg = arg;
  log_watch = watch_level;
  return 0;
}

void librados::RadosClient::handle_log(MLog *m)
{
  Mutex::Locker l(lock);
  ldout(cct, 10) << __func__ << " version " << m->version << dendl;

  if (log_last_version < m->version) {
    log_last_version = m->version;

    if (log_cb) {
      for (std::deque<LogEntry>::iterator it = m->entries.begin(); it != m->entries.end(); ++it) {
	LogEntry e = *it;
	ostringstream ss;
	ss << e.stamp << " " << e.who.name << " " << e.prio << " " << e.msg;
	string line = ss.str();
	string who = stringify(e.who);
	string level = stringify(e.prio);
	struct timespec stamp;
	e.stamp.to_timespec(&stamp);

	ldout(cct, 20) << __func__ << " delivering " << ss.str() << dendl;
	log_cb(log_cb_arg, line.c_str(), who.c_str(),
	       stamp.tv_sec, stamp.tv_nsec,
	       e.seq, level.c_str(), e.msg.c_str());
      }

      /*
	this was present in the old cephtool code, but does not appear to be necessary. :/

	version_t v = log_last_version + 1;
	ldout(cct, 10) << __func__ << " wanting " << log_watch << " ver " << v << dendl;
	monclient.sub_want(log_watch, v, 0);
      */
    }
  }

  m->put();
}
