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
#include "msg/SimpleMessenger.h"

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
    osdmap_epoch(0),
    pool_cache_epoch(0),
    lock("librados::RadosClient::lock"),
    pool_cache_rwl("librados::RadosClient::pool_cache_rwl"),
    timer(cct, lock),
    refcnt(1),
    log_last_version(0), log_cb(NULL), log_cb_arg(NULL),
    finisher(cct),
    max_watch_cookie(0)
{
}

int64_t librados::RadosClient::lookup_pool(const char *name)
{
  pool_cache_rwl.get_read();
  if (pool_cache_epoch && pool_cache_epoch == osdmap_epoch) {
    map<string, int64_t>::iterator iter = pool_cache.find(name);
    if (iter != pool_cache.end()) {
      uint64_t val = iter->second;
      pool_cache_rwl.unlock();
      return val;
    }
  }

  pool_cache_rwl.unlock();

  lock.Lock();

  int r = wait_for_osdmap();
  if (r < 0) {
    lock.Unlock();
    return r;
  }
  int64_t ret = osdmap.lookup_pg_pool_name(name);
  pool_cache_rwl.get_write();
  lock.Unlock();
  if (ret < 0) {
    pool_cache_rwl.unlock();
    return -ENOENT;
  }

  if (pool_cache_epoch != osdmap_epoch) {
    pool_cache.clear();
    pool_cache_epoch = osdmap_epoch;
  }
  pool_cache[name] = ret;
  pool_cache_rwl.unlock();
  return ret;
}

bool librados::RadosClient::pool_requires_alignment(int64_t pool_id)
{
  Mutex::Locker l(lock);
  return osdmap.have_pg_pool(pool_id) &&
    osdmap.get_pg_pool(pool_id)->requires_aligned_append();
}

uint64_t librados::RadosClient::pool_required_alignment(int64_t pool_id)
{
  Mutex::Locker l(lock);
  return osdmap.have_pg_pool(pool_id) ?
    osdmap.get_pg_pool(pool_id)->required_alignment() : 0;
}

const char *librados::RadosClient::get_pool_name(int64_t pool_id)
{
  Mutex::Locker l(lock);
  return osdmap.get_pool_name(pool_id);
}

int librados::RadosClient::pool_get_auid(uint64_t pool_id, unsigned long long *auid)
{
  Mutex::Locker l(lock);
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  const pg_pool_t *pg = osdmap.get_pg_pool(pool_id);
  if (!pg)
    return -ENOENT;
  *auid = pg->auid;
  return 0;
}

int librados::RadosClient::pool_get_name(uint64_t pool_id, std::string *s)
{
  Mutex::Locker l(lock);
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
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
  objecter = new (std::nothrow) Objecter(cct, messenger, &monclient, &osdmap, lock, timer,
			  cct->_conf->rados_mon_op_timeout,
			  cct->_conf->rados_osd_op_timeout);
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

  finisher.start();

  state = CONNECTED;
  instance_id = monclient.get_global_id();

  lock.Unlock();

  ldout(cct, 1) << "init done" << dendl;
  err = 0;

 out:
  if (err) {
    state = DISCONNECTED;

    if (objecter) {
      delete objecter;
      objecter = NULL;
    }
    if (messenger) {
      delete messenger;
      messenger = NULL;
    }
  }

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
  if (objecter && state == CONNECTED) {
    need_objecter = true;
    objecter->shutdown_locked();
  }
  state = DISCONNECTED;
  instance_id = 0;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  monclient.shutdown();
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
    pool_cache_rwl.get_write();
    osdmap_epoch = osdmap.get_epoch();
    pool_cache_rwl.unlock();
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

  case MSG_COMMAND_REPLY:
    objecter->handle_command_reply(static_cast<MCommandReply*>(m));
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    watch_notify(static_cast<MWatchNotify *>(m));
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
  assert(lock.is_locked());

  if (state != CONNECTED) {
    return -ENOTCONN;
  }

  utime_t timeout;
  if (cct->_conf->rados_mon_op_timeout > 0)
    timeout.set_from_double(cct->_conf->rados_mon_op_timeout);

  if (osdmap.get_epoch() == 0) {
    ldout(cct, 10) << __func__ << " waiting" << dendl;
    utime_t start = ceph_clock_now(cct);

    while (osdmap.get_epoch() == 0) {
      cond.WaitInterval(cct, lock, timeout);

      utime_t elapsed = ceph_clock_now(cct) - start;
      if (!timeout.is_zero() && elapsed > timeout)
	break;
    }

    ldout(cct, 10) << __func__ << " done waiting" << dendl;

    if (osdmap.get_epoch() == 0) {
      lderr(cct) << "timed out waiting for first osdmap from monitors" << dendl;
      return -ETIMEDOUT;
    }
  }
  return 0;
}

int librados::RadosClient::wait_for_latest_osdmap()
{
  Mutex mylock("RadosClient::wait_for_latest_osdmap");
  Cond cond;
  bool done;

  lock.Lock();
  objecter->wait_for_latest_osdmap(new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return 0;
}

int librados::RadosClient::pool_list(std::list<std::string>& v)
{
  Mutex::Locker l(lock);
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
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
  int ret = 0;

  lock.Lock();
  objecter->get_pool_stats(pools, &result, new C_SafeCond(&mylock, &cond, &done,
							  &ret));
  lock.Unlock();

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
    delete onfinish;
  }
  return r;
}

int librados::RadosClient::pool_delete(const char *name)
{
  lock.Lock();
  int r = wait_for_osdmap();
  if (r < 0) {
    lock.Unlock();
    return r;
  }
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
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  int tmp_pool_id = osdmap.lookup_pg_pool_name(name);
  if (tmp_pool_id < 0)
    return -ENOENT;

  Context *onfinish = new C_PoolAsync_Safe(c);
  r = objecter->delete_pool(tmp_pool_id, onfinish);
  if (r < 0) {
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
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(m->cookie);
  if (iter != watchers.end()) {
    WatchContext *wc = iter->second;
    assert(wc);
    wc->get();
    finisher.queue(new C_WatchNotify(wc, &lock, m->opcode, m->ver, m->notify_id, m->bl));
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
  ldout(cct, 10) << __func__ << " version " << m->version << dendl;

  if (log_last_version < m->version) {
    log_last_version = m->version;

    if (log_cb) {
      for (std::deque<LogEntry>::iterator it = m->entries.begin(); it != m->entries.end(); ++it) {
	LogEntry e = *it;
	ostringstream ss;
	ss << e.stamp << " " << e.who.name << " " << e.type << " " << e.msg;
	string line = ss.str();
	string who = stringify(e.who);
	string level = stringify(e.type);
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
