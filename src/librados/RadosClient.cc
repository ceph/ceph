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
#include <sstream>
#include <pthread.h>
#include <errno.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "include/buffer.h"
#include "include/stringify.h"

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
  : Dispatcher(cct_->get()),
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

  return objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
			       name);
}

bool librados::RadosClient::pool_requires_alignment(int64_t pool_id)
{
  bool requires;
  int r = pool_requires_alignment2(pool_id, &requires);
  if (r < 0) {
    // Cast answer to false, this is a little bit problematic
    // since we really don't know the answer yet, say.
    return false;
  }

  return requires;
}

// a safer version of pool_requires_alignment
int librados::RadosClient::pool_requires_alignment2(int64_t pool_id,
						    bool *requires)
{
  if (!requires)
    return -EINVAL;

  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  return objecter->with_osdmap([requires, pool_id](const OSDMap& o) {
      if (!o.have_pg_pool(pool_id)) {
	return -ENOENT;
      }
      *requires = o.get_pg_pool(pool_id)->requires_aligned_append();
      return 0;
    });
}

uint64_t librados::RadosClient::pool_required_alignment(int64_t pool_id)
{
  uint64_t alignment;
  int r = pool_required_alignment2(pool_id, &alignment);
  if (r < 0) {
    return 0;
  }

  return alignment;
}

// a safer version of pool_required_alignment
int librados::RadosClient::pool_required_alignment2(int64_t pool_id,
						    uint64_t *alignment)
{
  if (!alignment)
    return -EINVAL;

  int r = wait_for_osdmap();
  if (r < 0) {
    return r;
  }

  return objecter->with_osdmap([alignment, pool_id](const OSDMap &o) {
      if (!o.have_pg_pool(pool_id)) {
	return -ENOENT;
      }
      *alignment = o.get_pg_pool(pool_id)->required_alignment();
      return 0;
    });
}

int librados::RadosClient::pool_get_auid(uint64_t pool_id,
					 unsigned long long *auid)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  objecter->with_osdmap([&](const OSDMap& o) {
      const pg_pool_t *pg = o.get_pg_pool(pool_id);
      if (!pg) {
	r = -ENOENT;
      } else {
	r = 0;
	*auid = pg->auid;
      }
    });
  return r;
}

int librados::RadosClient::pool_get_name(uint64_t pool_id, std::string *s)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  objecter->with_osdmap([&](const OSDMap& o) {
      if (!o.have_pg_pool(pool_id)) {
	r = -ENOENT;
      } else {
	r = 0;
	*s = o.get_pool_name(pool_id);
      }
    });
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
  messenger = Messenger::create_client_messenger(cct, "radosclient");
  if (!messenger)
    goto out;

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

  ldout(cct, 1) << "starting objecter" << dendl;

  objecter = new (std::nothrow) Objecter(cct, messenger, &monclient,
			  &finisher,
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

  bool need_objecter = false;
  if (objecter && objecter->initialized.read()) {
    need_objecter = true;
  }

  if (state == CONNECTED) {
    if (need_objecter) {
      // make sure watch callbacks are flushed
      watch_flush();
    }
    finisher.wait_for_empty();
    finisher.stop();
  }
  state = DISCONNECTED;
  instance_id = 0;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  if (need_objecter) {
    objecter->shutdown();
  }
  monclient.shutdown();
  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
  ldout(cct, 1) << "shutdown" << dendl;
}

int librados::RadosClient::watch_flush()
{
  ldout(cct, 10) << __func__ << " enter" << dendl;
  Mutex mylock("RadosClient::watch_flush::mylock");
  Cond cond;
  bool done;
  objecter->linger_callback_flush(new C_SafeCond(&mylock, &cond, &done));

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  ldout(cct, 10) << __func__ << " exit" << dendl;
  return 0;
}

struct C_aio_watch_flush_Complete : public Context {
  librados::RadosClient *client;
  librados::AioCompletionImpl *c;

  C_aio_watch_flush_Complete(librados::RadosClient *_client, librados::AioCompletionImpl *_c)
    : client(_client), c(_c) {
    c->get();
  }

  virtual void finish(int r) {
    c->lock.Lock();
    c->rval = r;
    c->ack = true;
    c->safe = true;
    c->cond.Signal();

    if (c->callback_complete) {
      client->finisher.queue(new librados::C_AioComplete(c));
    }
    if (c->callback_safe) {
      client->finisher.queue(new librados::C_AioSafe(c));
    }
    c->put_unlock();
  }
};

int librados::RadosClient::async_watch_flush(AioCompletionImpl *c)
{
  ldout(cct, 10) << __func__ << " enter" << dendl;
  Context *oncomplete = new C_aio_watch_flush_Complete(this, c);
  objecter->linger_callback_flush(oncomplete);
  ldout(cct, 10) << __func__ << " exit" << dendl;
  return 0;
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

  *io = new librados::IoCtxImpl(this, objecter, poolid, CEPH_NOSNAP);
  return 0;
}

int librados::RadosClient::create_ioctx(int64_t pool_id, IoCtxImpl **io)
{
  *io = new librados::IoCtxImpl(this, objecter, pool_id, CEPH_NOSNAP);
  return 0;
}

bool librados::RadosClient::ms_dispatch(Message *m)
{
  bool ret;

  Mutex::Locker l(lock);
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
  assert(lock.is_locked());
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_MAP:
    cond.Signal();
    m->put();
    break;

  case CEPH_MSG_MDS_MAP:
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

  if (state != CONNECTED) {
    return -ENOTCONN;
  }

  bool need_map = false;
  objecter->with_osdmap([&](const OSDMap& o) {
      if (o.get_epoch() == 0) {
	need_map = true;
      }
    });

  if (need_map) {
    Mutex::Locker l(lock);

    utime_t timeout;
    if (cct->_conf->rados_mon_op_timeout > 0)
      timeout.set_from_double(cct->_conf->rados_mon_op_timeout);

    if (objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch)) == 0) {
      ldout(cct, 10) << __func__ << " waiting" << dendl;
      utime_t start = ceph_clock_now(cct);
      while (objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch)) == 0) {
	cond.WaitInterval(cct, lock, timeout);
	utime_t elapsed = ceph_clock_now(cct) - start;
	if (!timeout.is_zero() && elapsed > timeout) {
	  lderr(cct) << "timed out waiting for first osdmap from monitors"
		     << dendl;
	  return -ETIMEDOUT;
	}
      }
      ldout(cct, 10) << __func__ << " done waiting" << dendl;
    }
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

int librados::RadosClient::pool_list(std::list<std::pair<int64_t, string> >& v)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;

  objecter->with_osdmap([&](const OSDMap& o) {
      for (auto p : o.get_pools())
	v.push_back(std::make_pair(p.first, o.get_pool_name(p.first)));
    });
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
				       int16_t crush_rule)
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
					     int16_t crush_rule)
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

  objecter->with_osdmap([&](const OSDMap& o) {
      const pg_pool_t* pool = o.get_pg_pool(pool_id);
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
    });
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

int librados::RadosClient::blacklist_add(const string& client_address,
					 uint32_t expire_seconds)
{
  entity_addr_t addr;
  if (!addr.parse(client_address.c_str(), 0)) {
    lderr(cct) << "unable to parse address " << client_address << dendl;
    return -EINVAL;
  }

  std::stringstream cmd;
  cmd << "{"
      << "\"prefix\": \"osd blacklist\", "
      << "\"blacklistop\": \"add\", "
      << "\"addr\": \"" << client_address << "\"";
  if (expire_seconds != 0) {
    cmd << ", \"expire\": " << expire_seconds << ".0";
  }
  cmd << "}";

  std::vector<std::string> cmds;
  cmds.push_back(cmd.str());
  bufferlist inbl;
  int r = mon_command(cmds, inbl, NULL, NULL);
  if (r < 0) {
    return r;
  }

  // ensure we have the latest osd map epoch before proceeding
  r = wait_for_latest_osdmap();
  return r;
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
  Mutex::Locker l(lock);

  if (state != CONNECTED) {
    return -ENOTCONN;
  }

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
  assert(lock.is_locked());
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
    }

    monclient.sub_got(log_watch, log_last_version);
  }

  m->put();
}
