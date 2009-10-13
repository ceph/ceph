// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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
using namespace std;

#include "config.h"

#include "mon/MonMap.h"
#include "mds/MDS.h"
#include "osd/OSDMap.h"
#include "osd/PGLS.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"

#include "mon/MonClient.h"

#include "osdc/Objecter.h"

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"

#include "include/librados.h"

#define RADOS_LIST_MAX_ENTRIES 1024
#define DOUT_SUBSYS rados
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "librados: "


class RadosClient : public Dispatcher
{
  OSDMap osdmap;
  Messenger *messenger;
  MonClient monclient;
  SimpleMessenger rank;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);

  Objecter *objecter;

  Mutex lock;
  Cond cond;

 
public:
  RadosClient() : messenger(NULL), lock("radosclient") {}
  ~RadosClient();
  bool init();
  void shutdown();

  struct PoolCtx {
    int poolid;
    snapid_t snap_seq;
    SnapContext snapc;

    PoolCtx(int pid, snapid_t s) : poolid(pid), snap_seq(CEPH_NOSNAP) {}

    void set_snap(snapid_t s) {
      if (!s)
	s = CEPH_NOSNAP;
      snap_seq = s;
    }
  };

  int lookup_pool(const char *name) {
    int ret = osdmap.lookup_pg_pool_name(name);
    if (ret < 0)
      return -ENOENT;
    return ret;
  }

  // snaps
  int snap_list(PoolCtx *pool, vector<rados_snap_t> *snaps);
  int snap_lookup(PoolCtx *pool, const char *name, rados_snap_t *snapid);
  int snap_get_name(PoolCtx *pool, rados_snap_t snapid, std::string *s);
  int snap_get_stamp(PoolCtx *pool, rados_snap_t snapid, time_t *t);
  int snap_create(const rados_pool_t pool, const char* snapname);
  int snap_remove(const rados_pool_t pool, const char* snapname);

  // io
  int create(PoolCtx& pool, const object_t& oid, bool exclusive);
  int write(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int write_full(PoolCtx& pool, const object_t& oid, bufferlist& bl);
  int read(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(PoolCtx& pool, const object_t& oid);
  int stat(PoolCtx& pool, const object_t& oid, __u64 *psize, time_t *pmtime);

  int exec(PoolCtx& pool, const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(PoolCtx& pool, const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(PoolCtx& pool, const object_t& oid, const char *name, bufferlist& bl);
  int getxattrs(PoolCtx& pool, const object_t& oid, map<nstring, bufferlist>& attrset);

  int list_pools(std::vector<string>& ls);
  int get_pool_stats(std::vector<string>& ls, map<string,rados_pool_stat_t>& result);
  int get_fs_stats( rados_statfs_t& result );

  int create_pool(string& name);

  int list(PoolCtx& pool, int max_entries, std::list<object_t>& entries,
			Objecter::ListContext *context);

  // --- aio ---
  struct AioCompletion {
    Mutex lock;
    Cond cond;
    int ref, rval;
    bool ack, safe;

    rados_callback_t callback;
    void *callback_arg;

    // for read
    bufferlist bl, *pbl;
    char *buf;
    unsigned maxlen;

    AioCompletion() : lock("RadosClient::AioCompletion"),
		      ref(1), rval(0), ack(false), safe(false), 
		      callback(0), callback_arg(0),
		      pbl(0), buf(0), maxlen(0) { }

    int set_callback(rados_callback_t cb, void *cba) {
      lock.Lock();
      callback = cb;
      callback_arg = cba;
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

    void get() {
      lock.Lock();
      ref++;
      lock.Unlock();
    }
    void put() {
      lock.Lock();
      int n = --ref;
      lock.Unlock();
      if (!n)
	delete this;
    }
  };

  struct C_aio_Ack : public Context {
    AioCompletion *c;
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

      if (c->callback) {
	rados_callback_t cb = c->callback;
	void *cba = c->callback_arg;
	c->lock.Unlock();
	cb(c, cba);
	c->lock.Lock();
      }

      int n = --c->ref;
      c->lock.Unlock();
      if (!n)
	delete c;
    }
    C_aio_Ack(AioCompletion *_c) : c(_c) {
      c->get();
    }
  };
  
  struct C_aio_Safe : public Context {
    AioCompletion *c;
    void finish(int r) {
      c->lock.Lock();
      if (!c->ack) {
	c->rval = r;
	c->ack = true;
      }
      c->safe = true;
      c->cond.Signal();

      if (c->callback) {
	rados_callback_t cb = c->callback;
	void *cba = c->callback_arg;
	c->lock.Unlock();
	cb(c, cba);
	c->lock.Lock();
      }

      int n = --c->ref;
      c->lock.Unlock();
      if (!n)
	delete c;
    }
    C_aio_Safe(AioCompletion *_c) : c(_c) {
      c->get();
    }
  };

  int aio_read(PoolCtx& pool, object_t oid, off_t off, bufferlist *pbl, size_t len,
	       AioCompletion **pc);
  int aio_read(PoolCtx& pool, object_t oid, off_t off, char *buf, size_t len,
	       AioCompletion **pc);

  int aio_write(PoolCtx& pool, object_t oid, off_t off, const bufferlist& bl, size_t len,
		AioCompletion **pc);

};

bool RadosClient::init()
{
  // get monmap
  if (monclient.build_initial_monmap() < 0)
    return false;

  dout(1) << "starting msgr at " << rank.get_rank_addr() << dendl;

  messenger = rank.register_entity(entity_name_t::CLIENT(-1));
  assert_warn(messenger);
  if (!messenger)
    return false;

  monclient.set_messenger(messenger);
  
  messenger->add_dispatcher_head(this);

  rank.start(1);

  objecter = new Objecter(messenger, &monclient, &osdmap, lock);
  if (!objecter)
    return false;

  monclient.init();
  monclient.mount(g_conf.client_mount_timeout);

  lock.Lock();

  objecter->set_client_incarnation(0);
  objecter->init();
  monclient.renew_subs();

  while (osdmap.get_epoch() == 0) {
    dout(1) << "waiting for osdmap" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();

  dout(1) << "init done" << dendl;

  return true;
}

void RadosClient::shutdown()
{
  lock.Lock();
  objecter->shutdown();
  lock.Unlock();
  messenger->shutdown();
  rank.wait();
  dout(1) << "shutdown" << dendl;
}

RadosClient::~RadosClient()
{
  if (messenger)
    messenger->shutdown();
}


bool RadosClient::ms_dispatch(Message *m)
{
  lock.Lock();
  bool ret = _dispatch(m);
  lock.Unlock();
  return ret;
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

  case MSG_POOLOPREPLY:
    objecter->handle_pool_op_reply((MPoolOpReply*)m);
    break;
  default:
    return false;
  }

  return true;
}

int RadosClient::list_pools(std::vector<string>& v)
{
  Mutex::Locker l(lock);
  for (map<int,pg_pool_t>::const_iterator p = osdmap.get_pools().begin(); 
       p != osdmap.get_pools().end();
       p++)
    v.push_back(osdmap.get_pool_name(p->first));
  return 0;
}

int RadosClient::get_pool_stats(std::vector<string>& pools, map<string,rados_pool_stat_t>& result)
{
  map<string,pool_stat_t> r;
  Mutex mylock("RadosClient::get_pool_stats::mylock");
  Cond cond;
  bool done;

  lock.Lock();
  objecter->get_pool_stats(pools, &r, new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();
  
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  for (map<string,pool_stat_t>::iterator p = r.begin();
       p != r.end();
       p++) {
    rados_pool_stat_t& v = result[p->first];
    v.num_kb = p->second.num_kb;
    v.num_bytes = p->second.num_bytes;
    v.num_objects = p->second.num_objects;
    v.num_object_clones = p->second.num_object_clones;
    v.num_object_copies = p->second.num_object_copies;
    v.num_objects_missing_on_primary = p->second.num_objects_missing_on_primary;
    v.num_objects_degraded = p->second.num_objects_degraded;
    v.num_rd = p->second.num_rd;
    v.num_rd_kb = p->second.num_rd_kb;
    v.num_wr = p->second.num_wr;
    v.num_wr_kb = p->second.num_wr_kb;
  }

  return 0;
}

int RadosClient::get_fs_stats( rados_statfs_t& result ) {
  ceph_statfs stats;

  Mutex mylock ("RadosClient::get_fs_stats::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();

  result.kb = stats.kb;
  result.kb_used = stats.kb_used;
  result.kb_avail = stats.kb_avail;
  result.num_objects = stats.num_objects;
  return 0;
}


// SNAPS

int RadosClient::snap_create( const rados_pool_t pool, const char *snapName)
{
  int reply;
  int poolID = ((PoolCtx *)pool)->poolid;
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

int RadosClient::snap_remove( const rados_pool_t pool, const char *snapName)
{
  int reply;
  int poolID = ((PoolCtx *)pool)->poolid;
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

int RadosClient::create_pool(string& name)
{
  int reply;

  Mutex mylock ("RadosClient::pool_create::mylock");
  Cond cond;
  bool done;
  lock.Lock();
  objecter->create_pool(name,
			new C_SafeCond(&mylock, &cond, &done, &reply));
  lock.Unlock();

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int RadosClient::snap_list(PoolCtx *pool, vector<rados_snap_t> *snaps)
{
  Mutex::Locker l(lock);
  const pg_pool_t& pi = objecter->osdmap->get_pg_pool(pool->poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi.snaps.begin();
       p != pi.snaps.end();
       p++)
    snaps->push_back(p->first);
  return 0;
}

int RadosClient::snap_lookup(PoolCtx *pool, const char *name, rados_snap_t *snapid)
{
  Mutex::Locker l(lock);
  const pg_pool_t& pi = objecter->osdmap->get_pg_pool(pool->poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi.snaps.begin();
       p != pi.snaps.end();
       p++) {
    if (p->second.name == name) {
      *snapid = p->first;
      return 0;
    }
  }
  return -ENOENT;
}

int RadosClient::snap_get_name(PoolCtx *pool, rados_snap_t snapid, std::string *s)
{
  Mutex::Locker l(lock);
  const pg_pool_t& pi = objecter->osdmap->get_pg_pool(pool->poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi.snaps.find(snapid);
  if (p == pi.snaps.end())
    return -ENOENT;
  *s = p->second.name.c_str();
  return 0;
}

int RadosClient::snap_get_stamp(PoolCtx *pool, rados_snap_t snapid, time_t *t)
{
  Mutex::Locker l(lock);
  const pg_pool_t& pi = objecter->osdmap->get_pg_pool(pool->poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi.snaps.find(snapid);
  if (p == pi.snaps.end())
    return -ENOENT;
  *t = p->second.stamp.sec();
  return 0;
}


// IO

int RadosClient::list(PoolCtx& pool, int max_entries, std::list<object_t>& entries, Objecter::ListContext *context) {
  Cond cond;
  bool done;
  int r = 0;
  object_t oid;
  Mutex mylock("RadosClient::list::mylock");

  if (context->at_end)
    return 0;

  context->pool_id = pool.poolid;
  context->pool_snap_seq = pool.snap_seq;
  context->entries = &entries;
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

int RadosClient::create(PoolCtx& pool, const object_t& oid, bool exclusive)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (pool.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::create::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->create(oid, layout,
		  pool.snapc, ut, 0, (exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0),
		  onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

int RadosClient::write(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
#if 0
  static SnapContext snapc;
  static int i;

  snapc.snaps.clear();

#define START_SNAP 1

  if (snapc.seq == 0)
    snapc.seq = START_SNAP - 1;

  ++snapc.seq;
  for (i=0; i<snapc.seq-START_SNAP + 1; i++) {
     snapc.snaps.push_back(snapc.seq - i);
  }
  i = 0;
  for (vector<snapid_t>::iterator iter = snapc.snaps.begin();
       iter != snapc.snaps.end(); ++iter, ++i) {
    dout(0) << "snapc[" << i << "] = " << *iter << dendl;
  }
  dout(0) << "seq=" << snapc.seq << dendl;
  dout(0) << "snapc=" << snapc << dendl;
#endif
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (pool.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::write::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  dout(0) << "going to write" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->write(oid, layout,
		  off, len, pool.snapc, bl, ut, 0,
		  onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r < 0)
    return r;

  dout(0) << "did write" << dendl;

  return len;
}

int RadosClient::write_full(PoolCtx& pool, const object_t& oid, bufferlist& bl)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (pool.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  dout(0) << "going to write_full" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->write_full(oid, layout,
		  pool.snapc, bl, ut, 0,
		  onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  dout(0) << "did write_full" << dendl;

  return r;
}

int RadosClient::aio_read(PoolCtx& pool, const object_t oid, off_t off, bufferlist *pbl, size_t len,
			  AioCompletion **pc)
{
  Mutex::Locker l(lock);
 
  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);

  c->pbl = pbl;

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->read(oid, layout,
		 off, len, pool.snap_seq, &c->bl, 0,
		  onack);

  *pc = c;
  return 0;
}
int RadosClient::aio_read(PoolCtx& pool, const object_t oid, off_t off, char *buf, size_t len,
			  AioCompletion **pc)
{
  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);

  c->buf = buf;
  c->maxlen = len;

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->read(oid, layout,
		 off, len, pool.snap_seq, &c->bl, 0,
		  onack);

  *pc = c;
  return 0;
}

int RadosClient::aio_write(PoolCtx& pool, const object_t oid, off_t off, const bufferlist& bl, size_t len,
			   AioCompletion **pc)
{
  Mutex::Locker l(lock);

  SnapContext snapc;
  utime_t ut = g_clock.now();

  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->write(oid, layout,
		  off, len, pool.snapc, bl, ut, 0,
		  onack, onsafe);

  *pc = c;
  return 0;
}

int RadosClient::remove(PoolCtx& pool, const object_t& oid)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::remove::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  dout(0) << "going to write" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->remove(oid, layout,
		  snapc, ut, 0,
		  onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

int RadosClient::exec(PoolCtx& pool, const object_t& oid, const char *cls, const char *method,
		      bufferlist& inbl, bufferlist& outbl)
{
  utime_t ut = g_clock.now();

  Mutex mylock("RadosClient::exec::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);


  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  ObjectOperation rd;
  rd.call(cls, method, inbl);
  objecter->read(oid, layout, rd, pool.snap_seq, &outbl, 0, onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  dout(0) << "after call got " << outbl.length() << " bytes" << dendl;

  return r;
}

int RadosClient::read(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  SnapContext snapc;

  Mutex mylock("RadosClient::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);


  dout(0) << "going to read" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->read(oid, layout,
	      off, len, pool.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  if (bl.length() < len) {
    dout(10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int RadosClient::stat(PoolCtx& pool, const object_t& oid, __u64 *psize, time_t *pmtime)
{
  SnapContext snapc;

  Mutex mylock("RadosClient::stat::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  __u64 size;
  utime_t mtime;

  if (!psize)
    psize = &size;

  dout(0) << "going to stat" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->stat(oid, layout,
	      pool.snap_seq, psize, &mtime, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from stat" << dendl;

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  return r;
}

int RadosClient::getxattr(PoolCtx& pool, const object_t& oid, const char *name, bufferlist& bl)
{
  SnapContext snapc;

  Mutex mylock("RadosClient::getxattr::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);


  dout(0) << "going to getxattr" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->getxattr(oid, layout,
	      name, pool.snap_seq, &bl, 0,
              onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  dout(10) << "Objecter returned from getxattr" << dendl;

  if (r < 0)
    return r;

  return bl.length();
}

int RadosClient::setxattr(PoolCtx& pool, const object_t& oid, const char *name, bufferlist& bl)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (pool.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::setxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  dout(0) << "going to setxattr" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->setxattr(oid, layout, name,
		  pool.snapc, bl, ut, 0,
		  onack, NULL);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r < 0)
    return r;

  dout(0) << "did setxattr" << dendl;

  return bl.length();
}

int RadosClient::getxattrs(PoolCtx& pool, const object_t& oid, map<nstring, bufferlist>& attrset)
{
  utime_t ut = g_clock.now();

  /* can't write to a snapshot */
  if (pool.snap_seq != CEPH_NOSNAP)
    return -EINVAL;

  Mutex mylock("RadosClient::getexattrs::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  dout(0) << "going to setxattr" << dendl;

  lock.Lock();
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool.poolid);
  objecter->getxattrs(oid, layout, pool.snap_seq,
                  attrset,
		  0, onack);
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  dout(0) << "did setxattr" << dendl;

  return r;
}

// ---------------------------------------------

Rados::Rados() : client(NULL)
{
}

Rados::~Rados()
{
  if (client)
    delete client;
  client = NULL;
}

int Rados::initialize(int argc, const char *argv[])
{
 vector<const char*> args;

  if (argc && argv) {
    argv_to_vec(argc, argv, args);
    env_to_vec(args);
  }
  common_init(args, "librados", false);

  if (g_conf.clock_tare) g_clock.tare();

  client = new RadosClient();
  return client->init() ? 0 : -1;
}

void Rados::shutdown()
{
  client->shutdown();
}

int Rados::list_pools(std::vector<string>& v)
{
  if (!client)
    return -EINVAL;
  return client->list_pools(v);
}

int Rados::get_pool_stats(std::vector<string>& v, std::map<string,rados_pool_stat_t>& result)
{
  if (!client)
    return -EINVAL;
  return client->get_pool_stats(v, result);
}

int Rados::create_pool(const char *name)
{
  string str(name);
  if (!client)
    return -EINVAL;
  return client->create_pool(str);
}

int Rados::get_fs_stats(rados_statfs_t& result) {
  if(!client) return -EINVAL;
  return client->get_fs_stats(result);
}

int Rados::list(rados_pool_t pool, int max, std::list<object_t>& entries, Rados::ListCtx& ctx)
{
  if (!client)
    return -EINVAL;

  Objecter::ListContext *op;
  if (!ctx.ctx) {
    ctx.ctx = new Objecter::ListContext();
    if (!ctx.ctx)
      return -ENOMEM;
  }

  op = (Objecter::ListContext *) ctx.ctx;
  return client->list(*(RadosClient::PoolCtx *)pool, max, entries, op);
}

int Rados::create(rados_pool_t pool, const object_t& oid, bool exclusive)
{
  if (!client)
    return -EINVAL;

  return client->create(*(RadosClient::PoolCtx *)pool, oid, exclusive);
}

int Rados::write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  if (!client)
    return -EINVAL;

  return client->write(*(RadosClient::PoolCtx *)pool, oid, off, bl, len);
}

int Rados::write_full(rados_pool_t pool, const object_t& oid, bufferlist& bl)
{
  if (!client)
    return -EINVAL;

  return client->write_full(*(RadosClient::PoolCtx *)pool, oid, bl);
}

int Rados::remove(rados_pool_t pool, const object_t& oid)
{
  if (!client)
    return -EINVAL;

  return client->remove(*(RadosClient::PoolCtx *)pool, oid);
}

int Rados::read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  if (!client)
    return -EINVAL;

  return client->read(*(RadosClient::PoolCtx *)pool, oid, off, bl, len);
}

int Rados::getxattr(rados_pool_t pool, const object_t& oid, const char *name, bufferlist& bl)
{
  if (!client)
    return -EINVAL;

  return client->getxattr(*(RadosClient::PoolCtx *)pool, oid, name, bl);
}

int Rados::getxattrs(rados_pool_t pool, const object_t& oid, map<nstring, bufferlist>& attrset)
{
  if (!client)
    return -EINVAL;

  return client->getxattrs(*(RadosClient::PoolCtx *)pool, oid, attrset);
}

int Rados::setxattr(rados_pool_t pool, const object_t& oid, const char *name, bufferlist& bl)
{
  if (!client)
    return -EINVAL;

  return client->setxattr(*(RadosClient::PoolCtx *)pool, oid, name, bl);
}

int Rados::stat(rados_pool_t pool, const object_t& oid, __u64 *psize, time_t *pmtime)
{
  if (!client)
    return -EINVAL;

  return client->stat(*(RadosClient::PoolCtx *)pool, oid, psize, pmtime);
}

int Rados::exec(rados_pool_t pool, const object_t& oid, const char *cls, const char *method,
		bufferlist& inbl, bufferlist& outbl)
{
  if (!client)
    return -EINVAL;

  return client->exec(*(RadosClient::PoolCtx *)pool, oid, cls, method, inbl, outbl);
}

int Rados::open_pool(const char *name, rados_pool_t *pool)
{
  int poolid = client->lookup_pool(name);
  if (poolid >= 0) {
    RadosClient::PoolCtx *ctx = new RadosClient::PoolCtx(poolid, CEPH_NOSNAP);
    if (!ctx)
      return -ENOMEM;

    *pool = (rados_pool_t)ctx;
    return 0;
  }
  return poolid;
}

int Rados::close_pool(rados_pool_t pool)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  delete ctx;
  return 0;
}

// SNAPS

int Rados::snap_create(const rados_pool_t pool, const char *snapname) {
  if (!client) return -EINVAL;
  return client->snap_create(pool, snapname);
}

int Rados::snap_remove(const rados_pool_t pool, const char *snapname) {
  if (!client) return -EINVAL;
  return client->snap_remove(pool, snapname);
}


void Rados::set_snap(rados_pool_t pool, snapid_t seq)
{
  if (!client)
    return;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  ctx->set_snap(seq);
}

int Rados::snap_list(rados_pool_t pool, vector<rados_snap_t> *snaps)
{
  if (!client)
    return -EINVAL;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  return client->snap_list(ctx, snaps);
}

int Rados::snap_lookup(rados_pool_t pool, const char *name, rados_snap_t *snapid)
{
  if (!client)
    return -EINVAL;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  return client->snap_lookup(ctx, name, snapid);
}

int Rados::snap_get_name(rados_pool_t pool, rados_snap_t snapid, std::string *s)
{
  if (!client)
    return -EINVAL;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  return client->snap_get_name(ctx, snapid, s);
}
int Rados::snap_get_stamp(rados_pool_t pool, rados_snap_t snapid, time_t *t)
{
  if (!client)
    return -EINVAL;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  return client->snap_get_stamp(ctx, snapid, t);
}

// AIO
int Rados::aio_read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist *pbl, size_t len,
		    Rados::AioCompletion **pc)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  RadosClient::AioCompletion *c;
  int r = client->aio_read(*ctx, oid, off, pbl, len, &c);
  if (r >= 0) {
    *pc = new AioCompletion((void *)c);
  }
  return r;
}

int Rados::aio_write(rados_pool_t pool, const object_t& oid, off_t off, const bufferlist& bl, size_t len,
		     AioCompletion **pc)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  RadosClient::AioCompletion *c;
  int r = client->aio_write(*ctx, oid, off, bl, len, &c);
  if (r >= 0) {
    *pc = new AioCompletion((void *)c);
  }
  return r;
}

int Rados::AioCompletion::set_callback(rados_callback_t cb, void *cba)
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->set_callback(cb, cba);
}
int Rados::AioCompletion::wait_for_complete()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->wait_for_complete();
}
int Rados::AioCompletion::wait_for_safe()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->wait_for_safe();
}
bool Rados::AioCompletion::is_complete()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->is_complete();
}
bool Rados::AioCompletion::is_safe()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->is_safe();
}
int Rados::AioCompletion::get_return_value()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  return c->get_return_value();
}
void Rados::AioCompletion::release()
{
  RadosClient::AioCompletion *c = (RadosClient::AioCompletion *)pc;
  c->put();
}

// ---------------------------------------------

static void __rados_init(int argc, const char *argv[])
{
  vector<const char*> args;

  if (argc && argv) {
    argv_to_vec(argc, argv, args);
    env_to_vec(args);
  }
  common_init(args, "librados", false);

  if (g_conf.clock_tare) g_clock.tare();
}

static Mutex rados_init_mutex("rados_init");
static int rados_initialized = 0;

static RadosClient *radosp;

#include "include/librados.h"

extern "C" int rados_initialize(int argc, const char **argv) 
{
  int ret = 0;
  rados_init_mutex.Lock();

  if (!rados_initialized) {
    __rados_init(argc, argv);
    radosp = new RadosClient;

    if (!radosp) {
      dout(0) <<  "radosp is NULL" << dendl;
      ret = -ENOMEM;
      goto out;
    }
    radosp->init();
  }
  ++rados_initialized;

out:
  rados_init_mutex.Unlock();
  return ret;
}

extern "C" void rados_deinitialize()
{
  rados_init_mutex.Lock();
  if (!rados_initialized) {
    dout(0) << "rados_deinitialize() called without rados_initialize()" << dendl;
    return;
  }
  --rados_initialized;
  if (!rados_initialized) {
    radosp->shutdown();
    delete radosp;
    radosp = NULL;
  }

  rados_init_mutex.Unlock();
}

extern "C" int rados_open_pool(const char *name, rados_pool_t *pool)
{
  int poolid = radosp->lookup_pool(name);
  if (poolid >= 0) {
    RadosClient::PoolCtx *ctx = new RadosClient::PoolCtx(poolid, CEPH_NOSNAP);
    if (!ctx)
      return -ENOMEM;
    *pool = ctx;
    return 0;
  }
  return poolid;
}

extern "C" int rados_close_pool(rados_pool_t pool)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  delete ctx;
  return 0;
}

extern "C" void rados_set_snap(rados_pool_t pool, rados_snap_t seq)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  ctx->set_snap((snapid_t)seq);
}

extern "C" int rados_write(rados_pool_t pool, const char *o, off_t off, const char *buf, size_t len)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return radosp->write(*ctx, oid, off, bl, len);
}

extern "C" int rados_remove(rados_pool_t pool, const char *o)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  return radosp->remove(*ctx, oid);
}

extern "C" int rados_read(rados_pool_t pool, const char *o, off_t off, char *buf, size_t len)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  int ret;
  object_t oid(o);
  bufferlist bl;
  ret = radosp->read(*ctx, oid, off, bl, len);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    bl.copy(0, bl.length(), buf);
    ret = bl.length();    // hrm :/
  }

  return ret;
}

extern "C" int rados_getxattr(rados_pool_t pool, const char *o, const char *name, char *buf, size_t len)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  int ret;
  object_t oid(o);
  bufferlist bl;
  ret = radosp->getxattr(*ctx, oid, name, bl);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    bl.copy(0, bl.length(), buf);
    ret = bl.length();
  }

  return ret;
}

extern "C" int rados_setxattr(rados_pool_t pool, const char *o, const char *name, const char *buf, size_t len)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return radosp->setxattr(*ctx, oid, name, bl);
}

extern "C" int rados_stat(rados_pool_t pool, const char *o, __u64 *psize, time_t *pmtime)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  return radosp->stat(*ctx, oid, psize, pmtime);
}

extern "C" int rados_exec(rados_pool_t pool, const char *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = radosp->exec(*ctx, oid, cls, method, inbl, outbl);
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
extern "C" void rados_pool_init_ctx(rados_list_ctx_t *ctx)
{
  *ctx = NULL;
}

extern "C" void rados_pool_close_ctx(rados_list_ctx_t *ctx)
{
  if (*ctx) {
    Objecter::ListContext *op = (Objecter::ListContext *)*ctx;
    delete op;
    *ctx = NULL;
  }
}

extern "C" int rados_pool_list_next(rados_pool_t pool, const char **entry, rados_list_ctx_t *listctx)
{
  int ret;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;

  if (!*listctx) {
    *listctx = new Objecter::ListContext;
    if (!*listctx)
      return -ENOMEM;
  }


  Objecter::ListContext *op = (Objecter::ListContext *)*listctx;

  //if the list is non-empty, this method has been called before
  if(!op->list.empty())
    //so let's kill the previously-returned object
    op->list.pop_front();

  if (op->list.empty()) {
    op->list.clear();
    ret = radosp->list(*ctx, RADOS_LIST_MAX_ENTRIES, op->list, op);
    if (!op->list.size()) {
      delete op;
      *listctx = NULL;
      return -ENOENT;
    }
  }

  *entry = op->list.front().name.c_str();

  return 0;
}



// -------------------------
// aio

extern "C" int rados_aio_set_callback(rados_completion_t c, rados_callback_t cb, void *cba)
{
  return ((RadosClient::AioCompletion *)c)->set_callback(cb, cba);
}

extern "C" int rados_aio_wait_for_complete(rados_completion_t c)
{
  return ((RadosClient::AioCompletion *)c)->wait_for_complete();
}

extern "C" int rados_aio_wait_for_safe(rados_completion_t c)
{
  return ((RadosClient::AioCompletion *)c)->wait_for_safe();
}

extern "C" int rados_aio_is_complete(rados_completion_t c)
{
  return ((RadosClient::AioCompletion *)c)->is_complete();
}

extern "C" int rados_aio_is_safe(rados_completion_t c)
{
  return ((RadosClient::AioCompletion *)c)->is_safe();
}

extern "C" int rados_aio_get_return_value(rados_completion_t c)
{
  return ((RadosClient::AioCompletion *)c)->get_return_value();
}

extern "C" void rados_aio_release(rados_completion_t c)
{
  ((RadosClient::AioCompletion *)c)->put();
}

extern "C" int rados_aio_read(rados_pool_t pool, const char *o,
			       off_t off, char *buf, size_t len,
			       rados_completion_t *completion)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  return radosp->aio_read(*ctx, oid, off, buf, len, (RadosClient::AioCompletion**)completion);
}

extern "C" int rados_aio_write(rados_pool_t pool, const char *o,
			       off_t off, const char *buf, size_t len,
			       rados_completion_t *completion)
{
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return radosp->aio_write(*ctx, oid, off, bl, len, (RadosClient::AioCompletion**)completion);
}
