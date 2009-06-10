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

#include "messages/MOSDGetMap.h"
#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"

#include "include/librados.h"


class RadosClient : public Dispatcher
{
  MonMap monmap;
  OSDMap osdmap;
  Messenger *messenger;
  MonClient *mc;
  SimpleMessenger rank;

  bool _dispatch(Message *m);
  bool dispatch_impl(Message *m);

  Objecter *objecter;

  Mutex lock;
  Cond cond;

 
public:
  RadosClient() : messenger(NULL), mc(NULL), lock("radosclient") {}
  ~RadosClient();
  bool init();

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
    return osdmap.lookup_pg_pool_name(name);
  }

  int write(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int read(PoolCtx& pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(PoolCtx& pool, const object_t& oid);

  int exec(PoolCtx& pool, const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  struct PGLSOp {
    int seed;
    __u64 cookie;
    std::list<object_t> list;
    std::list<object_t>::iterator iter;
    __u64 pos;
    __u64 total;

   PGLSOp() : seed(0), cookie(0), pos(0), total(0) {}
  };

  int list_pools(std::vector<string>& ls);
  int list(PoolCtx& pool, int max_entries, std::list<object_t>& entries, RadosClient::PGLSOp& op);

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
  mc = new MonClient(&monmap, NULL);

  // get monmap
  if (!mc->get_monmap())
    return false;

  rank.bind();
  cout << "starting radosclient." << g_conf.id
       << " at " << rank.get_rank_addr() 
       << " fsid " << monmap.get_fsid()
       << std::endl;

  messenger = rank.register_entity(entity_name_t::CLIENT(-1));
  assert_warn(messenger);
  if (!messenger)
    return false;

  mc->set_messenger(messenger);

  rank.set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::lossy_fail_after(1.0));
  rank.set_policy(entity_name_t::TYPE_MDS, SimpleMessenger::Policy::lossless());
  rank.set_policy(entity_name_t::TYPE_OSD, SimpleMessenger::Policy::lossless());
  rank.set_policy(entity_name_t::TYPE_CLIENT, SimpleMessenger::Policy::lossless());  // mds does its own timeout/markdown

  rank.start(1);

  mc->link_dispatcher(this);

  objecter = new Objecter(messenger, &monmap, &osdmap, lock);
  if (!objecter)
    return false;

  mc->mount(g_conf.client_mount_timeout);

  lock.Lock();

  objecter->signed_ticket = mc->get_signed_ticket();
  objecter->set_client_incarnation(0);
  objecter->init();

  while (osdmap.get_epoch() == 0) {
    dout(0) << "waiting for osdmap" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();

  dout(0) << "init done" << dendl;

  return true;
}

RadosClient::~RadosClient()
{
  if (mc)
    delete mc;
  if (messenger)
    messenger->shutdown();
}


bool RadosClient::dispatch_impl(Message *m)
{
  bool ret;

  if (m->get_orig_source().is_mon() &&
      m->get_header().monc_protocol != CEPH_MONC_PROTOCOL) {
    dout(0) << "monc protocol v " << (int)m->get_header().monc_protocol << " != my " << CEPH_MONC_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;
    delete m;
    return true;
  }
  if (m->get_orig_source().is_osd() &&
      m->get_header().osdc_protocol != CEPH_OSDC_PROTOCOL) {
    dout(0) << "osdc protocol v " << (int)m->get_header().osdc_protocol << " != my " << CEPH_OSDC_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;
    delete m;
    return true;
  }

  lock.Lock();
  ret = _dispatch(m);
  lock.Unlock();

  return ret;
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
  case CEPH_MSG_MDS_MAP:
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

int RadosClient::list(PoolCtx& pool, int max_entries, std::list<object_t>& entries, RadosClient::PGLSOp& op)
{
  utime_t ut = g_clock.now();

  Cond cond;
  bool done;
  int r = 0;
  object_t oid;

  memset(&oid, 0, sizeof(oid));
  entries.clear();

  ceph_object_layout layout;
retry:
  int pg_num = objecter->osdmap->get_pg_num(pool.poolid);
  
  for (;op.seed <pg_num; op.seed++) {
    int response_size;
    int req_size;
    
    do {
      lock.Lock();
      int num = objecter->osdmap->get_pg_layout(pool.poolid, op.seed, layout);
      lock.Unlock();
      if (num != pg_num)  /* ahh.. race! */
        goto retry;
      
      ObjectRead rd;
      bufferlist bl;
#define MAX_REQ_SIZE 1024
      req_size = min(MAX_REQ_SIZE, max_entries);
      rd.pg_ls(req_size, op.cookie);

      Mutex mylock("RadosClient::list::mylock");
      Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

      lock.Lock();
      objecter->read(oid, layout, rd, pool.snap_seq, &bl, 0, onack);
      lock.Unlock();

      mylock.Lock();
      while (!done)
        cond.Wait(mylock);
      mylock.Unlock();

      bufferlist::iterator iter = bl.begin();
      PGLSResponse response;
      ::decode(response, iter);
      op.cookie = (__u64)response.handle;
      response_size = response.entries.size();
      if (response_size) {
	entries.merge(response.entries);
        max_entries -= response_size;
        if (!max_entries)
          return r;
      } else {
        op.cookie = 0;
      }
    } while ((response_size == req_size) && op.cookie);
  }
  
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

  dout(0) << "did write" << dendl;

  return len;
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
  ObjectRead rd;
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

  if (bl.length() < len)
    len = bl.length();

  return len;
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

void Rados::set_snap(rados_pool_t pool, snapid_t seq)
{
  if (!client)
    return;

  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;
  ctx->set_snap(seq);
}

int Rados::list_pools(std::vector<string>& v)
{
  if (!client)
    return -EINVAL;
  return client->list_pools(v);
}

int Rados::list(rados_pool_t pool, int max, std::list<object_t>& entries, Rados::ListCtx& ctx)
{
  if (!client)
    return -EINVAL;

  RadosClient::PGLSOp *op;
  if (!ctx.ctx) {
    ctx.ctx = new RadosClient::PGLSOp;
    if (!ctx.ctx)
      return -ENOMEM;
  }

  op = (RadosClient::PGLSOp *)ctx.ctx;

  return client->list(*(RadosClient::PoolCtx *)pool, max, entries, *op);
}

int Rados::write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  if (!client)
    return -EINVAL;

  return client->write(*(RadosClient::PoolCtx *)pool, oid, off, bl, len);
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

int Rados::aio_write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len,
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
  --rados_initialized;

  if (!rados_initialized)
    delete radosp;

  radosp = NULL;

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
    RadosClient::PGLSOp *op = (RadosClient::PGLSOp *)*ctx;
    delete op;
    *ctx = NULL;
  }
}

extern "C" int rados_pool_list_next(rados_pool_t pool, const char **entry, rados_list_ctx_t *listctx)
{
  int ret;
  RadosClient::PoolCtx *ctx = (RadosClient::PoolCtx *)pool;

  if (!*listctx) {
    *listctx = new RadosClient::PGLSOp;
    if (!*listctx)
      return -ENOMEM;
  }
  RadosClient::PGLSOp *op = (RadosClient::PGLSOp *)*listctx;
  if (op->pos == op->total) {
    op->list.clear();
#define MAX_ENTRIES 1024
    ret = radosp->list(*ctx, MAX_ENTRIES, op->list, *op);
    if (!op->list.size()) {
      delete op;
      *listctx = NULL;
      return -ENOENT;
    }
    op->pos = 0;
    op->total = op->list.size();
    op->iter = op->list.begin();
  }

  *entry = op->iter->name.c_str();
  op->pos++;
  op->iter++;

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
