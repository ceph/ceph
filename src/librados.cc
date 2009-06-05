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

  int lookup_pool(const char *name) {
    return osdmap.lookup_pg_pool_name(name);
  }

  int write(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int read(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(int pool, const object_t& oid);

  int exec(int pool, const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  struct PGLSOp {
    int seed;
    __u64 cookie;

   PGLSOp() : seed(0), cookie(0) {}
  };

  int list(int pool, int max_entries, vector<object_t>& entries, RadosClient::PGLSOp& op);

  // --- aio ---
  struct AioCompletion {
    Mutex lock;
    Cond cond;
    int ref, rval;
    bool ack, safe;

    // for read
    bufferlist bl, *pbl;
    char *buf;
    unsigned maxlen;

    AioCompletion() : lock("RadosClient::AioCompletion"),
		      ref(1), rval(0), ack(false), safe(false), pbl(0), buf(0), maxlen(0) { }

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
      int n = --c->ref;
      c->lock.Unlock();
      if (!n)
	delete c;
    }
    C_aio_Safe(AioCompletion *_c) : c(_c) {
      c->get();
    }
  };

  int aio_read(int pool, object_t oid, off_t off, bufferlist *pbl, size_t len,
	       AioCompletion **pc);
  int aio_read(int pool, object_t oid, off_t off, char *buf, size_t len,
	       AioCompletion **pc);

  int aio_write(int pool, object_t oid, off_t off, const bufferlist& bl, size_t len,
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

int RadosClient::list(int pool, int max_entries, vector<object_t>& entries, RadosClient::PGLSOp& op)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::list");
  Cond cond;
  bool done;
  int r;
  object_t oid;

  memset(&oid, 0, sizeof(oid));

  ceph_object_layout layout;
retry:
  int pg_num = objecter->osdmap->get_pg_num(pool);

  for (;op.seed <pg_num; op.seed++) {
   int response_size;
   int req_size;

   do {
     int num = objecter->osdmap->get_pg_layout(pool, op.seed, layout);
     if (num != pg_num)  /* ahh.. race! */
        goto retry;

      lock.Lock();

      ObjectRead rd;
      bufferlist bl;
#define MAX_REQ_SIZE 1024
      req_size = min(MAX_REQ_SIZE, max_entries);
      rd.pg_ls(req_size, op.cookie);

      Context *onack = new C_SafeCond(&lock, &cond, &done, &r);
      objecter->read(oid, layout, rd, CEPH_NOSNAP, &bl, 0, onack);

      while (!done)
        cond.Wait(lock);

      lock.Unlock();

      bufferlist::iterator iter = bl.begin();
      PGLSResponse response;
      ::decode(response, iter);
      op.cookie = (__u64)response.handle;
      response_size = response.entries.size();
      if (response_size) {
	entries.swap(response.entries);
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

int RadosClient::write(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::write");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&lock, &cond, &done, &r);
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);

  dout(0) << "going to write" << dendl;

  lock.Lock();
  objecter->write(oid, layout,
		  off, len, snapc, bl, ut, 0,
		  onack, NULL);
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

  return len;
}

int RadosClient::aio_read(int pool, const object_t oid, off_t off, bufferlist *pbl, size_t len,
			  AioCompletion **pc)
{
  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);

  c->pbl = pbl;

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);
  objecter->read(oid, layout,
		 off, len, CEPH_NOSNAP, &c->bl, 0,
		  onack);

  *pc = c;
  return 0;
}
int RadosClient::aio_read(int pool, const object_t oid, off_t off, char *buf, size_t len,
			  AioCompletion **pc)
{
  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);

  c->buf = buf;
  c->maxlen = len;

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);
  objecter->read(oid, layout,
		 off, len, CEPH_NOSNAP, &c->bl, 0,
		  onack);

  *pc = c;
  return 0;
}

int RadosClient::aio_write(int pool, const object_t oid, off_t off, const bufferlist& bl, size_t len,
			   AioCompletion **pc)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  AioCompletion *c = new AioCompletion;
  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);
  objecter->write(oid, layout,
		  off, len, snapc, bl, ut, 0,
		  onack, onsafe);

  *pc = c;
  return 0;
}

int RadosClient::remove(int pool, const object_t& oid)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::remove");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&lock, &cond, &done, &r);
  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);

  dout(0) << "going to write" << dendl;

  lock.Lock();
  objecter->remove(oid, layout,
		  snapc, ut, 0,
		  onack, NULL);
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

  return r;
}

int RadosClient::exec(int pool, const object_t& oid, const char *cls, const char *method,
		      bufferlist& inbl, bufferlist& outbl)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::rdcall");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&lock, &cond, &done, &r);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);

  lock.Lock();

  ObjectRead rd;
  rd.rdcall(cls, method, inbl);
  objecter->read(oid, layout, rd, CEPH_NOSNAP, &outbl, 0, onack);

  while (!done)
    cond.Wait(lock);

  lock.Unlock();
  dout(0) << "after rdcall got " << outbl.length() << " bytes" << dendl;

  return r;
}

int RadosClient::read(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  SnapContext snapc;

  Mutex lock("RadosClient::read");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&lock, &cond, &done, &r);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, pool);

  dout(0) << "going to read" << dendl;

  lock.Lock();
  objecter->read(oid, layout,
	      off, len, CEPH_NOSNAP, &bl, 0,
              onack);
  while (!done)
    cond.Wait(lock);

  lock.Unlock();

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

bool Rados::initialize(int argc, const char *argv[])
{
 vector<const char*> args;

  if (argc && argv) {
    argv_to_vec(argc, argv, args);
    env_to_vec(args);
  }
  common_init(args, "librados", false);

  if (g_conf.clock_tare) g_clock.tare();

  client = new RadosClient();
  return client->init();
}

int Rados::list(rados_pool_t pool, int max, vector<object_t>& entries, Rados::ListCtx& ctx)
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

  return client->list(pool, max, entries, *op);
}

int Rados::write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  if (!client)
    return -EINVAL;

  return client->write(pool, oid, off, bl, len);
}

int Rados::remove(rados_pool_t pool, const object_t& oid)
{
  if (!client)
    return -EINVAL;

  return client->remove(pool, oid);
}

int Rados::read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len)
{
  if (!client)
    return -EINVAL;

  return client->read(pool, oid, off, bl, len);
}

int Rados::exec(rados_pool_t pool, const object_t& oid, const char *cls, const char *method,
		bufferlist& inbl, bufferlist& outbl)
{
  if (!client)
    return -EINVAL;

  return client->exec(pool, oid, cls, method, inbl, outbl);
}

int Rados::open_pool(const char *name, rados_pool_t *pool)
{
  int poolid = client->lookup_pool(name);
  if (poolid >= 0) {
    *pool = poolid;
    return 0;
  }
  return poolid;
}

int Rados::close_pool(rados_pool_t pool)
{
  return 0;
}

int Rados::aio_read(int pool, const object_t& oid, off_t off, bufferlist *pbl, size_t len,
		    Rados::AioCompletion **pc)
{
  RadosClient::AioCompletion *c;
  int r = client->aio_read(pool, oid, off, pbl, len, &c);
  if (r >= 0) {
    *pc = new AioCompletion((void *)c);
  }
  return r;
}

int Rados::aio_write(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len,
		     AioCompletion **pc)
{
  RadosClient::AioCompletion *c;
  int r = client->aio_write(pool, oid, off, bl, len, &c);
  if (r >= 0) {
    *pc = new AioCompletion((void *)c);
  }
  return r;
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
void Rados::AioCompletion::put()
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
    *pool = poolid;
    return 0;
  }
  return poolid;
}

extern "C" int rados_close_pool(rados_pool_t pool)
{
  return 0;
}

extern "C" int rados_write(rados_pool_t pool, const char *o, off_t off, const char *buf, size_t len)
{
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return radosp->write(pool, oid, off, bl, len);
}

extern "C" int rados_remove(rados_pool_t pool, const char *o)
{
  object_t oid(o);
  return radosp->remove(pool, oid);
}

extern "C" int rados_read(rados_pool_t pool, const char *o, off_t off, char *buf, size_t len)
{
  int ret;
  object_t oid(o);
  bufferlist bl;
  ret = radosp->read(pool, oid, off, bl, len);
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
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = radosp->exec(pool, oid, cls, method, inbl, outbl);
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

extern "C" int rados_list(rados_pool_t pool, int max, char *entries, rados_list_ctx_t *ctx)
{
  int ret;

  if (!*ctx) {
    *ctx = new RadosClient::PGLSOp;
    if (!*ctx)
      return -ENOMEM;
  }
  RadosClient::PGLSOp *op = (RadosClient::PGLSOp *)*ctx;

  vector<object_t> vec;
  ret = radosp->list(pool, max, vec, *op);
  if (!vec.size()) {
    delete op;
    *ctx = NULL;
  }

#warning fixme
  /*for (int i=0; i<vec.size(); i++) {
    entries[i] = vec[i];
  }
  */

  return ret;
}



// -------------------------
// aio

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
  object_t oid(o);
  return radosp->aio_read(pool, oid, off, buf, len, (RadosClient::AioCompletion**)completion);
}

extern "C" int rados_aio_write(rados_pool_t pool, const char *o,
			       off_t off, const char *buf, size_t len,
			       rados_completion_t *completion)
{
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return radosp->aio_write(pool, oid, off, bl, len, (RadosClient::AioCompletion**)completion);
}
