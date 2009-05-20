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

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"

#include "mon/MonClient.h"

#include "osdc/Objecter.h"

#include "messages/MOSDGetMap.h"
#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"


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
  RadosClient() : messenger(NULL), mc(NULL), lock("c3") {}
  ~RadosClient();
  bool init();

  int lookup_pool(const char *name) {
    return osdmap.lookup_pg_pool_name(name);
  }

  int write(int pool, object_t& oid, const char *buf, off_t off, size_t len);
  int read(int pool, object_t& oid, char *buf, off_t off, size_t len);

  int exec(int pool, object_t& oid, const char *cls, const char *method, const char *inbuf, size_t in_len, char *buf, size_t out_len);
};

bool RadosClient::init()
{
  mc = new MonClient(&monmap, NULL);

  // get monmap
  if (!mc->get_monmap())
    return false;

  rank.bind();
  cout << "starting c3." << g_conf.id
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

int RadosClient::write(int pool, object_t& oid, const char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist bl;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::write");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&lock, &cond, &done, &r);

  bl.append(&buf[off], len);

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

int RadosClient::exec(int pool, object_t& oid, const char *cls, const char *method, const char *inbuf, size_t in_len, char *buf, size_t out_len)
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
  bufferlist inbl, outbl;

  inbl.append(inbuf, in_len);
  rd.rdcall(cls, method, inbl);
  objecter->read(oid, layout, rd, CEPH_NOSNAP, &outbl, 0, onack);

  dout(0) << "after rdcall got " << outbl.length() << " bytes" << dendl;

  while (!done)
    cond.Wait(lock);

  lock.Unlock();

  if (outbl.length() < out_len)
    out_len = outbl.length();

  if (out_len)
    memcpy(buf, outbl.c_str(), out_len);

  return out_len;
}

int RadosClient::read(int pool, object_t& oid, char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist bl;

  Mutex lock("RadosClient::rdcall");
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

  if (len)
    memcpy(buf, bl.c_str(), len);

  return len;
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
      ret = ENOMEM;
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

extern "C" int rados_write(rados_pool_t pool, ceph_object *o, const char *buf, off_t off, size_t len)
{
  object_t oid(*o);
  return radosp->write(pool, oid, buf, off, len);
}

extern "C" int rados_read(rados_pool_t pool, ceph_object *o, char *buf, off_t off, size_t len)
{
  object_t oid(*o);
  return radosp->read(pool, oid, buf, off, len);
}

extern "C" int rados_exec(rados_pool_t pool, ceph_object *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  object_t oid(*o);
  return radosp->exec(pool, oid, cls, method, inbuf, in_len, buf, out_len);
}

