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

 class C_WriteAck : public Context {
    object_t oid;
    loff_t start;
    size_t *length;
    Cond *pcond;
  public:
    tid_t tid;
    C_WriteAck(object_t o, loff_t s, size_t *l, Cond *cond) : oid(o), start(s), length(l), pcond(cond) {}
    void finish(int r) {
      if (pcond) {
        *length = r;
        pcond->Signal();
      }
    }
  };
  class C_WriteCommit : public Context {
    object_t oid;
    loff_t start;
    size_t *length;
    Cond *pcond;
  public:
    tid_t tid;
    C_WriteCommit(object_t o, loff_t s, size_t *l, Cond *cond) : oid(o), start(s), length(l), pcond(cond) {}
    void finish(int r) {
      if (pcond) {
        *length = r;
        pcond->Signal();
      }
    }
  };
  class C_ExecCommit : public Context {
    object_t oid;
    loff_t start;
    size_t *length;
    Cond *pcond;
  public:
    tid_t tid;
    C_ExecCommit(object_t o, loff_t s, size_t *l, Cond *cond) : oid(o), start(s), length(l), pcond(cond) {}
    void finish(int r) {
      if (pcond) {
        *length = r;
        pcond->Signal();
      }
    }
  };
  class C_ReadCommit : public Context {
    object_t oid;
    loff_t start;
    size_t *length;
    bufferlist *bl;
    Cond *pcond;
  public:
    tid_t tid;
    C_ReadCommit(object_t o, loff_t s, size_t *l, bufferlist *b, Cond *cond) : oid(o), start(s), length(l), bl(b),
        pcond(cond) {}
    void finish(int r) {
      *length = r;
      if (pcond)
        pcond->Signal();
    }
  };

public:
  RadosClient() : messenger(NULL), mc(NULL), lock("c3") {}
  ~RadosClient();
  bool init();

  int write(object_t& oid, const char *buf, off_t off, size_t len);
  int exec(object_t& oid, const char *cls, const char *method, const char *inbuf, size_t in_len, char *buf, size_t out_len);
  int read(object_t& oid, char *buf, off_t off, size_t len);
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

  mc->mount(g_conf.client_mount_timeout);
  mc->link_dispatcher(this);

  objecter = new Objecter(messenger, &monmap, &osdmap, lock);
  if (!objecter)
    return false;

  lock.Lock();

  objecter->set_client_incarnation(0);
  objecter->init();

  objecter->set_client_incarnation(0);

  lock.Unlock();

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

  // verify protocol version
  if (m->get_orig_source().is_mds() &&
      m->get_header().mds_protocol != CEPH_MDS_PROTOCOL) {
    dout(0) << "mds protocol v " << (int)m->get_header().mds_protocol << " != my " << CEPH_MDS_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;
    delete m;
    return true;
  }

  if (m->get_header().mdsc_protocol != CEPH_MDSC_PROTOCOL) {
    dout(0) << "mdsc protocol v " << (int)m->get_header().mdsc_protocol << " != my " << CEPH_MDSC_PROTOCOL
	    << " from " << m->get_orig_source_inst() << " " << *m << dendl;
    delete m;
    return true;
  }
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
    break;
  case CEPH_MSG_MDS_MAP:
    break;

  default:
    return false;
  }

  return true;
}

int RadosClient::write(object_t& oid, const char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist bl;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::write");
  Cond write_wait;
  bl.append(&buf[off], len);

  C_WriteAck *onack = new C_WriteAck(oid, off, &len, &write_wait);
  C_WriteCommit *oncommit = new C_WriteCommit(oid, off, &len, NULL);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, 0);

  dout(0) << "going to write" << dendl;

  lock.Lock();

  objecter->write(oid, layout,
	      off, len, snapc, bl, ut, 0,
              onack, oncommit);

  dout(0) << "after write call" << dendl;

  write_wait.Wait(lock);

  lock.Unlock();

  return len;
}

int RadosClient::exec(object_t& oid, const char *cls, const char *method, const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  SnapContext snapc;
  utime_t ut = g_clock.now();

  Mutex lock("RadosClient::exec");
  Cond exec_wait;

  C_ExecCommit *oncommit = new C_ExecCommit(oid, 0, &out_len, &exec_wait);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, 0);

  dout(0) << "going to exec" << dendl;

  lock.Lock();

  ObjectRead rd;
  bufferlist inbl, outbl;

  inbl.append(inbuf, in_len);
  rd.rdcall(cls, method, inbl);
  objecter->read(oid, layout, rd, CEPH_NOSNAP, &outbl, 0, oncommit);

  dout(0) << "after rdcall got " << outbl.length() << " bytes" << dendl;

  exec_wait.Wait(lock);

  lock.Unlock();

  if (out_len)
    memcpy(buf, outbl.c_str(), out_len);

  return out_len;
}

int RadosClient::read(object_t& oid, char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist *bl = new bufferlist;
  Mutex lock("RadosClient::read");
  Cond read_wait;

  C_ReadCommit *oncommit = new C_ReadCommit(oid, off, &len, bl, &read_wait);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, 0);

  dout(0) << "going to read" << dendl;

  lock.Lock();

  objecter->read(oid, layout,
	      off, len, CEPH_NOSNAP, bl, 0,
              oncommit);

  dout(0) << "after read call" << dendl;
  read_wait.Wait(lock);

  lock.Unlock();

  if (len)
    memcpy(buf, bl->c_str(), len);

  delete bl;

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

extern "C" int rados_write(ceph_object *o, const char *buf, off_t off, size_t len)
{
  object_t oid(*o);
  return radosp->write(oid, buf, off, len);
}

extern "C" int rados_read(ceph_object *o, char *buf, off_t off, size_t len)
{
  object_t oid(*o);
  return radosp->read(oid, buf, off, len);
}

extern "C" int rados_exec(ceph_object *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  object_t oid(*o);
  return radosp->exec(oid, cls, method, inbuf, in_len, buf, out_len);
}

