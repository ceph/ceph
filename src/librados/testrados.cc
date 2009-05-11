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

void usage()
{
  cerr << "usage: c3 -i name [flags] [--mds rank] [--shadow rank]\n";
  cerr << "  -m monitorip:port\n";
  cerr << "        connect to monitor at given address\n";
  cerr << "  --debug_mds n\n";
  cerr << "        debug MDS level (e.g. 10)\n";
  generic_server_usage();
}

class C3 : public Dispatcher
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
  C3() : messenger(NULL), mc(NULL), lock("c3") {}
  ~C3();
  bool init();

  int write(object_t& oid, const char *buf, off_t off, size_t len);
  int exec(object_t& oid, const char *code, off_t data_off, size_t data_len, char *buf, size_t out_len);
  int read(object_t& oid, char *buf, off_t off, size_t len);
};

bool C3::init()
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

  objecter = new Objecter(messenger, &monmap, &osdmap, lock);
  if (!objecter)
    return false;

  lock.Lock();
  mc->link_dispatcher(this);

  objecter->set_client_incarnation(0);
  objecter->init();

  objecter->set_client_incarnation(0);

  lock.Unlock();

  return true;
}

C3::~C3()
{
  if (mc)
    delete mc;
  if (messenger)
    messenger->shutdown();
}


bool C3::dispatch_impl(Message *m)
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


bool C3::_dispatch(Message *m)
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

int C3::write(object_t& oid, const char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist bl;
  utime_t ut = g_clock.now();

  Mutex lock("C3::write");
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

int C3::exec(object_t& oid, const char *code, off_t data_off, size_t data_len, char *buf, size_t out_len)
{
  SnapContext snapc;
  bufferlist bl, obl;
  utime_t ut = g_clock.now();

  Mutex lock("C3::exec");
  Cond exec_wait;
  bl.append(code, strlen(code) + 1);

  C_ExecCommit *oncommit = new C_ExecCommit(oid, data_off, &out_len, &exec_wait);

  ceph_object_layout layout = objecter->osdmap->make_object_layout(oid, 0);

  dout(0) << "going to exec" << dendl;

  lock.Lock();

  objecter->exec(oid, layout,
	      data_off, data_len, CEPH_NOSNAP, bl, 0,
	      &obl, out_len,
              oncommit);

  dout(0) << "after write call" << dendl;

  exec_wait.Wait(lock);

  lock.Unlock();

  if (out_len)
    memcpy(buf, obl.c_str(), out_len);

  return out_len;
}

int C3::read(object_t& oid, char *buf, off_t off, size_t len)
{
  SnapContext snapc;
  bufferlist *bl = new bufferlist;
  Mutex lock("C3::read");
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

static void __c3_init(int argc, const char *argv[])
{
  vector<const char*> args;

  if (argc && argv) {
    argv_to_vec(argc, argv, args);
    env_to_vec(args);
  }
  common_init(args, "ccc", true);

  if (g_conf.clock_tare) g_clock.tare();
}

static Mutex c3_init_mutex("c3_init");
static int c3_initialized = 0;

static C3 *c3p;

#include "librados.h"

extern "C" int c3_initialize(int argc, const char **argv) 
{
  int ret = 0;
  c3_init_mutex.Lock();

  if (!c3_initialized) {
    __c3_init(argc, argv);
    c3p = new C3();

    if (!c3p) {
      ret = ENOMEM;
      goto out;
    }
    c3p->init();
  }
  ++c3_initialized;

out:
  c3_init_mutex.Unlock();
  return ret;
}

extern "C" void c3_deinitialize()
{
  c3_init_mutex.Lock();
  --c3_initialized;

  if (!c3_initialized)
    delete c3p;

  c3p = NULL;

  c3_init_mutex.Unlock();
}

extern "C" int c3_write(object_t *oid, const char *buf, off_t off, size_t len)
{
    return c3p->write(*oid, buf, off, len);
}

extern "C" int c3_read(object_t *oid, char *buf, off_t off, size_t len)
{
    return c3p->read(*oid, buf, off, len);
}

extern "C" int c3_exec(object_t *oid, const char *code, off_t off, size_t len, char *buf, size_t out_len)
{
    return c3p->exec(*oid, code, off, len, buf, out_len);
}


int main(int argc, const char **argv) 
{
  if (c3_initialize(argc, argv)) {
    cerr << "error initializing" << std::endl;
    exit(1);
  }

  time_t tm;
  char buf[128], buf2[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));

  object_t oid(0x2010, 0);

  c3_write(&oid, buf, 0, strlen(buf) + 1);
  c3_exec(&oid, "code", 0, 128, buf, 128);
  cerr << "exec result=" << buf << std::endl;
  size_t size = c3_read(&oid, buf2, 0, 128);

  cerr << "read result=" << buf2 << "" << std::endl;
  cerr << "size=" << size << std::endl;


  c3_deinitialize();

  return 0;
}

