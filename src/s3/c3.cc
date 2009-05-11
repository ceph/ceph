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

#include "libs3.h"

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
  MonClient mc;
  Messenger *messenger;

  bool _dispatch(Message *m);
  bool dispatch_impl(Message *m);

  Objecter *objecter;

  Mutex lock;


 class C_WriteAck : public Context {
    object_t oid;
    loff_t start;
    size_t length;
  public:
    tid_t tid;
    C_WriteAck(object_t o, loff_t s, size_t l) : oid(o), start(s), length(l) {}
    void finish(int r) {
      cerr << "WriteAck finish" << std::endl;
    }
  };
  class C_WriteCommit : public Context {
    object_t oid;
    loff_t start;
    size_t length;
  public:
    tid_t tid;
    C_WriteCommit(object_t o, loff_t s, size_t l) : oid(o), start(s), length(l) {}
    void finish(int r) {
      cerr << "WriteCommit finish" << std::endl;
    }
  };

public:
  C3() : messenger(NULL), lock("c3") {}
  bool init();


  void write();
};


bool C3::init()
{
  // get monmap
  if (mc.get_monmap(&monmap) < 0)
    return false;

  rank.bind();
  cout << "starting c3." << g_conf.id
       << " at " << rank.get_rank_addr() 
       << " fsid " << monmap.get_fsid()
       << std::endl;

  messenger = rank.register_entity(entity_name_t::MDS(-1));
  assert_warn(messenger);
  if (!messenger)
    return false;

  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::lossy_fail_after(1.0));
  rank.set_policy(entity_name_t::TYPE_MDS, Rank::Policy::lossless());
  rank.set_policy(entity_name_t::TYPE_OSD, Rank::Policy::lossless());
  rank.set_policy(entity_name_t::TYPE_CLIENT, Rank::Policy::lossless());  // mds does its own timeout/markdown

  rank.start(1);

  objecter = new Objecter(messenger, &monmap, &osdmap, lock);
  if (!objecter)
    return false;

  objecter->set_client_incarnation(0);

  lock.Lock();
  objecter->init();
  messenger->set_dispatcher(this);

  lock.Unlock();

  return true;
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

  default:
    return false;
  }

  return true;
}

void C3::write()
{
  SnapContext snapc;
  object_t oid(0x1010, 0);
  loff_t off = 0;
  size_t len = 1024;
  bufferlist bl;
  utime_t ut;
  char buf[len];

  bl.append(buf, len);

  C_WriteAck *onack = new C_WriteAck(oid, off, len);
  C_WriteCommit *oncommit = new C_WriteCommit(oid, off, len);

  ceph_object_layout layout = objecter->osdmap->file_to_object_layout(oid, g_default_mds_dir_layout);

  dout(0) << "going to write" << dendl;

  objecter->write(oid, layout,
	      off, len, snapc, bl, ut, 0,
              onack, oncommit);

  dout(0) << "after write call" << dendl;

}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "ccc", true);

  // mds specific args
  for (unsigned i=0; i<args.size(); i++) {
    cerr << "unrecognized arg " << args[i] << std::endl;
    usage();
  }
  if (!g_conf.id) {
    cerr << "must specify '-i name' with the cmds instance name" << std::endl;
    usage();
  }

  if (g_conf.clock_tare) g_clock.tare();

  C3 c3;

  c3.init();

  c3.write();

  rank.wait();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  generic_dout(0) << "stopped." << dendl;
  return 0;
}

