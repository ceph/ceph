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

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"
#include "msg/SimpleMessenger.h"
#include "osd/OSDMap.h"
#include "messages/MOSDGetMap.h"
#include "osdc/Objecter.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"

#include "common/Timer.h"
#include "common/common_init.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


OSDMap osdmap;
Mutex lock("dumpjournal.cc lock");
Cond cond;

Messenger *messenger = 0;
Objecter *objecter = 0;
Journaler *journaler = 0;

class Dumper : public Dispatcher {
  bool dispatch_impl(Message *m) {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OPREPLY:
      objecter->handle_osd_op_reply((MOSDOpReply *)m);
      break;
    case CEPH_MSG_OSD_MAP:
      objecter->handle_osd_map((MOSDMap*)m);
      break;
    default:
      return false;
    }
    return true;
  }
} dispatcher;


void usage() 
{
  exit(1);
}

int main(int argc, const char **argv, const char *envp[]) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args);

  vec_to_argv(args, argc, argv);

  int mds = 0;

  // get monmap
  MonMap monmap;
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return -1;
  
  // start up network
  rank.bind();
  g_conf.daemonize = false; // not us!
  rank.start();
  messenger = rank.register_entity(entity_name_t::ADMIN());
  messenger->set_dispatcher(&dispatcher);

  inode_t log_inode;
  memset(&log_inode, 0, sizeof(log_inode));
  log_inode.ino = MDS_INO_LOG_OFFSET + mds;
  log_inode.layout = g_default_mds_log_layout;

  objecter = new Objecter(messenger, &monmap, &osdmap, lock);
  journaler = new Journaler(log_inode.ino, &log_inode.layout, objecter, 0, 0,  &lock);

  objecter->set_client_incarnation(0);

  bool done;
  journaler->recover(new C_SafeCond(&lock, &cond, &done));
  lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
  
  __u64 start = journaler->get_read_pos();
  __u64 end = journaler->get_write_pos();
  __u64 len = end-start;
  cout << "journal is " << start << "~" << len << std::endl;

  Filer filer(objecter);
  bufferlist bl;
  filer.read(log_inode.ino, &log_inode.layout, 0,
	     start, len, &bl, 0, new C_SafeCond(&lock, &cond, &done));
    lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

  cout << "read " << bl.length() << " bytes" << std::endl;
  bl.write_file("mds.journal.dump");
  messenger->shutdown();

  // wait for messenger to finish
  rank.wait();
  
  return 0;
}

