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
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"

#include "common/Timer.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

Mutex lock("cmonctl.cc lock");
Messenger *messenger = 0;

const char *outfile = 0;
int watch = 0;

MonMap monmap;

enum { OSD, MON, MDS, CLIENT, LAST };
int which = 0;
int same = 0;
const char *prefix[4] = { "mds", "osd", "pg", "client" };
map<string,string> status;

int lines = 0;

// refresh every second
void get_status(bool newmon=false);

struct C_Refresh : public Context {
  void finish(int r) {
    get_status(true);
  }
};

SafeTimer timer(lock);
Context *event = 0;

void get_status(bool newmon)
{
  int mon = monmap.pick_mon(newmon);

  vector<string> vcmd(2);
  vcmd[0] = prefix[which];
  vcmd[1] = "stat";
  
  MMonCommand *m = new MMonCommand(monmap.fsid);
  m->cmd.swap(vcmd);
  messenger->send_message(m, monmap.get_inst(mon));

  event = new C_Refresh;
  timer.add_event_after(.2, event);
}


void handle_ack(MMonCommandAck *ack)
{
  if (watch) {
    lock.Lock();

    which++;
    which = which % LAST;

    string w = ack->cmd[0];
    if (ack->rs != status[w]) {
      status[w] = ack->rs;
      generic_dout(0) << w << " " << status[w] << dendl;
      lines++;

      if (lines > 20) {
	generic_dout(0) << dendl;
	for (map<string,string>::iterator p = status.begin(); p != status.end(); p++)
	  generic_dout(0) << p->first << " " << p->second << dendl;
	generic_dout(0) << dendl;	
	lines = 0;
      }

      if (event)
	timer.cancel_event(event);
      get_status();
    }
    
    lock.Unlock();
  } else {
    generic_dout(0) << ack->get_source() << " -> '"
		    << ack->rs << "' (" << ack->r << ")"
		    << dendl;
    messenger->shutdown();
  }
  int len = ack->get_data().length();
  if (len) {
    if (outfile) {
      if (strcmp(outfile, "-") == 0) {
	::write(1, ack->get_data().c_str(), len);
      } else {
	int fd = ::open(outfile, O_WRONLY|O_TRUNC|O_CREAT);
	::write(fd, ack->get_data().c_str(), len);
	::fchmod(fd, 0777);
	::close(fd);
      }
      generic_dout(0) << "wrote " << len << " byte payload to " << outfile << dendl;
    } else {
      generic_dout(0) << "got " << len << " byte payload, discarding (specify -o <outfile)" << dendl;
    }
  }
}


class Admin : public Dispatcher {
  bool dispatch_impl(Message *m) {
    switch (m->get_type()) {
    case MSG_MON_COMMAND_ACK:
      handle_ack((MMonCommandAck*)m);
      break;
    default:
      return false;
    }
    return true;
  }
} dispatcher;


void usage() 
{
  cerr << "usage: cmonctl [options] monhost] command" << std::endl;
  cerr << "Options:" << std::endl;
  cerr << "   -m monhost        -- specify monitor hostname or ip" << std::endl;
  cerr << "   -i infile         -- specify input file" << std::endl;
  cerr << "   -o outfile        -- specify output file" << std::endl;
  cerr << "   -w or --watch     -- watch mds, osd, pg status" << std::endl;
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  exit(1);
}

int main(int argc, const char **argv, const char *envp[]) {

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  parse_config_options(args);

  vec_to_argv(args, argc, argv);

  srand(getpid());

  bufferlist indata;
  vector<const char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"-o") == 0) 
      outfile = args[++i];
    else if (strcmp(args[i], "-i") == 0) {
      int fd = ::open(args[++i], O_RDONLY);
      struct stat st;
      if (::fstat(fd, &st) == 0) {
	indata.push_back(buffer::create(st.st_size));
	indata.zero();
	::read(fd, indata.c_str(), st.st_size);
	::close(fd);
	cout << "read " << st.st_size << " bytes from " << args[i] << std::endl;
      }
    } else if (strcmp(args[i], "-w") == 0 ||
	       strcmp(args[i], "--watch") == 0) {
      watch = 1;
    } else
      nargs.push_back(args[i]);
  }

  // build command
  vector<string> vcmd;
  string cmd;
  if (!watch) {
    for (unsigned i=0; i<nargs.size(); i++) {
      if (i) cmd += " ";
      cmd += nargs[i];
      vcmd.push_back(string(nargs[i]));
    }
    if (vcmd.empty()) {
      cerr << "no mon command specified" << std::endl;
      usage();
    }
  }

  // get monmap
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return -1;
  
  // start up network
  rank.bind();
  g_conf.daemonize = false; // not us!
  messenger = rank.register_entity(entity_name_t::ADMIN());
  messenger->set_dispatcher(&dispatcher);

  rank.start();
  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::lossy_fail_after(1.0));

  if (watch) {
    lock.Lock();
    get_status();
    lock.Unlock();
  } else {
    // build command
    MMonCommand *m = new MMonCommand(monmap.fsid);
    m->set_data(indata);
    m->cmd.swap(vcmd);
    int mon = monmap.pick_mon();
    
    generic_dout(0) << "mon" << mon << " <- '" << cmd << "'" << dendl;
    
    // send it
    messenger->send_message(m, monmap.get_inst(mon));
  }

  // wait for messenger to finish
  rank.wait();
  messenger->destroy();
  return 0;
}

