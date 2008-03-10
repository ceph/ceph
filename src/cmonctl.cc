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


Messenger *messenger = 0;

const char *outfile = 0;

void handle_ack(MMonCommandAck *ack)
{
  generic_dout(0) << ack->get_source() << " -> '"
		  << ack->rs << "' (" << ack->r << ")"
		  << dendl;
  int len = ack->get_data().length();
  if (len) {
    if (outfile) {
      int fd = ::open(outfile, O_WRONLY|O_TRUNC|O_CREAT);
      ::write(fd, ack->get_data().c_str(), len);
      ::fchmod(fd, 0777);
      ::close(fd);
      generic_dout(0) << "wrote " << len << " byte payload to " << outfile << dendl;
    } else {
      generic_dout(0) << "got " << len << " byte payload, discarding (specify -o <outfile)" << dendl;
    }
  }
  messenger->shutdown();
}


class Admin : public Dispatcher {
  void dispatch(Message *m) {
    switch (m->get_type()) {
    case MSG_MON_COMMAND_ACK:
      handle_ack((MMonCommandAck*)m);
      break;      
    }
  }
} dispatcher;


int main(int argc, const char **argv, const char *envp[]) {

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  vec_to_argv(args, argc, argv);

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
    } else
      nargs.push_back(args[i]);
  }

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
  
  // build command
  MMonCommand *m = new MMonCommand(messenger->get_myinst());
  m->set_data(indata);
  string cmd;
  for (unsigned i=0; i<nargs.size(); i++) {
    if (i) cmd += " ";
    cmd += nargs[i];
    m->cmd.push_back(string(nargs[i]));
  }
  int mon = monmap.pick_mon();

  generic_dout(0) << "mon" << mon << " <- '" << cmd << "'" << dendl;

  // send it
  messenger->send_message(m, monmap.get_inst(mon));

  // wait for messenger to finish
  rank.wait();
  
  return 0;
}

