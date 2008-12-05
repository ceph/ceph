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
Cond cond;

Messenger *messenger = 0;

const char *outfile = 0;
int watch = 0;

MonMap monmap;

// sync command
vector<string> pending_cmd;
bufferlist pending_bl;
bool reply;
string reply_rs;
int reply_rc;
bufferlist reply_bl;
entity_inst_t reply_from;
Context *resend_event = 0;


// watch
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
    lock.Lock();
    reply = true;
    reply_from = ack->get_source_inst();
    reply_rs = ack->rs;
    reply_rc = ack->r;
    reply_bl = ack->get_data();
    cond.Signal();
    if (resend_event) {
      timer.cancel_event(resend_event);
      resend_event = 0;
    }
    lock.Unlock();
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

void send_command();

struct C_Resend : public Context {
  void finish(int) {
    monmap.pick_mon(true);  // pick a new mon
    if (!reply)
      send_command();
  }
};
void send_command()
{
  MMonCommand *m = new MMonCommand(monmap.fsid);
  m->cmd = pending_cmd;
  m->get_data() = pending_bl;

  int mon = monmap.pick_mon();
  generic_dout(0) << "mon" << mon << " <- " << pending_cmd << dendl;
  messenger->send_message(m, monmap.get_inst(mon));

  resend_event = new C_Resend;
  timer.add_event_after(5.0, resend_event);
}

int do_command(vector<string>& cmd, bufferlist& bl, string& rs, bufferlist& rbl)
{
  Mutex::Locker l(lock);

  pending_cmd = cmd;
  pending_bl = bl;
  reply = false;
  
  send_command();

  while (!reply)
    cond.Wait(lock);

  rs = rs;
  rbl = reply_bl;
  generic_dout(0) << reply_from.name << " -> '"
		  << reply_rs << "' (" << reply_rc << ")"
		  << dendl;

  return reply_rc;
}



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


#include <histedit.h>


const char *cli_prompt(EditLine *e) {
  return "monctl> ";
}

int do_cli()
{
  /* emacs style */
  EditLine *el = el_init("cmonctl", stdin, stdout, stderr);
  el_set(el, EL_PROMPT, &cli_prompt);
  el_set(el, EL_EDITOR, "emacs");

  History *myhistory = history_init();
  if (myhistory == 0) {
    fprintf(stderr, "history could not be initialized\n");
    return 1;
  }

  HistEvent ev;

  /* Set the size of the history */
  history(myhistory, &ev, H_SETSIZE, 800);

  /* This sets up the call back functions for history functionality */
  el_set(el, EL_HIST, history, myhistory);

  Tokenizer *tok = tok_init(NULL);

  while (1) {
    int count;  // # chars read
    const char *line = el_gets(el, &count);

    if (!count) {
      cout << "quit" << std::endl;
      break;
    }

    //cout << "typed '" << line << "'" << std::endl;

    if (strcmp(line, "quit\n") == 0)
      break;

    history(myhistory, &ev, H_ENTER, line);

    int argc;
    const char **argv;
    tok_str(tok, line, &argc, &argv);
    tok_reset(tok);

    vector<string> cmd;
    for (int i=0; i<argc; i++)
      cmd.push_back(argv[i]);
    if (cmd.empty())
      continue;

    //cout << "cmd is " << cmd << std::endl;

    bufferlist out, in;
    string rs;
    do_command(cmd,out, rs, in);
  }

  history_end(myhistory);
  el_end(el);

  return 0;
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
    if (vcmd.size()) {
      
      string rs;
      bufferlist odata;
      do_command(vcmd, indata, rs, odata);
      
      int len = odata.length();
      if (len) {
	if (outfile) {
	  if (strcmp(outfile, "-") == 0) {
	    ::write(1, odata.c_str(), len);
	  } else {
	    odata.write_file(outfile);
	  }
	  generic_dout(0) << "wrote " << len << " byte payload to " << outfile << dendl;
	} else {
	  generic_dout(0) << "got " << len << " byte payload, discarding (specify -o <outfile)" << dendl;
	}
      }
    } else {
      // interactive mode
      do_cli();
    }
    
    messenger->shutdown();
  }


  // wait for messenger to finish
  rank.wait();
  messenger->destroy();
  return 0;
}

