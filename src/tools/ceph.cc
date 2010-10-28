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

#include "acconfig.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "mon/MonClient.h"
#include "mon/MonMap.h"
#include "msg/SimpleMessenger.h"
#include "tools/ceph.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/common_init.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern "C" {
#include <histedit.h>
}

enum CephToolMode {
  CEPH_TOOL_MODE_CLI_INPUT = 0,
  CEPH_TOOL_MODE_OBSERVER = 1,
  CEPH_TOOL_MODE_ONE_SHOT_OBSERVER = 2,
  CEPH_TOOL_MODE_GUI = 3
};

static enum CephToolMode ceph_tool_mode(CEPH_TOOL_MODE_CLI_INPUT);

struct ceph_tool_data g;

static Cond cmd_cond;
static SimpleMessenger *messenger = 0;
static SafeTimer timer(g.lock);
static Tokenizer *tok;

static const char *outfile = 0;



// sync command
vector<string> pending_cmd;
bufferlist pending_bl;
bool reply;
string reply_rs;
int reply_rc;
bufferlist reply_bl;
entity_inst_t reply_from;
Context *resend_event = 0;



// observe (push)
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "mds/MDSMap.h"
#include "include/LogEntry.h"
#include "include/ClassLibrary.h"

#include "mon/mon_types.h"

#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"


static set<int> registered, seen;

version_t map_ver[PAXOS_NUM];

static void handle_observe(MMonObserve *observe)
{
  dout(1) << observe->get_source() << " -> " << get_paxos_name(observe->machine_id)
	  << " registered" << dendl;
  g.lock.Lock();
  registered.insert(observe->machine_id);  
  g.lock.Unlock();
  observe->put();
}

static void handle_notify(MMonObserveNotify *notify)
{
  utime_t now = g_clock.now();

  dout(1) << notify->get_source() << " -> " << get_paxos_name(notify->machine_id)
	  << " v" << notify->ver
	  << (notify->is_latest ? " (latest)" : "")
	  << dendl;
  
  if (ceph_fsid_compare(&notify->fsid, &g.mc.monmap.fsid)) {
    dout(0) << notify->get_source_inst() << " notify fsid " << notify->fsid << " != " << g.mc.monmap.fsid << dendl;
    notify->put();
    return;
  }

  if (map_ver[notify->machine_id] >= notify->ver)
    return;

  switch (notify->machine_id) {
  case PAXOS_PGMAP:
    {
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	g.pgmap.decode(p);
      } else {
	PGMap::Incremental inc;
	inc.decode(p);
	g.pgmap.apply_incremental(inc);
      }
      *g.log << now << "    pg " << g.pgmap << std::endl;
      g.updates |= PG_MON_UPDATE;
      break;
    }

  case PAXOS_MDSMAP:
    g.mdsmap.decode(notify->bl);
    *g.log << now << "   mds " << g.mdsmap << std::endl;
    g.updates |= MDS_MON_UPDATE;
    break;

  case PAXOS_OSDMAP:
    {
      if (notify->is_latest) {
	g.osdmap.decode(notify->bl);
      } else {
	OSDMap::Incremental inc(notify->bl);
	g.osdmap.apply_incremental(inc);
      }
      *g.log << now << "   osd " << g.osdmap << std::endl;
    }
    g.updates |= OSD_MON_UPDATE;
    break;

  case PAXOS_LOG:
    {
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	LogSummary summary;
	::decode(summary, p);
	// show last log message
	if (!summary.tail.empty())
	  *g.log << now << "   log " << summary.tail.back() << std::endl;
      } else {
	LogEntry le;
	__u8 v;
	::decode(v, p);
	while (!p.end()) {
	  le.decode(p);
	  *g.log << now << "   log " << le << std::endl;
	}
      }
      break;
    }

  case PAXOS_CLASS:
    {
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	ClassLibrary list;
	::decode(list, p);
	// show the first class info
        map<string, ClassVersionMap>::iterator mapiter = list.library_map.begin();
	if (mapiter != list.library_map.end()) {
          ClassVersionMap& map = mapiter->second;
          tClassVersionMap::iterator iter = map.begin();

          if (iter != map.end())
	    *g.log << now << "   class " <<  iter->second << std::endl;
	}
      } else {
	__u8 v;
	::decode(v, p);
	while (!p.end()) {
	  ClassLibraryIncremental inc;
          ::decode(inc, p);
	  ClassInfo info;
	  inc.decode_info(info);
	  *g.log << now << "   class " << info << std::endl;
	}
      }
      break;
    }

  case PAXOS_AUTH:
    {
#if 0
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	KeyServerData data;
	::decode(data, p);
	*g.log << now << "   auth " << std::endl;
      } else {
	while (!p.end()) {
	  AuthMonitor::Incremental inc;
          inc.decode(p);
	  *g.log << now << "   auth " << inc.name.to_str() << std::endl;
	}
      }
#endif
      /* ignoring auth incremental.. don't want to decode it */
      break;
    }

  case PAXOS_MONMAP:
    {
      g.mc.monmap.decode(notify->bl);
      *g.log << now << "   mon " << g.mc.monmap << std::endl;
    }
    break;

  default:
    *g.log << now << "  ignoring unknown machine id " << notify->machine_id << std::endl;
  }

  map_ver[notify->machine_id] = notify->ver;

  // have we seen them all?
  seen.insert(notify->machine_id);
  switch (ceph_tool_mode) {
    case CEPH_TOOL_MODE_ONE_SHOT_OBSERVER:
      if (seen.size() == PAXOS_NUM) {
	messenger->shutdown();
      }
      break;
    case CEPH_TOOL_MODE_GUI:
      g.gui_cond.Signal();
      break;
    default:
      // do nothing
      break;
  }

  notify->put();
}

static void send_observe_requests();

class C_ObserverRefresh : public Context {
public:
  bool newmon;
  C_ObserverRefresh(bool n) : newmon(n) {}
  void finish(int r) {
    send_observe_requests();
  }
};

static void send_observe_requests()
{
  dout(1) << "send_observe_requests " << dendl;

  bool sent = false;
  for (int i=0; i<PAXOS_NUM; i++) {
    MMonObserve *m = new MMonObserve(g.mc.monmap.fsid, i, map_ver[i]);
    dout(1) << "mon" << " <- observe " << get_paxos_name(i) << dendl;
    g.mc.send_mon_message(m);
    sent = true;
  }

  registered.clear();
  float seconds = g_conf.paxos_observer_timeout/2;
  dout(1) << " refresh after " << seconds << " with same mon" << dendl;
  timer.add_event_after(seconds, new C_ObserverRefresh(false));
}

static void handle_ack(MMonCommandAck *ack)
{
  g.lock.Lock();
  reply = true;
  reply_from = ack->get_source_inst();
  reply_rs = ack->rs;
  reply_rc = ack->r;
  reply_bl = ack->get_data();
  cmd_cond.Signal();
  if (resend_event) {
    timer.cancel_event(resend_event);
    resend_event = 0;
  }
  g.lock.Unlock();
  ack->put();
}

static void send_command()
{
  version_t last_seen_version = 0;
  MMonCommand *m = new MMonCommand(g.mc.monmap.fsid, last_seen_version);
  m->cmd = pending_cmd;
  m->set_data(pending_bl);

  *g.log << g_clock.now() << " mon" << " <- " << pending_cmd << std::endl;
  g.mc.send_mon_message(m);
}

class Admin : public Dispatcher {
  bool ms_dispatch(Message *m) {
    switch (m->get_type()) {
    case MSG_MON_COMMAND_ACK:
      handle_ack((MMonCommandAck*)m);
      break;
    case MSG_MON_OBSERVE_NOTIFY:
      handle_notify((MMonObserveNotify *)m);
      break;
    case MSG_MON_OBSERVE:
      handle_observe((MMonObserve *)m);
      break;
    case CEPH_MSG_MON_MAP:
      m->put();
      break;
    default:
      return false;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      g.lock.Lock();
      if (ceph_tool_mode != CEPH_TOOL_MODE_CLI_INPUT) {
	send_observe_requests();
      }
      if (pending_cmd.size())
	send_command();
      g.lock.Unlock();
    }
  }
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

} dispatcher;

int do_command(vector<string>& cmd, bufferlist& bl, string& rs, bufferlist& rbl)
{
  Mutex::Locker l(g.lock);

  pending_cmd = cmd;
  pending_bl = bl;
  reply = false;
  
  send_command();

  while (!reply)
    cmd_cond.Wait(g.lock);

  rs = rs;
  rbl = reply_bl;
  *g.log << g_clock.now() << " "
       << reply_from.name << " -> '"
       << reply_rs << "' (" << reply_rc << ")"
       << std::endl;

  return reply_rc;
}

static void usage() 
{
  cerr << "usage: ceph [options] [commands]" << std::endl;
  cerr << "If no commands are specified, enter interactive mode.\n";
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  cerr << "Options:" << std::endl;
  cerr << "   -i infile\n";
  cerr << "   -o outfile\n";
  cerr << "        specify input or output file (for certain commands)\n";
  cerr << "   -s or --status\n";
  cerr << "        print current system status\n";
  cerr << "   -w or --watch\n";
  cerr << "        watch system status changes in real time (push)\n";
  cerr << "   -g or --gui\n";
  cerr << "        watch system status changes graphically\n";
  generic_client_usage();
}

static const char *cli_prompt(EditLine *e)
{
  return "ceph> ";
}

int do_cli()
{
  /* emacs style */
  EditLine *el = el_init("ceph", stdin, stdout, stderr);
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

  while (1) {
    int chars_read;
    const char *line = el_gets(el, &chars_read);

    //*g.log << "typed '" << line << "'" << std::endl;

    if (chars_read == 0) {
      *g.log << "quit" << std::endl;
      break;
    }

    history(myhistory, &ev, H_ENTER, line);

    if (run_command(line))
      break;
  }

  history_end(myhistory);
  el_end(el);

  return 0;
}

int run_command(const char *line)
{
  if (strcmp(line, "quit\n") == 0)
    return 1;

  int argc;
  const char **argv;
  tok_str(tok, line, &argc, &argv);
  tok_reset(tok);

  vector<string> cmd;
  const char *infile = 0;
  const char *outfile = 0;
  for (int i=0; i<argc; i++) {
    if (strcmp(argv[i], ">") == 0 && i < argc-1) {
      outfile = argv[++i];
      continue;
    }
    if (argv[i][0] == '>') {
      outfile = argv[i] + 1;
      while (*outfile == ' ') outfile++;
      continue;
    }
    if (strcmp(argv[i], "<") == 0 && i < argc-1) {
      infile = argv[++i];
      continue;
    }
    if (argv[i][0] == '<') {
      infile = argv[i] + 1;
      while (*infile == ' ') infile++;
      continue;
    }
    cmd.push_back(argv[i]);
  }
  if (cmd.empty())
    return 0;

  bufferlist in;
  if (cmd.size() == 1 && cmd[0] == "print") {
    *g.log << "----" << std::endl;
    write(1, in.c_str(), in.length());
    *g.log << "---- (" << in.length() << " bytes)" << std::endl;
    return 0;
  }

  //out << "cmd is " << cmd << std::endl;

  bufferlist out;
  if (infile) {
    if (out.read_file(infile) == 0) {
      *g.log << "read " << out.length() << " from " << infile << std::endl;
    } else {
      char buf[80];
      *g.log << "couldn't read from " << infile << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      return 0;
    }
  }

  in.clear();
  string rs;
  do_command(cmd, out, rs, in);

  if (in.length() == 0)
    return 0;

  if (outfile) {
    if (strcmp(outfile, "-") == 0) {
      *g.log << "----" << std::endl;
      write(1, in.c_str(), in.length());
      *g.log << "---- (" << in.length() << " bytes)" << std::endl;
    }
    else {
      in.write_file(outfile);
      *g.log << "wrote " << in.length() << " to "
	     << outfile << std::endl;
    }
  }
  else {
    *g.log << "got " << in.length() << " byte payload; 'print' "
      << "to dump to terminal, or add '>-' to command." << std::endl;
  }
  return 0;
}

int main(int argc, const char **argv)
{
  ostringstream gss;
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");
  
  common_set_defaults(false);
  common_init(args, "ceph", true);

  vec_to_argv(args, argc, argv);

  srand(getpid());

  // default to 'admin' user
  if (!g_conf.id || !g_conf.id[0])
    g_conf.id = strdup("admin");

  char *fname;
  bufferlist indata;
  vector<const char*> nargs;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("out_file", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&outfile, OPT_STR);
    } else if (CONF_ARG_EQ("in_file", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&fname, OPT_STR);
      int fd = ::open(fname, O_RDONLY);
      struct stat st;
      if (::fstat(fd, &st) == 0) {
	indata.push_back(buffer::create(st.st_size));
	indata.zero();
	::read(fd, indata.c_str(), st.st_size);
	::close(fd);
	cout << "read " << st.st_size << " bytes from " << args[i] << std::endl;
      }
    } else if (CONF_ARG_EQ("status", 's')) {
      ceph_tool_mode = CEPH_TOOL_MODE_ONE_SHOT_OBSERVER;
    } else if (CONF_ARG_EQ("watch", 'w')) {
      ceph_tool_mode = CEPH_TOOL_MODE_OBSERVER;
    } else if (CONF_ARG_EQ("help", 'h')) {
      usage();
    } else if (CONF_ARG_EQ("gui", 'g')) {
      ceph_tool_mode = CEPH_TOOL_MODE_GUI;
    } else if (args[i][0] == '-' && nargs.empty()) {
      cerr << "unrecognized option " << args[i] << std::endl;
      usage();
    } else
      nargs.push_back(args[i]);
  }

  // build command
  vector<string> vcmd;
  string cmd;
  for (unsigned i=0; i<nargs.size(); i++) {
    if (i) cmd += " ";
    cmd += nargs[i];
    vcmd.push_back(string(nargs[i]));
  }

  // get monmap
  if (g.mc.build_initial_monmap() < 0)
    return -1;
  
  // initialize tokenizer
  tok = tok_init(NULL);

  // start up network
  messenger = new SimpleMessenger();
  messenger->register_entity(entity_name_t::CLIENT());
  messenger->add_dispatcher_head(&dispatcher);

  messenger->start();

  g.mc.set_messenger(messenger);
  g.mc.init();

  if (g.mc.authenticate() < 0) {
    cerr << "unable to authenticate as " << *g_conf.entity_name << std::endl;
    return -1;
  }
  if (g.mc.get_monmap() < 0) {
    cerr << "unable to get monmap" << std::endl;
    return -1;
  }

  int ret = 0;

  switch (ceph_tool_mode)
  {
    case CEPH_TOOL_MODE_OBSERVER:
    case CEPH_TOOL_MODE_ONE_SHOT_OBSERVER:
      g.lock.Lock();
      send_observe_requests();
      g.lock.Unlock();
      break;

    case CEPH_TOOL_MODE_CLI_INPUT: {
      if (vcmd.empty()) {
	// interactive mode
	do_cli();
	messenger->shutdown();
	break;
      }
      string rs;
      bufferlist odata;
      ret = do_command(vcmd, indata, rs, odata);
      int len = odata.length();
      if (len) {
	if (outfile) {
	  if (strcmp(outfile, "-") == 0) {
	    ::write(1, odata.c_str(), len);
	  } else {
	    odata.write_file(outfile);
	  }
	  cout << g_clock.now() << " wrote " << len << " byte payload to " << outfile << std::endl;
	} else {
	  cout << g_clock.now() << " got " << len << " byte payload, discarding (specify -o <outfile)" << std::endl;
	}
      }
      messenger->shutdown();
      break;
    }

    case CEPH_TOOL_MODE_GUI: {
#ifdef HAVE_GTK2
      g.log = &gss;
      g.slog = &gss;

      // TODO: make sure that we capture the log this generates in the GUI
      g.lock.Lock();
      send_observe_requests();
      g.lock.Unlock();

      run_gui(argc, (char **)argv);
#else
      cerr << "I'm sorry. This tool was not compiled with support for  "
	   << "GTK2." << std::endl;
      ret = EXIT_FAILURE;
#endif
      messenger->shutdown();
      break;
    }

    default:
      assert(0);
      break;
  }

  // wait for messenger to finish
  messenger->wait();
  messenger->destroy();
  tok_end(tok);
  return ret;
}
