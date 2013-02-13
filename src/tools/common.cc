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

#if !defined(DARWIN) && !defined(__FreeBSD__)
#include <envz.h>
#endif // DARWIN

#include <memory>
#include <sys/types.h>
#include <fcntl.h>

extern "C" {
#include <histedit.h>
}

#include "acconfig.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MLog.h"
#include "mon/MonClient.h"
#include "mon/MonMap.h"
#include "osd/OSDMap.h"
#include "msg/SimpleMessenger.h"
#include "tools/common.h"

#include "common/errno.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "global/global_init.h"


#include "include/assert.h"

#define dout_subsys ceph_subsys_

// TODO: should move these into CephToolCtx for consistency
static enum ceph_tool_mode_t
  ceph_tool_mode(CEPH_TOOL_MODE_CLI_INPUT);
static Cond cmd_cond;
static SimpleMessenger *messenger = 0;
static Tokenizer *tok;

// sync command
bool pending_tell;  // is tell, vs monitor command
bool pending_tell_pgid;
uint64_t pending_tid = 0;
EntityName pending_target;
pg_t pending_target_pgid;
bool cmd_waiting_for_osdmap = false;
vector<string> pending_cmd;
bufferlist pending_bl;
bool reply;
string reply_rs;
int reply_rc;
bufferlist reply_bl;
entity_inst_t reply_from;

OSDMap *osdmap = 0;

Connection *command_con = NULL;
Context *tick_event = 0;
float tick_interval = 3.0;

// observe (push)
#include "mon/PGMap.h"
#include "mds/MDSMap.h"
#include "common/LogEntry.h"

#include "mon/mon_types.h"

#include "messages/MOSDMap.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

static set<int> registered, seen;

struct C_Tick : public Context {
  CephToolCtx *ctx;
  C_Tick(CephToolCtx *c) : ctx(c) {}
  void finish(int r) {
    if (command_con)
      messenger->send_keepalive(command_con);

    assert(tick_event == this);
    tick_event = new C_Tick(ctx);
    ctx->timer.add_event_after(tick_interval, tick_event);
  }
};

static void send_command(CephToolCtx *ctx)
{
  if (!pending_tell && !pending_tell_pgid) {
    version_t last_seen_version = 0;
    MMonCommand *m = new MMonCommand(ctx->mc.monmap.fsid, last_seen_version);
    m->cmd = pending_cmd;
    m->set_data(pending_bl);

    if (!ctx->concise)
      *ctx->log << ceph_clock_now(g_ceph_context) << " mon" << " <- " << pending_cmd << std::endl;

    ctx->mc.send_mon_message(m);
    return;
  }

  if (pending_tell_pgid || pending_target.get_type() == (int)entity_name_t::TYPE_OSD) {
    if (!osdmap) {
      ctx->mc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
      ctx->mc.renew_subs();
      cmd_waiting_for_osdmap = true;
      return;
    }
  }

  if (!ctx->concise)
    *ctx->log << ceph_clock_now(g_ceph_context) << " " << pending_target << " <- " << pending_cmd << std::endl;

  if (pending_tell_pgid) {
    // pick target osd
    vector<int> osds;
    int r = osdmap->pg_to_acting_osds(pending_target_pgid, osds);
    if (r < 0) {
      reply_rs = "error mapping pgid to an osd";
      reply_rc = -EINVAL;
      reply = true;
      cmd_cond.Signal();
      return;
    }
    if (r == 0) {
      reply_rs = "pgid currently maps to no osd";
      reply_rc = -ENOENT;
      reply = true;
      cmd_cond.Signal();
      return;
    }
    pending_target.set_name(entity_name_t::OSD(osds[0]));
  }

  if (pending_target.get_type() == (int)entity_name_t::TYPE_OSD) {
    const char *start = pending_target.get_id().c_str();
    char *end;
    int n = strtoll(start, &end, 10);
    if (end <= start) {
      stringstream ss;
      ss << "invalid osd id " << pending_target;
      reply_rs = ss.str();
      reply_rc = -EINVAL;
      reply = true;
      cmd_cond.Signal();
      return;
    }
    
    if (!osdmap->is_up(n)) {
      stringstream ss;
      ss << pending_target << " is not up";
      reply_rs = ss.str();
      reply_rc = -ESRCH;
      reply = true;
      cmd_cond.Signal();
      return;
    } else {
      if (!ctx->concise)
	*ctx->log << ceph_clock_now(g_ceph_context) << " " << pending_target << " <- " << pending_cmd << std::endl;

      MCommand *m = new MCommand(ctx->mc.monmap.fsid);
      m->cmd = pending_cmd;
      m->set_data(pending_bl);
      m->set_tid(++pending_tid);

      command_con = messenger->get_connection(osdmap->get_inst(n));
      messenger->send_message(m, command_con);

      if (tick_event)
	ctx->timer.cancel_event(tick_event);
      tick_event = new C_Tick(ctx);
      ctx->timer.add_event_after(tick_interval, tick_event);
    }
    return;
  }

  reply_rc = -EINVAL;
  reply = true;
  cmd_cond.Signal();
}

static void handle_osd_map(CephToolCtx *ctx, MOSDMap *m)
{
  epoch_t e = m->get_first();
  assert(m->maps.count(e));
  ctx->lock.Lock();
  delete osdmap;
  osdmap = new OSDMap;
  osdmap->decode(m->maps[e]);
  if (cmd_waiting_for_osdmap) {
    cmd_waiting_for_osdmap = false;
    send_command(ctx);
  }
  ctx->lock.Unlock();
  m->put();
}

static void handle_ack(CephToolCtx *ctx, MMonCommandAck *ack)
{
  ctx->lock.Lock();
  reply = true;
  reply_from = ack->get_source_inst();
  reply_rs = ack->rs;
  reply_rc = ack->r;
  reply_bl = ack->get_data();
  cmd_cond.Signal();
  ctx->lock.Unlock();
  ack->put();
}

static void handle_ack(CephToolCtx *ctx, MCommandReply *ack)
{
  ctx->lock.Lock();
  if (ack->get_tid() == pending_tid) {
    reply = true;
    reply_from = ack->get_source_inst();
    reply_rs = ack->rs;
    reply_rc = ack->r;
    reply_bl = ack->get_data();
    cmd_cond.Signal();
    if (tick_event) {
      ctx->timer.cancel_event(tick_event);
      tick_event = 0;
    }
  }
  ctx->lock.Unlock();
  ack->put();
}

int do_command(CephToolCtx *ctx,
	       vector<string>& cmd, bufferlist& bl, bufferlist& rbl)
{
  Mutex::Locker l(ctx->lock);

  pending_target = EntityName();
  pending_cmd = cmd;
  pending_bl = bl;
  pending_tell = false;
  pending_tell_pgid = false;
  reply = false;
  
  if (!cmd.empty() && cmd[0] == "tell") {
    if (cmd.size() == 1) {
      cerr << "no tell target specified" << std::endl;
      return -EINVAL;
    }
    if (!pending_target.from_str(cmd[1])) {
      cerr << "tell target '" << cmd[1] << "' not a valid entity name" << std::endl;
      return -EINVAL;
    }
    pending_cmd.erase(pending_cmd.begin(), pending_cmd.begin() + 2);
    pending_tell = true;
  }
  if (!cmd.empty() && cmd[0] == "pg") {
    if (cmd.size() == 1) {
      cerr << "pg requires at least one argument" << std::endl;
      return -EINVAL;
    }
    if (pending_target_pgid.parse(cmd[1].c_str())) {
      pending_tell_pgid = true;
    }
    // otherwise, send the request on to the monitor (e.g., 'pg dump').  sigh.
  }

  send_command(ctx);

  while (!reply)
    cmd_cond.Wait(ctx->lock);

  rbl = reply_bl;
  if (!ctx->concise)
    *ctx->log << ceph_clock_now(g_ceph_context) << " "
	   << reply_from.name << " -> '"
	   << reply_rs << "' (" << reply_rc << ")"
	   << std::endl;
  else {
    if (reply_rc >= 0)
      cout << reply_rs << std::endl;
    else
      cerr << reply_rs << std::endl;
  }

  return reply_rc;
}

void do_status(CephToolCtx *ctx, bool shutdown) {
  vector<string> cmd;
  cmd.push_back("status");
  bufferlist bl;

  do_command(ctx, cmd, bl, bl);

  if (shutdown)
    messenger->shutdown();
}

static const char *cli_prompt(EditLine *e)
{
  return "ceph> ";
}

int ceph_tool_do_cli(CephToolCtx *ctx)
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

    //*ctx->log << "typed '" << line << "'" << std::endl;

    if (chars_read == 0) {
      *ctx->log << "quit" << std::endl;
      break;
    }

    history(myhistory, &ev, H_ENTER, line);

    if (run_command(ctx, line))
      break;
  }

  history_end(myhistory);
  el_end(el);

  return 0;
}

int run_command(CephToolCtx *ctx, const char *line)
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
    if (!ctx->concise)
      *ctx->log << "----" << std::endl;
    fwrite(in.c_str(), in.length(), 1, stdout);
    if (!ctx->concise)
      *ctx->log << "---- (" << in.length() << " bytes)" << std::endl;
    return 0;
  }

  //out << "cmd is " << cmd << std::endl;

  bufferlist out;
  if (infile) {
    std::string error;
    if (out.read_file(infile, &error) == 0) {
      if (!ctx->concise)
	*ctx->log << "read " << out.length() << " from " << infile << std::endl;
    } else {
      *ctx->log << "couldn't read from " << infile << ": "
	        << error <<  std::endl;
      return 0;
    }
  }

  in.clear();
  do_command(ctx, cmd, out, in);

  if (in.length() == 0)
    return 0;

  if (outfile) {
    if (strcmp(outfile, "-") == 0) {
      if (!ctx->concise)
	*ctx->log << "----" << std::endl;
      fwrite(in.c_str(), in.length(), 1, stdout);
      if (!ctx->concise)
	*ctx->log << "---- (" << in.length() << " bytes)" << std::endl;
    }
    else {
      in.write_file(outfile);
      if (!ctx->concise)
	*ctx->log << "wrote " << in.length() << " to "
	       << outfile << std::endl;
    }
  }
  else {
    if (!ctx->concise)
      *ctx->log << "got " << in.length() << " byte payload; 'print' "
	     << "to dump to terminal, or add '>-' to command." << std::endl;
  }
  return 0;
}

CephToolCtx* ceph_tool_common_init(ceph_tool_mode_t mode, bool concise)
{
  ceph_tool_mode = mode;

  std::auto_ptr <CephToolCtx> ctx;
  ctx.reset(new CephToolCtx(g_ceph_context, concise));

  // get monmap
  if (ctx->mc.build_initial_monmap() < 0)
    return NULL;
  
  // initialize tokenizer
  tok = tok_init(NULL);

  // start up network
  messenger = new SimpleMessenger(g_ceph_context, entity_name_t::CLIENT(), "client",
                                  getpid());
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));
  messenger->start();

  ctx->mc.set_messenger(messenger);

  ctx->dispatcher = new Admin(ctx.get());
  messenger->add_dispatcher_head(ctx->dispatcher);

  int r = ctx->mc.init();
  if (r < 0)
    return NULL;

  ctx->lock.Lock();
  ctx->timer.init();
  ctx->lock.Unlock();

  // in case we 'tell ...'
  ctx->mc.set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);

  if (ctx->mc.authenticate() < 0) {
    derr << "unable to authenticate as " << g_conf->name << dendl;
    ceph_tool_messenger_shutdown();
    ceph_tool_common_shutdown(ctx.get());
    return NULL;
  }
  if (ctx->mc.get_monmap() < 0) {
    derr << "unable to get monmap" << dendl;
    ceph_tool_messenger_shutdown();
    ceph_tool_common_shutdown(ctx.get());
    return NULL;
  }
  return ctx.release();
}

int ceph_tool_messenger_shutdown()
{
  return messenger->shutdown();
}

int ceph_tool_common_shutdown(CephToolCtx *ctx)
{
  // wait for messenger to finish
  messenger->wait();

  ctx->lock.Lock();
  ctx->mc.shutdown();
  ctx->timer.shutdown();
  ctx->lock.Unlock();

  delete messenger;
  tok_end(tok);
  
  return 0;
}

void Subscriptions::handle_log(MLog *m) 
{
  dout(10) << __func__ << " received log msg ver " << m->version << dendl;

  if (last_known_version >= m->version) {
    dout(10) << __func__ 
	    << " we have already received ver " << m->version 
	    << " (highest received: " << last_known_version << ") " << dendl;
    return;
  }
  last_known_version = m->version;

  std::deque<LogEntry>::iterator it = m->entries.begin();
  for (; it != m->entries.end(); it++) {
    LogEntry e = *it;
    cout << e.stamp << " " << e.who.name 
	 << " " << e.type << " " << e.msg << std::endl;
  }
 
  version_t v = last_known_version+1;
  dout(10) << __func__ << " wanting " << name << " ver " << v << dendl;
  ctx->mc.sub_want(name, v, 0);
}

bool Admin::ms_dispatch(Message *m) {
  switch (m->get_type()) {
  case MSG_MON_COMMAND_ACK:
    handle_ack(ctx, (MMonCommandAck*)m);
    break;
  case MSG_COMMAND_REPLY:
    handle_ack(ctx, (MCommandReply*)m);
    break;
  case CEPH_MSG_MON_MAP:
    m->put();
    break;
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(ctx, (MOSDMap *)m);
    break;
  case MSG_LOG:
   {
    MLog *mlog = (MLog*) m;
    subs.handle_log(mlog);
    break;
   }
  default:
    return false;
  }
  return true;
}

void Admin::ms_handle_connect(Connection *con) {
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    ctx->lock.Lock();
    if (!pending_cmd.empty())
      send_command(ctx);
    ctx->lock.Unlock();
  }
}

bool Admin::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(ctx->lock);
  if (con == command_con) {
    command_con->put();
    command_con = NULL;
    if (!pending_cmd.empty())
      send_command(ctx);
    return true;
  }
  return false;
}

bool Admin::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, 
			      bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = ctx->mc.auth->build_authorizer(dest_type);
  return true;
}

