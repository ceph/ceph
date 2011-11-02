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
#include "osd/OSDMap.h"
#include "msg/SimpleMessenger.h"
#include "tools/common.h"

#include "common/errno.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "global/global_init.h"

#if !defined(DARWIN) && !defined(__FreeBSD__)
#include <envz.h>
#endif // DARWIN

#include <memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern "C" {
#include <histedit.h>
}

// TODO: should move these into CephToolCtx for consistency
static enum ceph_tool_mode_t
  ceph_tool_mode(CEPH_TOOL_MODE_CLI_INPUT);
static Cond cmd_cond;
static SimpleMessenger *messenger = 0;
static Tokenizer *tok;

// sync command
bool pending_tell;  // is tell, vs monitor command
uint64_t pending_tid = 0;
EntityName pending_target;
vector<string> pending_cmd;
bufferlist pending_bl;
bool reply;
string reply_rs;
int reply_rc;
bufferlist reply_bl;
entity_inst_t reply_from;
Context *resend_event = 0;

OSDMap *osdmap = 0;

// observe (push)
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "mds/MDSMap.h"
#include "common/LogEntry.h"

#include "mon/mon_types.h"

#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"
#include "messages/MOSDMap.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

static set<int> registered, seen;

version_t map_ver[PAXOS_NUM];

static void handle_osd_map(CephToolCtx *ctx, MOSDMap *m)
{
  epoch_t e = m->get_first();
  assert(m->maps.count(e));
  ctx->lock.Lock();
  delete osdmap;
  osdmap = new OSDMap;
  osdmap->decode(m->maps[e]);
  cmd_cond.Signal();
  ctx->lock.Unlock();
  m->put();
}

static void handle_observe(CephToolCtx *ctx, MMonObserve *observe)
{
  dout(1) << observe->get_source() << " -> " << get_paxos_name(observe->machine_id)
	  << " registered" << dendl;
  ctx->lock.Lock();
  registered.insert(observe->machine_id);  
  ctx->lock.Unlock();
  observe->put();
}

static void handle_notify(CephToolCtx *ctx, MMonObserveNotify *notify)
{
  utime_t now = ceph_clock_now(g_ceph_context);

  dout(1) << notify->get_source() << " -> " << get_paxos_name(notify->machine_id)
	  << " v" << notify->ver
	  << (notify->is_latest ? " (latest)" : "")
	  << dendl;
  
  if (ceph_fsid_compare(&notify->fsid, &ctx->mc.monmap.fsid)) {
    dout(0) << notify->get_source_inst() << " notify fsid " << notify->fsid << " != "
	    << ctx->mc.monmap.fsid << dendl;
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
	ctx->pgmap.decode(p);
      } else {
	PGMap::Incremental inc;
	inc.decode(p);
	ctx->pgmap.apply_incremental(inc);
      }
      *ctx->log << now << "    pg " << ctx->pgmap << std::endl;
      ctx->updates |= PG_MON_UPDATE;
      break;
    }

  case PAXOS_MDSMAP:
    ctx->mdsmap.decode(notify->bl);
    *ctx->log << now << "   mds " << ctx->mdsmap << std::endl;
    ctx->updates |= MDS_MON_UPDATE;
    break;

  case PAXOS_OSDMAP:
    {
      if (notify->is_latest) {
	ctx->osdmap.decode(notify->bl);
      } else {
	OSDMap::Incremental inc(notify->bl);
	ctx->osdmap.apply_incremental(inc);
      }
      *ctx->log << now << "   osd " << ctx->osdmap << std::endl;
    }
    ctx->updates |= OSD_MON_UPDATE;
    break;

  case PAXOS_LOG:
    {
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	LogSummary summary;
	::decode(summary, p);
	// show last log message
	if (!summary.tail.empty())
	  *ctx->log << now << "   log " << summary.tail.back() << std::endl;
      } else {
	LogEntry le;
	__u8 v;
	::decode(v, p);
	while (!p.end()) {
	  le.decode(p);
	  *ctx->log << now << "   log " << le << std::endl;
	}
      }
      break;
    }

  case PAXOS_AUTH:
    {
#if 0
      bufferlist::iterator p = notify->bl.begin();
      if (notify->is_latest) {
	KeyServerData ctx;
	::decode(ctx, p);
	*ctx->log << now << "   auth " << std::endl;
      } else {
	while (!p.end()) {
	  AuthMonitor::Incremental inc;
          inc.decode(p);
	  *ctx->log << now << "   auth " << inc.name.to_str() << std::endl;
	}
      }
#endif
      /* ignoring auth incremental.. don't want to decode it */
      break;
    }

  case PAXOS_MONMAP:
    {
      ctx->mc.monmap.decode(notify->bl);
      *ctx->log << now << "   mon " << ctx->mc.monmap << std::endl;
    }
    break;

  default:
    *ctx->log << now << "  ignoring unknown machine id " << notify->machine_id << std::endl;
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
      ctx->gui_cond.Signal();
      break;
    default:
      // do nothing
      break;
  }

  notify->put();
}

class C_ObserverRefresh : public Context {
public:
  bool newmon;
  C_ObserverRefresh(bool n, CephToolCtx *ctx_) 
    : newmon(n),
      ctx(ctx_)
  {
  }
  void finish(int r) {
    send_observe_requests(ctx);
  }
private:
  CephToolCtx *ctx;
};

void send_observe_requests(CephToolCtx *ctx)
{
  dout(1) << "send_observe_requests " << dendl;

  for (int i=0; i<PAXOS_NUM; i++) {
    MMonObserve *m = new MMonObserve(ctx->mc.monmap.fsid, i, map_ver[i]);
    dout(1) << "mon" << " <- observe " << get_paxos_name(i) << dendl;
    ctx->mc.send_mon_message(m);
  }

  registered.clear();
  float seconds = g_conf->paxos_observer_timeout/2;
  dout(1) << " refresh after " << seconds << " with same mon" << dendl;
  ctx->timer.add_event_after(seconds, new C_ObserverRefresh(false, ctx));
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
  if (resend_event) {
    ctx->timer.cancel_event(resend_event);
    resend_event = 0;
  }
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
    if (resend_event) {
      ctx->timer.cancel_event(resend_event);
      resend_event = 0;
    }
  }
  ctx->lock.Unlock();
  ack->put();
}

static void send_command(CephToolCtx *ctx)
{
  if (!pending_tell) {
    version_t last_seen_version = 0;
    MMonCommand *m = new MMonCommand(ctx->mc.monmap.fsid, last_seen_version);
    m->cmd = pending_cmd;
    m->set_data(pending_bl);

    if (!ctx->concise)
      *ctx->log << ceph_clock_now(g_ceph_context) << " mon" << " <- " << pending_cmd << std::endl;

    ctx->mc.send_mon_message(m);
    return;
  }

  if (!ctx->concise)
    *ctx->log << ceph_clock_now(g_ceph_context) << " " << pending_target << " <- " << pending_cmd << std::endl;

  MCommand *m = new MCommand(ctx->mc.monmap.fsid);
  m->cmd = pending_cmd;
  m->set_data(pending_bl);
  m->set_tid(++pending_tid);
  
  if (pending_target.get_type() == (int)entity_name_t::TYPE_OSD) {
    if (!osdmap) {
      ctx->mc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
      ctx->mc.renew_subs();
      while (!osdmap)
	cmd_cond.Wait(ctx->lock);
    }

    const char *start = pending_target.get_id().c_str();
    char *end;
    int n = strtoll(start, &end, 10);
    if (end <= start) {
      reply_rc = -EINVAL;
      reply = true;
      return;
    }
    
    if (!osdmap->is_up(n)) {
      reply_rc = -ESRCH;
      reply = true;
    } else {
      messenger->send_message(m, osdmap->get_inst(n));
    }
    return;
  }

  reply_rc = -EINVAL;
  reply = true;
}

class Admin : public Dispatcher {
public:
  Admin(CephToolCtx *ctx_)
    : Dispatcher(g_ceph_context),
      ctx(ctx_)
  {
  }

  bool ms_dispatch(Message *m) {
    switch (m->get_type()) {
    case MSG_MON_COMMAND_ACK:
      handle_ack(ctx, (MMonCommandAck*)m);
      break;
    case MSG_COMMAND_REPLY:
      handle_ack(ctx, (MCommandReply*)m);
      break;
    case MSG_MON_OBSERVE_NOTIFY:
      handle_notify(ctx, (MMonObserveNotify *)m);
      break;
    case MSG_MON_OBSERVE:
      handle_observe(ctx, (MMonObserve *)m);
      break;
    case CEPH_MSG_MON_MAP:
      m->put();
      break;
    case CEPH_MSG_OSD_MAP:
      handle_osd_map(ctx, (MOSDMap *)m);
      break;
    default:
      return false;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      ctx->lock.Lock();
      if (ceph_tool_mode != CEPH_TOOL_MODE_CLI_INPUT) {
	send_observe_requests(ctx);
      }
      if (pending_cmd.size())
	send_command(ctx);
      ctx->lock.Unlock();
    }
  }
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
  {
    if (dest_type == CEPH_ENTITY_TYPE_MON)
      return true;
    *authorizer = ctx->mc.auth->build_authorizer(dest_type);
    return true;
  }

private:
  CephToolCtx *ctx;
};

int do_command(CephToolCtx *ctx,
	       vector<string>& cmd, bufferlist& bl, bufferlist& rbl)
{
  Mutex::Locker l(ctx->lock);

  pending_target = EntityName();
  pending_cmd = cmd;
  pending_bl = bl;
  pending_tell = false;
  reply = false;
  
  if (cmd.size() > 0 && cmd[0] == "tell") {
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

  send_command(ctx);

  while (!reply)
    cmd_cond.Wait(ctx->lock);

  rbl = reply_bl;
  if (!ctx->concise)
    *ctx->log << ceph_clock_now(g_ceph_context) << " "
	   << reply_from.name << " -> '"
	   << reply_rs << "' (" << reply_rc << ")"
	   << std::endl;
  else
    cout << reply_rs << std::endl;

  return reply_rc;
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
  messenger = new SimpleMessenger(g_ceph_context);
  messenger->register_entity(entity_name_t::CLIENT());
  messenger->start_with_nonce(getpid());
  ctx->dispatcher = new Admin(ctx.get());
  messenger->add_dispatcher_head(ctx->dispatcher);

  ctx->lock.Lock();
  ctx->timer.init();
  ctx->lock.Unlock();

  ctx->mc.set_messenger(messenger);
  ctx->mc.init();

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
  messenger->destroy();
  tok_end(tok);
  
  ctx->lock.Lock();
  ctx->mc.shutdown();
  ctx->timer.shutdown();
  ctx->lock.Unlock();
  return 0;
}
