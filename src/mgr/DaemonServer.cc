// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "DaemonServer.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.server " << __func__ << " "

DaemonServer::DaemonServer(MonClient *monc_,
  DaemonStateIndex &daemon_state_,
  ClusterState &cluster_state_,
  PyModules &py_modules_)
    : Dispatcher(g_ceph_context), msgr(nullptr), monc(monc_),
      daemon_state(daemon_state_),
      cluster_state(cluster_state_),
      py_modules(py_modules_),
      auth_registry(g_ceph_context,
                    g_conf->auth_supported.empty() ?
                      g_conf->auth_cluster_required :
                      g_conf->auth_supported),
      lock("DaemonServer")
{}

DaemonServer::~DaemonServer() {
  delete msgr;
}

int DaemonServer::init(uint64_t gid, entity_addr_t client_addr)
{
  // Initialize Messenger
  msgr = Messenger::create(g_ceph_context, g_conf->ms_type,
      entity_name_t::MGR(gid), "server", getpid());
  int r = msgr->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  msgr->set_myname(entity_name_t::MGR(gid));
  msgr->set_addr_unknowns(client_addr);

  msgr->start();
  msgr->add_dispatcher_tail(this);

  return 0;
}

entity_addr_t DaemonServer::get_myaddr() const
{
  return msgr->get_myaddr();
}


bool DaemonServer::ms_verify_authorizer(Connection *con,
    int peer_type,
    int protocol,
    ceph::bufferlist& authorizer_data,
    ceph::bufferlist& authorizer_reply,
    bool& is_valid,
    CryptoKey& session_key)
{
  auto handler = auth_registry.get_handler(protocol);
  if (!handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    assert(0);
    is_valid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id = 0;

  is_valid = handler->verify_authorizer(cct, monc->rotating_secrets,
						  authorizer_data,
                                                  authorizer_reply, name,
                                                  global_id, caps_info,
                                                  session_key);

  // TODO: invent some caps suitable for ceph-mgr

  return true;
}


bool DaemonServer::ms_get_authorizer(int dest_type,
    AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "type=" << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON) {
    return true;
  }

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  dout(20) << "got authorizer " << *authorizer << dendl;
  return *authorizer != NULL;
}


bool DaemonServer::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  switch(m->get_type()) {
    case MSG_PGSTATS:
      cluster_state.ingest_pgstats(static_cast<MPGStats*>(m));
      m->put();
      return true;
    case MSG_MGR_REPORT:
      return handle_report(static_cast<MMgrReport*>(m));
    case MSG_MGR_OPEN:
      return handle_open(static_cast<MMgrOpen*>(m));
    case MSG_COMMAND:
      return handle_command(static_cast<MCommand*>(m));
    default:
      dout(1) << "Unhandled message type " << m->get_type() << dendl;
      return false;
  };
}

void DaemonServer::shutdown()
{
  msgr->shutdown();
  msgr->wait();
}



bool DaemonServer::handle_open(MMgrOpen *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  auto configure = new MMgrConfigure();
  configure->stats_period = 5;
  m->get_connection()->send_message(configure);

  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    daemon_state.get(key)->perf_counters.clear();
  }

  m->put();
  return true;
}

bool DaemonServer::handle_report(MMgrReport *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  DaemonStatePtr daemon;
  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    daemon = daemon_state.get(key);
  } else {
    dout(4) << "constructing new DaemonState for " << m->daemon_name << dendl;
    daemon = std::make_shared<DaemonState>(daemon_state.types);
    // FIXME: crap, we don't know the hostname at this stage.
    daemon->key = key;
    daemon_state.insert(daemon);
  }

  assert(daemon != nullptr);
  auto &daemon_counters = daemon->perf_counters;
  daemon_counters.update(m);
  
  m->put();
  return true;
}

struct MgrCommand {
  string cmdstring;
  string helpstring;
  string module;
  string perm;
  string availability;
} mgr_commands[] = {

#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability},

COMMAND("foo " \
	"name=bar,type=CephString", \
	"do a thing", "mgr", "rw", "cli")
};

bool DaemonServer::handle_command(MCommand *m)
{
  int r = 0;
  std::stringstream ss;
  std::stringstream ds;
  bufferlist odata;
  std::string prefix;

  assert(lock.is_locked_by_me());

  cmdmap_t cmdmap;

  // TODO enforce some caps
  
  // TODO background the call into python land so that we don't
  // block a messenger thread on python code.

  ConnectionRef con = m->get_connection();

  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -EINVAL;
    goto out;
  }

  dout(4) << "decoded " << cmdmap.size() << dendl;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  dout(4) << "prefix=" << prefix << dendl;

  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;

    dout(10) << "reading commands from python modules" << dendl;
    auto py_commands = py_modules.get_commands();

    JSONFormatter f;
    f.open_object_section("command_descriptions");
    for (const auto &pyc : py_commands) {
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dout(20) << "Dumping " << pyc.cmdstring << " (" << pyc.helpstring
               << ")" << dendl;
      dump_cmddesc_to_json(&f, secname.str(), pyc.cmdstring, pyc.helpstring,
			   "mgr", pyc.perm, "cli");
      cmdnum++;
    }
#if 0
    for (MgrCommand *cp = mgr_commands;
	 cp < &mgr_commands[ARRAY_SIZE(mgr_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmddesc_to_json(f, secname.str(), cp->cmdstring, cp->helpstring,
			   cp->module, cp->perm, cp->availability);
      cmdnum++;
    }
#endif
    f.close_section();	// command_descriptions

    f.flush(ds);
    goto out;
  } else {
    // Let's find you a handler!
    MgrPyModule *handler = nullptr;
    auto py_commands = py_modules.get_commands();
    for (const auto &pyc : py_commands) {
      auto pyc_prefix = cmddesc_get_prefix(pyc.cmdstring);
      dout(1) << "pyc_prefix: '" << pyc_prefix << "'" << dendl;
      if (pyc_prefix == prefix) {
        handler = pyc.handler;
        break;
      }
    }

    if (handler == nullptr) {
      ss << "No handler found for '" << prefix << "'";
      dout(4) << "No handler found for '" << prefix << "'" << dendl;
      r = -EINVAL;
      goto out;
    }

    // FIXME: go run this python part in another thread, not inline
    // with a ms_dispatch, so that the python part can block if it
    // wants to.
    dout(4) << "passing through " << cmdmap.size() << dendl;
    r = handler->handle_command(cmdmap, &ds, &ss);
    goto out;
  }

 out:
  std::string rs;
  rs = ss.str();
  odata.append(ds);
  dout(1) << "do_command r=" << r << " " << rs << dendl;
  //clog->info() << rs << "\n";
  if (con) {
    MCommandReply *reply = new MCommandReply(r, rs);
    reply->set_tid(m->get_tid());
    reply->set_data(odata);
    con->send_message(reply);
  }

  m->put();
  return true;
}

