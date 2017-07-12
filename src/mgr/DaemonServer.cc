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

#include "include/str_list.h"
#include "auth/RotatingKeyRing.h"
#include "json_spirit/json_spirit_writer.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMonMgrReport.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"
#include "messages/MOSDScrub.h"
#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.server " << __func__ << " "

DaemonServer::DaemonServer(MonClient *monc_,
                           Finisher &finisher_,
			   DaemonStateIndex &daemon_state_,
			   ClusterState &cluster_state_,
			   PyModules &py_modules_,
			   LogChannelRef clog_,
			   LogChannelRef audit_clog_)
    : Dispatcher(g_ceph_context),
      client_byte_throttler(new Throttle(g_ceph_context, "mgr_client_bytes",
					 g_conf->mgr_client_bytes)),
      client_msg_throttler(new Throttle(g_ceph_context, "mgr_client_messages",
					g_conf->mgr_client_messages)),
      osd_byte_throttler(new Throttle(g_ceph_context, "mgr_osd_bytes",
				      g_conf->mgr_osd_bytes)),
      osd_msg_throttler(new Throttle(g_ceph_context, "mgr_osd_messsages",
				     g_conf->mgr_osd_messages)),
      mds_byte_throttler(new Throttle(g_ceph_context, "mgr_mds_bytes",
				      g_conf->mgr_mds_bytes)),
      mds_msg_throttler(new Throttle(g_ceph_context, "mgr_mds_messsages",
				     g_conf->mgr_mds_messages)),
      mon_byte_throttler(new Throttle(g_ceph_context, "mgr_mon_bytes",
				      g_conf->mgr_mon_bytes)),
      mon_msg_throttler(new Throttle(g_ceph_context, "mgr_mon_messsages",
				     g_conf->mgr_mon_messages)),
      msgr(nullptr),
      monc(monc_),
      finisher(finisher_),
      daemon_state(daemon_state_),
      cluster_state(cluster_state_),
      py_modules(py_modules_),
      clog(clog_),
      audit_clog(audit_clog_),
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
  std::string public_msgr_type = g_conf->ms_public_type.empty() ?
    g_conf->get_val<std::string>("ms_type") : g_conf->ms_public_type;
  msgr = Messenger::create(g_ceph_context, public_msgr_type,
			   entity_name_t::MGR(gid),
			   "mgr",
			   getpid(), 0);
  msgr->set_default_policy(Messenger::Policy::stateless_server(0));

  // throttle clients
  msgr->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
			      client_byte_throttler.get(),
			      client_msg_throttler.get());

  // servers
  msgr->set_policy_throttlers(entity_name_t::TYPE_OSD,
			      osd_byte_throttler.get(),
			      osd_msg_throttler.get());
  msgr->set_policy_throttlers(entity_name_t::TYPE_MDS,
			      mds_byte_throttler.get(),
			      mds_msg_throttler.get());
  msgr->set_policy_throttlers(entity_name_t::TYPE_MON,
			      mon_byte_throttler.get(),
			      mon_msg_throttler.get());

  int r = msgr->bind(g_conf->public_addr);
  if (r < 0) {
    derr << "unable to bind mgr to " << g_conf->public_addr << dendl;
    return r;
  }

  msgr->set_myname(entity_name_t::MGR(gid));
  msgr->set_addr_unknowns(client_addr);

  msgr->start();
  msgr->add_dispatcher_tail(this);

  started_at = ceph_clock_now();

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
    is_valid = false;
    return true;
  }

  MgrSessionRef s(new MgrSession(cct));
  s->inst.addr = con->get_peer_addr();
  AuthCapsInfo caps_info;

  is_valid = handler->verify_authorizer(
    cct, monc->rotating_secrets.get(),
    authorizer_data,
    authorizer_reply, s->entity_name,
    s->global_id, caps_info,
    session_key);

  if (is_valid) {
    if (caps_info.allow_all) {
      dout(10) << " session " << s << " " << s->entity_name
	       << " allow_all" << dendl;
      s->caps.set_allow_all();
    }
    if (caps_info.caps.length() > 0) {
      bufferlist::iterator p = caps_info.caps.begin();
      string str;
      try {
	::decode(str, p);
      }
      catch (buffer::error& e) {
      }
      bool success = s->caps.parse(str);
      if (success) {
	dout(10) << " session " << s << " " << s->entity_name
		 << " has caps " << s->caps << " '" << str << "'" << dendl;
      } else {
	dout(10) << " session " << s << " " << s->entity_name
		 << " failed to parse caps '" << str << "'" << dendl;
	is_valid = false;
      }
    }
    con->set_priv(s->get());

    if (peer_type == CEPH_ENTITY_TYPE_OSD) {
      Mutex::Locker l(lock);
      s->osd_id = atoi(s->entity_name.get_id().c_str());
      dout(10) << "registering osd." << s->osd_id << " session "
	       << s << " con " << con << dendl;
      osd_cons[s->osd_id].insert(con);
    }
  }

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

  *authorizer = monc->build_authorizer(dest_type);
  dout(20) << "got authorizer " << *authorizer << dendl;
  return *authorizer != NULL;
}

bool DaemonServer::ms_handle_reset(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    MgrSessionRef session(static_cast<MgrSession*>(con->get_priv()));
    if (!session) {
      return false;
    }
    session->put(); // SessionRef takes a ref
    Mutex::Locker l(lock);
    dout(10) << "unregistering osd." << session->osd_id
	     << "  session " << session << " con " << con << dendl;
    osd_cons[session->osd_id].erase(con);
  }
  return false;
}

bool DaemonServer::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

bool DaemonServer::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  switch (m->get_type()) {
    case MSG_PGSTATS:
      cluster_state.ingest_pgstats(static_cast<MPGStats*>(m));
      maybe_ready(m->get_source().num());
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

void DaemonServer::maybe_ready(int32_t osd_id)
{
  if (!pgmap_ready && reported_osds.find(osd_id) == reported_osds.end()) {
    dout(4) << "initial report from osd " << osd_id << dendl;
    reported_osds.insert(osd_id);
    std::set<int32_t> up_osds;

    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
        osdmap.get_up_osds(up_osds);
    });

    std::set<int32_t> unreported_osds;
    std::set_difference(up_osds.begin(), up_osds.end(),
                        reported_osds.begin(), reported_osds.end(),
                        std::inserter(unreported_osds, unreported_osds.begin()));

    if (unreported_osds.size() == 0) {
      dout(4) << "all osds have reported, sending PG state to mon" << dendl;
      pgmap_ready = true;
      reported_osds.clear();
      // Avoid waiting for next tick
      send_report();
    } else {
      dout(4) << "still waiting for " << unreported_osds.size() << " osds"
                 " to report in before PGMap is ready" << dendl;
    }
  }
}

void DaemonServer::shutdown()
{
  dout(10) << "begin" << dendl;
  msgr->shutdown();
  msgr->wait();
  dout(10) << "done" << dendl;
}



bool DaemonServer::handle_open(MMgrOpen *m)
{
  DaemonKey key;
  if (!m->service_name.empty()) {
    key.first = m->service_name;
  } else {
    key.first = ceph_entity_type_name(m->get_connection()->get_peer_type());
  }
  key.second = m->daemon_name;

  dout(4) << "from " << m->get_connection() << "  " << key << dendl;

  auto configure = new MMgrConfigure();
  configure->stats_period = g_conf->mgr_stats_period;
  m->get_connection()->send_message(configure);

  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    daemon_state.get(key)->perf_counters.clear();
  }

  if (m->service_daemon) {
    DaemonStatePtr daemon;
    if (daemon_state.exists(key)) {
      daemon = daemon_state.get(key);
    } else {
      dout(4) << "constructing new DaemonState for " << key << dendl;
      daemon = std::make_shared<DaemonState>(daemon_state.types);
      daemon->key = key;
      if (m->daemon_metadata.count("hostname")) {
        daemon->hostname = m->daemon_metadata["hostname"];
      }
      daemon_state.insert(daemon);
    }
    daemon->service_daemon = true;
    daemon->metadata = m->daemon_metadata;
    daemon->service_status = m->daemon_status;

    utime_t now = ceph_clock_now();
    auto d = pending_service_map.get_daemon(m->service_name,
					    m->daemon_name);
    if (d->gid != (uint64_t)m->get_source().num()) {
      dout(10) << "registering " << key << " in pending_service_map" << dendl;
      d->gid = m->get_source().num();
      d->addr = m->get_source_addr();
      d->start_epoch = pending_service_map.epoch;
      d->start_stamp = now;
      d->metadata = m->daemon_metadata;
      pending_service_map_dirty = pending_service_map.epoch;
    }
  }

  m->put();
  return true;
}

bool DaemonServer::handle_report(MMgrReport *m)
{
  DaemonKey key;
  if (!m->service_name.empty()) {
    key.first = m->service_name;
  } else {
    key.first = ceph_entity_type_name(m->get_connection()->get_peer_type());
  }
  key.second = m->daemon_name;

  dout(4) << "from " << m->get_connection() << " " << key << dendl;

  if (m->get_connection()->get_peer_type() == entity_name_t::TYPE_CLIENT &&
      m->service_name.empty()) {
    // Clients should not be sending us stats unless they are declaring
    // themselves to be a daemon for some service.
    dout(4) << "rejecting report from non-daemon client " << m->daemon_name
	    << dendl;
    m->put();
    return true;
  }

  DaemonStatePtr daemon;
  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << key << dendl;
    daemon = daemon_state.get(key);
  } else {
    dout(4) << "constructing new DaemonState for " << key << dendl;
    daemon = std::make_shared<DaemonState>(daemon_state.types);
    // FIXME: crap, we don't know the hostname at this stage.
    daemon->key = key;
    daemon_state.insert(daemon);
    // FIXME: we should avoid this case by rejecting MMgrReport from
    // daemons without sessions, and ensuring that session open
    // always contains metadata.
  }
  assert(daemon != nullptr);
  auto &daemon_counters = daemon->perf_counters;
  daemon_counters.update(m);

  if (daemon->service_daemon) {
    utime_t now = ceph_clock_now();
    if (m->daemon_status) {
      daemon->service_status = *m->daemon_status;
      daemon->service_status_stamp = now;
    }
    daemon->last_service_beacon = now;
  } else if (m->daemon_status) {
    derr << "got status from non-daemon " << key << dendl;
  }

  m->put();
  return true;
}

struct MgrCommand {
  string cmdstring;
  string helpstring;
  string module;
  string perm;
  string availability;

  bool requires_perm(char p) const {
    return (perm.find(p) != string::npos);
  }

} mgr_commands[] = {

#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability},
#include "MgrCommands.h"
#undef COMMAND
};

void DaemonServer::_generate_command_map(
  map<string,cmd_vartype>& cmdmap,
  map<string,string> &param_str_map)
{
  for (map<string,cmd_vartype>::const_iterator p = cmdmap.begin();
       p != cmdmap.end(); ++p) {
    if (p->first == "prefix")
      continue;
    if (p->first == "caps") {
      vector<string> cv;
      if (cmd_getval(g_ceph_context, cmdmap, "caps", cv) &&
	  cv.size() % 2 == 0) {
	for (unsigned i = 0; i < cv.size(); i += 2) {
	  string k = string("caps_") + cv[i];
	  param_str_map[k] = cv[i + 1];
	}
	continue;
      }
    }
    param_str_map[p->first] = cmd_vartype_stringify(p->second);
  }
}

const MgrCommand *DaemonServer::_get_mgrcommand(
  const string &cmd_prefix,
  MgrCommand *cmds,
  int cmds_size)
{
  MgrCommand *this_cmd = NULL;
  for (MgrCommand *cp = cmds;
       cp < &cmds[cmds_size]; cp++) {
    if (cp->cmdstring.compare(0, cmd_prefix.size(), cmd_prefix) == 0) {
      this_cmd = cp;
      break;
    }
  }
  return this_cmd;
}

bool DaemonServer::_allowed_command(
  MgrSession *s,
  const string &module,
  const string &prefix,
  const map<string,cmd_vartype>& cmdmap,
  const map<string,string>& param_str_map,
  const MgrCommand *this_cmd) {

  if (s->entity_name.is_mon()) {
    // mon is all-powerful.  even when it is forwarding commands on behalf of
    // old clients; we expect the mon is validating commands before proxying!
    return true;
  }

  bool cmd_r = this_cmd->requires_perm('r');
  bool cmd_w = this_cmd->requires_perm('w');
  bool cmd_x = this_cmd->requires_perm('x');

  bool capable = s->caps.is_capable(
    g_ceph_context,
    CEPH_ENTITY_TYPE_MGR,
    s->entity_name,
    module, prefix, param_str_map,
    cmd_r, cmd_w, cmd_x);

  dout(10) << " " << s->entity_name << " "
	   << (capable ? "" : "not ") << "capable" << dendl;
  return capable;
}

bool DaemonServer::handle_command(MCommand *m)
{
  int r = 0;
  std::stringstream ss;
  std::string prefix;

  assert(lock.is_locked_by_me());

  /**
   * The working data for processing an MCommand.  This lives in
   * a class to enable passing it into other threads for processing
   * outside of the thread/locks that called handle_command.
   */
  class CommandContext
  {
    public:
    MCommand *m;
    bufferlist odata;
    cmdmap_t cmdmap;

    CommandContext(MCommand *m_)
      : m(m_)
    {
    }

    ~CommandContext()
    {
      m->put();
    }

    void reply(int r, const std::stringstream &ss)
    {
      reply(r, ss.str());
    }

    void reply(int r, const std::string &rs)
    {
      // Let the connection drop as soon as we've sent our response
      ConnectionRef con = m->get_connection();
      if (con) {
        con->mark_disposable();
      }

      dout(1) << "handle_command " << cpp_strerror(r) << " " << rs << dendl;
      if (con) {
        MCommandReply *reply = new MCommandReply(r, rs);
        reply->set_tid(m->get_tid());
        reply->set_data(odata);
        con->send_message(reply);
      }
    }
  };

  /**
   * A context for receiving a bufferlist/error string from a background
   * function and then calling back to a CommandContext when it's done
   */
  class ReplyOnFinish : public Context {
    std::shared_ptr<CommandContext> cmdctx;

  public:
    bufferlist from_mon;
    string outs;

    ReplyOnFinish(std::shared_ptr<CommandContext> cmdctx_)
      : cmdctx(cmdctx_)
    {}
    void finish(int r) override {
      cmdctx->odata.claim_append(from_mon);
      cmdctx->reply(r, outs);
    }
  };

  std::shared_ptr<CommandContext> cmdctx = std::make_shared<CommandContext>(m);

  MgrSessionRef session(static_cast<MgrSession*>(m->get_connection()->get_priv()));
  if (!session) {
    return true;
  }
  session->put(); // SessionRef takes a ref
  if (session->inst.name == entity_name_t())
    session->inst.name = m->get_source();

  std::string format;
  boost::scoped_ptr<Formatter> f;
  map<string,string> param_str_map;

  if (!cmdmap_from_json(m->cmd, &(cmdctx->cmdmap), ss)) {
    cmdctx->reply(-EINVAL, ss);
    return true;
  }

  {
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "format", format, string("plain"));
    f.reset(Formatter::create(format));
  }

  cmd_getval(cct, cmdctx->cmdmap, "prefix", prefix);

  dout(4) << "decoded " << cmdctx->cmdmap.size() << dendl;
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
			   "mgr", pyc.perm, "cli", 0);
      cmdnum++;
    }

    for (const auto &cp : mgr_commands) {
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmddesc_to_json(&f, secname.str(), cp.cmdstring, cp.helpstring,
			   cp.module, cp.perm, cp.availability, 0);
      cmdnum++;
    }
    f.close_section();	// command_descriptions
    f.flush(cmdctx->odata);
    cmdctx->reply(0, ss);
    return true;
  }

  // lookup command
  const MgrCommand *mgr_cmd = _get_mgrcommand(prefix, mgr_commands,
                                              ARRAY_SIZE(mgr_commands));
  _generate_command_map(cmdctx->cmdmap, param_str_map);
  if (!mgr_cmd) {
    MgrCommand py_command = {"", "", "py", "rw", "cli"};
    if (!_allowed_command(session.get(), py_command.module, prefix, cmdctx->cmdmap,
                          param_str_map, &py_command)) {
      dout(1) << " access denied" << dendl;
      ss << "access denied; does your client key have mgr caps?"
	" See http://docs.ceph.com/docs/master/mgr/administrator/#client-authentication";
      cmdctx->reply(-EACCES, ss);
      return true;
    }
  } else {
    // validate user's permissions for requested command
    if (!_allowed_command(session.get(), mgr_cmd->module, prefix, cmdctx->cmdmap,
                          param_str_map, mgr_cmd)) {
      dout(1) << " access denied" << dendl;
      audit_clog->info() << "from='" << session->inst << "' "
                         << "entity='" << session->entity_name << "' "
                         << "cmd=" << m->cmd << ":  access denied";
      ss << "access denied' does your client key have mgr caps?"
	" See http://docs.ceph.com/docs/master/mgr/administrator/#client-authentication";
      cmdctx->reply(-EACCES, ss);
      return true;
    }
  }

  audit_clog->debug()
    << "from='" << session->inst << "' "
    << "entity='" << session->entity_name << "' "
    << "cmd=" << m->cmd << ": dispatch";

  // ----------------
  // service map commands
  if (prefix == "service dump") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    cluster_state.with_servicemap([&](const ServiceMap &service_map) {
	f->dump_object("service_map", service_map);
      });
    f->flush(cmdctx->odata);
    cmdctx->reply(0, ss);
    return true;
  }
  if (prefix == "service status") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    // only include state from services that are in the persisted service map
    f->open_object_section("service_status");
    ServiceMap s;
    cluster_state.with_servicemap([&](const ServiceMap& service_map) {
	s = service_map;
      });
    for (auto& p : s.services) {
      f->open_object_section(p.first.c_str());
      for (auto& q : p.second.daemons) {
	f->open_object_section(q.first.c_str());
	DaemonKey key(p.first, q.first);
	assert(daemon_state.exists(key));
	auto daemon = daemon_state.get(key);
	f->dump_stream("status_stamp") << daemon->service_status_stamp;
	f->dump_stream("last_beacon") << daemon->last_service_beacon;
	f->open_object_section("status");
	for (auto& r : daemon->service_status) {
	  f->dump_string(r.first.c_str(), r.second);
	}
	f->close_section();
	f->close_section();
      }
      f->close_section();
    }
    f->close_section();
    f->flush(cmdctx->odata);
    cmdctx->reply(0, ss);
    return true;
  }

  // -----------
  // PG commands

  if (prefix == "pg scrub" ||
      prefix == "pg repair" ||
      prefix == "pg deep-scrub") {
    string scrubop = prefix.substr(3, string::npos);
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    bool pg_exists = false;
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	pg_exists = osdmap.pg_exists(pgid);
      });
    if (!pg_exists) {
      ss << "pg " << pgid << " dne";
      cmdctx->reply(-ENOENT, ss);
      return true;
    }
    int acting_primary = -1;
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	acting_primary = osdmap.get_pg_acting_primary(pgid);
      });
    if (acting_primary == -1) {
      ss << "pg " << pgid << " has no primary osd";
      cmdctx->reply(-EAGAIN, ss);
      return true;
    }
    auto p = osd_cons.find(acting_primary);
    if (p == osd_cons.end()) {
      ss << "pg " << pgid << " primary osd." << acting_primary
	 << " is not currently connected";
      cmdctx->reply(-EAGAIN, ss);
    }
    vector<pg_t> pgs = { pgid };
    for (auto& con : p->second) {
      con->send_message(new MOSDScrub(monc->get_fsid(),
				      pgs,
				      scrubop == "repair",
				      scrubop == "deep-scrub"));
    }
    ss << "instructing pg " << pgid << " on osd." << acting_primary
       << " to " << scrubop;
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "osd scrub" ||
	      prefix == "osd deep-scrub" ||
	      prefix == "osd repair") {
    string whostr;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "who", whostr);
    vector<string> pvec;
    get_str_vec(prefix, pvec);

    set<int> osds;
    if (whostr == "*" || whostr == "all" || whostr == "any") {
      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	  for (int i = 0; i < osdmap.get_max_osd(); i++)
	    if (osdmap.is_up(i)) {
	      osds.insert(i);
	    }
	});
    } else {
      long osd = parse_osd_id(whostr.c_str(), &ss);
      if (osd < 0) {
	ss << "invalid osd '" << whostr << "'";
	cmdctx->reply(-EINVAL, ss);
	return true;
      }
      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	  if (osdmap.is_up(osd)) {
	    osds.insert(osd);
	  }
	});
      if (osds.empty()) {
	ss << "osd." << osd << " is not up";
	cmdctx->reply(-EAGAIN, ss);
	return true;
      }
    }
    set<int> sent_osds, failed_osds;
    for (auto osd : osds) {
      auto p = osd_cons.find(osd);
      if (p == osd_cons.end()) {
	failed_osds.insert(osd);
      } else {
	sent_osds.insert(osd);
	for (auto& con : p->second) {
	  con->send_message(new MOSDScrub(monc->get_fsid(),
					  pvec.back() == "repair",
					  pvec.back() == "deep-scrub"));
	}
      }
    }
    if (failed_osds.size() == osds.size()) {
      ss << "failed to instruct osd(s) " << osds << " to " << pvec.back()
	 << " (not connected)";
      r = -EAGAIN;
    } else {
      ss << "instructed osd(s) " << sent_osds << " to " << pvec.back();
      if (!failed_osds.empty()) {
	ss << "; osd(s) " << failed_osds << " were not connected";
      }
      r = 0;
    }
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "osd reweight-by-pg" ||
	     prefix == "osd reweight-by-utilization" ||
	     prefix == "osd test-reweight-by-pg" ||
	     prefix == "osd test-reweight-by-utilization") {
    bool by_pg =
      prefix == "osd reweight-by-pg" || prefix == "osd test-reweight-by-pg";
    bool dry_run =
      prefix == "osd test-reweight-by-pg" ||
      prefix == "osd test-reweight-by-utilization";
    int64_t oload;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "oload", oload, int64_t(120));
    set<int64_t> pools;
    vector<string> poolnames;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "pools", poolnames);
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	for (const auto& poolname : poolnames) {
	  int64_t pool = osdmap.lookup_pg_pool_name(poolname);
	  if (pool < 0) {
	    ss << "pool '" << poolname << "' does not exist";
	    r = -ENOENT;
	  }
	  pools.insert(pool);
	}
      });
    if (r) {
      cmdctx->reply(r, ss);
      return true;
    }
    double max_change = g_conf->mon_reweight_max_change;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "max_change", max_change);
    if (max_change <= 0.0) {
      ss << "max_change " << max_change << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    int64_t max_osds = g_conf->mon_reweight_max_osds;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "max_osds", max_osds);
    if (max_osds <= 0) {
      ss << "max_osds " << max_osds << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    string no_increasing;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "no_increasing", no_increasing);
    string out_str;
    mempool::osdmap::map<int32_t, uint32_t> new_weights;
    r = cluster_state.with_pgmap([&](const PGMap& pgmap) {
	return cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	    return reweight::by_utilization(osdmap, pgmap,
					    oload,
					    max_change,
					    max_osds,
					    by_pg,
					    pools.empty() ? NULL : &pools,
					    no_increasing == "--no-increasing",
					    &new_weights,
					    &ss, &out_str, f.get());
	  });
      });
    if (r >= 0) {
      dout(10) << "reweight::by_utilization: finished with " << out_str << dendl;
    }
    if (f) {
      f->flush(cmdctx->odata);
    } else {
      cmdctx->odata.append(out_str);
    }
    if (r < 0) {
      ss << "FAILED reweight-by-pg";
      cmdctx->reply(r, ss);
      return true;
    } else if (r == 0 || dry_run) {
      ss << "no change";
      cmdctx->reply(r, ss);
      return true;
    } else {
      json_spirit::Object json_object;
      for (const auto& osd_weight : new_weights) {
	json_spirit::Config::add(json_object,
				 std::to_string(osd_weight.first),
				 std::to_string(osd_weight.second));
      }
      string s = json_spirit::write(json_object);
      std::replace(begin(s), end(s), '\"', '\'');
      const string cmd =
	"{"
	"\"prefix\": \"osd reweightn\", "
	"\"weights\": \"" + s + "\""
	"}";
      auto on_finish = new ReplyOnFinish(cmdctx);
      monc->start_mon_command({cmd}, {},
			      &on_finish->from_mon, &on_finish->outs, on_finish);
      return true;
    }
  } else if (prefix == "osd df") {
    string method;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "output_method", method);
    r = cluster_state.with_pgservice([&](const PGMapStatService& pgservice) {
	return cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	    print_osd_utilization(osdmap, &pgservice, ss,
				  f.get(), method == "tree");
				  
	    cmdctx->odata.append(ss);
	    return 0;
	  });
      });
    cmdctx->reply(r, "");
    return true;
  } else {
    r = cluster_state.with_pgmap([&](const PGMap& pg_map) {
	return cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	    return process_pg_map_command(prefix, cmdctx->cmdmap, pg_map, osdmap,
					  f.get(), &ss, &cmdctx->odata);
	  });
      });

    if (r != -EOPNOTSUPP) {
      cmdctx->reply(r, ss);
      return true;
    }
  }

  // None of the special native commands, 
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
    cmdctx->reply(-EINVAL, ss);
    return true;
  } else {
    // Okay, now we have a handler to call, but we must not call it
    // in this thread, because the python handlers can do anything,
    // including blocking, and including calling back into mgr.
    dout(4) << "passing through " << cmdctx->cmdmap.size() << dendl;
    finisher.queue(new FunctionContext([cmdctx, handler](int r_) {
      std::stringstream ds;
      std::stringstream ss;
      int r = handler->handle_command(cmdctx->cmdmap, &ds, &ss);
      cmdctx->odata.append(ds);
      cmdctx->reply(r, ss);
    }));
    return true;
  }
}

void DaemonServer::_prune_pending_service_map()
{
  utime_t cutoff = ceph_clock_now();
  cutoff -= g_conf->mgr_service_beacon_grace;
  auto p = pending_service_map.services.begin();
  while (p != pending_service_map.services.end()) {
    auto q = p->second.daemons.begin();
    while (q != p->second.daemons.end()) {
      DaemonKey key(p->first, q->first);
      if (!daemon_state.exists(key)) {
	derr << "missing key " << key << dendl;
	++q;
	continue;
      }
      auto daemon = daemon_state.get(key);
      if (daemon->last_service_beacon == utime_t()) {
	// we must have just restarted; assume they are alive now.
	daemon->last_service_beacon = ceph_clock_now();
	++q;
	continue;
      }
      if (daemon->last_service_beacon < cutoff) {
	dout(10) << "pruning stale " << p->first << "." << q->first
		 << " last_beacon " << daemon->last_service_beacon << dendl;
	q = p->second.daemons.erase(q);
	pending_service_map_dirty = pending_service_map.epoch;
      } else {
	++q;
      }
    }
    if (p->second.daemons.empty()) {
      p = pending_service_map.services.erase(p);
      pending_service_map_dirty = pending_service_map.epoch;
    } else {
      ++p;
    }
  }
}

void DaemonServer::send_report()
{
  if (!pgmap_ready) {
    if (ceph_clock_now() - started_at > g_conf->mgr_stats_period * 4.0) {
      pgmap_ready = true;
      reported_osds.clear();
      dout(1) << "Giving up on OSDs that haven't reported yet, sending "
              << "potentially incomplete PG state to mon" << dendl;
    } else {
      dout(1) << "Not sending PG status to monitor yet, waiting for OSDs"
              << dendl;
      return;
    }
  }

  auto m = new MMonMgrReport();
  cluster_state.with_pgmap([&](const PGMap& pg_map) {
      cluster_state.update_delta_stats();

      if (pending_service_map.epoch) {
	_prune_pending_service_map();
	if (pending_service_map_dirty >= pending_service_map.epoch) {
	  pending_service_map.modified = ceph_clock_now();
	  ::encode(pending_service_map, m->service_map_bl, CEPH_FEATURES_ALL);
	  dout(10) << "sending service_map e" << pending_service_map.epoch
		   << dendl;
	  pending_service_map.epoch++;
	}
      }

      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	  // FIXME: no easy way to get mon features here.  this will do for
	  // now, though, as long as we don't make a backward-incompat change.
	  pg_map.encode_digest(osdmap, m->get_data(), CEPH_FEATURES_ALL);
	  dout(10) << pg_map << dendl;

	  pg_map.get_health_checks(g_ceph_context, osdmap,
				   &m->health_checks);
	  dout(10) << m->health_checks.checks.size() << " health checks"
		   << dendl;
	  dout(20) << "health checks:\n";
	  JSONFormatter jf(true);
	  jf.dump_object("health_checks", m->health_checks);
	  jf.flush(*_dout);
	  *_dout << dendl;
	});
    });
  // TODO? We currently do not notify the PyModules
  // TODO: respect needs_send, so we send the report only if we are asked to do
  //       so, or the state is updated.
  monc->send_mon_message(m);
}

void DaemonServer::got_service_map()
{
  Mutex::Locker l(lock);

  cluster_state.with_servicemap([&](const ServiceMap& service_map) {
      if (pending_service_map.epoch == 0) {
	// we just started up
	dout(10) << "got initial map e" << service_map.epoch << dendl;
	pending_service_map = service_map;
      } else {
	// we we already active and therefore must have persisted it,
	// which means ours is the same or newer.
	dout(10) << "got updated map e" << service_map.epoch << dendl;
      }
      pending_service_map.epoch = service_map.epoch + 1;
    });

  // cull missing daemons, populate new ones
  for (auto& p : pending_service_map.services) {
    std::set<std::string> names;
    for (auto& q : p.second.daemons) {
      names.insert(q.first);
      DaemonKey key(p.first, q.first);
      if (!daemon_state.exists(key)) {
	auto daemon = std::make_shared<DaemonState>(daemon_state.types);
	daemon->key = key;
	daemon->metadata = q.second.metadata;
        if (q.second.metadata.count("hostname")) {
          daemon->hostname = q.second.metadata["hostname"];
        }
	daemon->service_daemon = true;
	daemon_state.insert(daemon);
	dout(10) << "added missing " << key << dendl;
      }
    }
    daemon_state.cull(p.first, names);
  }
}
