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
#include "mgr/Mgr.h"

#include "include/stringify.h"
#include "include/str_list.h"
#include "auth/RotatingKeyRing.h"
#include "json_spirit/json_spirit_writer.h"

#include "mgr/mgr_commands.h"
#include "mgr/DaemonHealthMetricCollector.h"
#include "mgr/OSDPerfMetricCollector.h"
#include "mon/MonCommand.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMonMgrReport.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrub2.h"
#include "messages/MOSDForceRecovery.h"
#include "common/errno.h"
#include "common/pick_address.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.server " << __func__ << " "



DaemonServer::DaemonServer(MonClient *monc_,
                           Finisher &finisher_,
			   DaemonStateIndex &daemon_state_,
			   ClusterState &cluster_state_,
			   PyModuleRegistry &py_modules_,
			   LogChannelRef clog_,
			   LogChannelRef audit_clog_)
    : Dispatcher(g_ceph_context),
      client_byte_throttler(new Throttle(g_ceph_context, "mgr_client_bytes",
					 g_conf().get_val<Option::size_t>("mgr_client_bytes"))),
      client_msg_throttler(new Throttle(g_ceph_context, "mgr_client_messages",
					g_conf().get_val<uint64_t>("mgr_client_messages"))),
      osd_byte_throttler(new Throttle(g_ceph_context, "mgr_osd_bytes",
				      g_conf().get_val<Option::size_t>("mgr_osd_bytes"))),
      osd_msg_throttler(new Throttle(g_ceph_context, "mgr_osd_messsages",
				     g_conf().get_val<uint64_t>("mgr_osd_messages"))),
      mds_byte_throttler(new Throttle(g_ceph_context, "mgr_mds_bytes",
				      g_conf().get_val<Option::size_t>("mgr_mds_bytes"))),
      mds_msg_throttler(new Throttle(g_ceph_context, "mgr_mds_messsages",
				     g_conf().get_val<uint64_t>("mgr_mds_messages"))),
      mon_byte_throttler(new Throttle(g_ceph_context, "mgr_mon_bytes",
				      g_conf().get_val<Option::size_t>("mgr_mon_bytes"))),
      mon_msg_throttler(new Throttle(g_ceph_context, "mgr_mon_messsages",
				     g_conf().get_val<uint64_t>("mgr_mon_messages"))),
      msgr(nullptr),
      monc(monc_),
      finisher(finisher_),
      daemon_state(daemon_state_),
      cluster_state(cluster_state_),
      py_modules(py_modules_),
      clog(clog_),
      audit_clog(audit_clog_),
      lock("DaemonServer"),
      pgmap_ready(false),
      timer(g_ceph_context, lock),
      shutting_down(false),
      tick_event(nullptr),
      osd_perf_metric_collector_listener(this),
      osd_perf_metric_collector(osd_perf_metric_collector_listener)
{
  g_conf().add_observer(this);
}

DaemonServer::~DaemonServer() {
  delete msgr;
  g_conf().remove_observer(this);
}

int DaemonServer::init(uint64_t gid, entity_addrvec_t client_addrs)
{
  // Initialize Messenger
  std::string public_msgr_type = g_conf()->ms_public_type.empty() ?
    g_conf().get_val<std::string>("ms_type") : g_conf()->ms_public_type;
  msgr = Messenger::create(g_ceph_context, public_msgr_type,
			   entity_name_t::MGR(gid),
			   "mgr",
			   getpid(), 0);
  msgr->set_default_policy(Messenger::Policy::stateless_server(0));

  msgr->set_auth_client(monc);

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

  entity_addrvec_t addrs;
  int r = pick_addresses(cct, CEPH_PICK_ADDRESS_PUBLIC, &addrs);
  if (r < 0) {
    return r;
  }
  dout(20) << __func__ << " will bind to " << addrs << dendl;
  r = msgr->bindv(addrs);
  if (r < 0) {
    derr << "unable to bind mgr to " << addrs << dendl;
    return r;
  }

  msgr->set_myname(entity_name_t::MGR(gid));
  msgr->set_addr_unknowns(client_addrs);

  msgr->start();
  msgr->add_dispatcher_tail(this);

  msgr->set_auth_server(monc);
  monc->set_handle_authentication_dispatcher(this);

  started_at = ceph_clock_now();

  std::lock_guard l(lock);
  timer.init();

  schedule_tick_locked(
    g_conf().get_val<std::chrono::seconds>("mgr_tick_period").count());

  return 0;
}

entity_addrvec_t DaemonServer::get_myaddrs() const
{
  return msgr->get_myaddrs();
}

KeyStore *DaemonServer::ms_get_auth1_authorizer_keystore()
{
  return monc->rotating_secrets.get();
}

int DaemonServer::ms_handle_authentication(Connection *con)
{
  int ret = 0;
  MgrSession *s = new MgrSession(cct);
  con->set_priv(s);
  s->inst.addr = con->get_peer_addr();
  s->entity_name = con->peer_name;
  dout(10) << __func__ << " new session " << s << " con " << con
	   << " entity " << con->peer_name
	   << " addr " << con->get_peer_addrs()
	   << dendl;

  AuthCapsInfo &caps_info = con->get_peer_caps_info();
  if (caps_info.allow_all) {
    dout(10) << " session " << s << " " << s->entity_name
	     << " allow_all" << dendl;
    s->caps.set_allow_all();
  }

  if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    string str;
    try {
      decode(str, p);
    }
    catch (buffer::error& e) {
      ret = -EPERM;
    }
    bool success = s->caps.parse(str);
    if (success) {
      dout(10) << " session " << s << " " << s->entity_name
	       << " has caps " << s->caps << " '" << str << "'" << dendl;
      ret = 1;
    } else {
      dout(10) << " session " << s << " " << s->entity_name
	       << " failed to parse caps '" << str << "'" << dendl;
      ret = -EPERM;
    }
  }

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    std::lock_guard l(lock);
    s->osd_id = atoi(s->entity_name.get_id().c_str());
    dout(10) << "registering osd." << s->osd_id << " session "
	     << s << " con " << con << dendl;
    osd_cons[s->osd_id].insert(con);
  }

  return ret;
}

bool DaemonServer::ms_get_authorizer(
  int dest_type,
  AuthAuthorizer **authorizer)
{
  dout(10) << "type=" << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON) {
    return true;
  }

  *authorizer = monc->build_authorizer(dest_type);
  dout(20) << "got authorizer " << *authorizer << dendl;
  return *authorizer != NULL;
}

bool DaemonServer::ms_handle_reset(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    auto priv = con->get_priv();
    auto session = static_cast<MgrSession*>(priv.get());
    if (!session) {
      return false;
    }
    std::lock_guard l(lock);
    dout(10) << "unregistering osd." << session->osd_id
	     << "  session " << session << " con " << con << dendl;
    osd_cons[session->osd_id].erase(con);

    auto iter = daemon_connections.find(con);
    if (iter != daemon_connections.end()) {
      daemon_connections.erase(iter);
    }
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
  // Note that we do *not* take ::lock here, in order to avoid
  // serializing all message handling.  It's up to each handler
  // to take whatever locks it needs.
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
    case MSG_MGR_CLOSE:
      return handle_close(static_cast<MMgrClose*>(m));
    case MSG_COMMAND:
      return handle_command(static_cast<MCommand*>(m));
    default:
      dout(1) << "Unhandled message type " << m->get_type() << dendl;
      return false;
  };
}

void DaemonServer::maybe_ready(int32_t osd_id)
{
  if (pgmap_ready.load()) {
    // Fast path: we don't need to take lock because pgmap_ready
    // is already set
  } else {
    std::lock_guard l(lock);

    if (reported_osds.find(osd_id) == reported_osds.end()) {
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
}

void DaemonServer::tick()
{
  dout(10) << dendl;
  send_report();
  adjust_pgs();

  schedule_tick_locked(
    g_conf().get_val<std::chrono::seconds>("mgr_tick_period").count());
}

// Currently modules do not set health checks in response to events delivered to
// all modules (e.g. notify) so we do not risk a thundering hurd situation here.
// if this pattern emerges in the future, this scheduler could be modified to
// fire after all modules have had a chance to set their health checks.
void DaemonServer::schedule_tick_locked(double delay_sec)
{
  ceph_assert(lock.is_locked_by_me());

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = nullptr;
  }

  // on shutdown start rejecting explicit requests to send reports that may
  // originate from python land which may still be running.
  if (shutting_down)
    return;

  tick_event = timer.add_event_after(delay_sec,
    new FunctionContext([this](int r) {
      tick();
  }));
}

void DaemonServer::schedule_tick(double delay_sec)
{
  std::lock_guard l(lock);
  schedule_tick_locked(delay_sec);
}

void DaemonServer::handle_osd_perf_metric_query_updated()
{
  dout(10) << dendl;

  // Send a fresh MMgrConfigure to all clients, so that they can follow
  // the new policy for transmitting stats
  finisher.queue(new FunctionContext([this](int r) {
        std::lock_guard l(lock);
        for (auto &c : daemon_connections) {
          if (c->peer_is_osd()) {
            _send_configure(c);
          }
        }
      }));
}

void DaemonServer::shutdown()
{
  dout(10) << "begin" << dendl;
  msgr->shutdown();
  msgr->wait();
  dout(10) << "done" << dendl;

  std::lock_guard l(lock);
  shutting_down = true;
  timer.shutdown();
}

static DaemonKey key_from_service(
  const std::string& service_name,
  int peer_type,
  const std::string& daemon_name)
{
  if (!service_name.empty()) {
    return DaemonKey(service_name, daemon_name);
  } else {
    return DaemonKey(ceph_entity_type_name(peer_type), daemon_name);
  }
}

static bool key_from_string(
  const std::string& name,
  DaemonKey *out)
{
  auto p = name.find('.');
  if (p == std::string::npos) {
    return false;
  }
  out->first = name.substr(0, p);
  out->second = name.substr(p + 1);
  return true;
}

bool DaemonServer::handle_open(MMgrOpen *m)
{
  std::lock_guard l(lock);

  DaemonKey key = key_from_service(m->service_name,
				   m->get_connection()->get_peer_type(),
				   m->daemon_name);

  dout(4) << "from " << m->get_connection() << "  " << key << dendl;

  _send_configure(m->get_connection());

  DaemonStatePtr daemon;
  if (daemon_state.exists(key)) {
    daemon = daemon_state.get(key);
  }
  if (m->service_daemon && !daemon) {
    dout(4) << "constructing new DaemonState for " << key << dendl;
    daemon = std::make_shared<DaemonState>(daemon_state.types);
    daemon->key = key;
    daemon->service_daemon = true;
    if (m->daemon_metadata.count("hostname")) {
      daemon->hostname = m->daemon_metadata["hostname"];
    }
    daemon_state.insert(daemon);
  }
  if (daemon) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    std::lock_guard l(daemon->lock);
    daemon->perf_counters.clear();

    if (m->service_daemon) {
      daemon->set_metadata(m->daemon_metadata);
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

    auto p = m->config_bl.cbegin();
    if (p != m->config_bl.end()) {
      decode(daemon->config, p);
      decode(daemon->ignored_mon_config, p);
      dout(20) << " got config " << daemon->config
	       << " ignored " << daemon->ignored_mon_config << dendl;
    }
    daemon->config_defaults_bl = m->config_defaults_bl;
    daemon->config_defaults.clear();
    dout(20) << " got config_defaults_bl " << daemon->config_defaults_bl.length()
	     << " bytes" << dendl;
  }

  if (m->get_connection()->get_peer_type() != entity_name_t::TYPE_CLIENT &&
      m->service_name.empty())
  {
    // Store in set of the daemon/service connections, i.e. those
    // connections that require an update in the event of stats
    // configuration changes.
    daemon_connections.insert(m->get_connection());
  }

  m->put();
  return true;
}

bool DaemonServer::handle_close(MMgrClose *m)
{
  std::lock_guard l(lock);

  DaemonKey key = key_from_service(m->service_name,
				   m->get_connection()->get_peer_type(),
				   m->daemon_name);
  dout(4) << "from " << m->get_connection() << "  " << key << dendl;

  if (daemon_state.exists(key)) {
    DaemonStatePtr daemon = daemon_state.get(key);
    daemon_state.rm(key);
    {
      std::lock_guard l(daemon->lock);
      if (daemon->service_daemon) {
	pending_service_map.rm_daemon(m->service_name, m->daemon_name);
	pending_service_map_dirty = pending_service_map.epoch;
      }
    }
  }

  // send same message back as a reply
  m->get_connection()->send_message(m);
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
    m->get_connection()->mark_down();
    m->put();
    return true;
  }

  // Look up the DaemonState
  DaemonStatePtr daemon;
  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << key << dendl;
    daemon = daemon_state.get(key);
  } else {
    // we don't know the hostname at this stage, reject MMgrReport here.
    dout(5) << "rejecting report from " << key << ", since we do not have its metadata now."
	    << dendl;

    // issue metadata request in background
    if (!daemon_state.is_updating(key) && 
	(key.first == "osd" || key.first == "mds" || key.first == "mon")) {

      std::ostringstream oss;
      auto c = new MetadataUpdate(daemon_state, key);
      if (key.first == "osd") {
        oss << "{\"prefix\": \"osd metadata\", \"id\": "
            << key.second<< "}";

      } else if (key.first == "mds") {
        c->set_default("addr", stringify(m->get_source_addr()));
        oss << "{\"prefix\": \"mds metadata\", \"who\": \""
            << key.second << "\"}";
 
      } else if (key.first == "mon") {
        oss << "{\"prefix\": \"mon metadata\", \"id\": \""
            << key.second << "\"}";
      } else {
	ceph_abort();
      }

      monc->start_mon_command({oss.str()}, {}, &c->outbl, &c->outs, c);
    }
    
    {
      std::lock_guard l(lock);
      // kill session
      auto priv = m->get_connection()->get_priv();
      auto session = static_cast<MgrSession*>(priv.get());
      if (!session) {
	return false;
      }
      m->get_connection()->mark_down();

      dout(10) << "unregistering osd." << session->osd_id
	       << "  session " << session << " con " << m->get_connection() << dendl;
      
      if (osd_cons.find(session->osd_id) != osd_cons.end()) {
	   osd_cons[session->osd_id].erase(m->get_connection());
      } 

      auto iter = daemon_connections.find(m->get_connection());
      if (iter != daemon_connections.end()) {
	daemon_connections.erase(iter);
      }
    }

    return false;
  }

  // Update the DaemonState
  ceph_assert(daemon != nullptr);
  {
    std::lock_guard l(daemon->lock);
    auto &daemon_counters = daemon->perf_counters;
    daemon_counters.update(m);

    auto p = m->config_bl.cbegin();
    if (p != m->config_bl.end()) {
      decode(daemon->config, p);
      decode(daemon->ignored_mon_config, p);
      dout(20) << " got config " << daemon->config
	       << " ignored " << daemon->ignored_mon_config << dendl;
    }

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
    if (m->get_connection()->peer_is_osd() || m->get_connection()->peer_is_mon()) {
      // only OSD and MON send health_checks to me now
      daemon->daemon_health_metrics = std::move(m->daemon_health_metrics);
      dout(10) << "daemon_health_metrics " << daemon->daemon_health_metrics
	       << dendl;
    }
  }

  // if there are any schema updates, notify the python modules
  if (!m->declare_types.empty() || !m->undeclare_types.empty()) {
    ostringstream oss;
    oss << key.first << '.' << key.second;
    py_modules.notify_all("perf_schema_update", oss.str());
  }

  if (m->get_connection()->peer_is_osd()) {
    osd_perf_metric_collector.process_reports(m->osd_perf_metric_reports);
  }

  m->put();
  return true;
}


void DaemonServer::_generate_command_map(
  cmdmap_t& cmdmap,
  map<string,string> &param_str_map)
{
  for (auto p = cmdmap.begin();
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

const MonCommand *DaemonServer::_get_mgrcommand(
  const string &cmd_prefix,
  const std::vector<MonCommand> &cmds)
{
  const MonCommand *this_cmd = nullptr;
  for (const auto &cmd : cmds) {
    if (cmd.cmdstring.compare(0, cmd_prefix.size(), cmd_prefix) == 0) {
      this_cmd = &cmd;
      break;
    }
  }
  return this_cmd;
}

bool DaemonServer::_allowed_command(
  MgrSession *s,
  const string &module,
  const string &prefix,
  const cmdmap_t& cmdmap,
  const map<string,string>& param_str_map,
  const MonCommand *this_cmd) {

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
    cmd_r, cmd_w, cmd_x,
    s->get_peer_addr());

  dout(10) << " " << s->entity_name << " "
	   << (capable ? "" : "not ") << "capable" << dendl;
  return capable;
}

/**
 * The working data for processing an MCommand.  This lives in
 * a class to enable passing it into other threads for processing
 * outside of the thread/locks that called handle_command.
 */
class CommandContext {
public:
  MCommand *m;
  bufferlist odata;
  cmdmap_t cmdmap;

  explicit CommandContext(MCommand *m_)
    : m(m_) {
  }

  ~CommandContext() {
    m->put();
  }

  void reply(int r, const std::stringstream &ss) {
    reply(r, ss.str());
  }

  void reply(int r, const std::string &rs) {
    // Let the connection drop as soon as we've sent our response
    ConnectionRef con = m->get_connection();
    if (con) {
      con->mark_disposable();
    }

    if (r == 0) {
      dout(4) << __func__ << " success" << dendl;
    } else {
      derr << __func__ << " " << cpp_strerror(r) << " " << rs << dendl;
    }
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

  explicit ReplyOnFinish(const std::shared_ptr<CommandContext> &cmdctx_)
    : cmdctx(cmdctx_)
    {}
  void finish(int r) override {
    cmdctx->odata.claim_append(from_mon);
    cmdctx->reply(r, outs);
  }
};

bool DaemonServer::handle_command(MCommand *m)
{
  std::lock_guard l(lock);
  std::shared_ptr<CommandContext> cmdctx = std::make_shared<CommandContext>(m);
  try {
    return _handle_command(m, cmdctx);
  } catch (const bad_cmd_get& e) {
    cmdctx->reply(-EINVAL, e.what());
    return true;
  }
}

bool DaemonServer::_handle_command(
  MCommand *m,
  std::shared_ptr<CommandContext>& cmdctx)
{
  auto priv = m->get_connection()->get_priv();
  auto session = static_cast<MgrSession*>(priv.get());
  if (!session) {
    return true;
  }
  if (session->inst.name == entity_name_t())
    session->inst.name = m->get_source();

  std::string format;
  boost::scoped_ptr<Formatter> f;
  map<string,string> param_str_map;
  std::stringstream ss;
  int r = 0;

  if (!cmdmap_from_json(m->cmd, &(cmdctx->cmdmap), ss)) {
    cmdctx->reply(-EINVAL, ss);
    return true;
  }

  {
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "format", format, string("plain"));
    f.reset(Formatter::create(format));
  }

  string prefix;
  cmd_getval(cct, cmdctx->cmdmap, "prefix", prefix);

  dout(4) << "decoded " << cmdctx->cmdmap.size() << dendl;
  dout(4) << "prefix=" << prefix << dendl;

  if (prefix == "get_command_descriptions") {
    dout(10) << "reading commands from python modules" << dendl;
    const auto py_commands = py_modules.get_commands();

    int cmdnum = 0;
    JSONFormatter f;
    f.open_object_section("command_descriptions");

    auto dump_cmd = [&cmdnum, &f, m](const MonCommand &mc){
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmddesc_to_json(&f, m->get_connection()->get_features(),
                           secname.str(), mc.cmdstring, mc.helpstring,
                           mc.module, mc.req_perms, 0);
      cmdnum++;
    };

    for (const auto &pyc : py_commands) {
      dump_cmd(pyc);
    }

    for (const auto &mgr_cmd : mgr_commands) {
      dump_cmd(mgr_cmd);
    }

    f.close_section();	// command_descriptions
    f.flush(cmdctx->odata);
    cmdctx->reply(0, ss);
    return true;
  }

  // lookup command
  const MonCommand *mgr_cmd = _get_mgrcommand(prefix, mgr_commands);
  _generate_command_map(cmdctx->cmdmap, param_str_map);

  bool is_allowed;
  if (!mgr_cmd) {
    MonCommand py_command = {"", "", "py", "rw"};
    is_allowed = _allowed_command(session, py_command.module,
      prefix, cmdctx->cmdmap, param_str_map, &py_command);
  } else {
    // validate user's permissions for requested command
    is_allowed = _allowed_command(session, mgr_cmd->module,
      prefix, cmdctx->cmdmap,  param_str_map, mgr_cmd);
  }
  if (!is_allowed) {
      dout(1) << " access denied" << dendl;
      audit_clog->info() << "from='" << session->inst << "' "
                         << "entity='" << session->entity_name << "' "
                         << "cmd=" << m->cmd << ":  access denied";
      ss << "access denied: does your client key have mgr caps? "
            "See http://docs.ceph.com/docs/master/mgr/administrator/"
            "#client-authentication";
      cmdctx->reply(-EACCES, ss);
      return true;
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
    for (auto& p : pending_service_map.services) {
      f->open_object_section(p.first.c_str());
      for (auto& q : p.second.daemons) {
	f->open_object_section(q.first.c_str());
	DaemonKey key(p.first, q.first);
	ceph_assert(daemon_state.exists(key));
	auto daemon = daemon_state.get(key);
	std::lock_guard l(daemon->lock);
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

  if (prefix == "config set") {
    std::string key;
    std::string val;
    cmd_getval(cct, cmdctx->cmdmap, "key", key);
    cmd_getval(cct, cmdctx->cmdmap, "value", val);
    r = cct->_conf.set_val(key, val, &ss);
    if (r == 0) {
      cct->_conf.apply_changes(nullptr);
    }
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
    spg_t spgid;
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
      ss << "pg " << pgid << " does not exist";
      cmdctx->reply(-ENOENT, ss);
      return true;
    }
    int acting_primary = -1;
    epoch_t epoch;
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	epoch = osdmap.get_epoch();
	osdmap.get_primary_shard(pgid, &acting_primary, &spgid);
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
      return true;
    }
    for (auto& con : p->second) {
      if (HAVE_FEATURE(con->get_features(), SERVER_MIMIC)) {
	vector<spg_t> pgs = { spgid };
	con->send_message(new MOSDScrub2(monc->get_fsid(),
					 epoch,
					 pgs,
					 scrubop == "repair",
					 scrubop == "deep-scrub"));
      } else {
	vector<pg_t> pgs = { pgid };
	con->send_message(new MOSDScrub(monc->get_fsid(),
					pgs,
					scrubop == "repair",
					scrubop == "deep-scrub"));
      }
    }
    ss << "instructing pg " << spgid << " on osd." << acting_primary
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
      vector<spg_t> spgs;
      epoch_t epoch;
      cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pgmap) {
	  epoch = osdmap.get_epoch();
	  auto p = pgmap.pg_by_osd.find(osd);
	  if (p != pgmap.pg_by_osd.end()) {
	    for (auto pgid : p->second) {
	      int primary;
	      spg_t spg;
	      osdmap.get_primary_shard(pgid, &primary, &spg);
	      if (primary == osd) {
		spgs.push_back(spg);
	      }
	    }
	  }
	});
      auto p = osd_cons.find(osd);
      if (p == osd_cons.end()) {
	failed_osds.insert(osd);
      } else {
	sent_osds.insert(osd);
	for (auto& con : p->second) {
	  if (HAVE_FEATURE(con->get_features(), SERVER_MIMIC)) {
	    con->send_message(new MOSDScrub2(monc->get_fsid(),
					     epoch,
					     spgs,
					     pvec.back() == "repair",
					     pvec.back() == "deep-scrub"));
	  } else {
	    con->send_message(new MOSDScrub(monc->get_fsid(),
					    pvec.back() == "repair",
					    pvec.back() == "deep-scrub"));
	  }
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
    
    double max_change = g_conf().get_val<double>("mon_reweight_max_change");
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "max_change", max_change);
    if (max_change <= 0.0) {
      ss << "max_change " << max_change << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    int64_t max_osds = g_conf().get_val<int64_t>("mon_reweight_max_osds");
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "max_osds", max_osds);
    if (max_osds <= 0) {
      ss << "max_osds " << max_osds << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    bool no_increasing = false;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "no_increasing", no_increasing);
    string out_str;
    mempool::osdmap::map<int32_t, uint32_t> new_weights;
    r = cluster_state.with_osdmap_and_pgmap([&](const OSDMap &osdmap, const PGMap& pgmap) {
	return reweight::by_utilization(osdmap, pgmap,
					oload,
					max_change,
					max_osds,
					by_pg,
					pools.empty() ? NULL : &pools,
					no_increasing,
					&new_weights,
					&ss, &out_str, f.get());
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
    r = cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pgmap) {
	print_osd_utilization(osdmap, pgmap, ss,
			      f.get(), method == "tree");
	
	cmdctx->odata.append(ss);
	return 0;
      });
    cmdctx->reply(r, "");
    return true;
  } else if (prefix == "osd pool stats") {
    string pool_name;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "pool_name", pool_name);
    int64_t poolid = -ENOENT;
    bool one_pool = false;
    r = cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
        if (!pool_name.empty()) {
          poolid = osdmap.lookup_pg_pool_name(pool_name);
          if (poolid < 0) {
            ceph_assert(poolid == -ENOENT);
            ss << "unrecognized pool '" << pool_name << "'";
            return -ENOENT;
          }
          one_pool = true;
        }
        stringstream rs;
        if (f)
          f->open_array_section("pool_stats");
        else {
          if (osdmap.get_pools().empty()) {
            ss << "there are no pools!";
            goto stats_out;
          }
        }
        for (auto &p : osdmap.get_pools()) {
          if (!one_pool) {
            poolid = p.first;
          }
          pg_map.dump_pool_stats_and_io_rate(poolid, osdmap, f.get(), &rs); 
          if (one_pool) {
            break;
          }
        }
      stats_out:
        if (f) {
          f->close_section();
          f->flush(cmdctx->odata);
        } else {
          cmdctx->odata.append(rs.str());
        }
        return 0;
      });
    if (r != -EOPNOTSUPP) {
      cmdctx->reply(r, ss);
      return true;
    }
  } else if (prefix == "osd safe-to-destroy" ||
	     prefix == "osd destroy" ||
	     prefix == "osd purge") {
    set<int> osds;
    int r = 0;
    if (prefix == "osd safe-to-destroy") {
      vector<string> ids;
      cmd_getval(g_ceph_context, cmdctx->cmdmap, "ids", ids);
      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
				  r = osdmap.parse_osd_id_list(ids, &osds, &ss);
				});
      if (!r && osds.empty()) {
	ss << "must specify one or more OSDs";
	r = -EINVAL;
      }
    } else {
      int64_t id;
      if (!cmd_getval(g_ceph_context, cmdctx->cmdmap, "id", id)) {
	r = -EINVAL;
	ss << "must specify OSD id";
      } else {
	osds.insert(id);
      }
    }
    if (r < 0) {
      cmdctx->reply(r, ss);
      return true;
    }
    set<int> active_osds, missing_stats, stored_pgs, safe_to_destroy;
    int affected_pgs = 0;
    cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
	if (pg_map.num_pg_unknown > 0) {
	  ss << pg_map.num_pg_unknown << " pgs have unknown state; cannot draw"
	     << " any conclusions";
	  r = -EAGAIN;
	  return;
	}
	int num_active_clean = 0;
	for (auto& p : pg_map.num_pg_by_state) {
	  unsigned want = PG_STATE_ACTIVE|PG_STATE_CLEAN;
	  if ((p.first & want) == want) {
	    num_active_clean += p.second;
	  }
	}
	for (auto osd : osds) {
	  if (!osdmap.exists(osd)) {
	    safe_to_destroy.insert(osd);
	    continue;  // clearly safe to destroy
	  }
	  auto q = pg_map.num_pg_by_osd.find(osd);
	  if (q != pg_map.num_pg_by_osd.end()) {
	    if (q->second.acting > 0 || q->second.up > 0) {
	      active_osds.insert(osd);
	      affected_pgs += q->second.acting + q->second.up;
	      continue;
	    }
	  }
	  if (num_active_clean < pg_map.num_pg) {
	    // all pgs aren't active+clean; we need to be careful.
	    auto p = pg_map.osd_stat.find(osd);
	    if (p == pg_map.osd_stat.end()) {
	      missing_stats.insert(osd);
	      continue;
	    } else if (p->second.num_pgs > 0) {
	      stored_pgs.insert(osd);
	      continue;
	    }
	  }
	  safe_to_destroy.insert(osd);
	}
      });
    if (r && prefix == "osd safe-to-destroy") {
      cmdctx->reply(r, ss); // regardless of formatter
      return true;
    }
    if (!r && (!active_osds.empty() ||
               !missing_stats.empty() || !stored_pgs.empty())) {
       if (!safe_to_destroy.empty()) {
         ss << "OSD(s) " << safe_to_destroy
            << " are safe to destroy without reducing data durability. ";
       }
       if (!active_osds.empty()) {
         ss << "OSD(s) " << active_osds << " have " << affected_pgs
            << " pgs currently mapped to them. ";
       }
       if (!missing_stats.empty()) {
         ss << "OSD(s) " << missing_stats << " have no reported stats, and not all"
            << " PGs are active+clean; we cannot draw any conclusions. ";
       }
       if (!stored_pgs.empty()) {
         ss << "OSD(s) " << stored_pgs << " last reported they still store some PG"
            << " data, and not all PGs are active+clean; we cannot be sure they"
            << " aren't still needed.";
       }
       if (!active_osds.empty() || !stored_pgs.empty()) {
         r = -EBUSY;
       } else {
         r = -EAGAIN;
       }
    }

    if (prefix == "osd safe-to-destroy") {
      if (!r) {
        ss << "OSD(s) " << osds << " are safe to destroy without reducing data"
           << " durability.";
        safe_to_destroy.swap(osds);
      }
      if (f) {
        f->open_object_section("osd_status");
        f->open_array_section("safe_to_destroy");
        for (auto i : safe_to_destroy)
          f->dump_int("osd", i);
        f->close_section();
        f->open_array_section("active");
        for (auto i : active_osds)
          f->dump_int("osd", i);
        f->close_section();
        f->open_array_section("missing_stats");
        for (auto i : missing_stats)
          f->dump_int("osd", i);
        f->close_section();
        f->open_array_section("stored_pgs");
        for (auto i : stored_pgs)
          f->dump_int("osd", i);
        f->close_section();
        f->close_section(); // osd_status
        f->flush(cmdctx->odata);
        r = 0;
        std::stringstream().swap(ss);
      }
      cmdctx->reply(r, ss);
      return true;
    }

    if (r) {
      bool force = false;
      cmd_getval(cct, cmdctx->cmdmap, "force", force);
      if (!force) {
        // Backward compat
        cmd_getval(cct, cmdctx->cmdmap, "yes_i_really_mean_it", force);
      }
      if (!force) {
        ss << "\nYou can proceed by passing --force, but be warned that"
              " this will likely mean real, permanent data loss.";
      } else {
        r = 0;
      }
    }
    if (r) {
      cmdctx->reply(r, ss);
      return true;
    }
    const string cmd =
      "{"
      "\"prefix\": \"" + prefix + "-actual\", "
      "\"id\": " + stringify(osds) + ", "
      "\"yes_i_really_mean_it\": true"
      "}";
    auto on_finish = new ReplyOnFinish(cmdctx);
    monc->start_mon_command({cmd}, {}, nullptr, &on_finish->outs, on_finish);
    return true;
  } else if (prefix == "osd ok-to-stop") {
    vector<string> ids;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "ids", ids);
    set<int> osds;
    int r;
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	r = osdmap.parse_osd_id_list(ids, &osds, &ss);
      });
    if (!r && osds.empty()) {
      ss << "must specify one or more OSDs";
      r = -EINVAL;
    }
    if (r < 0) {
      cmdctx->reply(r, ss);
      return true;
    }
    map<pg_t,int> pg_delta;  // pgid -> net acting set size change
    int dangerous_pgs = 0;
    cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
	if (pg_map.num_pg_unknown > 0) {
	  ss << pg_map.num_pg_unknown << " pgs have unknown state; "
	     << "cannot draw any conclusions";
	  r = -EAGAIN;
	  return;
	}
	for (auto osd : osds) {
	  auto p = pg_map.pg_by_osd.find(osd);
	  if (p != pg_map.pg_by_osd.end()) {
	    for (auto& pgid : p->second) {
	      --pg_delta[pgid];
	    }
	  }
	}
	for (auto& p : pg_delta) {
	  auto q = pg_map.pg_stat.find(p.first);
	  if (q == pg_map.pg_stat.end()) {
	    ss << "missing information about " << p.first << "; cannot draw"
	       << " any conclusions";
	    r = -EAGAIN;
	    return;
	  }
	  if (!(q->second.state & PG_STATE_ACTIVE) ||
	      (q->second.state & PG_STATE_DEGRADED)) {
	    // we don't currently have a good way to tell *how* degraded
	    // a degraded PG is, so we have to assume we cannot remove
	    // any more replicas/shards.
	    ++dangerous_pgs;
	    continue;
	  }
	  const pg_pool_t *pi = osdmap.get_pg_pool(p.first.pool());
	  if (!pi) {
	    ++dangerous_pgs; // pool is creating or deleting
	  } else {
	    if (q->second.acting.size() + p.second < pi->min_size) {
	      ++dangerous_pgs;
	    }
	  }
	}
      });
    if (r) {
      cmdctx->reply(r, ss);
      return true;
    }
    if (dangerous_pgs) {
      ss << dangerous_pgs << " PGs are already degraded or might become "
	 << "unavailable";
      cmdctx->reply(-EBUSY, ss);
      return true;
    }
    ss << "OSD(s) " << osds << " are ok to stop without reducing"
       << " availability, provided there are no other concurrent failures"
       << " or interventions. " << pg_delta.size() << " PGs are likely to be"
       << " degraded (but remain available) as a result.";
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "pg force-recovery" ||
  	       prefix == "pg force-backfill" ||
  	       prefix == "pg cancel-force-recovery" ||
  	       prefix == "pg cancel-force-backfill") {
    string forceop = prefix.substr(3, string::npos);
    list<pg_t> parsed_pgs;

    // figure out actual op just once
    int actual_op = 0;
    if (forceop == "force-recovery") {
      actual_op = OFR_RECOVERY;
    } else if (forceop == "force-backfill") {
      actual_op = OFR_BACKFILL;
    } else if (forceop == "cancel-force-backfill") {
      actual_op = OFR_BACKFILL | OFR_CANCEL;
    } else if (forceop == "cancel-force-recovery") {
      actual_op = OFR_RECOVERY | OFR_CANCEL;
    }

    // covnert pg names to pgs, discard any invalid ones while at it
    {
      // we don't want to keep pgidstr and pgidstr_nodup forever
      vector<string> pgidstr;
      // get pgids to process and prune duplicates
      cmd_getval(g_ceph_context, cmdctx->cmdmap, "pgid", pgidstr);
      set<string> pgidstr_nodup(pgidstr.begin(), pgidstr.end());
      if (pgidstr.size() != pgidstr_nodup.size()) {
	// move elements only when there were duplicates, as this
	// reorders them
	pgidstr.resize(pgidstr_nodup.size());
	auto it = pgidstr_nodup.begin();
	for (size_t i = 0 ; i < pgidstr_nodup.size(); i++) {
	  pgidstr[i] = std::move(*it++);
	}
      }

      cluster_state.with_pgmap([&](const PGMap& pg_map) {
	for (auto& pstr : pgidstr) {
	  pg_t parsed_pg;
	  if (!parsed_pg.parse(pstr.c_str())) {
	    ss << "invalid pgid '" << pstr << "'; ";
	    r = -EINVAL;
	  } else {
	    auto workit = pg_map.pg_stat.find(parsed_pg);
	    if (workit == pg_map.pg_stat.end()) {
	      ss << "pg " << pstr << " does not exist; ";
	      r = -ENOENT;
	    } else {
	      pg_stat_t workpg = workit->second;

	      // discard pgs for which user requests are pointless
	      switch (actual_op)
	      {
		case OFR_RECOVERY:
		  if ((workpg.state & (PG_STATE_DEGRADED | PG_STATE_RECOVERY_WAIT | PG_STATE_RECOVERING)) == 0) {
		    // don't return error, user script may be racing with cluster. not fatal.
		    ss << "pg " << pstr << " doesn't require recovery; ";
		    continue;
		  } else  if (workpg.state & PG_STATE_FORCED_RECOVERY) {
		    ss << "pg " << pstr << " recovery already forced; ";
		    // return error, as it may be a bug in user script
		    r = -EINVAL;
		    continue;
		  }
		  break;
		case OFR_BACKFILL:
		  if ((workpg.state & (PG_STATE_DEGRADED | PG_STATE_BACKFILL_WAIT | PG_STATE_BACKFILLING)) == 0) {
		    ss << "pg " << pstr << " doesn't require backfilling; ";
		    continue;
		  } else  if (workpg.state & PG_STATE_FORCED_BACKFILL) {
		    ss << "pg " << pstr << " backfill already forced; ";
		    r = -EINVAL;
		    continue;
		  }
		  break;
		case OFR_BACKFILL | OFR_CANCEL:
		  if ((workpg.state & PG_STATE_FORCED_BACKFILL) == 0) {
		    ss << "pg " << pstr << " backfill not forced; ";
		    continue;
		  }
		  break;
		case OFR_RECOVERY | OFR_CANCEL:
		  if ((workpg.state & PG_STATE_FORCED_RECOVERY) == 0) {
		    ss << "pg " << pstr << " recovery not forced; ";
		    continue;
		  }
		  break;
		default:
		  ceph_abort_msg("actual_op value is not supported");
	      }

	      parsed_pgs.push_back(std::move(parsed_pg));
	    }
	  }
	}
      });
    }

    // respond with error only when no pgs are correct
    // yes, in case of mixed errors, only the last one will be emitted,
    // but the message presented will be fine
    if (parsed_pgs.size() != 0) {
      // clear error to not confuse users/scripts
      r = 0;
    }

    // optimize the command -> messages conversion, use only one
    // message per distinct OSD
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	// group pgs to process by osd
	map<int, vector<spg_t>> osdpgs;
	for (auto& pgid : parsed_pgs) {
	  int primary;
	  spg_t spg;
	  if (osdmap.get_primary_shard(pgid, &primary, &spg)) {
	    osdpgs[primary].push_back(spg);
	  }
	}
	for (auto& i : osdpgs) {
	  if (osdmap.is_up(i.first)) {
	    auto p = osd_cons.find(i.first);
	    if (p == osd_cons.end()) {
	      ss << "osd." << i.first << " is not currently connected";
	      r = -EAGAIN;
	      continue;
	    }
	    for (auto& con : p->second) {
	      con->send_message(
		new MOSDForceRecovery(monc->get_fsid(), i.second, actual_op));
	    }
	    ss << "instructing pg(s) " << i.second << " on osd." << i.first
	       << " to " << forceop << "; ";
	  }
	}
      });
    ss << std::endl;
    cmdctx->reply(r, ss);
    return true;
  } else if (prefix == "config show" ||
	     prefix == "config show-with-defaults") {
    string who;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "who", who);
    int r = 0;
    auto dot = who.find('.');
    DaemonKey key;
    key.first = who.substr(0, dot);
    key.second = who.substr(dot + 1);
    DaemonStatePtr daemon = daemon_state.get(key);
    string name;
    if (!daemon) {
      ss << "no config state for daemon " << who;
      cmdctx->reply(-ENOENT, ss);
      return true;
    }

    std::lock_guard l(daemon->lock);

    if (cmd_getval(g_ceph_context, cmdctx->cmdmap, "key", name)) {
      auto p = daemon->config.find(name);
      if (p != daemon->config.end() &&
	  !p->second.empty()) {
	cmdctx->odata.append(p->second.rbegin()->second + "\n");
      } else {
	auto& defaults = daemon->_get_config_defaults();
	auto q = defaults.find(name);
	if (q != defaults.end()) {
	  cmdctx->odata.append(q->second + "\n");
	} else {
	  r = -ENOENT;
	}
      }
    } else if (daemon->config_defaults_bl.length() > 0) {
      TextTable tbl;
      if (f) {
	f->open_array_section("config");
      } else {
	tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("VALUE", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("SOURCE", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("OVERRIDES", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("IGNORES", TextTable::LEFT, TextTable::LEFT);
      }
      if (prefix == "config show") {
	// show
	for (auto& i : daemon->config) {
	  dout(20) << " " << i.first << " -> " << i.second << dendl;
	  if (i.second.empty()) {
	    continue;
	  }
	  if (f) {
	    f->open_object_section("value");
	    f->dump_string("name", i.first);
	    f->dump_string("value", i.second.rbegin()->second);
	    f->dump_string("source", ceph_conf_level_name(
			     i.second.rbegin()->first));
	    if (i.second.size() > 1) {
	      f->open_array_section("overrides");
	      auto j = i.second.rend();
	      for (--j; j != i.second.rbegin(); --j) {
		f->open_object_section("value");
		f->dump_string("source", ceph_conf_level_name(j->first));
		f->dump_string("value", j->second);
		f->close_section();
	      }
	      f->close_section();
	    }
	    if (daemon->ignored_mon_config.count(i.first)) {
	      f->dump_string("ignores", "mon");
	    }
	    f->close_section();
	  } else {
	    tbl << i.first;
	    tbl << i.second.rbegin()->second;
	    tbl << ceph_conf_level_name(i.second.rbegin()->first);
	    if (i.second.size() > 1) {
	      list<string> ov;
	      auto j = i.second.rend();
	      for (--j; j != i.second.rbegin(); --j) {
		if (j->second == i.second.rbegin()->second) {
		  ov.push_front(string("(") + ceph_conf_level_name(j->first) +
				string("[") + j->second + string("]") +
				string(")"));
		} else {
                  ov.push_front(ceph_conf_level_name(j->first) +
                                string("[") + j->second + string("]"));

		}
	      }
	      tbl << ov;
	    } else {
	      tbl << "";
	    }
	    tbl << (daemon->ignored_mon_config.count(i.first) ? "mon" : "");
	    tbl << TextTable::endrow;
	  }
	}
      } else {
	// show-with-defaults
	auto& defaults = daemon->_get_config_defaults();
	for (auto& i : defaults) {
	  if (f) {
	    f->open_object_section("value");
	    f->dump_string("name", i.first);
	  } else {
	    tbl << i.first;
	  }
	  auto j = daemon->config.find(i.first);
	  if (j != daemon->config.end() && !j->second.empty()) {
	    // have config
	    if (f) {
	      f->dump_string("value", j->second.rbegin()->second);
	      f->dump_string("source", ceph_conf_level_name(
			       j->second.rbegin()->first));
	      if (j->second.size() > 1) {
		f->open_array_section("overrides");
		auto k = j->second.rend();
		for (--k; k != j->second.rbegin(); --k) {
		  f->open_object_section("value");
		  f->dump_string("source", ceph_conf_level_name(k->first));
		  f->dump_string("value", k->second);
		  f->close_section();
		}
		f->close_section();
	      }
	      if (daemon->ignored_mon_config.count(i.first)) {
		f->dump_string("ignores", "mon");
	      }
	      f->close_section();
	    } else {
	      tbl << j->second.rbegin()->second;
	      tbl << ceph_conf_level_name(j->second.rbegin()->first);
	      if (j->second.size() > 1) {
		list<string> ov;
		auto k = j->second.rend();
		for (--k; k != j->second.rbegin(); --k) {
		  if (k->second == j->second.rbegin()->second) {
		    ov.push_front(string("(") + ceph_conf_level_name(k->first) +
				  string("[") + k->second + string("]") +
				  string(")"));
		  } else {
                    ov.push_front(ceph_conf_level_name(k->first) +
                                  string("[") + k->second + string("]"));
		  }
		}
		tbl << ov;
	      } else {
		tbl << "";
	      }
	      tbl << (daemon->ignored_mon_config.count(i.first) ? "mon" : "");
	      tbl << TextTable::endrow;
	    }
	  } else {
	    // only have default
	    if (f) {
	      f->dump_string("value", i.second);
	      f->dump_string("source", ceph_conf_level_name(CONF_DEFAULT));
	      f->close_section();
	    } else {
	      tbl << i.second;
	      tbl << ceph_conf_level_name(CONF_DEFAULT);
	      tbl << "";
	      tbl << "";
	      tbl << TextTable::endrow;
	    }
	  }
	}
      }
      if (f) {
	f->close_section();
	f->flush(cmdctx->odata);
      } else {
	cmdctx->odata.append(stringify(tbl));
      }
    }
    cmdctx->reply(r, ss);
    return true;
  } else if (prefix == "device ls") {
    set<string> devids;
    TextTable tbl;
    if (f) {
      f->open_array_section("devices");
      daemon_state.with_devices([&f](const DeviceState& dev) {
	  f->dump_object("device", dev);
	});
      f->close_section();
      f->flush(cmdctx->odata);
    } else {
      tbl.define_column("DEVICE", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("HOST:DEV", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("DAEMONS", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("LIFE EXPECTANCY", TextTable::LEFT, TextTable::LEFT);
      auto now = ceph_clock_now();
      daemon_state.with_devices([&tbl, now](const DeviceState& dev) {
	  string h;
	  for (auto& i : dev.devnames) {
	    if (h.size()) {
	      h += " ";
	    }
	    h += i.first + ":" + i.second;
	  }
	  string d;
	  for (auto& i : dev.daemons) {
	    if (d.size()) {
	      d += " ";
	    }
	    d += to_string(i);
	  }
	  tbl << dev.devid
	      << h
	      << d
	      << dev.get_life_expectancy_str(now)
	      << TextTable::endrow;
	});
      cmdctx->odata.append(stringify(tbl));
    }
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "device ls-by-daemon") {
    string who;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "who", who);
    DaemonKey k;
    if (!key_from_string(who, &k)) {
      ss << who << " is not a valid daemon name";
      r = -EINVAL;
    } else {
      auto dm = daemon_state.get(k);
      if (dm) {
	if (f) {
	  f->open_array_section("devices");
	  for (auto& i : dm->devices) {
	    daemon_state.with_device(i.first, [&f] (const DeviceState& dev) {
		f->dump_object("device", dev);
	      });
	  }
	  f->close_section();
	  f->flush(cmdctx->odata);
	} else {
	  TextTable tbl;
	  tbl.define_column("DEVICE", TextTable::LEFT, TextTable::LEFT);
	  tbl.define_column("HOST:DEV", TextTable::LEFT, TextTable::LEFT);
	  tbl.define_column("EXPECTED FAILURE", TextTable::LEFT,
			    TextTable::LEFT);
	  auto now = ceph_clock_now();
	  for (auto& i : dm->devices) {
	    daemon_state.with_device(
	      i.first, [&tbl, now] (const DeviceState& dev) {
		string h;
		for (auto& i : dev.devnames) {
		  if (h.size()) {
		    h += " ";
		  }
		  h += i.first + ":" + i.second;
		}
		tbl << dev.devid
		    << h
		    << dev.get_life_expectancy_str(now)
		    << TextTable::endrow;
	      });
	  }
	  cmdctx->odata.append(stringify(tbl));
	}
      } else {
	r = -ENOENT;
	ss << "daemon " << who << " not found";
      }
      cmdctx->reply(r, ss);
    }
  } else if (prefix == "device ls-by-host") {
    string host;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "host", host);
    set<string> devids;
    daemon_state.list_devids_by_server(host, &devids);
    if (f) {
      f->open_array_section("devices");
      for (auto& devid : devids) {
	daemon_state.with_device(
	  devid, [&f] (const DeviceState& dev) {
	    f->dump_object("device", dev);
	  });
      }
      f->close_section();
      f->flush(cmdctx->odata);
    } else {
      TextTable tbl;
      tbl.define_column("DEVICE", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("DEV", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("DAEMONS", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("EXPECTED FAILURE", TextTable::LEFT, TextTable::LEFT);
      auto now = ceph_clock_now();
      for (auto& devid : devids) {
	daemon_state.with_device(
	  devid, [&tbl, &host, now] (const DeviceState& dev) {
	    string n;
	    for (auto& j : dev.devnames) {
	      if (j.first == host) {
		if (n.size()) {
		  n += " ";
		}
		n += j.second;
	      }
	    }
	    string d;
	    for (auto& i : dev.daemons) {
	      if (d.size()) {
		d += " ";
	      }
	      d += to_string(i);
	    }
	    tbl << dev.devid
		<< n
		<< d
		<< dev.get_life_expectancy_str(now)
		<< TextTable::endrow;
	  });
      }
      cmdctx->odata.append(stringify(tbl));
    }
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "device info") {
    string devid;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "devid", devid);
    int r = 0;
    ostringstream rs;
    if (!daemon_state.with_device(devid,
				  [&f, &rs] (const DeviceState& dev) {
	  if (f) {
	    f->dump_object("device", dev);
	  } else {
	    dev.print(rs);
	  }
	})) {
      ss << "device " << devid << " not found";
      r = -ENOENT;
    } else {
      if (f) {
	f->flush(cmdctx->odata);
      } else {
	cmdctx->odata.append(rs.str());
      }
    }
    cmdctx->reply(r, ss);
    return true;
  } else if (prefix == "device set-life-expectancy") {
    string devid;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "devid", devid);
    string from_str, to_str;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "from", from_str);
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "to", to_str);
    utime_t from, to;
    if (!from.parse(from_str)) {
      ss << "unable to parse datetime '" << from_str << "'";
      r = -EINVAL;
      cmdctx->reply(r, ss);
    } else if (to_str.size() && !to.parse(to_str)) {
      ss << "unable to parse datetime '" << to_str << "'";
      r = -EINVAL;
      cmdctx->reply(r, ss);
    } else {
      map<string,string> meta;
      daemon_state.with_device_create(
	devid,
	[from, to, &meta] (DeviceState& dev) {
	  dev.set_life_expectancy(from, to, ceph_clock_now());
	  meta = dev.metadata;
	});
      json_spirit::Object json_object;
      for (auto& i : meta) {
	json_spirit::Config::add(json_object, i.first, i.second);
      }
      bufferlist json;
      json.append(json_spirit::write(json_object));
      const string cmd =
	"{"
	"\"prefix\": \"config-key set\", "
	"\"key\": \"device/" + devid + "\""
	"}";
      auto on_finish = new ReplyOnFinish(cmdctx);
      monc->start_mon_command({cmd}, json, nullptr, nullptr, on_finish);
    }
    return true;
  } else if (prefix == "device rm-life-expectancy") {
    string devid;
    cmd_getval(g_ceph_context, cmdctx->cmdmap, "devid", devid);
    map<string,string> meta;
    if (daemon_state.with_device_write(devid, [&meta] (DeviceState& dev) {
	  dev.rm_life_expectancy();
	  meta = dev.metadata;
	})) {
      string cmd;
      bufferlist json;
      if (meta.empty()) {
	cmd =
	  "{"
	  "\"prefix\": \"config-key rm\", "
	  "\"key\": \"device/" + devid + "\""
	  "}";
      } else {
	json_spirit::Object json_object;
	for (auto& i : meta) {
	  json_spirit::Config::add(json_object, i.first, i.second);
	}
	json.append(json_spirit::write(json_object));
	cmd =
	  "{"
	  "\"prefix\": \"config-key set\", "
	  "\"key\": \"device/" + devid + "\""
	  "}";
      }
      auto on_finish = new ReplyOnFinish(cmdctx);
      monc->start_mon_command({cmd}, json, nullptr, nullptr, on_finish);
    } else {
      cmdctx->reply(0, ss);
    }
    return true;
  } else {
    if (!pgmap_ready) {
      ss << "Warning: due to ceph-mgr restart, some PG states may not be up to date\n";
    }
    if (f) {
       f->open_object_section("pg_info");
       f->dump_bool("pg_ready", pgmap_ready);
    }

    // fall back to feeding command to PGMap
    r = cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
	return process_pg_map_command(prefix, cmdctx->cmdmap, pg_map, osdmap,
				      f.get(), &ss, &cmdctx->odata);
      });

    if (f) {
      f->close_section();
    }
    if (r != -EOPNOTSUPP) {
      if (f) {
	f->flush(cmdctx->odata);
      }
      cmdctx->reply(r, ss);
      return true;
    }
  }

  // Resolve the command to the name of the module that will
  // handle it (if the command exists)
  std::string handler_name;
  auto py_commands = py_modules.get_py_commands();
  for (const auto &pyc : py_commands) {
    auto pyc_prefix = cmddesc_get_prefix(pyc.cmdstring);
    if (pyc_prefix == prefix) {
      handler_name = pyc.module_name;
      break;
    }
  }

  // Was the command unfound?
  if (handler_name.empty()) {
    ss << "No handler found for '" << prefix << "'";
    dout(4) << "No handler found for '" << prefix << "'" << dendl;
    cmdctx->reply(-EINVAL, ss);
    return true;
  }

  dout(4) << "passing through " << cmdctx->cmdmap.size() << dendl;
  finisher.queue(new FunctionContext([this, cmdctx, handler_name, prefix](int r_) {
    std::stringstream ss;

    // Validate that the module is enabled
    PyModuleRef module = py_modules.get_module(handler_name);
    ceph_assert(module);
    if (!module->is_enabled()) {
      ss << "Module '" << handler_name << "' is not enabled (required by "
            "command '" << prefix << "'): use `ceph mgr module enable "
            << handler_name << "` to enable it";
      dout(4) << ss.str() << dendl;
      cmdctx->reply(-EOPNOTSUPP, ss);
      return;
    }

    // Hack: allow the self-test method to run on unhealthy modules.
    // Fix this in future by creating a special path for self test rather
    // than having the hook be a normal module command.
    std::string self_test_prefix = handler_name + " " + "self-test";

    // Validate that the module is healthy
    bool accept_command;
    if (module->is_loaded()) {
      if (module->get_can_run() && !module->is_failed()) {
        // Healthy module
        accept_command = true;
      } else if (self_test_prefix == prefix) {
        // Unhealthy, but allow because it's a self test command
        accept_command = true;
      } else {
        accept_command = false;
        ss << "Module '" << handler_name << "' has experienced an error and "
              "cannot handle commands: " << module->get_error_string();
      }
    } else {
      // Module not loaded
      accept_command = false;
      ss << "Module '" << handler_name << "' failed to load and "
            "cannot handle commands: " << module->get_error_string();
    }

    if (!accept_command) {
      dout(4) << ss.str() << dendl;
      cmdctx->reply(-EIO, ss);
      return;
    }

    std::stringstream ds;
    bufferlist inbl = cmdctx->m->get_data();
    int r = py_modules.handle_command(handler_name, cmdctx->cmdmap, inbl, &ds, &ss);
    cmdctx->odata.append(ds);
    cmdctx->reply(r, ss);
  }));
  return true;
}

void DaemonServer::_prune_pending_service_map()
{
  utime_t cutoff = ceph_clock_now();
  cutoff -= g_conf().get_val<double>("mgr_service_beacon_grace");
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
      std::lock_guard l(daemon->lock);
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
    if (ceph_clock_now() - started_at > g_conf().get_val<int64_t>("mgr_stats_period") * 4.0) {
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
  py_modules.get_health_checks(&m->health_checks);
  py_modules.get_progress_events(&m->progress_events);

  cluster_state.with_mutable_pgmap([&](PGMap& pg_map) {
      cluster_state.update_delta_stats();

      if (pending_service_map.epoch) {
	_prune_pending_service_map();
	if (pending_service_map_dirty >= pending_service_map.epoch) {
	  pending_service_map.modified = ceph_clock_now();
	  encode(pending_service_map, m->service_map_bl, CEPH_FEATURES_ALL);
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
          if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
              clog->debug() << "pgmap v" << pg_map.version << ": " << pg_map;
          }
	});
    });

  map<daemon_metric, unique_ptr<DaemonHealthMetricCollector>> accumulated;
  for (auto service : {"osd", "mon"} ) {
    auto daemons = daemon_state.get_by_service(service);
    for (const auto& [key,state] : daemons) {
      std::lock_guard l{state->lock};
      for (const auto& metric : state->daemon_health_metrics) {
        auto acc = accumulated.find(metric.get_type());
        if (acc == accumulated.end()) {
          auto collector = DaemonHealthMetricCollector::create(metric.get_type());
          if (!collector) {
            derr << __func__ << " " << key.first << "." << key.second
		 << " sent me an unknown health metric: "
		 << std::hex << static_cast<uint8_t>(metric.get_type())
		 << std::dec << dendl;
            continue;
          }
	  dout(20) << " + " << state->key << " "
		   << metric << dendl;
          tie(acc, std::ignore) = accumulated.emplace(metric.get_type(),
              std::move(collector));
        }
        acc->second->update(key, metric);
      }
    }
  }
  for (const auto& acc : accumulated) {
    acc.second->summarize(m->health_checks);
  }
  // TODO? We currently do not notify the PyModules
  // TODO: respect needs_send, so we send the report only if we are asked to do
  //       so, or the state is updated.
  monc->send_mon_message(m);
}

void DaemonServer::adjust_pgs()
{
  dout(20) << dendl;
  unsigned max = std::max<int64_t>(1, g_conf()->mon_osd_max_creating_pgs);
  double max_misplaced = g_conf().get_val<double>("target_max_misplaced_ratio");
  bool aggro = g_conf().get_val<bool>("mgr_debug_aggressive_pg_num_changes");

  map<string,unsigned> pg_num_to_set;
  map<string,unsigned> pgp_num_to_set;
  set<pg_t> upmaps_to_clear;
  cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
      unsigned creating_or_unknown = 0;
      for (auto& i : pg_map.num_pg_by_state) {
	if ((i.first & (PG_STATE_CREATING)) ||
	    i.first == 0) {
	  creating_or_unknown += i.second;
	}
      }
      unsigned left = max;
      if (creating_or_unknown >= max) {
	return;
      }
      left -= creating_or_unknown;
      dout(10) << "creating_or_unknown " << creating_or_unknown
	       << " max_creating " << max
               << " left " << left
               << dendl;

      // FIXME: These checks are fundamentally racy given that adjust_pgs()
      // can run more frequently than we get updated pg stats from OSDs.  We
      // may make multiple adjustments with stale informaiton.
      double misplaced_ratio, degraded_ratio;
      double inactive_pgs_ratio, unknown_pgs_ratio;
      pg_map.get_recovery_stats(&misplaced_ratio, &degraded_ratio,
				&inactive_pgs_ratio, &unknown_pgs_ratio);
      dout(20) << "misplaced_ratio " << misplaced_ratio
	       << " degraded_ratio " << degraded_ratio
	       << " inactive_pgs_ratio " << inactive_pgs_ratio
	       << " unknown_pgs_ratio " << unknown_pgs_ratio
	       << "; target_max_misplaced_ratio " << max_misplaced
	       << dendl;

      for (auto& i : osdmap.get_pools()) {
	const pg_pool_t& p = i.second;

	// adjust pg_num?
	if (p.get_pg_num_target() != p.get_pg_num()) {
	  dout(20) << "pool " << i.first
		   << " pg_num " << p.get_pg_num()
		   << " target " << p.get_pg_num_target()
		   << dendl;
	  if (p.has_flag(pg_pool_t::FLAG_CREATING)) {
	    dout(10) << "pool " << i.first
		     << " pg_num_target " << p.get_pg_num_target()
		     << " pg_num " << p.get_pg_num()
		     << " - still creating initial pgs"
		     << dendl;
	  } else if (p.get_pg_num_target() < p.get_pg_num()) {
	    // pg_num decrease (merge)
	    pg_t merge_source(p.get_pg_num() - 1, i.first);
	    pg_t merge_target = merge_source.get_parent();
	    bool ok = true;

	    if (p.get_pg_num() != p.get_pg_num_pending()) {
	      dout(10) << "pool " << i.first
		       << " pg_num_target " << p.get_pg_num_target()
		       << " pg_num " << p.get_pg_num()
		       << " - decrease and pg_num_pending != pg_num, waiting"
		       << dendl;
	      ok = false;
	    } else if (p.get_pg_num() == p.get_pgp_num()) {
	      dout(10) << "pool " << i.first
		       << " pg_num_target " << p.get_pg_num_target()
		       << " pg_num " << p.get_pg_num()
		       << " - decrease blocked by pgp_num "
		       << p.get_pgp_num()
		       << dendl;
	      ok = false;
	    }
            for (auto &merge_participant : {merge_source, merge_target}) {
              bool is_merge_source = merge_participant == merge_source;
              if (osdmap.have_pg_upmaps(merge_participant)) {
                dout(10) << "pool " << i.first
                         << " pg_num_target " << p.get_pg_num_target()
                         << " pg_num " << p.get_pg_num()
                         << (is_merge_source ? " - merge source " : " - merge target ")
                         << merge_participant
                         << " has upmap" << dendl;
                upmaps_to_clear.insert(merge_participant);
                ok = false;
              }
              auto q = pg_map.pg_stat.find(merge_participant);
              if (q == pg_map.pg_stat.end()) {
	        dout(10) << "pool " << i.first
		         << " pg_num_target " << p.get_pg_num_target()
		         << " pg_num " << p.get_pg_num()
		         << " - no state for " << merge_participant
		         << (is_merge_source ? " (merge source)" : " (merge target)")
		         << dendl;
	        ok = false;
	      } else if ((q->second.state & (PG_STATE_ACTIVE | PG_STATE_CLEAN)) !=
                                            (PG_STATE_ACTIVE | PG_STATE_CLEAN)) {
	        dout(10) << "pool " << i.first
		         << " pg_num_target " << p.get_pg_num_target()
		         << " pg_num " << p.get_pg_num()
		         << (is_merge_source ? " - merge source " : " - merge target ")
                         << merge_participant
		         << " not clean (" << pg_state_string(q->second.state)
		         << ")" << dendl;
	        ok = false;
	      }
            }

	    if (ok) {
	      unsigned target = p.get_pg_num() - 1;
	      dout(10) << "pool " << i.first
		       << " pg_num_target " << p.get_pg_num_target()
		       << " pg_num " << p.get_pg_num()
		       << " -> " << target
		       << " (merging " << merge_source
		       << " and " << merge_target
		       << ")" << dendl;
	      pg_num_to_set[osdmap.get_pool_name(i.first)] = target;
	    }
	  } else if (p.get_pg_num_target() > p.get_pg_num()) {
	    // pg_num increase (split)
	    bool active = true;
	    auto q = pg_map.num_pg_by_pool_state.find(i.first);
	    if (q != pg_map.num_pg_by_pool_state.end()) {
	      for (auto& j : q->second) {
		if ((j.first & (PG_STATE_ACTIVE|PG_STATE_PEERED)) == 0) {
		  dout(20) << "pool " << i.first << " has " << j.second
			   << " pgs in " << pg_state_string(j.first)
			   << dendl;
		  active = false;
		  break;
		}
	      }
	    } else {
	      active = false;
	    }
	    if (!active) {
	      dout(10) << "pool " << i.first
		       << " pg_num_target " << p.get_pg_num_target()
		       << " pg_num " << p.get_pg_num()
		       << " - not all pgs active"
		       << dendl;
	    } else {
	      unsigned add = std::min(
		left,
		p.get_pg_num_target() - p.get_pg_num());
	      unsigned target = p.get_pg_num() + add;
	      left -= add;
	      dout(10) << "pool " << i.first
		       << " pg_num_target " << p.get_pg_num_target()
		       << " pg_num " << p.get_pg_num()
		       << " -> " << target << dendl;
	      pg_num_to_set[osdmap.get_pool_name(i.first)] = target;
	    }
	  }
	}

	// adjust pgp_num?
	unsigned target = std::min(p.get_pg_num_pending(),
				   p.get_pgp_num_target());
	if (target != p.get_pgp_num()) {
	  dout(20) << "pool " << i.first
		   << " pgp_num_target " << p.get_pgp_num_target()
		   << " pgp_num " << p.get_pgp_num()
		   << " -> " << target << dendl;
	  if (target > p.get_pgp_num() &&
	      p.get_pgp_num() == p.get_pg_num()) {
	    dout(10) << "pool " << i.first
		     << " pgp_num_target " << p.get_pgp_num_target()
		     << " pgp_num " << p.get_pgp_num()
		     << " - increase blocked by pg_num " << p.get_pg_num()
		     << dendl;
	  } else if (!aggro && (inactive_pgs_ratio > 0 ||
				degraded_ratio > 0 ||
				unknown_pgs_ratio > 0)) {
	    dout(10) << "pool " << i.first
		     << " pgp_num_target " << p.get_pgp_num_target()
		     << " pgp_num " << p.get_pgp_num()
		     << " - inactive|degraded|unknown pgs, deferring pgp_num"
		     << " update" << dendl;
	  } else if (!aggro && (misplaced_ratio > max_misplaced)) {
	    dout(10) << "pool " << i.first
		     << " pgp_num_target " << p.get_pgp_num_target()
		     << " pgp_num " << p.get_pgp_num()
		     << " - misplaced_ratio " << misplaced_ratio
		     << " > max " << max_misplaced
		     << ", deferring pgp_num update" << dendl;
	  } else {
	    // NOTE: this calculation assumes objects are
	    // basically uniformly distributed across all PGs
	    // (regardless of pool), which is probably not
	    // perfectly correct, but it's a start.  make no
	    // single adjustment that's more than half of the
	    // max_misplaced, to somewhat limit the magnitude of
	    // our potential error here.
	    int next;
	    if (aggro) {
	      next = target;
	    } else {
	      double room =
		std::min<double>(max_misplaced - misplaced_ratio,
				 misplaced_ratio / 2.0);
	      unsigned estmax = std::max<unsigned>(
		(double)p.get_pg_num() * room, 1u);
	      int delta = target - p.get_pgp_num();
	      next = p.get_pgp_num();
	      if (delta < 0) {
		next += std::max<int>(-estmax, delta);
	      } else {
		next += std::min<int>(estmax, delta);
	      }
	      dout(20) << " room " << room << " estmax " << estmax
		       << " delta " << delta << " next " << next << dendl;
	    }
	    dout(10) << "pool " << i.first
		     << " pgp_num_target " << p.get_pgp_num_target()
		     << " pgp_num " << p.get_pgp_num()
		     << " -> " << next << dendl;
	    pgp_num_to_set[osdmap.get_pool_name(i.first)] = next;
	  }
	}
	if (left == 0) {
	  return;
	}
      }
    });
  for (auto i : pg_num_to_set) {
    const string cmd =
      "{"
      "\"prefix\": \"osd pool set\", "
      "\"pool\": \"" + i.first + "\", "
      "\"var\": \"pg_num_actual\", "
      "\"val\": \"" + stringify(i.second) + "\""
      "}";
    monc->start_mon_command({cmd}, {}, nullptr, nullptr, nullptr);
  }
  for (auto i : pgp_num_to_set) {
    const string cmd =
      "{"
      "\"prefix\": \"osd pool set\", "
      "\"pool\": \"" + i.first + "\", "
      "\"var\": \"pgp_num_actual\", "
      "\"val\": \"" + stringify(i.second) + "\""
      "}";
    monc->start_mon_command({cmd}, {}, nullptr, nullptr, nullptr);
  }
  for (auto pg : upmaps_to_clear) {
    const string cmd =
      "{"
      "\"prefix\": \"osd rm-pg-upmap\", "
      "\"pgid\": \"" + stringify(pg) + "\""
      "}";
    monc->start_mon_command({cmd}, {}, nullptr, nullptr, nullptr);
    const string cmd2 =
      "{"
      "\"prefix\": \"osd rm-pg-upmap-items\", "
      "\"pgid\": \"" + stringify(pg) + "\"" +
      "}";
    monc->start_mon_command({cmd2}, {}, nullptr, nullptr, nullptr);
  }
}

void DaemonServer::got_service_map()
{
  std::lock_guard l(lock);

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
	daemon->set_metadata(q.second.metadata);
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

void DaemonServer::got_mgr_map()
{
  std::lock_guard l(lock);
  set<std::string> have;
  cluster_state.with_mgrmap([&](const MgrMap& mgrmap) {
      auto md_update = [&] (DaemonKey key) {
        std::ostringstream oss;
        auto c = new MetadataUpdate(daemon_state, key);
	// FIXME remove post-nautilus: include 'id' for luminous mons
        oss << "{\"prefix\": \"mgr metadata\", \"who\": \""
	    << key.second << "\", \"id\": \"" << key.second << "\"}";
        monc->start_mon_command({oss.str()}, {}, &c->outbl, &c->outs, c);
      };
      if (mgrmap.active_name.size()) {
	DaemonKey key("mgr", mgrmap.active_name);
	have.insert(mgrmap.active_name);
	if (!daemon_state.exists(key) && !daemon_state.is_updating(key)) {
	  md_update(key);
	  dout(10) << "triggered addition of " << key << " via metadata update" << dendl;
	}
      }
      for (auto& i : mgrmap.standbys) {
        DaemonKey key("mgr", i.second.name);
	have.insert(i.second.name);
	if (!daemon_state.exists(key) && !daemon_state.is_updating(key)) {
	  md_update(key);
	  dout(10) << "triggered addition of " << key << " via metadata update" << dendl;
	}
      }
    });
  daemon_state.cull("mgr", have);
}

const char** DaemonServer::get_tracked_conf_keys() const
{
  static const char *KEYS[] = {
    "mgr_stats_threshold",
    "mgr_stats_period",
    nullptr
  };

  return KEYS;
}

void DaemonServer::handle_conf_change(const ConfigProxy& conf,
				      const std::set <std::string> &changed)
{
  // We may be called within lock (via MCommand `config set`) or outwith the
  // lock (via admin socket `config set`), so handle either case.
  const bool initially_locked = lock.is_locked_by_me();
  if (!initially_locked) {
    lock.Lock();
  }

  if (changed.count("mgr_stats_threshold") || changed.count("mgr_stats_period")) {
    dout(4) << "Updating stats threshold/period on "
            << daemon_connections.size() << " clients" << dendl;
    // Send a fresh MMgrConfigure to all clients, so that they can follow
    // the new policy for transmitting stats
    for (auto &c : daemon_connections) {
      _send_configure(c);
    }
  }
}

void DaemonServer::_send_configure(ConnectionRef c)
{
  ceph_assert(lock.is_locked_by_me());

  auto configure = new MMgrConfigure();
  configure->stats_period = g_conf().get_val<int64_t>("mgr_stats_period");
  configure->stats_threshold = g_conf().get_val<int64_t>("mgr_stats_threshold");

  if (c->peer_is_osd()) {
    configure->osd_perf_metric_queries =
        osd_perf_metric_collector.get_queries();
  }

  c->send_message(configure);
}

OSDPerfMetricQueryID DaemonServer::add_osd_perf_query(
    const OSDPerfMetricQuery &query,
    const std::optional<OSDPerfMetricLimit> &limit)
{
  return osd_perf_metric_collector.add_query(query, limit);
}

int DaemonServer::remove_osd_perf_query(OSDPerfMetricQueryID query_id)
{
  return osd_perf_metric_collector.remove_query(query_id);
}

int DaemonServer::get_osd_perf_counters(
    OSDPerfMetricQueryID query_id,
    std::map<OSDPerfMetricKey, PerformanceCounters> *counters)
{
  return osd_perf_metric_collector.get_counters(query_id, counters);
}
