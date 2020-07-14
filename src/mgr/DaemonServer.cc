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
#include "mgr/MDSPerfMetricCollector.h"
#include "mon/MonCommand.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMonMgrReport.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MMgrCommand.h"
#include "messages/MMgrCommandReply.h"
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
using namespace TOPNSPC::common;
namespace {
  template <typename Map>
  bool map_compare(Map const &lhs, Map const &rhs) {
    return lhs.size() == rhs.size()
      && std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                    [] (auto a, auto b) { return a.first == b.first && a.second == b.second; });
  }
}

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
      pgmap_ready(false),
      timer(g_ceph_context, lock),
      shutting_down(false),
      tick_event(nullptr),
      osd_perf_metric_collector_listener(this),
      osd_perf_metric_collector(osd_perf_metric_collector_listener),
      mds_perf_metric_collector_listener(this),
      mds_perf_metric_collector(mds_perf_metric_collector_listener)
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
			   Messenger::get_pid_nonce(),
			   0);
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

int DaemonServer::ms_handle_authentication(Connection *con)
{
  auto s = ceph::make_ref<MgrSession>(cct);
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
  } else if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    string str;
    try {
      decode(str, p);
    }
    catch (buffer::error& e) {
      dout(10) << " session " << s << " " << s->entity_name
               << " failed to decode caps" << dendl;
      return -EACCES;
    }
    if (!s->caps.parse(str)) {
      dout(10) << " session " << s << " " << s->entity_name
	       << " failed to parse caps '" << str << "'" << dendl;
      return -EACCES;
    }
    dout(10) << " session " << s << " " << s->entity_name
             << " has caps " << s->caps << " '" << str << "'" << dendl;
  }

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    std::lock_guard l(lock);
    s->osd_id = atoi(s->entity_name.get_id().c_str());
    dout(10) << "registering osd." << s->osd_id << " session "
	     << s << " con " << con << dendl;
    osd_cons[s->osd_id].insert(con);
  }

  return 1;
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

bool DaemonServer::ms_dispatch2(const ref_t<Message>& m)
{
  // Note that we do *not* take ::lock here, in order to avoid
  // serializing all message handling.  It's up to each handler
  // to take whatever locks it needs.
  switch (m->get_type()) {
    case MSG_PGSTATS:
      cluster_state.ingest_pgstats(ref_cast<MPGStats>(m));
      maybe_ready(m->get_source().num());
      return true;
    case MSG_MGR_REPORT:
      return handle_report(ref_cast<MMgrReport>(m));
    case MSG_MGR_OPEN:
      return handle_open(ref_cast<MMgrOpen>(m));
    case MSG_MGR_CLOSE:
      return handle_close(ref_cast<MMgrClose>(m));
    case MSG_COMMAND:
      return handle_command(ref_cast<MCommand>(m));
    case MSG_MGR_COMMAND:
      return handle_command(ref_cast<MMgrCommand>(m));
    default:
      dout(1) << "Unhandled message type " << m->get_type() << dendl;
      return false;
  };
}

void DaemonServer::dump_pg_ready(ceph::Formatter *f)
{
  f->dump_bool("pg_ready", pgmap_ready.load());
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
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = nullptr;
  }

  // on shutdown start rejecting explicit requests to send reports that may
  // originate from python land which may still be running.
  if (shutting_down)
    return;

  tick_event = timer.add_event_after(delay_sec,
    new LambdaContext([this](int r) {
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
  finisher.queue(new LambdaContext([this](int r) {
        std::lock_guard l(lock);
        for (auto &c : daemon_connections) {
          if (c->peer_is_osd()) {
            _send_configure(c);
          }
        }
      }));
}

void DaemonServer::handle_mds_perf_metric_query_updated()
{
  dout(10) << dendl;

  // Send a fresh MMgrConfigure to all clients, so that they can follow
  // the new policy for transmitting stats
  finisher.queue(new LambdaContext([this](int r) {
        std::lock_guard l(lock);
        for (auto &c : daemon_connections) {
          if (c->peer_is_mds()) {
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
  cluster_state.shutdown();
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
    return DaemonKey{service_name, daemon_name};
  } else {
    return DaemonKey{ceph_entity_type_name(peer_type), daemon_name};
  }
}

void DaemonServer::fetch_missing_metadata(const DaemonKey& key,
					  const entity_addr_t& addr)
{
  if (!daemon_state.is_updating(key) &&
      (key.type == "osd" || key.type == "mds" || key.type == "mon")) {
    std::ostringstream oss;
    auto c = new MetadataUpdate(daemon_state, key);
    if (key.type == "osd") {
      oss << "{\"prefix\": \"osd metadata\", \"id\": "
	  << key.name<< "}";
    } else if (key.type == "mds") {
      c->set_default("addr", stringify(addr));
      oss << "{\"prefix\": \"mds metadata\", \"who\": \""
	  << key.name << "\"}";
    } else if (key.type == "mon") {
      oss << "{\"prefix\": \"mon metadata\", \"id\": \""
	  << key.name << "\"}";
    } else {
      ceph_abort();
    }
    monc->start_mon_command({oss.str()}, {}, &c->outbl, &c->outs, c);
  }
}

bool DaemonServer::handle_open(const ref_t<MMgrOpen>& m)
{
  std::unique_lock l(lock);

  DaemonKey key = key_from_service(m->service_name,
				   m->get_connection()->get_peer_type(),
				   m->daemon_name);

  auto con = m->get_connection();
  dout(10) << "from " << key << " " << con->get_peer_addr() << dendl;

  _send_configure(con);

  DaemonStatePtr daemon;
  if (daemon_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << key << dendl;
    daemon = daemon_state.get(key);
  }
  if (!daemon) {
    if (m->service_daemon) {
      dout(4) << "constructing new DaemonState for " << key << dendl;
      daemon = std::make_shared<DaemonState>(daemon_state.types);
      daemon->key = key;
      daemon->service_daemon = true;
      daemon_state.insert(daemon);
    } else {
      /* A normal Ceph daemon has connected but we are or should be waiting on
       * metadata for it. Close the session so that it tries to reconnect.
       */
      dout(2) << "ignoring open from " << key << " " << con->get_peer_addr()
              << "; not ready for session (expect reconnect)" << dendl;
      con->mark_down();
      l.unlock();
      fetch_missing_metadata(key, m->get_source_addr());
      return true;
    }
  }
  if (daemon) {
    if (m->service_daemon) {
      // update the metadata through the daemon state index to
      // ensure it's kept up-to-date
      daemon_state.update_metadata(daemon, m->daemon_metadata);
    }

    std::lock_guard l(daemon->lock);
    daemon->perf_counters.clear();

    daemon->service_daemon = m->service_daemon;
    if (m->service_daemon) {
      daemon->service_status = m->daemon_status;

      utime_t now = ceph_clock_now();
      auto [d, added] = pending_service_map.get_daemon(m->service_name,
						       m->daemon_name);
      if (added || d->gid != (uint64_t)m->get_source().num()) {
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

  if (con->get_peer_type() != entity_name_t::TYPE_CLIENT &&
      m->service_name.empty())
  {
    // Store in set of the daemon/service connections, i.e. those
    // connections that require an update in the event of stats
    // configuration changes.
    daemon_connections.insert(con);
  }

  return true;
}

bool DaemonServer::handle_close(const ref_t<MMgrClose>& m)
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
  m->get_connection()->send_message2(m);
  return true;
}

void DaemonServer::update_task_status(DaemonKey key, const ref_t<MMgrReport>& m) {
  dout(10) << "got task status from " << key << dendl;

  auto p = pending_service_map.get_daemon(key.type, key.name);
  if (!map_compare(p.first->task_status, *m->task_status)) {
    p.first->task_status = *m->task_status;
    pending_service_map_dirty = pending_service_map.epoch;
  }
}

bool DaemonServer::handle_report(const ref_t<MMgrReport>& m)
{
  DaemonKey key;
  if (!m->service_name.empty()) {
    key.type = m->service_name;
  } else {
    key.type = ceph_entity_type_name(m->get_connection()->get_peer_type());
  }
  key.name = m->daemon_name;

  dout(10) << "from " << m->get_connection() << " " << key << dendl;

  if (m->get_connection()->get_peer_type() == entity_name_t::TYPE_CLIENT &&
      m->service_name.empty()) {
    // Clients should not be sending us stats unless they are declaring
    // themselves to be a daemon for some service.
    dout(10) << "rejecting report from non-daemon client " << m->daemon_name
	     << dendl;
    clog->warn() << "rejecting report from non-daemon client " << m->daemon_name
		 << " at " << m->get_connection()->get_peer_addrs();
    m->get_connection()->mark_down();
    return true;
  }


  {
    std::unique_lock locker(lock);

    DaemonStatePtr daemon;
    // Look up the DaemonState
    if (daemon_state.exists(key)) {
      dout(20) << "updating existing DaemonState for " << key << dendl;
      daemon = daemon_state.get(key);
    } else {
      locker.unlock();

      // we don't know the hostname at this stage, reject MMgrReport here.
      dout(5) << "rejecting report from " << key << ", since we do not have its metadata now."
              << dendl;
      // issue metadata request in background
      fetch_missing_metadata(key, m->get_source_addr());

      locker.lock();

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

      return false;
    }

    // Update the DaemonState
    ceph_assert(daemon != nullptr);
    {
      std::lock_guard l(daemon->lock);
      auto &daemon_counters = daemon->perf_counters;
      daemon_counters.update(*m.get());

      auto p = m->config_bl.cbegin();
      if (p != m->config_bl.end()) {
        decode(daemon->config, p);
        decode(daemon->ignored_mon_config, p);
        dout(20) << " got config " << daemon->config
                 << " ignored " << daemon->ignored_mon_config << dendl;
      }

      utime_t now = ceph_clock_now();
      if (daemon->service_daemon) {
        if (m->daemon_status) {
          daemon->service_status_stamp = now;
          daemon->service_status = *m->daemon_status;
        }
        daemon->last_service_beacon = now;
      } else if (m->daemon_status) {
        derr << "got status from non-daemon " << key << dendl;
      }
      // update task status
      if (m->task_status) {
        update_task_status(key, m);
        daemon->last_service_beacon = now;
      }
      if (m->get_connection()->peer_is_osd() || m->get_connection()->peer_is_mon()) {
        // only OSD and MON send health_checks to me now
        daemon->daemon_health_metrics = std::move(m->daemon_health_metrics);
        dout(10) << "daemon_health_metrics " << daemon->daemon_health_metrics
                 << dendl;
      }
    }
  }

  // if there are any schema updates, notify the python modules
  if (!m->declare_types.empty() || !m->undeclare_types.empty()) {
    py_modules.notify_all("perf_schema_update", ceph::to_string(key));
  }

  if (m->get_connection()->peer_is_osd()) {
    osd_perf_metric_collector.process_reports(m->osd_perf_metric_reports);
  }

  if (m->metric_report_message) {
    const MetricReportMessage &message = *m->metric_report_message;
    boost::apply_visitor(HandlePayloadVisitor(this), message.payload);
  }

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
      if (cmd_getval(cmdmap, "caps", cv) &&
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
  const string &service,
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
    s->entity_name,
    service, module, prefix, param_str_map,
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
  ceph::ref_t<MCommand> m_tell;
  ceph::ref_t<MMgrCommand> m_mgr;
  const std::vector<std::string>& cmd;  ///< ref into m_tell or m_mgr
  const bufferlist& data;               ///< ref into m_tell or m_mgr
  bufferlist odata;
  cmdmap_t cmdmap;

  explicit CommandContext(ceph::ref_t<MCommand> m)
    : m_tell{std::move(m)},
      cmd(m_tell->cmd),
      data(m_tell->get_data()) {
  }
  explicit CommandContext(ceph::ref_t<MMgrCommand> m)
    : m_mgr{std::move(m)},
      cmd(m_mgr->cmd),
      data(m_mgr->get_data()) {
  }

  void reply(int r, const std::stringstream &ss) {
    reply(r, ss.str());
  }

  void reply(int r, const std::string &rs) {
    // Let the connection drop as soon as we've sent our response
    ConnectionRef con = m_tell ? m_tell->get_connection()
      : m_mgr->get_connection();
    if (con) {
      con->mark_disposable();
    }

    if (r == 0) {
      dout(20) << "success" << dendl;
    } else {
      derr << __func__ << " " << cpp_strerror(r) << " " << rs << dendl;
    }
    if (con) {
      if (m_tell) {
	MCommandReply *reply = new MCommandReply(r, rs);
	reply->set_tid(m_tell->get_tid());
	reply->set_data(odata);
	con->send_message(reply);
      } else {
	MMgrCommandReply *reply = new MMgrCommandReply(r, rs);
	reply->set_tid(m_mgr->get_tid());
	reply->set_data(odata);
	con->send_message(reply);
      }
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

bool DaemonServer::handle_command(const ref_t<MCommand>& m)
{
  std::lock_guard l(lock);
  auto cmdctx = std::make_shared<CommandContext>(m);
  try {
    return _handle_command(cmdctx);
  } catch (const bad_cmd_get& e) {
    cmdctx->reply(-EINVAL, e.what());
    return true;
  }
}

bool DaemonServer::handle_command(const ref_t<MMgrCommand>& m)
{
  std::lock_guard l(lock);
  auto cmdctx = std::make_shared<CommandContext>(m);
  try {
    return _handle_command(cmdctx);
  } catch (const bad_cmd_get& e) {
    cmdctx->reply(-EINVAL, e.what());
    return true;
  }
}

void DaemonServer::log_access_denied(
    std::shared_ptr<CommandContext>& cmdctx,
    MgrSession* session, std::stringstream& ss) {
  dout(1) << " access denied" << dendl;
  audit_clog->info() << "from='" << session->inst << "' "
                     << "entity='" << session->entity_name << "' "
                     << "cmd=" << cmdctx->cmd << ":  access denied";
  ss << "access denied: does your client key have mgr caps? "
        "See http://docs.ceph.com/docs/master/mgr/administrator/"
        "#client-authentication";
}

bool DaemonServer::_handle_command(
  std::shared_ptr<CommandContext>& cmdctx)
{
  MessageRef m;
  bool admin_socket_cmd = false;
  if (cmdctx->m_tell) {
    m = cmdctx->m_tell;
    // a blank fsid in MCommand signals a legacy client sending a "mon-mgr" CLI
    // command.
    admin_socket_cmd = (cmdctx->m_tell->fsid != uuid_d());
  } else {
    m = cmdctx->m_mgr;
  }
  auto priv = m->get_connection()->get_priv();
  auto session = static_cast<MgrSession*>(priv.get());
  if (!session) {
    return true;
  }
  if (session->inst.name == entity_name_t()) {
    session->inst.name = m->get_source();
  }

  std::string format;
  boost::scoped_ptr<Formatter> f;
  map<string,string> param_str_map;
  std::stringstream ss;
  int r = 0;

  if (!cmdmap_from_json(cmdctx->cmd, &(cmdctx->cmdmap), ss)) {
    cmdctx->reply(-EINVAL, ss);
    return true;
  }

  {
    cmd_getval(cmdctx->cmdmap, "format", format, string("plain"));
    f.reset(Formatter::create(format));
  }

  string prefix;
  cmd_getval(cmdctx->cmdmap, "prefix", prefix);

  dout(10) << "decoded-size=" << cmdctx->cmdmap.size() << " prefix=" << prefix  << dendl;

  // this is just for mgr commands - admin socket commands will fall
  // through and use the admin socket version of
  // get_command_descriptions
  if (prefix == "get_command_descriptions" && !admin_socket_cmd) {
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

  bool is_allowed = false;
  ModuleCommand py_command;
  if (admin_socket_cmd) {
    // admin socket commands require all capabilities
    is_allowed = session->caps.is_allow_all();
  } else if (!mgr_cmd) {
    // Resolve the command to the name of the module that will
    // handle it (if the command exists)
    auto py_commands = py_modules.get_py_commands();
    for (const auto &pyc : py_commands) {
      auto pyc_prefix = cmddesc_get_prefix(pyc.cmdstring);
      if (pyc_prefix == prefix) {
        py_command = pyc;
        break;
      }
    }

    MonCommand pyc = {"", "", "py", py_command.perm};
    is_allowed = _allowed_command(session, "py", py_command.module_name,
                                  prefix, cmdctx->cmdmap, param_str_map,
                                  &pyc);
  } else {
    // validate user's permissions for requested command
    is_allowed = _allowed_command(session, mgr_cmd->module, "",
      prefix, cmdctx->cmdmap,  param_str_map, mgr_cmd);
  }

  if (!is_allowed) {
      log_access_denied(cmdctx, session, ss);
      cmdctx->reply(-EACCES, ss);
      return true;
  }

  audit_clog->debug()
    << "from='" << session->inst << "' "
    << "entity='" << session->entity_name << "' "
    << "cmd=" << cmdctx->cmd << ": dispatch";

  if (admin_socket_cmd) {
    cct->get_admin_socket()->queue_tell_command(cmdctx->m_tell);
    return true;
  }

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
    for (auto& [type, service] : pending_service_map.services) {
      if (ServiceMap::is_normal_ceph_entity(type)) {
        continue;
      }

      f->open_object_section(type.c_str());
      for (auto& q : service.daemons) {
	f->open_object_section(q.first.c_str());
	DaemonKey key{type, q.first};
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
    cmd_getval(cmdctx->cmdmap, "key", key);
    cmd_getval(cmdctx->cmdmap, "value", val);
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
    cmd_getval(cmdctx->cmdmap, "pgid", pgidstr);
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
    cmd_getval(cmdctx->cmdmap, "who", whostr);
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
  } else if (prefix == "osd pool scrub" ||
             prefix == "osd pool deep-scrub" ||
             prefix == "osd pool repair") {
    vector<string> pool_names;
    cmd_getval(cmdctx->cmdmap, "who", pool_names);
    if (pool_names.empty()) {
      ss << "must specify one or more pool names";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    epoch_t epoch;
    map<int32_t, vector<pg_t>> pgs_by_primary; // legacy
    map<int32_t, vector<spg_t>> spgs_by_primary;
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
      epoch = osdmap.get_epoch();
      for (auto& pool_name : pool_names) {
        auto pool_id = osdmap.lookup_pg_pool_name(pool_name);
        if (pool_id < 0) {
          ss << "unrecognized pool '" << pool_name << "'";
          r = -ENOENT;
          return;
        }
        auto pool_pg_num = osdmap.get_pg_num(pool_id);
        for (int i = 0; i < pool_pg_num; i++) {
          pg_t pg(i, pool_id);
          int primary;
          spg_t spg;
          auto got = osdmap.get_primary_shard(pg, &primary, &spg);
          if (!got)
            continue;
          pgs_by_primary[primary].push_back(pg);
          spgs_by_primary[primary].push_back(spg);
        }
      }
    });
    if (r < 0) {
      cmdctx->reply(r, ss);
      return true;
    }
    for (auto& it : spgs_by_primary) {
      auto primary = it.first;
      auto p = osd_cons.find(primary);
      if (p == osd_cons.end()) {
        ss << "osd." << primary << " is not currently connected";
        cmdctx->reply(-EAGAIN, ss);
        return true;
      }
      for (auto& con : p->second) {
        if (HAVE_FEATURE(con->get_features(), SERVER_MIMIC)) {
          con->send_message(new MOSDScrub2(monc->get_fsid(),
                                           epoch,
                                           it.second,
                                           prefix == "osd pool repair",
                                           prefix == "osd pool deep-scrub"));
        } else {
          // legacy
          auto q = pgs_by_primary.find(primary);
          ceph_assert(q != pgs_by_primary.end());
          con->send_message(new MOSDScrub(monc->get_fsid(),
                                          q->second,
                                          prefix == "osd pool repair",
                                          prefix == "osd pool deep-scrub"));
        }
      }
    }
    cmdctx->reply(0, "");
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
    cmd_getval(cmdctx->cmdmap, "oload", oload, int64_t(120));
    set<int64_t> pools;
    vector<string> poolnames;
    cmd_getval(cmdctx->cmdmap, "pools", poolnames);
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
    cmd_getval(cmdctx->cmdmap, "max_change", max_change);
    if (max_change <= 0.0) {
      ss << "max_change " << max_change << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    int64_t max_osds = g_conf().get_val<int64_t>("mon_reweight_max_osds");
    cmd_getval(cmdctx->cmdmap, "max_osds", max_osds);
    if (max_osds <= 0) {
      ss << "max_osds " << max_osds << " must be positive";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    bool no_increasing = false;
    cmd_getval(cmdctx->cmdmap, "no_increasing", no_increasing);
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
    string method, filter;
    cmd_getval(cmdctx->cmdmap, "output_method", method);
    cmd_getval(cmdctx->cmdmap, "filter", filter);
    stringstream rs;
    r = cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pgmap) {
        // sanity check filter(s)
        if (!filter.empty() &&
             osdmap.lookup_pg_pool_name(filter) < 0 &&
            !osdmap.crush->class_exists(filter) &&
            !osdmap.crush->name_exists(filter)) {
          rs << "'" << filter << "' not a pool, crush node or device class name";
          return -EINVAL;
        }
	print_osd_utilization(osdmap, pgmap, ss,
                              f.get(), method == "tree", filter);
	cmdctx->odata.append(ss);
	return 0;
      });
    cmdctx->reply(r, rs);
    return true;
  } else if (prefix == "osd pool stats") {
    string pool_name;
    cmd_getval(cmdctx->cmdmap, "pool_name", pool_name);
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
      cmd_getval(cmdctx->cmdmap, "ids", ids);
      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
				  r = osdmap.parse_osd_id_list(ids, &osds, &ss);
				});
      if (!r && osds.empty()) {
	ss << "must specify one or more OSDs";
	r = -EINVAL;
      }
    } else {
      int64_t id;
      if (!cmd_getval(cmdctx->cmdmap, "id", id)) {
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
	    if (q->second.acting > 0 || q->second.up_not_acting > 0) {
	      active_osds.insert(osd);
	      // XXX: For overlapping PGs, this counts them again
	      affected_pgs += q->second.acting + q->second.up_not_acting;
	      continue;
	    }
	  }
	  if (num_active_clean < pg_map.num_pg) {
	    // all pgs aren't active+clean; we need to be careful.
	    auto p = pg_map.osd_stat.find(osd);
	    if (p == pg_map.osd_stat.end() || !osdmap.is_up(osd)) {
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
      cmd_getval(cmdctx->cmdmap, "force", force);
      if (!force) {
        // Backward compat
        cmd_getval(cmdctx->cmdmap, "yes_i_really_mean_it", force);
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
    cmd_getval(cmdctx->cmdmap, "ids", ids);
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
    int touched_pgs = 0;
    int dangerous_pgs = 0;
    cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap, const PGMap& pg_map) {
	if (pg_map.num_pg_unknown > 0) {
	  ss << pg_map.num_pg_unknown << " pgs have unknown state; "
	     << "cannot draw any conclusions";
	  r = -EAGAIN;
	  return;
	}
	for (const auto& q : pg_map.pg_stat) {
          set<int32_t> pg_acting;  // net acting sets (with no missing if degraded)
	  bool found = false;
	  if (q.second.state & PG_STATE_DEGRADED) {
	    for (auto& anm : q.second.avail_no_missing) {
	      if (osds.count(anm.osd)) {
		found = true;
		continue;
	      }
	      if (anm.osd != CRUSH_ITEM_NONE) {
		pg_acting.insert(anm.osd);
	      }
	    }
	  } else {
	    for (auto& a : q.second.acting) {
	      if (osds.count(a)) {
		found = true;
		continue;
	      }
	      if (a != CRUSH_ITEM_NONE) {
		pg_acting.insert(a);
	      }
	    }
	  }
	  if (!found) {
	    continue;
	  }
	  touched_pgs++;
	  if (!(q.second.state & PG_STATE_ACTIVE) ||
	      (q.second.state & PG_STATE_DEGRADED)) {
	    ++dangerous_pgs;
	    continue;
	  }
	  const pg_pool_t *pi = osdmap.get_pg_pool(q.first.pool());
	  if (!pi) {
	    ++dangerous_pgs; // pool is creating or deleting
	  } else {
	    if (pg_acting.size() < pi->min_size) {
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
      ss << dangerous_pgs << " PGs are already too degraded, would become"
	 << " too degraded or might become unavailable";
      cmdctx->reply(-EBUSY, ss);
      return true;
    }
    ss << "OSD(s) " << osds << " are ok to stop without reducing"
       << " availability or risking data, provided there are no other concurrent failures"
       << " or interventions." << std::endl;
    ss << touched_pgs << " PGs are likely to be"
       << " degraded (but remain available) as a result.";
    cmdctx->reply(0, ss);
    return true;
  } else if (prefix == "pg force-recovery" ||
  	     prefix == "pg force-backfill" ||
  	     prefix == "pg cancel-force-recovery" ||
  	     prefix == "pg cancel-force-backfill" ||
             prefix == "osd pool force-recovery" ||
             prefix == "osd pool force-backfill" ||
             prefix == "osd pool cancel-force-recovery" ||
             prefix == "osd pool cancel-force-backfill") {
    vector<string> vs;
    get_str_vec(prefix, vs);
    auto& granularity = vs.front();
    auto& forceop = vs.back();
    vector<pg_t> pgs;

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

    set<pg_t> candidates; // deduped
    if (granularity == "pg") {
      // covnert pg names to pgs, discard any invalid ones while at it
      vector<string> pgids;
      cmd_getval(cmdctx->cmdmap, "pgid", pgids);
      for (auto& i : pgids) {
        pg_t pgid;
        if (!pgid.parse(i.c_str())) {
          ss << "invlaid pgid '" << i << "'; ";
          r = -EINVAL;
          continue;
        }
        candidates.insert(pgid);
      }
    } else {
      // per pool
      vector<string> pool_names;
      cmd_getval(cmdctx->cmdmap, "who", pool_names);
      if (pool_names.empty()) {
        ss << "must specify one or more pool names";
        cmdctx->reply(-EINVAL, ss);
        return true;
      }
      cluster_state.with_osdmap([&](const OSDMap& osdmap) {
        for (auto& pool_name : pool_names) {
          auto pool_id = osdmap.lookup_pg_pool_name(pool_name);
          if (pool_id < 0) {
            ss << "unrecognized pool '" << pool_name << "'";
            r = -ENOENT;
            return;
          }
          auto pool_pg_num = osdmap.get_pg_num(pool_id);
          for (int i = 0; i < pool_pg_num; i++)
            candidates.insert({(unsigned int)i, (uint64_t)pool_id});
        }
      });
      if (r < 0) {
        cmdctx->reply(r, ss);
        return true;
      }
    }

    cluster_state.with_pgmap([&](const PGMap& pg_map) {
      for (auto& i : candidates) {
	auto it = pg_map.pg_stat.find(i);
	if (it == pg_map.pg_stat.end()) {
	  ss << "pg " << i << " does not exist; ";
	  r = -ENOENT;
          continue;
	}
        auto state = it->second.state;
	// discard pgs for which user requests are pointless
	switch (actual_op) {
        case OFR_RECOVERY:
          if ((state & (PG_STATE_DEGRADED |
                        PG_STATE_RECOVERY_WAIT |
                        PG_STATE_RECOVERING)) == 0) {
            // don't return error, user script may be racing with cluster.
            // not fatal.
            ss << "pg " << i << " doesn't require recovery; ";
            continue;
          } else  if (state & PG_STATE_FORCED_RECOVERY) {
            ss << "pg " << i << " recovery already forced; ";
            // return error, as it may be a bug in user script
            r = -EINVAL;
            continue;
          }
          break;
        case OFR_BACKFILL:
          if ((state & (PG_STATE_DEGRADED |
                        PG_STATE_BACKFILL_WAIT |
                        PG_STATE_BACKFILLING)) == 0) {
            ss << "pg " << i << " doesn't require backfilling; ";
            continue;
          } else if (state & PG_STATE_FORCED_BACKFILL) {
            ss << "pg " << i << " backfill already forced; ";
            r = -EINVAL;
            continue;
          }
          break;
        case OFR_BACKFILL | OFR_CANCEL:
          if ((state & PG_STATE_FORCED_BACKFILL) == 0) {
            ss << "pg " << i << " backfill not forced; ";
            continue;
          }
          break;
        case OFR_RECOVERY | OFR_CANCEL:
          if ((state & PG_STATE_FORCED_RECOVERY) == 0) {
            ss << "pg " << i << " recovery not forced; ";
            continue;
          }
          break;
        default:
          ceph_abort_msg("actual_op value is not supported");
        }
	pgs.push_back(i);
      } // for
    });

    // respond with error only when no pgs are correct
    // yes, in case of mixed errors, only the last one will be emitted,
    // but the message presented will be fine
    if (pgs.size() != 0) {
      // clear error to not confuse users/scripts
      r = 0;
    }

    // optimize the command -> messages conversion, use only one
    // message per distinct OSD
    cluster_state.with_osdmap([&](const OSDMap& osdmap) {
	// group pgs to process by osd
	map<int, vector<spg_t>> osdpgs;
	for (auto& pgid : pgs) {
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
    cmd_getval(cmdctx->cmdmap, "who", who);
    auto [key, valid] = DaemonKey::parse(who);
    if (!valid) {
      ss << "invalid daemon name: use <type>.<id>";
      cmdctx->reply(-EINVAL, ss);
      return true;
    }
    DaemonStatePtr daemon = daemon_state.get(key);
    if (!daemon) {
      ss << "no config state for daemon " << who;
      cmdctx->reply(-ENOENT, ss);
      return true;
    }

    std::lock_guard l(daemon->lock);

    int r = 0;
    string name;
    if (cmd_getval(cmdctx->cmdmap, "key", name)) {
      // handle special options
      if (name == "fsid") {
       cmdctx->odata.append(stringify(monc->get_fsid()) + "\n");
       cmdctx->reply(r, ss);
       return true;
      }
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
	  for (auto& i : dev.attachments) {
	    if (h.size()) {
	      h += " ";
	    }
	    h += std::get<0>(i) + ":" + std::get<1>(i);
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
    cmd_getval(cmdctx->cmdmap, "who", who);
    if (auto [k, valid] = DaemonKey::parse(who); !valid) {
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
		for (auto& i : dev.attachments) {
		  if (h.size()) {
		    h += " ";
		  }
		  h += std::get<0>(i) + ":" + std::get<1>(i);
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
    cmd_getval(cmdctx->cmdmap, "host", host);
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
	    for (auto& j : dev.attachments) {
	      if (std::get<0>(j) == host) {
		if (n.size()) {
		  n += " ";
		}
		n += std::get<1>(j);
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
    cmd_getval(cmdctx->cmdmap, "devid", devid);
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
    cmd_getval(cmdctx->cmdmap, "devid", devid);
    string from_str, to_str;
    cmd_getval(cmdctx->cmdmap, "from", from_str);
    cmd_getval(cmdctx->cmdmap, "to", to_str);
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
    cmd_getval(cmdctx->cmdmap, "devid", devid);
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

  // Was the command unfound?
  if (py_command.cmdstring.empty()) {
    ss << "No handler found for '" << prefix << "'";
    dout(4) << "No handler found for '" << prefix << "'" << dendl;
    cmdctx->reply(-EINVAL, ss);
    return true;
  }

  dout(10) << "passing through " << cmdctx->cmdmap.size() << dendl;
  finisher.queue(new LambdaContext([this, cmdctx, session, py_command, prefix]
                                   (int r_) mutable {
    std::stringstream ss;

    // Validate that the module is enabled
    auto& py_handler_name = py_command.module_name;
    PyModuleRef module = py_modules.get_module(py_handler_name);
    ceph_assert(module);
    if (!module->is_enabled()) {
      ss << "Module '" << py_handler_name << "' is not enabled (required by "
            "command '" << prefix << "'): use `ceph mgr module enable "
            << py_handler_name << "` to enable it";
      dout(4) << ss.str() << dendl;
      cmdctx->reply(-EOPNOTSUPP, ss);
      return;
    }

    // Hack: allow the self-test method to run on unhealthy modules.
    // Fix this in future by creating a special path for self test rather
    // than having the hook be a normal module command.
    std::string self_test_prefix = py_handler_name + " " + "self-test";

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
        ss << "Module '" << py_handler_name << "' has experienced an error and "
              "cannot handle commands: " << module->get_error_string();
      }
    } else {
      // Module not loaded
      accept_command = false;
      ss << "Module '" << py_handler_name << "' failed to load and "
            "cannot handle commands: " << module->get_error_string();
    }

    if (!accept_command) {
      dout(4) << ss.str() << dendl;
      cmdctx->reply(-EIO, ss);
      return;
    }

    std::stringstream ds;
    bufferlist inbl = cmdctx->data;
    int r = py_modules.handle_command(py_command, *session, cmdctx->cmdmap,
                                      inbl, &ds, &ss);
    if (r == -EACCES) {
      log_access_denied(cmdctx, session, ss);
    }

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
      DaemonKey key{p->first, q->first};
      if (!daemon_state.exists(key)) {
        if (ServiceMap::is_normal_ceph_entity(p->first)) {
          dout(10) << "daemon " << key << " in service map but not in daemon state "
                   << "index -- force pruning" << dendl;
          q = p->second.daemons.erase(q);
          pending_service_map_dirty = pending_service_map.epoch;
        } else {
          derr << "missing key " << key << dendl;
          ++q;
        }

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

  auto m = ceph::make_message<MMonMgrReport>();
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
          if (osdmap.require_osd_release >= ceph_release_t::luminous) {
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
            derr << __func__ << " " << key
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
  monc->send_mon_message(std::move(m));
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
	    vector<int32_t> source_acting;
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
	      if (is_merge_source) {
		source_acting = q->second.acting;
	      } else if (ok && q->second.acting != source_acting) {
		dout(10) << "pool " << i.first
		         << " pg_num_target " << p.get_pg_num_target()
		         << " pg_num " << p.get_pg_num()
		         << (is_merge_source ? " - merge source " : " - merge target ")
                         << merge_participant
			 << " acting does not match (source " << source_acting
			 << " != target " << q->second.acting
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
              continue;
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
	    static constexpr unsigned MAX_NUM_OBJECTS_PER_PG_FOR_LEAP = 1;
	    pool_stat_t s = pg_map.get_pg_pool_sum_stat(i.first);
	    if (aggro ||
		// pool is (virtually) empty; just jump to final pgp_num?
		(p.get_pgp_num_target() > p.get_pgp_num() &&
		 s.stats.sum.num_objects <= (MAX_NUM_OBJECTS_PER_PG_FOR_LEAP *
					     p.get_pgp_num_target()))) {
	      next = target;
	    } else {
	      double room =
		std::min<double>(max_misplaced - misplaced_ratio,
				 max_misplaced / 2.0);
	      unsigned estmax = std::max<unsigned>(
		(double)p.get_pg_num() * room, 1u);
	      next = std::clamp(target,
				p.get_pgp_num() - estmax,
				p.get_pgp_num() + estmax);
	      dout(20) << " room " << room << " estmax " << estmax
		       << " delta " << (target-p.get_pgp_num())
		       << " next " << next << dendl;
	      if (p.get_pgp_num_target() == p.get_pg_num_target() &&
		  p.get_pgp_num_target() < p.get_pg_num()) {
		// since pgp_num is tracking pg_num, ceph is handling
		// pgp_num.  so, be responsible: don't let pgp_num get
		// too far out ahead of merges (if we are merging).
		// this avoids moving lots of unmerged pgs onto a
		// small number of OSDs where we might blow out the
		// per-osd pg max.
		unsigned max_outpace_merges =
		  std::max<unsigned>(8, p.get_pg_num() * max_misplaced);
		if (next + max_outpace_merges < p.get_pg_num()) {
		  next = p.get_pg_num() - max_outpace_merges;
		  dout(10) << "  using next " << next
			   << " to avoid outpacing merges (max_outpace_merges "
			   << max_outpace_merges << ")" << dendl;
		}
	      }
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
  std::set<std::string> types;
  for (auto& [type, service] : pending_service_map.services) {
    if (ServiceMap::is_normal_ceph_entity(type)) {
      continue;
    }

    types.insert(type);

    std::set<std::string> names;
    for (auto& q : service.daemons) {
      names.insert(q.first);
      DaemonKey key{type, q.first};
      if (!daemon_state.exists(key)) {
	auto daemon = std::make_shared<DaemonState>(daemon_state.types);
	daemon->key = key;
	daemon->set_metadata(q.second.metadata);
	daemon->service_daemon = true;
	daemon_state.insert(daemon);
	dout(10) << "added missing " << key << dendl;
      }
    }
    daemon_state.cull(type, names);
  }
  daemon_state.cull_services(types);
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
	    << key.name << "\", \"id\": \"" << key.name << "\"}";
        monc->start_mon_command({oss.str()}, {}, &c->outbl, &c->outs, c);
      };
      if (mgrmap.active_name.size()) {
	DaemonKey key{"mgr", mgrmap.active_name};
	have.insert(mgrmap.active_name);
	if (!daemon_state.exists(key) && !daemon_state.is_updating(key)) {
	  md_update(key);
	  dout(10) << "triggered addition of " << key << " via metadata update" << dendl;
	}
      }
      for (auto& i : mgrmap.standbys) {
        DaemonKey key{"mgr", i.second.name};
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

  if (changed.count("mgr_stats_threshold") || changed.count("mgr_stats_period")) {
    dout(4) << "Updating stats threshold/period on "
            << daemon_connections.size() << " clients" << dendl;
    // Send a fresh MMgrConfigure to all clients, so that they can follow
    // the new policy for transmitting stats
    finisher.queue(new LambdaContext([this](int r) {
      std::lock_guard l(lock);
      for (auto &c : daemon_connections) {
        _send_configure(c);
      }
    }));
  }
}

void DaemonServer::_send_configure(ConnectionRef c)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  auto configure = make_message<MMgrConfigure>();
  configure->stats_period = g_conf().get_val<int64_t>("mgr_stats_period");
  configure->stats_threshold = g_conf().get_val<int64_t>("mgr_stats_threshold");

  if (c->peer_is_osd()) {
    configure->osd_perf_metric_queries =
        osd_perf_metric_collector.get_queries();
  } else if (c->peer_is_mds()) {
    configure->metric_config_message =
      MetricConfigMessage(MDSConfigPayload(mds_perf_metric_collector.get_queries()));
  }

  c->send_message2(configure);
}

MetricQueryID DaemonServer::add_osd_perf_query(
    const OSDPerfMetricQuery &query,
    const std::optional<OSDPerfMetricLimit> &limit)
{
  return osd_perf_metric_collector.add_query(query, limit);
}

int DaemonServer::remove_osd_perf_query(MetricQueryID query_id)
{
  return osd_perf_metric_collector.remove_query(query_id);
}

int DaemonServer::get_osd_perf_counters(
    MetricQueryID query_id,
    std::map<OSDPerfMetricKey, PerformanceCounters> *counters)
{
  return osd_perf_metric_collector.get_counters(query_id, counters);
}
