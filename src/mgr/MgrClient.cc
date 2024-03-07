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


#include "MgrClient.h"

#include "common/perf_counters_key.h"
#include "mgr/MgrContext.h"
#include "mon/MonMap.h"

#include "msg/Messenger.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrUpdate.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MMgrCommand.h"
#include "messages/MMgrCommandReply.h"
#include "messages/MPGStats.h"

using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::make_message;
using ceph::ref_cast;
using ceph::ref_t;

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(CephContext *cct_, Messenger *msgr_, MonMap *monmap_)
  : Dispatcher(cct_),
    cct(cct_),
    msgr(msgr_),
    monmap(monmap_),
    timer(cct_, lock)
{
  ceph_assert(cct != nullptr);
}

void MgrClient::init()
{
  std::lock_guard l(lock);

  ceph_assert(msgr != nullptr);

  timer.init();
  initialized = true;
}

void MgrClient::shutdown()
{
  std::unique_lock l(lock);
  ldout(cct, 10) << dendl;

  if (connect_retry_callback) {
    timer.cancel_event(connect_retry_callback);
    connect_retry_callback = nullptr;
  }

  // forget about in-flight commands if we are prematurely shut down
  // (e.g., by control-C)
  command_table.clear();
  if (service_daemon &&
      session &&
      session->con &&
      HAVE_FEATURE(session->con->get_features(), SERVER_MIMIC)) {
    ldout(cct, 10) << "closing mgr session" << dendl;
    auto m = make_message<MMgrClose>();
    m->daemon_name = daemon_name;
    m->service_name = service_name;
    session->con->send_message2(m);
    auto timeout = ceph::make_timespan(cct->_conf.get_val<double>(
			      "mgr_client_service_daemon_unregister_timeout"));
    shutdown_cond.wait_for(l, timeout);
  }

  timer.shutdown();
  if (session) {
    session->con->mark_down();
    session.reset();
  }
}

bool MgrClient::ms_dispatch2(const ref_t<Message>& m)
{
  std::lock_guard l(lock);

  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(ref_cast<MMgrMap>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_configure(ref_cast<MMgrConfigure>(m));
  case MSG_MGR_CLOSE:
    return handle_mgr_close(ref_cast<MMgrClose>(m));
  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MGR) {
      MCommandReply *c = static_cast<MCommandReply*>(m.get());
      handle_command_reply(c->get_tid(), c->get_data(), c->rs, c->r);
      return true;
    } else {
      return false;
    }
  case MSG_MGR_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MGR) {
      MMgrCommandReply *c = static_cast<MMgrCommandReply*>(m.get());
      handle_command_reply(c->get_tid(), c->get_data(), c->rs, c->r);
      return true;
    } else {
      return false;
    }
  default:
    ldout(cct, 30) << "Not handling " << *m << dendl; 
    return false;
  }
}

void MgrClient::reconnect()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  if (session) {
    ldout(cct, 4) << "Terminating session with "
		  << session->con->get_peer_addr() << dendl;
    session->con->mark_down();
    session.reset();
    stats_period = 0;
    if (report_callback != nullptr) {
      timer.cancel_event(report_callback);
      report_callback = nullptr;
    }
  }

  if (!map.get_available()) {
    ldout(cct, 4) << "No active mgr available yet" << dendl;
    return;
  }

  if (!clock_t::is_zero(last_connect_attempt)) {
    auto now = clock_t::now();
    auto when = last_connect_attempt +
      ceph::make_timespan(
        cct->_conf.get_val<double>("mgr_connect_retry_interval"));
    if (now < when) {
      if (!connect_retry_callback) {
	connect_retry_callback = timer.add_event_at(
	  when,
	  new LambdaContext([this](int r){
	      connect_retry_callback = nullptr;
	      reconnect();
	    }));
      }
      ldout(cct, 4) << "waiting to retry connect until " << when << dendl;
      return;
    }
  }

  if (connect_retry_callback) {
    timer.cancel_event(connect_retry_callback);
    connect_retry_callback = nullptr;
  }

  ldout(cct, 4) << "Starting new session with " << map.get_active_addrs()
		<< dendl;
  last_connect_attempt = clock_t::now();

  session.reset(new MgrSessionState());
  session->con = msgr->connect_to(CEPH_ENTITY_TYPE_MGR,
				  map.get_active_addrs());

  if (service_daemon) {
    daemon_dirty_status = true;
  }
  task_dirty_status = true;

  // Don't send an open if we're just a client (i.e. doing
  // command-sending, not stats etc)
  if (msgr->get_mytype() != CEPH_ENTITY_TYPE_CLIENT || service_daemon) {
    _send_open();
  }

  // resend any pending commands
  auto p = command_table.get_commands().begin();
  while (p != command_table.get_commands().end()) {
    auto tid = p->first;
    auto& op = p->second;
    ldout(cct,10) << "resending " << tid << (op.tell ? " (tell)":" (cli)") << dendl;
    MessageRef m;
    if (op.tell) {
      if (op.name.size() && op.name != map.active_name) {
	ldout(cct, 10) << "active mgr " << map.active_name << " != target "
		       << op.name << dendl;
	if (op.on_finish) {
	  op.on_finish->complete(-ENXIO);
	}
	++p;
	command_table.erase(tid);
	continue;
      }
      // Set fsid argument to signal that this is really a tell message (and
      // we are not a legacy client sending a non-tell command via MCommand).
      m = op.get_message(monmap->fsid, false);
    } else {
      m = op.get_message(
	{},
	HAVE_FEATURE(map.active_mgr_features, SERVER_OCTOPUS));
    }
    ceph_assert(session);
    ceph_assert(session->con);
    session->con->send_message2(std::move(m));
    ++p;
  }
}

void MgrClient::_send_open()
{
  if (session && session->con) {
    auto open = make_message<MMgrOpen>();
    if (!service_name.empty()) {
      open->service_name = service_name;
      open->daemon_name = daemon_name;
    } else {
      open->daemon_name = cct->_conf->name.get_id();
    }
    if (service_daemon) {
      open->service_daemon = service_daemon;
      open->daemon_metadata = daemon_metadata;
    }
    cct->_conf.get_config_bl(0, &open->config_bl, &last_config_bl_version);
    cct->_conf.get_defaults_bl(&open->config_defaults_bl);
    session->con->send_message2(open);
  }
}

void MgrClient::_send_update()
{
  if (session && session->con) {
    auto update = make_message<MMgrUpdate>();
    if (!service_name.empty()) {
      update->service_name = service_name;
      update->daemon_name = daemon_name;
    } else {
      update->daemon_name = cct->_conf->name.get_id();
    }
    if (need_metadata_update) {
      update->daemon_metadata = daemon_metadata;
    }
    update->need_metadata_update = need_metadata_update;
    session->con->send_message2(update);
  }
}

bool MgrClient::handle_mgr_map(ref_t<MMgrMap> m)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  ldout(cct, 20) << *m << dendl;

  map = m->get_map();
  ldout(cct, 4) << "Got map version " << map.epoch << dendl;

  ldout(cct, 4) << "Active mgr is now " << map.get_active_addrs() << dendl;

  // Reset session?
  if (!session ||
      session->con->get_peer_addrs() != map.get_active_addrs()) {
    reconnect();
  }

  return true;
}

bool MgrClient::ms_handle_reset(Connection *con)
{
  std::lock_guard l(lock);
  if (session && con == session->con) {
    ldout(cct, 4) << __func__ << " con " << con << dendl;
    reconnect();
    return true;
  }
  return false;
}

bool MgrClient::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

void MgrClient::_send_stats()
{
  _send_report();
  _send_pgstats();
  if (stats_period != 0) {
    report_callback = timer.add_event_after(
      stats_period,
      new LambdaContext([this](int) {
	  _send_stats();
	}));
  }
}

void MgrClient::_send_report()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  ceph_assert(session);
  report_callback = nullptr;

  auto report = make_message<MMgrReport>();
  auto pcc = cct->get_perfcounters_collection();

  pcc->with_counters([this, report](
        const PerfCountersCollectionImpl::CounterMap &by_path)
  {
    // Helper for checking whether a counter should be included
    auto include_counter = [this](
        const PerfCounters::perf_counter_data_any_d &ctr,
        const PerfCounters &perf_counters)
    {
      // FIXME: We don't send labeled perf counters to the mgr currently.
      auto labels = ceph::perf_counters::key_labels(perf_counters.get_name());
      if (labels.begin() != labels.end()) {
        return false;
      }

      return perf_counters.get_adjusted_priority(ctr.prio) >= (int)stats_threshold;
    };

    // Helper for cases where we want to forget a counter
    auto undeclare = [report, this](const std::string &path)
    {
      report->undeclare_types.push_back(path);
      ldout(cct,20) << " undeclare " << path << dendl;
      session->declared.erase(path);
    };

    ENCODE_START(1, 1, report->packed);

    // Find counters that no longer exist, and undeclare them
    for (auto p = session->declared.begin(); p != session->declared.end(); ) {
      const auto &path = *(p++);
      if (by_path.count(path) == 0) {
        undeclare(path);
      }
    }

    for (const auto &i : by_path) {
      auto& path = i.first;
      auto& data = *(i.second.data);
      auto& perf_counters = *(i.second.perf_counters);

      // Find counters that still exist, but are no longer permitted by
      // stats_threshold
      if (!include_counter(data, perf_counters)) {
        if (session->declared.count(path)) {
          undeclare(path);
        }
        continue;
      }

      if (session->declared.count(path) == 0) {
        ldout(cct, 20) << " declare " << path << dendl;
        PerfCounterType type;
        type.path = path;
        if (data.description) {
          type.description = data.description;
        }
        if (data.nick) {
          type.nick = data.nick;
        }
        type.type = data.type;
        type.priority = perf_counters.get_adjusted_priority(data.prio);
        type.unit = data.unit;
        report->declare_types.push_back(std::move(type));
        session->declared.insert(path);
      }

      encode(static_cast<uint64_t>(data.u64), report->packed);
      if (data.type & PERFCOUNTER_LONGRUNAVG) {
        encode(static_cast<uint64_t>(data.avgcount), report->packed);
        encode(static_cast<uint64_t>(data.avgcount2), report->packed);
      }
    }
    ENCODE_FINISH(report->packed);

    ldout(cct, 20) << "sending " << session->declared.size() << " counters ("
                      "of possible " << by_path.size() << "), "
		   << report->declare_types.size() << " new, "
                   << report->undeclare_types.size() << " removed"
                   << dendl;
  });

  ldout(cct, 20) << "encoded " << report->packed.length() << " bytes" << dendl;

  if (daemon_name.size()) {
    report->daemon_name = daemon_name;
  } else {
    report->daemon_name = cct->_conf->name.get_id();
  }
  report->service_name = service_name;

  if (daemon_dirty_status) {
    report->daemon_status = daemon_status;
    daemon_dirty_status = false;
  }

  if (task_dirty_status) {
    report->task_status = task_status;
    task_dirty_status = false;
  }

  report->daemon_health_metrics = std::move(daemon_health_metrics);

  cct->_conf.get_config_bl(last_config_bl_version, &report->config_bl,
			    &last_config_bl_version);

  if (get_perf_report_cb) {
    report->metric_report_message = MetricReportMessage(get_perf_report_cb());
  }

  session->con->send_message2(report);
}

void MgrClient::send_pgstats()
{
  std::lock_guard l(lock);
  _send_pgstats();
}

void MgrClient::_send_pgstats()
{
  if (pgstats_cb && session) {
    session->con->send_message(pgstats_cb());
  }
}

bool MgrClient::handle_mgr_configure(ref_t<MMgrConfigure> m)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  ldout(cct, 20) << *m << dendl;

  if (!session) {
    lderr(cct) << "dropping unexpected configure message" << dendl;
    return true;
  }

  ldout(cct, 4) << "stats_period=" << m->stats_period << dendl;

  if (stats_threshold != m->stats_threshold) {
    ldout(cct, 4) << "updated stats threshold: " << m->stats_threshold << dendl;
    stats_threshold = m->stats_threshold;
  }

  if (!m->osd_perf_metric_queries.empty()) {
    handle_config_payload(m->osd_perf_metric_queries);
  } else if (m->metric_config_message) {
    const MetricConfigMessage &message = *m->metric_config_message;
    boost::apply_visitor(HandlePayloadVisitor(this), message.payload);
  }

  bool starting = (stats_period == 0) && (m->stats_period != 0);
  stats_period = m->stats_period;
  if (starting) {
    _send_stats();
  }

  return true;
}

bool MgrClient::handle_mgr_close(ref_t<MMgrClose> m)
{
  service_daemon = false;
  shutdown_cond.notify_all();
  return true;
}

int MgrClient::start_command(const vector<string>& cmd, const bufferlist& inbl,
			     bufferlist *outbl, string *outs,
			     Context *onfinish)
{
  std::lock_guard l(lock);

  ldout(cct, 20) << "cmd: " << cmd << dendl;

  if (map.epoch == 0 && mgr_optional) {
    ldout(cct,20) << " no MgrMap, assuming EACCES" << dendl;
    return -EACCES;
  }

  auto &op = command_table.start_command();
  op.cmd = cmd;
  op.inbl = inbl;
  op.outbl = outbl;
  op.outs = outs;
  op.on_finish = onfinish;

  if (session && session->con) {
    // Leaving fsid argument null because it isn't used historically, and
    // we can use it as a signal that we are sending a non-tell command.
    auto m = op.get_message(
      {},
      HAVE_FEATURE(map.active_mgr_features, SERVER_OCTOPUS));
    session->con->send_message2(std::move(m));
  } else {
    ldout(cct, 5) << "no mgr session (no running mgr daemon?), waiting" << dendl;
  }
  return 0;
}

int MgrClient::start_tell_command(
  const string& name,
  const vector<string>& cmd, const bufferlist& inbl,
  bufferlist *outbl, string *outs,
  Context *onfinish)
{
  std::lock_guard l(lock);

  ldout(cct, 20) << "target: " << name << " cmd: " << cmd << dendl;

  if (map.epoch == 0 && mgr_optional) {
    ldout(cct,20) << " no MgrMap, assuming EACCES" << dendl;
    return -EACCES;
  }

  auto &op = command_table.start_command();
  op.tell = true;
  op.name = name;
  op.cmd = cmd;
  op.inbl = inbl;
  op.outbl = outbl;
  op.outs = outs;
  op.on_finish = onfinish;

  if (session && session->con && (name.size() == 0 || map.active_name == name)) {
    // Set fsid argument to signal that this is really a tell message (and
    // we are not a legacy client sending a non-tell command via MCommand).
    auto m = op.get_message(monmap->fsid, false);
    session->con->send_message2(std::move(m));
  } else {
    ldout(cct, 5) << "no mgr session (no running mgr daemon?), or "
		  << name << " not active mgr, waiting" << dendl;
  }
  return 0;
}

bool MgrClient::handle_command_reply(
  uint64_t tid,
  bufferlist& data,
  const std::string& rs,
  int r)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  ldout(cct, 20) << "tid " << tid << " r " << r << dendl;

  if (!command_table.exists(tid)) {
    ldout(cct, 4) << "handle_command_reply tid " << tid
            << " not found" << dendl;
    return true;
  }

  auto &op = command_table.get_command(tid);
  if (op.outbl) {
    *op.outbl = std::move(data);
  }

  if (op.outs) {
    *(op.outs) = rs;
  }

  if (op.on_finish) {
    op.on_finish->complete(r);
  }

  command_table.erase(tid);
  return true;
}

int MgrClient::update_daemon_metadata(
  const std::string& service,
  const std::string& name,
  const std::map<std::string,std::string>& metadata)
{
  std::lock_guard l(lock);
  if (service_daemon) {
    return -EEXIST;
  }
  ldout(cct,1) << service << "." << name << " metadata " << metadata << dendl;
  service_name = service;
  daemon_name = name;
  daemon_metadata = metadata;
  daemon_dirty_status = true;

  if (need_metadata_update &&
      !daemon_metadata.empty()) {
    _send_update();
    need_metadata_update = false;
  }

  return 0;
}

int MgrClient::service_daemon_register(
  const std::string& service,
  const std::string& name,
  const std::map<std::string,std::string>& metadata)
{
  std::lock_guard l(lock);
  if (service_daemon) {
    return -EEXIST;
  }
  ldout(cct,1) << service << "." << name << " metadata " << metadata << dendl;
  service_daemon = true;
  service_name = service;
  daemon_name = name;
  daemon_metadata = metadata;
  daemon_dirty_status = true;

  // late register?
  if (msgr->get_mytype() == CEPH_ENTITY_TYPE_CLIENT && session && session->con) {
    _send_open();
  }

  return 0;
}

int MgrClient::service_daemon_update_status(
  std::map<std::string,std::string>&& status)
{
  std::lock_guard l(lock);
  ldout(cct,10) << status << dendl;
  daemon_status = std::move(status);
  daemon_dirty_status = true;
  return 0;
}

int MgrClient::service_daemon_update_task_status(
  std::map<std::string,std::string> &&status) {
  std::lock_guard l(lock);
  ldout(cct,10) << status << dendl;
  task_status = std::move(status);
  task_dirty_status = true;
  return 0;
}

void MgrClient::update_daemon_health(std::vector<DaemonHealthMetric>&& metrics)
{
  std::lock_guard l(lock);
  daemon_health_metrics = std::move(metrics);
}

