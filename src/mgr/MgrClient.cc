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

#include "mgr/MgrContext.h"

#include "msg/Messenger.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"

using std::string;
using std::vector;

using ceph::bufferlist;

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(CephContext *cct_, Messenger *msgr_)
    : Dispatcher(cct_), cct(cct_), msgr(msgr_),
      timer(cct_, lock)
{
  ceph_assert(cct != nullptr);
}

void MgrClient::init()
{
  std::lock_guard l(lock);

  ceph_assert(msgr != nullptr);

  timer.init();
}

void MgrClient::shutdown()
{
  std::lock_guard l(lock);
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
    MMgrClose *m = new MMgrClose();
    m->daemon_name = daemon_name;
    m->service_name = service_name;
    session->con->send_message(m);
    utime_t timeout;
    timeout.set_from_double(cct->_conf.get_val<double>(
			      "mgr_client_service_daemon_unregister_timeout"));
    shutdown_cond.WaitInterval(lock, timeout);
  }

  timer.shutdown();
  if (session) {
    session->con->mark_down();
    session.reset();
  }
}

bool MgrClient::ms_dispatch(Message *m)
{
  std::lock_guard l(lock);

  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(static_cast<MMgrMap*>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_configure(static_cast<MMgrConfigure*>(m));
  case MSG_MGR_CLOSE:
    return handle_mgr_close(static_cast<MMgrClose*>(m));
  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MGR) {
      handle_command_reply(static_cast<MCommandReply*>(m));
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
  ceph_assert(lock.is_locked_by_me());

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

  if (last_connect_attempt != utime_t()) {
    utime_t now = ceph_clock_now();
    utime_t when = last_connect_attempt;
    when += cct->_conf.get_val<double>("mgr_connect_retry_interval");
    if (now < when) {
      if (!connect_retry_callback) {
	connect_retry_callback = timer.add_event_at(
	  when,
	  new FunctionContext([this](int r){
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
  last_connect_attempt = ceph_clock_now();

  session.reset(new MgrSessionState());
  session->con = msgr->connect_to(CEPH_ENTITY_TYPE_MGR,
				  map.get_active_addrs());

  if (service_daemon) {
    daemon_dirty_status = true;
  }

  // Don't send an open if we're just a client (i.e. doing
  // command-sending, not stats etc)
  if (!cct->_conf->name.is_client() || service_daemon) {
    _send_open();
  }

  // resend any pending commands
  for (const auto &p : command_table.get_commands()) {
    auto m = p.second.get_message({});
    ceph_assert(session);
    ceph_assert(session->con);
    session->con->send_message2(std::move(m));
  }
}

void MgrClient::_send_open()
{
  if (session && session->con) {
    auto open = new MMgrOpen();
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
    session->con->send_message(open);
  }
}

bool MgrClient::handle_mgr_map(MMgrMap *m)
{
  ceph_assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  map = m->get_map();
  ldout(cct, 4) << "Got map version " << map.epoch << dendl;
  m->put();

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
      new FunctionContext([this](int) {
	  _send_stats();
	}));
  }
}

void MgrClient::_send_report()
{
  ceph_assert(lock.is_locked_by_me());
  ceph_assert(session);
  report_callback = nullptr;

  auto report = new MMgrReport();
  auto pcc = cct->get_perfcounters_collection();

  pcc->with_counters([this, report](
        const PerfCountersCollectionImpl::CounterMap &by_path)
  {
    // Helper for checking whether a counter should be included
    auto include_counter = [this](
        const PerfCounters::perf_counter_data_any_d &ctr,
        const PerfCounters &perf_counters)
    {
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
	ldout(cct,20) << " declare " << path << dendl;
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

  report->daemon_health_metrics = std::move(daemon_health_metrics);

  cct->_conf.get_config_bl(last_config_bl_version, &report->config_bl,
			    &last_config_bl_version);

  if (get_perf_report_cb) {
    get_perf_report_cb(&report->osd_perf_metric_reports);
  }

  session->con->send_message(report);
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

bool MgrClient::handle_mgr_configure(MMgrConfigure *m)
{
  ceph_assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  if (!session) {
    lderr(cct) << "dropping unexpected configure message" << dendl;
    m->put();
    return true;
  }

  ldout(cct, 4) << "stats_period=" << m->stats_period << dendl;

  if (stats_threshold != m->stats_threshold) {
    ldout(cct, 4) << "updated stats threshold: " << m->stats_threshold << dendl;
    stats_threshold = m->stats_threshold;
  }

  if (set_perf_queries_cb) {
    set_perf_queries_cb(m->osd_perf_metric_queries);
  }

  bool starting = (stats_period == 0) && (m->stats_period != 0);
  stats_period = m->stats_period;
  if (starting) {
    _send_stats();
  }

  m->put();
  return true;
}

bool MgrClient::handle_mgr_close(MMgrClose *m)
{
  service_daemon = false;
  shutdown_cond.Signal();
  m->put();
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
    // Leaving fsid argument null because it isn't used.
    auto m = op.get_message({});
    session->con->send_message2(std::move(m));
  } else {
    ldout(cct, 5) << "no mgr session (no running mgr daemon?), waiting" << dendl;
  }
  return 0;
}

bool MgrClient::handle_command_reply(MCommandReply *m)
{
  ceph_assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  const auto tid = m->get_tid();
  if (!command_table.exists(tid)) {
    ldout(cct, 4) << "handle_command_reply tid " << m->get_tid()
            << " not found" << dendl;
    m->put();
    return true;
  }

  auto &op = command_table.get_command(tid);
  if (op.outbl) {
    op.outbl->claim(m->get_data());
  }

  if (op.outs) {
    *(op.outs) = m->rs;
  }

  if (op.on_finish) {
    op.on_finish->complete(m->r);
  }

  command_table.erase(tid);

  m->put();
  return true;
}

int MgrClient::service_daemon_register(
  const std::string& service,
  const std::string& name,
  const std::map<std::string,std::string>& metadata)
{
  std::lock_guard l(lock);
  if (service == "osd" ||
      service == "mds" ||
      service == "client" ||
      service == "mon" ||
      service == "mgr") {
    // normal ceph entity types are not allowed!
    return -EINVAL;
  }
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
  if (cct->_conf->name.is_client() && session && session->con) {
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

void MgrClient::update_daemon_health(std::vector<DaemonHealthMetric>&& metrics)
{
  std::lock_guard l(lock);
  daemon_health_metrics = std::move(metrics);
}

