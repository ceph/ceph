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
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(CephContext *cct_, Messenger *msgr_)
    : Dispatcher(cct_), cct(cct_), msgr(msgr_),
      timer(cct_, lock)
{
  assert(cct != nullptr);
}

void MgrClient::init()
{
  Mutex::Locker l(lock);

  assert(msgr != nullptr);

  timer.init();
}

void MgrClient::shutdown()
{
  Mutex::Locker l(lock);

  if (connect_retry_callback) {
    timer.cancel_event(connect_retry_callback);
    connect_retry_callback = nullptr;
  }

  // forget about in-flight commands if we are prematurely shut down
  // (e.g., by control-C)
  command_table.clear();

  timer.shutdown();
  if (session) {
    session->con->mark_down();
    session.reset();
  }
}

bool MgrClient::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(static_cast<MMgrMap*>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_configure(static_cast<MMgrConfigure*>(m));
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
  assert(lock.is_locked_by_me());

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
    when += cct->_conf->get_val<double>("mgr_connect_retry_interval");
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

  ldout(cct, 4) << "Starting new session with " << map.get_active_addr()
		<< dendl;
  entity_inst_t inst;
  inst.addr = map.get_active_addr();
  inst.name = entity_name_t::MGR(map.get_active_gid());
  last_connect_attempt = ceph_clock_now();

  session.reset(new MgrSessionState());
  session->con = msgr->get_connection(inst);

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
    MCommand *m = p.second.get_message({});
    assert(session);
    assert(session->con);
    session->con->send_message(m);
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
    session->con->send_message(open);
  }
}

bool MgrClient::handle_mgr_map(MMgrMap *m)
{
  assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  map = m->get_map();
  ldout(cct, 4) << "Got map version " << map.epoch << dendl;
  m->put();

  ldout(cct, 4) << "Active mgr is now " << map.get_active_addr() << dendl;

  // Reset session?
  if (!session ||
      session->con->get_peer_addr() != map.get_active_addr()) {
    reconnect();
  }

  return true;
}

bool MgrClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(lock);
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

void MgrClient::send_report()
{
  assert(lock.is_locked_by_me());
  assert(session);
  report_callback = nullptr;

  auto report = new MMgrReport();
  auto pcc = cct->get_perfcounters_collection();

  pcc->with_counters([this, report](
        const PerfCountersCollection::CounterMap &by_path)
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
	report->declare_types.push_back(std::move(type));
	session->declared.insert(path);
      }

      ::encode(static_cast<uint64_t>(data.u64), report->packed);
      if (data.type & PERFCOUNTER_LONGRUNAVG) {
        ::encode(static_cast<uint64_t>(data.avgcount), report->packed);
        ::encode(static_cast<uint64_t>(data.avgcount2), report->packed);
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

  session->con->send_message(report);

  if (stats_period != 0) {
    report_callback = new FunctionContext([this](int r){send_report();});
    timer.add_event_after(stats_period, report_callback);
  }

  send_pgstats();
}

void MgrClient::send_pgstats()
{
  if (pgstats_cb && session) {
    session->con->send_message(pgstats_cb());
  }
}

bool MgrClient::handle_mgr_configure(MMgrConfigure *m)
{
  assert(lock.is_locked_by_me());

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

  bool starting = (stats_period == 0) && (m->stats_period != 0);
  stats_period = m->stats_period;
  if (starting) {
    send_report();
  }

  m->put();
  return true;
}

int MgrClient::start_command(const vector<string>& cmd, const bufferlist& inbl,
                  bufferlist *outbl, string *outs,
                  Context *onfinish)
{
  Mutex::Locker l(lock);

  ldout(cct, 20) << "cmd: " << cmd << dendl;

  if (map.epoch == 0) {
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
    MCommand *m = op.get_message({});
    session->con->send_message(m);
  }
  return 0;
}

bool MgrClient::handle_command_reply(MCommandReply *m)
{
  assert(lock.is_locked_by_me());

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
  Mutex::Locker l(lock);
  if (name == "osd" ||
      name == "mds" ||
      name == "client" ||
      name == "mon" ||
      name == "mgr") {
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
  const std::map<std::string,std::string>& status)
{
  Mutex::Locker l(lock);
  ldout(cct,10) << status << dendl;
  daemon_status = status;
  daemon_dirty_status = true;
  return 0;
}
