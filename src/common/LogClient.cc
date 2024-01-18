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

#include "common/LogClient.h"
#include "include/str_map.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"
#include "msg/Messenger.h"
#include "mon/MonMap.h"
#include "common/Graylog.h"

#define dout_subsys ceph_subsys_monc

using std::map;
using std::ostream;
using std::ostringstream;
using std::string;

#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, LogClient *logc) {
  return *_dout << "log_client ";
}

static ostream& _prefix(std::ostream *_dout, LogChannel *lc) {
  return *_dout << "log_channel(" << lc->get_log_channel() << ") ";
}

LogChannel::LogChannel(CephContext *cct, LogClient *lc, const string &channel)
  : cct(cct), parent(lc),
    log_channel(channel), log_to_syslog(false), log_to_monitors(false)
{
}

LogChannel::LogChannel(CephContext *cct, LogClient *lc,
                       const string &channel, const string &facility,
                       const string &prio)
  : cct(cct), parent(lc),
    log_channel(channel), log_prio(prio), syslog_facility(facility),
    log_to_syslog(false), log_to_monitors(false)
{
}

LogClient::LogClient(CephContext *cct, Messenger *m, MonMap *mm,
		     enum logclient_flag_t flags)
  : cct(cct), messenger(m), monmap(mm), is_mon(flags & FLAG_MON),
    last_log_sent(0), last_log(0)
{
}

void LogChannel::set_log_to_monitors(bool v)
{
  if (log_to_monitors != v) {
    parent->reset();
    log_to_monitors = v;
  }
}

void LogChannel::update_config(const clog_targets_conf_t& conf_strings)
{
  ldout(cct, 20) << __func__ << " log_to_monitors " << conf_strings.log_to_monitors
		 << " log_to_syslog " << conf_strings.log_to_syslog
		 << " log_channels " << conf_strings.log_channels
		 << " log_prios " << conf_strings.log_prios
		 << dendl;

  bool to_monitors = (conf_strings.log_to_monitors == "true");
  bool to_syslog = (conf_strings.log_to_syslog == "true");
  bool to_graylog = (conf_strings.log_to_graylog == "true");
  auto graylog_port = atoi(conf_strings.log_to_graylog_port.c_str());

  set_log_to_monitors(to_monitors);
  set_log_to_syslog(to_syslog);
  set_syslog_facility(conf_strings.log_channels);
  set_log_prio(conf_strings.log_prios);

  if (to_graylog && !graylog) { /* should but isn't */
    graylog = std::make_shared<ceph::logging::Graylog>("clog");
  } else if (!to_graylog && graylog) { /* shouldn't but is */
    graylog.reset();
  }

  if (to_graylog && graylog) {
    graylog->set_fsid(conf_strings.fsid);
    graylog->set_hostname(conf_strings.host);
  }

  if (graylog && !conf_strings.log_to_graylog_host.empty() && (graylog_port != 0)) {
    graylog->set_destination(conf_strings.log_to_graylog_host, graylog_port);
  }

  ldout(cct, 10) << __func__
		 << " to_monitors: " << (to_monitors ? "true" : "false")
		 << " to_syslog: " << (to_syslog ? "true" : "false")
		 << " syslog_facility: " << conf_strings.log_channels
		 << " prio: " << conf_strings.log_prios
		 << " to_graylog: " << (to_graylog ? "true" : "false")
		 << " graylog_host: " << conf_strings.log_to_graylog_host
		 << " graylog_port: " << graylog_port
		 << ")" << dendl;
}

clog_targets_conf_t LogChannel::parse_client_options(CephContext* conf_cct)
{
  auto parsed_options = parse_log_client_options(conf_cct);
  update_config(parsed_options);
  return parsed_options;
}

clog_targets_conf_t LogChannel::parse_log_client_options(CephContext* cct)
{
  clog_targets_conf_t targets;

  targets.log_to_monitors =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_monitors"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_to_syslog =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_syslog"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_channels =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_syslog_facility"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_prios =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_syslog_level"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_to_graylog =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_graylog"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_to_graylog_host =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_graylog_host"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);
  targets.log_to_graylog_port =
    get_value_via_strmap(cct->_conf.get_val<string>("clog_to_graylog_port"),
                         log_channel, CLOG_CONFIG_DEFAULT_KEY);

  targets.fsid = cct->_conf.get_val<uuid_d>("fsid");
  targets.host = cct->_conf->host;
  return targets;
}

void LogChannel::do_log(clog_type prio, std::stringstream& ss)
{
  while (!ss.eof()) {
    string s;
    getline(ss, s);
    if (!s.empty())
      do_log(prio, s);
  }
}

void LogChannel::do_log(clog_type prio, const std::string& s)
{
  std::lock_guard l(channel_lock);
  if (CLOG_ERROR == prio) {
    ldout(cct,-1) << "log " << prio << " : " << s << dendl;
  } else {
    ldout(cct,0) << "log " << prio << " : " << s << dendl;
  }
  LogEntry e;
  e.stamp = ceph_clock_now();
  // seq and who should be set for syslog/graylog/log_to_mon
  e.addrs = parent->get_myaddrs();
  e.name = parent->get_myname();
  e.rank = parent->get_myrank();
  e.prio = prio;
  e.msg = s;
  e.channel = get_log_channel();

  // log to monitor?
  if (log_to_monitors) {
    e.seq = parent->queue(e);
  } else {
    e.seq = parent->get_next_seq();
  }

  // log to syslog?
  if (do_log_to_syslog()) {
    e.log_to_syslog(get_log_prio(), get_syslog_facility());
  }

  // log to graylog?
  if (do_log_to_graylog()) {
    graylog->log_log_entry(&e);
  }
}

ceph::ref_t<Message> LogClient::get_mon_log_message(bool flush)
{
  std::lock_guard l(log_lock);
  if (flush) {
    if (log_queue.empty())
      return nullptr;
    // reset session
    last_log_sent = log_queue.front().seq;
  }
  return _get_mon_log_message();
}

bool LogClient::are_pending()
{
  std::lock_guard l(log_lock);
  return last_log > last_log_sent;
}

ceph::ref_t<Message> LogClient::_get_mon_log_message()
{
  ceph_assert(ceph_mutex_is_locked(log_lock));
  if (log_queue.empty())
    return {};

  // only send entries that haven't been sent yet during this mon
  // session!  monclient needs to call reset_session() on mon session
  // reset for this to work right.

  if (last_log_sent == last_log)
    return {};

  // limit entries per message
  unsigned num_unsent = last_log - last_log_sent;
  unsigned num_send;
  if (cct->_conf->mon_client_max_log_entries_per_message > 0)
    num_send = std::min(num_unsent, (unsigned)cct->_conf->mon_client_max_log_entries_per_message);
  else
    num_send = num_unsent;

  ldout(cct,10) << " log_queue is " << log_queue.size() << " last_log " << last_log << " sent " << last_log_sent
		<< " num " << log_queue.size()
		<< " unsent " << num_unsent
		<< " sending " << num_send << dendl;
  ceph_assert(num_unsent <= log_queue.size());
  std::deque<LogEntry>::iterator p = log_queue.begin();
  std::deque<LogEntry> o;
  while (p->seq <= last_log_sent) {
    ++p;
    ceph_assert(p != log_queue.end());
  }
  while (num_send--) {
    ceph_assert(p != log_queue.end());
    o.push_back(*p);
    last_log_sent = p->seq;
    ldout(cct,10) << " will send " << *p << dendl;
    ++p;
  }
  
  return ceph::make_message<MLog>(monmap->get_fsid(),
				  std::move(o));
}

void LogClient::_send_to_mon()
{
  ceph_assert(ceph_mutex_is_locked(log_lock));
  ceph_assert(is_mon);
  ceph_assert(messenger->get_myname().is_mon());
  ldout(cct,10) << __func__ << " log to self" << dendl;
  auto log = _get_mon_log_message();
  messenger->get_loopback_connection()->send_message2(std::move(log));
}

version_t LogClient::queue(LogEntry &entry)
{
  std::lock_guard l(log_lock);
  entry.seq = ++last_log;
  log_queue.push_back(entry);

  if (is_mon) {
    _send_to_mon();
  }

  return entry.seq;
}

void LogClient::reset()
{
  std::lock_guard l(log_lock);
  if (log_queue.size()) {
    log_queue.clear();
  }
  last_log_sent = last_log;
}

uint64_t LogClient::get_next_seq()
{
  std::lock_guard l(log_lock);
  return ++last_log;
}

entity_addrvec_t LogClient::get_myaddrs()
{
  return messenger->get_myaddrs();
}

entity_name_t LogClient::get_myrank()
{
  return messenger->get_myname();
}

const EntityName& LogClient::get_myname()
{
  return cct->_conf->name;
}

bool LogClient::handle_log_ack(MLogAck *m)
{
  std::lock_guard l(log_lock);
  ldout(cct,10) << "handle_log_ack " << *m << dendl;

  version_t last = m->last;

  auto q = log_queue.begin();
  while (q != log_queue.end()) {
    const LogEntry &entry(*q);
    if (entry.seq > last)
      break;
    ldout(cct,10) << " logged " << entry << dendl;
    q = log_queue.erase(q);
  }
  return true;
}
