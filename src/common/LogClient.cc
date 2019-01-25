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

int parse_log_client_options(CephContext *cct,
			     map<string,string> &log_to_monitors,
			     map<string,string> &log_to_syslog,
			     map<string,string> &log_channels,
			     map<string,string> &log_prios,
			     map<string,string> &log_to_graylog,
			     map<string,string> &log_to_graylog_host,
			     map<string,string> &log_to_graylog_port,
			     uuid_d &fsid,
			     string &host)
{
  ostringstream oss;

  int r = get_conf_str_map_helper(cct->_conf->clog_to_monitors, oss,
                                  &log_to_monitors, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_monitors'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_syslog, oss,
                              &log_to_syslog, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_syslog'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_syslog_facility, oss,
                              &log_channels, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_syslog_facility'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_syslog_level, oss,
                              &log_prios, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_syslog_level'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_graylog, oss,
                              &log_to_graylog, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_graylog'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_graylog_host, oss,
                              &log_to_graylog_host, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_graylog_host'" << dendl;
    return r;
  }

  r = get_conf_str_map_helper(cct->_conf->clog_to_graylog_port, oss,
                              &log_to_graylog_port, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    lderr(cct) << __func__ << " error parsing 'clog_to_graylog_port'" << dendl;
    return r;
  }

  fsid = cct->_conf.get_val<uuid_d>("fsid");
  host = cct->_conf->host;
  return 0;
}

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

LogClientTemp::LogClientTemp(clog_type type_, LogChannel &parent_)
  : type(type_), parent(parent_)
{
}

LogClientTemp::LogClientTemp(const LogClientTemp &rhs)
  : type(rhs.type), parent(rhs.parent)
{
  // don't want to-- nor can we-- copy the ostringstream
}

LogClientTemp::~LogClientTemp()
{
  if (ss.peek() != EOF)
    parent.do_log(type, ss);
}

void LogChannel::update_config(map<string,string> &log_to_monitors,
			       map<string,string> &log_to_syslog,
			       map<string,string> &log_channels,
			       map<string,string> &log_prios,
			       map<string,string> &log_to_graylog,
			       map<string,string> &log_to_graylog_host,
			       map<string,string> &log_to_graylog_port,
			       uuid_d &fsid,
			       string &host)
{
  ldout(cct, 20) << __func__ << " log_to_monitors " << log_to_monitors
		 << " log_to_syslog " << log_to_syslog
		 << " log_channels " << log_channels
		 << " log_prios " << log_prios
		 << dendl;
  bool to_monitors = (get_str_map_key(log_to_monitors, log_channel,
                                      &CLOG_CONFIG_DEFAULT_KEY) == "true");
  bool to_syslog = (get_str_map_key(log_to_syslog, log_channel,
                                    &CLOG_CONFIG_DEFAULT_KEY) == "true");
  string syslog_facility = get_str_map_key(log_channels, log_channel,
					   &CLOG_CONFIG_DEFAULT_KEY);
  string prio = get_str_map_key(log_prios, log_channel,
				&CLOG_CONFIG_DEFAULT_KEY);
  bool to_graylog = (get_str_map_key(log_to_graylog, log_channel,
				     &CLOG_CONFIG_DEFAULT_KEY) == "true");
  string graylog_host = get_str_map_key(log_to_graylog_host, log_channel,
				       &CLOG_CONFIG_DEFAULT_KEY);
  string graylog_port_str = get_str_map_key(log_to_graylog_port, log_channel,
					    &CLOG_CONFIG_DEFAULT_KEY);
  int graylog_port = atoi(graylog_port_str.c_str());

  set_log_to_monitors(to_monitors);
  set_log_to_syslog(to_syslog);
  set_syslog_facility(syslog_facility);
  set_log_prio(prio);

  if (to_graylog && !graylog) { /* should but isn't */
    graylog = std::make_shared<ceph::logging::Graylog>("clog");
  } else if (!to_graylog && graylog) { /* shouldn't but is */
    graylog.reset();
  }

  if (to_graylog && graylog) {
    graylog->set_fsid(fsid);
    graylog->set_hostname(host);
  }

  if (graylog && (!graylog_host.empty()) && (graylog_port != 0)) {
    graylog->set_destination(graylog_host, graylog_port);
  }

  ldout(cct, 10) << __func__
		 << " to_monitors: " << (to_monitors ? "true" : "false")
		 << " to_syslog: " << (to_syslog ? "true" : "false")
		 << " syslog_facility: " << syslog_facility
		 << " prio: " << prio
		 << " to_graylog: " << (to_graylog ? "true" : "false")
		 << " graylog_host: " << graylog_host
		 << " graylog_port: " << graylog_port
		 << ")" << dendl;
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
    ldout(cct,0) << __func__ << " log to syslog"  << dendl;
    e.log_to_syslog(get_log_prio(), get_syslog_facility());
  }

  // log to graylog?
  if (do_log_to_graylog()) {
    ldout(cct,0) << __func__ << " log to graylog"  << dendl;
    graylog->log_log_entry(&e);
  }
}

Message *LogClient::get_mon_log_message(bool flush)
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

Message *LogClient::_get_mon_log_message()
{
  ceph_assert(ceph_mutex_is_locked(log_lock));
  if (log_queue.empty())
    return NULL;

  // only send entries that haven't been sent yet during this mon
  // session!  monclient needs to call reset_session() on mon session
  // reset for this to work right.

  if (last_log_sent == last_log)
    return NULL;

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
  
  MLog *log = new MLog(monmap->get_fsid());
  log->entries.swap(o);

  return log;
}

void LogClient::_send_to_mon()
{
  ceph_assert(ceph_mutex_is_locked(log_lock));
  ceph_assert(is_mon);
  ceph_assert(messenger->get_myname().is_mon());
  ldout(cct,10) << __func__ << " log to self" << dendl;
  Message *log = _get_mon_log_message();
  messenger->get_loopback_connection()->send_message(log);
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

  deque<LogEntry>::iterator q = log_queue.begin();
  while (q != log_queue.end()) {
    const LogEntry &entry(*q);
    if (entry.seq > last)
      break;
    ldout(cct,10) << " logged " << entry << dendl;
    q = log_queue.erase(q);
  }
  return true;
}

