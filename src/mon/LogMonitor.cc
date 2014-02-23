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

#include <sstream>
#include <syslog.h>

#include "LogMonitor.h"
#include "Monitor.h"
#include "MonitorDBStore.h"

#include "messages/MMonCommand.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"

#include "common/Timer.h"

#include "osd/osd_types.h"
#include "common/errno.h"
#include "common/config.h"
#include "include/assert.h"
#include "include/str_list.h"
#include "include/compat.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, get_last_committed())
static ostream& _prefix(std::ostream *_dout, Monitor *mon, version_t v) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").log v" << v << " ";
}

ostream& operator<<(ostream& out, LogMonitor& pm)
{
  /*
  std::stringstream ss;
  for (ceph::unordered_map<int,int>::iterator p = pm.pg_map.num_pg_by_state.begin();
       p != pm.pg_map.num_pg_by_state.end();
       ++p) {
    if (p != pm.pg_map.num_pg_by_state.begin())
      ss << ", ";
    ss << p->second << " " << pg_state_string(p->first);
  }
  string states = ss.str();
  return out << "v" << pm.pg_map.version << ": "
	     << pm.pg_map.pg_stat.size() << " pgs: "
	     << states << "; "
	     << kb_t(pm.pg_map.total_pg_kb()) << " data, " 
	     << kb_t(pm.pg_map.total_used_kb()) << " used, "
	     << kb_t(pm.pg_map.total_avail_kb()) << " / "
	     << kb_t(pm.pg_map.total_kb()) << " free";
  */
  return out << "log";
}

/*
 Tick function to update the map based on performance every N seconds
*/

void LogMonitor::tick() 
{
  if (!is_active()) return;

  dout(10) << *this << dendl;

  if (!mon->is_leader()) return; 

}

void LogMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
  LogEntry e;
  memset(&e.who, 0, sizeof(e.who));
  e.stamp = ceph_clock_now(g_ceph_context);
  e.type = CLOG_INFO;
  std::stringstream ss;
  ss << "mkfs " << mon->monmap->get_fsid();
  e.msg = ss.str();
  e.seq = 0;
  pending_log.insert(pair<utime_t,LogEntry>(e.stamp, e));
}

void LogMonitor::update_from_paxos(bool *need_bootstrap)
{
  dout(10) << __func__ << dendl;
  version_t version = get_last_committed();
  dout(10) << __func__ << " version " << version
           << " summary v " << summary.version << dendl;
  if (version == summary.version)
    return;
  assert(version >= summary.version);

  bufferlist blog;

  version_t latest_full = get_version_latest_full();
  dout(10) << __func__ << " latest full " << latest_full << dendl;
  if ((latest_full > 0) && (latest_full > summary.version)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading summary e" << latest_full << dendl;
    bufferlist::iterator p = latest_bl.begin();
    ::decode(summary, p);
    dout(7) << __func__ << " loaded summary e" << summary.version << dendl;
  }

  // walk through incrementals
  while (version > summary.version) {
    bufferlist bl;
    int err = get_version(summary.version+1, bl);
    assert(err == 0);
    assert(bl.length());

    bufferlist::iterator p = bl.begin();
    __u8 v;
    ::decode(v, p);
    while (!p.end()) {
      LogEntry le;
      le.decode(p);
      dout(7) << "update_from_paxos applying incremental log " << summary.version+1 <<  " " << le << dendl;

      if (g_conf->mon_cluster_log_to_syslog) {
	le.log_to_syslog(g_conf->mon_cluster_log_to_syslog_level,
			 g_conf->mon_cluster_log_to_syslog_facility);
      }
      if (g_conf->mon_cluster_log_file.length()) {
	int min = string_to_syslog_level(g_conf->mon_cluster_log_file_level);
	int l = clog_type_to_syslog_level(le.type);
	if (l <= min) {
	  stringstream ss;
	  ss << le << "\n";
	  blog.append(ss.str());
	}
      }

      summary.add(le);
    }

    summary.version++;
  }


  if (blog.length()) {
    int fd = ::open(g_conf->mon_cluster_log_file.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0600);
    if (fd < 0) {
      int err = -errno;
      dout(1) << "unable to write to " << g_conf->mon_cluster_log_file << ": " << cpp_strerror(err) << dendl;
    } else {
      int err = blog.write_fd(fd);
      if (err < 0) {
	dout(1) << "error writing to " << g_conf->mon_cluster_log_file
		<< ": " << cpp_strerror(err) << dendl;
      }
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
  }

  check_subs();
}

void LogMonitor::store_do_append(MonitorDBStore::Transaction *t,
    const string& key, bufferlist& bl)
{
  bufferlist existing_bl;
  int err = get_value(key, existing_bl);
  assert(err == 0);

  existing_bl.append(bl);
  put_value(t, key, existing_bl);
}

void LogMonitor::create_pending()
{
  pending_log.clear();
  pending_summary = summary;
  dout(10) << "create_pending v " << (get_last_committed() + 1) << dendl;
}

void LogMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  version_t version = get_last_committed() + 1;
  bufferlist bl;
  dout(10) << __func__ << " v" << version << dendl;
  __u8 v = 1;
  ::encode(v, bl);
  multimap<utime_t,LogEntry>::iterator p;
  for (p = pending_log.begin(); p != pending_log.end(); ++p)
    p->second.encode(bl);

  put_version(t, version, bl);
  put_last_committed(t, version);
}

void LogMonitor::encode_full(MonitorDBStore::Transaction *t)
{
  dout(10) << __func__ << " log v " << summary.version << dendl;
  assert(get_last_committed() == summary.version);

  bufferlist summary_bl;
  ::encode(summary, summary_bl);

  put_version_full(t, summary.version, summary_bl);
  put_version_latest_full(t, summary.version);
}

version_t LogMonitor::get_trim_to()
{
  unsigned max = g_conf->mon_max_log_epochs;
  version_t version = get_last_committed();
  if (mon->is_leader() && version > max)
    return version - max;
  return 0;
}

bool LogMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));

  case MSG_LOG:
    return preprocess_log((MLog*)m);

  default:
    assert(0);
    m->put();
    return true;
  }
}

bool LogMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));
  case MSG_LOG:
    return prepare_log((MLog*)m);
  default:
    assert(0);
    m->put();
    return false;
  }
}

bool LogMonitor::preprocess_log(MLog *m)
{
  dout(10) << "preprocess_log " << *m << " from " << m->get_orig_source() << dendl;
  int num_new = 0;

  MonSession *session = m->get_session();
  if (!session)
    goto done;
  if (!session->is_capable("log", MON_CAP_W)) {
    dout(0) << "preprocess_log got MLog from entity with insufficient privileges "
	    << session->caps << dendl;
    goto done;
  }
  
  for (deque<LogEntry>::iterator p = m->entries.begin();
       p != m->entries.end();
       ++p) {
    if (!pending_summary.contains(p->key()))
      num_new++;
  }
  if (!num_new) {
    dout(10) << "  nothing new" << dendl;
    goto done;
  }

  return false;

 done:
  m->put();
  return true;
}

bool LogMonitor::prepare_log(MLog *m) 
{
  dout(10) << "prepare_log " << *m << " from " << m->get_orig_source() << dendl;

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << "handle_log on fsid " << m->fsid << " != " << mon->monmap->fsid 
	    << dendl;
    m->put();
    return false;
  }

  for (deque<LogEntry>::iterator p = m->entries.begin();
       p != m->entries.end();
       ++p) {
    dout(10) << " logging " << *p << dendl;
    if (!pending_summary.contains(p->key())) {
      pending_summary.add(*p);
      pending_log.insert(pair<utime_t,LogEntry>(p->stamp, *p));
    }
  }
  wait_for_finished_proposal(new C_Log(this, m));
  return true;
}

void LogMonitor::_updated_log(MLog *m)
{
  dout(7) << "_updated_log for " << m->get_orig_source_inst() << dendl;
  mon->send_reply(m, new MLogAck(m->fsid, m->entries.rbegin()->seq));

  m->put();
}

bool LogMonitor::should_propose(double& delay)
{
  // commit now if we have a lot of pending events
  if (g_conf->mon_max_log_entries_per_event > 0 &&
      pending_log.size() >= (unsigned)g_conf->mon_max_log_entries_per_event)
    return true;

  // otherwise fall back to generic policy
  return PaxosService::should_propose(delay);
}


bool LogMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, get_last_committed());
    return true;
  } else
    return false;
}


bool LogMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", get_last_committed());
    return true;
  }

  if (prefix == "log") {
    vector<string> logtext;
    cmd_getval(g_ceph_context, cmdmap, "logtext", logtext);
    LogEntry le;
    le.who = m->get_orig_source_inst();
    le.stamp = m->get_recv_stamp();
    le.seq = 0;
    le.type = CLOG_INFO;
    le.msg = str_join(logtext, " ");
    pending_summary.add(le);
    pending_log.insert(pair<utime_t,LogEntry>(le.stamp, le));
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, string(),
					      get_last_committed() + 1));
    return true;
  }

  getline(ss, rs);
  mon->reply_command(m, err, rs, get_last_committed());
  return false;
}


int LogMonitor::sub_name_to_id(const string& n)
{
  if (n == "log-debug")
    return CLOG_DEBUG;
  if (n == "log-info")
    return CLOG_INFO;
  if (n == "log-sec")
    return CLOG_SEC;
  if (n == "log-warn")
    return CLOG_WARN;
  if (n == "log-error")
    return CLOG_ERROR;
  return -1;
}

void LogMonitor::check_subs()
{
  dout(10) << __func__ << dendl;
  for (map<string, xlist<Subscription*>*>::iterator i = mon->session_map.subs.begin();
       i != mon->session_map.subs.end();
       ++i) {
    for (xlist<Subscription*>::iterator j = i->second->begin(); !j.end(); ++j) {
      if (sub_name_to_id((*j)->type) >= 0)
	check_sub(*j);
    }
  }
}

void LogMonitor::check_sub(Subscription *s)
{
  dout(10) << __func__ << " client wants " << s->type << " ver " << s->next << dendl;

  int sub_level = sub_name_to_id(s->type);
  assert(sub_level >= 0);

  version_t summary_version = summary.version;
  if (s->next > summary_version) {
    dout(10) << __func__ << " client " << s->session->inst 
	    << " requested version (" << s->next << ") is greater than ours (" 
	    << summary_version << "), which means we already sent him" 
	    << " everything we have." << dendl;
    return;
  } 
 
  MLog *mlog = new MLog(mon->monmap->fsid);

  if (s->next == 0) { 
    /* First timer, heh? */
    bool ret = _create_sub_summary(mlog, sub_level);
    if (!ret) {
      dout(1) << __func__ << " ret = " << ret << dendl;
      mlog->put();
      return;
    }
  } else {
    /* let us send you an incremental log... */
    _create_sub_incremental(mlog, sub_level, s->next);
  }

  dout(1) << __func__ << " sending message to " << s->session->inst 
	  << " with " << mlog->entries.size() << " entries"
	  << " (version " << mlog->version << ")" << dendl;
  
  mon->messenger->send_message(mlog, s->session->inst);
  if (s->onetime)
    mon->session_map.remove_sub(s);
  else
    s->next = summary_version+1;
}

/**
 * Create a log message containing only the last message in the summary.
 *
 * @param mlog	Log message we'll send to the client.
 * @param level Maximum log level the client is interested in.
 * @return	'true' if we consider we successfully populated @mlog;
 *		'false' otherwise.
 */
bool LogMonitor::_create_sub_summary(MLog *mlog, int level)
{
  dout(10) << __func__ << dendl;

  assert(mlog != NULL);

  if (!summary.tail.size())
    return false;

  list<LogEntry>::reverse_iterator it = summary.tail.rbegin();
  for (; it != summary.tail.rend(); ++it) {
    LogEntry e = *it;
    if (e.type < level)
      continue;

    mlog->entries.push_back(e);
    mlog->version = summary.version;
    break;
  }

  return true;
}

/**
 * Create an incremental log message from version @sv to @summary.version
 *
 * @param mlog	Log message we'll send to the client with the messages received
 *		since version @sv, inclusive.
 * @param level	The max log level of the messages the client is interested in.
 * @param sv	The version the client is looking for.
 */
void LogMonitor::_create_sub_incremental(MLog *mlog, int level, version_t sv)
{
  dout(10) << __func__ << " level " << level << " ver " << sv 
	  << " cur summary ver " << summary.version << dendl; 

  if (sv < get_first_committed()) {
    dout(10) << __func__ << " skipped from " << sv
	     << " to first_committed " << get_first_committed() << dendl;
    LogEntry le;
    le.stamp = ceph_clock_now(NULL);
    le.type = CLOG_WARN;
    ostringstream ss;
    ss << "skipped log messages from " << sv << " to " << get_first_committed();
    le.msg = ss.str();
    mlog->entries.push_back(le);
    sv = get_first_committed();
  }

  version_t summary_ver = summary.version;
  while (sv <= summary_ver) {
    bufferlist bl;
    int err = get_version(sv, bl);
    assert(err == 0);
    assert(bl.length());
    bufferlist::iterator p = bl.begin();
    __u8 v;
    ::decode(v,p);
    while (!p.end()) {
      LogEntry le;
      le.decode(p);

      if (le.type < level) {
	dout(20) << __func__ << " requested " << level 
		 << " entry " << le.type << dendl;
	continue;
      }

      mlog->entries.push_back(le);
    }
    mlog->version = sv++;
  }

  dout(10) << __func__ << " incremental message ready (" 
	   << mlog->entries.size() << " entries)" << dendl;
}

