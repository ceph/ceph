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


/*

  -- Storage scheme --

  Pre-quincy:

    - LogSummary contains last N entries for every channel
    - LogSummary (as "full") written on every commit
    - LogSummary contains "keys" which LogEntryKey hash_set for the
      same set of entries (for deduping)

  Quincy+:

    - LogSummary contains, for each channel,
       - start seq
       - end seq (last written seq + 1)
    - LogSummary contains an LRUSet for tracking dups
    - LogSummary written every N commits
    - each LogEntry written in a separate key
       - "%s/%08x" % (channel, seq) -> LogEntry
    - per-commit record includes channel -> begin (trim bounds)
    - 'external_log_to' meta records version to which we have logged externally

*/



#include <boost/algorithm/string/predicate.hpp>

#include <iterator>
#include <sstream>
#include <syslog.h>

#include "LogMonitor.h"
#include "Monitor.h"
#include "MonitorDBStore.h"

#include "messages/MMonCommand.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"
#include "common/Graylog.h"
#include "common/Journald.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/ceph_assert.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "include/compat.h"
#include "include/utime_fmt.h"

#define dout_subsys ceph_subsys_mon

using namespace TOPNSPC::common;

using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::multimap;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;

string LogMonitor::log_channel_info::get_log_file(const string &channel)
{
  dout(25) << __func__ << " for channel '"
	   << channel << "'" << dendl;

  if (expanded_log_file.count(channel) == 0) {
    string fname = expand_channel_meta(
      get_str_map_key(log_file, channel, &CLOG_CONFIG_DEFAULT_KEY),
      channel);
    expanded_log_file[channel] = fname;

    dout(20) << __func__ << " for channel '"
	     << channel << "' expanded to '"
	     << fname << "'" << dendl;
  }
  return expanded_log_file[channel];
}


void LogMonitor::log_channel_info::expand_channel_meta(map<string,string> &m)
{
  dout(20) << __func__ << " expand map: " << m << dendl;
  for (map<string,string>::iterator p = m.begin(); p != m.end(); ++p) {
    m[p->first] = expand_channel_meta(p->second, p->first);
  }
  dout(20) << __func__ << " expanded map: " << m << dendl;
}

string LogMonitor::log_channel_info::expand_channel_meta(
    const string &input,
    const string &change_to)
{
  size_t pos = string::npos;
  string s(input);
  while ((pos = s.find(LOG_META_CHANNEL)) != string::npos) {
    string tmp = s.substr(0, pos) + change_to;
    if (pos+LOG_META_CHANNEL.length() < s.length())
      tmp += s.substr(pos+LOG_META_CHANNEL.length());
    s = tmp;
  }
  dout(20) << __func__ << " from '" << input
	   << "' to '" << s << "'" << dendl;

  return s;
}

bool LogMonitor::log_channel_info::do_log_to_syslog(const string &channel) {
  string v = get_str_map_key(log_to_syslog, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
  // We expect booleans, but they are in k/v pairs, kept
  // as strings, in 'log_to_syslog'. We must ensure
  // compatibility with existing boolean handling, and so
  // we are here using a modified version of how
  // md_config_t::set_val_raw() handles booleans. We will
  // accept both 'true' and 'false', but will also check for
  // '1' and '0'. The main distiction between this and the
  // original code is that we will assume everything not '1',
  // '0', 'true' or 'false' to be 'false'.
  bool ret = false;

  if (boost::iequals(v, "false")) {
    ret = false;
  } else if (boost::iequals(v, "true")) {
    ret = true;
  } else {
    std::string err;
    int b = strict_strtol(v.c_str(), 10, &err);
    ret = (err.empty() && b == 1);
  }

  return ret;
}

ceph::logging::Graylog::Ref LogMonitor::log_channel_info::get_graylog(
    const string &channel)
{
  dout(25) << __func__ << " for channel '"
	   << channel << "'" << dendl;

  if (graylogs.count(channel) == 0) {
    auto graylog(std::make_shared<ceph::logging::Graylog>("mon"));

    graylog->set_fsid(g_conf().get_val<uuid_d>("fsid"));
    graylog->set_hostname(g_conf()->host);
    graylog->set_destination(get_str_map_key(log_to_graylog_host, channel,
					     &CLOG_CONFIG_DEFAULT_KEY),
			     atoi(get_str_map_key(log_to_graylog_port, channel,
						  &CLOG_CONFIG_DEFAULT_KEY).c_str()));

    graylogs[channel] = graylog;
    dout(20) << __func__ << " for channel '"
	     << channel << "' to graylog host '"
	     << log_to_graylog_host[channel] << ":"
	     << log_to_graylog_port[channel]
	     << "'" << dendl;
  }
  return graylogs[channel];
}

ceph::logging::JournaldClusterLogger &LogMonitor::log_channel_info::get_journald()
{
  dout(25) << __func__ << dendl;

  if (!journald) {
    journald = std::make_unique<ceph::logging::JournaldClusterLogger>();
  }
  return *journald;
}

void LogMonitor::log_channel_info::clear()
{
  log_to_syslog.clear();
  syslog_level.clear();
  syslog_facility.clear();
  log_file.clear();
  expanded_log_file.clear();
  log_file_level.clear();
  log_to_graylog.clear();
  log_to_graylog_host.clear();
  log_to_graylog_port.clear();
  log_to_journald.clear();
  graylogs.clear();
  journald.reset();
}

LogMonitor::log_channel_info::log_channel_info() = default;
LogMonitor::log_channel_info::~log_channel_info() = default;


#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, get_last_committed())
static ostream& _prefix(std::ostream *_dout, Monitor &mon, version_t v) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name()
		<< ").log v" << v << " ";
}

ostream& operator<<(ostream &out, const LogMonitor &pm)
{
  return out << "log";
}

/*
 Tick function to update the map based on performance every N seconds
*/

void LogMonitor::tick() 
{
  if (!is_active()) return;

  dout(10) << *this << dendl;

}

void LogMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
  LogEntry e;
  e.name = g_conf()->name;
  e.rank = entity_name_t::MON(mon.rank);
  e.addrs = mon.messenger->get_myaddrs();
  e.stamp = ceph_clock_now();
  e.prio = CLOG_INFO;
  e.channel = CLOG_CHANNEL_CLUSTER;
  std::stringstream ss;
  ss << "mkfs " << mon.monmap->get_fsid();
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

  log_external_backlog();

  if (version == summary.version)
    return;
  ceph_assert(version >= summary.version);

  version_t latest_full = get_version_latest_full();
  dout(10) << __func__ << " latest full " << latest_full << dendl;
  if ((latest_full > 0) && (latest_full > summary.version)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    ceph_assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading summary e" << latest_full << dendl;
    auto p = latest_bl.cbegin();
    decode(summary, p);
    dout(7) << __func__ << " loaded summary e" << summary.version << dendl;
  }

  // walk through incrementals
  while (version > summary.version) {
    bufferlist bl;
    int err = get_version(summary.version+1, bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length());

    auto p = bl.cbegin();
    __u8 struct_v;
    decode(struct_v, p);
    if (struct_v == 1) {
      // legacy pre-quincy commits
      while (!p.end()) {
	LogEntry le;
	le.decode(p);
	dout(7) << "update_from_paxos applying incremental log "
		<< summary.version+1 <<  " " << le << dendl;
	summary.add_legacy(le);
      }
    } else {
      uint32_t num;
      decode(num, p);
      while (num--) {
	LogEntry le;
	le.decode(p);
	dout(7) << "update_from_paxos applying incremental log "
		<< summary.version+1 <<  " " << le << dendl;
	summary.recent_keys.insert(le.key());
	summary.channel_info[le.channel].second++;
	// we may have logged past the (persisted) summary in a prior quorum
	if (version > external_log_to) {
	  log_external(le);
	}
      }
      map<string,version_t> prune_channels_to;
      decode(prune_channels_to, p);
      for (auto& [channel, prune_to] : prune_channels_to) {
	dout(20) << __func__ << " channel " << channel
		 << " pruned to " << prune_to << dendl;
	summary.channel_info[channel].first = prune_to;
      }
      // zero out pre-quincy fields (encode_pending needs this to reliably detect
      // upgrade)
      summary.tail_by_channel.clear();
      summary.keys.clear();
    }

    summary.version++;
    summary.prune(g_conf()->mon_log_max_summary);
  }
  dout(10) << " summary.channel_info " << summary.channel_info << dendl;
  external_log_to = version;
  mon.store->write_meta("external_log_to", stringify(external_log_to));

  check_subs();
}

void LogMonitor::log_external(const LogEntry& le)
{
  string channel = le.channel;
  if (channel.empty()) { // keep retrocompatibility
    channel = CLOG_CHANNEL_CLUSTER;
  }

  if (channels.do_log_to_syslog(channel)) {
    string level = channels.get_level(channel);
    string facility = channels.get_facility(channel);
    if (level.empty() || facility.empty()) {
      derr << __func__ << " unable to log to syslog -- level or facility"
	   << " not defined (level: " << level << ", facility: "
	   << facility << ")" << dendl;
    } else {
      le.log_to_syslog(channels.get_level(channel),
		       channels.get_facility(channel));
    }
  }

  if (channels.do_log_to_graylog(channel)) {
    ceph::logging::Graylog::Ref graylog = channels.get_graylog(channel);
    if (graylog) {
      graylog->log_log_entry(&le);
    }
    dout(7) << "graylog: " << channel << " " << graylog
	    << " host:" << channels.log_to_graylog_host << dendl;
  }

  if (channels.do_log_to_journald(channel)) {
    auto &journald = channels.get_journald();
    journald.log_log_entry(le);
    dout(7) << "journald: " << channel << dendl;
  }

  bool do_stderr = g_conf().get_val<bool>("mon_cluster_log_to_stderr");
  int fd = -1;
  if (g_conf()->mon_cluster_log_to_file) {
    if (this->log_rotated.exchange(false)) {
      this->log_external_close_fds();
    }

    auto p = channel_fds.find(channel);
    if (p == channel_fds.end()) {
      string log_file = channels.get_log_file(channel);
      dout(20) << __func__ << " logging for channel '" << channel
	       << "' to file '" << log_file << "'" << dendl;
      if (!log_file.empty()) {
	fd = ::open(log_file.c_str(), O_WRONLY|O_APPEND|O_CREAT|O_CLOEXEC, 0600);
	if (fd < 0) {
	  int err = -errno;
	  dout(1) << "unable to write to '" << log_file << "' for channel '"
		  << channel << "': " << cpp_strerror(err) << dendl;
	} else {
	  channel_fds[channel] = fd;
	}
      }
    } else {
      fd = p->second;
    }
  }
  if (do_stderr || fd >= 0) {
    fmt::format_to(std::back_inserter(log_buffer), "{}\n", le);

    if (fd >= 0) {
      int err = safe_write(fd, log_buffer.data(), log_buffer.size());
      if (err < 0) {
	dout(1) << "error writing to '" << channels.get_log_file(channel)
		<< "' for channel '" << channel
		<< ": " << cpp_strerror(err) << dendl;
	::close(fd);
	channel_fds.erase(channel);
      }
    }

    if (do_stderr) {
      fmt::print(std::cerr, "{} {}", channel, std::string_view(log_buffer.data(), log_buffer.size()));
    }

    log_buffer.clear();
  }
}

void LogMonitor::log_external_close_fds()
{
  for (auto& [channel, fd] : channel_fds) {
    if (fd >= 0) {
      dout(10) << __func__ << " closing " << channel << " (" << fd << ")" << dendl;
      ::close(fd);
    }
  }
  channel_fds.clear();
}

/// catch external logs up to summary.version
void LogMonitor::log_external_backlog()
{
  if (!external_log_to) {
    std::string cur_str;
    int r = mon.store->read_meta("external_log_to", &cur_str);
    if (r == 0) {
      external_log_to = std::stoull(cur_str);
      dout(10) << __func__ << " initialized external_log_to = " << external_log_to
	       << " (recorded log_to position)" << dendl;
    } else {
      // pre-quincy, we assumed that anything through summary.version was
      // logged externally.
      assert(r == -ENOENT);
      external_log_to = summary.version;
      dout(10) << __func__ << " initialized external_log_to = " << external_log_to
	       << " (summary v " << summary.version << ")" << dendl;
    }
  }
  // we may have logged ahead of summary.version, but never ahead of paxos
  if (external_log_to > get_last_committed()) {
    derr << __func__ << " rewinding external_log_to from " << external_log_to
	 << " -> " << get_last_committed() << " (sync_force? mon rebuild?)" << dendl;
    external_log_to = get_last_committed();
  }
  if (external_log_to >= summary.version) {
    return;
  }
  if (auto first = get_first_committed(); external_log_to < first) {
    derr << __func__ << " local logs at " << external_log_to
	 << ", skipping to " << first << dendl;
    external_log_to = first;
    // FIXME: write marker in each channel log file?
  }
  for (; external_log_to < summary.version; ++external_log_to) {
    bufferlist bl;
    int err = get_version(external_log_to+1, bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length());
    auto p = bl.cbegin();
    __u8 v;
    decode(v, p);
    int32_t num = -2;
    if (v >= 2) {
      decode(num, p);
    }
    while ((num == -2 && !p.end()) || (num >= 0 && num--)) {
      LogEntry le;
      le.decode(p);
      log_external(le);
    }
  }
  mon.store->write_meta("external_log_to", stringify(external_log_to));
}

void LogMonitor::create_pending()
{
  pending_log.clear();
  pending_keys.clear();
  dout(10) << "create_pending v " << (get_last_committed() + 1) << dendl;
}

void LogMonitor::generate_logentry_key(
  const std::string& channel,
  version_t v,
  std::string *out)
{
  out->append(channel);
  out->append("/");
  char vs[10];
  snprintf(vs, sizeof(vs), "%08llx", (unsigned long long)v);
  out->append(vs);
}

void LogMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  version_t version = get_last_committed() + 1;
  bufferlist bl;
  dout(10) << __func__ << " v" << version << dendl;

  if (mon.monmap->min_mon_release < ceph_release_t::quincy) {
    // legacy encoding for pre-quincy quorum
    __u8 struct_v = 1;
    encode(struct_v, bl);
    for (auto& p : pending_log) {
      p.second.encode(bl, mon.get_quorum_con_features());
    }
    put_version(t, version, bl);
    put_last_committed(t, version);
    return;
  }
  
  __u8 struct_v = 2;
  encode(struct_v, bl);

  // first commit after upgrading to quincy?
  if (!summary.tail_by_channel.empty()) {
    // include past log entries
    for (auto& p : summary.tail_by_channel) {
      for (auto& q : p.second) {
	pending_log.emplace(make_pair(q.second.stamp, q.second));
      }
    }
  }

  // record new entries
  auto pending_channel_info = summary.channel_info;
  uint32_t num = pending_log.size();
  encode(num, bl);
  dout(20) << __func__ << " writing " << num << " entries" << dendl;
  for (auto& p : pending_log) {
    bufferlist ebl;
    p.second.encode(ebl, mon.get_quorum_con_features());

    auto& bounds = pending_channel_info[p.second.channel];
    version_t v = bounds.second++;
    std::string key;
    generate_logentry_key(p.second.channel, v, &key);
    t->put(get_service_name(), key, ebl);
    
    bl.claim_append(ebl);
  }

  // prune log entries?
  map<string,version_t> prune_channels_to;
  for (auto& [channel, info] : summary.channel_info) {
    if (info.second - info.first > g_conf()->mon_log_max) {
      const version_t from = info.first;
      const version_t to = info.second - g_conf()->mon_log_max;
      dout(10) << __func__ << " pruning channel " << channel
	       << " " << from << " -> " << to << dendl;
      prune_channels_to[channel] = to;
      pending_channel_info[channel].first = to;
      for (version_t v = from; v < to; ++v) {
	std::string key;
	generate_logentry_key(channel, v, &key);
	t->erase(get_service_name(), key);
      }
    }
  }
  dout(20) << __func__ << " prune_channels_to " << prune_channels_to << dendl;
  encode(prune_channels_to, bl);

  put_version(t, version, bl);
  put_last_committed(t, version);
}

bool LogMonitor::should_stash_full()
{
  if (mon.monmap->min_mon_release < ceph_release_t::quincy) {
    // commit a LogSummary on every commit
    return true;
  }

  // store periodic summary
  auto period = std::min<uint64_t>(
    g_conf()->mon_log_full_interval,
    g_conf()->mon_max_log_epochs
    );
  return (get_last_committed() - get_version_latest_full() > period);
}


void LogMonitor::encode_full(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << " log v " << summary.version << dendl;
  ceph_assert(get_last_committed() == summary.version);

  bufferlist summary_bl;
  encode(summary, summary_bl, mon.get_quorum_con_features());

  put_version_full(t, summary.version, summary_bl);
  put_version_latest_full(t, summary.version);
}

version_t LogMonitor::get_trim_to() const
{
  if (!mon.is_leader())
    return 0;

  unsigned max = g_conf()->mon_max_log_epochs;
  version_t version = get_last_committed();
  if (version > max)
    return version - max;
  return 0;
}

bool LogMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_logmon_event("preprocess_query");
  auto m = op->get_req<PaxosServiceMessage>();
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  case MSG_LOG:
    return preprocess_log(op);

  default:
    ceph_abort();
    return true;
  }
}

bool LogMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_logmon_event("prepare_update");
  auto m = op->get_req<PaxosServiceMessage>();
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case MSG_LOG:
    return prepare_log(op);
  default:
    ceph_abort();
    return false;
  }
}

bool LogMonitor::preprocess_log(MonOpRequestRef op)
{
  op->mark_logmon_event("preprocess_log");
  auto m = op->get_req<MLog>();
  dout(10) << "preprocess_log " << *m << " from " << m->get_orig_source() << dendl;
  int num_new = 0;

  MonSession *session = op->get_session();
  if (!session)
    goto done;
  if (!session->is_capable("log", MON_CAP_W)) {
    dout(0) << "preprocess_log got MLog from entity with insufficient privileges "
	    << session->caps << dendl;
    goto done;
  }
  
  for (auto p = m->entries.begin();
       p != m->entries.end();
       ++p) {
    if (!summary.contains(p->key()))
      num_new++;
  }
  if (!num_new) {
    dout(10) << "  nothing new" << dendl;
    goto done;
  }

  return false;

 done:
  mon.no_reply(op);
  return true;
}

struct LogMonitor::C_Log : public C_MonOp {
  LogMonitor *logmon;
  C_Log(LogMonitor *p, MonOpRequestRef o) :
    C_MonOp(o), logmon(p) {}
  void _finish(int r) override {
    if (r == -ECANCELED) {
      return;
    }
    logmon->_updated_log(op);
  }
};

bool LogMonitor::prepare_log(MonOpRequestRef op) 
{
  op->mark_logmon_event("prepare_log");
  auto m = op->get_req<MLog>();
  dout(10) << "prepare_log " << *m << " from " << m->get_orig_source() << dendl;

  if (m->fsid != mon.monmap->fsid) {
    dout(0) << "handle_log on fsid " << m->fsid << " != " << mon.monmap->fsid 
	    << dendl;
    return false;
  }

  for (auto p = m->entries.begin();
       p != m->entries.end();
       ++p) {
    dout(10) << " logging " << *p << dendl;
    if (!summary.contains(p->key()) &&
	!pending_keys.count(p->key())) {
      pending_keys.insert(p->key());
      pending_log.insert(pair<utime_t,LogEntry>(p->stamp, *p));
    }
  }
  wait_for_commit(op, new C_Log(this, op));
  return true;
}

void LogMonitor::_updated_log(MonOpRequestRef op)
{
  auto m = op->get_req<MLog>();
  dout(7) << "_updated_log for " << m->get_orig_source_inst() << dendl;
  mon.send_reply(op, new MLogAck(m->fsid, m->entries.rbegin()->seq));
}

bool LogMonitor::should_propose(double& delay)
{
  // commit now if we have a lot of pending events
  if (g_conf()->mon_max_log_entries_per_event > 0 &&
      pending_log.size() >= (unsigned)g_conf()->mon_max_log_entries_per_event)
    return true;

  // otherwise fall back to generic policy
  return PaxosService::should_propose(delay);
}


bool LogMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_logmon_event("preprocess_command");
  auto m = op->get_req<MMonCommand>();
  int r = -EINVAL;
  bufferlist rdata;
  stringstream ss;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }
  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "log last") {
    int64_t num = 20;
    cmd_getval(cmdmap, "num", num);
    if (f) {
      f->open_array_section("tail");
    }

    std::string level_str;
    clog_type level;
    if (cmd_getval(cmdmap, "level", level_str)) {
      level = LogEntry::str_to_level(level_str);
      if (level == CLOG_UNKNOWN) {
        ss << "Invalid severity '" << level_str << "'";
        mon.reply_command(op, -EINVAL, ss.str(), get_last_committed());
        return true;
      }
    } else {
      level = CLOG_INFO;
    }

    std::string channel;
    if (!cmd_getval(cmdmap, "channel", channel)) {
      channel = CLOG_CHANNEL_DEFAULT;
    }

    // We'll apply this twice, once while counting out lines
    // and once while outputting them.
    auto match = [level](const LogEntry &entry) {
      return entry.prio >= level;
    };

    ostringstream ss;
    if (!summary.tail_by_channel.empty()) {
      // pre-quincy compat
      // Decrement operation that sets to container end when hitting rbegin
      if (channel == "*") {
	list<LogEntry> full_tail;
	summary.build_ordered_tail_legacy(&full_tail);
	auto rp = full_tail.rbegin();
	for (; num > 0 && rp != full_tail.rend(); ++rp) {
	  if (match(*rp)) {
	    num--;
	  }
	}
	if (rp == full_tail.rend()) {
	  --rp;
	}

	// Decrement a reverse iterator such that going past rbegin()
	// sets it to rend().  This is for writing a for() loop that
	// goes up to (and including) rbegin()
	auto dec = [&rp, &full_tail] () {
		     if (rp == full_tail.rbegin()) {
		       rp = full_tail.rend();
		     } else {
		       --rp;
		     }
		   };

	// Move forward to the end of the container (decrement the reverse
	// iterator).
	for (; rp != full_tail.rend(); dec()) {
	  if (!match(*rp)) {
	    continue;
	  }
	  if (f) {
	    f->dump_object("entry", *rp);
	  } else {
	    ss << *rp << "\n";
	  }
	}
      } else {
	auto p = summary.tail_by_channel.find(channel);
	if (p != summary.tail_by_channel.end()) {
	  auto rp = p->second.rbegin();
	  for (; num > 0 && rp != p->second.rend(); ++rp) {
	    if (match(rp->second)) {
	      num--;
	    }
	  }
	  if (rp == p->second.rend()) {
	    --rp;
	  }

	  // Decrement a reverse iterator such that going past rbegin()
	  // sets it to rend().  This is for writing a for() loop that
	  // goes up to (and including) rbegin()
	  auto dec = [&rp, &p] () {
		       if (rp == p->second.rbegin()) {
			 rp = p->second.rend();
		       } else {
			 --rp;
		       }
		     };
	  
	  // Move forward to the end of the container (decrement the reverse
	  // iterator).
	  for (; rp != p->second.rend(); dec()) {
	    if (!match(rp->second)) {
	      continue;
	    }
	    if (f) {
	      f->dump_object("entry", rp->second);
	    } else {
	      ss << rp->second << "\n";
	    }
	  }
	}
      }
    } else {
      // quincy+
      if (channel == "*") {
	// tail all channels; we need to mix by timestamp
	multimap<utime_t,LogEntry> entries;  // merge+sort all channels by timestamp
	for (auto& p : summary.channel_info) {
	  version_t from = p.second.first;
	  version_t to = p.second.second;
	  version_t start;
	  if (to > (version_t)num) {
	    start = std::max(to - num, from);
	  } else {
	    start = from;
	  }
	  dout(10) << __func__ << " channel " << p.first
		   << " from " << from << " to " << to << dendl;
	  for (version_t v = start; v < to; ++v) {
	    bufferlist ebl;
	    string key;
	    generate_logentry_key(p.first, v, &key);
	    int r = mon.store->get(get_service_name(), key, ebl);
	    if (r < 0) {
	      derr << __func__ << " missing key " << key << dendl;
	      continue;
	    }
	    LogEntry le;
	    auto p = ebl.cbegin();
	    decode(le, p);
	    entries.insert(make_pair(le.stamp, le));
	  }
	}
	while ((int)entries.size() > num) {
	  entries.erase(entries.begin());
	}
	for (auto& p : entries) {
	  if (!match(p.second)) {
	    continue;
	  }
	  if (f) {
	    f->dump_object("entry", p.second);
	  } else {
	    ss << p.second << "\n";
	  }
	}
      } else {
	// tail one channel
	auto p = summary.channel_info.find(channel);
	if (p != summary.channel_info.end()) {
	  version_t from = p->second.first;
	  version_t to = p->second.second;
	  version_t start;
	  if (to > (version_t)num) {
	    start = std::max(to - num, from);
	  } else {
	    start = from;
	  }
	  dout(10) << __func__ << " from " << from << " to " << to << dendl;
	  for (version_t v = start; v < to; ++v) {
	    bufferlist ebl;
	    string key;
	    generate_logentry_key(channel, v, &key);
	    int r = mon.store->get(get_service_name(), key, ebl);
	    if (r < 0) {
	      derr << __func__ << " missing key " << key << dendl;
	      continue;
	    }
	    LogEntry le;
	    auto p = ebl.cbegin();
	    decode(le, p);
	    if (match(le)) {
	      if (f) {
	        f->dump_object("entry", le);
	      } else {
	        ss << le << "\n";
	      }
	    }
	  }
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(ss.str());
    }
    r = 0;
  } else {
    return false;
  }

  string rs;
  getline(ss, rs);
  mon.reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}


bool LogMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_logmon_event("prepare_command");
  auto m = op->get_req<MMonCommand>();
  stringstream ss;
  string rs;
  int err = -EINVAL;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  if (prefix == "log") {
    vector<string> logtext;
    cmd_getval(cmdmap, "logtext", logtext);
    LogEntry le;
    le.rank = m->get_orig_source();
    le.addrs.v.push_back(m->get_orig_source_addr());
    le.name = session->entity_name;
    le.stamp = m->get_recv_stamp();
    le.seq = 0;
    string level_str = cmd_getval_or<string>(cmdmap, "level", "info");
    le.prio = LogEntry::str_to_level(level_str);
    le.channel = CLOG_CHANNEL_DEFAULT;
    le.msg = str_join(logtext, " ");
    pending_keys.insert(le.key());
    pending_log.insert(pair<utime_t,LogEntry>(le.stamp, le));
    wait_for_commit(op, new Monitor::C_Command(
          mon, op, 0, string(), get_last_committed() + 1));
    return true;
  }

  getline(ss, rs);
  mon.reply_command(op, err, rs, get_last_committed());
  return false;
}

void LogMonitor::dump_info(Formatter *f)
{
  f->dump_unsigned("logm_first_committed", get_first_committed());
  f->dump_unsigned("logm_last_committed", get_last_committed());
}

int LogMonitor::sub_name_to_id(const string& n)
{
  if (n.substr(0, 4) == "log-" && n.size() > 4) {
    return LogEntry::str_to_level(n.substr(4));
  } else {
    return CLOG_UNKNOWN;
  }
}

void LogMonitor::check_subs()
{
  dout(10) << __func__ << dendl;
  for (map<string, xlist<Subscription*>*>::iterator i = mon.session_map.subs.begin();
       i != mon.session_map.subs.end();
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
  ceph_assert(sub_level >= 0);

  version_t summary_version = summary.version;
  if (s->next > summary_version) {
    dout(10) << __func__ << " client " << s->session->name
	    << " requested version (" << s->next << ") is greater than ours (" 
	    << summary_version << "), which means we already sent him" 
	    << " everything we have." << dendl;
    return;
  } 
 
  MLog *mlog = new MLog(mon.monmap->fsid);

  if (s->next == 0) { 
    /* First timer, heh? */
    _create_sub_incremental(mlog, sub_level, get_last_committed());
  } else {
    /* let us send you an incremental log... */
    _create_sub_incremental(mlog, sub_level, s->next);
  }

  dout(10) << __func__ << " sending message to " << s->session->name
	  << " with " << mlog->entries.size() << " entries"
	  << " (version " << mlog->version << ")" << dendl;
  
  if (!mlog->entries.empty()) {
    s->session->con->send_message(mlog);
  } else {
    mlog->put();
  }
  if (s->onetime)
    mon.session_map.remove_sub(s);
  else
    s->next = summary_version+1;
}

/**
 * Create an incremental log message from version \p sv to \p summary.version
 *
 * @param mlog	Log message we'll send to the client with the messages received
 *		since version \p sv, inclusive.
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
    le.stamp = ceph_clock_now();
    le.prio = CLOG_WARN;
    ostringstream ss;
    ss << "skipped log messages from " << sv << " to " << get_first_committed();
    le.msg = ss.str();
    mlog->entries.push_back(le);
    sv = get_first_committed();
  }

  version_t summary_ver = summary.version;
  while (sv && sv <= summary_ver) {
    bufferlist bl;
    int err = get_version(sv, bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length());
    auto p = bl.cbegin();
    __u8 v;
    decode(v, p);
    int32_t num = -2;
    if (v >= 2) {
      decode(num, p);
      dout(20) << __func__ << " sv " << sv << " has " << num << " entries" << dendl;
    }
    while ((num == -2 && !p.end()) || (num >= 0 && num--)) {
      LogEntry le;
      le.decode(p);
      if (le.prio < level) {
	dout(20) << __func__ << " requested " << level 
		 << ", skipping " << le << dendl;
	continue;
      }
      mlog->entries.push_back(le);
    }
    mlog->version = sv++;
  }

  dout(10) << __func__ << " incremental message ready (" 
	   << mlog->entries.size() << " entries)" << dendl;
}

void LogMonitor::update_log_channels()
{
  ostringstream oss;

  channels.clear();

  int r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_syslog"),
    oss, &channels.log_to_syslog,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_syslog'" << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_syslog_level"),
    oss, &channels.syslog_level,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_syslog_level'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_syslog_facility"),
    oss, &channels.syslog_facility,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_syslog_facility'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_file"), oss,
    &channels.log_file,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_file'" << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_file_level"), oss,
    &channels.log_file_level,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_file_level'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_graylog"), oss,
    &channels.log_to_graylog,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_graylog'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_graylog_host"), oss,
    &channels.log_to_graylog_host,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_graylog_host'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_graylog_port"), oss,
    &channels.log_to_graylog_port,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_graylog_port'"
         << dendl;
    return;
  }

  r = get_conf_str_map_helper(
    g_conf().get_val<string>("mon_cluster_log_to_journald"), oss,
    &channels.log_to_journald,
    CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    derr << __func__ << " error parsing 'mon_cluster_log_to_journald'"
         << dendl;
    return;
  }

  channels.expand_channel_meta();
  log_external_close_fds();
}


void LogMonitor::handle_conf_change(const ConfigProxy& conf,
                                    const std::set<std::string> &changed)
{
  if (changed.count("mon_cluster_log_to_syslog") ||
      changed.count("mon_cluster_log_to_syslog_level") ||
      changed.count("mon_cluster_log_to_syslog_facility") ||
      changed.count("mon_cluster_log_file") ||
      changed.count("mon_cluster_log_file_level") ||
      changed.count("mon_cluster_log_to_graylog") ||
      changed.count("mon_cluster_log_to_graylog_host") ||
      changed.count("mon_cluster_log_to_graylog_port") ||
      changed.count("mon_cluster_log_to_journald") ||
      changed.count("mon_cluster_log_to_file")) {
    update_log_channels();
  }
}
