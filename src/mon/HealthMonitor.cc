// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdlib.h>
#include <limits.h>
#include <sstream>
#include <regex>
#include <time.h>
#include <iterator>

#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "include/stringify.h"

#include "mon/Monitor.h"
#include "mon/HealthMonitor.h"

#include "messages/MMonHealthChecks.h"

#include "common/Formatter.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
using namespace TOPNSPC::common;

using namespace std::literals;
using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
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
using ceph::mono_clock;
using ceph::mono_time;
using ceph::parse_timespan;
using ceph::timespan_str;
static ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const HealthMonitor *hmon) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name() << ").health ";
}

HealthMonitor::HealthMonitor(Monitor &m, Paxos &p, const string& service_name)
  : PaxosService(m, p, service_name) {
}

void HealthMonitor::init()
{
  dout(10) << __func__ << dendl;
}

void HealthMonitor::create_initial()
{
  dout(10) << __func__ << dendl;
}

void HealthMonitor::update_from_paxos(bool *need_bootstrap)
{
  version = get_last_committed();
  dout(10) << __func__ << dendl;
  load_health();

  bufferlist qbl;
  mon.store->get(service_name, "quorum", qbl);
  if (qbl.length()) {
    auto p = qbl.cbegin();
    decode(quorum_checks, p);
  } else {
    quorum_checks.clear();
  }

  bufferlist lbl;
  mon.store->get(service_name, "leader", lbl);
  if (lbl.length()) {
    auto p = lbl.cbegin();
    decode(leader_checks, p);
  } else {
    leader_checks.clear();
  }

  {
    bufferlist bl;
    mon.store->get(service_name, "mutes", bl);
    if (bl.length()) {
      auto p = bl.cbegin();
      decode(mutes, p);
    } else {
      mutes.clear();
    }
  }

  dout(20) << "dump:";
  JSONFormatter jf(true);
  jf.open_object_section("health");
  jf.open_object_section("quorum_health");
  for (auto& p : quorum_checks) {
    string s = string("mon.") + stringify(p.first);
    jf.dump_object(s.c_str(), p.second);
  }
  jf.close_section();
  jf.dump_object("leader_health", leader_checks);
  jf.close_section();
  jf.flush(*_dout);
  *_dout << dendl;
}

void HealthMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending_mutes = mutes;
}

void HealthMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  dout(10) << " " << version << dendl;
  put_last_committed(t, version);

  bufferlist qbl;
  encode(quorum_checks, qbl);
  t->put(service_name, "quorum", qbl);
  bufferlist lbl;
  encode(leader_checks, lbl);
  t->put(service_name, "leader", lbl);
  {
    bufferlist bl;
    encode(pending_mutes, bl);
    t->put(service_name, "mutes", bl);
  }

  health_check_map_t pending_health;

  // combine per-mon details carefully...
  map<string,set<string>> names; // code -> <mon names>
  for (auto p : quorum_checks) {
    for (auto q : p.second.checks) {
      names[q.first].insert(mon.monmap->get_name(p.first));
    }
    pending_health.merge(p.second);
  }
  for (auto &p : pending_health.checks) {
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%hasorhave%"),
      names[p.first].size() > 1 ? "have" : "has");
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%names%"), stringify(names[p.first]));
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%plurals%"),
      names[p.first].size() > 1 ? "s" : "");
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%isorare%"),
      names[p.first].size() > 1 ? "are" : "is");
  }

  pending_health.merge(leader_checks);
  encode_health(pending_health, t);
}

version_t HealthMonitor::get_trim_to() const
{
  // we don't actually need *any* old states, but keep a few.
  if (version > 5) {
    return version - 5;
  }
  return 0;
}

bool HealthMonitor::preprocess_query(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return preprocess_command(op);
  case MSG_MON_HEALTH_CHECKS:
    return false;
  default:
    mon.no_reply(op);
    derr << "Unhandled message type " << m->get_type() << dendl;
    return true;
  }
}

bool HealthMonitor::prepare_update(MonOpRequestRef op)
{
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m
	  << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_HEALTH_CHECKS:
    return prepare_health_checks(op);
  case MSG_MON_COMMAND:
    return prepare_command(op);
  default:
    return false;
  }
}

bool HealthMonitor::preprocess_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  std::stringstream ss;
  bufferlist rdata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata,
		       get_last_committed());
    return true;
  }
  // more sanity checks
  try {
    string format;
    cmd_getval(cmdmap, "format", format);
    string prefix;
    cmd_getval(cmdmap, "prefix", prefix);
  } catch (const bad_cmd_get& e) {
    mon.reply_command(op, -EINVAL, e.what(), rdata, get_last_committed());
    return true;
  }
  return false;
}

bool HealthMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();

  std::stringstream ss;
  bufferlist rdata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string format;
  cmd_getval(cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);

  int r = 0;

  if (prefix == "health mute") {
    string code;
    bool sticky = false;
    if (!cmd_getval(cmdmap, "code", code) ||
	code == "") {
      r = -EINVAL;
      ss << "must specify an alert code to mute";
      goto out;
    }
    cmd_getval(cmdmap, "sticky", sticky);
    string ttl_str;
    utime_t ttl;
    std::chrono::seconds secs;
    if (cmd_getval(cmdmap, "ttl", ttl_str)) {
      try {
        secs = parse_timespan(ttl_str);
        if (secs == 0s) {
          throw std::invalid_argument("timespan = 0");
        }
      } catch (const std::invalid_argument& e) {
        ss << "invalid duration: " << ttl_str << " (" << e.what() << ")";
        r = -EINVAL;
        goto out;
      }
      
      ttl = ceph_clock_now();
      ttl += std::chrono::duration<double>(secs).count();
    }
    health_check_map_t all;
    gather_all_health_checks(&all);
    string summary;
    int64_t count = 0;
    if (!sticky) {
      auto p = all.checks.find(code);
      if (p == all.checks.end()) {
	r = -ENOENT;
	ss << "health alert " << code << " is not currently raised";
	goto out;
      }
      count = p->second.count;
      summary = p->second.summary;
    }
    auto& m = pending_mutes[code];
    m.code = code;
    m.ttl = ttl;
    m.sticky = sticky;
    m.summary = summary;
    m.count = count;
  } else if (prefix == "health unmute") {
    string code;
    if (cmd_getval(cmdmap, "code", code)) {
      pending_mutes.erase(code);
    } else {
      pending_mutes.clear();
    }
  } else {
    ss << "Command '" << prefix << "' not implemented!";
    r = -ENOSYS;
  }

out:
  dout(4) << __func__ << " done, r=" << r << dendl;
  /* Compose response */
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, r, rs,
					      get_last_committed() + 1));
    return true;
  } else {
    // reply immediately
    mon.reply_command(op, r, rs, rdata, get_last_committed());
    return false;
  }
}

bool HealthMonitor::prepare_health_checks(MonOpRequestRef op)
{
  auto m = op->get_req<MMonHealthChecks>();
  // no need to check if it's changed, the peon has done so
  quorum_checks[m->get_source().num()] = std::move(m->health_checks);
  return true;
}

void HealthMonitor::tick()
{
  if (!is_active()) {
    return;
  }
  dout(10) << __func__ << dendl;
  bool changed = false;
  if (check_member_health()) {
    changed = true;
  }
  if (!mon.is_leader()) {
    return;
  }
  if (check_leader_health()) {
    changed = true;
  }
  if (check_mutes()) {
    changed = true;
  }
  if (changed) {
    propose_pending();
  }
}

bool HealthMonitor::check_mutes()
{
  bool changed = false;
  auto now = ceph_clock_now();
  health_check_map_t all;
  gather_all_health_checks(&all);
  auto p = pending_mutes.begin();
  while (p != pending_mutes.end()) {
    if (p->second.ttl != utime_t() &&
	p->second.ttl <= now) {
      mon.clog->info() << "Health alert mute " << p->first
			<< " cleared (passed TTL " << p->second.ttl << ")";
      p = pending_mutes.erase(p);
      changed = true;
      continue;
    }
    if (!p->second.sticky) {
      auto q = all.checks.find(p->first);
      if (q == all.checks.end()) {
	mon.clog->info() << "Health alert mute " << p->first
			  << " cleared (health alert cleared)";
	p = pending_mutes.erase(p);
	changed = true;
	continue;
      }
      if (p->second.count) {
	// count-based mute
	if (q->second.count > p->second.count) {
	  mon.clog->info() << "Health alert mute " << p->first
			    << " cleared (count increased from " << p->second.count
			    << " to " << q->second.count << ")";
	  p = pending_mutes.erase(p);
	  changed = true;
	  continue;
	}
	if (q->second.count < p->second.count) {
	  // rachet down the mute
	  dout(10) << __func__ << " mute " << p->first << " count "
		   << p->second.count << " -> " << q->second.count
		   << dendl;
	  p->second.count = q->second.count;
	  changed = true;
	}
      } else {
	// summary-based mute
	if (p->second.summary != q->second.summary) {
	  mon.clog->info() << "Health alert mute " << p->first
			    << " cleared (summary changed)";
	  p = pending_mutes.erase(p);
	  changed = true;
	  continue;
	}
      }
    }
    ++p;
  }
  return changed;
}

void HealthMonitor::gather_all_health_checks(health_check_map_t *all)
{
  for (auto& svc : mon.paxos_service) {
    all->merge(svc->get_health_checks());
  }
}

health_status_t HealthMonitor::get_health_status(
  bool want_detail,
  Formatter *f,
  std::string *plain,
  const char *sep1,
  const char *sep2)
{
  health_check_map_t all;
  gather_all_health_checks(&all);
  health_status_t r = HEALTH_OK;
  for (auto& p : all.checks) {
    if (!mutes.count(p.first)) {
      if (r > p.second.severity) {
	r = p.second.severity;
      }
    }
  }
  if (f) {
    f->open_object_section("health");
    f->dump_stream("status") << r;
    f->open_object_section("checks");
    for (auto& p : all.checks) {
      f->open_object_section(p.first.c_str());
      p.second.dump(f, want_detail);
      f->dump_bool("muted", mutes.count(p.first));
      f->close_section();
    }
    f->close_section();
    f->open_array_section("mutes");
    for (auto& p : mutes) {
      f->dump_object("mute", p.second);
    }
    f->close_section();
    f->close_section();
  } else {
    auto now = ceph_clock_now();
    // one-liner: HEALTH_FOO[ thing1[; thing2 ...]]
    string summary;
    for (auto& p : all.checks) {
      if (!mutes.count(p.first)) {
	if (!summary.empty()) {
	  summary += sep2;
	}
	summary += p.second.summary;
      }
    }
    *plain = stringify(r);
    if (summary.size()) {
      *plain += sep1;
      *plain += summary;
    }
    if (!mutes.empty()) {
      if (summary.size()) {
	*plain += sep2;
      } else {
	*plain += sep1;
      }
      *plain += "(muted:";
      for (auto& p : mutes) {
	*plain += " ";
	*plain += p.first;
	if (p.second.ttl) {
	  if (p.second.ttl > now) {
	    auto left = p.second.ttl;
	    left -= now;
	    *plain += "("s + utimespan_str(left) + ")";
	  } else {
	    *plain += "(0s)";
	  }
	}
      }
      *plain += ")";
    }
    *plain += "\n";
    // detail
    if (want_detail) {
      for (auto& p : all.checks) {
	auto q = mutes.find(p.first);
	if (q != mutes.end()) {
	  *plain += "(MUTED";
	  if (q->second.ttl != utime_t()) {
	    if (q->second.ttl > now) {
	      auto left = q->second.ttl;
	      left -= now;
	      *plain += " ttl ";
	      *plain += utimespan_str(left);
	    } else {
	      *plain += "0s";
	    }
	  }
	  if (q->second.sticky) {
	    *plain += ", STICKY";
	  }
	  *plain += ") ";
	}
	*plain += "["s + short_health_string(p.second.severity) + "] " +
	  p.first + ": " + p.second.summary + "\n";
	for (auto& d : p.second.detail) {
	  *plain += "    ";
	  *plain += d;
	  *plain += "\n";
	}
      }
    }
  }
  return r;
}

bool HealthMonitor::check_member_health()
{
  dout(20) << __func__ << dendl;
  bool changed = false;
  const auto max = g_conf().get_val<uint64_t>("mon_health_max_detail");

  // snapshot of usage
  DataStats stats;
  get_fs_stats(stats.fs_stats, g_conf()->mon_data.c_str());
  map<string,uint64_t> extra;
  uint64_t store_size = mon.store->get_estimated_size(extra);
  ceph_assert(store_size > 0);
  stats.store_stats.bytes_total = store_size;
  stats.store_stats.bytes_sst = extra["sst"];
  stats.store_stats.bytes_log = extra["log"];
  stats.store_stats.bytes_misc = extra["misc"];
  stats.last_update = ceph_clock_now();
  dout(10) << __func__ << " avail " << stats.fs_stats.avail_percent << "%"
	   << " total " << byte_u_t(stats.fs_stats.byte_total)
	   << ", used " << byte_u_t(stats.fs_stats.byte_used)
	   << ", avail " << byte_u_t(stats.fs_stats.byte_avail) << dendl;

  // MON_DISK_{LOW,CRIT,BIG}
  health_check_map_t next;
  if (stats.fs_stats.avail_percent <= g_conf()->mon_data_avail_crit) {
    stringstream ss, ss2;
    ss << "mon%plurals% %names% %isorare% very low on available space";
    auto& d = next.add("MON_DISK_CRIT", HEALTH_ERR, ss.str(), 1);
    ss2 << "mon." << mon.name << " has " << stats.fs_stats.avail_percent
	<< "% avail";
    d.detail.push_back(ss2.str());
  } else if (stats.fs_stats.avail_percent <= g_conf()->mon_data_avail_warn) {
    stringstream ss, ss2;
    ss << "mon%plurals% %names% %isorare% low on available space";
    auto& d = next.add("MON_DISK_LOW", HEALTH_WARN, ss.str(), 1);
    ss2 << "mon." << mon.name << " has " << stats.fs_stats.avail_percent
	<< "% avail";
    d.detail.push_back(ss2.str());
  }
  if (stats.store_stats.bytes_total >= g_conf()->mon_data_size_warn) {
    stringstream ss, ss2;
    ss << "mon%plurals% %names% %isorare% using a lot of disk space";
    auto& d = next.add("MON_DISK_BIG", HEALTH_WARN, ss.str(), 1);
    ss2 << "mon." << mon.name << " is "
	<< byte_u_t(stats.store_stats.bytes_total)
	<< " >= mon_data_size_warn ("
	<< byte_u_t(g_conf()->mon_data_size_warn) << ")";
    d.detail.push_back(ss2.str());
  }

  // OSD_NO_DOWN_OUT_INTERVAL
  {
    // Warn if 'mon_osd_down_out_interval' is set to zero.
    // Having this option set to zero on the leader acts much like the
    // 'noout' flag.  It's hard to figure out what's going wrong with clusters
    // without the 'noout' flag set but acting like that just the same, so
    // we report a HEALTH_WARN in case this option is set to zero.
    // This is an ugly hack to get the warning out, but until we find a way
    // to spread global options throughout the mon cluster and have all mons
    // using a base set of the same options, we need to work around this sort
    // of things.
    // There's also the obvious drawback that if this is set on a single
    // monitor on a 3-monitor cluster, this warning will only be shown every
    // third monitor connection.
    if (g_conf()->mon_warn_on_osd_down_out_interval_zero &&
        g_conf()->mon_osd_down_out_interval == 0) {
      ostringstream ss, ds;
      ss << "mon%plurals% %names% %hasorhave% mon_osd_down_out_interval set to 0";
      auto& d = next.add("OSD_NO_DOWN_OUT_INTERVAL", HEALTH_WARN, ss.str(), 1);
      ds << "mon." << mon.name << " has mon_osd_down_out_interval set to 0";
      d.detail.push_back(ds.str());
    }
  }

  // AUTH_INSECURE_GLOBAL_ID_RECLAIM
  if (g_conf().get_val<bool>("mon_warn_on_insecure_global_id_reclaim") &&
      g_conf().get_val<bool>("auth_allow_insecure_global_id_reclaim")) {
    // Warn if there are any clients that are insecurely renewing their global_id
    std::lock_guard l(mon.session_map_lock);
    list<std::string> detail;
    for (auto p = mon.session_map.sessions.begin();
	 p != mon.session_map.sessions.end();
	 ++p) {
      if ((*p)->global_id_status == global_id_status_t::RECLAIM_INSECURE) {
	ostringstream ds;
	ds << (*p)->entity_name << " at " << (*p)->addrs
	   << " is using insecure global_id reclaim";
	detail.push_back(ds.str());
	if (detail.size() >= max) {
	  detail.push_back("...");
	  break;
	}
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << "client%plurals% %isorare% using insecure global_id reclaim";
      auto& d = next.add("AUTH_INSECURE_GLOBAL_ID_RECLAIM", HEALTH_WARN, ss.str(),
			 detail.size());
      d.detail.swap(detail);
    }
  }
  // AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED
  if (g_conf().get_val<bool>("mon_warn_on_insecure_global_id_reclaim_allowed") &&
      g_conf().get_val<bool>("auth_allow_insecure_global_id_reclaim")) {
    ostringstream ss, ds;
    ss << "mon%plurals% %isorare% allowing insecure global_id reclaim";
    auto& d = next.add("AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED", HEALTH_WARN, ss.str(), 1);
    ds << "mon." << mon.name << " has auth_allow_insecure_global_id_reclaim set to true";
    d.detail.push_back(ds.str());
  }

  auto p = quorum_checks.find(mon.rank);
  if (p == quorum_checks.end()) {
    if (next.empty()) {
      return false;
    }
  } else {
    if (p->second == next) {
      return false;
    }
  }

  if (mon.is_leader()) {
    // prepare to propose
    quorum_checks[mon.rank] = next;
    changed = true;
  } else {
    // tell the leader
    mon.send_mon_message(new MMonHealthChecks(next), mon.get_leader());
  }

  return changed;
}

bool HealthMonitor::check_leader_health()
{
  dout(20) << __func__ << dendl;
  bool changed = false;

  // prune quorum_health
  {
    auto& qset = mon.get_quorum();
    auto p = quorum_checks.begin();
    while (p != quorum_checks.end()) {
      if (qset.count(p->first) == 0) {
	p = quorum_checks.erase(p);
	changed = true;
      } else {
	++p;
      }
    }
  }

  health_check_map_t next;

 // DAEMON_OLD_VERSION
  if (g_conf().get_val<bool>("mon_warn_on_older_version")) {
    check_for_older_version(&next);
  }
  // MON_DOWN
  check_for_mon_down(&next);
  // MON_CLOCK_SKEW
  check_for_clock_skew(&next);
  // MON_MSGR2_NOT_ENABLED
  if (g_conf().get_val<bool>("mon_warn_on_msgr2_not_enabled")) {
    check_if_msgr2_enabled(&next);
  }

  if (next != leader_checks) {
    changed = true;
    leader_checks = next;
  }
  return changed;
}

void HealthMonitor::check_for_older_version(health_check_map_t *checks)
{
  static ceph::coarse_mono_time old_version_first_time =
    ceph::coarse_mono_clock::zero();

  auto now = ceph::coarse_mono_clock::now();
  if (ceph::coarse_mono_clock::is_zero(old_version_first_time)) {
    old_version_first_time = now;
  }
  const auto warn_delay = g_conf().get_val<std::chrono::seconds>("mon_warn_older_version_delay");
  if (now - old_version_first_time > warn_delay) {
    std::map<string, std::list<string> > all_versions;
    mon.get_all_versions(all_versions);
    if (all_versions.size() > 1) {
      dout(20) << __func__ << " all_versions=" << all_versions << dendl;
      // The last entry has the largest version
      dout(20) << __func__ << " highest version daemon count "
	       << all_versions.rbegin()->second.size() << dendl;
      // Erase last element (the highest version running)
      all_versions.erase(all_versions.rbegin()->first);
      ceph_assert(all_versions.size() > 0);
      ostringstream ss;
      unsigned daemon_count = 0;
      for (auto& g : all_versions) {
	daemon_count += g.second.size();
      }
      int ver_count = all_versions.size();
      ceph_assert(!(daemon_count == 1 && ver_count != 1));
      ss << "There " << (daemon_count == 1 ? "is a daemon" : "are daemons")
	 << " running " << (ver_count > 1 ? "multiple old versions" : "an older version")  << " of ceph";
      health_status_t status;
      if (ver_count > 1)
	status = HEALTH_ERR;
      else
	status = HEALTH_WARN;
      auto& d = checks->add("DAEMON_OLD_VERSION", status, ss.str(), all_versions.size());
      for (auto& g : all_versions) {
	ostringstream ds;
	for (auto& i : g.second) { // Daemon list
	  ds << i << " ";
	}
	ds << (g.second.size() == 1 ? "is" : "are")
	   << " running an older version of ceph: " << g.first;
	d.detail.push_back(ds.str());
      }
    } else {
      old_version_first_time = ceph::coarse_mono_clock::zero();
    }
  }
}

void HealthMonitor::check_for_mon_down(health_check_map_t *checks)
{
  int max = mon.monmap->size();
  int actual = mon.get_quorum().size();
  const auto now = ceph::real_clock::now();
  if (actual < max &&
      now > mon.monmap->created.to_real_time() + g_conf().get_val<std::chrono::seconds>("mon_down_mkfs_grace")) {
    ostringstream ss;
    ss << (max-actual) << "/" << max << " mons down, quorum "
       << mon.get_quorum_names();
    auto& d = checks->add("MON_DOWN", HEALTH_WARN, ss.str(), max - actual);
    set<int> q = mon.get_quorum();
    for (int i=0; i<max; i++) {
      if (q.count(i) == 0) {
	ostringstream ss;
	ss << "mon." << mon.monmap->get_name(i) << " (rank " << i
	   << ") addr " << mon.monmap->get_addrs(i)
	   << " is down (out of quorum)";
	d.detail.push_back(ss.str());
      }
    }
  }
}

void HealthMonitor::check_for_clock_skew(health_check_map_t *checks)
{
  if (!mon.timecheck_skews.empty()) {
    list<string> warns;
    list<string> details;
    for (auto& i : mon.timecheck_skews) {
      double skew = i.second;
      double latency = mon.timecheck_latencies[i.first];
      string name = mon.monmap->get_name(i.first);
      ostringstream tcss;
      health_status_t tcstatus = mon.timecheck_status(tcss, skew, latency);
      if (tcstatus != HEALTH_OK) {
	warns.push_back(name);
	ostringstream tmp_ss;
	tmp_ss << "mon." << name << " " << tcss.str()
	       << " (latency " << latency << "s)";
	details.push_back(tmp_ss.str());
      }
    }
    if (!warns.empty()) {
      ostringstream ss;
      ss << "clock skew detected on";
      while (!warns.empty()) {
	ss << " mon." << warns.front();
	warns.pop_front();
	if (!warns.empty())
	  ss << ",";
      }
      auto& d = checks->add("MON_CLOCK_SKEW", HEALTH_WARN, ss.str(), details.size());
      d.detail.swap(details);
    }
  }
}

void HealthMonitor::check_if_msgr2_enabled(health_check_map_t *checks)
{
  if (g_conf().get_val<bool>("ms_bind_msgr2") &&
      mon.monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_NAUTILUS)) {
    list<string> details;
    for (auto& i : mon.monmap->mon_info) {
      if (!i.second.public_addrs.has_msgr2()) {
	ostringstream ds;
	ds << "mon." << i.first << " is not bound to a msgr2 port, only "
	   << i.second.public_addrs;
	details.push_back(ds.str());
      }
    }
    if (!details.empty()) {
      ostringstream ss;
      ss << details.size() << " monitors have not enabled msgr2";
      auto &d = checks->add("MON_MSGR2_NOT_ENABLED", HEALTH_WARN, ss.str(),
			    details.size());
      d.detail.swap(details);
    }
  }
}
