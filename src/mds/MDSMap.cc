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

#include <ostream>

#include "common/debug.h"
#include "mon/health_check.h"

#include "MDSMap.h"

using std::dec;
using std::hex;
using std::list;
using std::make_pair;
using std::map;
using std::multimap;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::Formatter;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

// features
CompatSet MDSMap::get_compat_set_all() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_OMAPDIRFRAG);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_NOANCHOR);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_SNAPREALM_V2);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_QUIESCE_SUBVOLUMES);

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

CompatSet MDSMap::get_compat_set_default() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_OMAPDIRFRAG);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_NOANCHOR);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_SNAPREALM_V2);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_QUIESCE_SUBVOLUMES);

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

// base (pre v0.20)
CompatSet MDSMap::get_compat_set_base() {
  CompatSet::FeatureSet feature_compat_base;
  CompatSet::FeatureSet feature_incompat_base;
  feature_incompat_base.insert(MDS_FEATURE_INCOMPAT_BASE);
  CompatSet::FeatureSet feature_ro_compat_base;

  return CompatSet(feature_compat_base, feature_ro_compat_base, feature_incompat_base);
}

// pre-v16.2.5 CompatSet in MDS beacon
CompatSet MDSMap::get_compat_set_v16_2_4() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_OMAPDIRFRAG);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_NOANCHOR);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_SNAPREALM_V2);
  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

void MDSMap::mds_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("gid", global_id);
  f->dump_string("name", name);
  f->dump_int("rank", rank);
  f->dump_int("incarnation", inc);
  f->dump_stream("state") << ceph_mds_state_name(state);
  f->dump_int("state_seq", state_seq);
  f->dump_stream("addr") << addrs.get_legacy_str();
  f->dump_object("addrs", addrs);
  f->dump_int("join_fscid", join_fscid);
  if (laggy_since != utime_t())
    f->dump_stream("laggy_since") << laggy_since;
  
  f->open_array_section("export_targets");
  for (set<mds_rank_t>::iterator p = export_targets.begin();
       p != export_targets.end(); ++p) {
    f->dump_int("mds", *p);
  }
  f->close_section();
  f->dump_unsigned("features", mds_features);
  f->dump_unsigned("flags", flags);
  f->dump_object("compat", compat);
}

void MDSMap::mds_info_t::dump(std::ostream& o) const
{
  o << "[mds." << name << "{" <<  rank << ":" << global_id << "}"
       << " state " << ceph_mds_state_name(state)
       << " seq " << state_seq;
  if (laggy()) {
    o << " laggy since " << laggy_since;
  }
  if (!export_targets.empty()) {
    o << " export targets " << export_targets;
  }
  if (is_frozen()) {
    o << " frozen";
  }
  if (join_fscid != FS_CLUSTER_ID_NONE) {
    o << " join_fscid=" << join_fscid;
  }
  o << " addr " << addrs;
  o << " compat ";
  compat.printlite(o);
  o << "]";
}

void MDSMap::mds_info_t::generate_test_instances(std::list<mds_info_t*>& ls)
{
  mds_info_t *sample = new mds_info_t();
  ls.push_back(sample);
  sample = new mds_info_t();
  sample->global_id = 1;
  sample->name = "test_instance";
  sample->rank = 0;
  ls.push_back(sample);
}

void MDSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_unsigned("flags", flags);
  dump_flags_state(f);
  f->dump_unsigned("ever_allowed_features", ever_allowed_features);
  f->dump_unsigned("explicitly_allowed_features", explicitly_allowed_features);
  f->dump_stream("created") << created;
  f->dump_stream("modified") << modified;
  f->dump_int("tableserver", tableserver);
  f->dump_int("root", root);
  f->dump_int("session_timeout", session_timeout);
  f->dump_int("session_autoclose", session_autoclose);
  f->open_object_section("required_client_features");
  cephfs_dump_features(f, required_client_features);
  f->close_section();
  f->dump_int("max_file_size", max_file_size);
  f->dump_int("max_xattr_size", max_xattr_size);
  f->dump_int("last_failure", last_failure);
  f->dump_int("last_failure_osd_epoch", last_failure_osd_epoch);
  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();
  f->dump_int("max_mds", max_mds);
  f->open_array_section("in");
  for (set<mds_rank_t>::const_iterator p = in.begin(); p != in.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("up");
  for (map<mds_rank_t,mds_gid_t>::const_iterator p = up.begin(); p != up.end(); ++p) {
    char s[14];
    sprintf(s, "mds_%d", int(p->first));
    f->dump_int(s, p->second);
  }
  f->close_section();
  f->open_array_section("failed");
  for (set<mds_rank_t>::const_iterator p = failed.begin(); p != failed.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("damaged");
  for (set<mds_rank_t>::const_iterator p = damaged.begin(); p != damaged.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("stopped");
  for (set<mds_rank_t>::const_iterator p = stopped.begin(); p != stopped.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("info");
  for (const auto& [gid, info] : mds_info) {
    char s[25]; // 'gid_' + len(str(ULLONG_MAX)) + '\0'
    sprintf(s, "gid_%llu", (long long unsigned)gid);
    f->open_object_section(s);
    info.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("data_pools");
  for (const auto& p: data_pools)
    f->dump_int("pool", p);
  f->close_section();
  f->dump_int("metadata_pool", metadata_pool);
  f->dump_bool("enabled", enabled);
  f->dump_string("fs_name", fs_name);
  f->dump_string("balancer", balancer);
  f->dump_string("bal_rank_mask", bal_rank_mask);
  f->dump_int("standby_count_wanted", std::max(0, standby_count_wanted));
}

void MDSMap::dump_flags_state(Formatter *f) const
{
    f->open_object_section("flags_state");
    f->dump_bool(flag_display.at(CEPH_MDSMAP_NOT_JOINABLE), joinable());
    f->dump_bool(flag_display.at(CEPH_MDSMAP_ALLOW_SNAPS), allows_snaps());
    f->dump_bool(flag_display.at(CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS), allows_multimds_snaps());
    f->dump_bool(flag_display.at(CEPH_MDSMAP_ALLOW_STANDBY_REPLAY), allows_standby_replay());
    f->dump_bool(flag_display.at(CEPH_MDSMAP_REFUSE_CLIENT_SESSION), test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION));
    f->dump_bool(flag_display.at(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS), test_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS));
    f->close_section();
}

void MDSMap::generate_test_instances(std::list<MDSMap*>& ls)
{
  MDSMap *m = new MDSMap();
  m->max_mds = 1;
  m->data_pools.push_back(0);
  m->metadata_pool = 1;
  m->cas_pool = 2;
  m->compat = get_compat_set_all();

  // these aren't the defaults, just in case anybody gets confused
  m->session_timeout = 61;
  m->session_autoclose = 301;
  m->max_file_size = 1<<24;
  ls.push_back(m);
}

void MDSMap::print(ostream& out) const
{
  out << "fs_name\t" << fs_name << "\n";
  out << "epoch\t" << epoch << "\n";
  out << "flags\t" << hex << flags << dec;
  print_flags(out);
  out << "\n";
  out << "created\t" << created << "\n";
  out << "modified\t" << modified << "\n";
  out << "tableserver\t" << tableserver << "\n";
  out << "root\t" << root << "\n";
  out << "session_timeout\t" << session_timeout << "\n"
      << "session_autoclose\t" << session_autoclose << "\n";
  out << "max_file_size\t" << max_file_size << "\n";
  out << "max_xattr_size\t" << max_xattr_size << "\n";
  out << "required_client_features\t" << cephfs_stringify_features(required_client_features) << "\n";
  out << "last_failure\t" << last_failure << "\n"
      << "last_failure_osd_epoch\t" << last_failure_osd_epoch << "\n";
  out << "compat\t" << compat << "\n";
  out << "max_mds\t" << max_mds << "\n";
  out << "in\t" << in << "\n"
      << "up\t" << up << "\n"
      << "failed\t" << failed << "\n"
      << "damaged\t" << damaged << "\n"
      << "stopped\t" << stopped << "\n";
  out << "data_pools\t" << data_pools << "\n";
  out << "metadata_pool\t" << metadata_pool << "\n";
  out << "inline_data\t" << (inline_data_enabled ? "enabled" : "disabled") << "\n";
  out << "balancer\t" << balancer << "\n";
  out << "bal_rank_mask\t" << bal_rank_mask << "\n";
  out << "standby_count_wanted\t" << std::max(0, standby_count_wanted) << "\n";

  multimap< pair<mds_rank_t, unsigned>, mds_gid_t > foo;
  for (const auto &p : mds_info) {
    foo.insert(std::make_pair(
          std::make_pair(p.second.rank, p.second.inc-1), p.first));
  }

  for (const auto &p : foo) {
    out << mds_info.at(p.second) << "\n";
  }
}

void MDSMap::print_summary(Formatter *f, ostream *out) const
{
  map<mds_rank_t,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    f->dump_unsigned("up", up.size());
    f->dump_unsigned("in", in.size());
    f->dump_unsigned("max", max_mds);
  } else {
    *out << "e" << get_epoch() << ": " << up.size() << "/" << in.size() << "/" << max_mds << " up";
  }

  if (f)
    f->open_array_section("by_rank");
  for (const auto &p : mds_info) {
    string s = ceph_mds_state_name(p.second.state);
    if (p.second.laggy())
      s += "(laggy or crashed)";

    if (p.second.rank >= 0 && p.second.state != MDSMap::STATE_STANDBY_REPLAY) {
      if (f) {
	f->open_object_section("mds");
	f->dump_unsigned("rank", p.second.rank);
	f->dump_string("name", p.second.name);
	f->dump_string("status", s);
	f->close_section();
      } else {
	by_rank[p.second.rank] = p.second.name + "=" + s;
      }
    } else {
      by_state[s]++;
    }
  }
  if (f) {
    f->close_section();
  } else {
    if (!by_rank.empty())
      *out << " " << by_rank;
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  if (!failed.empty()) {
    if (f) {
      f->dump_unsigned("failed", failed.size());
    } else {
      *out << ", " << failed.size() << " failed";
    }
  }

  if (!damaged.empty()) {
    if (f) {
      f->dump_unsigned("damaged", damaged.size());
    } else {
      *out << ", " << damaged.size() << " damaged";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void MDSMap::print_flags(std::ostream& out) const {
 if (joinable())
    out << " " << flag_display.at(CEPH_MDSMAP_NOT_JOINABLE);
  if (allows_snaps())
    out << " " << flag_display.at(CEPH_MDSMAP_ALLOW_SNAPS);
  if (allows_multimds_snaps())
    out << " " << flag_display.at(CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS);
  if (allows_standby_replay())
    out << " " << flag_display.at(CEPH_MDSMAP_ALLOW_STANDBY_REPLAY);
  if (test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION))
    out << " " << flag_display.at(CEPH_MDSMAP_REFUSE_CLIENT_SESSION);
  if (test_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS))
    out << " " << flag_display.at(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS);
}

void MDSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  if (!failed.empty()) {
    CachedStackStringStream css;
    *css << "mds rank"
	<< ((failed.size() > 1) ? "s ":" ")
	<< failed
	<< ((failed.size() > 1) ? " have":" has")
	<< " failed";
    summary.push_back(make_pair(HEALTH_ERR, css->str()));
    if (detail) {
      for (const auto& r : failed) {
        CachedStackStringStream css;
	*css << "mds." << r << " has failed";
	detail->push_back(make_pair(HEALTH_ERR, css->str()));
      }
    }
  }

  if (!damaged.empty()) {
    CachedStackStringStream css;
    *css << "mds rank"
	 << ((damaged.size() > 1) ? "s ":" ")
	 << damaged
	 << ((damaged.size() > 1) ? " are":" is")
	 << " damaged";
    summary.push_back(make_pair(HEALTH_ERR, css->str()));
    if (detail) {
      for (const auto& r : damaged) {
        CachedStackStringStream css;
	*css << "mds." << r << " is damaged";
	detail->push_back(make_pair(HEALTH_ERR, css->str()));
      }
    }
  }

  if (is_degraded()) {
    summary.push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
    if (detail) {
      detail->push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
      for (mds_rank_t i = mds_rank_t(0); i< get_max_mds(); i++) {
	if (!is_up(i))
	  continue;
	mds_gid_t gid = up.find(i)->second;
	const auto& info = mds_info.at(gid);
        CachedStackStringStream css;
	if (is_resolve(i))
	  *css << "mds." << info.name << " at " << info.addrs
	     << " rank " << i << " is resolving";
	if (is_replay(i))
	  *css << "mds." << info.name << " at " << info.addrs
	     << " rank " << i << " is replaying journal";
	if (is_rejoin(i))
	  *css << "mds." << info.name << " at " << info.addrs
	     << " rank " << i << " is rejoining";
	if (is_reconnect(i))
	  *css << "mds." << info.name << " at " << info.addrs
	     << " rank " << i << " is reconnecting to clients";
	if (css->strv().length())
	  detail->push_back(make_pair(HEALTH_WARN, css->str()));
      }
    }
  }

  {
    CachedStackStringStream css;
    *css << fs_name << " max_mds " << max_mds;
    summary.push_back(make_pair(HEALTH_WARN, css->str()));
  }

  if ((mds_rank_t)up.size() < max_mds) {
    CachedStackStringStream css;
    *css << fs_name << " has " << up.size()
         << " active MDS(s), but has max_mds of " << max_mds;
    summary.push_back(make_pair(HEALTH_WARN, css->str()));
  }

  set<string> laggy;
  for (const auto &u : up) {
    const auto& info = mds_info.at(u.second);
    if (info.laggy()) {
      laggy.insert(info.name);
      if (detail) {
        CachedStackStringStream css;
	*css << "mds." << info.name << " at " << info.addrs
	    << " is laggy/unresponsive";
	detail->push_back(make_pair(HEALTH_WARN, css->str()));
      }
    }
  }

  if (!laggy.empty()) {
    CachedStackStringStream css;
    *css << "mds " << laggy
	 << ((laggy.size() > 1) ? " are":" is")
	 << " laggy";
    summary.push_back(make_pair(HEALTH_WARN, css->str()));
  }

  if (get_max_mds() > 1 &&
      was_snaps_ever_allowed() && !allows_multimds_snaps()) {
    CachedStackStringStream css;
    *css << "multi-active mds while there are snapshots possibly created by pre-mimic MDS";
    summary.push_back(make_pair(HEALTH_WARN, css->str()));
  }
}

void MDSMap::get_health_checks(health_check_map_t *checks) const
{
  // MDS_DAMAGE
  if (!damaged.empty()) {
    health_check_t& check = checks->get_or_add("MDS_DAMAGE", HEALTH_ERR,
					       "%num% mds daemon%plurals% damaged",
					       damaged.size());
    for (const auto& p : damaged) {
      CachedStackStringStream css;
      *css << "fs " << fs_name << " mds." << p << " is damaged";
      check.detail.push_back(css->str());
    }
  }

  // FS_DEGRADED
  if (is_degraded()) {
    health_check_t& fscheck = checks->get_or_add(
      "FS_DEGRADED", HEALTH_WARN,
      "%num% filesystem%plurals% %isorare% degraded", 1);
    CachedStackStringStream css;
    *css << "fs " << fs_name << " is degraded";
    fscheck.detail.push_back(css->str());

    list<string> detail;
    for (mds_rank_t i = mds_rank_t(0); i< get_max_mds(); i++) {
      if (!is_up(i))
	continue;
      mds_gid_t gid = up.find(i)->second;
      const auto& info = mds_info.at(gid);
      CachedStackStringStream css;
      *css << "fs " << fs_name << " mds." << info.name << " at "
	 << info.addrs << " rank " << i;
      if (is_resolve(i))
	*css << " is resolving";
      if (is_replay(i))
	*css << " is replaying journal";
      if (is_rejoin(i))
	*css << " is rejoining";
      if (is_reconnect(i))
	*css << " is reconnecting to clients";
      if (css->strv().length())
	detail.push_back(css->str());
    }
  }

  // MDS_UP_LESS_THAN_MAX
  if ((mds_rank_t)get_num_in_mds() < get_max_mds()) {
    health_check_t& check = checks->add(
      "MDS_UP_LESS_THAN_MAX", HEALTH_WARN,
      "%num% filesystem%plurals% %isorare% online with fewer MDS than max_mds", 1);
    CachedStackStringStream css;
    *css << "fs " << fs_name << " has " << get_num_in_mds()
         << " MDS online, but wants " << get_max_mds();
    check.detail.push_back(css->str());
  }

  // MDS_ALL_DOWN
  if ((mds_rank_t)get_num_up_mds() == 0 && get_max_mds() > 0) {
    health_check_t &check = checks->add(
      "MDS_ALL_DOWN", HEALTH_ERR,
      "%num% filesystem%plurals% %isorare% offline", 1);
    CachedStackStringStream css;
    *css << "fs " << fs_name << " is offline because no MDS is active for it.";
    check.detail.push_back(css->str());
  }

  if (get_max_mds() > 1 &&
      was_snaps_ever_allowed() && !allows_multimds_snaps()) {
    health_check_t &check = checks->add(
      "MULTIMDS_WITH_OLDSNAPS", HEALTH_ERR,
      "%num% filesystem%plurals% %isorare% multi-active mds with old snapshots", 1);
    CachedStackStringStream css;
    *css << "multi-active mds while there are snapshots possibly created by pre-mimic MDS";
    check.detail.push_back(css->str());
  }

  if (get_inline_data_enabled()) {
    health_check_t &check = checks->add(
      "FS_INLINE_DATA_DEPRECATED", HEALTH_WARN,
      "%num% filesystem%plurals% with deprecated feature inline_data", 1);
    CachedStackStringStream css;
    *css << "fs " << fs_name << " has deprecated feature inline_data enabled.";
    check.detail.push_back(css->str());
  }
}

void MDSMap::mds_info_t::encode_versioned(bufferlist& bl, uint64_t features) const
{
  __u8 v = 10;
  if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
    v = 7;
  }
  ENCODE_START(v, 4, bl);
  encode(global_id, bl);
  encode(name, bl);
  encode(rank, bl);
  encode(inc, bl);
  encode((int32_t)state, bl);
  encode(state_seq, bl);
  if (v < 8) {
    encode(addrs.legacy_addr(), bl, features);
  } else {
    encode(addrs, bl, features);
  }
  encode(laggy_since, bl);
  encode(MDS_RANK_NONE, bl); /* standby_for_rank */
  encode(std::string(), bl); /* standby_for_name */
  encode(export_targets, bl);
  encode(mds_features, bl);
  encode(join_fscid, bl); /* formerly: standby_for_fscid */
  encode(false, bl);
  if (v >= 9) {
    encode(flags, bl);
  }
  if (v >= 10) {
    encode(compat, bl);
  }
  ENCODE_FINISH(bl);
}

void MDSMap::mds_info_t::encode_unversioned(bufferlist& bl) const
{
  __u8 struct_v = 3;
  using ceph::encode;
  encode(struct_v, bl);
  encode(global_id, bl);
  encode(name, bl);
  encode(rank, bl);
  encode(inc, bl);
  encode((int32_t)state, bl);
  encode(state_seq, bl);
  encode(addrs.legacy_addr(), bl, 0);
  encode(laggy_since, bl);
  encode(MDS_RANK_NONE, bl);
  encode(std::string(), bl);
  encode(export_targets, bl);
}

void MDSMap::mds_info_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(10, 4, 4, bl);
  decode(global_id, bl);
  decode(name, bl);
  decode(rank, bl);
  decode(inc, bl);
  int32_t raw_state;
  decode(raw_state, bl);
  state = (MDSMap::DaemonState)raw_state;
  decode(state_seq, bl);
  decode(addrs, bl);
  decode(laggy_since, bl);
  {
    mds_rank_t standby_for_rank;
    decode(standby_for_rank, bl);
  }
  {
    std::string standby_for_name;
    decode(standby_for_name, bl);
  }
  if (struct_v >= 2)
    decode(export_targets, bl);
  if (struct_v >= 5)
    decode(mds_features, bl);
  if (struct_v >= 6) {
    decode(join_fscid, bl);
  }
  if (struct_v >= 7) {
    bool standby_replay;
    decode(standby_replay, bl);
  }
  if (struct_v >= 9) {
    decode(flags, bl);
  }
  if (struct_v >= 10) {
    decode(compat, bl);
  } else {
    compat = MDSMap::get_compat_set_v16_2_4();
  }
  DECODE_FINISH(bl);
}

std::string MDSMap::mds_info_t::human_name() const
{
  // Like "daemon mds.myhost restarted", "Activating daemon mds.myhost"
  CachedStackStringStream css;
  *css << "daemon mds." << name;
  return css->str();
}

void MDSMap::encode(bufferlist& bl, uint64_t features) const
{
  std::map<mds_rank_t,int32_t> inc;  // Legacy field, fake it so that
                                     // old-mon peers have something sane
                                     // during upgrade
  for (const auto rank : in) {
    inc.insert(std::make_pair(rank, epoch));
  }

  using ceph::encode;
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    __u16 v = 2;
    encode(v, bl);
    encode(epoch, bl);
    encode(flags, bl);
    encode(last_failure, bl);
    encode(root, bl);
    encode(session_timeout, bl);
    encode(session_autoclose, bl);
    encode(max_file_size, bl);
    encode(max_mds, bl);
    __u32 n = mds_info.size();
    encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      encode(i->first, bl);
      encode(i->second, bl, features);
    }
    n = data_pools.size();
    encode(n, bl);
    for (const auto p: data_pools) {
      n = p;
      encode(n, bl);
    }

    int32_t m = cas_pool;
    encode(m, bl);
    return;
  } else if ((features & CEPH_FEATURE_MDSENC) == 0) {
    __u16 v = 3;
    encode(v, bl);
    encode(epoch, bl);
    encode(flags, bl);
    encode(last_failure, bl);
    encode(root, bl);
    encode(session_timeout, bl);
    encode(session_autoclose, bl);
    encode(max_file_size, bl);
    encode(max_mds, bl);
    __u32 n = mds_info.size();
    encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      encode(i->first, bl);
      encode(i->second, bl, features);
    }
    encode(data_pools, bl);
    encode(cas_pool, bl);

    __u16 ev = 5;
    encode(ev, bl);
    encode(compat, bl);
    encode(metadata_pool, bl);
    encode(created, bl);
    encode(modified, bl);
    encode(tableserver, bl);
    encode(in, bl);
    encode(inc, bl);
    encode(up, bl);
    encode(failed, bl);
    encode(stopped, bl);
    encode(last_failure_osd_epoch, bl);
    return;
  }

  ENCODE_START(5, 4, bl);
  encode(epoch, bl);
  encode(flags, bl);
  encode(last_failure, bl);
  encode(root, bl);
  encode(session_timeout, bl);
  encode(session_autoclose, bl);
  encode(max_file_size, bl);
  encode(max_mds, bl);
  encode(mds_info, bl, features);
  encode(data_pools, bl);
  encode(cas_pool, bl);

  __u16 ev = 18;
  encode(ev, bl);
  encode(compat, bl);
  encode(metadata_pool, bl);
  encode(created, bl);
  encode(modified, bl);
  encode(tableserver, bl);
  encode(in, bl);
  encode(inc, bl);
  encode(up, bl);
  encode(failed, bl);
  encode(stopped, bl);
  encode(last_failure_osd_epoch, bl);
  encode(ever_allowed_features, bl);
  encode(explicitly_allowed_features, bl);
  encode(inline_data_enabled, bl);
  encode(enabled, bl);
  encode(fs_name, bl);
  encode(damaged, bl);
  encode(balancer, bl);
  encode(standby_count_wanted, bl);
  encode(old_max_mds, bl);
  {
    ceph_release_t min_compat_client = ceph_release_t::unknown;
    encode(min_compat_client, bl);
  }
  encode(required_client_features, bl);
  encode(max_xattr_size, bl);
  encode(bal_rank_mask, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::sanitize(const std::function<bool(int64_t pool)>& pool_exists)
{
  /* Before we did stricter checking, it was possible to remove a data pool
   * without also deleting it from the MDSMap. Check for that here after
   * decoding the data pools.
   */

  for (auto it = data_pools.begin(); it != data_pools.end();) {
    if (!pool_exists(*it)) {
      dout(0) << "removed non-existant data pool " << *it << " from MDSMap" << dendl;
      it = data_pools.erase(it);
    } else {
      it++;
    }
  }
}

void MDSMap::decode(bufferlist::const_iterator& p)
{
  std::map<mds_rank_t,int32_t> inc;  // Legacy field, parse and drop

  cached_up_features = 0;
  DECODE_START_LEGACY_COMPAT_LEN_16(5, 4, 4, p);
  decode(epoch, p);
  decode(flags, p);
  decode(last_failure, p);
  decode(root, p);
  decode(session_timeout, p);
  decode(session_autoclose, p);
  decode(max_file_size, p);
  decode(max_mds, p);
  decode(mds_info, p);
  if (struct_v < 3) {
    __u32 n;
    decode(n, p);
    while (n--) {
      __u32 m;
      decode(m, p);
      data_pools.push_back(m);
    }
    __s32 s;
    decode(s, p);
    cas_pool = s;
  } else {
    decode(data_pools, p);
    decode(cas_pool, p);
  }

  // kclient ignores everything from here
  __u16 ev = 1;
  if (struct_v >= 2)
    decode(ev, p);
  if (ev >= 3)
    decode(compat, p);
  else
    compat = get_compat_set_base();
  if (ev < 5) {
    __u32 n;
    decode(n, p);
    metadata_pool = n;
  } else {
    decode(metadata_pool, p);
  }
  decode(created, p);
  decode(modified, p);
  decode(tableserver, p);
  decode(in, p);
  decode(inc, p);
  decode(up, p);
  decode(failed, p);
  decode(stopped, p);
  if (ev >= 4)
    decode(last_failure_osd_epoch, p);
  if (ev >= 6) {
    if (ev < 10) {
      // previously this was a bool about snaps, not a flag map
      bool flag;
      decode(flag, p);
      ever_allowed_features = flag ? CEPH_MDSMAP_ALLOW_SNAPS : 0;
      decode(flag, p);
      explicitly_allowed_features = flag ? CEPH_MDSMAP_ALLOW_SNAPS : 0;
    } else {
      decode(ever_allowed_features, p);
      decode(explicitly_allowed_features, p);
    }
  } else {
    ever_allowed_features = 0;
    explicitly_allowed_features = 0;
  }
  if (ev >= 7)
    decode(inline_data_enabled, p);

  if (ev >= 8) {
    ceph_assert(struct_v >= 5);
    decode(enabled, p);
    decode(fs_name, p);
  } else {
    if (epoch > 1) {
      // If an MDS has ever been started, epoch will be greater than 1,
      // assume filesystem is enabled.
      enabled = true;
    } else {
      // Upgrading from a cluster that never used an MDS, switch off
      // filesystem until it's explicitly enabled.
      enabled = false;
    }
  }

  if (ev >= 9) {
    decode(damaged, p);
  }

  if (ev >= 11) {
    decode(balancer, p);
  }

  if (ev >= 12) {
    decode(standby_count_wanted, p);
  }

  if (ev >= 13) {
    decode(old_max_mds, p);
  }

  if (ev >= 14) {
    ceph_release_t min_compat_client;
    if (ev == 14) {
      int8_t r;
      decode(r, p);
      if (r < 0) {
	min_compat_client = ceph_release_t::unknown;
      } else {
	min_compat_client = ceph_release_t{static_cast<uint8_t>(r)};
      }
    } else if (ev >= 15) {
      decode(min_compat_client, p);
    }
    if (ev >= 16) {
      decode(required_client_features, p);
    } else {
      set_min_compat_client(min_compat_client);
    }
  }

  if (ev >= 17) {
    decode(max_xattr_size, p);
  }

  if (ev >= 18) {
    decode(bal_rank_mask, p);
  }

  /* All MDS since at least v14.0.0 understand INLINE */
  /* TODO: remove after R is released */
  compat.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);

  for (auto& p: mds_info) {
    static const CompatSet empty;
    auto& info = p.second;
    if (empty.compare(info.compat) == 0) {
      /* bootstrap old compat; mds_info_t::decode does not have access to MDSMap */
      info.compat = compat;
    }
    /* All MDS since at least v14.0.0 understand INLINE */
    /* TODO: remove after R is released */
    info.compat.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
  }

  DECODE_FINISH(p);
}

MDSMap::availability_t MDSMap::is_cluster_available() const
{
  if (epoch == 0) {
    // If I'm a client, this means I'm looking at an MDSMap instance
    // that was never actually initialized from the mons.  Client should
    // wait.
    return TRANSIENT_UNAVAILABLE;
  }

  // If a rank is marked damage (unavailable until operator intervenes)
  if (damaged.size()) {
    return STUCK_UNAVAILABLE;
  }

  // If no ranks are created (filesystem not initialized)
  if (in.empty()) {
    return STUCK_UNAVAILABLE;
  }

  for (const auto rank : in) {
    if (up.count(rank) && mds_info.at(up.at(rank)).laggy()) {
      // This might only be transient, but because we can't see
      // standbys, we have no way of knowing whether there is a
      // standby available to replace the laggy guy.
      return STUCK_UNAVAILABLE;
    }
  }

  if (get_num_mds(CEPH_MDS_STATE_ACTIVE) > 0) {
    // Nobody looks stuck, so indicate to client they should go ahead
    // and try mounting if anybody is active.  This may include e.g.
    // one MDS failing over and another active: the client should
    // proceed to start talking to the active one and let the
    // transiently-unavailable guy catch up later.
    return AVAILABLE;
  } else {
    // Nothing indicating we were stuck, but nobody active (yet)
    //return TRANSIENT_UNAVAILABLE;

    // Because we don't have standbys in the MDSMap any more, we can't
    // reliably indicate transient vs. stuck, so always say stuck so
    // that the client doesn't block.
    return STUCK_UNAVAILABLE;
  }
}

bool MDSMap::state_transition_valid(DaemonState prev, DaemonState next)
{
  if (next == prev)
    return true;
  if (next == MDSMap::STATE_DAMAGED)
    return true;

  if (prev == MDSMap::STATE_BOOT) {
    return next == MDSMap::STATE_STANDBY;
  } else if (prev == MDSMap::STATE_STANDBY) {
    return next == MDSMap::STATE_STANDBY_REPLAY ||
           next == MDSMap::STATE_REPLAY ||
           next == MDSMap::STATE_CREATING ||
           next == MDSMap::STATE_STARTING;
  } else if (prev == MDSMap::STATE_CREATING || prev == MDSMap::STATE_STARTING) {
    return next == MDSMap::STATE_ACTIVE;
  } else if (prev == MDSMap::STATE_STANDBY_REPLAY) {
    return next == MDSMap::STATE_REPLAY;
  } else if (prev == MDSMap::STATE_REPLAY) {
    return next == MDSMap::STATE_RESOLVE ||
           next == MDSMap::STATE_RECONNECT;
  } else if (prev >= MDSMap::STATE_RESOLVE && prev < MDSMap::STATE_ACTIVE) {
    // Once I have entered replay, the only allowable transitions are to
    // the next next along in the sequence.
    // Except...
    if (prev == MDSMap::STATE_REJOIN &&
        (next == MDSMap::STATE_ACTIVE ||    // No need to do client replay
         next == MDSMap::STATE_STOPPED)) {  // no subtrees
      return true;         
    }
    return next == prev + 1;
  } else if (prev == MDSMap::STATE_ACTIVE) {
    return next == MDSMap::STATE_STOPPING;
  } else if (prev == MDSMap::STATE_STOPPING) {
    return next == MDSMap::STATE_STOPPED;
  } else {
    derr << __func__ << ": Unknown prev state "
         << ceph_mds_state_name(prev) << "(" << prev << ")" << dendl;
    return false;
  }
}

bool MDSMap::check_health(mds_rank_t standby_daemon_count)
{
  std::set<mds_rank_t> standbys;
  get_standby_replay_mds_set(standbys);
  std::set<mds_rank_t> actives;
  get_active_mds_set(actives);
  mds_rank_t standbys_avail = (mds_rank_t)standbys.size()+standby_daemon_count;

  /* If there are standby daemons available/replaying and
   * standby_count_wanted is unset (default), then we set it to 1. This will
   * happen during health checks by the mons. Also, during initial creation
   * of the FS we will have no actives so we don't want to change the default
   * yet.
   */
  if (standby_count_wanted == -1 && actives.size() > 0 && standbys_avail > 0) {
    set_standby_count_wanted(1);
    return true;
  }
  return false;
}

mds_gid_t MDSMap::find_mds_gid_by_name(std::string_view s) const {
  for (const auto& [gid, info] : mds_info) {
    if (info.name == s) {
      return gid;
    }
  }
  return MDS_GID_NONE;
}

unsigned MDSMap::get_num_mds(int state) const {
  unsigned n = 0;
  for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
       p != mds_info.end();
       ++p)
    if (p->second.state == state) ++n;
  return n;
}

void MDSMap::get_up_mds_set(std::set<mds_rank_t>& s) const {
  for (std::map<mds_rank_t, mds_gid_t>::const_iterator p = up.begin();
       p != up.end();
       ++p)
    s.insert(p->first);
}

uint64_t MDSMap::get_up_features() {
  if (!cached_up_features) {
    bool first = true;
    for (std::map<mds_rank_t, mds_gid_t>::const_iterator p = up.begin();
         p != up.end();
         ++p) {
      std::map<mds_gid_t, mds_info_t>::const_iterator q =
        mds_info.find(p->second);
      ceph_assert(q != mds_info.end());
      if (first) {
        cached_up_features = q->second.mds_features;
        first = false;
      } else {
        cached_up_features &= q->second.mds_features;
      }
    }
  }
  return cached_up_features;
}

void MDSMap::get_recovery_mds_set(std::set<mds_rank_t>& s) const {
  s = failed;
  for (const auto& p : damaged)
    s.insert(p);
  for (const auto& p : mds_info)
    if (p.second.state >= STATE_REPLAY && p.second.state <= STATE_STOPPING)
      s.insert(p.second.rank);
}

void MDSMap::get_mds_set_lower_bound(std::set<mds_rank_t>& s, DaemonState first) const {
  for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
       p != mds_info.end();
       ++p)
    if (p->second.state >= first && p->second.state <= STATE_STOPPING)
      s.insert(p->second.rank);
}

void MDSMap::get_mds_set(std::set<mds_rank_t>& s, DaemonState state) const {
  for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
       p != mds_info.end();
       ++p)
    if (p->second.state == state)
      s.insert(p->second.rank);
}

mds_gid_t MDSMap::get_standby_replay(mds_rank_t r) const {
  for (auto& [gid,info] : mds_info) {
    if (info.rank == r && info.state == STATE_STANDBY_REPLAY) {
      return gid;
    }
  }
  return MDS_GID_NONE;
}

bool MDSMap::is_degraded() const {
  if (!failed.empty() || !damaged.empty())
    return true;
  for (const auto& p : mds_info) {
    if (p.second.is_degraded())
      return true;
  }
  return false;
}

void MDSMap::set_min_compat_client(ceph_release_t version)
{
  vector<size_t> bits;

  if (version >= ceph_release_t::octopus)
    bits.push_back(CEPHFS_FEATURE_OCTOPUS);
  else if (version >= ceph_release_t::nautilus)
    bits.push_back(CEPHFS_FEATURE_NAUTILUS);
  else if (version >= ceph_release_t::mimic)
    bits.push_back(CEPHFS_FEATURE_MIMIC);
  else if (version >= ceph_release_t::luminous)
    bits.push_back(CEPHFS_FEATURE_LUMINOUS);
  else if (version >= ceph_release_t::kraken)
    bits.push_back(CEPHFS_FEATURE_KRAKEN);
  else if (version >= ceph_release_t::jewel)
    bits.push_back(CEPHFS_FEATURE_JEWEL);

  std::sort(bits.begin(), bits.end());
  required_client_features = feature_bitset_t(bits);
}

const std::bitset<MAX_MDS>& MDSMap::get_bal_rank_mask_bitset() const {
  return bal_rank_mask_bitset;
}

void MDSMap::set_bal_rank_mask(std::string val)
{
  bal_rank_mask = val;
  dout(10) << "set bal_rank_mask to \"" << bal_rank_mask << "\""<< dendl;
}

const bool MDSMap::check_special_bal_rank_mask(std::string val, bal_rank_mask_type_t type) const
{
  if ((type == BAL_RANK_MASK_TYPE_ANY || type == BAL_RANK_MASK_TYPE_ALL) && (val == "-1" || val == "all")) {
    return true;
  }
  if ((type == BAL_RANK_MASK_TYPE_ANY || type == BAL_RANK_MASK_TYPE_NONE) && (val == "0x0" || val == "0")) {
    return true;
  }
  return false;
}

void MDSMap::update_num_mdss_in_rank_mask_bitset()
{
  int r = -EINVAL;

  if (bal_rank_mask.length() && !check_special_bal_rank_mask(bal_rank_mask, BAL_RANK_MASK_TYPE_ANY)) {
    std::string bin_string;
    CachedStackStringStream css;

    r = hex2bin(bal_rank_mask, bin_string, MAX_MDS, *css);
    if (r == 0) {
      auto _mds_bal_mask_bitset = std::bitset<MAX_MDS>(bin_string);
      bal_rank_mask_bitset = _mds_bal_mask_bitset;
      num_mdss_in_rank_mask_bitset = _mds_bal_mask_bitset.count();
    } else {
      dout(10) << css->str() << dendl;
    }
  }

  if (r == -EINVAL) {
    if (check_special_bal_rank_mask(bal_rank_mask, BAL_RANK_MASK_TYPE_NONE)) {
      dout(10) << "Balancer is disabled with bal_rank_mask " << bal_rank_mask << dendl;
      bal_rank_mask_bitset.reset();
      num_mdss_in_rank_mask_bitset = 0;
    } else {
      dout(10) << "Balancer distributes mds workloads to all ranks as bal_rank_mask is empty or invalid" << dendl;
      bal_rank_mask_bitset.set();
      num_mdss_in_rank_mask_bitset = get_max_mds();
    }
  }

  dout(10) << "update num_mdss_in_rank_mask_bitset to " << num_mdss_in_rank_mask_bitset << dendl;
}

int MDSMap::hex2bin(std::string hex_string, std::string &bin_string, unsigned int max_bits, std::ostream& ss) const
{
  static const unsigned int BITS_PER_QUARTET = CHAR_BIT / 2;
  static const unsigned int BITS_PER_ULLONG = sizeof(unsigned long long) * CHAR_BIT ;
  static const unsigned int QUARTETS_PER_ULLONG = BITS_PER_ULLONG/BITS_PER_QUARTET;
  unsigned int offset = 0;

  std::transform(hex_string.begin(), hex_string.end(), hex_string.begin(), ::tolower);

  if (hex_string.substr(0, 2) == "0x") {
    offset = 2;
  }

  for (unsigned int i = offset; i < hex_string.size(); i += QUARTETS_PER_ULLONG) {
    unsigned long long value;
    try {
      value = stoull(hex_string.substr(i, QUARTETS_PER_ULLONG), nullptr, 16);
    } catch (std::invalid_argument const& ex) {
      ss << "invalid hex value ";
      return -EINVAL;
    }
    auto bit_str = std::bitset<BITS_PER_ULLONG>(value);
    bin_string += bit_str.to_string();
  }

  if (bin_string.length() > max_bits) {
    ss << "a value exceeds max_mds " << max_bits;
    return -EINVAL;
  }

  if (bin_string.find('1') == std::string::npos) {
    ss << "at least one rank must be set";
    return -EINVAL;
  }

  if (bin_string.length() < max_bits) {
    bin_string.insert(0, max_bits - bin_string.length(), '0');
  }

  return 0;
}
