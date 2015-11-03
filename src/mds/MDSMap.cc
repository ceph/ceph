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


#include "MDSMap.h"

#include <sstream>
using std::stringstream;


const mds_rank_t MDSMap::MDS_NO_STANDBY_PREF(-1);
const mds_rank_t MDSMap::MDS_STANDBY_ANY(-2);
const mds_rank_t MDSMap::MDS_STANDBY_NAME(-3);
const mds_rank_t MDSMap::MDS_MATCHED_ACTIVE(-4);

// features
CompatSet get_mdsmap_compat_set_all() {
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

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

CompatSet get_mdsmap_compat_set_default() {
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

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

// base (pre v0.20)
CompatSet get_mdsmap_compat_set_base() {
  CompatSet::FeatureSet feature_compat_base;
  CompatSet::FeatureSet feature_incompat_base;
  feature_incompat_base.insert(MDS_FEATURE_INCOMPAT_BASE);
  CompatSet::FeatureSet feature_ro_compat_base;

  return CompatSet(feature_compat_base, feature_ro_compat_base, feature_incompat_base);
}

void MDSMap::mds_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("gid", global_id);
  f->dump_string("name", name);
  f->dump_int("rank", rank);
  f->dump_int("incarnation", inc);
  f->dump_stream("state") << ceph_mds_state_name(state);
  f->dump_int("state_seq", state_seq);
  f->dump_stream("addr") << addr;
  if (laggy_since != utime_t())
    f->dump_stream("laggy_since") << laggy_since;
  
  f->dump_int("standby_for_rank", standby_for_rank);
  f->dump_string("standby_for_name", standby_for_name);
  f->open_array_section("export_targets");
  for (set<mds_rank_t>::iterator p = export_targets.begin();
       p != export_targets.end(); ++p) {
    f->dump_int("mds", *p);
  }
  f->close_section();
}

void MDSMap::mds_info_t::generate_test_instances(list<mds_info_t*>& ls)
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
  f->dump_stream("created") << created;
  f->dump_stream("modified") << modified;
  f->dump_int("tableserver", tableserver);
  f->dump_int("root", root);
  f->dump_int("session_timeout", session_timeout);
  f->dump_int("session_autoclose", session_autoclose);
  f->dump_int("max_file_size", max_file_size);
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
  for (map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin(); p != mds_info.end(); ++p) {
    char s[25]; // 'gid_' + len(str(ULLONG_MAX)) + '\0'
    sprintf(s, "gid_%llu", (long long unsigned)p->first);
    f->open_object_section(s);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("data_pools");
  for (set<int64_t>::const_iterator p = data_pools.begin(); p != data_pools.end(); ++p)
    f->dump_int("pool", *p);
  f->close_section();
  f->dump_int("metadata_pool", metadata_pool);
  f->dump_bool("enabled", enabled);
  f->dump_string("fs_name", fs_name);
}

void MDSMap::generate_test_instances(list<MDSMap*>& ls)
{
  MDSMap *m = new MDSMap();
  m->max_mds = 1;
  m->data_pools.insert(0);
  m->metadata_pool = 1;
  m->cas_pool = 2;
  m->compat = get_mdsmap_compat_set_all();

  // these aren't the defaults, just in case anybody gets confused
  m->session_timeout = 61;
  m->session_autoclose = 301;
  m->max_file_size = 1<<24;
  ls.push_back(m);
}

void MDSMap::print(ostream& out) 
{
  out << "epoch\t" << epoch << "\n";
  out << "flags\t" << hex << flags << dec << "\n";
  out << "created\t" << created << "\n";
  out << "modified\t" << modified << "\n";
  out << "tableserver\t" << tableserver << "\n";
  out << "root\t" << root << "\n";
  out << "session_timeout\t" << session_timeout << "\n"
      << "session_autoclose\t" << session_autoclose << "\n";
  out << "max_file_size\t" << max_file_size << "\n";
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

  multimap< pair<mds_rank_t, unsigned>, mds_gid_t > foo;
  for (map<mds_gid_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p)
    foo.insert(std::make_pair(std::make_pair(p->second.rank, p->second.inc-1), p->first));

  for (multimap< pair<mds_rank_t, unsigned>, mds_gid_t >::iterator p = foo.begin();
       p != foo.end();
       ++p) {
    mds_info_t& info = mds_info[p->second];
    
    out << p->second << ":\t"
	<< info.addr
	<< " '" << info.name << "'"
	<< " mds." << info.rank
	<< "." << info.inc
	<< " " << ceph_mds_state_name(info.state)
	<< " seq " << info.state_seq;
    if (info.laggy())
      out << " laggy since " << info.laggy_since;
    if (info.standby_for_rank != -1 ||
	!info.standby_for_name.empty()) {
      out << " (standby for";
      //if (info.standby_for_rank >= 0)
	out << " rank " << info.standby_for_rank;
      if (!info.standby_for_name.empty())
	out << " '" << info.standby_for_name << "'";
      out << ")";
    }
    if (!info.export_targets.empty())
      out << " export_targets=" << info.export_targets;
    out << "\n";    
  }
}



void MDSMap::print_summary(Formatter *f, ostream *out)
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
  for (map<mds_gid_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p) {
    string s = ceph_mds_state_name(p->second.state);
    if (p->second.laggy())
      s += "(laggy or crashed)";

    if (p->second.rank >= 0) {
      if (f) {
	f->open_object_section("mds");
	f->dump_unsigned("rank", p->second.rank);
	f->dump_string("name", p->second.name);
	f->dump_string("status", s);
	f->close_section();
      } else {
	by_rank[p->second.rank] = p->second.name + "=" + s;
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

void MDSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  if (!failed.empty()) {
    std::ostringstream oss;
    oss << "mds rank"
	<< ((failed.size() > 1) ? "s ":" ")
	<< failed
	<< ((failed.size() > 1) ? " have":" has")
	<< " failed";
    summary.push_back(make_pair(HEALTH_ERR, oss.str()));
    if (detail) {
      for (set<mds_rank_t>::const_iterator p = failed.begin(); p != failed.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " has failed";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
      }
    }
  }

  if (!damaged.empty()) {
    std::ostringstream oss;
    oss << "mds rank"
	<< ((damaged.size() > 1) ? "s ":" ")
	<< damaged
	<< ((damaged.size() > 1) ? " are":" is")
	<< " damaged";
    summary.push_back(make_pair(HEALTH_ERR, oss.str()));
    if (detail) {
      for (set<mds_rank_t>::const_iterator p = damaged.begin(); p != damaged.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " is damaged";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
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
	map<mds_gid_t,mds_info_t>::const_iterator info = mds_info.find(gid);
	stringstream ss;
	if (is_resolve(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is resolving";
	if (is_replay(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is replaying journal";
	if (is_rejoin(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is rejoining";
	if (is_reconnect(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is reconnecting to clients";
	if (ss.str().length())
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
  }

  map<mds_rank_t, mds_gid_t>::const_iterator u = up.begin();
  map<mds_rank_t, mds_gid_t>::const_iterator u_end = up.end();
  map<mds_gid_t, mds_info_t>::const_iterator m_end = mds_info.end();
  set<string> laggy;
  for (; u != u_end; ++u) {
    map<mds_gid_t, mds_info_t>::const_iterator m = mds_info.find(u->second);
    if (m == m_end) {
      std::cerr << "Up rank " << u->first << " GID " << u->second << " not found!" << std::endl;
    }
    assert(m != m_end);
    const mds_info_t &mds_info(m->second);
    if (mds_info.laggy()) {
      laggy.insert(mds_info.name);
      if (detail) {
	std::ostringstream oss;
	oss << "mds." << mds_info.name << " at " << mds_info.addr << " is laggy/unresponsive";
	detail->push_back(make_pair(HEALTH_WARN, oss.str()));
      }
    }
  }

  if (!laggy.empty()) {
    std::ostringstream oss;
    oss << "mds " << laggy
	<< ((laggy.size() > 1) ? " are":" is")
	<< " laggy";
    summary.push_back(make_pair(HEALTH_WARN, oss.str()));
  }
}

void MDSMap::mds_info_t::encode_versioned(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(4, 4, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(rank, bl);
  ::encode(inc, bl);
  ::encode((int32_t)state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::mds_info_t::encode_unversioned(bufferlist& bl) const
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(rank, bl);
  ::encode(inc, bl);
  ::encode((int32_t)state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
}

void MDSMap::mds_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  ::decode(global_id, bl);
  ::decode(name, bl);
  ::decode(rank, bl);
  ::decode(inc, bl);
  ::decode((int32_t&)(state), bl);
  ::decode(state_seq, bl);
  ::decode(addr, bl);
  ::decode(laggy_since, bl);
  ::decode(standby_for_rank, bl);
  ::decode(standby_for_name, bl);
  if (struct_v >= 2)
    ::decode(export_targets, bl);
  DECODE_FINISH(bl);
}



void MDSMap::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    __u16 v = 2;
    ::encode(v, bl);
    ::encode(epoch, bl);
    ::encode(flags, bl);
    ::encode(last_failure, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_file_size, bl);
    ::encode(max_mds, bl);
    __u32 n = mds_info.size();
    ::encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      ::encode(i->first, bl);
      ::encode(i->second, bl, features);
    }
    n = data_pools.size();
    ::encode(n, bl);
    for (set<int64_t>::const_iterator p = data_pools.begin(); p != data_pools.end(); ++p) {
      n = *p;
      ::encode(n, bl);
    }

    int32_t m = cas_pool;
    ::encode(m, bl);
    return;
  } else if ((features & CEPH_FEATURE_MDSENC) == 0) {
    __u16 v = 3;
    ::encode(v, bl);
    ::encode(epoch, bl);
    ::encode(flags, bl);
    ::encode(last_failure, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_file_size, bl);
    ::encode(max_mds, bl);
    __u32 n = mds_info.size();
    ::encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      ::encode(i->first, bl);
      ::encode(i->second, bl, features);
    }
    ::encode(data_pools, bl);
    ::encode(cas_pool, bl);

    // kclient ignores everything from here
    __u16 ev = 5;
    ::encode(ev, bl);
    ::encode(compat, bl);
    ::encode(metadata_pool, bl);
    ::encode(created, bl);
    ::encode(modified, bl);
    ::encode(tableserver, bl);
    ::encode(in, bl);
    ::encode(inc, bl);
    ::encode(up, bl);
    ::encode(failed, bl);
    ::encode(stopped, bl);
    ::encode(last_failure_osd_epoch, bl);
    return;
  }

  ENCODE_START(5, 4, bl);
  ::encode(epoch, bl);
  ::encode(flags, bl);
  ::encode(last_failure, bl);
  ::encode(root, bl);
  ::encode(session_timeout, bl);
  ::encode(session_autoclose, bl);
  ::encode(max_file_size, bl);
  ::encode(max_mds, bl);
  ::encode(mds_info, bl, features);
  ::encode(data_pools, bl);
  ::encode(cas_pool, bl);

  // kclient ignores everything from here
  __u16 ev = 9;
  ::encode(ev, bl);
  ::encode(compat, bl);
  ::encode(metadata_pool, bl);
  ::encode(created, bl);
  ::encode(modified, bl);
  ::encode(tableserver, bl);
  ::encode(in, bl);
  ::encode(inc, bl);
  ::encode(up, bl);
  ::encode(failed, bl);
  ::encode(stopped, bl);
  ::encode(last_failure_osd_epoch, bl);
  ::encode(ever_allowed_snaps, bl);
  ::encode(explicitly_allowed_snaps, bl);
  ::encode(inline_data_enabled, bl);
  ::encode(enabled, bl);
  ::encode(fs_name, bl);
  ::encode(damaged, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN_16(5, 4, 4, p);
  ::decode(epoch, p);
  ::decode(flags, p);
  ::decode(last_failure, p);
  ::decode(root, p);
  ::decode(session_timeout, p);
  ::decode(session_autoclose, p);
  ::decode(max_file_size, p);
  ::decode(max_mds, p);
  ::decode(mds_info, p);
  if (struct_v < 3) {
    __u32 n;
    ::decode(n, p);
    while (n--) {
      __u32 m;
      ::decode(m, p);
      data_pools.insert(m);
    }
    __s32 s;
    ::decode(s, p);
    cas_pool = s;
  } else {
    ::decode(data_pools, p);
    ::decode(cas_pool, p);
  }

  // kclient ignores everything from here
  __u16 ev = 1;
  if (struct_v >= 2)
    ::decode(ev, p);
  if (ev >= 3)
    ::decode(compat, p);
  else
    compat = get_mdsmap_compat_set_base();
  if (ev < 5) {
    __u32 n;
    ::decode(n, p);
    metadata_pool = n;
  } else {
    ::decode(metadata_pool, p);
  }
  ::decode(created, p);
  ::decode(modified, p);
  ::decode(tableserver, p);
  ::decode(in, p);
  ::decode(inc, p);
  ::decode(up, p);
  ::decode(failed, p);
  ::decode(stopped, p);
  if (ev >= 4)
    ::decode(last_failure_osd_epoch, p);
  if (ev >= 6) {
    ::decode(ever_allowed_snaps, p);
    ::decode(explicitly_allowed_snaps, p);
  } else {
    ever_allowed_snaps = true;
    explicitly_allowed_snaps = false;
  }
  if (ev >= 7)
    ::decode(inline_data_enabled, p);

  if (ev >= 8) {
    assert(struct_v >= 5);
    ::decode(enabled, p);
    ::decode(fs_name, p);
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
    ::decode(damaged, p);
  }
  DECODE_FINISH(p);
}

MDSMap::availability_t MDSMap::is_cluster_available() const
{
  if (epoch == 0) {
    // This is ambiguous between "mds map was never initialized on mons" and
    // "we never got an mdsmap from the mons".  Treat it like the latter.
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
    std::string name;
    if (up.count(rank) != 0) {
      name = mds_info.at(up.at(rank)).name;
    }
    const mds_gid_t replacement = find_replacement_for(rank, name, false);
    const bool standby_avail = (replacement != MDS_GID_NONE);

    // If the rank is unfilled, and there are no standbys, we're unavailable
    if (up.count(rank) == 0 && !standby_avail) {
      return STUCK_UNAVAILABLE;
    } else if (up.count(rank) && mds_info.at(up.at(rank)).laggy() && !standby_avail) {
      // If the daemon is laggy and there are no standbys, we're unavailable.
      // It would be nice to give it some grace here, but to do so callers
      // would have to poll this time-wise, vs. just waiting for updates
      // to mdsmap, so it's not worth the complexity.
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
    return TRANSIENT_UNAVAILABLE;
  }
}
