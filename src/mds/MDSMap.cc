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


// features
CompatSet get_mdsmap_compat_set() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

// base (pre v0.20)
CompatSet get_mdsmap_compat_set_base() {
  CompatSet::FeatureSet feature_compat_base;
  CompatSet::FeatureSet feature_incompat_base;
  feature_incompat_base.insert(MDS_FEATURE_INCOMPAT_BASE);
  CompatSet::FeatureSet feature_ro_compat_base;

  return CompatSet(feature_compat_base, feature_incompat_base, feature_ro_compat_base);
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
  for (set<int32_t>::iterator p = export_targets.begin();
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
  for (set<int32_t>::const_iterator p = in.begin(); p != in.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("up");
  for (map<int32_t,uint64_t>::const_iterator p = up.begin(); p != up.end(); ++p) {
    char s[10];
    sprintf(s, "%d", p->first);
    f->dump_int(s, p->second);
  }
  f->close_section();
  f->open_array_section("failed");
  for (set<int32_t>::const_iterator p = failed.begin(); p != failed.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("stopped");
  for (set<int32_t>::const_iterator p = stopped.begin(); p != stopped.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("info");
  for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin(); p != mds_info.end(); ++p) {
    char s[10];
    sprintf(s, "%llu", (long long unsigned)p->first);
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
}

void MDSMap::generate_test_instances(list<MDSMap*>& ls)
{
  MDSMap *m = new MDSMap();
  m->max_mds = 1;
  m->data_pools.insert(0);
  m->metadata_pool = 1;
  m->cas_pool = 2;
  m->compat = get_mdsmap_compat_set();

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
  out << "last_failure\t" << last_failure << "\n"
      << "last_failure_osd_epoch\t" << last_failure_osd_epoch << "\n";
  out << "compat\t" << compat << "\n";
  out << "max_mds\t" << max_mds << "\n";
  out << "in\t" << in << "\n"
      << "up\t" << up << "\n"
      << "failed\t" << failed << "\n"
      << "stopped\t" << stopped << "\n";
  out << "data_pools\t" << data_pools << "\n";
  out << "metadata_pool\t" << metadata_pool << "\n";

  multimap< pair<unsigned,unsigned>, uint64_t > foo;
  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++)
    foo.insert(pair<pair<unsigned,unsigned>,uint64_t>(pair<unsigned,unsigned>(p->second.rank, p->second.inc-1), p->first));

  for (multimap< pair<unsigned,unsigned>, uint64_t >::iterator p = foo.begin();
       p != foo.end();
       p++) {
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
    if (info.export_targets.size())
      out << " export_targets=" << info.export_targets;
    out << "\n";    
  }
}



void MDSMap::print_summary(ostream& out) 
{
  map<int,string> by_rank;
  map<string,int> by_state;

  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++) {
    string s = ceph_mds_state_name(p->second.state);
    if (p->second.laggy())
      s += "(laggy or crashed)";

    if (p->second.rank >= 0)
      by_rank[p->second.rank] = p->second.name + "=" + s;
    else
      by_state[s]++;
  }

  out << "e" << get_epoch() << ": " << up.size() << "/" << in.size() << "/" << max_mds << " up";

  if (by_rank.size())
    out << " " << by_rank;

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); p++)
    out << ", " << p->second << " " << p->first;
  
  if (failed.size())
    out << ", " << failed.size() << " failed";
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
      for (set<int>::iterator p = failed.begin(); p != failed.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " has failed";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
      }
    }
  }

  map<int32_t,uint64_t>::const_iterator u = up.begin();
  map<int32_t,uint64_t>::const_iterator u_end = up.end();
  map<uint64_t,mds_info_t>::const_iterator m_end = mds_info.end();
  set<string> laggy;
  for (; u != u_end; ++u) {
    map<uint64_t,mds_info_t>::const_iterator m = mds_info.find(u->second);
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
  if (laggy.size()) {
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
  ::encode(state, bl);
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
  ::encode(state, bl);
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
  ::decode(state, bl);
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
    for (map<uint64_t, mds_info_t>::const_iterator i = mds_info.begin();
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
    for (map<uint64_t, mds_info_t>::const_iterator i = mds_info.begin();
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
  } else {// have MDS encoding feature!
    ENCODE_START(4, 4, bl);
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
    ENCODE_FINISH(bl);
  }
}

void MDSMap::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN_16(4, 4, 4, p);
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
  DECODE_FINISH(p);
}
