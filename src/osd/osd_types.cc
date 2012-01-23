// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "osd_types.h"

// -- osd_reqid_t --
void osd_reqid_t::encode(bufferlist &bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(name, bl);
  ::encode(tid, bl);
  ::encode(inc, bl);
}

void osd_reqid_t::decode(bufferlist::iterator &bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(name, bl);
  ::decode(tid, bl);
  ::decode(inc, bl);
}

// -- osd_stat_t --
void osd_stat_t::generate_test_instances(std::list<osd_stat_t>& o)
{
  osd_stat_t z;
  o.push_back(z);

  osd_stat_t a;
  a.kb = 1;
  a.kb_used = 2;
  a.kb_avail = 3;
  a.hb_in.push_back(5);
  a.hb_in.push_back(6);
  a.hb_out = a.hb_in;
  a.hb_out.push_back(7);
  a.snap_trim_queue_len = 8;
  a.num_snap_trimming = 99;
  o.push_back(a);
}

// -- pg_t --

int pg_t::print(char *o, int maxlen) const
{
  if (preferred() >= 0)
    return snprintf(o, maxlen, "%llu.%xp%d", (unsigned long long)pool(), ps(), preferred());
  else
    return snprintf(o, maxlen, "%llu.%x", (unsigned long long)pool(), ps());
}

bool pg_t::parse(const char *s)
{
  uint64_t ppool;
  uint32_t pseed;
  int32_t pref;
  int r = sscanf(s, "%llu.%xp%d", (long long unsigned *)&ppool, &pseed, &pref);
  if (r < 2)
    return false;
  m_pool = ppool;
  m_seed = pseed;
  if (r == 3)
    m_preferred = pref;
  else
    m_preferred = -1;
  return true;
}

ostream& operator<<(ostream& out, const pg_t &pg)
{
  out << pg.pool() << '.';
  out << hex << pg.ps() << dec;

  if (pg.preferred() >= 0)
    out << 'p' << pg.preferred();

  //out << "=" << hex << (__uint64_t)pg << dec;
  return out;
}


// -- coll_t --

bool coll_t::is_pg(pg_t& pgid, snapid_t& snap) const
{
  const char *cstr(str.c_str());

  if (!pgid.parse(cstr))
    return false;
  const char *snap_start = strchr(cstr, '_');
  if (!snap_start)
    return false;
  if (strncmp(snap_start, "_head", 5) == 0)
    snap = CEPH_NOSNAP;
  else
    snap = strtoull(snap_start+1, 0, 16);
  return true;
}


void coll_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(str, bl);
}

void coll_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  switch (struct_v) {
  case 1: {
    pg_t pgid;
    snapid_t snap;

    ::decode(pgid, bl);
    ::decode(snap, bl);
    // infer the type
    if (pgid == pg_t() && snap == 0)
      str = "meta";
    else
      str = pg_and_snap_to_str(pgid, snap);
    break;
  }

  case 2: {
    __u8 type;
    pg_t pgid;
    snapid_t snap;
    
    ::decode(type, bl);
    ::decode(pgid, bl);
    ::decode(snap, bl);
    switch (type) {
    case 0:
      str = "meta";
      break;
    case 1:
      str = "temp";
      break;
    case 2:
      str = pg_and_snap_to_str(pgid, snap);
      break;
    default: {
      ostringstream oss;
      oss << "coll_t::decode(): can't understand type " << type;
      throw std::domain_error(oss.str());
    }
    }
    break;
  }

  case 3:
    ::decode(str, bl);
    break;
    
  default: {
    ostringstream oss;
    oss << "coll_t::decode(): don't know how to decode version "
	<< struct_v;
    throw std::domain_error(oss.str());
  }
  }
}

// ---

std::string pg_state_string(int state)
{
  ostringstream oss;
  if (state & PG_STATE_CREATING)
    oss << "creating+";
  if (state & PG_STATE_ACTIVE)
    oss << "active+";
  if (state & PG_STATE_CLEAN)
    oss << "clean+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_REPLAY)
    oss << "replay+";
  if (state & PG_STATE_STRAY)
    oss << "stray+";
  if (state & PG_STATE_SPLITTING)
    oss << "splitting+";
  if (state & PG_STATE_DEGRADED)
    oss << "degraded+";
  if (state & PG_STATE_SCRUBBING)
    oss << "scrubbing+";
  if (state & PG_STATE_SCRUBQ)
    oss << "scrubq+";
  if (state & PG_STATE_INCONSISTENT)
    oss << "inconsistent+";
  if (state & PG_STATE_PEERING)
    oss << "peering+";
  if (state & PG_STATE_REPAIR)
    oss << "repair+";
  if (state & PG_STATE_BACKFILL)
    oss << "backfill+";
  if (state & PG_STATE_INCOMPLETE)
    oss << "incomplete+";
  string ret(oss.str());
  if (ret.length() > 0)
    ret.resize(ret.length() - 1);
  else
    ret = "inactive";
  return ret;
}



// -- pool_snap_info_t --
void pool_snap_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("snapid", snapid);
  f->dump_stream("stamp") << stamp;
  f->dump_string("name", name);
}

void pool_snap_info_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(snapid, bl);
  ::encode(stamp, bl);
  ::encode(name, bl);
}

void pool_snap_info_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(snapid, bl);
  ::decode(stamp, bl);
  ::decode(name, bl);
}

void pool_snap_info_t::generate_test_instances(list<pool_snap_info_t>& o)
{
  pool_snap_info_t a;
  o.push_back(a);
  a.snapid = 1;
  a.stamp = utime_t(1, 2);
  a.name = "foo";
  o.push_back(a);  
}


// -- pg_pool_t --

void pg_pool_t::dump(Formatter *f) const
{
  f->dump_unsigned("flags", get_flags());
  f->dump_int("type", get_type());
  f->dump_int("size", get_size());
  f->dump_int("crush_ruleset", get_crush_ruleset());
  f->dump_int("object_hash", get_object_hash());
  f->dump_int("pg_num", get_pg_num());
  f->dump_int("pg_placement_num", get_pgp_num());
  f->dump_int("localized_pg_num", get_lpg_num());
  f->dump_int("localized_pg_placement_num", get_lpgp_num());
  f->dump_unsigned("crash_replay_interval", get_crash_replay_interval());
  f->dump_stream("last_change") << get_last_change();
  f->dump_unsigned("auid", get_auid());
  f->dump_string("snap_mode", is_pool_snaps_mode() ? "pool" : "selfmanaged");
  f->dump_unsigned("snap_seq", get_snap_seq());
  f->dump_unsigned("snap_epoch", get_snap_epoch());
  f->open_object_section("pool_snaps");
  for (map<snapid_t, pool_snap_info_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
    f->open_object_section("pool_snap_info");
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_stream("removed_snaps") << removed_snaps;
}


int pg_pool_t::calc_bits_of(int t)
{
  int b = 0;
  while (t > 0) {
    t = t >> 1;
    b++;
  }
  return b;
}

void pg_pool_t::calc_pg_masks()
{
  pg_num_mask = (1 << calc_bits_of(pg_num-1)) - 1;
  pgp_num_mask = (1 << calc_bits_of(pgp_num-1)) - 1;
  lpg_num_mask = (1 << calc_bits_of(lpg_num-1)) - 1;
  lpgp_num_mask = (1 << calc_bits_of(lpgp_num-1)) - 1;
}

/*
 * we have two snap modes:
 *  - pool global snaps
 *    - snap existence/non-existence defined by snaps[] and snap_seq
 *  - user managed snaps
 *    - removal governed by removed_snaps
 *
 * we know which mode we're using based on whether removed_snaps is empty.
 */
bool pg_pool_t::is_pool_snaps_mode() const
{
  return removed_snaps.empty() && get_snap_seq() > 0;
}

bool pg_pool_t::is_removed_snap(snapid_t s) const
{
  if (is_pool_snaps_mode())
    return s <= get_snap_seq() && snaps.count(s) == 0;
  else
    return removed_snaps.contains(s);
}

/*
 * build set of known-removed sets from either pool snaps or
 * explicit removed_snaps set.
 */
void pg_pool_t::build_removed_snaps(interval_set<snapid_t>& rs) const
{
  if (is_pool_snaps_mode()) {
    rs.clear();
    for (snapid_t s = 1; s <= get_snap_seq(); s = s + 1)
      if (snaps.count(s) == 0)
	rs.insert(s);
  } else {
    rs = removed_snaps;
  }
}

snapid_t pg_pool_t::snap_exists(const char *s) const
{
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = snaps.begin();
       p != snaps.end();
       p++)
    if (p->second.name == s)
      return p->second.snapid;
  return 0;
}

void pg_pool_t::add_snap(const char *n, utime_t stamp)
{
  assert(removed_snaps.empty());
  snapid_t s = get_snap_seq() + 1;
  snap_seq = s;
  snaps[s].snapid = s;
  snaps[s].name = n;
  snaps[s].stamp = stamp;
}

void pg_pool_t::add_unmanaged_snap(uint64_t& snapid)
{
  if (removed_snaps.empty()) {
    assert(snaps.empty());
    removed_snaps.insert(snapid_t(1));
    snap_seq = 1;
  }
  snapid = snap_seq = snap_seq + 1;
}

void pg_pool_t::remove_snap(snapid_t s)
{
  assert(snaps.count(s));
  snaps.erase(s);
  snap_seq = snap_seq + 1;
}

void pg_pool_t::remove_unmanaged_snap(snapid_t s)
{
  assert(snaps.empty());
  removed_snaps.insert(s);
  snap_seq = snap_seq + 1;
  removed_snaps.insert(get_snap_seq());
}

SnapContext pg_pool_t::get_snap_context() const
{
  vector<snapid_t> s(snaps.size());
  unsigned i = 0;
  for (map<snapid_t, pool_snap_info_t>::const_reverse_iterator p = snaps.rbegin();
       p != snaps.rend();
       p++)
    s[i++] = p->first;
  return SnapContext(get_snap_seq(), s);
}

/*
 * map a raw pg (with full precision ps) into an actual pg, for storage
 */
pg_t pg_pool_t::raw_pg_to_pg(pg_t pg) const
{
  if (pg.preferred() >= 0 && lpg_num)
    pg.set_ps(ceph_stable_mod(pg.ps(), lpg_num, lpg_num_mask));
  else
    pg.set_ps(ceph_stable_mod(pg.ps(), pg_num, pg_num_mask));
  return pg;
}
  
/*
 * map raw pg (full precision ps) into a placement seed.  include
 * pool id in that value so that different pools don't use the same
 * seeds.
 */
ps_t pg_pool_t::raw_pg_to_pps(pg_t pg) const
{
  if (pg.preferred() >= 0 && lpgp_num)
    return ceph_stable_mod(pg.ps(), lpgp_num, lpgp_num_mask) + pg.pool();
  else
    return ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask) + pg.pool();
}

void pg_pool_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    // this encoding matches the old struct ceph_pg_pool
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);
    ::encode(snap_seq, bl);
    ::encode(snap_epoch, bl);

    __u32 n = snaps.size();
    ::encode(n, bl);
    n = removed_snaps.num_intervals();
    ::encode(n, bl);

    ::encode(auid, bl);

    ::encode_nohead(snaps, bl);
    removed_snaps.encode_nohead(bl);
    return;
  }

  __u8 struct_v = 4;
  ::encode(struct_v, bl);
  ::encode(type, bl);
  ::encode(size, bl);
  ::encode(crush_ruleset, bl);
  ::encode(object_hash, bl);
  ::encode(pg_num, bl);
  ::encode(pgp_num, bl);
  ::encode(lpg_num, bl);
  ::encode(lpgp_num, bl);
  ::encode(last_change, bl);
  ::encode(snap_seq, bl);
  ::encode(snap_epoch, bl);
  ::encode(snaps, bl);
  ::encode(removed_snaps, bl);
  ::encode(auid, bl);
  ::encode(flags, bl);
  ::encode(crash_replay_interval, bl);
}

void pg_pool_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  if (struct_v > 4)
    throw buffer::error();

  ::decode(type, bl);
  ::decode(size, bl);
  ::decode(crush_ruleset, bl);
  ::decode(object_hash, bl);
  ::decode(pg_num, bl);
  ::decode(pgp_num, bl);
  ::decode(lpg_num, bl);
  ::decode(lpgp_num, bl);
  ::decode(last_change, bl);
  ::decode(snap_seq, bl);
  ::decode(snap_epoch, bl);

  if (struct_v >= 3) {
    ::decode(snaps, bl);
    ::decode(removed_snaps, bl);
    ::decode(auid, bl);
  } else {
    __u32 n, m;
    ::decode(n, bl);
    ::decode(m, bl);
    ::decode(auid, bl);
    ::decode_nohead(n, snaps, bl);
    removed_snaps.decode_nohead(m, bl);
  }

  if (struct_v >= 4) {
    ::decode(flags, bl);
    ::decode(crash_replay_interval, bl);
  } else {
    flags = 0;

    // if this looks like the 'data' pool, set the
    // crash_replay_interval appropriately.  unfortunately, we can't
    // be precise here.  this should be good enough to preserve replay
    // on the data pool for the majority of cluster upgrades, though.
    if (crush_ruleset == 0 && auid == 0)
      crash_replay_interval = 60;
    else
      crash_replay_interval = 0;
  }

  calc_pg_masks();
}

void pg_pool_t::generate_test_instances(list<pg_pool_t>& o)
{
  pg_pool_t a;
  o.push_back(a);

  a.type = TYPE_REP;
  a.size = 2;
  a.crush_ruleset = 3;
  a.object_hash = 4;
  a.pg_num = 6;
  a.pgp_num = 5;
  a.lpg_num = 8;
  a.lpgp_num = 7;
  a.last_change = 9;
  a.snap_seq = 10;
  a.snap_epoch = 11;
  a.auid = 12;
  a.crash_replay_interval = 13;
  o.push_back(a);

  a.snaps[3].name = "asdf";
  a.snaps[3].snapid = 3;
  a.snaps[3].stamp = utime_t(123, 4);
  a.snaps[6].name = "qwer";
  a.snaps[6].snapid = 6;
  a.snaps[6].stamp = utime_t(23423, 4);
  o.push_back(a);

  a.removed_snaps.insert(2);   // not quite valid to combine with snaps!
  o.push_back(a);
}

ostream& operator<<(ostream& out, const pg_pool_t& p)
{
  out << p.get_type_name()
      << " size " << p.get_size()
      << " crush_ruleset " << p.get_crush_ruleset()
      << " object_hash " << p.get_object_hash_name()
      << " pg_num " << p.get_pg_num()
      << " pgp_num " << p.get_pgp_num()
      << " lpg_num " << p.get_lpg_num()
      << " lpgp_num " << p.get_lpgp_num()
      << " last_change " << p.get_last_change()
      << " owner " << p.get_auid();
  if (p.flags)
    out << " flags " << p.flags;
  if (p.crash_replay_interval)
    out << " crash_replay_interval " << p.crash_replay_interval;
  return out;
}


// -- object_stat_sum_t --

void object_stat_sum_t::dump(Formatter *f) const
{
  f->dump_unsigned("num_bytes", num_bytes);
  f->dump_unsigned("num_kb", num_kb);
  f->dump_unsigned("num_objects", num_objects);
  f->dump_unsigned("num_object_clones", num_object_clones);
  f->dump_unsigned("num_object_copies", num_object_copies);
  f->dump_unsigned("num_objects_missing_on_primary", num_objects_missing_on_primary);
  f->dump_unsigned("num_objects_degraded", num_objects_degraded);
  f->dump_unsigned("num_objects_unfound", num_objects_unfound);
  f->dump_unsigned("num_read", num_rd);
  f->dump_unsigned("num_read_kb", num_rd_kb);
  f->dump_unsigned("num_write", num_wr);
  f->dump_unsigned("num_write_kb", num_wr_kb);
}

void object_stat_sum_t::encode(bufferlist& bl) const
{
  __u8 v = 2;
  ::encode(v, bl);
  ::encode(num_bytes, bl);
  ::encode(num_kb, bl);
  ::encode(num_objects, bl);
  ::encode(num_object_clones, bl);
  ::encode(num_object_copies, bl);
  ::encode(num_objects_missing_on_primary, bl);
  ::encode(num_objects_degraded, bl);
  ::encode(num_objects_unfound, bl);
  ::encode(num_rd, bl);
  ::encode(num_rd_kb, bl);
  ::encode(num_wr, bl);
  ::encode(num_wr_kb, bl);
}

void object_stat_sum_t::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(num_bytes, bl);
  ::decode(num_kb, bl);
  ::decode(num_objects, bl);
  ::decode(num_object_clones, bl);
  ::decode(num_object_copies, bl);
  ::decode(num_objects_missing_on_primary, bl);
  ::decode(num_objects_degraded, bl);
  if (v >= 2)
    ::decode(num_objects_unfound, bl);
  ::decode(num_rd, bl);
  ::decode(num_rd_kb, bl);
  ::decode(num_wr, bl);
  ::decode(num_wr_kb, bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t>& o)
{
  object_stat_sum_t a;
  o.push_back(a);

  a.num_bytes = 1;
  a.num_kb = 2;
  a.num_objects = 3;
  a.num_object_clones = 4;
  a.num_object_copies = 5;
  a.num_objects_missing_on_primary = 6;
  a.num_objects_degraded = 7;
  a.num_objects_unfound = 8;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  o.push_back(a);
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
  num_kb += o.num_kb;
  num_objects += o.num_objects;
  num_object_clones += o.num_object_clones;
  num_object_copies += o.num_object_copies;
  num_objects_missing_on_primary += o.num_objects_missing_on_primary;
  num_objects_degraded += o.num_objects_degraded;
  num_rd += o.num_rd;
  num_rd_kb += o.num_rd_kb;
  num_wr += o.num_wr;
  num_wr_kb += o.num_wr_kb;
  num_objects_unfound += o.num_objects_unfound;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
  num_kb -= o.num_kb;
  num_objects -= o.num_objects;
  num_object_clones -= o.num_object_clones;
  num_object_copies -= o.num_object_copies;
  num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
  num_objects_degraded -= o.num_objects_degraded;
  num_rd -= o.num_rd;
  num_rd_kb -= o.num_rd_kb;
  num_wr -= o.num_wr;
  num_wr_kb -= o.num_wr_kb;
  num_objects_unfound -= o.num_objects_unfound;
}


// -- object_stat_collection_t --

void object_stat_collection_t::dump(Formatter *f) const
{
  f->open_object_section("stat_sum");
  sum.dump(f);
  f->close_section();
  f->open_object_section("stat_cat_sum");
  for (map<string,object_stat_sum_t>::const_iterator p = cat_sum.begin(); p != cat_sum.end(); ++p) {
    f->open_object_section(p->first.c_str());
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void object_stat_collection_t::encode(bufferlist& bl) const
{
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(sum, bl);
  ::encode(cat_sum, bl);
}

void object_stat_collection_t::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(sum, bl);
  ::decode(cat_sum, bl);
}

void object_stat_collection_t::generate_test_instances(list<object_stat_collection_t>& o)
{
  object_stat_collection_t a;
  o.push_back(a);
  list<object_stat_sum_t> l;
  object_stat_sum_t::generate_test_instances(l);
  char n[2] = { 'a', 0 };
  for (list<object_stat_sum_t>::iterator p = l.begin(); p != l.end(); ++p) {
    a.add(*p, n);
    n[0]++;
    o.push_back(a);
  }
}


// -- pg_stat_t --

void pg_stat_t::dump(Formatter *f) const
{
  f->dump_stream("version") << version;
  f->dump_stream("reported") << reported;
  f->dump_string("state", pg_state_string(state));
  f->dump_stream("log_start") << log_start;
  f->dump_stream("ondisk_log_start") << ondisk_log_start;
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_epoch_clean", created);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
  stats.dump(f);
  f->open_array_section("up");
  for (vector<int>::const_iterator p = up.begin(); p != up.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (vector<int>::const_iterator p = acting.begin(); p != acting.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
}

void pg_stat_t::encode(bufferlist &bl) const
{
  __u8 v = 7;
  ::encode(v, bl);
  
  ::encode(version, bl);
  ::encode(reported, bl);
  ::encode(state, bl);
  ::encode(log_start, bl);
  ::encode(ondisk_log_start, bl);
  ::encode(created, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(parent, bl);
  ::encode(parent_split_bits, bl);
  ::encode(last_scrub, bl);
  ::encode(last_scrub_stamp, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
  ::encode(up, bl);
  ::encode(acting, bl);
}

void pg_stat_t::decode(bufferlist::iterator &bl)
{
  __u8 v;
  ::decode(v, bl);
  if (v > 7)
    throw buffer::malformed_input("unknown pg_stat_t encoding version > 7");

  ::decode(version, bl);
  ::decode(reported, bl);
  ::decode(state, bl);
  ::decode(log_start, bl);
  ::decode(ondisk_log_start, bl);
  ::decode(created, bl);
  if (v >= 7)
    ::decode(last_epoch_clean, bl);
  else
    last_epoch_clean = 0;
  if (v < 6) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    parent = opgid;
  } else {
    ::decode(parent, bl);
  }
  ::decode(parent_split_bits, bl);
  ::decode(last_scrub, bl);
  ::decode(last_scrub_stamp, bl);
  if (v <= 4) {
    ::decode(stats.sum.num_bytes, bl);
    ::decode(stats.sum.num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (v >= 3) {
      ::decode(up, bl);
    }
    if (v == 4) {
      ::decode(stats.sum.num_objects_unfound, bl);  // sigh.
    }
    ::decode(acting, bl);
  } else {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    ::decode(up, bl);
    ::decode(acting, bl);
  }
}

void pg_stat_t::generate_test_instances(list<pg_stat_t>& o)
{
  pg_stat_t a;
  o.push_back(a);

  a.version = eversion_t(1, 3);
  a.reported = eversion_t(1, 2);
  a.state = 123;
  a.log_start = eversion_t(1, 4);
  a.ondisk_log_start = eversion_t(1, 5);
  a.created = 6;
  a.last_epoch_clean = 7;
  a.parent = pg_t(1, 2, 3);
  a.parent_split_bits = 12;
  a.last_scrub = eversion_t(9, 10);
  a.last_scrub_stamp = utime_t(11, 12);
  list<object_stat_collection_t> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = l.back();
  a.log_size = 99;
  a.ondisk_log_size = 88;
  a.up.push_back(123);
  a.acting.push_back(456);
  o.push_back(a);
}


// -- pool_stat_t --

void pool_stat_t::dump(Formatter *f) const
{
  stats.dump(f);
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
}

void pool_stat_t::encode(bufferlist &bl) const
{
  __u8 v = 4;
  ::encode(v, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
}

void pool_stat_t::decode(bufferlist::iterator &bl)
{
  __u8 v;
  ::decode(v, bl);
  if (v >= 4) {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
  } else {
    ::decode(stats.sum.num_bytes, bl);
    ::decode(stats.sum.num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (v >= 3) {
      ::decode(stats.sum.num_objects_unfound, bl);
    }
  }
}

void pool_stat_t::generate_test_instances(list<pool_stat_t>& o)
{
  pool_stat_t a;
  o.push_back(a);

  list<object_stat_collection_t> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = l.back();
  a.log_size = 123;
  a.ondisk_log_size = 456;
  o.push_back(a);
}


// -- OSDSuperblock --

void OSDSuperblock::encode(bufferlist &bl) const
{
  __u8 v = 4;
  ::encode(v, bl);

  ::encode(cluster_fsid, bl);
  ::encode(whoami, bl);
  ::encode(current_epoch, bl);
  ::encode(oldest_map, bl);
  ::encode(newest_map, bl);
  ::encode(weight, bl);
  compat_features.encode(bl);
  ::encode(clean_thru, bl);
  ::encode(mounted, bl);
  ::encode(osd_fsid, bl);
}

void OSDSuperblock::decode(bufferlist::iterator &bl)
{
  __u8 v;
  ::decode(v, bl);

  if (v < 3) {
    string magic;
    ::decode(magic, bl);
  }
  ::decode(cluster_fsid, bl);
  ::decode(whoami, bl);
  ::decode(current_epoch, bl);
  ::decode(oldest_map, bl);
  ::decode(newest_map, bl);
  ::decode(weight, bl);
  if (v >= 2) {
    compat_features.decode(bl);
  } else { //upgrade it!
    compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  }
  ::decode(clean_thru, bl);
  ::decode(mounted, bl);
  if (v >= 4)
    ::decode(osd_fsid, bl);
}

void OSDSuperblock::dump(Formatter *f) const
{
  f->dump_stream("cluster_fsid") << cluster_fsid;
  f->dump_stream("osd_fsid") << osd_fsid;
  f->dump_int("whoami", whoami);
  f->dump_int("current_epoch", current_epoch);
  f->dump_int("oldest_map", oldest_map);
  f->dump_int("newest_map", newest_map);
  f->dump_float("weight", weight);
  f->open_object_section("compat");
  compat_features.dump(f);
  f->close_section();
  f->dump_int("clean_thru", clean_thru);
  f->dump_int("last_epoch_mounted", mounted);
}

void OSDSuperblock::generate_test_instances(list<OSDSuperblock>& o)
{
  OSDSuperblock z;
  o.push_back(z);
  memset(&z.cluster_fsid, 1, sizeof(z.cluster_fsid));
  memset(&z.osd_fsid, 2, sizeof(z.osd_fsid));
  z.whoami = 3;
  z.current_epoch = 4;
  z.oldest_map = 5;
  z.newest_map = 9;
  z.mounted = 8;
  z.clean_thru = 7;
  o.push_back(z);
}

// -- SnapSet --

void SnapSet::encode(bufferlist& bl) const
{
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(seq, bl);
  ::encode(head_exists, bl);
  ::encode(snaps, bl);
  ::encode(clones, bl);
  ::encode(clone_overlap, bl);
  ::encode(clone_size, bl);
}

void SnapSet::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(seq, bl);
  ::decode(head_exists, bl);
  ::decode(snaps, bl);
  ::decode(clones, bl);
  ::decode(clone_overlap, bl);
  ::decode(clone_size, bl);
}

ostream& operator<<(ostream& out, const SnapSet& cs)
{
  return out << cs.seq << "=" << cs.snaps << ":"
	     << cs.clones
	     << (cs.head_exists ? "+head":"");
}


// -- watch_info_t --

void watch_info_t::encode(bufferlist& bl) const
{
  const __u8 v = 2;
  ::encode(v, bl);
  ::encode(cookie, bl);
  ::encode(timeout_seconds, bl);
}

void watch_info_t::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(cookie, bl);
  if (v < 2) {
    uint64_t ver;
    ::decode(ver, bl);
  }
  ::decode(timeout_seconds, bl);
}

void watch_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("cookie", cookie);
  f->dump_unsigned("timeout_seconds", timeout_seconds);
}

void watch_info_t::generate_test_instances(list<watch_info_t>& o)
{
  watch_info_t a;
  o.push_back(a);
 
  a.cookie = 123;
  a.timeout_seconds = 99;
  o.push_back(a);
}


// -- object_info_t --

void object_info_t::copy_user_bits(const object_info_t& other)
{
  // these bits are copied from head->clone.
  size = other.size;
  mtime = other.mtime;
  last_reqid = other.last_reqid;
  truncate_seq = other.truncate_seq;
  truncate_size = other.truncate_size;
  lost = other.lost;
  category = other.category;
}

ps_t object_info_t::legacy_object_locator_to_ps(const object_t &oid, 
						const object_locator_t &loc) {
  ps_t ps;
  if (loc.key.length())
    // Hack, we don't have the osd map, so we don't really know the hash...
    ps = ceph_str_hash(CEPH_STR_HASH_RJENKINS, loc.key.c_str(), 
		       loc.key.length());
  else
    ps = ceph_str_hash(CEPH_STR_HASH_RJENKINS, oid.name.c_str(),
		       oid.name.length());
  // mix in preferred osd, so we don't get the same peers for
  // all of the placement pgs (e.g. 0.0p*)
  if (loc.get_preferred() >= 0)
    ps += loc.get_preferred();
  return ps;
}

void object_info_t::encode(bufferlist& bl) const
{
  const __u8 v = 7;
  ::encode(v, bl);
  ::encode(soid, bl);
  ::encode(oloc, bl);
  ::encode(category, bl);
  ::encode(version, bl);
  ::encode(prior_version, bl);
  ::encode(last_reqid, bl);
  ::encode(size, bl);
  ::encode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP)
    ::encode(wrlock_by, bl);
  else
    ::encode(snaps, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(lost, bl);
  ::encode(watchers, bl);
  ::encode(user_version, bl);
}

void object_info_t::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  if (v >= 2 && v <= 5) {
    sobject_t obj;
    ::decode(obj, bl);
    ::decode(oloc, bl);
    soid = hobject_t(obj.oid, oloc.key, obj.snap, 0);
    soid.hash = legacy_object_locator_to_ps(soid.oid, oloc);
  } else if (v >= 6) {
    ::decode(soid, bl);
    ::decode(oloc, bl);
    if (v == 6) {
      hobject_t hoid(soid.oid, oloc.key, soid.snap, hoid.hash);
      soid = hoid;
    }
  }
    
  if (v >= 5)
    ::decode(category, bl);
  ::decode(version, bl);
  ::decode(prior_version, bl);
  ::decode(last_reqid, bl);
  ::decode(size, bl);
  ::decode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP)
    ::decode(wrlock_by, bl);
  else
    ::decode(snaps, bl);
  ::decode(truncate_seq, bl);
  ::decode(truncate_size, bl);
  if (v >= 3)
    ::decode(lost, bl);
  else
    lost = false;
  if (v >= 4) {
    ::decode(watchers, bl);
    ::decode(user_version, bl);
  }
}

void object_info_t::dump(Formatter *f) const
{
  f->dump_stream("oid") << soid;
  f->dump_stream("locator") << oloc;
  f->dump_string("category", category);
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << prior_version;
  f->dump_stream("last_reqid") << last_reqid;
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("lost", lost);
  f->dump_stream("wrlock_by") << wrlock_by;
  f->open_array_section("snaps");
  for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->open_object_section("watchers");
  for (map<entity_name_t,watch_info_t>::const_iterator p = watchers.begin(); p != watchers.end(); ++p) {
    stringstream ss;
    ss << p->first;
    f->open_object_section(ss.str().c_str());
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void object_info_t::generate_test_instances(list<object_info_t>& o)
{
  object_info_t a;
  o.push_back(a);
  
  // fixme
}


ostream& operator<<(ostream& out, const object_info_t& oi)
{
  out << oi.soid << "(" << oi.version
      << " " << oi.last_reqid;
  if (oi.soid.snap == CEPH_NOSNAP)
    out << " wrlock_by=" << oi.wrlock_by;
  else
    out << " " << oi.snaps;
  if (oi.lost)
    out << " LOST";
  out << ")";
  return out;
}


// -- ScrubMap --

void ScrubMap::merge_incr(const ScrubMap &l)
{
  assert(valid_through == l.incr_since);
  attrs = l.attrs;
  logbl = l.logbl;
  valid_through = l.valid_through;

  for (map<hobject_t,object>::const_iterator p = l.objects.begin();
       p != l.objects.end();
       p++){
    if (p->second.negative) {
      map<hobject_t,object>::iterator q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}          

void ScrubMap::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(objects, bl);
  ::encode(attrs, bl);
  ::encode(logbl, bl);
  ::encode(valid_through, bl);
  ::encode(incr_since, bl);
}

void ScrubMap::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(objects, bl);
  ::decode(attrs, bl);
  ::decode(logbl, bl);
  ::decode(valid_through, bl);
  ::decode(incr_since, bl);
}


// -- OSDOp --

ostream& operator<<(ostream& out, const OSDOp& op)
{
  out << ceph_osd_op_name(op.op.op);
  if (ceph_osd_op_type_data(op.op.op)) {
    // data extent
    switch (op.op.op) {
    case CEPH_OSD_OP_DELETE:
      break;
    case CEPH_OSD_OP_TRUNCATE:
      out << " " << op.op.extent.offset;
      break;
    case CEPH_OSD_OP_MASKTRUNC:
    case CEPH_OSD_OP_TRIMTRUNC:
      out << " " << op.op.extent.truncate_seq << "@" << (int64_t)op.op.extent.truncate_size;
      break;
    case CEPH_OSD_OP_ROLLBACK:
      out << " " << snapid_t(op.op.snap.snapid);
      break;
    default:
      out << " " << op.op.extent.offset << "~" << op.op.extent.length;
      if (op.op.extent.truncate_seq)
	out << " [" << op.op.extent.truncate_seq << "@" << (int64_t)op.op.extent.truncate_size << "]";
    }
  } else if (ceph_osd_op_type_attr(op.op.op)) {
    // xattr name
    if (op.op.xattr.name_len && op.indata.length()) {
      out << " ";
      op.indata.write(0, op.op.xattr.name_len, out);
    }
    if (op.op.xattr.value_len)
      out << " (" << op.op.xattr.value_len << ")";
    if (op.op.op == CEPH_OSD_OP_CMPXATTR)
      out << " op " << (int)op.op.xattr.cmp_op << " mode " << (int)op.op.xattr.cmp_mode;
  } else if (ceph_osd_op_type_exec(op.op.op)) {
    // class.method
    if (op.op.cls.class_len && op.indata.length()) {
      out << " ";
      op.indata.write(0, op.op.cls.class_len, out);
      out << ".";
      op.indata.write(op.op.cls.class_len, op.op.cls.method_len, out);
    }
  } else if (ceph_osd_op_type_pg(op.op.op)) {
    switch (op.op.op) {
    case CEPH_OSD_OP_PGLS:
    case CEPH_OSD_OP_PGLS_FILTER:
      out << " start_epoch " << op.op.pgls.start_epoch;
      break;
    }
  } else if (ceph_osd_op_type_multi(op.op.op)) {
    switch (op.op.op) {
    case CEPH_OSD_OP_CLONERANGE:
      out << " " << op.op.clonerange.offset << "~" << op.op.clonerange.length
	  << " from " << op.soid
	  << " offset " << op.op.clonerange.src_offset;
      break;
    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      out << " v" << op.op.watch.ver
	  << " of " << op.soid;
      break;
    case CEPH_OSD_OP_SRC_CMPXATTR:
      out << " " << op.soid;
      if (op.op.xattr.name_len && op.indata.length()) {
	out << " ";
	op.indata.write(0, op.op.xattr.name_len, out);
      }
      if (op.op.xattr.value_len)
	out << " (" << op.op.xattr.value_len << ")";
      if (op.op.op == CEPH_OSD_OP_CMPXATTR)
	out << " op " << (int)op.op.xattr.cmp_op << " mode " << (int)op.op.xattr.cmp_mode;
      break;
    }
  }
  return out;
}


void OSDOp::split_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& in)
{
  bufferlist::iterator datap = in.begin();
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ceph_osd_op_type_multi(ops[i].op.op)) {
      ::decode(ops[i].soid, datap);
    }
    if (ops[i].op.payload_len) {
      datap.copy(ops[i].op.payload_len, ops[i].indata);
    }
  }
}

void OSDOp::merge_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& out)
{
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ceph_osd_op_type_multi(ops[i].op.op)) {
      ::encode(ops[i].soid, out);
    }
    if (ops[i].indata.length()) {
      ops[i].op.payload_len = ops[i].indata.length();
      out.append(ops[i].indata);
    }
  }
}

void OSDOp::split_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& in)
{
  bufferlist::iterator datap = in.begin();
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ops[i].op.payload_len) {
      datap.copy(ops[i].op.payload_len, ops[i].outdata);
    }
  }
}

void OSDOp::merge_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& out)
{
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ops[i].outdata.length()) {
      ops[i].op.payload_len = ops[i].outdata.length();
      out.append(ops[i].outdata);
    }
  }
}
