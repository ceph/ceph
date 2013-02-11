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
#include "include/ceph_features.h"
#include "PG.h"
#include "OSDMap.h"

// -- osd_reqid_t --
void osd_reqid_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(name, bl);
  ::encode(tid, bl);
  ::encode(inc, bl);
  ENCODE_FINISH(bl);
}

void osd_reqid_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(name, bl);
  ::decode(tid, bl);
  ::decode(inc, bl);
  DECODE_FINISH(bl);
}

void osd_reqid_t::dump(Formatter *f) const
{
  f->dump_stream("name") << name;
  f->dump_int("inc", inc);
  f->dump_unsigned("tid", tid);
}

void osd_reqid_t::generate_test_instances(list<osd_reqid_t*>& o)
{
  o.push_back(new osd_reqid_t);
  o.push_back(new osd_reqid_t(entity_name_t::CLIENT(123), 1, 45678));
}

// -- object_locator_t --

void object_locator_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(pool, bl);
  int32_t preferred = -1;  // tell old code there is no preferred osd (-1).
  ::encode(preferred, bl);
  ::encode(key, bl);
  ENCODE_FINISH(bl);
}

void object_locator_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, p);
  if (struct_v < 2) {
    int32_t op;
    ::decode(op, p);
    pool = op;
    int16_t pref;
    ::decode(pref, p);
  } else {
    ::decode(pool, p);
    int32_t preferred;
    ::decode(preferred, p);
  }
  ::decode(key, p);
  DECODE_FINISH(p);
}

void object_locator_t::dump(Formatter *f) const
{
  f->dump_int("pool", pool);
  f->dump_string("key", key);
}

void object_locator_t::generate_test_instances(list<object_locator_t*>& o)
{
  o.push_back(new object_locator_t);
  o.push_back(new object_locator_t(123));
  o.push_back(new object_locator_t(1234, "key"));
  o.push_back(new object_locator_t(12, "key2"));
}


// -- osd_stat_t --
void osd_stat_t::dump(Formatter *f) const
{
  f->dump_unsigned("kb", kb);
  f->dump_unsigned("kb_used", kb_used);
  f->dump_unsigned("kb_avail", kb_avail);
  f->open_array_section("hb_in");
  for (vector<int>::const_iterator p = hb_in.begin(); p != hb_in.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("hb_out");
  for (vector<int>::const_iterator p = hb_out.begin(); p != hb_out.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->dump_int("snap_trim_queue_len", snap_trim_queue_len);
  f->dump_int("num_snap_trimming", num_snap_trimming);
}

void osd_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(kb, bl);
  ::encode(kb_used, bl);
  ::encode(kb_avail, bl);
  ::encode(snap_trim_queue_len, bl);
  ::encode(num_snap_trimming, bl);
  ::encode(hb_in, bl);
  ::encode(hb_out, bl);
  ENCODE_FINISH(bl);
}

void osd_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(kb, bl);
  ::decode(kb_used, bl);
  ::decode(kb_avail, bl);
  ::decode(snap_trim_queue_len, bl);
  ::decode(num_snap_trimming, bl);
  ::decode(hb_in, bl);
  ::decode(hb_out, bl);
  DECODE_FINISH(bl);
}

void osd_stat_t::generate_test_instances(std::list<osd_stat_t*>& o)
{
  o.push_back(new osd_stat_t);

  o.push_back(new osd_stat_t);
  o.back()->kb = 1;
  o.back()->kb_used = 2;
  o.back()->kb_avail = 3;
  o.back()->hb_in.push_back(5);
  o.back()->hb_in.push_back(6);
  o.back()->hb_out = o.back()->hb_in;
  o.back()->hb_out.push_back(7);
  o.back()->snap_trim_queue_len = 8;
  o.back()->num_snap_trimming = 99;
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

bool pg_t::is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *children) const
{
  assert(m_seed < old_pg_num);
  if (new_pg_num <= old_pg_num)
    return false;

  bool split = false;
  if (true) {
    int old_bits = pg_pool_t::calc_bits_of(old_pg_num);
    int old_mask = (1 << old_bits) - 1;
    for (int n = 1; ; n++) {
      int next_bit = (n << (old_bits-1));
      unsigned s = next_bit | m_seed;

      if (s < old_pg_num || s == m_seed)
	continue;
      if (s >= new_pg_num)
	break;
      if ((unsigned)ceph_stable_mod(s, old_pg_num, old_mask) == m_seed) {
	split = true;
	if (children)
	  children->insert(pg_t(s, m_pool, m_preferred));
      }
    }
  }
  if (false) {
    // brute force
    int old_bits = pg_pool_t::calc_bits_of(old_pg_num);
    int old_mask = (1 << old_bits) - 1;
    for (unsigned x = old_pg_num; x < new_pg_num; ++x) {
      unsigned o = ceph_stable_mod(x, old_pg_num, old_mask);
      if (o == m_seed) {
	split = true;
	children->insert(pg_t(x, m_pool, m_preferred));
      }
    }
  }
  return split;
}

unsigned pg_t::get_split_bits(unsigned pg_num) const {
  assert(pg_num > 1);

  // Find unique p such that pg_num \in [2^(p-1), 2^p)
  unsigned p = pg_pool_t::calc_bits_of(pg_num);

  if ((m_seed % (1<<(p-1))) < (pg_num % (1<<(p-1))))
    return p;
  else
    return p - 1;
}

pg_t pg_t::get_parent() const
{
  unsigned bits = pg_pool_t::calc_bits_of(m_seed);
  assert(bits);
  pg_t retval = *this;
  retval.m_seed &= ~((~0)<<(bits - 1));
  return retval;
}

void pg_t::dump(Formatter *f) const
{
  f->dump_unsigned("pool", m_pool);
  f->dump_unsigned("seed", m_seed);
  f->dump_int("preferred_osd", m_preferred);
}

void pg_t::generate_test_instances(list<pg_t*>& o)
{
  o.push_back(new pg_t);
  o.push_back(new pg_t(1, 2, -1));
  o.push_back(new pg_t(13123, 3, -1));
  o.push_back(new pg_t(131223, 4, 23));
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

bool coll_t::is_temp(pg_t& pgid) const
{
  const char *cstr(str.c_str());
  if (!pgid.parse(cstr))
    return false;
  const char *tmp_start = strchr(cstr, '_');
  if (!tmp_start)
    return false;
  if (strncmp(tmp_start, "_TEMP", 5) == 0)
    return true;
  return false;
}

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

bool coll_t::is_removal(uint64_t *seq, pg_t *pgid) const
{
  if (str.substr(0, 11) != string("FORREMOVAL_"))
    return false;

  stringstream ss(str.substr(11));
  ss >> *seq;
  char sep;
  ss >> sep;
  assert(sep == '_');
  string pgid_str;
  ss >> pgid_str;
  if (!pgid->parse(pgid_str.c_str())) {
    assert(0);
    return false;
  }
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

void coll_t::dump(Formatter *f) const
{
  f->dump_string("name", str);
}

void coll_t::generate_test_instances(list<coll_t*>& o)
{
  o.push_back(new coll_t);
  o.push_back(new coll_t("meta"));
  o.push_back(new coll_t("temp"));
  o.push_back(new coll_t("foo"));
  o.push_back(new coll_t("bar"));
}

// ---

std::string pg_state_string(int state)
{
  ostringstream oss;
  if (state & PG_STATE_STALE)
    oss << "stale+";
  if (state & PG_STATE_CREATING)
    oss << "creating+";
  if (state & PG_STATE_ACTIVE)
    oss << "active+";
  if (state & PG_STATE_CLEAN)
    oss << "clean+";
  if (state & PG_STATE_RECOVERY_WAIT)
    oss << "recovery_wait+";
  if (state & PG_STATE_RECOVERING)
    oss << "recovering+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_REPLAY)
    oss << "replay+";
  if (state & PG_STATE_SPLITTING)
    oss << "splitting+";
  if (state & PG_STATE_DEGRADED)
    oss << "degraded+";
  if (state & PG_STATE_REMAPPED)
    oss << "remapped+";
  if (state & PG_STATE_SCRUBBING)
    oss << "scrubbing+";
  if (state & PG_STATE_DEEP_SCRUB)
    oss << "deep+";
  if (state & PG_STATE_SCRUBQ)
    oss << "scrubq+";
  if (state & PG_STATE_INCONSISTENT)
    oss << "inconsistent+";
  if (state & PG_STATE_PEERING)
    oss << "peering+";
  if (state & PG_STATE_REPAIR)
    oss << "repair+";
  if (state & PG_STATE_BACKFILL_WAIT &&
      !(state &PG_STATE_BACKFILL))
    oss << "wait_backfill+";
  if (state & PG_STATE_BACKFILL)
    oss << "backfilling+";
  if (state & PG_STATE_BACKFILL_TOOFULL)
    oss << "backfill_toofull+";
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

void pool_snap_info_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(snapid, bl);
    ::encode(stamp, bl);
    ::encode(name, bl);
    return;
  }
  ENCODE_START(2, 2, bl);
  ::encode(snapid, bl);
  ::encode(stamp, bl);
  ::encode(name, bl);
  ENCODE_FINISH(bl);
}

void pool_snap_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(snapid, bl);
  ::decode(stamp, bl);
  ::decode(name, bl);
  DECODE_FINISH(bl);
}

void pool_snap_info_t::generate_test_instances(list<pool_snap_info_t*>& o)
{
  o.push_back(new pool_snap_info_t);
  o.push_back(new pool_snap_info_t);
  o.back()->snapid = 1;
  o.back()->stamp = utime_t(1, 2);
  o.back()->name = "foo";
}


// -- pg_pool_t --

void pg_pool_t::dump(Formatter *f) const
{
  f->dump_unsigned("flags", get_flags());
  f->dump_int("type", get_type());
  f->dump_int("size", get_size());
  f->dump_int("min_size", get_min_size());
  f->dump_int("crush_ruleset", get_crush_ruleset());
  f->dump_int("object_hash", get_object_hash());
  f->dump_int("pg_num", get_pg_num());
  f->dump_int("pg_placement_num", get_pgp_num());
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

bool pg_pool_t::is_unmanaged_snaps_mode() const
{
  return removed_snaps.size() && get_snap_seq() > 0;
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
  assert(!is_unmanaged_snaps_mode());
  snapid_t s = get_snap_seq() + 1;
  snap_seq = s;
  snaps[s].snapid = s;
  snaps[s].name = n;
  snaps[s].stamp = stamp;
}

void pg_pool_t::add_unmanaged_snap(uint64_t& snapid)
{
  if (removed_snaps.empty()) {
    assert(!is_pool_snaps_mode());
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
  assert(is_unmanaged_snaps_mode());
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
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
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

    ::encode_nohead(snaps, bl, features);
    removed_snaps.encode_nohead(bl);
    return;
  }

  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 struct_v = 4;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);
    ::encode(snap_seq, bl);
    ::encode(snap_epoch, bl);
    ::encode(snaps, bl, features);
    ::encode(removed_snaps, bl);
    ::encode(auid, bl);
    ::encode(flags, bl);
    ::encode(crash_replay_interval, bl);
    return;
  }

  ENCODE_START(7, 5, bl);
  ::encode(type, bl);
  ::encode(size, bl);
  ::encode(crush_ruleset, bl);
  ::encode(object_hash, bl);
  ::encode(pg_num, bl);
  ::encode(pgp_num, bl);
  __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
  ::encode(lpg_num, bl);
  ::encode(lpgp_num, bl);
  ::encode(last_change, bl);
  ::encode(snap_seq, bl);
  ::encode(snap_epoch, bl);
  ::encode(snaps, bl, features);
  ::encode(removed_snaps, bl);
  ::encode(auid, bl);
  ::encode(flags, bl);
  ::encode(crash_replay_interval, bl);
  ::encode(min_size, bl);
  ENCODE_FINISH(bl);
}

void pg_pool_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(type, bl);
  ::decode(size, bl);
  ::decode(crush_ruleset, bl);
  ::decode(object_hash, bl);
  ::decode(pg_num, bl);
  ::decode(pgp_num, bl);
  {
    __u32 lpg_num, lpgp_num;
    ::decode(lpg_num, bl);
    ::decode(lpgp_num, bl);
  }
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
  if (struct_v >= 7) {
    ::decode(min_size, bl);
  } else {
    min_size = size - size/2;
  }
  DECODE_FINISH(bl);
  calc_pg_masks();
}

void pg_pool_t::generate_test_instances(list<pg_pool_t*>& o)
{
  pg_pool_t a;
  o.push_back(new pg_pool_t(a));

  a.type = TYPE_REP;
  a.size = 2;
  a.crush_ruleset = 3;
  a.object_hash = 4;
  a.pg_num = 6;
  a.pgp_num = 5;
  a.last_change = 9;
  a.snap_seq = 10;
  a.snap_epoch = 11;
  a.auid = 12;
  a.crash_replay_interval = 13;
  o.push_back(new pg_pool_t(a));

  a.snaps[3].name = "asdf";
  a.snaps[3].snapid = 3;
  a.snaps[3].stamp = utime_t(123, 4);
  a.snaps[6].name = "qwer";
  a.snaps[6].snapid = 6;
  a.snaps[6].stamp = utime_t(23423, 4);
  o.push_back(new pg_pool_t(a));

  a.removed_snaps.insert(2);   // not quite valid to combine with snaps!
  o.push_back(new pg_pool_t(a));
}

ostream& operator<<(ostream& out, const pg_pool_t& p)
{
  out << p.get_type_name()
      << " size " << p.get_size()
      << " min_size " << p.get_min_size()
      << " crush_ruleset " << p.get_crush_ruleset()
      << " object_hash " << p.get_object_hash_name()
      << " pg_num " << p.get_pg_num()
      << " pgp_num " << p.get_pgp_num()
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
  f->dump_int("num_bytes", num_bytes);
  f->dump_int("num_objects", num_objects);
  f->dump_int("num_object_clones", num_object_clones);
  f->dump_int("num_object_copies", num_object_copies);
  f->dump_int("num_objects_missing_on_primary", num_objects_missing_on_primary);
  f->dump_int("num_objects_degraded", num_objects_degraded);
  f->dump_int("num_objects_unfound", num_objects_unfound);
  f->dump_int("num_read", num_rd);
  f->dump_int("num_read_kb", num_rd_kb);
  f->dump_int("num_write", num_wr);
  f->dump_int("num_write_kb", num_wr_kb);
  f->dump_int("num_scrub_errors", num_scrub_errors);
  f->dump_int("num_objects_recovered", num_objects_recovered);
  f->dump_int("num_bytes_recovered", num_bytes_recovered);
  f->dump_int("num_keys_recovered", num_keys_recovered);
}

void object_stat_sum_t::encode(bufferlist& bl) const
{
  ENCODE_START(5, 3, bl);
  ::encode(num_bytes, bl);
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
  ::encode(num_scrub_errors, bl);
  ::encode(num_objects_recovered, bl);
  ::encode(num_bytes_recovered, bl);
  ::encode(num_keys_recovered, bl);
  ENCODE_FINISH(bl);
}

void object_stat_sum_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
  ::decode(num_bytes, bl);
  if (struct_v < 3) {
    uint64_t num_kb;
    ::decode(num_kb, bl);
  }
  ::decode(num_objects, bl);
  ::decode(num_object_clones, bl);
  ::decode(num_object_copies, bl);
  ::decode(num_objects_missing_on_primary, bl);
  ::decode(num_objects_degraded, bl);
  if (struct_v >= 2)
    ::decode(num_objects_unfound, bl);
  ::decode(num_rd, bl);
  ::decode(num_rd_kb, bl);
  ::decode(num_wr, bl);
  ::decode(num_wr_kb, bl);
  if (struct_v >= 4)
    ::decode(num_scrub_errors, bl);
  else
    num_scrub_errors = 0;
  if (struct_v >= 5) {
    ::decode(num_objects_recovered, bl);
    ::decode(num_bytes_recovered, bl);
    ::decode(num_keys_recovered, bl);
  } else {
    num_objects_recovered = 0;
    num_bytes_recovered = 0;
    num_keys_recovered = 0;
  }
  DECODE_FINISH(bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t*>& o)
{
  object_stat_sum_t a;
  o.push_back(new object_stat_sum_t(a));

  a.num_bytes = 1;
  a.num_objects = 3;
  a.num_object_clones = 4;
  a.num_object_copies = 5;
  a.num_objects_missing_on_primary = 6;
  a.num_objects_degraded = 7;
  a.num_objects_unfound = 8;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  a.num_scrub_errors = 13;
  a.num_objects_recovered = 14;
  a.num_bytes_recovered = 15;
  a.num_keys_recovered = 16;
  o.push_back(new object_stat_sum_t(a));
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
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
  num_scrub_errors += o.num_scrub_errors;
  num_objects_recovered += o.num_objects_recovered;
  num_bytes_recovered += o.num_bytes_recovered;
  num_keys_recovered += o.num_keys_recovered;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
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
  num_scrub_errors -= o.num_scrub_errors;
  num_objects_recovered -= o.num_objects_recovered;
  num_bytes_recovered -= o.num_bytes_recovered;
  num_keys_recovered -= o.num_keys_recovered;
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
  ENCODE_START(2, 2, bl);
  ::encode(sum, bl);
  ::encode(cat_sum, bl);
  ENCODE_FINISH(bl);
}

void object_stat_collection_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(sum, bl);
  ::decode(cat_sum, bl);
  DECODE_FINISH(bl);
}

void object_stat_collection_t::generate_test_instances(list<object_stat_collection_t*>& o)
{
  object_stat_collection_t a;
  o.push_back(new object_stat_collection_t(a));
  list<object_stat_sum_t*> l;
  object_stat_sum_t::generate_test_instances(l);
  char n[2] = { 'a', 0 };
  for (list<object_stat_sum_t*>::iterator p = l.begin(); p != l.end(); ++p) {
    a.add(**p, n);
    n[0]++;
    o.push_back(new object_stat_collection_t(a));
  }
}


// -- pg_stat_t --

void pg_stat_t::dump(Formatter *f) const
{
  f->dump_stream("version") << version;
  f->dump_stream("reported") << reported;
  f->dump_string("state", pg_state_string(state));
  f->dump_stream("last_fresh") << last_fresh;
  f->dump_stream("last_change") << last_change;
  f->dump_stream("last_active") << last_active;
  f->dump_stream("last_clean") << last_clean;
  f->dump_stream("last_unstale") << last_unstale;
  f->dump_unsigned("mapping_epoch", mapping_epoch);
  f->dump_stream("log_start") << log_start;
  f->dump_stream("ondisk_log_start") << ondisk_log_start;
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_epoch_clean", created);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
  f->dump_stream("stats_invalid") << stats_invalid;
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
  ENCODE_START(12, 8, bl);
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
  ::encode(last_fresh, bl);
  ::encode(last_change, bl);
  ::encode(last_active, bl);
  ::encode(last_clean, bl);
  ::encode(last_unstale, bl);
  ::encode(mapping_epoch, bl);
  ::encode(last_deep_scrub, bl);
  ::encode(last_deep_scrub_stamp, bl);
  ::encode(stats_invalid, bl);
  ::encode(last_clean_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void pg_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(12, 8, 8, bl);
  ::decode(version, bl);
  ::decode(reported, bl);
  ::decode(state, bl);
  ::decode(log_start, bl);
  ::decode(ondisk_log_start, bl);
  ::decode(created, bl);
  if (struct_v >= 7)
    ::decode(last_epoch_clean, bl);
  else
    last_epoch_clean = 0;
  if (struct_v < 6) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    parent = opgid;
  } else {
    ::decode(parent, bl);
  }
  ::decode(parent_split_bits, bl);
  ::decode(last_scrub, bl);
  ::decode(last_scrub_stamp, bl);
  if (struct_v <= 4) {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (struct_v >= 3) {
      ::decode(up, bl);
    }
    if (struct_v == 4) {
      ::decode(stats.sum.num_objects_unfound, bl);  // sigh.
    }
    ::decode(acting, bl);
  } else {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    ::decode(up, bl);
    ::decode(acting, bl);
    if (struct_v >= 9) {
      ::decode(last_fresh, bl);
      ::decode(last_change, bl);
      ::decode(last_active, bl);
      ::decode(last_clean, bl);
      ::decode(last_unstale, bl);
      ::decode(mapping_epoch, bl);
      if (struct_v >= 10) {
        ::decode(last_deep_scrub, bl);
        ::decode(last_deep_scrub_stamp, bl);
      }
    }
  }
  if (struct_v < 11) {
    stats_invalid = false;
  } else {
    ::decode(stats_invalid, bl);
  }
  if (struct_v >= 12) {
    ::decode(last_clean_scrub_stamp, bl);
  } else {
    last_clean_scrub_stamp = utime_t();
  }
  DECODE_FINISH(bl);
}

void pg_stat_t::generate_test_instances(list<pg_stat_t*>& o)
{
  pg_stat_t a;
  o.push_back(new pg_stat_t(a));

  a.version = eversion_t(1, 3);
  a.reported = eversion_t(1, 2);
  a.state = 123;
  a.mapping_epoch = 998;
  a.last_fresh = utime_t(1002, 1);
  a.last_change = utime_t(1002, 2);
  a.last_active = utime_t(1002, 3);
  a.last_clean = utime_t(1002, 4);
  a.last_unstale = utime_t(1002, 5);
  a.log_start = eversion_t(1, 4);
  a.ondisk_log_start = eversion_t(1, 5);
  a.created = 6;
  a.last_epoch_clean = 7;
  a.parent = pg_t(1, 2, 3);
  a.parent_split_bits = 12;
  a.last_scrub = eversion_t(9, 10);
  a.last_scrub_stamp = utime_t(11, 12);
  a.last_deep_scrub = eversion_t(13, 14);
  a.last_deep_scrub_stamp = utime_t(15, 16);
  a.last_clean_scrub_stamp = utime_t(17, 18);
  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.log_size = 99;
  a.ondisk_log_size = 88;
  a.up.push_back(123);
  a.acting.push_back(456);
  o.push_back(new pg_stat_t(a));
}


// -- pool_stat_t --

void pool_stat_t::dump(Formatter *f) const
{
  stats.dump(f);
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
}

void pool_stat_t::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 v = 4;
    ::encode(v, bl);
    ::encode(stats, bl);
    ::encode(log_size, bl);
    ::encode(ondisk_log_size, bl);
    return;
  }

  ENCODE_START(5, 5, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
  ENCODE_FINISH(bl);
}

void pool_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
  if (struct_v >= 4) {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
  } else {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (struct_v >= 3) {
      ::decode(stats.sum.num_objects_unfound, bl);
    }
  }
  DECODE_FINISH(bl);
}

void pool_stat_t::generate_test_instances(list<pool_stat_t*>& o)
{
  pool_stat_t a;
  o.push_back(new pool_stat_t(a));

  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.log_size = 123;
  a.ondisk_log_size = 456;
  o.push_back(new pool_stat_t(a));
}


// -- pg_history_t --

void pg_history_t::encode(bufferlist &bl) const
{
  ENCODE_START(6, 4, bl);
  ::encode(epoch_created, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(last_epoch_split, bl);
  ::encode(same_interval_since, bl);
  ::encode(same_up_since, bl);
  ::encode(same_primary_since, bl);
  ::encode(last_scrub, bl);
  ::encode(last_scrub_stamp, bl);
  ::encode(last_deep_scrub, bl);
  ::encode(last_deep_scrub_stamp, bl);
  ::encode(last_clean_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void pg_history_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, bl);
  ::decode(epoch_created, bl);
  ::decode(last_epoch_started, bl);
  if (struct_v >= 3)
    ::decode(last_epoch_clean, bl);
  else
    last_epoch_clean = last_epoch_started;  // careful, it's a lie!
  ::decode(last_epoch_split, bl);
  ::decode(same_interval_since, bl);
  ::decode(same_up_since, bl);
  ::decode(same_primary_since, bl);
  if (struct_v >= 2) {
    ::decode(last_scrub, bl);
    ::decode(last_scrub_stamp, bl);
  }
  if (struct_v >= 5) {
    ::decode(last_deep_scrub, bl);
    ::decode(last_deep_scrub_stamp, bl);
  }
  if (struct_v >= 6) {
    ::decode(last_clean_scrub_stamp, bl);
  }
  DECODE_FINISH(bl);
}

void pg_history_t::dump(Formatter *f) const
{
  f->dump_int("epoch_created", epoch_created);
  f->dump_int("last_epoch_started", last_epoch_started);
  f->dump_int("last_epoch_clean", last_epoch_clean);
  f->dump_int("last_epoch_split", last_epoch_split);
  f->dump_int("same_up_since", same_up_since);
  f->dump_int("same_interval_since", same_interval_since);
  f->dump_int("same_primary_since", same_primary_since);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
}

void pg_history_t::generate_test_instances(list<pg_history_t*>& o)
{
  o.push_back(new pg_history_t);
  o.push_back(new pg_history_t);
  o.back()->epoch_created = 1;
  o.back()->last_epoch_started = 2;
  o.back()->last_epoch_clean = 3;
  o.back()->last_epoch_split = 4;
  o.back()->same_up_since = 5;
  o.back()->same_interval_since = 6;
  o.back()->same_primary_since = 7;
  o.back()->last_scrub = eversion_t(8, 9);
  o.back()->last_scrub_stamp = utime_t(10, 11);
  o.back()->last_deep_scrub = eversion_t(12, 13);
  o.back()->last_deep_scrub_stamp = utime_t(14, 15);
  o.back()->last_clean_scrub_stamp = utime_t(16, 17);
}


// -- pg_info_t --

void pg_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(27, 26, bl);
  ::encode(pgid, bl);
  ::encode(last_update, bl);
  ::encode(last_complete, bl);
  ::encode(log_tail, bl);
  ::encode(last_backfill, bl);
  ::encode(stats, bl);
  history.encode(bl);
  ::encode(purged_snaps, bl);
  ::encode(last_epoch_started, bl);
  ENCODE_FINISH(bl);
}

void pg_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(27, 26, 26, bl);
  if (struct_v < 23) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    pgid = opgid;
  } else {
    ::decode(pgid, bl);
  }
  ::decode(last_update, bl);
  ::decode(last_complete, bl);
  ::decode(log_tail, bl);
  if (struct_v < 25) {
    bool log_backlog;
    ::decode(log_backlog, bl);
  }
  if (struct_v >= 24)
    ::decode(last_backfill, bl);
  ::decode(stats, bl);
  history.decode(bl);
  if (struct_v >= 22)
    ::decode(purged_snaps, bl);
  else {
    set<snapid_t> snap_trimq;
    ::decode(snap_trimq, bl);
  }
  if (struct_v < 27) {
    last_epoch_started = history.last_epoch_started;
  } else {
    ::decode(last_epoch_started, bl);
  }
  DECODE_FINISH(bl);
}

// -- pg_info_t --

void pg_info_t::dump(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_stream("last_update") << last_update;
  f->dump_stream("last_complete") << last_complete;
  f->dump_stream("log_tail") << log_tail;
  f->dump_stream("last_backfill") << last_backfill;
  f->dump_stream("purged_snaps") << purged_snaps;
  f->open_object_section("history");
  history.dump(f);
  f->close_section();
  f->open_object_section("stats");
  stats.dump(f);
  f->close_section();

  f->dump_int("empty", is_empty());
  f->dump_int("dne", dne());
  f->dump_int("incomplete", is_incomplete());
  f->dump_int("last_epoch_started", last_epoch_started);
}

void pg_info_t::generate_test_instances(list<pg_info_t*>& o)
{
  o.push_back(new pg_info_t);
  o.push_back(new pg_info_t);
  list<pg_history_t*> h;
  pg_history_t::generate_test_instances(h);
  o.back()->history = *h.back();
  o.back()->pgid = pg_t(1, 2, -1);
  o.back()->last_update = eversion_t(3, 4);
  o.back()->last_complete = eversion_t(5, 6);
  o.back()->log_tail = eversion_t(7, 8);
  o.back()->last_backfill = hobject_t(object_t("objname"), "key", 123, 456, -1);
  list<pg_stat_t*> s;
  pg_stat_t::generate_test_instances(s);
  o.back()->stats = *s.back();
}

// -- pg_notify_t --
void pg_notify_t::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(query_epoch, bl);
  ::encode(epoch_sent, bl);
  ::encode(info, bl);
  ENCODE_FINISH(bl);
}

void pg_notify_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(query_epoch, bl);
  ::decode(epoch_sent, bl);
  ::decode(info, bl);
  DECODE_FINISH(bl);
}

void pg_notify_t::dump(Formatter *f) const
{
  f->dump_stream("query_epoch") << query_epoch;
  f->dump_stream("epoch_sent") << epoch_sent;
  {
    f->open_object_section("info");
    info.dump(f);
    f->close_section();
  }
}

void pg_notify_t::generate_test_instances(list<pg_notify_t*>& o)
{
  o.push_back(new pg_notify_t(1,1,pg_info_t()));
  o.push_back(new pg_notify_t(3,10,pg_info_t()));
}

ostream &operator<<(ostream &lhs, const pg_notify_t &notify)
{
  return lhs << "(query_epoch:" << notify.query_epoch
	     << ", epoch_sent:" << notify.epoch_sent
	     << ", info:" << notify.info << ")";
}

// -- pg_interval_t --

void pg_interval_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(first, bl);
  ::encode(last, bl);
  ::encode(up, bl);
  ::encode(acting, bl);
  ::encode(maybe_went_rw, bl);
  ENCODE_FINISH(bl);
}

void pg_interval_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(first, bl);
  ::decode(last, bl);
  ::decode(up, bl);
  ::decode(acting, bl);
  ::decode(maybe_went_rw, bl);
  DECODE_FINISH(bl);
}

void pg_interval_t::dump(Formatter *f) const
{
  f->dump_unsigned("first", first);
  f->dump_unsigned("last", last);
  f->dump_int("maybe_went_rw", maybe_went_rw ? 1 : 0);
  f->open_array_section("up");
  for (vector<int>::const_iterator p = up.begin(); p != up.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (vector<int>::const_iterator p = acting.begin(); p != acting.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
}

void pg_interval_t::generate_test_instances(list<pg_interval_t*>& o)
{
  o.push_back(new pg_interval_t);
  o.push_back(new pg_interval_t);
  o.back()->up.push_back(1);
  o.back()->acting.push_back(2);
  o.back()->acting.push_back(3);
  o.back()->first = 4;
  o.back()->last = 5;
  o.back()->maybe_went_rw = true;
}

bool pg_interval_t::check_new_interval(
  const vector<int> &old_acting,
  const vector<int> &new_acting,
  const vector<int> &old_up,
  const vector<int> &new_up,
  epoch_t same_interval_since,
  epoch_t last_epoch_clean,
  OSDMapRef osdmap,
  OSDMapRef lastmap,
  int64_t pool_id,
  pg_t pgid,
  map<epoch_t, pg_interval_t> *past_intervals,
  std::ostream *out)
{
  // remember past interval
  if (new_acting != old_acting || new_up != old_up ||
      (!(lastmap->get_pools().count(pool_id))) ||
      (lastmap->get_pools().find(pool_id)->second.min_size !=
       osdmap->get_pools().find(pool_id)->second.min_size)  ||
      pgid.is_split(lastmap->get_pg_num(pgid.pool()),
        osdmap->get_pg_num(pgid.pool()), 0)) {
    pg_interval_t& i = (*past_intervals)[same_interval_since];
    i.first = same_interval_since;
    i.last = osdmap->get_epoch() - 1;
    i.acting = old_acting;
    i.up = old_up;

    if (i.acting.size() >=
	osdmap->get_pools().find(pool_id)->second.min_size) {
      if (lastmap->get_up_thru(i.acting[0]) >= i.first &&
	  lastmap->get_up_from(i.acting[0]) <= i.first) {
	i.maybe_went_rw = true;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : primary up " << lastmap->get_up_from(i.acting[0])
	       << "-" << lastmap->get_up_thru(i.acting[0])
	       << std::endl;
      } else if (last_epoch_clean >= i.first &&
		 last_epoch_clean <= i.last) {
	// If the last_epoch_clean is included in this interval, then
	// the pg must have been rw (for recovery to have completed).
	// This is important because we won't know the _real_
	// first_epoch because we stop at last_epoch_clean, and we
	// don't want the oldest interval to randomly have
	// maybe_went_rw false depending on the relative up_thru vs
	// last_epoch_clean timing.
	i.maybe_went_rw = true;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : includes last_epoch_clean " << last_epoch_clean
	       << " and presumed to have been rw"
	       << std::endl;
      } else {
	i.maybe_went_rw = false;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : primary up " << lastmap->get_up_from(i.acting[0])
	       << "-" << lastmap->get_up_thru(i.acting[0])
	       << " does not include interval"
	       << std::endl;
      }
    } else {
      i.maybe_went_rw = false;
      if (out)
	*out << "generate_past_intervals " << i << " : empty" << std::endl;
    }
    return true;
  } else {
    return false;
  }
}

ostream& operator<<(ostream& out, const pg_interval_t& i)
{
  out << "interval(" << i.first << "-" << i.last << " " << i.up << "/" << i.acting;
  if (i.maybe_went_rw)
    out << " maybe_went_rw";
  out << ")";
  return out;
}



// -- pg_query_t --

void pg_query_t::encode(bufferlist &bl, uint64_t features) const {
  if (features & CEPH_FEATURE_QUERY_T) {
    ENCODE_START(2, 2, bl);
    ::encode(type, bl);
    ::encode(since, bl);
    history.encode(bl);
    ::encode(epoch_sent, bl);
    ENCODE_FINISH(bl);
  } else {
    ::encode(type, bl);
    ::encode(since, bl);
    history.encode(bl);
  }
}

void pg_query_t::decode(bufferlist::iterator &bl) {
  bufferlist::iterator bl2 = bl;
  try {
    DECODE_START(2, bl);
    ::decode(type, bl);
    ::decode(since, bl);
    history.decode(bl);
    ::decode(epoch_sent, bl);
    DECODE_FINISH(bl);
  } catch (...) {
    bl = bl2;
    ::decode(type, bl);
    ::decode(since, bl);
    history.decode(bl);
  }
}

void pg_query_t::dump(Formatter *f) const
{
  f->dump_string("type", get_type_name());
  f->dump_stream("since") << since;
  f->dump_stream("epoch_sent") << epoch_sent;
  f->open_object_section("history");
  history.dump(f);
  f->close_section();
}
void pg_query_t::generate_test_instances(list<pg_query_t*>& o)
{
  o.push_back(new pg_query_t());
  list<pg_history_t*> h;
  pg_history_t::generate_test_instances(h);
  o.push_back(new pg_query_t(pg_query_t::INFO, *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::MISSING, *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::LOG, eversion_t(4, 5), *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::FULLLOG, *h.back(), 5));
}


// -- pg_log_entry_t --

void pg_log_entry_t::encode(bufferlist &bl) const
{
  ENCODE_START(7, 4, bl);
  ::encode(op, bl);
  ::encode(soid, bl);
  ::encode(version, bl);

  /**
   * Added with reverting_to:
   * Previous code used prior_version to encode
   * what we now call reverting_to.  This will
   * allow older code to decode reverting_to
   * into prior_version as expected.
   */
  if (op == LOST_REVERT)
    ::encode(reverting_to, bl);
  else
    ::encode(prior_version, bl);

  ::encode(reqid, bl);
  ::encode(mtime, bl);
  if (op == LOST_REVERT)
    ::encode(prior_version, bl);
  ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void pg_log_entry_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 4, 4, bl);
  ::decode(op, bl);
  if (struct_v < 2) {
    sobject_t old_soid;
    ::decode(old_soid, bl);
    soid.oid = old_soid.oid;
    soid.snap = old_soid.snap;
    invalid_hash = true;
  } else {
    ::decode(soid, bl);
  }
  if (struct_v < 3)
    invalid_hash = true;
  ::decode(version, bl);

  if (struct_v >= 6 && op == LOST_REVERT)
    ::decode(reverting_to, bl);
  else
    ::decode(prior_version, bl);

  ::decode(reqid, bl);
  ::decode(mtime, bl);
  if (struct_v < 5)
    invalid_pool = true;

  if (op == LOST_REVERT) {
    if (struct_v >= 6) {
      ::decode(prior_version, bl);
    } else {
      reverting_to = prior_version;
    }
  }
  if (struct_v >= 7 ||  // for v >= 7, this is for all ops.
      op == CLONE) {    // for v < 7, it's only present for CLONE.
    ::decode(snaps, bl);
  }

  DECODE_FINISH(bl);
}

void pg_log_entry_t::dump(Formatter *f) const
{
  f->dump_string("op", get_op_name());
  f->dump_stream("object") << soid;
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << version;
  f->dump_stream("reqid") << reqid;
  f->dump_stream("mtime") << mtime;
}

void pg_log_entry_t::generate_test_instances(list<pg_log_entry_t*>& o)
{
  o.push_back(new pg_log_entry_t());
  hobject_t oid(object_t("objname"), "key", 123, 456, 0);
  o.push_back(new pg_log_entry_t(MODIFY, oid, eversion_t(1,2), eversion_t(3,4),
				 osd_reqid_t(entity_name_t::CLIENT(777), 8, 999), utime_t(8,9)));
}

ostream& operator<<(ostream& out, const pg_log_entry_t& e)
{
  out << e.version << " (" << e.prior_version << ") "
      << e.get_op_name() << ' ' << e.soid << " by " << e.reqid << " " << e.mtime;
  if (e.snaps.length()) {
    vector<snapid_t> snaps;
    bufferlist c = e.snaps;
    bufferlist::iterator p = c.begin();
    try {
      ::decode(snaps, p);
    } catch (...) {
      snaps.clear();
    }
    out << " snaps " << snaps;
  }
  return out;
}


// -- pg_log_t --

void pg_log_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(head, bl);
  ::encode(tail, bl);
  ::encode(log, bl);
  ENCODE_FINISH(bl);
}
 
void pg_log_t::decode(bufferlist::iterator &bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  ::decode(head, bl);
  ::decode(tail, bl);
  if (struct_v < 2) {
    bool backlog;
    ::decode(backlog, bl);
  }
  ::decode(log, bl);
  DECODE_FINISH(bl);

  // handle hobject_t format change
  if (struct_v < 4) {
    for (list<pg_log_entry_t>::iterator i = log.begin();
	 i != log.end();
	 ++i) {
      if (i->soid.pool == -1)
	i->soid.pool = pool;
    }
  }
}

void pg_log_t::dump(Formatter *f) const
{
  f->dump_stream("head") << head;
  f->dump_stream("tail") << head;
  f->open_array_section("log");
  for (list<pg_log_entry_t>::const_iterator p = log.begin(); p != log.end(); ++p) {
    f->open_object_section("entry");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void pg_log_t::generate_test_instances(list<pg_log_t*>& o)
{
  o.push_back(new pg_log_t);

  // this is nonsensical:
  o.push_back(new pg_log_t);
  o.back()->head = eversion_t(1,2);
  o.back()->tail = eversion_t(3,4);
  list<pg_log_entry_t*> e;
  pg_log_entry_t::generate_test_instances(e);
  for (list<pg_log_entry_t*>::iterator p = e.begin(); p != e.end(); ++p)
    o.back()->log.push_back(**p);
}

void pg_log_t::copy_after(const pg_log_t &other, eversion_t v) 
{
  head = other.head;
  tail = other.tail;
  for (list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       i++) {
    assert(i->version > other.tail);
    if (i->version <= v) {
      // make tail accurate.
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

void pg_log_t::copy_range(const pg_log_t &other, eversion_t from, eversion_t to)
{
  list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
  assert(i != other.log.rend());
  while (i->version > to) {
    ++i;
    assert(i != other.log.rend());
  }
  assert(i->version == to);
  head = to;
  for ( ; i != other.log.rend(); ++i) {
    if (i->version <= from) {
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

void pg_log_t::copy_up_to(const pg_log_t &other, int max)
{
  int n = 0;
  head = other.head;
  tail = other.tail;
  for (list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       ++i) {
    if (n++ >= max) {
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

ostream& pg_log_t::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       p++) 
    out << *p << std::endl;
  return out;
}


// -- pg_missing_t --

void pg_missing_t::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  ::encode(missing, bl);
  ENCODE_FINISH(bl);
}

void pg_missing_t::decode(bufferlist::iterator &bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(missing, bl);
  DECODE_FINISH(bl);

  if (struct_v < 3) {
    // Handle hobject_t upgrade
    map<hobject_t, item> tmp;
    for (map<hobject_t, item>::iterator i = missing.begin();
	 i != missing.end();
      ) {
      if (i->first.pool == -1) {
	hobject_t to_insert(i->first);
	to_insert.pool = pool;
	tmp[to_insert] = i->second;
	missing.erase(i++);
      } else {
	++i;
      }
    }
    missing.insert(tmp.begin(), tmp.end());
  }

  for (map<hobject_t,item>::iterator it = missing.begin();
       it != missing.end();
       ++it)
    rmissing[it->second.need.version] = it->first;
}

void pg_missing_t::dump(Formatter *f) const
{
  f->open_array_section("missing");
  for (map<hobject_t,item>::const_iterator p = missing.begin(); p != missing.end(); ++p) {
    f->open_object_section("item");
    f->dump_stream("object") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void pg_missing_t::generate_test_instances(list<pg_missing_t*>& o)
{
  o.push_back(new pg_missing_t);
  o.push_back(new pg_missing_t);
  o.back()->add(hobject_t(object_t("foo"), "foo", 123, 456, 0), eversion_t(5, 6), eversion_t(5, 1));
}

ostream& operator<<(ostream& out, const pg_missing_t::item& i) 
{
  out << i.need;
  if (i.have != eversion_t())
    out << "(" << i.have << ")";
  return out;
}

ostream& operator<<(ostream& out, const pg_missing_t& missing) 
{
  out << "missing(" << missing.num_missing();
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}


unsigned int pg_missing_t::num_missing() const
{
  return missing.size();
}

bool pg_missing_t::have_missing() const
{
  return !missing.empty();
}

void pg_missing_t::swap(pg_missing_t& o)
{
  missing.swap(o.missing);
  rmissing.swap(o.rmissing);
}

bool pg_missing_t::is_missing(const hobject_t& oid) const
{
  return (missing.find(oid) != missing.end());
}

bool pg_missing_t::is_missing(const hobject_t& oid, eversion_t v) const
{
  map<hobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return false;
  const pg_missing_t::item &item(m->second);
  if (item.need > v)
    return false;
  return true;
}

eversion_t pg_missing_t::have_old(const hobject_t& oid) const
{
  map<hobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return eversion_t();
  const pg_missing_t::item &item(m->second);
  return item.have;
}

/*
 * this needs to be called in log order as we extend the log.  it
 * assumes missing is accurate up through the previous log entry.
 */
void pg_missing_t::add_next_event(const pg_log_entry_t& e)
{
  if (e.is_update()) {
    if (e.prior_version == eversion_t() || e.is_clone()) {
      // new object.
      //assert(missing.count(e.soid) == 0);  // might already be missing divergent item.
      if (missing.count(e.soid))  // already missing divergent item
	rmissing.erase(missing[e.soid].need.version);
      missing[e.soid] = item(e.version, eversion_t());  // .have = nil
    } else if (missing.count(e.soid)) {
      // already missing (prior).
      //assert(missing[e.soid].need == e.prior_version);
      rmissing.erase(missing[e.soid].need.version);
      missing[e.soid].need = e.version;  // leave .have unchanged.
    } else if (e.is_backlog()) {
      // May not have prior version
      assert(0 == "these don't exist anymore");
    } else {
      // not missing, we must have prior_version (if any)
      missing[e.soid] = item(e.version, e.prior_version);
    }
    rmissing[e.version.version] = e.soid;
  } else
    rm(e.soid, e.version);
}

void pg_missing_t::revise_need(hobject_t oid, eversion_t need)
{
  if (missing.count(oid)) {
    rmissing.erase(missing[oid].need.version);
    missing[oid].need = need;            // no not adjust .have
  } else {
    missing[oid] = item(need, eversion_t());
  }
  rmissing[need.version] = oid;
}

void pg_missing_t::revise_have(hobject_t oid, eversion_t have)
{
  if (missing.count(oid)) {
    missing[oid].have = have;
  }
}

void pg_missing_t::add(const hobject_t& oid, eversion_t need, eversion_t have)
{
  missing[oid] = item(need, have);
  rmissing[need.version] = oid;
}

void pg_missing_t::rm(const hobject_t& oid, eversion_t v)
{
  std::map<hobject_t, pg_missing_t::item>::iterator p = missing.find(oid);
  if (p != missing.end() && p->second.need <= v)
    rm(p);
}

void pg_missing_t::rm(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
{
  rmissing.erase(m->second.need.version);
  missing.erase(m);
}

void pg_missing_t::got(const hobject_t& oid, eversion_t v)
{
  std::map<hobject_t, pg_missing_t::item>::iterator p = missing.find(oid);
  assert(p != missing.end());
  assert(p->second.need <= v);
  got(p);
}

void pg_missing_t::got(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
{
  rmissing.erase(m->second.need.version);
  missing.erase(m);
}

void pg_missing_t::split_into(
  pg_t child_pgid,
  unsigned split_bits,
  pg_missing_t *omissing)
{
  unsigned mask = ~((~0)<<split_bits);
  for (map<hobject_t, item>::iterator i = missing.begin();
       i != missing.end();
       ) {
    if ((i->first.hash & mask) == child_pgid.m_seed) {
      omissing->add(i->first, i->second.need, i->second.have);
      rm(i++);
    } else {
      ++i;
    }
  }
}

// -- pg_create_t --

void pg_create_t::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(created, bl);
  ::encode(parent, bl);
  ::encode(split_bits, bl);
  ENCODE_FINISH(bl);
}

void pg_create_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(created, bl);
  ::decode(parent, bl);
  ::decode(split_bits, bl);
  DECODE_FINISH(bl);
}

void pg_create_t::dump(Formatter *f) const
{
  f->dump_unsigned("created", created);
  f->dump_stream("parent") << parent;
  f->dump_int("split_bits", split_bits);
}

void pg_create_t::generate_test_instances(list<pg_create_t*>& o)
{
  o.push_back(new pg_create_t);
  o.push_back(new pg_create_t(1, pg_t(3, 4, -1), 2));
}


// -- osd_peer_stat_t --

void osd_peer_stat_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void osd_peer_stat_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void osd_peer_stat_t::dump(Formatter *f) const
{
  f->dump_stream("stamp") << stamp;
}

void osd_peer_stat_t::generate_test_instances(list<osd_peer_stat_t*>& o)
{
  o.push_back(new osd_peer_stat_t);
  o.push_back(new osd_peer_stat_t);
  o.back()->stamp = utime_t(1, 2);
}

ostream& operator<<(ostream& out, const osd_peer_stat_t &stat)
{
  return out << "stat(" << stat.stamp << ")";
}


// -- OSDSuperblock --

void OSDSuperblock::encode(bufferlist &bl) const
{
  ENCODE_START(5, 5, bl);
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
  ENCODE_FINISH(bl);
}

void OSDSuperblock::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
  if (struct_v < 3) {
    string magic;
    ::decode(magic, bl);
  }
  ::decode(cluster_fsid, bl);
  ::decode(whoami, bl);
  ::decode(current_epoch, bl);
  ::decode(oldest_map, bl);
  ::decode(newest_map, bl);
  ::decode(weight, bl);
  if (struct_v >= 2) {
    compat_features.decode(bl);
  } else { //upgrade it!
    compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  }
  ::decode(clean_thru, bl);
  ::decode(mounted, bl);
  if (struct_v >= 4)
    ::decode(osd_fsid, bl);
  DECODE_FINISH(bl);
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

void OSDSuperblock::generate_test_instances(list<OSDSuperblock*>& o)
{
  OSDSuperblock z;
  o.push_back(new OSDSuperblock(z));
  memset(&z.cluster_fsid, 1, sizeof(z.cluster_fsid));
  memset(&z.osd_fsid, 2, sizeof(z.osd_fsid));
  z.whoami = 3;
  z.current_epoch = 4;
  z.oldest_map = 5;
  z.newest_map = 9;
  z.mounted = 8;
  z.clean_thru = 7;
  o.push_back(new OSDSuperblock(z));
}

// -- SnapSet --

void SnapSet::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(seq, bl);
  ::encode(head_exists, bl);
  ::encode(snaps, bl);
  ::encode(clones, bl);
  ::encode(clone_overlap, bl);
  ::encode(clone_size, bl);
  ENCODE_FINISH(bl);
}

void SnapSet::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(seq, bl);
  ::decode(head_exists, bl);
  ::decode(snaps, bl);
  ::decode(clones, bl);
  ::decode(clone_overlap, bl);
  ::decode(clone_size, bl);
  DECODE_FINISH(bl);
}

void SnapSet::dump(Formatter *f) const
{
  SnapContext sc(seq, snaps);
  f->open_object_section("snap_context");
  sc.dump(f);
  f->close_section();
  f->dump_int("head_exists", head_exists);
  f->open_array_section("clones");
  for (vector<snapid_t>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
    f->open_object_section("clone");
    f->dump_unsigned("snap", *p);
    f->dump_unsigned("size", clone_size.find(*p)->second);
    f->dump_stream("overlap") << clone_overlap.find(*p)->second;
    f->close_section();
  }
  f->close_section();
}

void SnapSet::generate_test_instances(list<SnapSet*>& o)
{
  o.push_back(new SnapSet);
  o.push_back(new SnapSet);
  o.back()->head_exists = true;
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.push_back(new SnapSet);
  o.back()->head_exists = true;
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.back()->clones.push_back(12);
  o.back()->clone_size[12] = 12345;
  o.back()->clone_overlap[12];
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
  ENCODE_START(3, 3, bl);
  ::encode(cookie, bl);
  ::encode(timeout_seconds, bl);
  ENCODE_FINISH(bl);
}

void watch_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  ::decode(cookie, bl);
  if (struct_v < 2) {
    uint64_t ver;
    ::decode(ver, bl);
  }
  ::decode(timeout_seconds, bl);
  DECODE_FINISH(bl);
}

void watch_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("cookie", cookie);
  f->dump_unsigned("timeout_seconds", timeout_seconds);
}

void watch_info_t::generate_test_instances(list<watch_info_t*>& o)
{
  o.push_back(new watch_info_t);
  o.push_back(new watch_info_t);
  o.back()->cookie = 123;
  o.back()->timeout_seconds = 99;
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
  uses_tmap = other.uses_tmap;
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
  return ps;
}

void object_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(10, 8, bl);
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
  ::encode(uses_tmap, bl);
  ENCODE_FINISH(bl);
}

void object_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(10, 8, 8, bl);
  if (struct_v >= 2 && struct_v <= 5) {
    sobject_t obj;
    ::decode(obj, bl);
    ::decode(oloc, bl);
    soid = hobject_t(obj.oid, oloc.key, obj.snap, 0, 0);
    soid.hash = legacy_object_locator_to_ps(soid.oid, oloc);
  } else if (struct_v >= 6) {
    ::decode(soid, bl);
    ::decode(oloc, bl);
    if (struct_v == 6) {
      hobject_t hoid(soid.oid, oloc.key, soid.snap, soid.hash, 0);
      soid = hoid;
    }
  }
    
  if (struct_v >= 5)
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
  if (struct_v >= 3)
    ::decode(lost, bl);
  else
    lost = false;
  if (struct_v >= 4) {
    ::decode(watchers, bl);
    ::decode(user_version, bl);
  }
  if (struct_v >= 9)
    ::decode(uses_tmap, bl);
  else
    uses_tmap = true;
  if (struct_v < 10)
    soid.pool = oloc.pool;
  DECODE_FINISH(bl);
}

void object_info_t::dump(Formatter *f) const
{
  f->open_object_section("oid");
  soid.dump(f);
  f->close_section();
  f->open_object_section("locator");
  oloc.dump(f);
  f->close_section();
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

void object_info_t::generate_test_instances(list<object_info_t*>& o)
{
  o.push_back(new object_info_t());
  
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

// -- ObjectRecovery --
void ObjectRecoveryProgress::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(first, bl);
  ::encode(data_complete, bl);
  ::encode(data_recovered_to, bl);
  ::encode(omap_recovered_to, bl);
  ::encode(omap_complete, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryProgress::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(first, bl);
  ::decode(data_complete, bl);
  ::decode(data_recovered_to, bl);
  ::decode(omap_recovered_to, bl);
  ::decode(omap_complete, bl);
  DECODE_FINISH(bl);
}

ostream &operator<<(ostream &out, const ObjectRecoveryProgress &prog)
{
  return prog.print(out);
}

void ObjectRecoveryProgress::generate_test_instances(
  list<ObjectRecoveryProgress*>& o)
{
  o.push_back(new ObjectRecoveryProgress);
  o.back()->first = false;
  o.back()->data_complete = true;
  o.back()->omap_complete = true;
  o.back()->data_recovered_to = 100;

  o.push_back(new ObjectRecoveryProgress);
  o.back()->first = true;
  o.back()->data_complete = false;
  o.back()->omap_complete = false;
  o.back()->data_recovered_to = 0;
}

ostream &ObjectRecoveryProgress::print(ostream &out) const
{
  return out << "ObjectRecoveryProgress("
	     << ( first ? "" : "!" ) << "first, "
	     << "data_recovered_to:" << data_recovered_to
	     << ", data_complete:" << ( data_complete ? "true" : "false" )
	     << ", omap_recovered_to:" << omap_recovered_to
	     << ", omap_complete:" << ( omap_complete ? "true" : "false" )
	     << ")";
}

void ObjectRecoveryProgress::dump(Formatter *f) const
{
  f->dump_int("first?", first);
  f->dump_int("data_complete?", data_complete);
  f->dump_unsigned("data_recovered_to", data_recovered_to);
  f->dump_int("omap_complete?", omap_complete);
  f->dump_string("omap_recovered_to", omap_recovered_to);
}

void ObjectRecoveryInfo::encode(bufferlist &bl) const
{
  ENCODE_START(2, 1, bl);
  ::encode(soid, bl);
  ::encode(version, bl);
  ::encode(size, bl);
  ::encode(oi, bl);
  ::encode(ss, bl);
  ::encode(copy_subset, bl);
  ::encode(clone_subset, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryInfo::decode(bufferlist::iterator &bl,
				int64_t pool)
{
  DECODE_START(2, bl);
  ::decode(soid, bl);
  ::decode(version, bl);
  ::decode(size, bl);
  ::decode(oi, bl);
  ::decode(ss, bl);
  ::decode(copy_subset, bl);
  ::decode(clone_subset, bl);
  DECODE_FINISH(bl);

  if (struct_v < 2) {
    if (soid.pool == -1)
      soid.pool = pool;
    map<hobject_t, interval_set<uint64_t> > tmp;
    tmp.swap(clone_subset);
    for (map<hobject_t, interval_set<uint64_t> >::iterator i = tmp.begin();
	 i != tmp.end();
	 ++i) {
      hobject_t first(i->first);
      if (first.pool == -1)
	first.pool = pool;
      clone_subset[first].swap(i->second);
    }
  }
}

void ObjectRecoveryInfo::generate_test_instances(
  list<ObjectRecoveryInfo*>& o)
{
  o.push_back(new ObjectRecoveryInfo);
  o.back()->soid = hobject_t(sobject_t("key", CEPH_NOSNAP));
  o.back()->version = eversion_t(0,0);
  o.back()->size = 100;
}


void ObjectRecoveryInfo::dump(Formatter *f) const
{
  f->dump_stream("object") << soid;
  f->dump_stream("at_version") << version;
  f->dump_stream("size") << size;
  {
    f->open_object_section("object_info");
    oi.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("snapset");
    ss.dump(f);
    f->close_section();
  }
  f->dump_stream("copy_subset") << copy_subset;
  f->dump_stream("clone_subset") << clone_subset;
}

ostream& operator<<(ostream& out, const ObjectRecoveryInfo &inf)
{
  return inf.print(out);
}

ostream &ObjectRecoveryInfo::print(ostream &out) const
{
  return out << "ObjectRecoveryInfo("
	     << soid << "@" << version
	     << ", copy_subset: " << copy_subset
	     << ", clone_subset: " << clone_subset
	     << ")";
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
  ENCODE_START(3, 2, bl);
  ::encode(objects, bl);
  ::encode(attrs, bl);
  ::encode(logbl, bl);
  ::encode(valid_through, bl);
  ::encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::decode(bufferlist::iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(objects, bl);
  ::decode(attrs, bl);
  ::decode(logbl, bl);
  ::decode(valid_through, bl);
  ::decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (map<hobject_t, object>::iterator i = tmp.begin();
	 i != tmp.end();
	 ++i) {
      hobject_t first(i->first);
      if (first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void ScrubMap::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->open_array_section("objects");
  for (map<hobject_t,object>::const_iterator p = objects.begin(); p != objects.end(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.hash);
    f->dump_string("key", p->first.get_key());
    f->dump_int("snapid", p->first.snap);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void ScrubMap::generate_test_instances(list<ScrubMap*>& o)
{
  o.push_back(new ScrubMap);
  o.push_back(new ScrubMap);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  o.back()->attrs["foo"] = buffer::copy("foo", 3);
  o.back()->attrs["bar"] = buffer::copy("barval", 6);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0)] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0)] = *obj.back();
}

// -- ScrubMap::object --

void ScrubMap::object::encode(bufferlist& bl) const
{
  ENCODE_START(5, 2, bl);
  ::encode(size, bl);
  ::encode(negative, bl);
  ::encode(attrs, bl);
  ::encode(digest, bl);
  ::encode(digest_present, bl);
  ::encode(nlinks, bl);
  ::encode(snapcolls, bl);
  ::encode(omap_digest, bl);
  ::encode(omap_digest_present, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::object::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
  ::decode(size, bl);
  ::decode(negative, bl);
  ::decode(attrs, bl);
  if (struct_v >= 3) {
    ::decode(digest, bl);
    ::decode(digest_present, bl);
  }
  if (struct_v >= 4) {
    ::decode(nlinks, bl);
    ::decode(snapcolls, bl);
  } else {
    /* Indicates that encoder was not aware of this field since stat must
     * return nlink >= 1 */
    nlinks = 0;
  }
  if (struct_v >= 5) {
    ::decode(omap_digest, bl);
    ::decode(omap_digest_present, bl);
  }
  DECODE_FINISH(bl);
}

void ScrubMap::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

void ScrubMap::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  o.back()->attrs["foo"] = buffer::copy("foo", 3);
  o.back()->attrs["bar"] = buffer::copy("barval", 6);
}

// -- OSDOp --

ostream& operator<<(ostream& out, const OSDOp& op)
{
  out << ceph_osd_op_name(op.op.op);
  if (ceph_osd_op_type_data(op.op.op)) {
    // data extent
    switch (op.op.op) {
    case CEPH_OSD_OP_STAT:
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
