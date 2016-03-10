// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "OSDMap.h"

#include "common/config.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "include/ceph_features.h"
#include "include/str_map.h"
#include "include/stringify.h"

#include "common/code_environment.h"

#include "crush/CrushTreeDumper.h"

#define dout_subsys ceph_subsys_osd

// ----------------------------------
// osd_info_t

void osd_info_t::dump(Formatter *f) const
{
  f->dump_int("last_clean_begin", last_clean_begin);
  f->dump_int("last_clean_end", last_clean_end);
  f->dump_int("up_from", up_from);
  f->dump_int("up_thru", up_thru);
  f->dump_int("down_at", down_at);
  f->dump_int("lost_at", lost_at);
}

void osd_info_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(last_clean_begin, bl);
  ::encode(last_clean_end, bl);
  ::encode(up_from, bl);
  ::encode(up_thru, bl);
  ::encode(down_at, bl);
  ::encode(lost_at, bl);
}

void osd_info_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(last_clean_begin, bl);
  ::decode(last_clean_end, bl);
  ::decode(up_from, bl);
  ::decode(up_thru, bl);
  ::decode(down_at, bl);
  ::decode(lost_at, bl);
}

void osd_info_t::generate_test_instances(list<osd_info_t*>& o)
{
  o.push_back(new osd_info_t);
  o.push_back(new osd_info_t);
  o.back()->last_clean_begin = 1;
  o.back()->last_clean_end = 2;
  o.back()->up_from = 30;
  o.back()->up_thru = 40;
  o.back()->down_at = 5;
  o.back()->lost_at = 6;
}

ostream& operator<<(ostream& out, const osd_info_t& info)
{
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at
      << " last_clean_interval [" << info.last_clean_begin << "," << info.last_clean_end << ")";
  if (info.lost_at)
    out << " lost_at " << info.lost_at;
  return out;
}

// ----------------------------------
// osd_xinfo_t

void osd_xinfo_t::dump(Formatter *f) const
{
  f->dump_stream("down_stamp") << down_stamp;
  f->dump_float("laggy_probability", laggy_probability);
  f->dump_int("laggy_interval", laggy_interval);
  f->dump_int("features", features);
  f->dump_unsigned("old_weight", old_weight);
}

void osd_xinfo_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 1, bl);
  ::encode(down_stamp, bl);
  __u32 lp = laggy_probability * 0xfffffffful;
  ::encode(lp, bl);
  ::encode(laggy_interval, bl);
  ::encode(features, bl);
  ::encode(old_weight, bl);
  ENCODE_FINISH(bl);
}

void osd_xinfo_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(3, bl);
  ::decode(down_stamp, bl);
  __u32 lp;
  ::decode(lp, bl);
  laggy_probability = (float)lp / (float)0xffffffff;
  ::decode(laggy_interval, bl);
  if (struct_v >= 2)
    ::decode(features, bl);
  else
    features = 0;
  if (struct_v >= 3)
    ::decode(old_weight, bl);
  else
    old_weight = 0;
  DECODE_FINISH(bl);
}

void osd_xinfo_t::generate_test_instances(list<osd_xinfo_t*>& o)
{
  o.push_back(new osd_xinfo_t);
  o.push_back(new osd_xinfo_t);
  o.back()->down_stamp = utime_t(2, 3);
  o.back()->laggy_probability = .123;
  o.back()->laggy_interval = 123456;
  o.back()->old_weight = 0x7fff;
}

ostream& operator<<(ostream& out, const osd_xinfo_t& xi)
{
  return out << "down_stamp " << xi.down_stamp
	     << " laggy_probability " << xi.laggy_probability
	     << " laggy_interval " << xi.laggy_interval
	     << " old_weight " << xi.old_weight;
}

// ----------------------------------
// OSDMap::Incremental

int OSDMap::Incremental::get_net_marked_out(const OSDMap *previous) const
{
  int n = 0;
  for (map<int32_t,uint32_t>::const_iterator p = new_weight.begin();
       p != new_weight.end();
       ++p) {
    if (p->second == CEPH_OSD_OUT && !previous->is_out(p->first))
      n++;  // marked out
    if (p->second != CEPH_OSD_OUT && previous->is_out(p->first))
      n--;  // marked in
  }
  return n;
}

int OSDMap::Incremental::get_net_marked_down(const OSDMap *previous) const
{
  int n = 0;
  for (map<int32_t,uint8_t>::const_iterator p = new_state.begin();
       p != new_state.end();
       ++p) {
    if (p->second & CEPH_OSD_UP) {
      if (previous->is_up(p->first))
	n++;  // marked down
      else
	n--;  // marked up
    }
  }
  return n;
}

int OSDMap::Incremental::identify_osd(uuid_d u) const
{
  for (map<int32_t,uuid_d>::const_iterator p = new_uuid.begin();
       p != new_uuid.end();
       ++p)
    if (p->second == u)
      return p->first;
  return -1;
}

int OSDMap::Incremental::propagate_snaps_to_tiers(CephContext *cct,
						  const OSDMap& osdmap)
{
  assert(epoch == osdmap.get_epoch() + 1);
  for (map<int64_t,pg_pool_t>::iterator p = new_pools.begin();
       p != new_pools.end(); ++p) {
    if (!p->second.tiers.empty()) {
      pg_pool_t& base = p->second;
      for (set<uint64_t>::const_iterator q = base.tiers.begin();
	   q != base.tiers.end();
	   ++q) {
	map<int64_t,pg_pool_t>::iterator r = new_pools.find(*q);
	pg_pool_t *tier = 0;
	if (r == new_pools.end()) {
	  const pg_pool_t *orig = osdmap.get_pg_pool(*q);
	  if (!orig) {
	    lderr(cct) << __func__ << " no pool " << *q << dendl;
	    return -EIO;
	  }
	  tier = get_new_pool(*q, orig);
	} else {
	  tier = &r->second;
	}
	if (tier->tier_of != p->first) {
	  lderr(cct) << __func__ << " " << r->first << " tier_of != " << p->first << dendl;
	  return -EIO;
	}

	ldout(cct, 10) << __func__ << " from " << p->first << " to "
		       << r->first << dendl;
	tier->snap_seq = base.snap_seq;
	tier->snap_epoch = base.snap_epoch;
	tier->snaps = base.snaps;
	tier->removed_snaps = base.removed_snaps;
      }
    }
  }
  return 0;
}


bool OSDMap::subtree_is_down(int id, set<int> *down_cache) const
{
  if (id >= 0)
    return is_down(id);

  if (down_cache &&
      down_cache->count(id)) {
    return true;
  }

  list<int> children;
  crush->get_children(id, &children);
  for (list<int>::iterator p = children.begin(); p != children.end(); ++p) {
    if (!subtree_is_down(*p, down_cache)) {
      return false;
    }
  }
  if (down_cache) {
    down_cache->insert(id);
  }
  return true;
}

bool OSDMap::containing_subtree_is_down(CephContext *cct, int id, int subtree_type, set<int> *down_cache) const
{
  // use a stack-local down_cache if we didn't get one from the
  // caller.  then at least this particular call will avoid duplicated
  // work.
  set<int> local_down_cache;
  if (!down_cache) {
    down_cache = &local_down_cache;
  }

  int current = id;
  while (true) {
    int type;
    if (current >= 0) {
      type = 0;
    } else {
      type = crush->get_bucket_type(current);
    }
    assert(type >= 0);

    if (!subtree_is_down(current, down_cache)) {
      ldout(cct, 30) << "containing_subtree_is_down(" << id << ") = false" << dendl;
      return false;
    }

    // is this a big enough subtree to be done?
    if (type >= subtree_type) {
      ldout(cct, 30) << "containing_subtree_is_down(" << id << ") = true ... " << type << " >= " << subtree_type << dendl;
      return true;
    }

    int r = crush->get_immediate_parent_id(current, &current);
    if (r < 0) {
      return false;
    }
  }
}

void OSDMap::Incremental::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  int32_t new_t = new_pool_max;
  ::encode(new_t, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  // for ::encode(new_pools, bl);
  __u32 n = new_pools.size();
  ::encode(n, bl);
  for (map<int64_t,pg_pool_t>::const_iterator p = new_pools.begin();
       p != new_pools.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl, 0);
  }
  // for ::encode(new_pool_names, bl);
  n = new_pool_names.size();
  ::encode(n, bl);
  for (map<int64_t, string>::const_iterator p = new_pool_names.begin(); p != new_pool_names.end(); ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl);
  }
  // for ::encode(old_pools, bl);
  n = old_pools.size();
  ::encode(n, bl);
  for (set<int64_t>::iterator p = old_pools.begin(); p != old_pools.end(); ++p) {
    n = *p;
    ::encode(n, bl);
  }
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  // for ::encode(new_pg_temp, bl);
  n = new_pg_temp.size();
  ::encode(n, bl);
  for (map<pg_t,vector<int32_t> >::const_iterator p = new_pg_temp.begin();
       p != new_pg_temp.end();
       ++p) {
    old_pg_t opg = p->first.get_old_pg();
    ::encode(opg, bl);
    ::encode(p->second, bl);
  }
}

void OSDMap::Incremental::encode_classic(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  // base
  __u16 v = 6;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  ::encode(new_pool_max, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  ::encode(new_pools, bl, features);
  ::encode(new_pool_names, bl);
  ::encode(old_pools, bl);
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  ::encode(new_pg_temp, bl);

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(new_hb_back_up, bl);
  ::encode(new_up_thru, bl);
  ::encode(new_last_clean_interval, bl);
  ::encode(new_lost, bl);
  ::encode(new_blacklist, bl);
  ::encode(old_blacklist, bl);
  ::encode(new_up_cluster, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(new_uuid, bl);
  ::encode(new_xinfo, bl);
  ::encode(new_hb_front_up, bl);
}

void OSDMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  buffer::list::iterator crc_it;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    ENCODE_START(3, 1, bl); // client-usable data
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(modified, bl);
    ::encode(new_pool_max, bl);
    ::encode(new_flags, bl);
    ::encode(fullmap, bl);
    ::encode(crush, bl);

    ::encode(new_max_osd, bl);
    ::encode(new_pools, bl, features);
    ::encode(new_pool_names, bl);
    ::encode(old_pools, bl);
    ::encode(new_up_client, bl);
    ::encode(new_state, bl);
    ::encode(new_weight, bl);
    ::encode(new_pg_temp, bl);
    ::encode(new_primary_temp, bl);
    ::encode(new_primary_affinity, bl);
    ::encode(new_erasure_code_profiles, bl);
    ::encode(old_erasure_code_profiles, bl);
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    ENCODE_START(2, 1, bl); // extended, osd-only data
    ::encode(new_hb_back_up, bl);
    ::encode(new_up_thru, bl);
    ::encode(new_last_clean_interval, bl);
    ::encode(new_lost, bl);
    ::encode(new_blacklist, bl);
    ::encode(old_blacklist, bl);
    ::encode(new_up_cluster, bl);
    ::encode(cluster_snapshot, bl);
    ::encode(new_uuid, bl);
    ::encode(new_xinfo, bl);
    ::encode(new_hb_front_up, bl);
    ::encode(features, bl);         // NOTE: features arg, not the member
    ENCODE_FINISH(bl); // osd-only data
  }

  ::encode((uint32_t)0, bl); // dummy inc_crc
  crc_it = bl.end();
  crc_it.advance(-4);
  tail_offset = bl.length();

  ::encode(full_crc, bl);

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  bufferlist front;
  front.substr_of(bl, start_offset, crc_it.get_off() - start_offset);
  inc_crc = front.crc32c(-1);
  bufferlist tail;
  tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
  inc_crc = tail.crc32c(inc_crc);
  ceph_le32 crc_le;
  crc_le = inc_crc;
  crc_it.copy_in(4, (char*)&crc_le);
  have_crc = true;
}

void OSDMap::Incremental::decode_classic(bufferlist::iterator &p)
{
  __u32 n, t;
  // base
  __u16 v;
  ::decode(v, p);
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(modified, p);
  if (v == 4 || v == 5) {
    ::decode(n, p);
    new_pool_max = n;
  } else if (v >= 6)
    ::decode(new_pool_max, p);
  ::decode(new_flags, p);
  ::decode(fullmap, p);
  ::decode(crush, p);

  ::decode(new_max_osd, p);
  if (v < 6) {
    new_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pools[t], p);
    }
  } else {
    ::decode(new_pools, p);
  }
  if (v == 5) {
    new_pool_names.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pool_names[t], p);
    }
  } else if (v >= 6) {
    ::decode(new_pool_names, p);
  }
  if (v < 6) {
    old_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      old_pools.insert(t);
    }
  } else {
    ::decode(old_pools, p);
  }
  ::decode(new_up_client, p);
  ::decode(new_state, p);
  ::decode(new_weight, p);

  if (v < 6) {
    new_pg_temp.clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode(new_pg_temp[pg_t(opg)], p);
    }
  } else {
    ::decode(new_pg_temp, p);
  }

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(new_hb_back_up, p);
  if (v < 5)
    ::decode(new_pool_names, p);
  ::decode(new_up_thru, p);
  ::decode(new_last_clean_interval, p);
  ::decode(new_lost, p);
  ::decode(new_blacklist, p);
  ::decode(old_blacklist, p);
  if (ev >= 6)
    ::decode(new_up_cluster, p);
  if (ev >= 7)
    ::decode(cluster_snapshot, p);
  if (ev >= 8)
    ::decode(new_uuid, p);
  if (ev >= 9)
    ::decode(new_xinfo, p);
  if (ev >= 10)
    ::decode(new_hb_front_up, p);
}

void OSDMap::Incremental::decode(bufferlist::iterator& bl)
{
  /**
   * Older encodings of the Incremental had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  bufferlist crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    int struct_v_size = sizeof(struct_v);
    bl.advance(-struct_v_size);
    decode_classic(bl);
    encode_features = 0;
    if (struct_v >= 6)
      encode_features = CEPH_FEATURE_PGID64;
    else
      encode_features = 0;
    return;
  }
  {
    DECODE_START(3, bl); // client-usable data
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(modified, bl);
    ::decode(new_pool_max, bl);
    ::decode(new_flags, bl);
    ::decode(fullmap, bl);
    ::decode(crush, bl);

    ::decode(new_max_osd, bl);
    ::decode(new_pools, bl);
    ::decode(new_pool_names, bl);
    ::decode(old_pools, bl);
    ::decode(new_up_client, bl);
    ::decode(new_state, bl);
    ::decode(new_weight, bl);
    ::decode(new_pg_temp, bl);
    ::decode(new_primary_temp, bl);
    if (struct_v >= 2)
      ::decode(new_primary_affinity, bl);
    else
      new_primary_affinity.clear();
    if (struct_v >= 3) {
      ::decode(new_erasure_code_profiles, bl);
      ::decode(old_erasure_code_profiles, bl);
    } else {
      new_erasure_code_profiles.clear();
      old_erasure_code_profiles.clear();
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(2, bl); // extended, osd-only data
    ::decode(new_hb_back_up, bl);
    ::decode(new_up_thru, bl);
    ::decode(new_last_clean_interval, bl);
    ::decode(new_lost, bl);
    ::decode(new_blacklist, bl);
    ::decode(old_blacklist, bl);
    ::decode(new_up_cluster, bl);
    ::decode(cluster_snapshot, bl);
    ::decode(new_uuid, bl);
    ::decode(new_xinfo, bl);
    ::decode(new_hb_front_up, bl);
    if (struct_v >= 2)
      ::decode(encode_features, bl);
    else
      encode_features = CEPH_FEATURE_PGID64 | CEPH_FEATURE_OSDMAP_ENC;
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    have_crc = true;
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    ::decode(inc_crc, bl);
    tail_offset = bl.get_off();
    ::decode(full_crc, bl);
  } else {
    have_crc = false;
    full_crc = 0;
    inc_crc = 0;
  }

  DECODE_FINISH(bl); // wrapper

  if (have_crc) {
    // verify crc
    uint32_t actual = crc_front.crc32c(-1);
    if (tail_offset < bl.get_off()) {
      bufferlist tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (inc_crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << inc_crc;
      string s = ss.str();
      throw buffer::malformed_input(s.c_str());
    }
  }
}

void OSDMap::Incremental::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") << fsid;
  f->dump_stream("modified") << modified;
  f->dump_int("new_pool_max", new_pool_max);
  f->dump_int("new_flags", new_flags);

  if (fullmap.length()) {
    f->open_object_section("full_map");
    OSDMap full;
    bufferlist fbl = fullmap;  // kludge around constness.
    bufferlist::iterator p = fbl.begin();
    full.decode(p);
    full.dump(f);
    f->close_section();
  }
  if (crush.length()) {
    f->open_object_section("crush");
    CrushWrapper c;
    bufferlist tbl = crush;  // kludge around constness.
    bufferlist::iterator p = tbl.begin();
    c.decode(p);
    c.dump(f);
    f->close_section();
  }

  f->dump_int("new_max_osd", new_max_osd);

  f->open_array_section("new_pools");
  for (map<int64_t,pg_pool_t>::const_iterator p = new_pools.begin(); p != new_pools.end(); ++p) {
    f->open_object_section("pool");
    f->dump_int("pool", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_pool_names");
  for (map<int64_t,string>::const_iterator p = new_pool_names.begin(); p != new_pool_names.end(); ++p) {
    f->open_object_section("pool_name");
    f->dump_int("pool", p->first);
    f->dump_string("name", p->second);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pools");
  for (set<int64_t>::const_iterator p = old_pools.begin(); p != old_pools.end(); ++p)
    f->dump_int("pool", *p);
  f->close_section();

  f->open_array_section("new_up_osds");
  for (map<int32_t,entity_addr_t>::const_iterator p = new_up_client.begin(); p != new_up_client.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("public_addr") << p->second;
    f->dump_stream("cluster_addr") << new_up_cluster.find(p->first)->second;
    f->dump_stream("heartbeat_back_addr") << new_hb_back_up.find(p->first)->second;
    map<int32_t, entity_addr_t>::const_iterator q;
    if ((q = new_hb_front_up.find(p->first)) != new_hb_front_up.end())
      f->dump_stream("heartbeat_front_addr") << q->second;
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_weight");
  for (map<int32_t,uint32_t>::const_iterator p = new_weight.begin(); p != new_weight.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("weight", p->second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_state_xor");
  for (map<int32_t,uint8_t>::const_iterator p = new_state.begin(); p != new_state.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    set<string> st;
    calc_state_set(new_state.find(p->first)->second, st);
    f->open_array_section("state_xor");
    for (set<string>::iterator p = st.begin(); p != st.end(); ++p)
      f->dump_string("state", *p);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_pg_temp");
  for (map<pg_t,vector<int32_t> >::const_iterator p = new_pg_temp.begin();
       p != new_pg_temp.end();
       ++p) {
    f->open_object_section("pg");
    f->dump_stream("pgid") << p->first;
    f->open_array_section("osds");
    for (vector<int>::const_iterator q = p->second.begin(); q != p->second.end(); ++q)
      f->dump_int("osd", *q);
    f->close_section();
    f->close_section();    
  }
  f->close_section();

  f->open_array_section("primary_temp");
  for (map<pg_t, int32_t>::const_iterator p = new_primary_temp.begin();
      p != new_primary_temp.end();
      ++p) {
    f->dump_stream("pgid") << p->first;
    f->dump_int("osd", p->second);
  }
  f->close_section(); // primary_temp

  f->open_array_section("new_up_thru");
  for (map<int32_t,uint32_t>::const_iterator p = new_up_thru.begin(); p != new_up_thru.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("up_thru", p->second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_lost");
  for (map<int32_t,uint32_t>::const_iterator p = new_lost.begin(); p != new_lost.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("epoch_lost", p->second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_last_clean_interval");
  for (map<int32_t,pair<epoch_t,epoch_t> >::const_iterator p = new_last_clean_interval.begin();
       p != new_last_clean_interval.end();
       ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("first", p->second.first);
    f->dump_int("last", p->second.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_blacklist");
  for (map<entity_addr_t,utime_t>::const_iterator p = new_blacklist.begin();
       p != new_blacklist.end();
       ++p) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();
  f->open_array_section("old_blacklist");
  for (vector<entity_addr_t>::const_iterator p = old_blacklist.begin(); p != old_blacklist.end(); ++p)
    f->dump_stream("addr") << *p;
  f->close_section();

  f->open_array_section("new_xinfo");
  for (map<int32_t,osd_xinfo_t>::const_iterator p = new_xinfo.begin(); p != new_xinfo.end(); ++p) {
    f->open_object_section("xinfo");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  if (cluster_snapshot.size())
    f->dump_string("cluster_snapshot", cluster_snapshot);

  f->open_array_section("new_uuid");
  for (map<int32_t,uuid_d>::const_iterator p = new_uuid.begin(); p != new_uuid.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("uuid") << p->second;
    f->close_section();
  }
  f->close_section();

  OSDMap::dump_erasure_code_profiles(new_erasure_code_profiles, f);
  f->open_array_section("old_erasure_code_profiles");
  for (vector<string>::const_iterator p = old_erasure_code_profiles.begin();
       p != old_erasure_code_profiles.end();
       ++p) {
    f->dump_string("old", p->c_str());
  }
  f->close_section();
}

void OSDMap::Incremental::generate_test_instances(list<Incremental*>& o)
{
  o.push_back(new Incremental);
}

// ----------------------------------
// OSDMap

void OSDMap::set_epoch(epoch_t e)
{
  epoch = e;
  for (map<int64_t,pg_pool_t>::iterator p = pools.begin();
       p != pools.end();
       ++p)
    p->second.last_change = e;
}

bool OSDMap::is_blacklisted(const entity_addr_t& a) const
{
  if (blacklist.empty())
    return false;

  // this specific instance?
  if (blacklist.count(a))
    return true;

  // is entire ip blacklisted?
  if (a.is_ip()) {
    entity_addr_t b = a;
    b.set_port(0);
    b.set_nonce(0);
    if (blacklist.count(b)) {
      return true;
    }
  }

  return false;
}

void OSDMap::get_blacklist(list<pair<entity_addr_t,utime_t> > *bl) const
{
  for (ceph::unordered_map<entity_addr_t,utime_t>::const_iterator it = blacklist.begin() ;
			 it != blacklist.end(); ++it) {
    bl->push_back(*it);
  }
}

void OSDMap::set_max_osd(int m)
{
  int o = max_osd;
  max_osd = m;
  osd_state.resize(m);
  osd_weight.resize(m);
  for (; o<max_osd; o++) {
    osd_state[o] = 0;
    osd_weight[o] = CEPH_OSD_OUT;
  }
  osd_info.resize(m);
  osd_xinfo.resize(m);
  osd_addrs->client_addr.resize(m);
  osd_addrs->cluster_addr.resize(m);
  osd_addrs->hb_back_addr.resize(m);
  osd_addrs->hb_front_addr.resize(m);
  osd_uuid->resize(m);
  if (osd_primary_affinity)
    osd_primary_affinity->resize(m, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);

  calc_num_osds();
}

int OSDMap::calc_num_osds()
{
  num_osd = 0;
  for (int i=0; i<max_osd; i++)
    if (osd_state[i] & CEPH_OSD_EXISTS)
      num_osd++;
  return num_osd;
}

void OSDMap::get_all_osds(set<int32_t>& ls) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i))
      ls.insert(i);
}

void OSDMap::get_up_osds(set<int32_t>& ls) const
{
  for (int i = 0; i < max_osd; i++) {
    if (is_up(i))
      ls.insert(i);
  }
}

unsigned OSDMap::get_num_up_osds() const
{
  unsigned n = 0;
  for (int i=0; i<max_osd; i++)
    if ((osd_state[i] & CEPH_OSD_EXISTS) &&
	(osd_state[i] & CEPH_OSD_UP)) n++;
  return n;
}

unsigned OSDMap::get_num_in_osds() const
{
  unsigned n = 0;
  for (int i=0; i<max_osd; i++)
    if ((osd_state[i] & CEPH_OSD_EXISTS) &&
	get_weight(i) != CEPH_OSD_OUT) n++;
  return n;
}

void OSDMap::calc_state_set(int state, set<string>& st)
{
  unsigned t = state;
  for (unsigned s = 1; t; s <<= 1) {
    if (t & s) {
      t &= ~s;
      st.insert(ceph_osd_state_name(s));
    }
  }
}

void OSDMap::adjust_osd_weights(const map<int,double>& weights, Incremental& inc) const
{
  float max = 0;
  for (map<int,double>::const_iterator p = weights.begin();
       p != weights.end(); ++p) {
    if (p->second > max)
      max = p->second;
  }

  for (map<int,double>::const_iterator p = weights.begin();
       p != weights.end(); ++p) {
    inc.new_weight[p->first] = (unsigned)((p->second / max) * CEPH_OSD_IN);
  }
}

int OSDMap::identify_osd(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i) == addr || get_cluster_addr(i) == addr))
      return i;
  return -1;
}

int OSDMap::identify_osd(const uuid_d& u) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && get_uuid(i) == u)
      return i;
  return -1;
}

bool OSDMap::find_osd_on_ip(const entity_addr_t& ip) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i).is_same_host(ip) || get_cluster_addr(i).is_same_host(ip)))
      return i;
  return -1;
}


uint64_t OSDMap::get_features(int entity_type, uint64_t *pmask) const
{
  uint64_t features = 0;  // things we actually have
  uint64_t mask = 0;      // things we could have

  if (crush->has_nondefault_tunables())
    features |= CEPH_FEATURE_CRUSH_TUNABLES;
  if (crush->has_nondefault_tunables2())
    features |= CEPH_FEATURE_CRUSH_TUNABLES2;
  if (crush->has_nondefault_tunables3())
    features |= CEPH_FEATURE_CRUSH_TUNABLES3;
  if (crush->has_v4_buckets())
    features |= CEPH_FEATURE_CRUSH_V4;
  mask |= CEPH_FEATURES_CRUSH;

  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    if (p->second.has_flag(pg_pool_t::FLAG_HASHPSPOOL)) {
      features |= CEPH_FEATURE_OSDHASHPSPOOL;
    }
    if (p->second.is_erasure() &&
	entity_type != CEPH_ENTITY_TYPE_CLIENT) { // not for clients
      features |= CEPH_FEATURE_OSD_ERASURE_CODES;
    }
    if (!p->second.tiers.empty() ||
	p->second.is_tier()) {
      features |= CEPH_FEATURE_OSD_CACHEPOOL;
    }
    int ruleid = crush->find_rule(p->second.get_crush_ruleset(),
				  p->second.get_type(),
				  p->second.get_size());
    if (ruleid >= 0) {
      if (crush->is_v2_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_V2;
      if (crush->is_v3_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_TUNABLES3;
    }
  }
  if (entity_type == CEPH_ENTITY_TYPE_OSD) {
    for (map<string,map<string,string> >::const_iterator p = erasure_code_profiles.begin();
	 p != erasure_code_profiles.end();
	 ++p) {
      const map<string,string> &profile = p->second;
      map<string,string>::const_iterator plugin = profile.find("plugin");
      if (plugin != profile.end() && (plugin->second == "isa" ||
				      plugin->second == "lrc"))
	features |= CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2;
    }
  }
  mask |= CEPH_FEATURE_OSDHASHPSPOOL | CEPH_FEATURE_OSD_CACHEPOOL;
  if (entity_type != CEPH_ENTITY_TYPE_CLIENT)
    mask |= CEPH_FEATURE_OSD_ERASURE_CODES;

  if (osd_primary_affinity) {
    for (int i = 0; i < max_osd; ++i) {
      if ((*osd_primary_affinity)[i] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
	features |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;
	break;
      }
    }
  }
  mask |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;

  if (pmask)
    *pmask = mask;
  return features;
}

void OSDMap::_calc_up_osd_features()
{
  bool first = true;
  cached_up_osd_features = 0;
  for (int osd = 0; osd < max_osd; ++osd) {
    if (!is_up(osd))
      continue;
    const osd_xinfo_t &xi = get_xinfo(osd);
    if (first) {
      cached_up_osd_features = xi.features;
      first = false;
    } else {
      cached_up_osd_features &= xi.features;
    }
  }
}

uint64_t OSDMap::get_up_osd_features() const
{
  return cached_up_osd_features;
}

void OSDMap::dedup(const OSDMap *o, OSDMap *n)
{
  if (o->epoch == n->epoch)
    return;

  int diff = 0;

  // do addrs match?
  if (o->max_osd != n->max_osd)
    diff++;
  for (int i = 0; i < o->max_osd && i < n->max_osd; i++) {
    if ( n->osd_addrs->client_addr[i] &&  o->osd_addrs->client_addr[i] &&
	*n->osd_addrs->client_addr[i] == *o->osd_addrs->client_addr[i])
      n->osd_addrs->client_addr[i] = o->osd_addrs->client_addr[i];
    else
      diff++;
    if ( n->osd_addrs->cluster_addr[i] &&  o->osd_addrs->cluster_addr[i] &&
	*n->osd_addrs->cluster_addr[i] == *o->osd_addrs->cluster_addr[i])
      n->osd_addrs->cluster_addr[i] = o->osd_addrs->cluster_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_back_addr[i] &&  o->osd_addrs->hb_back_addr[i] &&
	*n->osd_addrs->hb_back_addr[i] == *o->osd_addrs->hb_back_addr[i])
      n->osd_addrs->hb_back_addr[i] = o->osd_addrs->hb_back_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_front_addr[i] &&  o->osd_addrs->hb_front_addr[i] &&
	*n->osd_addrs->hb_front_addr[i] == *o->osd_addrs->hb_front_addr[i])
      n->osd_addrs->hb_front_addr[i] = o->osd_addrs->hb_front_addr[i];
    else
      diff++;
  }
  if (diff == 0) {
    // zoinks, no differences at all!
    n->osd_addrs = o->osd_addrs;
  }

  // does crush match?
  bufferlist oc, nc;
  ::encode(*o->crush, oc);
  ::encode(*n->crush, nc);
  if (oc.contents_equal(nc)) {
    n->crush = o->crush;
  }

  // does pg_temp match?
  if (o->pg_temp->size() == n->pg_temp->size()) {
    if (*o->pg_temp == *n->pg_temp)
      n->pg_temp = o->pg_temp;
  }

  // does primary_temp match?
  if (o->primary_temp->size() == n->primary_temp->size()) {
    if (*o->primary_temp == *n->primary_temp)
      n->primary_temp = o->primary_temp;
  }

  // do uuids match?
  if (o->osd_uuid->size() == n->osd_uuid->size() &&
      *o->osd_uuid == *n->osd_uuid)
    n->osd_uuid = o->osd_uuid;
}

void OSDMap::remove_redundant_temporaries(CephContext *cct, const OSDMap& osdmap,
					  OSDMap::Incremental *pending_inc)
{
  ldout(cct, 10) << "remove_redundant_temporaries" << dendl;

  for (map<pg_t,vector<int32_t> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {

    // if pool does not exist, remove any existing pg_temps associated with
    // it.  we don't care about pg_temps on the pending_inc either; if there
    // are new_pg_temp entries on the pending, clear them out just as well.
    if (!osdmap.have_pg_pool(p->first.pool())) {
      ldout(cct, 10) << " removing pg_temp " << p->first
        << " for inexistent pool " << p->first.pool() << dendl;
      pending_inc->new_pg_temp[p->first].clear();

    } else if (pending_inc->new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      int primary;
      osdmap.pg_to_raw_up(p->first, &raw_up, &primary);
      if (raw_up == p->second) {
        ldout(cct, 10) << " removing unnecessary pg_temp " << p->first << " -> " << p->second << dendl;
        pending_inc->new_pg_temp[p->first].clear();
      }
    }
  }
  if (!osdmap.primary_temp->empty()) {
    OSDMap templess;
    templess.deepish_copy_from(osdmap);
    templess.primary_temp->clear();
    for (map<pg_t,int32_t>::iterator p = osdmap.primary_temp->begin();
        p != osdmap.primary_temp->end();
        ++p) {
      if (pending_inc->new_primary_temp.count(p->first) == 0) {
        vector<int> real_up, templess_up;
        int real_primary, templess_primary;
        osdmap.pg_to_acting_osds(p->first, &real_up, &real_primary);
        templess.pg_to_acting_osds(p->first, &templess_up, &templess_primary);
        if (real_primary == templess_primary){
          ldout(cct, 10) << " removing unnecessary primary_temp "
                         << p->first << " -> " << p->second << dendl;
          pending_inc->new_primary_temp[p->first] = -1;
        }
      }
    }
  }
}

void OSDMap::remove_down_temps(CephContext *cct,
                               const OSDMap& osdmap, Incremental *pending_inc)
{
  ldout(cct, 10) << "remove_down_pg_temp" << dendl;
  OSDMap tmpmap;
  tmpmap.deepish_copy_from(osdmap);
  tmpmap.apply_incremental(*pending_inc);

  for (map<pg_t,vector<int32_t> >::iterator p = tmpmap.pg_temp->begin();
       p != tmpmap.pg_temp->end();
       ++p) {
    unsigned num_up = 0;
    for (vector<int32_t>::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      if (!tmpmap.is_down(*i))
	++num_up;
    }
    if (num_up == 0)
      pending_inc->new_pg_temp[p->first].clear();
  }
  for (map<pg_t,int32_t>::iterator p = tmpmap.primary_temp->begin();
      p != tmpmap.primary_temp->end();
      ++p) {
    if (tmpmap.is_down(p->second))
      pending_inc->new_primary_temp[p->first] = -1;
  }
}

int OSDMap::apply_incremental(const Incremental &inc)
{
  new_blacklist_entries = false;
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    return -EINVAL;
  
  assert(inc.epoch == epoch+1);

  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    bufferlist bl(inc.fullmap);
    decode(bl);
    return 0;
  }

  // nope, incremental.
  if (inc.new_flags >= 0)
    flags = inc.new_flags;

  if (inc.new_max_osd >= 0)
    set_max_osd(inc.new_max_osd);

  if (inc.new_pool_max != -1)
    pool_max = inc.new_pool_max;

  for (map<int64_t,pg_pool_t>::const_iterator p = inc.new_pools.begin();
       p != inc.new_pools.end();
       ++p) {
    pools[p->first] = p->second;
    pools[p->first].last_change = epoch;
  }
  for (map<int64_t,string>::const_iterator p = inc.new_pool_names.begin();
       p != inc.new_pool_names.end();
       ++p) {
    if (pool_name.count(p->first))
      name_pool.erase(pool_name[p->first]);
    pool_name[p->first] = p->second;
    name_pool[p->second] = p->first;
  }
  for (set<int64_t>::const_iterator p = inc.old_pools.begin();
       p != inc.old_pools.end();
       ++p) {
    pools.erase(*p);
    name_pool.erase(pool_name[*p]);
    pool_name.erase(*p);
  }

  for (map<int32_t,uint32_t>::const_iterator i = inc.new_weight.begin();
       i != inc.new_weight.end();
       ++i) {
    set_weight(i->first, i->second);

    // if we are marking in, clear the AUTOOUT and NEW bits, and clear
    // xinfo old_weight.
    if (i->second) {
      osd_state[i->first] &= ~(CEPH_OSD_AUTOOUT | CEPH_OSD_NEW);
      osd_xinfo[i->first].old_weight = 0;
    }
  }

  for (map<int32_t,uint32_t>::const_iterator i = inc.new_primary_affinity.begin();
       i != inc.new_primary_affinity.end();
       ++i) {
    set_primary_affinity(i->first, i->second);
  }

  // erasure_code_profiles
  for (vector<string>::const_iterator i = inc.old_erasure_code_profiles.begin();
       i != inc.old_erasure_code_profiles.end();
       ++i)
    erasure_code_profiles.erase(*i);
  
  for (map<string,map<string,string> >::const_iterator i =
	 inc.new_erasure_code_profiles.begin();
       i != inc.new_erasure_code_profiles.end();
       ++i) {
    set_erasure_code_profile(i->first, i->second);
  }
  
  // up/down
  for (map<int32_t,uint8_t>::const_iterator i = inc.new_state.begin();
       i != inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if ((osd_state[i->first] & CEPH_OSD_UP) &&
	(s & CEPH_OSD_UP)) {
      osd_info[i->first].down_at = epoch;
      osd_xinfo[i->first].down_stamp = modified;
    }
    if ((osd_state[i->first] & CEPH_OSD_EXISTS) &&
	(s & CEPH_OSD_EXISTS))
      (*osd_uuid)[i->first] = uuid_d();
    osd_state[i->first] ^= s;
  }
  for (map<int32_t,entity_addr_t>::const_iterator i = inc.new_up_client.begin();
       i != inc.new_up_client.end();
       ++i) {
    osd_state[i->first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
    osd_addrs->client_addr[i->first].reset(new entity_addr_t(i->second));
    if (inc.new_hb_back_up.empty())
      osd_addrs->hb_back_addr[i->first].reset(new entity_addr_t(i->second)); //this is a backward-compatibility hack
    else
      osd_addrs->hb_back_addr[i->first].reset(
	new entity_addr_t(inc.new_hb_back_up.find(i->first)->second));
    map<int32_t,entity_addr_t>::const_iterator j = inc.new_hb_front_up.find(i->first);
    if (j != inc.new_hb_front_up.end())
      osd_addrs->hb_front_addr[i->first].reset(new entity_addr_t(j->second));
    else
      osd_addrs->hb_front_addr[i->first].reset();

    osd_info[i->first].up_from = epoch;
  }
  for (map<int32_t,entity_addr_t>::const_iterator i = inc.new_up_cluster.begin();
       i != inc.new_up_cluster.end();
       ++i)
    osd_addrs->cluster_addr[i->first].reset(new entity_addr_t(i->second));

  // info
  for (map<int32_t,epoch_t>::const_iterator i = inc.new_up_thru.begin();
       i != inc.new_up_thru.end();
       ++i)
    osd_info[i->first].up_thru = i->second;
  for (map<int32_t,pair<epoch_t,epoch_t> >::const_iterator i = inc.new_last_clean_interval.begin();
       i != inc.new_last_clean_interval.end();
       ++i) {
    osd_info[i->first].last_clean_begin = i->second.first;
    osd_info[i->first].last_clean_end = i->second.second;
  }
  for (map<int32_t,epoch_t>::const_iterator p = inc.new_lost.begin(); p != inc.new_lost.end(); ++p)
    osd_info[p->first].lost_at = p->second;

  // xinfo
  for (map<int32_t,osd_xinfo_t>::const_iterator p = inc.new_xinfo.begin(); p != inc.new_xinfo.end(); ++p)
    osd_xinfo[p->first] = p->second;

  // uuid
  for (map<int32_t,uuid_d>::const_iterator p = inc.new_uuid.begin(); p != inc.new_uuid.end(); ++p) 
    (*osd_uuid)[p->first] = p->second;

  // pg rebuild
  for (map<pg_t, vector<int> >::const_iterator p = inc.new_pg_temp.begin(); p != inc.new_pg_temp.end(); ++p) {
    if (p->second.empty())
      pg_temp->erase(p->first);
    else
      (*pg_temp)[p->first] = p->second;
  }

  for (map<pg_t,int32_t>::const_iterator p = inc.new_primary_temp.begin();
      p != inc.new_primary_temp.end();
      ++p) {
    if (p->second == -1)
      primary_temp->erase(p->first);
    else
      (*primary_temp)[p->first] = p->second;
  }

  // blacklist
  for (map<entity_addr_t,utime_t>::const_iterator p = inc.new_blacklist.begin();
       p != inc.new_blacklist.end();
       ++p) {
    blacklist[p->first] = p->second;
    new_blacklist_entries = true;
  }
  for (vector<entity_addr_t>::const_iterator p = inc.old_blacklist.begin();
       p != inc.old_blacklist.end();
       ++p)
    blacklist.erase(*p);

  // cluster snapshot?
  if (inc.cluster_snapshot.length()) {
    cluster_snapshot = inc.cluster_snapshot;
    cluster_snapshot_epoch = inc.epoch;
  } else {
    cluster_snapshot.clear();
    cluster_snapshot_epoch = 0;
  }

  // do new crush map last (after up/down stuff)
  if (inc.crush.length()) {
    bufferlist bl(inc.crush);
    bufferlist::iterator blp = bl.begin();
    crush.reset(new CrushWrapper);
    crush->decode(blp);
  }

  calc_num_osds();
  _calc_up_osd_features();
  return 0;
}

// mapping
int OSDMap::object_locator_to_pg(
	const object_t& oid,
	const object_locator_t& loc,
	pg_t &pg) const
{
  // calculate ps (placement seed)
  const pg_pool_t *pool = get_pg_pool(loc.get_pool());
  if (!pool)
    return -ENOENT;
  ps_t ps;
  if (loc.hash >= 0) {
    ps = loc.hash;
  } else {
    if (!loc.key.empty())
      ps = pool->hash_key(loc.key, loc.nspace);
    else
      ps = pool->hash_key(oid.name, loc.nspace);
  }
  pg = pg_t(ps, loc.get_pool(), -1);
  return 0;
}

ceph_object_layout OSDMap::make_object_layout(
  object_t oid, int pg_pool, string nspace) const
{
  object_locator_t loc(pg_pool, nspace);

  ceph_object_layout ol;
  pg_t pgid = object_locator_to_pg(oid, loc);
  ol.ol_pgid = pgid.get_old_pg().v;
  ol.ol_stripe_unit = 0;
  return ol;
}

void OSDMap::_remove_nonexistent_osds(const pg_pool_t& pool,
				      vector<int>& osds) const
{
  if (pool.can_shift_osds()) {
    unsigned removed = 0;
    for (unsigned i = 0; i < osds.size(); i++) {
      if (!exists(osds[i])) {
	removed++;
	continue;
      }
      if (removed) {
	osds[i - removed] = osds[i];
      }
    }
    if (removed)
      osds.resize(osds.size() - removed);
  } else {
    for (vector<int>::iterator p = osds.begin(); p != osds.end(); ++p) {
      if (!exists(*p))
	*p = CRUSH_ITEM_NONE;
    }
  }
}

int OSDMap::_pg_to_osds(const pg_pool_t& pool, pg_t pg,
                        vector<int> *osds, int *primary,
			ps_t *ppps) const
{
  // map to osds[]
  ps_t pps = pool.raw_pg_to_pps(pg);  // placement ps
  unsigned size = pool.get_size();

  // what crush rule?
  int ruleno = crush->find_rule(pool.get_crush_ruleset(), pool.get_type(), size);
  if (ruleno >= 0)
    crush->do_rule(ruleno, pps, *osds, size, osd_weight);

  _remove_nonexistent_osds(pool, *osds);

  *primary = -1;
  for (unsigned i = 0; i < osds->size(); ++i) {
    if ((*osds)[i] != CRUSH_ITEM_NONE) {
      *primary = (*osds)[i];
      break;
    }
  }
  if (ppps)
    *ppps = pps;

  return osds->size();
}

// pg -> (up osd list)
void OSDMap::_raw_to_up_osds(const pg_pool_t& pool, const vector<int>& raw,
                             vector<int> *up, int *primary) const
{
  if (pool.can_shift_osds()) {
    // shift left
    up->clear();
    for (unsigned i=0; i<raw.size(); i++) {
      if (!exists(raw[i]) || is_down(raw[i]))
	continue;
      up->push_back(raw[i]);
    }
    *primary = (up->empty() ? -1 : up->front());
  } else {
    // set down/dne devices to NONE
    *primary = -1;
    up->resize(raw.size());
    for (int i = raw.size() - 1; i >= 0; --i) {
      if (!exists(raw[i]) || is_down(raw[i])) {
	(*up)[i] = CRUSH_ITEM_NONE;
      } else {
	*primary = (*up)[i] = raw[i];
      }
    }
  }
}

void OSDMap::_apply_primary_affinity(ps_t seed,
				     const pg_pool_t& pool,
				     vector<int> *osds,
				     int *primary) const
{
  // do we have any non-default primary_affinity values for these osds?
  if (!osd_primary_affinity)
    return;

  bool any = false;
  for (vector<int>::const_iterator p = osds->begin(); p != osds->end(); ++p) {
    if (*p != CRUSH_ITEM_NONE &&
	(*osd_primary_affinity)[*p] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
      any = true;
      break;
    }
  }
  if (!any)
    return;

  // pick the primary.  feed both the seed (for the pg) and the osd
  // into the hash/rng so that a proportional fraction of an osd's pgs
  // get rejected as primary.
  int pos = -1;
  for (unsigned i = 0; i < osds->size(); ++i) {
    int o = (*osds)[i];
    if (o == CRUSH_ITEM_NONE)
      continue;
    unsigned a = (*osd_primary_affinity)[o];
    if (a < CEPH_OSD_MAX_PRIMARY_AFFINITY &&
	(crush_hash32_2(CRUSH_HASH_RJENKINS1,
			seed, o) >> 16) >= a) {
      // we chose not to use this primary.  note it anyway as a
      // fallback in case we don't pick anyone else, but keep looking.
      if (pos < 0)
	pos = i;
    } else {
      pos = i;
      break;
    }
  }
  if (pos < 0)
    return;

  *primary = (*osds)[pos];

  if (pool.can_shift_osds() && pos > 0) {
    // move the new primary to the front.
    for (int i = pos; i > 0; --i) {
      (*osds)[i] = (*osds)[i-1];
    }
    (*osds)[0] = *primary;
  }
}

void OSDMap::_get_temp_osds(const pg_pool_t& pool, pg_t pg,
                            vector<int> *temp_pg, int *temp_primary) const
{
  pg = pool.raw_pg_to_pg(pg);
  map<pg_t,vector<int32_t> >::const_iterator p = pg_temp->find(pg);
  temp_pg->clear();
  if (p != pg_temp->end()) {
    for (unsigned i=0; i<p->second.size(); i++) {
      if (!exists(p->second[i]) || is_down(p->second[i])) {
	if (pool.can_shift_osds()) {
	  continue;
	} else {
	  temp_pg->push_back(CRUSH_ITEM_NONE);
	}
      } else {
	temp_pg->push_back(p->second[i]);
      }
    }
  }
  map<pg_t,int32_t>::const_iterator pp = primary_temp->find(pg);
  *temp_primary = -1;
  if (pp != primary_temp->end()) {
    *temp_primary = pp->second;
  } else if (!temp_pg->empty()) { // apply pg_temp's primary
    for (unsigned i = 0; i < temp_pg->size(); ++i) {
      if ((*temp_pg)[i] != CRUSH_ITEM_NONE) {
	*temp_primary = (*temp_pg)[i];
	break;
      }
    }
  }
}

int OSDMap::pg_to_osds(pg_t pg, vector<int> *raw, int *primary) const
{
  *primary = -1;
  raw->clear();
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return 0;
  int r = _pg_to_osds(*pool, pg, raw, primary, NULL);
  return r;
}

void OSDMap::pg_to_raw_up(pg_t pg, vector<int> *up, int *primary) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool) {
    if (primary)
      *primary = -1;
    if (up)
      up->clear();
    return;
  }
  vector<int> raw;
  ps_t pps;
  _pg_to_osds(*pool, pg, &raw, primary, &pps);
  _raw_to_up_osds(*pool, raw, up, primary);
  _apply_primary_affinity(pps, *pool, up, primary);
}
  
void OSDMap::_pg_to_up_acting_osds(const pg_t& pg, vector<int> *up, int *up_primary,
                                   vector<int> *acting, int *acting_primary) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool) {
    if (up)
      up->clear();
    if (up_primary)
      *up_primary = -1;
    if (acting)
      acting->clear();
    if (acting_primary)
      *acting_primary = -1;
    return;
  }
  vector<int> raw;
  vector<int> _up;
  vector<int> _acting;
  int _up_primary;
  int _acting_primary;
  ps_t pps;
  _pg_to_osds(*pool, pg, &raw, &_up_primary, &pps);
  _raw_to_up_osds(*pool, raw, &_up, &_up_primary);
  _apply_primary_affinity(pps, *pool, &_up, &_up_primary);
  _get_temp_osds(*pool, pg, &_acting, &_acting_primary);
  if (_acting.empty()) {
    _acting = _up;
    if (_acting_primary == -1) {
      _acting_primary = _up_primary;
    }
  }
  if (up)
    up->swap(_up);
  if (up_primary)
    *up_primary = _up_primary;
  if (acting)
    acting->swap(_acting);
  if (acting_primary)
    *acting_primary = _acting_primary;
}

int OSDMap::calc_pg_rank(int osd, const vector<int>& acting, int nrep)
{
  if (!nrep)
    nrep = acting.size();
  for (int i=0; i<nrep; i++) 
    if (acting[i] == osd)
      return i;
  return -1;
}

int OSDMap::calc_pg_role(int osd, const vector<int>& acting, int nrep)
{
  if (!nrep)
    nrep = acting.size();
  return calc_pg_rank(osd, acting, nrep);
}

bool OSDMap::primary_changed(
  int oldprimary,
  const vector<int> &oldacting,
  int newprimary,
  const vector<int> &newacting)
{
  if (oldacting.empty() && newacting.empty())
    return false;    // both still empty
  if (oldacting.empty() ^ newacting.empty())
    return true;     // was empty, now not, or vice versa
  if (oldprimary != newprimary)
    return true;     // primary changed
  if (calc_pg_rank(oldprimary, oldacting) !=
      calc_pg_rank(newprimary, newacting))
    return true;
  return false;      // same primary (tho replicas may have changed)
}


// serialize, unserialize
void OSDMap::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);

  // base
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(created, bl);
  ::encode(modified, bl);

  // for ::encode(pools, bl);
  __u32 n = pools.size();
  ::encode(n, bl);
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin();
       p != pools.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl, 0);
  }
  // for ::encode(pool_name, bl);
  n = pool_name.size();
  ::encode(n, bl);
  for (map<int64_t, string>::const_iterator p = pool_name.begin();
       p != pool_name.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl);
  }
  // for ::encode(pool_max, bl);
  n = pool_max;
  ::encode(n, bl);

  ::encode(flags, bl);

  ::encode(max_osd, bl);
  ::encode(osd_state, bl);
  ::encode(osd_weight, bl);
  ::encode(osd_addrs->client_addr, bl);

  // for ::encode(pg_temp, bl);
  n = pg_temp->size();
  ::encode(n, bl);
  for (map<pg_t,vector<int32_t> >::const_iterator p = pg_temp->begin();
       p != pg_temp->end();
       ++p) {
    old_pg_t opg = p->first.get_old_pg();
    ::encode(opg, bl);
    ::encode(p->second, bl);
  }

  // crush
  bufferlist cbl;
  crush->encode(cbl);
  ::encode(cbl, bl);
}

void OSDMap::encode_classic(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  __u16 v = 6;
  ::encode(v, bl);

  // base
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(created, bl);
  ::encode(modified, bl);

  ::encode(pools, bl, features);
  ::encode(pool_name, bl);
  ::encode(pool_max, bl);

  ::encode(flags, bl);

  ::encode(max_osd, bl);
  ::encode(osd_state, bl);
  ::encode(osd_weight, bl);
  ::encode(osd_addrs->client_addr, bl);

  ::encode(*pg_temp, bl);

  // crush
  bufferlist cbl;
  crush->encode(cbl);
  ::encode(cbl, bl);

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(osd_addrs->hb_back_addr, bl);
  ::encode(osd_info, bl);
  ::encode(blacklist, bl);
  ::encode(osd_addrs->cluster_addr, bl);
  ::encode(cluster_snapshot_epoch, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(*osd_uuid, bl);
  ::encode(osd_xinfo, bl);
  ::encode(osd_addrs->hb_front_addr, bl);
}

void OSDMap::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  buffer::list::iterator crc_it;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    ENCODE_START(3, 1, bl); // client-usable data
    // base
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(created, bl);
    ::encode(modified, bl);

    ::encode(pools, bl, features);
    ::encode(pool_name, bl);
    ::encode(pool_max, bl);

    ::encode(flags, bl);

    ::encode(max_osd, bl);
    ::encode(osd_state, bl);
    ::encode(osd_weight, bl);
    ::encode(osd_addrs->client_addr, bl);

    ::encode(*pg_temp, bl);
    ::encode(*primary_temp, bl);
    if (osd_primary_affinity) {
      ::encode(*osd_primary_affinity, bl);
    } else {
      vector<__u32> v;
      ::encode(v, bl);
    }

    // crush
    bufferlist cbl;
    crush->encode(cbl);
    ::encode(cbl, bl);
    ::encode(erasure_code_profiles, bl);
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    ENCODE_START(1, 1, bl); // extended, osd-only data
    ::encode(osd_addrs->hb_back_addr, bl);
    ::encode(osd_info, bl);
    {
      // put this in a sorted, ordered map<> so that we encode in a
      // deterministic order.
      map<entity_addr_t,utime_t> blacklist_map;
      for (ceph::unordered_map<entity_addr_t,utime_t>::const_iterator p =
	     blacklist.begin(); p != blacklist.end(); ++p)
	blacklist_map.insert(make_pair(p->first, p->second));
      ::encode(blacklist_map, bl);
    }
    ::encode(osd_addrs->cluster_addr, bl);
    ::encode(cluster_snapshot_epoch, bl);
    ::encode(cluster_snapshot, bl);
    ::encode(*osd_uuid, bl);
    ::encode(osd_xinfo, bl);
    ::encode(osd_addrs->hb_front_addr, bl);
    ENCODE_FINISH(bl); // osd-only data
  }

  ::encode((uint32_t)0, bl); // dummy crc
  crc_it = bl.end();
  crc_it.advance(-4);
  tail_offset = bl.length();

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  bufferlist front;
  front.substr_of(bl, start_offset, crc_it.get_off() - start_offset);
  crc = front.crc32c(-1);
  if (tail_offset < bl.length()) {
    bufferlist tail;
    tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
    crc = tail.crc32c(crc);
  }
  ceph_le32 crc_le;
  crc_le = crc;
  crc_it.copy_in(4, (char*)&crc_le);
  crc_defined = true;
}

void OSDMap::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}

void OSDMap::decode_classic(bufferlist::iterator& p)
{
  __u32 n, t;
  __u16 v;
  ::decode(v, p);

  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(created, p);
  ::decode(modified, p);

  if (v < 6) {
    if (v < 4) {
      int32_t max_pools = 0;
      ::decode(max_pools, p);
      pool_max = max_pools;
    }
    pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pools[t], p);
    }
    if (v == 4) {
      ::decode(n, p);
      pool_max = n;
    } else if (v == 5) {
      pool_name.clear();
      ::decode(n, p);
      while (n--) {
	::decode(t, p);
	::decode(pool_name[t], p);
      }
      ::decode(n, p);
      pool_max = n;
    }
  } else {
    ::decode(pools, p);
    ::decode(pool_name, p);
    ::decode(pool_max, p);
  }
  // kludge around some old bug that zeroed out pool_max (#2307)
  if (pools.size() && pool_max < pools.rbegin()->first) {
    pool_max = pools.rbegin()->first;
  }

  ::decode(flags, p);

  ::decode(max_osd, p);
  ::decode(osd_state, p);
  ::decode(osd_weight, p);
  ::decode(osd_addrs->client_addr, p);
  if (v <= 5) {
    pg_temp->clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode((*pg_temp)[pg_t(opg)], p);
    }
  } else {
    ::decode(*pg_temp, p);
  }

  // crush
  bufferlist cbl;
  ::decode(cbl, p);
  bufferlist::iterator cblp = cbl.begin();
  crush->decode(cblp);

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(osd_addrs->hb_back_addr, p);
  ::decode(osd_info, p);
  if (v < 5)
    ::decode(pool_name, p);

  ::decode(blacklist, p);
  if (ev >= 6)
    ::decode(osd_addrs->cluster_addr, p);
  else
    osd_addrs->cluster_addr.resize(osd_addrs->client_addr.size());

  if (ev >= 7) {
    ::decode(cluster_snapshot_epoch, p);
    ::decode(cluster_snapshot, p);
  }

  if (ev >= 8) {
    ::decode(*osd_uuid, p);
  } else {
    osd_uuid->resize(max_osd);
  }
  if (ev >= 9)
    ::decode(osd_xinfo, p);
  else
    osd_xinfo.resize(max_osd);

  if (ev >= 10)
    ::decode(osd_addrs->hb_front_addr, p);
  else
    osd_addrs->hb_front_addr.resize(osd_addrs->hb_back_addr.size());

  osd_primary_affinity.reset();

  post_decode();
}

void OSDMap::decode(bufferlist::iterator& bl)
{
  /**
   * Older encodings of the OSDMap had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  bufferlist crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    int struct_v_size = sizeof(struct_v);
    bl.advance(-struct_v_size);
    decode_classic(bl);
    return;
  }
  /**
   * Since we made it past that hurdle, we can use our normal paths.
   */
  {
    DECODE_START(3, bl); // client-usable data
    // base
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(created, bl);
    ::decode(modified, bl);

    ::decode(pools, bl);
    ::decode(pool_name, bl);
    ::decode(pool_max, bl);

    ::decode(flags, bl);

    ::decode(max_osd, bl);
    ::decode(osd_state, bl);
    ::decode(osd_weight, bl);
    ::decode(osd_addrs->client_addr, bl);

    ::decode(*pg_temp, bl);
    ::decode(*primary_temp, bl);
    if (struct_v >= 2) {
      osd_primary_affinity.reset(new vector<__u32>);
      ::decode(*osd_primary_affinity, bl);
      if (osd_primary_affinity->empty())
	osd_primary_affinity.reset();
    } else {
      osd_primary_affinity.reset();
    }

    // crush
    bufferlist cbl;
    ::decode(cbl, bl);
    bufferlist::iterator cblp = cbl.begin();
    crush->decode(cblp);
    if (struct_v >= 3) {
      ::decode(erasure_code_profiles, bl);
    } else {
      erasure_code_profiles.clear();
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(1, bl); // extended, osd-only data
    ::decode(osd_addrs->hb_back_addr, bl);
    ::decode(osd_info, bl);
    ::decode(blacklist, bl);
    ::decode(osd_addrs->cluster_addr, bl);
    ::decode(cluster_snapshot_epoch, bl);
    ::decode(cluster_snapshot, bl);
    ::decode(*osd_uuid, bl);
    ::decode(osd_xinfo, bl);
    ::decode(osd_addrs->hb_front_addr, bl);
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    ::decode(crc, bl);
    tail_offset = bl.get_off();
    crc_defined = true;
  } else {
    crc_defined = false;
    crc = 0;
  }

  DECODE_FINISH(bl); // wrapper

  if (tail_offset) {
    // verify crc
    uint32_t actual = crc_front.crc32c(-1);
    if (tail_offset < bl.get_off()) {
      bufferlist tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << crc;
      string s = ss.str();
      throw buffer::malformed_input(s.c_str());
    }
  }

  post_decode();
}

void OSDMap::post_decode()
{
  // index pool names
  name_pool.clear();
  for (map<int64_t,string>::iterator i = pool_name.begin();
       i != pool_name.end(); ++i) {
    name_pool[i->second] = i->first;
  }

  calc_num_osds();
  _calc_up_osd_features();
}

void OSDMap::dump_erasure_code_profiles(const map<string,map<string,string> > &profiles,
					Formatter *f)
{
  f->open_object_section("erasure_code_profiles");
  for (map<string,map<string,string> >::const_iterator i = profiles.begin();
       i != profiles.end();
       ++i) {
    f->open_object_section(i->first.c_str());
    for (map<string,string>::const_iterator j = i->second.begin();
	 j != i->second.end();
	 ++j) {
      f->dump_string(j->first.c_str(), j->second.c_str());
    }
    f->close_section();
  }
  f->close_section();
}

void OSDMap::dump_json(ostream& out) const
{
  JSONFormatter jsf(true);
  jsf.open_object_section("osdmap");
  dump(&jsf);
  jsf.close_section();
  jsf.flush(out);
}

void OSDMap::dump(Formatter *f) const
{
  f->dump_int("epoch", get_epoch());
  f->dump_stream("fsid") << get_fsid();
  f->dump_stream("created") << get_created();
  f->dump_stream("modified") << get_modified();
  f->dump_string("flags", get_flag_string());
  f->dump_string("cluster_snapshot", get_cluster_snapshot());
  f->dump_int("pool_max", get_pool_max());
  f->dump_int("max_osd", get_max_osd());

  f->open_array_section("pools");
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int64_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    f->open_object_section("pool");
    f->dump_int("pool", p->first);
    f->dump_string("pool_name", name);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osds");
  for (int i=0; i<get_max_osd(); i++)
    if (exists(i)) {
      f->open_object_section("osd_info");
      f->dump_int("osd", i);
      f->dump_stream("uuid") << get_uuid(i);
      f->dump_int("up", is_up(i));
      f->dump_int("in", is_in(i));
      f->dump_float("weight", get_weightf(i));
      f->dump_float("primary_affinity", get_primary_affinityf(i));
      get_info(i).dump(f);
      f->dump_stream("public_addr") << get_addr(i);
      f->dump_stream("cluster_addr") << get_cluster_addr(i);
      f->dump_stream("heartbeat_back_addr") << get_hb_back_addr(i);
      f->dump_stream("heartbeat_front_addr") << get_hb_front_addr(i);

      set<string> st;
      get_state(i, st);
      f->open_array_section("state");
      for (set<string>::iterator p = st.begin(); p != st.end(); ++p)
	f->dump_string("state", *p);
      f->close_section();

      f->close_section();
    }
  f->close_section();

  f->open_array_section("osd_xinfo");
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      f->open_object_section("xinfo");
      f->dump_int("osd", i);
      osd_xinfo[i].dump(f);
      f->close_section();
    }
  }
  f->close_section();

  f->open_array_section("pg_temp");
  for (map<pg_t,vector<int32_t> >::const_iterator p = pg_temp->begin();
       p != pg_temp->end();
       ++p) {
    f->open_object_section("osds");
    f->dump_stream("pgid") << p->first;
    f->open_array_section("osds");
    for (vector<int>::const_iterator q = p->second.begin(); q != p->second.end(); ++q)
      f->dump_int("osd", *q);
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("primary_temp");
  for (map<pg_t, int32_t>::const_iterator p = primary_temp->begin();
      p != primary_temp->end();
      ++p) {
    f->dump_stream("pgid") << p->first;
    f->dump_int("osd", p->second);
  }
  f->close_section(); // primary_temp

  f->open_array_section("blacklist");
  for (ceph::unordered_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       ++p) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();

  dump_erasure_code_profiles(erasure_code_profiles, f);
}

void OSDMap::generate_test_instances(list<OSDMap*>& o)
{
  o.push_back(new OSDMap);

  CephContext *cct = new CephContext(CODE_ENVIRONMENT_UTILITY);
  o.push_back(new OSDMap);
  uuid_d fsid;
  o.back()->build_simple(cct, 1, fsid, 16, 7, 8);
  o.back()->created = o.back()->modified = utime_t(1, 2);  // fix timestamp
  o.back()->blacklist[entity_addr_t()] = utime_t(5, 6);
  cct->put();
}

string OSDMap::get_flag_string(unsigned f)
{
  string s;
  if ( f& CEPH_OSDMAP_NEARFULL)
    s += ",nearfull";
  if (f & CEPH_OSDMAP_FULL)
    s += ",full";
  if (f & CEPH_OSDMAP_PAUSERD)
    s += ",pauserd";
  if (f & CEPH_OSDMAP_PAUSEWR)
    s += ",pausewr";
  if (f & CEPH_OSDMAP_PAUSEREC)
    s += ",pauserec";
  if (f & CEPH_OSDMAP_NOUP)
    s += ",noup";
  if (f & CEPH_OSDMAP_NODOWN)
    s += ",nodown";
  if (f & CEPH_OSDMAP_NOOUT)
    s += ",noout";
  if (f & CEPH_OSDMAP_NOIN)
    s += ",noin";
  if (f & CEPH_OSDMAP_NOBACKFILL)
    s += ",nobackfill";
  if (f & CEPH_OSDMAP_NOREBALANCE)
    s += ",norebalance";
  if (f & CEPH_OSDMAP_NORECOVER)
    s += ",norecover";
  if (f & CEPH_OSDMAP_NOSCRUB)
    s += ",noscrub";
  if (f & CEPH_OSDMAP_NODEEP_SCRUB)
    s += ",nodeep-scrub";
  if (f & CEPH_OSDMAP_NOTIERAGENT)
    s += ",notieragent";
  if (s.length())
    s.erase(0, 1);
  return s;
}

string OSDMap::get_flag_string() const
{
  return get_flag_string(flags);
}

struct qi {
  int item;
  int depth;
  float weight;
  qi() : item(0), depth(0), weight(0) {}
  qi(int i, int d, float w) : item(i), depth(d), weight(w) {}
};

void OSDMap::print_pools(ostream& out) const
{
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int64_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    out << "pool " << p->first
	<< " '" << name
	<< "' " << p->second << "\n";
    for (map<snapid_t,pool_snap_info_t>::const_iterator q = p->second.snaps.begin();
	 q != p->second.snaps.end();
	 ++q)
      out << "\tsnap " << q->second.snapid << " '" << q->second.name << "' " << q->second.stamp << "\n";
    if (!p->second.removed_snaps.empty())
      out << "\tremoved_snaps " << p->second.removed_snaps << "\n";
  }
  out << std::endl;
}

void OSDMap::print(ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modified " << get_modified() << "\n";

  out << "flags " << get_flag_string() << "\n";
  if (get_cluster_snapshot().length())
    out << "cluster_snapshot " << get_cluster_snapshot() << "\n";
  out << "\n";

  print_pools(out);

  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd." << i;
      out << (is_up(i) ? " up  ":" down");
      out << (is_in(i) ? " in ":" out");
      out << " weight " << get_weightf(i);
      if (get_primary_affinity(i) != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY)
	out << " primary_affinity " << get_primary_affinityf(i);
      const osd_info_t& info(get_info(i));
      out << " " << info;
      out << " " << get_addr(i) << " " << get_cluster_addr(i) << " " << get_hb_back_addr(i)
	  << " " << get_hb_front_addr(i);
      set<string> st;
      get_state(i, st);
      out << " " << st;
      if (!get_uuid(i).is_zero())
	out << " " << get_uuid(i);
      out << "\n";
    }
  }
  out << std::endl;

  for (map<pg_t,vector<int32_t> >::const_iterator p = pg_temp->begin();
       p != pg_temp->end();
       ++p)
    out << "pg_temp " << p->first << " " << p->second << "\n";

  for (map<pg_t,int32_t>::const_iterator p = primary_temp->begin();
      p != primary_temp->end();
      ++p)
    out << "primary_temp " << p->first << " " << p->second << "\n";

  for (ceph::unordered_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       ++p)
    out << "blacklist " << p->first << " expires " << p->second << "\n";

  // ignore pg_swap_primary
}

class OSDTreePlainDumper : public CrushTreeDumper::Dumper<TextTable> {
public:
  typedef CrushTreeDumper::Dumper<TextTable> Parent;
  OSDTreePlainDumper(const CrushWrapper *crush, const OSDMap *osdmap_)
    : Parent(crush), osdmap(osdmap_) {}

  void dump(TextTable *tbl) {
    tbl->define_column("ID", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("WEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("TYPE NAME", TextTable::LEFT, TextTable::LEFT);
    tbl->define_column("UP/DOWN", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("REWEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("PRIMARY-AFFINITY", TextTable::LEFT, TextTable::RIGHT);

    Parent::dump(tbl);

    for (int i = 0; i <= osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0), tbl);
    }
  }

protected:
  virtual void dump_item(const CrushTreeDumper::Item &qi, TextTable *tbl) {

    *tbl << qi.id
	 << weightf_t(qi.weight);

    ostringstream name;
    for (int k = 0; k < qi.depth; k++)
      name << "    ";
    if (qi.is_bucket()) {
      name << crush->get_type_name(crush->get_bucket_type(qi.id)) << " "
	   << crush->get_item_name(qi.id);
    } else {
      name << "osd." << qi.id;
    }
    *tbl << name.str();

    if (!qi.is_bucket()) {
      if (!osdmap->exists(qi.id)) {
	*tbl << "DNE"
	     << 0;
      } else {
	*tbl << (osdmap->is_up(qi.id) ? "up" : "down")
	     << weightf_t(osdmap->get_weightf(qi.id))
	     << weightf_t(osdmap->get_primary_affinityf(qi.id));
      }
    }
    *tbl << TextTable::endrow;
  }

private:
  const OSDMap *osdmap;
};

class OSDTreeFormattingDumper : public CrushTreeDumper::FormattingDumper {
public:
  typedef CrushTreeDumper::FormattingDumper Parent;

  OSDTreeFormattingDumper(const CrushWrapper *crush, const OSDMap *osdmap_)
    : Parent(crush), osdmap(osdmap_) {}

  void dump(Formatter *f) {
    f->open_array_section("nodes");
    Parent::dump(f);
    f->close_section();
    f->open_array_section("stray");
    for (int i = 0; i <= osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0), f);
    }
    f->close_section();
  }

protected:
  virtual void dump_item_fields(const CrushTreeDumper::Item &qi, Formatter *f) {
    Parent::dump_item_fields(qi, f);
    if (!qi.is_bucket())
    {
      f->dump_unsigned("exists", (int)osdmap->exists(qi.id));
      f->dump_string("status", osdmap->is_up(qi.id) ? "up" : "down");
      f->dump_float("reweight", osdmap->get_weightf(qi.id));
      f->dump_float("primary_affinity", osdmap->get_primary_affinityf(qi.id));
    }
  }

private:
  const OSDMap *osdmap;
};

void OSDMap::print_tree(ostream *out, Formatter *f) const
{
  if (out) {
    TextTable tbl;
    OSDTreePlainDumper(crush.get(), this).dump(&tbl);
    *out << tbl;
  }
  if (f)
    OSDTreeFormattingDumper(crush.get(), this).dump(f);
}

void OSDMap::print_summary(Formatter *f, ostream& out) const
{
  if (f) {
    f->open_object_section("osdmap");
    f->dump_int("epoch", get_epoch());
    f->dump_int("num_osds", get_num_osds());
    f->dump_int("num_up_osds", get_num_up_osds());
    f->dump_int("num_in_osds", get_num_in_osds());
    f->dump_bool("full", test_flag(CEPH_OSDMAP_FULL) ? true : false);
    f->dump_bool("nearfull", test_flag(CEPH_OSDMAP_NEARFULL) ? true : false);
    f->dump_unsigned("num_remapped_pgs", get_num_pg_temp());
    f->close_section();
  } else {
    out << "     osdmap e" << get_epoch() << ": "
	<< get_num_osds() << " osds: "
	<< get_num_up_osds() << " up, "
	<< get_num_in_osds() << " in";
    if (get_num_pg_temp())
      out << "; " << get_num_pg_temp() << " remapped pgs";
    out << "\n";
    if (flags)
      out << "            flags " << get_flag_string() << "\n";
  }
}

void OSDMap::print_oneline_summary(ostream& out) const
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " osds: "
      << get_num_up_osds() << " up, "
      << get_num_in_osds() << " in";
  if (test_flag(CEPH_OSDMAP_FULL))
    out << " full";
  else if (test_flag(CEPH_OSDMAP_NEARFULL))
    out << " nearfull";
}

bool OSDMap::crush_ruleset_in_use(int ruleset) const
{
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    if (p->second.crush_ruleset == ruleset)
      return true;
  }
  return false;
}

int OSDMap::build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
			  int nosd, int pg_bits, int pgp_bits)
{
  ldout(cct, 10) << "build_simple on " << num_osd
		 << " osds with " << pg_bits << " pg bits per osd, "
		 << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph_clock_now(cct);

  if (nosd >=  0) {
    set_max_osd(nosd);
  } else {
    // count osds
    int maxosd = 0, numosd = 0;
    const md_config_t *conf = cct->_conf;
    vector<string> sections;
    conf->get_all_sections(sections);
    for (vector<string>::iterator i = sections.begin(); i != sections.end(); ++i) {
      if (i->find("osd.") != 0)
	continue;

      const char *begin = i->c_str() + 4;
      char *end = (char*)begin;
      int o = strtol(begin, &end, 10);
      if (*end != '\0')
	continue;

      if (o > cct->_conf->mon_max_osd) {
	lderr(cct) << "[osd." << o << "] in config has id > mon_max_osd " << cct->_conf->mon_max_osd << dendl;
	return -ERANGE;
      }
      numosd++;
      if (o > maxosd)
	maxosd = o;
    }

    set_max_osd(maxosd + 1);
  }

  // pgp_num <= pg_num
  if (pgp_bits > pg_bits)
    pgp_bits = pg_bits;

  vector<string> pool_names;
  pool_names.push_back("rbd");

  stringstream ss;
  int r;
  if (nosd >= 0)
    r = build_simple_crush_map(cct, *crush, nosd, &ss);
  else
    r = build_simple_crush_map_from_conf(cct, *crush, &ss);

  int poolbase = get_max_osd() ? get_max_osd() : 1;

  int const default_replicated_ruleset = crush->get_osd_pool_default_crush_replicated_ruleset(cct);
  assert(default_replicated_ruleset >= 0);

  for (vector<string>::iterator p = pool_names.begin();
       p != pool_names.end(); ++p) {
    int64_t pool = ++pool_max;
    pools[pool].type = pg_pool_t::TYPE_REPLICATED;
    pools[pool].flags = cct->_conf->osd_pool_default_flags;
    if (cct->_conf->osd_pool_default_flag_hashpspool)
      pools[pool].set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    if (cct->_conf->osd_pool_default_flag_nodelete)
      pools[pool].set_flag(pg_pool_t::FLAG_NODELETE);
    if (cct->_conf->osd_pool_default_flag_nopgchange)
      pools[pool].set_flag(pg_pool_t::FLAG_NOPGCHANGE);
    if (cct->_conf->osd_pool_default_flag_nosizechange)
      pools[pool].set_flag(pg_pool_t::FLAG_NOSIZECHANGE);
    pools[pool].size = cct->_conf->osd_pool_default_size;
    pools[pool].min_size = cct->_conf->get_osd_pool_default_min_size();
    pools[pool].crush_ruleset = default_replicated_ruleset;
    pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
    pools[pool].set_pg_num(poolbase << pg_bits);
    pools[pool].set_pgp_num(poolbase << pgp_bits);
    pools[pool].last_change = epoch;
    pool_name[pool] = *p;
    name_pool[*p] = pool;
  }

  if (r < 0)
    lderr(cct) << ss.str() << dendl;
  
  for (int i=0; i<get_max_osd(); i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }

  map<string,string> profile_map;
  r = get_erasure_code_profile_default(cct, profile_map, &ss);
  if (r < 0) {
    lderr(cct) << ss.str() << dendl;
    return r;
  }
  set_erasure_code_profile("default", profile_map);
  return 0;
}

int OSDMap::get_erasure_code_profile_default(CephContext *cct,
					     map<string,string> &profile_map,
					     ostream *ss)
{
  int r = get_json_str_map(cct->_conf->osd_pool_default_erasure_code_profile,
		      *ss,
		      &profile_map);
  profile_map["directory"] =
    cct->_conf->osd_pool_default_erasure_code_directory;
  return r;
}

int OSDMap::_build_crush_types(CrushWrapper& crush)
{
  crush.set_type_name(0, "osd");
  crush.set_type_name(1, "host");
  crush.set_type_name(2, "chassis");
  crush.set_type_name(3, "rack");
  crush.set_type_name(4, "row");
  crush.set_type_name(5, "pdu");
  crush.set_type_name(6, "pod");
  crush.set_type_name(7, "room");
  crush.set_type_name(8, "datacenter");
  crush.set_type_name(9, "region");
  crush.set_type_name(10, "root");
  return 10;
}

int OSDMap::build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				   int nosd, ostream *ss)
{
  crush.create();

  // root
  int root_type = _build_crush_types(crush);
  int rootid;
  int r = crush.add_bucket(0, 0, CRUSH_HASH_DEFAULT,
			   root_type, 0, NULL, NULL, &rootid);
  assert(r == 0);
  crush.set_item_name(rootid, "default");

  for (int o=0; o<nosd; o++) {
    map<string,string> loc;
    loc["host"] = "localhost";
    loc["rack"] = "localrack";
    loc["root"] = "default";
    ldout(cct, 10) << " adding osd." << o << " at " << loc << dendl;
    char name[8];
    sprintf(name, "osd.%d", o);
    crush.insert_item(cct, o, 1.0, name, loc);
  }

  build_simple_crush_rulesets(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}

int OSDMap::build_simple_crush_map_from_conf(CephContext *cct,
					     CrushWrapper& crush,
					     ostream *ss)
{
  const md_config_t *conf = cct->_conf;

  crush.create();

  set<string> hosts, racks;

  // root
  int root_type = _build_crush_types(crush);
  int rootid;
  int r = crush.add_bucket(0, 0,
			   CRUSH_HASH_DEFAULT,
			   root_type, 0, NULL, NULL, &rootid);
  assert(r == 0);
  crush.set_item_name(rootid, "default");

  // add osds
  vector<string> sections;
  conf->get_all_sections(sections);
  for (vector<string>::iterator i = sections.begin(); i != sections.end(); ++i) {
    if (i->find("osd.") != 0)
      continue;

    const char *begin = i->c_str() + 4;
    char *end = (char*)begin;
    int o = strtol(begin, &end, 10);
    if (*end != '\0')
      continue;

    string host, rack, row, room, dc, pool;
    vector<string> sections;
    sections.push_back("osd");
    sections.push_back(*i);
    conf->get_val_from_conf_file(sections, "host", host, false);
    conf->get_val_from_conf_file(sections, "rack", rack, false);
    conf->get_val_from_conf_file(sections, "row", row, false);
    conf->get_val_from_conf_file(sections, "room", room, false);
    conf->get_val_from_conf_file(sections, "datacenter", dc, false);
    conf->get_val_from_conf_file(sections, "root", pool, false);

    if (host.length() == 0)
      host = "unknownhost";
    if (rack.length() == 0)
      rack = "unknownrack";

    hosts.insert(host);
    racks.insert(rack);

    map<string,string> loc;
    loc["host"] = host;
    loc["rack"] = rack;
    if (row.size())
      loc["row"] = row;
    if (room.size())
      loc["room"] = room;
    if (dc.size())
      loc["datacenter"] = dc;
    loc["root"] = "default";

    ldout(cct, 5) << " adding osd." << o << " at " << loc << dendl;
    crush.insert_item(cct, o, 1.0, *i, loc);
  }

  build_simple_crush_rulesets(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}


int OSDMap::build_simple_crush_rulesets(CephContext *cct,
					CrushWrapper& crush,
					const string& root,
					ostream *ss)
{
  string failure_domain =
    crush.get_type_name(cct->_conf->osd_crush_chooseleaf_type);

  int r;
  r = crush.add_simple_ruleset("replicated_ruleset", root, failure_domain,
			       "firstn", pg_pool_t::TYPE_REPLICATED, ss);
  if (r < 0)
    return r;
  // do not add an erasure rule by default or else we will implicitly
  // require the crush_v2 feature of clients
  return 0;
}

int OSDMap::summarize_mapping_stats(
  OSDMap *newmap,
  const set<int64_t> *pools,
  std::string *out,
  Formatter *f) const
{
  set<int64_t> ls;
  if (pools) {
    ls = *pools;
  } else {
    for (map<int64_t, pg_pool_t>::const_iterator i = get_pools().begin();
	 i != get_pools().end();
	 ++i) {
      ls.insert(i->first);
    }
  }

  unsigned total_pg = 0;
  unsigned moved_pg = 0;
  vector<unsigned> base_by_osd(get_max_osd(), 0);
  vector<unsigned> new_by_osd(get_max_osd(), 0);
  for (set<int64_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    int64_t pool_id = *p;
    const pg_pool_t *pi = get_pg_pool(pool_id);
    vector<int> up, up2, acting;
    int up_primary, acting_primary;
    for (unsigned ps = 0; ps < pi->get_pg_num(); ++ps) {
      pg_t pgid(ps, pool_id, -1);
      total_pg += pi->get_size();
      pg_to_up_acting_osds(pgid, &up, &up_primary,
			   &acting, &acting_primary);
      for (vector<int>::iterator q = up.begin(); q != up.end(); ++q) {
	int osd = *q;
	if (osd >= 0 && osd < get_max_osd())
	  ++base_by_osd[osd];
      }
      if (newmap) {
	newmap->pg_to_up_acting_osds(pgid, &up2, &up_primary,
				     &acting, &acting_primary);
	for (vector<int>::iterator q = up2.begin(); q != up2.end(); ++q) {
	  int osd = *q;
	  if (osd >= 0 && osd < get_max_osd())
	    ++new_by_osd[osd];
	}
	if (pi->type == pg_pool_t::TYPE_ERASURE) {
	  for (unsigned i=0; i<up.size(); ++i) {
	    if (up[i] != up2[i]) {
	      ++moved_pg;
	    }
	  }
	} else if (pi->type == pg_pool_t::TYPE_REPLICATED) {
	  for (vector<int>::iterator q = up.begin(); q != up.end(); ++q) {
	    int osd = *q;
	    if (std::find(up2.begin(), up2.end(), osd) == up2.end()) {
	      ++moved_pg;
	    }
	  }
	} else {
	  assert(0 == "unhandled pool type");
	}
      }
    }
  }

  unsigned num_up_in = 0;
  for (int osd = 0; osd < get_max_osd(); ++osd) {
    if (is_up(osd) && is_in(osd))
      ++num_up_in;
  }
  if (!num_up_in) {
    return -EINVAL;
  }

  float avg_pg = (float)total_pg / (float)num_up_in;
  float base_stddev = 0, new_stddev = 0;
  int min = -1, max = -1;
  unsigned min_base_pg = 0, max_base_pg = 0;
  unsigned min_new_pg = 0, max_new_pg = 0;
  for (int osd = 0; osd < get_max_osd(); ++osd) {
    if (is_up(osd) && is_in(osd)) {
      float base_diff = (float)base_by_osd[osd] - avg_pg;
      base_stddev += base_diff * base_diff;
      float new_diff = (float)new_by_osd[osd] - avg_pg;
      new_stddev += new_diff * new_diff;
      if (min < 0 || min_base_pg < base_by_osd[osd]) {
	min = osd;
	min_base_pg = base_by_osd[osd];
	min_new_pg = new_by_osd[osd];
      }
      if (max < 0 || max_base_pg > base_by_osd[osd]) {
	max = osd;
	max_base_pg = base_by_osd[osd];
	max_new_pg = new_by_osd[osd];
      }
    }
  }
  base_stddev = sqrt(base_stddev / num_up_in);
  new_stddev = sqrt(new_stddev / num_up_in);

  float edev = sqrt(avg_pg * (1.0 - (1.0 / (double)num_up_in)));

  ostringstream ss;
  if (f)
    f->open_object_section("utilization");
  if (newmap) {
    if (f) {
      f->dump_unsigned("moved_pgs", moved_pg);
      f->dump_unsigned("total_pgs", total_pg);
    } else {
      ss << "moved " << moved_pg << " / " << total_pg
	 << " (" << ((float)moved_pg * 100.0 / (float)total_pg) << "%)\n";
    }
  }
  if (f) {
    f->dump_float("avg_pgs", avg_pg);
    f->dump_float("std_dev", base_stddev);
    f->dump_float("expected_baseline_std_dev", edev);
    if (newmap)
      f->dump_float("new_std_dev", new_stddev);
  } else {
    ss << "avg " << avg_pg << "\n";
    ss << "stddev " << base_stddev;
    if (newmap)
      ss << " -> " << new_stddev;
    ss << " (expected baseline " << edev << ")\n";
  }
  if (min >= 0) {
    if (f) {
      f->dump_unsigned("min_osd", min);
      f->dump_unsigned("min_osd_pgs", min_base_pg);
      if (newmap)
	f->dump_unsigned("new_min_osd_pgs", min_new_pg);
    } else {
      ss << "min osd." << min << " with " << min_base_pg;
      if (newmap)
	ss << " -> " << min_new_pg;
      ss << " pgs (" << (float)min_base_pg / avg_pg;
      if (newmap)
	ss << " -> " << (float)min_new_pg / avg_pg;
      ss << " * mean)\n";
    }
  }
  if (max >= 0) {
    if (f) {
      f->dump_unsigned("max_osd", max);
      f->dump_unsigned("max_osd_pgs", max_base_pg);
      if (newmap)
	f->dump_unsigned("new_max_osd_pgs", max_new_pg);
    } else {
      ss << "max osd." << min << " with " << max_base_pg;
      if (newmap)
	ss << " -> " << max_new_pg;
      ss << " pgs (" << (float)max_base_pg / avg_pg;
      if (newmap)
	ss << " -> " << (float)max_new_pg / avg_pg;
      ss << " * mean)\n";
    }
  }
  if (f)
    f->close_section();
  if (out)
    *out = ss.str();
  return 0;
}
