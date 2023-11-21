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

#include <algorithm>
#include <bit>
#include <optional>
#include <random>
#include <fmt/format.h>

#include <boost/algorithm/string.hpp>

#include "OSDMap.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "include/ceph_features.h"
#include "include/common_fwd.h"
#include "include/str_map.h"

#include "common/code_environment.h"
#include "mon/health_check.h"

#include "crush/CrushTreeDumper.h"
#include "common/Clock.h"
#include "mon/PGMap.h"
#include "common/pick_address.h"

using std::list;
using std::make_pair;
using std::map;
using std::multimap;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::vector;

using ceph::decode;
using ceph::encode;
using ceph::Formatter;

#define dout_subsys ceph_subsys_osd

MEMPOOL_DEFINE_OBJECT_FACTORY(OSDMap, osdmap, osdmap);
MEMPOOL_DEFINE_OBJECT_FACTORY(OSDMap::Incremental, osdmap_inc, osdmap);


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

void osd_info_t::encode(ceph::buffer::list& bl) const
{
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  encode(last_clean_begin, bl);
  encode(last_clean_end, bl);
  encode(up_from, bl);
  encode(up_thru, bl);
  encode(down_at, bl);
  encode(lost_at, bl);
}

void osd_info_t::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  decode(last_clean_begin, bl);
  decode(last_clean_end, bl);
  decode(up_from, bl);
  decode(up_thru, bl);
  decode(down_at, bl);
  decode(lost_at, bl);
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
  f->dump_stream("last_purged_snaps_scrub") << last_purged_snaps_scrub;
  f->dump_int("dead_epoch", dead_epoch);
}

void osd_xinfo_t::encode(ceph::buffer::list& bl, uint64_t enc_features) const
{
  uint8_t v = 4;
  if (!HAVE_FEATURE(enc_features, SERVER_OCTOPUS)) {
    v = 3;
  }
  ENCODE_START(v, 1, bl);
  encode(down_stamp, bl);
  __u32 lp = laggy_probability * float(0xfffffffful);
  encode(lp, bl);
  encode(laggy_interval, bl);
  encode(features, bl);
  encode(old_weight, bl);
  if (v >= 4) {
    encode(last_purged_snaps_scrub, bl);
    encode(dead_epoch, bl);
  }
  ENCODE_FINISH(bl);
}

void osd_xinfo_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(4, bl);
  decode(down_stamp, bl);
  __u32 lp;
  decode(lp, bl);
  laggy_probability = (float)lp / (float)0xffffffff;
  decode(laggy_interval, bl);
  if (struct_v >= 2)
    decode(features, bl);
  else
    features = 0;
  if (struct_v >= 3)
    decode(old_weight, bl);
  else
    old_weight = 0;
  if (struct_v >= 4) {
    decode(last_purged_snaps_scrub, bl);
    decode(dead_epoch, bl);
  } else {
    dead_epoch = 0;
  }
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
	     << " old_weight " << xi.old_weight
	     << " last_purged_snaps_scrub " << xi.last_purged_snaps_scrub
	     << " dead_epoch " << xi.dead_epoch;
}

// ----------------------------------
// OSDMap::Incremental

int OSDMap::Incremental::get_net_marked_out(const OSDMap *previous) const
{
  int n = 0;
  for (auto &weight : new_weight) {
    if (weight.second == CEPH_OSD_OUT && !previous->is_out(weight.first))
      n++;  // marked out
    else if (weight.second != CEPH_OSD_OUT && previous->is_out(weight.first))
      n--;  // marked in
  }
  return n;
}

int OSDMap::Incremental::get_net_marked_down(const OSDMap *previous) const
{
  int n = 0;
  for (auto &state : new_state) { // 
    if (state.second & CEPH_OSD_UP) {
      if (previous->is_up(state.first))
	n++;  // marked down
      else
	n--;  // marked up
    }
  }
  return n;
}

int OSDMap::Incremental::identify_osd(uuid_d u) const
{
  for (auto &uuid : new_uuid)
    if (uuid.second == u)
      return uuid.first;
  return -1;
}

int OSDMap::Incremental::propagate_base_properties_to_tiers(CephContext *cct,
							    const OSDMap& osdmap)
{
  ceph_assert(epoch == osdmap.get_epoch() + 1);

  for (auto &new_pool : new_pools) {
    if (!new_pool.second.tiers.empty()) {
      pg_pool_t& base = new_pool.second;

      auto new_rem_it = new_removed_snaps.find(new_pool.first);

      for (const auto &tier_pool : base.tiers) {
	const auto &r = new_pools.find(tier_pool);
	pg_pool_t *tier = 0;
	if (r == new_pools.end()) {
	  const pg_pool_t *orig = osdmap.get_pg_pool(tier_pool);
	  if (!orig) {
	    lderr(cct) << __func__ << " no pool " << tier_pool << dendl;
	    return -EIO;
	  }
	  tier = get_new_pool(tier_pool, orig);
	} else {
	  tier = &r->second;
	}
	if (tier->tier_of != new_pool.first) {
	  lderr(cct) << __func__ << " " << r->first << " tier_of != " << new_pool.first << dendl;
	  return -EIO;
	}

        ldout(cct, 10) << __func__ << " from " << new_pool.first << " to "
                       << tier_pool << dendl;
	tier->snap_seq = base.snap_seq;
	tier->snap_epoch = base.snap_epoch;
	tier->snaps = base.snaps;
	tier->removed_snaps = base.removed_snaps;
	tier->flags |= base.flags & (pg_pool_t::FLAG_SELFMANAGED_SNAPS|
				     pg_pool_t::FLAG_POOL_SNAPS);

	if (new_rem_it != new_removed_snaps.end()) {
	  new_removed_snaps[tier_pool] = new_rem_it->second;
	}

	tier->application_metadata = base.application_metadata;
      }
    }
  }
  return 0;
}

// ----------------------------------
// OSDMap

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
  for (const auto &child : children) {
    if (!subtree_is_down(child, down_cache)) {
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
    ceph_assert(type >= 0);

    if (!subtree_is_down(current, down_cache)) {
      ldout(cct, 30) << "containing_subtree_is_down(" << id << ") = false" << dendl;
      return false;
    }

    // is this a big enough subtree to be marked as down?
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

bool OSDMap::subtree_type_is_down(
  CephContext *cct,
  int id,
  int subtree_type,
  set<int> *down_in_osds,
  set<int> *up_in_osds,
  set<int> *subtree_up,
  unordered_map<int, set<int> > *subtree_type_down) const
{
  if (id >= 0) {
    bool is_down_ret = is_down(id);
    if (!is_out(id)) {
      if (is_down_ret) {
        down_in_osds->insert(id);
      } else {
        up_in_osds->insert(id);
      }
    }
    return is_down_ret;
  }

  if (subtree_type_down &&
      (*subtree_type_down)[subtree_type].count(id)) {
    return true;
  }

  list<int> children;
  crush->get_children(id, &children);
  for (const auto &child : children) {
    if (!subtree_type_is_down(
	  cct, child, crush->get_bucket_type(child),
	  down_in_osds, up_in_osds, subtree_up, subtree_type_down)) {
      subtree_up->insert(id);
      return false;
    }
  }
  if (subtree_type_down) {
    (*subtree_type_down)[subtree_type].insert(id);
  }
  return true;
}

void OSDMap::Incremental::encode_client_old(ceph::buffer::list& bl) const
{
  using ceph::encode;
  __u16 v = 5;
  encode(v, bl);
  encode(fsid, bl);
  encode(epoch, bl);
  encode(modified, bl);
  int32_t new_t = new_pool_max;
  encode(new_t, bl);
  encode(new_flags, bl);
  encode(fullmap, bl);
  encode(crush, bl);

  encode(new_max_osd, bl);
  // for encode(new_pools, bl);
  __u32 n = new_pools.size();
  encode(n, bl);
  for (const auto &new_pool : new_pools) {
    n = new_pool.first;
    encode(n, bl);
    encode(new_pool.second, bl, 0);
  }
  // for encode(new_pool_names, bl);
  n = new_pool_names.size();
  encode(n, bl);

  for (const auto &new_pool_name : new_pool_names) {
    n = new_pool_name.first;
    encode(n, bl);
    encode(new_pool_name.second, bl);
  }
  // for encode(old_pools, bl);
  n = old_pools.size();
  encode(n, bl);
  for (auto &old_pool : old_pools) {
    n = old_pool;
    encode(n, bl);
  }
  encode(new_up_client, bl, 0);
  {
    // legacy is map<int32_t,uint8_t>
    map<int32_t, uint8_t> os;
    for (auto p : new_state) {
      // new_state may only inculde some new flags(e.g., CEPH_OSD_NOOUT)
      // that an old client could not understand.
      // skip those!
      uint8_t s = p.second;
      if (p.second != 0 && s == 0)
        continue;
      os[p.first] = s;
    }
    uint32_t n = os.size();
    encode(n, bl);
    for (auto p : os) {
      encode(p.first, bl);
      encode(p.second, bl);
    }
  }
  encode(new_weight, bl);
  // for encode(new_pg_temp, bl);
  n = new_pg_temp.size();
  encode(n, bl);

  for (const auto &pg_temp : new_pg_temp) {
    old_pg_t opg = pg_temp.first.get_old_pg();
    encode(opg, bl);
    encode(pg_temp.second, bl);
  }
}

void OSDMap::Incremental::encode_classic(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  // base
  __u16 v = 6;
  encode(v, bl);
  encode(fsid, bl);
  encode(epoch, bl);
  encode(modified, bl);
  encode(new_pool_max, bl);
  encode(new_flags, bl);
  encode(fullmap, bl);
  encode(crush, bl);

  encode(new_max_osd, bl);
  encode(new_pools, bl, features);
  encode(new_pool_names, bl);
  encode(old_pools, bl);
  encode(new_up_client, bl, features);
  {
    map<int32_t, uint8_t> os;
    for (auto p : new_state) {
      // new_state may only inculde some new flags(e.g., CEPH_OSD_NOOUT)
      // that an old client could not understand.
      // skip those!
      uint8_t s = p.second;
      if (p.second != 0 && s == 0)
        continue;
      os[p.first] = s;
    }
    uint32_t n = os.size();
    encode(n, bl);
    for (auto p : os) {
      encode(p.first, bl);
      encode(p.second, bl);
    }
  }
  encode(new_weight, bl);
  encode(new_pg_temp, bl);

  // extended
  __u16 ev = 10;
  encode(ev, bl);
  encode(new_hb_back_up, bl, features);
  encode(new_up_thru, bl);
  encode(new_last_clean_interval, bl);
  encode(new_lost, bl);
  encode(new_blocklist, bl, features);
  encode(old_blocklist, bl, features);
  encode(new_up_cluster, bl, features);
  encode(cluster_snapshot, bl);
  encode(new_uuid, bl);
  encode(new_xinfo, bl, features);
  encode(new_hb_front_up, bl, features);
}

template<class T>
static void encode_addrvec_map_as_addr(const T& m, ceph::buffer::list& bl, uint64_t f)
{
  uint32_t n = m.size();
  encode(n, bl);
  for (auto& i : m) {
    encode(i.first, bl);
    encode(i.second.legacy_addr(), bl, f);
  }
}

template<class T>
static void encode_addrvec_pvec_as_addr(const T& m, ceph::buffer::list& bl, uint64_t f)
{
  uint32_t n = m.size();
  encode(n, bl);
  for (auto& i : m) {
    if (i) {
      encode(i->legacy_addr(), bl, f);
    } else {
      encode(entity_addr_t(), bl, f);
    }
  }
}

/* for a description of osdmap incremental versions, and when they were
 * introduced, please refer to
 *    doc/dev/osd_internals/osdmap_versions.txt
 */
void OSDMap::Incremental::encode(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  ceph_assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  size_t crc_offset;
  std::optional<ceph::buffer::list::contiguous_filler> crc_filler;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    uint8_t v = 9;
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      v = 3;
    } else if (!HAVE_FEATURE(features, SERVER_MIMIC)) {
      v = 5;
    } else if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      v = 6;
    } /* else if (!HAVE_FEATURE(features, SERVER_REEF)) {
      v = 8;
    } */
    ENCODE_START(v, 1, bl); // client-usable data
    encode(fsid, bl);
    encode(epoch, bl);
    encode(modified, bl);
    encode(new_pool_max, bl);
    encode(new_flags, bl);
    encode(fullmap, bl);
    encode(crush, bl);

    encode(new_max_osd, bl);
    encode(new_pools, bl, features);
    encode(new_pool_names, bl);
    encode(old_pools, bl);
    if (v >= 7) {
      encode(new_up_client, bl, features);
    } else {
      encode_addrvec_map_as_addr(new_up_client, bl, features);
    }
    if (v >= 5) {
      encode(new_state, bl);
    } else {
      map<int32_t, uint8_t> os;
      for (auto p : new_state) {
        // new_state may only inculde some new flags(e.g., CEPH_OSD_NOOUT)
        // that an old client could not understand.
        // skip those!
        uint8_t s = p.second;
        if (p.second != 0 && s == 0)
          continue;
        os[p.first] = s;
      }
      uint32_t n = os.size();
      encode(n, bl);
      for (auto p : os) {
        encode(p.first, bl);
        encode(p.second, bl);
      }
    }
    encode(new_weight, bl);
    encode(new_pg_temp, bl);
    encode(new_primary_temp, bl);
    encode(new_primary_affinity, bl);
    encode(new_erasure_code_profiles, bl);
    encode(old_erasure_code_profiles, bl);
    if (v >= 4) {
      encode(new_pg_upmap, bl);
      encode(old_pg_upmap, bl);
      encode(new_pg_upmap_items, bl);
      encode(old_pg_upmap_items, bl);
    }
    if (v >= 6) {
      encode(new_removed_snaps, bl);
      encode(new_purged_snaps, bl);
    }
    if (v >= 8) {
      encode(new_last_up_change, bl);
      encode(new_last_in_change, bl);
    }
    if (v >= 9) {
      encode(new_pg_upmap_primary, bl);
      encode(old_pg_upmap_primary, bl);
    }
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    uint8_t target_v = 9; // if bumping this, be aware of allow_crimson 12
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      target_v = 2;
    } else if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      target_v = 6;
    }
    if (change_stretch_mode) {
      target_v = std::max((uint8_t)10, target_v);
    }
    if (!new_range_blocklist.empty() ||
	!old_range_blocklist.empty()) {
      target_v = std::max((uint8_t)11, target_v);
    }
    if (mutate_allow_crimson != mutate_allow_crimson_t::NONE) {
      target_v = std::max((uint8_t)12, target_v);
    }
    ENCODE_START(target_v, 1, bl); // extended, osd-only data
    if (target_v < 7) {
      encode_addrvec_map_as_addr(new_hb_back_up, bl, features);
    } else {
      encode(new_hb_back_up, bl, features);
    }
    encode(new_up_thru, bl);
    encode(new_last_clean_interval, bl);
    encode(new_lost, bl);
    encode(new_blocklist, bl, features);
    encode(old_blocklist, bl, features);
    if (target_v < 7) {
      encode_addrvec_map_as_addr(new_up_cluster, bl, features);
    } else {
      encode(new_up_cluster, bl, features);
    }
    encode(cluster_snapshot, bl);
    encode(new_uuid, bl);
    encode(new_xinfo, bl, features);
    if (target_v < 7) {
      encode_addrvec_map_as_addr(new_hb_front_up, bl, features);
    } else {
      encode(new_hb_front_up, bl, features);
    }
    encode(features, bl);         // NOTE: features arg, not the member
    if (target_v >= 3) {
      encode(new_nearfull_ratio, bl);
      encode(new_full_ratio, bl);
      encode(new_backfillfull_ratio, bl);
    }
    // 5 was string-based new_require_min_compat_client
    if (target_v >= 6) {
      encode(new_require_min_compat_client, bl);
      encode(new_require_osd_release, bl);
    }
    if (target_v >= 8) {
      encode(new_crush_node_flags, bl);
    }
    if (target_v >= 9) {
      encode(new_device_class_flags, bl);
    }
    if (target_v >= 10) {
      encode(change_stretch_mode, bl);
      encode(new_stretch_bucket_count, bl);
      encode(new_degraded_stretch_mode, bl);
      encode(new_recovering_stretch_mode, bl);
      encode(new_stretch_mode_bucket, bl);
      encode(stretch_mode_enabled, bl);
    }
    if (target_v >= 11) {
      encode(new_range_blocklist, bl, features);
      encode(old_range_blocklist, bl, features);
    }
    if (target_v >= 12) {
      encode(mutate_allow_crimson, bl);
    }
    ENCODE_FINISH(bl); // osd-only data
  }

  crc_offset = bl.length();
  crc_filler = bl.append_hole(sizeof(uint32_t));
  tail_offset = bl.length();

  encode(full_crc, bl);

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  ceph::buffer::list front;
  front.substr_of(bl, start_offset, crc_offset - start_offset);
  inc_crc = front.crc32c(-1);
  ceph::buffer::list tail;
  tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
  inc_crc = tail.crc32c(inc_crc);
  ceph_le32 crc_le;
  crc_le = inc_crc;
  crc_filler->copy_in(4u, (char*)&crc_le);
  have_crc = true;
}

void OSDMap::Incremental::decode_classic(ceph::buffer::list::const_iterator &p)
{
  using ceph::decode;
  __u32 n, t;
  // base
  __u16 v;
  decode(v, p);
  decode(fsid, p);
  decode(epoch, p);
  decode(modified, p);
  if (v == 4 || v == 5) {
    decode(n, p);
    new_pool_max = n;
  } else if (v >= 6)
    decode(new_pool_max, p);
  decode(new_flags, p);
  decode(fullmap, p);
  decode(crush, p);

  decode(new_max_osd, p);
  if (v < 6) {
    new_pools.clear();
    decode(n, p);
    while (n--) {
      decode(t, p);
      decode(new_pools[t], p);
    }
  } else {
    decode(new_pools, p);
  }
  if (v == 5) {
    new_pool_names.clear();
    decode(n, p);
    while (n--) {
      decode(t, p);
      decode(new_pool_names[t], p);
    }
  } else if (v >= 6) {
    decode(new_pool_names, p);
  }
  if (v < 6) {
    old_pools.clear();
    decode(n, p);
    while (n--) {
      decode(t, p);
      old_pools.insert(t);
    }
  } else {
    decode(old_pools, p);
  }
  decode(new_up_client, p);
  {
    map<int32_t,uint8_t> ns;
    decode(ns, p);
    for (auto q : ns) {
      new_state[q.first] = q.second;
    }
  }
  decode(new_weight, p);

  if (v < 6) {
    new_pg_temp.clear();
    decode(n, p);
    while (n--) {
      old_pg_t opg;
      ceph::decode_raw(opg, p);
      decode(new_pg_temp[pg_t(opg)], p);
    }
  } else {
    decode(new_pg_temp, p);
  }

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    decode(ev, p);
  decode(new_hb_back_up, p);
  if (v < 5)
    decode(new_pool_names, p);
  decode(new_up_thru, p);
  decode(new_last_clean_interval, p);
  decode(new_lost, p);
  decode(new_blocklist, p);
  decode(old_blocklist, p);
  if (ev >= 6)
    decode(new_up_cluster, p);
  if (ev >= 7)
    decode(cluster_snapshot, p);
  if (ev >= 8)
    decode(new_uuid, p);
  if (ev >= 9)
    decode(new_xinfo, p);
  if (ev >= 10)
    decode(new_hb_front_up, p);
}

/* for a description of osdmap incremental versions, and when they were
 * introduced, please refer to
 *    doc/dev/osd_internals/osdmap_versions.txt
 */
void OSDMap::Incremental::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  /**
   * Older encodings of the Incremental had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  ceph::buffer::list crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    bl.seek(start_offset);
    decode_classic(bl);
    encode_features = 0;
    if (struct_v >= 6)
      encode_features = CEPH_FEATURE_PGID64;
    else
      encode_features = 0;
    return;
  }
  {
    DECODE_START(8, bl); // client-usable data
    decode(fsid, bl);
    decode(epoch, bl);
    decode(modified, bl);
    decode(new_pool_max, bl);
    decode(new_flags, bl);
    decode(fullmap, bl);
    decode(crush, bl);

    decode(new_max_osd, bl);
    decode(new_pools, bl);
    decode(new_pool_names, bl);
    decode(old_pools, bl);
    decode(new_up_client, bl);
    if (struct_v >= 5) {
      decode(new_state, bl);
    } else {
      map<int32_t,uint8_t> ns;
      decode(ns, bl);
      for (auto q : ns) {
	new_state[q.first] = q.second;
      }
    }
    decode(new_weight, bl);
    decode(new_pg_temp, bl);
    decode(new_primary_temp, bl);
    if (struct_v >= 2)
      decode(new_primary_affinity, bl);
    else
      new_primary_affinity.clear();
    if (struct_v >= 3) {
      decode(new_erasure_code_profiles, bl);
      decode(old_erasure_code_profiles, bl);
    } else {
      new_erasure_code_profiles.clear();
      old_erasure_code_profiles.clear();
    }
    if (struct_v >= 4) {
      decode(new_pg_upmap, bl);
      decode(old_pg_upmap, bl);
      decode(new_pg_upmap_items, bl);
      decode(old_pg_upmap_items, bl);
    }
    if (struct_v >= 6) {
      decode(new_removed_snaps, bl);
      decode(new_purged_snaps, bl);
    }
    if (struct_v >= 8) {
      decode(new_last_up_change, bl);
      decode(new_last_in_change, bl);
    }
    if (struct_v >= 9) {
      decode(new_pg_upmap_primary, bl);
      decode(old_pg_upmap_primary, bl);
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(10, bl); // extended, osd-only data
    decode(new_hb_back_up, bl);
    decode(new_up_thru, bl);
    decode(new_last_clean_interval, bl);
    decode(new_lost, bl);
    decode(new_blocklist, bl);
    decode(old_blocklist, bl);
    decode(new_up_cluster, bl);
    decode(cluster_snapshot, bl);
    decode(new_uuid, bl);
    decode(new_xinfo, bl);
    decode(new_hb_front_up, bl);
    if (struct_v >= 2)
      decode(encode_features, bl);
    else
      encode_features = CEPH_FEATURE_PGID64 | CEPH_FEATURE_OSDMAP_ENC;
    if (struct_v >= 3) {
      decode(new_nearfull_ratio, bl);
      decode(new_full_ratio, bl);
    } else {
      new_nearfull_ratio = -1;
      new_full_ratio = -1;
    }
    if (struct_v >= 4) {
      decode(new_backfillfull_ratio, bl);
    } else {
      new_backfillfull_ratio = -1;
    }
    if (struct_v == 5) {
      string r;
      decode(r, bl);
      if (r.length()) {
	new_require_min_compat_client = ceph_release_from_name(r);
      }
    }
    if (struct_v >= 6) {
      decode(new_require_min_compat_client, bl);
      decode(new_require_osd_release, bl);
    } else {
      if (new_flags >= 0 && (new_flags & CEPH_OSDMAP_REQUIRE_LUMINOUS)) {
	// only for compat with post-kraken pre-luminous test clusters
	new_require_osd_release = ceph_release_t::luminous;
	new_flags &= ~(CEPH_OSDMAP_LEGACY_REQUIRE_FLAGS);
      } else if (new_flags >= 0 && (new_flags & CEPH_OSDMAP_REQUIRE_KRAKEN)) {
	new_require_osd_release = ceph_release_t::kraken;
      } else if (new_flags >= 0 && (new_flags & CEPH_OSDMAP_REQUIRE_JEWEL)) {
	new_require_osd_release = ceph_release_t::jewel;
      } else {
	new_require_osd_release = ceph_release_t::unknown;
      }
    }
    if (struct_v >= 8) {
      decode(new_crush_node_flags, bl);
    }
    if (struct_v >= 9) {
      decode(new_device_class_flags, bl);
    }
    if (struct_v >= 10) {
      decode(change_stretch_mode, bl);
      decode(new_stretch_bucket_count, bl);
      decode(new_degraded_stretch_mode, bl);
      decode(new_recovering_stretch_mode, bl);
      decode(new_stretch_mode_bucket, bl);
      decode(stretch_mode_enabled, bl);
    }
    if (struct_v >= 11) {
      decode(new_range_blocklist, bl);
      decode(old_range_blocklist, bl);
    }
    if (struct_v >= 12) {
      decode(mutate_allow_crimson, bl);
    }
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    have_crc = true;
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    decode(inc_crc, bl);
    tail_offset = bl.get_off();
    decode(full_crc, bl);
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
      ceph::buffer::list tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (inc_crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << inc_crc;
      string s = ss.str();
      throw ceph::buffer::malformed_input(s.c_str());
    }
  }
}

void OSDMap::Incremental::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") << fsid;
  f->dump_stream("modified") << modified;
  f->dump_stream("new_last_up_change") << new_last_up_change;
  f->dump_stream("new_last_in_change") << new_last_in_change;
  f->dump_int("new_pool_max", new_pool_max);
  f->dump_int("new_flags", new_flags);
  f->dump_float("new_full_ratio", new_full_ratio);
  f->dump_float("new_nearfull_ratio", new_nearfull_ratio);
  f->dump_float("new_backfillfull_ratio", new_backfillfull_ratio);
  f->dump_int("new_require_min_compat_client", to_integer<int>(new_require_min_compat_client));
  f->dump_int("new_require_osd_release", to_integer<int>(new_require_osd_release));
  f->dump_unsigned("mutate_allow_crimson", static_cast<unsigned>(mutate_allow_crimson));

  if (fullmap.length()) {
    f->open_object_section("full_map");
    OSDMap full;
    ceph::buffer::list fbl = fullmap;  // kludge around constness.
    auto p = fbl.cbegin();
    full.decode(p);
    full.dump(f);
    f->close_section();
  }
  if (crush.length()) {
    f->open_object_section("crush");
    CrushWrapper c;
    ceph::buffer::list tbl = crush;  // kludge around constness.
    auto p = tbl.cbegin();
    c.decode(p);
    c.dump(f);
    f->close_section();
  }

  f->dump_int("new_max_osd", new_max_osd);

  f->open_array_section("new_pools");

  for (const auto &new_pool : new_pools) {
    f->open_object_section("pool");
    f->dump_int("pool", new_pool.first);
    new_pool.second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_pool_names");

  for (const auto &new_pool_name : new_pool_names) {
    f->open_object_section("pool_name");
    f->dump_int("pool", new_pool_name.first);
    f->dump_string("name", new_pool_name.second);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pools");

  for (const auto &old_pool : old_pools)
    f->dump_int("pool", old_pool);
  f->close_section();

  f->open_array_section("new_up_osds");

  for (const auto &upclient : new_up_client) {
    f->open_object_section("osd");
    f->dump_int("osd", upclient.first);
    f->dump_stream("public_addr") << upclient.second.legacy_addr();
    f->dump_object("public_addrs", upclient.second);
    if (auto p = new_up_cluster.find(upclient.first);
	p != new_up_cluster.end()) {
      f->dump_stream("cluster_addr") << p->second.legacy_addr();
      f->dump_object("cluster_addrs", p->second);
    }
    if (auto p = new_hb_back_up.find(upclient.first);
	p != new_hb_back_up.end()) {
      f->dump_object("heartbeat_back_addrs", p->second);
    }
    if (auto p = new_hb_front_up.find(upclient.first);
	p != new_hb_front_up.end()) {
      f->dump_object("heartbeat_front_addrs", p->second);
    }
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_weight");

  for (const auto &weight : new_weight) {
    f->open_object_section("osd");
    f->dump_int("osd", weight.first);
    f->dump_int("weight", weight.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_state_xor");
  for (const auto &ns : new_state) {
    f->open_object_section("osd");
    f->dump_int("osd", ns.first);
    set<string> st;
    calc_state_set(new_state.find(ns.first)->second, st);
    f->open_array_section("state_xor");
    for (auto &state : st)
      f->dump_string("state", state);
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_pg_temp");

  for (const auto &pg_temp : new_pg_temp) {
    f->open_object_section("pg");
    f->dump_stream("pgid") << pg_temp.first;
    f->open_array_section("osds");

    for (const auto &osd : pg_temp.second)
      f->dump_int("osd", osd);
    f->close_section();
    f->close_section();    
  }
  f->close_section();

  f->open_array_section("primary_temp");

  for (const auto &primary_temp : new_primary_temp) {
    f->dump_stream("pgid") << primary_temp.first;
    f->dump_int("osd", primary_temp.second);
  }
  f->close_section(); // primary_temp

  f->open_array_section("new_pg_upmap");
  for (auto& i : new_pg_upmap) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << i.first;
    f->open_array_section("osds");
    for (auto osd : i.second) {
      f->dump_int("osd", osd);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pg_upmap");
  for (auto& i : old_pg_upmap) {
    f->dump_stream("pgid") << i;
  }
  f->close_section();

  f->open_array_section("new_pg_upmap_items");
  for (auto& i : new_pg_upmap_items) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << i.first;
    f->open_array_section("mappings");
    for (auto& p : i.second) {
      f->open_object_section("mapping");
      f->dump_int("from", p.first);
      f->dump_int("to", p.second);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pg_upmap_items");
  for (auto& i : old_pg_upmap_items) {
    f->dump_stream("pgid") << i;
  }
  f->close_section();

  // dump upmap_primaries
  f->open_array_section("new_pg_upmap_primaries");
  for (auto& [pg, osd] : new_pg_upmap_primary) {
    f->open_object_section("primary_mapping");
    f->dump_stream("pgid") << pg;
    f->dump_int("primary_osd", osd);
    f->close_section();
  }
  f->close_section();  // new_pg_upmap_primaries

  // dump old_pg_upmap_primaries (removed primary mappings)
  f->open_array_section("old_pg_upmap_primaries");
  for (auto& pg : old_pg_upmap_primary) {
    f->dump_stream("pgid") << pg;
  }
  f->close_section();  // old_pg_upmap_primaries

  f->open_array_section("new_up_thru");

  for (const auto &up_thru : new_up_thru) {
    f->open_object_section("osd");
    f->dump_int("osd", up_thru.first);
    f->dump_int("up_thru", up_thru.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_lost");

  for (const auto &lost : new_lost) {
    f->open_object_section("osd");
    f->dump_int("osd", lost.first);
    f->dump_int("epoch_lost", lost.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_last_clean_interval");

  for (const auto &last_clean_interval : new_last_clean_interval) {
    f->open_object_section("osd");
    f->dump_int("osd", last_clean_interval.first);
    f->dump_int("first", last_clean_interval.second.first);
    f->dump_int("last", last_clean_interval.second.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_blocklist");
  for (const auto &blist : new_blocklist) {
    stringstream ss;
    ss << blist.first;
    f->dump_stream(ss.str().c_str()) << blist.second;
  }
  f->close_section();
  f->open_array_section("old_blocklist");
  for (const auto &blist : old_blocklist)
    f->dump_stream("addr") << blist;
  f->close_section();
  f->open_array_section("new_range_blocklist");
  for (const auto &blist : new_range_blocklist) {
    stringstream ss;
    ss << blist.first;
    f->dump_stream(ss.str().c_str()) << blist.second;
  }
  f->close_section();
  f->open_array_section("old_range_blocklist");
  for (const auto &blist : old_range_blocklist)
    f->dump_stream("addr") << blist;
  f->close_section();

  f->open_array_section("new_xinfo");
  for (const auto &xinfo : new_xinfo) {
    f->open_object_section("xinfo");
    f->dump_int("osd", xinfo.first);
    xinfo.second.dump(f);
    f->close_section();
  }
  f->close_section();

  if (cluster_snapshot.size())
    f->dump_string("cluster_snapshot", cluster_snapshot);

  f->open_array_section("new_uuid");
  for (const auto &uuid : new_uuid) {
    f->open_object_section("osd");
    f->dump_int("osd", uuid.first);
    f->dump_stream("uuid") << uuid.second;
    f->close_section();
  }
  f->close_section();

  OSDMap::dump_erasure_code_profiles(new_erasure_code_profiles, f);
  f->open_array_section("old_erasure_code_profiles");
  for (const auto &erasure_code_profile : old_erasure_code_profiles) {
    f->dump_string("old", erasure_code_profile);
  }
  f->close_section();

  f->open_array_section("new_removed_snaps");
  for (auto& p : new_removed_snaps) {
    f->open_object_section("pool");
    f->dump_int("pool", p.first);
    f->open_array_section("snaps");
    for (auto q = p.second.begin(); q != p.second.end(); ++q) {
      f->open_object_section("interval");
      f->dump_unsigned("begin", q.get_start());
      f->dump_unsigned("length", q.get_len());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_purged_snaps");
  for (auto& p : new_purged_snaps) {
    f->open_object_section("pool");
    f->dump_int("pool", p.first);
    f->open_array_section("snaps");
    for (auto q = p.second.begin(); q != p.second.end(); ++q) {
      f->open_object_section("interval");
      f->dump_unsigned("begin", q.get_start());
      f->dump_unsigned("length", q.get_len());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->open_array_section("new_crush_node_flags");
  for (auto& i : new_crush_node_flags) {
    f->open_object_section("node");
    f->dump_int("id", i.first);
    set<string> st;
    calc_state_set(i.second, st);
    for (auto& j : st) {
      f->dump_string("flag", j);
    }
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_device_class_flags");
  for (auto& i : new_device_class_flags) {
    f->open_object_section("device_class");
    f->dump_int("id", i.first);
    set<string> st;
    calc_state_set(i.second, st);
    for (auto& j : st) {
      f->dump_string("flag", j);
    }
    f->close_section();
  }
  f->close_section();
  f->open_object_section("stretch_mode");
  {
    f->dump_bool("change_stretch_mode", change_stretch_mode);
    f->dump_bool("stretch_mode_enabled", stretch_mode_enabled);
    f->dump_unsigned("new_stretch_bucket_count", new_stretch_bucket_count);
    f->dump_unsigned("new_degraded_stretch_mode", new_degraded_stretch_mode);
    f->dump_unsigned("new_recovering_stretch_mode", new_recovering_stretch_mode);
    f->dump_int("new_stretch_mode_bucket", new_stretch_mode_bucket);
  }
  f->close_section();
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
  for (auto &pool : pools)
    pool.second.last_change = e;
}

OSDMap::range_bits::range_bits() : ipv6(false) {
  memset(&bits, 0, sizeof(bits));
}

OSDMap::range_bits::range_bits(const entity_addr_t& addr) : ipv6(false) {
  memset(&bits, 0, sizeof(bits));
  parse(addr);
}

void OSDMap::range_bits::get_ipv6_bytes(unsigned const char *addr,
					uint64_t *upper, uint64_t *lower)
{
  *upper = ((uint64_t)(ntohl(*(uint32_t*)(addr)))) << 32 |
    ((uint64_t)(ntohl(*(uint32_t*)(&addr[4]))));
  *lower = ((uint64_t)(ntohl(*(uint32_t*)(&addr[8])))) << 32 |
    ((uint64_t)(ntohl(*(uint32_t*)(&addr[12]))));
}

void OSDMap::range_bits::parse(const entity_addr_t& addr) {
  // parse it into meaningful data
  if (addr.is_ipv6()) {
    get_ipv6_bytes(addr.in6_addr().sin6_addr.s6_addr,
		   &bits.ipv6.upper_64_bits, &bits.ipv6.lower_64_bits);
    int32_t lower_shift = std::min(128-
				   static_cast<int32_t>(addr.get_nonce()), 64);
    int32_t upper_shift = std::max(64- //(128-b.first.get_nonce())-64
				   static_cast<int32_t>(addr.get_nonce()), 0); 

    auto get_mask = [](int32_t shift) -> uint64_t {
      if (shift >= 0 && shift < 64) {
	return UINT64_MAX << shift;
      }
      return 0;
    };

    bits.ipv6.lower_mask = get_mask(lower_shift);
    bits.ipv6.upper_mask = get_mask(upper_shift);
    ipv6 = true;
  } else if (addr.is_ipv4()) {
    bits.ipv4.ip_32_bits = ntohl(addr.in4_addr().sin_addr.s_addr);
    if (addr.get_nonce() > 0) {
      bits.ipv4.mask = UINT32_MAX << (32-addr.get_nonce());
    } else {
      bits.ipv4.mask = 0;
    }
  } else {
    // uh...
  }
}

bool OSDMap::range_bits::matches(const entity_addr_t& addr) const {
  if (addr.is_ipv4() && !ipv6) {
    return ((ntohl(addr.in4_addr().sin_addr.s_addr) & bits.ipv4.mask) ==
	    (bits.ipv4.ip_32_bits & bits.ipv4.mask));
  } else if (addr.is_ipv6() && ipv6) {
    uint64_t upper_64, lower_64;
    get_ipv6_bytes(addr.in6_addr().sin6_addr.s6_addr, &upper_64, &lower_64);
    return (((upper_64 & bits.ipv6.upper_mask) ==
	     (bits.ipv6.upper_64_bits & bits.ipv6.upper_mask)) &&
	    ((lower_64 & bits.ipv6.lower_mask) ==
	     (bits.ipv6.lower_64_bits & bits.ipv6.lower_mask)));
  }
  return false;
}

bool OSDMap::is_blocklisted(const entity_addr_t& orig, CephContext *cct) const
{
  if (cct) ldout(cct, 25) << "is_blocklisted: " << orig << dendl;
  if (blocklist.empty() && range_blocklist.empty()) {
    if (cct) ldout(cct, 30) << "not blocklisted: " << orig << dendl;
    return false;
  }

  // all blocklist entries are type ANY for nautilus+
  // FIXME: avoid this copy!
  entity_addr_t a = orig;
  if (require_osd_release < ceph_release_t::nautilus) {
    a.set_type(entity_addr_t::TYPE_LEGACY);
  } else {
    a.set_type(entity_addr_t::TYPE_ANY);
  }

  // this specific instance?
  if (blocklist.count(a)) {
    if (cct) ldout(cct, 20) << "blocklist contains " << a << dendl;
    return true;
  }

  // is entire ip blocklisted?
  if (a.is_ip()) {
    a.set_port(0);
    a.set_nonce(0);
    if (blocklist.count(a)) {
      if (cct) ldout(cct, 20) << "blocklist contains " << a << dendl;
      return true;
    }
  }

  // is it in a blocklisted range?
  for (const auto& i : calculated_ranges) {
    bool blocked = i.second.matches(a);
    if (blocked) {
      if (cct) ldout(cct, 20) << "range_blocklist contains " << a << dendl;
      return true;
    }
  }

  if (cct) ldout(cct, 25) << "not blocklisted: " << orig << dendl;
  return false;
}

bool OSDMap::is_blocklisted(const entity_addrvec_t& av, CephContext *cct) const
{
  if (blocklist.empty() && range_blocklist.empty())
    return false;

  for (auto& a : av.v) {
    if (is_blocklisted(a, cct)) {
      return true;
    }
  }

  return false;
}

void OSDMap::get_blocklist(list<pair<entity_addr_t,utime_t> > *bl,
			   std::list<std::pair<entity_addr_t,utime_t> > *rl) const
{
   std::copy(blocklist.begin(), blocklist.end(), std::back_inserter(*bl));
   std::copy(range_blocklist.begin(), range_blocklist.end(),
	     std::back_inserter(*rl));
}

void OSDMap::get_blocklist(std::set<entity_addr_t> *bl,
			   std::set<entity_addr_t> *rl) const
{
  for (const auto &i : blocklist) {
    bl->insert(i.first);
  }
  for (const auto &i : range_blocklist) {
    rl->insert(i.first);
  }
}

void OSDMap::set_max_osd(int m)
{
  max_osd = m;
  osd_state.resize(max_osd, 0);
  osd_weight.resize(max_osd, CEPH_OSD_OUT);
  osd_info.resize(max_osd);
  osd_xinfo.resize(max_osd);
  osd_addrs->client_addrs.resize(max_osd);
  osd_addrs->cluster_addrs.resize(max_osd);
  osd_addrs->hb_back_addrs.resize(max_osd);
  osd_addrs->hb_front_addrs.resize(max_osd);
  osd_uuid->resize(max_osd);
  if (osd_primary_affinity)
    osd_primary_affinity->resize(max_osd, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);

  calc_num_osds();
}

int OSDMap::calc_num_osds()
{
  num_osd = 0;
  num_up_osd = 0;
  num_in_osd = 0;
  for (int i=0; i<max_osd; i++) {
    if (osd_state[i] & CEPH_OSD_EXISTS) {
      ++num_osd;
      if (osd_state[i] & CEPH_OSD_UP) {
	++num_up_osd;
      }
      if (get_weight(i) != CEPH_OSD_OUT) {
	++num_in_osd;
      }
    }
  }
  return num_osd;
}

void OSDMap::get_full_pools(CephContext *cct,
                            set<int64_t> *full,
                            set<int64_t> *backfillfull,
                            set<int64_t> *nearfull) const
{
  ceph_assert(full);
  ceph_assert(backfillfull);
  ceph_assert(nearfull);
  full->clear();
  backfillfull->clear();
  nearfull->clear();

  vector<int> full_osds;
  vector<int> backfillfull_osds;
  vector<int> nearfull_osds;
  for (int i = 0; i < max_osd; ++i) {
    if (exists(i) && is_up(i) && is_in(i)) {
      if (osd_state[i] & CEPH_OSD_FULL)
        full_osds.push_back(i);
      else if (osd_state[i] & CEPH_OSD_BACKFILLFULL)
	backfillfull_osds.push_back(i);
      else if (osd_state[i] & CEPH_OSD_NEARFULL)
	nearfull_osds.push_back(i);
    }
  }

  for (auto i: full_osds) {
    get_pool_ids_by_osd(cct, i, full);
  }
  for (auto i: backfillfull_osds) {
    get_pool_ids_by_osd(cct, i, backfillfull);
  }
  for (auto i: nearfull_osds) {
    get_pool_ids_by_osd(cct, i, nearfull);
  }
}

void OSDMap::get_full_osd_counts(set<int> *full, set<int> *backfill,
				 set<int> *nearfull) const
{
  full->clear();
  backfill->clear();
  nearfull->clear();
  for (int i = 0; i < max_osd; ++i) {
    if (exists(i) && is_up(i) && is_in(i)) {
      if (osd_state[i] & CEPH_OSD_FULL)
	full->emplace(i);
      else if (osd_state[i] & CEPH_OSD_BACKFILLFULL)
	backfill->emplace(i);
      else if (osd_state[i] & CEPH_OSD_NEARFULL)
	nearfull->emplace(i);
    }
  }
}

void OSDMap::get_out_of_subnet_osd_counts(CephContext *cct,
                                          std::string const &public_network,
                                          set<int> *unreachable) const
{
  unreachable->clear();
  for (int i = 0; i < max_osd; i++) {
    if (exists(i) && is_up(i)) {
      if (const auto& addrs = get_addrs(i).v; addrs.size() >= 2) {
        auto v1_addr = addrs[0].ip_only_to_str();
        if (!is_addr_in_subnet(cct, public_network, v1_addr)) {
          unreachable->emplace(i);
        }
        auto v2_addr = addrs[1].ip_only_to_str();
        if (!is_addr_in_subnet(cct, public_network, v2_addr)) {
          unreachable->emplace(i);
        }
      }
    }
  }
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

void OSDMap::get_out_existing_osds(set<int32_t>& ls) const
{
  for (int i = 0; i < max_osd; i++) {
    if (exists(i) && get_weight(i) == CEPH_OSD_OUT)
      ls.insert(i);
  }
}

void OSDMap::get_flag_set(set<string> *flagset) const
{
  for (unsigned i = 0; i < sizeof(flags) * 8; ++i) {
    if (flags & (1<<i)) {
      flagset->insert(get_flag_string(flags & (1<<i)));
    }
  }
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
  for (const auto &weight : weights) {
    if (weight.second > max)
      max = weight.second;
  }

  for (const auto &weight : weights) {
    inc.new_weight[weight.first] = (unsigned)((weight.second / max) * CEPH_OSD_IN);
  }
}

int OSDMap::identify_osd(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addrs(i).contains(addr) ||
		      get_cluster_addrs(i).contains(addr)))
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

int OSDMap::identify_osd_on_all_channels(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addrs(i).contains(addr) ||
		      get_cluster_addrs(i).contains(addr) ||
		      get_hb_back_addrs(i).contains(addr) ||
		      get_hb_front_addrs(i).contains(addr)))
      return i;
  return -1;
}

int OSDMap::find_osd_on_ip(const entity_addr_t& ip) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addrs(i).is_same_host(ip) ||
		      get_cluster_addrs(i).is_same_host(ip)))
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
  if (crush->has_nondefault_tunables5())
    features |= CEPH_FEATURE_CRUSH_TUNABLES5;
  if (crush->has_incompat_choose_args())
    features |= CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS;
  if (crush->has_nondefault_tunables_msr())
    features |= CEPH_FEATURE_CRUSH_MSR;
  mask |= CEPH_FEATURES_CRUSH;

  if (!pg_upmap.empty() || !pg_upmap_items.empty() || !pg_upmap_primaries.empty())
    features |= CEPH_FEATUREMASK_OSDMAP_PG_UPMAP;
  mask |= CEPH_FEATUREMASK_OSDMAP_PG_UPMAP;

  for (auto &pool: pools) {
    if (pool.second.has_flag(pg_pool_t::FLAG_HASHPSPOOL)) {
      features |= CEPH_FEATURE_OSDHASHPSPOOL;
    }
    if (!pool.second.tiers.empty() ||
	pool.second.is_tier()) {
      features |= CEPH_FEATURE_OSD_CACHEPOOL;
    }
    int ruleid = pool.second.get_crush_rule();
    if (ruleid >= 0) {
      if (crush->is_v2_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_V2;
      if (crush->is_v3_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_TUNABLES3;
      if (crush->is_v5_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_TUNABLES5;
      if (crush->is_msr_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_MSR;
    }
  }
  mask |= CEPH_FEATURE_OSDHASHPSPOOL | CEPH_FEATURE_OSD_CACHEPOOL;

  if (osd_primary_affinity) {
    for (int i = 0; i < max_osd; ++i) {
      if ((*osd_primary_affinity)[i] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
	features |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;
	break;
      }
    }
  }
  mask |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;

  if (entity_type == CEPH_ENTITY_TYPE_OSD) {
    const uint64_t jewel_features = CEPH_FEATURE_SERVER_JEWEL;
    if (require_osd_release >= ceph_release_t::jewel) {
      features |= jewel_features;
    }
    mask |= jewel_features;

    const uint64_t kraken_features = CEPH_FEATUREMASK_SERVER_KRAKEN
      | CEPH_FEATURE_MSG_ADDR2;
    if (require_osd_release >= ceph_release_t::kraken) {
      features |= kraken_features;
    }
    mask |= kraken_features;

    if (stretch_mode_enabled) {
      features |= CEPH_FEATUREMASK_STRETCH_MODE;
      mask |= CEPH_FEATUREMASK_STRETCH_MODE;
    }
  }

  if (require_min_compat_client >= ceph_release_t::nautilus) {
    // if min_compat_client is >= nautilus, require v2 cephx signatures
    // from everyone
    features |= CEPH_FEATUREMASK_CEPHX_V2;
  } else if (require_osd_release >= ceph_release_t::nautilus &&
	     entity_type == CEPH_ENTITY_TYPE_OSD) {
    // if osds are >= nautilus, at least require the signatures from them
    features |= CEPH_FEATUREMASK_CEPHX_V2;
  }
  mask |= CEPH_FEATUREMASK_CEPHX_V2;

  if (pmask)
    *pmask = mask;
  return features;
}

ceph_release_t OSDMap::get_min_compat_client() const
{
  uint64_t f = get_features(CEPH_ENTITY_TYPE_CLIENT, nullptr);

  if (HAVE_FEATURE(f, CRUSH_MSR)) {      // TODOSAM -- add version right before merge
    return ceph_release_t::squid;        // v19.2.0
  }
  if (HAVE_FEATURE(f, OSDMAP_PG_UPMAP) ||      // v12.0.0-1733-g27d6f43
      HAVE_FEATURE(f, CRUSH_CHOOSE_ARGS)) {    // v12.0.1-2172-gef1ef28
    return ceph_release_t::luminous;  // v12.2.0
  }
  if (HAVE_FEATURE(f, CRUSH_TUNABLES5)) {      // v10.0.0-612-g043a737
    return ceph_release_t::jewel;     // v10.2.0
  }
  if (HAVE_FEATURE(f, CRUSH_V4)) {             // v0.91-678-g325fc56
    return ceph_release_t::hammer;    // v0.94.0
  }
  if (HAVE_FEATURE(f, OSD_PRIMARY_AFFINITY) || // v0.76-553-gf825624
      HAVE_FEATURE(f, CRUSH_TUNABLES3) ||      // v0.76-395-ge20a55d
      HAVE_FEATURE(f, OSD_CACHEPOOL)) {        // v0.67-401-gb91c1c5
    return ceph_release_t::firefly;   // v0.80.0
  }
  if (HAVE_FEATURE(f, CRUSH_TUNABLES2) ||      // v0.54-684-g0cc47ff
      HAVE_FEATURE(f, OSDHASHPSPOOL)) {        // v0.57-398-g8cc2b0f
    return ceph_release_t::dumpling;  // v0.67.0
  }
  if (HAVE_FEATURE(f, CRUSH_TUNABLES)) {       // v0.48argonaut-206-g6f381af
    return ceph_release_t::argonaut;  // v0.48argonaut-206-g6f381af
  }
  return ceph_release_t::argonaut;    // v0.48argonaut-206-g6f381af
}

ceph_release_t OSDMap::get_require_min_compat_client() const
{
  return require_min_compat_client;
}

void OSDMap::_calc_up_osd_features()
{
  bool first = true;
  cached_up_osd_features = 0;
  for (int osd = 0; osd < max_osd; ++osd) {
    if (!is_up(osd))
      continue;
    const osd_xinfo_t &xi = get_xinfo(osd);
    if (xi.features == 0)
      continue;  // bogus xinfo, maybe #20751 or similar, skipping
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

bool OSDMap::any_osd_laggy() const
{
  for (int osd = 0; osd < max_osd; ++osd) {
    if (!is_up(osd)) { continue; }
    const auto &xi = get_xinfo(osd);
    if (xi.laggy_probability || xi.laggy_interval) {
      return true;
    }
  }
  return false;
}

void OSDMap::dedup(const OSDMap *o, OSDMap *n)
{
  using ceph::encode;
  if (o->epoch == n->epoch)
    return;

  int diff = 0;

  // do addrs match?
  if (o->max_osd != n->max_osd)
    diff++;
  for (int i = 0; i < o->max_osd && i < n->max_osd; i++) {
    if ( n->osd_addrs->client_addrs[i] &&  o->osd_addrs->client_addrs[i] &&
	*n->osd_addrs->client_addrs[i] == *o->osd_addrs->client_addrs[i])
      n->osd_addrs->client_addrs[i] = o->osd_addrs->client_addrs[i];
    else
      diff++;
    if ( n->osd_addrs->cluster_addrs[i] &&  o->osd_addrs->cluster_addrs[i] &&
	*n->osd_addrs->cluster_addrs[i] == *o->osd_addrs->cluster_addrs[i])
      n->osd_addrs->cluster_addrs[i] = o->osd_addrs->cluster_addrs[i];
    else
      diff++;
    if ( n->osd_addrs->hb_back_addrs[i] &&  o->osd_addrs->hb_back_addrs[i] &&
	*n->osd_addrs->hb_back_addrs[i] == *o->osd_addrs->hb_back_addrs[i])
      n->osd_addrs->hb_back_addrs[i] = o->osd_addrs->hb_back_addrs[i];
    else
      diff++;
    if ( n->osd_addrs->hb_front_addrs[i] &&  o->osd_addrs->hb_front_addrs[i] &&
	*n->osd_addrs->hb_front_addrs[i] == *o->osd_addrs->hb_front_addrs[i])
      n->osd_addrs->hb_front_addrs[i] = o->osd_addrs->hb_front_addrs[i];
    else
      diff++;
  }
  if (diff == 0) {
    // zoinks, no differences at all!
    n->osd_addrs = o->osd_addrs;
  }

  // does crush match?
  ceph::buffer::list oc, nc;
  encode(*o->crush, oc, CEPH_FEATURES_SUPPORTED_DEFAULT);
  encode(*n->crush, nc, CEPH_FEATURES_SUPPORTED_DEFAULT);
  if (oc.contents_equal(nc)) {
    n->crush = o->crush;
  }

  // does pg_temp match?
  if (*o->pg_temp == *n->pg_temp)
    n->pg_temp = o->pg_temp;

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

void OSDMap::clean_temps(CephContext *cct,
			 const OSDMap& oldmap,
			 const OSDMap& nextmap,
			 Incremental *pending_inc)
{
  ldout(cct, 10) << __func__ << dendl;

  for (auto pg : *nextmap.pg_temp) {
    // if pool does not exist, remove any existing pg_temps associated with
    // it.  we don't care about pg_temps on the pending_inc either; if there
    // are new_pg_temp entries on the pending, clear them out just as well.
    if (!nextmap.have_pg_pool(pg.first.pool())) {
      ldout(cct, 10) << __func__ << " removing pg_temp " << pg.first
		     << " for nonexistent pool " << pg.first.pool() << dendl;
      pending_inc->new_pg_temp[pg.first].clear();
      continue;
    }
    if (!nextmap.pg_exists(pg.first)) {
      ldout(cct, 10) << __func__ << " removing pg_temp " << pg.first
                     << " for nonexistent pg " << dendl;
      pending_inc->new_pg_temp[pg.first].clear();
      continue;
    }
    // all osds down?
    unsigned num_up = 0;
    for (auto o : pg.second) {
      if (!nextmap.is_down(o)) {
	++num_up;
	break;
      }
    }
    if (num_up == 0) {
      ldout(cct, 10) << __func__ << "  removing pg_temp " << pg.first
		     << " with all down osds" << pg.second << dendl;
      pending_inc->new_pg_temp[pg.first].clear();
      continue;
    }
    // redundant pg_temp?
    vector<int> raw_up;
    int primary;
    nextmap.pg_to_raw_up(pg.first, &raw_up, &primary);
    bool remove = false;
    if (raw_up == pg.second) {
      ldout(cct, 10) << __func__ << "  removing pg_temp " << pg.first << " "
		     << pg.second << " that matches raw_up mapping" << dendl;
      remove = true;
    }
    // oversized pg_temp?
    if (pg.second.size() > nextmap.get_pg_pool(pg.first.pool())->get_size()) {
      ldout(cct, 10) << __func__ << "  removing pg_temp " << pg.first << " "
		     << pg.second << " exceeds pool size" << dendl;
      remove = true;
    }
    if (remove) {
      if (oldmap.pg_temp->count(pg.first))
	pending_inc->new_pg_temp[pg.first].clear();
      else
	pending_inc->new_pg_temp.erase(pg.first);
    }
  }
  
  for (auto &pg : *nextmap.primary_temp) {
    // primary down?
    if (nextmap.is_down(pg.second)) {
      ldout(cct, 10) << __func__ << "  removing primary_temp " << pg.first
		     << " to down " << pg.second << dendl;
      pending_inc->new_primary_temp[pg.first] = -1;
      continue;
    }
    // redundant primary_temp?
    vector<int> real_up, templess_up;
    int real_primary, templess_primary;
    pg_t pgid = pg.first;
    nextmap.pg_to_acting_osds(pgid, &real_up, &real_primary);
    nextmap.pg_to_raw_up(pgid, &templess_up, &templess_primary);
    if (real_primary == templess_primary){
      ldout(cct, 10) << __func__ << "  removing primary_temp "
		     << pgid << " -> " << real_primary
		     << " (unnecessary/redundant)" << dendl;
      if (oldmap.primary_temp->count(pgid))
	pending_inc->new_primary_temp[pgid] = -1;
      else
	pending_inc->new_primary_temp.erase(pgid);
    }
  }
}

void OSDMap::get_upmap_pgs(vector<pg_t> *upmap_pgs) const
{
  upmap_pgs->reserve(pg_upmap.size() + pg_upmap_items.size());
  for (auto& p : pg_upmap)
    upmap_pgs->push_back(p.first);
  for (auto& p : pg_upmap_items)
    upmap_pgs->push_back(p.first);
}

bool OSDMap::check_pg_upmaps(
  CephContext *cct,
  const vector<pg_t>& to_check,
  vector<pg_t> *to_cancel,
  map<pg_t, mempool::osdmap::vector<pair<int,int>>> *to_remap) const
{
  bool any_change = false;
  map<int, map<int, float>> rule_weight_map;
  for (auto& pg : to_check) {
    const pg_pool_t *pi = get_pg_pool(pg.pool());
    if (!pi || pg.ps() >= pi->get_pg_num_pending()) {
      ldout(cct, 0) << __func__ << " pg " << pg << " is gone or merge source"
		    << dendl;
      to_cancel->push_back(pg);
      continue;
    }
    if (pi->is_pending_merge(pg, nullptr)) {
      ldout(cct, 0) << __func__ << " pg " << pg << " is pending merge"
		    << dendl;
      to_cancel->push_back(pg);
      continue;
    }
    vector<int> raw, up;
    pg_to_raw_upmap(pg, &raw, &up);
    auto crush_rule = get_pg_pool_crush_rule(pg);
    auto r = crush->verify_upmap(cct,
                                 crush_rule,
                                 get_pg_pool_size(pg),
                                 up);
    if (r < 0) {
      ldout(cct, 0) << __func__ << " verify_upmap of pg " << pg
                    << " returning " << r
                    << dendl;
      to_cancel->push_back(pg);
      continue;
    }
    // below we check against crush-topology changing..
    map<int, float> weight_map;
    auto it = rule_weight_map.find(crush_rule);
    if (it == rule_weight_map.end()) {
      auto r = crush->get_rule_weight_osd_map(crush_rule, &weight_map);
      if (r < 0) {
        lderr(cct) << __func__ << " unable to get crush weight_map for "
                   << "crush_rule " << crush_rule
                   << dendl;
        continue;
      }
      rule_weight_map[crush_rule] = weight_map;
    } else {
      weight_map = it->second;
    }
    ldout(cct, 10) << __func__ << " pg " << pg
                   << " weight_map " << weight_map
                   << dendl;
    for (auto osd : up) {
      auto it = weight_map.find(osd);
      if (it == weight_map.end()) {
        ldout(cct, 10) << __func__ << " pg " << pg << ": osd " << osd << " is gone or has "
	              << "been moved out of the specific crush-tree"
		      << dendl;
        to_cancel->push_back(pg);
        break;
      }
      auto adjusted_weight = get_weightf(it->first) * it->second;
      if (adjusted_weight == 0) {
        ldout(cct, 10) << __func__ << " pg " << pg << ": osd " << osd
	              << " is out/crush-out"
		    << dendl;
        to_cancel->push_back(pg);
        break;
      }
    }
    if (!to_cancel->empty() && to_cancel->back() == pg)
      continue;
    // okay, upmap is valid
    // continue to check if it is still necessary
    auto i = pg_upmap.find(pg);
    if (i != pg_upmap.end()) {
      if (i->second == raw) {
        ldout(cct, 10) << __func__ << "removing redundant pg_upmap " << i->first << " "
                       << i->second << dendl;
        to_cancel->push_back(pg);
        continue;
      }
      if ((int)i->second.size() != get_pg_pool_size(pg)) {
        ldout(cct, 10) << __func__ << "removing pg_upmap " << i->first << " "
                       << i->second << " != pool size " << get_pg_pool_size(pg)
                       << dendl;
        to_cancel->push_back(pg);
        continue;
      }
    }
    auto j = pg_upmap_items.find(pg);
    if (j != pg_upmap_items.end()) {
      mempool::osdmap::vector<pair<int,int>> newmap;
      for (auto& p : j->second) {
	auto osd_from = p.first;
	auto osd_to = p.second;
        if (std::find(raw.begin(), raw.end(), osd_from) == raw.end()) {
          // cancel mapping if source osd does not exist anymore
          ldout(cct, 20) << __func__ << " pg_upmap_items (source osd does not exist) " << pg_upmap_items << dendl;
          continue;
        }
        if (osd_to != CRUSH_ITEM_NONE && osd_to < max_osd &&
            osd_to >= 0 && osd_weight[osd_to] == 0) {
          // cancel mapping if target osd is out
          ldout(cct, 20) << __func__ << " pg_upmap_items (target osd is out) " << pg_upmap_items << dendl;
          continue;
        }
        newmap.push_back(p);
      }
      if (newmap.empty()) {
        ldout(cct, 10) << __func__ << " removing no-op pg_upmap_items "
                       << j->first << " " << j->second
                       << dendl;
        to_cancel->push_back(pg);
      } else if (newmap != j->second) {
        // check partial no-op here.
        ldout(cct, 10) << __func__ << " simplifying partially no-op pg_upmap_items "
                       << j->first << " " << j->second
                       << " -> " << newmap
                       << dendl;
        to_remap->insert({pg, newmap});
        any_change = true;
      }
    }
  }
  any_change = any_change || !to_cancel->empty();
  return any_change;
}

void OSDMap::clean_pg_upmaps(
  CephContext *cct,
  Incremental *pending_inc,
  const vector<pg_t>& to_cancel,
  const map<pg_t, mempool::osdmap::vector<pair<int,int>>>& to_remap) const
{
  for (auto &pg: to_cancel) {
    auto i = pending_inc->new_pg_upmap.find(pg);
    if (i != pending_inc->new_pg_upmap.end()) {
      ldout(cct, 10) << __func__ << " cancel invalid pending "
                     << "pg_upmap entry "
                     << i->first << "->" << i->second
                     << dendl;
      pending_inc->new_pg_upmap.erase(i);
    }
    auto j = pg_upmap.find(pg);
    if (j != pg_upmap.end()) {
      ldout(cct, 10) << __func__ << " cancel invalid pg_upmap entry "
                     << j->first << "->" << j->second
                     << dendl;
      pending_inc->old_pg_upmap.insert(pg);
    }
    auto p = pending_inc->new_pg_upmap_items.find(pg);
    if (p != pending_inc->new_pg_upmap_items.end()) {
      ldout(cct, 10) << __func__ << " cancel invalid pending "
                     << "pg_upmap_items entry "
                     << p->first << "->" << p->second
                     << dendl;
      pending_inc->new_pg_upmap_items.erase(p);
    }
    auto q = pg_upmap_items.find(pg);
    if (q != pg_upmap_items.end()) {
      ldout(cct, 10) << __func__ << " cancel invalid "
                     << "pg_upmap_items entry "
                     << q->first << "->" << q->second
                     << dendl;
      pending_inc->old_pg_upmap_items.insert(pg);
    }
  }
  for (auto& i : to_remap)
    pending_inc->new_pg_upmap_items[i.first] = i.second;
}

bool OSDMap::clean_pg_upmaps(
  CephContext *cct,
  Incremental *pending_inc) const
{
  ldout(cct, 10) << __func__ << dendl;
  vector<pg_t> to_check;
  vector<pg_t> to_cancel;
  map<pg_t, mempool::osdmap::vector<pair<int,int>>> to_remap;

  get_upmap_pgs(&to_check);
  auto any_change = check_pg_upmaps(cct, to_check, &to_cancel, &to_remap);
  clean_pg_upmaps(cct, pending_inc, to_cancel, to_remap);
  //TODO: Create these 3 functions for pg_upmap_primaries and so they can be checked 
  //      and cleaned in the same way as pg_upmap. This is not critical since invalid
  //      pg_upmap_primaries are never applied, (the final check is in _apply_upmap).
  return any_change;
}

int OSDMap::apply_incremental(const Incremental &inc)
{
  new_blocklist_entries = false;
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    return -EINVAL;
  
  ceph_assert(inc.epoch == epoch+1);

  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    ceph::buffer::list bl(inc.fullmap);
    decode(bl);
    return 0;
  }

  // nope, incremental.
  if (inc.new_flags >= 0) {
    flags = inc.new_flags;
    // the below is just to cover a newly-upgraded luminous mon
    // cluster that has to set require_jewel_osds or
    // require_kraken_osds before the osds can be upgraded to
    // luminous.
    if (flags & CEPH_OSDMAP_REQUIRE_KRAKEN) {
      if (require_osd_release < ceph_release_t::kraken) {
	require_osd_release = ceph_release_t::kraken;
      }
    } else if (flags & CEPH_OSDMAP_REQUIRE_JEWEL) {
      if (require_osd_release < ceph_release_t::jewel) {
	require_osd_release = ceph_release_t::jewel;
      }
    }
  }

  if (inc.new_max_osd >= 0)
    set_max_osd(inc.new_max_osd);

  if (inc.new_pool_max != -1)
    pool_max = inc.new_pool_max;

  for (const auto &pool : inc.new_pools) {
    pools[pool.first] = pool.second;
    pools[pool.first].last_change = epoch;
  }

  new_removed_snaps = inc.new_removed_snaps;
  new_purged_snaps = inc.new_purged_snaps;
  for (auto p = new_removed_snaps.begin();
       p != new_removed_snaps.end();
       ++p) {
    removed_snaps_queue[p->first].union_of(p->second);
  }
  for (auto p = new_purged_snaps.begin();
       p != new_purged_snaps.end();
       ++p) {
    auto q = removed_snaps_queue.find(p->first);
    ceph_assert(q != removed_snaps_queue.end());
    q->second.subtract(p->second);
    if (q->second.empty()) {
      removed_snaps_queue.erase(q);
    }
  }

  if (inc.new_last_up_change != utime_t()) {
    last_up_change = inc.new_last_up_change;
  }
  if (inc.new_last_in_change != utime_t()) {
    last_in_change = inc.new_last_in_change;
  }

  for (const auto &pname : inc.new_pool_names) {
    auto pool_name_entry = pool_name.find(pname.first);
    if (pool_name_entry != pool_name.end()) {
      name_pool.erase(pool_name_entry->second);
      pool_name_entry->second = pname.second;
    } else {
      pool_name[pname.first] = pname.second;
    }
    name_pool[pname.second] = pname.first;
  }
  
  for (const auto &pool : inc.old_pools) {
    pools.erase(pool);
    name_pool.erase(pool_name[pool]);
    pool_name.erase(pool);
  }

  for (const auto &weight : inc.new_weight) {
    set_weight(weight.first, weight.second);

    // if we are marking in, clear the AUTOOUT and NEW bits, and clear
    // xinfo old_weight.
    if (weight.second) {
      osd_state[weight.first] &= ~(CEPH_OSD_AUTOOUT | CEPH_OSD_NEW);
      osd_xinfo[weight.first].old_weight = 0;
    }
  }

  for (const auto &primary_affinity : inc.new_primary_affinity) {
    set_primary_affinity(primary_affinity.first, primary_affinity.second);
  }

  // erasure_code_profiles
  for (const auto &profile : inc.old_erasure_code_profiles)
    erasure_code_profiles.erase(profile);
  
  for (const auto &profile : inc.new_erasure_code_profiles) {
    set_erasure_code_profile(profile.first, profile.second);
  }
  
  // up/down
  for (const auto &state : inc.new_state) {
    const auto osd = state.first;
    int s = state.second ? state.second : CEPH_OSD_UP;
    if ((osd_state[osd] & CEPH_OSD_UP) &&
	(s & CEPH_OSD_UP)) {
      osd_info[osd].down_at = epoch;
      osd_xinfo[osd].down_stamp = modified;
    }
    if ((osd_state[osd] & CEPH_OSD_EXISTS) &&
	(s & CEPH_OSD_EXISTS)) {
      // osd is destroyed; clear out anything interesting.
      (*osd_uuid)[osd] = uuid_d();
      osd_info[osd] = osd_info_t();
      osd_xinfo[osd] = osd_xinfo_t();
      set_primary_affinity(osd, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
      osd_addrs->client_addrs[osd].reset(new entity_addrvec_t());
      osd_addrs->cluster_addrs[osd].reset(new entity_addrvec_t());
      osd_addrs->hb_front_addrs[osd].reset(new entity_addrvec_t());
      osd_addrs->hb_back_addrs[osd].reset(new entity_addrvec_t());
      osd_state[osd] = 0;
    } else {
      osd_state[osd] ^= s;
    }
  }

  for (const auto &client : inc.new_up_client) {
    osd_state[client.first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
    osd_state[client.first] &= ~CEPH_OSD_STOP; // if any
    osd_addrs->client_addrs[client.first].reset(
      new entity_addrvec_t(client.second));
    osd_addrs->hb_back_addrs[client.first].reset(
      new entity_addrvec_t(inc.new_hb_back_up.find(client.first)->second));
    osd_addrs->hb_front_addrs[client.first].reset(
      new entity_addrvec_t(inc.new_hb_front_up.find(client.first)->second));

    osd_info[client.first].up_from = epoch;
  }

  for (const auto &cluster : inc.new_up_cluster)
    osd_addrs->cluster_addrs[cluster.first].reset(
      new entity_addrvec_t(cluster.second));

  // info
  for (const auto &thru : inc.new_up_thru)
    osd_info[thru.first].up_thru = thru.second;
  
  for (const auto &interval : inc.new_last_clean_interval) {
    osd_info[interval.first].last_clean_begin = interval.second.first;
    osd_info[interval.first].last_clean_end = interval.second.second;
  }
  
  for (const auto &lost : inc.new_lost)
    osd_info[lost.first].lost_at = lost.second;

  // xinfo
  for (const auto &xinfo : inc.new_xinfo)
    osd_xinfo[xinfo.first] = xinfo.second;

  // uuid
  for (const auto &uuid : inc.new_uuid)
    (*osd_uuid)[uuid.first] = uuid.second;

  // pg rebuild
  for (const auto &pg : inc.new_pg_temp) {
    if (pg.second.empty())
      pg_temp->erase(pg.first);
    else
      pg_temp->set(pg.first, pg.second);
  }
  if (!inc.new_pg_temp.empty()) {
    // make sure pg_temp is efficiently stored
    pg_temp->rebuild();
  }

  for (const auto &pg : inc.new_primary_temp) {
    if (pg.second == -1)
      primary_temp->erase(pg.first);
    else
      (*primary_temp)[pg.first] = pg.second;
  }

  for (auto& p : inc.new_pg_upmap) {
    pg_upmap[p.first] = p.second;
  }
  for (auto& pg : inc.old_pg_upmap) {
    pg_upmap.erase(pg);
  }
  for (auto& p : inc.new_pg_upmap_items) {
    pg_upmap_items[p.first] = p.second;
  }
  for (auto& pg : inc.old_pg_upmap_items) {
    pg_upmap_items.erase(pg);
  }

  for (auto& [pg, prim] : inc.new_pg_upmap_primary) {
    pg_upmap_primaries[pg] = prim;
  }
  for (auto& pg : inc.old_pg_upmap_primary) {
    pg_upmap_primaries.erase(pg);
  }

  // blocklist
  if (!inc.new_blocklist.empty()) {
    blocklist.insert(inc.new_blocklist.begin(),inc.new_blocklist.end());
    new_blocklist_entries = true;
  }
  for (const auto &addr : inc.old_blocklist)
    blocklist.erase(addr);

  for (const auto& addr_p : inc.new_range_blocklist) {
    range_blocklist.insert(addr_p);
    calculated_ranges.emplace(addr_p.first, addr_p.first);
    new_blocklist_entries = true;
  }
  for (const auto &addr : inc.old_range_blocklist) {
    calculated_ranges.erase(addr);
    range_blocklist.erase(addr);
  }

  for (auto& i : inc.new_crush_node_flags) {
    if (i.second) {
      crush_node_flags[i.first] = i.second;
    } else {
      crush_node_flags.erase(i.first);
    }
  }

  for (auto& i : inc.new_device_class_flags) {
    if (i.second) {
      device_class_flags[i.first] = i.second;
    } else {
      device_class_flags.erase(i.first);
    }
  }

  // cluster snapshot?
  if (inc.cluster_snapshot.length()) {
    cluster_snapshot = inc.cluster_snapshot;
    cluster_snapshot_epoch = inc.epoch;
  } else {
    cluster_snapshot.clear();
    cluster_snapshot_epoch = 0;
  }

  if (inc.new_nearfull_ratio >= 0) {
    nearfull_ratio = inc.new_nearfull_ratio;
  }
  if (inc.new_backfillfull_ratio >= 0) {
    backfillfull_ratio = inc.new_backfillfull_ratio;
  }
  if (inc.new_full_ratio >= 0) {
    full_ratio = inc.new_full_ratio;
  }
  if (inc.new_require_min_compat_client > ceph_release_t::unknown) {
    require_min_compat_client = inc.new_require_min_compat_client;
  }
  if (inc.new_require_osd_release >= ceph_release_t::unknown) {
    require_osd_release = inc.new_require_osd_release;
    if (require_osd_release >= ceph_release_t::luminous) {
      flags &= ~(CEPH_OSDMAP_LEGACY_REQUIRE_FLAGS);
      flags |= CEPH_OSDMAP_RECOVERY_DELETES;
    }
  }

  if (inc.new_require_osd_release >= ceph_release_t::unknown) {
    require_osd_release = inc.new_require_osd_release;
    if (require_osd_release >= ceph_release_t::nautilus) {
      flags |= CEPH_OSDMAP_PGLOG_HARDLIMIT;
    }
  }
  // do new crush map last (after up/down stuff)
  if (inc.crush.length()) {
    ceph::buffer::list bl(inc.crush);
    auto blp = bl.cbegin();
    crush.reset(new CrushWrapper);
    crush->decode(blp);
    if (require_osd_release >= ceph_release_t::luminous) {
      // only increment if this is a luminous-encoded osdmap, lest
      // the mon's crush_version diverge from what the osds or others
      // are decoding and applying on their end.  if we won't encode
      // it in the canonical version, don't change it.
      ++crush_version;
    }
    for (auto it = device_class_flags.begin();
         it != device_class_flags.end();) {
      const char* class_name = crush->get_class_name(it->first);
      if (!class_name) // device class is gone
        it = device_class_flags.erase(it);
      else
        it++;
    }
  }

  if (inc.change_stretch_mode) {
    stretch_mode_enabled = inc.stretch_mode_enabled;
    stretch_bucket_count = inc.new_stretch_bucket_count;
    degraded_stretch_mode = inc.new_degraded_stretch_mode;
    recovering_stretch_mode = inc.new_recovering_stretch_mode;
    stretch_mode_bucket = inc.new_stretch_mode_bucket;
  }

  switch (inc.mutate_allow_crimson) {
  case Incremental::mutate_allow_crimson_t::NONE:
    break;
  case Incremental::mutate_allow_crimson_t::SET:
    allow_crimson = true;
    break;
  case Incremental::mutate_allow_crimson_t::CLEAR:
    allow_crimson = false;
    break;
  }

  calc_num_osds();
  _calc_up_osd_features();
  return 0;
}

// mapping
int OSDMap::map_to_pg(
  int64_t poolid,
  const string& name,
  const string& key,
  const string& nspace,
  pg_t *pg) const
{
  // calculate ps (placement seed)
  const pg_pool_t *pool = get_pg_pool(poolid);
  if (!pool)
    return -ENOENT;
  ps_t ps;
  if (!key.empty())
    ps = pool->hash_key(key, nspace);
  else
    ps = pool->hash_key(name, nspace);
  *pg = pg_t(ps, poolid);
  return 0;
}

int OSDMap::object_locator_to_pg(
  const object_t& oid, const object_locator_t& loc, pg_t &pg) const
{
  if (loc.hash >= 0) {
    if (!get_pg_pool(loc.get_pool())) {
      return -ENOENT;
    }
    pg = pg_t(loc.hash, loc.get_pool());
    return 0;
  }
  return map_to_pg(loc.get_pool(), oid.name, loc.key, loc.nspace, &pg);
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
    for (auto& osd : osds) {
      if (!exists(osd))
	osd = CRUSH_ITEM_NONE;
    }
  }
}

void OSDMap::_pg_to_raw_osds(
  const pg_pool_t& pool, pg_t pg,
  vector<int> *osds,
  ps_t *ppps) const
{
  // map to osds[]
  ps_t pps = pool.raw_pg_to_pps(pg);  // placement ps
  unsigned size = pool.get_size();

  // what crush rule?
  int ruleno = pool.get_crush_rule();
  if (ruleno >= 0)
    crush->do_rule(ruleno, pps, *osds, size, osd_weight, pg.pool());

  _remove_nonexistent_osds(pool, *osds);

  if (ppps)
    *ppps = pps;
}

int OSDMap::_pick_primary(const vector<int>& osds) const
{
  for (auto osd : osds) {
    if (osd != CRUSH_ITEM_NONE) {
      return osd;
    }
  }
  return -1;
}

void OSDMap::_apply_upmap(const pg_pool_t& pi, pg_t raw_pg, vector<int> *raw) const
{
  pg_t pg = pi.raw_pg_to_pg(raw_pg);
  auto p = pg_upmap.find(pg);
  if (p != pg_upmap.end()) {
    // make sure targets aren't marked out
    for (auto osd : p->second) {
      if (osd != CRUSH_ITEM_NONE && osd < max_osd && osd >= 0 &&
          osd_weight[osd] == 0) {
	// reject/ignore the explicit mapping
	return;
      }
    }
    *raw = vector<int>(p->second.begin(), p->second.end());
    // continue to check and apply pg_upmap_items if any
  }

  auto q = pg_upmap_items.find(pg);
  if (q != pg_upmap_items.end()) {
    // NOTE: this approach does not allow a bidirectional swap,
    // e.g., [[1,2],[2,1]] applied to [0,1,2] -> [0,2,1].
    for (auto& [osd_from, osd_to] : q->second) {
      // A capcaity change upmap (repace osd in the pg with osd not in the pg)
      // make sure the replacement value doesn't already appear
      bool exists = false;
      ssize_t pos = -1;
      for (unsigned i = 0; i < raw->size(); ++i) {
	int osd = (*raw)[i];
	if (osd == osd_to) {
	  exists = true;
	  break;
	}
	// ignore mapping if target is marked out (or invalid osd id)
	if (osd == osd_from &&
	    pos < 0 &&
	    !(osd_to != CRUSH_ITEM_NONE && osd_to < max_osd &&
	    osd_to >= 0 && osd_weight[osd_to] == 0)) {
	  pos = i;
	  }
      }
      if (!exists && pos >= 0) {
	(*raw)[pos] = osd_to;
      }
    }
  }
  auto r = pg_upmap_primaries.find(pg);
  if (r != pg_upmap_primaries.end()) {
    auto new_prim = r->second;	
    // Apply mapping only if new primary is not marked out and valid osd id
    if (new_prim != CRUSH_ITEM_NONE && new_prim < max_osd && new_prim >= 0 &&
	osd_weight[new_prim] != 0) {
      int new_prim_idx = 0;
      for (int i = 1 ; i < (int)raw->size(); i++) {  // start from 1 on purpose
        if ((*raw)[i] == new_prim) {
	  new_prim_idx = i;
	  break;
        }
      }
      if (new_prim_idx > 0) {
	// swap primary
        (*raw)[new_prim_idx] = (*raw)[0];
        (*raw)[0] = new_prim;
      }
    }
  }
}

// pg -> (up osd list)
void OSDMap::_raw_to_up_osds(const pg_pool_t& pool, const vector<int>& raw,
                             vector<int> *up) const
{
  if (pool.can_shift_osds()) {
    // shift left
    up->clear();
    up->reserve(raw.size());
    for (unsigned i=0; i<raw.size(); i++) {
      if (!exists(raw[i]) || is_down(raw[i]))
	continue;
      up->push_back(raw[i]);
    }
  } else {
    // set down/dne devices to NONE
    up->resize(raw.size());
    for (int i = raw.size() - 1; i >= 0; --i) {
      if (!exists(raw[i]) || is_down(raw[i])) {
	(*up)[i] = CRUSH_ITEM_NONE;
      } else {
	(*up)[i] = raw[i];
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
  for (const auto osd : *osds) {
    if (osd != CRUSH_ITEM_NONE &&
	(*osd_primary_affinity)[osd] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
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
  const auto p = pg_temp->find(pg);
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
  const auto &pp = primary_temp->find(pg);
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

void OSDMap::pg_to_raw_osds(pg_t pg, vector<int> *raw, int *primary) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool) {
    *primary = -1;
    raw->clear();
    return;
  }
  _pg_to_raw_osds(*pool, pg, raw, NULL);
  *primary = _pick_primary(*raw);
}

void OSDMap::pg_to_raw_upmap(pg_t pg, vector<int>*raw,
                             vector<int> *raw_upmap) const
{
  auto pool = get_pg_pool(pg.pool());
  if (!pool) {
    raw_upmap->clear();
    return;
  }
  _pg_to_raw_osds(*pool, pg, raw, NULL);
  *raw_upmap = *raw;
  _apply_upmap(*pool, pg, raw_upmap);
}

void OSDMap::pg_to_raw_up(pg_t pg, vector<int> *up, int *primary) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool) {
    *primary = -1;
    up->clear();
    return;
  }
  vector<int> raw;
  ps_t pps;
  _pg_to_raw_osds(*pool, pg, &raw, &pps);
  _apply_upmap(*pool, pg, &raw);
  _raw_to_up_osds(*pool, raw, up);
  *primary = _pick_primary(raw);
  _apply_primary_affinity(pps, *pool, up, primary);
}

void OSDMap::_pg_to_up_acting_osds(
  const pg_t& pg, vector<int> *up, int *up_primary,
  vector<int> *acting, int *acting_primary,
  bool raw_pg_to_pg) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool ||
      (!raw_pg_to_pg && pg.ps() >= pool->get_pg_num())) {
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
  _get_temp_osds(*pool, pg, &_acting, &_acting_primary);
  if (_acting.empty() || up || up_primary) {
    _pg_to_raw_osds(*pool, pg, &raw, &pps);
    _apply_upmap(*pool, pg, &raw);
    _raw_to_up_osds(*pool, raw, &_up);
    _up_primary = _pick_primary(_up);
    _apply_primary_affinity(pps, *pool, &_up, &_up_primary);
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
  }

  if (acting)
    acting->swap(_acting);
  if (acting_primary)
    *acting_primary = _acting_primary;
}

int OSDMap::calc_pg_role_broken(int osd, const vector<int>& acting, int nrep)
{
  // This implementation is broken for EC PGs since the osd may appear
  // multiple times in the acting set.  See
  // https://tracker.ceph.com/issues/43213
  if (!nrep)
    nrep = acting.size();
  for (int i=0; i<nrep; i++) 
    if (acting[i] == osd)
      return i;
  return -1;
}

int OSDMap::calc_pg_role(pg_shard_t who, const vector<int>& acting)
{
  int nrep = acting.size();
  if (who.shard == shard_id_t::NO_SHARD) {
    for (int i=0; i<nrep; i++) {
      if (acting[i] == who.osd) {
	return i;
      }
    }
  } else {
    if (who.shard < nrep && acting[who.shard] == who.osd) {
      return who.shard;
    }
  }
  return -1;
}

bool OSDMap::primary_changed_broken(
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
  if (calc_pg_role_broken(oldprimary, oldacting) !=
      calc_pg_role_broken(newprimary, newacting))
    return true;
  return false;      // same primary (tho replicas may have changed)
}

uint64_t OSDMap::get_encoding_features() const
{
  uint64_t f = SIGNIFICANT_FEATURES;
  if (require_osd_release < ceph_release_t::octopus) {
    f &= ~CEPH_FEATURE_SERVER_OCTOPUS;
  }
  if (require_osd_release < ceph_release_t::nautilus) {
    f &= ~CEPH_FEATURE_SERVER_NAUTILUS;
  }
  if (require_osd_release < ceph_release_t::mimic) {
    f &= ~CEPH_FEATURE_SERVER_MIMIC;
  }
  if (require_osd_release < ceph_release_t::luminous) {
    f &= ~(CEPH_FEATURE_SERVER_LUMINOUS |
	   CEPH_FEATURE_CRUSH_CHOOSE_ARGS);
  }
  if (require_osd_release < ceph_release_t::kraken) {
    f &= ~(CEPH_FEATURE_SERVER_KRAKEN |
	   CEPH_FEATURE_MSG_ADDR2);
  }
  if (require_osd_release < ceph_release_t::jewel) {
    f &= ~(CEPH_FEATURE_SERVER_JEWEL |
	   CEPH_FEATURE_NEW_OSDOP_ENCODING |
	   CEPH_FEATURE_CRUSH_TUNABLES5);
  }
  return f;
}

// serialize, unserialize
void OSDMap::encode_client_old(ceph::buffer::list& bl) const
{
  using ceph::encode;
  __u16 v = 5;
  encode(v, bl);

  // base
  encode(fsid, bl);
  encode(epoch, bl);
  encode(created, bl);
  encode(modified, bl);

  // for encode(pools, bl);
  __u32 n = pools.size();
  encode(n, bl);

  for (const auto &pool : pools) {
    n = pool.first;
    encode(n, bl);
    encode(pool.second, bl, 0);
  }
  // for encode(pool_name, bl);
  n = pool_name.size();
  encode(n, bl);
  for (const auto &pname : pool_name) {
    n = pname.first;
    encode(n, bl);
    encode(pname.second, bl);
  }
  // for encode(pool_max, bl);
  n = pool_max;
  encode(n, bl);

  encode(flags, bl);

  encode(max_osd, bl);
  {
    uint32_t n = osd_state.size();
    encode(n, bl);
    for (auto s : osd_state) {
      encode((uint8_t)s, bl);
    }
  }
  encode(osd_weight, bl);
  encode(osd_addrs->client_addrs, bl, 0);

  // for encode(pg_temp, bl);
  n = pg_temp->size();
  encode(n, bl);
  for (const auto& pg : *pg_temp) {
    old_pg_t opg = pg.first.get_old_pg();
    encode(opg, bl);
    encode(pg.second, bl);
  }

  // crush
  ceph::buffer::list cbl;
  crush->encode(cbl, 0 /* legacy (no) features */);
  encode(cbl, bl);
}

void OSDMap::encode_classic(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  __u16 v = 6;
  encode(v, bl);

  // base
  encode(fsid, bl);
  encode(epoch, bl);
  encode(created, bl);
  encode(modified, bl);

  encode(pools, bl, features);
  encode(pool_name, bl);
  encode(pool_max, bl);

  encode(flags, bl);

  encode(max_osd, bl);
  {
    uint32_t n = osd_state.size();
    encode(n, bl);
    for (auto s : osd_state) {
      encode((uint8_t)s, bl);
    }
  }
  encode(osd_weight, bl);
  encode(osd_addrs->client_addrs, bl, features);

  encode(*pg_temp, bl);

  // crush
  ceph::buffer::list cbl;
  crush->encode(cbl, 0 /* legacy (no) features */);
  encode(cbl, bl);

  // extended
  __u16 ev = 10;
  encode(ev, bl);
  encode(osd_addrs->hb_back_addrs, bl, features);
  encode(osd_info, bl);
  encode(blocklist, bl, features);
  encode(osd_addrs->cluster_addrs, bl, features);
  encode(cluster_snapshot_epoch, bl);
  encode(cluster_snapshot, bl);
  encode(*osd_uuid, bl);
  encode(osd_xinfo, bl, features);
  encode(osd_addrs->hb_front_addrs, bl, features);
}

/* for a description of osdmap versions, and when they were introduced, please
 * refer to
 *    doc/dev/osd_internals/osdmap_versions.txt
 */
void OSDMap::encode(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  ceph_assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  size_t crc_offset;
  std::optional<ceph::buffer::list::contiguous_filler> crc_filler;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    // NOTE: any new encoding dependencies must be reflected by
    // SIGNIFICANT_FEATURES
    uint8_t v = 10;
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      v = 3;
    } else if (!HAVE_FEATURE(features, SERVER_MIMIC)) {
      v = 6;
    } else if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      v = 7;
    } /* else if (!HAVE_FEATURE(features, SERVER_REEF)) {
      v = 9;
    } */
    ENCODE_START(v, 1, bl); // client-usable data
    // base
    encode(fsid, bl);
    encode(epoch, bl);
    encode(created, bl);
    encode(modified, bl);

    encode(pools, bl, features);
    encode(pool_name, bl);
    encode(pool_max, bl);

    if (v < 4) {
      decltype(flags) f = flags;
      if (require_osd_release >= ceph_release_t::luminous)
	f |= CEPH_OSDMAP_REQUIRE_LUMINOUS | CEPH_OSDMAP_RECOVERY_DELETES;
      else if (require_osd_release == ceph_release_t::kraken)
	f |= CEPH_OSDMAP_REQUIRE_KRAKEN;
      else if (require_osd_release == ceph_release_t::jewel)
	f |= CEPH_OSDMAP_REQUIRE_JEWEL;
      encode(f, bl);
    } else {
      encode(flags, bl);
    }

    encode(max_osd, bl);
    if (v >= 5) {
      encode(osd_state, bl);
    } else {
      uint32_t n = osd_state.size();
      encode(n, bl);
      for (auto s : osd_state) {
	encode((uint8_t)s, bl);
      }
    }
    encode(osd_weight, bl);
    if (v >= 8) {
      encode(osd_addrs->client_addrs, bl, features);
    } else {
      encode_addrvec_pvec_as_addr(osd_addrs->client_addrs, bl, features);
    }

    encode(*pg_temp, bl);
    encode(*primary_temp, bl);
    if (osd_primary_affinity) {
      encode(*osd_primary_affinity, bl);
    } else {
      vector<__u32> v;
      encode(v, bl);
    }

    // crush
    ceph::buffer::list cbl;
    crush->encode(cbl, features);
    encode(cbl, bl);
    encode(erasure_code_profiles, bl);

    if (v >= 4) {
      encode(pg_upmap, bl);
      encode(pg_upmap_items, bl);
    } else {
      ceph_assert(pg_upmap.empty());
      ceph_assert(pg_upmap_items.empty());
    }
    if (v >= 6) {
      encode(crush_version, bl);
    }
    if (v >= 7) {
      encode(new_removed_snaps, bl);
      encode(new_purged_snaps, bl);
    }
    if (v >= 9) {
      encode(last_up_change, bl);
      encode(last_in_change, bl);
    }
    if (v >= 10) {
      encode(pg_upmap_primaries, bl);
    } else {
      ceph_assert(pg_upmap_primaries.empty());
    }
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    // NOTE: any new encoding dependencies must be reflected by
    // SIGNIFICANT_FEATURES
    uint8_t target_v = 9; // when bumping this, be aware of allow_crimson
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      target_v = 1;
    } else if (!HAVE_FEATURE(features, SERVER_MIMIC)) {
      target_v = 5;
    } else if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      target_v = 6;
    }
    if (stretch_mode_enabled) {
      target_v = std::max((uint8_t)10, target_v);
    }
    if (!range_blocklist.empty()) {
      target_v = std::max((uint8_t)11, target_v);
    }
    if (allow_crimson) {
      target_v = std::max((uint8_t)12, target_v);
    }
    ENCODE_START(target_v, 1, bl); // extended, osd-only data
    if (target_v < 7) {
      encode_addrvec_pvec_as_addr(osd_addrs->hb_back_addrs, bl, features);
    } else {
      encode(osd_addrs->hb_back_addrs, bl, features);
    }
    encode(osd_info, bl);
    {
      // put this in a sorted, ordered map<> so that we encode in a
      // deterministic order.
      map<entity_addr_t,utime_t> blocklist_map;
      for (const auto &addr : blocklist)
	blocklist_map.insert(make_pair(addr.first, addr.second));
      encode(blocklist_map, bl, features);
    }
    if (target_v < 7) {
      encode_addrvec_pvec_as_addr(osd_addrs->cluster_addrs, bl, features);
    } else {
      encode(osd_addrs->cluster_addrs, bl, features);
    }
    encode(cluster_snapshot_epoch, bl);
    encode(cluster_snapshot, bl);
    encode(*osd_uuid, bl);
    encode(osd_xinfo, bl, features);
    if (target_v < 7) {
      encode_addrvec_pvec_as_addr(osd_addrs->hb_front_addrs, bl, features);
    } else {
      encode(osd_addrs->hb_front_addrs, bl, features);
    }
    if (target_v >= 2) {
      encode(nearfull_ratio, bl);
      encode(full_ratio, bl);
      encode(backfillfull_ratio, bl);
    }
    // 4 was string-based new_require_min_compat_client
    if (target_v >= 5) {
      encode(require_min_compat_client, bl);
      encode(require_osd_release, bl);
    }
    if (target_v >= 6) {
      encode(removed_snaps_queue, bl);
    }
    if (target_v >= 8) {
      encode(crush_node_flags, bl);
    }
    if (target_v >= 9) {
      encode(device_class_flags, bl);
    }
    if (target_v >= 10) {
      encode(stretch_mode_enabled, bl);
      encode(stretch_bucket_count, bl);
      encode(degraded_stretch_mode, bl);
      encode(recovering_stretch_mode, bl);
      encode(stretch_mode_bucket, bl);
    }
    if (target_v >= 11) {
      ::encode(range_blocklist, bl, features);
    }
    if (target_v >= 12) {
      ::encode(allow_crimson, bl);
    }
    ENCODE_FINISH(bl); // osd-only data
  }

  crc_offset = bl.length();
  crc_filler = bl.append_hole(sizeof(uint32_t));
  tail_offset = bl.length();

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  ceph::buffer::list front;
  front.substr_of(bl, start_offset, crc_offset - start_offset);
  crc = front.crc32c(-1);
  if (tail_offset < bl.length()) {
    ceph::buffer::list tail;
    tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
    crc = tail.crc32c(crc);
  }
  ceph_le32 crc_le;
  crc_le = crc;
  crc_filler->copy_in(4, (char*)&crc_le);
  crc_defined = true;
}

/* for a description of osdmap versions, and when they were introduced, please
 * refer to
 *    doc/dev/osd_internals/osdmap_versions.txt
 */
void OSDMap::decode(ceph::buffer::list& bl)
{
  auto p = bl.cbegin();
  decode(p);
}

void OSDMap::decode_classic(ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;
  __u32 n, t;
  __u16 v;
  decode(v, p);

  // base
  decode(fsid, p);
  decode(epoch, p);
  decode(created, p);
  decode(modified, p);

  if (v < 6) {
    if (v < 4) {
      int32_t max_pools = 0;
      decode(max_pools, p);
      pool_max = max_pools;
    }
    pools.clear();
    decode(n, p);
    while (n--) {
      decode(t, p);
      decode(pools[t], p);
    }
    if (v == 4) {
      decode(n, p);
      pool_max = n;
    } else if (v == 5) {
      pool_name.clear();
      decode(n, p);
      while (n--) {
	decode(t, p);
	decode(pool_name[t], p);
      }
      decode(n, p);
      pool_max = n;
    }
  } else {
    decode(pools, p);
    decode(pool_name, p);
    decode(pool_max, p);
  }
  // kludge around some old bug that zeroed out pool_max (#2307)
  if (pools.size() && pool_max < pools.rbegin()->first) {
    pool_max = pools.rbegin()->first;
  }

  decode(flags, p);

  decode(max_osd, p);
  {
    vector<uint8_t> os;
    decode(os, p);
    osd_state.resize(os.size());
    for (unsigned i = 0; i < os.size(); ++i) {
      osd_state[i] = os[i];
    }
  }
  decode(osd_weight, p);
  decode(osd_addrs->client_addrs, p);
  if (v <= 5) {
    pg_temp->clear();
    decode(n, p);
    while (n--) {
      old_pg_t opg;
      ceph::decode_raw(opg, p);
      mempool::osdmap::vector<int32_t> v;
      decode(v, p);
      pg_temp->set(pg_t(opg), v);
    }
  } else {
    decode(*pg_temp, p);
  }

  // crush
  ceph::buffer::list cbl;
  decode(cbl, p);
  auto cblp = cbl.cbegin();
  crush->decode(cblp);

  // extended
  __u16 ev = 0;
  if (v >= 5)
    decode(ev, p);
  decode(osd_addrs->hb_back_addrs, p);
  decode(osd_info, p);
  if (v < 5)
    decode(pool_name, p);

  decode(blocklist, p);
  if (ev >= 6)
    decode(osd_addrs->cluster_addrs, p);
  else
    osd_addrs->cluster_addrs.resize(osd_addrs->client_addrs.size());

  if (ev >= 7) {
    decode(cluster_snapshot_epoch, p);
    decode(cluster_snapshot, p);
  }

  if (ev >= 8) {
    decode(*osd_uuid, p);
  } else {
    osd_uuid->resize(max_osd);
  }
  if (ev >= 9)
    decode(osd_xinfo, p);
  else
    osd_xinfo.resize(max_osd);

  if (ev >= 10)
    decode(osd_addrs->hb_front_addrs, p);
  else
    osd_addrs->hb_front_addrs.resize(osd_addrs->hb_back_addrs.size());

  osd_primary_affinity.reset();

  post_decode();
}

void OSDMap::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  /**
   * Older encodings of the OSDMap had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  ceph::buffer::list crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    bl.seek(start_offset);
    decode_classic(bl);
    return;
  }
  /**
   * Since we made it past that hurdle, we can use our normal paths.
   */
  {
    DECODE_START(9, bl); // client-usable data
    // base
    decode(fsid, bl);
    decode(epoch, bl);
    decode(created, bl);
    decode(modified, bl);

    decode(pools, bl);
    decode(pool_name, bl);
    decode(pool_max, bl);

    decode(flags, bl);

    decode(max_osd, bl);
    if (struct_v >= 5) {
      decode(osd_state, bl);
    } else {
      vector<uint8_t> os;
      decode(os, bl);
      osd_state.resize(os.size());
      for (unsigned i = 0; i < os.size(); ++i) {
	osd_state[i] = os[i];
      }
    }
    decode(osd_weight, bl);
    decode(osd_addrs->client_addrs, bl);

    decode(*pg_temp, bl);
    decode(*primary_temp, bl);
    // dates back to firefly. version increased from 2 to 3 still in firefly.
    // do we really still need to keep this around? even for old clients?
    if (struct_v >= 2) {
      osd_primary_affinity.reset(new mempool::osdmap::vector<__u32>);
      decode(*osd_primary_affinity, bl);
      if (osd_primary_affinity->empty())
	osd_primary_affinity.reset();
    } else {
      osd_primary_affinity.reset();
    }

    // crush
    ceph::buffer::list cbl;
    decode(cbl, bl);
    auto cblp = cbl.cbegin();
    crush->decode(cblp);
    // added in firefly; version increased in luminous, so it affects
    // giant, hammer, infernallis, jewel, and kraken. probably should be left
    // alone until we require clients to be all luminous?
    if (struct_v >= 3) {
      decode(erasure_code_profiles, bl);
    } else {
      erasure_code_profiles.clear();
    }
    // version increased from 3 to 4 still in luminous, so same as above
    // applies.
    if (struct_v >= 4) {
      decode(pg_upmap, bl);
      decode(pg_upmap_items, bl);
    } else {
      pg_upmap.clear();
      pg_upmap_items.clear();
    }
    // again, version increased from 5 to 6 still in luminous, so above
    // applies.
    if (struct_v >= 6) {
      decode(crush_version, bl);
    }
    // version increase from 6 to 7 in mimic
    if (struct_v >= 7) {
      decode(new_removed_snaps, bl);
      decode(new_purged_snaps, bl);
    }
    // version increase from 7 to 8, 8 to 9, in nautilus.
    if (struct_v >= 9) {
      decode(last_up_change, bl);
      decode(last_in_change, bl);
    }
    if (struct_v >= 10) {
      decode(pg_upmap_primaries, bl);
    } else {
      pg_upmap_primaries.clear();
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(10, bl); // extended, osd-only data
    decode(osd_addrs->hb_back_addrs, bl);
    decode(osd_info, bl);
    decode(blocklist, bl);
    decode(osd_addrs->cluster_addrs, bl);
    decode(cluster_snapshot_epoch, bl);
    decode(cluster_snapshot, bl);
    decode(*osd_uuid, bl);
    decode(osd_xinfo, bl);
    decode(osd_addrs->hb_front_addrs, bl);
    // 
    if (struct_v >= 2) {
      decode(nearfull_ratio, bl);
      decode(full_ratio, bl);
    } else {
      nearfull_ratio = 0;
      full_ratio = 0;
    }
    if (struct_v >= 3) {
      decode(backfillfull_ratio, bl);
    } else {
      backfillfull_ratio = 0;
    }
    if (struct_v == 4) {
      string r;
      decode(r, bl);
      if (r.length())
	require_min_compat_client = ceph_release_from_name(r.c_str());
    }
    if (struct_v >= 5) {
      decode(require_min_compat_client, bl);
      decode(require_osd_release, bl);
      if (require_osd_release >= ceph_release_t::nautilus) {
	flags |= CEPH_OSDMAP_PGLOG_HARDLIMIT;
      }
      if (require_osd_release >= ceph_release_t::luminous) {
	flags &= ~(CEPH_OSDMAP_LEGACY_REQUIRE_FLAGS);
	flags |= CEPH_OSDMAP_RECOVERY_DELETES;
      }
    } else {
      if (flags & CEPH_OSDMAP_REQUIRE_LUMINOUS) {
	// only for compat with post-kraken pre-luminous test clusters
	require_osd_release = ceph_release_t::luminous;
	flags &= ~(CEPH_OSDMAP_LEGACY_REQUIRE_FLAGS);
	flags |= CEPH_OSDMAP_RECOVERY_DELETES;
      } else if (flags & CEPH_OSDMAP_REQUIRE_KRAKEN) {
	require_osd_release = ceph_release_t::kraken;
      } else if (flags & CEPH_OSDMAP_REQUIRE_JEWEL) {
	require_osd_release = ceph_release_t::jewel;
      } else {
	require_osd_release = ceph_release_t::unknown;
      }
    }
    if (struct_v >= 6) {
      decode(removed_snaps_queue, bl);
    }
    if (struct_v >= 8) {
      decode(crush_node_flags, bl);
    } else {
      crush_node_flags.clear();
    }
    if (struct_v >= 9) {
      decode(device_class_flags, bl);
    } else {
      device_class_flags.clear();
    }
    if (struct_v >= 10) {
      decode(stretch_mode_enabled, bl);
      decode(stretch_bucket_count, bl);
      decode(degraded_stretch_mode, bl);
      decode(recovering_stretch_mode, bl);
      decode(stretch_mode_bucket, bl);
    } else {
      stretch_mode_enabled = false;
      stretch_bucket_count = 0;
      degraded_stretch_mode = 0;
      recovering_stretch_mode = 0;
      stretch_mode_bucket = 0;
    }
    if (struct_v >= 11) {
      decode(range_blocklist, bl);
      calculated_ranges.clear();
      for (const auto& i : range_blocklist) {
	calculated_ranges.emplace(i.first, i.first);
      }
    }
    if (struct_v >= 12) {
      decode(allow_crimson, bl);
    }
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    decode(crc, bl);
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
      ceph::buffer::list tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << crc;
      string s = ss.str();
      throw ceph::buffer::malformed_input(s.c_str());
    }
  }

  post_decode();
}

void OSDMap::post_decode()
{
  // index pool names
  name_pool.clear();
  for (const auto &pname : pool_name) {
    name_pool[pname.second] = pname.first;
  }

  calc_num_osds();
  _calc_up_osd_features();
}

void OSDMap::dump_erasure_code_profiles(
  const mempool::osdmap::map<string,map<string,string>>& profiles,
  Formatter *f)
{
  f->open_object_section("erasure_code_profiles");
  for (const auto &profile : profiles) {
    f->open_object_section(profile.first.c_str());
    for (const auto &profm : profile.second) {
      f->dump_string(profm.first.c_str(), profm.second);
    }
    f->close_section();
  }
  f->close_section();
}

void OSDMap::dump_osds(Formatter *f) const
{
  f->open_array_section("osds");
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      dump_osd(i, f);
    }
  }
  f->close_section();
}

void OSDMap::dump_osd(int id, Formatter *f) const
{
  ceph_assert(f != nullptr);
  if (!exists(id)) {
    return;
  }

  f->open_object_section("osd_info");
  f->dump_int("osd", id);
  f->dump_stream("uuid") << get_uuid(id);
  f->dump_int("up", is_up(id));
  f->dump_int("in", is_in(id));
  f->dump_float("weight", get_weightf(id));
  f->dump_float("primary_affinity", get_primary_affinityf(id));
  get_info(id).dump(f);
  f->dump_object("public_addrs", get_addrs(id));
  f->dump_object("cluster_addrs", get_cluster_addrs(id));
  f->dump_object("heartbeat_back_addrs", get_hb_back_addrs(id));
  f->dump_object("heartbeat_front_addrs", get_hb_front_addrs(id));
  // compat
  f->dump_stream("public_addr") << get_addrs(id).get_legacy_str();
  f->dump_stream("cluster_addr") << get_cluster_addrs(id).get_legacy_str();
  f->dump_stream("heartbeat_back_addr")
    << get_hb_back_addrs(id).get_legacy_str();
  f->dump_stream("heartbeat_front_addr")
    << get_hb_front_addrs(id).get_legacy_str();

  set<string> st;
  get_state(id, st);
  f->open_array_section("state");
  for (const auto &state : st)
    f->dump_string("state", state);
  f->close_section();

  f->close_section();
}

void OSDMap::dump_pool(CephContext *cct,
		       int64_t pid,
                       const pg_pool_t &pdata,
		       ceph::Formatter *f) const
{
  std::string name("<unknown>");
  const auto &pni = pool_name.find(pid);
  if (pni != pool_name.end())
    name = pni->second;
  f->open_object_section("pool");
  f->dump_int("pool", pid);
  f->dump_string("pool_name", name);
  pdata.dump(f);
  dump_read_balance_score(cct, pid, pdata, f);
  f->close_section(); // pool
}

void OSDMap::dump_read_balance_score(CephContext *cct,
				     int64_t pid,
				     const pg_pool_t &pdata,
				     ceph::Formatter *f) const
{
  if (pdata.is_replicated()) {
    // Add rb section with values for score, optimal score, raw score
    //       // and primary_affinity average
    OSDMap::read_balance_info_t rb_info;
    auto rc = calc_read_balance_score(cct, pid, &rb_info);
    if (rc >= 0) {
      f->open_object_section("read_balance");
      f->dump_float("score_acting", rb_info.acting_adj_score);
      f->dump_float("score_stable", rb_info.adjusted_score);
      f->dump_float("optimal_score", rb_info.optimal_score);
      f->dump_float("raw_score_acting", rb_info.acting_raw_score);
      f->dump_float("raw_score_stable", rb_info.raw_score);
      f->dump_float("primary_affinity_weighted", rb_info.pa_weighted);
      f->dump_float("average_primary_affinity", rb_info.pa_avg);
      f->dump_float("average_primary_affinity_weighted", rb_info.pa_weighted_avg);
      if (rb_info.err_msg.length() > 0) {
        f->dump_string("error_message", rb_info.err_msg);
      }
      f->close_section(); // read_balance
    }
    else {
      if (rb_info.err_msg.length() > 0) {
        f->open_object_section("read_balance");
        f->dump_string("error_message", rb_info.err_msg);
        f->dump_float("score_acting", rb_info.acting_adj_score);
        f->dump_float("score_stable", rb_info.adjusted_score);
        f->close_section(); // read_balance
      }
    }
  }
}

void OSDMap::dump(Formatter *f, CephContext *cct) const
{
  f->dump_int("epoch", get_epoch());
  f->dump_stream("fsid") << get_fsid();
  f->dump_stream("created") << get_created();
  f->dump_stream("modified") << get_modified();
  f->dump_stream("last_up_change") << last_up_change;
  f->dump_stream("last_in_change") << last_in_change;
  f->dump_string("flags", get_flag_string());
  f->dump_unsigned("flags_num", flags);
  f->open_array_section("flags_set");
  set<string> flagset;
  get_flag_set(&flagset);
  for (auto p : flagset) {
    f->dump_string("flag", p);
  }
  f->close_section();
  f->dump_unsigned("crush_version", get_crush_version());
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("backfillfull_ratio", backfillfull_ratio);
  f->dump_float("nearfull_ratio", nearfull_ratio);
  f->dump_string("cluster_snapshot", get_cluster_snapshot());
  f->dump_int("pool_max", get_pool_max());
  f->dump_int("max_osd", get_max_osd());
  f->dump_string("require_min_compat_client",
		 to_string(require_min_compat_client));
  f->dump_string("min_compat_client",
		 to_string(get_min_compat_client()));
  f->dump_string("require_osd_release",
		 to_string(require_osd_release));

  f->dump_bool("allow_crimson", allow_crimson);
  f->open_array_section("pools");
  for (const auto &[pid, pdata] : pools) {
    dump_pool(cct, pid, pdata, f);
  }
  f->close_section();

  dump_osds(f);

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

  f->open_array_section("pg_upmap");
  for (auto& p : pg_upmap) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << p.first;
    f->open_array_section("osds");
    for (auto q : p.second) {
      f->dump_int("osd", q);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pg_upmap_items");
  for (auto& [pgid, mappings] : pg_upmap_items) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << pgid;
    f->open_array_section("mappings");
    for (auto& [from, to] : mappings) {
      f->open_object_section("mapping");
      f->dump_int("from", from);
      f->dump_int("to", to);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pg_upmap_primaries");
  for (const auto& [pg, osd] : pg_upmap_primaries) {
    f->open_object_section("primary_mapping");
    f->dump_stream("pgid") << pg;
    f->dump_int("primary_osd", osd);
    f->close_section();
  }
  f->close_section(); // primary_temp

  f->open_array_section("pg_temp");
  pg_temp->dump(f);
  f->close_section();

  f->open_array_section("primary_temp");
  for (const auto &pg : *primary_temp) {
    f->dump_stream("pgid") << pg.first;
    f->dump_int("osd", pg.second);
  }
  f->close_section(); // primary_temp

  f->open_object_section("blocklist");
  for (const auto &addr : blocklist) {
    stringstream ss;
    ss << addr.first;
    f->dump_stream(ss.str().c_str()) << addr.second;
  }
  f->close_section();
  f->open_object_section("range_blocklist");
  for (const auto &addr : range_blocklist) {
    stringstream ss;
    ss << addr.first;
    f->dump_stream(ss.str().c_str()) << addr.second;
  }
  f->close_section();

  dump_erasure_code_profiles(erasure_code_profiles, f);

  f->open_array_section("removed_snaps_queue");
  for (auto& p : removed_snaps_queue) {
    f->open_object_section("pool");
    f->dump_int("pool", p.first);
    f->open_array_section("snaps");
    for (auto q = p.second.begin(); q != p.second.end(); ++q) {
      f->open_object_section("interval");
      f->dump_unsigned("begin", q.get_start());
      f->dump_unsigned("length", q.get_len());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_removed_snaps");
  for (auto& p : new_removed_snaps) {
    f->open_object_section("pool");
    f->dump_int("pool", p.first);
    f->open_array_section("snaps");
    for (auto q = p.second.begin(); q != p.second.end(); ++q) {
      f->open_object_section("interval");
      f->dump_unsigned("begin", q.get_start());
      f->dump_unsigned("length", q.get_len());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_purged_snaps");
  for (auto& p : new_purged_snaps) {
    f->open_object_section("pool");
    f->dump_int("pool", p.first);
    f->open_array_section("snaps");
    for (auto q = p.second.begin(); q != p.second.end(); ++q) {
      f->open_object_section("interval");
      f->dump_unsigned("begin", q.get_start());
      f->dump_unsigned("length", q.get_len());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_object_section("crush_node_flags");
  for (auto& i : crush_node_flags) {
    string s = crush->item_exists(i.first) ? crush->get_item_name(i.first)
      : stringify(i.first);
    f->open_array_section(s.c_str());
    set<string> st;
    calc_state_set(i.second, st);
    for (auto& j : st) {
      f->dump_string("flag", j);
    }
    f->close_section();
  }
  f->close_section();
  f->open_object_section("device_class_flags");
  for (auto& i : device_class_flags) {
    const char* class_name = crush->get_class_name(i.first);
    string s = class_name ? class_name : stringify(i.first);
    f->open_array_section(s.c_str());
    set<string> st;
    calc_state_set(i.second, st);
    for (auto& j : st) {
      f->dump_string("flag", j);
    }
    f->close_section();
  }
  f->close_section();
  f->open_object_section("stretch_mode");
  {
    f->dump_bool("stretch_mode_enabled", stretch_mode_enabled);
    f->dump_unsigned("stretch_bucket_count", stretch_bucket_count);
    f->dump_unsigned("degraded_stretch_mode", degraded_stretch_mode);
    f->dump_unsigned("recovering_stretch_mode", recovering_stretch_mode);
    f->dump_int("stretch_mode_bucket", stretch_mode_bucket);
  }
  f->close_section();
}

void OSDMap::generate_test_instances(list<OSDMap*>& o)
{
  o.push_back(new OSDMap);

  CephContext *cct = new CephContext(CODE_ENVIRONMENT_UTILITY);
  o.push_back(new OSDMap);
  uuid_d fsid;
  o.back()->build_simple(cct, 1, fsid, 16);
  o.back()->created = o.back()->modified = utime_t(1, 2);  // fix timestamp
  o.back()->blocklist[entity_addr_t()] = utime_t(5, 6);
  cct->put();
}

string OSDMap::get_flag_string(unsigned f)
{
  string s;
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
  if (f & CEPH_OSDMAP_NOSNAPTRIM)
    s += ",nosnaptrim";
  if (f & CEPH_OSDMAP_SORTBITWISE)
    s += ",sortbitwise";
  if (f & CEPH_OSDMAP_REQUIRE_JEWEL)
    s += ",require_jewel_osds";
  if (f & CEPH_OSDMAP_REQUIRE_KRAKEN)
    s += ",require_kraken_osds";
  if (f & CEPH_OSDMAP_REQUIRE_LUMINOUS)
    s += ",require_luminous_osds";
  if (f & CEPH_OSDMAP_RECOVERY_DELETES)
    s += ",recovery_deletes";
  if (f & CEPH_OSDMAP_PURGED_SNAPDIRS)
    s += ",purged_snapdirs";
  if (f & CEPH_OSDMAP_PGLOG_HARDLIMIT)
    s += ",pglog_hardlimit";
  if (f & CEPH_OSDMAP_NOAUTOSCALE)
    s += ",noautoscale";
  if (s.length())
    s.erase(0, 1);
  return s;
}

string OSDMap::get_flag_string() const
{
  return get_flag_string(flags);
}

void OSDMap::print_pools(CephContext *cct, ostream& out) const
{
  for (const auto &[pid, pdata] : pools) {
    std::string name("<unknown>");
    const auto &pni = pool_name.find(pid);
    if (pni != pool_name.end())
      name = pni->second;
    char rb_score_str[32] = "";
    int rc = 0;
    read_balance_info_t rb_info;
    if (pdata.is_replicated()) {
      rc = calc_read_balance_score(cct, pid, &rb_info);
      if (rc >= 0)
        snprintf (rb_score_str, sizeof(rb_score_str),
		  " read_balance_score %.2f", rb_info.acting_adj_score);
    }

    out << "pool " << pid
	<< " '" << name
	<< "' " << pdata
	<< rb_score_str << "\n";
    if (rb_info.err_msg.length() > 0) {
      out << (rc < 0 ? " ERROR: " : " Warning: ") << rb_info.err_msg << "\n";
    }

  //TODO - print error messages here.

    for (const auto &snap : pdata.snaps)
      out << "\tsnap " << snap.second.snapid << " '" << snap.second.name << "' " << snap.second.stamp << "\n";

    if (!pdata.removed_snaps.empty())
      out << "\tremoved_snaps " << pdata.removed_snaps << "\n";
    auto p = removed_snaps_queue.find(pid);
    if (p != removed_snaps_queue.end()) {
      out << "\tremoved_snaps_queue " << p->second << "\n";
    }
  }
  out << std::endl;
}

void OSDMap::print_osds(ostream& out) const
{
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      print_osd(i, out);
    }
  }
}
void OSDMap::print_osd(int id, ostream& out) const
{
  if (!exists(id)) {
    return;
  }

  out << "osd." << id;
  out << (is_up(id) ? " up  ":" down");
  out << (is_in(id) ? " in ":" out");
  out << " weight " << get_weightf(id);
  if (get_primary_affinity(id) != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
    out << " primary_affinity " << get_primary_affinityf(id);
  }
  const osd_info_t& info(get_info(id));
  out << " " << info;
  out << " " << get_addrs(id) << " " << get_cluster_addrs(id);
  set<string> st;
  get_state(id, st);
  out << " " << st;
  if (!get_uuid(id).is_zero()) {
    out << " " << get_uuid(id);
  }
  out << "\n";
}

void OSDMap::print(CephContext *cct, ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modified " << get_modified() << "\n";

  out << "flags " << get_flag_string() << "\n";
  out << "crush_version " << get_crush_version() << "\n";
  out << "full_ratio " << full_ratio << "\n";
  out << "backfillfull_ratio " << backfillfull_ratio << "\n";
  out << "nearfull_ratio " << nearfull_ratio << "\n";
  if (require_min_compat_client != ceph_release_t::unknown) {
    out << "require_min_compat_client "
	<< require_min_compat_client << "\n";
  }
  out << "min_compat_client " << get_min_compat_client()
      << "\n";
  if (require_osd_release > ceph_release_t::unknown) {
    out << "require_osd_release " << require_osd_release
	<< "\n";
  }
  out << "stretch_mode_enabled " << (stretch_mode_enabled ? "true" : "false") << "\n";
  if (stretch_mode_enabled) {
    out << "stretch_bucket_count " << stretch_bucket_count << "\n";
    out << "degraded_stretch_mode " << degraded_stretch_mode << "\n";
    out << "recovering_stretch_mode " << recovering_stretch_mode << "\n";
    out << "stretch_mode_bucket " << stretch_mode_bucket << "\n";
  }
  if (get_cluster_snapshot().length())
    out << "cluster_snapshot " << get_cluster_snapshot() << "\n";
  if (allow_crimson) {
    out << "allow_crimson=true\n";
  }
  out << "\n";

  print_pools(cct, out);

  out << "max_osd " << get_max_osd() << "\n";
  print_osds(out);
  out << std::endl;

  for (auto& p : pg_upmap) {
    out << "pg_upmap " << p.first << " " << p.second << "\n";
  }
  for (auto& p : pg_upmap_items) {
    out << "pg_upmap_items " << p.first << " " << p.second << "\n";
  }

  for (auto& [pg, osd] : pg_upmap_primaries) {
    out << "pg_upmap_primary " << pg << " " << osd << "\n";
  }

  for (const auto& pg : *pg_temp)
    out << "pg_temp " << pg.first << " " << pg.second << "\n";

  for (const auto& pg : *primary_temp)
    out << "primary_temp " << pg.first << " " << pg.second << "\n";

  for (const auto &addr : blocklist)
    out << "blocklist " << addr.first << " expires " << addr.second << "\n";
  for (const auto &addr : range_blocklist)
    out << "range blocklist " << addr.first << " expires " << addr.second << "\n";
}

class OSDTreePlainDumper : public CrushTreeDumper::Dumper<TextTable> {
public:
  typedef CrushTreeDumper::Dumper<TextTable> Parent;

  OSDTreePlainDumper(const CrushWrapper *crush, const OSDMap *osdmap_,
		     unsigned f)
    : Parent(crush, osdmap_->get_pool_names()), osdmap(osdmap_), filter(f) { }

  bool should_dump_leaf(int i) const override {
    if (!filter) {
      return true; // normal case
    }
    if (((filter & OSDMap::DUMP_UP) && osdmap->is_up(i)) ||
	((filter & OSDMap::DUMP_DOWN) && osdmap->is_down(i)) ||
	((filter & OSDMap::DUMP_IN) && osdmap->is_in(i)) ||
	((filter & OSDMap::DUMP_OUT) && osdmap->is_out(i)) ||
        ((filter & OSDMap::DUMP_DESTROYED) && osdmap->is_destroyed(i))) {
      return true;
    }
    return false;
  }

  bool should_dump_empty_bucket() const override {
    return !filter;
  }

  void init_table(TextTable *tbl) {
    tbl->define_column("ID", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("CLASS", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("WEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("TYPE NAME", TextTable::LEFT, TextTable::LEFT);
    tbl->define_column("STATUS", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("REWEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("PRI-AFF", TextTable::LEFT, TextTable::RIGHT);
  }
  void dump(TextTable *tbl, string& bucket) {
    init_table(tbl);

    if (!bucket.empty()) {
      set_root(bucket);
      Parent::dump(tbl);
    } else {
      Parent::dump(tbl);
      for (int i = 0; i < osdmap->get_max_osd(); i++) {
	if (osdmap->exists(i) && !is_touched(i) && should_dump_leaf(i)) {
	  dump_item(CrushTreeDumper::Item(i, 0, 0, 0), tbl);
	}
      }
    }
  }

protected:
  void dump_item(const CrushTreeDumper::Item &qi, TextTable *tbl) override {
    const char *c = crush->get_item_class(qi.id);
    if (!c)
      c = "";
    *tbl << qi.id
	 << c
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
        string s;
        if (osdmap->is_up(qi.id)) {
          s = "up";
        } else if (osdmap->is_destroyed(qi.id)) {
          s = "destroyed";
        } else {
          s = "down";
        }
	*tbl << s
	     << weightf_t(osdmap->get_weightf(qi.id))
	     << weightf_t(osdmap->get_primary_affinityf(qi.id));
      }
    }
    *tbl << TextTable::endrow;
  }

private:
  const OSDMap *osdmap;
  const unsigned filter;
};

class OSDTreeFormattingDumper : public CrushTreeDumper::FormattingDumper {
public:
  typedef CrushTreeDumper::FormattingDumper Parent;

  OSDTreeFormattingDumper(const CrushWrapper *crush, const OSDMap *osdmap_,
			  unsigned f)
    : Parent(crush, osdmap_->get_pool_names()), osdmap(osdmap_), filter(f) { }

  bool should_dump_leaf(int i) const override {
    if (!filter) {
      return true; // normal case
    }
    if (((filter & OSDMap::DUMP_UP) && osdmap->is_up(i)) ||
        ((filter & OSDMap::DUMP_DOWN) && osdmap->is_down(i)) ||
        ((filter & OSDMap::DUMP_IN) && osdmap->is_in(i)) ||
        ((filter & OSDMap::DUMP_OUT) && osdmap->is_out(i)) ||
        ((filter & OSDMap::DUMP_DESTROYED) && osdmap->is_destroyed(i))) {
      return true;
    }
    return false;
  }

  bool should_dump_empty_bucket() const override {
    return !filter;
  }

  void dump(Formatter *f, string& bucket) {
    if (!bucket.empty()) {
      set_root(bucket);
      f->open_array_section("nodes");
      Parent::dump(f);
      f->close_section();
    } else {
      f->open_array_section("nodes");
      Parent::dump(f);
      f->close_section();
      f->open_array_section("stray");
      for (int i = 0; i < osdmap->get_max_osd(); i++) {
	if (osdmap->exists(i) && !is_touched(i) && should_dump_leaf(i))
	  dump_item(CrushTreeDumper::Item(i, 0, 0, 0), f);
      }
      f->close_section();
    }
  }

protected:
  void dump_item_fields(const CrushTreeDumper::Item &qi, Formatter *f) override {
    Parent::dump_item_fields(qi, f);
    if (!qi.is_bucket())
    {
      string s;
      if (osdmap->is_up(qi.id)) {
        s = "up";
      } else if (osdmap->is_destroyed(qi.id)) {
        s = "destroyed";
      } else {
        s = "down";
      }
      f->dump_unsigned("exists", (int)osdmap->exists(qi.id));
      f->dump_string("status", s);
      f->dump_float("reweight", osdmap->get_weightf(qi.id));
      f->dump_float("primary_affinity", osdmap->get_primary_affinityf(qi.id));
    }
  }

private:
  const OSDMap *osdmap;
  const unsigned filter;
};

void OSDMap::print_tree(Formatter *f, ostream *out, unsigned filter, string bucket) const
{
  if (f) {
    OSDTreeFormattingDumper(crush.get(), this, filter).dump(f, bucket);
  } else {
    ceph_assert(out);
    TextTable tbl;
    OSDTreePlainDumper(crush.get(), this, filter).dump(&tbl, bucket);
    *out << tbl;
  }
}

void OSDMap::print_summary(Formatter *f, ostream& out,
			   const string& prefix, bool extra) const
{
  if (f) {
    f->dump_int("epoch", get_epoch());
    f->dump_int("num_osds", get_num_osds());
    f->dump_int("num_up_osds", get_num_up_osds());
    f->dump_int("osd_up_since", last_up_change.to_msec() / 1000);
    f->dump_int("num_in_osds", get_num_in_osds());
    f->dump_int("osd_in_since", last_in_change.to_msec() / 1000);
    f->dump_unsigned("num_remapped_pgs", get_num_pg_temp());
  } else {
    utime_t now = ceph_clock_now();
    out << get_num_osds() << " osds: "
	<< get_num_up_osds() << " up";
    if (last_up_change != utime_t()) {
      out << " (since " << utimespan_str(now - last_up_change) << ")";
    }
    out << ", " << get_num_in_osds() << " in";
    if (last_in_change != utime_t()) {
      out << " (since " << utimespan_str(now - last_in_change) << ")";
    }
    if (extra)
      out << "; epoch: e" << get_epoch();
    if (get_num_pg_temp())
      out << "; " << get_num_pg_temp() << " remapped pgs";
    out << "\n";
    uint64_t important_flags = flags & ~CEPH_OSDMAP_SEMIHIDDEN_FLAGS;
    if (important_flags)
      out << prefix << "flags " << get_flag_string(important_flags) << "\n";
  }
}

void OSDMap::print_oneline_summary(ostream& out) const
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " total, "
      << get_num_up_osds() << " up, "
      << get_num_in_osds() << " in";
}

bool OSDMap::crush_rule_in_use(int rule_id) const
{
  for (const auto &pool : pools) {
    if (pool.second.crush_rule == rule_id)
      return true;
  }
  return false;
}

int OSDMap::validate_crush_rules(CrushWrapper *newcrush,
				 ostream *ss) const
{
  for (auto& i : pools) {
    auto& pool = i.second;
    int ruleno = pool.get_crush_rule();
    if (!newcrush->rule_exists(ruleno)) {
      *ss << "pool " << i.first << " references crush_rule " << ruleno
	  << " but it is not present";
      return -EINVAL;
    }
    if (!newcrush->rule_valid_for_pool_type(ruleno, pool.get_type())) {
      *ss << "pool " << i.first << " type does not match rule " << ruleno;
      return -EINVAL;
    }
  }
  return 0;
}

int OSDMap::build_simple_optioned(CephContext *cct, epoch_t e, uuid_d &fsid,
				  int nosd, int pg_bits, int pgp_bits,
				  bool default_pool)
{
  ldout(cct, 10) << "build_simple on " << nosd
		 << " osds" << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph_clock_now();

  if (nosd >=  0) {
    set_max_osd(nosd);
  } else {
    // count osds
    int maxosd = 0;
    const auto& conf = cct->_conf;
    vector<string> sections;
    conf.get_all_sections(sections);

    for (auto &section : sections) {
      if (section.find("osd.") != 0)
	continue;

      const char *begin = section.c_str() + 4;
      char *end = (char*)begin;
      int o = strtol(begin, &end, 10);
      if (*end != '\0')
	continue;

      if (o > cct->_conf->mon_max_osd) {
	lderr(cct) << "[osd." << o << "] in config has id > mon_max_osd " << cct->_conf->mon_max_osd << dendl;
	return -ERANGE;
      }

      if (o > maxosd)
	maxosd = o;
    }

    set_max_osd(maxosd + 1);
  }


  stringstream ss;
  int r;
  if (nosd >= 0)
    r = build_simple_crush_map(cct, *crush, nosd, &ss);
  else
    r = build_simple_crush_map_from_conf(cct, *crush, &ss);
  ceph_assert(r == 0);

  int poolbase = get_max_osd() ? get_max_osd() : 1;

  const int default_replicated_rule = crush->get_osd_pool_default_crush_replicated_rule(cct);
  ceph_assert(default_replicated_rule >= 0);

  if (default_pool) {
    // pgp_num <= pg_num
    if (pgp_bits > pg_bits)
      pgp_bits = pg_bits;

    vector<string> pool_names;
    pool_names.push_back("rbd");
    for (auto &plname : pool_names) {
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
      if (cct->_conf->osd_pool_default_flag_bulk)
        pools[pool].set_flag(pg_pool_t::FLAG_BULK);
      pools[pool].size = cct->_conf.get_val<uint64_t>("osd_pool_default_size");
      pools[pool].min_size = cct->_conf.get_osd_pool_default_min_size(
                                 pools[pool].size);
      pools[pool].crush_rule = default_replicated_rule;
      pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
      pools[pool].set_pg_num(poolbase << pg_bits);
      pools[pool].set_pgp_num(poolbase << pgp_bits);
      pools[pool].set_pg_num_target(poolbase << pg_bits);
      pools[pool].set_pgp_num_target(poolbase << pgp_bits);
      pools[pool].last_change = epoch;
      pools[pool].application_metadata.insert(
        {pg_pool_t::APPLICATION_NAME_RBD, {}});
      if (auto m = pg_pool_t::get_pg_autoscale_mode_by_name(
            cct->_conf.get_val<string>("osd_pool_default_pg_autoscale_mode"));
	  m != pg_pool_t::pg_autoscale_mode_t::UNKNOWN) {
	pools[pool].pg_autoscale_mode = m;
      } else {
	pools[pool].pg_autoscale_mode = pg_pool_t::pg_autoscale_mode_t::OFF;
      }
      pool_name[pool] = plname;
      name_pool[plname] = pool;
    }
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
  int r = get_json_str_map(cct->_conf.get_val<string>("osd_pool_default_erasure_code_profile"),
		      *ss,
		      &profile_map);
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
  crush.set_type_name(9, "zone");
  crush.set_type_name(10, "region");
  crush.set_type_name(11, "root");
  return 11;
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
  ceph_assert(r == 0);
  crush.set_item_name(rootid, "default");

  map<string,string> loc{
    {"host", "localhost"},
    {"rack", "localrack"},
    {"root", "default"}
  };
  for (int o=0; o<nosd; o++) {
    ldout(cct, 10) << " adding osd." << o << " at " << loc << dendl;
    char name[32];
    snprintf(name, sizeof(name), "osd.%d", o);
    crush.insert_item(cct, o, 1.0, name, loc);
  }

  build_simple_crush_rules(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}

int OSDMap::build_simple_crush_map_from_conf(CephContext *cct,
					     CrushWrapper& crush,
					     ostream *ss)
{
  const auto& conf = cct->_conf;

  crush.create();

  // root
  int root_type = _build_crush_types(crush);
  int rootid;
  int r = crush.add_bucket(0, 0,
			   CRUSH_HASH_DEFAULT,
			   root_type, 0, NULL, NULL, &rootid);
  ceph_assert(r == 0);
  crush.set_item_name(rootid, "default");

  // add osds
  vector<string> sections;
  conf.get_all_sections(sections);

  for (auto &section : sections) {
    if (section.find("osd.") != 0)
      continue;

    const char *begin = section.c_str() + 4;
    char *end = (char*)begin;
    int o = strtol(begin, &end, 10);
    if (*end != '\0')
      continue;

    string host, rack, row, room, dc, pool;
    vector<string> sectiontmp;
    sectiontmp.push_back("osd");
    sectiontmp.push_back(section);
    conf.get_val_from_conf_file(sectiontmp, "host", host, false);
    conf.get_val_from_conf_file(sectiontmp, "rack", rack, false);
    conf.get_val_from_conf_file(sectiontmp, "row", row, false);
    conf.get_val_from_conf_file(sectiontmp, "room", room, false);
    conf.get_val_from_conf_file(sectiontmp, "datacenter", dc, false);
    conf.get_val_from_conf_file(sectiontmp, "root", pool, false);

    if (host.length() == 0)
      host = "unknownhost";
    if (rack.length() == 0)
      rack = "unknownrack";

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
    crush.insert_item(cct, o, 1.0, section, loc);
  }

  build_simple_crush_rules(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}


int OSDMap::build_simple_crush_rules(
  CephContext *cct,
  CrushWrapper& crush,
  const string& root,
  ostream *ss)
{
  int crush_rule = crush.get_osd_pool_default_crush_replicated_rule(cct);
  string failure_domain =
    crush.get_type_name(cct->_conf->osd_crush_chooseleaf_type);

  int r;
  r = crush.add_simple_rule_at(
    "replicated_rule", root, failure_domain, "",
    "firstn", pg_pool_t::TYPE_REPLICATED,
    crush_rule, ss);
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
    for (auto &p : get_pools())
      ls.insert(p.first);
  }

  unsigned total_pg = 0;
  unsigned moved_pg = 0;
  vector<unsigned> base_by_osd(get_max_osd(), 0);
  vector<unsigned> new_by_osd(get_max_osd(), 0);
  for (int64_t pool_id : ls) {
    const pg_pool_t *pi = get_pg_pool(pool_id);
    vector<int> up, up2;
    int up_primary;
    for (unsigned ps = 0; ps < pi->get_pg_num(); ++ps) {
      pg_t pgid(ps, pool_id);
      total_pg += pi->get_size();
      pg_to_up_acting_osds(pgid, &up, &up_primary, nullptr, nullptr);
      for (int osd : up) {
	if (osd >= 0 && osd < get_max_osd())
	  ++base_by_osd[osd];
      }
      if (newmap) {
	newmap->pg_to_up_acting_osds(pgid, &up2, &up_primary, nullptr, nullptr);
	for (int osd : up2) {
	  if (osd >= 0 && osd < get_max_osd())
	    ++new_by_osd[osd];
	}
	if (pi->is_erasure()) {
	  for (unsigned i=0; i<up.size(); ++i) {
	    if (up[i] != up2[i]) {
	      ++moved_pg;
	    }
	  }
	} else if (pi->is_replicated()) {
	  for (int osd : up) {
	    if (std::find(up2.begin(), up2.end(), osd) == up2.end()) {
	      ++moved_pg;
	    }
	  }
	} else {
	  ceph_abort_msg("unhandled pool type");
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
      if (min < 0 || base_by_osd[osd] < min_base_pg) {
	min = osd;
	min_base_pg = base_by_osd[osd];
	min_new_pg = new_by_osd[osd];
      }
      if (max < 0 || base_by_osd[osd] > max_base_pg) {
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
      float percent = 0;
      if (total_pg)
        percent = (float)moved_pg * 100.0 / (float)total_pg;
      ss << "moved " << moved_pg << " / " << total_pg
	 << " (" << percent << "%)\n";
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
      ss << "max osd." << max << " with " << max_base_pg;
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

bool OSDMap::try_pg_upmap(
  CephContext *cct,
  pg_t pg,                       ///< pg to potentially remap
  const set<int>& overfull,      ///< osds we'd want to evacuate
  const vector<int>& underfull,  ///< osds to move to, in order of preference
  const vector<int>& more_underfull,  ///< more osds only slightly underfull
  vector<int> *orig,
  vector<int> *out)              ///< resulting alternative mapping
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return false;
  int rule = pool->get_crush_rule();
  if (rule < 0)
    return false;

  // make sure there is something there to remap
  bool any = false;
  for (auto osd : *orig) {
    if (overfull.count(osd)) {
      any = true;
      break;
    }
  }
  if (!any) {
    return false;
  }

  int r = crush->try_remap_rule(
    cct,
    rule,
    pool->get_size(),
    overfull, underfull,
    more_underfull,
    *orig,
    out);
  if (r < 0)
    return false;
  if (*out == *orig)
    return false;
  return true;
}


int OSDMap::balance_primaries(
  CephContext *cct,
  int64_t pid,
  OSDMap::Incremental *pending_inc,
  OSDMap& tmp_osd_map) const
{
  // This function only handles replicated pools.
  const pg_pool_t* pool = get_pg_pool(pid);
  if (! pool->is_replicated()) {
    ldout(cct, 10) << __func__ << " skipping erasure pool "
               << get_pool_name(pid) << dendl;
    return -EINVAL;
  }

  // Info to be used in verify_upmap
  int pool_size = pool->get_size();
  int crush_rule = pool->get_crush_rule();

  // Get pgs by osd (map of osd -> pgs)
  // Get primaries by osd (map of osd -> primary)
  map<uint64_t,set<pg_t>> pgs_by_osd;
  map<uint64_t,set<pg_t>> prim_pgs_by_osd;
  map<uint64_t,set<pg_t>> acting_prims_by_osd;
  pgs_by_osd = tmp_osd_map.get_pgs_by_osd(cct, pid, &prim_pgs_by_osd, &acting_prims_by_osd);

  // Construct information about the pgs and osds we will consider in new primary mappings,
  // as well as a map of all pgs and their original primary osds.
  map<pg_t,bool> prim_pgs_to_check;
  vector<uint64_t> osds_to_check;
  map<pg_t, uint64_t> orig_prims;
  for (const auto & [osd, pgs] : prim_pgs_by_osd) {
    osds_to_check.push_back(osd);
    for (const auto & pg : pgs) {
      prim_pgs_to_check.insert({pg, false});
      orig_prims.insert({pg, osd});
    }
  }

  // calculate desired primary distribution for each osd
  map<uint64_t,float> desired_prim_dist;
  int rc = 0;
  rc = calc_desired_primary_distribution(cct, pid, osds_to_check, desired_prim_dist);
  if (rc < 0) {
    ldout(cct, 10) << __func__ << " Error in calculating desired primary distribution" << dendl;
    return -EINVAL;
  }
  map<uint64_t,float> prim_dist_scores;
  float actual;
  float desired;
  for (auto osd : osds_to_check) {
    actual = prim_pgs_by_osd[osd].size();
    desired = desired_prim_dist[osd];
    prim_dist_scores[osd] = actual - desired;
    ldout(cct, 10) << __func__ << " desired distribution for osd." << osd << " " << desired << dendl;
  }

  // get read balance score before balancing
  float read_balance_score_before = 0.0;
  read_balance_info_t rb_info;
  rc = tmp_osd_map.calc_read_balance_score(cct, pid, &rb_info);
  if (rc >= 0) {
    read_balance_score_before = rb_info.adjusted_score;
  }
  if (rb_info.err_msg.length() > 0) {
    ldout(cct, 10) << __func__ << (rc < 0 ? " ERROR: " : " Warning: ") << rb_info.err_msg << dendl;
    return -EINVAL;
  }

  // get ready to swap pgs
  while (true) {
    int curr_num_changes = 0;
    vector<int> up_osds;
    vector<int> acting_osds;
    int up_primary, acting_primary;
    for (const auto & [pg, mapped] : prim_pgs_to_check) {
      // fill in the up, up primary, acting, and acting primary for the current PG
      tmp_osd_map.pg_to_up_acting_osds(pg, &up_osds, &up_primary,
	  &acting_osds, &acting_primary);
      
      // find the OSD that would make the best swap based on its score
      // We start by first testing the OSD that is currently primary for the PG we are checking.
      uint64_t curr_best_osd = up_primary;
      float prim_score = prim_dist_scores[up_primary];
      for (auto potential_osd : up_osds) {
	float potential_score = prim_dist_scores[potential_osd];
	if ((prim_score > 0) && // taking 1 pg from the prim would not make its score worse
	    (potential_score < 0) && // adding 1 pg to the potential would not make its score worse
	    ((prim_score - potential_score) > 1) && // swapping a pg would not just keep the scores the same
	    (desired_prim_dist[potential_osd] > 0)) // the potential is not off limits (the primary affinity is above 0)
	{
	  curr_best_osd = potential_osd;
	}
      }

      // Make the swap only if:
      //    1. The swap is legal
      //    2. The balancer has chosen a new primary
      auto legal_swap = crush->verify_upmap(cct,
                                 crush_rule,
                                 pool_size,
                                 {(int)curr_best_osd});
      if (legal_swap >= 0 &&
	  ((int)curr_best_osd != up_primary)) {
	// Update prim_dist_scores
	prim_dist_scores[curr_best_osd] += 1;
	prim_dist_scores[up_primary] -= 1;

	// Update the mappings
	tmp_osd_map.pg_upmap_primaries[pg] = curr_best_osd;
	if (curr_best_osd == orig_prims[pg]) {
          pending_inc->new_pg_upmap_primary.erase(pg);
          prim_pgs_to_check[pg] = false;
	} else {
	  pending_inc->new_pg_upmap_primary[pg] = curr_best_osd;
          prim_pgs_to_check[pg] = true; // mark that this pg changed mappings
	}

	curr_num_changes++;
      }
      ldout(cct, 20) << __func__ << " curr_num_changes: " << curr_num_changes << dendl;
    }
    // If there are no changes after one pass through the pgs, then no further optimizations can be made.
    if (curr_num_changes == 0) {
      ldout(cct, 20) << __func__ << " curr_num_changes is 0; no further optimizations can be made." << dendl;
      break;
    }
  }

  // get read balance score after balancing
  float read_balance_score_after = 0.0;
  rc = tmp_osd_map.calc_read_balance_score(cct, pid, &rb_info);
  if (rc >= 0) {
    read_balance_score_after = rb_info.adjusted_score;
  }
  if (rb_info.err_msg.length() > 0) {
    ldout(cct, 10) << __func__ << (rc < 0 ? " ERROR: " : " Warning: ") << rb_info.err_msg << dendl;
    return -EINVAL;
  }

  // Tally total number of changes
  int num_changes = 0;
  if (read_balance_score_after < read_balance_score_before) {
    for (auto [pg, mapped] : prim_pgs_to_check) {
      if (mapped) {
        num_changes++;
      }
    }
  }

  ldout(cct, 10) << __func__ << " num_changes " << num_changes << dendl;
  return num_changes;
}

int OSDMap::calc_desired_primary_distribution(
  CephContext *cct,
  int64_t pid,
  const vector<uint64_t> &osds,
  std::map<uint64_t, float>& desired_primary_distribution) const
{
  // will return a perfect distribution of floats
  // without calculating the floor of each value
  //
  // This function only handles replicated pools.
  const pg_pool_t* pool = get_pg_pool(pid);
  if (pool->is_replicated()) {
    ldout(cct, 20) << __func__ << " calculating distribution for replicated pool "
                   << get_pool_name(pid) << dendl;
    uint64_t replica_count = pool->get_size();
    
    map<uint64_t,set<pg_t>> pgs_by_osd;
    pgs_by_osd = get_pgs_by_osd(cct, pid);

    // First calculate the distribution using primary affinity and tally up the sum
    auto distribution_sum = 0.0;
    for (const auto & osd : osds) {
      float osd_primary_count = ((float)pgs_by_osd[osd].size() / (float)replica_count) * get_primary_affinityf(osd);
      desired_primary_distribution.insert({osd, osd_primary_count});
      distribution_sum += osd_primary_count;
    }
    if (distribution_sum <= 0) {
      ldout(cct, 10) << __func__ << " Unable to calculate primary distribution, likely because primary affinity is"
	                    << " set to 0 on all OSDs." << dendl;
      return -EINVAL;
    }

    // Then, stretch the value (necessary when primary affinity is smaller than 1)
    float factor = (float)pool->get_pg_num() / (float)distribution_sum;
    float distribution_sum_desired = 0.0;

    ceph_assert(factor >= 1.0);
    for (const auto & [osd, osd_primary_count] : desired_primary_distribution) {
      desired_primary_distribution[osd] *= factor;
      distribution_sum_desired += desired_primary_distribution[osd];
    }
    ceph_assert(fabs(distribution_sum_desired - pool->get_pg_num()) < 0.01);
  } else {
    ldout(cct, 10) << __func__ <<" skipping erasure pool "
                   << get_pool_name(pid) << dendl;
    return -EINVAL;
  }

  return 0;
}

int OSDMap::calc_pg_upmaps(
  CephContext *cct,
  uint32_t max_deviation,
  int max,
  const set<int64_t>& only_pools,
  OSDMap::Incremental *pending_inc,
  std::random_device::result_type *p_seed)
{
  ldout(cct, 10) << __func__ << " pools " << only_pools << dendl;
  OSDMap tmp_osd_map;
  // Can't be less than 1 pg
  if (max_deviation < 1)
    max_deviation = 1;
  tmp_osd_map.deepish_copy_from(*this);
  int num_changed = 0;
  map<int,set<pg_t>> pgs_by_osd;
  int total_pgs = 0;
  float osd_weight_total = 0;
  map<int,float> osd_weight;

  if (max <= 0) {
    lderr(cct) << __func__ << " abort due to max <= 0" << dendl;
    return 0;
  }

  osd_weight_total = build_pool_pgs_info(cct, only_pools, tmp_osd_map, 
                                         total_pgs, pgs_by_osd, osd_weight);
  if (osd_weight_total == 0) {
    lderr(cct) << __func__ << " abort due to osd_weight_total == 0" << dendl;
    return 0;
  }

  float pgs_per_weight = total_pgs / osd_weight_total;
  ldout(cct, 10) << " osd_weight_total " << osd_weight_total << dendl;
  ldout(cct, 10) << " pgs_per_weight " << pgs_per_weight << dendl;

  float stddev = 0;
  map<int,float> osd_deviation;       // osd, deviation(pgs)
  multimap<float,int> deviation_osd;  // deviation(pgs), osd
  float cur_max_deviation = calc_deviations(cct, pgs_by_osd, osd_weight, pgs_per_weight,
				      	    osd_deviation, deviation_osd, stddev);

  ldout(cct, 20) << " stdev " << stddev << " max_deviation " << cur_max_deviation << dendl;
  if (cur_max_deviation <= max_deviation) {
    ldout(cct, 10) << __func__ << " distribution is almost perfect"
                   << dendl;
    return 0;
  }

  bool skip_overfull = false;
  auto aggressive =
    cct->_conf.get_val<bool>("osd_calc_pg_upmaps_aggressively");
  auto fast_aggressive = aggressive &&
    cct->_conf.get_val<bool>("osd_calc_pg_upmaps_aggressively_fast");
  auto local_fallback_retries =
    cct->_conf.get_val<uint64_t>("osd_calc_pg_upmaps_local_fallback_retries");
    
  while (max--) {
    ldout(cct, 30) << "Top of loop #" << max+1 << dendl;
    // build overfull and underfull
    set<int> overfull;
    set<int> more_overfull;
    bool using_more_overfull = false;
    vector<int> underfull;
    vector<int> more_underfull;
    fill_overfull_underfull(cct, deviation_osd, max_deviation, 
    			    overfull, more_overfull,
			    underfull, more_underfull);

    if (underfull.empty() && overfull.empty()) {
      ldout(cct, 20) << __func__ << " failed to build overfull and underfull" << dendl;
      break;
    }
    if (overfull.empty() && !underfull.empty()) {
      ldout(cct, 20) << __func__ << " Using more_overfull since we still have underfull" << dendl;
      overfull = more_overfull;
      using_more_overfull = true;
    }

    ldout(cct, 10) << " overfull " << overfull
                   << " underfull " << underfull
                   << dendl;
    set<pg_t> to_skip;
    uint64_t local_fallback_retried = 0;

    // Used to prevent some of the unsuccessful loop iterations (save runtime)
    // If we can't find a change per OSD we skip further iterations for this OSD
    uint n_changes = 0, prev_n_changes = 0;
    set<int> osd_to_skip;

  retry:

    set<pg_t> to_unmap;
    map<pg_t, mempool::osdmap::vector<pair<int32_t,int32_t>>> to_upmap;
    auto temp_pgs_by_osd = pgs_by_osd;
    // always start with fullest, break if we find any changes to make
    for (auto p = deviation_osd.rbegin(); p != deviation_osd.rend(); ++p) {
      if (skip_overfull && !underfull.empty()) {
        ldout(cct, 10) << " skipping overfull " << dendl;
        break; // fall through to check underfull
      }
      int osd = p->second;
      float deviation = p->first;
      if (fast_aggressive && osd_to_skip.count(osd)) {
	ldout(cct, 20) << " Fast aggressive mode: skipping osd " << osd 
	               << " osd_to_skip size = " << osd_to_skip.size() << dendl;
	continue;
      }

      if (deviation < 0) {
        ldout(cct, 10) << " hitting underfull osds now"
                       << " when trying to remap overfull osds"
                       << dendl;
        break;
      }
      float target = osd_weight[osd] * pgs_per_weight;
      ldout(cct, 10) << " Overfull search osd." << osd
                       << " target " << target
                       << " deviation " << deviation
		       << dendl;
      ceph_assert(target > 0);
      if (!using_more_overfull && deviation <= max_deviation) {
	ldout(cct, 10) << " osd." << osd
                       << " target " << target
                       << " deviation " << deviation
                       << " < max deviation " << max_deviation
                       << dendl;
	break;
      }

      vector<pg_t> pgs;
      pgs.reserve(pgs_by_osd[osd].size());
      for (auto& pg : pgs_by_osd[osd]) {
        if (to_skip.count(pg))
          continue;
        pgs.push_back(pg);
      }
      if (aggressive) {
        // shuffle PG list so they all get equal (in)attention
        std::shuffle(pgs.begin(), pgs.end(), get_random_engine(cct, p_seed));
      }
      // look for remaps we can un-remap
      if (try_drop_remap_overfull(cct, pgs, tmp_osd_map, osd,
				  temp_pgs_by_osd, to_unmap, to_upmap))
	goto test_change;

      // try upmap
      for (auto pg : pgs) {
        auto temp_it = tmp_osd_map.pg_upmap.find(pg);
        if (temp_it != tmp_osd_map.pg_upmap.end()) {
          // leave pg_upmap alone
          // it must be specified by admin since balancer does not
          // support pg_upmap yet
	  ldout(cct, 10) << " " << pg << " already has pg_upmap "
                         << temp_it->second << ", skipping"
                         << dendl;
	  continue;
	}
        auto pg_pool_size = tmp_osd_map.get_pg_pool_size(pg);
        mempool::osdmap::vector<pair<int32_t,int32_t>> new_upmap_items;
        set<int> existing;
        auto it = tmp_osd_map.pg_upmap_items.find(pg);
        if (it != tmp_osd_map.pg_upmap_items.end()) {
	  auto& um_items = it->second;
          if (um_items.size() >= (size_t)pg_pool_size) {
            ldout(cct, 10) << " " << pg << " already has full-size pg_upmap_items "
                           << um_items << ", skipping"
                           << dendl;
            continue;
          } else {
            ldout(cct, 10) << " " << pg << " already has pg_upmap_items "
                           << um_items
                           << dendl;
            new_upmap_items = um_items;
            // build existing too (for dedup)
            for (auto [um_from, um_to] : um_items) {
              existing.insert(um_from);
              existing.insert(um_to);
            }
	  }
          // fall through
          // to see if we can append more remapping pairs
	}
	ldout(cct, 10) << " trying " << pg << dendl;
        vector<int> raw, orig, out;
        tmp_osd_map.pg_to_raw_upmap(pg, &raw, &orig); // including existing upmaps too
	if (!try_pg_upmap(cct, pg, overfull, underfull, more_underfull, &orig, &out)) {
	  continue;
	}
	ldout(cct, 10) << " " << pg << " " << orig << " -> " << out << dendl;
	if (orig.size() != out.size()) {
	  continue;
	}
	ceph_assert(orig != out);
	int pos = find_best_remap(cct, orig, out, existing, osd_deviation);
	if (pos != -1) {
          // append new remapping pairs slowly
          // This way we can make sure that each tiny change will
          // definitely make distribution of PGs converging to
          // the perfect status.
	  add_remap_pair(cct, orig[pos], out[pos], pg, (size_t)pg_pool_size, 
	  		 osd, existing, temp_pgs_by_osd,
			 new_upmap_items, to_upmap);
          goto test_change;
	}
      }
      if (fast_aggressive) {
	if (prev_n_changes == n_changes) {  // no changes for prev OSD
		osd_to_skip.insert(osd);
	}
	else {
		prev_n_changes = n_changes;
	}
      }

    }

    ceph_assert(!(to_unmap.size() || to_upmap.size()));
    ldout(cct, 10) << " failed to find any changes for overfull osds"
                   << dendl;
    for (auto& [deviation, osd] : deviation_osd) {
      if (std::find(underfull.begin(), underfull.end(), osd) ==
                    underfull.end())
        break;
      float target = osd_weight[osd] * pgs_per_weight;
      ceph_assert(target > 0);
      if (fabsf(deviation) < max_deviation) {
        // respect max_deviation too
        ldout(cct, 10) << " osd." << osd
                       << " target " << target
                       << " deviation " << deviation
                       << " -> absolute " << fabsf(deviation)
                       << " < max " << max_deviation
                       << dendl;
        break;
      }
      // look for remaps we can un-remap
      candidates_t candidates = build_candidates(cct, tmp_osd_map, to_skip,
      						 only_pools, aggressive, p_seed);
      if (try_drop_remap_underfull(cct, candidates, osd, temp_pgs_by_osd,
          to_unmap, to_upmap)) {
	goto test_change;
      }
    }

    ceph_assert(!(to_unmap.size() || to_upmap.size()));
    ldout(cct, 10) << " failed to find any changes for underfull osds"
                   << dendl;
    if (!aggressive) {
      ldout(cct, 10) << " break due to aggressive mode not enabled" << dendl;
      break;
    } else if (!skip_overfull) {
      // safe to quit because below here we know
      // we've done checking both overfull and underfull osds..
      ldout(cct, 10) << " break due to not being able to find any"
                     << " further optimizations"
                     << dendl;
      break;
    }
    // restart with fullest and do exhaustive searching
    skip_overfull = false;
    continue;

  test_change:

    // test change, apply if change is good
    ceph_assert(to_unmap.size() || to_upmap.size());
    float new_stddev = 0;
    map<int,float> temp_osd_deviation;
    multimap<float,int> temp_deviation_osd;
    float cur_max_deviation = calc_deviations(cct, temp_pgs_by_osd, osd_weight,
    					      pgs_per_weight, temp_osd_deviation,
					      temp_deviation_osd, new_stddev);
    ldout(cct, 10) << " stddev " << stddev << " -> " << new_stddev << dendl;
    if (new_stddev >= stddev) {
      if (!aggressive) {
        ldout(cct, 10) << " break because stddev is not decreasing"
                       << " and aggressive mode is not enabled"
                       << dendl;
        break;
      }
      local_fallback_retried++;
      if (local_fallback_retried >= local_fallback_retries) {
        // does not make progress
        // flip *skip_overfull* so both overfull and underfull
        // get equal (in)attention
        skip_overfull = !skip_overfull;
        ldout(cct, 10) << " hit local_fallback_retries "
                       << local_fallback_retries
                       << dendl;
        continue;
      }
      for (auto& i : to_unmap)
        to_skip.insert(i);
      for (auto& i : to_upmap)
        to_skip.insert(i.first);
      ldout(cct, 20) << " local_fallback_retried " << local_fallback_retried
                     << " to_skip " << to_skip
                     << dendl;
      goto retry;
    }

    // ready to go
    ceph_assert(new_stddev < stddev);
    stddev = new_stddev;
    pgs_by_osd = temp_pgs_by_osd;
    osd_deviation = temp_osd_deviation;
    deviation_osd = temp_deviation_osd;
    n_changes++;


    num_changed += pack_upmap_results(cct, to_unmap, to_upmap, tmp_osd_map, pending_inc);

    ldout(cct, 20) << " stdev " << stddev << " max_deviation " << cur_max_deviation << dendl;
    if (cur_max_deviation <= max_deviation) {
      ldout(cct, 10) << __func__ << " Optimization plan is almost perfect"
                     << dendl;
      break;
    }
  }
  ldout(cct, 10) << " num_changed = " << num_changed << dendl;
  return num_changed;
}

map<uint64_t,set<pg_t>> OSDMap::get_pgs_by_osd(
    CephContext *cct,
    int64_t pid,
    map<uint64_t, set<pg_t>> *p_primaries_by_osd,
    map<uint64_t, set<pg_t>> *p_acting_primaries_by_osd) const
{
  // Set up the OSDMap
  OSDMap tmp_osd_map;
  tmp_osd_map.deepish_copy_from(*this);

  // Get the pool from the provided pool id
  const pg_pool_t* pool = get_pg_pool(pid);

  // build array of pgs from the pool
  map<uint64_t,set<pg_t>> pgs_by_osd;
  for (unsigned ps = 0; ps < pool->get_pg_num(); ++ps) {
    pg_t pg(ps, pid);
    vector<int> up;
    int primary;
    int acting_prim;
    tmp_osd_map.pg_to_up_acting_osds(pg, &up, &primary, nullptr, &acting_prim);
    if (cct != nullptr)
      ldout(cct, 20) << __func__ << " " << pg
                     << " up " << up
		     << " primary " << primary
		     << " acting_primary " << acting_prim
		     << dendl;

    if (!up.empty()) {  // up can be empty is test generated files
			// in this case, we return empty result
      for (auto osd : up) {
        if (osd != CRUSH_ITEM_NONE)
          pgs_by_osd[osd].insert(pg);
      }
      if (p_primaries_by_osd != nullptr) {
        if (primary != CRUSH_ITEM_NONE)
	  (*p_primaries_by_osd)[primary].insert(pg);
      }
      if (p_acting_primaries_by_osd != nullptr) {
        if (acting_prim != CRUSH_ITEM_NONE)
	  (*p_acting_primaries_by_osd)[acting_prim].insert(pg);
      }
    }
  }
  return pgs_by_osd;
}

float OSDMap::get_osds_weight(
  CephContext *cct,
  const OSDMap& tmp_osd_map,
  int64_t pid,
  map<int,float>& osds_weight) const
{
  map<int,float> pmap;
  ceph_assert(pools.count(pid));
  int ruleno = pools.at(pid).get_crush_rule();
  tmp_osd_map.crush->get_rule_weight_osd_map(ruleno, &pmap);
    ldout(cct,20) << __func__ << " pool " << pid
                  << " ruleno " << ruleno
                  << " weight-map " << pmap
                  << dendl;
  float osds_weight_total = 0;
  for (auto [oid, oweight] : pmap) {
    auto adjusted_weight = tmp_osd_map.get_weightf(oid) * oweight;
    if (adjusted_weight != 0) {
      osds_weight[oid] += adjusted_weight;
      osds_weight_total += adjusted_weight;
    }
  }
  return osds_weight_total;
}

float OSDMap::build_pool_pgs_info (
  CephContext *cct,
  const std::set<int64_t>& only_pools,        ///< [optional] restrict to pool
  const OSDMap& tmp_osd_map,
  int& total_pgs,
  map<int,set<pg_t>>& pgs_by_osd,
  map<int,float>& osds_weight)
{
  //
  // This function builds some data structures that are used by calc_pg_upmaps.
  // Specifically it builds pgs_by_osd and osd_weight maps, updates total_pgs 
  // and returns the osd_weight_total
  //
  float osds_weight_total = 0.0;
  for (auto& [pid, pdata] : pools) {
    if (!only_pools.empty() && !only_pools.count(pid))
      continue;
    for (unsigned ps = 0; ps < pdata.get_pg_num(); ++ps) {
      pg_t pg(ps, pid);
      vector<int> up;
      tmp_osd_map.pg_to_up_acting_osds(pg, &up, nullptr, nullptr, nullptr);
      ldout(cct, 20) << __func__ << " " << pg << " up " << up << dendl;
      for (auto osd : up) {
        if (osd != CRUSH_ITEM_NONE)
	  pgs_by_osd[osd].insert(pg);
      }
    }
    total_pgs += pdata.get_size() * pdata.get_pg_num();

    osds_weight_total = get_osds_weight(cct, tmp_osd_map, pid, osds_weight);
  }
  for (auto& [oid, oweight] : osds_weight) {
    int pgs = 0;
    auto p = pgs_by_osd.find(oid);
    if (p != pgs_by_osd.end())
      pgs = p->second.size();
    else
      pgs_by_osd.emplace(oid, set<pg_t>());
    ldout(cct, 20) << " osd." << oid << " weight " << oweight
		   << " pgs " << pgs << dendl;
  }
  return osds_weight_total;

} // return total weight of all OSDs

float OSDMap::calc_deviations (
  CephContext *cct,
  const map<int,set<pg_t>>& pgs_by_osd,
  const map<int,float>& osd_weight,
  float pgs_per_weight,
  map<int,float>& osd_deviation,
  multimap<float,int>& deviation_osd,
  float& stddev)  // return current max deviation
{
  //
  // This function calculates the 2 maps osd_deviation and deviation_osd which 
  // hold the deviation between the current number of PGs which map to an OSD 
  // and the optimal number. Ot also calculates the stddev of the deviations and
  // returns the current max deviation. 
  // NOTE - the calculation is not exactly stddev it is actually sttdev^2 but as
  //        long as it is monotonic with stddev (and it is), it is sufficient for
  //        the balancer code.
  //
  float cur_max_deviation = 0.0;
  stddev = 0.0;
  for (auto& [oid, opgs] : pgs_by_osd) {
    // make sure osd is still there (belongs to this crush-tree)
    ceph_assert(osd_weight.count(oid));
    float target = osd_weight.at(oid) * pgs_per_weight;
    float deviation = (float)opgs.size() - target;
    ldout(cct, 20) << " osd." << oid
                   << "\tpgs " << opgs.size()
                   << "\ttarget " << target
                   << "\tdeviation " << deviation
                   << dendl;
    osd_deviation[oid] = deviation;
    deviation_osd.insert(make_pair(deviation, oid));
    stddev += deviation * deviation;
    if (fabsf(deviation) > cur_max_deviation)
      cur_max_deviation = fabsf(deviation);
  }
  return cur_max_deviation;
}

void OSDMap::fill_overfull_underfull (
  CephContext *cct,
  const std::multimap<float,int>& deviation_osd,
  int max_deviation,
  std::set<int>& overfull,
  std::set<int>& more_overfull,
  std::vector<int>& underfull,
  std::vector<int>& more_underfull)
{
  // 
  // This function just fills the overfull and underfull data structures for the
  // use of calc_pg_upmaps
  //
  for (auto i = deviation_osd.rbegin(); i != deviation_osd.rend(); i++) {
    auto& odev = i->first;
    auto& oid = i->second;
    ldout(cct, 30) << " check " << odev << " <= " << max_deviation << dendl;
    if (odev <= 0)
      break;
    if (odev > max_deviation) {
      ldout(cct, 30) << " add overfull osd." << oid << dendl;
      overfull.insert(oid);
    } else {
      more_overfull.insert(oid);
    }
  }

  for (auto i = deviation_osd.begin(); i != deviation_osd.end(); i++) {
    auto& odev = i->first;
    auto& oid = i->second;
    ldout(cct, 30) << " check " << odev << " >= " << -(int)max_deviation << dendl;
    if (odev >= 0)
      break;
    if (odev < -(int)max_deviation) {
      ldout(cct, 30) << " add underfull osd." << oid << dendl;
      underfull.push_back(oid);
    } else {
      more_underfull.push_back(oid);
    }
  }
}

int OSDMap::pack_upmap_results(
  CephContext *cct,
  const std::set<pg_t>& to_unmap,
  const std::map<pg_t, mempool::osdmap::vector<std::pair<int, int>>>& to_upmap,
  OSDMap& tmp_osd_map,
  OSDMap::Incremental *pending_inc) 
{
  //
  // This function takes the input from the local variables to_unmap and to_upmap
  // and updates tmp_osd_map (so that another iteration can run) and pending_inc
  // (so that the results are visible outside calc_pg_upmaps)
  //
  int num_changed = 0;
  for (auto& i : to_unmap) {
    ldout(cct, 10) << " unmap pg " << i << dendl;
    ceph_assert(tmp_osd_map.pg_upmap_items.count(i));
    tmp_osd_map.pg_upmap_items.erase(i);
    pending_inc->old_pg_upmap_items.insert(i);
    ++num_changed;
  }
  for (auto& [pg, um_items] : to_upmap) {
    ldout(cct, 10) << " upmap pg " << pg
                   << " new pg_upmap_items " << um_items
                   << dendl;
    tmp_osd_map.pg_upmap_items[pg] = um_items;
    pending_inc->new_pg_upmap_items[pg] = um_items;
    ++num_changed;
  }

  return num_changed; 
}

std::default_random_engine OSDMap::get_random_engine(
  CephContext *cct,
  std::random_device::result_type *p_seed)
{
  //
  // This function creates a random_engine to be used for shuffling.
  // When p_seed == nullptr it generates random engine with a seed from /dev/random
  // when p_seed is not null, it uses (*p_seed + seed_set) as the seed and 
  // increments seed_set. This is used in order to craete regression test without 
  // random effect on the results. 
  //
  static std::random_device::result_type seed_set = 0;
  std::random_device::result_type seed;
  if (p_seed == nullptr) {
    std::random_device rd;
    seed = rd();
  }
  else {
    seed = *p_seed + seed_set;
    ldout(cct, 30) << " Starting random engine with seed " 
		   << seed << dendl;
    seed_set++;
  }
  return std::default_random_engine{seed};
}

bool OSDMap::try_drop_remap_overfull(
  CephContext *cct,
  const std::vector<pg_t>& pgs,
  const OSDMap& tmp_osd_map,
  int osd,
  map<int,std::set<pg_t>>& temp_pgs_by_osd,
  set<pg_t>& to_unmap,
  map<pg_t, mempool::osdmap::vector<pair<int32_t,int32_t>>>& to_upmap)
{
  //
  // This function tries to drop existimg upmap items which map data to overfull 
  // OSDs. It updates temp_pgs_by_osd, to_unmap and to_upmap and rerturns true 
  // if it found an item that can be dropped, false if not. 
  //
  for (auto pg : pgs) {
    auto p = tmp_osd_map.pg_upmap_items.find(pg);
    if (p == tmp_osd_map.pg_upmap_items.end())
      continue;
    mempool::osdmap::vector<pair<int32_t,int32_t>> new_upmap_items;
    auto& pg_upmap_items = p->second;
    for (auto um_pair : pg_upmap_items) {
      auto& um_from = um_pair.first;
      auto& um_to = um_pair.second;
      if (um_to == osd) {
        ldout(cct, 10) << " will try dropping existing"
                       << " remapping pair "
                       << um_from << " -> " << um_to
                       << " which remapped " << pg
                       << " into overfull osd." << osd
                       << dendl;
        temp_pgs_by_osd[um_to].erase(pg);
        temp_pgs_by_osd[um_from].insert(pg);
        } else {
          new_upmap_items.push_back(um_pair);
        }
    }
    if (new_upmap_items.empty()) {
      // drop whole item
      ldout(cct, 10) << " existing pg_upmap_items " << pg_upmap_items
                     << " remapped " << pg << " into overfull osd." << osd
                     << ", will try cancelling it entirely"
                     << dendl;
      to_unmap.insert(pg);
      return true;
    } else if (new_upmap_items.size() != pg_upmap_items.size()) {
      // drop single remapping pair, updating
      ceph_assert(new_upmap_items.size() < pg_upmap_items.size());
      ldout(cct, 10) << " existing pg_upmap_items " << pg_upmap_items
                     << " remapped " << pg << " into overfull osd." << osd
                     << ", new_pg_upmap_items now " << new_upmap_items
                     << dendl;
      to_upmap[pg] = new_upmap_items;
      return true;
    }
  }
  return false;
}

bool OSDMap::try_drop_remap_underfull(
    CephContext *cct,
    const candidates_t& candidates,
    int osd,
    map<int,std::set<pg_t>>& temp_pgs_by_osd,
    set<pg_t>& to_unmap,
    map<pg_t, mempool::osdmap::vector<std::pair<int32_t,int32_t>>>& to_upmap)
{
  // 
  // This function tries to drop existimg upmap items which map data from underfull
  // OSDs. It updates temp_pgs_by_osd, to_unmap and to_upmap and rerturns true 
  // if it found an item that can be dropped, false if not. 
  //
  for (auto& [pg, um_pairs] : candidates) {
    mempool::osdmap::vector<pair<int32_t,int32_t>> new_upmap_items;
    for (auto& ump : um_pairs) {
      auto& um_from = ump.first;
      auto& um_to = ump.second;
      if (um_from == osd) {
        ldout(cct, 10) << " will try dropping existing"
                       << " remapping pair "
                       << um_from << " -> " << um_to
                       << " which remapped " << pg
                       << " out from underfull osd." << osd
                       << dendl;
        temp_pgs_by_osd[um_to].erase(pg);
        temp_pgs_by_osd[um_from].insert(pg);
      } else {
        new_upmap_items.push_back(ump);
      }
    }
    if (new_upmap_items.empty()) {
      // drop whole item
      ldout(cct, 10) << " existing pg_upmap_items " << um_pairs
                     << " remapped " << pg
                     << " out from underfull osd." << osd
                     << ", will try cancelling it entirely"
                     << dendl;
      to_unmap.insert(pg);
      return true;
    } else if (new_upmap_items.size() != um_pairs.size()) {
      // drop single remapping pair, updating
      ceph_assert(new_upmap_items.size() < um_pairs.size());
      ldout(cct, 10) << " existing pg_upmap_items " << um_pairs
                     << " remapped " << pg
                     << " out from underfull osd." << osd
                     << ", new_pg_upmap_items now " << new_upmap_items
                     << dendl;
      to_upmap[pg] = new_upmap_items;
      return true;
    }
  }
  return false;
}

void OSDMap::add_remap_pair(
  CephContext *cct,
  int orig,
  int out, 
  pg_t pg,
  size_t pg_pool_size,
  int osd,
  set<int>& existing,
  map<int,set<pg_t>>& temp_pgs_by_osd,
  mempool::osdmap::vector<pair<int32_t,int32_t>> new_upmap_items,
  map<pg_t, mempool::osdmap::vector<pair<int32_t,int32_t>>>& to_upmap) 
{
  //
  // add a single remap pair (in pg <pg> remap osd from <orig> to <out>) to all 
  // the relevant data structures
  //
  ldout(cct, 10) << " will try adding new remapping pair "
                 << orig << " -> " << out << " for " << pg
		 << (orig != osd ? " NOT selected osd" : "")
                 << dendl;
  existing.insert(orig);
  existing.insert(out);
  temp_pgs_by_osd[orig].erase(pg);
  temp_pgs_by_osd[out].insert(pg);
  ceph_assert(new_upmap_items.size() < pg_pool_size);
  new_upmap_items.push_back(make_pair(orig, out));
  // append new remapping pairs slowly
  // This way we can make sure that each tiny change will
  // definitely make distribution of PGs converging to
  // the perfect status.
  to_upmap[pg] = new_upmap_items;

}

int OSDMap::find_best_remap (
  CephContext *cct,
  const vector<int>& orig,
  const vector<int>& out,
  const set<int>& existing,
  const map<int,float> osd_deviation) 
{
  //
  // Find the best remap from the suggestions in orig and out - the best remap 
  // is the one which maps from the OSD with the largest deviatoion (from the 
  // OSDs which are part of orig)
  //
  int best_pos = -1;
  float max_dev = 0;
  for (unsigned i = 0; i < out.size(); ++i) {
    if (orig[i] == out[i])
      continue; // skip invalid remappings
    if (existing.count(orig[i]) || existing.count(out[i]))
      continue; // we want new remappings only!
    if (osd_deviation.at(orig[i]) > max_dev) {
      max_dev = osd_deviation.at(orig[i]);
      best_pos = i;
      ldout(cct, 30) << "Max osd." << orig[i] << " pos " << i << " dev " << osd_deviation.at(orig[i]) << dendl;
    }
  }
  return best_pos;
}

OSDMap::candidates_t OSDMap::build_candidates(
  CephContext *cct,
  const OSDMap& tmp_osd_map,
  const set<pg_t> to_skip,
  const set<int64_t>& only_pools,
  bool aggressive,
  std::random_device::result_type *p_seed)
{
  //
  // build the candidates data structure
  //
  candidates_t candidates;
  candidates.reserve(tmp_osd_map.pg_upmap_items.size());
  for (auto& [pg, um_pair] : tmp_osd_map.pg_upmap_items) {
    if (to_skip.count(pg))
      continue;
    if (!only_pools.empty() && !only_pools.count(pg.pool()))
      continue;
    candidates.push_back(make_pair(pg, um_pair));
  }
  if (aggressive) {
    // shuffle candidates so they all get equal (in)attention
    std::shuffle(candidates.begin(), candidates.end(), get_random_engine(cct, p_seed));
  }
  return candidates;
}

// return -1 if all PGs are OK, else the first PG which includes only zero PA OSDs
int64_t OSDMap::has_zero_pa_pgs(CephContext *cct, int64_t pool_id) const
{
  const pg_pool_t* pool = get_pg_pool(pool_id);
  for (unsigned ps = 0; ps < pool->get_pg_num(); ++ps) {
    pg_t pg(ps, pool_id);
    vector<int> acting;
    pg_to_up_acting_osds(pg, nullptr, nullptr, &acting, nullptr);
    if (cct != nullptr) {
      ldout(cct, 30) << __func__ << " " << pg << " acting " << acting << dendl;
    }
    bool pg_zero_pa = true;
    for (auto osd : acting) {
      if (get_primary_affinityf(osd) != 0) {
        pg_zero_pa = false;
        break;
      }
    }
    if (pg_zero_pa) {
      if (cct != nullptr) {
        ldout(cct, 20) << __func__ << " " << pg << " - maps only to OSDs with primiary affinity 0" << dendl;
      }
      return (int64_t)ps;
    }
  }
  return -1;
}

void OSDMap::zero_rbi(read_balance_info_t &rbi) const {
  rbi.pa_avg = 0.;
  rbi.pa_weighted = 0.;
  rbi.pa_weighted_avg = 0.;
  rbi.raw_score = 0.;
  rbi.optimal_score = 0.;
  rbi.adjusted_score = 0.;
  rbi.acting_raw_score = 0.;
  rbi.acting_adj_score = 0.;
  rbi.err_msg = "";
}

int OSDMap::set_rbi(
    CephContext *cct,
    read_balance_info_t &rbi,
    int64_t pool_id,
    float total_w_pa,
    float pa_sum,
    int num_osds,
    int osd_pa_count,
    float total_osd_weight,
    uint max_prims_per_osd,
    uint max_acting_prims_per_osd,
    float avg_prims_per_osd,
    bool prim_on_zero_pa,
    bool acting_on_zero_pa,
    float max_osd_score) const
{
  // put all the ugly code here, so rest of code is nicer.
  const pg_pool_t* pool = get_pg_pool(pool_id);
  zero_rbi(rbi);

  if (total_w_pa / total_osd_weight < 1. / float(pool->get_size())) {
    ldout(cct, 20) << __func__ << " pool " << pool_id << " average primary affinity is lower than"
                    << 1. / float(pool->get_size()) << dendl;
    rbi.err_msg = fmt::format(
              "pool {} average primary affinity is lower than {:.2f}, read balance score is not reliable",
              pool_id, 1. / float(pool->get_size()));
    return -EINVAL;
  }
  rbi.pa_weighted = total_w_pa;

  // weighted_prim_affinity_avg
  rbi.pa_weighted_avg = rbi_round(rbi.pa_weighted / total_osd_weight); // in [0..1]
  // p_rbi->pa_weighted / osd_pa_count; // in [0..1]

  rbi.raw_score = rbi_round((float)max_prims_per_osd / avg_prims_per_osd); // >=1
  if (acting_on_zero_pa) {
    rbi.acting_raw_score = rbi_round(max_osd_score);
    rbi.err_msg = fmt::format(
              "pool {} has acting primaries on OSD(s) with primary affinity 0, read balance score is not accurate",
              pool_id);
  } else {
    rbi.acting_raw_score = rbi_round((float)max_acting_prims_per_osd / avg_prims_per_osd);
  }

  if (osd_pa_count != 0) {
    // this implies that pa_sum > 0
    rbi.pa_avg = rbi_round(pa_sum / osd_pa_count);  // in [0..1]
  } else {
    rbi.pa_avg = 0.;
  }

  if (rbi.pa_avg != 0.) {
    int64_t zpg;
    if ((zpg = has_zero_pa_pgs(cct, pool_id)) >= 0) {
      pg_t pg(zpg, pool_id);
      std::stringstream ss;
      ss << pg;
      ldout(cct, 10) << __func__ << " pool " << pool_id << " has some PGs where all OSDs are with primary_affinity 0 (" << pg << ",...)" << dendl;
      rbi.err_msg = fmt::format(
                      "pool {} has some PGs where all OSDs are with primary_affinity 0 (at least pg {}), read balance score may not be reliable",
                      pool_id, ss.str());
      return -EINVAL;
    }
    rbi.optimal_score = rbi_round(float(num_osds) / float(osd_pa_count)); // >= 1
    // adjust the score to the primary affinity setting (if prim affinity is set
    // the raw score can't be 1 and the optimal (perfect) score is hifgher than 1)
    // When total system primary affinity is too low (average < 1 / pool replica count)
    // the score is negative in order to grab the user's attention.
    rbi.adjusted_score = rbi_round(rbi.raw_score / rbi.optimal_score); // >= 1 if PA is not low
    rbi.acting_adj_score = rbi_round(rbi.acting_raw_score / rbi.optimal_score); // >= 1 if PA is not low

  } else {
    // We should never get here - this condition is checked before calling this function - this is just sanity check code.
    rbi.err_msg = fmt::format(
            "pool {} all OSDs have zero primary affinity, can't calculate a reliable read balance score",
            pool_id);
    return -EINVAL;
  }

  return 0;
}

int OSDMap::calc_read_balance_score(CephContext *cct, int64_t pool_id,
				    read_balance_info_t *p_rbi) const
{
  //BUG: wrong score with one PG replica 3 and 4 OSDs
  if (cct != nullptr)
    ldout(cct,20) << __func__ << " pool " << get_pool_name(pool_id) << dendl;

  OSDMap tmp_osd_map;
  tmp_osd_map.deepish_copy_from(*this);
  if (p_rbi == nullptr) {
    // The only case where error message is not set - this is not tested in the unit test.
    if (cct != nullptr)
      ldout(cct,30) << __func__ << " p_rbi is nullptr." << dendl;
    return -EINVAL;
  }

  if (tmp_osd_map.pools.count(pool_id) == 0) {
    if (cct != nullptr)
      ldout(cct,30) << __func__ << " pool " << pool_id << " not found." << dendl;
    zero_rbi(*p_rbi);
    p_rbi->err_msg = fmt::format("pool {} not found", pool_id);
    return -ENOENT;
  }
  int rc = 0;
  const pg_pool_t* pool = tmp_osd_map.get_pg_pool(pool_id);
  auto num_pgs = pool->get_pg_num();

  map<uint64_t,set<pg_t>> pgs_by_osd;
  map<uint64_t,set<pg_t>> prim_pgs_by_osd;
  map<uint64_t,set<pg_t>> acting_prims_by_osd;

  pgs_by_osd = tmp_osd_map.get_pgs_by_osd(cct, pool_id, &prim_pgs_by_osd, &acting_prims_by_osd);

  if (cct != nullptr)
    ldout(cct,30) << __func__ << " Primaries for pool: "
		  << prim_pgs_by_osd << dendl;

  if (pgs_by_osd.empty()) {
    //p_rbi->err_msg = fmt::format("pool {} has no PGs mapped to OSDs", pool_id);
    return -EINVAL;
  }
  if (cct != nullptr) {
    for (auto& [osd,pgs] : prim_pgs_by_osd) {
      ldout(cct,20) << __func__ << " Pool " << pool_id << " OSD." << osd
                    << " has " << pgs.size() << " primary PGs, "
		    << acting_prims_by_osd[osd].size() << " acting primaries."
		    << dendl;
    }
  }

  auto num_osds = pgs_by_osd.size();

  float avg_prims_per_osd = (float)num_pgs / (float)num_osds;
  uint64_t max_prims_per_osd = 0;
  uint64_t max_acting_prims_per_osd = 0;
  float    max_osd_score = 0.;
  bool     prim_on_zero_pa = false;
  bool     acting_on_zero_pa = false;

  float prim_affinity_sum = 0.;
  float total_osd_weight = 0.;
  float total_weighted_pa = 0.;

  map<int,float> osds_crush_weight;
  // Set up the OSDMap
  int ruleno = tmp_osd_map.pools.at(pool_id).get_crush_rule();
  tmp_osd_map.crush->get_rule_weight_osd_map(ruleno, &osds_crush_weight);

  if (cct != nullptr) {
    ldout(cct,20) << __func__ << " pool " << pool_id
                  << " ruleno " << ruleno
                  << " weight-map " << osds_crush_weight
                  << dendl;
  }
  uint osd_pa_count = 0;

  for (auto [osd, oweight] : osds_crush_weight) {  // loop over all OSDs
    total_osd_weight += oweight;
    float osd_pa = tmp_osd_map.get_primary_affinityf(osd);
    total_weighted_pa += oweight * osd_pa;
    if (osd_pa != 0.) {
      osd_pa_count++;
    }
    if (prim_pgs_by_osd.count(osd)) {
      auto n_prims = prim_pgs_by_osd.at(osd).size();
      max_prims_per_osd = std::max(max_prims_per_osd, n_prims);
      if (osd_pa == 0.) {
        prim_on_zero_pa = true;
      }
    }
    if (acting_prims_by_osd.count(osd)) {
      auto n_aprims = acting_prims_by_osd.at(osd).size();
      max_acting_prims_per_osd = std::max(max_acting_prims_per_osd, n_aprims);
      if (osd_pa != 0.) {
        max_osd_score = std::max(max_osd_score, float(n_aprims) / osd_pa);
      }
      else {
        acting_on_zero_pa = true;
      }
    }

    prim_affinity_sum += osd_pa;
    if (cct != nullptr) {
      auto np = prim_pgs_by_osd.count(osd) ? prim_pgs_by_osd.at(osd).size() : 0;
      auto nap = acting_prims_by_osd.count(osd) ? acting_prims_by_osd.at(osd).size() : 0;
      auto wt = osds_crush_weight.count(osd) ? osds_crush_weight.at(osd) : 0.;
      ldout(cct,30) << __func__ << " OSD." << osd << " info: "
		    << " num_primaries " << np
		    << " num_acting_prims " << nap
		    << " prim_affinity " << tmp_osd_map.get_primary_affinityf(osd)
		    << " weight " << wt
		    << dendl;
    }
  }
  if (cct != nullptr) {
    ldout(cct,30) << __func__ << " pool " << pool_id
		  << " total_osd_weight " << total_osd_weight
		  << " total_weighted_pa " << total_weighted_pa
		  << dendl;
  }

  if (prim_affinity_sum == 0.0) {
    if (cct != nullptr) {
      ldout(cct, 10) << __func__ << " pool " << pool_id
	         << " has primary_affinity set to zero on all OSDs" << dendl;
    }
    zero_rbi(*p_rbi);
    p_rbi->err_msg = fmt::format("pool {} has primary_affinity set to zero on all OSDs", pool_id);

    return -ERANGE;   // score has a different meaning now.
  }
  else {
    max_osd_score *= prim_affinity_sum / num_osds;
  }

  rc = tmp_osd_map.set_rbi(cct, *p_rbi, pool_id, total_weighted_pa,
                           prim_affinity_sum, num_osds, osd_pa_count,
                           total_osd_weight, max_prims_per_osd,
                           max_acting_prims_per_osd, avg_prims_per_osd,
                           prim_on_zero_pa, acting_on_zero_pa, max_osd_score);

  if (cct != nullptr) {
    ldout(cct,30) << __func__ << " pool " << get_pool_name(pool_id)
                  << " pa_avg " << p_rbi->pa_avg
                  << " pa_weighted " << p_rbi->pa_weighted
                  << " pa_weighted_avg " << p_rbi->pa_weighted_avg
                  << " optimal_score " << p_rbi->optimal_score
                  << " adjusted_score " << p_rbi->adjusted_score
                  << " acting_adj_score " << p_rbi->acting_adj_score
                  << dendl;
    ldout(cct,20) << __func__ << " pool " << get_pool_name(pool_id)
		  << " raw_score: " << p_rbi->raw_score
		  << " acting_raw_score: " << p_rbi->acting_raw_score
		  << dendl;
    ldout(cct,10) << __func__ << " pool " << get_pool_name(pool_id)
		  << " wl_score: " << p_rbi->acting_adj_score << dendl;
  }

  return rc;
}

int OSDMap::get_osds_by_bucket_name(const string &name, set<int> *osds) const
{
  return crush->get_leaves(name, osds);
}

// get pools whose crush rules might reference the given osd
void OSDMap::get_pool_ids_by_osd(CephContext *cct,
                                int osd,
                                set<int64_t> *pool_ids) const
{
  ceph_assert(pool_ids);
  set<int> raw_rules;
  int r = crush->get_rules_by_osd(osd, &raw_rules);
  if (r < 0) {
    lderr(cct) << __func__ << " get_rules_by_osd failed: " << cpp_strerror(r)
               << dendl;
    ceph_assert(r >= 0);
  }
  set<int> rules;
  for (auto &i: raw_rules) {
    // exclude any dead rule
    if (crush_rule_in_use(i)) {
      rules.insert(i);
    }
  }
  for (auto &r: rules) {
    get_pool_ids_by_rule(r, pool_ids);
  }
}

template <typename F>
class OSDUtilizationDumper : public CrushTreeDumper::Dumper<F> {
public:
  typedef CrushTreeDumper::Dumper<F> Parent;

  OSDUtilizationDumper(const CrushWrapper *crush, const OSDMap *osdmap_,
                       const PGMap& pgmap_, bool tree_,
                       const string& filter) :
    Parent(crush, osdmap_->get_pool_names()),
    osdmap(osdmap_),
    pgmap(pgmap_),
    tree(tree_),
    min_var(-1),
    max_var(-1),
    stddev(0),
    sum(0) {
    if (osdmap->crush->name_exists(filter)) {
      // filter by crush node
      auto item_id = osdmap->crush->get_item_id(filter);
      allowed.insert(item_id);
      osdmap->crush->get_all_children(item_id, &allowed);
    } else if (osdmap->crush->class_exists(filter)) {
      // filter by device class
      class_id = osdmap->crush->get_class_id(filter);
    } else if (auto pool_id = osdmap->lookup_pg_pool_name(filter);
               pool_id >= 0) {
      // filter by pool
      auto crush_rule = osdmap->get_pool_crush_rule(pool_id);
      set<int> roots;
      osdmap->crush->find_takes_by_rule(crush_rule, &roots);
      allowed = roots;
      for (auto r : roots)
        osdmap->crush->get_all_children(r, &allowed);
    }
    average_util = average_utilization();
  }

protected:

  bool should_dump(int id) const {
    if (!allowed.empty() && !allowed.count(id)) // filter by name
      return false;
    if (id >= 0 && class_id >= 0) {
      auto item_class_id = osdmap->crush->get_item_class_id(id);
      if (item_class_id < 0 || // not bound to a class yet
          item_class_id != class_id) // or already bound to a different class
        return false;
    }
    return true;
  }

  set<int> get_dumped_osds() {
    if (allowed.empty() && class_id < 0) {
      // old way, all
      return {};
    }
    return dumped_osds;
  }

  void dump_stray(F *f) {
    for (int i = 0; i < osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !this->is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0, 0), f);
    }
  }

  void dump_item(const CrushTreeDumper::Item &qi, F *f) override {
    if (!tree && (qi.is_bucket() || dumped_osds.count(qi.id)))
      return;
    if (!should_dump(qi.id))
      return;

    if (!qi.is_bucket())
      dumped_osds.insert(qi.id);
    float reweight = qi.is_bucket() ? -1 : osdmap->get_weightf(qi.id);
    int64_t kb = 0, kb_used = 0, kb_used_data = 0, kb_used_omap = 0,
      kb_used_meta = 0, kb_avail = 0;
    double util = 0;
    if (get_bucket_utilization(qi.id, &kb, &kb_used, &kb_used_data,
			       &kb_used_omap, &kb_used_meta, &kb_avail))
      if (kb_used && kb)
        util = 100.0 * (double)kb_used / (double)kb;

    double var = 1.0;
    if (average_util)
      var = util / average_util;

    size_t num_pgs = qi.is_bucket() ? 0 : pgmap.get_num_pg_by_osd(qi.id);

    dump_item(qi, reweight, kb, kb_used,
	      kb_used_data, kb_used_omap, kb_used_meta,
	      kb_avail, util, var, num_pgs, f);

    if (!qi.is_bucket() && reweight > 0) {
      if (min_var < 0 || var < min_var)
	min_var = var;
      if (max_var < 0 || var > max_var)
	max_var = var;

      double dev = util - average_util;
      dev *= dev;
      stddev += reweight * dev;
      sum += reweight;
    }
  }

  virtual void dump_item(const CrushTreeDumper::Item &qi,
			 float &reweight,
			 int64_t kb,
			 int64_t kb_used,
			 int64_t kb_used_data,
			 int64_t kb_used_omap,
			 int64_t kb_used_meta,
			 int64_t kb_avail,
			 double& util,
			 double& var,
			 const size_t num_pgs,
			 F *f) = 0;

  double dev() {
    return sum > 0 ? sqrt(stddev / sum) : 0;
  }

  double average_utilization() {
    int64_t kb = 0, kb_used = 0;
    for (int i = 0; i < osdmap->get_max_osd(); i++) {
      if (!osdmap->exists(i) ||
           osdmap->get_weight(i) == 0 ||
          !should_dump(i))
	continue;
      int64_t kb_i, kb_used_i, kb_used_data_i, kb_used_omap_i, kb_used_meta_i,
	kb_avail_i;
      if (get_osd_utilization(i, &kb_i, &kb_used_i, &kb_used_data_i,
			      &kb_used_omap_i, &kb_used_meta_i, &kb_avail_i)) {
	kb += kb_i;
	kb_used += kb_used_i;
      }
    }
    return kb > 0 ? 100.0 * (double)kb_used / (double)kb : 0;
  }

  bool get_osd_utilization(int id, int64_t* kb, int64_t* kb_used,
			   int64_t* kb_used_data,
			   int64_t* kb_used_omap,
			   int64_t* kb_used_meta,
			   int64_t* kb_avail) const {
    const osd_stat_t *p = pgmap.get_osd_stat(id);
    if (!p) return false;
    *kb = p->statfs.kb();
    *kb_used = p->statfs.kb_used_raw();
    *kb_used_data = p->statfs.kb_used_data();
    *kb_used_omap = p->statfs.kb_used_omap();
    *kb_used_meta = p->statfs.kb_used_internal_metadata();
    *kb_avail = p->statfs.kb_avail();
    
    return true;
  }

  bool get_bucket_utilization(int id, int64_t* kb, int64_t* kb_used,
			      int64_t* kb_used_data,
			      int64_t* kb_used_omap,
			      int64_t* kb_used_meta,
			      int64_t* kb_avail) const {
    if (id >= 0) {
      if (osdmap->is_out(id) || !should_dump(id)) {
        *kb = 0;
        *kb_used = 0;
	*kb_used_data = 0;
	*kb_used_omap = 0;
	*kb_used_meta = 0;
        *kb_avail = 0;
        return true;
      }
      return get_osd_utilization(id, kb, kb_used, kb_used_data,
				 kb_used_omap, kb_used_meta, kb_avail);
    }

    *kb = 0;
    *kb_used = 0;
    *kb_used_data = 0;
    *kb_used_omap = 0;
    *kb_used_meta = 0;
    *kb_avail = 0;

    for (int k = osdmap->crush->get_bucket_size(id) - 1; k >= 0; k--) {
      int item = osdmap->crush->get_bucket_item(id, k);
      int64_t kb_i = 0, kb_used_i = 0, kb_used_data_i = 0,
	kb_used_omap_i = 0, kb_used_meta_i = 0, kb_avail_i = 0;
      if (!get_bucket_utilization(item, &kb_i, &kb_used_i,
				  &kb_used_data_i, &kb_used_omap_i,
				  &kb_used_meta_i, &kb_avail_i))
	return false;
      *kb += kb_i;
      *kb_used += kb_used_i;
      *kb_used_data += kb_used_data_i;
      *kb_used_omap += kb_used_omap_i;
      *kb_used_meta += kb_used_meta_i;
      *kb_avail += kb_avail_i;
    }
    return true;
  }

protected:
  const OSDMap *osdmap;
  const PGMap& pgmap;
  bool tree;
  double average_util;
  double min_var;
  double max_var;
  double stddev;
  double sum;
  int class_id = -1;
  set<int> allowed;
  set<int> dumped_osds;
};


class OSDUtilizationPlainDumper : public OSDUtilizationDumper<TextTable> {
public:
  typedef OSDUtilizationDumper<TextTable> Parent;

  OSDUtilizationPlainDumper(const CrushWrapper *crush, const OSDMap *osdmap,
                            const PGMap& pgmap, bool tree,
                            const string& filter) :
    Parent(crush, osdmap, pgmap, tree, filter) {}

  void dump(TextTable *tbl) {
    tbl->define_column("ID", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("CLASS", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("WEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("REWEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("RAW USE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("DATA", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("OMAP", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("META", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("%USE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("VAR", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("PGS", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("STATUS", TextTable::LEFT, TextTable::RIGHT);
    if (tree)
      tbl->define_column("TYPE NAME", TextTable::LEFT, TextTable::LEFT);

    Parent::dump(tbl);

    dump_stray(tbl);

    auto sum = pgmap.get_osd_sum(get_dumped_osds());
    *tbl << ""
	 << ""
	 << "" << "TOTAL"
	 << byte_u_t(sum.statfs.total)
	 << byte_u_t(sum.statfs.get_used_raw())
	 << byte_u_t(sum.statfs.allocated)
	 << byte_u_t(sum.statfs.omap_allocated)
	 << byte_u_t(sum.statfs.internal_metadata)
	 << byte_u_t(sum.statfs.available)
	 << lowprecision_t(average_util)
	 << ""
	 << TextTable::endrow;
  }

protected:
  struct lowprecision_t {
    float v;
    explicit lowprecision_t(float _v) : v(_v) {}
  };
  friend std::ostream &operator<<(ostream& out, const lowprecision_t& v);

  using OSDUtilizationDumper<TextTable>::dump_item;
  void dump_item(const CrushTreeDumper::Item &qi,
			 float &reweight,
			 int64_t kb,
			 int64_t kb_used,
			 int64_t kb_used_data,
			 int64_t kb_used_omap,
			 int64_t kb_used_meta,
			 int64_t kb_avail,
			 double& util,
			 double& var,
			 const size_t num_pgs,
			 TextTable *tbl) override {
    const char *c = crush->get_item_class(qi.id);
    if (!c)
      c = "";
    *tbl << qi.id
	 << c
	 << weightf_t(qi.weight)
	 << weightf_t(reweight)
	 << byte_u_t(kb << 10)
	 << byte_u_t(kb_used << 10)
	 << byte_u_t(kb_used_data << 10)
	 << byte_u_t(kb_used_omap << 10)
	 << byte_u_t(kb_used_meta << 10)
	 << byte_u_t(kb_avail << 10)
	 << lowprecision_t(util)
	 << lowprecision_t(var);

    if (qi.is_bucket()) {
      *tbl << "-";
      *tbl << "";
    } else {
      *tbl << num_pgs;
      if (osdmap->is_up(qi.id)) {
        *tbl << "up";
      } else if (osdmap->is_destroyed(qi.id)) {
        *tbl << "destroyed";
      } else {
        *tbl << "down";
      }
    }

    if (tree) {
      ostringstream name;
      for (int k = 0; k < qi.depth; k++)
	name << "    ";
      if (qi.is_bucket()) {
	int type = crush->get_bucket_type(qi.id);
	name << crush->get_type_name(type) << " "
	     << crush->get_item_name(qi.id);
      } else {
	name << "osd." << qi.id;
      }
      *tbl << name.str();
    }

    *tbl << TextTable::endrow;
  }

public:
  string summary() {
    ostringstream out;
    out << "MIN/MAX VAR: " << lowprecision_t(min_var)
	<< "/" << lowprecision_t(max_var) << "  "
	<< "STDDEV: " << lowprecision_t(dev());
    return out.str();
  }
};

ostream& operator<<(ostream& out,
		    const OSDUtilizationPlainDumper::lowprecision_t& v)
{
  if (v.v < -0.01) {
    return out << "-";
  } else if (v.v < 0.001) {
    return out << "0";
  } else {
    std::streamsize p = out.precision();
    return out << std::fixed << std::setprecision(2) << v.v << std::setprecision(p);
  }
}

class OSDUtilizationFormatDumper : public OSDUtilizationDumper<Formatter> {
public:
  typedef OSDUtilizationDumper<Formatter> Parent;

  OSDUtilizationFormatDumper(const CrushWrapper *crush, const OSDMap *osdmap,
                             const PGMap& pgmap, bool tree,
                             const string& filter) :
    Parent(crush, osdmap, pgmap, tree, filter) {}

  void dump(Formatter *f) {
    f->open_array_section("nodes");
    Parent::dump(f);
    f->close_section();

    f->open_array_section("stray");
    dump_stray(f);
    f->close_section();
  }

protected:
  using OSDUtilizationDumper<Formatter>::dump_item;
  void dump_item(const CrushTreeDumper::Item &qi,
		 float &reweight,
		 int64_t kb,
		 int64_t kb_used,
		 int64_t kb_used_data,
		 int64_t kb_used_omap,
		 int64_t kb_used_meta,
		 int64_t kb_avail,
		 double& util,
		 double& var,
		 const size_t num_pgs,
		 Formatter *f) override {
    f->open_object_section("item");
    CrushTreeDumper::dump_item_fields(crush, weight_set_names, qi, f);
    f->dump_float("reweight", reweight);
    f->dump_int("kb", kb);
    f->dump_int("kb_used", kb_used);
    f->dump_int("kb_used_data", kb_used_data);
    f->dump_int("kb_used_omap", kb_used_omap);
    f->dump_int("kb_used_meta", kb_used_meta);
    f->dump_int("kb_avail", kb_avail);
    f->dump_float("utilization", util);
    f->dump_float("var", var);
    f->dump_unsigned("pgs", num_pgs);
    if (!qi.is_bucket()) {
      if (osdmap->is_up(qi.id)) {
        f->dump_string("status", "up");
      } else if (osdmap->is_destroyed(qi.id)) {
        f->dump_string("status", "destroyed");
      } else {
        f->dump_string("status", "down");
      }
    }
    CrushTreeDumper::dump_bucket_children(crush, qi, f);
    f->close_section();
  }

public:
  void summary(Formatter *f) {
    f->open_object_section("summary");
    auto sum = pgmap.get_osd_sum(get_dumped_osds());
    auto& s = sum.statfs;

    f->dump_int("total_kb", s.kb());
    f->dump_int("total_kb_used", s.kb_used_raw());
    f->dump_int("total_kb_used_data", s.kb_used_data());
    f->dump_int("total_kb_used_omap", s.kb_used_omap());
    f->dump_int("total_kb_used_meta", s.kb_used_internal_metadata());
    f->dump_int("total_kb_avail", s.kb_avail());
    f->dump_float("average_utilization", average_util);
    f->dump_float("min_var", min_var);
    f->dump_float("max_var", max_var);
    f->dump_float("dev", dev());
    f->close_section();
  }
};

void print_osd_utilization(const OSDMap& osdmap,
                           const PGMap& pgmap,
                           ostream& out,
                           Formatter *f,
                           bool tree,
                           const string& filter)
{
  const CrushWrapper *crush = osdmap.crush.get();
  if (f) {
    f->open_object_section("df");
    OSDUtilizationFormatDumper d(crush, &osdmap, pgmap, tree, filter);
    d.dump(f);
    d.summary(f);
    f->close_section();
    f->flush(out);
  } else {
    OSDUtilizationPlainDumper d(crush, &osdmap, pgmap, tree, filter);
    TextTable tbl;
    d.dump(&tbl);
    out << tbl << d.summary() << "\n";
  }
}

void OSDMap::check_health(CephContext *cct,
			  health_check_map_t *checks) const
{
  int num_osds = get_num_osds();

  // OSD_DOWN
  // OSD_$subtree_DOWN
  // OSD_ORPHAN
  if (num_osds >= 0) {
    int num_in_osds = 0;
    int num_down_in_osds = 0;
    set<int> osds;
    set<int> down_in_osds;
    set<int> up_in_osds;
    set<int> subtree_up;
    unordered_map<int, set<int> > subtree_type_down;
    unordered_map<int, int> num_osds_subtree;
    int max_type = crush->get_max_type_id();

    for (int i = 0; i < get_max_osd(); i++) {
      if (!exists(i)) {
        if (crush->item_exists(i)) {
          osds.insert(i);
        }
	continue;
      }
      if (is_out(i) || (osd_state[i] & CEPH_OSD_NEW))
        continue;
      ++num_in_osds;
      if (down_in_osds.count(i) || up_in_osds.count(i))
	continue;
      if (!is_up(i)) {
	down_in_osds.insert(i);
	int parent_id = 0;
	int current = i;
	for (int type = 0; type <= max_type; type++) {
	  if (!crush->get_type_name(type))
	    continue;
	  int r = crush->get_immediate_parent_id(current, &parent_id);
	  if (r == -ENOENT)
	    break;
	  // break early if this parent is already marked as up
	  if (subtree_up.count(parent_id))
	    break;
	  type = crush->get_bucket_type(parent_id);
	  if (!subtree_type_is_down(
		cct, parent_id, type,
		&down_in_osds, &up_in_osds, &subtree_up, &subtree_type_down))
	    break;
	  current = parent_id;
	}
      }
    }

    // calculate the number of down osds in each down subtree and
    // store it in num_osds_subtree
    for (int type = 1; type <= max_type; type++) {
      if (!crush->get_type_name(type))
	continue;
      for (auto j = subtree_type_down[type].begin();
	   j != subtree_type_down[type].end();
	   ++j) {
	list<int> children;
	int num = 0;
	int num_children = crush->get_children(*j, &children);
	if (num_children == 0)
	  continue;
	for (auto l = children.begin(); l != children.end(); ++l) {
	  if (*l >= 0) {
	    ++num;
	  } else if (num_osds_subtree[*l] > 0) {
	    num = num + num_osds_subtree[*l];
	  }
	}
	num_osds_subtree[*j] = num;
      }
    }
    num_down_in_osds = down_in_osds.size();
    ceph_assert(num_down_in_osds <= num_in_osds);
    if (num_down_in_osds > 0) {
      // summary of down subtree types and osds
      for (int type = max_type; type > 0; type--) {
	if (!crush->get_type_name(type))
	  continue;
	if (subtree_type_down[type].size() > 0) {
	  ostringstream ss;
	  ss << subtree_type_down[type].size() << " "
	     << crush->get_type_name(type);
	  if (subtree_type_down[type].size() > 1) {
	    ss << "s";
	  }
	  int sum_down_osds = 0;
	  for (auto j = subtree_type_down[type].begin();
	       j != subtree_type_down[type].end();
	       ++j) {
	    sum_down_osds = sum_down_osds + num_osds_subtree[*j];
	  }
          ss << " (" << sum_down_osds << " osds) down";
	  string err = string("OSD_") +
	    string(crush->get_type_name(type)) + "_DOWN";
	  boost::to_upper(err);
	  auto& d = checks->add(err, HEALTH_WARN, ss.str(),
				subtree_type_down[type].size());
	  for (auto j = subtree_type_down[type].rbegin();
	       j != subtree_type_down[type].rend();
	       ++j) {
	    ostringstream ss;
	    ss << crush->get_type_name(type);
	    ss << " ";
	    ss << crush->get_item_name(*j);
	    // at the top level, do not print location
	    if (type != max_type) {
              ss << " (";
              ss << crush->get_full_location_ordered_string(*j);
              ss << ")";
	    }
	    int num = num_osds_subtree[*j];
	    ss << " (" << num << " osds)";
	    ss << " is down";
	    d.detail.push_back(ss.str());
	  }
	}
      }
      ostringstream ss;
      ss << down_in_osds.size() << " osds down";
      auto& d = checks->add("OSD_DOWN", HEALTH_WARN, ss.str(),
			    down_in_osds.size());
      for (auto it = down_in_osds.begin(); it != down_in_osds.end(); ++it) {
	ostringstream ss;
	ss << "osd." << *it << " (";
	ss << crush->get_full_location_ordered_string(*it);
	ss << ") is down";
	d.detail.push_back(ss.str());
      }
    }

    if (!osds.empty()) {
      ostringstream ss;
      ss << osds.size() << " osds exist in the crush map but not in the osdmap";
      auto& d = checks->add("OSD_ORPHAN", HEALTH_WARN, ss.str(),
			    osds.size());
      for (auto osd : osds) {
	ostringstream ss;
	ss << "osd." << osd << " exists in crush map but not in osdmap";
	d.detail.push_back(ss.str());
      }
    }
  }

  std::list<std::string> scrub_messages;
  bool noscrub = false, nodeepscrub = false;
  for (const auto &p : pools) {
    if (p.second.flags & pg_pool_t::FLAG_NOSCRUB) {
      ostringstream ss;
      ss << "Pool " << get_pool_name(p.first) << " has noscrub flag";
      scrub_messages.push_back(ss.str());
      noscrub = true;
    }
    if (p.second.flags & pg_pool_t::FLAG_NODEEP_SCRUB) {
      ostringstream ss;
      ss << "Pool " << get_pool_name(p.first) << " has nodeep-scrub flag";
      scrub_messages.push_back(ss.str());
      nodeepscrub = true;
    }
  }
  if (noscrub || nodeepscrub) {
    string out = "";
    out += noscrub ? string("noscrub") + (nodeepscrub ? ", " : "") : "";
    out += nodeepscrub ? "nodeep-scrub" : "";
    auto& d = checks->add("POOL_SCRUB_FLAGS", HEALTH_OK,
			  "Some pool(s) have the " + out + " flag(s) set", 0);
    d.detail.splice(d.detail.end(), scrub_messages);
  }

  // OSD_OUT_OF_ORDER_FULL
  {
    // An osd could configure failsafe ratio, to something different
    // but for now assume it is the same here.
    float fsr = cct->_conf->osd_failsafe_full_ratio;
    if (fsr > 1.0) fsr /= 100;
    float fr = get_full_ratio();
    float br = get_backfillfull_ratio();
    float nr = get_nearfull_ratio();

    list<string> detail;
    // These checks correspond to how OSDService::check_full_status() in an OSD
    // handles the improper setting of these values.
    if (br < nr) {
      ostringstream ss;
      ss << "backfillfull_ratio (" << br
	 << ") < nearfull_ratio (" << nr << "), increased";
      detail.push_back(ss.str());
      br = nr;
    }
    if (fr < br) {
      ostringstream ss;
      ss << "full_ratio (" << fr << ") < backfillfull_ratio (" << br
	 << "), increased";
      detail.push_back(ss.str());
      fr = br;
    }
    if (fsr < fr) {
      ostringstream ss;
      ss << "osd_failsafe_full_ratio (" << fsr << ") < full_ratio (" << fr
	 << "), increased";
      detail.push_back(ss.str());
    }
    if (!detail.empty()) {
      auto& d = checks->add("OSD_OUT_OF_ORDER_FULL", HEALTH_ERR,
			    "full ratio(s) out of order", 0);
      d.detail.swap(detail);
    }
  }

  // OSD_FULL
  // OSD_NEARFULL
  // OSD_BACKFILLFULL
  // OSD_FAILSAFE_FULL
  {
    set<int> full, backfillfull, nearfull;
    get_full_osd_counts(&full, &backfillfull, &nearfull);
    if (full.size()) {
      ostringstream ss;
      ss << full.size() << " full osd(s)";
      auto& d = checks->add("OSD_FULL", HEALTH_ERR, ss.str(), full.size());
      for (auto& i: full) {
	ostringstream ss;
	ss << "osd." << i << " is full";
	d.detail.push_back(ss.str());
      }
    }
    if (backfillfull.size()) {
      ostringstream ss;
      ss << backfillfull.size() << " backfillfull osd(s)";
      auto& d = checks->add("OSD_BACKFILLFULL", HEALTH_WARN, ss.str(),
			    backfillfull.size());
      for (auto& i: backfillfull) {
	ostringstream ss;
	ss << "osd." << i << " is backfill full";
	d.detail.push_back(ss.str());
      }
    }
    if (nearfull.size()) {
      ostringstream ss;
      ss << nearfull.size() << " nearfull osd(s)";
      auto& d = checks->add("OSD_NEARFULL", HEALTH_WARN, ss.str(), nearfull.size());
      for (auto& i: nearfull) {
	ostringstream ss;
	ss << "osd." << i << " is near full";
	d.detail.push_back(ss.str());
      }
    }
  }

  // OSDMAP_FLAGS
  {
    // warn about flags
    uint64_t warn_flags =
      CEPH_OSDMAP_PAUSERD |
      CEPH_OSDMAP_PAUSEWR |
      CEPH_OSDMAP_PAUSEREC |
      CEPH_OSDMAP_NOUP |
      CEPH_OSDMAP_NODOWN |
      CEPH_OSDMAP_NOIN |
      CEPH_OSDMAP_NOOUT |
      CEPH_OSDMAP_NOBACKFILL |
      CEPH_OSDMAP_NORECOVER |
      CEPH_OSDMAP_NOSCRUB |
      CEPH_OSDMAP_NODEEP_SCRUB |
      CEPH_OSDMAP_NOTIERAGENT |
      CEPH_OSDMAP_NOSNAPTRIM |
      CEPH_OSDMAP_NOREBALANCE;
    if (test_flag(warn_flags)) {
      ostringstream ss;
      string s = get_flag_string(get_flags() & warn_flags);
      ss << s << " flag(s) set";
      checks->add("OSDMAP_FLAGS", HEALTH_WARN, ss.str(),
		  s.size() /* kludgey but sufficient */);
    }
  }

  // OSD_FLAGS
  {
    list<string> detail;
    const unsigned flags =
      CEPH_OSD_NOUP |
      CEPH_OSD_NOIN |
      CEPH_OSD_NODOWN |
      CEPH_OSD_NOOUT;
    for (int i = 0; i < max_osd; ++i) {
      if (osd_state[i] & flags) {
	ostringstream ss;
	set<string> states;
	OSDMap::calc_state_set(osd_state[i] & flags, states);
	ss << "osd." << i << " has flags " << states;
	detail.push_back(ss.str());
      }
    }
    for (auto& i : crush_node_flags) {
      if (i.second && crush->item_exists(i.first)) {
	ostringstream ss;
	set<string> states;
	OSDMap::calc_state_set(i.second, states);
	int t = i.first >= 0 ? 0 : crush->get_bucket_type(i.first);
	const char *tn = crush->get_type_name(t);
	ss << (tn ? tn : "node") << " "
	   << crush->get_item_name(i.first) << " has flags " << states;
	detail.push_back(ss.str());
      }
    }
    for (auto& i : device_class_flags) {
      const char* class_name = crush->get_class_name(i.first);
      if (i.second && class_name) {
        ostringstream ss;
        set<string> states;
        OSDMap::calc_state_set(i.second, states);
        ss << "device class '" << class_name << "' has flags " << states;
        detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " OSDs or CRUSH {nodes, device-classes} have {NOUP,NODOWN,NOIN,NOOUT} flags set";
      auto& d = checks->add("OSD_FLAGS", HEALTH_WARN, ss.str(), detail.size());
      d.detail.swap(detail);
    }
  }

  // OLD_CRUSH_TUNABLES
  if (cct->_conf->mon_warn_on_legacy_crush_tunables) {
    string min = crush->get_min_required_version();
    if (min < cct->_conf->mon_crush_min_required_version) {
      ostringstream ss;
      ss << "crush map has legacy tunables (require " << min
	 << ", min is " << cct->_conf->mon_crush_min_required_version << ")";
      auto& d = checks->add("OLD_CRUSH_TUNABLES", HEALTH_WARN, ss.str(), 0);
      d.detail.push_back("see http://docs.ceph.com/en/latest/rados/operations/crush-map/#tunables");
    }
  }

  // OLD_CRUSH_STRAW_CALC_VERSION
  if (cct->_conf->mon_warn_on_crush_straw_calc_version_zero) {
    if (crush->get_straw_calc_version() == 0) {
      ostringstream ss;
      ss << "crush map has straw_calc_version=0";
      auto& d = checks->add("OLD_CRUSH_STRAW_CALC_VERSION", HEALTH_WARN, ss.str(), 0);
      d.detail.push_back(
	"see http://docs.ceph.com/en/latest/rados/operations/crush-map/#tunables");
    }
  }

  // CACHE_POOL_NO_HIT_SET
  if (cct->_conf->mon_warn_on_cache_pools_without_hit_sets) {
    list<string> detail;
    for (auto p = pools.cbegin(); p != pools.cend(); ++p) {
      const pg_pool_t& info = p->second;
      if (info.cache_mode_requires_hit_set() &&
	  info.hit_set_params.get_type() == HitSet::TYPE_NONE) {
	ostringstream ss;
	ss << "pool '" << get_pool_name(p->first)
	   << "' with cache_mode " << info.get_cache_mode_name()
	   << " needs hit_set_type to be set but it is not";
	detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " cache pools are missing hit_sets";
      auto& d = checks->add("CACHE_POOL_NO_HIT_SET", HEALTH_WARN, ss.str(),
			    detail.size());
      d.detail.swap(detail);
    }
  }

  // OSD_NO_SORTBITWISE
  if (!test_flag(CEPH_OSDMAP_SORTBITWISE)) {
    ostringstream ss;
    ss << "'sortbitwise' flag is not set";
    checks->add("OSD_NO_SORTBITWISE", HEALTH_WARN, ss.str(), 0);
  }

  // OSD_UPGRADE_FINISHED
  if (auto require_release = pending_require_osd_release()) {
    ostringstream ss;
    ss << "all OSDs are running " << *require_release << " or later but"
       << " require_osd_release < " << *require_release;
    auto& d = checks->add("OSD_UPGRADE_FINISHED", HEALTH_WARN, ss.str(), 0);
    d.detail.push_back(ss.str());
  }

  // POOL_NEARFULL/BACKFILLFULL/FULL
  {
    list<string> full_detail, backfillfull_detail, nearfull_detail;
    for (auto it : get_pools()) {
      const pg_pool_t &pool = it.second;
      const string& pool_name = get_pool_name(it.first);
      if (pool.has_flag(pg_pool_t::FLAG_FULL)) {
	stringstream ss;
        if (pool.has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
          // may run out of space too,
          // but we want EQUOTA taking precedence
          ss << "pool '" << pool_name << "' is full (running out of quota)";
        } else {
          ss << "pool '" << pool_name << "' is full (no space)";
        }
	full_detail.push_back(ss.str());
      } else if (pool.has_flag(pg_pool_t::FLAG_BACKFILLFULL)) {
        stringstream ss;
        ss << "pool '" << pool_name << "' is backfillfull";
        backfillfull_detail.push_back(ss.str());
      } else if (pool.has_flag(pg_pool_t::FLAG_NEARFULL)) {
        stringstream ss;
        ss << "pool '" << pool_name << "' is nearfull";
        nearfull_detail.push_back(ss.str());
      }
    }
    if (!full_detail.empty()) {
      ostringstream ss;
      ss << full_detail.size() << " pool(s) full";
      auto& d = checks->add("POOL_FULL", HEALTH_WARN, ss.str(), full_detail.size());
      d.detail.swap(full_detail);
    }
    if (!backfillfull_detail.empty()) {
      ostringstream ss;
      ss << backfillfull_detail.size() << " pool(s) backfillfull";
      auto& d = checks->add("POOL_BACKFILLFULL", HEALTH_WARN, ss.str(),
			    backfillfull_detail.size());
      d.detail.swap(backfillfull_detail);
    }
    if (!nearfull_detail.empty()) {
      ostringstream ss;
      ss << nearfull_detail.size() << " pool(s) nearfull";
      auto& d = checks->add("POOL_NEARFULL", HEALTH_WARN, ss.str(),
			    nearfull_detail.size());
      d.detail.swap(nearfull_detail);
    }
  }

  // POOL_PG_NUM_NOT_POWER_OF_TWO
  if (cct->_conf.get_val<bool>("mon_warn_on_pool_pg_num_not_power_of_two")) {
    list<string> detail;
    for (auto it : get_pools()) {
      if (!std::has_single_bit(it.second.get_pg_num_target())) {
	ostringstream ss;
	ss << "pool '" << get_pool_name(it.first)
	   << "' pg_num " << it.second.get_pg_num_target()
	   << " is not a power of two";
	detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " pool(s) have non-power-of-two pg_num";
      auto& d = checks->add("POOL_PG_NUM_NOT_POWER_OF_TWO", HEALTH_WARN,
			    ss.str(), detail.size());
      d.detail.swap(detail);
    }
  }

  // POOL_NO_REDUNDANCY
  if (cct->_conf.get_val<bool>("mon_warn_on_pool_no_redundancy"))
  {
    list<string> detail;
    for (auto it : get_pools()) {
      if (it.second.get_size() == 1) {
        ostringstream ss;
        ss << "pool '" << get_pool_name(it.first)
           << "' has no replicas configured";
        detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " pool(s) have no replicas configured";
      auto& d = checks->add("POOL_NO_REDUNDANCY", HEALTH_WARN,
        ss.str(), detail.size());
      d.detail.swap(detail);
    }
  }

  // DEGRADED STRETCH MODE
  if (cct->_conf.get_val<bool>("mon_warn_on_degraded_stretch_mode")) {
    if (recovering_stretch_mode) {
      stringstream ss;
      ss << "We are recovering stretch mode buckets, only requiring "
	 << degraded_stretch_mode << " of " << stretch_bucket_count << " buckets to peer" ;
      checks->add("RECOVERING_STRETCH_MODE", HEALTH_WARN,
			    ss.str(), 0);
    } else if (degraded_stretch_mode) {
      stringstream ss;
      ss << "We are missing stretch mode buckets, only requiring "
	 << degraded_stretch_mode << " of " << stretch_bucket_count << " buckets to peer" ;
      checks->add("DEGRADED_STRETCH_MODE", HEALTH_WARN,
			    ss.str(), 0);
    }
  }
  // UNEQUAL_WEIGHT
  if (stretch_mode_enabled) {
    vector<int> subtrees;
    crush->get_subtree_of_type(stretch_mode_bucket, &subtrees);
    if (subtrees.size() != 2) {
      stringstream ss;
      ss << "Stretch mode buckets != 2";
      checks->add("INCORRECT_NUM_BUCKETS_STRETCH_MODE", HEALTH_WARN, ss.str(), 0);
      return;
    }
    int weight1 = crush->get_item_weight(subtrees[0]);
    int weight2 = crush->get_item_weight(subtrees[1]);
    stringstream ss;
    if (weight1 != weight2) {
      ss << "Stretch mode buckets have different weights!";
      checks->add("UNEVEN_WEIGHTS_STRETCH_MODE", HEALTH_WARN, ss.str(), 0);
    }
  }

  // PUBLIC ADDRESS IS IN SUBNET MASK
  {
    auto public_network = cct->_conf.get_val<std::string>("public_network");

    if (!public_network.empty()) {
      set<int> unreachable;
      get_out_of_subnet_osd_counts(cct, public_network, &unreachable);
      if (unreachable.size()) {
        ostringstream ss;
        ss << unreachable.size()
           << " osds(s) "
           << (unreachable.size() == 1 ? "is" : "are")
           << " not reachable";
        auto& d = checks->add("OSD_UNREACHABLE", HEALTH_ERR, ss.str(), unreachable.size());
        for (auto& i: unreachable) {
          ostringstream ss;
          ss << "osd." << i << "'s public address is not in '" << public_network << "' subnet";
          d.detail.push_back(ss.str());
        }
      }
    }
  }
}

int OSDMap::parse_osd_id_list(const vector<string>& ls, set<int> *out,
			      ostream *ss) const
{
  out->clear();
  for (auto i = ls.begin(); i != ls.end(); ++i) {
    if (i == ls.begin() &&
	(*i == "any" || *i == "all" || *i == "*")) {
      get_all_osds(*out);
      break;
    }
    long osd = ceph::common::parse_osd_id(i->c_str(), ss);
    if (osd < 0) {
      *ss << "invalid osd id '" << *i << "'";
      return -EINVAL;
    }
    out->insert(osd);
  }
  return 0;
}

void OSDMap::get_random_up_osds_by_subtree(int n,     // whoami
                                           string &subtree,
                                           int limit, // how many
                                           set<int> skip,
                                           set<int> *want) const {
  if (limit <= 0)
    return;
  int subtree_type = crush->get_type_id(subtree);
  if (subtree_type < 1)
    return;
  vector<int> subtrees;
  crush->get_subtree_of_type(subtree_type, &subtrees);
  std::random_device rd;
  std::default_random_engine rng{rd()};
  std::shuffle(subtrees.begin(), subtrees.end(), rng);
  for (auto s : subtrees) {
    if (limit <= 0)
      break;
    if (crush->subtree_contains(s, n))
      continue;
    vector<int> osds;
    crush->get_children_of_type(s, 0, &osds);
    if (osds.empty())
      continue;
    vector<int> up_osds;
    for (auto o : osds) {
      if (is_up(o) && !skip.count(o))
        up_osds.push_back(o);
    }
    if (up_osds.empty())
      continue;
    auto it = up_osds.begin();
    std::advance(it, (n % up_osds.size()));
    want->insert(*it);
    --limit;
  }
}

float OSDMap::pool_raw_used_rate(int64_t poolid) const
{
  const pg_pool_t *pool = get_pg_pool(poolid);
  assert(pool != nullptr);

  switch (pool->get_type()) {
  case pg_pool_t::TYPE_REPLICATED:
    return pool->get_size();
  case pg_pool_t::TYPE_ERASURE:
  {
    auto& ecp =
      get_erasure_code_profile(pool->erasure_code_profile);
    auto pm = ecp.find("m");
    auto pk = ecp.find("k");
    if (pm != ecp.end() && pk != ecp.end()) {
      int k = atoi(pk->second.c_str());
      int m = atoi(pm->second.c_str());
      int mk = m + k;
      ceph_assert(mk != 0);
      ceph_assert(k != 0);
      return (float)mk / k;
    } else {
      return 0.0;
    }
  }
  break;
  default:
    ceph_abort_msg("unrecognized pool type");
  }
}

unsigned OSDMap::get_osd_crush_node_flags(int osd) const
{
  unsigned flags = 0;
  if (!crush_node_flags.empty()) {
    // the map will contain type -> name
    std::map<std::string,std::string> ploc = crush->get_full_location(osd);
    for (auto& i : ploc) {
      int id = crush->get_item_id(i.second);
      auto p = crush_node_flags.find(id);
      if (p != crush_node_flags.end()) {
	flags |= p->second;
      }
    }
  }
  return flags;
}

unsigned OSDMap::get_crush_node_flags(int id) const
{
  unsigned flags = 0;
  auto it = crush_node_flags.find(id);
  if (it != crush_node_flags.end())
    flags = it->second;
  return flags;
}

unsigned OSDMap::get_device_class_flags(int id) const
{
  unsigned flags = 0;
  auto it = device_class_flags.find(id);
  if (it != device_class_flags.end())
    flags = it->second;
  return flags;
}

std::optional<std::string> OSDMap::pending_require_osd_release() const
{
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_SQUID) &&
      require_osd_release < ceph_release_t::squid) {
    return "squid";
  }
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_REEF) &&
      require_osd_release < ceph_release_t::reef) {
    return "reef";
  }
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_QUINCY) &&
      require_osd_release < ceph_release_t::quincy) {
    return "quincy";
  }
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_PACIFIC) &&
      require_osd_release < ceph_release_t::pacific) {
    return "pacific";
  }
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_OCTOPUS) &&
      require_osd_release < ceph_release_t::octopus) {
    return "octopus";
  }
  if (HAVE_FEATURE(get_up_osd_features(), SERVER_NAUTILUS) &&
      require_osd_release < ceph_release_t::nautilus) {
    return "nautilus";
  }

  return std::nullopt;
}
