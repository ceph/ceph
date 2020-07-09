// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
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

#include <list>
#include <map>
#include <ostream>
#include <sstream>
#include <set>
#include <string>
#include <utility>
#include <vector>


#include <boost/assign/list_of.hpp>

#include "include/ceph_features.h"
#include "include/encoding.h"
#include "include/stringify.h"
extern "C" {
#include "crush/hash.h"
}

#include "common/Formatter.h"
#include "OSDMap.h"
#include "osd_types.h"
#include "os/Transaction.h"

using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::decode_nohead;
using ceph::encode;
using ceph::encode_nohead;
using ceph::Formatter;
using ceph::make_timespan;
using ceph::JSONFormatter;

using namespace std::literals;

const char *ceph_osd_flag_name(unsigned flag)
{
  switch (flag) {
  case CEPH_OSD_FLAG_ACK: return "ack";
  case CEPH_OSD_FLAG_ONNVRAM: return "onnvram";
  case CEPH_OSD_FLAG_ONDISK: return "ondisk";
  case CEPH_OSD_FLAG_RETRY: return "retry";
  case CEPH_OSD_FLAG_READ: return "read";
  case CEPH_OSD_FLAG_WRITE: return "write";
  case CEPH_OSD_FLAG_ORDERSNAP: return "ordersnap";
  case CEPH_OSD_FLAG_PEERSTAT_OLD: return "peerstat_old";
  case CEPH_OSD_FLAG_BALANCE_READS: return "balance_reads";
  case CEPH_OSD_FLAG_PARALLELEXEC: return "parallelexec";
  case CEPH_OSD_FLAG_PGOP: return "pgop";
  case CEPH_OSD_FLAG_EXEC: return "exec";
  case CEPH_OSD_FLAG_EXEC_PUBLIC: return "exec_public";
  case CEPH_OSD_FLAG_LOCALIZE_READS: return "localize_reads";
  case CEPH_OSD_FLAG_RWORDERED: return "rwordered";
  case CEPH_OSD_FLAG_IGNORE_CACHE: return "ignore_cache";
  case CEPH_OSD_FLAG_SKIPRWLOCKS: return "skiprwlocks";
  case CEPH_OSD_FLAG_IGNORE_OVERLAY: return "ignore_overlay";
  case CEPH_OSD_FLAG_FLUSH: return "flush";
  case CEPH_OSD_FLAG_MAP_SNAP_CLONE: return "map_snap_clone";
  case CEPH_OSD_FLAG_ENFORCE_SNAPC: return "enforce_snapc";
  case CEPH_OSD_FLAG_REDIRECTED: return "redirected";
  case CEPH_OSD_FLAG_KNOWN_REDIR: return "known_if_redirected";
  case CEPH_OSD_FLAG_FULL_TRY: return "full_try";
  case CEPH_OSD_FLAG_FULL_FORCE: return "full_force";
  case CEPH_OSD_FLAG_IGNORE_REDIRECT: return "ignore_redirect";
  case CEPH_OSD_FLAG_RETURNVEC: return "returnvec";
  default: return "???";
  }
}

string ceph_osd_flag_string(unsigned flags)
{
  string s;
  for (unsigned i=0; i<32; ++i) {
    if (flags & (1u<<i)) {
      if (s.length())
	s += "+";
      s += ceph_osd_flag_name(1u << i);
    }
  }
  if (s.length())
    return s;
  return string("-");
}

const char * ceph_osd_op_flag_name(unsigned flag)
{
  const char *name;

  switch(flag) {
    case CEPH_OSD_OP_FLAG_EXCL:
      name = "excl";
      break;
    case CEPH_OSD_OP_FLAG_FAILOK:
      name = "failok";
      break;
    case CEPH_OSD_OP_FLAG_FADVISE_RANDOM:
      name = "fadvise_random";
      break;
    case CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL:
      name = "fadvise_sequential";
      break;
    case CEPH_OSD_OP_FLAG_FADVISE_WILLNEED:
      name = "favise_willneed";
      break;
    case CEPH_OSD_OP_FLAG_FADVISE_DONTNEED:
      name = "fadvise_dontneed";
      break;
    case CEPH_OSD_OP_FLAG_FADVISE_NOCACHE:
      name = "fadvise_nocache";
      break;
    case CEPH_OSD_OP_FLAG_WITH_REFERENCE:
      name = "with_reference";
      break;
    case CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE:
      name = "bypass_clean_cache";
      break;
    default:
      name = "???";
  };

  return name;
}

string ceph_osd_op_flag_string(unsigned flags)
{
  string s;
  for (unsigned i=0; i<32; ++i) {
    if (flags & (1u<<i)) {
      if (s.length())
	s += "+";
      s += ceph_osd_op_flag_name(1u << i);
    }
  }
  if (s.length())
    return s;
  return string("-");
}

string ceph_osd_alloc_hint_flag_string(unsigned flags)
{
  string s;
  for (unsigned i=0; i<32; ++i) {
    if (flags & (1u<<i)) {
      if (s.length())
	s += "+";
      s += ceph_osd_alloc_hint_flag_name(1u << i);
    }
  }
  if (s.length())
    return s;
  return string("-");
}

void pg_shard_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(osd, bl);
  encode(shard, bl);
  ENCODE_FINISH(bl);
}
void pg_shard_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(osd, bl);
  decode(shard, bl);
  DECODE_FINISH(bl);
}

ostream &operator<<(ostream &lhs, const pg_shard_t &rhs)
{
  if (rhs.is_undefined())
    return lhs << "?";
  if (rhs.shard == shard_id_t::NO_SHARD)
    return lhs << rhs.get_osd();
  return lhs << rhs.get_osd() << '(' << (unsigned)(rhs.shard) << ')';
}

void dump(Formatter* f, const osd_alerts_t& alerts)
{
  for (auto& a : alerts) {
    string s0 = " osd: ";
    s0 += stringify(a.first);
    string s;
    for (auto& aa : a.second) {
      s = s0;
      s += " ";
      s += aa.first;
      s += ":";
      s += aa.second;
      f->dump_string("alert", s);
    }
  }
}

// -- osd_reqid_t --
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

void object_locator_t::encode(ceph::buffer::list& bl) const
{
  // verify that nobody's corrupted the locator
  ceph_assert(hash == -1 || key.empty());
  __u8 encode_compat = 3;
  ENCODE_START(6, encode_compat, bl);
  encode(pool, bl);
  int32_t preferred = -1;  // tell old code there is no preferred osd (-1).
  encode(preferred, bl);
  encode(key, bl);
  encode(nspace, bl);
  encode(hash, bl);
  if (hash != -1)
    encode_compat = std::max<std::uint8_t>(encode_compat, 6); // need to interpret the hash
  ENCODE_FINISH_NEW_COMPAT(bl, encode_compat);
}

void object_locator_t::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, p);
  if (struct_v < 2) {
    int32_t op;
    decode(op, p);
    pool = op;
    int16_t pref;
    decode(pref, p);
  } else {
    decode(pool, p);
    int32_t preferred;
    decode(preferred, p);
  }
  decode(key, p);
  if (struct_v >= 5)
    decode(nspace, p);
  if (struct_v >= 6)
    decode(hash, p);
  else
    hash = -1;
  DECODE_FINISH(p);
  // verify that nobody's corrupted the locator
  ceph_assert(hash == -1 || key.empty());
}

void object_locator_t::dump(Formatter *f) const
{
  f->dump_int("pool", pool);
  f->dump_string("key", key);
  f->dump_string("namespace", nspace);
  f->dump_int("hash", hash);
}

void object_locator_t::generate_test_instances(list<object_locator_t*>& o)
{
  o.push_back(new object_locator_t);
  o.push_back(new object_locator_t(123));
  o.push_back(new object_locator_t(123, 876));
  o.push_back(new object_locator_t(1, "n2"));
  o.push_back(new object_locator_t(1234, "", "key"));
  o.push_back(new object_locator_t(12, "n1", "key2"));
}

// -- request_redirect_t --
void request_redirect_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(redirect_locator, bl);
  encode(redirect_object, bl);
  // legacy of the removed osd_instructions member
  encode((uint32_t)0, bl);
  ENCODE_FINISH(bl);
}

void request_redirect_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(1, bl);
  uint32_t legacy_osd_instructions_len;
  decode(redirect_locator, bl);
  decode(redirect_object, bl);
  decode(legacy_osd_instructions_len, bl);
  if (legacy_osd_instructions_len) {
    bl += legacy_osd_instructions_len;
  }
  DECODE_FINISH(bl);
}

void request_redirect_t::dump(Formatter *f) const
{
  f->dump_string("object", redirect_object);
  f->open_object_section("locator");
  redirect_locator.dump(f);
  f->close_section(); // locator
}

void request_redirect_t::generate_test_instances(list<request_redirect_t*>& o)
{
  object_locator_t loc(1, "redir_obj");
  o.push_back(new request_redirect_t());
  o.push_back(new request_redirect_t(loc, 0));
  o.push_back(new request_redirect_t(loc, "redir_obj"));
  o.push_back(new request_redirect_t(loc));
}

void objectstore_perf_stat_t::dump(Formatter *f) const
{
  // *_ms values just for compatibility.
  f->dump_float("commit_latency_ms", os_commit_latency_ns / 1000000.0);
  f->dump_float("apply_latency_ms", os_apply_latency_ns / 1000000.0);
  f->dump_unsigned("commit_latency_ns", os_commit_latency_ns);
  f->dump_unsigned("apply_latency_ns", os_apply_latency_ns);
}

void objectstore_perf_stat_t::encode(ceph::buffer::list &bl, uint64_t features) const
{
  uint8_t target_v = 2;
  if (!HAVE_FEATURE(features, OS_PERF_STAT_NS)) {
    target_v = 1;
  }
  ENCODE_START(target_v, target_v, bl);
  if (target_v >= 2) {
    encode(os_commit_latency_ns, bl);
    encode(os_apply_latency_ns, bl);
  } else {
    constexpr auto NS_PER_MS = std::chrono::nanoseconds(1ms).count();
    uint32_t commit_latency_ms = os_commit_latency_ns / NS_PER_MS;
    uint32_t apply_latency_ms = os_apply_latency_ns / NS_PER_MS;
    encode(commit_latency_ms, bl); // for compatibility with older monitor.
    encode(apply_latency_ms, bl); // for compatibility with older monitor.
  }
  ENCODE_FINISH(bl);
}

void objectstore_perf_stat_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(2, bl);
  if (struct_v >= 2) {
    decode(os_commit_latency_ns, bl);
    decode(os_apply_latency_ns, bl);
  } else {
    uint32_t commit_latency_ms;
    uint32_t apply_latency_ms;
    decode(commit_latency_ms, bl);
    decode(apply_latency_ms, bl);
    constexpr auto NS_PER_MS = std::chrono::nanoseconds(1ms).count();
    os_commit_latency_ns = commit_latency_ms * NS_PER_MS;
    os_apply_latency_ns = apply_latency_ms * NS_PER_MS;
  }
  DECODE_FINISH(bl);
}

void objectstore_perf_stat_t::generate_test_instances(std::list<objectstore_perf_stat_t*>& o)
{
  o.push_back(new objectstore_perf_stat_t());
  o.push_back(new objectstore_perf_stat_t());
  o.back()->os_commit_latency_ns = 20000000;
  o.back()->os_apply_latency_ns = 30000000;
}

// -- osd_stat_t --
void osd_stat_t::dump(Formatter *f, bool with_net) const
{
  f->dump_unsigned("up_from", up_from);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("num_pgs", num_pgs);
  f->dump_unsigned("num_osds", num_osds);
  f->dump_unsigned("num_per_pool_osds", num_per_pool_osds);
  f->dump_unsigned("num_per_pool_omap_osds", num_per_pool_omap_osds);

  /// dump legacy stats fields to ensure backward compatibility.
  f->dump_unsigned("kb", statfs.kb());
  f->dump_unsigned("kb_used", statfs.kb_used_raw());
  f->dump_unsigned("kb_used_data", statfs.kb_used_data());
  f->dump_unsigned("kb_used_omap", statfs.kb_used_omap());
  f->dump_unsigned("kb_used_meta", statfs.kb_used_internal_metadata());
  f->dump_unsigned("kb_avail", statfs.kb_avail());
  ////////////////////

  f->open_object_section("statfs");
  statfs.dump(f);
  f->close_section();
  f->open_array_section("hb_peers");
  for (auto p : hb_peers)
    f->dump_int("osd", p);
  f->close_section();
  f->dump_int("snap_trim_queue_len", snap_trim_queue_len);
  f->dump_int("num_snap_trimming", num_snap_trimming);
  f->dump_int("num_shards_repaired", num_shards_repaired);
  f->open_object_section("op_queue_age_hist");
  op_queue_age_hist.dump(f);
  f->close_section();
  f->open_object_section("perf_stat");
  os_perf_stat.dump(f);
  f->close_section();
  f->open_array_section("alerts");
  ::dump(f, os_alerts);
  f->close_section();
  if (with_net) {
    dump_ping_time(f);
  }
}

void osd_stat_t::dump_ping_time(Formatter *f) const
{
  f->open_array_section("network_ping_times");
  for (auto &i : hb_pingtime) {
    f->open_object_section("entry");
    f->dump_int("osd", i.first);
    const time_t lu(i.second.last_update);
    char buffer[26];
    string lustr(ctime_r(&lu, buffer));
    lustr.pop_back();   // Remove trailing \n
    f->dump_string("last update", lustr);
    f->open_array_section("interfaces");
    f->open_object_section("interface");
    f->dump_string("interface", "back");
    f->open_object_section("average");
    f->dump_float("1min", i.second.back_pingtime[0]/1000.0);
    f->dump_float("5min", i.second.back_pingtime[1]/1000.0);
    f->dump_float("15min", i.second.back_pingtime[2]/1000.0);
    f->close_section(); // average
    f->open_object_section("min");
    f->dump_float("1min", i.second.back_min[0]/1000.0);
    f->dump_float("5min", i.second.back_min[1]/1000.0);
    f->dump_float("15min", i.second.back_min[2]/1000.0);
    f->close_section(); // min
    f->open_object_section("max");
    f->dump_float("1min", i.second.back_max[0]/1000.0);
    f->dump_float("5min", i.second.back_max[1]/1000.0);
    f->dump_float("15min", i.second.back_max[2]/1000.0);
    f->close_section(); // max
    f->dump_float("last", i.second.back_last/1000.0);
    f->close_section(); // interface

    if (i.second.front_pingtime[0] != 0) {
      f->open_object_section("interface");
      f->dump_string("interface", "front");
      f->open_object_section("average");
      f->dump_float("1min", i.second.front_pingtime[0]/1000.0);
      f->dump_float("5min", i.second.front_pingtime[1]/1000.0);
      f->dump_float("15min", i.second.front_pingtime[2]/1000.0);
      f->close_section(); // average
      f->open_object_section("min");
      f->dump_float("1min", i.second.front_min[0]/1000.0);
      f->dump_float("5min", i.second.front_min[1]/1000.0);
      f->dump_float("15min", i.second.front_min[2]/1000.0);
      f->close_section(); // min
      f->open_object_section("max");
      f->dump_float("1min", i.second.front_max[0]/1000.0);
      f->dump_float("5min", i.second.front_max[1]/1000.0);
      f->dump_float("15min", i.second.front_max[2]/1000.0);
      f->close_section(); // max
      f->dump_float("last", i.second.front_last/1000.0);
      f->close_section(); // interface
    }
    f->close_section(); // interfaces
    f->close_section(); // entry
  }
  f->close_section(); // network_ping_time
}

void osd_stat_t::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(14, 2, bl);

  //////// for compatibility ////////
  int64_t kb = statfs.kb();
  int64_t kb_used = statfs.kb_used_raw();
  int64_t kb_avail = statfs.kb_avail();
  encode(kb, bl);
  encode(kb_used, bl);
  encode(kb_avail, bl);
  ///////////////////////////////////

  encode(snap_trim_queue_len, bl);
  encode(num_snap_trimming, bl);
  encode(hb_peers, bl);
  encode((uint32_t)0, bl);
  encode(op_queue_age_hist, bl);
  encode(os_perf_stat, bl, features);
  encode(up_from, bl);
  encode(seq, bl);
  encode(num_pgs, bl);

  //////// for compatibility ////////
  int64_t kb_used_data = statfs.kb_used_data();
  int64_t kb_used_omap = statfs.kb_used_omap();
  int64_t kb_used_meta = statfs.kb_used_internal_metadata();
  encode(kb_used_data, bl);
  encode(kb_used_omap, bl);
  encode(kb_used_meta, bl);
  encode(statfs, bl);
  ///////////////////////////////////
  encode(os_alerts, bl);
  encode(num_shards_repaired, bl);
  encode(num_osds, bl);
  encode(num_per_pool_osds, bl);
  encode(num_per_pool_omap_osds, bl);

  // hb_pingtime map
  encode((int)hb_pingtime.size(), bl);
  for (auto i : hb_pingtime) {
    encode(i.first, bl); // osd
    encode(i.second.last_update, bl);
    encode(i.second.back_pingtime[0], bl);
    encode(i.second.back_pingtime[1], bl);
    encode(i.second.back_pingtime[2], bl);
    encode(i.second.back_min[0], bl);
    encode(i.second.back_min[1], bl);
    encode(i.second.back_min[2], bl);
    encode(i.second.back_max[0], bl);
    encode(i.second.back_max[1], bl);
    encode(i.second.back_max[2], bl);
    encode(i.second.back_last, bl);
    encode(i.second.front_pingtime[0], bl);
    encode(i.second.front_pingtime[1], bl);
    encode(i.second.front_pingtime[2], bl);
    encode(i.second.front_min[0], bl);
    encode(i.second.front_min[1], bl);
    encode(i.second.front_min[2], bl);
    encode(i.second.front_max[0], bl);
    encode(i.second.front_max[1], bl);
    encode(i.second.front_max[2], bl);
    encode(i.second.front_last, bl);
  }
  ENCODE_FINISH(bl);
}

void osd_stat_t::decode(ceph::buffer::list::const_iterator &bl)
{
  int64_t kb, kb_used,kb_avail;
  int64_t kb_used_data, kb_used_omap, kb_used_meta;
  DECODE_START_LEGACY_COMPAT_LEN(14, 2, 2, bl);
  decode(kb, bl);
  decode(kb_used, bl);
  decode(kb_avail, bl);
  decode(snap_trim_queue_len, bl);
  decode(num_snap_trimming, bl);
  decode(hb_peers, bl);
  vector<int> num_hb_out;
  decode(num_hb_out, bl);
  if (struct_v >= 3)
    decode(op_queue_age_hist, bl);
  if (struct_v >= 4)
    decode(os_perf_stat, bl);
  if (struct_v >= 6) {
    decode(up_from, bl);
    decode(seq, bl);
  }
  if (struct_v >= 7) {
    decode(num_pgs, bl);
  }
  if (struct_v >= 8) {
    decode(kb_used_data, bl);
    decode(kb_used_omap, bl);
    decode(kb_used_meta, bl);
  } else {
    kb_used_data = kb_used;
    kb_used_omap = 0;
    kb_used_meta = 0;
  }
  if (struct_v >= 9) {
    decode(statfs, bl);
  } else {
    statfs.reset();
    statfs.total = kb << 10;
    statfs.available = kb_avail << 10;
    // actually it's totally unexpected to have ststfs.total < statfs.available
    // here but unfortunately legacy generate_test_instances produced such a
    // case hence inserting some handling rather than assert
    statfs.internally_reserved =
      statfs.total > statfs.available ? statfs.total - statfs.available : 0;
    kb_used <<= 10;
    if ((int64_t)statfs.internally_reserved > kb_used) {
      statfs.internally_reserved -= kb_used;
    } else {
      statfs.internally_reserved = 0;
    }
    statfs.allocated = kb_used_data << 10;
    statfs.omap_allocated = kb_used_omap << 10;
    statfs.internal_metadata = kb_used_meta << 10;
  }
  if (struct_v >= 10) {
    decode(os_alerts, bl);
  } else {
    os_alerts.clear();
  }
  if (struct_v >= 11) {
    decode(num_shards_repaired, bl);
  } else {
    num_shards_repaired = 0;
  }
  if (struct_v >= 12) {
    decode(num_osds, bl);
    decode(num_per_pool_osds, bl);
  } else {
    num_osds = 0;
    num_per_pool_osds = 0;
  }
  if (struct_v >= 13) {
    decode(num_per_pool_omap_osds, bl);
  } else {
    num_per_pool_omap_osds = 0;
  }
  hb_pingtime.clear();
  if (struct_v >= 14) {
    int count;
    decode(count, bl);
    for (int i = 0 ; i < count ; i++) {
      int osd;
      decode(osd, bl);
      struct Interfaces ifs;
      decode(ifs.last_update, bl);
      decode(ifs.back_pingtime[0],bl);
      decode(ifs.back_pingtime[1], bl);
      decode(ifs.back_pingtime[2], bl);
      decode(ifs.back_min[0],bl);
      decode(ifs.back_min[1], bl);
      decode(ifs.back_min[2], bl);
      decode(ifs.back_max[0],bl);
      decode(ifs.back_max[1], bl);
      decode(ifs.back_max[2], bl);
      decode(ifs.back_last, bl);
      decode(ifs.front_pingtime[0], bl);
      decode(ifs.front_pingtime[1], bl);
      decode(ifs.front_pingtime[2], bl);
      decode(ifs.front_min[0], bl);
      decode(ifs.front_min[1], bl);
      decode(ifs.front_min[2], bl);
      decode(ifs.front_max[0], bl);
      decode(ifs.front_max[1], bl);
      decode(ifs.front_max[2], bl);
      decode(ifs.front_last, bl);
      hb_pingtime[osd] = ifs;
    }
  }
  DECODE_FINISH(bl);
}

void osd_stat_t::generate_test_instances(std::list<osd_stat_t*>& o)
{
  o.push_back(new osd_stat_t);

  o.push_back(new osd_stat_t);
  list<store_statfs_t*> ll;
  store_statfs_t::generate_test_instances(ll);
  o.back()->statfs = *ll.back();
  o.back()->hb_peers.push_back(7);
  o.back()->snap_trim_queue_len = 8;
  o.back()->num_snap_trimming = 99;
  o.back()->num_shards_repaired = 101;
  o.back()->os_alerts[0].emplace(
    "some alert", "some alert details");
  o.back()->os_alerts[1].emplace(
    "some alert2", "some alert2 details");
  struct Interfaces gen_interfaces = {
	123456789, { 1000, 900, 800 }, { 990, 890, 790 }, { 1010, 910, 810 }, 1001,
	 { 1100, 1000, 900 }, { 1090, 990, 890 }, { 1110, 1010, 910 }, 1101 };
  o.back()->hb_pingtime[20] = gen_interfaces;
  gen_interfaces = {
	987654321, { 100, 200, 300 }, { 90, 190, 290 }, { 110, 210, 310 }, 101 };
  o.back()->hb_pingtime[30] = gen_interfaces;
}

// -- pg_t --

int pg_t::print(char *o, int maxlen) const
{
  return snprintf(o, maxlen, "%llu.%x", (unsigned long long)pool(), ps());
}

bool pg_t::parse(const char *s)
{
  uint64_t ppool;
  uint32_t pseed;
  int r = sscanf(s, "%llu.%x", (long long unsigned *)&ppool, &pseed);
  if (r < 2)
    return false;
  m_pool = ppool;
  m_seed = pseed;
  return true;
}

bool spg_t::parse(const char *s)
{
  shard = shard_id_t::NO_SHARD;
  uint64_t ppool;
  uint32_t pseed;
  uint32_t pshard;
  int r = sscanf(s, "%llu.%x", (long long unsigned *)&ppool, &pseed);
  if (r < 2)
    return false;
  pgid.set_pool(ppool);
  pgid.set_ps(pseed);

  const char *p = strchr(s, 's');
  if (p) {
    r = sscanf(p, "s%u", &pshard);
    if (r == 1) {
      shard = shard_id_t(pshard);
    } else {
      return false;
    }
  }
  return true;
}

char *spg_t::calc_name(char *buf, const char *suffix_backwords) const
{
  while (*suffix_backwords)
    *--buf = *suffix_backwords++;

  if (!is_no_shard()) {
    buf = ritoa<uint8_t, 10>((uint8_t)shard.id, buf);
    *--buf = 's';
  }

  return pgid.calc_name(buf, "");
}

ostream& operator<<(ostream& out, const spg_t &pg)
{
  char buf[spg_t::calc_name_buf_size];
  buf[spg_t::calc_name_buf_size - 1] = '\0';
  out << pg.calc_name(buf + spg_t::calc_name_buf_size - 1, "");
  return out;
}

pg_t pg_t::get_ancestor(unsigned old_pg_num) const
{
  int old_bits = cbits(old_pg_num);
  int old_mask = (1 << old_bits) - 1;
  pg_t ret = *this;
  ret.m_seed = ceph_stable_mod(m_seed, old_pg_num, old_mask);
  return ret;
}

bool pg_t::is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *children) const
{
  //ceph_assert(m_seed < old_pg_num);
  if (m_seed >= old_pg_num) {
    // degenerate case
    return false;
  }
  if (new_pg_num <= old_pg_num)
    return false;

  bool split = false;
  if (true) {
    unsigned old_bits = cbits(old_pg_num);
    unsigned old_mask = (1 << old_bits) - 1;
    for (unsigned n = 1; ; n++) {
      unsigned next_bit = (n << (old_bits-1));
      unsigned s = next_bit | m_seed;

      if (s < old_pg_num || s == m_seed)
	continue;
      if (s >= new_pg_num)
	break;
      if ((unsigned)ceph_stable_mod(s, old_pg_num, old_mask) == m_seed) {
	split = true;
	if (children)
	  children->insert(pg_t(s, m_pool));
      }
    }
  }
  if (false) {
    // brute force
    int old_bits = cbits(old_pg_num);
    int old_mask = (1 << old_bits) - 1;
    for (unsigned x = old_pg_num; x < new_pg_num; ++x) {
      unsigned o = ceph_stable_mod(x, old_pg_num, old_mask);
      if (o == m_seed) {
	split = true;
	children->insert(pg_t(x, m_pool));
      }
    }
  }
  return split;
}

unsigned pg_t::get_split_bits(unsigned pg_num) const {
  if (pg_num == 1)
    return 0;
  ceph_assert(pg_num > 1);

  // Find unique p such that pg_num \in [2^(p-1), 2^p)
  unsigned p = cbits(pg_num);
  ceph_assert(p); // silence coverity #751330 

  if ((m_seed % (1<<(p-1))) < (pg_num % (1<<(p-1))))
    return p;
  else
    return p - 1;
}

bool pg_t::is_merge_source(
  unsigned old_pg_num,
  unsigned new_pg_num,
  pg_t *parent) const
{
  if (m_seed < old_pg_num &&
      m_seed >= new_pg_num) {
    if (parent) {
      pg_t t = *this;
      while (t.m_seed >= new_pg_num) {
	t = t.get_parent();
      }
      *parent = t;
    }
    return true;
  }
  return false;
}

pg_t pg_t::get_parent() const
{
  unsigned bits = cbits(m_seed);
  ceph_assert(bits);
  pg_t retval = *this;
  retval.m_seed &= ~((~0)<<(bits - 1));
  return retval;
}

hobject_t pg_t::get_hobj_start() const
{
  return hobject_t(object_t(), string(), 0, m_seed, m_pool,
		   string());
}

hobject_t pg_t::get_hobj_end(unsigned pg_num) const
{
  // note: this assumes a bitwise sort; with the legacy nibblewise
  // sort a PG did not always cover a single contiguous range of the
  // (bit-reversed) hash range.
  unsigned bits = get_split_bits(pg_num);
  uint64_t rev_start = hobject_t::_reverse_bits(m_seed);
  uint64_t rev_end = (rev_start | (0xffffffff >> bits)) + 1;
  if (rev_end >= 0x100000000) {
    ceph_assert(rev_end == 0x100000000);
    return hobject_t::get_max();
  } else {
    return hobject_t(object_t(), string(), CEPH_NOSNAP,
		   hobject_t::_reverse_bits(rev_end), m_pool,
		   string());
  }
}

void pg_t::dump(Formatter *f) const
{
  f->dump_unsigned("pool", m_pool);
  f->dump_unsigned("seed", m_seed);
}

void pg_t::generate_test_instances(list<pg_t*>& o)
{
  o.push_back(new pg_t);
  o.push_back(new pg_t(1, 2));
  o.push_back(new pg_t(13123, 3));
  o.push_back(new pg_t(131223, 4));
}

char *pg_t::calc_name(char *buf, const char *suffix_backwords) const
{
  while (*suffix_backwords)
    *--buf = *suffix_backwords++;

  buf = ritoa<uint32_t, 16>(m_seed, buf);

  *--buf = '.';

  return  ritoa<uint64_t, 10>(m_pool, buf);
}

ostream& operator<<(ostream& out, const pg_t &pg)
{
  char buf[pg_t::calc_name_buf_size];
  buf[pg_t::calc_name_buf_size - 1] = '\0';
  out << pg.calc_name(buf + pg_t::calc_name_buf_size - 1, "");
  return out;
}


// -- coll_t --

void coll_t::calc_str()
{
  switch (type) {
  case TYPE_META:
    strcpy(_str_buff, "meta");
    _str = _str_buff;
    break;
  case TYPE_PG:
    _str_buff[spg_t::calc_name_buf_size - 1] = '\0';
    _str = pgid.calc_name(_str_buff + spg_t::calc_name_buf_size - 1, "daeh_");
    break;
  case TYPE_PG_TEMP:
    _str_buff[spg_t::calc_name_buf_size - 1] = '\0';
    _str = pgid.calc_name(_str_buff + spg_t::calc_name_buf_size - 1, "PMET_");
    break;
  default:
    ceph_abort_msg("unknown collection type");
  }
}

bool coll_t::parse(const std::string& s)
{
  if (s == "meta") {
    type = TYPE_META;
    pgid = spg_t();
    removal_seq = 0;
    calc_str();
    ceph_assert(s == _str);
    return true;
  }
  if (s.find("_head") == s.length() - 5 &&
      pgid.parse(s.substr(0, s.length() - 5))) {
    type = TYPE_PG;
    removal_seq = 0;
    calc_str();
    ceph_assert(s == _str);
    return true;
  }
  if (s.find("_TEMP") == s.length() - 5 &&
      pgid.parse(s.substr(0, s.length() - 5))) {
    type = TYPE_PG_TEMP;
    removal_seq = 0;
    calc_str();
    ceph_assert(s == _str);
    return true;
  }
  return false;
}

void coll_t::encode(ceph::buffer::list& bl) const
{
  using ceph::encode;
  // when changing this, remember to update encoded_size() too.
  if (is_temp()) {
    // can't express this as v2...
    __u8 struct_v = 3;
    encode(struct_v, bl);
    encode(to_str(), bl);
  } else {
    __u8 struct_v = 2;
    encode(struct_v, bl);
    encode((__u8)type, bl);
    encode(pgid, bl);
    snapid_t snap = CEPH_NOSNAP;
    encode(snap, bl);
  }
}

size_t coll_t::encoded_size() const
{
  size_t r = sizeof(__u8);
  if (is_temp()) {
    // v3
    r += sizeof(__u32);
    if (_str) {
      r += strlen(_str);
    }
  } else {
      // v2
      // 1. type
      r += sizeof(__u8);
      // 2. pgid
      //  - encoding header
      r += sizeof(ceph_le32) + 2 * sizeof(__u8);
      // - pg_t
      r += sizeof(__u8) + sizeof(uint64_t) + 2 * sizeof(uint32_t);
      // - shard_id_t
      r += sizeof(int8_t);
      // 3. snapid_t
      r += sizeof(uint64_t);
  }

  return r;
}

void coll_t::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  switch (struct_v) {
  case 1:
    {
      snapid_t snap;
      decode(pgid, bl);
      decode(snap, bl);

      // infer the type
      if (pgid == spg_t() && snap == 0) {
	type = TYPE_META;
      } else {
	type = TYPE_PG;
      }
      removal_seq = 0;
    }
    break;

  case 2:
    {
      __u8 _type;
      snapid_t snap;
      decode(_type, bl);
      decode(pgid, bl);
      decode(snap, bl);
      type = (type_t)_type;
      removal_seq = 0;
    }
    break;

  case 3:
    {
      string str;
      decode(str, bl);
      bool ok = parse(str);
      if (!ok)
	throw std::domain_error(std::string("unable to parse pg ") + str);
    }
    break;

  default:
    {
      ostringstream oss;
      oss << "coll_t::decode(): don't know how to decode version "
	  << struct_v;
      throw std::domain_error(oss.str());
    }
  }
}

void coll_t::dump(Formatter *f) const
{
  f->dump_unsigned("type_id", (unsigned)type);
  if (type != TYPE_META)
    f->dump_stream("pgid") << pgid;
  f->dump_string("name", to_str());
}

void coll_t::generate_test_instances(list<coll_t*>& o)
{
  o.push_back(new coll_t());
  o.push_back(new coll_t(spg_t(pg_t(1, 0), shard_id_t::NO_SHARD)));
  o.push_back(new coll_t(o.back()->get_temp()));
  o.push_back(new coll_t(spg_t(pg_t(3, 2), shard_id_t(12))));
  o.push_back(new coll_t(o.back()->get_temp()));
  o.push_back(new coll_t());
}

// ---

std::string pg_vector_string(const vector<int32_t> &a)
{
  ostringstream oss;
  oss << "[";
  for (auto i = a.cbegin(); i != a.cend(); ++i) {
    if (i != a.begin())
      oss << ",";
    if (*i != CRUSH_ITEM_NONE)
      oss << *i;
    else
      oss << "NONE";
  }
  oss << "]";
  return oss.str();
}

std::string pg_state_string(uint64_t state)
{
  ostringstream oss;
  if (state & PG_STATE_STALE)
    oss << "stale+";
  if (state & PG_STATE_CREATING)
    oss << "creating+";
  if (state & PG_STATE_ACTIVE)
    oss << "active+";
  if (state & PG_STATE_ACTIVATING)
    oss << "activating+";
  if (state & PG_STATE_CLEAN)
    oss << "clean+";
  if (state & PG_STATE_RECOVERY_WAIT)
    oss << "recovery_wait+";
  if (state & PG_STATE_RECOVERY_TOOFULL)
    oss << "recovery_toofull+";
  if (state & PG_STATE_RECOVERING)
    oss << "recovering+";
  if (state & PG_STATE_FORCED_RECOVERY)
    oss << "forced_recovery+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_RECOVERY_UNFOUND)
    oss << "recovery_unfound+";
  if (state & PG_STATE_BACKFILL_UNFOUND)
    oss << "backfill_unfound+";
  if (state & PG_STATE_UNDERSIZED)
    oss << "undersized+";
  if (state & PG_STATE_DEGRADED)
    oss << "degraded+";
  if (state & PG_STATE_REMAPPED)
    oss << "remapped+";
  if (state & PG_STATE_PREMERGE)
    oss << "premerge+";
  if (state & PG_STATE_SCRUBBING)
    oss << "scrubbing+";
  if (state & PG_STATE_DEEP_SCRUB)
    oss << "deep+";
  if (state & PG_STATE_INCONSISTENT)
    oss << "inconsistent+";
  if (state & PG_STATE_PEERING)
    oss << "peering+";
  if (state & PG_STATE_REPAIR)
    oss << "repair+";
  if (state & PG_STATE_BACKFILL_WAIT)
    oss << "backfill_wait+";
  if (state & PG_STATE_BACKFILLING)
    oss << "backfilling+";
  if (state & PG_STATE_FORCED_BACKFILL)
    oss << "forced_backfill+";
  if (state & PG_STATE_BACKFILL_TOOFULL)
    oss << "backfill_toofull+";
  if (state & PG_STATE_INCOMPLETE)
    oss << "incomplete+";
  if (state & PG_STATE_PEERED)
    oss << "peered+";
  if (state & PG_STATE_SNAPTRIM)
    oss << "snaptrim+";
  if (state & PG_STATE_SNAPTRIM_WAIT)
    oss << "snaptrim_wait+";
  if (state & PG_STATE_SNAPTRIM_ERROR)
    oss << "snaptrim_error+";
  if (state & PG_STATE_FAILED_REPAIR)
    oss << "failed_repair+";
  if (state & PG_STATE_LAGGY)
    oss << "laggy+";
  if (state & PG_STATE_WAIT)
    oss << "wait+";
  string ret(oss.str());
  if (ret.length() > 0)
    ret.resize(ret.length() - 1);
  else
    ret = "unknown";
  return ret;
}

std::optional<uint64_t> pg_string_state(const std::string& state)
{
  std::optional<uint64_t> type;
  if (state == "active")
    type = PG_STATE_ACTIVE;
  else if (state == "clean")
    type = PG_STATE_CLEAN;
  else if (state == "down")
    type = PG_STATE_DOWN;
  else if (state == "recovery_unfound")
    type = PG_STATE_RECOVERY_UNFOUND;
  else if (state == "backfill_unfound")
    type = PG_STATE_BACKFILL_UNFOUND;
  else if (state == "premerge")
    type = PG_STATE_PREMERGE;
  else if (state == "scrubbing")
    type = PG_STATE_SCRUBBING;
  else if (state == "degraded")
    type = PG_STATE_DEGRADED;
  else if (state == "inconsistent")
    type = PG_STATE_INCONSISTENT;
  else if (state == "peering")
    type = PG_STATE_PEERING;
  else if (state == "repair")
    type = PG_STATE_REPAIR;
  else if (state == "recovering")
    type = PG_STATE_RECOVERING;
  else if (state == "forced_recovery")
    type = PG_STATE_FORCED_RECOVERY;
  else if (state == "backfill_wait")
    type = PG_STATE_BACKFILL_WAIT;
  else if (state == "incomplete")
    type = PG_STATE_INCOMPLETE;
  else if (state == "stale")
    type = PG_STATE_STALE;
  else if (state == "remapped")
    type = PG_STATE_REMAPPED;
  else if (state == "deep")
    type = PG_STATE_DEEP_SCRUB;
  else if (state == "backfilling")
    type = PG_STATE_BACKFILLING;
  else if (state == "forced_backfill")
    type = PG_STATE_FORCED_BACKFILL;
  else if (state == "backfill_toofull")
    type = PG_STATE_BACKFILL_TOOFULL;
  else if (state == "recovery_wait")
    type = PG_STATE_RECOVERY_WAIT;
  else if (state == "recovery_toofull")
    type = PG_STATE_RECOVERY_TOOFULL;
  else if (state == "undersized")
    type = PG_STATE_UNDERSIZED;
  else if (state == "activating")
    type = PG_STATE_ACTIVATING;
  else if (state == "peered")
    type = PG_STATE_PEERED;
  else if (state == "snaptrim")
    type = PG_STATE_SNAPTRIM;
  else if (state == "snaptrim_wait")
    type = PG_STATE_SNAPTRIM_WAIT;
  else if (state == "snaptrim_error")
    type = PG_STATE_SNAPTRIM_ERROR;
  else if (state == "creating")
    type = PG_STATE_CREATING;
  else if (state == "failed_repair")
    type = PG_STATE_FAILED_REPAIR;
  else if (state == "laggy")
    type = PG_STATE_LAGGY;
  else if (state == "wait")
    type = PG_STATE_WAIT;
  else if (state == "unknown")
    type = 0;
  else
    type = std::nullopt;
  return type;
}

// -- eversion_t --
string eversion_t::get_key_name() const
{
  std::string key(32, ' ');
  get_key_name(&key[0]);
  key.resize(31); // remove the null terminator
  return key;
}

// -- pool_snap_info_t --
void pool_snap_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("snapid", snapid);
  f->dump_stream("stamp") << stamp;
  f->dump_string("name", name);
}

void pool_snap_info_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(snapid, bl);
    encode(stamp, bl);
    encode(name, bl);
    return;
  }
  ENCODE_START(2, 2, bl);
  encode(snapid, bl);
  encode(stamp, bl);
  encode(name, bl);
  ENCODE_FINISH(bl);
}

void pool_snap_info_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(snapid, bl);
  decode(stamp, bl);
  decode(name, bl);
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

// -- pool_opts_t --

typedef std::map<std::string, pool_opts_t::opt_desc_t> opt_mapping_t;
static opt_mapping_t opt_mapping = boost::assign::map_list_of
	   ("scrub_min_interval", pool_opts_t::opt_desc_t(
	     pool_opts_t::SCRUB_MIN_INTERVAL, pool_opts_t::DOUBLE))
	   ("scrub_max_interval", pool_opts_t::opt_desc_t(
	     pool_opts_t::SCRUB_MAX_INTERVAL, pool_opts_t::DOUBLE))
	   ("deep_scrub_interval", pool_opts_t::opt_desc_t(
	     pool_opts_t::DEEP_SCRUB_INTERVAL, pool_opts_t::DOUBLE))
           ("recovery_priority", pool_opts_t::opt_desc_t(
             pool_opts_t::RECOVERY_PRIORITY, pool_opts_t::INT))
           ("recovery_op_priority", pool_opts_t::opt_desc_t(
             pool_opts_t::RECOVERY_OP_PRIORITY, pool_opts_t::INT))
           ("scrub_priority", pool_opts_t::opt_desc_t(
             pool_opts_t::SCRUB_PRIORITY, pool_opts_t::INT))
           ("compression_mode", pool_opts_t::opt_desc_t(
	     pool_opts_t::COMPRESSION_MODE, pool_opts_t::STR))
           ("compression_algorithm", pool_opts_t::opt_desc_t(
	     pool_opts_t::COMPRESSION_ALGORITHM, pool_opts_t::STR))
           ("compression_required_ratio", pool_opts_t::opt_desc_t(
	     pool_opts_t::COMPRESSION_REQUIRED_RATIO, pool_opts_t::DOUBLE))
           ("compression_max_blob_size", pool_opts_t::opt_desc_t(
	     pool_opts_t::COMPRESSION_MAX_BLOB_SIZE, pool_opts_t::INT))
           ("compression_min_blob_size", pool_opts_t::opt_desc_t(
	     pool_opts_t::COMPRESSION_MIN_BLOB_SIZE, pool_opts_t::INT))
           ("csum_type", pool_opts_t::opt_desc_t(
	     pool_opts_t::CSUM_TYPE, pool_opts_t::INT))
           ("csum_max_block", pool_opts_t::opt_desc_t(
	     pool_opts_t::CSUM_MAX_BLOCK, pool_opts_t::INT))
           ("csum_min_block", pool_opts_t::opt_desc_t(
	     pool_opts_t::CSUM_MIN_BLOCK, pool_opts_t::INT))
           ("fingerprint_algorithm", pool_opts_t::opt_desc_t(
	     pool_opts_t::FINGERPRINT_ALGORITHM, pool_opts_t::STR))
           ("pg_num_min", pool_opts_t::opt_desc_t(
	     pool_opts_t::PG_NUM_MIN, pool_opts_t::INT))
           ("target_size_bytes", pool_opts_t::opt_desc_t(
	     pool_opts_t::TARGET_SIZE_BYTES, pool_opts_t::INT))
           ("target_size_ratio", pool_opts_t::opt_desc_t(
	     pool_opts_t::TARGET_SIZE_RATIO, pool_opts_t::DOUBLE))
           ("pg_autoscale_bias", pool_opts_t::opt_desc_t(
	     pool_opts_t::PG_AUTOSCALE_BIAS, pool_opts_t::DOUBLE))
           ("read_lease_interval", pool_opts_t::opt_desc_t(
	     pool_opts_t::READ_LEASE_INTERVAL, pool_opts_t::DOUBLE));

bool pool_opts_t::is_opt_name(const std::string& name)
{
  return opt_mapping.count(name);
}

pool_opts_t::opt_desc_t pool_opts_t::get_opt_desc(const std::string& name)
{
  auto i = opt_mapping.find(name);
  ceph_assert(i != opt_mapping.end());
  return i->second;
}

bool pool_opts_t::is_set(pool_opts_t::key_t key) const
{
  return opts.count(key);
}

const pool_opts_t::value_t& pool_opts_t::get(pool_opts_t::key_t key) const
{
  auto i = opts.find(key);
  ceph_assert(i != opts.end());
  return i->second;
}

bool pool_opts_t::unset(pool_opts_t::key_t key) {
  return opts.erase(key) > 0;
}

class pool_opts_dumper_t : public boost::static_visitor<> {
public:
  pool_opts_dumper_t(const std::string& name_, Formatter* f_) :
    name(name_.c_str()), f(f_) {}

  void operator()(std::string s) const {
    f->dump_string(name, s);
  }
  void operator()(int64_t i) const {
    f->dump_int(name, i);
  }
  void operator()(double d) const {
    f->dump_float(name, d);
  }

private:
  const char* name;
  Formatter* f;
};

void pool_opts_t::dump(const std::string& name, Formatter* f) const
{
  const opt_desc_t& desc = get_opt_desc(name);
  auto i = opts.find(desc.key);
  if (i == opts.end()) {
      return;
  }
  boost::apply_visitor(pool_opts_dumper_t(name, f), i->second);
}

void pool_opts_t::dump(Formatter* f) const
{
  for (auto i = opt_mapping.cbegin(); i != opt_mapping.cend(); ++i) {
    const std::string& name = i->first;
    const opt_desc_t& desc = i->second;
    auto j = opts.find(desc.key);
    if (j == opts.end()) {
      continue;
    }
    boost::apply_visitor(pool_opts_dumper_t(name, f), j->second);
  }
}

class pool_opts_encoder_t : public boost::static_visitor<> {
public:
  explicit pool_opts_encoder_t(ceph::buffer::list& bl_, uint64_t features)
    : bl(bl_),
      features(features) {}

  void operator()(const std::string &s) const {
    encode(static_cast<int32_t>(pool_opts_t::STR), bl);
    encode(s, bl);
  }
  void operator()(int64_t i) const {
    encode(static_cast<int32_t>(pool_opts_t::INT), bl);
    if (HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      encode(i, bl);
    } else {
      encode(static_cast<int32_t>(i), bl);
    }
  }
  void operator()(double d) const {
    encode(static_cast<int32_t>(pool_opts_t::DOUBLE), bl);
    encode(d, bl);
  }

private:
  ceph::buffer::list& bl;
  uint64_t features;
};

void pool_opts_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  unsigned v = 2;
  if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
    v = 1;
  }
  ENCODE_START(v, 1, bl);
  uint32_t n = static_cast<uint32_t>(opts.size());
  encode(n, bl);
  for (auto i = opts.cbegin(); i != opts.cend(); ++i) {
    encode(static_cast<int32_t>(i->first), bl);
    boost::apply_visitor(pool_opts_encoder_t(bl, features), i->second);
  }
  ENCODE_FINISH(bl);
}

void pool_opts_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(1, bl);
  __u32 n;
  decode(n, bl);
  opts.clear();
  while (n--) {
    int32_t k, t;
    decode(k, bl);
    decode(t, bl);
    if (t == STR) {
      std::string s;
      decode(s, bl);
      opts[static_cast<key_t>(k)] = s;
    } else if (t == INT) {
      int64_t i;
      if (struct_v >= 2) {
	decode(i, bl);
      } else {
	int ii;
	decode(ii, bl);
	i = ii;
      }
      opts[static_cast<key_t>(k)] = i;
    } else if (t == DOUBLE) {
      double d;
      decode(d, bl);
      opts[static_cast<key_t>(k)] = d;
    } else {
      ceph_assert(!"invalid type");
    }
  }
  DECODE_FINISH(bl);
}

ostream& operator<<(ostream& out, const pool_opts_t& opts)
{
  for (auto i = opt_mapping.begin(); i != opt_mapping.end(); ++i) {
    const std::string& name = i->first;
    const pool_opts_t::opt_desc_t& desc = i->second;
    auto j = opts.opts.find(desc.key);
    if (j == opts.opts.end()) {
      continue;
    }
    out << " " << name << " " << j->second;
  }
  return out;
}

// -- pg_pool_t --

const char *pg_pool_t::APPLICATION_NAME_CEPHFS("cephfs");
const char *pg_pool_t::APPLICATION_NAME_RBD("rbd");
const char *pg_pool_t::APPLICATION_NAME_RGW("rgw");

void pg_pool_t::dump(Formatter *f) const
{
  f->dump_stream("create_time") << get_create_time();
  f->dump_unsigned("flags", get_flags());
  f->dump_string("flags_names", get_flags_string());
  f->dump_int("type", get_type());
  f->dump_int("size", get_size());
  f->dump_int("min_size", get_min_size());
  f->dump_int("crush_rule", get_crush_rule());
  f->dump_int("object_hash", get_object_hash());
  f->dump_string("pg_autoscale_mode",
		 get_pg_autoscale_mode_name(pg_autoscale_mode));
  f->dump_unsigned("pg_num", get_pg_num());
  f->dump_unsigned("pg_placement_num", get_pgp_num());
  f->dump_unsigned("pg_placement_num_target", get_pgp_num_target());
  f->dump_unsigned("pg_num_target", get_pg_num_target());
  f->dump_unsigned("pg_num_pending", get_pg_num_pending());
  f->dump_object("last_pg_merge_meta", last_pg_merge_meta);
  f->dump_stream("last_change") << get_last_change();
  f->dump_stream("last_force_op_resend") << get_last_force_op_resend();
  f->dump_stream("last_force_op_resend_prenautilus")
    << get_last_force_op_resend_prenautilus();
  f->dump_stream("last_force_op_resend_preluminous")
    << get_last_force_op_resend_preluminous();
  f->dump_unsigned("auid", get_auid());
  f->dump_string("snap_mode", is_pool_snaps_mode() ? "pool" : "selfmanaged");
  f->dump_unsigned("snap_seq", get_snap_seq());
  f->dump_unsigned("snap_epoch", get_snap_epoch());
  f->open_array_section("pool_snaps");
  for (auto p = snaps.cbegin(); p != snaps.cend(); ++p) {
    f->open_object_section("pool_snap_info");
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_stream("removed_snaps") << removed_snaps;
  f->dump_unsigned("quota_max_bytes", quota_max_bytes);
  f->dump_unsigned("quota_max_objects", quota_max_objects);
  f->open_array_section("tiers");
  for (auto p = tiers.cbegin(); p != tiers.cend(); ++p)
    f->dump_unsigned("pool_id", *p);
  f->close_section();
  f->dump_int("tier_of", tier_of);
  f->dump_int("read_tier", read_tier);
  f->dump_int("write_tier", write_tier);
  f->dump_string("cache_mode", get_cache_mode_name());
  f->dump_unsigned("target_max_bytes", target_max_bytes);
  f->dump_unsigned("target_max_objects", target_max_objects);
  f->dump_unsigned("cache_target_dirty_ratio_micro",
		   cache_target_dirty_ratio_micro);
  f->dump_unsigned("cache_target_dirty_high_ratio_micro",
		   cache_target_dirty_high_ratio_micro);
  f->dump_unsigned("cache_target_full_ratio_micro",
		   cache_target_full_ratio_micro);
  f->dump_unsigned("cache_min_flush_age", cache_min_flush_age);
  f->dump_unsigned("cache_min_evict_age", cache_min_evict_age);
  f->dump_string("erasure_code_profile", erasure_code_profile);
  f->open_object_section("hit_set_params");
  hit_set_params.dump(f);
  f->close_section(); // hit_set_params
  f->dump_unsigned("hit_set_period", hit_set_period);
  f->dump_unsigned("hit_set_count", hit_set_count);
  f->dump_bool("use_gmt_hitset", use_gmt_hitset);
  f->dump_unsigned("min_read_recency_for_promote", min_read_recency_for_promote);
  f->dump_unsigned("min_write_recency_for_promote", min_write_recency_for_promote);
  f->dump_unsigned("hit_set_grade_decay_rate", hit_set_grade_decay_rate);
  f->dump_unsigned("hit_set_search_last_n", hit_set_search_last_n);
  f->open_array_section("grade_table");
  for (unsigned i = 0; i < hit_set_count; ++i)
    f->dump_unsigned("value", get_grade(i));
  f->close_section();
  f->dump_unsigned("stripe_width", get_stripe_width());
  f->dump_unsigned("expected_num_objects", expected_num_objects);
  f->dump_bool("fast_read", fast_read);
  f->open_object_section("options");
  opts.dump(f);
  f->close_section(); // options
  f->open_object_section("application_metadata");
  for (auto &app_pair : application_metadata) {
    f->open_object_section(app_pair.first.c_str());
    for (auto &kv_pair : app_pair.second) {
      f->dump_string(kv_pair.first.c_str(), kv_pair.second);
    }
    f->close_section(); // application
  }
  f->close_section(); // application_metadata
}

void pg_pool_t::convert_to_pg_shards(const vector<int> &from, set<pg_shard_t>* to) const {
  for (size_t i = 0; i < from.size(); ++i) {
    if (from[i] != CRUSH_ITEM_NONE) {
      to->insert(
        pg_shard_t(
          from[i],
          is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
  }
}

void pg_pool_t::calc_pg_masks()
{
  pg_num_mask = (1 << cbits(pg_num-1)) - 1;
  pgp_num_mask = (1 << cbits(pgp_num-1)) - 1;
}

unsigned pg_pool_t::get_pg_num_divisor(pg_t pgid) const
{
  if (pg_num == pg_num_mask + 1)
    return pg_num;                    // power-of-2 split
  unsigned mask = pg_num_mask >> 1;
  if ((pgid.ps() & mask) < (pg_num & mask))
    return pg_num_mask + 1;           // smaller bin size (already split)
  else
    return (pg_num_mask + 1) >> 1;    // bigger bin (not yet split)
}

bool pg_pool_t::is_pending_merge(pg_t pgid, bool *target) const
{
  if (pg_num_pending >= pg_num) {
    return false;
  }
  if (pgid.ps() >= pg_num_pending && pgid.ps() < pg_num) {
    if (target) {
      *target = false;
    }
    return true;
  }
  for (unsigned ps = pg_num_pending; ps < pg_num; ++ps) {
    if (pg_t(ps, pgid.pool()).get_parent() == pgid) {
      if (target) {
	*target = true;
      }
      return true;
    }
  }
  return false;
}

/*
 * we have two snap modes:
 *  - pool snaps
 *    - snap existence/non-existence defined by snaps[] and snap_seq
 *  - user managed snaps
 *    - existence tracked by librados user
 */
bool pg_pool_t::is_pool_snaps_mode() const
{
  return has_flag(FLAG_POOL_SNAPS);
}

bool pg_pool_t::is_unmanaged_snaps_mode() const
{
  return has_flag(FLAG_SELFMANAGED_SNAPS);
}

bool pg_pool_t::is_removed_snap(snapid_t s) const
{
  if (is_pool_snaps_mode())
    return s <= get_snap_seq() && snaps.count(s) == 0;
  else
    return removed_snaps.contains(s);
}

snapid_t pg_pool_t::snap_exists(std::string_view s) const
{
  for (auto p = snaps.cbegin(); p != snaps.cend(); ++p)
    if (p->second.name == s)
      return p->second.snapid;
  return 0;
}

void pg_pool_t::add_snap(const char *n, utime_t stamp)
{
  ceph_assert(!is_unmanaged_snaps_mode());
  flags |= FLAG_POOL_SNAPS;
  snapid_t s = get_snap_seq() + 1;
  snap_seq = s;
  snaps[s].snapid = s;
  snaps[s].name = n;
  snaps[s].stamp = stamp;
}

uint64_t pg_pool_t::add_unmanaged_snap(bool preoctopus_compat)
{
  ceph_assert(!is_pool_snaps_mode());
  if (snap_seq == 0) {
    if (preoctopus_compat) {
      // kludge for pre-mimic tracking of pool vs selfmanaged snaps.  after
      // mimic this field is not decoded but our flag is set; pre-mimic, we
      // have a non-empty removed_snaps to signifiy a non-pool-snaps pool.
      removed_snaps.insert(snapid_t(1));
    }
    snap_seq = 1;
  }
  flags |= FLAG_SELFMANAGED_SNAPS;
  snap_seq = snap_seq + 1;
  return snap_seq;
}

void pg_pool_t::remove_snap(snapid_t s)
{
  ceph_assert(snaps.count(s));
  snaps.erase(s);
  snap_seq = snap_seq + 1;
}

void pg_pool_t::remove_unmanaged_snap(snapid_t s, bool preoctopus_compat)
{
  ceph_assert(is_unmanaged_snaps_mode());
  ++snap_seq;
  if (preoctopus_compat) {
    removed_snaps.insert(s);
    // try to add in the new seq, just to try to keep the interval_set contiguous
    if (!removed_snaps.contains(get_snap_seq())) {
      removed_snaps.insert(get_snap_seq());
    }
  }
}

SnapContext pg_pool_t::get_snap_context() const
{
  vector<snapid_t> s(snaps.size());
  unsigned i = 0;
  for (auto p = snaps.crbegin(); p != snaps.crend(); ++p)
    s[i++] = p->first;
  return SnapContext(get_snap_seq(), s);
}

uint32_t pg_pool_t::hash_key(const string& key, const string& ns) const
{
 if (ns.empty()) 
    return ceph_str_hash(object_hash, key.data(), key.length());
  int nsl = ns.length();
  int len = key.length() + nsl + 1;
  char buf[len];
  memcpy(&buf[0], ns.data(), nsl);
  buf[nsl] = '\037';
  memcpy(&buf[nsl+1], key.data(), key.length());
  return ceph_str_hash(object_hash, &buf[0], len);
}

uint32_t pg_pool_t::raw_hash_to_pg(uint32_t v) const
{
  return ceph_stable_mod(v, pg_num, pg_num_mask);
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
  if (flags & FLAG_HASHPSPOOL) {
    // Hash the pool id so that pool PGs do not overlap.
    return
      crush_hash32_2(CRUSH_HASH_RJENKINS1,
		     ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask),
		     pg.pool());
  } else {
    // Legacy behavior; add ps and pool together.  This is not a great
    // idea because the PGs from each pool will essentially overlap on
    // top of each other: 0.5 == 1.4 == 2.3 == ...
    return
      ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask) +
      pg.pool();
  }
}

uint32_t pg_pool_t::get_random_pg_position(pg_t pg, uint32_t seed) const
{
  uint32_t r = crush_hash32_2(CRUSH_HASH_RJENKINS1, seed, 123);
  if (pg_num == pg_num_mask + 1) {
    r &= ~pg_num_mask;
  } else {
    unsigned smaller_mask = pg_num_mask >> 1;
    if ((pg.ps() & smaller_mask) < (pg_num & smaller_mask)) {
      r &= ~pg_num_mask;
    } else {
      r &= ~smaller_mask;
    }
  }
  r |= pg.ps();
  return r;
}

void pg_pool_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    // this encoding matches the old struct ceph_pg_pool
    __u8 struct_v = 2;
    encode(struct_v, bl);
    encode(type, bl);
    encode(size, bl);
    encode(crush_rule, bl);
    encode(object_hash, bl);
    encode(pg_num, bl);
    encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    encode(lpg_num, bl);
    encode(lpgp_num, bl);
    encode(last_change, bl);
    encode(snap_seq, bl);
    encode(snap_epoch, bl);

    __u32 n = snaps.size();
    encode(n, bl);
    n = removed_snaps.num_intervals();
    encode(n, bl);

    encode(auid, bl);

    encode_nohead(snaps, bl, features);
    encode_nohead(removed_snaps, bl);
    return;
  }

  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 struct_v = 4;
    encode(struct_v, bl);
    encode(type, bl);
    encode(size, bl);
    encode(crush_rule, bl);
    encode(object_hash, bl);
    encode(pg_num, bl);
    encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    encode(lpg_num, bl);
    encode(lpgp_num, bl);
    encode(last_change, bl);
    encode(snap_seq, bl);
    encode(snap_epoch, bl);
    encode(snaps, bl, features);
    encode(removed_snaps, bl);
    encode(auid, bl);
    encode(flags, bl);
    encode((uint32_t)0, bl); // crash_replay_interval
    return;
  }

  if ((features & CEPH_FEATURE_OSD_POOLRESEND) == 0) {
    // we simply added last_force_op_resend here, which is a fully
    // backward compatible change.  however, encoding the same map
    // differently between monitors triggers scrub noise (even though
    // they are decodable without the feature), so let's be pendantic
    // about it.
    ENCODE_START(14, 5, bl);
    encode(type, bl);
    encode(size, bl);
    encode(crush_rule, bl);
    encode(object_hash, bl);
    encode(pg_num, bl);
    encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    encode(lpg_num, bl);
    encode(lpgp_num, bl);
    encode(last_change, bl);
    encode(snap_seq, bl);
    encode(snap_epoch, bl);
    encode(snaps, bl, features);
    encode(removed_snaps, bl);
    encode(auid, bl);
    encode(flags, bl);
    encode((uint32_t)0, bl); // crash_replay_interval
    encode(min_size, bl);
    encode(quota_max_bytes, bl);
    encode(quota_max_objects, bl);
    encode(tiers, bl);
    encode(tier_of, bl);
    __u8 c = cache_mode;
    encode(c, bl);
    encode(read_tier, bl);
    encode(write_tier, bl);
    encode(properties, bl);
    encode(hit_set_params, bl);
    encode(hit_set_period, bl);
    encode(hit_set_count, bl);
    encode(stripe_width, bl);
    encode(target_max_bytes, bl);
    encode(target_max_objects, bl);
    encode(cache_target_dirty_ratio_micro, bl);
    encode(cache_target_full_ratio_micro, bl);
    encode(cache_min_flush_age, bl);
    encode(cache_min_evict_age, bl);
    encode(erasure_code_profile, bl);
    ENCODE_FINISH(bl);
    return;
  }

  uint8_t v = 29;
  // NOTE: any new encoding dependencies must be reflected by
  // SIGNIFICANT_FEATURES
  if (!(features & CEPH_FEATURE_NEW_OSDOP_ENCODING)) {
    // this was the first post-hammer thing we added; if it's missing, encode
    // like hammer.
    v = 21;
  } else if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
    v = 24;
  } else if (!HAVE_FEATURE(features, SERVER_MIMIC)) {
    v = 26;
  } else if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
    v = 27;
  }

  ENCODE_START(v, 5, bl);
  encode(type, bl);
  encode(size, bl);
  encode(crush_rule, bl);
  encode(object_hash, bl);
  encode(pg_num, bl);
  encode(pgp_num, bl);
  __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
  encode(lpg_num, bl);
  encode(lpgp_num, bl);
  encode(last_change, bl);
  encode(snap_seq, bl);
  encode(snap_epoch, bl);
  encode(snaps, bl, features);
  encode(removed_snaps, bl);
  encode(auid, bl);
  if (v >= 27) {
    encode(flags, bl);
  } else {
    auto tmp = flags;
    tmp &= ~(FLAG_SELFMANAGED_SNAPS | FLAG_POOL_SNAPS | FLAG_CREATING);
    encode(tmp, bl);
  }
  encode((uint32_t)0, bl); // crash_replay_interval
  encode(min_size, bl);
  encode(quota_max_bytes, bl);
  encode(quota_max_objects, bl);
  encode(tiers, bl);
  encode(tier_of, bl);
  __u8 c = cache_mode;
  encode(c, bl);
  encode(read_tier, bl);
  encode(write_tier, bl);
  encode(properties, bl);
  encode(hit_set_params, bl);
  encode(hit_set_period, bl);
  encode(hit_set_count, bl);
  encode(stripe_width, bl);
  encode(target_max_bytes, bl);
  encode(target_max_objects, bl);
  encode(cache_target_dirty_ratio_micro, bl);
  encode(cache_target_full_ratio_micro, bl);
  encode(cache_min_flush_age, bl);
  encode(cache_min_evict_age, bl);
  encode(erasure_code_profile, bl);
  encode(last_force_op_resend_preluminous, bl);
  encode(min_read_recency_for_promote, bl);
  encode(expected_num_objects, bl);
  if (v >= 19) {
    encode(cache_target_dirty_high_ratio_micro, bl);
  }
  if (v >= 20) {
    encode(min_write_recency_for_promote, bl);
  }
  if (v >= 21) {
    encode(use_gmt_hitset, bl);
  }
  if (v >= 22) {
    encode(fast_read, bl);
  }
  if (v >= 23) {
    encode(hit_set_grade_decay_rate, bl);
    encode(hit_set_search_last_n, bl);
  }
  if (v >= 24) {
    encode(opts, bl, features);
  }
  if (v >= 25) {
    encode(last_force_op_resend_prenautilus, bl);
  }
  if (v >= 26) {
    encode(application_metadata, bl);
  }
  if (v >= 27) {
    encode(create_time, bl);
  }
  if (v >= 28) {
    encode(pg_num_target, bl);
    encode(pgp_num_target, bl);
    encode(pg_num_pending, bl);
    encode((epoch_t)0, bl);  // pg_num_dec_last_epoch_started from 14.1.[01]
    encode((epoch_t)0, bl);  // pg_num_dec_last_epoch_clean from 14.1.[01]
    encode(last_force_op_resend, bl);
    encode(pg_autoscale_mode, bl);
  }
  if (v >= 29) {
    encode(last_pg_merge_meta, bl);
  }
  ENCODE_FINISH(bl);
}

void pg_pool_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(29, 5, 5, bl);
  decode(type, bl);
  decode(size, bl);
  decode(crush_rule, bl);
  decode(object_hash, bl);
  decode(pg_num, bl);
  decode(pgp_num, bl);
  {
    __u32 lpg_num, lpgp_num;
    decode(lpg_num, bl);
    decode(lpgp_num, bl);
  }
  decode(last_change, bl);
  decode(snap_seq, bl);
  decode(snap_epoch, bl);

  if (struct_v >= 3) {
    decode(snaps, bl);
    decode(removed_snaps, bl);
    decode(auid, bl);
  } else {
    __u32 n, m;
    decode(n, bl);
    decode(m, bl);
    decode(auid, bl);
    decode_nohead(n, snaps, bl);
    decode_nohead(m, removed_snaps, bl);
  }

  if (struct_v >= 4) {
    decode(flags, bl);
    uint32_t crash_replay_interval;
    decode(crash_replay_interval, bl);
  } else {
    flags = 0;
  }
  // upgrade path for selfmanaged vs pool snaps
  if (snap_seq > 0 && (flags & (FLAG_SELFMANAGED_SNAPS|FLAG_POOL_SNAPS)) == 0) {
    if (!removed_snaps.empty()) {
      flags |= FLAG_SELFMANAGED_SNAPS;
    } else {
      flags |= FLAG_POOL_SNAPS;
    }
  }
  if (struct_v >= 7) {
    decode(min_size, bl);
  } else {
    min_size = size - size/2;
  }
  if (struct_v >= 8) {
    decode(quota_max_bytes, bl);
    decode(quota_max_objects, bl);
  }
  if (struct_v >= 9) {
    decode(tiers, bl);
    decode(tier_of, bl);
    __u8 v;
    decode(v, bl);
    cache_mode = (cache_mode_t)v;
    decode(read_tier, bl);
    decode(write_tier, bl);
  }
  if (struct_v >= 10) {
    decode(properties, bl);
  }
  if (struct_v >= 11) {
    decode(hit_set_params, bl);
    decode(hit_set_period, bl);
    decode(hit_set_count, bl);
  } else {
    pg_pool_t def;
    hit_set_period = def.hit_set_period;
    hit_set_count = def.hit_set_count;
  }
  if (struct_v >= 12) {
    decode(stripe_width, bl);
  } else {
    set_stripe_width(0);
  }
  if (struct_v >= 13) {
    decode(target_max_bytes, bl);
    decode(target_max_objects, bl);
    decode(cache_target_dirty_ratio_micro, bl);
    decode(cache_target_full_ratio_micro, bl);
    decode(cache_min_flush_age, bl);
    decode(cache_min_evict_age, bl);
  } else {
    target_max_bytes = 0;
    target_max_objects = 0;
    cache_target_dirty_ratio_micro = 0;
    cache_target_full_ratio_micro = 0;
    cache_min_flush_age = 0;
    cache_min_evict_age = 0;
  }
  if (struct_v >= 14) {
    decode(erasure_code_profile, bl);
  }
  if (struct_v >= 15) {
    decode(last_force_op_resend_preluminous, bl);
  } else {
    last_force_op_resend_preluminous = 0;
  }
  if (struct_v >= 16) {
    decode(min_read_recency_for_promote, bl);
  } else {
    min_read_recency_for_promote = 1;
  }
  if (struct_v >= 17) {
    decode(expected_num_objects, bl);
  } else {
    expected_num_objects = 0;
  }
  if (struct_v >= 19) {
    decode(cache_target_dirty_high_ratio_micro, bl);
  } else {
    cache_target_dirty_high_ratio_micro = cache_target_dirty_ratio_micro;
  }
  if (struct_v >= 20) {
    decode(min_write_recency_for_promote, bl);
  } else {
    min_write_recency_for_promote = 1;
  }
  if (struct_v >= 21) {
    decode(use_gmt_hitset, bl);
  } else {
    use_gmt_hitset = false;
  }
  if (struct_v >= 22) {
    decode(fast_read, bl);
  } else {
    fast_read = false;
  }
  if (struct_v >= 23) {
    decode(hit_set_grade_decay_rate, bl);
    decode(hit_set_search_last_n, bl);
  } else {
    hit_set_grade_decay_rate = 0;
    hit_set_search_last_n = 1;
  }
  if (struct_v >= 24) {
    decode(opts, bl);
  }
  if (struct_v >= 25) {
    decode(last_force_op_resend_prenautilus, bl);
  } else {
    last_force_op_resend_prenautilus = last_force_op_resend_preluminous;
  }
  if (struct_v >= 26) {
    decode(application_metadata, bl);
  }
  if (struct_v >= 27) {
    decode(create_time, bl);
  }
  if (struct_v >= 28) {
    decode(pg_num_target, bl);
    decode(pgp_num_target, bl);
    decode(pg_num_pending, bl);
    epoch_t old_merge_last_epoch_clean, old_merge_last_epoch_started;
    decode(old_merge_last_epoch_started, bl);
    decode(old_merge_last_epoch_clean, bl);
    decode(last_force_op_resend, bl);
    decode(pg_autoscale_mode, bl);
    if (struct_v >= 29) {
      decode(last_pg_merge_meta, bl);
    } else {
      last_pg_merge_meta.last_epoch_clean = old_merge_last_epoch_clean;
      last_pg_merge_meta.last_epoch_started = old_merge_last_epoch_started;
    }
  } else {
    pg_num_target = pg_num;
    pgp_num_target = pgp_num;
    pg_num_pending = pg_num;
    last_force_op_resend = last_force_op_resend_prenautilus;
    pg_autoscale_mode = pg_autoscale_mode_t::WARN;    // default to warn on upgrade
  }
  DECODE_FINISH(bl);
  calc_pg_masks();
  calc_grade_table();
}

void pg_pool_t::generate_test_instances(list<pg_pool_t*>& o)
{
  pg_pool_t a;
  o.push_back(new pg_pool_t(a));

  a.create_time = utime_t(4,5);
  a.type = TYPE_REPLICATED;
  a.size = 2;
  a.crush_rule = 3;
  a.object_hash = 4;
  a.pg_num = 6;
  a.pgp_num = 4;
  a.pgp_num_target = 4;
  a.pg_num_target = 5;
  a.pg_num_pending = 5;
  a.last_pg_merge_meta.last_epoch_started = 2;
  a.last_pg_merge_meta.last_epoch_clean = 2;
  a.last_change = 9;
  a.last_force_op_resend = 123823;
  a.last_force_op_resend_preluminous = 123824;
  a.snap_seq = 10;
  a.snap_epoch = 11;
  a.flags = FLAG_POOL_SNAPS;
  a.auid = 12;
  a.quota_max_bytes = 473;
  a.quota_max_objects = 474;
  o.push_back(new pg_pool_t(a));

  a.snaps[3].name = "asdf";
  a.snaps[3].snapid = 3;
  a.snaps[3].stamp = utime_t(123, 4);
  a.snaps[6].name = "qwer";
  a.snaps[6].snapid = 6;
  a.snaps[6].stamp = utime_t(23423, 4);
  o.push_back(new pg_pool_t(a));

  a.flags = FLAG_SELFMANAGED_SNAPS;
  a.snaps.clear();
  a.removed_snaps.insert(2);
  a.quota_max_bytes = 2473;
  a.quota_max_objects = 4374;
  a.tiers.insert(0);
  a.tiers.insert(1);
  a.tier_of = 2;
  a.cache_mode = CACHEMODE_WRITEBACK;
  a.read_tier = 1;
  a.write_tier = 1;
  a.hit_set_params = HitSet::Params(new BloomHitSet::Params);
  a.hit_set_period = 3600;
  a.hit_set_count = 8;
  a.min_read_recency_for_promote = 1;
  a.min_write_recency_for_promote = 1;
  a.hit_set_grade_decay_rate = 50;
  a.hit_set_search_last_n = 1;
  a.calc_grade_table();
  a.set_stripe_width(12345);
  a.target_max_bytes = 1238132132;
  a.target_max_objects = 1232132;
  a.cache_target_dirty_ratio_micro = 187232;
  a.cache_target_dirty_high_ratio_micro = 309856;
  a.cache_target_full_ratio_micro = 987222;
  a.cache_min_flush_age = 231;
  a.cache_min_evict_age = 2321;
  a.erasure_code_profile = "profile in osdmap";
  a.expected_num_objects = 123456;
  a.fast_read = false;
  a.application_metadata = {{"rbd", {{"key", "value"}}}};
  o.push_back(new pg_pool_t(a));
}

ostream& operator<<(ostream& out, const pg_pool_t& p)
{
  out << p.get_type_name();
  if (p.get_type_name() == "erasure") {
    out << " profile " << p.erasure_code_profile;
  }
  out << " size " << p.get_size()
      << " min_size " << p.get_min_size()
      << " crush_rule " << p.get_crush_rule()
      << " object_hash " << p.get_object_hash_name()
      << " pg_num " << p.get_pg_num()
      << " pgp_num " << p.get_pgp_num();
  if (p.get_pg_num_target() != p.get_pg_num()) {
    out << " pg_num_target " << p.get_pg_num_target();
  }
  if (p.get_pgp_num_target() != p.get_pgp_num()) {
    out << " pgp_num_target " << p.get_pgp_num_target();
  }
  if (p.get_pg_num_pending() != p.get_pg_num()) {
    out << " pg_num_pending " << p.get_pg_num_pending();
  }
  if (p.pg_autoscale_mode != pg_pool_t::pg_autoscale_mode_t::UNKNOWN) {
    out << " autoscale_mode " << p.get_pg_autoscale_mode_name(p.pg_autoscale_mode);
  }
  out << " last_change " << p.get_last_change();
  if (p.get_last_force_op_resend() ||
      p.get_last_force_op_resend_prenautilus() ||
      p.get_last_force_op_resend_preluminous())
    out << " lfor " << p.get_last_force_op_resend() << "/"
	<< p.get_last_force_op_resend_prenautilus() << "/"
	<< p.get_last_force_op_resend_preluminous();
  if (p.get_auid())
    out << " owner " << p.get_auid();
  if (p.flags)
    out << " flags " << p.get_flags_string();
  if (p.quota_max_bytes)
    out << " max_bytes " << p.quota_max_bytes;
  if (p.quota_max_objects)
    out << " max_objects " << p.quota_max_objects;
  if (!p.tiers.empty())
    out << " tiers " << p.tiers;
  if (p.is_tier())
    out << " tier_of " << p.tier_of;
  if (p.has_read_tier())
    out << " read_tier " << p.read_tier;
  if (p.has_write_tier())
    out << " write_tier " << p.write_tier;
  if (p.cache_mode)
    out << " cache_mode " << p.get_cache_mode_name();
  if (p.target_max_bytes)
    out << " target_bytes " << p.target_max_bytes;
  if (p.target_max_objects)
    out << " target_objects " << p.target_max_objects;
  if (p.hit_set_params.get_type() != HitSet::TYPE_NONE) {
    out << " hit_set " << p.hit_set_params
	<< " " << p.hit_set_period << "s"
	<< " x" << p.hit_set_count << " decay_rate "
	<< p.hit_set_grade_decay_rate
	<< " search_last_n " << p.hit_set_search_last_n;
  }
  if (p.min_read_recency_for_promote)
    out << " min_read_recency_for_promote " << p.min_read_recency_for_promote;
  if (p.min_write_recency_for_promote)
    out << " min_write_recency_for_promote " << p.min_write_recency_for_promote;
  out << " stripe_width " << p.get_stripe_width();
  if (p.expected_num_objects)
    out << " expected_num_objects " << p.expected_num_objects;
  if (p.fast_read)
    out << " fast_read " << p.fast_read;
  out << p.opts;
  if (!p.application_metadata.empty()) {
    out << " application ";
    for (auto it = p.application_metadata.begin();
         it != p.application_metadata.end(); ++it) {
      if (it != p.application_metadata.begin())
        out << ",";
      out << it->first;
    }
  }
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
  f->dump_int("num_objects_missing", num_objects_missing);
  f->dump_int("num_objects_degraded", num_objects_degraded);
  f->dump_int("num_objects_misplaced", num_objects_misplaced);
  f->dump_int("num_objects_unfound", num_objects_unfound);
  f->dump_int("num_objects_dirty", num_objects_dirty);
  f->dump_int("num_whiteouts", num_whiteouts);
  f->dump_int("num_read", num_rd);
  f->dump_int("num_read_kb", num_rd_kb);
  f->dump_int("num_write", num_wr);
  f->dump_int("num_write_kb", num_wr_kb);
  f->dump_int("num_scrub_errors", num_scrub_errors);
  f->dump_int("num_shallow_scrub_errors", num_shallow_scrub_errors);
  f->dump_int("num_deep_scrub_errors", num_deep_scrub_errors);
  f->dump_int("num_objects_recovered", num_objects_recovered);
  f->dump_int("num_bytes_recovered", num_bytes_recovered);
  f->dump_int("num_keys_recovered", num_keys_recovered);
  f->dump_int("num_objects_omap", num_objects_omap);
  f->dump_int("num_objects_hit_set_archive", num_objects_hit_set_archive);
  f->dump_int("num_bytes_hit_set_archive", num_bytes_hit_set_archive);
  f->dump_int("num_flush", num_flush);
  f->dump_int("num_flush_kb", num_flush_kb);
  f->dump_int("num_evict", num_evict);
  f->dump_int("num_evict_kb", num_evict_kb);
  f->dump_int("num_promote", num_promote);
  f->dump_int("num_flush_mode_high", num_flush_mode_high);
  f->dump_int("num_flush_mode_low", num_flush_mode_low);
  f->dump_int("num_evict_mode_some", num_evict_mode_some);
  f->dump_int("num_evict_mode_full", num_evict_mode_full);
  f->dump_int("num_objects_pinned", num_objects_pinned);
  f->dump_int("num_legacy_snapsets", num_legacy_snapsets);
  f->dump_int("num_large_omap_objects", num_large_omap_objects);
  f->dump_int("num_objects_manifest", num_objects_manifest);
  f->dump_int("num_omap_bytes", num_omap_bytes);
  f->dump_int("num_omap_keys", num_omap_keys);
  f->dump_int("num_objects_repaired", num_objects_repaired);
}

void object_stat_sum_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(20, 14, bl);
#if defined(CEPH_LITTLE_ENDIAN)
  bl.append((char *)(&num_bytes), sizeof(object_stat_sum_t));
#else
  encode(num_bytes, bl);
  encode(num_objects, bl);
  encode(num_object_clones, bl);
  encode(num_object_copies, bl);
  encode(num_objects_missing_on_primary, bl);
  encode(num_objects_degraded, bl);
  encode(num_objects_unfound, bl);
  encode(num_rd, bl);
  encode(num_rd_kb, bl);
  encode(num_wr, bl);
  encode(num_wr_kb, bl);
  encode(num_scrub_errors, bl);
  encode(num_objects_recovered, bl);
  encode(num_bytes_recovered, bl);
  encode(num_keys_recovered, bl);
  encode(num_shallow_scrub_errors, bl);
  encode(num_deep_scrub_errors, bl);
  encode(num_objects_dirty, bl);
  encode(num_whiteouts, bl);
  encode(num_objects_omap, bl);
  encode(num_objects_hit_set_archive, bl);
  encode(num_objects_misplaced, bl);
  encode(num_bytes_hit_set_archive, bl);
  encode(num_flush, bl);
  encode(num_flush_kb, bl);
  encode(num_evict, bl);
  encode(num_evict_kb, bl);
  encode(num_promote, bl);
  encode(num_flush_mode_high, bl);
  encode(num_flush_mode_low, bl);
  encode(num_evict_mode_some, bl);
  encode(num_evict_mode_full, bl);
  encode(num_objects_pinned, bl);
  encode(num_objects_missing, bl);
  encode(num_legacy_snapsets, bl);
  encode(num_large_omap_objects, bl);
  encode(num_objects_manifest, bl);
  encode(num_omap_bytes, bl);
  encode(num_omap_keys, bl);
  encode(num_objects_repaired, bl);
#endif
  ENCODE_FINISH(bl);
}

void object_stat_sum_t::decode(ceph::buffer::list::const_iterator& bl)
{
  bool decode_finish = false;
  static const int STAT_SUM_DECODE_VERSION = 20;
  DECODE_START(STAT_SUM_DECODE_VERSION, bl);
#if defined(CEPH_LITTLE_ENDIAN)
  if (struct_v == STAT_SUM_DECODE_VERSION) {
    bl.copy(sizeof(object_stat_sum_t), (char*)(&num_bytes));
    decode_finish = true;
  }
#endif
  if (!decode_finish) {
    decode(num_bytes, bl);
    decode(num_objects, bl);
    decode(num_object_clones, bl);
    decode(num_object_copies, bl);
    decode(num_objects_missing_on_primary, bl);
    decode(num_objects_degraded, bl);
    decode(num_objects_unfound, bl);
    decode(num_rd, bl);
    decode(num_rd_kb, bl);
    decode(num_wr, bl);
    decode(num_wr_kb, bl);
    decode(num_scrub_errors, bl);
    decode(num_objects_recovered, bl);
    decode(num_bytes_recovered, bl);
    decode(num_keys_recovered, bl);
    decode(num_shallow_scrub_errors, bl);
    decode(num_deep_scrub_errors, bl);
    decode(num_objects_dirty, bl);
    decode(num_whiteouts, bl);
    decode(num_objects_omap, bl);
    decode(num_objects_hit_set_archive, bl);
    decode(num_objects_misplaced, bl);
    decode(num_bytes_hit_set_archive, bl);
    decode(num_flush, bl);
    decode(num_flush_kb, bl);
    decode(num_evict, bl);
    decode(num_evict_kb, bl);
    decode(num_promote, bl);
    decode(num_flush_mode_high, bl);
    decode(num_flush_mode_low, bl);
    decode(num_evict_mode_some, bl);
    decode(num_evict_mode_full, bl);
    decode(num_objects_pinned, bl);
    decode(num_objects_missing, bl);
    if (struct_v >= 16) {
      decode(num_legacy_snapsets, bl);
    } else {
      num_legacy_snapsets = num_object_clones;  // upper bound
    }
    if (struct_v >= 17) {
      decode(num_large_omap_objects, bl);
    }
    if (struct_v >= 18) {
      decode(num_objects_manifest, bl);
    }
    if (struct_v >= 19) {
      decode(num_omap_bytes, bl);
      decode(num_omap_keys, bl);
    }
    if (struct_v >= 20) {
      decode(num_objects_repaired, bl);
    }
  }
  DECODE_FINISH(bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t*>& o)
{
  object_stat_sum_t a;

  a.num_bytes = 1;
  a.num_objects = 3;
  a.num_object_clones = 4;
  a.num_object_copies = 5;
  a.num_objects_missing_on_primary = 6;
  a.num_objects_missing = 123;
  a.num_objects_degraded = 7;
  a.num_objects_unfound = 8;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  a.num_objects_recovered = 14;
  a.num_bytes_recovered = 15;
  a.num_keys_recovered = 16;
  a.num_deep_scrub_errors = 17;
  a.num_shallow_scrub_errors = 18;
  a.num_scrub_errors = a.num_deep_scrub_errors + a.num_shallow_scrub_errors;
  a.num_objects_dirty = 21;
  a.num_whiteouts = 22;
  a.num_objects_misplaced = 1232;
  a.num_objects_hit_set_archive = 2;
  a.num_bytes_hit_set_archive = 27;
  a.num_flush = 5;
  a.num_flush_kb = 6;
  a.num_evict = 7;
  a.num_evict_kb = 8;
  a.num_promote = 9;
  a.num_flush_mode_high = 0;
  a.num_flush_mode_low = 1;
  a.num_evict_mode_some = 1;
  a.num_evict_mode_full = 0;
  a.num_objects_pinned = 20;
  a.num_large_omap_objects = 5;
  a.num_objects_manifest = 2;
  a.num_omap_bytes = 20000;
  a.num_omap_keys = 200;
  a.num_objects_repaired = 300;
  o.push_back(new object_stat_sum_t(a));
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
  num_objects += o.num_objects;
  num_object_clones += o.num_object_clones;
  num_object_copies += o.num_object_copies;
  num_objects_missing_on_primary += o.num_objects_missing_on_primary;
  num_objects_missing += o.num_objects_missing;
  num_objects_degraded += o.num_objects_degraded;
  num_objects_misplaced += o.num_objects_misplaced;
  num_rd += o.num_rd;
  num_rd_kb += o.num_rd_kb;
  num_wr += o.num_wr;
  num_wr_kb += o.num_wr_kb;
  num_objects_unfound += o.num_objects_unfound;
  num_scrub_errors += o.num_scrub_errors;
  num_shallow_scrub_errors += o.num_shallow_scrub_errors;
  num_deep_scrub_errors += o.num_deep_scrub_errors;
  num_objects_recovered += o.num_objects_recovered;
  num_bytes_recovered += o.num_bytes_recovered;
  num_keys_recovered += o.num_keys_recovered;
  num_objects_dirty += o.num_objects_dirty;
  num_whiteouts += o.num_whiteouts;
  num_objects_omap += o.num_objects_omap;
  num_objects_hit_set_archive += o.num_objects_hit_set_archive;
  num_bytes_hit_set_archive += o.num_bytes_hit_set_archive;
  num_flush += o.num_flush;
  num_flush_kb += o.num_flush_kb;
  num_evict += o.num_evict;
  num_evict_kb += o.num_evict_kb;
  num_promote += o.num_promote;
  num_flush_mode_high += o.num_flush_mode_high;
  num_flush_mode_low += o.num_flush_mode_low;
  num_evict_mode_some += o.num_evict_mode_some;
  num_evict_mode_full += o.num_evict_mode_full;
  num_objects_pinned += o.num_objects_pinned;
  num_legacy_snapsets += o.num_legacy_snapsets;
  num_large_omap_objects += o.num_large_omap_objects;
  num_objects_manifest += o.num_objects_manifest;
  num_omap_bytes += o.num_omap_bytes;
  num_omap_keys += o.num_omap_keys;
  num_objects_repaired += o.num_objects_repaired;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
  num_objects -= o.num_objects;
  num_object_clones -= o.num_object_clones;
  num_object_copies -= o.num_object_copies;
  num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
  num_objects_missing -= o.num_objects_missing;
  num_objects_degraded -= o.num_objects_degraded;
  num_objects_misplaced -= o.num_objects_misplaced;
  num_rd -= o.num_rd;
  num_rd_kb -= o.num_rd_kb;
  num_wr -= o.num_wr;
  num_wr_kb -= o.num_wr_kb;
  num_objects_unfound -= o.num_objects_unfound;
  num_scrub_errors -= o.num_scrub_errors;
  num_shallow_scrub_errors -= o.num_shallow_scrub_errors;
  num_deep_scrub_errors -= o.num_deep_scrub_errors;
  num_objects_recovered -= o.num_objects_recovered;
  num_bytes_recovered -= o.num_bytes_recovered;
  num_keys_recovered -= o.num_keys_recovered;
  num_objects_dirty -= o.num_objects_dirty;
  num_whiteouts -= o.num_whiteouts;
  num_objects_omap -= o.num_objects_omap;
  num_objects_hit_set_archive -= o.num_objects_hit_set_archive;
  num_bytes_hit_set_archive -= o.num_bytes_hit_set_archive;
  num_flush -= o.num_flush;
  num_flush_kb -= o.num_flush_kb;
  num_evict -= o.num_evict;
  num_evict_kb -= o.num_evict_kb;
  num_promote -= o.num_promote;
  num_flush_mode_high -= o.num_flush_mode_high;
  num_flush_mode_low -= o.num_flush_mode_low;
  num_evict_mode_some -= o.num_evict_mode_some;
  num_evict_mode_full -= o.num_evict_mode_full;
  num_objects_pinned -= o.num_objects_pinned;
  num_legacy_snapsets -= o.num_legacy_snapsets;
  num_large_omap_objects -= o.num_large_omap_objects;
  num_objects_manifest -= o.num_objects_manifest;
  num_omap_bytes -= o.num_omap_bytes;
  num_omap_keys -= o.num_omap_keys;
  num_objects_repaired -= o.num_objects_repaired;
}

bool operator==(const object_stat_sum_t& l, const object_stat_sum_t& r)
{
  return
    l.num_bytes == r.num_bytes &&
    l.num_objects == r.num_objects &&
    l.num_object_clones == r.num_object_clones &&
    l.num_object_copies == r.num_object_copies &&
    l.num_objects_missing_on_primary == r.num_objects_missing_on_primary &&
    l.num_objects_missing == r.num_objects_missing &&
    l.num_objects_degraded == r.num_objects_degraded &&
    l.num_objects_misplaced == r.num_objects_misplaced &&
    l.num_objects_unfound == r.num_objects_unfound &&
    l.num_rd == r.num_rd &&
    l.num_rd_kb == r.num_rd_kb &&
    l.num_wr == r.num_wr &&
    l.num_wr_kb == r.num_wr_kb &&
    l.num_scrub_errors == r.num_scrub_errors &&
    l.num_shallow_scrub_errors == r.num_shallow_scrub_errors &&
    l.num_deep_scrub_errors == r.num_deep_scrub_errors &&
    l.num_objects_recovered == r.num_objects_recovered &&
    l.num_bytes_recovered == r.num_bytes_recovered &&
    l.num_keys_recovered == r.num_keys_recovered &&
    l.num_objects_dirty == r.num_objects_dirty &&
    l.num_whiteouts == r.num_whiteouts &&
    l.num_objects_omap == r.num_objects_omap &&
    l.num_objects_hit_set_archive == r.num_objects_hit_set_archive &&
    l.num_bytes_hit_set_archive == r.num_bytes_hit_set_archive &&
    l.num_flush == r.num_flush &&
    l.num_flush_kb == r.num_flush_kb &&
    l.num_evict == r.num_evict &&
    l.num_evict_kb == r.num_evict_kb &&
    l.num_promote == r.num_promote &&
    l.num_flush_mode_high == r.num_flush_mode_high &&
    l.num_flush_mode_low == r.num_flush_mode_low &&
    l.num_evict_mode_some == r.num_evict_mode_some &&
    l.num_evict_mode_full == r.num_evict_mode_full &&
    l.num_objects_pinned == r.num_objects_pinned &&
    l.num_legacy_snapsets == r.num_legacy_snapsets &&
    l.num_large_omap_objects == r.num_large_omap_objects &&
    l.num_objects_manifest == r.num_objects_manifest &&
    l.num_omap_bytes == r.num_omap_bytes &&
    l.num_omap_keys == r.num_omap_keys &&
    l.num_objects_repaired == r.num_objects_repaired;
}

// -- object_stat_collection_t --

void object_stat_collection_t::dump(Formatter *f) const
{
  f->open_object_section("stat_sum");
  sum.dump(f);
  f->close_section();
}

void object_stat_collection_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(sum, bl);
  encode((__u32)0, bl);
  ENCODE_FINISH(bl);
}

void object_stat_collection_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(sum, bl);
  {
    map<string,object_stat_sum_t> cat_sum;
    decode(cat_sum, bl);
  }
  DECODE_FINISH(bl);
}

void object_stat_collection_t::generate_test_instances(list<object_stat_collection_t*>& o)
{
  object_stat_collection_t a;
  o.push_back(new object_stat_collection_t(a));
  list<object_stat_sum_t*> l;
  object_stat_sum_t::generate_test_instances(l);
  for (auto p = l.begin(); p != l.end(); ++p) {
    a.add(**p);
    o.push_back(new object_stat_collection_t(a));
  }
}


// -- pg_stat_t --

bool pg_stat_t::is_acting_osd(int32_t osd, bool primary) const
{
  if (primary && osd == acting_primary) {
    return true;
  } else if (!primary) {
    for(auto it = acting.cbegin(); it != acting.cend(); ++it)
    {
      if (*it == osd)
        return true;
    }
  }
  return false;
}

void pg_stat_t::dump(Formatter *f) const
{
  f->dump_stream("version") << version;
  f->dump_stream("reported_seq") << reported_seq;
  f->dump_stream("reported_epoch") << reported_epoch;
  f->dump_string("state", pg_state_string(state));
  f->dump_stream("last_fresh") << last_fresh;
  f->dump_stream("last_change") << last_change;
  f->dump_stream("last_active") << last_active;
  f->dump_stream("last_peered") << last_peered;
  f->dump_stream("last_clean") << last_clean;
  f->dump_stream("last_became_active") << last_became_active;
  f->dump_stream("last_became_peered") << last_became_peered;
  f->dump_stream("last_unstale") << last_unstale;
  f->dump_stream("last_undegraded") << last_undegraded;
  f->dump_stream("last_fullsized") << last_fullsized;
  f->dump_unsigned("mapping_epoch", mapping_epoch);
  f->dump_stream("log_start") << log_start;
  f->dump_stream("ondisk_log_start") << ondisk_log_start;
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_epoch_clean", last_epoch_clean);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
  f->dump_int("log_size", log_size);
  f->dump_int("ondisk_log_size", ondisk_log_size);
  f->dump_bool("stats_invalid", stats_invalid);
  f->dump_bool("dirty_stats_invalid", dirty_stats_invalid);
  f->dump_bool("omap_stats_invalid", omap_stats_invalid);
  f->dump_bool("hitset_stats_invalid", hitset_stats_invalid);
  f->dump_bool("hitset_bytes_stats_invalid", hitset_bytes_stats_invalid);
  f->dump_bool("pin_stats_invalid", pin_stats_invalid);
  f->dump_bool("manifest_stats_invalid", manifest_stats_invalid);
  f->dump_unsigned("snaptrimq_len", snaptrimq_len);
  stats.dump(f);
  f->open_array_section("up");
  for (auto p = up.cbegin(); p != up.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (auto p = acting.cbegin(); p != acting.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("avail_no_missing");
  for (auto p = avail_no_missing.cbegin(); p != avail_no_missing.cend(); ++p)
    f->dump_stream("shard") << *p;
  f->close_section();
  f->open_array_section("object_location_counts");
  for (auto p = object_location_counts.cbegin(); p != object_location_counts.cend(); ++p) {
    f->open_object_section("entry");
    f->dump_stream("shards") << p->first;
    f->dump_int("objects", p->second);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("blocked_by");
  for (auto p = blocked_by.cbegin(); p != blocked_by.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->dump_int("up_primary", up_primary);
  f->dump_int("acting_primary", acting_primary);
  f->open_array_section("purged_snaps");
  for (auto i = purged_snaps.begin(); i != purged_snaps.end(); ++i) {
    f->open_object_section("interval");
    f->dump_stream("start") << i.get_start();
    f->dump_stream("length") << i.get_len();
    f->close_section();
  }
  f->close_section();
}

void pg_stat_t::dump_brief(Formatter *f) const
{
  f->dump_string("state", pg_state_string(state));
  f->open_array_section("up");
  for (auto p = up.cbegin(); p != up.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (auto p = acting.cbegin(); p != acting.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->dump_int("up_primary", up_primary);
  f->dump_int("acting_primary", acting_primary);
}

void pg_stat_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(26, 22, bl);
  encode(version, bl);
  encode(reported_seq, bl);
  encode(reported_epoch, bl);
  encode((__u32)state, bl);   // for older peers
  encode(log_start, bl);
  encode(ondisk_log_start, bl);
  encode(created, bl);
  encode(last_epoch_clean, bl);
  encode(parent, bl);
  encode(parent_split_bits, bl);
  encode(last_scrub, bl);
  encode(last_scrub_stamp, bl);
  encode(stats, bl);
  encode(log_size, bl);
  encode(ondisk_log_size, bl);
  encode(up, bl);
  encode(acting, bl);
  encode(last_fresh, bl);
  encode(last_change, bl);
  encode(last_active, bl);
  encode(last_clean, bl);
  encode(last_unstale, bl);
  encode(mapping_epoch, bl);
  encode(last_deep_scrub, bl);
  encode(last_deep_scrub_stamp, bl);
  encode(stats_invalid, bl);
  encode(last_clean_scrub_stamp, bl);
  encode(last_became_active, bl);
  encode(dirty_stats_invalid, bl);
  encode(up_primary, bl);
  encode(acting_primary, bl);
  encode(omap_stats_invalid, bl);
  encode(hitset_stats_invalid, bl);
  encode(blocked_by, bl);
  encode(last_undegraded, bl);
  encode(last_fullsized, bl);
  encode(hitset_bytes_stats_invalid, bl);
  encode(last_peered, bl);
  encode(last_became_peered, bl);
  encode(pin_stats_invalid, bl);
  encode(snaptrimq_len, bl);
  __u32 top_state = (state >> 32);
  encode(top_state, bl);
  encode(purged_snaps, bl);
  encode(manifest_stats_invalid, bl);
  encode(avail_no_missing, bl);
  encode(object_location_counts, bl);
  ENCODE_FINISH(bl);
}

void pg_stat_t::decode(ceph::buffer::list::const_iterator &bl)
{
  bool tmp;
  uint32_t old_state;
  DECODE_START(26, bl);
  decode(version, bl);
  decode(reported_seq, bl);
  decode(reported_epoch, bl);
  decode(old_state, bl);
  decode(log_start, bl);
  decode(ondisk_log_start, bl);
  decode(created, bl);
  decode(last_epoch_clean, bl);
  decode(parent, bl);
  decode(parent_split_bits, bl);
  decode(last_scrub, bl);
  decode(last_scrub_stamp, bl);
  decode(stats, bl);
  decode(log_size, bl);
  decode(ondisk_log_size, bl);
  decode(up, bl);
  decode(acting, bl);
  decode(last_fresh, bl);
  decode(last_change, bl);
  decode(last_active, bl);
  decode(last_clean, bl);
  decode(last_unstale, bl);
  decode(mapping_epoch, bl);
  decode(last_deep_scrub, bl);
  decode(last_deep_scrub_stamp, bl);
  decode(tmp, bl);
  stats_invalid = tmp;
  decode(last_clean_scrub_stamp, bl);
  decode(last_became_active, bl);
  decode(tmp, bl);
  dirty_stats_invalid = tmp;
  decode(up_primary, bl);
  decode(acting_primary, bl);
  decode(tmp, bl);
  omap_stats_invalid = tmp;
  decode(tmp, bl);
  hitset_stats_invalid = tmp;
  decode(blocked_by, bl);
  decode(last_undegraded, bl);
  decode(last_fullsized, bl);
  decode(tmp, bl);
  hitset_bytes_stats_invalid = tmp;
  decode(last_peered, bl);
  decode(last_became_peered, bl);
  decode(tmp, bl);
  pin_stats_invalid = tmp;
  if (struct_v >= 23) {
    decode(snaptrimq_len, bl);
    if (struct_v >= 24) {
      __u32 top_state;
      decode(top_state, bl);
      state = (uint64_t)old_state | ((uint64_t)top_state << 32);
      decode(purged_snaps, bl);
    } else {
      state = old_state;
    }
    if (struct_v >= 25) {
      decode(tmp, bl);
      manifest_stats_invalid = tmp;
    } else {
      manifest_stats_invalid = true;
    }
    if (struct_v >= 26) {
      decode(avail_no_missing, bl);
      decode(object_location_counts, bl);
    }
  }
  DECODE_FINISH(bl);
}

void pg_stat_t::generate_test_instances(list<pg_stat_t*>& o)
{
  pg_stat_t a;
  o.push_back(new pg_stat_t(a));

  a.version = eversion_t(1, 3);
  a.reported_epoch = 1;
  a.reported_seq = 2;
  a.state = 123;
  a.mapping_epoch = 998;
  a.last_fresh = utime_t(1002, 1);
  a.last_change = utime_t(1002, 2);
  a.last_active = utime_t(1002, 3);
  a.last_clean = utime_t(1002, 4);
  a.last_unstale = utime_t(1002, 5);
  a.last_undegraded = utime_t(1002, 7);
  a.last_fullsized = utime_t(1002, 8);
  a.log_start = eversion_t(1, 4);
  a.ondisk_log_start = eversion_t(1, 5);
  a.created = 6;
  a.last_epoch_clean = 7;
  a.parent = pg_t(1, 2);
  a.parent_split_bits = 12;
  a.last_scrub = eversion_t(9, 10);
  a.last_scrub_stamp = utime_t(11, 12);
  a.last_deep_scrub = eversion_t(13, 14);
  a.last_deep_scrub_stamp = utime_t(15, 16);
  a.last_clean_scrub_stamp = utime_t(17, 18);
  a.snaptrimq_len = 1048576;
  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.log_size = 99;
  a.ondisk_log_size = 88;
  a.up.push_back(123);
  a.up_primary = 123;
  a.acting.push_back(456);
  a.avail_no_missing.push_back(pg_shard_t(456, shard_id_t::NO_SHARD));
  set<pg_shard_t> sset = { pg_shard_t(0), pg_shard_t(1) };
  a.object_location_counts.insert(make_pair(sset, 10));
  sset.insert(pg_shard_t(2));
  a.object_location_counts.insert(make_pair(sset, 5));
  a.acting_primary = 456;
  o.push_back(new pg_stat_t(a));

  a.up.push_back(124);
  a.up_primary = 124;
  a.acting.push_back(124);
  a.acting_primary = 124;
  a.blocked_by.push_back(155);
  a.blocked_by.push_back(156);
  o.push_back(new pg_stat_t(a));
}

bool operator==(const pg_stat_t& l, const pg_stat_t& r)
{
  return
    l.version == r.version &&
    l.reported_seq == r.reported_seq &&
    l.reported_epoch == r.reported_epoch &&
    l.state == r.state &&
    l.last_fresh == r.last_fresh &&
    l.last_change == r.last_change &&
    l.last_active == r.last_active &&
    l.last_peered == r.last_peered &&
    l.last_clean == r.last_clean &&
    l.last_unstale == r.last_unstale &&
    l.last_undegraded == r.last_undegraded &&
    l.last_fullsized == r.last_fullsized &&
    l.log_start == r.log_start &&
    l.ondisk_log_start == r.ondisk_log_start &&
    l.created == r.created &&
    l.last_epoch_clean == r.last_epoch_clean &&
    l.parent == r.parent &&
    l.parent_split_bits == r.parent_split_bits &&
    l.last_scrub == r.last_scrub &&
    l.last_deep_scrub == r.last_deep_scrub &&
    l.last_scrub_stamp == r.last_scrub_stamp &&
    l.last_deep_scrub_stamp == r.last_deep_scrub_stamp &&
    l.last_clean_scrub_stamp == r.last_clean_scrub_stamp &&
    l.stats == r.stats &&
    l.stats_invalid == r.stats_invalid &&
    l.log_size == r.log_size &&
    l.ondisk_log_size == r.ondisk_log_size &&
    l.up == r.up &&
    l.acting == r.acting &&
    l.avail_no_missing == r.avail_no_missing &&
    l.object_location_counts == r.object_location_counts &&
    l.mapping_epoch == r.mapping_epoch &&
    l.blocked_by == r.blocked_by &&
    l.last_became_active == r.last_became_active &&
    l.last_became_peered == r.last_became_peered &&
    l.dirty_stats_invalid == r.dirty_stats_invalid &&
    l.omap_stats_invalid == r.omap_stats_invalid &&
    l.hitset_stats_invalid == r.hitset_stats_invalid &&
    l.hitset_bytes_stats_invalid == r.hitset_bytes_stats_invalid &&
    l.up_primary == r.up_primary &&
    l.acting_primary == r.acting_primary &&
    l.pin_stats_invalid == r.pin_stats_invalid &&
    l.manifest_stats_invalid == r.manifest_stats_invalid &&
    l.purged_snaps == r.purged_snaps &&
    l.snaptrimq_len == r.snaptrimq_len;
}

// -- store_statfs_t --

bool store_statfs_t::operator==(const store_statfs_t& other) const
{
  return total == other.total
    && available == other.available
    && allocated == other.allocated
    && internally_reserved == other.internally_reserved
    && data_stored == other.data_stored
    && data_compressed == other.data_compressed
    && data_compressed_allocated == other.data_compressed_allocated
    && data_compressed_original == other.data_compressed_original
    && omap_allocated == other.omap_allocated
    && internal_metadata == other.internal_metadata;
}

void store_statfs_t::dump(Formatter *f) const
{
  f->dump_int("total", total);
  f->dump_int("available", available);
  f->dump_int("internally_reserved", internally_reserved);
  f->dump_int("allocated", allocated);
  f->dump_int("data_stored", data_stored);
  f->dump_int("data_compressed", data_compressed);
  f->dump_int("data_compressed_allocated", data_compressed_allocated);
  f->dump_int("data_compressed_original", data_compressed_original);
  f->dump_int("omap_allocated", omap_allocated);
  f->dump_int("internal_metadata", internal_metadata);
}

ostream& operator<<(ostream& out, const store_statfs_t &s)
{
  out << std::hex
      << "store_statfs(0x" << s.available
      << "/0x"  << s.internally_reserved
      << "/0x"  << s.total
      << ", data 0x" << s.data_stored
      << "/0x"  << s.allocated
      << ", compress 0x" << s.data_compressed
      << "/0x"  << s.data_compressed_allocated
      << "/0x"  << s.data_compressed_original
      << ", omap 0x" << s.omap_allocated
      << ", meta 0x" << s.internal_metadata
      << std::dec
      << ")";
  return out;
}

void store_statfs_t::generate_test_instances(list<store_statfs_t*>& o)
{
  store_statfs_t a;
  o.push_back(new store_statfs_t(a));
  a.total = 234;
  a.available = 123;
  a.internally_reserved = 33;
  a.allocated = 32;
  a.data_stored = 44;
  a.data_compressed = 21;
  a.data_compressed_allocated = 12;
  a.data_compressed_original = 13;
  a.omap_allocated = 14;
  a.internal_metadata = 15;
  o.push_back(new store_statfs_t(a));
}

// -- pool_stat_t --

void pool_stat_t::dump(Formatter *f) const
{
  stats.dump(f);
  f->open_object_section("store_stats");
  store_stats.dump(f);
  f->close_section();
  f->dump_int("log_size", log_size);
  f->dump_int("ondisk_log_size", ondisk_log_size);
  f->dump_int("up", up);
  f->dump_int("acting", acting);
  f->dump_int("num_store_stats", num_store_stats);
}

void pool_stat_t::encode(ceph::buffer::list &bl, uint64_t features) const
{
  using ceph::encode;
  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 v = 4;
    encode(v, bl);
    encode(stats, bl);
    encode(log_size, bl);
    encode(ondisk_log_size, bl);
    return;
  }

  ENCODE_START(7, 5, bl);
  encode(stats, bl);
  encode(log_size, bl);
  encode(ondisk_log_size, bl);
  encode(up, bl);
  encode(acting, bl);
  encode(store_stats, bl);
  encode(num_store_stats, bl);
  ENCODE_FINISH(bl);
}

void pool_stat_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  if (struct_v >= 4) {
    decode(stats, bl);
    decode(log_size, bl);
    decode(ondisk_log_size, bl);
    if (struct_v >= 6) {
      decode(up, bl);
      decode(acting, bl);
    } else {
      up = 0;
      acting = 0;
    }
    if (struct_v >= 7) {
      decode(store_stats, bl);
      decode(num_store_stats, bl);
    } else {
      store_stats.reset();
      num_store_stats = 0;
    }

  } else {
    decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    decode(num_kb, bl);
    decode(stats.sum.num_objects, bl);
    decode(stats.sum.num_object_clones, bl);
    decode(stats.sum.num_object_copies, bl);
    decode(stats.sum.num_objects_missing_on_primary, bl);
    decode(stats.sum.num_objects_degraded, bl);
    decode(log_size, bl);
    decode(ondisk_log_size, bl);
    if (struct_v >= 2) {
      decode(stats.sum.num_rd, bl);
      decode(stats.sum.num_rd_kb, bl);
      decode(stats.sum.num_wr, bl);
      decode(stats.sum.num_wr_kb, bl);
    }
    if (struct_v >= 3) {
      decode(stats.sum.num_objects_unfound, bl);
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
  list<store_statfs_t*> ll;
  store_statfs_t::generate_test_instances(ll);
  a.stats = *l.back();
  a.store_stats = *ll.back();
  a.log_size = 123;
  a.ondisk_log_size = 456;
  a.acting = 3;
  a.up = 4;
  a.num_store_stats = 1;
  o.push_back(new pool_stat_t(a));
}


// -- pg_history_t --

void pg_history_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(10, 4, bl);
  encode(epoch_created, bl);
  encode(last_epoch_started, bl);
  encode(last_epoch_clean, bl);
  encode(last_epoch_split, bl);
  encode(same_interval_since, bl);
  encode(same_up_since, bl);
  encode(same_primary_since, bl);
  encode(last_scrub, bl);
  encode(last_scrub_stamp, bl);
  encode(last_deep_scrub, bl);
  encode(last_deep_scrub_stamp, bl);
  encode(last_clean_scrub_stamp, bl);
  encode(last_epoch_marked_full, bl);
  encode(last_interval_started, bl);
  encode(last_interval_clean, bl);
  encode(epoch_pool_created, bl);
  encode(prior_readable_until_ub, bl);
  ENCODE_FINISH(bl);
}

void pg_history_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(10, 4, 4, bl);
  decode(epoch_created, bl);
  decode(last_epoch_started, bl);
  if (struct_v >= 3)
    decode(last_epoch_clean, bl);
  else
    last_epoch_clean = last_epoch_started;  // careful, it's a lie!
  decode(last_epoch_split, bl);
  decode(same_interval_since, bl);
  decode(same_up_since, bl);
  decode(same_primary_since, bl);
  if (struct_v >= 2) {
    decode(last_scrub, bl);
    decode(last_scrub_stamp, bl);
  }
  if (struct_v >= 5) {
    decode(last_deep_scrub, bl);
    decode(last_deep_scrub_stamp, bl);
  }
  if (struct_v >= 6) {
    decode(last_clean_scrub_stamp, bl);
  }
  if (struct_v >= 7) {
    decode(last_epoch_marked_full, bl);
  }
  if (struct_v >= 8) {
    decode(last_interval_started, bl);
    decode(last_interval_clean, bl);
  } else {
    if (last_epoch_started >= same_interval_since) {
      last_interval_started = same_interval_since;
    } else {
      last_interval_started = last_epoch_started; // best guess
    }
    if (last_epoch_clean >= same_interval_since) {
      last_interval_clean = same_interval_since;
    } else {
      last_interval_clean = last_epoch_clean; // best guess
    }
  }
  if (struct_v >= 9) {
    decode(epoch_pool_created, bl);
  } else {
    epoch_pool_created = epoch_created;
  }
  if (struct_v >= 10) {
    decode(prior_readable_until_ub, bl);
  }
  DECODE_FINISH(bl);
}

void pg_history_t::dump(Formatter *f) const
{
  f->dump_int("epoch_created", epoch_created);
  f->dump_int("epoch_pool_created", epoch_pool_created);
  f->dump_int("last_epoch_started", last_epoch_started);
  f->dump_int("last_interval_started", last_interval_started);
  f->dump_int("last_epoch_clean", last_epoch_clean);
  f->dump_int("last_interval_clean", last_interval_clean);
  f->dump_int("last_epoch_split", last_epoch_split);
  f->dump_int("last_epoch_marked_full", last_epoch_marked_full);
  f->dump_int("same_up_since", same_up_since);
  f->dump_int("same_interval_since", same_interval_since);
  f->dump_int("same_primary_since", same_primary_since);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
  f->dump_float(
    "prior_readable_until_ub",
    std::chrono::duration<double>(prior_readable_until_ub).count());
}

void pg_history_t::generate_test_instances(list<pg_history_t*>& o)
{
  o.push_back(new pg_history_t);
  o.push_back(new pg_history_t);
  o.back()->epoch_created = 1;
  o.back()->epoch_pool_created = 1;
  o.back()->last_epoch_started = 2;
  o.back()->last_interval_started = 2;
  o.back()->last_epoch_clean = 3;
  o.back()->last_interval_clean = 2;
  o.back()->last_epoch_split = 4;
  o.back()->prior_readable_until_ub = make_timespan(3.1415);
  o.back()->same_up_since = 5;
  o.back()->same_interval_since = 6;
  o.back()->same_primary_since = 7;
  o.back()->last_scrub = eversion_t(8, 9);
  o.back()->last_scrub_stamp = utime_t(10, 11);
  o.back()->last_deep_scrub = eversion_t(12, 13);
  o.back()->last_deep_scrub_stamp = utime_t(14, 15);
  o.back()->last_clean_scrub_stamp = utime_t(16, 17);
  o.back()->last_epoch_marked_full = 18;
}


// -- pg_info_t --

void pg_info_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(32, 26, bl);
  encode(pgid.pgid, bl);
  encode(last_update, bl);
  encode(last_complete, bl);
  encode(log_tail, bl);
  encode(hobject_t(), bl);  // old (nibblewise) last_backfill
  encode(stats, bl);
  history.encode(bl);
  encode(purged_snaps, bl);
  encode(last_epoch_started, bl);
  encode(last_user_version, bl);
  encode(hit_set, bl);
  encode(pgid.shard, bl);
  encode(last_backfill, bl);
  encode(true, bl); // was last_backfill_bitwise
  encode(last_interval_started, bl);
  ENCODE_FINISH(bl);
}

void pg_info_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(32, bl);
  decode(pgid.pgid, bl);
  decode(last_update, bl);
  decode(last_complete, bl);
  decode(log_tail, bl);
  {
    hobject_t old_last_backfill;
    decode(old_last_backfill, bl);
  }
  decode(stats, bl);
  history.decode(bl);
  decode(purged_snaps, bl);
  decode(last_epoch_started, bl);
  decode(last_user_version, bl);
  decode(hit_set, bl);
  decode(pgid.shard, bl);
  decode(last_backfill, bl);
  {
    bool last_backfill_bitwise;
    decode(last_backfill_bitwise, bl);
    // note: we may see a false value here since the default value for
    // the member was false, so it often didn't get set to true until
    // peering progressed.
  }
  if (struct_v >= 32) {
    decode(last_interval_started, bl);
  } else {
    last_interval_started = last_epoch_started;
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
  f->dump_int("last_user_version", last_user_version);
  f->dump_stream("last_backfill") << last_backfill;
  f->open_array_section("purged_snaps");
  for (interval_set<snapid_t>::const_iterator i=purged_snaps.begin();
       i != purged_snaps.end();
       ++i) {
    f->open_object_section("purged_snap_interval");
    f->dump_stream("start") << i.get_start();
    f->dump_stream("length") << i.get_len();
    f->close_section();
  }
  f->close_section();
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

  f->open_object_section("hit_set_history");
  hit_set.dump(f);
  f->close_section();
}

void pg_info_t::generate_test_instances(list<pg_info_t*>& o)
{
  o.push_back(new pg_info_t);
  o.push_back(new pg_info_t);
  list<pg_history_t*> h;
  pg_history_t::generate_test_instances(h);
  o.back()->history = *h.back();
  o.back()->pgid = spg_t(pg_t(1, 2), shard_id_t::NO_SHARD);
  o.back()->last_update = eversion_t(3, 4);
  o.back()->last_complete = eversion_t(5, 6);
  o.back()->last_user_version = 2;
  o.back()->log_tail = eversion_t(7, 8);
  o.back()->last_backfill = hobject_t(object_t("objname"), "key", 123, 456, -1, "");
  {
    list<pg_stat_t*> s;
    pg_stat_t::generate_test_instances(s);
    o.back()->stats = *s.back();
  }
  {
    list<pg_hit_set_history_t*> s;
    pg_hit_set_history_t::generate_test_instances(s);
    o.back()->hit_set = *s.back();
  }
}

// -- pg_notify_t --
void pg_notify_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(query_epoch, bl);
  encode(epoch_sent, bl);
  encode(info, bl);
  encode(to, bl);
  encode(from, bl);
  encode(past_intervals, bl);
  ENCODE_FINISH(bl);
}

void pg_notify_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(3, bl);
  decode(query_epoch, bl);
  decode(epoch_sent, bl);
  decode(info, bl);
  decode(to, bl);
  decode(from, bl);
  if (struct_v >= 3) {
    decode(past_intervals, bl);
  }
  DECODE_FINISH(bl);
}

void pg_notify_t::dump(Formatter *f) const
{
  f->dump_int("from", from);
  f->dump_int("to", to);
  f->dump_unsigned("query_epoch", query_epoch);
  f->dump_unsigned("epoch_sent", epoch_sent);
  {
    f->open_object_section("info");
    info.dump(f);
    f->close_section();
  }
  f->dump_object("past_intervals", past_intervals);
}

void pg_notify_t::generate_test_instances(list<pg_notify_t*>& o)
{
  o.push_back(new pg_notify_t(shard_id_t(3), shard_id_t::NO_SHARD, 1, 1,
			      pg_info_t(), PastIntervals()));
  o.push_back(new pg_notify_t(shard_id_t(0), shard_id_t(0), 3, 10,
			      pg_info_t(), PastIntervals()));
}

ostream &operator<<(ostream &lhs, const pg_notify_t &notify)
{
  lhs << "(query:" << notify.query_epoch
      << " sent:" << notify.epoch_sent
      << " " << notify.info;
  if (notify.from != shard_id_t::NO_SHARD ||
      notify.to != shard_id_t::NO_SHARD)
    lhs << " " << (unsigned)notify.from
	<< "->" << (unsigned)notify.to;
  lhs << " " << notify.past_intervals;
  return lhs << ")";
}

// -- pg_interval_t --

void PastIntervals::pg_interval_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(4, 2, bl);
  encode(first, bl);
  encode(last, bl);
  encode(up, bl);
  encode(acting, bl);
  encode(maybe_went_rw, bl);
  encode(primary, bl);
  encode(up_primary, bl);
  ENCODE_FINISH(bl);
}

void PastIntervals::pg_interval_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
  decode(first, bl);
  decode(last, bl);
  decode(up, bl);
  decode(acting, bl);
  decode(maybe_went_rw, bl);
  if (struct_v >= 3) {
    decode(primary, bl);
  } else {
    if (acting.size())
      primary = acting[0];
  }
  if (struct_v >= 4) {
    decode(up_primary, bl);
  } else {
    if (up.size())
      up_primary = up[0];
  }
  DECODE_FINISH(bl);
}

void PastIntervals::pg_interval_t::dump(Formatter *f) const
{
  f->dump_unsigned("first", first);
  f->dump_unsigned("last", last);
  f->dump_int("maybe_went_rw", maybe_went_rw ? 1 : 0);
  f->open_array_section("up");
  for (auto p = up.cbegin(); p != up.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (auto p = acting.cbegin(); p != acting.cend(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->dump_int("primary", primary);
  f->dump_int("up_primary", up_primary);
}

void PastIntervals::pg_interval_t::generate_test_instances(list<pg_interval_t*>& o)
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

WRITE_CLASS_ENCODER(PastIntervals::pg_interval_t)


/**
 * pi_compact_rep
 *
 * PastIntervals only needs to be able to answer two questions:
 * 1) Where should the primary look for unfound objects?
 * 2) List a set of subsets of the OSDs such that contacting at least
 *    one from each subset guarantees we speak to at least one witness
 *    of any completed write.
 *
 * Crucially, 2) does not require keeping *all* past intervals.  Certainly,
 * we don't need to keep any where maybe_went_rw would be false.  We also
 * needn't keep two intervals where the actingset in one is a subset
 * of the other (only need to keep the smaller of the two sets).  In order
 * to accurately trim the set of intervals as last_epoch_started changes
 * without rebuilding the set from scratch, we'll retain the larger set
 * if it in an older interval.
 */
struct compact_interval_t {
  epoch_t first;
  epoch_t last;
  set<pg_shard_t> acting;
  bool supersedes(const compact_interval_t &other) {
    for (auto &&i: acting) {
      if (!other.acting.count(i))
	return false;
    }
    return true;
  }
  void dump(Formatter *f) const {
    f->open_object_section("compact_interval_t");
    f->dump_stream("first") << first;
    f->dump_stream("last") << last;
    f->dump_stream("acting") << acting;
    f->close_section();
  }
  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(first, bl);
    encode(last, bl);
    encode(acting, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(first, bl);
    decode(last, bl);
    decode(acting, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(list<compact_interval_t*> & o) {
    /* Not going to be used, we'll generate pi_compact_rep directly */
  }
};
ostream &operator<<(ostream &o, const compact_interval_t &rhs)
{
  return o << "([" << rhs.first << "," << rhs.last
	   << "] acting " << rhs.acting << ")";
}
WRITE_CLASS_ENCODER(compact_interval_t)

class pi_compact_rep : public PastIntervals::interval_rep {
  epoch_t first = 0;
  epoch_t last = 0; // inclusive
  set<pg_shard_t> all_participants;
  list<compact_interval_t> intervals;
  pi_compact_rep(
    bool ec_pool,
    std::list<PastIntervals::pg_interval_t> &&intervals) {
    for (auto &&i: intervals)
      add_interval(ec_pool, i);
  }
public:
  pi_compact_rep() = default;
  pi_compact_rep(const pi_compact_rep &) = default;
  pi_compact_rep(pi_compact_rep &&) = default;
  pi_compact_rep &operator=(const pi_compact_rep &) = default;
  pi_compact_rep &operator=(pi_compact_rep &&) = default;

  size_t size() const override { return intervals.size(); }
  bool empty() const override {
    return first > last || (first == 0 && last == 0);
  }
  void clear() override {
    *this = pi_compact_rep();
  }
  pair<epoch_t, epoch_t> get_bounds() const override {
    return make_pair(first, last + 1);
  }
  void adjust_start_backwards(epoch_t last_epoch_clean) override {
    first = last_epoch_clean;
  }

  set<pg_shard_t> get_all_participants(
    bool ec_pool) const override {
    return all_participants;
  }
  void add_interval(
    bool ec_pool, const PastIntervals::pg_interval_t &interval) override {
    if (first == 0)
      first = interval.first;
    ceph_assert(interval.last > last);
    last = interval.last;
    set<pg_shard_t> acting;
    for (unsigned i = 0; i < interval.acting.size(); ++i) {
      if (interval.acting[i] == CRUSH_ITEM_NONE)
	continue;
      acting.insert(
	pg_shard_t(
	  interval.acting[i],
	  ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    all_participants.insert(acting.begin(), acting.end());
    if (!interval.maybe_went_rw)
      return;
    intervals.push_back(
      compact_interval_t{interval.first, interval.last, acting});
    auto plast = intervals.end();
    --plast;
    for (auto cur = intervals.begin(); cur != plast; ) {
      if (plast->supersedes(*cur)) {
	intervals.erase(cur++);
      } else {
	++cur;
      }
    }
  }
  unique_ptr<PastIntervals::interval_rep> clone() const override {
    return unique_ptr<PastIntervals::interval_rep>(new pi_compact_rep(*this));
  }
  ostream &print(ostream &out) const override {
    return out << "([" << first << "," << last
	       << "] all_participants=" << all_participants
	       << " intervals=" << intervals << ")";
  }
  void encode(ceph::buffer::list &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(first, bl);
    encode(last, bl);
    encode(all_participants, bl);
    encode(intervals, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) override {
    DECODE_START(1, bl);
    decode(first, bl);
    decode(last, bl);
    decode(all_participants, bl);
    decode(intervals, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override {
    f->open_object_section("PastIntervals::compact_rep");
    f->dump_stream("first") << first;
    f->dump_stream("last") << last;
    f->open_array_section("all_participants");
    for (auto& i : all_participants) {
      f->dump_object("pg_shard", i);
    }
    f->close_section();
    f->open_array_section("intervals");
    for (auto &&i: intervals) {
      i.dump(f);
    }
    f->close_section();
    f->close_section();
  }
  static void generate_test_instances(list<pi_compact_rep*> &o) {
    using ival = PastIntervals::pg_interval_t;
    using ivallst = std::list<ival>;
    o.push_back(
      new pi_compact_rep(
	true, ivallst
	{ ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
	, ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
	, ival{{      2}, {      2}, 31, 35, false, 2, 2}
	, ival{{0,    2}, {0,    2}, 36, 50,  true, 0, 0}
	}));
    o.push_back(
      new pi_compact_rep(
	false, ivallst
	{ ival{{0, 1, 2}, {0, 1, 2}, 10, 20,  true, 0, 0}
	, ival{{   1, 2}, {   1, 2}, 21, 30,  true, 1, 1}
	, ival{{      2}, {      2}, 31, 35, false, 2, 2}
	, ival{{0,    2}, {0,    2}, 36, 50,  true, 0, 0}
	}));
    o.push_back(
      new pi_compact_rep(
	true, ivallst
	{ ival{{2, 1, 0}, {2, 1, 0}, 10, 20,  true, 1, 1}
	, ival{{   0, 2}, {   0, 2}, 21, 30,  true, 0, 0}
	, ival{{   0, 2}, {2,    0}, 31, 35,  true, 2, 2}
	, ival{{   0, 2}, {   0, 2}, 36, 50,  true, 0, 0}
	}));
  }
  void iterate_mayberw_back_to(
    epoch_t les,
    std::function<void(epoch_t, const set<pg_shard_t> &)> &&f) const override {
    for (auto i = intervals.rbegin(); i != intervals.rend(); ++i) {
      if (i->last < les)
	break;
      f(i->first, i->acting);
    }
  }
  virtual ~pi_compact_rep() override {}
};
WRITE_CLASS_ENCODER(pi_compact_rep)

PastIntervals::PastIntervals()
{
  past_intervals.reset(new pi_compact_rep);
}

PastIntervals::PastIntervals(const PastIntervals &rhs)
  : past_intervals(rhs.past_intervals ?
		   rhs.past_intervals->clone() :
		   nullptr) {}

PastIntervals &PastIntervals::operator=(const PastIntervals &rhs)
{
  PastIntervals other(rhs);
  swap(other);
  return *this;
}

ostream& operator<<(ostream& out, const PastIntervals &i)
{
  if (i.past_intervals) {
    return i.past_intervals->print(out);
  } else {
    return out << "(empty)";
  }
}

ostream& operator<<(ostream& out, const PastIntervals::PriorSet &i)
{
  return out << "PriorSet("
	     << "ec_pool: " << i.ec_pool
	     << ", probe: " << i.probe
	     << ", down: " << i.down
	     << ", blocked_by: " << i.blocked_by
	     << ", pg_down: " << i.pg_down
	     << ")";
}

void PastIntervals::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  __u8 type = 0;
  decode(type, bl);
  switch (type) {
  case 0:
    break;
  case 1:
    ceph_abort_msg("pi_simple_rep support removed post-luminous");
    break;
  case 2:
    past_intervals.reset(new pi_compact_rep);
    past_intervals->decode(bl);
    break;
  }
  DECODE_FINISH(bl);
}

void PastIntervals::generate_test_instances(list<PastIntervals*> &o)
{
  {
    list<pi_compact_rep *> compact;
    pi_compact_rep::generate_test_instances(compact);
    for (auto &&i: compact) {
      // takes ownership of contents
      o.push_back(new PastIntervals(i));
    }
  }
  return;
}

bool PastIntervals::is_new_interval(
  int old_acting_primary,
  int new_acting_primary,
  const vector<int> &old_acting,
  const vector<int> &new_acting,
  int old_up_primary,
  int new_up_primary,
  const vector<int> &old_up,
  const vector<int> &new_up,
  int old_size,
  int new_size,
  int old_min_size,
  int new_min_size,
  unsigned old_pg_num,
  unsigned new_pg_num,
  unsigned old_pg_num_pending,
  unsigned new_pg_num_pending,
  bool old_sort_bitwise,
  bool new_sort_bitwise,
  bool old_recovery_deletes,
  bool new_recovery_deletes,
  pg_t pgid) {
  return old_acting_primary != new_acting_primary ||
    new_acting != old_acting ||
    old_up_primary != new_up_primary ||
    new_up != old_up ||
    old_min_size != new_min_size ||
    old_size != new_size ||
    pgid.is_split(old_pg_num, new_pg_num, 0) ||
    // (is or was) pre-merge source
    pgid.is_merge_source(old_pg_num_pending, new_pg_num_pending, 0) ||
    pgid.is_merge_source(new_pg_num_pending, old_pg_num_pending, 0) ||
    // merge source
    pgid.is_merge_source(old_pg_num, new_pg_num, 0) ||
    // (is or was) pre-merge target
    pgid.is_merge_target(old_pg_num_pending, new_pg_num_pending) ||
    pgid.is_merge_target(new_pg_num_pending, old_pg_num_pending) ||
    // merge target
    pgid.is_merge_target(old_pg_num, new_pg_num) ||
    old_sort_bitwise != new_sort_bitwise ||
    old_recovery_deletes != new_recovery_deletes;
}

bool PastIntervals::is_new_interval(
  int old_acting_primary,
  int new_acting_primary,
  const vector<int> &old_acting,
  const vector<int> &new_acting,
  int old_up_primary,
  int new_up_primary,
  const vector<int> &old_up,
  const vector<int> &new_up,
  const OSDMap *osdmap,
  const OSDMap *lastmap,
  pg_t pgid)
{
  const pg_pool_t *plast = lastmap->get_pg_pool(pgid.pool());
  if (!plast) {
    return false; // after pool is deleted there are no more interval changes
  }
  const pg_pool_t *pi = osdmap->get_pg_pool(pgid.pool());
  if (!pi) {
    return true;  // pool was deleted this epoch -> (final!) interval change
  }
  return
    is_new_interval(old_acting_primary,
		    new_acting_primary,
		    old_acting,
		    new_acting,
		    old_up_primary,
		    new_up_primary,
		    old_up,
		    new_up,
		    plast->size,
		    pi->size,
		    plast->min_size,
		    pi->min_size,
		    plast->get_pg_num(),
		    pi->get_pg_num(),
		    plast->get_pg_num_pending(),
		    pi->get_pg_num_pending(),
		    lastmap->test_flag(CEPH_OSDMAP_SORTBITWISE),
		    osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE),
		    lastmap->test_flag(CEPH_OSDMAP_RECOVERY_DELETES),
		    osdmap->test_flag(CEPH_OSDMAP_RECOVERY_DELETES),
		    pgid);
}

bool PastIntervals::check_new_interval(
  int old_acting_primary,
  int new_acting_primary,
  const vector<int> &old_acting,
  const vector<int> &new_acting,
  int old_up_primary,
  int new_up_primary,
  const vector<int> &old_up,
  const vector<int> &new_up,
  epoch_t same_interval_since,
  epoch_t last_epoch_clean,
  const OSDMap *osdmap,
  const OSDMap *lastmap,
  pg_t pgid,
  const IsPGRecoverablePredicate &could_have_gone_active,
  PastIntervals *past_intervals,
  std::ostream *out)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike) 
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (up_thru still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // remember past interval
  //  NOTE: a change in the up set primary triggers an interval
  //  change, even though the interval members in the pg_interval_t
  //  do not change.
  ceph_assert(past_intervals);
  ceph_assert(past_intervals->past_intervals);
  if (is_new_interval(
	old_acting_primary,
	new_acting_primary,
	old_acting,
	new_acting,
	old_up_primary,
	new_up_primary,
	old_up,
	new_up,
	osdmap,
	lastmap,
	pgid)) {
    pg_interval_t i;
    i.first = same_interval_since;
    i.last = osdmap->get_epoch() - 1;
    ceph_assert(i.first <= i.last);
    i.acting = old_acting;
    i.up = old_up;
    i.primary = old_acting_primary;
    i.up_primary = old_up_primary;

    unsigned num_acting = 0;
    for (auto p = i.acting.cbegin(); p != i.acting.cend(); ++p)
      if (*p != CRUSH_ITEM_NONE)
	++num_acting;

    ceph_assert(lastmap->get_pools().count(pgid.pool()));
    const pg_pool_t& old_pg_pool = lastmap->get_pools().find(pgid.pool())->second;
    set<pg_shard_t> old_acting_shards;
    old_pg_pool.convert_to_pg_shards(old_acting, &old_acting_shards);

    if (num_acting &&
	i.primary != -1 &&
	num_acting >= old_pg_pool.min_size &&
        could_have_gone_active(old_acting_shards)) {
      if (out)
	*out << __func__ << " " << i
	     << " up_thru " << lastmap->get_up_thru(i.primary)
	     << " up_from " << lastmap->get_up_from(i.primary)
	     << " last_epoch_clean " << last_epoch_clean;
      if (lastmap->get_up_thru(i.primary) >= i.first &&
	  lastmap->get_up_from(i.primary) <= i.first) {
	i.maybe_went_rw = true;
	if (out)
	  *out << " " << i
	       << " : primary up " << lastmap->get_up_from(i.primary)
	       << "-" << lastmap->get_up_thru(i.primary)
	       << " includes interval"
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
	  *out << " " << i
	       << " : includes last_epoch_clean " << last_epoch_clean
	       << " and presumed to have been rw"
	       << std::endl;
      } else {
	i.maybe_went_rw = false;
	if (out)
	  *out << " " << i
	       << " : primary up " << lastmap->get_up_from(i.primary)
	       << "-" << lastmap->get_up_thru(i.primary)
	       << " does not include interval"
               << std::endl;
      }
    } else {
      i.maybe_went_rw = false;
      if (out)
	*out << __func__ << " " << i << " : acting set is too small" << std::endl;
    }
    past_intervals->past_intervals->add_interval(old_pg_pool.is_erasure(), i);
    return true;
  } else {
    return false;
  }
}


// true if the given map affects the prior set
bool PastIntervals::PriorSet::affected_by_map(
  const OSDMap &osdmap,
  const DoutPrefixProvider *dpp) const
{
  for (auto p = probe.begin(); p != probe.end(); ++p) {
    int o = p->osd;

    // did someone in the prior set go down?
    if (osdmap.is_down(o) && down.count(o) == 0) {
      ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " now down" << dendl;
      return true;
    }

    // did a down osd in cur get (re)marked as lost?
    auto r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (!osdmap.exists(o)) {
	ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " no longer exists" << dendl;
	return true;
      }
      if (osdmap.get_info(o).lost_at != r->second) {
	ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
	return true;
      }
    }
  }

  // did someone in the prior down set go up?
  for (auto p = down.cbegin(); p != down.cend(); ++p) {
    int o = *p;

    if (osdmap.is_up(o)) {
      ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap.exists(o)) {
      ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " no longer exists" << dendl;
      return true;
    }
    // did a down osd in down get (re)marked as lost?
    auto r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (osdmap.get_info(o).lost_at != r->second) {
        ldpp_dout(dpp, 10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
        return true;
      }
    }
  }

  return false;
}

ostream& operator<<(ostream& out, const PastIntervals::pg_interval_t& i)
{
  out << "interval(" << i.first << "-" << i.last
      << " up " << i.up << "(" << i.up_primary << ")"
      << " acting " << i.acting << "(" << i.primary << ")";
  if (i.maybe_went_rw)
    out << " maybe_went_rw";
  out << ")";
  return out;
}



// -- pg_query_t --

void pg_query_t::encode(ceph::buffer::list &bl, uint64_t features) const {
  ENCODE_START(3, 3, bl);
  encode(type, bl);
  encode(since, bl);
  history.encode(bl);
  encode(epoch_sent, bl);
  encode(to, bl);
  encode(from, bl);
  ENCODE_FINISH(bl);
}

void pg_query_t::decode(ceph::buffer::list::const_iterator &bl) {
  DECODE_START(3, bl);
  decode(type, bl);
  decode(since, bl);
  history.decode(bl);
  decode(epoch_sent, bl);
  decode(to, bl);
  decode(from, bl);
  DECODE_FINISH(bl);
}

void pg_query_t::dump(Formatter *f) const
{
  f->dump_int("from", from);
  f->dump_int("to", to);
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
  o.push_back(new pg_query_t(pg_query_t::INFO, shard_id_t(1), shard_id_t(2), *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::MISSING, shard_id_t(2), shard_id_t(3), *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::LOG, shard_id_t(0), shard_id_t(0),
			     eversion_t(4, 5), *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::FULLLOG,
			     shard_id_t::NO_SHARD, shard_id_t::NO_SHARD,
			     *h.back(), 5));
}

// -- pg_lease_t --

void pg_lease_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(readable_until, bl);
  encode(readable_until_ub, bl);
  encode(interval, bl);
  ENCODE_FINISH(bl);
}

void pg_lease_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(readable_until, p);
  decode(readable_until_ub, p);
  decode(interval, p);
  DECODE_FINISH(p);
}

void pg_lease_t::dump(Formatter *f) const
{
  f->dump_stream("readable_until") << readable_until;
  f->dump_stream("readable_until_ub") << readable_until_ub;
  f->dump_stream("interval") << interval;
}

void pg_lease_t::generate_test_instances(std::list<pg_lease_t*>& o)
{
  o.push_back(new pg_lease_t());
  o.push_back(new pg_lease_t());
  o.back()->readable_until = make_timespan(1.5);
  o.back()->readable_until_ub = make_timespan(3.4);
  o.back()->interval = make_timespan(1.0);
}

// -- pg_lease_ack_t --

void pg_lease_ack_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(readable_until_ub, bl);
  ENCODE_FINISH(bl);
}

void pg_lease_ack_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(readable_until_ub, p);
  DECODE_FINISH(p);
}

void pg_lease_ack_t::dump(Formatter *f) const
{
  f->dump_stream("readable_until_ub") << readable_until_ub;
}

void pg_lease_ack_t::generate_test_instances(std::list<pg_lease_ack_t*>& o)
{
  o.push_back(new pg_lease_ack_t());
  o.push_back(new pg_lease_ack_t());
  o.back()->readable_until_ub = make_timespan(3.4);
}


// -- ObjectModDesc --
void ObjectModDesc::visit(Visitor *visitor) const
{
  auto bp = bl.cbegin();
  try {
    while (!bp.end()) {
      DECODE_START(max_required_version, bp);
      uint8_t code;
      decode(code, bp);
      switch (code) {
      case APPEND: {
	uint64_t size;
	decode(size, bp);
	visitor->append(size);
	break;
      }
      case SETATTRS: {
	map<string, std::optional<ceph::buffer::list> > attrs;
	decode(attrs, bp);
	visitor->setattrs(attrs);
	break;
      }
      case DELETE: {
	version_t old_version;
	decode(old_version, bp);
	visitor->rmobject(old_version);
	break;
      }
      case CREATE: {
	visitor->create();
	break;
      }
      case UPDATE_SNAPS: {
	set<snapid_t> snaps;
	decode(snaps, bp);
	visitor->update_snaps(snaps);
	break;
      }
      case TRY_DELETE: {
	version_t old_version;
	decode(old_version, bp);
	visitor->try_rmobject(old_version);
	break;
      }
      case ROLLBACK_EXTENTS: {
	vector<pair<uint64_t, uint64_t> > extents;
	version_t gen;
	decode(gen, bp);
	decode(extents, bp);
	visitor->rollback_extents(gen,extents);
	break;
      }
      default:
	ceph_abort_msg("Invalid rollback code");
      }
      DECODE_FINISH(bp);
    }
  } catch (...) {
    ceph_abort_msg("Invalid encoding");
  }
}

struct DumpVisitor : public ObjectModDesc::Visitor {
  Formatter *f;
  explicit DumpVisitor(Formatter *f) : f(f) {}
  void append(uint64_t old_size) override {
    f->open_object_section("op");
    f->dump_string("code", "APPEND");
    f->dump_unsigned("old_size", old_size);
    f->close_section();
  }
  void setattrs(map<string, std::optional<ceph::buffer::list> > &attrs) override {
    f->open_object_section("op");
    f->dump_string("code", "SETATTRS");
    f->open_array_section("attrs");
    for (auto i = attrs.begin(); i != attrs.end(); ++i) {
      f->dump_string("attr_name", i->first);
    }
    f->close_section();
    f->close_section();
  }
  void rmobject(version_t old_version) override {
    f->open_object_section("op");
    f->dump_string("code", "RMOBJECT");
    f->dump_unsigned("old_version", old_version);
    f->close_section();
  }
  void try_rmobject(version_t old_version) override {
    f->open_object_section("op");
    f->dump_string("code", "TRY_RMOBJECT");
    f->dump_unsigned("old_version", old_version);
    f->close_section();
  }
  void create() override {
    f->open_object_section("op");
    f->dump_string("code", "CREATE");
    f->close_section();
  }
  void update_snaps(const set<snapid_t> &snaps) override {
    f->open_object_section("op");
    f->dump_string("code", "UPDATE_SNAPS");
    f->dump_stream("snaps") << snaps;
    f->close_section();
  }
  void rollback_extents(
    version_t gen,
    const vector<pair<uint64_t, uint64_t> > &extents) override {
    f->open_object_section("op");
    f->dump_string("code", "ROLLBACK_EXTENTS");
    f->dump_unsigned("gen", gen);
    f->dump_stream("snaps") << extents;
    f->close_section();
  }
};

void ObjectModDesc::dump(Formatter *f) const
{
  f->open_object_section("object_mod_desc");
  f->dump_bool("can_local_rollback", can_local_rollback);
  f->dump_bool("rollback_info_completed", rollback_info_completed);
  {
    f->open_array_section("ops");
    DumpVisitor vis(f);
    visit(&vis);
    f->close_section();
  }
  f->close_section();
}

void ObjectModDesc::generate_test_instances(list<ObjectModDesc*>& o)
{
  map<string, std::optional<ceph::buffer::list> > attrs;
  attrs[OI_ATTR];
  attrs[SS_ATTR];
  attrs["asdf"];
  o.push_back(new ObjectModDesc());
  o.back()->append(100);
  o.back()->setattrs(attrs);
  o.push_back(new ObjectModDesc());
  o.back()->rmobject(1001);
  o.push_back(new ObjectModDesc());
  o.back()->create();
  o.back()->setattrs(attrs);
  o.push_back(new ObjectModDesc());
  o.back()->create();
  o.back()->setattrs(attrs);
  o.back()->mark_unrollbackable();
  o.back()->append(1000);
}

void ObjectModDesc::encode(ceph::buffer::list &_bl) const
{
  ENCODE_START(max_required_version, max_required_version, _bl);
  encode(can_local_rollback, _bl);
  encode(rollback_info_completed, _bl);
  encode(bl, _bl);
  ENCODE_FINISH(_bl);
}
void ObjectModDesc::decode(ceph::buffer::list::const_iterator &_bl)
{
  DECODE_START(2, _bl);
  max_required_version = struct_v;
  decode(can_local_rollback, _bl);
  decode(rollback_info_completed, _bl);
  decode(bl, _bl);
  // ensure bl does not pin a larger ceph::buffer in memory
  bl.rebuild();
  bl.reassign_to_mempool(mempool::mempool_osd_pglog);
  DECODE_FINISH(_bl);
}

std::atomic<uint32_t> ObjectCleanRegions::max_num_intervals = {10};

void ObjectCleanRegions::set_max_num_intervals(uint32_t num)
{
  max_num_intervals = num;
}

void ObjectCleanRegions::trim()
{
  while(clean_offsets.num_intervals() > max_num_intervals) {
    typename interval_set<uint64_t>::iterator shortest_interval = clean_offsets.begin();
    if (shortest_interval == clean_offsets.end())
      break;
    for (typename interval_set<uint64_t>::iterator it = clean_offsets.begin();
        it != clean_offsets.end();
        ++it) {
      if (it.get_len() < shortest_interval.get_len())
        shortest_interval = it;
    }
    clean_offsets.erase(shortest_interval);
  }
}

void ObjectCleanRegions::merge(const ObjectCleanRegions &other)
{
  clean_offsets.intersection_of(other.clean_offsets);
  clean_omap = clean_omap && other.clean_omap;
  trim();
}

void ObjectCleanRegions::mark_data_region_dirty(uint64_t offset, uint64_t len)
{
  interval_set<uint64_t> clean_region;
  clean_region.insert(0, (uint64_t)-1);
  clean_region.erase(offset, len);
  clean_offsets.intersection_of(clean_region);
  trim();
}

void ObjectCleanRegions::mark_omap_dirty()
{
  clean_omap = false;
}

void ObjectCleanRegions::mark_object_new()
{
  new_object = true;
}

void ObjectCleanRegions::mark_fully_dirty()
{
  mark_data_region_dirty(0, (uint64_t)-1);
  mark_omap_dirty();
  mark_object_new();
}

interval_set<uint64_t> ObjectCleanRegions::get_dirty_regions() const
{
   interval_set<uint64_t> dirty_region;
   dirty_region.insert(0, (uint64_t)-1);
   dirty_region.subtract(clean_offsets);
   return dirty_region;
}

bool ObjectCleanRegions::omap_is_dirty() const
{
  return !clean_omap;
}

bool ObjectCleanRegions::object_is_exist() const
{
  return !new_object;
}

void ObjectCleanRegions::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  using ceph::encode;
  encode(clean_offsets, bl);
  encode(clean_omap, bl);
  encode(new_object, bl);
  ENCODE_FINISH(bl);
}

void ObjectCleanRegions::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  using ceph::decode;
  decode(clean_offsets, bl);
  decode(clean_omap, bl);
  decode(new_object, bl);
  DECODE_FINISH(bl);
}

void ObjectCleanRegions::dump(Formatter *f) const
{
  f->open_object_section("object_clean_regions");
  f->dump_stream("clean_offsets") << clean_offsets;
  f->dump_bool("clean_omap", clean_omap);
  f->dump_bool("new_object", new_object);
  f->close_section();
}

void ObjectCleanRegions::generate_test_instances(list<ObjectCleanRegions*>& o)
{
  o.push_back(new ObjectCleanRegions());
  o.push_back(new ObjectCleanRegions());
  o.back()->mark_data_region_dirty(4096, 40960);
  o.back()->mark_omap_dirty();
  o.back()->mark_object_new();
}

ostream& operator<<(ostream& out, const ObjectCleanRegions& ocr)
{
  return out << "clean_offsets: " << ocr.clean_offsets
             << ", clean_omap: " << ocr.clean_omap
             << ", new_object: " << ocr.new_object;
}

// -- pg_log_entry_t --

string pg_log_entry_t::get_key_name() const
{
  return version.get_key_name();
}

void pg_log_entry_t::encode_with_checksum(ceph::buffer::list& bl) const
{
  using ceph::encode;
  ceph::buffer::list ebl(sizeof(*this)*2);
  this->encode(ebl);
  __u32 crc = ebl.crc32c(0);
  encode(ebl, bl);
  encode(crc, bl);
}

void pg_log_entry_t::decode_with_checksum(ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;
  ceph::buffer::list bl;
  decode(bl, p);
  __u32 crc;
  decode(crc, p);
  if (crc != bl.crc32c(0))
    throw ceph::buffer::malformed_input("bad checksum on pg_log_entry_t");
  auto q = bl.cbegin();
  this->decode(q);
}

void pg_log_entry_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(14, 4, bl);
  encode(op, bl);
  encode(soid, bl);
  encode(version, bl);

  /**
   * Added with reverting_to:
   * Previous code used prior_version to encode
   * what we now call reverting_to.  This will
   * allow older code to decode reverting_to
   * into prior_version as expected.
   */
  if (op == LOST_REVERT)
    encode(reverting_to, bl);
  else
    encode(prior_version, bl);

  encode(reqid, bl);
  encode(mtime, bl);
  if (op == LOST_REVERT)
    encode(prior_version, bl);
  encode(snaps, bl);
  encode(user_version, bl);
  encode(mod_desc, bl);
  encode(extra_reqids, bl);
  if (op == ERROR)
    encode(return_code, bl);
  if (!extra_reqids.empty())
    encode(extra_reqid_return_codes, bl);
  encode(clean_regions, bl);
  if (op != ERROR)
    encode(return_code, bl);
  encode(op_returns, bl);
  ENCODE_FINISH(bl);
}

void pg_log_entry_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(14, 4, 4, bl);
  decode(op, bl);
  if (struct_v < 2) {
    sobject_t old_soid;
    decode(old_soid, bl);
    soid.oid = old_soid.oid;
    soid.snap = old_soid.snap;
    invalid_hash = true;
  } else {
    decode(soid, bl);
  }
  if (struct_v < 3)
    invalid_hash = true;
  decode(version, bl);

  if (struct_v >= 6 && op == LOST_REVERT)
    decode(reverting_to, bl);
  else
    decode(prior_version, bl);

  decode(reqid, bl);

  decode(mtime, bl);
  if (struct_v < 5)
    invalid_pool = true;

  if (op == LOST_REVERT) {
    if (struct_v >= 6) {
      decode(prior_version, bl);
    } else {
      reverting_to = prior_version;
    }
  }
  if (struct_v >= 7 ||  // for v >= 7, this is for all ops.
      op == CLONE) {    // for v < 7, it's only present for CLONE.
    decode(snaps, bl);
    // ensure snaps does not pin a larger ceph::buffer in memory
    snaps.rebuild();
    snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }

  if (struct_v >= 8)
    decode(user_version, bl);
  else
    user_version = version.version;

  if (struct_v >= 9)
    decode(mod_desc, bl);
  else
    mod_desc.mark_unrollbackable();
  if (struct_v >= 10)
    decode(extra_reqids, bl);
  if (struct_v >= 11 && op == ERROR)
    decode(return_code, bl);
  if (struct_v >= 12 && !extra_reqids.empty())
    decode(extra_reqid_return_codes, bl);
  if (struct_v >= 13)
    decode(clean_regions, bl);
  else
    clean_regions.mark_fully_dirty();
  if (struct_v >= 14) {
    if (op != ERROR) {
      decode(return_code, bl);
    }
    decode(op_returns, bl);
  }
  DECODE_FINISH(bl);
}

void pg_log_entry_t::dump(Formatter *f) const
{
  f->dump_string("op", get_op_name());
  f->dump_stream("object") << soid;
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << prior_version;
  f->dump_stream("reqid") << reqid;
  f->open_array_section("extra_reqids");
  uint32_t idx = 0;
  for (auto p = extra_reqids.begin();
       p != extra_reqids.end();
       ++idx, ++p) {
    f->open_object_section("extra_reqid");
    f->dump_stream("reqid") << p->first;
    f->dump_stream("user_version") << p->second;
    auto it = extra_reqid_return_codes.find(idx);
    if (it != extra_reqid_return_codes.end()) {
      f->dump_int("return_code", it->second);
    }
    f->close_section();
  }
  f->close_section();
  f->dump_stream("mtime") << mtime;
  f->dump_int("return_code", return_code);
  if (!op_returns.empty()) {
    f->open_array_section("op_returns");
    for (auto& i : op_returns) {
      f->dump_object("op", i);
    }
    f->close_section();
  }
  if (snaps.length() > 0) {
    vector<snapid_t> v;
    ceph::buffer::list c = snaps;
    auto p = c.cbegin();
    try {
      using ceph::decode;
      decode(v, p);
    } catch (...) {
      v.clear();
    }
    f->open_object_section("snaps");
    for (auto p = v.begin(); p != v.end(); ++p)
      f->dump_unsigned("snap", *p);
    f->close_section();
  }
  {
    f->open_object_section("mod_desc");
    mod_desc.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("clean_regions");
    clean_regions.dump(f);
    f->close_section();
  }
}

void pg_log_entry_t::generate_test_instances(list<pg_log_entry_t*>& o)
{
  o.push_back(new pg_log_entry_t());
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  o.push_back(new pg_log_entry_t(MODIFY, oid, eversion_t(1,2), eversion_t(3,4),
				 1, osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
				 utime_t(8,9), 0));
  o.push_back(new pg_log_entry_t(ERROR, oid, eversion_t(1,2), eversion_t(3,4),
				 1, osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
				 utime_t(8,9), -ENOENT));
}

ostream& operator<<(ostream& out, const pg_log_entry_t& e)
{
  out << e.version << " (" << e.prior_version << ") "
      << std::left << std::setw(8) << e.get_op_name() << ' '
      << e.soid << " by " << e.reqid << " " << e.mtime
      << " " << e.return_code;
  if (!e.op_returns.empty()) {
    out << " " << e.op_returns;
  }
  if (e.snaps.length()) {
    vector<snapid_t> snaps;
    ceph::buffer::list c = e.snaps;
    auto p = c.cbegin();
    try {
      decode(snaps, p);
    } catch (...) {
      snaps.clear();
    }
    out << " snaps " << snaps;
  }
  out << " ObjectCleanRegions " << e.clean_regions;
  return out;
}

// -- pg_log_dup_t --

std::string pg_log_dup_t::get_key_name() const
{
  static const char prefix[] = "dup_";
  std::string key(36, ' ');
  memcpy(&key[0], prefix, 4);
  version.get_key_name(&key[4]);
  key.resize(35); // remove the null terminator
  return key;
}

void pg_log_dup_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(2, 1, bl);
  encode(reqid, bl);
  encode(version, bl);
  encode(user_version, bl);
  encode(return_code, bl);
  encode(op_returns, bl);
  ENCODE_FINISH(bl);
}

void pg_log_dup_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(2, bl);
  decode(reqid, bl);
  decode(version, bl);
  decode(user_version, bl);
  decode(return_code, bl);
  if (struct_v >= 2) {
    decode(op_returns, bl);
  }
  DECODE_FINISH(bl);
}

void pg_log_dup_t::dump(Formatter *f) const
{
  f->dump_stream("reqid") << reqid;
  f->dump_stream("version") << version;
  f->dump_stream("user_version") << user_version;
  f->dump_stream("return_code") << return_code;
  if (!op_returns.empty()) {
    f->open_array_section("op_returns");
    for (auto& i : op_returns) {
      f->dump_object("op", i);
    }
    f->close_section();
  }
}

void pg_log_dup_t::generate_test_instances(list<pg_log_dup_t*>& o)
{
  o.push_back(new pg_log_dup_t());
  o.push_back(new pg_log_dup_t(eversion_t(1,2),
			       1,
			       osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
			       0));
  o.push_back(new pg_log_dup_t(eversion_t(1,2),
			       2,
			       osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
			       -ENOENT));
}


std::ostream& operator<<(std::ostream& out, const pg_log_dup_t& e) {
  out << "log_dup(reqid=" << e.reqid <<
    " v=" << e.version << " uv=" << e.user_version <<
    " rc=" << e.return_code;
  if (!e.op_returns.empty()) {
    out << " " << e.op_returns;
  }
  return out << ")";
}


// -- pg_log_t --

// out: pg_log_t that only has entries that apply to import_pgid using curmap
// reject: Entries rejected from "in" are in the reject.log.  Other fields not set.
void pg_log_t::filter_log(spg_t import_pgid, const OSDMap &curmap,
  const string &hit_set_namespace, const pg_log_t &in,
  pg_log_t &out, pg_log_t &reject)
{
  out = in;
  out.log.clear();
  reject.log.clear();

  for (auto i = in.log.cbegin(); i != in.log.cend(); ++i) {

    // Reject pg log entries for temporary objects
    if (i->soid.is_temp()) {
      reject.log.push_back(*i);
      continue;
    }

    if (i->soid.nspace != hit_set_namespace) {
      object_t oid = i->soid.oid;
      object_locator_t loc(i->soid);
      pg_t raw_pgid = curmap.object_locator_to_pg(oid, loc);
      pg_t pgid = curmap.raw_pg_to_pg(raw_pgid);

      if (import_pgid.pgid == pgid) {
        out.log.push_back(*i);
      } else {
        reject.log.push_back(*i);
      }
    } else {
      out.log.push_back(*i);
    }
  }
}

void pg_log_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(7, 3, bl);
  encode(head, bl);
  encode(tail, bl);
  encode(log, bl);
  encode(can_rollback_to, bl);
  encode(rollback_info_trimmed_to, bl);
  encode(dups, bl);
  ENCODE_FINISH(bl);
}
 
void pg_log_t::decode(ceph::buffer::list::const_iterator &bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 3, 3, bl);
  decode(head, bl);
  decode(tail, bl);
  if (struct_v < 2) {
    bool backlog;
    decode(backlog, bl);
  }
  decode(log, bl);
  if (struct_v >= 5)
    decode(can_rollback_to, bl);

  if (struct_v >= 6)
    decode(rollback_info_trimmed_to, bl);
  else
    rollback_info_trimmed_to = tail;

  if (struct_v >= 7)
    decode(dups, bl);

  DECODE_FINISH(bl);

  // handle hobject_t format change
  if (struct_v < 4) {
    for (auto i = log.begin(); i != log.end(); ++i) {
      if (!i->soid.is_max() && i->soid.pool == -1)
	i->soid.pool = pool;
    }
  }
}

void pg_log_t::dump(Formatter *f) const
{
  f->dump_stream("head") << head;
  f->dump_stream("tail") << tail;
  f->open_array_section("log");
  for (auto p = log.cbegin(); p != log.cend(); ++p) {
    f->open_object_section("entry");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("dups");
  for (const auto& entry : dups) {
    f->open_object_section("entry");
    entry.dump(f);
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
  for (auto p = e.begin(); p != e.end(); ++p)
    o.back()->log.push_back(**p);
}

static void _handle_dups(CephContext* cct, pg_log_t &target, const pg_log_t &other, unsigned maxdups)
{
  auto earliest_dup_version =
	        target.head.version < maxdups ? 0u : target.head.version - maxdups + 1;
  lgeneric_subdout(cct, osd, 20) << "copy_up_to/copy_after earliest_dup_version " << earliest_dup_version << dendl;

  for (auto d = other.dups.cbegin(); d != other.dups.cend(); ++d) {
    if (d->version.version >= earliest_dup_version) {
      lgeneric_subdout(cct, osd, 20)
	      << "copy_up_to/copy_after copy dup version "
	      << d->version << dendl;
      target.dups.push_back(pg_log_dup_t(*d));
    }
  }

  for (auto i = other.log.cbegin(); i != other.log.cend(); ++i) {
    ceph_assert(i->version > other.tail);
    if (i->version > target.tail)
      break;
    if (i->version.version >= earliest_dup_version) {
      lgeneric_subdout(cct, osd, 20)
		<< "copy_up_to/copy_after copy dup from log version "
		<< i->version << dendl;
      target.dups.push_back(pg_log_dup_t(*i));
    }
  }
}


void pg_log_t::copy_after(CephContext* cct, const pg_log_t &other, eversion_t v)
{
  can_rollback_to = other.can_rollback_to;
  head = other.head;
  tail = other.tail;
  lgeneric_subdout(cct, osd, 20) << __func__ << " v " << v << dendl;
  for (auto i = other.log.crbegin(); i != other.log.crend(); ++i) {
    ceph_assert(i->version > other.tail);
    if (i->version <= v) {
      // make tail accurate.
      tail = i->version;
      break;
    }
    lgeneric_subdout(cct, osd, 20) << __func__ << " copy log version " << i->version << dendl;
    log.push_front(*i);
  }
  _handle_dups(cct, *this, other, cct->_conf->osd_pg_log_dups_tracked);
}

void pg_log_t::copy_up_to(CephContext* cct, const pg_log_t &other, int max)
{
  can_rollback_to = other.can_rollback_to;
  int n = 0;
  head = other.head;
  tail = other.tail;
  lgeneric_subdout(cct, osd, 20) << __func__ << " max " << max << dendl;
  for (auto i = other.log.crbegin(); i != other.log.crend(); ++i) {
    ceph_assert(i->version > other.tail);
    if (n++ >= max) {
      tail = i->version;
      break;
    }
    lgeneric_subdout(cct, osd, 20) << __func__ << " copy log version " << i->version << dendl;
    log.push_front(*i);
  }
  _handle_dups(cct, *this, other, cct->_conf->osd_pg_log_dups_tracked);
}

ostream& pg_log_t::print(ostream& out) const
{
  out << *this << std::endl;
  for (auto p = log.cbegin(); p != log.cend(); ++p)
    out << *p << std::endl;
  for (const auto& entry : dups) {
    out << " dup entry: " << entry << std::endl;
  }
  return out;
}

// -- pg_missing_t --

ostream& operator<<(ostream& out, const pg_missing_item& i)
{
  out << i.need;
  if (i.have != eversion_t())
    out << "(" << i.have << ")";
  out << " flags = " << i.flag_str()
      << " " << i.clean_regions;
  return out;
}

// -- object_copy_cursor_t --

void object_copy_cursor_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(attr_complete, bl);
  encode(data_offset, bl);
  encode(data_complete, bl);
  encode(omap_offset, bl);
  encode(omap_complete, bl);
  ENCODE_FINISH(bl);
}

void object_copy_cursor_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(attr_complete, bl);
  decode(data_offset, bl);
  decode(data_complete, bl);
  decode(omap_offset, bl);
  decode(omap_complete, bl);
  DECODE_FINISH(bl);
}

void object_copy_cursor_t::dump(Formatter *f) const
{
  f->dump_unsigned("attr_complete", (int)attr_complete);
  f->dump_unsigned("data_offset", data_offset);
  f->dump_unsigned("data_complete", (int)data_complete);
  f->dump_string("omap_offset", omap_offset);
  f->dump_unsigned("omap_complete", (int)omap_complete);
}

void object_copy_cursor_t::generate_test_instances(list<object_copy_cursor_t*>& o)
{
  o.push_back(new object_copy_cursor_t);
  o.push_back(new object_copy_cursor_t);
  o.back()->attr_complete = true;
  o.back()->data_offset = 123;
  o.push_back(new object_copy_cursor_t);
  o.back()->attr_complete = true;
  o.back()->data_complete = true;
  o.back()->omap_offset = "foo";
  o.push_back(new object_copy_cursor_t);
  o.back()->attr_complete = true;
  o.back()->data_complete = true;
  o.back()->omap_complete = true;
}

// -- object_copy_data_t --

void object_copy_data_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  ENCODE_START(8, 5, bl);
  encode(size, bl);
  encode(mtime, bl);
  encode(attrs, bl);
  encode(data, bl);
  encode(omap_data, bl);
  encode(cursor, bl);
  encode(omap_header, bl);
  encode(snaps, bl);
  encode(snap_seq, bl);
  encode(flags, bl);
  encode(data_digest, bl);
  encode(omap_digest, bl);
  encode(reqids, bl);
  encode(truncate_seq, bl);
  encode(truncate_size, bl);
  encode(reqid_return_codes, bl);
  ENCODE_FINISH(bl);
}

void object_copy_data_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(8, bl);
  if (struct_v < 5) {
    // old
    decode(size, bl);
    decode(mtime, bl);
    {
      string category;
      decode(category, bl);  // no longer used
    }
    decode(attrs, bl);
    decode(data, bl);
    {
      map<string,ceph::buffer::list> omap;
      decode(omap, bl);
      omap_data.clear();
      if (!omap.empty()) {
	using ceph::encode;
	encode(omap, omap_data);
      }
    }
    decode(cursor, bl);
    if (struct_v >= 2)
      decode(omap_header, bl);
    if (struct_v >= 3) {
      decode(snaps, bl);
      decode(snap_seq, bl);
    } else {
      snaps.clear();
      snap_seq = 0;
    }
    if (struct_v >= 4) {
      decode(flags, bl);
      decode(data_digest, bl);
      decode(omap_digest, bl);
    }
  } else {
    // current
    decode(size, bl);
    decode(mtime, bl);
    decode(attrs, bl);
    decode(data, bl);
    decode(omap_data, bl);
    decode(cursor, bl);
    decode(omap_header, bl);
    decode(snaps, bl);
    decode(snap_seq, bl);
    if (struct_v >= 4) {
      decode(flags, bl);
      decode(data_digest, bl);
      decode(omap_digest, bl);
    }
    if (struct_v >= 6) {
      decode(reqids, bl);
    }
    if (struct_v >= 7) {
      decode(truncate_seq, bl);
      decode(truncate_size, bl);
    }
    if (struct_v >= 8) {
      decode(reqid_return_codes, bl);
    }
  }
  DECODE_FINISH(bl);
}

void object_copy_data_t::generate_test_instances(list<object_copy_data_t*>& o)
{
  o.push_back(new object_copy_data_t());

  list<object_copy_cursor_t*> cursors;
  object_copy_cursor_t::generate_test_instances(cursors);
  auto ci = cursors.begin();
  o.back()->cursor = **(ci++);

  o.push_back(new object_copy_data_t());
  o.back()->cursor = **(ci++);

  o.push_back(new object_copy_data_t());
  o.back()->size = 1234;
  o.back()->mtime.set_from_double(1234);
  ceph::buffer::ptr bp("there", 5);
  ceph::buffer::list bl;
  bl.push_back(bp);
  o.back()->attrs["hello"] = bl;
  ceph::buffer::ptr bp2("not", 3);
  ceph::buffer::list bl2;
  bl2.push_back(bp2);
  map<string,ceph::buffer::list> omap;
  omap["why"] = bl2;
  using ceph::encode;
  encode(omap, o.back()->omap_data);
  ceph::buffer::ptr databp("iamsomedatatocontain", 20);
  o.back()->data.push_back(databp);
  o.back()->omap_header.append("this is an omap header");
  o.back()->snaps.push_back(123);
  o.back()->reqids.push_back(make_pair(osd_reqid_t(), version_t()));
}

void object_copy_data_t::dump(Formatter *f) const
{
  f->open_object_section("cursor");
  cursor.dump(f);
  f->close_section(); // cursor
  f->dump_int("size", size);
  f->dump_stream("mtime") << mtime;
  /* we should really print out the attrs here, but ceph::buffer::list
     const-correctness prevents that */
  f->dump_int("attrs_size", attrs.size());
  f->dump_int("flags", flags);
  f->dump_unsigned("data_digest", data_digest);
  f->dump_unsigned("omap_digest", omap_digest);
  f->dump_int("omap_data_length", omap_data.length());
  f->dump_int("omap_header_length", omap_header.length());
  f->dump_int("data_length", data.length());
  f->open_array_section("snaps");
  for (auto p = snaps.cbegin(); p != snaps.cend(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
  f->open_array_section("reqids");
  uint32_t idx = 0;
  for (auto p = reqids.begin();
       p != reqids.end();
       ++idx, ++p) {
    f->open_object_section("extra_reqid");
    f->dump_stream("reqid") << p->first;
    f->dump_stream("user_version") << p->second;
    auto it = reqid_return_codes.find(idx);
    if (it != reqid_return_codes.end()) {
      f->dump_int("return_code", it->second);
    }
    f->close_section();
  }
  f->close_section();
}

// -- pg_create_t --

void pg_create_t::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(created, bl);
  encode(parent, bl);
  encode(split_bits, bl);
  ENCODE_FINISH(bl);
}

void pg_create_t::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(created, bl);
  decode(parent, bl);
  decode(split_bits, bl);
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
  o.push_back(new pg_create_t(1, pg_t(3, 4), 2));
}


// -- pg_hit_set_info_t --

void pg_hit_set_info_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(2, 1, bl);
  encode(begin, bl);
  encode(end, bl);
  encode(version, bl);
  encode(using_gmt, bl);
  ENCODE_FINISH(bl);
}

void pg_hit_set_info_t::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(2, p);
  decode(begin, p);
  decode(end, p);
  decode(version, p);
  if (struct_v >= 2) {
    decode(using_gmt, p);
  } else {
    using_gmt = false;
  }
  DECODE_FINISH(p);
}

void pg_hit_set_info_t::dump(Formatter *f) const
{
  f->dump_stream("begin") << begin;
  f->dump_stream("end") << end;
  f->dump_stream("version") << version;
  f->dump_stream("using_gmt") << using_gmt;
}

void pg_hit_set_info_t::generate_test_instances(list<pg_hit_set_info_t*>& ls)
{
  ls.push_back(new pg_hit_set_info_t);
  ls.push_back(new pg_hit_set_info_t);
  ls.back()->begin = utime_t(1, 2);
  ls.back()->end = utime_t(3, 4);
}


// -- pg_hit_set_history_t --

void pg_hit_set_history_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(current_last_update, bl);
  {
    utime_t dummy_stamp;
    encode(dummy_stamp, bl);
  }
  {
    pg_hit_set_info_t dummy_info;
    encode(dummy_info, bl);
  }
  encode(history, bl);
  ENCODE_FINISH(bl);
}

void pg_hit_set_history_t::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(1, p);
  decode(current_last_update, p);
  {
    utime_t dummy_stamp;
    decode(dummy_stamp, p);
  }
  {
    pg_hit_set_info_t dummy_info;
    decode(dummy_info, p);
  }
  decode(history, p);
  DECODE_FINISH(p);
}

void pg_hit_set_history_t::dump(Formatter *f) const
{
  f->dump_stream("current_last_update") << current_last_update;
  f->open_array_section("history");
  for (auto p = history.cbegin(); p != history.cend(); ++p) {
    f->open_object_section("info");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void pg_hit_set_history_t::generate_test_instances(list<pg_hit_set_history_t*>& ls)
{
  ls.push_back(new pg_hit_set_history_t);
  ls.push_back(new pg_hit_set_history_t);
  ls.back()->current_last_update = eversion_t(1, 2);
  ls.back()->history.push_back(pg_hit_set_info_t());
}

// -- OSDSuperblock --

void OSDSuperblock::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(9, 5, bl);
  encode(cluster_fsid, bl);
  encode(whoami, bl);
  encode(current_epoch, bl);
  encode(oldest_map, bl);
  encode(newest_map, bl);
  encode(weight, bl);
  compat_features.encode(bl);
  encode(clean_thru, bl);
  encode(mounted, bl);
  encode(osd_fsid, bl);
  encode((epoch_t)0, bl);  // epoch_t last_epoch_marked_full
  encode((uint32_t)0, bl);  // map<int64_t,epoch_t> pool_last_epoch_marked_full
  encode(purged_snaps_last, bl);
  encode(last_purged_snaps_scrub, bl);
  ENCODE_FINISH(bl);
}

void OSDSuperblock::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(9, 5, 5, bl);
  if (struct_v < 3) {
    string magic;
    decode(magic, bl);
  }
  decode(cluster_fsid, bl);
  decode(whoami, bl);
  decode(current_epoch, bl);
  decode(oldest_map, bl);
  decode(newest_map, bl);
  decode(weight, bl);
  if (struct_v >= 2) {
    compat_features.decode(bl);
  } else { //upgrade it!
    compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  }
  decode(clean_thru, bl);
  decode(mounted, bl);
  if (struct_v >= 4)
    decode(osd_fsid, bl);
  if (struct_v >= 6) {
    epoch_t last_map_marked_full;
    decode(last_map_marked_full, bl);
  }
  if (struct_v >= 7) {
    map<int64_t,epoch_t> pool_last_map_marked_full;
    decode(pool_last_map_marked_full, bl);
  }
  if (struct_v >= 9) {
    decode(purged_snaps_last, bl);
    decode(last_purged_snaps_scrub, bl);
  } else {
    purged_snaps_last = 0;
  }
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
  f->dump_unsigned("purged_snaps_last", purged_snaps_last);
  f->dump_stream("last_purged_snaps_scrub") << last_purged_snaps_scrub;
}

void OSDSuperblock::generate_test_instances(list<OSDSuperblock*>& o)
{
  OSDSuperblock z;
  o.push_back(new OSDSuperblock(z));
  z.cluster_fsid.parse("01010101-0101-0101-0101-010101010101");
  z.osd_fsid.parse("02020202-0202-0202-0202-020202020202");
  z.whoami = 3;
  z.current_epoch = 4;
  z.oldest_map = 5;
  z.newest_map = 9;
  z.mounted = 8;
  z.clean_thru = 7;
  o.push_back(new OSDSuperblock(z));
  o.push_back(new OSDSuperblock(z));
}

// -- SnapSet --

void SnapSet::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(seq, bl);
  encode(true, bl);  // head_exists
  encode(snaps, bl);
  encode(clones, bl);
  encode(clone_overlap, bl);
  encode(clone_size, bl);
  encode(clone_snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapSet::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(seq, bl);
  bl += 1u;  // skip legacy head_exists (always true)
  decode(snaps, bl);
  decode(clones, bl);
  decode(clone_overlap, bl);
  decode(clone_size, bl);
  if (struct_v >= 3) {
    decode(clone_snaps, bl);
  } else {
    clone_snaps.clear();
  }
  DECODE_FINISH(bl);
}

void SnapSet::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("clones");
  for (auto p = clones.cbegin(); p != clones.cend(); ++p) {
    f->open_object_section("clone");
    f->dump_unsigned("snap", *p);
    auto cs = clone_size.find(*p);
    if (cs != clone_size.end())
      f->dump_unsigned("size", cs->second);
    else
      f->dump_string("size", "????");
    auto co = clone_overlap.find(*p);
    if (co != clone_overlap.end())
      f->dump_stream("overlap") << co->second;
    else
      f->dump_stream("overlap") << "????";
    auto q = clone_snaps.find(*p);
    if (q != clone_snaps.end()) {
      f->open_array_section("snaps");
      for (auto s : q->second) {
	f->dump_unsigned("snap", s);
      }
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}

void SnapSet::generate_test_instances(list<SnapSet*>& o)
{
  o.push_back(new SnapSet);
  o.push_back(new SnapSet);
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.push_back(new SnapSet);
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.back()->clones.push_back(12);
  o.back()->clone_size[12] = 12345;
  o.back()->clone_overlap[12];
  o.back()->clone_snaps[12] = {12, 10, 8};
}

ostream& operator<<(ostream& out, const SnapSet& cs)
{
  return out << cs.seq << "=" << cs.snaps << ":"
	     << cs.clone_snaps;
}

void SnapSet::from_snap_set(const librados::snap_set_t& ss, bool legacy)
{
  // NOTE: our reconstruction of snaps (and the snapc) is not strictly
  // correct: it will not include snaps that still logically exist
  // but for which there was no clone that is defined.  For all
  // practical purposes this doesn't matter, since we only use that
  // information to clone on the OSD, and we have already moved
  // forward past that part of the object history.

  seq = ss.seq;
  set<snapid_t> _snaps;
  set<snapid_t> _clones;
  for (auto p = ss.clones.cbegin(); p != ss.clones.cend(); ++p) {
    if (p->cloneid != librados::SNAP_HEAD) {
      _clones.insert(p->cloneid);
      _snaps.insert(p->snaps.begin(), p->snaps.end());
      clone_size[p->cloneid] = p->size;
      clone_overlap[p->cloneid];  // the entry must exist, even if it's empty.
      for (auto q = p->overlap.cbegin(); q != p->overlap.cend(); ++q)
	clone_overlap[p->cloneid].insert(q->first, q->second);
      if (!legacy) {
	// p->snaps is ascending; clone_snaps is descending
	vector<snapid_t>& v = clone_snaps[p->cloneid];
	for (auto q = p->snaps.rbegin(); q != p->snaps.rend(); ++q) {
	  v.push_back(*q);
	}
      }
    }
  }

  // ascending
  clones.clear();
  clones.reserve(_clones.size());
  for (auto p = _clones.begin(); p != _clones.end(); ++p)
    clones.push_back(*p);

  // descending
  snaps.clear();
  snaps.reserve(_snaps.size());
  for (auto p = _snaps.rbegin();
       p != _snaps.rend(); ++p)
    snaps.push_back(*p);
}

uint64_t SnapSet::get_clone_bytes(snapid_t clone) const
{
  ceph_assert(clone_size.count(clone));
  uint64_t size = clone_size.find(clone)->second;
  ceph_assert(clone_overlap.count(clone));
  const interval_set<uint64_t> &overlap = clone_overlap.find(clone)->second;
  ceph_assert(size >= (uint64_t)overlap.size());
  return size - overlap.size();
}

void SnapSet::filter(const pg_pool_t &pinfo)
{
  vector<snapid_t> oldsnaps;
  oldsnaps.swap(snaps);
  for (auto i = oldsnaps.cbegin(); i != oldsnaps.cend(); ++i) {
    if (!pinfo.is_removed_snap(*i))
      snaps.push_back(*i);
  }
}

SnapSet SnapSet::get_filtered(const pg_pool_t &pinfo) const
{
  SnapSet ss = *this;
  ss.filter(pinfo);
  return ss;
}

// -- watch_info_t --

void watch_info_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  ENCODE_START(4, 3, bl);
  encode(cookie, bl);
  encode(timeout_seconds, bl);
  encode(addr, bl, features);
  ENCODE_FINISH(bl);
}

void watch_info_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  decode(cookie, bl);
  if (struct_v < 2) {
    uint64_t ver;
    decode(ver, bl);
  }
  decode(timeout_seconds, bl);
  if (struct_v >= 4) {
    decode(addr, bl);
  }
  DECODE_FINISH(bl);
}

void watch_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("cookie", cookie);
  f->dump_unsigned("timeout_seconds", timeout_seconds);
  f->open_object_section("addr");
  addr.dump(f);
  f->close_section();
}

void watch_info_t::generate_test_instances(list<watch_info_t*>& o)
{
  o.push_back(new watch_info_t);
  o.push_back(new watch_info_t);
  o.back()->cookie = 123;
  o.back()->timeout_seconds = 99;
  entity_addr_t ea;
  ea.set_type(entity_addr_t::TYPE_LEGACY);
  ea.set_nonce(1);
  ea.set_family(AF_INET);
  ea.set_in4_quad(0, 127);
  ea.set_in4_quad(1, 0);
  ea.set_in4_quad(2, 1);
  ea.set_in4_quad(3, 2);
  ea.set_port(2);
  o.back()->addr = ea;
}

// -- chunk_info_t --

void chunk_info_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(offset, bl);
  encode(length, bl);
  encode(oid, bl);
  __u32 _flags = flags;
  encode(_flags, bl);
  ENCODE_FINISH(bl);
}

void chunk_info_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(offset, bl);
  decode(length, bl);
  decode(oid, bl);
  __u32 _flags;
  decode(_flags, bl);
  flags = (cflag_t)_flags;
  DECODE_FINISH(bl);
}

void chunk_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("length", length);
  f->open_object_section("oid");
  oid.dump(f);
  f->close_section();
  f->dump_unsigned("flags", flags);
}


bool chunk_info_t::operator==(const chunk_info_t& cit) const
{
  if (has_fingerprint()) {
    if (oid.oid.name == cit.oid.oid.name) {
      return true;
    }
  } else {
    if (offset == cit.offset && length == cit.length &&
	oid.oid.name == cit.oid.oid.name) {
      return true;
    }

  }
  return false;
}

bool operator==(const std::pair<const long unsigned int, chunk_info_t> & l,
		const std::pair<const long unsigned int, chunk_info_t> & r) 
{
  return l.first == r.first &&
	 l.second == r.second;
}

ostream& operator<<(ostream& out, const chunk_info_t& ci)
{
  return out << "(len: " << ci.length << " oid: " << ci.oid
	     << " offset: " << ci.offset
	     << " flags: " << ci.get_flag_string(ci.flags) << ")";
}

// -- object_manifest_t --

std::ostream& operator<<(std::ostream& out, const object_ref_delta_t & ci)
{
  return out << ci.ref_delta << std::endl;
}

void object_manifest_t::calc_refs_to_drop_on_removal(
  const object_manifest_t* _g,
  const object_manifest_t* _l,
  object_ref_delta_t &refs) const
{
  /* At a high level, the rule is that consecutive clones with the same reference
   * at the same offset share a reference.  As such, removing *this may result
   * in removing references in two cases:
   * 1) *this has a reference which it shares with neither _g nor _l
   * 2) _g and _l have a reference which they share with each other but not
   *   *this.
   *
   * For a particular offset, both 1 and 2 can happen.
   *
   * Notably, this means that to evaluate the reference change from removing
   * the object with *this, we only need to look at the two adjacent clones.
   */

  // Paper over possibly missing _g or _l -- nullopt is semantically the same
  // as an empty chunk_map
  static const object_manifest_t empty;
  const object_manifest_t &g = _g ? *_g : empty;
  const object_manifest_t &l = _l ? *_l : empty;

  auto giter = g.chunk_map.begin();
  auto iter = chunk_map.begin();
  auto liter = l.chunk_map.begin();

  // Translate iter, map pair to the current offset, end() -> max
  auto get_offset = [](decltype(iter) &i, const object_manifest_t &manifest)
    -> uint64_t {
    return i == manifest.chunk_map.end() ?
      std::numeric_limits<uint64_t>::max() : i->first;
  };

  /* If current matches the offset at iter, returns the chunk at *iter
   * and increments iter.  Otherwise, returns nullptr.
   *
   * current will always be derived from the min of *giter, *iter, and
   * *liter on each cycle, so the result will be that each loop iteration
   * will pick up all chunks at the offest being considered, each offset
   * will be considered once, and all offsets will be considered.
   */
  auto get_chunk = [](
    uint64_t current, decltype(iter) &i, const object_manifest_t &manifest)
    -> const chunk_info_t * {
    if (i == manifest.chunk_map.end() || current != i->first) {
      return nullptr;
    } else {
      return &(i++)->second;
    }
  };

  while (giter != g.chunk_map.end() ||
	 iter != chunk_map.end() ||
	 liter != l.chunk_map.end()) {
    auto current = std::min(
      std::min(get_offset(giter, g), get_offset(iter, *this)),
      get_offset(liter, l));

    auto gchunk = get_chunk(current, giter, g);
    auto chunk = get_chunk(current, iter, *this);
    auto lchunk = get_chunk(current, liter, l);

    if (gchunk && lchunk && *gchunk == *lchunk &&
	(!chunk || *gchunk != *chunk)) {
      // case 1 from above: l and g match, chunk does not
      refs.dec_ref(gchunk->oid);
    }

    if (chunk &&
	(!gchunk || chunk->oid != gchunk->oid) &&
	(!lchunk || chunk->oid != lchunk->oid)) {
      // case 2 from above: *this matches neither
      refs.dec_ref(chunk->oid);
    }
  }
}

void object_manifest_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(type, bl);
  switch (type) {
    case TYPE_NONE: break;
    case TYPE_REDIRECT: 
      encode(redirect_target, bl);
      break;
    case TYPE_CHUNKED:
      encode(chunk_map, bl);
      break;
    default:
      ceph_abort();
  }
  ENCODE_FINISH(bl);
}

void object_manifest_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(type, bl);
  switch (type) {
    case TYPE_NONE: break;
    case TYPE_REDIRECT: 
      decode(redirect_target, bl);
      break;
    case TYPE_CHUNKED:
      decode(chunk_map, bl);
      break;
    default:
      ceph_abort();
  }
  DECODE_FINISH(bl);
}

void object_manifest_t::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
  if (type == TYPE_REDIRECT) {
    f->open_object_section("redirect_target");
    redirect_target.dump(f);
    f->close_section();
  } else if (type == TYPE_CHUNKED) {
    f->open_array_section("chunk_map");
    for (auto& p : chunk_map) {
      f->open_object_section("chunk");
      f->dump_unsigned("offset", p.first);
      p.second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
}

void object_manifest_t::generate_test_instances(list<object_manifest_t*>& o)
{
  o.push_back(new object_manifest_t());
  o.back()->type = TYPE_REDIRECT;
}

ostream& operator<<(ostream& out, const object_manifest_t& om)
{
  out << "manifest(" << om.get_type_name();
  if (om.is_redirect()) {
    out << " " << om.redirect_target;
  } else if (om.is_chunked()) {
    out << " " << om.chunk_map;
  }
  out << ")";
  return out;
}

// -- object_info_t --

void object_info_t::copy_user_bits(const object_info_t& other)
{
  // these bits are copied from head->clone.
  size = other.size;
  mtime = other.mtime;
  local_mtime = other.local_mtime;
  last_reqid = other.last_reqid;
  truncate_seq = other.truncate_seq;
  truncate_size = other.truncate_size;
  flags = other.flags;
  user_version = other.user_version;
  data_digest = other.data_digest;
  omap_digest = other.omap_digest;
}

void object_info_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  object_locator_t myoloc(soid);
  map<entity_name_t, watch_info_t> old_watchers;
  for (auto i = watchers.cbegin(); i != watchers.cend(); ++i) {
    old_watchers.insert(make_pair(i->first.second, i->second));
  }
  ENCODE_START(17, 8, bl);
  encode(soid, bl);
  encode(myoloc, bl);	//Retained for compatibility
  encode((__u32)0, bl); // was category, no longer used
  encode(version, bl);
  encode(prior_version, bl);
  encode(last_reqid, bl);
  encode(size, bl);
  encode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP)
    encode(osd_reqid_t(), bl);  // used to be wrlock_by
  else
    encode((uint32_t)0, bl);    // was legacy_snaps
  encode(truncate_seq, bl);
  encode(truncate_size, bl);
  encode(is_lost(), bl);
  encode(old_watchers, bl, features);
  /* shenanigans to avoid breaking backwards compatibility in the disk format.
   * When we can, switch this out for simply putting the version_t on disk. */
  eversion_t user_eversion(0, user_version);
  encode(user_eversion, bl);
  encode(test_flag(FLAG_USES_TMAP), bl);
  encode(watchers, bl, features);
  __u32 _flags = flags;
  encode(_flags, bl);
  encode(local_mtime, bl);
  encode(data_digest, bl);
  encode(omap_digest, bl);
  encode(expected_object_size, bl);
  encode(expected_write_size, bl);
  encode(alloc_hint_flags, bl);
  if (has_manifest()) {
    encode(manifest, bl);
  }
  ENCODE_FINISH(bl);
}

void object_info_t::decode(ceph::buffer::list::const_iterator& bl)
{
  object_locator_t myoloc;
  DECODE_START_LEGACY_COMPAT_LEN(17, 8, 8, bl);
  map<entity_name_t, watch_info_t> old_watchers;
  decode(soid, bl);
  decode(myoloc, bl);
  {
    string category;
    decode(category, bl);  // no longer used
  }
  decode(version, bl);
  decode(prior_version, bl);
  decode(last_reqid, bl);
  decode(size, bl);
  decode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP) {
    osd_reqid_t wrlock_by;
    decode(wrlock_by, bl);
  } else {
    vector<snapid_t> legacy_snaps;
    decode(legacy_snaps, bl);
  }
  decode(truncate_seq, bl);
  decode(truncate_size, bl);

  // if this is struct_v >= 13, we will overwrite this
  // below since this field is just here for backwards
  // compatibility
  __u8 lo;
  decode(lo, bl);
  flags = (flag_t)lo;

  decode(old_watchers, bl);
  eversion_t user_eversion;
  decode(user_eversion, bl);
  user_version = user_eversion.version;

  if (struct_v >= 9) {
    bool uses_tmap = false;
    decode(uses_tmap, bl);
    if (uses_tmap)
      set_flag(FLAG_USES_TMAP);
  } else {
    set_flag(FLAG_USES_TMAP);
  }
  if (struct_v < 10)
    soid.pool = myoloc.pool;
  if (struct_v >= 11) {
    decode(watchers, bl);
  } else {
    for (auto i = old_watchers.begin(); i != old_watchers.end(); ++i) {
      watchers.insert(
	make_pair(
	  make_pair(i->second.cookie, i->first), i->second));
    }
  }
  if (struct_v >= 13) {
    __u32 _flags;
    decode(_flags, bl);
    flags = (flag_t)_flags;
  }
  if (struct_v >= 14) {
    decode(local_mtime, bl);
  } else {
    local_mtime = utime_t();
  }
  if (struct_v >= 15) {
    decode(data_digest, bl);
    decode(omap_digest, bl);
  } else {
    data_digest = omap_digest = -1;
    clear_flag(FLAG_DATA_DIGEST);
    clear_flag(FLAG_OMAP_DIGEST);
  }
  if (struct_v >= 16) {
    decode(expected_object_size, bl);
    decode(expected_write_size, bl);
    decode(alloc_hint_flags, bl);
  } else {
    expected_object_size = 0;
    expected_write_size = 0;
    alloc_hint_flags = 0;
  }
  if (struct_v >= 17) {
    if (has_manifest()) {
      decode(manifest, bl);
    }
  }
  DECODE_FINISH(bl);
}

void object_info_t::dump(Formatter *f) const
{
  f->open_object_section("oid");
  soid.dump(f);
  f->close_section();
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << prior_version;
  f->dump_stream("last_reqid") << last_reqid;
  f->dump_unsigned("user_version", user_version);
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("local_mtime") << local_mtime;
  f->dump_unsigned("lost", (int)is_lost());
  vector<string> sv = get_flag_vector(flags);
  f->open_array_section("flags");
  for (auto str: sv)
    f->dump_string("flags", str);
  f->close_section();
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_format("data_digest", "0x%08x", data_digest);
  f->dump_format("omap_digest", "0x%08x", omap_digest);
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
  f->dump_unsigned("alloc_hint_flags", alloc_hint_flags);
  f->dump_object("manifest", manifest);
  f->open_object_section("watchers");
  for (auto p = watchers.cbegin(); p != watchers.cend(); ++p) {
    stringstream ss;
    ss << p->first.second;
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
  if (oi.flags)
    out << " " << oi.get_flag_string();
  out << " s " << oi.size;
  out << " uv " << oi.user_version;
  if (oi.is_data_digest())
    out << " dd " << std::hex << oi.data_digest << std::dec;
  if (oi.is_omap_digest())
    out << " od " << std::hex << oi.omap_digest << std::dec;
  out << " alloc_hint [" << oi.expected_object_size
      << " " << oi.expected_write_size
      << " " << oi.alloc_hint_flags << "]";
  if (oi.has_manifest())
    out << " " << oi.manifest;
  out << ")";
  return out;
}

// -- ObjectRecovery --
void ObjectRecoveryProgress::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(first, bl);
  encode(data_complete, bl);
  encode(data_recovered_to, bl);
  encode(omap_recovered_to, bl);
  encode(omap_complete, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryProgress::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(first, bl);
  decode(data_complete, bl);
  decode(data_recovered_to, bl);
  decode(omap_recovered_to, bl);
  decode(omap_complete, bl);
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
	     << ", error:" << ( error ? "true" : "false" )
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

void ObjectRecoveryInfo::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(3, 1, bl);
  encode(soid, bl);
  encode(version, bl);
  encode(size, bl);
  encode(oi, bl, features);
  encode(ss, bl);
  encode(copy_subset, bl);
  encode(clone_subset, bl);
  encode(object_exist, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryInfo::decode(ceph::buffer::list::const_iterator &bl,
				int64_t pool)
{
  DECODE_START(3, bl);
  decode(soid, bl);
  decode(version, bl);
  decode(size, bl);
  decode(oi, bl);
  decode(ss, bl);
  decode(copy_subset, bl);
  decode(clone_subset, bl);
  if (struct_v > 2)
    decode(object_exist, bl);
  else
    object_exist = false;
  DECODE_FINISH(bl);
  if (struct_v < 2) {
    if (!soid.is_max() && soid.pool == -1)
      soid.pool = pool;
    map<hobject_t, interval_set<uint64_t>> tmp;
    tmp.swap(clone_subset);
    for (auto i = tmp.begin(); i != tmp.end(); ++i) {
      hobject_t first(i->first);
      if (!first.is_max() && first.pool == -1)
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
  o.back()->object_exist = false;
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
  f->dump_stream("object_exist") << object_exist;
}

ostream& operator<<(ostream& out, const ObjectRecoveryInfo &inf)
{
  return inf.print(out);
}

ostream &ObjectRecoveryInfo::print(ostream &out) const
{
  return out << "ObjectRecoveryInfo("
	     << soid << "@" << version
	     << ", size: " << size
	     << ", copy_subset: " << copy_subset
	     << ", clone_subset: " << clone_subset
	     << ", snapset: " << ss
	     << ", object_exist: " << object_exist
	     << ")";
}

// -- PushReplyOp --
void PushReplyOp::generate_test_instances(list<PushReplyOp*> &o)
{
  o.push_back(new PushReplyOp);
  o.push_back(new PushReplyOp);
  o.back()->soid = hobject_t(sobject_t("asdf", 2));
  o.push_back(new PushReplyOp);
  o.back()->soid = hobject_t(sobject_t("asdf", CEPH_NOSNAP));
}

void PushReplyOp::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(soid, bl);
  ENCODE_FINISH(bl);
}

void PushReplyOp::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(soid, bl);
  DECODE_FINISH(bl);
}

void PushReplyOp::dump(Formatter *f) const
{
  f->dump_stream("soid") << soid;
}

ostream &PushReplyOp::print(ostream &out) const
{
  return out
    << "PushReplyOp(" << soid
    << ")";
}

ostream& operator<<(ostream& out, const PushReplyOp &op)
{
  return op.print(out);
}

uint64_t PushReplyOp::cost(CephContext *cct) const
{

  return cct->_conf->osd_push_per_object_cost +
    cct->_conf->osd_recovery_max_chunk;
}

// -- PullOp --
void PullOp::generate_test_instances(list<PullOp*> &o)
{
  o.push_back(new PullOp);
  o.push_back(new PullOp);
  o.back()->soid = hobject_t(sobject_t("asdf", 2));
  o.back()->recovery_info.version = eversion_t(3, 10);
  o.push_back(new PullOp);
  o.back()->soid = hobject_t(sobject_t("asdf", CEPH_NOSNAP));
  o.back()->recovery_info.version = eversion_t(0, 0);
}

void PullOp::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(soid, bl);
  encode(recovery_info, bl, features);
  encode(recovery_progress, bl);
  ENCODE_FINISH(bl);
}

void PullOp::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(soid, bl);
  decode(recovery_info, bl);
  decode(recovery_progress, bl);
  DECODE_FINISH(bl);
}

void PullOp::dump(Formatter *f) const
{
  f->dump_stream("soid") << soid;
  {
    f->open_object_section("recovery_info");
    recovery_info.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("recovery_progress");
    recovery_progress.dump(f);
    f->close_section();
  }
}

ostream &PullOp::print(ostream &out) const
{
  return out
    << "PullOp(" << soid
    << ", recovery_info: " << recovery_info
    << ", recovery_progress: " << recovery_progress
    << ")";
}

ostream& operator<<(ostream& out, const PullOp &op)
{
  return op.print(out);
}

uint64_t PullOp::cost(CephContext *cct) const
{
  return cct->_conf->osd_push_per_object_cost +
    cct->_conf->osd_recovery_max_chunk;
}

// -- PushOp --
void PushOp::generate_test_instances(list<PushOp*> &o)
{
  o.push_back(new PushOp);
  o.push_back(new PushOp);
  o.back()->soid = hobject_t(sobject_t("asdf", 2));
  o.back()->version = eversion_t(3, 10);
  o.push_back(new PushOp);
  o.back()->soid = hobject_t(sobject_t("asdf", CEPH_NOSNAP));
  o.back()->version = eversion_t(0, 0);
}

void PushOp::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(soid, bl);
  encode(version, bl);
  encode(data, bl);
  encode(data_included, bl);
  encode(omap_header, bl);
  encode(omap_entries, bl);
  encode(attrset, bl);
  encode(recovery_info, bl, features);
  encode(after_progress, bl);
  encode(before_progress, bl);
  ENCODE_FINISH(bl);
}

void PushOp::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(soid, bl);
  decode(version, bl);
  decode(data, bl);
  decode(data_included, bl);
  decode(omap_header, bl);
  decode(omap_entries, bl);
  decode(attrset, bl);
  decode(recovery_info, bl);
  decode(after_progress, bl);
  decode(before_progress, bl);
  DECODE_FINISH(bl);
}

void PushOp::dump(Formatter *f) const
{
  f->dump_stream("soid") << soid;
  f->dump_stream("version") << version;
  f->dump_int("data_len", data.length());
  f->dump_stream("data_included") << data_included;
  f->dump_int("omap_header_len", omap_header.length());
  f->dump_int("omap_entries_len", omap_entries.size());
  f->dump_int("attrset_len", attrset.size());
  {
    f->open_object_section("recovery_info");
    recovery_info.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("after_progress");
    after_progress.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("before_progress");
    before_progress.dump(f);
    f->close_section();
  }
}

ostream &PushOp::print(ostream &out) const
{
  return out
    << "PushOp(" << soid
    << ", version: " << version
    << ", data_included: " << data_included
    << ", data_size: " << data.length()
    << ", omap_header_size: " << omap_header.length()
    << ", omap_entries_size: " << omap_entries.size()
    << ", attrset_size: " << attrset.size()
    << ", recovery_info: " << recovery_info
    << ", after_progress: " << after_progress
    << ", before_progress: " << before_progress
    << ")";
}

ostream& operator<<(ostream& out, const PushOp &op)
{
  return op.print(out);
}

uint64_t PushOp::cost(CephContext *cct) const
{
  uint64_t cost = data_included.size();
  for (auto i = omap_entries.cbegin(); i != omap_entries.cend(); ++i) {
    cost += i->second.length();
  }
  cost += cct->_conf->osd_push_per_object_cost;
  return cost;
}

// -- ScrubMap --

void ScrubMap::merge_incr(const ScrubMap &l)
{
  ceph_assert(valid_through == l.incr_since);
  valid_through = l.valid_through;

  for (auto p = l.objects.cbegin(); p != l.objects.cend(); ++p){
    if (p->second.negative) {
      auto q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}          

void ScrubMap::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(objects, bl);
  encode((__u32)0, bl); // used to be attrs; now deprecated
  ceph::buffer::list old_logbl;  // not used
  encode(old_logbl, bl);
  encode(valid_through, bl);
  encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::decode(ceph::buffer::list::const_iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(objects, bl);
  {
    map<string,string> attrs;  // deprecated
    decode(attrs, bl);
  }
  ceph::buffer::list old_logbl;   // not used
  decode(old_logbl, bl);
  decode(valid_through, bl);
  decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (auto i = tmp.begin(); i != tmp.end(); ++i) {
      hobject_t first(i->first);
      if (!first.is_max() && first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void ScrubMap::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("objects");
  for (auto p = objects.cbegin(); p != objects.cend(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.get_hash());
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
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
}

// -- ScrubMap::object --

void ScrubMap::object::encode(ceph::buffer::list& bl) const
{
  bool compat_read_error = read_error || ec_hash_mismatch || ec_size_mismatch;
  ENCODE_START(10, 7, bl);
  encode(size, bl);
  encode(negative, bl);
  encode(attrs, bl);
  encode(digest, bl);
  encode(digest_present, bl);
  encode((uint32_t)0, bl);  // obsolete nlinks
  encode((uint32_t)0, bl);  // snapcolls
  encode(omap_digest, bl);
  encode(omap_digest_present, bl);
  encode(compat_read_error, bl);
  encode(stat_error, bl);
  encode(read_error, bl);
  encode(ec_hash_mismatch, bl);
  encode(ec_size_mismatch, bl);
  encode(large_omap_object_found, bl);
  encode(large_omap_object_key_count, bl);
  encode(large_omap_object_value_size, bl);
  encode(object_omap_bytes, bl);
  encode(object_omap_keys, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::object::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(10, bl);
  decode(size, bl);
  bool tmp, compat_read_error = false;
  decode(tmp, bl);
  negative = tmp;
  decode(attrs, bl);
  decode(digest, bl);
  decode(tmp, bl);
  digest_present = tmp;
  {
    uint32_t nlinks;
    decode(nlinks, bl);
    set<snapid_t> snapcolls;
    decode(snapcolls, bl);
  }
  decode(omap_digest, bl);
  decode(tmp, bl);
  omap_digest_present = tmp;
  decode(compat_read_error, bl);
  decode(tmp, bl);
  stat_error = tmp;
  if (struct_v >= 8) {
    decode(tmp, bl);
    read_error = tmp;
    decode(tmp, bl);
    ec_hash_mismatch = tmp;
    decode(tmp, bl);
    ec_size_mismatch = tmp;
  }
  // If older encoder found a read_error, set read_error
  if (compat_read_error && !read_error && !ec_hash_mismatch && !ec_size_mismatch)
    read_error = true;
  if (struct_v >= 9) {
    decode(tmp, bl);
    large_omap_object_found = tmp;
    decode(large_omap_object_key_count, bl);
    decode(large_omap_object_value_size, bl);
  }
  if (struct_v >= 10) {
    decode(object_omap_bytes, bl);
    decode(object_omap_keys, bl);
  }
  DECODE_FINISH(bl);
}

void ScrubMap::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (auto p = attrs.cbegin(); p != attrs.cend(); ++p) {
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
  o.back()->attrs["foo"] = ceph::buffer::copy("foo", 3);
  o.back()->attrs["bar"] = ceph::buffer::copy("barval", 6);
}

// -- OSDOp --

ostream& operator<<(ostream& out, const OSDOp& op)
{
  out << ceph_osd_op_name(op.op.op);
  if (ceph_osd_op_type_data(op.op.op)) {
    // data extent
    switch (op.op.op) {
    case CEPH_OSD_OP_ASSERT_VER:
      out << " v" << op.op.assert_ver.ver;
      break;
    case CEPH_OSD_OP_TRUNCATE:
      out << " " << op.op.extent.offset;
      break;
    case CEPH_OSD_OP_MASKTRUNC:
    case CEPH_OSD_OP_TRIMTRUNC:
      out << " " << op.op.extent.truncate_seq << "@"
	  << (int64_t)op.op.extent.truncate_size;
      break;
    case CEPH_OSD_OP_ROLLBACK:
      out << " " << snapid_t(op.op.snap.snapid);
      break;
    case CEPH_OSD_OP_WATCH:
      out << " " << ceph_osd_watch_op_name(op.op.watch.op)
	  << " cookie " << op.op.watch.cookie;
      if (op.op.watch.gen)
	out << " gen " << op.op.watch.gen;
      break;
    case CEPH_OSD_OP_NOTIFY:
      out << " cookie " << op.op.notify.cookie;
      break;
    case CEPH_OSD_OP_COPY_GET:
      out << " max " << op.op.copy_get.max;
      break;
    case CEPH_OSD_OP_COPY_FROM:
      out << " ver " << op.op.copy_from.src_version;
      break;
    case CEPH_OSD_OP_SETALLOCHINT:
      out << " object_size " << op.op.alloc_hint.expected_object_size
          << " write_size " << op.op.alloc_hint.expected_write_size;
      break;
    case CEPH_OSD_OP_READ:
    case CEPH_OSD_OP_SPARSE_READ:
    case CEPH_OSD_OP_SYNC_READ:
    case CEPH_OSD_OP_WRITE:
    case CEPH_OSD_OP_WRITEFULL:
    case CEPH_OSD_OP_ZERO:
    case CEPH_OSD_OP_APPEND:
    case CEPH_OSD_OP_MAPEXT:
    case CEPH_OSD_OP_CMPEXT:
      out << " " << op.op.extent.offset << "~" << op.op.extent.length;
      if (op.op.extent.truncate_seq)
	out << " [" << op.op.extent.truncate_seq << "@"
	    << (int64_t)op.op.extent.truncate_size << "]";
      if (op.op.flags)
	out << " [" << ceph_osd_op_flag_string(op.op.flags) << "]";
    default:
      // don't show any arg info
      break;
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
      out << " op " << (int)op.op.xattr.cmp_op
	  << " mode " << (int)op.op.xattr.cmp_mode;
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
    case CEPH_OSD_OP_PGNLS:
    case CEPH_OSD_OP_PGNLS_FILTER:
      out << " start_epoch " << op.op.pgls.start_epoch;
      break;
    case CEPH_OSD_OP_PG_HITSET_LS:
      break;
    case CEPH_OSD_OP_PG_HITSET_GET:
      out << " " << utime_t(op.op.hit_set_get.stamp);
      break;
    case CEPH_OSD_OP_SCRUBLS:
      break;
    }
  }
  if (op.indata.length()) {
    out << " in=" << op.indata.length() << "b";
  }
  if (op.outdata.length()) {
    out << " out=" << op.outdata.length() << "b";
  }
  return out;
}


void OSDOp::split_osd_op_vector_out_data(vector<OSDOp>& ops, ceph::buffer::list& in)
{
  auto datap = in.begin();
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ops[i].op.payload_len) {
      datap.copy(ops[i].op.payload_len, ops[i].outdata);
    }
  }
}

void OSDOp::merge_osd_op_vector_out_data(vector<OSDOp>& ops, ceph::buffer::list& out)
{
  for (unsigned i = 0; i < ops.size(); i++) {
    ops[i].op.payload_len = ops[i].outdata.length();
    if (ops[i].outdata.length()) {
      out.append(ops[i].outdata);
    }
  }
}

int prepare_info_keymap(
  CephContext* cct,
  map<string,bufferlist> *km,
  string *key_to_remove,
  epoch_t epoch,
  pg_info_t &info,
  pg_info_t &last_written_info,
  PastIntervals &past_intervals,
  bool dirty_big_info,
  bool dirty_epoch,
  bool try_fast_info,
  PerfCounters *logger,
  DoutPrefixProvider *dpp)
{
  if (dirty_epoch) {
    encode(epoch, (*km)[string(epoch_key)]);
  }

  if (logger)
    logger->inc(l_osd_pg_info);

  // try to do info efficiently?
  if (!dirty_big_info && try_fast_info &&
      info.last_update > last_written_info.last_update) {
    pg_fast_info_t fast;
    fast.populate_from(info);
    bool did = fast.try_apply_to(&last_written_info);
    ceph_assert(did);  // we verified last_update increased above
    if (info == last_written_info) {
      encode(fast, (*km)[string(fastinfo_key)]);
      if (logger)
	logger->inc(l_osd_pg_fastinfo);
      return 0;
    }
    if (dpp) {
      ldpp_dout(dpp, 30) << __func__ << " fastinfo failed, info:\n";
      {
	JSONFormatter jf(true);
	jf.dump_object("info", info);
	jf.flush(*_dout);
      }
      {
	*_dout << "\nlast_written_info:\n";
	JSONFormatter jf(true);
	jf.dump_object("last_written_info", last_written_info);
	jf.flush(*_dout);
      }
      *_dout << dendl;
    }
  } else if (info.last_update <= last_written_info.last_update) {
    // clean up any potentially stale fastinfo key resulting from last_update
    // not moving forwards (e.g., a backwards jump during peering)
    *key_to_remove = fastinfo_key;
  }

  last_written_info = info;

  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  purged_snaps.swap(info.purged_snaps);
  encode(info, (*km)[string(info_key)]);
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = (*km)[string(biginfo_key)];
    encode(past_intervals, bigbl);
    encode(info.purged_snaps, bigbl);
    //dout(20) << "write_info bigbl " << bigbl.length() << dendl;
    if (logger)
      logger->inc(l_osd_pg_biginfo);
  }

  return 0;
}

void create_pg_collection(
  ceph::os::Transaction& t, spg_t pgid, int bits)
{
  coll_t coll(pgid);
  t.create_collection(coll, bits);
}

void init_pg_ondisk(
  ceph::os::Transaction& t,
  spg_t pgid,
  const pg_pool_t *pool)
{
  coll_t coll(pgid);
  if (pool) {
    // Give a hint to the PG collection
    bufferlist hint;
    uint32_t pg_num = pool->get_pg_num();
    uint64_t expected_num_objects_pg = pool->expected_num_objects / pg_num;
    encode(pg_num, hint);
    encode(expected_num_objects_pg, hint);
    uint32_t hint_type = ceph::os::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS;
    t.collection_hint(coll, hint_type, hint);
  }

  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  t.touch(coll, pgmeta_oid);
  map<string,bufferlist> values;
  __u8 struct_v = pg_latest_struct_v;
  encode(struct_v, values[string(infover_key)]);
  t.omap_setkeys(coll, pgmeta_oid, values);
}

PGLSFilter::PGLSFilter() : cct(nullptr)
{
}

PGLSFilter::~PGLSFilter()
{
}

int PGLSPlainFilter::init(ceph::bufferlist::const_iterator &params)
{
  try {
    decode(xattr, params);
    decode(val, params);
  } catch (ceph::buffer::error &e) {
    return -EINVAL;
  }
  return 0;
}

bool PGLSPlainFilter::filter(const hobject_t& obj,
                             const ceph::bufferlist& xattr_data) const
{
  return xattr_data.contents_equal(val.c_str(), val.size());
}
