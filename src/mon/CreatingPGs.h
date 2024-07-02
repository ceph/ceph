// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <set>
#include <vector>

#include "include/encoding.h"
#include "include/utime.h"

#include "osd/osd_types.h"

struct creating_pgs_t {
  epoch_t last_scan_epoch = 0;

  struct pg_create_info {
    epoch_t create_epoch;
    utime_t create_stamp;

    // NOTE: pre-octopus instances of this class will have a
    // zeroed-out history
    std::vector<int> up;
    int up_primary = -1;
    std::vector<int> acting;
    int acting_primary = -1;
    pg_history_t history;
    PastIntervals past_intervals;

    void encode(ceph::buffer::list& bl, uint64_t features) const {
      using ceph::encode;
      if (!HAVE_FEATURE(features, SERVER_OCTOPUS)) {
	// was pair<epoch_t,utime_t> prior to octopus
	encode(create_epoch, bl);
	encode(create_stamp, bl);
	return;
      }
      ENCODE_START(1, 1, bl);
      encode(create_epoch, bl);
      encode(create_stamp, bl);
      encode(up, bl);
      encode(up_primary, bl);
      encode(acting, bl);
      encode(acting_primary, bl);
      encode(history, bl);
      encode(past_intervals, bl);
      ENCODE_FINISH(bl);
    }
    void decode_legacy(ceph::buffer::list::const_iterator& p) {
      using ceph::decode;
      decode(create_epoch, p);
      decode(create_stamp, p);
    }
    void decode(ceph::buffer::list::const_iterator& p) {
      using ceph::decode;
      DECODE_START(1, p);
      decode(create_epoch, p);
      decode(create_stamp, p);
      decode(up, p);
      decode(up_primary, p);
      decode(acting, p);
      decode(acting_primary, p);
      decode(history, p);
      decode(past_intervals, p);
      DECODE_FINISH(p);
    }
    void dump(ceph::Formatter *f) const {
      f->dump_unsigned("create_epoch", create_epoch);
      f->dump_stream("create_stamp") << create_stamp;
      f->open_array_section("up");
      for (auto& i : up) {
	f->dump_unsigned("osd", i);
      }
      f->close_section();
      f->dump_int("up_primary", up_primary);
      f->open_array_section("acting");
      for (auto& i : acting) {
	f->dump_unsigned("osd", i);
      }
      f->close_section();
      f->dump_int("acting_primary", up_primary);
      f->dump_object("pg_history", history);
      f->dump_object("past_intervals", past_intervals);
    }
    static void generate_test_instances(std::list<pg_create_info*>& o) {
      o.push_back(new pg_create_info);
      o.back()->create_epoch = 10;
      o.push_back(new pg_create_info);
      o.back()->create_epoch = 1;
      o.back()->create_stamp = utime_t(2, 3);
      o.back()->up = {1, 2, 3};
      o.back()->up_primary = 1;
      o.back()->acting = {1, 2, 3};
      o.back()->acting_primary = 1;
    }

    pg_create_info() 
      : create_epoch(0) {}
    pg_create_info(epoch_t e, utime_t t)
      : create_epoch(e),
	create_stamp(t) {
      // NOTE: we don't initialize the other fields here; see
      // OSDMonitor::update_pending_pgs()
    }
  };

  /// pgs we are currently creating
  std::map<pg_t, pg_create_info> pgs;

  struct pool_create_info {
    epoch_t created;
    utime_t modified;
    uint64_t start = 0;
    uint64_t end = 0;
    bool done() const {
      return start >= end;
    }
    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(created, bl);
      encode(modified, bl);
      encode(start, bl);
      encode(end, bl);
    }
    void decode(ceph::buffer::list::const_iterator& p) {
      using ceph::decode;
      decode(created, p);
      decode(modified, p);
      decode(start, p);
      decode(end, p);
    }
  };

  /// queue of pgs we still need to create (poolid -> <created, set of ps>)
  std::map<int64_t,pool_create_info> queue;

  /// pools that exist in the osdmap for which at least one pg has been created
  std::set<int64_t> created_pools;

  bool still_creating_pool(int64_t poolid) {
    for (auto& i : pgs) {
      if (i.first.pool() == poolid) {
	return true;
      }
    }
    if (queue.count(poolid)) {
      return true;
    }
    return false;
  }
  void create_pool(int64_t poolid, uint32_t pg_num,
		   epoch_t created, utime_t modified) {
    ceph_assert(created_pools.count(poolid) == 0);
    auto& c = queue[poolid];
    c.created = created;
    c.modified = modified;
    c.end = pg_num;
    created_pools.insert(poolid);
  }
  unsigned remove_pool(int64_t removed_pool) {
    const unsigned total = pgs.size();
    auto first = pgs.lower_bound(pg_t{0, (uint64_t)removed_pool});
    auto last = pgs.lower_bound(pg_t{0, (uint64_t)removed_pool + 1});
    pgs.erase(first, last);
    created_pools.erase(removed_pool);
    queue.erase(removed_pool);
    return total - pgs.size();
  }
  void encode(ceph::buffer::list& bl, uint64_t features) const {
    unsigned v = 3;
    if (!HAVE_FEATURE(features, SERVER_OCTOPUS)) {
      v = 2;
    }
    ENCODE_START(v, 1, bl);
    encode(last_scan_epoch, bl);
    encode(pgs, bl, features);
    encode(created_pools, bl);
    encode(queue, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(last_scan_epoch, bl);
    if (struct_v >= 3) {
      decode(pgs, bl);
    } else {
      // legacy pg encoding
      pgs.clear();
      uint32_t num;
      decode(num, bl);
      while (num--) {
	pg_t pgid;
	decode(pgid, bl);
	pgs[pgid].decode_legacy(bl);
      }
    }
    decode(created_pools, bl);
    if (struct_v >= 2)
      decode(queue, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("last_scan_epoch", last_scan_epoch);
    f->open_array_section("creating_pgs");
    for (auto& pg : pgs) {
      f->open_object_section("pg");
      f->dump_stream("pgid") << pg.first;
      f->dump_object("pg_create_info", pg.second);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queue");
    for (auto& p : queue) {
      f->open_object_section("pool");
      f->dump_unsigned("pool", p.first);
      f->dump_unsigned("created", p.second.created);
      f->dump_stream("modified") << p.second.modified;
      f->dump_unsigned("ps_start", p.second.start);
      f->dump_unsigned("ps_end", p.second.end);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("created_pools");
    for (auto pool : created_pools) {
      f->dump_unsigned("pool", pool);
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<creating_pgs_t*>& o) {
    auto c = new creating_pgs_t;
    c->last_scan_epoch = 17;
    c->pgs.emplace(pg_t{42, 2}, pg_create_info(31, utime_t{891, 113}));
    c->pgs.emplace(pg_t{44, 2}, pg_create_info(31, utime_t{891, 113}));
    c->created_pools = {0, 1};
    o.push_back(c);
    c = new creating_pgs_t;
    c->last_scan_epoch = 18;
    c->pgs.emplace(pg_t{42, 3}, pg_create_info(31, utime_t{891, 113}));
    c->created_pools = {};
    o.push_back(c);
  }
};
WRITE_CLASS_ENCODER_FEATURES(creating_pgs_t::pg_create_info)
WRITE_CLASS_ENCODER(creating_pgs_t::pool_create_info)
WRITE_CLASS_ENCODER_FEATURES(creating_pgs_t)
