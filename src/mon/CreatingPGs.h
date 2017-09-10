// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <set>
#include "include/encoding.h"

struct creating_pgs_t {
  epoch_t last_scan_epoch = 0;

  /// pgs we are currently creating
  std::map<pg_t, std::pair<epoch_t, utime_t> > pgs;

  struct create_info {
    epoch_t created;
    utime_t modified;
    uint64_t start = 0;
    uint64_t end = 0;
    bool done() const {
      return start >= end;
    }
    void encode(bufferlist& bl) const {
      ::encode(created, bl);
      ::encode(modified, bl);
      ::encode(start, bl);
      ::encode(end, bl);
    }
    void decode(bufferlist::iterator& p) {
      ::decode(created, p);
      ::decode(modified, p);
      ::decode(start, p);
      ::decode(end, p);
    }
  };

  /// queue of pgs we still need to create (poolid -> <created, set of ps>)
  map<int64_t,create_info> queue;

  /// pools that exist in the osdmap for which at least one pg has been created
  std::set<int64_t> created_pools;

  bool create_pool(int64_t poolid, uint32_t pg_num,
		   epoch_t created, utime_t modified) {
    if (created_pools.count(poolid) == 0) {
      auto& c = queue[poolid];
      c.created = created;
      c.modified = modified;
      c.end = pg_num;
      created_pools.insert(poolid);
      return true;
    } else {
      return false;
    }
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
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(last_scan_epoch, bl);
    ::encode(pgs, bl);
    ::encode(created_pools, bl);
    ::encode(queue, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(last_scan_epoch, bl);
    ::decode(pgs, bl);
    ::decode(created_pools, bl);
    if (struct_v >= 2)
      ::decode(queue, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("last_scan_epoch", last_scan_epoch);
    f->open_array_section("creating_pgs");
    for (auto& pg : pgs) {
      f->open_object_section("pg");
      f->dump_stream("pgid") << pg.first;
      f->dump_unsigned("epoch", pg.second.first);
      f->dump_stream("ctime") << pg.second.second;
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
  static void generate_test_instances(list<creating_pgs_t*>& o) {
    auto c = new creating_pgs_t;
    c->last_scan_epoch = 17;
    c->pgs.emplace(pg_t{42, 2}, make_pair(31, utime_t{891, 113}));
    c->pgs.emplace(pg_t{44, 2}, make_pair(31, utime_t{891, 113}));
    c->created_pools = {0, 1};
    o.push_back(c);
    c = new creating_pgs_t;
    c->last_scan_epoch = 18;
    c->pgs.emplace(pg_t{42, 3}, make_pair(31, utime_t{891, 113}));
    c->created_pools = {};
    o.push_back(c);
  }
};
WRITE_CLASS_ENCODER(creating_pgs_t::create_info);
WRITE_CLASS_ENCODER(creating_pgs_t);
