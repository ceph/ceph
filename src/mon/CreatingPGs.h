// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <set>
#include "include/encoding.h"

struct creating_pgs_t {
  epoch_t last_scan_epoch = 0;
  std::map<pg_t, std::pair<epoch_t, utime_t> > pgs;
  std::set<int64_t> created_pools;
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(last_scan_epoch, bl);
    ::encode(pgs, bl);
    ::encode(created_pools, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(last_scan_epoch, bl);
    ::decode(pgs, bl);
    ::decode(created_pools, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_object_section("creating_pgs");
    f->dump_unsigned("last_scan_epoch", last_scan_epoch);
    for (auto& pg : pgs) {
      f->open_object_section("pg");
      f->dump_stream("pgid") << pg.first;
      f->dump_unsigned("epoch", pg.second.first);
      f->dump_stream("ctime") << pg.second.second;
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
WRITE_CLASS_ENCODER(creating_pgs_t);
