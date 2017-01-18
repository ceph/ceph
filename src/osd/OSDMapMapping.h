// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_OSDMAPMAPPING_H
#define CEPH_OSDMAPMAPPING_H

#include <vector>
#include <map>

#include "osd/osd_types.h"

class OSDMap;

/// a precalculated mapping of every PG for a given OSDMap
class OSDMapMapping {
  struct PoolMapping {
    unsigned size = 0;
    unsigned pg_num = 0;
    std::vector<int32_t> table;

    size_t row_size() const {
      return
	1 + // acting_primary
	1 + // up_primary
	1 + // num acting
	1 + // num up
	size + // acting
	size;  // up
    }

    PoolMapping(int s, int p)
      : size(s),
	pg_num(p),
	table(pg_num * row_size()) {
    }

    void get(size_t ps,
	     std::vector<int> *up,
	     int *up_primary,
	     std::vector<int> *acting,
	     int *acting_primary) {
      int32_t *row = &table[row_size() * ps];
      if (acting_primary) {
	*acting_primary = row[0];
      }
      if (up_primary) {
	*up_primary = row[1];
      }
      if (acting) {
	acting->resize(row[2]);
	for (int i = 0; i < row[2]; ++i) {
	  (*acting)[i] = row[4 + i];
	}
      }
      if (up) {
	up->resize(row[3]);
	for (int i = 0; i < row[3]; ++i) {
	  (*up)[i] = row[4 + size + i];
	}
      }
    }

    void set(size_t ps,
	     const std::vector<int>& up,
	     int up_primary,
	     const std::vector<int>& acting,
	     int acting_primary) {
      int32_t *row = &table[row_size() * ps];
      row[0] = acting_primary;
      row[1] = up_primary;
      row[2] = acting.size();
      row[3] = up.size();
      for (int i = 0; i < row[2]; ++i) {
	row[4 + i] = acting[i];
      }
      for (int i = 0; i < row[3]; ++i) {
	row[4 + size + i] = up[i];
      }
    }
  };

  std::map<int64_t,PoolMapping> pools;
  std::vector<std::vector<pg_t>> acting_rmap;  // osd -> pg
  std::vector<std::vector<pg_t>> up_rmap;  // osd -> pg

  void _init_mappings(const OSDMap& osdmap);
  void _update_range(
    const OSDMap& map,
    int64_t pool,
    unsigned pg_begin, unsigned pg_end);

  void _build_rmap(const OSDMap& osdmap);

  void _dump();

public:
  void get(pg_t pgid,
	   std::vector<int> *up,
	   int *up_primary,
	   std::vector<int> *acting,
	   int *acting_primary) {
    auto p = pools.find(pgid.pool());
    assert(p != pools.end());
    p->second.get(pgid.ps(), up, up_primary, acting, acting_primary);
  }

  const std::vector<pg_t>& get_osd_acting_pgs(unsigned osd) {
    assert(osd < acting_rmap.size());
    return acting_rmap[osd];
  }
  const std::vector<pg_t>& get_osd_up_pgs(unsigned osd) {
    assert(osd < up_rmap.size());
    return up_rmap[osd];
  }

  void update(const OSDMap& map);
};


#endif
