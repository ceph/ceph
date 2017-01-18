// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OSDMapMapping.h"
#include "OSDMap.h"

// ensure that we have a PoolMappings for each pool and that
// the dimensions (pg_num and size) match up.
void OSDMapMapping::_init_mappings(const OSDMap& osdmap)
{
  auto q = pools.begin();
  for (auto& p : osdmap.get_pools()) {
    // drop unneeded pools
    while (q != pools.end() && q->first < p.first) {
      q = pools.erase(q);
    }
    if (q != pools.end() && q->first == p.first) {
      if (q->second.pg_num != p.second.get_pg_num() ||
	  q->second.size != p.second.get_size()) {
	// pg_num changed
	q = pools.erase(q);
      } else {
	// keep it
	++q;
	continue;
      }
    }
    pools.emplace(p.first, PoolMapping(p.second.get_size(),
				       p.second.get_pg_num()));
  }
  pools.erase(q, pools.end());
  assert(pools.size() == osdmap.get_pools().size());
}

void OSDMapMapping::update(const OSDMap& osdmap)
{
  _init_mappings(osdmap);
  for (auto& p : osdmap.get_pools()) {
    _update_range(osdmap, p.first, 0, p.second.get_pg_num());
  }
  _build_rmap(osdmap);
  //_dump();  // for debugging
}

void OSDMapMapping::_build_rmap(const OSDMap& osdmap)
{
  acting_rmap.resize(osdmap.get_max_osd());
  up_rmap.resize(osdmap.get_max_osd());
  for (auto& v : acting_rmap) {
    v.resize(0);
  }
  for (auto& v : up_rmap) {
    v.resize(0);
  }
  for (auto& p : pools) {
    pg_t pgid(0, p.first);
    for (unsigned ps = 0; ps < p.second.pg_num; ++ps) {
      pgid.set_ps(ps);
      int32_t *row = &p.second.table[p.second.row_size() * ps];
      for (int i = 0; i < row[2]; ++i) {
	if (row[4 + i] != CRUSH_ITEM_NONE) {
	  acting_rmap[row[4 + i]].push_back(pgid);
	}
      }
      for (int i = 0; i < row[3]; ++i) {
	up_rmap[row[4 + p.second.size + i]].push_back(pgid);
      }
    }
  }
}

void OSDMapMapping::_dump()
{
  for (auto& p : pools) {
    cout << "pool " << p.first << std::endl;
    for (unsigned i = 0; i < p.second.table.size(); ++i) {
      cout << " " << p.second.table[i];
      if (i % p.second.row_size() == p.second.row_size() - 1)
	cout << std::endl;
    }
  }
}

void OSDMapMapping::_update_range(
  const OSDMap& osdmap,
  int64_t pool,
  unsigned pg_begin,
  unsigned pg_end)
{
  auto i = pools.find(pool);
  assert(i != pools.end());
  assert(pg_begin <= pg_end);
  assert(pg_end <= i->second.pg_num);
  for (unsigned ps = pg_begin; ps < pg_end; ++ps) {
    vector<int> up, acting;
    int up_primary, acting_primary;
    osdmap.pg_to_up_acting_osds(
      pg_t(ps, pool),
      &up, &up_primary, &acting, &acting_primary);
    i->second.set(ps, std::move(up), up_primary,
		  std::move(acting), acting_primary);
  }
}
