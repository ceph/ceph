#pragma once

#include <mds/MDSMap.h>

class MDSTestMap: public MDSMap
{
public:
  MDSTestMap(int max_mds = 1)
  {
    this->max_mds = max_mds;
    data_pools.push_back(0);
    metadata_pool = 1;
    cas_pool = 2;
    compat = get_compat_set_all();
  }

  mds_info_t *add_rank(mds_rank_t rank) {
    mds_info_t info;
    info.global_id = 1;
    info.name = "test_instance";
    info.rank = rank;
    info.state = STATE_ACTIVE;

    in.insert(rank);
    up[rank] = info.global_id;

    auto [info_it, _] = mds_info.insert({ info.global_id, info});
    return &info_it->second;
  }
};
