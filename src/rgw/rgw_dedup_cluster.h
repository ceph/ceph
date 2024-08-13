#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_dedup_store.h"
#include <string>
namespace rgw::dedup {
  class cluster{
  public:
    cluster(rgw::sal::RadosStore       *_store,
	    const DoutPrefixProvider   *_dpp);

    work_shard_t get_next_work_shard();
    void         mark_work_shard_completed();
    bool         all_work_shards_completed(uint32_t *ttl);
    md5_shard_t  get_next_md5_shard();
    void         mark_md5_shard_completed();
  private:
    void assign_cluster_id();
    int connect();
    void cleanup_prev_run();
    rgw::sal::RadosStore     *store;
    const DoutPrefixProvider *dpp;
    std::string cluster_id;
  };

} //namespace rgw::dedup
