#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup_store.h"
#include <string>

namespace rgw::dedup {
#define DEDUP_POOL_NAME       "rgw_dedup_pool"
#define MD5_SHARD_PREFIX      "MD5.SHRD.TKN."
#define WORKER_SHARD_PREFIX   "WRK.SHRD.TKN."
  int   init_dedup_pool_ioctx(RGWRados                 *rados,
			      const DoutPrefixProvider *dpp,
			      librados::IoCtx          *p_ioctx);

  class cluster{
  public:
    cluster(const DoutPrefixProvider   *_dpp);

    int          init(rgw::sal::RadosStore *store, librados::IoCtx *p_ioctx);
    work_shard_t get_next_work_shard_token(librados::IoCtx *p_ioctx);
    md5_shard_t  get_next_md5_shard_token(librados::IoCtx *p_ioctx);

    static int   collect_all_md5_shard_stats(RGWRados *rados,
					     const DoutPrefixProvider *dpp,
					     md5_stats_t *p_stats/* OUT-PARAM */);

    static int   collect_all_work_shard_stats(RGWRados *rados,
					      const DoutPrefixProvider *dpp,
					      worker_stats_t *p_stats/*OUT-PARAM*/);

    //---------------------------------------------------------------------------
    int mark_work_shard_token_completed(librados::IoCtx *p_ioctx,
					work_shard_t work_shard,
					const worker_stats_t *p_stats) {
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      return mark_shard_token_completed(p_ioctx, work_shard, WORKER_SHARD_PREFIX, bl);
    }

    //---------------------------------------------------------------------------
    int mark_md5_shard_token_completed(librados::IoCtx *p_ioctx,
				       md5_shard_t md5_shard,
				       const md5_stats_t *p_stats){
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      return mark_shard_token_completed(p_ioctx, md5_shard, MD5_SHARD_PREFIX, bl);
    }

    //---------------------------------------------------------------------------
    int update_work_shard_token_heartbeat(librados::IoCtx *p_ioctx, work_shard_t shard) {
      return update_shard_token_heartbeat(p_ioctx, shard, WORKER_SHARD_PREFIX);
    }

    //---------------------------------------------------------------------------
    int update_md5_shard_token_heartbeat(librados::IoCtx *p_ioctx, md5_shard_t shard) {
      return update_shard_token_heartbeat(p_ioctx, shard, MD5_SHARD_PREFIX);
    }

    //---------------------------------------------------------------------------
    bool all_work_shard_tokens_completed(librados::IoCtx *p_ioctx, uint32_t *ttl) {
      return all_shard_tokens_completed(p_ioctx, MAX_WORK_SHARD, WORKER_SHARD_PREFIX, ttl);
    }

    //---------------------------------------------------------------------------
    bool all_md5_shard_tokens_completed(librados::IoCtx *p_ioctx, uint32_t *ttl) {
      return all_shard_tokens_completed(p_ioctx, MAX_MD5_SHARD, MD5_SHARD_PREFIX, ttl);
    }

  private:
    void assign_cluster_id();
    bool all_shard_tokens_completed(librados::IoCtx *p_ioctx,
				    unsigned shards_count,
				    const char *prefix,
				    uint32_t *ttl);
    int leader_elect(rgw::sal::RadosStore *store);
    int cleanup_prev_run(librados::IoCtx *p_ioctx);
    int get_next_shard_token(librados::IoCtx *p_ioctx,
			     unsigned start_shard,
			     unsigned max_count,
			     const char *prefix);
    int create_shard_tokens(librados::IoCtx *p_ioctx,
			    unsigned shards_count,
			    const char *prefix);
    int update_shard_token_heartbeat(librados::IoCtx *p_ioctx,
				     unsigned shard,
				     const char *prefix);
    int mark_shard_token_completed(librados::IoCtx *p_ioctx,
				   unsigned shard,
				   const char *prefix,
				   const bufferlist &bl);

    const DoutPrefixProvider *dpp;
    std::string               cluster_id;
    bool                      is_leader = false;
    bool                      was_initialized = false;
    md5_shard_t               curr_md5_shard = 0;
    work_shard_t              curr_worker_shard = 0;
    utime_t                   epoch;
  };

} //namespace rgw::dedup
