#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup_store.h"
#include <string>

struct named_time_lock_t;
namespace rgw::dedup {
#define DEDUP_POOL_NAME       "rgw_dedup_pool"
#define MD5_SHARD_PREFIX      "MD5.SHRD.TKN."
#define WORKER_SHARD_PREFIX   "WRK.SHRD.TKN."
  int   init_dedup_pool_ioctx(RGWRados                 *rados,
			      const DoutPrefixProvider *dpp,
			      librados::IoCtx          *p_ioctx);
  class cluster{
#define TOKEN_STATE_PENDING   0x00
#define TOKEN_STATE_TIMED_OUT 0xDD
#define TOKEN_STATE_COMPLETED 0xFF

  public:
    cluster(const DoutPrefixProvider   *_dpp);

    int          init(rgw::sal::RadosStore *store, librados::IoCtx *p_ioctx);
    bool         was_initialized() { return d_was_initialized; }
    utime_t      get_epoch() { return d_epoch; }
    work_shard_t get_next_work_shard_token(librados::IoCtx *p_ioctx,
					   int *p_urgent_msg /* IN-OUT PARAM */);
    md5_shard_t  get_next_md5_shard_token(librados::IoCtx *p_ioctx,
					  int *p_urgent_msg /* IN-OUT PARAM */);
    static bool  can_start_new_scan(rgw::sal::RadosStore *store,
				    const utime_t &epoch,
				    const DoutPrefixProvider *dpp);
    static bool  got_start_scan_req(librados::IoCtx *p_ioctx);
    static int   collect_all_shard_stats(rgw::sal::RadosStore *store, const DoutPrefixProvider *dpp);
    static int   dedup_control(rgw::sal::RadosStore *store, const DoutPrefixProvider *dpp, int urgent_msg);
    static int   dedup_restart_scan(rgw::sal::RadosStore *store, const DoutPrefixProvider *dpp);
    //---------------------------------------------------------------------------
    int mark_work_shard_token_completed(librados::IoCtx *p_ioctx,
					work_shard_t work_shard,
					const worker_stats_t *p_stats)
    {
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      d_num_completed_workers++;
      d_completed_workers[work_shard] = TOKEN_STATE_COMPLETED;
      d_total_ingressed_obj += p_stats->ingress_obj;

      return mark_shard_token_completed(p_ioctx, work_shard, p_stats->ingress_obj,
					WORKER_SHARD_PREFIX, bl);
    }

    //---------------------------------------------------------------------------
    int mark_md5_shard_token_completed(librados::IoCtx *p_ioctx,
				       md5_shard_t md5_shard,
				       const md5_stats_t *p_stats)
    {
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      d_num_completed_md5++;
      d_completed_md5[md5_shard] = TOKEN_STATE_COMPLETED;
      return mark_shard_token_completed(p_ioctx, md5_shard, p_stats->loaded_objects,
					MD5_SHARD_PREFIX, bl);
    }

    int update_shard_token_heartbeat(librados::IoCtx *p_ioctx,
				     unsigned shard,
				     uint64_t count_a,
				     uint64_t count_b,
				     const char *prefix,
				     int *p_urgent_msg /* OUT-PARAM */);

    //---------------------------------------------------------------------------
    int update_work_shard_token_heartbeat(librados::IoCtx *p_ioctx,
					  work_shard_t shard,
					  uint64_t obj_count,
					  int *p_urgent_msg /* OUT-PARAM */)
    {
      return update_shard_token_heartbeat(p_ioctx, shard, obj_count, 0,
					  WORKER_SHARD_PREFIX, p_urgent_msg);
    }

    //---------------------------------------------------------------------------
    int update_md5_shard_token_heartbeat(librados::IoCtx *p_ioctx,
					 md5_shard_t shard,
					 uint64_t load_count,
					 uint64_t dedup_count,
					 int *p_urgent_msg /* OUT-PARAM */)
    {
      return update_shard_token_heartbeat(p_ioctx, shard, load_count, dedup_count,
					  MD5_SHARD_PREFIX, p_urgent_msg);
    }

    //---------------------------------------------------------------------------
    bool all_work_shard_tokens_completed(librados::IoCtx *p_ioctx, uint32_t *ttl,
					 uint64_t *p_total_ingressed)
    {
      return all_shard_tokens_completed(p_ioctx,
					MAX_WORK_SHARD,
					WORKER_SHARD_PREFIX,
					&d_num_completed_workers,
					d_completed_workers,
					ttl,
					p_total_ingressed);
    }
#if 1
    // TBD: d_total_ingressed_obj must be passed in as it is modified by worker code!
    //---------------------------------------------------------------------------
    bool all_md5_shard_tokens_completed(librados::IoCtx *p_ioctx, uint32_t *ttl,
					uint64_t *p_total_ingressed)
    {
      return all_shard_tokens_completed(p_ioctx,
					MAX_MD5_SHARD,
					MD5_SHARD_PREFIX,
					&d_num_completed_md5,
					d_completed_md5,
					ttl,
					p_total_ingressed);
    }
#endif

    int  get_urgent_msg_state(librados::IoCtx *p_ioctx,
			      int *urgent_msg /* OUT-PARAM */);

  private:
    static int get_epoch(rgw::sal::RadosStore *store,
			 const DoutPrefixProvider *dpp,
			 utime_t *p_epoch, /* OUT */
			 const char* caller);
    static int collect_shard_stats(librados::IoCtx *p_ioctx,
				   const DoutPrefixProvider *dpp,
				   unsigned shards_count,
				   const char *prefix,
				   bufferlist bl_arr[],
				   named_time_lock_t *ntl_arr);
    void reset();
    void assign_cluster_id();
    bool all_shard_tokens_completed(librados::IoCtx *p_ioctx,
				    unsigned shards_count,
				    const char *prefix,
				    uint16_t *p_num_completed,
				    uint8_t completed_arr[],
				    uint32_t *ttl,
				    uint64_t *p_total_ingressed);
    int set_epoch(rgw::sal::RadosStore *store);
    int cleanup_prev_run(librados::IoCtx *p_ioctx);
    int get_next_shard_token(librados::IoCtx *p_ioctx,
			     unsigned start_shard,
			     unsigned max_count,
			     const char *prefix,
			     int *p_urgent_msg /* IN-OUT PARAM */);
    int create_shard_tokens(librados::IoCtx *p_ioctx,
			    unsigned shards_count,
			    const char *prefix);
    int mark_shard_token_completed(librados::IoCtx *p_ioctx,
				   unsigned shard,
				   uint64_t obj_count,
				   const char *prefix,
				   const bufferlist &bl);

    const DoutPrefixProvider *dpp;
    std::string               d_cluster_id;
    bool                      d_was_initialized = false;
    md5_shard_t               d_curr_md5_shard = 0;
    work_shard_t              d_curr_worker_shard = 0;
    utime_t                   d_epoch;
    uint64_t                  d_total_ingressed_obj = 0;
    uint8_t                   d_completed_workers[MAX_WORK_SHARD];
    uint8_t                   d_completed_md5[MAX_WORK_SHARD];
    uint16_t                  d_num_completed_workers = 0;
    uint16_t                  d_num_completed_md5 = 0;
    uint16_t                  d_num_failed_workers = 0;
  };

} //namespace rgw::dedup
