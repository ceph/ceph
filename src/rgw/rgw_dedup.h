#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup_table.h"
#include "rgw_dedup_cluster.h"
#include "rgw_realm_reloader.h"
#include <string>
#include <unordered_map>
#include <variant>
#include <iostream>
#include <ostream>
using namespace std;
namespace cls::cmpxattr {
  struct dedup_epoch_t;
}
namespace rgw::dedup {
  class disk_block_array_t;
  struct disk_record_t;
  struct key_t;
  //Interval between each execution of the script is set to 5 seconds
  static inline constexpr int INIT_EXECUTE_INTERVAL = 5;
  class Background : public RGWRealmReloader::Pauser {
  public:
    Background(rgw::sal::Driver* _driver,
	       CephContext* _cct,
	       int _execute_interval = INIT_EXECUTE_INTERVAL);

    ~Background();
    void start();
    void shutdown();
    void pause() override;
    void resume(rgw::sal::Driver* _driver) override;

  private:
    enum dedup_step_t {
      STEP_NONE,
      STEP_BUCKET_INDEX_INGRESS,
      STEP_BUILD_TABLE,
      STEP_READ_ATTRIBUTES,
      STEP_REMOVE_DUPLICATES
    };

    void run();
    int  setup(::cls::cmpxattr::dedup_epoch_t*);
    bool all_shards_completed(dedup_step_t step, uint32_t *ttl,
			      uint64_t *p_total_ingressed);
    void all_shards_barrier(dedup_step_t step, const char *stepname);
    void handle_pause_req(const char* caller);
    bool should_stop() {
      return (unlikely(d_shutdown_req || d_remote_abort_req));
    }
    const char* dedup_step_name(dedup_step_t step);
    int  read_buckets();
    bool need_to_update_heartbeat();
    int  check_and_update_heartbeat(unsigned shard_id, uint64_t count_a, uint64_t count_b,
				    const char *prefix);

    inline int  check_and_update_worker_heartbeat(work_shard_t worker_id, int64_t obj_count);
    inline int  check_and_update_md5_heartbeat(md5_shard_t md5_id,
					       uint64_t load_count,
					       uint64_t dedup_count);
    int  ingress_bucket_idx_single_object(rgw::sal::Bucket           *bucket,
					  const rgw_bucket_dir_entry &entry,
					  worker_stats_t             *p_worker_stats /*IN-OUT*/);
    int  process_bucket_shards(rgw::sal::Bucket     *bucket,
			       std::map<int, string> &oids,
			       librados::IoCtx       &ioctx,
			       work_shard_t           shard_id,
			       worker_stats_t        *p_worker_stats /*IN-OUT*/);
    int  ingress_bucket_objects_single_shard(const rgw_bucket &bucket_rec,
					     work_shard_t      worker_id,
					     worker_stats_t   *p_worker_stats /*IN-OUT*/);
    int  objects_ingress_single_work_shard(work_shard_t worker_id,
					   worker_stats_t *p_worker_stats);
    int  f_ingress_work_shard(unsigned shard_id);
    int  f_dedup_md5_shard(unsigned shard_id);
    int  process_all_shards(bool ingress_work_shards, int (Background::* func)(unsigned));
    int  read_bucket_stats(const rgw_bucket &bucket_rec,
			   uint64_t     *p_num_obj,
			   uint64_t     *p_size);
    int  collect_all_buckets_stats();
    int  run_dedup_step(dedup_step_t step,
			md5_shard_t md5_shard,
			work_shard_t work_shard,
			uint32_t seq_count_arr[],
			md5_stats_t *p_stats /* IN-OUT */,
			disk_block_array_t *p_disk_block_arr);

    int calc_object_sha256(const disk_record_t *p_rec, unsigned char *p_sha256);
    void reduce_md5_collision_chances(uint64_t obj_count_in_shard);
    int objects_dedup_single_md5_shard(md5_shard_t md5_shard,
				       md5_stats_t *p_stats);
    int add_disk_rec_from_bucket_idx(const rgw::sal::Bucket *p_bucket,
				     const parsed_etag_t    *p_parsed_etag,
				     const string           &obj_name,
				     uint64_t                obj_size);

    int read_object_attribute(const disk_record_t *p_rec,
			      disk_block_id_t      block_id,
			      record_id_t          rec_id,
			      md5_shard_t          md5_shard,
			      md5_stats_t         *p_stats /* IN-OUT */,
			      disk_block_array_t  *p_disk);
    int try_deduping_record(const disk_record_t *p_rec,
			    disk_block_id_t      block_id,
			    record_id_t          rec_id,
			    md5_shard_t          md5_shard,
			    md5_stats_t         *p_stats /* IN-OUT */);
    int add_record_to_dedup_table(const struct disk_record_t *p_rec,
				  disk_block_id_t block_id,
				  record_id_t rec_id);
    int inc_ref_count_by_manifest(const string   &ref_tag,
				  const string   &oid,
				  RGWObjManifest &manifest);
    int rollback_ref_by_manifest(const string   &ref_tag,
				 const string   &oid,
				 RGWObjManifest &tgt_manifest);
    int free_tail_objs_by_manifest(const string   &ref_tag,
				   const string   &oid,
				   RGWObjManifest &tgt_manifest);
    int dedup_object(const disk_record_t *p_src_rec,
		     const disk_record_t *p_tgt_rec,
		     bool                 is_shared_manifest_src,
		     bool                 src_has_sha256);

    int  remove_slabs(unsigned worker_id, unsigned md5_shard, uint32_t slab_count_arr[]);
    void init_rados_access_handles();

    // private data members
    rgw::sal::Driver* driver = nullptr;
    rgw::sal::RadosStore* store = nullptr;
    RGWRados* rados = nullptr;
    librados::Rados* rados_handle = nullptr;
    const DoutPrefix dp;
    const DoutPrefixProvider* const dpp;
    CephContext* const cct;
    dedup_table_t d_table;
    cluster d_cluster;
    disk_block_array_t *p_disk_arr[MAX_MD5_SHARD];
    librados::IoCtx    *p_dedup_cluster_ioctx = nullptr;
    utime_t  d_heart_beat_last_update;
    unsigned d_heart_beat_max_elapsed_sec;

    // A pool with 6 billion objects has a  1/(2^64) chance for collison with a 128bit MD5
    uint64_t d_max_protected_objects   = (6ULL * 1024 * 1024 * 1024);
    uint64_t d_all_buckets_obj_count   = 0;
    uint64_t d_all_buckets_obj_size    = 0;
    uint64_t d_num_rados_objects       = 0;
    uint64_t d_num_rados_objects_bytes = 0;
    // we don't benefit from deduping RGW objects smaller than head-object size
    uint32_t d_min_obj_size_for_dedup = (4ULL * 1024 * 1024);
    // allow to start/pasue/resume/stop execution
    bool d_shutdown_req     = false;
    bool d_remote_abort_req = false;
    bool d_started          = false;
    bool d_local_paused     = false;
    bool d_local_pause_req  = false;
    bool d_remote_paused    = false;
    bool d_remote_pause_req = false;
    int  d_dedup_type       = 0; //DEDUP_TYPE_NONE;
    int  d_execute_interval;

    std::thread d_runner;
    std::mutex  d_cond_mutex;
    std::mutex  d_pause_mutex;
    std::condition_variable d_cond;
  };

} //namespace rgw::dedup
