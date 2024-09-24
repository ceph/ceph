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
    void run();
    int  setup();
    void handle_pause_req();
    bool should_stop() {
      return (unlikely(d_shutdown_req || d_remote_abort_req));
    }
    enum dedup_step_t {
      STEP_NONE,
      STEP_BUILD_TABLE,
      STEP_REMOVE_DUPLICATES
    };
    int  read_buckets();
    bool need_to_update_heartbeat();
    int  check_and_update_heartbeat(unsigned shard_id, uint64_t count_a, uint64_t count_b,
				    const char *prefix);

    inline int  check_and_update_worker_heartbeat(work_shard_t worker_id, int64_t obj_count);
    inline int  check_and_update_md5_heartbeat(md5_shard_t md5_id,
					       uint64_t load_count,
					       uint64_t dedup_count);
    int  ingress_single_object(rgw::sal::Bucket           *bucket,
			       const rgw_bucket_dir_entry &entry,
			       work_shard_t                worker_id,
			       worker_stats_t             *p_worker_stats /*IN-OUT*/);
    int  process_bucket_shards(rgw::sal::Bucket     *bucket,
			       std::map<int, string> &oids,
			       librados::IoCtx       &ioctx,
			       work_shard_t           shard_id,
			       worker_stats_t        *p_worker_stats /*IN-OUT*/);
    int  ingress_bucket_objects_single_shard(const string   &bucket_name,
					     work_shard_t    worker_id,
					     worker_stats_t *p_worker_stats /*IN-OUT*/);
    int  objects_ingress_single_shard(work_shard_t worker_id,
				      worker_stats_t *p_worker_stats);
    int  f_ingress(unsigned shard_id);
    int  f_dedup(unsigned shard_id);
    int  process_all_shards(bool ingress_work_shards, int (Background::* func)(unsigned));
    int  read_bucket_stats(rgw::sal::Bucket *bucket, const std::string &bucket_name);
    int  run_dedup_step(dedup_step_t step,
			md5_shard_t md5_shard,
			work_shard_t work_shard,
			md5_stats_t *p_stats /* IN-OUT */);

    int objects_dedup_single_shard(md5_shard_t md5_shard, md5_stats_t *p_stats);
    int add_disk_record(const rgw::sal::Bucket *p_bucket,
			const rgw::sal::Object *p_obj,
			uint64_t                obj_size);

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

    // allow to start/pasue/resume/stop execution
    bool d_shutdown_req     = false;
    bool d_remote_abort_req = false;
    bool d_started          = false;
    bool d_local_paused     = false;
    bool d_local_pause_req  = false;
    bool d_remote_paused    = false;
    bool d_remote_pause_req = false;
    int  d_execute_interval;

    std::thread d_runner;
    std::mutex  d_cond_mutex;
    std::mutex  d_pause_mutex;
    std::condition_variable d_cond;
  };

} //namespace rgw::dedup
