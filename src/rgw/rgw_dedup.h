#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_dedup_table.h"
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
  const work_shard_t MAX_WORK_SHARD = 2;
  const md5_shard_t  MAX_MD5_SHARD  = 4;
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
      STEP_BUILD_TABLE,
      STEP_REMOVE_DUPLICATES
    };
    void run();
    int  setup();
    int  scan_bucket_list(bool deep_scan, ceph::real_time last_scan_time);
    int  read_buckets();
    int  process_single_entry(rgw::sal::Bucket           *bucket,
			      const rgw_bucket_dir_entry &entry,
			      work_shard_t                worker_id,
			      const ceph::real_time      &last_scan_time);
    int  process_bucket_shards(rgw::sal::Bucket     *bucket,
			       std::map<int, string> &oids,
			       librados::IoCtx       &ioctx,
			       work_shard_t           shard_id,
			       const ceph::real_time &last_scan_time,
			       uint64_t              *p_obj_count /* IN-OUT */);
    int  list_bucket_by_shard(const string   &bucket_name,
			      ceph::real_time last_scan_time,
			      work_shard_t    worker_id,
			      uint64_t       *p_obj_count /* IN-OUT */);
    int  read_bucket_stats(rgw::sal::Bucket *bucket, const std::string &bucket_name);
    int run_dedup_step(dedup_step_t step,
		       md5_shard_t md5_shard,
		       work_shard_t work_shard,
		       uint32_t *p_rec_count);

    int add_disk_record(const rgw::sal::Bucket *p_bucket,
			const rgw::sal::Object *p_obj,
			work_shard_t            worker_id,
			uint64_t                obj_size);

    //void calc_object_key(uint64_t object_size, bufferlist &etag_bl, struct Key *p_key);
    int try_deduping_record(const disk_record_t *p_rec,
			    disk_block_id_t      block_id,
			    record_id_t          rec_id,
			    md5_shard_t          md5_shard);
    int add_record_to_dedup_table(const struct disk_record_t *p_rec,
				  disk_block_id_t block_id,
				  record_id_t rec_id);
    int inc_ref_count_by_manifest(const string   &ref_tag, RGWObjManifest &manifest);
    int rollback_ref_by_manifest(const string &ref_tag, RGWObjManifest &manifest);
    int free_tail_objs_by_manifest(const string &ref_tag, RGWObjManifest &tgt_manifest);
#if 0
    int dedup_object(const string     &src_bucket_name,
		     const string     &src_obj_name,
		     rgw::sal::Bucket *tgt_bucket,
		     rgw::sal::Object *tgt_obj,
		     uint64_t          tgt_size,
		     bufferlist       &tgt_etag_bl);
#endif
    int dedup_object(const disk_record_t *p_src_rec,
		     const disk_record_t *p_tgt_rec,
		     const struct key_t  *p_key);

    void init_rados_access_handles();

    rgw::sal::Driver* driver = nullptr;
    rgw::sal::RadosStore* store = nullptr;
    RGWRados* rados = nullptr;
    librados::Rados* rados_handle = nullptr;
    bool stopped = false;
    bool started = false;
    bool paused = false;
    bool pause_req = false;
    int execute_interval;
    const DoutPrefix dp;
    const DoutPrefixProvider* const dpp;
    CephContext* const cct;
    dedup_table_t d_table;
    disk_block_array_t *p_disk_arr[MAX_MD5_SHARD][MAX_WORK_SHARD];
    std::thread runner;
    //mutable std::mutex table_mutex;
    std::mutex cond_mutex;
    std::mutex pause_mutex;
    std::condition_variable cond;

    struct bucket_state_t {
      uint64_t num_objects = 0;
      uint64_t size = 0;
      utime_t  modification_time;
    };
    std::unordered_map<std::string, bucket_state_t> bucket_set;
  };

} //namespace rgw::dedup
