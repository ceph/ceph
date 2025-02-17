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
  struct dedup_epoch_t;
  struct control_t {
    control_t() {
      reset();
    }
    void reset();
    inline bool local_urgent_req() const {
      return (shutdown_req || local_pause_req);
    }
    inline bool should_stop() const {
      return (shutdown_req || remote_abort_req);
    }
    inline bool should_pause() const {
      return (local_pause_req || remote_pause_req);
    }
    void show_dedup_type(const DoutPrefixProvider* const dpp, const char* caller);

    // TBD: pack state data members in a struct
    // allow to start/pasue/resume/stop execution
    int  dedup_type         = 0;
    bool started            = false;
    bool dedup_exec         = false;
    bool shutdown_req       = false;
    bool shutdown_done      = false;
    bool local_pause_req    = false;
    bool local_paused       = false;
    bool remote_abort_req   = false;
    bool remote_aborted     = false;
    bool remote_pause_req   = false;
    bool remote_paused      = false;
    bool remote_restart_req = false;
  };
  std::ostream& operator<<(std::ostream &out, const control_t &ctl);
  void encode(const control_t& ctl, ceph::bufferlist& bl);
  void decode(control_t& ctl, ceph::bufferlist::const_iterator& bl);

  class disk_block_seq_t;
  struct disk_record_t;
  struct key_t;
  //Interval between each execution of the script is set to 5 seconds
  static inline constexpr int INIT_EXECUTE_INTERVAL = 5;
  class Background : public RGWRealmReloader::Pauser {
    class DedupWatcher : public librados::WatchCtx2, public DoutPrefixProvider {
      Background* const parent;
    public:
      DedupWatcher(Background* _parent) : parent(_parent) {}
      ~DedupWatcher() override = default;
      void handle_notify(uint64_t notify_id, uint64_t cookie,
			 uint64_t notifier_id, bufferlist& bl) override;
      void handle_error(uint64_t cookie, int err) override;

      // DoutPrefixProvider iterface
      CephContext* get_cct() const override;
      unsigned get_subsys() const override;
      std::ostream& gen_prefix(std::ostream& out) const override;
    };

  public:
    Background(rgw::sal::Driver* _driver,
	       CephContext* _cct,
	       int _execute_interval = INIT_EXECUTE_INTERVAL);

    ~Background();
    int  watch_reload(const DoutPrefixProvider* dpp);
    int  unwatch_reload(const DoutPrefixProvider* dpp);
    void handle_notify(uint64_t notify_id, uint64_t cookie, bufferlist &bl);
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

    void ack_notify(uint64_t notify_id, uint64_t cookie, int status);
    void run();
    int  setup(struct dedup_epoch_t*);
    void work_shards_barrier(work_shard_t num_work_shards);
    void handle_pause_req(const char* caller);
    const char* dedup_step_name(dedup_step_t step);
    int  read_buckets();
    void check_and_update_heartbeat(unsigned shard_id, uint64_t count_a, uint64_t count_b,
				    const char *prefix);

    inline void check_and_update_worker_heartbeat(work_shard_t worker_id, int64_t obj_count);
    inline void check_and_update_md5_heartbeat(md5_shard_t md5_id,
					       uint64_t load_count,
					       uint64_t dedup_count);
    int  ingress_bucket_idx_single_object(disk_block_array_t         &disk_arr,
					  const rgw::sal::Bucket     *bucket,
					  const rgw_bucket_dir_entry &entry,
					  worker_stats_t             *p_worker_stats /*IN-OUT*/);
    int  process_bucket_shards(disk_block_array_t     &disk_arr,
			       const rgw::sal::Bucket *bucket,
			       std::map<int, string>  &oids,
			       librados::IoCtx        &ioctx,
			       work_shard_t            shard_id,
			       work_shard_t            num_work_shards,
			       worker_stats_t         *p_worker_stats /*IN-OUT*/);
    int  ingress_bucket_objects_single_shard(disk_block_array_t &disk_arr,
					     const rgw_bucket   &bucket_rec,
					     work_shard_t        worker_id,
					     work_shard_t        num_work_shards,
					     worker_stats_t     *p_worker_stats /*IN-OUT*/);
    int  objects_ingress_single_work_shard(work_shard_t worker_id,
					   work_shard_t num_work_shards,
					   md5_shard_t num_md5_shards,
					   worker_stats_t *p_worker_stats,
					   uint8_t *raw_mem,
					   uint64_t raw_mem_size);
    int  f_ingress_work_shard(unsigned shard_id,
			      uint8_t *raw_mem,
			      uint64_t raw_mem_size,
			      work_shard_t num_work_shards,
			      md5_shard_t num_md5_shards);
    int  f_dedup_md5_shard(unsigned shard_id,
			   uint8_t *raw_mem,
			   uint64_t raw_mem_size,
			   work_shard_t num_work_shards,
			   md5_shard_t num_md5_shards);
    int  process_all_shards(bool ingress_work_shards,
			    int (Background::* func)(unsigned, uint8_t*, uint64_t, work_shard_t, md5_shard_t),
			    uint8_t *raw_mem,
			    uint64_t raw_mem_size,
			    work_shard_t num_work_shards,
			    md5_shard_t num_md5_shards);
    int  read_bucket_stats(const rgw_bucket &bucket_rec,
			   uint64_t     *p_num_obj,
			   uint64_t     *p_size);
    int  collect_all_buckets_stats();
    int  run_dedup_step(dedup_table_t *p_table,
			dedup_step_t step,
			md5_shard_t md5_shard,
			work_shard_t work_shard,
			uint32_t *p_seq_count,
			md5_stats_t *p_stats /* IN-OUT */,
			disk_block_seq_t *p_disk_block_arr);

    int calc_object_sha256(const disk_record_t *p_rec, unsigned char *p_sha256);
    void reduce_md5_collision_chances(dedup_table_t *p_table,
				      uint64_t obj_count_in_shard);
    int objects_dedup_single_md5_shard(dedup_table_t *p_table,
				       md5_shard_t md5_shard,
				       md5_stats_t *p_stats,
				       work_shard_t num_work_shards);
    int add_disk_rec_from_bucket_idx(disk_block_array_t     &disk_arr,
				     const rgw::sal::Bucket *p_bucket,
				     const parsed_etag_t    *p_parsed_etag,
				     const string           &obj_name,
				     uint64_t                obj_size);

    int read_object_attribute(dedup_table_t       *p_table,
			      const disk_record_t *p_rec,
			      disk_block_id_t      block_id,
			      record_id_t          rec_id,
			      md5_shard_t          md5_shard,
			      md5_stats_t         *p_stats /* IN-OUT */,
			      disk_block_seq_t  *p_disk);
    int try_deduping_record(dedup_table_t       *p_table,
			    const disk_record_t *p_rec,
			    disk_block_id_t      block_id,
			    record_id_t          rec_id,
			    md5_shard_t          md5_shard,
			    md5_stats_t         *p_stats /* IN-OUT */);
    int add_record_to_dedup_table(dedup_table_t *p_table,
				  const struct disk_record_t *p_rec,
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

    int  remove_slabs(unsigned worker_id, unsigned md5_shard, uint32_t slab_count);
    void init_rados_access_handles();

    // private data members
    rgw::sal::Driver* driver = nullptr;
    rgw::sal::RadosStore* store = nullptr;
    RGWRados* rados = nullptr;
    librados::Rados* rados_handle = nullptr;
    const DoutPrefix dp;
    const DoutPrefixProvider* const dpp;
    CephContext* const cct;
    cluster d_cluster;
    librados::IoCtx d_dedup_cluster_ioctx;
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
    int  d_execute_interval;
    control_t d_ctl;
    uint64_t d_watch_handle = 0;
    DedupWatcher d_watcher_ctx;

    std::thread d_runner;
    std::mutex  d_cond_mutex;
    std::mutex  d_pause_mutex;
    std::condition_variable d_cond;
  };

} //namespace rgw::dedup
