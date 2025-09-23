// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include "common/dout.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup_store.h"
#include <string>

namespace rgw::dedup {
  static constexpr const char* WORKER_SHARD_PREFIX = "WRK.SHRD.TK.";
  static constexpr const char* MD5_SHARD_PREFIX    = "MD5.SHRD.TK.";
  struct control_t;
  struct dedup_epoch_t;

  class cluster{
  public:
    //==================================================================================
    class shard_token_oid {
    public:
      //---------------------------------------------------------------------------
      shard_token_oid(const char *prefix) {
        this->prefix_len = snprintf(this->buff, BUFF_SIZE, "%s", prefix);
        this->total_len = this->prefix_len;
      }

      //---------------------------------------------------------------------------
      shard_token_oid(const char *prefix, uint16_t shard) {
        this->prefix_len = snprintf(this->buff, BUFF_SIZE, "%s", prefix);
        set_shard(shard);
      }

      //---------------------------------------------------------------------------
      void set_shard(uint16_t shard) {
        int n = snprintf(this->buff + this->prefix_len, BUFF_SIZE, "%03x", shard);
        this->total_len = this->prefix_len + n;
      }

      //---------------------------------------------------------------------------
      static bool legal_oid_name(const std::string& oid) {
        return ((oid.length() <= BUFF_SIZE) &&
                (oid.starts_with(WORKER_SHARD_PREFIX)||oid.starts_with(MD5_SHARD_PREFIX)));
      }
      inline const char* get_buff() { return this->buff; }
      inline unsigned get_buff_size() { return this->total_len; }
    private:
      static const unsigned BUFF_SIZE = 15;
      unsigned total_len  = 0;
      unsigned prefix_len = 0;
      char buff[BUFF_SIZE];
    };

    //==================================================================================
    cluster(const DoutPrefixProvider *_dpp,
            CephContext* cct,
            rgw::sal::Driver* driver);
    int          reset(rgw::sal::RadosStore *store,
                       struct dedup_epoch_t*,
                       work_shard_t num_work_shards,
                       md5_shard_t num_md5_shards);

    utime_t      get_epoch_time() { return d_epoch_time; }
    work_shard_t get_next_work_shard_token(rgw::sal::RadosStore *store,
                                           work_shard_t num_work_shards);
    md5_shard_t  get_next_md5_shard_token(rgw::sal::RadosStore *store,
                                          md5_shard_t num_md5_shards);
    bool         can_start_new_scan(rgw::sal::RadosStore *store);
    static int   collect_all_shard_stats(rgw::sal::RadosStore *store,
                                         Formatter *p_formatter,
                                         const DoutPrefixProvider *dpp);
    static int   watch_reload(rgw::sal::RadosStore *store,
                              const DoutPrefixProvider* dpp,
                              uint64_t *p_watch_handle,
                              librados::WatchCtx2 *ctx);
    static int   unwatch_reload(rgw::sal::RadosStore *store,
                                const DoutPrefixProvider* dpp,
                                uint64_t watch_handle);
    static int   ack_notify(rgw::sal::RadosStore *store,
                            const DoutPrefixProvider *dpp,
                            const struct control_t *p_ctl,
                            uint64_t notify_id,
                            uint64_t cookie,
                            int status);
    static int   dedup_control(rgw::sal::RadosStore *store,
                               const DoutPrefixProvider *dpp,
                               urgent_msg_t urgent_msg);
    static int   dedup_restart_scan(rgw::sal::RadosStore *store,
                                    dedup_req_type_t dedup_type,
                                    const DoutPrefixProvider *dpp);

    //---------------------------------------------------------------------------
    int mark_work_shard_token_completed(rgw::sal::RadosStore *store,
                                        work_shard_t work_shard,
                                        const worker_stats_t *p_stats)
    {
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      d_num_completed_workers++;
      d_completed_workers[work_shard] = TOKEN_STATE_COMPLETED;

      return mark_shard_token_completed(store, work_shard, p_stats->ingress_obj,
                                        WORKER_SHARD_PREFIX, bl);
    }

    //---------------------------------------------------------------------------
    int mark_md5_shard_token_completed(rgw::sal::RadosStore *store,
                                       md5_shard_t md5_shard,
                                       const md5_stats_t *p_stats)
    {
      ceph::bufferlist bl;
      encode(*p_stats, bl);
      d_num_completed_md5++;
      d_completed_md5[md5_shard] = TOKEN_STATE_COMPLETED;
      return mark_shard_token_completed(store, md5_shard, p_stats->loaded_objects,
                                        MD5_SHARD_PREFIX, bl);
    }

    int update_shard_token_heartbeat(rgw::sal::RadosStore *store,
                                     unsigned shard,
                                     uint64_t count_a,
                                     uint64_t count_b,
                                     const char *prefix);

    //---------------------------------------------------------------------------
    int all_work_shard_tokens_completed(rgw::sal::RadosStore *store,
                                        work_shard_t num_work_shards)
    {
      return all_shard_tokens_completed(store, num_work_shards, WORKER_SHARD_PREFIX,
                                        &d_num_completed_workers, d_completed_workers);
    }

    //---------------------------------------------------------------------------
    int all_md5_shard_tokens_completed(rgw::sal::RadosStore *store,
                                       md5_shard_t num_md5_shards)
    {
      return all_shard_tokens_completed(store, num_md5_shards, MD5_SHARD_PREFIX,
                                        &d_num_completed_md5, d_completed_md5);
    }

  private:
    static constexpr unsigned TOKEN_STATE_PENDING   = 0x00;
    static constexpr unsigned TOKEN_STATE_CORRUPTED = 0xCC;
    static constexpr unsigned TOKEN_STATE_TIMED_OUT = 0xDD;
    static constexpr unsigned TOKEN_STATE_COMPLETED = 0xFF;

    void clear();
    int  all_shard_tokens_completed(rgw::sal::RadosStore *store,
                                    unsigned shards_count,
                                    const char *prefix,
                                    uint16_t *p_num_completed,
                                    uint8_t completed_arr[]);
    int cleanup_prev_run(rgw::sal::RadosStore *store);
    int32_t get_next_shard_token(rgw::sal::RadosStore *store,
                                 uint16_t start_shard,
                                 uint16_t max_count,
                                 const char *prefix);
    int create_shard_tokens(rgw::sal::RadosStore *store,
                            unsigned shards_count,
                            const char *prefix);
    int verify_all_shard_tokens(rgw::sal::RadosStore *store,
                                unsigned shards_count,
                                const char *prefix);
    int mark_shard_token_completed(rgw::sal::RadosStore *store,
                                   unsigned shard,
                                   uint64_t obj_count,
                                   const char *prefix,
                                   const bufferlist &bl);

    const DoutPrefixProvider *dpp;
    std::string               d_lock_cookie;
    std::string               d_cluster_id;
    md5_shard_t               d_curr_md5_shard = 0;
    work_shard_t              d_curr_worker_shard = 0;
    utime_t                   d_epoch_time;
    utime_t                   d_token_creation_time;
    uint8_t                   d_completed_workers[MAX_WORK_SHARD];
    uint8_t                   d_completed_md5[MAX_MD5_SHARD];
    uint16_t                  d_num_completed_workers = 0;
    uint16_t                  d_num_completed_md5 = 0;
  };

} //namespace rgw::dedup
