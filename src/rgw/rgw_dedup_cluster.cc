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

#include "rgw_dedup_cluster.h"
#include "rgw_dedup.h"
#include "rgw_dedup_epoch.h"
#include "rgw_common.h"
#include "rgw_dedup_store.h"
#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "svc_zone.h"
#include "common/config.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "include/denc.h"
#include "rgw_sal.h"
#include "driver/rados/rgw_sal_rados.h"
#include <cstdlib>
#include <ctime>
#include <string>

namespace rgw::dedup {
  const char* DEDUP_EPOCH_TOKEN = "EPOCH_TOKEN";

  static constexpr unsigned EPOCH_MAX_LOCK_DURATION_SEC = 30;
  struct shard_progress_t;
  static int collect_shard_stats(librados::IoCtx &ioctx,
                                 const DoutPrefixProvider *dpp,
                                 utime_t epoch_time,
                                 unsigned shards_count,
                                 const char *prefix,
                                 bufferlist bl_arr[],
                                 struct shard_progress_t *sp_arr);

  const uint64_t SP_ALL_OBJECTS = ULLONG_MAX;
  const uint64_t SP_NO_OBJECTS  = 0ULL;
  const char* SHARD_PROGRESS_ATTR = "shard_progress";

  //---------------------------------------------------------------------------
  static int get_epoch(librados::IoCtx &ioctx,
                       const DoutPrefixProvider *dpp,
                       dedup_epoch_t *p_epoch, /* OUT */
                       const char *caller)
  {
    std::string oid(DEDUP_EPOCH_TOKEN);
    bufferlist bl;
    int ret = ioctx.getxattr(oid, RGW_DEDUP_ATTR_EPOCH, bl);
    if (ret > 0) {
      try {
        auto p = bl.cbegin();
        decode(*p_epoch, p);
      }catch (const buffer::error&) {
        ldpp_dout(dpp, 0) << __func__ << "::failed epoch decode!" << dendl;
        return -EINVAL;
      }
      if (caller) {
        ldpp_dout(dpp, 10) << __func__ << "::"<< caller<< "::" << *p_epoch << dendl;
      }
      return 0;
    }
    else {
      // zero length read means no data
      if (ret == 0) {
        ret = -ENODATA;
      }
      ldpp_dout(dpp, 10) << __func__ << "::" << (caller ? caller : "")
                         << "::failed ioctx.getxattr() with: "
                         << cpp_strerror(-ret) << ", ret=" << ret << dendl;
      return ret;
    }
  }

  //---------------------------------------------------------------------------
  static int set_epoch(librados::IoCtx &ioctx,
                       const std::string &cluster_id,
                       const DoutPrefixProvider *dpp,
                       work_shard_t num_work_shards,
                       md5_shard_t num_md5_shards)
  {
    std::string oid(DEDUP_EPOCH_TOKEN);
    ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << dendl;
    bool exclusive = true; // block overwrite of old objects
    int ret = ioctx.create(oid, exclusive);
    if (ret >= 0) {
      ldpp_dout(dpp, 10) << __func__ << "::successfully created Epoch object!" << dendl;
      // now try and take ownership
    }
    else if (ret == -EEXIST) {
      ldpp_dout(dpp, 10) << __func__ << "::Epoch object exists -> trying to take over" << dendl;
      // try and take ownership
    }
    else{
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: failed to create " << oid
                        <<" with: "<< cpp_strerror(-ret) << ", ret=" << ret <<dendl;
      return ret;
    }

    uint32_t serial = 0;
    dedup_req_type_t dedup_type = dedup_req_type_t::DEDUP_TYPE_ESTIMATE;
    dedup_epoch_t new_epoch = { serial, dedup_type, ceph_clock_now(),
                                num_work_shards, num_md5_shards };
    bufferlist new_epoch_bl, empty_bl;
    encode(new_epoch, new_epoch_bl);
    librados::ObjectWriteOperation op;
    op.cmpxattr(RGW_DEDUP_ATTR_EPOCH, CEPH_OSD_CMPXATTR_OP_EQ, empty_bl);
    op.setxattr(RGW_DEDUP_ATTR_EPOCH, new_epoch_bl);

    ldpp_dout(dpp, 10) << __func__ << "::send EPOCH CLS" << dendl;
    ret = ioctx.operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Epoch object was written" << dendl;
    }
    // TBD: must check for failure caused by an existing EPOCH xattr!
    // probably best to read attribute from epoch!
    else if (ret == -ECANCELED) {
      dedup_epoch_t epoch;
      ret = get_epoch(ioctx, dpp, &epoch, __func__);
      if (ret == 0) {
        ldpp_dout(dpp, 10) << __func__ << "::Accept existing Epoch object" << dendl;
      }
      return ret;
    }
    else {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed ioctx.operate("
                        << oid << "), err is " << cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  static int swap_epoch(const DoutPrefixProvider *dpp,
                        librados::IoCtx &ioctx,
                        const dedup_epoch_t *p_old_epoch,
                        dedup_req_type_t dedup_type,
                        work_shard_t num_work_shards,
                        md5_shard_t num_md5_shards)
  {
    dedup_epoch_t new_epoch = { p_old_epoch->serial + 1, dedup_type,
                                ceph_clock_now(), num_work_shards, num_md5_shards};
    bufferlist old_epoch_bl, new_epoch_bl, err_bl;
    encode(*p_old_epoch, old_epoch_bl);
    encode(new_epoch, new_epoch_bl);
    librados::ObjectWriteOperation op;
    op.cmpxattr(RGW_DEDUP_ATTR_EPOCH, CEPH_OSD_CMPXATTR_OP_EQ, old_epoch_bl);
    op.setxattr(RGW_DEDUP_ATTR_EPOCH, new_epoch_bl);

    ldpp_dout(dpp, 10) << __func__ << "::send EPOCH CLS" << dendl;
    std::string oid(DEDUP_EPOCH_TOKEN);
    int ret = ioctx.operate(oid, &op);
    if (ret != 0) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed ioctx.operate("
                        << oid << "), err is " << cpp_strerror(-ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  struct shard_progress_t {
    shard_progress_t() {
      // init an empty object
      this->progress_a = SP_NO_OBJECTS;
      this->progress_b = SP_NO_OBJECTS;
      this->completed  = false;

      // set all timers to now
      this->creation_time   = utime_t();
      this->completion_time = utime_t();
      this->update_time     = utime_t();

      // owner and stats_bl are empty until set
    }

    shard_progress_t(uint64_t _progress_a,
                     uint64_t _progress_b,
                     bool _completed,
                     const std::string &_owner,
                     const bufferlist  &_stats_bl) : owner(_owner), stats_bl(_stats_bl) {
      this->progress_a  = _progress_a;
      this->progress_b  = _progress_b;
      this->completed   = _completed;
      this->update_time = ceph_clock_now();

      if (_progress_a == SP_NO_OBJECTS && _progress_b == SP_NO_OBJECTS) {
        this->creation_time = ceph_clock_now();
      }
      if (_completed) {
        this->completion_time = ceph_clock_now();
      }
    }

    bool is_completed() const {
      if (this->progress_b == SP_ALL_OBJECTS) {
        ceph_assert(this->completed);
        return true;
      }
      else {
        ceph_assert(!this->completed);
        return false;
      }
    }
    uint64_t    progress_a;
    uint64_t    progress_b;
    bool        completed;
    utime_t     update_time;
    utime_t     creation_time;
    utime_t     completion_time;
    std::string owner;
    bufferlist  stats_bl;
  };

  //---------------------------------------------------------------------------
  void encode(const shard_progress_t& sp, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(sp.progress_a, bl);
    encode(sp.progress_b, bl);
    encode(sp.completed, bl);
    encode(sp.creation_time, bl);
    encode(sp.completion_time, bl);
    encode(sp.update_time, bl);
    encode(sp.owner, bl);
    encode(sp.stats_bl, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(shard_progress_t & sp, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(sp.progress_a, bl);
    decode(sp.progress_b, bl);
    decode(sp.completed, bl);
    decode(sp.creation_time, bl);
    decode(sp.completion_time, bl);
    decode(sp.update_time, bl);
    decode(sp.owner, bl);
    decode(sp.stats_bl, bl);
    DECODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  int init_dedup_pool_ioctx(RGWRados                 *rados,
                            const DoutPrefixProvider *dpp,
                            librados::IoCtx          &ioctx)
  {
    rgw_pool dedup_pool(DEDUP_POOL_NAME);
    std::string pool_name(DEDUP_POOL_NAME);
#if 0
    // using Replica-1 for the intermediate data
    // since it can be regenerated in case of a failure
    std::string replica_count(std::to_string(1));
#else
    // temporary solution until we find a way to disable the health warn on replica1
    std::string replica_count(std::to_string(2));
#endif
    librados::bufferlist inbl;
    std::string output;
    std::string command = R"(
    {
      "prefix": "osd pool create",
      "pool": ")" + pool_name +
      R"(",
      "pool_type": "replicated",
      "size": )" + replica_count +
      R"(
    })";

    auto rados_handle = rados->get_rados_handle();
    int ret = rados_handle->mon_command(command, inbl, nullptr, &output);
    if (output.length()) {
      if (output != "pool 'rgw_dedup_pool' already exists") {
        ldpp_dout(dpp, 10) << __func__ << "::" << output << dendl;
      }
    }
    if (ret != 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to create pool "
                        << DEDUP_POOL_NAME << " with: "
                        << cpp_strerror(-ret) << ", ret=" << ret << dendl;
      return ret;
    }

    ret = rgw_init_ioctx(dpp, rados_handle, dedup_pool, ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed to initialize pool for listing with: "
                        << cpp_strerror(-ret) << dendl;
    }

    ret = ioctx.application_enable("dedup", false);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::pool " << DEDUP_POOL_NAME
                         << " was associated with dedup app" << dendl;
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to enable pool "
                        << DEDUP_POOL_NAME << " with: "
                        << cpp_strerror(-ret) << ", ret=" << ret << dendl;
    }
    return ret;
  }

  //==========================================================================

  //---------------------------------------------------------------------------
  void cluster::clear()
  {
    d_curr_md5_shard = 0;
    d_curr_worker_shard = 0;

    d_num_completed_workers = 0;
    d_num_completed_md5 = 0;

    memset(d_completed_workers, TOKEN_STATE_PENDING, sizeof(d_completed_workers));
    memset(d_completed_md5, TOKEN_STATE_PENDING, sizeof(d_completed_md5));

    d_total_ingressed_obj = 0;
    d_num_failed_workers = 0;
  }


  static constexpr auto COOKIE_LEN = 15;
  static constexpr auto CLUSTER_ID_LEN = 15;
  //---------------------------------------------------------------------------
  cluster::cluster(const DoutPrefixProvider *_dpp,
                   CephContext *cct,
                   rgw::sal::Driver* driver):
    dpp(_dpp),
    d_lock_cookie(gen_rand_alphanumeric(cct, COOKIE_LEN)),
    d_cluster_id (gen_rand_alphanumeric(cct, CLUSTER_ID_LEN))
  {
    clear();

    auto store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      ldpp_dout(dpp, 0) << "ERR: failed dynamic_cast to RadosStore" << dendl;
      ceph_abort("non-rados backend");
      return;
    }

    librados::IoCtx ioctx;
    if (init_dedup_pool_ioctx(store->getRados(), dpp, ioctx) != 0) {
      throw std::runtime_error("Failed init_dedup_pool_ioctx()");
    }

    // generate an empty epoch with zero counters
    int ret = set_epoch(ioctx, d_cluster_id, dpp, 0, 0);
    if (ret != 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed set_epoch()! ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
      throw std::runtime_error("Failed set_epoch()");
    }
  }

  //---------------------------------------------------------------------------
  int cluster::reset(rgw::sal::RadosStore *store,
                     librados::IoCtx &ioctx,
                     dedup_epoch_t *p_epoch,
                     work_shard_t num_work_shards,
                     md5_shard_t num_md5_shards)
  {
    ldpp_dout(dpp, 10) << __func__ << "::REQ num_work_shards=" << num_work_shards
                       << "::num_md5_shards=" << num_md5_shards << dendl;
    clear();

    while (true) {
      int ret = get_epoch(ioctx, dpp, p_epoch, __func__);
      if (ret != 0) {
        return ret;
      }
      if (p_epoch->num_work_shards && p_epoch->num_md5_shards) {
        ldpp_dout(dpp, 10) << __func__ << "::ACC num_work_shards=" << p_epoch->num_work_shards
                           << "::num_md5_shards=" << p_epoch->num_md5_shards << dendl;
        break;
      }
      else if (!num_work_shards && !num_md5_shards) {
        ldpp_dout(dpp, 10) << __func__ << "::Init flow, no need to wait" << dendl;
        break;
      }
      else {
        ret = swap_epoch(dpp, ioctx, p_epoch,
                         static_cast<dedup_req_type_t> (p_epoch->dedup_type),
                         num_work_shards, num_md5_shards);
      }
    }

    d_epoch_time = p_epoch->time;
    // retry cleanup 3 times before declaring failure
    const unsigned RETRY_LIMIT = 3;
    int ret = 1;
    for (unsigned i = 0; i < RETRY_LIMIT && ret != 0; i++) {
      ret = cleanup_prev_run(ioctx);
    }
    if (ret != 0) {
      return ret;
    }

    create_shard_tokens(ioctx, p_epoch->num_work_shards, WORKER_SHARD_PREFIX);
    create_shard_tokens(ioctx, p_epoch->num_md5_shards, MD5_SHARD_PREFIX);

    ret = verify_all_shard_tokens(ioctx, p_epoch->num_work_shards,
                                  WORKER_SHARD_PREFIX);
    if (ret != 0) {
      return ret;
    }
    return verify_all_shard_tokens(ioctx, p_epoch->num_md5_shards,
                                   MD5_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  int cluster::cleanup_prev_run(librados::IoCtx &ioctx)
  {
    int error_code = 0;
    constexpr uint32_t max = 100;
    std::string marker;
    bool truncated = false;
    rgw::AccessListFilter filter{};
    unsigned deleted_count = 0, skipped_count  = 0;
    unsigned failed_count  = 0, no_entry_count = 0;
    do {
      std::vector<std::string> oids;
      int ret = rgw_list_pool(dpp, ioctx, max, filter, marker, &oids, &truncated);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 10) << __func__ << "::rgw_list_pool() ret == -ENOENT"<< dendl;
        break;
      }
      else if (ret < 0) {
        ldpp_dout(dpp, 1) << "failed rgw_list_pool()! ret=" << ret
                          << "::" << cpp_strerror(-ret) << dendl;
        return ret;
      }

      for (const std::string& oid : oids) {
        if (oid == DEDUP_WATCH_OBJ || oid == DEDUP_EPOCH_TOKEN) {
          ldpp_dout(dpp, 10) << __func__ << "::skipping " << oid << dendl;
          skipped_count++;
          continue;
        }
        uint64_t size;
        struct timespec tspec;
        ret = ioctx.stat2(oid, &size, &tspec);
        if (ret == -ENOENT) {
          ldpp_dout(dpp, 20) << __func__ << "::" << oid
                             << " was removed by others" << dendl;
          no_entry_count++;
          continue;
        }
        else if (ret != 0) {
          ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.stat( " << oid << " )" << dendl;
          error_code = ret;
          failed_count++;
          continue;
        }
        utime_t mtime(tspec);
        if (d_epoch_time < mtime) {
          ldpp_dout(dpp, 10) << __func__ << "::skipping new obj! "
                             << "::EPOCH={" << d_epoch_time.tv.tv_sec << ":" << d_epoch_time.tv.tv_nsec << "} "
                             << "::mtime={" << mtime.tv.tv_sec << ":" << mtime.tv.tv_nsec << "}" << dendl;
          skipped_count++;
          continue;
        }
        ldpp_dout(dpp, 10) << __func__ << "::removing object: " << oid << dendl;
        ret = ioctx.remove(oid);
        if (ret == 0) {
          deleted_count++;
        }
        else if (ret == -ENOENT) {
          ldpp_dout(dpp, 20) << __func__ << "::" << oid
                             << " was removed by others" << dendl;
          no_entry_count++;
          continue;
        }
        else {
          error_code = ret;
          failed_count++;
          ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.remove( " << oid
                             << " ), ret=" << ret << "::" << cpp_strerror(-ret) << dendl;
        }
      }
      ldpp_dout(dpp, 10) << __func__ << "::oids.size()=" << oids.size()
                         << "::deleted=" << deleted_count
                         << "::failed="  << failed_count
                         << "::no entry="  << no_entry_count
                         << "::skipped=" << skipped_count << dendl;
    } while (truncated);

    return error_code;
  }

  //---------------------------------------------------------------------------
  int cluster::create_shard_tokens(librados::IoCtx &ioctx,
                                   unsigned shards_count,
                                   const char *prefix)
  {
    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 15) << __func__ << "::creating object: " << oid << dendl;
      bool exclusive = true;
      int ret = ioctx.create(oid, exclusive);
      if (ret >= 0) {
        ldpp_dout(dpp, 15) << __func__ << "::oid=" << oid << " was created!" << dendl;
      }
      else if (ret == -EEXIST) {
        ldpp_dout(dpp, 15) << __func__ << "::failed ioctx.create("
                           << oid << ") -EEXIST!" << dendl;
      }
      else {
        // TBD: can it happen legally ?
        ldpp_dout(dpp, 1) << __func__ << "::failed ioctx.create(" << oid
                          << ") with: " << ret  << "::" << cpp_strerror(-ret) << dendl;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::verify_all_shard_tokens(librados::IoCtx &ioctx,
                                       unsigned shards_count,
                                       const char *prefix)
  {
    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 10) << __func__ << "::checking object: " << oid << dendl;

      uint64_t size;
      struct timespec tspec;
      int ret = ioctx.stat2(oid, &size, &tspec);
      if (ret != 0) {
        ldpp_dout(dpp, 5) << __func__ << "::failed ioctx.stat( " << oid << " )"
                          << "::shards_count=" << shards_count << dendl;
        return ret;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::update_shard_token_heartbeat(librados::IoCtx &ioctx,
                                            unsigned shard,
                                            uint64_t count_a,
                                            uint64_t count_b,
                                            const char *prefix)
  {
    shard_token_oid sto(prefix, shard);
    std::string oid(sto.get_buff(), sto.get_buff_size());
    bufferlist empty_bl;
    shard_progress_t sp(count_a, count_b, false, d_cluster_id, empty_bl);
    sp.creation_time = d_token_creation_time;
    bufferlist sp_bl;
    encode(sp, sp_bl);
    return ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
  }

  //---------------------------------------------------------------------------
  int cluster::mark_shard_token_completed(librados::IoCtx &ioctx,
                                          unsigned shard,
                                          uint64_t obj_count,
                                          const char *prefix,
                                          const bufferlist &bl)
  {
    shard_token_oid sto(prefix, shard);
    std::string oid(sto.get_buff(), sto.get_buff_size());
    ldpp_dout(dpp, 10) << __func__ << "::" << prefix << "::" << oid << dendl;

    shard_progress_t sp(obj_count, SP_ALL_OBJECTS, true, d_cluster_id, bl);
    sp.creation_time = d_token_creation_time;
    bufferlist sp_bl;
    encode(sp, sp_bl);
    int ret = ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Done ioctx.setxattr(" << oid << ")" << dendl;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::Failed ioctx.setxattr(" << oid << ") ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int32_t cluster::get_next_shard_token(librados::IoCtx &ioctx,
                                        uint16_t start_shard,
                                        uint16_t max_shard,
                                        const char *prefix)
  {
    // lock paramters:
    const utime_t     lock_duration;  // zero duration means lock doesn't expire
    const uint8_t     lock_flags = 0; // no flags
    const std::string lock_tag;       // no tag

    shard_token_oid sto(prefix);
    for (auto shard = start_shard; shard < max_shard; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 10) << __func__ << "::try garbbing " << oid << dendl;
      librados::ObjectWriteOperation op;
      op.assert_exists();
      rados::cls::lock::lock(&op, oid, ClsLockType::EXCLUSIVE, d_lock_cookie,
                             lock_tag, "dedup_shard_token", lock_duration, lock_flags);
      int ret = rgw_rados_operate(dpp, ioctx, oid, std::move(op), null_yield);
      if (ret == -EBUSY) {
        // someone else took this token -> move to the next one
        ldpp_dout(dpp, 10) << __func__ << "::Failed lock. " << oid <<
          " is owned by other rgw" << dendl;
        continue;
      }
      else if (ret == -ENOENT) {
        // token is deleted - processing will stop the next time we try to read from the queue
        ldpp_dout(dpp, 5) << __func__ << "::" << oid
                          << " token doesn't exist, fail lock!" << dendl;
        continue;
      }
      else if (ret < 0) {
        // failed to lock for another reason, continue to process other queues
        ldpp_dout(dpp, 1) << __func__ << "::ERROR: failed to lock token: " << oid
                          << ":: ret=" << ret << "::" << cpp_strerror(-ret) << dendl;
        //has_error = true;
        continue;
      }
      ldpp_dout(dpp, 10) << __func__ << "::successfully locked " << oid << dendl;
      bufferlist empty_bl;
      shard_progress_t sp(SP_NO_OBJECTS, SP_NO_OBJECTS, false, d_cluster_id, empty_bl);
      d_token_creation_time = sp.creation_time;

      bufferlist sp_bl;
      encode(sp, sp_bl);
      ret = ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
      if (ret == 0) {
        ldpp_dout(dpp, 10) << __func__ << "::SUCCESS!::" << oid << dendl;
        return shard;
      }
    }

    return NULL_SHARD;
  }

  //---------------------------------------------------------------------------
  work_shard_t cluster::get_next_work_shard_token(librados::IoCtx &ioctx,
                                                  work_shard_t num_work_shards)
  {
    int32_t shard = get_next_shard_token(ioctx, d_curr_worker_shard, num_work_shards,
                                         WORKER_SHARD_PREFIX);
    if (shard >= 0 && shard < num_work_shards) {
      d_curr_worker_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_WORK_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  md5_shard_t cluster::get_next_md5_shard_token(librados::IoCtx &ioctx,
                                                md5_shard_t num_md5_shards)
  {
    int32_t shard = get_next_shard_token(ioctx, d_curr_md5_shard, num_md5_shards,
                                         MD5_SHARD_PREFIX);
    if (shard >= 0 && shard < num_md5_shards) {
      d_curr_md5_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_MD5_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  bool cluster::all_shard_tokens_completed(librados::IoCtx &ioctx,
                                           unsigned shards_count,
                                           const char *prefix,
                                           uint16_t *p_num_completed,
                                           uint8_t completed_arr[],
                                           uint64_t *p_total_ingressed)
  {
    unsigned count = 0;
    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      if (completed_arr[shard] != TOKEN_STATE_PENDING) {
        count++;
        continue;
      }

      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 10) << __func__ << "::checking object: " << oid << dendl;
      bufferlist bl;
      int ret = ioctx.getxattr(oid, SHARD_PROGRESS_ATTR, bl);
      if (unlikely(ret <= 0)) {
        if (ret != -ENODATA) {
          ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.getxattr() ret="
                             << ret << "::" << cpp_strerror(-ret) << dendl;
        }
        continue;
      }

      shard_progress_t sp;
      try {
        auto p = bl.cbegin();
        decode(sp, p);
      }
      catch (const buffer::error&) {
        ldpp_dout(dpp, 0) << __func__ << "::failed shard_progress_t decode!" << dendl;
        return false;
      }

      if (sp.progress_b == SP_ALL_OBJECTS) {
        ceph_assert(sp.completed);
        utime_t duration = sp.completion_time - sp.creation_time;
        // mark token completed;
        (*p_num_completed)++;
        completed_arr[shard] = TOKEN_STATE_COMPLETED;
        d_total_ingressed_obj += sp.progress_a;
        ldpp_dout(dpp, 20) << __func__ << "::" << oid
                           << "::completed! duration=" << duration << dendl;
        count++;
      }
      else {
        static const utime_t heartbeat_timeout(EPOCH_MAX_LOCK_DURATION_SEC, 0);
        utime_t time_elapsed = sp.update_time - sp.creation_time;
        if (time_elapsed > heartbeat_timeout) {
          // lock expired -> try and break lock
          ldpp_dout(dpp, 0) << __func__ << "::" << oid << "::expired lock, skipping" << dendl;
          completed_arr[shard] = TOKEN_STATE_TIMED_OUT;
          d_num_failed_workers++;
          continue;
        }
        else {
          return false;
        }
        // TBD: need to store copies and declare token with no progress for N seconds
        // as failing and then skip it
        return false;
      }
    } // loop

    *p_total_ingressed = d_total_ingressed_obj;
    if (count < shards_count) {
      unsigned n = shards_count - count;
      ldpp_dout(dpp, 10) << __func__ << "::waiting for " << n << " tokens" << dendl;
    }
    return (count == shards_count);
  }

  //---------------------------------------------------------------------------
  static int collect_shard_stats(librados::IoCtx &ioctx,
                                 const DoutPrefixProvider *dpp,
                                 utime_t epoch_time,
                                 unsigned shards_count,
                                 const char *prefix,
                                 bufferlist bl_arr[],
                                 shard_progress_t *sp_arr)
  {
    unsigned count = 0;
    cluster::shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 20) << __func__ << "::checking object: " << oid << dendl;

      uint64_t size;
      struct timespec tspec;
      if (ioctx.stat2(oid, &size, &tspec) != 0) {
        ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.stat( " << oid << " )"
                           << "::shards_count=" << shards_count << dendl;
        continue;
      }
      utime_t mtime(tspec);
      if (epoch_time > mtime) {
        ldpp_dout(dpp, 10) << __func__ << "::skipping old obj! "
                           << "::EPOCH={" << epoch_time.tv.tv_sec << ":" << epoch_time.tv.tv_nsec << "} "
                           << "::mtime={" << mtime.tv.tv_sec << ":" << mtime.tv.tv_nsec << "}" << dendl;
        continue;
      }

      shard_progress_t sp;
      bufferlist bl;
      int ret = ioctx.getxattr(oid, SHARD_PROGRESS_ATTR, bl);
      if (ret > 0) {
        try {
          auto p = bl.cbegin();
          decode(sp, p);
          sp_arr[shard] = sp;
          count++;
        }
        catch (const buffer::error&) {
          ldpp_dout(dpp, 10) << __func__ << "::(1)failed shard_progress_t decode!" << dendl;
          return -EINVAL;
        }
      }
      else if (ret != -ENODATA) {
        ldpp_dout(dpp, 10) << __func__ << "::" << oid << "::failed getxattr() ret="
                           << ret << "::" << cpp_strerror(-ret) << dendl;
        continue;
      }
      bl_arr[shard] = sp.stats_bl;
    }

    if (count != shards_count) {
      ldpp_dout(dpp, 10) << __func__ << "::missing shards stats! we got "
                         << count << " / " << shards_count << dendl;
    }

    return count;
  }

  struct member_time_t {
    utime_t start_time;
    utime_t end_time;
    utime_t aggregated_time;
  };

  //---------------------------------------------------------------------------
  static void collect_single_shard_stats(const DoutPrefixProvider *dpp,
                                         std::map<std::string, member_time_t> &owner_map,
                                         const shard_progress_t sp_arr[],
                                         unsigned shard,
                                         bool *p_show_time,
                                         const char *name)
  {
    const utime_t null_time;
    const shard_progress_t &sp = sp_arr[shard];
    if (sp.creation_time == null_time || sp.completion_time == null_time) {
      *p_show_time = false;
      return;
    }

    const std::string &owner = sp.owner;
    utime_t duration = sp.completion_time - sp.creation_time;
    if (owner_map.find(owner) != owner_map.end()) {
      owner_map[owner].aggregated_time += duration;
      owner_map[owner].end_time = sp.completion_time;
    }
    else {
      owner_map[owner].start_time = sp.creation_time;
      owner_map[owner].aggregated_time = duration;
      owner_map[owner].end_time = sp.completion_time;
    }
    ldpp_dout(dpp, 10) << __func__ << "::Got " << name
                       << " stats for shard #" << shard << dendl;
  }

  //---------------------------------------------------------------------------
  static void show_incomplete_shards_fmt(bool has_incomplete_shards,
                                         unsigned num_shards,
                                         const shard_progress_t sp_arr[],
                                         Formatter *fmt)

  {
    if (!has_incomplete_shards) {
      return;
    }
    Formatter::ArraySection array_section{*fmt, "incomplete_shards"};
    for (unsigned shard = 0; shard < num_shards; shard++) {
      if (sp_arr[shard].is_completed() ) {
        continue;
      }
      Formatter::ObjectSection object_section{*fmt, "shard_progress"};
      fmt->dump_unsigned("shard_id", shard);
      fmt->dump_string("owner", sp_arr[shard].owner);
      fmt->dump_unsigned("progress_a", sp_arr[shard].progress_a);
      fmt->dump_unsigned("progress_b", sp_arr[shard].progress_b);
      fmt->dump_stream("last updated") << sp_arr[shard].update_time;
    }
  }

  //---------------------------------------------------------------------------
  static utime_t show_time_func_fmt(const utime_t &start_time,
                                    bool show_time,
                                    const std::map<std::string, member_time_t> &owner_map,
                                    Formatter *fmt)
  {
    member_time_t all_members_time;
    all_members_time.start_time = start_time;
    all_members_time.end_time   = start_time;
    all_members_time.aggregated_time = utime_t();

    Formatter::ObjectSection section{*fmt, "time"};
    {
      Formatter::ArraySection array_section{*fmt, "per-shard time"};
      for (const auto& [owner, value] : owner_map) {
        uint32_t sec = value.end_time.tv.tv_sec - value.start_time.tv.tv_sec;
        fmt->dump_stream("member time")
          << owner << "::start time = [" << value.start_time.tv.tv_sec % 1000
          << ":" << value.start_time.tv.tv_nsec / (1000*1000) << "] "
          << "::aggregated time = " << value.aggregated_time.tv.tv_sec
          << "(" << sec << ") seconds";
        all_members_time.aggregated_time += value.aggregated_time;
        if (all_members_time.end_time < value.end_time) {
          all_members_time.end_time = value.end_time;
        }
      }
    }

    if (show_time) {
      uint32_t sec = all_members_time.end_time.tv.tv_sec - all_members_time.start_time.tv.tv_sec;

      Formatter::ObjectSection section{*fmt, "All shards time"};
      fmt->dump_stream("start time") << all_members_time.start_time;
      fmt->dump_stream("end time")
        << all_members_time.end_time << " (" << sec << " seconds total)";
      fmt->dump_unsigned("aggregated time (sec)", all_members_time.aggregated_time.tv.tv_sec);
    }

    return all_members_time.end_time;
  }

  //---------------------------------------------------------------------------
  static void show_dedup_ratio_estimate_fmt(const worker_stats_t &wrk_stats_sum,
                                            const md5_stats_t    &md5_stats_sum,
                                            Formatter *fmt)
  {
    uint64_t s3_bytes_before = wrk_stats_sum.ingress_obj_bytes;
    uint64_t s3_dedup_bytes  = md5_stats_sum.dedup_bytes_estimate;
    uint64_t s3_bytes_after  = s3_bytes_before - s3_dedup_bytes;

    Formatter::ObjectSection section{*fmt, "dedup_ratio_estimate"};
    fmt->dump_unsigned("s3_bytes_before", s3_bytes_before);
    fmt->dump_unsigned("s3_bytes_after", s3_bytes_after);

    if (s3_bytes_before > s3_bytes_after && s3_bytes_after) {
      double dedup_ratio = (double)s3_bytes_before/s3_bytes_after;
      fmt->dump_float("dedup_ratio", dedup_ratio);
    }
    else {
      fmt->dump_float("dedup_ratio", 0);
    }
  }

  //---------------------------------------------------------------------------
  static void show_dedup_ratio_actual_fmt(const worker_stats_t &wrk_stats_sum,
                                          const md5_stats_t    &md5_stats_sum,
                                          Formatter *fmt)
  {
    uint64_t s3_bytes_before = wrk_stats_sum.ingress_obj_bytes;
    uint64_t s3_dedup_bytes  = (md5_stats_sum.deduped_objects_bytes +
                                md5_stats_sum.shared_manifest_dedup_bytes);
    uint64_t s3_bytes_after  = s3_bytes_before - s3_dedup_bytes;

    Formatter::ObjectSection section{*fmt, "dedup_ratio_actual"};
    fmt->dump_unsigned("s3_bytes_before", s3_bytes_before);
    fmt->dump_unsigned("s3_bytes_after", s3_bytes_after);

    if (s3_bytes_before > s3_bytes_after && s3_bytes_after) {
      double dedup_ratio = (double)s3_bytes_before/s3_bytes_after;
      fmt->dump_float("dedup_ratio", dedup_ratio);
    }
    else {
      fmt->dump_float("dedup_ratio", 0);
    }
  }

  //---------------------------------------------------------------------------
  // command-line called from radosgw-admin.cc
  int cluster::collect_all_shard_stats(rgw::sal::RadosStore *store,
                                       Formatter *fmt,
                                       const DoutPrefixProvider *dpp)
  {
    librados::IoCtx ioctx;
    int ret = init_dedup_pool_ioctx(store->getRados(), dpp, ioctx);
    if (ret != 0) {
      return ret;
    }

    dedup_epoch_t epoch;
    ret = get_epoch(ioctx, dpp, &epoch, nullptr);
    if (ret != 0) {
      return ret;
    }

    Formatter::ObjectSection section{*fmt, "DEDUP STAT COUNTERS"};
    work_shard_t num_work_shards = epoch.num_work_shards;
    md5_shard_t  num_md5_shards  = epoch.num_md5_shards;

    unsigned completed_work_shards_count = 0;
    unsigned completed_md5_shards_count  = 0;
    utime_t md5_start_time;
    worker_stats_t wrk_stats_sum;
    {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      bufferlist bl_arr[num_work_shards];
      shard_progress_t sp_arr[num_work_shards];
      int cnt = collect_shard_stats(ioctx, dpp, epoch.time, num_work_shards,
                                    WORKER_SHARD_PREFIX, bl_arr, sp_arr);
      if (cnt != num_work_shards && 0) {
        std::cerr << ">>>Partial work shard stats recived " << cnt << " / "
                  << num_work_shards << "\n" << std::endl;
      }
      bool has_incomplete_shards = false;
      for (unsigned shard = 0; shard < num_work_shards; shard++) {
        if (bl_arr[shard].length() == 0) {
          has_incomplete_shards = true;
          continue;
        }
        completed_work_shards_count++;
        worker_stats_t stats;
        try {
          auto p = bl_arr[shard].cbegin();
          decode(stats, p);
          wrk_stats_sum += stats;
        }catch (const buffer::error&) {
          // TBD: can we use std::cerr or should we use formatter ??
          std::cerr << __func__ << "::(2)failed worker_stats_t decode #" << shard << std::endl;
          continue;
        }
        collect_single_shard_stats(dpp, owner_map, sp_arr, shard, &show_time, "WORKER");
      }
      Formatter::ObjectSection worker_stats(*fmt, "worker_stats");
      wrk_stats_sum.dump(fmt);
      show_incomplete_shards_fmt(has_incomplete_shards, num_work_shards, sp_arr, fmt);
      md5_start_time = show_time_func_fmt(epoch.time, show_time, owner_map, fmt);
    }

    if (completed_work_shards_count == num_work_shards) {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      md5_stats_t md5_stats_sum;
      bufferlist bl_arr[num_md5_shards];
      shard_progress_t sp_arr[num_md5_shards];
      int cnt = collect_shard_stats(ioctx, dpp, epoch.time, num_md5_shards,
                                    MD5_SHARD_PREFIX, bl_arr, sp_arr);
      if (cnt != num_md5_shards && 0) {
        std::cerr << ">>>Partial MD5_SHARD stats recived " << cnt << " / "
                  << num_md5_shards << "\n" << std::endl;
      }
      bool has_incomplete_shards = false;
      for (unsigned shard = 0; shard < num_md5_shards; shard++) {
        if (bl_arr[shard].length() == 0) {
          has_incomplete_shards = true;
          continue;
        }
        completed_md5_shards_count++;
        md5_stats_t stats;
        try {
          auto p = bl_arr[shard].cbegin();
          decode(stats, p);
          md5_stats_sum += stats;
        }catch (const buffer::error&) {
          // TBD: can we use std::cerr or should we use formatter ??
          std::cerr << __func__ << "::failed md5_stats_t decode #" << shard << std::endl;
          continue;
        }
        collect_single_shard_stats(dpp, owner_map, sp_arr, shard, &show_time, "MD5");
      }
      {
        Formatter::ObjectSection outer(*fmt, "md5_stats");
        md5_stats_sum.dump(fmt);
        show_incomplete_shards_fmt(has_incomplete_shards, num_md5_shards, sp_arr, fmt);
        show_time_func_fmt(md5_start_time, show_time, owner_map, fmt);
      }
      show_dedup_ratio_estimate_fmt(wrk_stats_sum, md5_stats_sum, fmt);
      show_dedup_ratio_actual_fmt(wrk_stats_sum, md5_stats_sum, fmt);
    }

    fmt->dump_bool("completed", (completed_md5_shards_count == num_md5_shards));
    return 0;
  }

  //---------------------------------------------------------------------------
  // command-line called from radosgw-admin.cc
  int cluster::dedup_control(rgw::sal::RadosStore *store,
                             const DoutPrefixProvider *dpp,
                             urgent_msg_t urgent_msg)
  {
    ldpp_dout(dpp, 20) << __func__ << "::dedup_control req = "
                       << get_urgent_msg_names(urgent_msg) << dendl;
    if (urgent_msg != URGENT_MSG_RESUME  &&
        urgent_msg != URGENT_MSG_PASUE   &&
        urgent_msg != URGENT_MSG_RESTART &&
        urgent_msg != URGENT_MSG_ABORT) {
      ldpp_dout(dpp, 1) << __func__ << "::illegal urgent_msg="<< urgent_msg << dendl;
      return -EINVAL;
    }

    librados::IoCtx ioctx;
    int ret = init_dedup_pool_ioctx(store->getRados(), dpp, ioctx);
    if (ret != 0) {
      return ret;
    }
    // 10 seconds timeout
    const uint64_t timeout_ms = 10*1000;
    bufferlist reply_bl, urgent_msg_bl;
    ceph::encode(urgent_msg, urgent_msg_bl);
    ret = rgw_rados_notify(dpp, ioctx, DEDUP_WATCH_OBJ, urgent_msg_bl,
                           timeout_ms, &reply_bl, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed rgw_rados_notify("
                        << DEDUP_WATCH_OBJ << ")::err="<<cpp_strerror(-ret) << dendl;
      return ret;
    }
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    ioctx.decode_notify_response(reply_bl, &acks, &timeouts);
    if (timeouts.size() > 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed rgw_rados_notify("
                        << DEDUP_WATCH_OBJ << ")::timeout error" << dendl;
      return -EAGAIN;
    }

    for (auto& ack : acks) {
      try {
        ldpp_dout(dpp, 20) << __func__ << "::ACK: notifier_id=" << ack.notifier_id
                           << "::cookie=" << ack.cookie << dendl;
        auto iter = ack.payload_bl.cbegin();
        ceph::decode(ret, iter);
        struct rgw::dedup::control_t ctl;
        decode(ctl, iter);
        ldpp_dout(dpp, 10) << __func__ << "::++ACK::ctl=" << ctl << "::ret=" << ret << dendl;
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 1) << __func__ << "::failed decoding notify acks" << dendl;
        return -EINVAL;
      }
      if (ret != 0) {
        ldpp_dout(dpp, 1) << __func__ << "::Bad notify ack, ret=" << ret
                          << "::err=" << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }
    ldpp_dout(dpp, 10) << __func__ << "::" << get_urgent_msg_names(urgent_msg)
                       << " finished successfully!" << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  // command-line called from radosgw-admin.cc
  int cluster::dedup_restart_scan(rgw::sal::RadosStore *store,
                                  dedup_req_type_t dedup_type,
                                  const DoutPrefixProvider *dpp)
  {
    librados::IoCtx ioctx;
    int ret = init_dedup_pool_ioctx(store->getRados(), dpp, ioctx);
    if (ret != 0) {
      return ret;
    }

    dedup_epoch_t old_epoch;
    // store the previous epoch for cmp-swap
    ret = get_epoch(ioctx, dpp, &old_epoch, __func__);
    if (ret != 0) {
      return ret;
    }

    // first abort all dedup work!
    ret = dedup_control(store, dpp, URGENT_MSG_ABORT);
    if (ret != 0) {
      return ret;
    }

    ldpp_dout(dpp, 10) << __func__ << dedup_type << dendl;
#ifdef FULL_DEDUP_SUPPORT
    ceph_assert(dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE ||
                dedup_type == dedup_req_type_t::DEDUP_TYPE_FULL);
#else
    ceph_assert(dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE);
#endif
    ret = swap_epoch(dpp, ioctx, &old_epoch, dedup_type, 0, 0);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Epoch object was reset" << dendl;
      return dedup_control(store, dpp, URGENT_MSG_RESTART);
    }
    else {
      return ret;
    }
  }

  //---------------------------------------------------------------------------
  bool cluster::can_start_new_scan(rgw::sal::RadosStore *store)
  {
    ldpp_dout(dpp, 10) << __func__ << "::epoch=" << d_epoch_time << dendl;
    librados::IoCtx ioctx;
    int ret = init_dedup_pool_ioctx(store->getRados(), dpp, ioctx);
    if (ret != 0) {
      return ret;
    }

    dedup_epoch_t new_epoch;
    if (get_epoch(ioctx, dpp, &new_epoch, nullptr) != 0) {
      ldpp_dout(dpp, 1) << __func__ << "::No Epoch Object::"
                        << "::scan can be restarted!\n\n\n" << dendl;
      // no epoch object exists -> we should start a new scan
      return true;
    }

    if (new_epoch.time <= d_epoch_time) {
      if (new_epoch.time == d_epoch_time) {
        ldpp_dout(dpp, 10) << __func__ << "::Epoch hasn't change - > Do not restart scan!!" << dendl;
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << " ::Do not restart scan!\n    epoch="
                          << d_epoch_time << "\nnew_epoch="<< new_epoch.time <<dendl;
      }
      return false;
    }
    // allow members to join within a 30 sec limit
    utime_t limit = {30, 0};
    utime_t now = ceph_clock_now();
    ldpp_dout(dpp, 1) << __func__ << "\n::new_epoch=" << new_epoch.time
                      << "\n::now      =" << now << dendl;
    if ((now > new_epoch.time) && ((now - new_epoch.time) < limit)) {
      ldpp_dout(dpp, 1) << __func__ << "::Epoch is less than 30 seconds old!"
                        << " Restart scan\n\n\n" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "\n::new_epoch - now = " << (new_epoch.time - now)
                      << "\n::limit           = " << limit << dendl;

    if (new_epoch.time > now) {
      ldpp_dout(dpp, 1) << ":new_epoch > now = TRUE " << dendl;
    }
    return false;
  }
} // namespace rgw::dedup
