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
  const char* DEDUP_WATCH_OBJ = "DEDUP_WATCH_OBJ";

  static constexpr unsigned EPOCH_MAX_LOCK_DURATION_SEC = 30;
  struct shard_progress_t;
  static int collect_shard_stats(rgw::sal::RadosStore *store,
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
  static int get_control_ioctx(rgw::sal::RadosStore     *store,
                               const DoutPrefixProvider *dpp,
                               librados::IoCtx &ctl_ioctx /* OUT-PARAM */)
  {
    const auto& control_pool = store->svc()->zone->get_zone_params().control_pool;
    auto rados_handle = store->getRados()->get_rados_handle();
    int ret = rgw_init_ioctx(dpp, rados_handle, control_pool, ctl_ioctx);
    if (unlikely(ret < 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed rgw_init_ioctx() for control_pool ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  static int get_epoch(rgw::sal::RadosStore     *store,
                       const DoutPrefixProvider *dpp,
                       dedup_epoch_t *p_epoch, /* OUT */
                       const char *caller)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    std::string oid(DEDUP_EPOCH_TOKEN);
    bufferlist bl;
    ret = ctl_ioctx.getxattr(oid, RGW_DEDUP_ATTR_EPOCH, bl);
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
                         << "::failed ctl_ioctx.getxattr() with: "
                         << cpp_strerror(-ret) << ", ret=" << ret << dendl;
      return ret;
    }
  }

  //---------------------------------------------------------------------------
  static int set_epoch(rgw::sal::RadosStore *store,
                       const std::string &cluster_id,
                       const DoutPrefixProvider *dpp,
                       work_shard_t num_work_shards,
                       md5_shard_t num_md5_shards)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    std::string oid(DEDUP_EPOCH_TOKEN);
    ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << dendl;
    bool exclusive = true; // block overwrite of old objects
    ret = ctl_ioctx.create(oid, exclusive);
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
    ret = ctl_ioctx.operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Epoch object was written" << dendl;
    }
    // TBD: must check for failure caused by an existing EPOCH xattr!
    // probably best to read attribute from epoch!
    else if (ret == -ECANCELED) {
      dedup_epoch_t epoch;
      ret = get_epoch(store, dpp, &epoch, __func__);
      if (ret == 0) {
        ldpp_dout(dpp, 10) << __func__ << "::Accept existing Epoch object" << dendl;
      }
      return ret;
    }
    else {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed ctl_ioctx.operate("
                        << oid << "), err is " << cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  static int swap_epoch(rgw::sal::RadosStore     *store,
                        const DoutPrefixProvider *dpp,
                        const dedup_epoch_t *p_old_epoch,
                        dedup_req_type_t dedup_type,
                        work_shard_t num_work_shards,
                        md5_shard_t num_md5_shards)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

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
    ret = ctl_ioctx.operate(oid, &op);
    if (ret != 0) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed ctl_ioctx.operate("
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

      utime_t now = ceph_clock_now();
      this->update_time = now;

      if (_progress_a == SP_NO_OBJECTS && _progress_b == SP_NO_OBJECTS) {
        this->creation_time = now;
      }
      if (_completed) {
        this->completion_time = now;
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

    bool was_not_started() const {
      return (this->creation_time == this->update_time);
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
  std::ostream& operator<<(std::ostream &out, shard_progress_t& sp)
  {
    out << (sp.completed ? " + ::" : " - ::");
    out << sp.owner << "::[" << sp.progress_a << ", " << sp.progress_b << "]";
    out << "::creation: " << sp.creation_time;
    out << "::update: " << sp.update_time;
    out << "::completion: " << sp.completion_time;
    return out;
  }

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
  }

  //---------------------------------------------------------------------------
  int cluster::reset(rgw::sal::RadosStore *store,
                     dedup_epoch_t *p_epoch,
                     work_shard_t num_work_shards,
                     md5_shard_t num_md5_shards)
  {
    ldpp_dout(dpp, 10) << __func__ << "::REQ num_work_shards=" << num_work_shards
                       << "::num_md5_shards=" << num_md5_shards << dendl;
    clear();

    while (true) {
      int ret = get_epoch(store, dpp, p_epoch, __func__);
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
        ret = swap_epoch(store, dpp, p_epoch,
                         static_cast<dedup_req_type_t> (p_epoch->dedup_type),
                         num_work_shards, num_md5_shards);
      }
    }

    d_epoch_time = p_epoch->time;
    // retry cleanup 3 times before declaring failure
    const unsigned RETRY_LIMIT = 3;
    int ret = 1;
    for (unsigned i = 0; i < RETRY_LIMIT && ret != 0; i++) {
      ret = cleanup_prev_run(store);
    }
    if (ret != 0) {
      return ret;
    }

    create_shard_tokens(store, p_epoch->num_work_shards, WORKER_SHARD_PREFIX);
    create_shard_tokens(store, p_epoch->num_md5_shards, MD5_SHARD_PREFIX);

    ret = verify_all_shard_tokens(store, p_epoch->num_work_shards,
                                  WORKER_SHARD_PREFIX);
    if (ret != 0) {
      return ret;
    }
    return verify_all_shard_tokens(store, p_epoch->num_md5_shards,
                                   MD5_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  int cluster::cleanup_prev_run(rgw::sal::RadosStore *store)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    int error_code = 0;
    constexpr uint32_t max = 100;
    std::string marker;
    bool truncated = false;
    rgw::AccessListFilter filter{};
    unsigned deleted_count = 0, skipped_count  = 0;
    unsigned failed_count  = 0, no_entry_count = 0;
    do {
      std::vector<std::string> oids;
      int ret = rgw_list_pool(dpp, ctl_ioctx, max, filter, marker, &oids, &truncated);
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
        if (shard_token_oid::legal_oid_name(oid) == false) {
          ldpp_dout(dpp, 10) << __func__ << "::skipping " << oid << dendl;
          skipped_count++;
          continue;
        }

        uint64_t size;
        struct timespec tspec;
        ret = ctl_ioctx.stat2(oid, &size, &tspec);
        if (ret == -ENOENT) {
          ldpp_dout(dpp, 20) << __func__ << "::" << oid
                             << " was removed by others" << dendl;
          no_entry_count++;
          continue;
        }
        else if (ret != 0) {
          ldpp_dout(dpp, 10) << __func__ << "::failed ctl_ioctx.stat( "
                             << oid << " )" << dendl;
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
        ret = ctl_ioctx.remove(oid);
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
          ldpp_dout(dpp, 10) << __func__ << "::failed ctl_ioctx.remove( " << oid
                             << " ), ret=" << ret << "::" << cpp_strerror(-ret) << dendl;
        }
      }
      ldpp_dout(dpp, 10) << __func__ << "::oids.size()=" << oids.size()
                         << "::deleted="  << deleted_count
                         << "::failed="   << failed_count
                         << "::no entry=" << no_entry_count
                         << "::skipped="  << skipped_count << dendl;
    } while (truncated);

    return error_code;
  }

  //---------------------------------------------------------------------------
  int cluster::create_shard_tokens(rgw::sal::RadosStore *store,
                                   unsigned shards_count,
                                   const char *prefix)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 15) << __func__ << "::creating object: " << oid << dendl;
      bool exclusive = true;
      ret = ctl_ioctx.create(oid, exclusive);
      if (ret >= 0) {
        ldpp_dout(dpp, 15) << __func__ << "::oid=" << oid << " was created!" << dendl;
      }
      else if (ret == -EEXIST) {
        ldpp_dout(dpp, 15) << __func__ << "::failed ctl_ioctx.create("
                           << oid << ") -EEXIST!" << dendl;
      }
      else {
        // TBD: can it happen legally ?
        ldpp_dout(dpp, 1) << __func__ << "::failed ctl_ioctx.create(" << oid
                          << ") with: " << ret  << "::" << cpp_strerror(-ret) << dendl;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::verify_all_shard_tokens(rgw::sal::RadosStore *store,
                                       unsigned shards_count,
                                       const char *prefix)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 10) << __func__ << "::checking object: " << oid << dendl;

      uint64_t size;
      struct timespec tspec;
      ret = ctl_ioctx.stat2(oid, &size, &tspec);
      if (ret != 0) {
        ldpp_dout(dpp, 5) << __func__ << "::failed ctl_ioctx.stat( " << oid << " )"
                          << "::shards_count=" << shards_count << dendl;
        return ret;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::update_shard_token_heartbeat(rgw::sal::RadosStore *store,
                                            unsigned shard,
                                            uint64_t count_a,
                                            uint64_t count_b,
                                            const char *prefix)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    shard_token_oid sto(prefix, shard);
    std::string oid(sto.get_buff(), sto.get_buff_size());
    bufferlist empty_bl;
    shard_progress_t sp(count_a, count_b, false, d_cluster_id, empty_bl);
    sp.creation_time = d_token_creation_time;
    bufferlist sp_bl;
    encode(sp, sp_bl);
    return ctl_ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
  }

  //---------------------------------------------------------------------------
  int cluster::mark_shard_token_completed(rgw::sal::RadosStore *store,
                                          unsigned shard,
                                          uint64_t obj_count,
                                          const char *prefix,
                                          const bufferlist &bl)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    shard_token_oid sto(prefix, shard);
    std::string oid(sto.get_buff(), sto.get_buff_size());
    ldpp_dout(dpp, 10) << __func__ << "::" << prefix << "::" << oid << dendl;

    shard_progress_t sp(obj_count, SP_ALL_OBJECTS, true, d_cluster_id, bl);
    sp.creation_time = d_token_creation_time;
    bufferlist sp_bl;
    encode(sp, sp_bl);
    ret = ctl_ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Done ctl_ioctx.setxattr(" << oid << ")"
                         << dendl;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::Failed ctl_ioctx.setxattr(" << oid
                        << ") ret=" << ret << "::" << cpp_strerror(-ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int32_t cluster::get_next_shard_token(rgw::sal::RadosStore *store,
                                        uint16_t start_shard,
                                        uint16_t max_shard,
                                        const char *prefix)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

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
      ret = rgw_rados_operate(dpp, ctl_ioctx, oid, std::move(op), null_yield);
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
      ret = ctl_ioctx.setxattr(oid, SHARD_PROGRESS_ATTR, sp_bl);
      if (ret == 0) {
        ldpp_dout(dpp, 10) << __func__ << "::SUCCESS!::" << oid << dendl;
        return shard;
      }
    }

    return NULL_SHARD;
  }

  //---------------------------------------------------------------------------
  work_shard_t cluster::get_next_work_shard_token(rgw::sal::RadosStore *store,
                                                  work_shard_t num_work_shards)
  {
    int32_t shard = get_next_shard_token(store, d_curr_worker_shard,
                                         num_work_shards, WORKER_SHARD_PREFIX);
    if (shard >= 0 && shard < num_work_shards) {
      d_curr_worker_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_WORK_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  md5_shard_t cluster::get_next_md5_shard_token(rgw::sal::RadosStore *store,
                                                md5_shard_t num_md5_shards)
  {
    int32_t shard = get_next_shard_token(store, d_curr_md5_shard, num_md5_shards,
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
  int cluster::all_shard_tokens_completed(rgw::sal::RadosStore *store,
                                          unsigned shards_count,
                                          const char *prefix,
                                          uint16_t *p_num_completed,
                                          uint8_t completed_arr[])
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    int err_code = 0;
    unsigned count = 0;
    shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      if (completed_arr[shard] == TOKEN_STATE_COMPLETED) {
        count++;
        continue;
      }

      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 10) << __func__ << "::checking object: " << oid << dendl;
      bufferlist bl;
      ret = ctl_ioctx.getxattr(oid, SHARD_PROGRESS_ATTR, bl);
      if (unlikely(ret <= 0)) {
        if (ret != -ENODATA) {
          ldpp_dout(dpp, 10) << __func__ << "::failed ctl_ioctx.getxattr() ret="
                             << ret << "::" << cpp_strerror(-ret) << dendl;
        }
        completed_arr[shard] = TOKEN_STATE_CORRUPTED;
        // all failures to get valid token state return ENODATA
        err_code = -ENODATA;
        continue;
      }

      shard_progress_t sp;
      try {
        auto p = bl.cbegin();
        decode(sp, p);
      }
      catch (const buffer::error&) {
        ldpp_dout(dpp, 1) << __func__ << "::failed shard_progress_t decode!" << dendl;
        completed_arr[shard] = TOKEN_STATE_CORRUPTED;
        // all failures to get valid token state return ENODATA
        err_code = -ENODATA;
        continue;
      }

      if (sp.is_completed()) {
        utime_t duration = sp.completion_time - sp.creation_time;
        // mark token completed;
        (*p_num_completed)++;
        completed_arr[shard] = TOKEN_STATE_COMPLETED;
        ldpp_dout(dpp, 20) << __func__ << "::" << oid
                           << "::completed! duration=" << duration << dendl;
        count++;
      }
      else if (sp.was_not_started()) {
        // token was not started yet
        // TBD:
        // If it is not locked we can process it (by why we skipped it)??
        // If locked, check when it was done and if timed-out
        ldpp_dout(dpp, 10) << __func__ << "::" << oid
                           << "::was not started, skipping" << dendl;
        return -EAGAIN;
      }
      else {
        static const utime_t heartbeat_timeout(EPOCH_MAX_LOCK_DURATION_SEC, 0);
        utime_t time_elapsed = ceph_clock_now() - sp.update_time;
        if (time_elapsed > heartbeat_timeout) {
          // lock expired -> try and break lock
          ldpp_dout(dpp, 5) << __func__ << "::" << oid
                            << "::expired lock, skipping:" << time_elapsed
                            << "::" << sp << dendl;
          completed_arr[shard] = TOKEN_STATE_TIMED_OUT;
          err_code = -ETIME;
          continue;
        }
        else {
          return -EAGAIN;
        }
      }
    } // loop

    if (count < shards_count) {
      unsigned n = shards_count - count;
      ldpp_dout(dpp, 10) << __func__ << "::waiting for " << n << " tokens" << dendl;
    }
    return err_code;
  }

  //---------------------------------------------------------------------------
  static int collect_shard_stats(rgw::sal::RadosStore *store,
                                 const DoutPrefixProvider *dpp,
                                 utime_t epoch_time,
                                 unsigned shards_count,
                                 const char *prefix,
                                 bufferlist bl_arr[],
                                 shard_progress_t *sp_arr)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    unsigned count = 0;
    cluster::shard_token_oid sto(prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      sto.set_shard(shard);
      std::string oid(sto.get_buff(), sto.get_buff_size());
      ldpp_dout(dpp, 20) << __func__ << "::checking object: " << oid << dendl;

      uint64_t size;
      struct timespec tspec;
      if (ctl_ioctx.stat2(oid, &size, &tspec) != 0) {
        ldpp_dout(dpp, 10) << __func__ << "::failed ctl_ioctx.stat( " << oid << " )"
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
      ret = ctl_ioctx.getxattr(oid, SHARD_PROGRESS_ATTR, bl);
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
                                            const md5_stats_t &md5_stats_sum,
                                            Formatter *fmt)
  {
    uint64_t s3_bytes_before = wrk_stats_sum.ingress_obj_bytes;
    uint64_t s3_dedup_bytes  = md5_stats_sum.big_objs_stat.dedup_bytes_estimate;
    uint64_t s3_bytes_after  = s3_bytes_before - s3_dedup_bytes;
    Formatter::ObjectSection section{*fmt, "dedup_ratio_estimate"};
    fmt->dump_unsigned("s3_bytes_before", s3_bytes_before);
    fmt->dump_unsigned("s3_bytes_after", s3_bytes_after);
    fmt->dump_unsigned("dup_head_bytes", md5_stats_sum.dup_head_bytes_estimate);

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
    fmt->dump_unsigned("dup_head_bytes", md5_stats_sum.dup_head_bytes);
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
    dedup_epoch_t epoch;
    int ret = get_epoch(store, dpp, &epoch, nullptr);
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
      int cnt = collect_shard_stats(store, dpp, epoch.time, num_work_shards,
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
      int cnt = collect_shard_stats(store, dpp, epoch.time, num_md5_shards,
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
  int cluster::watch_reload(rgw::sal::RadosStore *store,
                            const DoutPrefixProvider* dpp,
                            uint64_t *p_watch_handle,
                            librados::WatchCtx2 *ctx)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    const std::string & oid = DEDUP_WATCH_OBJ;
    // create the object to watch (object may already exist)
    bool exclusive = true;
    ret = ctl_ioctx.create(oid, exclusive);
    if (ret >= 0) {
      ldpp_dout(dpp, 10) << "dedup_bg::watch_reload():" << oid
                         << " was created!" << dendl;
    }
    else if (ret == -EEXIST) {
      ldpp_dout(dpp, 5) << __func__ << "::"<< oid << " exists" << dendl;
    }
    else {
      ldpp_dout(dpp, 1) << "dedup_bg::watch_reload(): failed ctl_ioctx.create("
                        << oid << ") ret=" << ret << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }

    ret = ctl_ioctx.watch2(oid, p_watch_handle, ctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "dedup_bg::watch_reload(): failed watch2() " << oid
                        << ". error: " << cpp_strerror(-ret) << dendl;
      *p_watch_handle = 0;
      return ret;
    }
    ldpp_dout(dpp, 5) << "dedup_bg::watch_reload(): Started watching "
                      << oid << "::watch_handle=" << *p_watch_handle << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::unwatch_reload(rgw::sal::RadosStore *store,
                              const DoutPrefixProvider* dpp,
                              uint64_t watch_handle)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    ret = ctl_ioctx.unwatch2(watch_handle);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "dedup_bg::unwatch_reload() failed unwatch2() "
                        << DEDUP_WATCH_OBJ << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::ack_notify(rgw::sal::RadosStore *store,
                          const DoutPrefixProvider *dpp,
                          const control_t *p_ctl,
                          uint64_t notify_id,
                          uint64_t cookie,
                          int status)
  {
    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    ldpp_dout(dpp, 5) << __func__ << "::status=" << status << dendl;
    bufferlist reply_bl;
    ceph::encode(status, reply_bl);
    encode(*p_ctl, reply_bl);
    ctl_ioctx.notify_ack(DEDUP_WATCH_OBJ, notify_id, cookie, reply_bl);

    return 0;
  }

  //---------------------------------------------------------------------------
  // command-line called from radosgw-admin.cc
  int cluster::dedup_control(rgw::sal::RadosStore *store,
                             const DoutPrefixProvider *dpp,
                             urgent_msg_t urgent_msg)
  {
    ldpp_dout(dpp, 10) << __func__ << "::dedup_control req = "
                       << get_urgent_msg_names(urgent_msg) << dendl;
    if (urgent_msg != URGENT_MSG_RESUME  &&
        urgent_msg != URGENT_MSG_PASUE   &&
        urgent_msg != URGENT_MSG_RESTART &&
        urgent_msg != URGENT_MSG_ABORT) {
      ldpp_dout(dpp, 1) << __func__ << "::illegal urgent_msg="<< urgent_msg << dendl;
      return -EINVAL;
    }

    librados::IoCtx ctl_ioctx;
    int ret = get_control_ioctx(store, dpp, ctl_ioctx);
    if (unlikely(ret != 0)) {
      return ret;
    }

    // 10 seconds timeout
    const uint64_t timeout_ms = 10*1000;
    bufferlist reply_bl, urgent_msg_bl;
    ceph::encode(urgent_msg, urgent_msg_bl);
    ret = rgw_rados_notify(dpp, ctl_ioctx, DEDUP_WATCH_OBJ, urgent_msg_bl,
                           timeout_ms, &reply_bl, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed rgw_rados_notify("
                        << DEDUP_WATCH_OBJ << ")::err="<<cpp_strerror(-ret) << dendl;
      return ret;
    }
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    ctl_ioctx.decode_notify_response(reply_bl, &acks, &timeouts);
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
    ldpp_dout(dpp, 1) << __func__ << "::dedup_type = " << dedup_type << dendl;

    dedup_epoch_t old_epoch;
    // store the previous epoch for cmp-swap
    int ret = get_epoch(store, dpp, &old_epoch, __func__);
    if (ret != 0) {
      // generate an empty epoch with zero counters
      std::string cluster_id("NULL_CLUSTER_ID");
      ldpp_dout(dpp, 1) << __func__ << "::set empty EPOCH using cluster_id: "
                        << cluster_id << dendl;
      set_epoch(store, cluster_id, dpp, 0, 0);
      ret = get_epoch(store, dpp, &old_epoch, __func__);
      if (ret) {
        return ret;
      }
    }

    // first abort all dedup work!
    ret = dedup_control(store, dpp, URGENT_MSG_ABORT);
    if (ret != 0) {
      return ret;
    }
#if 0
    // then delete dedup-pool to ensure a clean start
    const rgw_pool& dedup_pool = store->svc()->zone->get_zone_params().dedup_pool;
    auto rados_handle = store->getRados()->get_rados_handle();
    ldpp_dout(dpp, 5) <<__func__ << "::delete pool: " << dedup_pool.name << dendl;
    rados_handle->pool_delete(dedup_pool.name.c_str());
#endif

    ldpp_dout(dpp, 10) << __func__ << dedup_type << dendl;
#ifdef FULL_DEDUP_SUPPORT
    ceph_assert(dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE ||
                dedup_type == dedup_req_type_t::DEDUP_TYPE_FULL);
#else
    ceph_assert(dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE);
#endif
    ret = swap_epoch(store, dpp, &old_epoch, dedup_type, 0, 0);
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
    dedup_epoch_t new_epoch;
    if (get_epoch(store, dpp, &new_epoch, nullptr) != 0) {
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
