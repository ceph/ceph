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

#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "rgw_tools.h"
#include "svc_zone.h"
#include "common/config.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_aio_throttle.h"
#include "driver/rados/rgw_bucket.h"
#include "rgw_sal_config.h"
#include "rgw_lib.h"
#include "rgw_placement_types.h"
#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_sal_rados.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "fmt/ranges.h"
#include "osd/osd_types.h"
#include "common/ceph_crypto.h"

#include <filesystem>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <limits>
#include <climits>
#include <cinttypes>
#include <cstring>
#include <span>
#include <mutex>
#include <thread>

//using namespace std::chrono_literals;
using namespace librados;
using namespace std;
using namespace rgw::dedup;

#include "rgw_dedup_remap.h"
#include "rgw_sal_rados.h"
#include "rgw_dedup_table.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup.h"
#include "rgw_dedup_store.h"
#include "rgw_dedup_cluster.h"
#include "rgw_dedup_epoch.h"
#include "rgw_perf_counters.h"
#include "include/ceph_assert.h"

static constexpr auto dout_subsys = ceph_subsys_rgw_dedup;

namespace rgw::dedup {
  static inline constexpr unsigned MAX_STORAGE_CLASS_IDX = 128;
  using storage_class_idx_t = uint8_t;

  //---------------------------------------------------------------------------
  void Background::DedupWatcher::handle_notify(uint64_t notify_id, uint64_t cookie,
                                               uint64_t notifier_id, bufferlist &bl)
  {
    ldpp_dout(parent->dpp, 10) << __func__ << "::notify_id=" << notify_id
                               << "::cookie=" << cookie
                               << "::notifier_id=" << notifier_id << dendl;
    if (parent->d_watch_handle != cookie) {
      ldpp_dout(parent->dpp, 1) << __func__ << "::ERR: wrong cookie=" << cookie
                                << "::d_watch_handle=" << parent->d_watch_handle
                                << dendl;
      return;
    }
    parent->handle_notify(notify_id, cookie, bl);
  }

  //---------------------------------------------------------------------------
  void Background::DedupWatcher::handle_error(uint64_t cookie, int err)
  {
    if (parent->d_watch_handle != cookie) {
      ldpp_dout(parent->dpp, 1) << __func__ << "::ERR: wrong cookie=" << cookie
                                << "::d_watch_handle=" << parent->d_watch_handle
                                << dendl;
      return;
    }
    ldpp_dout(parent->dpp, 1) << __func__ << "::error=" << err << dendl;

    parent->unwatch_reload(parent->dpp);
    parent->watch_reload(parent->dpp);
  }

  //---------------------------------------------------------------------------
  void control_t::reset()
  {
    this->dedup_type         = dedup_req_type_t::DEDUP_TYPE_NONE;
    this->started            = false;
    this->dedup_exec         = false;
    this->shutdown_req       = false;
    this->shutdown_done      = false;
    this->local_pause_req    = false;
    this->local_paused       = false;
    this->remote_abort_req   = false;
    this->remote_aborted     = false;
    this->remote_pause_req   = false;
    this->remote_paused      = false;
    this->remote_restart_req = false;
  }

  //---------------------------------------------------------------------------
  void encode(const control_t& ctl, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(static_cast<int32_t>(ctl.dedup_type), bl);
    encode(ctl.started, bl);
    encode(ctl.dedup_exec, bl);
    encode(ctl.shutdown_req, bl);
    encode(ctl.shutdown_done, bl);
    encode(ctl.local_pause_req, bl);
    encode(ctl.local_paused, bl);
    encode(ctl.remote_abort_req, bl);
    encode(ctl.remote_aborted, bl);
    encode(ctl.remote_pause_req, bl);
    encode(ctl.remote_paused, bl);
    encode(ctl.remote_restart_req, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(control_t& ctl, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    int32_t dedup_type;
    decode(dedup_type, bl);
    ctl.dedup_type = static_cast<dedup_req_type_t> (dedup_type);
    decode(ctl.started, bl);
    decode(ctl.dedup_exec, bl);
    decode(ctl.shutdown_req, bl);
    decode(ctl.shutdown_done, bl);
    decode(ctl.local_pause_req, bl);
    decode(ctl.local_paused, bl);
    decode(ctl.remote_abort_req, bl);
    decode(ctl.remote_aborted, bl);
    decode(ctl.remote_pause_req, bl);
    decode(ctl.remote_paused, bl);
    decode(ctl.remote_restart_req, bl);
    DECODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const control_t &ctl)
  {
    out << ctl.dedup_type;
    if (ctl.started) {
      out << "::started";
    }
    if (ctl.dedup_exec) {
      out << "::dedup_exec";
    }
    if (ctl.shutdown_req) {
      out << "::shutdown_req";
    }
    if (ctl.shutdown_done) {
      out << "::shutdown_done";
    }
    if (ctl.local_pause_req) {
      out << "::local_pause_req";
    }
    if (ctl.local_paused) {
      out << "::local_paused";
    }
    if (ctl.remote_abort_req) {
      out << "::remote_abort_req";
    }
    if (ctl.remote_aborted) {
      out << "::remote_aborted";
    }
    if (ctl.remote_pause_req) {
      out << "::remote_pause_req";
    }
    if (ctl.remote_paused) {
      out << "::remote_paused";
    }
    if (ctl.remote_restart_req) {
      out << "::remote_restart_req";
    }

    return out;
  }

  //===========================================================================
  // rgw::dedup::Background
  //===========================================================================
  //---------------------------------------------------------------------------
  static void display_ioctx_state(const DoutPrefixProvider *dpp,
                                  const librados::IoCtx &ioctx,
                                  const char *caller)
  {
    if (ioctx.is_valid()) {
      ldpp_dout(dpp, 5) << caller << "::valid ioctx, instance_id="
                        << ioctx.get_instance_id() << dendl;
    }
    else {
      ldpp_dout(dpp, 5) << caller << "::invalid ioctx" << dendl;
    }
  }

  //---------------------------------------------------------------------------
  static int safe_pool_delete(rgw::sal::RadosStore     *store,
                              const DoutPrefixProvider *dpp,
                              int64_t                   expected_pool_id)
  {
    const rgw_pool& dedup_pool = store->svc()->zone->get_zone_params().dedup_pool;
    auto rados_handle = store->getRados()->get_rados_handle();
    int64_t pool_id = rados_handle->pool_lookup(dedup_pool.name.c_str());
    if (pool_id < 0) {
      int err = pool_id;
      if (err == ENOENT) {
        ldpp_dout(dpp, 10) <<__func__ << "::pool doesn't exist (probably was removed by other RGW)::"
                           << dedup_pool.name << "::expected_pool_id="
                           << expected_pool_id << dendl;
      }
      else {
        ldpp_dout(dpp, 5) <<__func__ << "::failed pool_lookup(" << dedup_pool.name
                          << ") err=" << cpp_strerror(-err) << dendl;
      }
      return err;
    }

    if (pool_id != expected_pool_id) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: pool_id was changed from: "
                        << expected_pool_id << " to: " << pool_id
                        << " abort pool_delete() request!" << dendl;
      // report Stale file handle
      return -ESTALE;
    }

    ldpp_dout(dpp, 10) <<__func__ << "::calling delete pool(" << dedup_pool.name
                       << ") pool_id=" << pool_id << dendl;
    return rados_handle->pool_delete(dedup_pool.name.c_str());
  }

  //---------------------------------------------------------------------------
  static int64_t create_pool(rgw::sal::RadosStore     *store,
                             const DoutPrefixProvider *dpp,
                             const std::string        &pool_name)
  {
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

    auto rados_handle = store->getRados()->get_rados_handle();
    int ret = rados_handle->mon_command(command, inbl, nullptr, &output);
    if (output.length()) {
      if (output != "pool 'rgw_dedup_pool' already exists") {
        ldpp_dout(dpp, 10) << __func__ << "::" << output << dendl;
      }
    }
    if (ret != 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to create pool "
                        << pool_name << " with: "
                        << cpp_strerror(-ret) << ", ret=" << ret << dendl;
      return ret;
    }
    const rgw_pool& dedup_pool = store->svc()->zone->get_zone_params().dedup_pool;
    return rados_handle->pool_lookup(dedup_pool.name.c_str());
  }

  //---------------------------------------------------------------------------
  static int init_dedup_pool_ioctx(rgw::sal::RadosStore     *store,
                                   const DoutPrefixProvider *dpp,
                                   bool                      create,
                                   librados::IoCtx          &ioctx)
  {
    const rgw_pool& dedup_pool = store->svc()->zone->get_zone_params().dedup_pool;
    std::string pool_name(dedup_pool.name.c_str());
    auto rados_handle = store->getRados()->get_rados_handle();
    int64_t pool_id = rados_handle->pool_lookup(dedup_pool.name.c_str());
    if (pool_id >= 0) {
      // TBD: what to do when create option is passed
      ldpp_dout(dpp, 10) << __func__ << "::pool " << dedup_pool.name
                         << " already exists, pool_id=" << pool_id << dendl;
    }
    else if (create) {
      pool_id = create_pool(store, dpp, pool_name);
      if (pool_id >= 0) {
        ldpp_dout(dpp, 10) << __func__ << "::pool " << dedup_pool.name
                           << " was created, pool_id=" << pool_id << dendl;
      }
      else {
        return pool_id;
      }
    }
    else {
      ldpp_dout(dpp, 1) << __func__
                        << "::ERR: pool doesn't exist and no create option" << dendl;
      return -ENOENT;
    }

    int ret = rgw_init_ioctx(dpp, rados_handle, dedup_pool, ioctx);
    if (unlikely(ret < 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed rgw_init_ioctx() ret=" << ret
                        << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }

    ret = ioctx.application_enable("rgw_dedup", false);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::pool " << dedup_pool.name
                         << " was associated with dedup app" << dendl;
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to enable pool "
                        << dedup_pool.name << " with: "
                        << cpp_strerror(-ret) << ", ret=" << ret << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::init_rados_access_handles(bool init_pool)
  {
    store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      ldpp_dout(dpp, 0) << "ERR: failed dynamic_cast to RadosStore" << dendl;
      // this is the return code used in rgw_bucket.cc
      return -ENOTSUP;
    }

    rados = store->getRados();
    rados_handle = rados->get_rados_handle();
    if (init_pool) {
      int ret = init_dedup_pool_ioctx(store, dpp, true, d_dedup_cluster_ioctx);
      display_ioctx_state(dpp, d_dedup_cluster_ioctx, __func__);
      return ret;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  Background::Background(rgw::sal::Driver* _driver, CephContext* _cct) :
    driver(_driver),
    dp(_cct, dout_subsys, "dedup background: "),
    dpp(&dp),
    cct(_cct),
    d_cluster(dpp, cct, driver),
    d_watcher_ctx(this)
  {
    d_min_obj_size_for_dedup = cct->_conf->rgw_max_chunk_size;
    d_head_object_size = cct->_conf->rgw_max_chunk_size;
    //ceph_assert(4*1024*1024 == d_head_object_size);

    int ret = init_rados_access_handles(false);
    if (ret != 0) {
      derr << __func__ << "::ERR: failed init_rados_access_handles() ret="
           << ret << "::" << cpp_strerror(-ret) << dendl;
      throw std::runtime_error("Failed init_rados_access_handles()");
    }

    d_heart_beat_last_update = ceph_clock_now();
    d_heart_beat_max_elapsed_sec = 3;
  }

  //---------------------------------------------------------------------------
  int Background::add_disk_rec_from_bucket_idx(disk_block_array_t     &disk_arr,
                                               const rgw::sal::Bucket *p_bucket,
                                               const parsed_etag_t    *p_parsed_etag,
                                               const std::string      &obj_name,
                                               uint64_t                obj_size,
                                               const std::string      &storage_class)
  {
    disk_record_t rec(p_bucket, obj_name, p_parsed_etag, obj_size, storage_class);
    // First pass using only ETAG and size taken from bucket-index
    rec.s.flags.set_fastlane();

    auto p_disk = disk_arr.get_shard_block_seq(p_parsed_etag->md5_low);
    disk_block_seq_t::record_info_t rec_info;
    int ret = p_disk->add_record(d_dedup_cluster_ioctx, &rec, &rec_info);
    if (unlikely(ret != 0)) {
      return ret;
    }
    ldpp_dout(dpp, 20) << __func__ << "::" << p_bucket->get_name() << "/"
                       << obj_name << " was written to block_idx="
                       << rec_info.block_id << " rec_id=" << rec_info.rec_id << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::add_record_to_dedup_table(dedup_table_t *p_table,
                                            const disk_record_t *p_rec,
                                            disk_block_id_t block_id,
                                            record_id_t rec_id,
                                            md5_stats_t *p_stats,
                                            remapper_t *remapper)
  {
    uint32_t size_4k_units = byte_size_to_disk_blocks(p_rec->s.obj_bytes_size);
    storage_class_idx_t sc_idx = remapper->remap(p_rec->stor_class, dpp,
                                                 &p_stats->failed_map_overflow);
    if (unlikely(sc_idx == remapper_t::NULL_IDX)) {
      // TBD: need stat counters
      return -EOVERFLOW;
    }
    key_t key(p_rec->s.md5_high, p_rec->s.md5_low, size_4k_units,
              p_rec->s.num_parts, sc_idx);
    bool has_shared_manifest = p_rec->has_shared_manifest();
    ldpp_dout(dpp, 20) << __func__ << "::bucket=" << p_rec->bucket_name
                       << ", obj=" << p_rec->obj_name << ", block_id="
                       << (uint32_t)block_id << ", rec_id=" << (uint32_t)rec_id
                       << ", shared_manifest=" << has_shared_manifest
                       << "::num_parts=" << p_rec->s.num_parts
                       << "::size_4k_units=" << key.size_4k_units
                       << "::ETAG=" << std::hex << p_rec->s.md5_high
                       << p_rec->s.md5_low << std::dec << dendl;

    int ret = p_table->add_entry(&key, block_id, rec_id, has_shared_manifest);
    if (ret == 0) {
      p_stats->loaded_objects ++;
      ldpp_dout(dpp, 20) << __func__ << "::" << p_rec->bucket_name << "/"
                         << p_rec->obj_name << " was added successfully to table"
                         << "::loaded_objects=" << p_stats->loaded_objects << dendl;
    }
    else {
      // We allocate memory for the dedup on startup based on the existing obj count
      // If the system grew significantly since that point we won't be able to
      // accommodate all the objects in the hash-table.
      // Please keep in mind that it is very unlikely since duplicates objects will
      // consume a single entry and since we skip small objects so in reality
      // I expect the allocation to be more than sufficient.
      //
      // However, if we filled up the system there is still value is continuing
      // with this process since we might find duplicates to existing object (which
      // don't take extra space)

      int level = 15;
      if (p_stats->failed_table_load % 0x10000 == 0) {
        level = 5;
      }
      else if (p_stats->failed_table_load % 0x100 == 0) {
        level = 10;
      }
      ldpp_dout(dpp, level) << __func__ << "::Failed p_table->add_entry (overflow)"
                            << "::loaded_objects=" << p_stats->loaded_objects
                            << "::failed_table_load=" << p_stats->failed_table_load
                            << dendl;

      p_stats->failed_table_load++;
    }
    return ret;
  }

#ifdef FULL_DEDUP_SUPPORT

  static constexpr uint64_t cost = 1; // 1 throttle unit per request
  static constexpr uint64_t id = 0; // ids unused
  //---------------------------------------------------------------------------
  [[maybe_unused]]static void show_ref_tags(const DoutPrefixProvider* dpp, std::string &oid, rgw_rados_ref &obj)
  {
    unsigned idx = 0;
    std::list<std::string> refs;
    std::string wildcard_tag;
    int ret = cls_refcount_read(obj.ioctx, oid, &refs, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "::ERR: manifest::failed cls_refcount_read()"
                        << " idx=" << idx << dendl;
      return;
    }

    for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
      ldpp_dout(dpp, 20) << __func__ << "::manifest::" << oid << "::" << idx
                         << "::TAG=" << *iter << dendl;
    }
  }

  //---------------------------------------------------------------------------
  int Background::free_tail_objs_by_manifest(const string   &ref_tag,
                                             const string   &oid,
                                             RGWObjManifest &tgt_manifest)
  {
    unsigned idx = 0;
    for (auto p = tgt_manifest.obj_begin(dpp); p != tgt_manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      if (oid == raw_obj.oid) {
        ldpp_dout(dpp, 20) << __func__ << "::[" << idx <<"] Skip HEAD OBJ: " << raw_obj.oid << dendl;
        continue;
      }

      rgw_rados_ref obj;
      int ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << "ERR: manifest::failed to open context "
                          << obj << dendl;
        continue;
      }
      librados::IoCtx ioctx = obj.ioctx;
      ldpp_dout(dpp, 20) << __func__ << "::removing tail object: " << raw_obj.oid
                         << dendl;
      ret = ioctx.remove(raw_obj.oid);
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::rollback_ref_by_manifest(const string   &ref_tag,
                                           const string   &oid,
                                           RGWObjManifest &manifest)
  {
    unsigned idx = 0;
    int ret_code = 0;
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, null_yield);
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      if (oid == raw_obj.oid) {
        ldpp_dout(dpp, 20) << __func__ << "::[" << idx <<"] Skip HEAD OBJ: "
                           << raw_obj.oid << dendl;
        continue;
      }

      rgw_rados_ref obj;
      int local_ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (local_ret < 0) {
        ret_code = local_ret;
        ldpp_dout(dpp, 1) << __func__ << "::ERR: manifest::failed to open context "
                          << obj << dendl;
        // skip bad objects, nothing we can do
        continue;
      }

      ObjectWriteOperation op;
      cls_refcount_put(op, ref_tag, true);
      rgw::AioResultList completed = aio->get(obj.obj,
                                              rgw::Aio::librados_op(obj.ioctx, std::move(op), null_yield),
                                              cost, id);
    }
    rgw::AioResultList completed = aio->drain();
    return ret_code;
  }

  //---------------------------------------------------------------------------
  int Background::inc_ref_count_by_manifest(const string   &ref_tag,
                                            const string   &oid,
                                            RGWObjManifest &manifest)
  {
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, null_yield);
    rgw::AioResultList all_results;
    int ret = 0;
    unsigned idx = 0;
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      if (oid == raw_obj.oid) {
        ldpp_dout(dpp, 20) << __func__ << "::[" << idx <<"] Skip HEAD OBJ: " << raw_obj.oid << dendl;
        continue;
      }

      rgw_rados_ref obj;
      ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: manifest::failed to open context "
                          << obj << dendl;
        break;
      }

      ObjectWriteOperation op;
      cls_refcount_get(op, ref_tag, true);
      ldpp_dout(dpp, 20) << __func__ << "::inc ref-count on tail object: " << raw_obj.oid << dendl;
      rgw::AioResultList completed = aio->get(obj.obj,
                                              rgw::Aio::librados_op(obj.ioctx, std::move(op), null_yield),
                                              cost, id);
      ret = rgw::check_for_errors(completed);
      all_results.splice(all_results.end(), completed);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << "::ERROR: failed to copy obj=" << obj
                          << ", the error code = " << ret << dendl;
        break;
      }
    }

    if (ret == 0) {
      rgw::AioResultList completed = aio->drain();
      int ret = rgw::check_for_errors(completed);
      all_results.splice(all_results.end(), completed);
      if (ret == 0) {
        return 0;
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: manifest: failed to drain ios ret="
                          << ret <<dendl;
      }
    }

    // if arrived here we failed somewhere -> rollback all ref-inc operations
    /* wait all pending op done */
    rgw::AioResultList completed = aio->drain();
    all_results.splice(all_results.end(), completed);
    int ret2 = 0;
    for (auto& aio_res : all_results) {
      if (aio_res.result < 0) {
        continue; // skip errors
      }
      rgw_rados_ref obj;
      ret2 = rgw_get_rados_ref(dpp, rados_handle, aio_res.obj, &obj);
      if (ret2 < 0) {
        continue;
      }

      ObjectWriteOperation op;
      cls_refcount_put(op, ref_tag, true);
      rgw::AioResultList completed = aio->get(obj.obj,
                                              rgw::Aio::librados_op(obj.ioctx, std::move(op), null_yield),
                                              cost, id);
      ret2 = rgw::check_for_errors(completed);
      if (ret2 < 0) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: cleanup after error failed to drop reference on obj=" << aio_res.obj << dendl;
      }
    }
    completed = aio->drain();
    ret2 = rgw::check_for_errors(completed);
    if (ret2 < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to drain rollback ios, ret="
                        << ret2 <<dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  static int get_ioctx(const DoutPrefixProvider* const dpp,
                       rgw::sal::Driver* driver,
                       RGWRados* rados,
                       const disk_record_t *p_rec,
                       librados::IoCtx *p_ioctx,
                       std::string *oid)
  {
    unique_ptr<rgw::sal::Bucket> bucket;
    {
      rgw_bucket b{p_rec->tenant_name, p_rec->bucket_name, p_rec->bucket_id};
      int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
      if (unlikely(ret != 0)) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: driver->load_bucket(): "
                          << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }

    build_oid(p_rec->bucket_id, p_rec->obj_name, oid);
    //ldpp_dout(dpp, 0) << __func__ << "::OID=" << oid << " || bucket_id=" << bucket_id << dendl;
    rgw_pool data_pool;
    rgw_obj obj{bucket->get_key(), *oid};
    if (!rados->get_obj_data_pool(bucket->get_placement_rule(), obj, &data_pool)) {
      ldpp_dout(dpp, 1) << __func__ << "::failed to get data pool for bucket "
                        << bucket->get_name()  << dendl;
      return -EIO;
    }
    int ret = rgw_init_ioctx(dpp, rados->get_rados_handle(), data_pool, *p_ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to get ioctx from data pool:"
                        << data_pool.to_str() << dendl;
      return -EIO;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  static void init_cmp_pairs(const disk_record_t *p_rec,
                             const bufferlist    &etag_bl,
                             bufferlist          &sha256_bl, // OUT PARAM
                             librados::ObjectWriteOperation *p_op)
  {
    p_op->cmpxattr(RGW_ATTR_ETAG, CEPH_OSD_CMPXATTR_OP_EQ, etag_bl);
    // TBD: do we really need the secondary compare using the full manifest?
    // Can replace it with something cheaper like size/version?
    p_op->cmpxattr(RGW_ATTR_MANIFEST, CEPH_OSD_CMPXATTR_OP_EQ, p_rec->manifest_bl);

    // SHA has 256 bit splitted into multiple 64bit units
    const unsigned units = (256 / (sizeof(uint64_t)*8));
    static_assert(units == 4);
    for (unsigned i = 0; i < units; i++) {
      ceph::encode(p_rec->s.sha256[i], sha256_bl);
    }

    if (!p_rec->s.flags.sha256_calculated()) {
      p_op->cmpxattr(RGW_ATTR_SHA256, CEPH_OSD_CMPXATTR_OP_EQ, sha256_bl);
    }
  }

  //---------------------------------------------------------------------------
  int Background::dedup_object(const disk_record_t *p_src_rec,
                               const disk_record_t *p_tgt_rec,
                               md5_stats_t         *p_stats,
                               bool                 has_shared_manifest_src)
  {
    RGWObjManifest src_manifest;
    try {
      auto bl_iter = p_src_rec->manifest_bl.cbegin();
      decode(src_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: bad src manifest" << dendl;
      return -EINVAL;
    }
    RGWObjManifest tgt_manifest;
    try {
      auto bl_iter = p_tgt_rec->manifest_bl.cbegin();
      decode(tgt_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: bad tgt manifest" << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 20) << __func__ << "::DEDUP From: "
                       << p_src_rec->bucket_name << "/" << p_src_rec->obj_name << " -> "
                       << p_tgt_rec->bucket_name << "/" << p_tgt_rec->obj_name << dendl;

    bufferlist etag_bl;
    etag_to_bufferlist(p_tgt_rec->s.md5_high, p_tgt_rec->s.md5_low, p_tgt_rec->s.num_parts, &etag_bl);
    ldpp_dout(dpp, 20) << __func__ << "::num_parts=" << p_tgt_rec->s.num_parts
                       << "::ETAG=" << etag_bl.to_str() << dendl;

    bufferlist hash_bl, manifest_hash_bl, tgt_sha256_bl;
    crypto::digest<crypto::SHA1>(p_src_rec->manifest_bl).encode(hash_bl);
    // Use a shorter hash (64bit instead of 160bit)
    hash_bl.splice(0, 8, &manifest_hash_bl);
    librados::ObjectWriteOperation tgt_op;
    init_cmp_pairs(p_tgt_rec, etag_bl, tgt_sha256_bl, &tgt_op);
    tgt_op.setxattr(RGW_ATTR_SHARE_MANIFEST, manifest_hash_bl);
    tgt_op.setxattr(RGW_ATTR_MANIFEST, p_src_rec->manifest_bl);
    if (p_tgt_rec->s.flags.sha256_calculated()) {
      tgt_op.setxattr(RGW_ATTR_SHA256, tgt_sha256_bl);
      p_stats->set_sha256_attrs++;
    }

    std::string src_oid, tgt_oid;
    librados::IoCtx src_ioctx, tgt_ioctx;
    int ret1 = get_ioctx(dpp, driver, rados, p_src_rec, &src_ioctx, &src_oid);
    int ret2 = get_ioctx(dpp, driver, rados, p_tgt_rec, &tgt_ioctx, &tgt_oid);
    if (unlikely(ret1 != 0 || ret2 != 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed get_ioctx()" << dendl;
      return (ret1 ? ret1 : ret2);
    }

    // TBD: Do we need to remove target RGW_ATTR_TAIL_TAG??
    string ref_tag = p_tgt_rec->ref_tag;
    ldpp_dout(dpp, 20) << __func__ << "::ref_tag=" << ref_tag << dendl;
    int ret = inc_ref_count_by_manifest(ref_tag, src_oid, src_manifest);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << __func__ << "::send TGT CLS (Shared_Manifest)" << dendl;
      ret = tgt_ioctx.operate(tgt_oid, &tgt_op);
      if (unlikely(ret != 0)) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: failed tgt_ioctx.operate("
                          << tgt_oid << "), err is " << cpp_strerror(-ret) << dendl;
        rollback_ref_by_manifest(ref_tag, src_oid, src_manifest);
        return ret;
      }

      // free tail objects based on TGT manifest
      free_tail_objs_by_manifest(ref_tag, tgt_oid, tgt_manifest);

      if (!has_shared_manifest_src) {
        // When SRC OBJ A has two or more dups (B, C) we set SHARED_MANIFEST
        // after deduping B and update it in dedup_table, but don't update the
        // disk-record (as require an expensive random-disk-write).
        // When deduping C we can trust the shared_manifest state in the table and
        // skip a redundant update to SRC object attribute
        bufferlist src_sha256_bl;
        librados::ObjectWriteOperation src_op;
        init_cmp_pairs(p_src_rec, etag_bl, src_sha256_bl, &src_op);
        src_op.setxattr(RGW_ATTR_SHARE_MANIFEST, manifest_hash_bl);
        if (p_src_rec->s.flags.sha256_calculated()) {
          src_op.setxattr(RGW_ATTR_SHA256, src_sha256_bl);
          p_stats->set_sha256_attrs++;
        }

        ldpp_dout(dpp, 20) << __func__ <<"::send SRC CLS (Shared_Manifest)"<< dendl;
        ret = src_ioctx.operate(src_oid, &src_op);
        if (unlikely(ret != 0)) {
          ldpp_dout(dpp, 1) << __func__ << "::ERR: failed src_ioctx.operate("
                            << src_oid << "), err is " << cpp_strerror(-ret)<<dendl;
          return ret;
        }
      }
    }

    // do we need to set compression on the head object or is it set on tail?
    // RGW_ATTR_COMPRESSION
    return ret;
  }

  using ceph::crypto::SHA256;
  //---------------------------------------------------------------------------
  int Background::calc_object_sha256(const disk_record_t *p_rec, uint8_t *p_sha256)
  {
    ldpp_dout(dpp, 20) << __func__ << "::p_rec->obj_name=" << p_rec->obj_name << dendl;
    // Open questions -
    // 1) do we need the secret if so what is the correct one to use?
    // 2) are we passing the head/tail objects in the correct order?
    RGWObjManifest manifest;
    try {
      auto bl_iter = p_rec->manifest_bl.cbegin();
      decode(manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: bad src manifest" << dendl;
      return -EINVAL;
    }
    std::string oid;
    build_oid(p_rec->bucket_id, p_rec->obj_name, &oid);
    librados::IoCtx head_ioctx;
    const char *secret = "0555b35654ad1656d804f1b017cd26e9";
    TOPNSPC::crypto::HMACSHA256 hmac((const uint8_t*)secret, strlen(secret));
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      rgw_rados_ref obj;
      int ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << "::failed rgw_get_rados_ref() for raw_obj="
                          << raw_obj << dendl;
        return ret;
      }

      if (oid == raw_obj.oid) {
        ldpp_dout(dpp, 20) << __func__ << "::manifest: head object=" << oid << dendl;
        head_ioctx = obj.ioctx;
      }
      bufferlist bl;
      librados::IoCtx ioctx = obj.ioctx;
      // read full object
      ret = ioctx.read(raw_obj.oid, bl, 0, 0);
      if (ret > 0) {
        for (const auto& bptr : bl.buffers()) {
          hmac.Update((const unsigned char *)bptr.c_str(), bptr.length());
        }
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to read " << oid
                          << ", error is " << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }
    hmac.Final(p_sha256);
    return 0;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void __attribute__ ((noinline))
  print_record(const DoutPrefixProvider* dpp,
               const disk_record_t *p_tgt_rec,
               disk_block_id_t      block_id,
               record_id_t          rec_id,
               md5_shard_t          md5_shard)
  {
    ldpp_dout(dpp, 20) << __func__ << "::bucket=" << p_tgt_rec->bucket_name
                       << ", obj=" << p_tgt_rec->obj_name
                       << ", block_id=" << block_id
                       << ", rec_id=" << (int)rec_id
                       << ", md5_shard=" << (int)md5_shard << dendl;

    ldpp_dout(dpp, 20) << __func__ << "::md5_shard=" << (int)md5_shard
                       << "::" << p_tgt_rec->bucket_name
                       << "/" << p_tgt_rec->obj_name
                       << "::num_parts=" << p_tgt_rec->s.num_parts
                       << "::ETAG=" << std::hex << p_tgt_rec->s.md5_high
                       << p_tgt_rec->s.md5_low << std::dec << dendl;
  }

  //---------------------------------------------------------------------------
  int Background::add_obj_attrs_to_record(rgw_bucket            *p_rb,
                                          disk_record_t         *p_rec,
                                          const rgw::sal::Attrs &attrs,
                                          dedup_table_t         *p_table,
                                          md5_stats_t           *p_stats) /*IN-OUT*/
  {
    // if TAIL_TAG exists -> use it as ref-tag, eitherwise take ID_TAG
    auto itr = attrs.find(RGW_ATTR_TAIL_TAG);
    if (itr != attrs.end()) {
      p_rec->ref_tag = itr->second.to_str();
    }
    else {
      itr = attrs.find(RGW_ATTR_ID_TAG);
      if (itr != attrs.end()) {
        p_rec->ref_tag = itr->second.to_str();
      }
      else {
        ldpp_dout(dpp, 5) << __func__ << "::No TAIL_TAG and no ID_TAG" << dendl;
        return -EINVAL;
      }
    }
    p_rec->s.ref_tag_len = p_rec->ref_tag.length();

    // clear bufferlist first
    p_rec->manifest_bl.clear();

    itr = attrs.find(RGW_ATTR_MANIFEST);
    if (itr != attrs.end()) {
      const bufferlist &bl = itr->second;
      RGWObjManifest manifest;
      try {
        auto bl_iter = bl.cbegin();
        decode(manifest, bl_iter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 1)  << __func__
                           << "::ERROR: unable to decode manifest" << dendl;
        return -EINVAL;
      }

      // force explicit tail_placement as the dedup could be on another bucket
      const rgw_bucket_placement& tail_placement = manifest.get_tail_placement();
      if (tail_placement.bucket.name.empty()) {
        ldpp_dout(dpp, 20) << __func__ << "dedup::updating tail placement" << dendl;
        manifest.set_tail_placement(tail_placement.placement_rule, *p_rb);
        encode(manifest, p_rec->manifest_bl);
      }
      else {
        p_rec->manifest_bl = bl;
      }
      p_rec->s.manifest_len = p_rec->manifest_bl.length();
    }
    else {
      ldpp_dout(dpp, 5)  << __func__ << "::ERROR: no manifest" << dendl;
      return -EINVAL;
    }

    itr = attrs.find(RGW_ATTR_SHARE_MANIFEST);
    if (itr != attrs.end()) {
      uint64_t hash = 0;
      try {
        auto bl_iter = itr->second.cbegin();
        ceph::decode(hash, bl_iter);
        p_rec->s.shared_manifest = hash;
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 1) << __func__ << "::ERROR: bad shared_manifest" << dendl;
        return -EINVAL;
      }
      ldpp_dout(dpp, 20) << __func__ << "::Set Shared_Manifest::OBJ_NAME="
                         << p_rec->obj_name << "::shared_manifest=0x" << std::hex
                         << p_rec->s.shared_manifest << std::dec << dendl;
      p_rec->s.flags.set_shared_manifest();
    }
    else {
      memset(&p_rec->s.shared_manifest, 0, sizeof(p_rec->s.shared_manifest));
    }

    itr = attrs.find(RGW_ATTR_SHA256);
    if (itr != attrs.end()) {
      try {
        auto bl_iter = itr->second.cbegin();
        // SHA has 256 bit splitted into multiple 64bit units
        const unsigned units = (256 / (sizeof(uint64_t)*8));
        static_assert(units == 4);
        for (unsigned i = 0; i < units; i++) {
          uint64_t val;
          ceph::decode(val, bl_iter);
          p_rec->s.sha256[i] = val;
        }
        p_stats->valid_sha256_attrs++;
        return 0;
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: failed SHA256 decode" << dendl;
        return -EINVAL;
      }
    }

    p_stats->invalid_sha256_attrs++;
    // TBD: redundant memset...
    memset(p_rec->s.sha256, 0, sizeof(p_rec->s.sha256));
    // CEPH_CRYPTO_HMACSHA256_DIGESTSIZE is 32 Bytes (32*8=256)
    int ret = calc_object_sha256(p_rec, (uint8_t*)p_rec->s.sha256);
    if (ret == 0) {
      p_rec->s.flags.set_sha256_calculated();
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  // We purged all entries not marked for-dedup (i.e. singleton bit is set) from the table
  //   so all entries left are sources of dedup with multiple copies.
  // Need to read attributes from the Head-Object and output them to a new SLAB
  int Background::read_object_attribute(dedup_table_t    *p_table,
                                        disk_record_t    *p_rec,
                                        disk_block_id_t   old_block_id,
                                        record_id_t       old_rec_id,
                                        md5_shard_t       md5_shard,
                                        md5_stats_t      *p_stats /* IN-OUT */,
                                        disk_block_seq_t *p_disk,
                                        remapper_t       *remapper)
  {
    bool should_print_debug = cct->_conf->subsys.should_gather<ceph_subsys_rgw_dedup, 20>();
    if (unlikely(should_print_debug)) {
      print_record(dpp, p_rec, old_block_id, old_rec_id, md5_shard);
    }
    p_stats->processed_objects ++;

    uint32_t size_4k_units = byte_size_to_disk_blocks(p_rec->s.obj_bytes_size);
    uint64_t ondisk_byte_size = disk_blocks_to_byte_size(size_4k_units);
    storage_class_idx_t sc_idx = remapper->remap(p_rec->stor_class, dpp,
                                                 &p_stats->failed_map_overflow);
    if (unlikely(sc_idx == remapper_t::NULL_IDX)) {
      // TBD: need stat counters
      return -EOVERFLOW;
    }
    key_t key_from_bucket_index(p_rec->s.md5_high, p_rec->s.md5_low, size_4k_units,
                                p_rec->s.num_parts, sc_idx);
    dedup_table_t::value_t src_val;
    int ret = p_table->get_val(&key_from_bucket_index, &src_val);
    if (ret != 0) {
      if (ondisk_byte_size <= d_min_obj_size_for_dedup && p_rec->s.num_parts == 0) {
        // record has no valid entry in table because it is a too small
        // It was loaded to table for calculation and then purged
        p_stats->skipped_purged_small++;
        ldpp_dout(dpp, 20) << __func__ << "::skipped purged small obj::"
                           << p_rec->obj_name << "::" << ondisk_byte_size << dendl;
        // help small object tests pass - avoid complication differentiating between
        // small objects ( < 64KB,  >= 64KB <= 4MB, > 4MB
        p_stats->processed_objects--;
      }
      else {
        // record has no valid entry in table because it is a singleton
        p_stats->skipped_singleton++;
        p_stats->skipped_singleton_bytes += ondisk_byte_size;
        ldpp_dout(dpp, 20) << __func__ << "::skipped singleton::"
                           << p_rec->obj_name << std::dec << dendl;
      }
      return 0;
    }

    // Every object after this point was counted as a dedup potential
    // If we conclude that it can't be dedup it should be accounted for
    rgw_bucket b{p_rec->tenant_name, p_rec->bucket_name, p_rec->bucket_id};
    unique_ptr<rgw::sal::Bucket> bucket;
    ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      // could happen when the bucket is removed between passes
      p_stats->ingress_failed_load_bucket++;
      ldpp_dout(dpp, 15) << __func__ << "::Failed driver->load_bucket(): "
                         << cpp_strerror(-ret) << dendl;
      return 0;
    }

    unique_ptr<rgw::sal::Object> p_obj = bucket->get_object(p_rec->obj_name);
    if (unlikely(!p_obj)) {
      // could happen when the object is removed between passes
      p_stats->ingress_failed_get_object++;
      ldpp_dout(dpp, 15) << __func__ << "::Failed bucket->get_object("
                         << p_rec->obj_name << ")" << dendl;
      return 0;
    }

    ret = p_obj->get_obj_attrs(null_yield, dpp);
    if (unlikely(ret < 0)) {
      p_stats->ingress_failed_get_obj_attrs++;
      ldpp_dout(dpp, 10) << __func__ << "::ERR: failed to stat object(" << p_rec->obj_name
                         << "), returned error: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    const rgw::sal::Attrs& attrs = p_obj->get_attrs();
    if (attrs.find(RGW_ATTR_CRYPT_MODE) != attrs.end()) {
      p_stats->ingress_skip_encrypted++;
      p_stats->ingress_skip_encrypted_bytes += ondisk_byte_size;
      ldpp_dout(dpp, 20) <<__func__ << "::Skipping encrypted object "
                         << p_rec->obj_name << dendl;
      return 0;
    }

    // TBD: We should be able to support RGW_ATTR_COMPRESSION when all copies are compressed
    if (attrs.find(RGW_ATTR_COMPRESSION) != attrs.end()) {
      p_stats->ingress_skip_compressed++;
      p_stats->ingress_skip_compressed_bytes += ondisk_byte_size;
      ldpp_dout(dpp, 20) <<__func__ << "::Skipping compressed object "
                         << p_rec->obj_name << dendl;
      return 0;
    }

    // extract ETAG and Size and compare with values taken from the bucket-index
    parsed_etag_t parsed_etag;
    auto itr = attrs.find(RGW_ATTR_ETAG);
    if (itr != attrs.end()) {
      if (unlikely(!parse_etag_string(itr->second.to_str(), &parsed_etag))) {
        p_stats->ingress_corrupted_etag++;
        ldpp_dout(dpp, 10) << __func__ << "::ERROR: corrupted etag::" << p_rec->obj_name << dendl;
        return -EINVAL;
      }
    }
    else {
      p_stats->ingress_corrupted_etag++;
      ldpp_dout(dpp, 10)  << __func__ << "::ERROR: no etag" << p_rec->obj_name << dendl;
      return -EINVAL;
    }

    std::string storage_class;
    itr = attrs.find(RGW_ATTR_STORAGE_CLASS);
    if (itr != attrs.end()) {
      storage_class = itr->second.to_str();
    }
    else {
      storage_class = RGW_STORAGE_CLASS_STANDARD;
    }
    // no need to check for remap success as we compare keys bellow
    sc_idx = remapper->remap(storage_class, dpp, &p_stats->failed_map_overflow);
    key_t key_from_obj(parsed_etag.md5_high, parsed_etag.md5_low,
                       byte_size_to_disk_blocks(p_obj->get_size()),
                       parsed_etag.num_parts, sc_idx);
    if (unlikely(key_from_obj != key_from_bucket_index ||
                 p_rec->s.obj_bytes_size != p_obj->get_size())) {
      ldpp_dout(dpp, 15) <<__func__ << "::Skipping changed object "
                         << p_rec->obj_name << dendl;
      p_stats->ingress_skip_changed_objs++;
      return 0;
    }

    // reset flags
    p_rec->s.flags.clear();
    ret = add_obj_attrs_to_record(&b, p_rec, attrs, p_table, p_stats);
    if (unlikely(ret != 0)) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed add_obj_attrs_to_record() ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }

    disk_block_seq_t::record_info_t rec_info;
    ret = p_disk->add_record(d_dedup_cluster_ioctx, p_rec, &rec_info);
    if (ret == 0) {
      // set the disk_block_id_t to this unless the existing disk_block_id is marked as shared-manifest
      ceph_assert(rec_info.rec_id < MAX_REC_IN_BLOCK);
      ldpp_dout(dpp, 20)  << __func__ << "::" << p_rec->bucket_name << "/"
                          << p_rec->obj_name << " was written to block_idx="
                          << rec_info.block_id << "::rec_id=" << (int)rec_info.rec_id
                          << "::shared_manifest=" << p_rec->has_shared_manifest() << dendl;
      p_table->update_entry(&key_from_bucket_index, rec_info.block_id,
                            rec_info.rec_id, p_rec->has_shared_manifest());
    }
    else {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: Failed p_disk->add_record()"<< dendl;
      if (ret == -EINVAL) {
        p_stats->ingress_corrupted_obj_attrs++;
      }
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  static int write_sha256_object_attribute(const DoutPrefixProvider* const dpp,
                                           rgw::sal::Driver* driver,
                                           RGWRados* rados,
                                           const disk_record_t *p_rec)
  {
    bufferlist etag_bl;
    bufferlist sha256_bl;
    librados::ObjectWriteOperation op;
    etag_to_bufferlist(p_rec->s.md5_high, p_rec->s.md5_low, p_rec->s.num_parts,
                       &etag_bl);
    init_cmp_pairs(p_rec, etag_bl, sha256_bl /*OUT PARAM*/, &op);
    op.setxattr(RGW_ATTR_SHA256, sha256_bl);

    std::string oid;
    librados::IoCtx ioctx;
    int ret = get_ioctx(dpp, driver, rados, p_rec, &ioctx, &oid);
    if (unlikely(ret != 0)) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed get_ioctx()" << dendl;
      return ret;
    }

    ret = ioctx.operate(oid, &op);
    if (unlikely(ret != 0)) {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed ioctx.operate("
                        << oid << "), err is " << cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  // We purged all entries not marked for-dedup (i.e. singleton bit is set) from the table
  //   so all entries left are sources of dedup with multiple copies.
  // If the record is marked as Shared-Manifest-Object -> skip it
  // if the record's key doesn’t exist in table -> skip it (it is a singleton and it was purged)
  // If the record block-index matches the hashtable entry -> skip it (it is the SRC object)
  // All other entries are Dedicated-Manifest-Objects with a valid SRC object

  // we can withstand most errors moving to the next object
  // only report an error if we recived a stop scan request!
  //
  int Background::try_deduping_record(dedup_table_t       *p_table,
                                      const disk_record_t *p_tgt_rec,
                                      disk_block_id_t      block_id,
                                      record_id_t          rec_id,
                                      md5_shard_t          md5_shard,
                                      md5_stats_t         *p_stats, /* IN-OUT */
                                      remapper_t          *remapper)
  {
    bool should_print_debug = cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>();
    if (unlikely(should_print_debug)) {
      print_record(dpp, p_tgt_rec, block_id, rec_id, md5_shard);
    }

    uint32_t size_4k_units = byte_size_to_disk_blocks(p_tgt_rec->s.obj_bytes_size);
    storage_class_idx_t sc_idx = remapper->remap(p_tgt_rec->stor_class, dpp,
                                                 &p_stats->failed_map_overflow);
    ceph_assert(sc_idx != remapper_t::NULL_IDX);
    key_t key(p_tgt_rec->s.md5_high, p_tgt_rec->s.md5_low, size_4k_units,
              p_tgt_rec->s.num_parts, sc_idx);
    dedup_table_t::value_t src_val;
    int ret = p_table->get_val(&key, &src_val);
    if (ret != 0) {
      // record has no valid entry in table because it is a singleton
      // should never happened since we purged all singletons before
      ldpp_dout(dpp, 5) << __func__ << "::skipped singleton::" << p_tgt_rec->bucket_name
                        << "/" << p_tgt_rec->obj_name << "::num_parts=" << p_tgt_rec->s.num_parts
                        << "::ETAG=" << std::hex << p_tgt_rec->s.md5_high
                        << p_tgt_rec->s.md5_low << std::dec << dendl;
      ceph_abort("Unexpcted singleton");
      return 0;
    }

    disk_block_id_t src_block_id = src_val.block_idx;
    record_id_t src_rec_id = src_val.rec_id;
    if (block_id == src_block_id && rec_id == src_rec_id) {
      // the table entry point to this record which means it is a dedup source so nothing to do
      p_stats->skipped_source_record++;
      ldpp_dout(dpp, 20) << __func__ << "::skipped source-record" << dendl;
      return 0;
    }

    // ceph store full blocks so need to round up and multiply by block_size
    uint64_t ondisk_byte_size = disk_blocks_to_byte_size(size_4k_units);
    uint64_t dedupable_objects_bytes = calc_deduped_bytes(d_head_object_size,
                                                          p_tgt_rec->s.num_parts,
                                                          ondisk_byte_size);
    if (p_tgt_rec->s.flags.has_shared_manifest()) {
      // record holds a shared_manifest object so can't be a dedup target
      p_stats->skipped_shared_manifest++;
      p_stats->shared_manifest_dedup_bytes += dedupable_objects_bytes;
      ldpp_dout(dpp, 20) << __func__ << "::skipped shared_manifest" << dendl;
      return 0;
    }

    // This records is a dedup target with source record on source_block_id
    disk_record_t src_rec;
    ret = load_record(d_dedup_cluster_ioctx, p_tgt_rec, &src_rec, src_block_id,
                      src_rec_id, md5_shard, dpp);
    if (unlikely(ret != 0)) {
      p_stats->failed_src_load++;
      // we can withstand most errors moving to the next object
      ldpp_dout(dpp, 5) << __func__ << "::ERR: Failed load_record("
                        << src_block_id << ", " << src_rec_id << ")" << dendl;
      return 0;
    }

    ldpp_dout(dpp, 20) << __func__ << "::SRC=" << src_rec.bucket_name
                       << "/" << src_rec.obj_name << dendl;
    // verify that SRC and TGT records don't refer to the same physical object
    // This could happen in theory if we read the same objects twice
    if (src_rec.obj_name == p_tgt_rec->obj_name && src_rec.bucket_name == p_tgt_rec->bucket_name) {
      p_stats->duplicate_records++;
      ldpp_dout(dpp, 10) << __func__ << "::WARN: Duplicate records for object="
                         << src_rec.obj_name << dendl;
      return 0;
    }

    // the hash table size is rounded to the nearest 4KB and will wrap after 16G
    if (unlikely(src_rec.s.obj_bytes_size != p_tgt_rec->s.obj_bytes_size)) {
      p_stats->size_mismatch++;
      ldpp_dout(dpp, 10) << __func__ << "::WARN: differnt byte size for objects::"
                         << src_rec.obj_name << "::" << src_rec.s.obj_bytes_size
                         << "::" << p_tgt_rec->obj_name << "::"
                         << p_tgt_rec->s.obj_bytes_size << dendl;
      return 0;
    }

    if (memcmp(src_rec.s.sha256, p_tgt_rec->s.sha256, sizeof(src_rec.s.sha256)) != 0) {
      p_stats->sha256_mismatch++;
      ldpp_dout(dpp, 10) << __func__ << "::SHA256 mismatch" << dendl;
      // TBD: set sha256 attributes on head objects to save calc next time
      if (src_rec.s.flags.sha256_calculated()) {
        write_sha256_object_attribute(dpp, driver, rados, &src_rec);
        p_stats->set_sha256_attrs++;
      }
      if (p_tgt_rec->s.flags.sha256_calculated()) {
        write_sha256_object_attribute(dpp, driver, rados, p_tgt_rec);
        p_stats->set_sha256_attrs++;
      }
      return 0;
    }

    ret = dedup_object(&src_rec, p_tgt_rec, p_stats, src_val.has_shared_manifest());
    if (ret == 0) {
      p_stats->deduped_objects++;
      p_stats->deduped_objects_bytes += dedupable_objects_bytes;
      if (p_tgt_rec->s.num_parts == 0) {
        // single part objects duplicate the head object when dedup is used
        p_stats->dup_head_bytes += d_head_object_size;
      }

      // mark the SRC object as a providor of a shared manifest
      if (!src_val.has_shared_manifest()) {
        p_stats->set_shared_manifest_src++;
        // set the shared manifest flag in the dedup table
        p_table->set_shared_manifest_src_mode(&key, src_block_id, src_rec_id);
      }
      else {
        ldpp_dout(dpp, 20) << __func__ << "::SRC object already marked as shared_manifest" << dendl;
      }
    }
    else {
      ldpp_dout(dpp, 10) << __func__ << "::ERR: Failed dedup for "
                         << src_rec.bucket_name << "/" << src_rec.obj_name << dendl;
      p_stats->failed_dedup++;
    }

    return 0;
  }

#endif // #ifdef FULL_DEDUP_SUPPORT
  //---------------------------------------------------------------------------
  const char* Background::dedup_step_name(dedup_step_t step)
  {
    static const char* names[] = {"STEP_NONE",
                                  "STEP_BUCKET_INDEX_INGRESS",
                                  "STEP_BUILD_TABLE",
                                  "STEP_READ_ATTRIBUTES",
                                  "STEP_REMOVE_DUPLICATES"};
    static const char* undefined_step = "UNDEFINED_STEP";
    if (step >= STEP_NONE && step <= STEP_REMOVE_DUPLICATES) {
      return names[step];
    }
    else {
      return undefined_step;
    }
  }

  //---------------------------------------------------------------------------
  int Background::process_all_slabs(dedup_table_t *p_table,
                                    dedup_step_t step,
                                    md5_shard_t md5_shard,
                                    work_shard_t worker_id,
                                    uint32_t *p_slab_count,
                                    md5_stats_t *p_stats, /* IN-OUT */
                                    disk_block_seq_t *p_disk_block_seq,
                                    remapper_t *remapper)
  {
    char block_buff[sizeof(disk_block_t)];
    const int MAX_OBJ_LOAD_FAILURE = 3;
    const int MAX_BAD_BLOCKS = 2;
    bool      has_more = true;
    uint32_t  seq_number = 0;
    int       failure_count = 0;
    ldpp_dout(dpp, 20) << __func__ << "::" << dedup_step_name(step) << "::worker_id="
                       << worker_id << ", md5_shard=" << md5_shard << dendl;
    *p_slab_count = 0;
    while (has_more) {
      bufferlist bl;
      int ret = load_slab(d_dedup_cluster_ioctx, bl, md5_shard, worker_id, seq_number, dpp);
      if (unlikely(ret < 0)) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR::Failed loading object!! md5_shard=" << md5_shard
                          << ", worker_id=" << worker_id << ", seq_number=" << seq_number
                          << ", failure_count=" << failure_count << dendl;
        // skip to the next SLAB stopping after 3 bad objects
        if (failure_count++ < MAX_OBJ_LOAD_FAILURE) {
          seq_number += DISK_BLOCK_COUNT;
          continue;
        }
        else {
          return ret;
        }
      }

      (*p_slab_count)++;
      failure_count = 0;
      unsigned slab_rec_count = 0;
      auto bl_itr = bl.cbegin();
      for (uint32_t block_num = 0; block_num < DISK_BLOCK_COUNT; block_num++, seq_number++) {
        disk_block_id_t disk_block_id(worker_id, seq_number);
        const char *p = get_next_data_ptr(bl_itr, block_buff, sizeof(block_buff),
                                          dpp);
        disk_block_t *p_disk_block = (disk_block_t*)p;
        disk_block_header_t *p_header = p_disk_block->get_header();
        p_header->deserialize();
        if (unlikely(p_header->verify(disk_block_id, dpp) != 0)) {
          p_stats->failed_block_load++;
          // move to next block until reaching a valid block
          if (failure_count++ < MAX_BAD_BLOCKS) {
            continue;
          }
          else {
            ldpp_dout(dpp, 1) << __func__ << "::Skipping slab with too many bad blocks::"
                              << (int)md5_shard << ", worker_id=" << (int)worker_id
                              << ", seq_number=" << seq_number << dendl;
            failure_count = 0;
            break;
          }
        }

        if (p_header->rec_count == 0) {
          ldpp_dout(dpp, 20) << __func__ << "::Block #" << block_num
                             << " has an empty header, no more blocks" << dendl;
          has_more = false;
          break;
        }

        for (unsigned rec_id = 0; rec_id < p_header->rec_count; rec_id++) {
          unsigned offset = p_header->rec_offsets[rec_id];
          // We deserialize the record inside the CTOR
          disk_record_t rec(p + offset);
          ret = rec.validate(__func__, dpp, disk_block_id, rec_id);
          if (unlikely(ret != 0)) {
            p_stats->failed_rec_load++;
            return ret;
          }

          if (step == STEP_BUILD_TABLE) {
            add_record_to_dedup_table(p_table, &rec, disk_block_id, rec_id, p_stats, remapper);
            slab_rec_count++;
          }
#ifdef FULL_DEDUP_SUPPORT
          else if (step == STEP_READ_ATTRIBUTES) {
            read_object_attribute(p_table, &rec, disk_block_id, rec_id, md5_shard,
                                  p_stats, p_disk_block_seq, remapper);
            slab_rec_count++;
          }
          else if (step == STEP_REMOVE_DUPLICATES) {
            try_deduping_record(p_table, &rec, disk_block_id, rec_id, md5_shard,
                                p_stats, remapper);
            slab_rec_count++;
          }
#endif // #ifdef FULL_DEDUP_SUPPORT
          else {
            ceph_abort("unexpected step");
          }
        }

        check_and_update_md5_heartbeat(md5_shard, p_stats->loaded_objects,
                                       p_stats->processed_objects);
        if (unlikely(d_ctl.should_pause())) {
          handle_pause_req(__func__);
        }
        if (unlikely(d_ctl.should_stop())) {
          return -ECANCELED;
        }

        has_more = (p_header->offset == BLOCK_MAGIC);
        ceph_assert(p_header->offset == BLOCK_MAGIC || p_header->offset == LAST_BLOCK_MAGIC);
        if (!has_more) {
          ldpp_dout(dpp, 20) << __func__ << "::No more blocks! block_id=" << disk_block_id
                             << ", rec_count=" << p_header->rec_count << dendl;
          break;
        }
      }
      ldpp_dout(dpp, 20) <<__func__ << "::slab seq_number=" << seq_number
                         << ", rec_count=" << slab_rec_count << dendl;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  static void __attribute__ ((noinline))
  show_ingress_bucket_idx_obj(const DoutPrefixProvider *dpp,
                              const parsed_etag_t &parsed_etag,
                              const string &bucket_name,
                              const string &obj_name)
  {
    ldpp_dout(dpp, 20) << __func__ << "::(1)::" << bucket_name << "/" << obj_name
                       << "::num_parts=" << parsed_etag.num_parts
                       << "::ETAG=" << std::hex << parsed_etag.md5_high
                       << parsed_etag.md5_low << std::dec << dendl;
  }

  //---------------------------------------------------------------------------
  int Background::ingress_bucket_idx_single_object(disk_block_array_t         &disk_arr,
                                                   const rgw::sal::Bucket     *p_bucket,
                                                   const rgw_bucket_dir_entry &entry,
                                                   worker_stats_t             *p_worker_stats /*IN-OUT*/)
  {
    // ceph store full blocks so need to round up and multiply by block_size
    uint64_t ondisk_byte_size = calc_on_disk_byte_size(entry.meta.size);
    // count all objects including too small and non default storage_class objs
    p_worker_stats->ingress_obj++;
    p_worker_stats->ingress_obj_bytes += ondisk_byte_size;

    parsed_etag_t parsed_etag;
    if (unlikely(!parse_etag_string(entry.meta.etag, &parsed_etag))) {
      p_worker_stats->ingress_corrupted_etag++;
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: corrupted etag" << dendl;
      return -EINVAL;
    }

    if (unlikely((cct->_conf->subsys.should_gather<ceph_subsys_rgw_dedup, 20>()))) {
      show_ingress_bucket_idx_obj(dpp, parsed_etag, p_bucket->get_name(), entry.key.name);
    }

    // We limit dedup to objects from the same storage_class
    // TBD:
    // Should we use a skip-list of storage_classes we should skip (like glacier) ?
    const std::string& storage_class =
      rgw_placement_rule::get_canonical_storage_class(entry.meta.storage_class);
    if (storage_class == RGW_STORAGE_CLASS_STANDARD) {
      p_worker_stats->default_storage_class_objs++;
      p_worker_stats->default_storage_class_objs_bytes += ondisk_byte_size;
    }
    else {
      ldpp_dout(dpp, 20) << __func__ << "::" << entry.key.name
                         << "::storage_class:" << entry.meta.storage_class << dendl;
      p_worker_stats->non_default_storage_class_objs++;
      p_worker_stats->non_default_storage_class_objs_bytes += ondisk_byte_size;
    }

    if (ondisk_byte_size <= d_min_obj_size_for_dedup) {
      if (parsed_etag.num_parts == 0) {
        // dedup only useful for objects bigger than 4MB
        p_worker_stats->ingress_skip_too_small++;
        p_worker_stats->ingress_skip_too_small_bytes += ondisk_byte_size;

        if (ondisk_byte_size >= 64*1024) {
          p_worker_stats->ingress_skip_too_small_64KB++;
          p_worker_stats->ingress_skip_too_small_64KB_bytes += ondisk_byte_size;
        }
        else {
          return 0;
        }
      }
      else {
        // multipart objects are always good candidates for dedup
        // the head object is empty and data is stored only in tail objs
        p_worker_stats->small_multipart_obj++;
      }
    }
    // multipart/single_part counters are for objects being fully processed
    if (parsed_etag.num_parts > 0) {
      p_worker_stats->multipart_objs++;
    }
    else {
      p_worker_stats->single_part_objs++;
    }

    return add_disk_rec_from_bucket_idx(disk_arr, p_bucket, &parsed_etag,
                                        entry.key.name, entry.meta.size,
                                        storage_class);
  }

  //---------------------------------------------------------------------------
  void Background::check_and_update_heartbeat(unsigned shard_id, uint64_t count_a,
                                              uint64_t count_b, const char *prefix)
  {
    utime_t now = ceph_clock_now();
    utime_t time_elapsed = now - d_heart_beat_last_update;
    if (unlikely(time_elapsed.tv.tv_sec >= d_heart_beat_max_elapsed_sec)) {
      ldpp_dout(dpp, 20) << __func__ << "::max_elapsed_sec="
                         << d_heart_beat_max_elapsed_sec << dendl;
      d_heart_beat_last_update = now;
      d_cluster.update_shard_token_heartbeat(store, shard_id, count_a, count_b,
                                             prefix);
    }
  }

  //---------------------------------------------------------------------------
  void Background::check_and_update_worker_heartbeat(work_shard_t worker_id,
                                                     int64_t ingress_obj_count)
  {
    check_and_update_heartbeat(worker_id, ingress_obj_count, 0, WORKER_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  void Background::check_and_update_md5_heartbeat(md5_shard_t md5_id,
                                                  uint64_t load_count,
                                                  uint64_t dedup_count)
  {
    check_and_update_heartbeat(md5_id, load_count, dedup_count, MD5_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  static uint32_t move_to_next_bucket_index_shard(const DoutPrefixProvider* dpp,
                                                  unsigned current_shard,
                                                  unsigned num_work_shards,
                                                  const std::string &bucket_name,
                                                  rgw_obj_index_key *p_marker /* OUT-PARAM */)
  {
    uint32_t next_shard = current_shard + num_work_shards;
    ldpp_dout(dpp, 20) << __func__ << "::" << bucket_name << "::curr_shard="
                       << current_shard << ", next shard=" << next_shard << dendl;
    *p_marker = rgw_obj_index_key(); // reset marker to an empty index
    return next_shard;
  }

  // This function process bucket-index shards of a given @bucket
  // The bucket-index-shards are stored in a group of @oids
  // The @oids are using a simple map from the shard-id to the oid holding bucket-indices
  // We start by processing all bucket-indices owned by this @worker-id
  // Once we are done with a given bucket-index shard we skip to the next
  //      bucket-index-shard owned by this worker-id
  // if (bucket_index_shard % work_id) == 0) -> read and process bucket_index_shard
  // else -> skip bucket_index_shard and don't read it
  //---------------------------------------------------------------------------
  int Background::process_bucket_shards(disk_block_array_t     &disk_arr,
                                        const rgw::sal::Bucket *bucket,
                                        std::map<int, string>  &oids,
                                        librados::IoCtx        &ioctx,
                                        work_shard_t            worker_id,
                                        work_shard_t            num_work_shards,
                                        worker_stats_t         *p_worker_stats /*IN-OUT*/)
  {
    const uint32_t num_shards = oids.size();
    uint32_t current_shard = worker_id;
    rgw_obj_index_key marker; // start with an empty marker
    const string null_prefix, null_delimiter;
    const bool list_versions = true;
    const int max_entries = 1000;
    uint32_t obj_count = 0;

    while (current_shard < num_shards ) {
      check_and_update_worker_heartbeat(worker_id, p_worker_stats->ingress_obj);
      if (unlikely(d_ctl.should_pause())) {
        handle_pause_req(__func__);
      }
      if (unlikely(d_ctl.should_stop())) {
        return -ECANCELED;
      }

      const string& oid = oids[current_shard];
      rgw_cls_list_ret result;
      librados::ObjectReadOperation op;
      // get bucket-indices of @current_shard
      cls_rgw_bucket_list_op(op, marker, null_prefix, null_delimiter, max_entries,
                             list_versions, &result);
      int ret = rgw_rados_operate(dpp, ioctx, oid, std::move(op), nullptr, null_yield);
      if (unlikely(ret < 0)) {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: failed rgw_rados_operate() ret="
                          << ret << "::" << cpp_strerror(-ret) << dendl;
        current_shard = move_to_next_bucket_index_shard(dpp, current_shard, num_work_shards,
                                                        bucket->get_name(), &marker);
        continue;
      }
      obj_count += result.dir.m.size();
      for (auto& entry : result.dir.m) {
        const rgw_bucket_dir_entry& dirent = entry.second;
        if (unlikely((!dirent.exists && !dirent.is_delete_marker()) || !dirent.pending_map.empty())) {
          // TBD: should we bailout ???
          ldpp_dout(dpp, 1) << __func__ << "::ERR: calling check_disk_state bucket="
                            << bucket->get_name() << " entry=" << dirent.key << dendl;
          // make sure we're advancing marker
          marker = dirent.key;
          continue;
        }
        marker = dirent.key;
        ret = ingress_bucket_idx_single_object(disk_arr, bucket, dirent, p_worker_stats);
      }
      // TBD: advance marker only once here!
      if (result.is_truncated) {
        ldpp_dout(dpp, 15) << __func__ << "::[" << current_shard
                           << "]result.is_truncated::count=" << obj_count << dendl;
      }
      else {
        // we reached the end of this shard -> move to the next shard
        current_shard = move_to_next_bucket_index_shard(dpp, current_shard, num_work_shards,
                                                        bucket->get_name(), &marker);
        ldpp_dout(dpp, 15) << __func__ << "::move_to_next_bucket_index_shard::count="
                           << obj_count << "::new_shard=" << current_shard << dendl;
      }
    }
    ldpp_dout(dpp, 15) << __func__ << "::Finished processing Bucket "
                       << bucket->get_name() << ", num_shards=" << num_shards
                       << ", obj_count=" << obj_count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::ingress_bucket_objects_single_shard(disk_block_array_t &disk_arr,
                                                      const rgw_bucket   &bucket_rec,
                                                      work_shard_t       worker_id,
                                                      work_shard_t       num_work_shards,
                                                      worker_stats_t     *p_worker_stats /*IN-OUT*/)
  {
    unique_ptr<rgw::sal::Bucket> bucket;
    int ret = driver->load_bucket(dpp, bucket_rec, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: driver->load_bucket(): "
                        << cpp_strerror(-ret) << dendl;
      return ret;
    }

    const std::string bucket_id = bucket->get_key().get_key();
    RGWBucketInfo bucket_info;
    ret = rados->get_bucket_instance_info(bucket_id, bucket_info,
                                          nullptr, nullptr, null_yield, dpp);
    if (unlikely(ret < 0)) {
      if (ret == -ENOENT) {
        // probably a race condition with bucket removal
        ldpp_dout(dpp, 10) << __func__ << "::ret == -ENOENT" << dendl;
        return 0;
      }
      ldpp_dout(dpp, 5) << __func__ << "::ERROR: get_bucket_instance_info(), ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    const rgw::bucket_index_layout_generation idx_layout = bucket_info.layout.current_index;
    librados::IoCtx ioctx;
    // objects holding the bucket-listings
    std::map<int, std::string> oids;
    ret = store->svc()->bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt,
                                                    idx_layout, &ioctx, &oids, nullptr);
    if (ret >= 0) {
      // process all the shards in this bucket owned by the worker_id
      return process_bucket_shards(disk_arr, bucket.get(), oids, ioctx, worker_id,
                                   num_work_shards, p_worker_stats);
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: open_bucket_index() ret="
                        << ret << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  //---------------------------------------------------------------------------
  static void display_table_stat_counters(const DoutPrefixProvider* dpp,
                                          const md5_stats_t *p_stats)
  {
    uint64_t obj_count_in_shard = (p_stats->big_objs_stat.singleton_count +
                                   p_stats->big_objs_stat.unique_count +
                                   p_stats->big_objs_stat.duplicate_count);

    ldpp_dout(dpp, 10) << "\n>>>>>" << __func__ << "::FINISHED STEP_BUILD_TABLE\n"
                       << "::total_count="      << obj_count_in_shard
                       << "::loaded_objects="   << p_stats->loaded_objects
                       << p_stats->big_objs_stat << dendl;
    ldpp_dout(dpp, 10) << __func__ << "::small objs::"
                       << p_stats->small_objs_stat << dendl;
  }

  //---------------------------------------------------------------------------
  int Background::objects_dedup_single_md5_shard(dedup_table_t *p_table,
                                                 md5_shard_t md5_shard,
                                                 md5_stats_t *p_stats,
                                                 work_shard_t num_work_shards)
  {
    remapper_t remapper(MAX_STORAGE_CLASS_IDX);
    // make sure that the standard storage_class is always in the mapper!
    storage_class_idx_t sc_idx = remapper.remap(RGW_STORAGE_CLASS_STANDARD, dpp,
                                                &p_stats->failed_map_overflow);
    ceph_assert(sc_idx == 0);
    uint32_t slab_count_arr[num_work_shards];
    // first load all etags to hashtable to find dedups
    // the entries come from bucket-index and got minimal info (etag, size)
    for (work_shard_t worker_id = 0; worker_id < num_work_shards; worker_id++) {
      process_all_slabs(p_table, STEP_BUILD_TABLE, md5_shard, worker_id,
                        slab_count_arr+worker_id, p_stats, nullptr, &remapper);
      if (unlikely(d_ctl.should_stop())) {
        ldpp_dout(dpp, 5) << __func__ << "::STEP_BUILD_TABLE::STOPPED\n" << dendl;
        return -ECANCELED;
      }
    }
    p_table->count_duplicates(&p_stats->small_objs_stat, &p_stats->big_objs_stat,
                              &p_stats->dup_head_bytes_estimate);
    display_table_stat_counters(dpp, p_stats);

    ldpp_dout(dpp, 10) << __func__ << "::MD5 Loop::" << d_ctl.dedup_type << dendl;
    if (d_ctl.dedup_type != dedup_req_type_t::DEDUP_TYPE_FULL) {
      for (work_shard_t worker_id = 0; worker_id < num_work_shards; worker_id++) {
        remove_slabs(worker_id, md5_shard, slab_count_arr[worker_id]);
      }
      return 0;
    }

#ifndef FULL_DEDUP_SUPPORT
    // we don't support full dedup with this release
    return 0;
#endif

    p_table->remove_singletons_and_redistribute_keys();
    // The SLABs holds minimal data set brought from the bucket-index
    // Objects participating in DEDUP need to read attributes from the Head-Object
    // TBD  - find a better name than num_work_shards for the combined output
    {
      disk_block_t arr[DISK_BLOCK_COUNT];
      worker_stats_t wstat;
      disk_block_seq_t disk_block_seq(dpp, arr, num_work_shards, md5_shard, &wstat);
      for (work_shard_t worker_id = 0; worker_id < num_work_shards; worker_id++) {
        process_all_slabs(p_table, STEP_READ_ATTRIBUTES, md5_shard, worker_id,
                          slab_count_arr+worker_id, p_stats, &disk_block_seq, &remapper);
        if (unlikely(d_ctl.should_stop())) {
          ldpp_dout(dpp, 5) << __func__ << "::STEP_READ_ATTRIBUTES::STOPPED\n" << dendl;
          return -ECANCELED;
        }
        // we finished processing output SLAB from @worker_id -> remove them
        remove_slabs(worker_id, md5_shard, slab_count_arr[worker_id]);
      }
      disk_block_seq.flush_disk_records(d_dedup_cluster_ioctx);
    }

    ldpp_dout(dpp, 10) << __func__ << "::STEP_REMOVE_DUPLICATES::started..." << dendl;
    uint32_t slab_count = 0;
    process_all_slabs(p_table, STEP_REMOVE_DUPLICATES, md5_shard, num_work_shards,
                      &slab_count, p_stats, nullptr, &remapper);
    if (unlikely(d_ctl.should_stop())) {
      ldpp_dout(dpp, 5) << __func__ << "::STEP_REMOVE_DUPLICATES::STOPPED\n" << dendl;
      return -ECANCELED;
    }
    ldpp_dout(dpp, 10) << __func__ << "::STEP_REMOVE_DUPLICATES::finished..." << dendl;
    // remove the special SLAB holding aggragted data
    remove_slabs(num_work_shards, md5_shard, slab_count);
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::read_bucket_stats(const rgw_bucket &bucket_rec,
                                    uint64_t         *p_num_obj,
                                    uint64_t         *p_size)
  {
    unique_ptr<rgw::sal::Bucket> bucket;
    int ret = driver->load_bucket(dpp, bucket_rec, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: driver->load_bucket(): "
                        << cpp_strerror(-ret) << dendl;
      return ret;
    }

    const auto& index = bucket->get_info().get_current_index();
    if (is_layout_indexless(index)) {
      ldpp_dout(dpp, 1) << __func__
                        << "::ERR, indexless buckets do not maintain stats; bucket="
                        << bucket->get_name() << dendl;
      return -EINVAL;
    }

    std::map<RGWObjCategory, RGWStorageStats> stats;
    std::string bucket_ver, master_ver;
    std::string max_marker;
    ret = bucket->read_stats(dpp, null_yield, index, RGW_NO_SHARD, &bucket_ver,
                             &master_ver, stats, &max_marker);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR getting bucket stats bucket="
                        << bucket->get_name() << " ret=" << ret << dendl;
      return ret;
    }

    for (auto itr = stats.begin(); itr != stats.end(); ++itr) {
      RGWStorageStats& s = itr->second;
      ldpp_dout(dpp, 20) << __func__ << "::" << bucket->get_name() << "::"
                         << to_string(itr->first) << "::num_obj=" << s.num_objects
                         << "::size=" << s.size << dendl;
      *p_num_obj += s.num_objects;
      *p_size    += s.size;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::collect_all_buckets_stats()
  {
    int ret = 0;
    std::string section("bucket.instance");
    std::string marker;
    void *handle = nullptr;
    ret = driver->meta_list_keys_init(dpp, section, marker, &handle);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: Failed meta_list_keys_init: "
                        << cpp_strerror(-ret) << dendl;
      return ret;
    }

    d_all_buckets_obj_count = 0;
    d_all_buckets_obj_size  = 0;

    bool has_more = true;
    while (has_more) {
      std::list<std::string> entries;
      constexpr int max_keys = 1000;
      ret = driver->meta_list_keys_next(dpp, handle, max_keys, entries, &has_more);
      if (ret == 0) {
        for (auto& entry : entries) {
          ldpp_dout(dpp, 20) <<__func__ << "::bucket_name=" << entry << dendl;
          rgw_bucket bucket;
          ret = rgw_bucket_parse_bucket_key(cct, entry, &bucket, nullptr);
          if (unlikely(ret < 0)) {
            ldpp_dout(dpp, 1) << __func__ << "::ERR: Failed rgw_bucket_parse_bucket_key: "
                              << cpp_strerror(-ret) << dendl;
            goto err;
          }
          ldpp_dout(dpp, 20) <<__func__ << "::bucket=" << bucket << dendl;
          ret = read_bucket_stats(bucket, &d_all_buckets_obj_count,
                                  &d_all_buckets_obj_size);
          if (unlikely(ret != 0)) {
            goto err;
          }
        }
        driver->meta_list_keys_complete(handle);
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: failed driver->meta_list_keys_next()" << dendl;
        goto err;
      }
    }
    ldpp_dout(dpp, 10) <<__func__
                       << "::all_buckets_obj_count=" << d_all_buckets_obj_count
                       << "::all_buckets_obj_size=" << d_all_buckets_obj_size
                       << dendl;
    return 0;

  err:
    ldpp_dout(dpp, 1) << __func__ << "::error handler" << dendl;
    // reset counters to mark that we don't have the info
    d_all_buckets_obj_count = 0;
    d_all_buckets_obj_size  = 0;
    if (handle) {
      driver->meta_list_keys_complete(handle);
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::objects_ingress_single_work_shard(work_shard_t worker_id,
                                                    work_shard_t num_work_shards,
                                                    md5_shard_t num_md5_shards,
                                                    worker_stats_t *p_worker_stats,
                                                    uint8_t *raw_mem,
                                                    uint64_t raw_mem_size)
  {
    int ret = 0;
    std::string section("bucket.instance");
    std::string marker;
    void *handle = nullptr;
    ret = driver->meta_list_keys_init(dpp, section, marker, &handle);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: Failed meta_list_keys_init: "
                        << cpp_strerror(-ret) << dendl;
      return ret;
    }
    disk_block_array_t disk_arr(dpp, raw_mem, raw_mem_size, worker_id,
                                p_worker_stats, num_md5_shards);
    bool has_more = true;
    // iterate over all buckets
    while (ret == 0 && has_more) {
      std::list<std::string> entries;
      constexpr int max_keys = 1000;
      ret = driver->meta_list_keys_next(dpp, handle, max_keys, entries, &has_more);
      if (ret == 0) {
        ldpp_dout(dpp, 20) <<__func__ << "::entries.size()=" << entries.size() << dendl;
        for (auto& entry : entries) {
          ldpp_dout(dpp, 20) <<__func__ << "::bucket_name=" << entry << dendl;
          rgw_bucket bucket;
          ret = rgw_bucket_parse_bucket_key(cct, entry, &bucket, nullptr);
          if (unlikely(ret < 0)) {
            // bad bucket entry, skip to the next one
            ldpp_dout(dpp, 1) << __func__ << "::ERR: Failed rgw_bucket_parse_bucket_key: "
                              << cpp_strerror(-ret) << dendl;
            continue;
          }
          ldpp_dout(dpp, 20) <<__func__ << "::bucket=" << bucket << dendl;
          ret = ingress_bucket_objects_single_shard(disk_arr, bucket, worker_id,
                                                    num_work_shards, p_worker_stats);
          if (unlikely(ret != 0)) {
            if (d_ctl.should_stop()) {
              driver->meta_list_keys_complete(handle);
              return -ECANCELED;
            }
            ldpp_dout(dpp, 1) << __func__ << "::Failed ingress_bucket_objects_single_shard()" << dendl;
            // skip bad bucket and move on to the next one
            continue;
          }
        }
        driver->meta_list_keys_complete(handle);
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << "::failed driver->meta_list_keys_next()" << dendl;
        driver->meta_list_keys_complete(handle);
        // TBD: what can we do here?
        break;
      }
    }
    ldpp_dout(dpp, 20) <<__func__ << "::flush_output_buffers() worker_id="
                       << worker_id << dendl;
    disk_arr.flush_output_buffers(dpp, d_dedup_cluster_ioctx);
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::remove_slabs(unsigned worker_id, unsigned md5_shard, uint32_t slab_count)
  {
    unsigned failure_count = 0;

    for (uint32_t slab_id = 0; slab_id < slab_count; slab_id++) {
      uint32_t seq_number = disk_block_id_t::slab_id_to_seq_num(slab_id);
      disk_block_id_t block_id(worker_id, seq_number);
      std::string oid(block_id.get_slab_name(md5_shard));
      ldpp_dout(dpp, 20) << __func__ << "::calling ioctx->remove(" << oid << ")" << dendl;
      int ret = d_dedup_cluster_ioctx.remove(oid);
      if (ret != 0) {
        ldpp_dout(dpp, 0) << __func__ << "::ERR Failed ioctx->remove(" << oid << ")" << dendl;
        failure_count++;
      }
    }

    return failure_count;
  }

  //---------------------------------------------------------------------------
  int Background::f_ingress_work_shard(unsigned worker_id,
                                       uint8_t *raw_mem,
                                       uint64_t raw_mem_size,
                                       work_shard_t num_work_shards,
                                       md5_shard_t num_md5_shards)
  {
    ldpp_dout(dpp, 20) << __func__ << "::worker_id=" << worker_id << dendl;
    utime_t start_time = ceph_clock_now();
    worker_stats_t worker_stats;
    int ret = objects_ingress_single_work_shard(worker_id, num_work_shards, num_md5_shards,
                                                &worker_stats,raw_mem, raw_mem_size);
    if (ret == 0) {
      worker_stats.duration = ceph_clock_now() - start_time;
      d_cluster.mark_work_shard_token_completed(store, worker_id, &worker_stats);
      ldpp_dout(dpp, 10) << "stat counters [worker]:\n" << worker_stats << dendl;
      ldpp_dout(dpp, 10) << "Shard Process Duration   = "
                         << worker_stats.duration << dendl;
    }
    //ldpp_dout(dpp, 0) << __func__ << "::sleep for 2 seconds\n" << dendl;
    //std::this_thread::sleep_for(std::chrono::seconds(2));
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::f_dedup_md5_shard(unsigned md5_shard,
                                    uint8_t *raw_mem,
                                    uint64_t raw_mem_size,
                                    work_shard_t num_work_shards,
                                    md5_shard_t num_md5_shards)
  {
    utime_t start_time = ceph_clock_now();
    md5_stats_t md5_stats;
    //DEDUP_DYN_ALLOC
    dedup_table_t table(dpp, d_head_object_size, raw_mem, raw_mem_size);
    int ret = objects_dedup_single_md5_shard(&table, md5_shard, &md5_stats, num_work_shards);
    if (ret == 0) {
      md5_stats.duration = ceph_clock_now() - start_time;
      d_cluster.mark_md5_shard_token_completed(store, md5_shard, &md5_stats);
      ldpp_dout(dpp, 10) << "stat counters [md5]:\n" << md5_stats << dendl;
      ldpp_dout(dpp, 10) << "Shard Process Duration   = "
                         << md5_stats.duration << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::process_all_shards(bool ingress_work_shards,
                                     int (Background::*func)(unsigned, uint8_t*, uint64_t, work_shard_t, md5_shard_t),
                                     uint8_t *raw_mem,
                                     uint64_t raw_mem_size,
                                     work_shard_t num_work_shards,
                                     md5_shard_t num_md5_shards)
  {
    while (true) {
      d_heart_beat_last_update = ceph_clock_now();
      uint16_t shard_id;
      if (ingress_work_shards) {
        shard_id = d_cluster.get_next_work_shard_token(store, num_work_shards);
      }
      else {
        shard_id = d_cluster.get_next_md5_shard_token(store, num_md5_shards);
      }

      // start with a common error handler
      if (shard_id != NULL_SHARD) {
        ldpp_dout(dpp, 10) << __func__ << "::Got shard_id=" << shard_id << dendl;
        int ret = (this->*func)(shard_id, raw_mem, raw_mem_size, num_work_shards,
                                num_md5_shards);
        if (unlikely(ret != 0)) {
          if (d_ctl.should_stop()) {
            ldpp_dout(dpp, 5) << __func__ << "::stop execution" << dendl;
            return -ECANCELED;
          }
          else {
            ldpp_dout(dpp, 5) << __func__ << "::Skip shard #" << shard_id << dendl;
          }
        }
      }
      else {
        ldpp_dout(dpp, 10) << __func__ << "::finished processing all shards" <<dendl;
        break;
      }
    } // while loop
    return 0;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static int collect_pool_stats(const DoutPrefixProvider* const dpp,
                                                RGWRados* rados,
                                                uint64_t *p_num_objects,
                                                uint64_t *p_num_objects_bytes)
  {
    *p_num_objects       = 0;
    *p_num_objects_bytes = 0;
    list<string> vec;
    vec.push_back("default.rgw.buckets.data");
    map<string,librados::pool_stat_t> stats;
    auto rados_handle = rados->get_rados_handle();
    int ret = rados_handle->get_pool_stats(vec, stats);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ":ERROR: fetching pool stats: "
                        << cpp_strerror(-ret) << dendl;
      return ret;
    }

    for (auto i = stats.begin(); i != stats.end(); ++i) {
      const char *pool_name = i->first.c_str();
      librados::pool_stat_t& s = i->second;
      // TBD: add support for EC
      // We need to find the user byte size without the added protection
      double replica_level = (double)s.num_object_copies / s.num_objects;
      *p_num_objects       = s.num_objects;
      *p_num_objects_bytes = s.num_bytes / replica_level;
      ldpp_dout(dpp, 10) <<__func__ << "::" << pool_name << "::num_objects="
                         << s.num_objects << "::num_copies=" << s.num_object_copies
                         << "::num_bytes=" << s.num_bytes << "/" << *p_num_objects_bytes << dendl;
    }
    return 0;
  }

  //-------------------------------------------------------------------------------
  //  32B per object-entry in the hashtable
  //  2MB per shard-buffer
  //=============||==============||=========||===================================||
  // Obj Count   || shard count  || memory  ||         calculation               ||
  // ------------||--------------||---------||---------------------------------- ||
  //     1M      ||      4       ||     8MB ||    8MB/32 =  0.25M *   4 =     1M ||
  //     4M      ||      8       ||    16MB ||   16MB/32 =  0.50M *   8 =     4M ||
  //-------------------------------------------------------------------------------
  //    16M      ||     16       ||    32MB ||   32MB/32 =  1.00M *  16 =    16M ||
  //-------------------------------------------------------------------------------
  //    64M      ||     32       ||    64MB ||   64MB/32 =  2.00M *  32 =    64M ||
  //   256M      ||     64       ||   128MB ||  128MB/32 =  4.00M *  64 =   256M ||
  //  1024M( 1G) ||    128       ||   256MB ||  256MB/32 =  8.00M * 128 =  1024M ||
  //  4096M( 4G) ||    256       ||   512MB ||  512MB/32 = 16M.00 * 256 =  4096M ||
  // 16384M(16G) ||    512       ||  1024MB || 1024MB/32 = 32M.00 * 512 = 16384M ||
  //-------------||--------------||---------||-----------------------------------||
  static md5_shard_t calc_num_md5_shards(uint64_t obj_count)
  {
    // create headroom by allocating space for a 10% bigger system
    obj_count = obj_count + (obj_count/10);

    uint64_t M = 1024 * 1024;
    if (obj_count < 1*M) {
      // less than 1M objects -> use 4 shards (8MB)
      return 4;
    }
    else if (obj_count < 4*M) {
      // less than 4M objects -> use 8 shards (16MB)
      return 8;
    }
    else if (obj_count < 16*M) {
      // less than 16M objects -> use 16 shards (32MB)
      return 16;
    }
    else if (obj_count < 64*M) {
      // less than 64M objects -> use 32 shards (64MB)
      return 32;
    }
    else if (obj_count < 256*M) {
      // less than 256M objects -> use 64 shards (128MB)
      return 64;
    }
    else if (obj_count < 1024*M) {
      // less than 1024M objects -> use 128 shards (256MB)
      return 128;
    }
    else if (obj_count < 4*1024*M) {
      // less than 4096M objects -> use 256 shards (512MB)
      return 256;
    }
    else {
      return 512;
    }
  }

  //---------------------------------------------------------------------------
  int Background::setup(dedup_epoch_t *p_epoch)
  {
    int ret = collect_all_buckets_stats();
    if (unlikely(ret != 0)) {
      return ret;
    }

    md5_shard_t num_md5_shards = calc_num_md5_shards(d_all_buckets_obj_count);
    num_md5_shards = std::min(num_md5_shards, MAX_MD5_SHARD);
    num_md5_shards = std::max(num_md5_shards, MIN_MD5_SHARD);
    work_shard_t num_work_shards = num_md5_shards;
    num_work_shards = std::min(num_work_shards, MAX_WORK_SHARD);

    ldpp_dout(dpp, 5) << __func__ << "::obj_count=" <<d_all_buckets_obj_count
                      << "::num_md5_shards=" << num_md5_shards
                      << "::num_work_shards=" << num_work_shards << dendl;
    // init handles and create the dedup_pool
    ret = init_rados_access_handles(true);
    if (ret != 0) {
      derr << "dedup_bg::resume() failed init_rados_access_handles() ret="
           << ret << "::" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    display_ioctx_state(dpp, d_dedup_cluster_ioctx, __func__);

    ret = d_cluster.reset(store, p_epoch, num_work_shards, num_md5_shards);
    if (ret != 0) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed cluster.init()" << dendl;
      return ret;
    }

    if (unlikely(p_epoch->num_work_shards > MAX_WORK_SHARD)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: p_epoch->num_work_shards="
                        << p_epoch->num_work_shards
                        << " is larger than MAX_WORK_SHARD ("
                        << MAX_WORK_SHARD << ")" << dendl;
      return -EOVERFLOW;
    }
    if (unlikely(p_epoch->num_md5_shards > MAX_MD5_SHARD)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: p_epoch->num_md5_shards="
                        << p_epoch->num_md5_shards
                        << " is larger than MAX_MD5_SHARD ("
                        << MAX_MD5_SHARD << ")" << dendl;
      return -EOVERFLOW;
    }

    ldpp_dout(dpp, 10) <<__func__ << "::" << *p_epoch << dendl;
    d_ctl.dedup_type = p_epoch->dedup_type;
#ifdef FULL_DEDUP_SUPPORT
    ceph_assert(d_ctl.dedup_type == dedup_req_type_t::DEDUP_TYPE_FULL ||
                d_ctl.dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE);
#else
    ceph_assert(d_ctl.dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE);
#endif
    ldpp_dout(dpp, 10) << __func__ << "::" << d_ctl.dedup_type << dendl;

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::watch_reload(const DoutPrefixProvider* dpp)
  {
    return cluster::watch_reload(store, dpp, &d_watch_handle, &d_watcher_ctx);
  }

  //---------------------------------------------------------------------------
  int Background::unwatch_reload(const DoutPrefixProvider* dpp)
  {
    if (d_watch_handle == 0) {
      // nothing to unwatch
      ldpp_dout(dpp, 1) << "dedup_bg::unwatch_reload(): nothing to watch"
                        << dendl;
      return 0;
    }

    ldpp_dout(dpp, 5) << "dedup_bg::unwatch_reload(): watch_handle="
                      << d_watch_handle << dendl;

    int ret = cluster::unwatch_reload(store, dpp, d_watch_handle);
    if (ret == 0) {
      ldpp_dout(dpp, 5) << "dedup_bg::unwatch_reload():Stopped watching "
                        << "::d_watch_handle=" << d_watch_handle << dendl;
      d_watch_handle = 0;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  void Background::handle_notify(uint64_t notify_id, uint64_t cookie, bufferlist &bl)
  {
    int ret = 0;
    int32_t urgent_msg = URGENT_MSG_NONE;
    try {
      auto bl_iter = bl.cbegin();
      ceph::decode(urgent_msg, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << __func__ << "::ERROR: bad urgent_msg" << dendl;
      ret = -EINVAL;
    }
    ldpp_dout(dpp, 5) << __func__ << "::-->" << get_urgent_msg_names(urgent_msg) << dendl;

    // use lock to prevent concurrent pause/resume requests
    std::unique_lock cond_lock(d_cond_mutex); // [------>open lock block
    if (unlikely(d_ctl.local_urgent_req())) {
      // can't operate when the system is paused/shutdown
      cond_lock.unlock(); // close lock block------>]
      ldpp_dout(dpp, 5) << __func__
                        << "::system is paused/shutdown -> cancel notification" << dendl;
      cluster::ack_notify(store, dpp, &d_ctl, notify_id, cookie, -EBUSY);
      return;
    }

    switch(urgent_msg) {
    case URGENT_MSG_ABORT:
      if (d_ctl.dedup_exec) {
        d_ctl.remote_abort_req = true;
        d_cond.notify_all();
        d_cond.wait(cond_lock, [this]{return d_ctl.remote_aborted || d_ctl.local_urgent_req();});
        d_ctl.remote_aborted ? ret = 0 : ret = -EBUSY;
      }
      else {
        ldpp_dout(dpp, 5) << __func__ << "::inactive dedup->nothing to do" << dendl;
      }
      break;
    case URGENT_MSG_RESTART:
      if (!d_ctl.dedup_exec) {
        d_ctl.remote_restart_req = true;
        d_cond.notify_all();
      }
      else {
        ldpp_dout(dpp, 5) << __func__ << "::\ncan't restart active dedup\n"<< dendl;
        ret = -EEXIST;
      }
      break;
    case URGENT_MSG_PASUE:
      if (d_ctl.dedup_exec && !d_ctl.remote_paused) {
        d_ctl.remote_pause_req = true;
        d_cond.notify_all();
        d_cond.wait(cond_lock, [this]{return d_ctl.remote_paused || d_ctl.local_urgent_req();});
        d_ctl.remote_paused ? ret = 0 : ret = -EBUSY;
      }
      else {
        if (d_ctl.remote_paused) {
          ldpp_dout(dpp, 5) << __func__ << "::dedup is already paused" << dendl;
        }
        else {
          ldpp_dout(dpp, 5) << __func__ << "::inactive dedup->nothing to do" << dendl;
        }
      }
      break;
    case URGENT_MSG_RESUME:
      if (d_ctl.remote_pause_req || d_ctl.remote_paused) {
        d_ctl.remote_pause_req = false;
        d_ctl.remote_paused = false;
        d_cond.notify_all();
      }
      else {
        ldpp_dout(dpp, 5) << __func__ << "::dedup is not paused->nothing to do" << dendl;
      }
      break;
    default:
      ldpp_dout(dpp, 1) << __func__ << "::unexpected urgent_msg: "
                        << get_urgent_msg_names(urgent_msg) << dendl;
      ret = -EINVAL;
    }

    cond_lock.unlock(); // close lock block------>]
    cluster::ack_notify(store, dpp, &d_ctl, notify_id, cookie, ret);
  }

  //---------------------------------------------------------------------------
  void Background::start()
  {
    const DoutPrefixProvider* const dpp = &dp;
    ldpp_dout(dpp, 10) <<  __FILE__ << "::" <<__func__ << dendl;
    {
      std::unique_lock pause_lock(d_cond_mutex);
      if (d_ctl.started) {
        // start the thread only once
        ldpp_dout(dpp, 1) << "dedup_bg already started" << dendl;
        return;
      }
      d_ctl.started = true;
    }
    d_runner = std::thread(&Background::run, this);
  }

  //------------------------- --------------------------------------------------
  void Background::shutdown()
  {
    ldpp_dout(dpp, 5) <<__func__ << "::dedup_bg shutdown()" << dendl;
    std::unique_lock cond_lock(d_cond_mutex);
    bool nested_call = false;
    if (d_ctl.shutdown_req) {
      // should never happen!
      ldpp_dout(dpp, 1) <<__func__ << "dedup_bg nested call" << dendl;
      nested_call = true;
    }
    d_ctl.shutdown_req = true;
    d_cond.notify_all();
    ldpp_dout(dpp, 1) <<__func__ << "dedup_bg shutdown waiting..." << dendl;
    d_cond.wait(cond_lock, [this]{return d_ctl.shutdown_done;});
    //cond_lock.unlock();

    if (nested_call) {
      ldpp_dout(dpp, 1) <<__func__ << "::nested call:: repeat notify" << dendl;
      d_cond.notify_all();
    }

    if (d_runner.joinable()) {
      ldpp_dout(dpp, 5) <<__func__ << "::dedup_bg wait join()" << dendl;
      d_runner.join();
      ldpp_dout(dpp, 5) <<__func__ << "::dedup_bg finished join()" << dendl;
    }
    else {
      ldpp_dout(dpp, 5) <<__func__ << "::dedup_bg not joinable()" << dendl;
    }

    d_ctl.reset();
  }

  //---------------------------------------------------------------------------
  void Background::pause()
  {
    display_ioctx_state(dpp, d_dedup_cluster_ioctx, "dedup_bg->pause() request");
    std::unique_lock cond_lock(d_cond_mutex);

    if (d_ctl.local_paused || d_ctl.shutdown_done) {
      cond_lock.unlock();
      ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__
                        << "::dedup_bg is already paused/stopped" << dendl;
      return;
    }

    bool nested_call = false;
    if (d_ctl.local_pause_req) {
      // should never happen!
      ldpp_dout(dpp, 1) <<__func__ << "::nested call" << dendl;
      nested_call = true;
    }
    d_ctl.local_pause_req = true;
    d_cond.notify_all();
    d_cond.wait(cond_lock, [this]{return d_ctl.local_paused||d_ctl.shutdown_done;});
    if (nested_call) {
      ldpp_dout(dpp, 1) << "dedup_bg::nested call:: repeat notify" << dendl;
      d_cond.notify_all();
    }

    // destory open watch request and pool handle before pause() is completed
    unwatch_reload(dpp);
    d_dedup_cluster_ioctx.close();
    ldpp_dout(dpp, 5) << "dedup_bg paused" << dendl;
  }

  //---------------------------------------------------------------------------
  void Background::resume(rgw::sal::Driver* _driver)
  {
    ldpp_dout(dpp, 5) << "dedup_bg->resume()" << dendl;
    // use lock to prevent concurrent pause/resume requests
    std::unique_lock cond_lock(d_cond_mutex);

    if (!d_ctl.local_paused) {
      cond_lock.unlock();
      ldpp_dout(dpp, 5) << "dedup_bg::resume thread is not paused!" << dendl;
      if (_driver != driver) {
        ldpp_dout(dpp, 1) << "dedup_bg attempt to change driver on an active system was refused" << dendl;
      }
      return;
    }

    driver = _driver;
    // can pool change its uid between pause/resume ???
    int ret = init_rados_access_handles(false);
    if (ret != 0) {
      derr << "dedup_bg::resume() failed init_rados_access_handles() ret="
           << ret << "::" << cpp_strerror(-ret) << dendl;
      throw std::runtime_error("Failed init_rados_access_handles()");
    }
    display_ioctx_state(dpp, d_dedup_cluster_ioctx, "dedup_bg->resume() done");
    // create new watch request using the new pool handle
    watch_reload(dpp);
    d_ctl.local_pause_req = false;
    d_ctl.local_paused    = false;

    // wake up threads blocked after seeing pause state
    d_cond.notify_all();
    ldpp_dout(dpp, 5) << "dedup_bg was resumed" << dendl;
  }

  //---------------------------------------------------------------------------
  void Background::handle_pause_req(const char *caller)
  {
    ldpp_dout(dpp, 5) << __func__ << "::caller=" << caller << dendl;
    ldpp_dout(dpp, 5) << __func__ << "::" << d_ctl << dendl;
    while (d_ctl.local_pause_req || d_ctl.local_paused || d_ctl.remote_pause_req || d_ctl.remote_paused) {
      std::unique_lock cond_lock(d_cond_mutex);
      if (d_ctl.should_stop()) {
        ldpp_dout(dpp, 5) << __func__ << "::should_stop!" << dendl;
        return;
      }

      if (d_ctl.local_pause_req) {
        d_ctl.local_pause_req = false;
        d_ctl.local_paused    = true;
      }

      if (d_ctl.remote_pause_req) {
        d_ctl.remote_pause_req = false;
        d_ctl.remote_paused    = true;
      }

      d_cond.notify_all();

      if (d_ctl.local_paused) {
        ldpp_dout(dpp, 10) << __func__ << "::wait on d_ctl.local_paused" << dendl;
        d_cond.wait(cond_lock, [this]{return !d_ctl.local_paused || d_ctl.should_stop() ;});
      }

      if (d_ctl.remote_paused) {
        ldpp_dout(dpp, 10) << __func__ << "::wait on d_ctl.remote_paused" << dendl;
        d_cond.wait(cond_lock, [this]{return !d_ctl.remote_paused || d_ctl.should_stop() || d_ctl.local_pause_req;});
      }
    } // while loop

    ldpp_dout(dpp, 5) << "Dedup background thread resumed!" << dendl;
  }

  //---------------------------------------------------------------------------
  void Background::work_shards_barrier(work_shard_t num_work_shards)
  {
    // Wait for other worker to finish ingress step
    // We can move to the next step even if some token are in failed state
    const unsigned MAX_WAIT_SEC = 120; // wait 2 minutes for failing members
    unsigned ttl = 3;
    unsigned time_elapsed = 0;

    while (true) {
      int ret = d_cluster.all_work_shard_tokens_completed(store, num_work_shards);
      // we start incrementing time_elapsed only after all valid tokens finish
      if (ret == 0 || (time_elapsed > MAX_WAIT_SEC) ) {
        break;
      }

      ldpp_dout(dpp, 10) << __func__ << "::Wait for object ingress completion, ttl="
                         << ttl << " seconds" << dendl;
      std::unique_lock cond_lock(d_cond_mutex);
      d_cond.wait_for(cond_lock, std::chrono::seconds(ttl),
                      [this]{return d_ctl.should_stop() || d_ctl.should_pause();});
      if (unlikely(d_ctl.should_pause())) {
        handle_pause_req(__func__);
      }
      if (unlikely(d_ctl.should_stop())) {
        return;
      }

      if (ret != -EAGAIN) {
        // All incomplete tokens are corrupted or in time out state
        // Give them an extra 120 seconds just in case ...
        time_elapsed += ttl;
      }
      // else there are still good tokens in process, wait for them
    }

    ldpp_dout(dpp, 10) << "\n\n==Object Ingress step was completed on all shards==\n"
                       << dendl;
    if (unlikely(d_ctl.should_pause())) {
      handle_pause_req(__func__);
    }
  }

  //---------------------------------------------------------------------------
  static bool all_md5_shards_completed(cluster *p_cluster,
                                       rgw::sal::RadosStore *store,
                                       md5_shard_t num_md5_shards)
  {
    return (p_cluster->all_md5_shard_tokens_completed(store, num_md5_shards) == 0);
  }

  //---------------------------------------------------------------------------
  void Background::md5_shards_barrier(md5_shard_t num_md5_shards)
  {
    // Wait for others to finish step
    unsigned ttl = 3;
    // require that everything completed successfully before deleting the pool
    while (!all_md5_shards_completed(&d_cluster, store, num_md5_shards)) {
      ldpp_dout(dpp, 10) << __func__ << "::Wait for md5 completion, ttl="
                         << ttl << " seconds" << dendl;
      std::unique_lock cond_lock(d_cond_mutex);
      d_cond.wait_for(cond_lock, std::chrono::seconds(ttl),
                      [this]{return d_ctl.should_stop() || d_ctl.should_pause();});
      if (unlikely(d_ctl.should_pause())) {
        handle_pause_req(__func__);
      }
      if (unlikely(d_ctl.should_stop())) {
        return;
      }
    }

    ldpp_dout(dpp, 10) << "\n\n==MD5 processing was completed on all shards!==\n"
                       << dendl;
    if (unlikely(d_ctl.should_pause())) {
      handle_pause_req(__func__);
    }
  }

  //---------------------------------------------------------------------------
  void Background::run()
  {
    const auto rc = ceph_pthread_setname("dedup_bg");
    ldpp_dout(dpp, 5) << __func__ << "ceph_pthread_setname() ret=" << rc << dendl;

    // 256x8KB=2MB
    const uint64_t PER_SHARD_BUFFER_SIZE = DISK_BLOCK_COUNT *sizeof(disk_block_t);
    ldpp_dout(dpp, 20) <<__func__ << "::dedup::main loop" << dendl;

    while (!d_ctl.shutdown_req) {
      if (unlikely(d_ctl.should_pause())) {
        handle_pause_req(__func__);
        if (unlikely(d_ctl.should_stop())) {
          ldpp_dout(dpp, 5) <<__func__ << "::stop req after a pause" << dendl;
          d_ctl.dedup_exec = false;
        }
      }

      if (d_ctl.dedup_exec) {
        dedup_epoch_t epoch;
        if (setup(&epoch) != 0) {
          ldpp_dout(dpp, 1) << __func__ << "::failed setup()" << dendl;
          return;
        }
        const rgw_pool& dedup_pool = store->svc()->zone->get_zone_params().dedup_pool;
        int64_t pool_id = rados_handle->pool_lookup(dedup_pool.name.c_str());
        if (pool_id < 0) {
          ldpp_dout(dpp, 1) << __func__ << "::bad pool_id" << dendl;
          return;
        }
        work_shard_t num_work_shards = epoch.num_work_shards;
        md5_shard_t  num_md5_shards  = epoch.num_md5_shards;
        const uint64_t RAW_MEM_SIZE = PER_SHARD_BUFFER_SIZE * num_md5_shards;
        ldpp_dout(dpp, 5) <<__func__ << "::RAW_MEM_SIZE=" << RAW_MEM_SIZE
                          << "::num_work_shards=" << num_work_shards
                          << "::num_md5_shards=" << num_md5_shards << dendl;
        // DEDUP_DYN_ALLOC
        auto raw_mem = std::make_unique<uint8_t[]>(RAW_MEM_SIZE);
        if (raw_mem == nullptr) {
          ldpp_dout(dpp, 1) << "failed slab memory allocation - size=" << RAW_MEM_SIZE << dendl;
          return;
        }

        process_all_shards(true, &Background::f_ingress_work_shard, raw_mem.get(),
                           RAW_MEM_SIZE, num_work_shards, num_md5_shards);
        if (!d_ctl.should_stop()) {
          // Wait for all other workers to finish ingress step
          work_shards_barrier(num_work_shards);
          if (!d_ctl.should_stop()) {
            process_all_shards(false, &Background::f_dedup_md5_shard, raw_mem.get(),
                               RAW_MEM_SIZE, num_work_shards, num_md5_shards);
            // Wait for all other md5 shards to finish
            md5_shards_barrier(num_md5_shards);
            safe_pool_delete(store, dpp, pool_id);
          }
          else {
            ldpp_dout(dpp, 5) <<__func__ << "::stop req from barrier" << dendl;
          }
        }
        else {
          ldpp_dout(dpp, 5) <<__func__ << "::stop req from ingress_work_shard" << dendl;
        }
      } // dedup_exec

      std::unique_lock cond_lock(d_cond_mutex);
      d_ctl.dedup_exec = false;
      if (d_ctl.remote_abort_req) {
        d_ctl.remote_aborted = true;

        d_ctl.remote_abort_req = false;
        d_ctl.remote_paused = false;
        d_cond.notify_all();
        ldpp_dout(dpp, 5) << __func__ << "::Dedup was aborted on a remote req" << dendl;
      }
      d_cond.wait(cond_lock, [this]{return d_ctl.remote_restart_req || d_ctl.should_stop() || d_ctl.should_pause();});
      if (!d_ctl.should_stop() && !d_ctl.should_pause()) {
        // TBD: should we release lock here ???
        if (d_cluster.can_start_new_scan(store)) {
          d_ctl.dedup_exec = true;
          d_ctl.remote_aborted = false;
          d_ctl.remote_paused = false;
          d_ctl.remote_restart_req = false;
          d_cond.notify_all();
        }
      }else if (d_ctl.should_stop()) {
        ldpp_dout(dpp, 5) << "main loop::should_stop::" << d_ctl << dendl;
      }
      else {
        ldpp_dout(dpp, 5) << "main loop::should_pause::" << d_ctl << dendl;
      }
    }
    d_ctl.shutdown_done = true;
    d_cond.notify_all();
    // shutdown
    ldpp_dout(dpp, 5) << __func__ << "::Dedup background thread stopped" << dendl;
  }

}; //namespace rgw::dedup
