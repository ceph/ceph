#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "rgw_tools.h"
#include "svc_zone.h"
//#include "include/rados/rgw_tools.h"

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
#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_sal_rados.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
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
#include <mutex>
#include <thread>

//using namespace std::chrono_literals;
using namespace librados;
using namespace std;
using namespace rgw::dedup;
#include "rgw_sal_rados.h"
#include "rgw_dedup_table.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup.h"
#include "rgw_dedup_store.h"
#include "rgw_perf_counters.h"
#include "include/ceph_assert.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;
static constexpr uint64_t cost = 1; // 1 throttle unit per request
static constexpr uint64_t id = 0; // ids unused

namespace rgw::dedup {
  //---------------------------------------------------------------------------
  void Background::init_rados_access_handles()
  {
    store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr << "ERROR: command only works with RADOS back-ends" << std::endl;
      ceph_abort("Bad Rados driver");
    }

    rados = store->getRados();
    rados_handle = rados->get_rados_handle();
  }

  //---------------------------------------------------------------------------
  Background::Background(rgw::sal::Driver* _driver,
			 CephContext* _cct,
			 int _execute_interval) :
    driver(_driver),
    execute_interval(_execute_interval),
    dp(_cct, dout_subsys, "dedup background: "),
    dpp(&dp),
    cct(_cct),
    d_table(16*1024, dpp)
  {
    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	// TBD: check allocation success
	p_disk_arr[md5_shard][worker_id] = new disk_block_array_t(dpp, md5_shard, worker_id);
      }
    }
    // TBD: check allocation success
    init_rados_access_handles();
  }

  //---------------------------------------------------------------------------
  Background::~Background()
  {
    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	if (p_disk_arr[md5_shard][worker_id]) {
	  delete p_disk_arr[md5_shard][worker_id];
	}
      }
    }
  }

  //---------------------------------------------------------------------------
  void Background::start()
  {
    const DoutPrefixProvider* const dpp = &dp;
    ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__ << dendl;
    {
      std::unique_lock pause_lock(pause_mutex);
      if (started) {
	// start the thread only once
	return;
      }
      started = true;
    }
    runner = std::thread(&Background::run, this);
    const auto rc = ceph_pthread_setname(runner.native_handle(), "dedup_bg");
    ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__ << "::setname rc=" << rc << dendl;
  }

  //------------------------- --------------------------------------------------
  void Background::shutdown()
  {
    stopped = true;
    cond.notify_all();
    if (runner.joinable()) {
      runner.join();
    }
    {
      std::unique_lock pause_lock(pause_mutex);
      started = false;
      stopped = false;
      pause_req = false;
      paused = false;
    }
  }

  //---------------------------------------------------------------------------
  void Background::pause()
  {
    {
      std::unique_lock pause_lock(pause_mutex);
      if (paused) {
	derr <<  __FILE__ << "::" <<__func__
	     << "::background is already paused!!!" << dendl;
	pause_req = true;
      }
      std::unique_lock cond_lock(cond_mutex);
      cond.wait(cond_lock, [this]{return paused || stopped;});
    }
  }

  //---------------------------------------------------------------------------
  void Background::resume(rgw::sal::Driver* _driver)
  {
    {
      std::unique_lock pause_lock(pause_mutex);
      if (!paused) {
	derr <<  __FILE__ << "::" <<__func__
	     << "::background is not paused!!!" << dendl;
	if (_driver != driver) {
	  derr <<  __FILE__ << "::" <<__func__
	       << "::attempt to change driver on a live task failed!!!" << dendl;
	}
	return;
      }
      pause_req = false;
      paused    = false;
      driver    = _driver;
      init_rados_access_handles();
    }
    // wake up threads blocked after seeing pause state
    cond.notify_all();
  }

  //---------------------------------------------------------------------------
  int Background::read_bucket_stats(rgw::sal::Bucket *bucket, const string &bucket_name)
  {
    const auto& index = bucket->get_info().get_current_index();
    if (is_layout_indexless(index)) {
      derr << "error, indexless buckets do not maintain stats; bucket="
	   << bucket_name << dendl;
      return -EINVAL;
    }

    std::map<RGWObjCategory, RGWStorageStats> stats;
    std::string bucket_ver, master_ver;
    std::string max_marker;
    int ret = bucket->read_stats(dpp, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
    if (ret < 0) {
      derr << "error getting bucket stats bucket=" << bucket_name << " ret=" << ret << dendl;
      return ret;
    }
#if 0
    for (auto itr = stats.begin(); itr != stats.end(); ++itr) {
      RGWStorageStats& s = itr->second;
      ldpp_dout(dpp, 1) << __func__ << "::" << to_string(itr->first)<< "::num_obj" << s.num_objects << "::size=" << s.size << dendl;
    }

    utime_t ut(bucket->get_modification_time());
    ldpp_dout(dpp, 1) << __func__ << "::modification_time=" << ut << dendl;
#endif
    return 0;
  }
#if 0
  //---------------------------------------------------------------------------
  void Background::calc_object_key(uint64_t object_size, bufferlist &etag_bl, struct Key *p_key)
  {
    //const string& etag = etag_bl.to_str();
    parsed_etag_t parsed_etag;
    parse_etag_string(etag_bl.to_str(), &parsed_etag);
    const uint32_t size_4k   = uint32_t(object_size/(4*1024));

    p_key->md5_high  = parsed_etag.md5_high;
    p_key->md5_low   = parsed_etag.md5_low;
    p_key->size_4k_units  = size_4k;
    p_key->num_parts = parsed_etag.num_parts;
  }
#endif
  //---------------------------------------------------------------------------
  [[maybe_unused]]static void show_ref_tags(const DoutPrefixProvider* dpp, std::string &oid, rgw_rados_ref &obj)
  {
    unsigned idx = 0;
    std::list<std::string> refs;
    std::string wildcard_tag;
    //std::string oid = raw_obj.oid;
    int ret = cls_refcount_read(obj.ioctx, oid, &refs, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "manifest::failed cls_refcount_read() idx=" << idx << dendl;
      return;
    }

    for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
      ldpp_dout(dpp, 1) << "manifest::" << oid << "::" << idx << "::TAG=" << *iter << dendl;
    }
  }

  //---------------------------------------------------------------------------
  int Background::free_tail_objs_by_manifest(const string &ref_tag, RGWObjManifest &tgt_manifest)
  {
    unsigned idx = 0;
    for (auto p = tgt_manifest.obj_begin(dpp); p != tgt_manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      rgw_rados_ref obj;
      int ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "manifest::failed to open rados context for " << obj << dendl;
	continue;
      }
      if (idx == 0) {
	if (raw_obj.oid.find("shadow") != std::string::npos) {
	  ldpp_dout(dpp, 0) << __func__ << "::dedup:: Bad manifest idx 0 " << raw_obj.oid << dendl;
	}
	else {
	  continue;
	}
      }
      librados::IoCtx ioctx = obj.ioctx;
      //p_obj_ioctx->set_namespace(itr->get_nspace());
      //p_obj_ioctx->locator_set_key(itr->get_locator());
      ldpp_dout(dpp, 1) << "dedup::removing tail object: " << raw_obj.oid<< dendl;
      ret = ioctx.remove(raw_obj.oid);
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::rollback_ref_by_manifest(const string &ref_tag, RGWObjManifest &manifest)

  {
    unsigned idx = 0;
    int ret_code = 0;
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, null_yield);
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      rgw_rados_ref obj;
      int local_ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (local_ret < 0) {
	ret_code = local_ret;
	ldpp_dout(dpp, 0) << "manifest::failed to open rados context for " << obj << dendl;
	// skip bad objects, nothing we can do
	continue;
      }
      if (idx == 0) {
	if (raw_obj.oid.find("shadow") != std::string::npos) {
	  ldpp_dout(dpp, 0) << __func__ << "::dedup:: Bad manifest idx 0 " << raw_obj.oid << dendl;
	}
	else {
	  continue;
	}
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
  int Background::inc_ref_count_by_manifest(const string   &ref_tag, RGWObjManifest &manifest)
  {
    optional_yield oy{null_yield};
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, oy);
    rgw::AioResultList all_results;
    int ret = 0;
    unsigned idx = 0;
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      rgw_rados_ref obj;
      ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "manifest::failed to open rados context for " << obj << dendl;
	break;
      }
      if (idx == 0) {
	if (raw_obj.oid.find("shadow") != std::string::npos) {
	  ldpp_dout(dpp, 0) << __func__ << "::dedup:: Bad manifest idx 0 " << raw_obj.oid << dendl;
	}
	else {
	  continue;
	}
      }

      ObjectWriteOperation op;
      cls_refcount_get(op, ref_tag, true);
      ldpp_dout(dpp, 1) << "dedup::inc ref-count on tail object: " << raw_obj.oid << dendl;
      rgw::AioResultList completed = aio->get(obj.obj,
					      rgw::Aio::librados_op(obj.ioctx, std::move(op), oy),
					      cost, id);
      ret = rgw::check_for_errors(completed);
      all_results.splice(all_results.end(), completed);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "manifest::ERROR: failed to copy obj=" << obj << ", the error code = " << ret << dendl;
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
	ldpp_dout(dpp, 0) << "manifest::ERROR: failed to drain ios, the error code = " << ret <<dendl;
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
					      rgw::Aio::librados_op(obj.ioctx, std::move(op), oy),
					      cost, id);
      ret2 = rgw::check_for_errors(completed);
      if (ret2 < 0) {
	ldpp_dout(dpp, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << aio_res.obj << dendl;
      }
    }
    completed = aio->drain();
    ret2 = rgw::check_for_errors(completed);
    if (ret2 < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to drain rollback ios, the error code = " << ret2 <<dendl;
    }

    return ret;
  }
#if 0
  //---------------------------------------------------------------------------
  int Background::dedup_object(const string     &src_bucket_name,
			       const string     &src_obj_name,
			       rgw::sal::Bucket *tgt_bucket,
			       rgw::sal::Object *tgt_obj,
			       uint64_t          tgt_size,
			       bufferlist       &tgt_etag_bl)
  {
    const string tgt_etag = tgt_etag_bl.to_str();
    const uint16_t tgt_num_parts = get_num_parts(tgt_etag);
    unique_ptr<rgw::sal::Bucket> src_bucket;
    const string src_tenant_name, src_bucket_id;
    rgw_bucket b{src_tenant_name, src_bucket_name, src_bucket_id};
    int ret = driver->load_bucket(dpp, b, &src_bucket, null_yield);
    if (unlikely(ret != 0)) {
      derr << "ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    unique_ptr<rgw::sal::Object> src_obj = src_bucket->get_object(src_obj_name);
    ret = src_obj->get_obj_attrs(null_yield, dpp);
    if (unlikely(ret < 0)) {
      derr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
      return 1;
    }

    auto itr = src_obj->get_attrs().find(RGW_ATTR_ETAG);
    if (itr != src_obj->get_attrs().end()) {
      const string src_etag = itr->second.to_str();
      if (src_etag != tgt_etag) {
	derr << __func__ << "::ERROR::src_etag= " << src_etag << " != tgt_etag= " << tgt_etag << dendl;
	return -1;
      }
      const uint16_t src_num_parts = get_num_parts(src_etag);
      if (src_num_parts != tgt_num_parts) {
	derr << __func__ << "::ERROR::src_num_parts= " << src_num_parts
	     << " != tgt_num_parts= " << tgt_num_parts << dendl;
	return -1;
      }
    }

    itr = src_obj->get_attrs().find(RGW_ATTR_MANIFEST);
    if (itr != src_obj->get_attrs().end()) {
      string ref_tag = rgw_obj::calc_refcount_tag_hash(tgt_bucket->get_name(),
						       tgt_obj->get_name());
      //ldpp_dout(dpp, 0) << __func__ << "::GBH::bucket=" << tgt_bucket->get_name()<< ", obj=" << tgt_obj->get_name() << ", ref_tag=" << ref_tag <<dendl;
      bufferlist &src_manifest_bl = itr->second;
      RGWObjManifest src_manifest;
      try {
	auto bl_iter = src_manifest_bl.cbegin();
	decode(src_manifest, bl_iter);
      } catch (buffer::error& err) {
	derr << "ERROR: unable to decode manifest" << dendl;
	return EIO;
      }

      ret = inc_ref_count_by_manifest(ref_tag, src_manifest);
      if (ret == 0) {
	RGWObjManifest tgt_manifest;
	itr = tgt_obj->get_attrs().find(RGW_ATTR_MANIFEST);
	if (itr == tgt_obj->get_attrs().end()) {
	  derr << __func__ << "::ERROR::failed getting TGT manifest for obj " << src_obj_name << dendl;
	  rollback_ref_by_manifest(ref_tag, src_manifest);
	}
	try {
	  auto bl_iter = itr->second.cbegin();
	  decode(tgt_manifest, bl_iter);
	} catch (buffer::error& err) {
	  derr << __func__ << "::ERROR: unable to decode manifest for obj " << src_obj_name << dendl;
	  rollback_ref_by_manifest(ref_tag, src_manifest);
	}

	// TBD::
	// WE need to update attribute atomicly using a CLS -
	// 1) Overwrite manifest
	// 2) Remove Tail-Tag
#if 1
	// use explicit tail_placement as the dedup could be on another bucket
	const rgw_bucket_placement& tail_placement = src_manifest.get_tail_placement();
	if (tail_placement.bucket.name.empty()) {
	  //ldpp_dout(dpp, 1) << "dedup::updating tail placement" << dendl;
	  // TBD: is this enough to define an rgw_bucket???
	  //rgw_bucket b{src_tenant_name, src_bucket_name, src_bucket_id};
	  src_manifest.set_tail_placement(tail_placement.placement_rule, b);
	  // clear bufferlist first
	  src_manifest_bl.clear();
	  ceph_assert(src_manifest_bl.length() == 0);
	  encode(src_manifest, src_manifest_bl);
	}
#endif
	// overwrite TGT manifest
	//ldpp_dout(dpp, 1) << "dedup::set manifest on: " << tgt_obj->get_name() << dendl;
	tgt_obj->modify_obj_attrs(RGW_ATTR_MANIFEST, src_manifest_bl, null_yield, dpp);

	// do we need to set compression on the head object or is it set on tail?
	// RGW_ATTR_COMPRESSION

	// delete TAIL-TAG on TGT
	itr = tgt_obj->get_attrs().find(RGW_ATTR_TAIL_TAG);
	if (itr != tgt_obj->get_attrs().end()) {
	  //ldpp_dout(dpp, 1) << "dedup::delete tail on: " << tgt_obj->get_name() << dendl;
	  tgt_obj->delete_obj_attrs(dpp, RGW_ATTR_TAIL_TAG, null_yield);
	}

	// free tail objects based on TGT manifest
	free_tail_objs_by_manifest(ref_tag, tgt_manifest);
      }
    }

    //ldpp_dout(dpp, 1) << "dedup::copy " << src_bucket_name << "::" << src_obj_name << " to " << tgt_bucket->get_name() << "::" << tgt_obj->get_name() << dendl;
    return 0;
  }
#endif
  //---------------------------------------------------------------------------
  int Background::add_disk_record(const rgw::sal::Bucket *p_bucket,
				  const rgw::sal::Object *p_obj,
				  work_shard_t            worker_id,
				  uint64_t                obj_size)
  {
    parsed_etag_t parsed_etag;
    const rgw::sal::Attrs& attrs = p_obj->get_attrs();
    auto itr = attrs.find(RGW_ATTR_ETAG);
    if (itr != attrs.end()) {
      parse_etag_string(itr->second.to_str(), &parsed_etag);
    }
    else {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: no etag" << dendl;
      return -1;
    }

    md5_shard_t md5_shard = parsed_etag.md5_low % MAX_MD5_SHARD;
    auto p_disk = p_disk_arr[md5_shard][worker_id];
    if (p_disk) {
      int ret = p_disk->add_record(store, rados, p_bucket, p_obj, &parsed_etag, obj_size);
      if (unlikely(ret != 0)) {
	return ret;
      }

      return 0;
    }
    else {
      derr << __func__ << "::ERROR::Undefined disk array!!" << dendl;
      return -1;
    }
  }

  //---------------------------------------------------------------------------
  int Background::add_record_to_dedup_table(const disk_record_t *p_rec,
					    disk_block_id_t block_id,
					    record_id_t rec_id)
  {
    key_t key(p_rec->s.md5_high, p_rec->s.md5_low, p_rec->s.size_4k_units,
	      p_rec->s.num_parts);
    bool valid_sha256 = p_rec->s.flags & RGW_DEDUP_FLAG_SHA256;
    bool shared_manifest = p_rec->s.flags & RGW_DEDUP_FLAG_SHARED_MANIFEST;
    return d_table.add_entry(&key, block_id, rec_id, shared_manifest, valid_sha256);
  }

  //---------------------------------------------------------------------------
  int Background::dedup_object(const disk_record_t *p_src_rec,
			       const disk_record_t *p_tgt_rec,
			       const key_t *p_key)
  {
    RGWObjManifest src_manifest;
    try {
      auto bl_iter = p_src_rec->manifest_bl.cbegin();
      decode(src_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: unable to decode manifest" << dendl;
      return -1;
    }

    RGWObjManifest tgt_manifest;
    try {
      auto bl_iter = p_tgt_rec->manifest_bl.cbegin();
      decode(src_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: unable to decode manifest" << dendl;
      return -1;
    }

    // TBD: need to read target object attributes (RGW_ATTR_TAIL_TAG/RGW_ATTR_TAG)
    string ref_tag = rgw_obj::calc_refcount_tag_hash(p_tgt_rec->bucket_name,
						     p_tgt_rec->obj_name);
    int ret = inc_ref_count_by_manifest(ref_tag, src_manifest);
    if (ret == 0) {
      // TBD:: WE need to update attribute atomicly using a CLS
      // First verify TGT objects state(MD5/SHA256/versionm/timestamp)
      // If all looks fine overwrite manifest and return status
      // If return status shows success we will free tail-objects on TGT
      // CLS_MODIFY_MANIFEST(src_manifest, tgt_manifest, p_key, version/timestamp);
#if 1
      unique_ptr<rgw::sal::Bucket> tgt_bucket;
      const string tgt_tenant_name, tgt_bucket_id;
      rgw_bucket b{tgt_tenant_name, p_tgt_rec->bucket_name, tgt_bucket_id};
      ret = driver->load_bucket(dpp, b, &tgt_bucket, null_yield);
      if (unlikely(ret != 0)) {
	derr << __func__ << "::ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
	return -ret;
      }
      unique_ptr<rgw::sal::Object> tgt_obj = tgt_bucket->get_object(p_tgt_rec->obj_name);
      //if () {}
      // TBD3 - remove the const or use cast
      bufferlist src_manifest_bl = p_src_rec->manifest_bl;
      tgt_obj->modify_obj_attrs(RGW_ATTR_MANIFEST, src_manifest_bl, null_yield, dpp);
#endif
      // free tail objects based on TGT manifest
      free_tail_objs_by_manifest(ref_tag, tgt_manifest);
    }

    // do we need to set compression on the head object or is it set on tail?
    // RGW_ATTR_COMPRESSION
    return ret;
  }

  //---------------------------------------------------------------------------
  // We purged all entries not marked for-dedup (i.e. singleton bit is set) from the table
  //   so all entries left are sources of dedup with multiple copies.
  // If the record is marked as Shared-Manifest-Object -> skip it
  // if the record's key doesn’t exist in table -> skip it (it is a singleton and it was purged)
  // If the record block-index matches the hashtable entry -> skip it (it is the SRC object)
  // All other entries are Dedicated-Manifest-Objects with a valid SRC object
  //
  int Background::try_deduping_record(const disk_record_t *p_tgt_rec,
				      disk_block_id_t      block_id,
				      record_id_t          rec_id,
				      md5_shard_t          md5_shard)
  {
    if (p_tgt_rec->s.flags & RGW_DEDUP_FLAG_SHARED_MANIFEST) {
      // record holds a shared_manifest object so can't be a dedup target
      return 0;
    }

    disk_block_id_t src_block_id;
    record_id_t src_rec_id;
    bool is_shared_manifest;
    key_t key(p_tgt_rec->s.md5_high, p_tgt_rec->s.md5_low, p_tgt_rec->s.size_4k_units, p_tgt_rec->s.num_parts);
    int ret = d_table.get_block_id(&key, &src_block_id, &src_rec_id, &is_shared_manifest);
    if (ret != 0) {
      // record has no valid entry in table because it is a singleton
      return 0;
    }

    if (block_id == src_block_id && rec_id == src_rec_id) {
      // the table entry point to this record which means it is a dedup source so nothing to do
      return 0;
    }

    // This records is a dedup target with source record on source_block_id
    disk_record_t src_rec;
    ret = load_record(store, &src_rec, src_block_id, src_rec_id, md5_shard, &key, dpp);
    if (ret == 0) {
      ret = dedup_object(&src_rec, p_tgt_rec, &key);

      // mark the SRC object as a providor of a shared manifest
      if (!is_shared_manifest) {
	// set the shared manifest flag in the dedup table
	d_table.set_shared_manifest_mode(&key, src_block_id, src_rec_id);

	// TBD:
	// Set Shared-Manifest attribute in the SRC-OBJ
      }

    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::run_dedup_step(dedup_step_t step,
				 md5_shard_t md5_shard,
				 work_shard_t worker_id,
				 uint32_t *p_rec_count /* IN-OUT */)
  {
    const int MAX_OBJ_LOAD_FAILURE = 3;
    bool      has_more = true;
    uint32_t  seq_number = 0;
    int       failure_count = 0;
    ldpp_dout(dpp, 0)  << "\n>>>>>>>>>>>" << __func__
		       << ((step==STEP_BUILD_TABLE) ? "::Build Table" : "::REMOVE_DUPS")
		       << "::worker_id=" << (uint32_t)worker_id
		       << ", md5_shard=" << (uint32_t)md5_shard << dendl;
    while (has_more) {
      bufferlist bl;
      int ret = load_slab(store, bl, md5_shard, worker_id, seq_number, dpp);
      if (unlikely(ret < 0)) {
	derr << __func__ << "::ERROR::Failed loading object!! md5_shard=" << (uint32_t)md5_shard
	     << ", worker_id=" << (uint32_t)worker_id << ", seq_number=" << seq_number << dendl;
	// skip to the next SLAB stopping after 3 bad objects
	if (failure_count++ < MAX_OBJ_LOAD_FAILURE) {
	  seq_number += DISK_BLOCK_COUNT;
	  continue;
	}
	else {
	  return -1;
	}
      }
      auto bl_itr = bl.cbegin();
      for (uint32_t block_num = 0; block_num < DISK_BLOCK_COUNT; block_num++, seq_number++) {
	disk_block_id_t disk_block_id(worker_id, seq_number);
	const char *p = nullptr;
	size_t n = bl_itr.get_ptr_and_advance(sizeof(disk_block_t), &p);
	if (n == sizeof(disk_block_t)) {
	  disk_block_t *p_disk_block = (disk_block_t*)p;
	  disk_block_header_t *p_header = p_disk_block->get_header();
	  p_header->deserialize();
	  if (p_header->verify(disk_block_id, dpp) != 0) {
	    return -1;
	  }

	  for (unsigned rec_id = 0; rec_id < p_header->rec_count; rec_id++) {
	    unsigned offset = p_header->rec_offsets[rec_id];
	    // We deserialize the record inside the CTOR
	    disk_record_t rec(p + offset);
	    if (step == STEP_BUILD_TABLE) {
	      add_record_to_dedup_table(&rec, disk_block_id, rec_id);
	    }
	    else if (step == STEP_REMOVE_DUPLICATES) {
	      try_deduping_record(&rec, disk_block_id, rec_id, md5_shard);
	    }
	    else {
	      ceph_abort("unexpected step");
	    }
	  }
	  *p_rec_count += p_header->rec_count;
	  has_more = (p_header->offset == BLOCK_MAGIC);
	  ceph_assert(p_header->offset == BLOCK_MAGIC || p_header->offset == LAST_BLOCK_MAGIC);
	  if (!has_more) {
	    ldpp_dout(dpp, 0)  << __func__ << "::No more blocks! block_id=" << disk_block_id
			       << ", rec_count=" << p_header->rec_count << dendl;
	    break;
	  }
	}
	else {
	  // TBD - How to test this path?
	  // It requires that rados.read() will allocate bufferlist in small chunks
	  derr << __func__ << "::unexpected short read n=" << n << dendl;
	  break;
	}
      }
      ldpp_dout(dpp, 1) <<__func__ << "::Finished processing slab at seq_number=" << seq_number << dendl;
    }
    if (step == STEP_BUILD_TABLE) {
      uint32_t singleton_count = 0, duplicate_count = 0;
      d_table.count_duplicates(&singleton_count, &duplicate_count);
      ldpp_dout(dpp, 1) <<__func__ << "::dedup::singleton_count=" << singleton_count
			<< ", duplicate_count=" << duplicate_count << dendl;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::process_single_entry(rgw::sal::Bucket           *bucket,
				       const rgw_bucket_dir_entry &entry,
				       work_shard_t                worker_id,
				       const ceph::real_time      &last_scan_time)
  {
    //ldpp_dout(dpp, 0) << __func__ << ": got " << entry.key << dendl;
    unique_ptr<rgw::sal::Object> obj = bucket->get_object(entry.key);
    if (unlikely(!obj)) {
      derr << "ERROR: failed bucket->get_object(" << entry.key << ")" << dendl;
      return 1;
    }
    int ret = obj->get_obj_attrs(null_yield, dpp);
    if (unlikely(ret < 0)) {
      derr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
      return 1;
    }
    uint64_t size = obj->get_obj_size();
#if 0
    if (size <= 4*1024*1024) {
      // dedup only useful for objects bigger than 4MB
      return 0;
    }
#endif
    ceph::real_time mtime = obj->get_mtime();
    //ldpp_dout(dpp, 1) <<__func__ << "::entry.key=" << entry.key << "::size=" << size << dendl;
    // check if object was modified since last scan
    if (mtime < last_scan_time) {
      return 0;
    }

    if (obj->get_attrs().find(RGW_ATTR_CRYPT_MODE) != obj->get_attrs().end()) {
      ldpp_dout(dpp, 1) <<__func__ << "::Skipping encrypted object " << entry.key << dendl;
      return 0;
    }

    // TBD: We should be able to support RGW_ATTR_COMPRESSION when all copies are compressed
    if (obj->get_attrs().find(RGW_ATTR_COMPRESSION) != obj->get_attrs().end()) {
      ldpp_dout(dpp, 1) <<__func__ << "::Skipping compressed object " << entry.key << dendl;
      return 0;
    }

    return add_disk_record(bucket, obj.get(), worker_id, size);
  }

  //---------------------------------------------------------------------------
  int Background::process_bucket_shards(rgw::sal::Bucket      *bucket,
					std::map<int, string> &oids,
					librados::IoCtx       &ioctx,
					work_shard_t           worker_id,
					const ceph::real_time &last_scan_time,
					uint64_t              *p_obj_count /* IN-OUT */)
  {
    const uint32_t num_shards = oids.size();
    uint32_t current_shard = worker_id;
    rgw_obj_index_key marker; // start with an empty marker
    const string null_prefix, null_delimiter;
    const bool list_versions = true;
    const int max_entries = 1000;
    uint32_t obj_count = 0;

    ldpp_dout(dpp, 0) << __func__ << "::current_shard=" << current_shard
		      << ", worker_id=" << (uint32_t)worker_id << dendl;

    while (current_shard < num_shards ) {
      const string& oid = oids[current_shard];
      rgw_cls_list_ret result;
      librados::ObjectReadOperation op;
      cls_rgw_bucket_list_op(op, marker, null_prefix, null_delimiter,
			     max_entries, list_versions, &result);
      int ret = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, null_yield);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << __func__ << "::failed rgw_rados_operate() ret=" << ret << dendl;
	return ret;
      }

      *p_obj_count += result.dir.m.size();
      obj_count += result.dir.m.size();
      for (auto& entry : result.dir.m) {
	const rgw_bucket_dir_entry& dirent = entry.second;
	if (unlikely((!dirent.exists && !dirent.is_delete_marker()) || !dirent.pending_map.empty())) {
	  // TBD: should we bailout ???
	  ldpp_dout(dpp, 0) << __func__ << "::ERROR: calling check_disk_state bucket="
			    << bucket->get_name() << " entry=" << dirent.key << dendl;
	  // make sure we're advancing marker
	  marker = dirent.key;
	  continue;
	}
	marker = dirent.key;
	ret = process_single_entry(bucket, dirent, worker_id, last_scan_time);
      }
      // TBD: advance marker only once here!
      if (!result.is_truncated) {
	// if we reached the end of the shard read next shard
	ldpp_dout(dpp, 0) << __func__ << "::" << bucket->get_name() << "::curr_shard=" << current_shard
			  << ", next shard=" << current_shard + MAX_WORK_SHARD << dendl;
	current_shard += MAX_WORK_SHARD;
	marker = rgw_obj_index_key(); // reset marker to empty index
      }
    }
    ldpp_dout(dpp, 0) << __func__ << "::Finished processing Bucket " << bucket->get_name()
		      << ", num_shards=" << num_shards << ", obj_count=" << obj_count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::list_bucket_by_shard(const string   &bucket_name,
				       ceph::real_time last_scan_time,
				       work_shard_t    worker_id,
				       uint64_t       *p_obj_count /* IN-OUT */)
  {
    //ldpp_dout(dpp, 0) << __func__ << "::bucket_name=" << bucket_name << dendl;
    unique_ptr<rgw::sal::Bucket> bucket;
    const string tenant_name, empty_bucket_id;
    rgw_bucket b{tenant_name, bucket_name, empty_bucket_id};
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      derr << "ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    utime_t ut(bucket->get_modification_time());
    if (bucket_set[bucket_name].modification_time == ut && worker_id == 0) {
      return 0;
    }
    bucket_set[bucket_name].modification_time = ut;

    const std::string bucket_id = bucket->get_key().get_key();
    RGWBucketInfo bucket_info;
    ret = rados->get_bucket_instance_info(bucket_id, bucket_info, nullptr, nullptr, null_yield, dpp);
    if (ret < 0) {
      if (ret == -ENOENT) {
	// probably raced with bucket removal
	ldpp_dout(dpp, 0) << __func__ << "::ret == -ENOENT" << dendl;
	return 0;
      }
      ldpp_dout(dpp, 0) << __func__ << ":: ERROR: get_bucket_instance_info() returned ret=" << ret << dendl;
      return ret;
    }
    const rgw::bucket_index_layout_generation idx_layout = bucket_info.layout.current_index;
    librados::IoCtx ioctx;
    // objects holding the bucket-listings
    std::map<int, std::string> oids;
    ret = store->svc()->bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt,
						    idx_layout, &ioctx, &oids, nullptr);
    if (ret < 0) {
      return ret;
    }
#if 0
    const uint32_t num_shards = oids.size();
    ldpp_dout(dpp, 0) << __func__ << "::bucket_name=" << bucket_name
		      << ", worker_id=" << (uint32_t)worker_id << ", num_shards=" << num_shards << dendl;
#endif
    return process_bucket_shards(bucket.get(), oids, ioctx, worker_id, last_scan_time, p_obj_count);
  }

  //---------------------------------------------------------------------------
  int Background::scan_bucket_list(bool deep_scan, ceph::real_time last_scan_time)
  {
    static bool first_time = true;
    if (!first_time) return 0;
    if (deep_scan) {
      first_time = false;
    }
    bucket_state_t null_bucket_state;
    int ret = 0;
    std::string section("bucket");
    std::string marker;
    void *handle = nullptr;
    uint64_t obj_count = 0;
    ret = driver->meta_list_keys_init(dpp, section, marker, &handle);

    bool has_more = true;
    while (ret == 0 && has_more) {
      std::list<std::string> buckets;
      constexpr int max_keys = 1000;
      ret = driver->meta_list_keys_next(dpp, handle, max_keys, buckets, &has_more);
      if (ret == 0) {
	for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	  ldpp_dout(dpp, 0) <<__func__ << "::listing buckets:: worker_id=" << (uint32_t)worker_id << dendl;
	  for (auto& bucket_name : buckets) {
	    if (bucket_set.try_emplace(bucket_name, null_bucket_state).second) {
	      ldpp_dout(dpp, 1) << __func__ << "::" << bucket_name << "::"
				<< (deep_scan ? "Deep Scan" : "Shallow Scan") << dendl;
	    }
	    if (deep_scan) {
	      //ldpp_dout(dpp, 0) <<__func__ << "::list_bucket_by_shard:: bucket_name=" << bucket_name << dendl;
	      list_bucket_by_shard(bucket_name, last_scan_time, worker_id, &obj_count);
	    }
	  }
	}
	driver->meta_list_keys_complete(handle);
      }
      else {
	derr << __func__ << "::failed driver->meta_list_keys_next()" << dendl;
      }
    }

    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	p_disk_arr[md5_shard][worker_id]->flush_disk_records(store, rados);
      }
    }

    if (deep_scan && obj_count) {
      uint32_t rec_count = 0;
      ldpp_dout(dpp, 0) <<__func__ << "::scanned objects count = " << obj_count << dendl;
      for (md5_shard_t md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
	for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	  //build_dedup_table(md5_shard, worker_id);
	  run_dedup_step(STEP_BUILD_TABLE, md5_shard, worker_id, &rec_count);
	}
      }
      ldpp_dout(dpp, 0) <<__func__ << "::BUILD_TABLE::scanned records count = " << rec_count << dendl;
      d_table.remove_singletons_and_redistribute_keys();
      rec_count = 0;
      for (md5_shard_t md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
	for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
	  //dedup_objects(md5_shard, worker_id);
	  run_dedup_step(STEP_REMOVE_DUPLICATES, md5_shard, worker_id, &rec_count);
	}
      }
      ldpp_dout(dpp, 0) <<__func__ << "::REMOVE_DUPLICATES::scanned records count = " << rec_count << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::setup()
  {
    ceph::real_time last_scan_time;
    return scan_bucket_list(false, last_scan_time);
  }

  //---------------------------------------------------------------------------
  void Background::run()
  {
    if (setup() != 0) {
      derr << "failed setup()" << dendl;
      return;
    }

    ceph::real_time last_scan_time;
    ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__ << "::dedup::main loop" << dendl;
    while (!stopped) {
      if (pause_req) {
	paused = true;
	cond.notify_all();
	ldpp_dout(dpp, 10) << "Dedup background thread paused" << dendl;
	std::unique_lock cond_lock(cond_mutex);
	cond.wait(cond_lock, [this]{return !paused || stopped;});
	if (stopped) {
	  ldpp_dout(dpp, 10) << "Dedup background thread stopped" << dendl;
	  return;
	}
	ldpp_dout(dpp, 10) << "Dedup background thread resumed" << dendl;
      }

      // -->
      // Do Work !!!
      // -->
      int ret = scan_bucket_list(true, last_scan_time);
      if (ret != 0) {
	derr << "failed scan_bucket_list(), ret=" << ret << dendl;
	return;
      }
      last_scan_time = ceph_clock_now().to_real_time();
      std::unique_lock cond_lock(cond_mutex);
      cond.wait_for(cond_lock, std::chrono::seconds(execute_interval), [this]{return stopped;});
    }

    ldpp_dout(dpp, 10) << "Dedup background thread stopped" << dendl;
  }
}; //namespace rgw::dedup
