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

#include "cls/cmpxattr/client.h"
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
using namespace ::cls::cmpxattr;
#include "rgw_sal_rados.h"
#include "rgw_dedup_table.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup.h"
#include "rgw_dedup_store.h"
#include "rgw_dedup_cluster.h"
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

    if (init_dedup_pool_ioctx(rados, dpp, p_dedup_cluster_ioctx) != 0) {
      //return -1;
      return;
    }
  }

  //---------------------------------------------------------------------------
  Background::Background(rgw::sal::Driver* _driver,
			 CephContext* _cct,
			 int _execute_interval) :
    driver(_driver),
    dp(_cct, dout_subsys, "dedup background: "),
    dpp(&dp),
    cct(_cct),
    d_table(16*1024*1024, dpp), // 16M entries per-shard
    d_cluster(dpp),
    d_execute_interval(_execute_interval)
  {
    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      // TBD: check allocation success
      p_disk_arr[md5_shard] = new disk_block_array_t(dpp, md5_shard);
    }
    p_dedup_cluster_ioctx = new librados::IoCtx();
    ceph_assert(p_dedup_cluster_ioctx);

    d_heart_beat_last_update = ceph_clock_now();
    d_heart_beat_max_elapsed_sec = 3;
  }

  //---------------------------------------------------------------------------
  Background::~Background()
  {
    // TBD:
    // Do we need to stop the runner thread first ???
    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      if (p_disk_arr[md5_shard]) {
	delete p_disk_arr[md5_shard];
      }
    }
    delete p_dedup_cluster_ioctx;
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
	ldpp_dout(dpp, 1) << "manifest::failed to open rados context for " << obj << dendl;
	continue;
      }
      librados::IoCtx ioctx = obj.ioctx;
      ldpp_dout(dpp, 20) << __func__ << "::removing tail object: " << raw_obj.oid << dendl;
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
	ldpp_dout(dpp, 10) << __func__ << "::[" << idx <<"] Skip HEAD OBJ: " << raw_obj.oid << dendl;
	continue;
      }

      rgw_rados_ref obj;
      int local_ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (local_ret < 0) {
	ret_code = local_ret;
	ldpp_dout(dpp, 1) << "manifest::failed to open rados context for " << obj << dendl;
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
    //optional_yield oy{null_yield};
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, null_yield);
    rgw::AioResultList all_results;
    int ret = 0;
    unsigned idx = 0;
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      if (oid == raw_obj.oid) {
	ldpp_dout(dpp, 10) << __func__ << "::[" << idx <<"] Skip HEAD OBJ: " << raw_obj.oid << dendl;
	continue;
      }

      rgw_rados_ref obj;
      ret = rgw_get_rados_ref(dpp, rados_handle, raw_obj, &obj);
      if (ret < 0) {
	ldpp_dout(dpp, 1) << "manifest::failed to open rados context for " << obj << dendl;
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
	ldpp_dout(dpp, 1) << "manifest::ERROR: **failed to drain ios, the error code = " << ret <<dendl;
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
	ldpp_dout(dpp, 1) << "ERROR: cleanup after error failed to drop reference on obj=" << aio_res.obj << dendl;
      }
    }
    completed = aio->drain();
    ret2 = rgw::check_for_errors(completed);
    if (ret2 < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to drain rollback ios, the error code = " << ret2 <<dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::add_disk_record(const rgw::sal::Bucket *p_bucket,
				  const rgw::sal::Object *p_obj,
				  uint64_t                obj_size)
  {
    parsed_etag_t parsed_etag;
    const rgw::sal::Attrs& attrs = p_obj->get_attrs();
    auto itr = attrs.find(RGW_ATTR_ETAG);
    if (itr != attrs.end()) {
      parse_etag_string(itr->second.to_str(), &parsed_etag);
      ldpp_dout(dpp, 20) << __func__ << "::(1)::" << p_bucket->get_name()
			 << "/" << p_obj->get_name()
			 << "::num_parts=" << parsed_etag.num_parts
			 << "::ETAG=" << std::hex << parsed_etag.md5_high
			 << parsed_etag.md5_low << std::dec << dendl;
    }
    else {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: no etag" << dendl;
      return -1;
    }

    md5_shard_t md5_shard = parsed_etag.md5_low % MAX_MD5_SHARD;
    auto p_disk = p_disk_arr[md5_shard];
    if (p_disk) {
      int ret = p_disk->add_record(p_dedup_cluster_ioctx, p_bucket, p_obj, &parsed_etag, obj_size);
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
    bool has_valid_sha256 = p_rec->has_valid_sha256();
    bool has_shared_manifest = p_rec->has_shared_manifest();
    ldpp_dout(dpp, 20) << __func__ << "::bucket=" << p_rec->bucket_name
		       << ", obj=" << p_rec->obj_name << ", block_id="
		       << (uint32_t)block_id << ", rec_id=" << (uint32_t)rec_id
		       << ", shared_manifest=" << has_shared_manifest << dendl;
    ldpp_dout(dpp, 20) << __func__ << "::(2)::" << p_rec->bucket_name
		       << "/" << p_rec->obj_name
		       << "::num_parts=" << p_rec->s.num_parts
		       << "::ETAG=" << std::hex << p_rec->s.md5_high
		       << p_rec->s.md5_low << std::dec << dendl;

    return d_table.add_entry(&key, block_id, rec_id, has_shared_manifest, has_valid_sha256);
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static int get_ioctx1(const DoutPrefixProvider* const dpp,
					rgw::sal::Driver* driver,
					RGWRados* rados,
					const std::string &bucket_name,
					const std::string& obj_name,
					librados::IoCtx *p_ioctx,
					std::string *oid)
  {
    unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket b{"", bucket_name, ""};
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      derr << "ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    //const std::string bucket_name("b908ebb0-d7d6-431e-8bec-6ca176387468.4172.1");
    const std::string bucket_id = bucket->get_key().bucket_id;
    *oid = bucket_id + "_" + obj_name;
    //ldpp_dout(dpp, 0) << __func__ << "::OID=" << oid << " || bucket_id=" << bucket_id << dendl;
    rgw_pool data_pool;
    //rgw_obj obj{bucket->get_key(), obj_name};
    rgw_obj obj{bucket->get_key(), *oid};
    if (!rados->get_obj_data_pool(bucket->get_placement_rule(), obj, &data_pool)) {
      ldpp_dout(dpp, 1) << "failed to get data pool for bucket '"
			<< bucket->get_name() <<"' when writing logging object" << dendl;
      return -EIO;
    }
    ret = rgw_init_ioctx(dpp, rados->get_rados_handle(), data_pool, *p_ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to get IO context for logging object from data pool:"
			<< data_pool.to_str() << dendl;
      return -EIO;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static int get_ioctx2(const DoutPrefixProvider* const dpp,
					rgw::sal::Driver* driver,
					rgw::sal::RadosStore *store,
					const std::string &bucket_name,
					const std::string &obj_name,
					librados::IoCtx *p_ioctx)
  {
    unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket b{"", bucket_name, ""};
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      derr << "ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    const std::string bucket_id_key = bucket->get_key().get_key();
    RGWBucketInfo bucket_info;
    ret = store->getRados()->get_bucket_instance_info(bucket_id_key, bucket_info, nullptr,
						      nullptr, null_yield, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << ":: ERROR: get_bucket_instance_info() returned ret="
			<< ret << dendl;
      return -1;
    }
    const std::string bucket_id = bucket->get_key().bucket_id;
    const std::string oid = bucket_id + "_" + obj_name;
    ldpp_dout(dpp, 20) << __func__ << "::OID=" << oid << " || bucket_id=" << bucket_id << dendl;

    rgw_obj obj(b, oid);
    ret = store->get_obj_head_ioctx(dpp, bucket_info, obj, p_ioctx);
    if (ret < 0) {
      cerr << "ERROR: get_obj_head_ioctx() returned ret=" << ret << std::endl;
      return ret;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  static void init_cmp_pairs(const disk_record_t *p_rec,
			     const bufferlist    &etag_bl,
			     ComparisonMap       *p_cmp_pairs)
  {
    (*p_cmp_pairs)[RGW_ATTR_ETAG] = etag_bl;
    (*p_cmp_pairs)[RGW_ATTR_MANIFEST] = p_rec->manifest_bl;

    if (p_rec->s.flags.has_valid_sha256() ) {
      bufferlist bl;
      sha256_to_bufferlist(p_rec->s.sha256[0], p_rec->s.sha256[1],
			   p_rec->s.sha256[2], p_rec->s.sha256[3], &bl);
      (*p_cmp_pairs)[RGW_ATTR_SHA256] = bl;
    }
  }

  //---------------------------------------------------------------------------
  int Background::dedup_object(const disk_record_t *p_src_rec,
			       const disk_record_t *p_tgt_rec,
			       bool                 has_shared_manifest_src,
			       bool                 src_has_sha256)
  {
    RGWObjManifest src_manifest;
    try {
      auto bl_iter = p_src_rec->manifest_bl.cbegin();
      decode(src_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: bad src manifest" << dendl;
      return -1;
    }
    RGWObjManifest tgt_manifest;
    try {
      auto bl_iter = p_tgt_rec->manifest_bl.cbegin();
      decode(tgt_manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: bad tgt manifest" << dendl;
      return -1;
    }
    ldpp_dout(dpp, 20) << __func__ << "::DEDUP From: "
		       << p_src_rec->bucket_name << "/" << p_src_rec->obj_name << " -> "
		       << p_tgt_rec->bucket_name << "/" << p_tgt_rec->obj_name << dendl;

    bufferlist etag_bl;
    etag_to_bufferlist(p_tgt_rec->s.md5_high, p_tgt_rec->s.md5_low, p_tgt_rec->s.num_parts, &etag_bl);
    ldpp_dout(dpp, 20) << __func__ << "::num_parts=" << p_tgt_rec->s.num_parts
		       << "::ETAG=" << etag_bl.to_str() << dendl;
    int ret;
    bufferlist shared_manifest_hash_bl;
    // TBD1: used shorter hash (64bit instead of 160bit)
    crypto::digest<crypto::SHA1>(p_src_rec->manifest_bl).encode(shared_manifest_hash_bl);
    librados::ObjectWriteOperation src_op, tgt_op;
    ComparisonMap src_cmp_pairs, tgt_cmp_pairs;
    init_cmp_pairs(p_src_rec, etag_bl, &src_cmp_pairs);
    init_cmp_pairs(p_tgt_rec, etag_bl, &tgt_cmp_pairs);
    map<string, bufferlist> src_set_pairs = {{RGW_ATTR_SHARE_MANIFEST, shared_manifest_hash_bl}};
    map<string, bufferlist> tgt_set_pairs = {{RGW_ATTR_SHARE_MANIFEST, shared_manifest_hash_bl},
					     {RGW_ATTR_MANIFEST, p_src_rec->manifest_bl}};
    ret = cmp_vals_set_vals(src_op, Mode::String, Op::EQ, src_cmp_pairs, src_set_pairs);
    ret = cmp_vals_set_vals(tgt_op, Mode::String, Op::EQ, tgt_cmp_pairs, tgt_set_pairs);

    std::string src_oid, tgt_oid;
    librados::IoCtx src_ioctx, tgt_ioctx;
    int ret1 = get_ioctx1(dpp, driver, rados, p_src_rec->bucket_name, p_src_rec->obj_name, &src_ioctx, &src_oid);
    int ret2 = get_ioctx1(dpp, driver, rados, p_tgt_rec->bucket_name, p_tgt_rec->obj_name, &tgt_ioctx, &tgt_oid);
    if (unlikely(ret1 != 0 || ret2 != 0)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed get_ioctx()" << dendl;
      return -1;
    }

    // TBD: need to read target object attributes (RGW_ATTR_TAIL_TAG/RGW_ATTR_TAG)
    string ref_tag = calc_refcount_tag_hash(p_tgt_rec->bucket_name, p_tgt_rec->obj_name);
    ldpp_dout(dpp, 20) << __func__ << "::ref_tag=" << ref_tag << dendl;
    ret = inc_ref_count_by_manifest(ref_tag, src_oid, src_manifest);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << __func__ << "::send TGT CLS" << dendl;
      ret = tgt_ioctx.operate(tgt_oid, &tgt_op);
      if (ret < 0) {
	ldpp_dout(dpp, 1) << __func__ << "::failed TGT rgw_rados_operate() ret=" << ret << dendl;
	rollback_ref_by_manifest(ref_tag, src_oid, src_manifest);
	return ret;
      }

      // free tail objects based on TGT manifest
      free_tail_objs_by_manifest(ref_tag, tgt_oid, tgt_manifest);

      if(!has_shared_manifest_src) {
	ldpp_dout(dpp, 20) << __func__ << "::send SRC CLS" << dendl;
	ret = src_ioctx.operate(src_oid, &src_op);
	if (ret < 0) {
	  ldpp_dout(dpp, 1) << __func__ << "::failed SRC rgw_rados_operate() ret=" << ret << dendl;
	  return ret;
	}
      }
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

  // we can withstand most errors moving to the next object
  // only report an error if we recived a stop scan request!
  //
  int Background::try_deduping_record(const disk_record_t *p_tgt_rec,
				      disk_block_id_t      block_id,
				      record_id_t          rec_id,
				      md5_shard_t          md5_shard,
				      md5_stats_t         *p_stats /* IN-OUT */)
  {
    ldpp_dout(dpp, 20) << __func__ << "::bucket=" << p_tgt_rec->bucket_name
		       << ", obj=" << p_tgt_rec->obj_name
		       << ", block_id=" << (uint32_t)block_id << ", rec_id=" << (uint32_t)rec_id
		       << ", md5_shard=" << (int)md5_shard << dendl;

    ldpp_dout(dpp, 20) << __func__ << "::(3)::md5_shard=" << (int)md5_shard
		       << "::"<< p_tgt_rec->bucket_name
		       << "/" << p_tgt_rec->obj_name
		       << "::num_parts=" << p_tgt_rec->s.num_parts
		       << "::ETAG=" << std::hex << p_tgt_rec->s.md5_high
		       << p_tgt_rec->s.md5_low << std::dec << dendl;

    p_stats->processed_objects ++;
    if (p_tgt_rec->s.flags.has_shared_manifest()) {
      // record holds a shared_manifest object so can't be a dedup target
      ldpp_dout(dpp, 20) << __func__ << "::skipped shared_manifest" << dendl;
      p_stats->skipped_shared_manifest++;
      return 0;
    }

    key_t key(p_tgt_rec->s.md5_high, p_tgt_rec->s.md5_low, p_tgt_rec->s.size_4k_units, p_tgt_rec->s.num_parts);
    dedup_table_t::value_t val;
    int ret = d_table.get_val(&key, &val);
    if (ret != 0) {
      // record has no valid entry in table because it is a singleton
      p_stats->skipped_singleton++;
      ldpp_dout(dpp, 20) << __func__ << ":(4)skipped singleton:" << p_tgt_rec->bucket_name
			 << "/" << p_tgt_rec->obj_name
			 << "::num_parts=" << p_tgt_rec->s.num_parts
			 << "::ETAG=" << std::hex << p_tgt_rec->s.md5_high
			 << p_tgt_rec->s.md5_low << std::dec << dendl;
      return 0;
    }

    disk_block_id_t src_block_id = val.block_idx;
    record_id_t src_rec_id = val.rec_id;
    bool has_shared_manifest = val.has_shared_manifest();
    bool src_has_sha256 = val.has_valid_sha256();
    if (block_id == src_block_id && rec_id == src_rec_id) {
      // the table entry point to this record which means it is a dedup source so nothing to do
      ldpp_dout(dpp, 20) << __func__ << "::skipped source-record" << dendl;
      p_stats->skipped_source_record++;
      return 0;
    }

    // This records is a dedup target with source record on source_block_id
    disk_record_t src_rec;
    ret = load_record(p_dedup_cluster_ioctx, &src_rec, src_block_id, src_rec_id, md5_shard, &key, dpp);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << __func__ << "::src_bucket=" << src_rec.bucket_name
			 << ", src_object=" << src_rec.obj_name << dendl;
      // verify that SRC and TGT records don't refer to the same physical object
      // This could happen in theory if we read the same objects twice
      if (src_rec.obj_name == p_tgt_rec->obj_name && src_rec.bucket_name == p_tgt_rec->bucket_name) {
	p_stats->skipped_duplicate++;
	ldpp_dout(dpp, 10) << __func__ << "::WARN: Duplicate records for object=" << src_rec.obj_name << dendl;
	return 0;
      }

      if (!src_has_sha256) {
	// TBD:  if SRC has no valid SHA256 -> read the full object and calc it
	// TBD2: after calculation need to store the SHA256 on the disk-record
	// TBD3  after storing SHA256 on disk-record -> update flags in dedup-table
	ldpp_dout(dpp, 20) << __func__ << "::No Valid SHA256 for: "
			   << src_rec.bucket_name << " / " << src_rec.obj_name << dendl;
      }

      if (!p_tgt_rec->s.flags.has_valid_sha256() ) {
	// TBD:  if TGT has no valid SHA256 ->
	//              read the full object, calc SHA256 adding it to in-memory record
      }

      // temporary code until SHA256 support is added
      if (src_has_sha256 && p_tgt_rec->s.flags.has_valid_sha256()) {
	if (memcmp(src_rec.s.sha256, p_tgt_rec->s.sha256, sizeof(src_rec.s.sha256)) != 0) {
	  derr << __func__ << "::SHA256 mismatch" << dendl;
	  p_stats->skipped_bad_sha256++;
	  return 0;
	}
      }
      else {
	p_stats->skip_sha256_cmp++;
      }
      ret = dedup_object(&src_rec, p_tgt_rec, has_shared_manifest, src_has_sha256);
      if (ret == 0) {
	p_stats->deduped_objects++;
	if (!src_has_sha256) {
	  // TBD: calculate SHA256 for SRC and set flag in table!!
	}
	// mark the SRC object as a providor of a shared manifest
	if (!has_shared_manifest) {
	  p_stats->set_shared_manifest++;
	  // set the shared manifest flag in the dedup table
	  d_table.set_shared_manifest_mode(&key, src_block_id, src_rec_id);
	}
	else {
	  ldpp_dout(dpp, 20) << __func__ << "::SRC object already marked as shared_manifest" << dendl;
	}
      }
    }
    // we can withstand most errors moving to the next object
    // only report an error if we recived a stop scan request!
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::run_dedup_step(dedup_step_t step,
				 md5_shard_t md5_shard,
				 work_shard_t worker_id,
				 md5_stats_t *p_stats /* IN-OUT */)
  {
    const int MAX_OBJ_LOAD_FAILURE = 3;
    const int MAX_BAD_BLOCKS = 3;
    bool      has_more = true;
    uint32_t  seq_number = 0;
    int       failure_count = 0;
    ldpp_dout(dpp, 10) << __func__
		       << ((step==STEP_BUILD_TABLE) ? "::Build Table" : "::REMOVE_DUPS")
		       << "::worker_id=" << (uint32_t)worker_id
		       << ", md5_shard=" << (uint32_t)md5_shard << dendl;
    while (has_more) {
      bufferlist bl;
      int ret = load_slab(p_dedup_cluster_ioctx, bl, md5_shard, worker_id, seq_number, dpp);
      if (unlikely(ret < 0)) {
	derr << __func__ << "::ERROR::Failed loading object!! md5_shard="
	     << (uint32_t)md5_shard << ", worker_id=" << (uint32_t)worker_id
	     << ", seq_number=" << seq_number << ", failure_count=" << failure_count << dendl;
	// skip to the next SLAB stopping after 3 bad objects
	if (failure_count++ < MAX_OBJ_LOAD_FAILURE) {
	  seq_number += DISK_BLOCK_COUNT;
	  continue;
	}
	else {
	  return -1;
	}
      }
      failure_count = 0;
      unsigned slab_rec_count = 0;
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
	    // move to next block until reaching a valid block
	    if (failure_count++ < MAX_BAD_BLOCKS) {
	      continue;
	    }
	    else {
	      derr << __func__ << "::Skipping slab with too many bad blocks::"
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
	    if (step == STEP_BUILD_TABLE) {
	      add_record_to_dedup_table(&rec, disk_block_id, rec_id);
	      p_stats->loaded_objects ++;
	      slab_rec_count++;
	    }
	    else if (step == STEP_REMOVE_DUPLICATES) {
	      try_deduping_record(&rec, disk_block_id, rec_id, md5_shard, p_stats);
	      slab_rec_count++;
	    }
	    else {
	      ceph_abort("unexpected step");
	    }
	  }

	  ret = check_and_update_md5_heartbeat(md5_shard, p_stats->loaded_objects,
					       p_stats->processed_objects);
	  if (unlikely(ret != 0)) {
	    return ret;
	  }

	  has_more = (p_header->offset == BLOCK_MAGIC);
	  ceph_assert(p_header->offset == BLOCK_MAGIC || p_header->offset == LAST_BLOCK_MAGIC);
	  if (!has_more) {
	    ldpp_dout(dpp, 20) << __func__ << "::No more blocks! block_id=" << disk_block_id
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
      ldpp_dout(dpp, 10) <<__func__ << "::slab seq_number=" << seq_number
			 << ", rec_count=" << slab_rec_count << dendl;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::ingress_single_object(rgw::sal::Bucket           *bucket,
					const rgw_bucket_dir_entry &entry,
					work_shard_t                worker_id,
					worker_stats_t             *p_worker_stats /*IN-OUT*/)
  {
    ldpp_dout(dpp, 20) << __func__ << ": got " << entry.key << dendl;
    unique_ptr<rgw::sal::Object> obj = bucket->get_object(entry.key);
    if (unlikely(!obj)) {
      derr << "ERROR: failed bucket->get_object(" << entry.key << ")" << dendl;
      p_worker_stats->ingress_failed_get_object++;
      return 1;
    }
    int ret = obj->get_obj_attrs(null_yield, dpp);
    if (unlikely(ret < 0)) {
      derr << "ERROR: failed to stat object(" << entry.key << "), returned error: "
	   << cpp_strerror(-ret) << dendl;
      p_worker_stats->ingress_failed_get_obj_attrs++;
      return 1;
    }
    uint64_t size = obj->get_obj_size();
#if 0
    if (size <= 4*1024*1024) {
      p_worker_stats->ingress_skip_too_small++;
      // dedup only useful for objects bigger than 4MB
      return 0;
    }
#endif
#if 0
    ceph::real_time mtime = obj->get_mtime();
    if (mtime < last_scan_time) {
      return 0;
    }
#endif
    if (obj->get_attrs().find(RGW_ATTR_CRYPT_MODE) != obj->get_attrs().end()) {
      ldpp_dout(dpp, 10) <<__func__ << "::Skipping encrypted object " << entry.key << dendl;
      p_worker_stats->ingress_skip_encrypted++;
      return 0;
    }

    // TBD: We should be able to support RGW_ATTR_COMPRESSION when all copies are compressed
    if (obj->get_attrs().find(RGW_ATTR_COMPRESSION) != obj->get_attrs().end()) {
      ldpp_dout(dpp, 10) <<__func__ << "::Skipping compressed object " << entry.key << dendl;
      p_worker_stats->ingress_skip_compressed++;
      return 0;
    }

    return add_disk_record(bucket, obj.get(), size);
  }

  //---------------------------------------------------------------------------
  bool Background::need_to_update_heartbeat()
  {
    static unsigned s_count = 0;

    utime_t now = ceph_clock_now();
    utime_t time_elapsed = now - d_heart_beat_last_update;
    if (likely(time_elapsed.tv.tv_sec < d_heart_beat_max_elapsed_sec)) {
      s_count++;
      return false;
    }
    else {
      ldpp_dout(dpp, 10) << __func__ << "::s_count=" << s_count
			 << "::max_elapsed_sec=" << d_heart_beat_max_elapsed_sec
			 << dendl;
      s_count = 0;
      d_heart_beat_last_update = now;
      return true;
    }
  }

  //---------------------------------------------------------------------------
  int Background::check_and_update_heartbeat(unsigned shard_id, uint64_t count_a,
					     uint64_t count_b, const char *prefix)
  {
    if (unlikely(need_to_update_heartbeat())) {
      int urgent_msg = URGENT_MSG_NONE;
      d_cluster.update_shard_token_heartbeat(p_dedup_cluster_ioctx, shard_id, count_a,
					     count_b, prefix, &urgent_msg);
      if (unlikely(urgent_msg == URGENT_MSG_ABORT)) {
	ldpp_dout(dpp, 1) << "::Remote Stop Request" << dendl;
	d_remote_abort_req = true;
      }
      else if (unlikely(urgent_msg == URGENT_MSG_PASUE)) {
	d_remote_pause_req = true;
      }
      else if (unlikely(urgent_msg == URGENT_MSG_SKIP)) {
	return -1;
      }
    }

    if (unlikely(d_local_pause_req || d_remote_pause_req)) {
      ldpp_dout(dpp, 1) << (d_local_pause_req ? "::Local" : "::Remote")
			<< " pause request" << dendl;
      handle_pause_req();
    }

    if (unlikely(should_stop())) {
      ldpp_dout(dpp, 1) << "Dedup background thread stopped" << dendl;
      return -1;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::check_and_update_worker_heartbeat(work_shard_t worker_id,
						    int64_t ingress_obj_count)
  {
    return check_and_update_heartbeat(worker_id, ingress_obj_count, 0, WORKER_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  int Background::check_and_update_md5_heartbeat(md5_shard_t md5_id,
						 uint64_t load_count,
						 uint64_t dedup_count)
  {
    return check_and_update_heartbeat(md5_id, load_count, dedup_count, MD5_SHARD_PREFIX);
  }

  //---------------------------------------------------------------------------
  int Background::process_bucket_shards(rgw::sal::Bucket      *bucket,
					std::map<int, string> &oids,
					librados::IoCtx       &ioctx,
					work_shard_t           worker_id,
					worker_stats_t        *p_worker_stats /*IN-OUT*/)
  {
    const uint32_t num_shards = oids.size();
    uint32_t current_shard = worker_id;
    rgw_obj_index_key marker; // start with an empty marker
    const string null_prefix, null_delimiter;
    const bool list_versions = true;
    const int max_entries = 1000;
    uint32_t obj_count = 0;

    ldpp_dout(dpp, 20) << __func__ << "::current_shard=" << current_shard
		       << ", worker_id=" << (uint32_t)worker_id << dendl;

    while (current_shard < num_shards ) {
      const string& oid = oids[current_shard];
      rgw_cls_list_ret result;
      librados::ObjectReadOperation op;
      cls_rgw_bucket_list_op(op, marker, null_prefix, null_delimiter,
			     max_entries, list_versions, &result);
      int ret = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, null_yield);
      if (unlikely(ret < 0)) {
	// TBD: should we retry/skip or simply abort everything ???
	ldpp_dout(dpp, 1) << __func__ << "::failed rgw_rados_operate() ret=" << ret << dendl;
	return ret;
      }
      ret = check_and_update_worker_heartbeat(worker_id, p_worker_stats->ingress_obj);
      if (unlikely(ret < 0)) {
	return ret;
      }
      obj_count += result.dir.m.size();
      for (auto& entry : result.dir.m) {
	const rgw_bucket_dir_entry& dirent = entry.second;
	if (unlikely((!dirent.exists && !dirent.is_delete_marker()) || !dirent.pending_map.empty())) {
	  // TBD: should we bailout ???
	  ldpp_dout(dpp, 1) << __func__ << "::ERROR: calling check_disk_state bucket="
			    << bucket->get_name() << " entry=" << dirent.key << dendl;
	  // make sure we're advancing marker
	  marker = dirent.key;
	  continue;
	}
	marker = dirent.key;
	p_worker_stats->ingress_obj++;
	ret = ingress_single_object(bucket, dirent, worker_id, p_worker_stats);
      }
      // TBD: advance marker only once here!
      if (result.is_truncated) {
	//marker = result.marker
      }
      else {
	// if we reached the end of the shard read next shard
	ldpp_dout(dpp, 10) << __func__ << "::" << bucket->get_name() << "::curr_shard=" << current_shard
			   << ", next shard=" << current_shard + MAX_WORK_SHARD << dendl;
	current_shard += MAX_WORK_SHARD;
	marker = rgw_obj_index_key(); // reset marker to empty index
      }
    }
    ldpp_dout(dpp, 20) << __func__ << "::Finished processing Bucket " << bucket->get_name()
		       << ", num_shards=" << num_shards << ", obj_count=" << obj_count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::ingress_bucket_objects_single_shard(const string   &bucket_name,
						      work_shard_t    worker_id,
						      worker_stats_t *p_worker_stats /*IN-OUT*/)
  {
    //ldpp_dout(dpp, 20) << __func__ << "::bucket_name=" << bucket_name << dendl;
    unique_ptr<rgw::sal::Bucket> bucket;
    const string tenant_name, empty_bucket_id;
    rgw_bucket b{tenant_name, bucket_name, empty_bucket_id};
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (unlikely(ret != 0)) {
      derr << "ERROR: driver->load_bucket(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    const std::string bucket_id = bucket->get_key().get_key();
    RGWBucketInfo bucket_info;
    ret = rados->get_bucket_instance_info(bucket_id, bucket_info, nullptr, nullptr, null_yield, dpp);
    if (ret < 0) {
      if (ret == -ENOENT) {
	// probably raced with bucket removal
	ldpp_dout(dpp, 1) << __func__ << "::ret == -ENOENT" << dendl;
	return 0;
      }
      ldpp_dout(dpp, 1) << __func__ << ":: ERROR: get_bucket_instance_info() returned ret=" << ret << dendl;
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
    // process all the shards in this bucket owned by the worker_id
    return process_bucket_shards(bucket.get(), oids, ioctx, worker_id, p_worker_stats);
  }

  //---------------------------------------------------------------------------
  int Background::objects_dedup_single_shard(md5_shard_t md5_shard, md5_stats_t *p_stats)
  {
    for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
      run_dedup_step(STEP_BUILD_TABLE, md5_shard, worker_id, p_stats);
      if (unlikely(should_stop())) {
	ldpp_dout(dpp, 1) << __func__ << "::STEP_BUILD_TABLE::STOPPED\n" << dendl;
	return -1;
      }
    }

    d_table.count_duplicates(&(p_stats->singleton_count), &(p_stats->unique_count),
			     &(p_stats->duplicate_count));
    ldpp_dout(dpp, 1) << "\n>>>>>" << __func__ << "::FINISHED STEP_BUILD_TABLE"
		      << "::singleton_count="  << p_stats->singleton_count
		      << "::unique_count="     << p_stats->unique_count
		      << "::duplicate_count="  << p_stats->duplicate_count << dendl;

    d_table.remove_singletons_and_redistribute_keys();
    for (work_shard_t worker_id = 0; worker_id < MAX_WORK_SHARD; worker_id++) {
      run_dedup_step(STEP_REMOVE_DUPLICATES, md5_shard, worker_id, p_stats);
      if (unlikely(should_stop())) {
	ldpp_dout(dpp, 1) << __func__ << "::STEP_REMOVE_DUPLICATES::STOPPED\n" << dendl;
	return -1;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::objects_ingress_single_shard(work_shard_t worker_id,
					       worker_stats_t *p_worker_stats)
  {
    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      p_disk_arr[md5_shard]->set_worker_id(worker_id, p_worker_stats);
    }
    int ret = 0;
    std::string section("bucket");
    std::string marker;
    void *handle = nullptr;
    ret = driver->meta_list_keys_init(dpp, section, marker, &handle);

    bool has_more = true;
    while (ret == 0 && has_more) {
      std::list<std::string> buckets;
      constexpr int max_keys = 1000;
      ret = driver->meta_list_keys_next(dpp, handle, max_keys, buckets, &has_more);
      if (ret == 0) {
	ldpp_dout(dpp, 20) <<__func__ << "::listing buckets:: worker_id=" << (uint32_t)worker_id << dendl;
	for (auto& bucket_name : buckets) {
	  //ldpp_dout(dpp, 20) <<__func__ << "::ingress_bucket_objects_single_shard:: bucket_name=" << bucket_name << dendl;
	  ret = ingress_bucket_objects_single_shard(bucket_name, worker_id, p_worker_stats);
	  if (unlikely(ret != 0)) {
	    return ret;
	  }
	}
	driver->meta_list_keys_complete(handle);
      }
      else {
	derr << __func__ << "::failed driver->meta_list_keys_next()" << dendl;
      }
    }

    for (unsigned md5_shard = 0; md5_shard < MAX_MD5_SHARD; md5_shard++) {
      ldpp_dout(dpp, 20) <<__func__ << "::flush buffers:: worker_id="
			 << (int)worker_id<< ", md5_shard=" << (int) md5_shard << dendl;
      p_disk_arr[md5_shard]->flush_disk_records(p_dedup_cluster_ioctx);
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::f_ingress(unsigned worker_id)
  {
    utime_t start_time = ceph_clock_now();
    worker_stats_t worker_stats;
    int ret = objects_ingress_single_shard(worker_id, &worker_stats);
    if (ret == 0) {
      worker_stats.duration = ceph_clock_now() - start_time;
      d_cluster.mark_work_shard_token_completed(p_dedup_cluster_ioctx, worker_id, &worker_stats);
      ldpp_dout(dpp, 0) << "stat counters [worker]:\n" << worker_stats << dendl;
      ldpp_dout(dpp, 0) << "Shard Process Duration   = " << worker_stats.duration << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::f_dedup(unsigned md5_shard)
  {
    utime_t start_time = ceph_clock_now();
    md5_stats_t md5_stats;
    int ret = objects_dedup_single_shard(md5_shard, &md5_stats);
    if (ret == 0) {
      md5_stats.duration = ceph_clock_now() - start_time;
      d_cluster.mark_md5_shard_token_completed(p_dedup_cluster_ioctx, md5_shard, &md5_stats);
      ldpp_dout(dpp, 0) << "stat counters [md5]:\n" << md5_stats << dendl;
      ldpp_dout(dpp, 0) << "Shard Process Duration   = " << md5_stats.duration << dendl;
      d_table.reset();
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int Background::process_all_shards(bool ingress_work_shards, int (Background::* func)(unsigned))
  {
    while (true) {
      d_heart_beat_last_update = ceph_clock_now();
      int urgent_msg = URGENT_MSG_NONE;
      unsigned shard_id;
      if (ingress_work_shards) {
	shard_id = d_cluster.get_next_work_shard_token(p_dedup_cluster_ioctx, &urgent_msg);
      }
      else {
	shard_id = d_cluster.get_next_md5_shard_token(p_dedup_cluster_ioctx, &urgent_msg);
      }
      // start with a common error handler
      if (shard_id != NULL_SHARD) {
	int ret = (this->*func)(shard_id);
	if (unlikely(ret != 0)) {
	  if (should_stop()) {
	    ldpp_dout(dpp, 0) << __func__ << "stop execution" << dendl;
	    return -1;
	  }
	  else {
	    ldpp_dout(dpp, 0) << "Skip shard # " << shard_id << dendl;
	  }
	}
      }
      else { // we got a NULL_SHARD
	ldpp_dout(dpp, 10) << __func__ << "::Got NULL_SHARD" << dendl;
	if (urgent_msg == URGENT_MSG_NONE) {
	  ldpp_dout(dpp, 0) << __func__ << "::finished scan" << dendl;
	  break;
	}
	const char* name = get_urgent_msg_names(urgent_msg);
	ldpp_dout(dpp, 0) << __func__ << "::NULL_SHARD::urgent_msg=" << urgent_msg
			  << "::" << name << dendl;
	if (urgent_msg == URGENT_MSG_PASUE) {
	  d_remote_pause_req = true;
	}

	if (d_local_pause_req || d_remote_pause_req) {
	  ldpp_dout(dpp, 0) << __func__ << "::PAUSE REQ" << dendl;
	  handle_pause_req();
	  ldpp_dout(dpp, 0) << __func__ << "::PAUSE RESUME" << dendl;
	  // fall through ABORT code
	}

	if (urgent_msg == URGENT_MSG_ABORT) {
	  ldpp_dout(dpp, 0) << __func__ << "::URGENT_MSG_ABORT" << dendl;
	  d_remote_abort_req = true;
	}

	if (should_stop()) {
	  return -1;
	}

	if (urgent_msg >= URGENT_MSG_INVALID) {
	  return -1;
	}
      }
    } // while loop
    return 0;
  }

  //---------------------------------------------------------------------------
  int Background::setup()
  {
    init_rados_access_handles();
    int ret = d_cluster.init(store, p_dedup_cluster_ioctx);
    if (ret != 0) {
      derr << __func__ << "::failed cluster.init()" << dendl;
      return -1;
    }
    d_table.reset();

    return 0;
  }

  //---------------------------------------------------------------------------
  void Background::start()
  {
    const DoutPrefixProvider* const dpp = &dp;
    ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__ << dendl;
    {
      std::unique_lock pause_lock(d_pause_mutex);
      if (d_started) {
	// start the thread only once
	return;
      }
      d_started = true;
    }
    d_runner = std::thread(&Background::run, this);
    const auto rc = ceph_pthread_setname(d_runner.native_handle(), "dedup_bg");
    ldpp_dout(dpp, 1) <<  __FILE__ << "::" <<__func__ << "::setname rc=" << rc << dendl;
  }

  //------------------------- --------------------------------------------------
  void Background::shutdown()
  {
    d_shutdown_req = true;
    d_cond.notify_all();
    if (d_runner.joinable()) {
      d_runner.join();
    }
    {
      std::unique_lock pause_lock(d_pause_mutex);
      d_started          = false;
      d_shutdown_req     = false;
      d_remote_abort_req = false;
      d_local_pause_req  = false;
      d_local_paused     = false;
    }
  }

  //---------------------------------------------------------------------------
  void Background::pause()
  {
    {
      std::unique_lock pause_lock(d_pause_mutex);
      d_local_pause_req = true;
      if (d_local_paused || d_shutdown_req) {
	derr <<  __FILE__ << "::" <<__func__
	     << "::background is already paused/stopped!!!" << dendl;
	return;
      }
      std::unique_lock cond_lock(d_cond_mutex);
      d_cond.wait(cond_lock, [this]{return d_local_paused || d_shutdown_req;});
    }
  }

  //---------------------------------------------------------------------------
  void Background::resume(rgw::sal::Driver* _driver)
  {
    {
      std::unique_lock pause_lock(d_pause_mutex);
      if (!d_local_paused) {
	derr <<  __FILE__ << "::" <<__func__
	     << "::background is not paused!!!" << dendl;
	if (_driver != driver) {
	  derr <<  __FILE__ << "::" <<__func__
	       << "::attempt to change driver on a live task failed!!!" << dendl;
	}
	return;
      }
      d_local_pause_req = false;
      d_local_paused    = false;
      driver = _driver;
      init_rados_access_handles();
    }
    // wake up threads blocked after seeing pause state
    d_cond.notify_all();
  }

  //---------------------------------------------------------------------------
  void Background::handle_pause_req()
  {
    bool first_time = true;
    while (d_local_pause_req || d_local_paused || d_remote_pause_req || d_remote_paused) {

      if (should_stop()) {
	return;
      }

      if (d_local_pause_req) {
	d_local_paused = true;
      }

      if (d_remote_pause_req) {
	d_remote_paused = true;
      }

      d_cond.notify_all();
      std::unique_lock cond_lock(d_cond_mutex);

      if (d_local_paused) {
	if (first_time) {
	  first_time = false;
	  ldpp_dout(dpp, 1) << "Dedup background thread paused" << dendl;
	}
	d_cond.wait(cond_lock, [this]{return !d_local_paused || d_shutdown_req;});
	continue;
      }

      if (d_remote_paused) {
	unsigned interval = 3;
	if (first_time) {
	  //first_time = false;
	  ldpp_dout(dpp, 1) << "Dedup background thread paused (remote request)" << dendl;
	}
	d_cond.wait_for(cond_lock, std::chrono::seconds(interval),
			[this]{return d_shutdown_req || d_local_pause_req ;});
	if (!d_local_paused && !d_local_pause_req && !d_shutdown_req) {
	  d_remote_pause_req = false;
	  d_remote_paused = false;
	  int urgent_msg = URGENT_MSG_NONE;
	  d_cluster.get_urgent_msg_state(p_dedup_cluster_ioctx, &urgent_msg);
	  if (urgent_msg == URGENT_MSG_PASUE) {
	    d_remote_pause_req = true;
	  }
	  else if (urgent_msg == URGENT_MSG_ABORT) {
	    d_remote_abort_req = true;
	  }
	}
      }

    } // while loop

    ldpp_dout(dpp, 0) << "Dedup background thread resumed!" << dendl;
  }

  //---------------------------------------------------------------------------
  void Background::run()
  {
    init_rados_access_handles();
    utime_t epoch = ceph_clock_now();
    bool need_to_scan = true;
    ldpp_dout(dpp, 1) <<__func__ << "::dedup::main loop" << dendl;
    while (!d_shutdown_req) {
      if (unlikely(d_local_pause_req)) {
	handle_pause_req();
	if (d_shutdown_req) {
	  goto shutdown;
	}
      }

      if (need_to_scan) {
	if (setup() != 0) {
	  derr << "failed setup()" << dendl;
	  return;
	}
	epoch = d_cluster.get_epoch();
	ldpp_dout(dpp, 1) << __func__ << "::Epoch=" << epoch << dendl;
	process_all_shards(true, &Background::f_ingress);
	if (should_stop()) {
	  goto finish_scan;
	}

	// Wait for other worker to finish ingress step
	uint32_t ttl = 0;
	uint64_t total_ingressed = 0;
	while (!d_cluster.all_work_shard_tokens_completed(p_dedup_cluster_ioctx, &ttl, &total_ingressed)) {
	  ldpp_dout(dpp, 0) << "Waiting for INGRESS step completion ttl=" << ttl << dendl;
	  std::unique_lock cond_lock(d_cond_mutex);
	  d_cond.wait_for(cond_lock, std::chrono::seconds(ttl), [this]{return d_shutdown_req || d_local_pause_req;});
	  if (unlikely(d_local_pause_req)) {
	    handle_pause_req();
	  }
	  if (should_stop()) {
	    goto finish_scan;
	  }
	}

	ldpp_dout(dpp, 1) << "\n\n==INGRESS step was completed on all shards! ("
			  << total_ingressed << ")==\n" << dendl;
	if (unlikely(d_local_pause_req)) {
	  handle_pause_req();
	}
	if (should_stop()) {
	  goto finish_scan;
	}
	process_all_shards(false, &Background::f_dedup);
	ldpp_dout(dpp, 1) << "\n\n==DEDUP step was completed on all shards! ==\n" << dendl;
      }
    finish_scan:
      need_to_scan = false;
      std::unique_lock cond_lock(d_cond_mutex);
      unsigned interval = 10;
      d_cond.wait_for(cond_lock, std::chrono::seconds(interval), [this]{return d_shutdown_req || d_local_pause_req;});
      if (!d_local_paused && !d_local_pause_req && !d_shutdown_req) {
	if (d_cluster.can_start_new_scan(store, epoch, dpp)) {
	  d_remote_pause_req = false;
	  d_remote_abort_req = false;
	  need_to_scan = true;
	}
      }
    }
  shutdown:
    ldpp_dout(dpp, 1) << "Dedup background thread stopped" << dendl;
  }

}; //namespace rgw::dedup
