// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 *
 * This file implements C wrapper functions for RGW SAL.
 *
 * These are minimal implementations to enable external applications like
 * LanceDB to work with RGW's Storage Abstraction Layer (SAL) via FFI.
 * They provide basic object storage operations (put, get, delete, list, etc.)
 * without the full complexity of the S3 REST API handlers.
 */

#include "rgw_sal_wrapper.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_bucket.h"
#include "rgw/rgw_req_context.h"
#include "rgw/rgw_obj_types.h"
#include "rgw/rgw_compression_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_crypto.h"
#include "global/global_context.h"
#include "common/async/yield_context.h"

#include <cstring>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#define dout_subsys ceph_subsys_rgw

static constexpr size_t MAX_ETAG_LEN = 128;
static constexpr size_t MAX_UPLOAD_ID_LEN = 256;

// Collect data into a bufferlist
class BufferlistDataCB : public RGWGetDataCB {
  bufferlist& bl_;
public:
  explicit BufferlistDataCB(bufferlist& bl) : bl_(bl) {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    if (bl_len > 0 && bl.length() > 0) {
      bl.begin(bl_ofs).copy(bl_len, bl_);
    }
    return 0;
  }
};

static inline const DoutPrefixProvider* get_dpp(const RGWDoutPrefix* dpp) {
  return reinterpret_cast<const DoutPrefixProvider*>(dpp);
}

static inline rgw::sal::Driver* get_driver(RGWSalDriver* driver) {
  return reinterpret_cast<rgw::sal::Driver*>(driver);
}

static inline optional_yield get_yield(RGWYieldContext* yield_ctx) {
  if (yield_ctx) {
    return *reinterpret_cast<optional_yield*>(yield_ctx);
  }
  return null_yield;
}

static int load_bucket( rgw::sal::Driver* driver, const DoutPrefixProvider* dpp,
        const char* bucket_name, std::unique_ptr<rgw::sal::Bucket>& bucket_out,
        optional_yield y) {
  if (!driver || !bucket_name) {
    ldpp_dout(dpp, 1) << "ERROR: sal_wrapper: load_bucket: invalid args"
                      << " driver=" << driver << " bucket=" << (bucket_name ? bucket_name : "null") << dendl;
    return -EINVAL;
  }

  rgw_bucket bucket_id;
  bucket_id.name = bucket_name;

  int ret = driver->load_bucket(dpp, bucket_id, &bucket_out, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: sal_wrapper: load_bucket failed for '"
                      << bucket_name << "' ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

// Convert RGWObject to rgw_obj_key, using version_id as instance if provided
static inline rgw_obj_key make_obj_key(const RGWObject* obj) {
  std::string name(obj->key);
  if (obj->version_id) {
    return rgw_obj_key(name, std::string(obj->version_id));
  }
  return rgw_obj_key(name);
}

extern "C" {

int rgw_put_object( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      const uint8_t* data, size_t len, const char* content_type) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || (!data && len > 0)) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(make_obj_key(obj_id));
  if (!obj) {
    return -ENOMEM;
  }

  ACLOwner owner = bucket->get_acl().get_owner();
  const rgw_placement_rule& placement_rule = bucket->get_placement_rule();
  std::string unique_tag = driver->zone_unique_id(driver->get_new_req_id());

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
    dpp, y, obj.get(), owner, &placement_rule, 0, unique_tag);

  if (!writer) {
    return -ENOMEM;
  }

  ret = writer->prepare(y);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<const char*>(data), len);

  ret = writer->process(std::move(bl), 0);
  if (ret < 0) {
    return ret;
  }

  ret = writer->process(bufferlist(), len);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Attrs attrs;
  if (content_type && strlen(content_type) > 0) {
    bufferlist ct_bl;
    ct_bl.append(content_type);
    attrs[RGW_ATTR_CONTENT_TYPE] = ct_bl;
  }

  ceph::real_time mtime = ceph::real_clock::now();
  req_context rctx{dpp, y, nullptr};

  ret = writer->complete(
    len, "", &mtime, mtime, attrs, std::nullopt, ceph::real_time(),
    nullptr, nullptr, nullptr, nullptr, nullptr, rctx, 0);

  return ret;
}

int rgw_put_object_conditional( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      const uint8_t* data, size_t len, const char* content_type,
      const char* if_match, const char* if_nomatch, int* canceled) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (canceled) *canceled = 0;

  if (!driver || !bucket_name || !obj_id || !obj_id->key || (!data && len > 0)) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(make_obj_key(obj_id));
  if (!obj) {
    return -ENOMEM;
  }

  ACLOwner owner = bucket->get_acl().get_owner();
  const rgw_placement_rule& placement_rule = bucket->get_placement_rule();
  std::string unique_tag = driver->zone_unique_id(driver->get_new_req_id());

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
    dpp, y, obj.get(), owner, &placement_rule, 0, unique_tag);

  if (!writer) {
    return -ENOMEM;
  }

  ret = writer->prepare(y);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<const char*>(data), len);
  ret = writer->process(std::move(bl), 0);
  if (ret < 0) {
    return ret;
  }

  ret = writer->process(bufferlist(), len);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Attrs attrs;
  if (content_type && strlen(content_type) > 0) {
    bufferlist ct_bl;
    ct_bl.append(content_type);
    attrs[RGW_ATTR_CONTENT_TYPE] = ct_bl;
  }

  ceph::real_time mtime = ceph::real_clock::now();
  req_context rctx{dpp, y, nullptr};
  bool was_canceled = false;

  ret = writer->complete(
    len, "", &mtime, mtime, attrs, std::nullopt, ceph::real_time(),
    if_match, if_nomatch, nullptr, nullptr, &was_canceled, rctx, 0);

  if (canceled) *canceled = was_canceled ? 1 : 0;
  return ret;
}

// TODO: callers are expected to do ranged reads (offset/length) for large
// objects. Streaming reads via callback-based Rust FFI could be explored
// in a follow-up.
int rgw_get_object( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      uint64_t offset, uint64_t length, RGWBuffer* buffer) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !buffer) {
    return -EINVAL;
  }

  buffer->data = nullptr;
  buffer->len = 0;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(make_obj_key(obj_id));
  if (!obj) {
    return -ENOMEM;
  }

  ret = obj->load_obj_state(dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (!obj->exists()) {
    return -ENOENT;
  }

  uint64_t obj_size = obj->get_size();

  if (offset >= obj_size) {
    return 0;
  }

  uint64_t read_len = length;
  if (length == UINT64_MAX || length > obj_size - offset) {
    read_len = obj_size - offset;
  }

  if (read_len == 0) {
    return 0;
  }

  buffer->data = static_cast<uint8_t*>(malloc(read_len));
  if (!buffer->data) {
    return -ENOMEM;
  }

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op();

  ret = read_op->prepare(y, dpp);
  if (ret < 0) {
    free(buffer->data);
    buffer->data = nullptr;
    return ret;
  }

  bufferlist bl;
  int64_t end_ofs = offset + read_len - 1;
  ret = read_op->read(offset, end_ofs, bl, y, dpp);
  if (ret < 0) {
    free(buffer->data);
    buffer->data = nullptr;
    return ret;
  }

  size_t actual_len = bl.length();
  if (actual_len > read_len) {
    actual_len = read_len;
  }
  if (actual_len > 0) {
    memcpy(buffer->data, bl.c_str(), actual_len);
  }
  buffer->len = actual_len;

  return 0;
}

int rgw_delete_object( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
    RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(make_obj_key(obj_id));
  if (!obj) {
    return -ENOMEM;
  }

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
  ret = del_op->delete_obj(dpp, y, 0);

  if (ret == -ENOENT) {
    return 0;
  }

  return ret;
}

int rgw_head_object( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      RGWObjectMeta* meta) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !meta) {
    return -EINVAL;
  }

  meta->size = 0;
  meta->etag = nullptr;
  meta->content_type = nullptr;
  meta->last_modified = 0;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(make_obj_key(obj_id));
  if (!obj) {
    return -ENOMEM;
  }

  ret = obj->load_obj_state(dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (!obj->exists()) {
    return -ENOENT;
  }

  meta->size = obj->get_size();
  meta->last_modified = ceph::real_clock::to_time_t(obj->get_mtime());

  const rgw::sal::Attrs& attrs = obj->get_attrs();

  auto etag_iter = attrs.find(RGW_ATTR_ETAG);
  if (etag_iter != attrs.end()) {
    meta->etag = strndup(etag_iter->second.to_str().c_str(), MAX_ETAG_LEN);
    if (!meta->etag) {
      return -ENOMEM;
    }
  }

  auto ct_iter = attrs.find(RGW_ATTR_CONTENT_TYPE);
  if (ct_iter != attrs.end()) {
    meta->content_type = strdup(ct_iter->second.to_str().c_str());
    if (!meta->content_type) {
      free(meta->etag);
      meta->etag = nullptr;
      return -ENOMEM;
    }
  }

  return 0;
}

int rgw_list_objects( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const char* prefix,
      const char* delimiter, const char* marker, uint32_t max_keys,
      RGWListResult* result) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !result) {
    return -EINVAL;
  }

  result->entries = nullptr;
  result->count = 0;
  result->is_truncated = 0;
  result->next_marker = nullptr;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  params.prefix = prefix ? prefix : "";
  params.delim = delimiter ? delimiter : "";
  params.marker = rgw_obj_key(marker ? marker : "");
  params.list_versions = false;
  params.allow_unordered = false;

  ldpp_dout(dpp, 10) << "rgw_list_objects: bucket=" << bucket_name
           << " prefix='" << params.prefix << "'"
           << " delimiter='" << params.delim << "'"
           << " marker='" << params.marker.name << "'"
           << " max_keys=" << max_keys << dendl;

  rgw::sal::Bucket::ListResults results;

  ret = bucket->list(dpp, params, max_keys, results, y);
  if (ret < 0) {
    return ret;
  }

  size_t obj_count = results.objs.size();
  size_t prefix_count = results.common_prefixes.size();
  size_t total_count = obj_count + prefix_count;

  ldpp_dout(dpp, 10) << "rgw_list_objects: found " << obj_count << " objects, "
           << prefix_count << " common_prefixes, "
           << "is_truncated=" << results.is_truncated << dendl;

  if (total_count > 0) {
    result->entries = static_cast<RGWListEntry*>(
      calloc(total_count, sizeof(RGWListEntry)));
    if (!result->entries) {
      return -ENOMEM;
    }

    size_t i = 0;
    for (const auto& obj : results.objs) {
      result->entries[i].key = strdup(obj.key.name.c_str());
      if (!result->entries[i].key) {
        result->count = i;
        rgw_free_list_result(result);
        return -ENOMEM;
      }
      result->entries[i].size = obj.meta.size;
      result->entries[i].last_modified =
        ceph::real_clock::to_time_t(obj.meta.mtime);
      i++;
    }

    for (const auto& [prefix_name, _] : results.common_prefixes) {
      result->entries[i].key = strdup(prefix_name.c_str());
      if (!result->entries[i].key) {
        result->count = i;
        rgw_free_list_result(result);
        return -ENOMEM;
      }
      result->entries[i].size = 0;
      result->entries[i].last_modified = time(nullptr);
      i++;
    }
  }

  result->count = total_count;
  result->is_truncated = results.is_truncated ? 1 : 0;

  if (results.is_truncated && !results.next_marker.name.empty()) {
    result->next_marker = strdup(results.next_marker.name.c_str());
    if (!result->next_marker) {
      rgw_free_list_result(result);
      return -ENOMEM;
    }
  }

  return 0;
}

int rgw_copy_object( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* src_bucket_name, const RGWObject* src_obj_id,
      const char* dst_bucket_name, const RGWObject* dst_obj_id) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !src_bucket_name || !src_obj_id || !src_obj_id->key ||
    !dst_bucket_name || !dst_obj_id || !dst_obj_id->key) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> src_bucket;
  int ret = load_bucket(driver, dpp, src_bucket_name, src_bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Bucket> dst_bucket;
  ret = load_bucket(driver, dpp, dst_bucket_name, dst_bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> src_obj =
    src_bucket->get_object(make_obj_key(src_obj_id));
  if (!src_obj) {
    return -ENOMEM;
  }

  std::unique_ptr<rgw::sal::Object> dst_obj =
    dst_bucket->get_object(make_obj_key(dst_obj_id));
  if (!dst_obj) {
    return -ENOMEM;
  }

  ACLOwner owner = dst_bucket->get_acl().get_owner();
  rgw_user remote_user;
  rgw_zone_id source_zone;
  const rgw_placement_rule& dest_placement = dst_bucket->get_placement_rule();
  rgw::sal::Attrs attrs;

  ret = src_obj->copy_object(
    owner, remote_user, nullptr, source_zone,
    dst_obj.get(), dst_bucket.get(), src_bucket.get(),
    dest_placement,
    nullptr, nullptr, nullptr, nullptr,
    false, nullptr, nullptr,
    rgw::sal::ATTRSMOD_NONE, false, attrs,
    RGWObjCategory::Main, 0, boost::none,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    dpp, y);

  return ret;
}

int rgw_copy_object_conditional( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* src_bucket_name, const RGWObject* src_obj_id,
      const char* dst_bucket_name, const RGWObject* dst_obj_id, const char* if_match,
      const char* if_nomatch) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !src_bucket_name || !src_obj_id || !src_obj_id->key ||
    !dst_bucket_name || !dst_obj_id || !dst_obj_id->key) {
    return -EINVAL;
  }

  if (if_nomatch && std::string(if_nomatch) == "*") {
    std::unique_ptr<rgw::sal::Bucket> check_bucket;
    int ret = load_bucket(driver, dpp, dst_bucket_name, check_bucket, y);
    if (ret < 0) return ret;

    std::unique_ptr<rgw::sal::Object> check_obj =
      check_bucket->get_object(make_obj_key(dst_obj_id));
    if (check_obj) {
      ret = check_obj->load_obj_state(dpp, y);
      if (ret == 0 && check_obj->exists()) {
        return -EEXIST;
      }
    }
  }

  std::unique_ptr<rgw::sal::Bucket> src_bucket;
  int ret = load_bucket(driver, dpp, src_bucket_name, src_bucket, y);
  if (ret < 0) return ret;

  std::unique_ptr<rgw::sal::Bucket> dst_bucket;
  ret = load_bucket(driver, dpp, dst_bucket_name, dst_bucket, y);
  if (ret < 0) return ret;

  std::unique_ptr<rgw::sal::Object> src_obj =
    src_bucket->get_object(make_obj_key(src_obj_id));
  if (!src_obj) return -ENOMEM;

  std::unique_ptr<rgw::sal::Object> dst_obj =
    dst_bucket->get_object(make_obj_key(dst_obj_id));
  if (!dst_obj) return -ENOMEM;

  ACLOwner owner = dst_bucket->get_acl().get_owner();
  rgw_user remote_user;
  rgw_zone_id source_zone;
  const rgw_placement_rule& dest_placement = dst_bucket->get_placement_rule();
  rgw::sal::Attrs attrs;

  const char* copy_if_nomatch = nullptr;
  if (if_nomatch && std::string(if_nomatch) != "*") {
    copy_if_nomatch = if_nomatch;
  }

  ret = src_obj->copy_object(
    owner, remote_user, nullptr, source_zone,
    dst_obj.get(), dst_bucket.get(), src_bucket.get(),
    dest_placement,
    nullptr, nullptr, nullptr, nullptr,
    false, if_match, copy_if_nomatch,
    rgw::sal::ATTRSMOD_NONE, false, attrs,
    RGWObjCategory::Main, 0, boost::none,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    dpp, y);

  return ret;
}

int rgw_delete_objects( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const char* const* keys,
      size_t count) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || (!keys && count > 0)) {
    return -EINVAL;
  }

  if (count == 0) {
    return 0;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  int failed = 0;
  for (size_t i = 0; i < count; i++) {
    if (!keys[i]) continue;

    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(keys[i]));
    if (!obj) {
      ldpp_dout(dpp, 1) << "rgw_delete_objects: failed to create object for key '"
               << keys[i] << "'" << dendl;
      failed++;
      continue;
    }

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
    int del_ret = del_op->delete_obj(dpp, y, 0);
    if (del_ret < 0 && del_ret != -ENOENT) {
      ldpp_dout(dpp, 1) << "rgw_delete_objects: failed to delete '"
               << keys[i] << "': " << del_ret << dendl;
      failed++;
    } else {
      ldpp_dout(dpp, 10) << "rgw_delete_objects: deleted '"
                << keys[i] << "'" << dendl;
    }
  }

  if (failed > 0) {
    ldpp_dout(dpp, 1) << "rgw_delete_objects: " << failed << " of "
             << count << " deletes failed" << dendl;
  }

  return 0;
}

int rgw_init_multipart( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      RGWString* upload_id) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !upload_id) {
    return -EINVAL;
  }

  upload_id->str = nullptr;
  upload_id->len = 0;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(obj_id->key);
  if (!upload) {
    return -ENOMEM;
  }

  ACLOwner owner = bucket->get_acl().get_owner();
  rgw_placement_rule placement = bucket->get_placement_rule();
  rgw::sal::Attrs attrs;

  ret = upload->init(dpp, y, owner, placement, attrs);
  if (ret < 0) {
    return ret;
  }

  std::string id = upload->get_upload_id();
  upload_id->str = strndup(id.c_str(), MAX_UPLOAD_ID_LEN);
  if (!upload_id->str) {
    return -ENOMEM;
  }
  upload_id->len = strlen(upload_id->str);

  return 0;
}

int rgw_multipart_put_part( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      const char* upload_id, uint32_t part_num, const uint8_t* data,
      size_t len, RGWString* etag) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !upload_id ||
    !etag || (!data && len > 0)) {
    return -EINVAL;
  }

  etag->str = nullptr;
  etag->len = 0;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(obj_id->key, upload_id);
  if (!upload) {
    return -ENOMEM;
  }

  ACLOwner owner = bucket->get_acl().get_owner();
  const rgw_placement_rule& placement_rule = bucket->get_placement_rule();
  std::unique_ptr<rgw::sal::Writer> writer = upload->get_writer(
    dpp, y, nullptr, owner, &placement_rule, part_num,
    std::to_string(part_num));

  if (!writer) {
    return -ENOMEM;
  }

  ret = writer->prepare(y);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<const char*>(data), len);

  ret = writer->process(std::move(bl), 0);
  if (ret < 0) {
    return ret;
  }

  ret = writer->process(bufferlist(), len);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Attrs attrs;
  ceph::real_time mtime = ceph::real_clock::now();
  req_context rctx{dpp, y, nullptr};

  ret = writer->complete(
    len, "", &mtime, mtime, attrs, std::nullopt, ceph::real_time(),
    nullptr, nullptr, nullptr, nullptr, nullptr, rctx, 0);

  if (ret < 0) {
    return ret;
  }

  unsigned char md5_digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  ceph::crypto::MD5 md5_hash;
  md5_hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  md5_hash.Update(data, len);
  md5_hash.Final(md5_digest);

  std::string md5_hex;
  md5_hex.reserve(CEPH_CRYPTO_MD5_DIGESTSIZE * 2);
  buf_to_hex(md5_digest, std::back_inserter(md5_hex));

  etag->str = strndup(md5_hex.c_str(), MAX_ETAG_LEN);
  if (!etag->str) {
    return -ENOMEM;
  }
  etag->len = strlen(etag->str);

  return 0;
}

int rgw_multipart_complete( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      const char* upload_id, const char* const* etags, size_t count) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !upload_id ||
    (!etags && count > 0)) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(obj_id->key, upload_id);
  if (!upload) {
    return -ENOMEM;
  }

  std::map<int, std::string> part_etags;
  for (size_t i = 0; i < count; i++) {
    if (etags[i]) {
      part_etags[static_cast<int>(i + 1)] = etags[i];
    }
  }

  std::list<rgw_obj_index_key> remove_objs;
  uint64_t accounted_size = 0;
  bool compressed = false;
  RGWCompressionInfo cs_info;
  off_t ofs = 0;
  std::string tag;
  ACLOwner owner = bucket->get_acl().get_owner();
  rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;

  std::unique_ptr<rgw::sal::Object> target_obj = bucket->get_object(make_obj_key(obj_id));

  ret = upload->complete(
    dpp, y, g_ceph_context, part_etags, remove_objs,
    accounted_size, compressed, cs_info, ofs, tag, owner,
    0, target_obj.get(), processed_prefixes, nullptr, nullptr);

  return ret;
}

int rgw_multipart_abort( RGWSalDriver* driver_ptr, const RGWDoutPrefix* dpp_ptr,
      RGWYieldContext* yield_ctx, const char* bucket_name, const RGWObject* obj_id,
      const char* upload_id) {
  auto* driver = get_driver(driver_ptr);
  auto* dpp = get_dpp(dpp_ptr);
  auto y = get_yield(yield_ctx);

  if (!driver || !bucket_name || !obj_id || !obj_id->key || !upload_id) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = load_bucket(driver, dpp, bucket_name, bucket, y);
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(obj_id->key, upload_id);
  if (!upload) {
    return -ENOMEM;
  }

  ret = upload->abort(dpp, nullptr, y);

  return ret;
}

void rgw_free_buffer(RGWBuffer* buffer) {
  if (buffer) {
    if (buffer->data) {
      free(buffer->data);
      buffer->data = nullptr;
    }
    buffer->len = 0;
  }
}

void rgw_free_object_meta(RGWObjectMeta* meta) {
  if (meta) {
    if (meta->etag) {
      free(meta->etag);
      meta->etag = nullptr;
    }
    if (meta->content_type) {
      free(meta->content_type);
      meta->content_type = nullptr;
    }
    meta->size = 0;
    meta->last_modified = 0;
  }
}

void rgw_free_list_result(RGWListResult* result) {
  if (result) {
    if (result->entries) {
      for (size_t i = 0; i < result->count; i++) {
        if (result->entries[i].key) {
          free(result->entries[i].key);
        }
      }
      free(result->entries);
      result->entries = nullptr;
    }
    if (result->next_marker) {
      free(result->next_marker);
      result->next_marker = nullptr;
    }
    result->count = 0;
    result->is_truncated = 0;
  }
}

void rgw_free_string(RGWString* str) {
  if (str) {
    free(str->str);
    str->str = nullptr;
    str->len = 0;
  }
}

uint64_t rgw_get_max_chunk_size(RGWSalDriver* driver_ptr) {
  auto* driver = get_driver(driver_ptr);
  if (!driver) {
    return 4 * 1024 * 1024; // 4 MB fallback
  }
  return driver->ctx()->_conf->rgw_max_chunk_size;
}

const char* rgw_sal_wrapper_version(void) {
  static char version[16];
  static bool initialized = false;
  if (!initialized) {
    snprintf(version, sizeof(version), "%d.%d",
             RGW_SAL_WRAPPER_VERSION_MAJOR,
             RGW_SAL_WRAPPER_VERSION_MINOR);
    initialized = true;
  }
  return version;
}

} // extern "C"
