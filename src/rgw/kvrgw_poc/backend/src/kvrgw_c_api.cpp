// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "kvrgw_c_api.h"

#include "byte_range.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "id_meta.hpp"
#include "id_tag.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <new>
#include <openssl/evp.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kvrgw::kFirstVersionId;
using kvrgw::kMaxContentTypeLen;
using kvrgw::kMaxTags;
using kvrgw::kNullVersion;
using kvrgw::MAX_META_COUNT;
using kvrgw::MAX_META_FRAME_BYTES;
using kvrgw::MAX_TAG_COUNT;
using kvrgw::MetaPair;
using kvrgw::ObjectValue;
using kvrgw::TagPair;
using kvrgw::version_id_t;
using kvrgw::VERSIONING_DISABLED;
using kvrgw::VERSIONING_ENABLED;
using kvrgw::VERSIONING_SUSPENDED;
using kvrgw::VersioningState;

std::string_view sv(const char *p, size_t n)
{
  if (p == nullptr || n == 0) {
    return {};
  }
  return {p, n};
}

void copy_cstr(char *dst, size_t dst_cap, std::string_view s)
{
  if (dst == nullptr || dst_cap == 0) {
    return;
  }
  const size_t n = s.size() < dst_cap - 1 ? s.size() : dst_cap - 1;
  if (n > 0) {
    std::memcpy(dst, s.data(), n);
  }
  dst[n] = '\0';
}

kvrgw_err_t arena_put(char *arena, size_t cap, size_t *used, std::string_view s,
                      uint32_t *off, uint32_t *len)
{
  if (s.empty()) {
    *off = 0;
    *len = 0;
    return KVRGW_ERR_OK;
  }
  if (arena == nullptr || *used + s.size() > cap) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  *off = static_cast<uint32_t>(*used);
  *len = static_cast<uint32_t>(s.size());
  std::memcpy(arena + *used, s.data(), s.size());
  *used += s.size();
  return KVRGW_ERR_OK;
}

void fill_object_meta(const ObjectValue &v, kvrgw_object_meta *meta)
{
  std::memset(meta, 0, sizeof(*meta));
  copy_cstr(meta->etag, sizeof(meta->etag), v.etag_display());
  meta->size = v.hdr.size;
  meta->last_modified_unix = v.hdr.last_modified_sec;
  copy_cstr(meta->content_type, sizeof(meta->content_type), v.content_type);
  if (!(v.hdr.version_id == kNullVersion &&
        v.hdr.next_vid == kFirstVersionId)) {
    copy_cstr(meta->version_id, sizeof(meta->version_id),
              v.hdr.version_id.to_hex());
  }
  meta->tags_count = v.hdr.tags_count;
}

kvrgw_err_t fill_kv_pairs(std::span<const MetaPair> kvs, uint32_t n,
                          kvrgw_kv *out, uint32_t cap, uint32_t *count,
                          char *arena, size_t arena_cap)
{
  if (count == nullptr) {
    return KVRGW_ERR_OK;
  }
  *count = n;
  if (n == 0) {
    return KVRGW_ERR_OK;
  }
  if (out == nullptr || n > cap) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  size_t used = 0;
  for (uint32_t i = 0; i < n; ++i) {
    uint32_t koff = 0;
    uint32_t klen = 0;
    uint32_t voff = 0;
    uint32_t vlen = 0;
    if (auto a = arena_put(arena, arena_cap, &used, kvs[i].first, &koff, &klen);
        a != KVRGW_ERR_OK) {
      return a;
    }
    if (auto a =
            arena_put(arena, arena_cap, &used, kvs[i].second, &voff, &vlen);
        a != KVRGW_ERR_OK) {
      return a;
    }
    out[i].key = arena + koff;
    out[i].key_len = klen;
    out[i].value = arena + voff;
    out[i].value_len = vlen;
  }
  return KVRGW_ERR_OK;
}

kvrgw_err_t fill_decoded_metadata(std::span<const uint8_t> live, uint16_t n,
                                  kvrgw_kv *metadata, uint32_t metadata_cap,
                                  uint32_t *metadata_count, char *meta_arena,
                                  size_t meta_arena_cap)
{
  if (n == 0) {
    if (metadata_count != nullptr) {
      *metadata_count = 0;
    }
    return KVRGW_ERR_OK;
  }
  std::array<MetaPair, MAX_META_COUNT> views{};
  if (!kvrgw::decode_metadata(live, views)) {
    return KVRGW_ERR_CORRUPT_VALUE;
  }
  return fill_kv_pairs(std::span<const MetaPair>(views.data(), n), n, metadata,
                       metadata_cap, metadata_count, meta_arena,
                       meta_arena_cap);
}

kvrgw_err_t encode_kv_tags(const kvrgw_kv *tags, size_t n,
                           std::vector<uint8_t> &out)
{
  if (n == 0 || tags == nullptr) {
    return KVRGW_ERR_INVALID_TAG;
  }
  if (n > MAX_TAG_COUNT) {
    return KVRGW_ERR_INVALID_TAG;
  }
  std::array<TagPair, MAX_TAG_COUNT> pairs;
  for (size_t i = 0; i < n; ++i) {
    pairs[i] = {sv(tags[i].key, tags[i].key_len),
                sv(tags[i].value, tags[i].value_len)};
  }
  if (!kvrgw::encode(std::span<const TagPair>(pairs.data(), n), out)) {
    return KVRGW_ERR_INVALID_TAG;
  }
  return KVRGW_ERR_OK;
}

kvrgw_err_t encode_kv_metadata(const kvrgw_kv *meta, size_t n,
                               std::span<uint8_t> out, size_t &out_size)
{
  if (n == 0 || meta == nullptr) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  if (n > MAX_META_COUNT) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  std::array<MetaPair, MAX_META_COUNT> pairs;
  for (size_t i = 0; i < n; ++i) {
    pairs[i] = {sv(meta[i].key, meta[i].key_len),
                sv(meta[i].value, meta[i].value_len)};
  }
  if (!kvrgw::encode_metadata(std::span<const MetaPair>(pairs.data(), n), out,
                              out_size)) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  return KVRGW_ERR_OK;
}

std::optional<version_id_t> parse_version(const char *p, size_t n)
{
  const auto hex = sv(p, n);
  if (hex.empty()) {
    return std::nullopt;
  }
  return version_id_t::from_hex(hex);
}

VersioningState to_vs(uint8_t state)
{
  if (state == KVRGW_VERSIONING_ENABLED) {
    return VERSIONING_ENABLED;
  }
  if (state == KVRGW_VERSIONING_SUSPENDED) {
    return VERSIONING_SUSPENDED;
  }
  return VERSIONING_DISABLED;
}

kvrgw::KvRgwServiceImpl *svc(KvRgwHandle *h)
{
  return h == nullptr ? nullptr
                      : &reinterpret_cast<kvrgw::KvRgwRuntime *>(h)->service();
}

kvrgw_err_t abi_guard(auto &&fn)
{
  try {
    return fn();
  }
  catch (...) {
    return KVRGW_ERR_INTERNAL;
  }
}

bool ok(auto ec) { return static_cast<kvrgw_err_t>(ec) == KVRGW_ERR_OK; }

} // namespace

extern "C" {

kvrgw_err_t kvrgw_start(const char *data_root, size_t data_root_len,
                        KvRgwHandle **out)
{
  return abi_guard([&]() -> kvrgw_err_t {
    if (out == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    *out = nullptr;
    auto *rt = new (std::nothrow) kvrgw::KvRgwRuntime();
    if (rt == nullptr) {
      return KVRGW_ERR_INTERNAL;
    }
    kvrgw::KvRgwStartOptions opts;
    opts.perf_mode = false;
    opts.data_root = std::string(sv(data_root, data_root_len));
    if (!rt->start(opts)) {
      delete rt;
      return KVRGW_ERR_INTERNAL;
    }
    *out = reinterpret_cast<KvRgwHandle *>(rt);
    return KVRGW_ERR_OK;
  });
}

void kvrgw_stop(KvRgwHandle *h)
{
  if (h == nullptr) {
    return;
  }
  try {
    auto *rt = reinterpret_cast<kvrgw::KvRgwRuntime *>(h);
    rt->stop();
    delete rt;
  }
  catch (...) {
  }
}

kvrgw_err_t kvrgw_add_tenant(KvRgwHandle *h, const char *name, size_t name_len,
                             uint32_t *out_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_id == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::tenant_id_t id = 0;
    const auto ec = s->add_tenant(sv(name, name_len), &id);
    if (ok(ec)) {
      *out_id = id;
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_resolve_tenant(KvRgwHandle *h, const char *name,
                                 size_t name_len, int *out_exists,
                                 uint32_t *out_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_exists == nullptr || out_id == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    bool exists = false;
    kvrgw::tenant_id_t id = 0;
    const auto ec = s->resolve_tenant(sv(name, name_len), &exists, &id);
    *out_exists = exists ? 1 : 0;
    *out_id = id;
    return ec;
  });
}

kvrgw_err_t kvrgw_create_bucket(KvRgwHandle *h, uint32_t tenant_id,
                                const char *bucket, size_t bucket_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->create_bucket(tenant_id, sv(bucket, bucket_len));
  });
}

kvrgw_err_t kvrgw_delete_bucket(KvRgwHandle *h, uint32_t tenant_id,
                                const char *bucket, size_t bucket_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->delete_bucket(tenant_id, sv(bucket, bucket_len));
  });
}

kvrgw_err_t kvrgw_bucket_exists(KvRgwHandle *h, uint32_t tenant_id,
                                const char *bucket, size_t bucket_len,
                                int *out_exists, uint64_t *out_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_exists == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    bool exists = false;
    kvrgw::bucket_id_t id{};
    const auto ec =
        s->bucket_exists(tenant_id, sv(bucket, bucket_len), &exists, &id);
    *out_exists = exists ? 1 : 0;
    if (out_id != nullptr) {
      *out_id = static_cast<uint64_t>(id);
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_bucket_exists_cached(KvRgwHandle *h, uint32_t tenant_id,
                                       const char *bucket, size_t bucket_len,
                                       int *out_exists, uint64_t *out_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_exists == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    bool exists = false;
    kvrgw::bucket_id_t id{};
    const auto ec = s->bucket_exists_cached(tenant_id, sv(bucket, bucket_len),
                                            &exists, &id);
    *out_exists = exists ? 1 : 0;
    if (out_id != nullptr) {
      *out_id = static_cast<uint64_t>(id);
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_put_bucket_versioning(KvRgwHandle *h, uint32_t tenant_id,
                                        const char *bucket, size_t bucket_len,
                                        uint8_t state)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->put_bucket_versioning(tenant_id, sv(bucket, bucket_len),
                                    to_vs(state));
  });
}

kvrgw_err_t kvrgw_get_bucket_versioning(KvRgwHandle *h, uint32_t tenant_id,
                                        const char *bucket, size_t bucket_len,
                                        uint8_t *out_state)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_state == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    VersioningState st = VERSIONING_DISABLED;
    const auto ec =
        s->get_bucket_versioning(tenant_id, sv(bucket, bucket_len), &st);
    *out_state = static_cast<uint8_t>(st);
    return ec;
  });
}

kvrgw_err_t kvrgw_put_bucket_policy(KvRgwHandle *h, uint32_t tenant_id,
                                    const char *bucket, size_t bucket_len,
                                    const char *policy, size_t policy_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->put_bucket_policy(tenant_id, sv(bucket, bucket_len),
                                sv(policy, policy_len));
  });
}

kvrgw_err_t kvrgw_get_bucket_policy(KvRgwHandle *h, uint32_t tenant_id,
                                    const char *bucket, size_t bucket_len,
                                    char *buf, size_t buf_cap, size_t *out_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || out_len == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    std::string policy;
    const auto ec =
        s->get_bucket_policy(tenant_id, sv(bucket, bucket_len), &policy);
    *out_len = policy.size();
    if (!ok(ec)) {
      return ec;
    }
    if (buf == nullptr || buf_cap < policy.size()) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (!policy.empty()) {
      std::memcpy(buf, policy.data(), policy.size());
    }
    return KVRGW_ERR_OK;
  });
}

kvrgw_err_t kvrgw_delete_bucket_policy(KvRgwHandle *h, uint32_t tenant_id,
                                       const char *bucket, size_t bucket_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->delete_bucket_policy(tenant_id, sv(bucket, bucket_len));
  });
}

kvrgw_err_t kvrgw_put_object(
    KvRgwHandle *h, uint32_t tenant_id, const char *bucket, size_t bucket_len,
    const char *key, size_t key_len, const uint8_t *data, size_t data_len,
    const char *content_type, size_t content_type_len, uint64_t estimated_size,
    const char *if_match, size_t if_match_len, const char *if_none_match,
    size_t if_none_match_len, const kvrgw_kv *tags, size_t tags_count,
    const kvrgw_kv *metadata, size_t metadata_count, char *etag_out,
    size_t etag_cap, char *version_id_out, size_t version_id_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (data_len > 0 && data == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (tags_count > kMaxTags) {
      return KVRGW_ERR_INVALID_TAG;
    }

    auto ct = sv(content_type, content_type_len);
    if (ct.empty()) {
      ct = "application/octet-stream";
    }
    if (ct.size() > kMaxContentTypeLen) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }

    const auto ref_tag = s->ref_tags().next();

    unsigned char digest[16];
    unsigned int digest_len = 0;
    EVP_MD_CTX *md5_ctx = EVP_MD_CTX_new();
    if (md5_ctx == nullptr) {
      return KVRGW_ERR_INTERNAL;
    }
    EVP_DigestInit_ex(md5_ctx, EVP_md5(), nullptr);
    if (data_len > 0) {
      EVP_DigestUpdate(md5_ctx, data, data_len);
    }
    EVP_DigestFinal_ex(md5_ctx, digest, &digest_len);
    EVP_MD_CTX_free(md5_ctx);

    ObjectValue object_value;
    std::memcpy(object_value.hdr.ref_tag, ref_tag.data(),
                sizeof(object_value.hdr.ref_tag));
    object_value.set_etag_raw(digest);
    object_value.hdr.size = data_len;
    const auto now_tp = std::chrono::system_clock::now();
    const auto now_s = std::chrono::duration_cast<std::chrono::seconds>(
        now_tp.time_since_epoch());
    const auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now_tp.time_since_epoch()) -
        std::chrono::duration_cast<std::chrono::nanoseconds>(now_s);
    object_value.hdr.last_modified_sec = static_cast<uint32_t>(now_s.count());
    object_value.hdr.last_modified_nsec = static_cast<uint32_t>(now_ns.count());
    object_value.content_type = std::string(ct);

    std::optional<kvrgw::KvRgwServiceImpl::PutCondition> put_cond;
    if (if_match_len > 0 || if_none_match_len > 0) {
      put_cond.emplace();
      put_cond->if_match = std::string(sv(if_match, if_match_len));
      put_cond->if_none_match =
          std::string(sv(if_none_match, if_none_match_len));
    }

    std::vector<uint8_t> tag_buf;
    if (tags_count == 0) {
      if (tags != nullptr) {
        return KVRGW_ERR_INVALID_TAG;
      }
    }
    else {
      if (tags == nullptr) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      if (auto ec = encode_kv_tags(tags, tags_count, tag_buf);
          ec != KVRGW_ERR_OK) {
        return ec;
      }
    }

    std::array<uint8_t, MAX_META_FRAME_BYTES> meta_buf{};
    size_t meta_len = 0;
    if (metadata_count > 0) {
      if (metadata == nullptr) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      if (auto ec =
              encode_kv_metadata(metadata, metadata_count, meta_buf, meta_len);
          ec != KVRGW_ERR_OK) {
        return ec;
      }
      object_value.hdr.metadata_count = static_cast<uint16_t>(metadata_count);
      object_value.metadata_frame.assign(meta_buf.data(),
                                         meta_buf.data() + meta_len);
    }

    kvrgw::KvRgwServiceImpl::PutObjectRequest req;
    req.tenant_id = tenant_id;
    req.bucket_name = std::string(sv(bucket, bucket_len));
    req.object_name = std::string(sv(key, key_len));
    req.ref_tag = ref_tag;
    req.value = std::move(object_value);
    req.estimated_size = estimated_size > 0 ? estimated_size : data_len;
    req.cond = put_cond ? &*put_cond : nullptr;
    if (!tag_buf.empty()) {
      req.tags = std::span<const uint8_t>(tag_buf.data(), tag_buf.size());
    }
    if (meta_len > 0) {
      req.metadata = std::span<const uint8_t>(meta_buf.data(), meta_len);
    }

    auto result = s->put_object_route(req, data, data_len);
    if (!ok(result.error_code)) {
      return result.error_code;
    }
    copy_cstr(etag_out, etag_cap, result.etag);
    if (result.version_id.is_valid()) {
      copy_cstr(version_id_out, version_id_cap, result.version_id.to_hex());
    }
    else if (version_id_out != nullptr && version_id_cap > 0) {
      version_id_out[0] = '\0';
    }
    return KVRGW_ERR_OK;
  });
}

kvrgw_err_t kvrgw_get_object(
    KvRgwHandle *h, uint32_t tenant_id, const char *bucket, size_t bucket_len,
    const char *key, size_t key_len, const char *version_id,
    size_t version_id_len, const kvrgw_byte_range *range,
    kvrgw_object_meta *meta, uint8_t *body, size_t body_cap, size_t *body_len,
    kvrgw_kv *metadata, uint32_t metadata_cap, uint32_t *metadata_count,
    char *meta_arena, size_t meta_arena_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || meta == nullptr || body_len == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::ByteRange br;
    const kvrgw::ByteRange *brp = nullptr;
    if (range != nullptr) {
      br.start = range->start;
      br.end = range->end;
      br.from_end = range->from_end != 0;
      br.end_unbounded = range->end_unbounded != 0;
      brp = &br;
    }
    kvrgw::KvRgwServiceImpl::GetObjectResult out;
    const auto ec =
        s->get_object(tenant_id, sv(bucket, bucket_len), sv(key, key_len),
                      parse_version(version_id, version_id_len), brp, &out);
    std::memset(meta, 0, sizeof(*meta));
    *body_len = out.body.size();
    if (!out.error_detail.empty()) {
      copy_cstr(meta->error_detail, sizeof(meta->error_detail),
                out.error_detail);
    }
    if (!ok(ec)) {
      return ec;
    }
    fill_object_meta(out.value, meta);
    if (body == nullptr || body_cap < out.body.size()) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (!out.body.empty()) {
      std::memcpy(body, out.body.data(), out.body.size());
    }
    return fill_decoded_metadata(
        out.value.metadata_frame, out.value.hdr.metadata_count, metadata,
        metadata_cap, metadata_count, meta_arena, meta_arena_cap);
  });
}

kvrgw_err_t kvrgw_head_object(KvRgwHandle *h, uint32_t tenant_id,
                              const char *bucket, size_t bucket_len,
                              const char *key, size_t key_len,
                              const char *version_id, size_t version_id_len,
                              kvrgw_object_meta *meta, kvrgw_kv *metadata,
                              uint32_t metadata_cap, uint32_t *metadata_count,
                              char *meta_arena, size_t meta_arena_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || meta == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    ObjectValue value;
    std::string error_detail;
    const auto ec = s->head_object(
        tenant_id, sv(bucket, bucket_len), sv(key, key_len),
        parse_version(version_id, version_id_len), &value, &error_detail);
    std::memset(meta, 0, sizeof(*meta));
    if (!error_detail.empty()) {
      copy_cstr(meta->error_detail, sizeof(meta->error_detail), error_detail);
    }
    if (!ok(ec)) {
      return ec;
    }
    fill_object_meta(value, meta);
    return fill_decoded_metadata(value.metadata_frame, value.hdr.metadata_count,
                                 metadata, metadata_cap, metadata_count,
                                 meta_arena, meta_arena_cap);
  });
}

kvrgw_err_t kvrgw_delete_object(KvRgwHandle *h, uint32_t tenant_id,
                                const char *bucket, size_t bucket_len,
                                const char *key, size_t key_len,
                                const char *if_match, size_t if_match_len,
                                int64_t if_match_mtime, int64_t if_match_size,
                                int has_if_match_size, int *created_dm,
                                uint32_t *dm_version_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::DeleteCondition cond;
    const kvrgw::KvRgwServiceImpl::DeleteCondition *cp = nullptr;
    if (if_match_len > 0 || if_match_mtime != 0 || has_if_match_size) {
      cond.if_match = std::string(sv(if_match, if_match_len));
      cond.if_match_last_modified_time = if_match_mtime;
      cond.if_match_size = if_match_size;
      cond.has_if_match_size = has_if_match_size != 0;
      cp = &cond;
    }
    kvrgw::KvRgwServiceImpl::DeleteResult out;
    const auto ec = s->delete_object(tenant_id, sv(bucket, bucket_len),
                                     sv(key, key_len), cp, &out);
    if (created_dm != nullptr) {
      *created_dm = out.created_dm ? 1 : 0;
    }
    if (dm_version_id != nullptr) {
      *dm_version_id = out.dm_version_id.raw();
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_delete_object_version(
    KvRgwHandle *h, uint32_t tenant_id, const char *bucket, size_t bucket_len,
    const char *key, size_t key_len, const char *version_id,
    size_t version_id_len, const char *if_match, size_t if_match_len,
    int64_t if_match_mtime, int64_t if_match_size, int has_if_match_size)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || version_id_len == 0) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::DeleteCondition cond;
    const kvrgw::KvRgwServiceImpl::DeleteCondition *cp = nullptr;
    if (if_match_len > 0 || if_match_mtime != 0 || has_if_match_size) {
      cond.if_match = std::string(sv(if_match, if_match_len));
      cond.if_match_last_modified_time = if_match_mtime;
      cond.if_match_size = if_match_size;
      cond.has_if_match_size = has_if_match_size != 0;
      cp = &cond;
    }
    return s->delete_object_version(
        tenant_id, sv(bucket, bucket_len), sv(key, key_len),
        version_id_t::from_hex(sv(version_id, version_id_len)), cp);
  });
}

kvrgw_err_t kvrgw_delete_multi(KvRgwHandle *h, uint32_t tenant_id,
                               const char *bucket, size_t bucket_len,
                               const kvrgw_buf *keys, size_t keys_count,
                               const kvrgw_kv *objects, size_t objects_count,
                               kvrgw_del_multi_out *outs, uint32_t outs_cap,
                               uint32_t *outs_count, char *arena,
                               size_t arena_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || outs_count == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if ((keys_count > 0 && keys == nullptr) ||
        (objects_count > 0 && objects == nullptr)) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    std::vector<std::string> key_store;
    key_store.reserve(keys_count);
    for (size_t i = 0; i < keys_count; ++i) {
      key_store.emplace_back(sv(keys[i].data, keys[i].len));
    }
    std::vector<kvrgw::KvRgwServiceImpl::DeleteMultiObjectRef> obj_refs;
    obj_refs.reserve(objects_count);
    for (size_t i = 0; i < objects_count; ++i) {
      kvrgw::KvRgwServiceImpl::DeleteMultiObjectRef r;
      r.key = sv(objects[i].key, objects[i].key_len);
      r.version_id = sv(objects[i].value, objects[i].value_len);
      obj_refs.push_back(r);
    }
    std::vector<kvrgw::KvRgwServiceImpl::DeleteMultiKeyOutcome> outcomes;
    const auto ec = s->delete_multi(tenant_id, sv(bucket, bucket_len),
                                    key_store, obj_refs, &outcomes);
    if (outcomes.size() > outs_cap) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    size_t used = 0;
    *outs_count = static_cast<uint32_t>(outcomes.size());
    for (size_t i = 0; i < outcomes.size(); ++i) {
      auto &o = outcomes[i];
      auto &e = outs[i];
      std::memset(&e, 0, sizeof(e));
      if (auto a =
              arena_put(arena, arena_cap, &used, o.key, &e.key_off, &e.key_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      e.status = (o.status ==
                  kvrgw::KvRgwServiceImpl::DeleteMultiKeyOutcome::Status::Error)
                     ? 1
                     : 0;
      if (auto a = arena_put(arena, arena_cap, &used, o.error_code,
                             &e.err_code_off, &e.err_code_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      if (auto a = arena_put(arena, arena_cap, &used, o.error_message,
                             &e.err_msg_off, &e.err_msg_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      if (auto a = arena_put(arena, arena_cap, &used, o.version_id,
                             &e.version_id_off, &e.version_id_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      e.created_dm = o.created_dm ? 1 : 0;
      e.dm_version_id = o.dm_version_id.raw();
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_list_buckets(KvRgwHandle *h, uint32_t tenant_id,
                               const char *prefix, size_t prefix_len,
                               const char *continuation,
                               size_t continuation_len, uint32_t max_buckets,
                               kvrgw_list_bkt_entry *entries,
                               uint32_t entries_cap, uint32_t *entries_count,
                               char *arena, size_t arena_cap, char *next_token,
                               size_t next_token_cap, size_t *next_token_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || entries_count == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::ListBucketsResult out;
    const auto ec =
        s->list_buckets(tenant_id, sv(prefix, prefix_len),
                        sv(continuation, continuation_len), max_buckets, &out);
    *entries_count = static_cast<uint32_t>(out.buckets.size());
    if (!ok(ec)) {
      return static_cast<kvrgw_err_t>(ec);
    }
    if (out.buckets.size() > entries_cap) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    size_t used = 0;
    for (size_t i = 0; i < out.buckets.size(); ++i) {
      auto &e = entries[i];
      e.created_at_unix = out.buckets[i].created_at_unix;
      if (auto a = arena_put(arena, arena_cap, &used, out.buckets[i].name,
                             &e.name_off, &e.name_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
    }
    if (next_token_len != nullptr) {
      *next_token_len = out.continuation_token.size();
    }
    if (!out.continuation_token.empty()) {
      if (next_token == nullptr ||
          next_token_cap < out.continuation_token.size()) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      std::memcpy(next_token, out.continuation_token.data(),
                  out.continuation_token.size());
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_list_objects(
    KvRgwHandle *h, uint32_t tenant_id, const char *bucket, size_t bucket_len,
    const char *prefix, size_t prefix_len, const char *delimiter,
    size_t delimiter_len, uint32_t max_keys, const char *continuation,
    size_t continuation_len, const char *marker, size_t marker_len,
    kvrgw_list_obj_entry *entries, uint32_t entries_cap,
    uint32_t *entries_count, uint32_t *prefix_offs, uint32_t *prefix_lens,
    uint32_t prefixes_cap, uint32_t *prefixes_count, char *arena,
    size_t arena_cap, int *is_truncated, char *next_token,
    size_t next_token_cap, size_t *next_token_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || entries_count == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (max_keys > KVRGW_MAX_LIST_KEYS) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::ListObjectsResult out;
    const auto ec = s->list_objects(
        tenant_id, sv(bucket, bucket_len), sv(prefix, prefix_len),
        sv(delimiter, delimiter_len), max_keys,
        sv(continuation, continuation_len), sv(marker, marker_len), &out);
    if (out.objects.size() > entries_cap ||
        out.common_prefixes.size() > prefixes_cap) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    size_t used = 0;
    *entries_count = static_cast<uint32_t>(out.objects.size());
    for (size_t i = 0; i < out.objects.size(); ++i) {
      auto &e = entries[i];
      e.size = out.objects[i].size;
      e.last_modified_unix = out.objects[i].last_modified_unix;
      if (auto a = arena_put(arena, arena_cap, &used, out.objects[i].key,
                             &e.key_off, &e.key_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      if (auto a = arena_put(arena, arena_cap, &used, out.objects[i].etag,
                             &e.etag_off, &e.etag_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
    }
    if (prefixes_count != nullptr) {
      *prefixes_count = static_cast<uint32_t>(out.common_prefixes.size());
    }
    for (size_t i = 0; i < out.common_prefixes.size(); ++i) {
      if (auto a = arena_put(arena, arena_cap, &used, out.common_prefixes[i],
                             &prefix_offs[i], &prefix_lens[i]);
          a != KVRGW_ERR_OK) {
        return a;
      }
    }
    if (is_truncated != nullptr) {
      *is_truncated = out.is_truncated ? 1 : 0;
    }
    if (next_token_len != nullptr) {
      *next_token_len = out.next_continuation_token.size();
    }
    if (!out.next_continuation_token.empty()) {
      if (next_token == nullptr ||
          next_token_cap < out.next_continuation_token.size()) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      std::memcpy(next_token, out.next_continuation_token.data(),
                  out.next_continuation_token.size());
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_list_object_versions(
    KvRgwHandle *h, uint32_t tenant_id, const char *bucket, size_t bucket_len,
    const char *prefix, size_t prefix_len, uint32_t max_keys,
    const char *key_marker, size_t key_marker_len, uint32_t version_id_marker,
    kvrgw_list_ver_entry *entries, uint32_t entries_cap,
    uint32_t *entries_count, char *arena, size_t arena_cap, int *is_truncated,
    char *next_key, size_t next_key_cap, size_t *next_key_len,
    uint32_t *next_version_id)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || entries_count == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (max_keys > KVRGW_MAX_LIST_KEYS) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::ListObjectVersionsResult out;
    const auto ec = s->list_object_versions(
        tenant_id, sv(bucket, bucket_len), sv(prefix, prefix_len), max_keys,
        sv(key_marker, key_marker_len), version_id_t{version_id_marker}, &out);
    if (out.versions.size() > entries_cap) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    size_t used = 0;
    *entries_count = static_cast<uint32_t>(out.versions.size());
    for (size_t i = 0; i < out.versions.size(); ++i) {
      auto &e = entries[i];
      e.version_id = out.versions[i].version_id.raw();
      e.is_latest = out.versions[i].is_latest ? 1 : 0;
      e.is_delete_marker = out.versions[i].is_delete_marker ? 1 : 0;
      e.size = out.versions[i].size;
      e.last_modified_unix = out.versions[i].last_modified_unix;
      if (auto a = arena_put(arena, arena_cap, &used, out.versions[i].key,
                             &e.key_off, &e.key_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
      if (auto a = arena_put(arena, arena_cap, &used, out.versions[i].etag,
                             &e.etag_off, &e.etag_len);
          a != KVRGW_ERR_OK) {
        return a;
      }
    }
    if (is_truncated != nullptr) {
      *is_truncated = out.is_truncated ? 1 : 0;
    }
    if (next_key_len != nullptr) {
      *next_key_len = out.next_key_marker.size();
    }
    if (!out.next_key_marker.empty()) {
      if (next_key == nullptr || next_key_cap < out.next_key_marker.size()) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      std::memcpy(next_key, out.next_key_marker.data(),
                  out.next_key_marker.size());
    }
    if (next_version_id != nullptr) {
      *next_version_id = out.next_version_id_marker.raw();
    }
    return ec;
  });
}

kvrgw_err_t kvrgw_copy_object(KvRgwHandle *h, uint32_t tenant_id,
                              const kvrgw_copy_args *args, char *etag_out,
                              size_t etag_cap, int64_t *last_modified_unix,
                              char *version_id_out, size_t version_id_cap,
                              char *src_version_id_out,
                              size_t src_version_id_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || args == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    std::array<uint8_t, MAX_META_FRAME_BYTES> copy_meta_buf{};
    size_t meta_n = args->metadata_count;
    if (meta_n > 0 && args->metadata == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    kvrgw::KvRgwServiceImpl::CopyObjectRequest req;
    req.tenant_id = tenant_id;
    req.src_bucket_name = sv(args->src_bucket.data, args->src_bucket.len);
    req.src_key = sv(args->src_key.data, args->src_key.len);
    req.dst_bucket_name = sv(args->dst_bucket.data, args->dst_bucket.len);
    req.dst_key = sv(args->dst_key.data, args->dst_key.len);
    req.src_version_id =
        parse_version(args->src_version_id.data, args->src_version_id.len);
    req.if_match = sv(args->if_match.data, args->if_match.len);
    req.if_none_match = sv(args->if_none_match.data, args->if_none_match.len);
    req.dst_if_match = sv(args->dst_if_match.data, args->dst_if_match.len);
    req.dst_if_none_match =
        sv(args->dst_if_none_match.data, args->dst_if_none_match.len);
    req.content_type = sv(args->content_type.data, args->content_type.len);
    req.replace_metadata = args->replace_metadata != 0;
    if (req.replace_metadata && meta_n > 0) {
      size_t copy_meta_len = 0;
      if (auto ec = encode_kv_metadata(args->metadata, meta_n, copy_meta_buf,
                                       copy_meta_len);
          ec != KVRGW_ERR_OK) {
        return ec;
      }
      req.metadata =
          std::span<const uint8_t>(copy_meta_buf.data(), copy_meta_len);
    }
    req.replace_tags = args->replace_tags != 0;
    std::vector<uint8_t> copy_tag_buf;
    if (req.replace_tags) {
      if (auto ec = encode_kv_tags(args->tags, args->tags_count, copy_tag_buf);
          ec != KVRGW_ERR_OK) {
        return ec;
      }
      req.tags =
          std::span<const uint8_t>(copy_tag_buf.data(), copy_tag_buf.size());
    }

    kvrgw::KvRgwServiceImpl::CopyObjectResult out;
    const auto ec = s->copy_object(req, &out);
    if (!ok(ec)) {
      return ec;
    }
    copy_cstr(etag_out, etag_cap, out.etag);
    if (last_modified_unix != nullptr) {
      *last_modified_unix = out.last_modified_unix;
    }
    if (out.version_id) {
      copy_cstr(version_id_out, version_id_cap, out.version_id->to_hex());
    }
    else if (version_id_out != nullptr && version_id_cap > 0) {
      version_id_out[0] = '\0';
    }
    copy_cstr(src_version_id_out, src_version_id_cap,
              out.copy_source_version_id.to_hex());
    return KVRGW_ERR_OK;
  });
}

kvrgw_err_t kvrgw_put_object_tagging(KvRgwHandle *h, uint32_t tenant_id,
                                     const char *bucket, size_t bucket_len,
                                     const char *key, size_t key_len,
                                     const kvrgw_kv *tags, size_t tags_count)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    if (tags_count > kMaxTags) {
      return KVRGW_ERR_INVALID_TAG;
    }
    if (tags_count == 0) {
      return s->put_object_tagging(tenant_id, sv(bucket, bucket_len),
                                   sv(key, key_len), {});
    }
    if (tags == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    std::vector<uint8_t> tag_buf;
    if (auto ec = encode_kv_tags(tags, tags_count, tag_buf);
        ec != KVRGW_ERR_OK) {
      return ec;
    }
    return s->put_object_tagging(
        tenant_id, sv(bucket, bucket_len), sv(key, key_len),
        std::span<const uint8_t>(tag_buf.data(), tag_buf.size()));
  });
}

kvrgw_err_t kvrgw_get_object_tagging(KvRgwHandle *h, uint32_t tenant_id,
                                     const char *bucket, size_t bucket_len,
                                     const char *key, size_t key_len,
                                     kvrgw_kv *tags, uint32_t tags_cap,
                                     uint32_t *tags_count, char *arena,
                                     size_t arena_cap)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr || tags_count == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    std::vector<uint8_t> live;
    std::array<TagPair, MAX_TAG_COUNT> views{};
    size_t n = 0;
    const auto ec = s->get_object_tagging(tenant_id, sv(bucket, bucket_len),
                                          sv(key, key_len), live, views, &n);
    if (!ok(ec)) {
      return ec;
    }
    *tags_count = static_cast<uint32_t>(n);
    if (n > tags_cap) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    size_t used = 0;
    for (size_t i = 0; i < n; ++i) {
      uint32_t koff = 0, klen = 0, voff = 0, vlen = 0;
      if (auto a =
              arena_put(arena, arena_cap, &used, views[i].first, &koff, &klen);
          a != KVRGW_ERR_OK) {
        return a;
      }
      if (auto a =
              arena_put(arena, arena_cap, &used, views[i].second, &voff, &vlen);
          a != KVRGW_ERR_OK) {
        return a;
      }
      tags[i].key = arena + koff;
      tags[i].key_len = klen;
      tags[i].value = arena + voff;
      tags[i].value_len = vlen;
    }
    return KVRGW_ERR_OK;
  });
}

kvrgw_err_t kvrgw_delete_object_tagging(KvRgwHandle *h, uint32_t tenant_id,
                                        const char *bucket, size_t bucket_len,
                                        const char *key, size_t key_len)
{
  return abi_guard([&]() -> kvrgw_err_t {
    auto *s = svc(h);
    if (s == nullptr) {
      return KVRGW_ERR_INVALID_ARGUMENT;
    }
    return s->delete_object_tagging(tenant_id, sv(bucket, bucket_len),
                                    sv(key, key_len));
  });
}

} // extern "C"
