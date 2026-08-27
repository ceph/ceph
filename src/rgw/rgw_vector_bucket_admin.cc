// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_vector_bucket_admin.h"

#include <cerrno>
#include <memory>

#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "rgw_sal.h"
#include "rgw_formats.h"
#include "rgw_s3vector.h"
#include "rgw_s3vector_background.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

static void dump_cache_stats(const char* name,
                             const LanceDBSessionCacheStats& stats,
                             Formatter* formatter)
{
  formatter->open_object_section(name);
  encode_json("hits", stats.hits, formatter);
  encode_json("misses", stats.misses, formatter);
  encode_json("num_entries", stats.num_entries, formatter);
  encode_json("size_bytes", stats.size_bytes, formatter);
  formatter->close_section();
}

static int load_vector_bucket(rgw::sal::Driver* driver,
                              const RGWVectorBucketAdminOpState& op_state,
                              std::unique_ptr<rgw::sal::VectorBucket>& bucket,
                              optional_yield y,
                              const DoutPrefixProvider* dpp)
{
  if (op_state.bucket_name.empty()) {
    return -EINVAL;
  }

  const rgw_bucket bucket_id(op_state.uid.tenant, op_state.bucket_name);
  int ret = driver->load_vector_bucket(dpp, bucket_id, &bucket, y);
  if (ret == -ENOENT) {
    return -ERR_NO_SUCH_BUCKET;
  }
  return ret;
}

int RGWVectorBucketAdminOp::get_session_info(rgw::sal::Driver* driver,
                                             RGWVectorBucketAdminOpState& op_state,
                                             RGWFormatterFlusher& flusher,
                                             optional_yield y,
                                             const DoutPrefixProvider* dpp)
{
  if (op_state.bucket_name.empty() || !op_state.uid.empty()) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::VectorBucket> bucket;
  int ret = load_vector_bucket(driver, op_state, bucket, y, dpp);
  if (ret < 0) {
    return ret;
  }

  LanceDBSessionCacheStats index_cache_stats{};
  LanceDBSessionCacheStats metadata_cache_stats{};
  bool session_active = false;

  ret = get_index_cache_stats(dpp, op_state.bucket_name, index_cache_stats);
  if (ret == -ENOENT) {
    ret = 0;
  } else if (ret < 0) {
    return ret;
  } else {
    ret = get_metadata_cache_stats(dpp, op_state.bucket_name,
                                   metadata_cache_stats);
    if (ret < 0) {
      return ret;
    }
    session_active = true;
  }

  Formatter* formatter = flusher.get_formatter();
  flusher.start(0);
  formatter->open_object_section("");
  encode_json("vectorbucket", op_state.bucket_name, formatter);
  formatter->open_object_section("session");
  encode_json("active", session_active, formatter);
  if (session_active) {
    dump_cache_stats("index_cache", index_cache_stats, formatter);
    dump_cache_stats("metadata_cache", metadata_cache_stats, formatter);
  }
  formatter->close_section();
  formatter->close_section();
  flusher.flush();
  return 0;
}

int RGWVectorBucketAdminOp::list_sessions(rgw::sal::Driver* driver,
                                          RGWVectorBucketAdminOpState& op_state,
                                          RGWFormatterFlusher& flusher,
                                          optional_yield y,
                                          const DoutPrefixProvider* dpp)
{
  if (op_state.uid.empty() || !op_state.bucket_name.empty()) {
    return -EINVAL;
  }

  auto user = driver->get_user(op_state.uid);
  int ret = user->load_user(dpp, y);
  if (ret == -ENOENT) {
    return -ERR_NO_SUCH_USER;
  }
  if (ret < 0) {
    return ret;
  }

  rgw::sal::BucketList listing;
  ret = driver->list_vector_buckets(dpp, user->get_id(), op_state.uid.tenant,
                                    op_state.marker, "", op_state.max_entries,
                                    listing, y);
  if (ret < 0) {
    return ret;
  }

  Formatter* formatter = flusher.get_formatter();
  flusher.start(0);
  formatter->open_object_section("");
  encode_json("uid", op_state.uid.to_str(), formatter);
  if (!listing.next_marker.empty()) {
    encode_json("marker", op_state.marker, formatter);
    encode_json("next_marker", listing.next_marker, formatter);
  }
  formatter->open_array_section("sessions");
  for (const auto& bucket : listing.buckets) {
    LanceDBSessionCacheStats index_cache_stats{};
    LanceDBSessionCacheStats metadata_cache_stats{};

    ret = get_index_cache_stats(dpp, bucket.bucket.name, index_cache_stats);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0) {
      return ret;
    }

    ret = get_metadata_cache_stats(dpp, bucket.bucket.name, metadata_cache_stats);
    if (ret < 0) {
      return ret;
    }

    formatter->open_object_section("");
    encode_json("vectorbucket", bucket.bucket.name, formatter);
    dump_cache_stats("index_cache", index_cache_stats, formatter);
    dump_cache_stats("metadata_cache", metadata_cache_stats, formatter);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->close_section();
  flusher.flush();
  return 0;
}

int RGWVectorBucketAdminOp::remove_session(rgw::sal::Driver* driver,
                                           RGWVectorBucketAdminOpState& op_state,
                                           const DoutPrefixProvider* dpp,
                                           optional_yield y)
{
  if (op_state.bucket_name.empty() || !op_state.uid.empty()) {
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::VectorBucket> bucket;
  int ret = load_vector_bucket(driver, op_state, bucket, y, dpp);
  if (ret < 0) {
    return ret;
  }

  ret = delete_session(dpp, op_state.bucket_name);
  return ret == -ENOENT ? 0 : ret;
}

} // namespace rgw::s3vector
