// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * SAL-level fixtures for the librgw test suites.
 *
 * The rgw_file C API cannot express some bucket state that NFS
 * behaviour depends on.  Versioning is the case that matters:  S3 sets
 * it through PutBucketVersioning, and librgw has no equivalent, but the
 * driver must still behave correctly for an NFS client operating in a
 * versioned bucket.
 *
 * Reach the SAL directly for that rather than adding API surface for
 * testability -- the same reasoning as the private-view tests, which
 * reach RGWLibFS/RGWFileHandle to observe refcounts and binding state.
 * A driver hint would earn its place if this had to be driven from
 * outside the process, or if the driver had to misbehave deliberately;
 * bucket state is neither.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rgw_lib.h"
#include "rgw_sal.h"
#include "rgw_common.h"

namespace librgw_test {

enum class Versioning { Off, Enabled, Suspended };

/* Set a bucket's versioning state.
 *
 * Suspended is not the same as Off:  a suspended bucket is still
 * versioned() -- its existing versions remain -- but is no longer
 * versioning_enabled(), and S3 says a write to it overwrites the null
 * version rather than minting a new one. */
inline int set_bucket_versioning(const DoutPrefixProvider* dpp,
                                 const std::string& bucket_name,
                                 Versioning v)
{
  auto* driver = rgw::g_rgwlib->get_driver();
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int rc = driver->load_bucket(dpp, rgw_bucket("", bucket_name),
                               &bucket, null_yield);
  if (rc < 0) {
    return rc;
  }

  auto& info = bucket->get_info();
  info.flags &= ~(BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED);
  switch (v) {
  case Versioning::Enabled:
    info.flags |= BUCKET_VERSIONED;
    break;
  case Versioning::Suspended:
    info.flags |= BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED;
    break;
  case Versioning::Off:
    break;
  }

  return bucket->put_info(dpp, false, real_time(), null_yield);
}

/* Read back what the driver believes, so a test can assert its fixture
 * actually took effect rather than assuming it did. */
inline int get_bucket_versioning(const DoutPrefixProvider* dpp,
                                 const std::string& bucket_name,
                                 Versioning& out)
{
  auto* driver = rgw::g_rgwlib->get_driver();
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int rc = driver->load_bucket(dpp, rgw_bucket("", bucket_name),
                               &bucket, null_yield);
  if (rc < 0) {
    return rc;
  }
  const auto& info = bucket->get_info();
  if (!info.versioned()) {
    out = Versioning::Off;
  } else if (info.versioning_enabled()) {
    out = Versioning::Enabled;
  } else {
    out = Versioning::Suspended;
  }
  return 0;
}

/* List a bucket through the SAL.  With want_versions the listing
 * includes non-current versions and delete markers. */
inline int list_bucket(const DoutPrefixProvider* dpp,
                       const std::string& bucket_name,
                       bool want_versions,
                       std::vector<rgw_bucket_dir_entry>& out,
                       int max = 1000)
{
  auto* driver = rgw::g_rgwlib->get_driver();
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int rc = driver->load_bucket(dpp, rgw_bucket("", bucket_name),
                               &bucket, null_yield);
  if (rc < 0) {
    return rc;
  }

  rgw::sal::Bucket::ListParams params;
  params.list_versions = want_versions;
  rgw::sal::Bucket::ListResults results;
  rc = bucket->list(dpp, params, max, results, null_yield);
  if (rc < 0) {
    return rc;
  }
  out = std::move(results.objs);
  return 0;
}

/* The listing is cached, and listing a cold bucket rebuilds that cache
 * from disk -- which repairs whatever an incremental update got wrong.
 * A test that means to exercise the incremental path must list once
 * before the operation it cares about, or it passes either way. */
inline int warm_listing_cache(const DoutPrefixProvider* dpp,
                              const std::string& bucket_name,
                              bool want_versions = true)
{
  std::vector<rgw_bucket_dir_entry> ignored;
  return list_bucket(dpp, bucket_name, want_versions, ignored);
}

/* One object's attribute, as HEAD would report it. */
inline int get_object_attr(const DoutPrefixProvider* dpp,
                           const std::string& bucket_name,
                           const rgw_obj_key& key,
                           const std::string& attr_name,
                           std::string& out)
{
  auto* driver = rgw::g_rgwlib->get_driver();
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int rc = driver->load_bucket(dpp, rgw_bucket("", bucket_name),
                               &bucket, null_yield);
  if (rc < 0) {
    return rc;
  }

  auto obj = bucket->get_object(key);
  rc = obj->get_obj_attrs(null_yield, dpp);
  if (rc < 0) {
    return rc;
  }

  auto& attrs = obj->get_attrs();
  auto it = attrs.find(attr_name);
  if (it == attrs.end()) {
    return -ENODATA;
  }
  out = it->second.to_str();
  return 0;
}

/* Drop a bucket's listing cache so the next listing rebuilds it from the
 * store.  Goes through driver_hint() because the cache is driver-internal
 * and a test binary cannot reach it without pulling in driver headers --
 * which is what hints are for. */
inline int invalidate_listing_cache(const DoutPrefixProvider* dpp,
                                    const std::string& bucket_name)
{
  auto* driver = rgw::g_rgwlib->get_driver();
  std::map<std::string, std::string> params{{"bucket", bucket_name}};
  std::map<std::string, std::string> out;
  return driver->driver_hint(dpp, "invalidate-cache", params, &out);
}

} /* namespace librgw_test */
