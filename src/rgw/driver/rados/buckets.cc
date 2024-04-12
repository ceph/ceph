// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "buckets.h"
#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "cls/user/cls_user_client.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_tools.h"

namespace rgwrados::buckets {

static int set(const DoutPrefixProvider* dpp, optional_yield y,
               librados::Rados& rados, const rgw_raw_obj& obj,
               cls_user_bucket_entry&& entry, bool add)
{
  std::list<cls_user_bucket_entry> entries;
  entries.push_back(std::move(entry));

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_set_buckets(op, entries, add);
  return ref.operate(dpp, &op, y);
}

int add(const DoutPrefixProvider* dpp, optional_yield y,
        librados::Rados& rados, const rgw_raw_obj& obj,
        const rgw_bucket& bucket, ceph::real_time creation_time)
{
  cls_user_bucket_entry entry;
  bucket.convert(&entry.bucket);

  if (ceph::real_clock::is_zero(creation_time)) {
    entry.creation_time = ceph::real_clock::now();
  } else {
    entry.creation_time = creation_time;
  }

  constexpr bool add = true; // create/update entry
  return set(dpp, y, rados, obj, std::move(entry), add);
}

int remove(const DoutPrefixProvider* dpp, optional_yield y,
           librados::Rados& rados, const rgw_raw_obj& obj,
           const rgw_bucket& bucket)
{
  cls_user_bucket clsbucket;
  bucket.convert(&clsbucket);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_remove_bucket(op, clsbucket);
  return ref.operate(dpp, &op, y);
}

int list(const DoutPrefixProvider* dpp, optional_yield y,
         librados::Rados& rados, const rgw_raw_obj& obj,
         const std::string& tenant, const std::string& start_marker,
         const std::string& end_marker, uint64_t max,
         rgw::sal::BucketList& listing)
{
  listing.buckets.clear();

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  std::string marker = start_marker;
  bool truncated = false;

  do {
    const uint64_t count = max - listing.buckets.size();
    std::list<cls_user_bucket_entry> entries;

    librados::ObjectReadOperation op;
    int rc = 0;
    ::cls_user_bucket_list(op, marker, end_marker, count,
                           entries, &marker, &truncated, &rc);

    bufferlist bl;
    int r = ref.operate(dpp, &op, &bl, y);
    if (r == -ENOENT) {
      listing.next_marker.clear();
      return 0;
    }
    if (r < 0) {
      return r;
    }
    if (rc < 0) {
      return rc;
    }

    for (auto& entry : entries) {
      RGWBucketEnt ent;
      ent.bucket.tenant = tenant;
      ent.bucket.name = std::move(entry.bucket.name);
      ent.bucket.marker = std::move(entry.bucket.marker);
      ent.bucket.bucket_id = std::move(entry.bucket.bucket_id);
      ent.size = entry.size;
      ent.size_rounded = entry.size_rounded;
      ent.creation_time = entry.creation_time;
      ent.count = entry.count;

      listing.buckets.push_back(std::move(ent));
    }
  } while (truncated && listing.buckets.size() < max);

  if (truncated) {
    listing.next_marker = std::move(marker);
  } else {
    listing.next_marker.clear();
  }
  return 0;
}

int write_stats(const DoutPrefixProvider* dpp, optional_yield y,
                librados::Rados& rados, const rgw_raw_obj& obj,
                const RGWBucketEnt& ent)
{
  cls_user_bucket_entry entry;
  ent.convert(&entry);

  constexpr bool add = false; // bucket entry must exist
  return set(dpp, y, rados, obj, std::move(entry), add);
}

int read_stats(const DoutPrefixProvider* dpp, optional_yield y,
               librados::Rados& rados, const rgw_raw_obj& obj,
               RGWStorageStats& stats, ceph::real_time* last_synced,
               ceph::real_time* last_updated)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  cls_user_header header;
  ::cls_user_get_header(op, &header, nullptr);

  bufferlist bl;
  r = ref.operate(dpp, &op, &bl, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  stats.size = header.stats.total_bytes;
  stats.size_rounded = header.stats.total_bytes_rounded;
  stats.num_objects = header.stats.total_entries;
  if (last_synced) {
    *last_synced = header.last_stats_sync;
  }
  if (last_updated) {
    *last_updated = header.last_stats_update;
  }
  return 0;
}

// callback wrapper for cls_user_get_header_async()
class AsyncHeaderCB : public RGWGetUserHeader_CB {
  boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb;
 public:
  explicit AsyncHeaderCB(boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb)
    : cb(std::move(cb)) {}

  void handle_response(int r, cls_user_header& header) override {
    const cls_user_stats& hs = header.stats;
    RGWStorageStats stats;
    stats.size = hs.total_bytes;
    stats.size_rounded = hs.total_bytes_rounded;
    stats.num_objects = hs.total_entries;
    cb->handle_response(r, stats);
    cb.reset();
  }
};

int read_stats_async(const DoutPrefixProvider* dpp,
                     librados::Rados& rados,
                     const rgw_raw_obj& obj,
                     boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  auto headercb = std::make_unique<AsyncHeaderCB>(std::move(cb));
  r = ::cls_user_get_header_async(ref.ioctx, ref.obj.oid, headercb.get());
  if (r >= 0) {
    headercb.release(); // release ownership, handle_response() will free
  }
  return r;
}

int reset_stats(const DoutPrefixProvider* dpp, optional_yield y,
                librados::Rados& rados, const rgw_raw_obj& obj)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  int rval;

  cls_user_reset_stats2_op call;
  cls_user_reset_stats2_ret ret;

  do {
    buffer::list in, out;
    librados::ObjectWriteOperation op;

    call.time = ceph::real_clock::now();
    ret.update_call(call);

    encode(call, in);
    op.exec("user", "reset_user_stats2", in, &out, &rval);
    r = ref.operate(dpp, &op, y, librados::OPERATION_RETURNVEC);
    if (r < 0) {
      return r;
    }
    try {
      auto bliter = out.cbegin();
      decode(ret, bliter);
    } catch (ceph::buffer::error& err) {
      return -EINVAL;
    }
  } while (ret.truncated);

  return rval;
}

int complete_flush_stats(const DoutPrefixProvider* dpp, optional_yield y,
                         librados::Rados& rados, const rgw_raw_obj& obj)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_complete_stats_sync(op);
  return ref.operate(dpp, &op, y);
}

} // namespace rgwrados::buckets
