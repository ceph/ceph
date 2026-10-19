// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include "gtest/gtest.h"
#include "common/dout.h"
#include "global/global_context.h"
#include "rgw/driver/rados/rgw_bucket.h"
#include "rgw/driver/rados/rgw_sal_rados.h"
#include "rgw/rgw_sal_filter.h"
#include "rgw/services/svc_bi_rados.h"
#include "rgw/services/svc_bucket_sobj.h"
#include "rgw/services/svc_zone.h"

namespace {

class BucketInstanceMetadata : public ::testing::TestWithParam<uint32_t> {
protected:
  class BucketService : public RGWSI_Bucket_SObj {
  public:
    RGWBucketInfo info;
    bool exists = false;
    int reads = 0;
    int writes = 0;
    int removes = 0;

    BucketService() : RGWSI_Bucket_SObj(g_ceph_context) {}

    int read_bucket_instance_info(const std::string& key, RGWBucketInfo* result,
        real_time* mtime, std::map<std::string, bufferlist>* attrs,
        optional_yield y, const DoutPrefixProvider* dpp,
        rgw_cache_entry_info* cache_info,
        boost::optional<obj_version> refresh_version) override {
      ++reads;
      if (!exists) {
        return -ENOENT;
      }
      *result = info;
      return 0;
    }

    int store_bucket_instance_info(const std::string& key, RGWBucketInfo& value,
        std::optional<RGWBucketInfo*> orig_info, bool exclusive,
        real_time mtime, const std::map<std::string, bufferlist>* attrs,
        optional_yield y, const DoutPrefixProvider* dpp) override {
      ++writes;
      info = value;
      exists = true;
      return 0;
    }

    int remove_bucket_instance_info(const std::string& key,
        const RGWBucketInfo& bucket_info, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider* dpp) override {
      ++removes;
      exists = false;
      return 0;
    }
  } service;

  class IndexService : public RGWSI_BucketIndex_RADOS {
  public:
    int calls = 0;

    IndexService() : RGWSI_BucketIndex_RADOS(g_ceph_context) {}

    int init_index(const DoutPrefixProvider* dpp, optional_yield y,
        const RGWBucketInfo& info,
        const rgw::bucket_index_layout_generation& layout,
        bool judge_support_logrecord) override {
      ++calls;
      return 0;
    }
  } index;

  class Driver : public rgw::sal::FilterDriver {
    class Zone : public rgw::sal::RadosZone {
    public:
      Zone() : RadosZone(nullptr, nullptr) {}
      const std::string_view get_tier_type() override { return {}; }
    } zone;

  public:
    Driver() : FilterDriver(nullptr) {}

    rgw::sal::Zone* get_zone() override { return &zone; }

    // These tests have no lifecycle policy or topics, so no SAL bucket is used.
    std::unique_ptr<rgw::sal::Bucket> get_bucket(const RGWBucketInfo&) override {
      return nullptr;
    }
    RGWLC* get_rgwlc() override { return nullptr; }
  } driver;

  RGWSI_Zone zone{g_ceph_context, nullptr, nullptr};
  NoDoutPrefix dpp{g_ceph_context, ceph_subsys_rgw};
  std::string entry = "tenant/bucket:instance";
  std::unique_ptr<RGWMetadataHandler> handler =
      create_bucket_instance_metadata_handler(&driver, &zone, &service,
                                              &index, nullptr);

  RGWBucketInfo bucket_info() const {
    RGWBucketInfo info;
    info.flags = GetParam();
    info.bucket.name = "bucket";
    info.bucket.bucket_id = "instance";
    info.bucket.tenant = "tenant";
    info.layout.current_index.layout.type = rgw::BucketIndexType::Normal;
    info.layout.current_index.layout.normal.num_shards = 1;
    return info;
  }

  int put(RGWBucketInfo info, bool from_remote_zone) {
    RGWBucketCompleteInfo bci;
    bci.info = std::move(info);
    RGWObjVersionTracker objv;
    RGWBucketInstanceMetadataObject obj(bci, {}, {});
    return handler->put(entry, &obj, objv, null_yield, &dpp,
                        RGWMDLogSyncType::APPLY_ALWAYS, from_remote_zone);
  }
  int remove() {
    RGWObjVersionTracker objv;
    return handler->remove(entry, objv, null_yield, &dpp);
  }
};

TEST_P(BucketInstanceMetadata, Remove) {
  service.exists = true;
  service.info = bucket_info();
  EXPECT_EQ(0, remove());
  if (service.info.is_vector()) {
    // vector instances are never trimmed by sync, so the metadata removes them
    EXPECT_EQ(1, service.removes);
    EXPECT_FALSE(service.exists);
  } else {
    // S3 instances are left to the trimming of each zone
    EXPECT_EQ(0, service.removes);
    EXPECT_TRUE(service.exists);
  }
  // a missing instance is not an error
  service.exists = false;
  EXPECT_EQ(0, remove());
}

TEST_P(BucketInstanceMetadata, Get) {
  service.exists = true;
  service.info = bucket_info();
  RGWMetadataObject* result = nullptr;
  ASSERT_EQ(0, handler->get(entry, &result, null_yield, &dpp));
  std::unique_ptr<RGWMetadataObject> object(result);
  const auto& info = static_cast<RGWBucketInstanceMetadataObject*>(result)->get_bci().info;
  EXPECT_EQ(GetParam(), info.flags);
  EXPECT_EQ("bucket", info.bucket.name);
  EXPECT_EQ("instance", info.bucket.bucket_id);
}

TEST_P(BucketInstanceMetadata, Put) {
  ASSERT_EQ(STATUS_APPLIED, put(bucket_info(), false));
  EXPECT_EQ("bucket", service.info.bucket.name);
  EXPECT_EQ("instance", service.info.bucket.bucket_id);
  EXPECT_EQ("tenant", service.info.bucket.tenant);
  EXPECT_EQ(GetParam(), service.info.flags);
  EXPECT_EQ(1, service.writes);
  EXPECT_EQ(1, index.calls);
}

TEST_P(BucketInstanceMetadata, RejectBucketTypeChanges) {
  service.exists = true;
  service.info = bucket_info();
  auto incoming = service.info;
  incoming.flags ^= BUCKET_VECTOR;
  EXPECT_EQ(-EINVAL, put(incoming, false));
  EXPECT_EQ(0, service.writes);
  EXPECT_EQ(0, index.calls);
}

TEST_P(BucketInstanceMetadata, MetadataSync) {
  service.exists = true;
  service.info = bucket_info();
  auto incoming = service.info;
  incoming.flags |= BUCKET_DATASYNC_DISABLED;
  incoming.layout.current_index.layout.normal.num_shards = 2;

  if (incoming.is_vector()) {
    EXPECT_EQ(STATUS_NO_APPLY, put(incoming, true));
    EXPECT_EQ(0, service.reads);
    EXPECT_EQ(0, service.writes);
    EXPECT_EQ(0, index.calls);
  } else {
    ASSERT_EQ(STATUS_APPLIED, put(incoming, true));
    EXPECT_EQ(incoming.flags, service.info.flags);
    EXPECT_EQ(1, service.writes);
    EXPECT_EQ(1, index.calls);
  }
  // Remote metadata must never replace this zone's index layout.
  EXPECT_EQ(1u, service.info.layout.current_index.layout.normal.num_shards);
}

INSTANTIATE_TEST_SUITE_P(
    BucketTypes, BucketInstanceMetadata,
    ::testing::Values(0u, uint32_t(BUCKET_VECTOR)),
    [](const ::testing::TestParamInfo<uint32_t>& info) {
      return info.param == BUCKET_VECTOR ? "Vector" : "S3";
    });

} // namespace
