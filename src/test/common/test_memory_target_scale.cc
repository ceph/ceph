// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "include/msgr.h"
#include "common/ceph_context.h"
#include "common/memory_target_scale.h"

static boost::intrusive_ptr<CephContext> osd_context(const char *target)
{
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  if (target) {
    cct->_conf.set_val_or_die("osd_memory_target", target);
  }
  return cct;
}

TEST(MemoryTargetScale, unchanged_at_or_above_default)
{
  for (auto target : {(const char*)nullptr, "4294967296", "8589934592"}) {
    auto cct = osd_context(target);
    EXPECT_DOUBLE_EQ(1.0, ceph::osd_memory_target_scale(cct->_conf));
    EXPECT_EQ(1000u, ceph::osd_memory_target_scaled(cct->_conf, 1000));
    EXPECT_EQ(1000u, ceph::osd_memory_target_scaled(cct->_conf, 1000, 0.5));
    EXPECT_EQ(500u << 20, ceph::osd_memory_target_scaled<Option::size_t>(
                cct->_conf, "osd_client_message_size_cap"));
  }
}

TEST(MemoryTargetScale, scales_below_default)
{
  auto cct = osd_context("1073741824");  // 1 GiB
  EXPECT_DOUBLE_EQ(0.25, ceph::osd_memory_target_scale(cct->_conf));
  EXPECT_EQ(250u, ceph::osd_memory_target_scaled(cct->_conf, 1000));
  // the RocksDB WAL is scaled by the square root
  EXPECT_EQ(512ull << 20,
            ceph::osd_memory_target_scaled(cct->_conf, 1ull << 30, 0.5));
}

TEST(MemoryTargetScale, zero_stays_zero_and_small_values_stay_positive)
{
  auto cct = osd_context("1073741824");
  EXPECT_EQ(0u, ceph::osd_memory_target_scaled(cct->_conf, 0));
  EXPECT_EQ(1u, ceph::osd_memory_target_scaled(cct->_conf, 1));
}

TEST(MemoryTargetScale, options_scale_only_at_their_defaults)
{
  auto cct = osd_context("1073741824");
  // at their defaults, size and uint options are scaled
  EXPECT_EQ(125u << 20, ceph::osd_memory_target_scaled<Option::size_t>(
              cct->_conf, "osd_client_message_size_cap"));
  EXPECT_EQ(750u, ceph::osd_memory_target_scaled<uint64_t>(
              cct->_conf, "osd_pg_log_dups_tracked"));
  // a value set explicitly is used as is
  cct->_conf.set_val_or_die("osd_client_message_size_cap", "209715200");
  cct->_conf.set_val_or_die("osd_pg_log_dups_tracked", "1000");
  EXPECT_EQ(200u << 20, ceph::osd_memory_target_scaled<Option::size_t>(
              cct->_conf, "osd_client_message_size_cap"));
  EXPECT_EQ(1000u, ceph::osd_memory_target_scaled<uint64_t>(
              cct->_conf, "osd_pg_log_dups_tracked"));
}
