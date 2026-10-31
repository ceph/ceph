// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "include/msgr.h"
#include "common/ceph_context.h"
#include "osd/OSD.h"

static boost::intrusive_ptr<CephContext> osd_context(const char *target)
{
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  cct->_conf.set_val_or_die("osd_memory_target", target);
  return cct;
}

TEST(OSDMemoryTargetDefaults, scaled_below_4g_and_restored_above)
{
  auto cct = osd_context("1073741824");  // 1 GiB
  const std::string rocksdb_defaults = cct->_conf->bluestore_rocksdb_options;
  OSD::scale_memory_target_defaults(cct.get(), true);
  EXPECT_EQ(125u << 20, cct->_conf->osd_client_message_size_cap);
  EXPECT_EQ(75000u, cct->_conf->osd_target_pg_log_entries_per_osd);
  EXPECT_EQ(750u, cct->_conf->osd_pg_log_dups_tracked);
  // the 1 GiB WAL is scaled by the square root, in place
  std::string expected = rocksdb_defaults;
  auto pos = expected.find("max_total_wal_size=1073741824");
  ASSERT_NE(std::string::npos, pos);
  expected.replace(pos, strlen("max_total_wal_size=1073741824"),
                   "max_total_wal_size=536870912");
  EXPECT_EQ(expected, cct->_conf->bluestore_rocksdb_options);

  // raising the target at runtime restores the runtime options; the
  // rocksdb options are read at mount and left alone
  cct->_conf.set_val_or_die("osd_memory_target", "4294967296");
  OSD::scale_memory_target_defaults(cct.get(), false);
  EXPECT_EQ(500u << 20, cct->_conf->osd_client_message_size_cap);
  EXPECT_EQ(300000u, cct->_conf->osd_target_pg_log_entries_per_osd);
  EXPECT_EQ(3000u, cct->_conf->osd_pg_log_dups_tracked);
  EXPECT_EQ(expected, cct->_conf->bluestore_rocksdb_options);
}

TEST(OSDMemoryTargetDefaults, unchanged_at_the_default_target)
{
  auto cct = osd_context("4294967296");
  const std::string rocksdb_defaults = cct->_conf->bluestore_rocksdb_options;
  OSD::scale_memory_target_defaults(cct.get(), true);
  EXPECT_EQ(500u << 20, cct->_conf->osd_client_message_size_cap);
  EXPECT_EQ(3000u, cct->_conf->osd_pg_log_dups_tracked);
  EXPECT_EQ(rocksdb_defaults, cct->_conf->bluestore_rocksdb_options);
}

TEST(OSDMemoryTargetDefaults, explicit_values_are_kept)
{
  auto cct = osd_context("1073741824");
  cct->_conf.set_val_or_die("osd_client_message_size_cap", "209715200");
  cct->_conf.set_val_or_die("osd_pg_log_dups_tracked", "3000");  // the default
  cct->_conf.set_val_or_die("bluestore_rocksdb_options",
                            "compression=kNoCompression");
  OSD::scale_memory_target_defaults(cct.get(), true);
  EXPECT_EQ(200u << 20, cct->_conf->osd_client_message_size_cap);
  EXPECT_EQ(75000u, cct->_conf->osd_target_pg_log_entries_per_osd);
  EXPECT_EQ(3000u, cct->_conf->osd_pg_log_dups_tracked);
  EXPECT_EQ("compression=kNoCompression", cct->_conf->bluestore_rocksdb_options);
}
