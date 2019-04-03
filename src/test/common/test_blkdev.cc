// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <linux/kdev_t.h>

#include "include/types.h"
#include "common/blkdev.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <iostream>

using namespace std;
using namespace testing;

class MockBlkDev : public BlkDev {
 public:
  // pass 0 as fd, so it won't try to use the empty devname
  MockBlkDev() : BlkDev(0) {};
  virtual ~MockBlkDev() {}

  MOCK_CONST_METHOD0(sysfsdir, const char*());
  MOCK_CONST_METHOD2(wholedisk, int(char* device, size_t max));
};


class BlockDevTest : public ::testing::Test {
public:
  string *root;

protected:
  virtual void SetUp() {
    const char *sda_name = "sda";
    const char *sdb_name = "sdb";
    const char* env = getenv("CEPH_ROOT");
    ASSERT_NE(env, nullptr) << "Environment Variable CEPH_ROOT not found!";
    root = new string(env);
    *root += "/src/test/common/test_blkdev_sys_block/sys";

    EXPECT_CALL(sda, sysfsdir())
      .WillRepeatedly(Return(root->c_str()));
    EXPECT_CALL(sda, wholedisk(NotNull(), Ge(0ul)))
      .WillRepeatedly(
        DoAll(
          SetArrayArgument<0>(sda_name, sda_name + strlen(sda_name) + 1),
          Return(0)));

    EXPECT_CALL(sdb, sysfsdir())
      .WillRepeatedly(Return(root->c_str()));
    EXPECT_CALL(sdb, wholedisk(NotNull(), Ge(0ul)))
      .WillRepeatedly(
        DoAll(
          SetArrayArgument<0>(sdb_name, sdb_name + strlen(sdb_name) + 1),
          Return(0)));
  }

  virtual void TearDown() {
    delete root;
  }

  MockBlkDev sda, sdb;
};

TEST_F(BlockDevTest, device_model)
{
  char model[1000] = {0};
  int rc = sda.model(model, sizeof(model));
  ASSERT_EQ(0, rc);
  ASSERT_STREQ(model, "myfancymodel");
}

TEST_F(BlockDevTest, discard)
{
  EXPECT_TRUE(sda.support_discard());
  EXPECT_TRUE(sdb.support_discard());
}

TEST_F(BlockDevTest, is_nvme)
{
  // It would be nice to have a positive NVME test too, but I don't have any
  // examples for the canned data.
  EXPECT_FALSE(sda.is_nvme());
  EXPECT_FALSE(sdb.is_nvme());
}

TEST_F(BlockDevTest, is_rotational)
{
  EXPECT_FALSE(sda.is_rotational());
  EXPECT_TRUE(sdb.is_rotational());
}

TEST(blkdev, _decode_model_enc)
{

  const char *foo[][2] = {
    { "WDC\\x20WDS200T2B0A-00SM50\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20",
      "WDC_WDS200T2B0A-00SM50" },
    { 0, 0},
  };

  for (unsigned i = 0; foo[i][0]; ++i) {
    std::string d = _decode_model_enc(foo[i][0]);
    cout << " '" << foo[i][0] << "' -> '" << d << "'" << std::endl;
    ASSERT_EQ(std::string(foo[i][1]), d);
  }
}
