#include <cstdint>

#include <gtest/gtest.h>

#include "rgw/rgw_common.h"

TEST(RGWFlagParsing, CapPermissions)
{
  std::uint32_t permissions = RGW_CAP_ALL;

  EXPECT_EQ(0, RGWUserCaps::parse_cap_perm("unknown", &permissions));
  EXPECT_EQ(0, permissions);

  EXPECT_EQ(0, RGWUserCaps::parse_cap_perm("read; write,unknown=read\twrite", &permissions));
  EXPECT_EQ(RGW_CAP_ALL, permissions);

  EXPECT_EQ(0, RGWUserCaps::parse_cap_perm("*", &permissions));
  EXPECT_EQ(RGW_CAP_ALL, permissions);
}

TEST(RGWFlagParsing, OperationTypes)
{
  std::uint32_t permissions = RGW_OP_TYPE_ALL;

  EXPECT_EQ(0, rgw_parse_op_type_list("", &permissions));
  EXPECT_EQ(0, permissions);

  EXPECT_EQ(0, rgw_parse_op_type_list("read; write,unknown=delete\tread", &permissions));
  EXPECT_EQ(RGW_OP_TYPE_ALL, permissions);

  EXPECT_EQ(0, rgw_parse_op_type_list("*", &permissions));
  EXPECT_EQ(RGW_OP_TYPE_ALL, permissions);
}
