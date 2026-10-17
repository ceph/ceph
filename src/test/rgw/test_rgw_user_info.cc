#include <list>
#include <string>
#include <vector>
#include <iterator>

#include <gtest/gtest.h>

#include "rgw_common.h"

TEST(RGWUserInfo, PlacementTagEncodingRemainsCompatible)
{
  const std::list<std::string> legacy_tags {"fast", "archive", "tenant-a"};
  const std::vector<std::string> current_tags(std::begin(legacy_tags), std::end(legacy_tags));
  bufferlist legacy_encoding;
  bufferlist current_encoding;

  encode(legacy_tags, legacy_encoding);
  encode(current_tags, current_encoding);

  ASSERT_TRUE(legacy_encoding.contents_equal(current_encoding));

  auto encoded = std::cbegin(legacy_encoding);
  std::vector<std::string> decoded;
  decode(decoded, encoded);

  EXPECT_EQ(current_tags, decoded);
  EXPECT_EQ(0, encoded.get_remaining());
}

TEST(RGWUserInfo, PlacementTagsRoundTripWithFollowingFields)
{
  RGWUserInfo expected;
  expected.user_id = rgw_user {"tenant", "user"};
  expected.placement_tags = {"fast", "archive", "tenant-a"};
  expected.account_id = "RGW01234567890123456";
  expected.path = "/engineering/";
  expected.group_ids = {"developers", "operators"};

  bufferlist encoded;
  encode(expected, encoded);

  RGWUserInfo decoded;
  auto input = std::cbegin(encoded);
  decode(decoded, input);

  EXPECT_EQ(expected.placement_tags, decoded.placement_tags);
  EXPECT_EQ(expected.account_id, decoded.account_id);
  EXPECT_EQ(expected.path, decoded.path);
  EXPECT_EQ(expected.group_ids, decoded.group_ids);
  EXPECT_EQ(0, input.get_remaining());
}
