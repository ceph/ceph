// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "rgw_tag.h"

using namespace std;

// Helper: binary-encode an RGWObjTags into a bufferlist (the normal path)
static bufferlist encode_tags(const RGWObjTags& tags) {
  bufferlist bl;
  tags.encode(bl);
  return bl;
}

// Helper: store a raw plain-text string into a bufferlist the way
// rgw_get_request_metadata() does: bl.append(str.c_str(), str.size() + 1)
// (includes trailing null byte)
static bufferlist plain_text_bl(const string& s) {
  bufferlist bl;
  bl.append(s.c_str(), s.size() + 1);
  return bl;
}

// ---------- Binary-encoded tags (the normal path) ----------

TEST(RGWObjTagsDecode, BinaryEncodedSingleTag)
{
  RGWObjTags src;
  src.add_tag("ttl_tag", "23");

  bufferlist bl = encode_tags(src);

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 1u);
  auto& m = dst.get_tags();
  auto it = m.find("ttl_tag");
  ASSERT_NE(it, m.end());
  EXPECT_EQ(it->second, "23");
}

TEST(RGWObjTagsDecode, BinaryEncodedMultipleTags)
{
  RGWObjTags src;
  src.add_tag("env", "production");
  src.add_tag("team", "storage");
  src.add_tag("ttl", "86400");

  bufferlist bl = encode_tags(src);

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 3u);
  auto& m = dst.get_tags();
  EXPECT_EQ(m.find("env")->second, "production");
  EXPECT_EQ(m.find("team")->second, "storage");
  EXPECT_EQ(m.find("ttl")->second, "86400");
}

// ---------- Plain-text tags (before the fix for tracker #53016 used) -----

TEST(RGWObjTagsDecode, PlainTextSingleTag)
{
  // This is what rgw_get_request_metadata() stored for multipart uploads:
  // the raw HTTP header value "key=value" plus a trailing null byte.
  bufferlist bl = plain_text_bl("ttl_tag=23");

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 1u);
  auto& m = dst.get_tags();
  auto it = m.find("ttl_tag");
  ASSERT_NE(it, m.end());
  EXPECT_EQ(it->second, "23");
}

TEST(RGWObjTagsDecode, PlainTextMultipleTags)
{
  // Multiple tags are &-separated, URL-encoded
  bufferlist bl = plain_text_bl("env=production&team=storage&ttl=86400");

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 3u);
  auto& m = dst.get_tags();
  EXPECT_EQ(m.find("env")->second, "production");
  EXPECT_EQ(m.find("team")->second, "storage");
  EXPECT_EQ(m.find("ttl")->second, "86400");
}

TEST(RGWObjTagsDecode, PlainTextUrlEncodedTag)
{
  // Keys/values with special characters get URL-encoded
  bufferlist bl = plain_text_bl("my%20key=my%20value");

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 1u);
  auto& m = dst.get_tags();
  auto it = m.find("my key");
  ASSERT_NE(it, m.end());
  EXPECT_EQ(it->second, "my value");
}

TEST(RGWObjTagsDecode, PlainTextNoValue)
{
  // Tag with key only, no '='
  bufferlist bl = plain_text_bl("orphan_key");

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 1u);
  auto& m = dst.get_tags();
  auto it = m.find("orphan_key");
  ASSERT_NE(it, m.end());
  EXPECT_EQ(it->second, "");
}

// ---------- Edge cases ----------

TEST(RGWObjTagsDecode, PlainTextNoTrailingNull)
{
  // Plain text without trailing null (just raw bytes)
  string s = "key1=val1&key2=val2";
  bufferlist bl;
  bl.append(s.data(), s.size()); // no trailing '\0'

  RGWObjTags dst;
  auto iter = bl.cbegin();
  dst.decode(iter);

  ASSERT_EQ(dst.count(), 2u);
  auto& m = dst.get_tags();
  EXPECT_EQ(m.find("key1")->second, "val1");
  EXPECT_EQ(m.find("key2")->second, "val2");
}

TEST(RGWObjTagsDecode, BinaryRoundTrip)
{
  // Encode -> decode -> re-encode should produce identical bufferlist
  RGWObjTags src;
  src.add_tag("alpha", "1");
  src.add_tag("beta", "2");

  bufferlist bl1 = encode_tags(src);

  RGWObjTags dst;
  auto iter = bl1.cbegin();
  dst.decode(iter);

  bufferlist bl2 = encode_tags(dst);

  EXPECT_TRUE(bl1.contents_equal(bl2));
}

// ---------- Exception handling tests ----------

TEST(RGWObjTagsDecode, TagKeyTooLongThrowsOriginalException)
{
  // Create plain-text tag with key longer than max_tag_key_size (128)
  // set_from_string() will return -ERR_INVALID_TAG
  string long_key(200, 'k');  // 200 chars, exceeds 128 limit
  string tag_string = long_key + "=value";
  bufferlist bl = plain_text_bl(tag_string);

  RGWObjTags dst;
  auto iter = bl.cbegin();
  EXPECT_THROW(dst.decode(iter), buffer::error);
}

TEST(RGWObjTagsDecode, EmptyTagKeyThrowsOriginalException)
{
  // Plain-text with empty key: "=value" - empty key is invalid
  // set_from_string() returns -ERR_INVALID_TAG for empty key
  bufferlist bl = plain_text_bl("=value");

  RGWObjTags dst;
  auto iter = bl.cbegin();
  EXPECT_THROW(dst.decode(iter), buffer::error);
}

TEST(RGWObjTagsDecode, EmptyAfterNullStrippingThrowsOriginalException)
{
  // Buffer containing only null bytes: fails binary decode, then after
  // stripping trailing nulls we have empty raw string -> throws original exception
  bufferlist bl;
  bl.append("\0\0\0", 3);  // just null bytes

  RGWObjTags dst;
  auto iter = bl.cbegin();
  EXPECT_THROW(dst.decode(iter), buffer::error);
}

TEST(RGWObjTagsDecode, EmptyBufferlist)
{
  // Empty bufferlist should produce no tags and also raise exception
  bufferlist bl;

  RGWObjTags dst;
  auto iter = bl.cbegin();

  EXPECT_THROW(dst.decode(iter), buffer::error);
  EXPECT_EQ(dst.count(), 0u);
}
