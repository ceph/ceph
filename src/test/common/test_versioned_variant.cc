// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include "common/versioned_variant.h"
#include <bitset>
#include <string>
#include <gtest/gtest.h>

namespace {

// type with custom encoding
struct custom_type {
  void encode(bufferlist& bl) const {
    ENCODE_START(0, 0, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(0, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(custom_type);

} // anonymous namespace

namespace ceph {

TEST(VersionedVariant, Monostate)
{
  using Variant = std::variant<std::monostate>;
  bufferlist bl;
  {
    Variant in;
    versioned_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    EXPECT_TRUE(std::holds_alternative<std::monostate>(out));
  }
}

TEST(VersionedVariant, Custom)
{
  using Variant = std::variant<std::monostate, custom_type>;
  bufferlist bl;
  {
    Variant in = custom_type{};
    versioned_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    EXPECT_TRUE(std::holds_alternative<custom_type>(out));
  }
}

TEST(VersionedVariant, DuplicateFirst)
{
  using Variant = std::variant<int, int>;
  bufferlist bl;
  {
    Variant in;
    in.emplace<0>(42);
    versioned_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_EQ(0, out.index());
    EXPECT_EQ(42, std::get<0>(out));
  }
}

TEST(VersionedVariant, DuplicateSecond)
{
  using Variant = std::variant<int, int>;
  bufferlist bl;
  {
    Variant in;
    in.emplace<1>(42);
    versioned_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_EQ(1, out.index());
    EXPECT_EQ(42, std::get<1>(out));
  }
}

TEST(VersionedVariant, EncodeOld)
{
  using V1 = std::variant<int>;
  using V2 = std::variant<int, std::string>;

  bufferlist bl;
  {
    // use V1 to encode the initial type
    V1 in = 42;
    versioned_variant::encode(in, bl);
  }
  {
    // can decode as V1
    V1 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<int>(out));
    EXPECT_EQ(42, std::get<int>(out));
  }
  {
    // can also decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<int>(out));
    EXPECT_EQ(42, std::get<int>(out));
  }
}

TEST(VersionedVariant, EncodeExisting)
{
  using V1 = std::variant<int>;
  using V2 = std::variant<int, std::string>;

  bufferlist bl;
  {
    // use V2 to encode the type shared with V1
    V2 in = 42;
    versioned_variant::encode(in, bl);
  }
  {
    // can decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<int>(out));
    EXPECT_EQ(42, std::get<int>(out));
  }
  {
    // can also decode as V1
    V1 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<int>(out));
    EXPECT_EQ(42, std::get<int>(out));
  }
}

TEST(VersionedVariant, EncodeNew)
{
  using V1 = std::variant<int>;
  using V2 = std::variant<int, std::string>;

  bufferlist bl;
  {
    // use V2 to encode the new string type
    V2 in = "42";
    versioned_variant::encode(in, bl);
  }
  {
    // can decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(versioned_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<std::string>(out));
    EXPECT_EQ("42", std::get<std::string>(out));
  }
  {
    // can't decode as V1
    V1 out;
    auto p = bl.cbegin();
    EXPECT_THROW(versioned_variant::decode(out, p), buffer::malformed_input);
  }
}


TEST(ConvertedVariant, Custom)
{
  using Variant = std::variant<custom_type>;
  bufferlist bl;
  {
    Variant in = custom_type{};
    converted_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    EXPECT_TRUE(std::holds_alternative<custom_type>(out));
  }
}

TEST(ConvertedVariant, DuplicateFirst)
{
  using Variant = std::variant<custom_type, int, int>;
  bufferlist bl;
  {
    Variant in;
    in.emplace<1>(42);
    converted_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    ASSERT_EQ(1, out.index());
    EXPECT_EQ(42, std::get<1>(out));
  }
}

TEST(ConvertedVariant, DuplicateSecond)
{
  using Variant = std::variant<custom_type, int, int>;
  bufferlist bl;
  {
    Variant in;
    in.emplace<2>(42);
    converted_variant::encode(in, bl);
  }
  {
    Variant out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    ASSERT_EQ(2, out.index());
    EXPECT_EQ(42, std::get<2>(out));
  }
}

TEST(ConvertedVariant, EncodeOld)
{
  using V1 = custom_type;
  using V2 = std::variant<custom_type, int>;

  bufferlist bl;
  {
    // use V1 to encode the initial type
    V1 in;
    encode(in, bl);
  }
  {
    // can decode as V1
    V1 out;
    auto p = bl.cbegin();
    EXPECT_NO_THROW(decode(out, p));
  }
  {
    // can also decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    EXPECT_TRUE(std::holds_alternative<custom_type>(out));
  }
}

TEST(ConvertedVariant, EncodeExisting)
{
  using V1 = custom_type;
  using V2 = std::variant<custom_type, int>;

  bufferlist bl;
  {
    // use V2 to encode the type shared with V1
    V2 in;
    converted_variant::encode(in, bl);
  }
  {
    // can decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    EXPECT_TRUE(std::holds_alternative<custom_type>(out));
  }
  {
    // can also decode as V1
    V1 out;
    auto p = bl.cbegin();
    EXPECT_NO_THROW(decode(out, p));
  }
}

TEST(ConvertedVariant, EncodeNew)
{
  using V1 = custom_type;
  using V2 = std::variant<custom_type, int>;

  bufferlist bl;
  {
    // use V2 to encode the new type
    V2 in = 42;
    converted_variant::encode(in, bl);
  }
  {
    // can decode as V2
    V2 out;
    auto p = bl.cbegin();
    ASSERT_NO_THROW(converted_variant::decode(out, p));
    ASSERT_TRUE(std::holds_alternative<int>(out));
    EXPECT_EQ(42, std::get<int>(out));
  }
  {
    // can't decode as V1
    V1 out;
    auto p = bl.cbegin();
    EXPECT_THROW(decode(out, p), buffer::malformed_input);
  }
}

TEST(Variant, GenerateTestInstances)
{
  using Variant = std::variant<int, bool, double>;

  std::bitset<std::variant_size_v<Variant>> bits;
  ASSERT_TRUE(bits.none());

  std::list<Variant> instances;
  generate_test_instances(instances);

  for (const auto& v : instances) {
    bits.set(v.index());
  }

  EXPECT_TRUE(bits.all());
}

} // namespace ceph
