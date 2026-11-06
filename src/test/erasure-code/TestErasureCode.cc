// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <stdlib.h>

#include "erasure-code/ErasureCode.h"
#include "crush/CrushWrapper.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

using namespace std;

class ErasureCodeTest : public ErasureCode {
public:
  shard_id_map<bufferlist> encode_chunks_encoded;
  unsigned int k;
  unsigned int m;
  unsigned int chunk_size;

  ErasureCodeTest(unsigned int _k, unsigned int _m, unsigned int _chunk_size) :
  encode_chunks_encoded(_k + _m), k(_k), m(_m), chunk_size(_chunk_size) {}
  ~ErasureCodeTest() override {}

  int init(ErasureCodeProfile &profile, ostream *ss) override {
    return 0;
  }

  uint64_t get_supported_optimizations() const override { return 0; }
  unsigned int get_chunk_count() const override { return k + m; }
  unsigned int get_data_chunk_count() const override { return k; }
  unsigned int get_chunk_size(unsigned int object_size) const override {
    return chunk_size;
  }
  size_t get_minimum_granularity() override { return 1; }
  [[deprecated]]
  int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded) override {
    ceph_abort_msg("Only new API is tested");
    //encode_chunks_encoded = *encoded;
    return 0;
  }
  int encode_chunks(const shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override {
    return 0;
  }
  [[deprecated]]
  int decode_chunks(const set<int> &want_to_read,
                    const map<int, bufferlist> &chunks,
                    map<int, bufferlist> *decoded) override {
    ceph_abort_msg("ErasureCode::decode_chunks not implemented");
  }
  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override {
    ceph_abort_msg("ErasureCode::decode_chunks not implemented");
  }

  using ErasureCode::create_rule;
  int create_rule(const string &name,
		  CrushWrapper &crush,
		  ostream *ss) const override { return 0; }
};
static_assert(!std::is_abstract<ErasureCodeTest>());

/*
 *  If we have a buffer of 5 bytes (X below) and a chunk size of 3
 *  bytes, for k=3, m=1 an additional 7 bytes (P and C below) will
 *  need to be allocated for padding (P) and the 3 coding bytes (C).
 *
 *  X -+ +----------+ +-X
 *  X  | | data   0 | | X
 *  X  | +----------+ | X
 *  X  | +----------+ | X -> +-X
 *  X -+ | data   1 | +-X -> | X
 *  P -+ +----------+        | P
 *  P  | +----------+        | P
 *  P  | | data   2 |        | P
 *  P  | +----------+        | P
 *  C  | +----------+        | C
 *  C  | | coding 3 |        | C
 *  C -+ +----------+        +-C
 *
 *  The data chunks 1 and 2 (data 1 and data 2 above) overflow the
 *  original buffer because it needs padding. A new buffer will
 *  be allocated to contain the chunk that overflows and all other
 *  chunks after it, including the coding chunk(s).
 *
 *  The following test creates a siguation where the buffer provided
 *  for encoding is not memory aligned. After encoding it asserts that:
 *
 *   a) each chunk is SIMD aligned
 *   b) the data 1 chunk content is as expected which implies that its
 *      content has been copied over.
 *
 *  It is possible for a flawed implementation to pas the test because the
 *  underlying allocation function enforces it.
 */
TEST(ErasureCodeTest, encode_memory_align)
{
  int k = 3;
  int m = 1;
  unsigned chunk_size = ErasureCode::SIMD_ALIGN * 7;
  ErasureCodeTest erasure_code(k, m, chunk_size);

  shard_id_set  want_to_encode;
  want_to_encode.insert_range(shard_id_t(0), erasure_code.get_chunk_count());
  string data(chunk_size + chunk_size / 2, 'X'); // uses 1.5 chunks out of 3
  // make sure nothing is memory aligned
  bufferptr ptr(buffer::create_aligned(data.length() + 1, ErasureCode::SIMD_ALIGN));
  ptr.copy_in(1, data.length(), data.c_str());
  ptr.set_offset(1);
  ptr.set_length(data.length());
  bufferlist in;
  in.append(ptr);
  shard_id_map<bufferlist> encoded(k+m);

  ASSERT_FALSE(in.is_aligned(ErasureCode::SIMD_ALIGN));
  ASSERT_EQ(0, erasure_code.encode(want_to_encode, in, &encoded));
  for (shard_id_t i; i < erasure_code.get_chunk_count(); ++i)
    ASSERT_TRUE(encoded[i].is_aligned(ErasureCode::SIMD_ALIGN));
  for (unsigned i = 0; i < chunk_size / 2; i++)
    ASSERT_EQ(encoded[shard_id_t(1)][i], 'X');
  ASSERT_NE(encoded[shard_id_t(1)][chunk_size / 2], 'X');
}

TEST(ErasureCodeTest, encode_misaligned_non_contiguous)
{
  int k = 3;
  int m = 1;
  unsigned chunk_size = ErasureCode::SIMD_ALIGN * 7;
  ErasureCodeTest erasure_code(k, m, chunk_size);

  shard_id_set  want_to_encode;
  for (shard_id_t i; i < erasure_code.get_chunk_count(); ++i)
    want_to_encode.insert(i);
  string data(chunk_size, 'X');
  // create a non contiguous bufferlist where the frist and the second
  // bufferptr are not size aligned although they are memory aligned
  bufferlist in;
  {
    bufferptr ptr(buffer::create_aligned(data.length() - 1, ErasureCode::SIMD_ALIGN));
    in.append(ptr);
  }
  {
    bufferptr ptr(buffer::create_aligned(data.length() + 1, ErasureCode::SIMD_ALIGN));
    in.append(ptr);
  }
  shard_id_map<bufferlist> encoded(k + m);

  ASSERT_FALSE(in.is_contiguous());
  ASSERT_TRUE(in.front().is_aligned(ErasureCode::SIMD_ALIGN));
  ASSERT_FALSE(in.front().is_n_align_sized(chunk_size));
  ASSERT_TRUE(in.back().is_aligned(ErasureCode::SIMD_ALIGN));
  ASSERT_FALSE(in.back().is_n_align_sized(chunk_size));
  ASSERT_EQ(0, erasure_code.encode(want_to_encode, in, &encoded));
  for (shard_id_t i; i < erasure_code.get_chunk_count(); ++i) {
    ASSERT_TRUE(encoded[i].is_aligned(ErasureCode::SIMD_ALIGN));
    ASSERT_TRUE(encoded[i].is_n_align_sized(chunk_size));
  }
}

// A profile records a zone type only when it is given: the Monitor resolves
// the default, and a rule over zones needs the zone type it is given.
TEST(ErasureCodeTest, zone_failure_domain_comes_from_the_caller)
{
  ErasureCodeTest erasure_code(2, 1, 4096);
  ErasureCodeProfile profile;
  ostringstream ss;
  ASSERT_EQ(0, erasure_code.ErasureCode::init(profile, &ss));
  EXPECT_FALSE(profile.contains("crush-zone-failure-domain"));
  EXPECT_EQ("host", profile["crush-failure-domain"]);

  CrushWrapper crush;
  crush.create();
  EXPECT_EQ(-EINVAL, erasure_code.create_rule("r", 2, "", "", "", std::nullopt, crush, &ss));
}

// A pool going to one zone must honour the root (and OSD failure domain and
// device class) the caller gives create_rule, not the profile's. Here the
// caller's root "myroot" exists but the profile's default root ("default")
// does not, so a rule can be built only if create_rule uses the given root.
TEST(ErasureCodeTest, one_zone_rule_honours_the_given_root)
{
  ErasureCodeTest erasure_code(2, 1, 4096);
  ErasureCodeProfile profile;
  ostringstream ss;
  ASSERT_EQ(0, erasure_code.ErasureCode::init(profile, &ss));

  CrushWrapper crush;
  crush.create();
  crush.set_type_name(0, "osd");
  crush.set_type_name(1, "host");
  crush.set_type_name(2, "root");
  int rootno;
  crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1, 2, 0,
                   NULL, NULL, &rootno);
  crush.set_item_name(rootno, "myroot");

  const int rule =
    erasure_code.create_rule("r", 1, "myroot", "", "host", std::nullopt, crush, &ss);
  ASSERT_LE(0, rule) << ss.str();
  // the TAKE step (indep mode: steps 0 and 1 set the tries, step 2 is TAKE)
  // targets the given root, not the profile's default root
  EXPECT_EQ(CRUSH_RULE_TAKE, crush.get_rule_op(rule, 2));
  EXPECT_EQ(crush.get_item_id("myroot"), crush.get_rule_arg1(rule, 2));
}

// create_rule's device_class is tri-state. std::nullopt inherits the
// profile's crush-device-class; an explicit value is used verbatim, and an
// empty string forces a rule with no device class. Before that distinction a
// pool could not drop a class its profile set without editing the profile.
// The profile here names a class the crushmap lacks, so the inherit path
// fails to resolve it while the explicit-no-class path builds the rule.
TEST(ErasureCodeTest, one_zone_rule_device_class_is_tristate)
{
  ErasureCodeTest erasure_code(2, 1, 4096);
  ErasureCodeProfile profile;
  profile["crush-device-class"] = "ssd";
  ostringstream ss;
  ASSERT_EQ(0, erasure_code.ErasureCode::init(profile, &ss));

  auto fresh_crush = [] {
    auto c = std::make_unique<CrushWrapper>();
    c->create();
    c->set_type_name(0, "osd");
    c->set_type_name(1, "host");
    c->set_type_name(2, "root");
    int rootno;
    c->add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1, 2, 0,
                  NULL, NULL, &rootno);
    c->set_item_name(rootno, "myroot");
    return c;
  };

  // std::nullopt inherits the profile's 'ssd' class, which the crushmap has
  // no devices of, so the rule cannot be built.
  {
    auto crush = fresh_crush();
    EXPECT_GT(0, erasure_code.create_rule("inherit", 1, "myroot", "", "host",
                                          std::nullopt, *crush, &ss));
  }
  // an explicit empty class forces no device class, so the same rule builds.
  {
    auto crush = fresh_crush();
    EXPECT_LE(0, erasure_code.create_rule("noclass", 1, "myroot", "", "host",
                                          std::string(), *crush, &ss))
      << ss.str();
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make -j4 unittest_erasure_code &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_erasure_code \
 *      --gtest_filter=*.* --log-to-stderr=true"
 * End:
 */
