// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"

#include "gtest/gtest.h"

// Pin the rdma delivery descriptor wire format (carried as a trailing
// per-op vector on the MOSDOp for header.version >= 10, aligned with
// the ops). Any change here is a wire format change and needs a struct
// version bump.
TEST(RdmaDelivery, WireFormat)
{
  ceph::rdma::delivery_t d;
  d.token = "deadbeef:1000:x";
  d.base_offset = 0xa1b2c3d4e5f60718ull;
  d.flags = 0;

  bufferlist bl;
  encode(d, bl);

  // exact bytes: ENCODE_START(1,1) header [u8 v, u8 compat, le32 len],
  // le32 token length + token bytes, le64 base_offset, le32 flags
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x1f, 0x00, 0x00, 0x00,              // struct v1, compat 1, len 31
    0x0f, 0x00, 0x00, 0x00,                          // token length (le32)
    'd', 'e', 'a', 'd', 'b', 'e', 'e', 'f', ':',
    '1', '0', '0', '0', ':', 'x',                    // token
    0x18, 0x07, 0xf6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1,  // base_offset (le64)
    0x00, 0x00, 0x00, 0x00,                          // flags (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "rdma delivery descriptor layout changed; this is a wire format break";

  // and it round-trips
  ceph::rdma::delivery_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(d.token, out.token);
  EXPECT_EQ(d.base_offset, out.base_offset);
  EXPECT_EQ(d.flags, out.flags);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, OobResultWireFormat)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 0x0102030405060708ull;
  r.crc64 = 0xae8b14860a799888ull;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;

  bufferlist bl;
  encode(r, bl);
  // ENCODE_START(1,1) header, le64 bytes, le64 crc64, le32 flags
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x14, 0x00, 0x00, 0x00,              // v1, compat 1, len 20
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // bytes (le64)
    0x88, 0x98, 0x79, 0x0a, 0x86, 0x14, 0x8b, 0xae,  // crc64 (le64)
    0x01, 0x00, 0x00, 0x00,                          // flags (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "oob result layout changed; this is a wire format break";

  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(r.bytes, out.bytes);
  EXPECT_EQ(r.crc64, out.crc64);
  EXPECT_EQ(r.flags, out.flags);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, PerOpVectorRoundTrip)
{
  // the descriptors ride as a per-op vector on the MOSDOp tail: empty
  // when nothing is requested, otherwise one entry per op where an
  // empty token means "inline for this op"
  std::vector<ceph::rdma::delivery_t> none;
  std::vector<ceph::rdma::delivery_t> some = {
    ceph::rdma::delivery_t{},                          // op 0: inline
    ceph::rdma::delivery_t{"aa:bb:opaque", 42, 0},     // op 1
    ceph::rdma::delivery_t{"aa:bb:opaque", 4096,
			   ceph::rdma::delivery_t::FLAG_CRC64NVME},  // op 2
  };
  EXPECT_TRUE(some[0].empty());
  EXPECT_FALSE(some[1].empty());

  bufferlist bl;
  encode(none, bl);
  encode(some, bl);

  std::vector<ceph::rdma::delivery_t> out1, out2;
  auto p = bl.cbegin();
  decode(out1, p);
  decode(out2, p);
  EXPECT_TRUE(out1.empty());
  ASSERT_EQ(3u, out2.size());
  EXPECT_TRUE(out2[0].empty());
  EXPECT_EQ("aa:bb:opaque", out2[1].token);
  EXPECT_EQ(42u, out2[1].base_offset);
  EXPECT_EQ(0u, out2[1].flags);
  EXPECT_EQ(4096u, out2[2].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME, out2[2].flags);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, Crc64ValidityIsSeparateFromCombinability)
{
  // distinct bits: a scattered placement reports a checksum of the
  // bytes it moved without claiming it folds with its neighbours
  static_assert(ceph::rdma::oob_result_t::FLAG_CRC64NVME !=
		ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);

  ceph::rdma::oob_result_t scattered;
  scattered.bytes = 1048576;
  scattered.crc64 = 0x0123456789abcdefull;
  scattered.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;
  EXPECT_TRUE(scattered.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_FALSE(scattered.flags &
	       ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);

  ceph::rdma::oob_result_t linear = scattered;
  linear.flags |= ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE;

  // the extra bit rides in the existing flags field, so neither
  // combination changes the encoded length
  bufferlist a, b;
  encode(scattered, a);
  encode(linear, b);
  EXPECT_EQ(a.length(), b.length());

  ceph::rdma::oob_result_t out;
  auto p = b.cbegin();
  decode(out, p);
  EXPECT_EQ(linear.crc64, out.crc64);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);
  EXPECT_TRUE(p.end());
}
