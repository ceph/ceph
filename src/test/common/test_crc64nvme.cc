// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include <random>

#include "common/Checksummer.h"
#include "include/buffer.h"
#include "gtest/gtest.h"

extern "C" {
#include "common/madler/crc64nvme.h"
#include "common/spdk/crc64.h"
}

// canonical CRC-64/NVME check value from the NVMe specification
static constexpr uint64_t CHECK_123456789 = 0xae8b14860a799888ull;

TEST(Crc64Nvme, Canonical)
{
  const char* s = "123456789";
  EXPECT_EQ(CHECK_123456789, ceph::crc64nvme(0, s, 9));
  // empty input
  EXPECT_EQ(0u, ceph::crc64nvme(0, s, 0));
  // incremental == one-shot
  uint64_t inc = ceph::crc64nvme(0, s, 4);
  inc = ceph::crc64nvme(inc, s + 4, 5);
  EXPECT_EQ(CHECK_123456789, inc);
}

TEST(Crc64Nvme, MatchesSpdk)
{
  // the OSD computes with the madler implementation; rgw's digests use
  // the vendored spdk tables - they must agree
  std::mt19937_64 rng(7);
  std::vector<char> buf(1 << 16);
  for (auto& c : buf) {
    c = static_cast<char>(rng());
  }
  for (size_t len : {size_t(0), size_t(1), size_t(9), size_t(4096),
		     buf.size()}) {
    EXPECT_EQ(spdk_crc64_nvme(buf.data(), len, 0),
	      ceph::crc64nvme(0, buf.data(), len)) << "len=" << len;
  }
}

TEST(Crc64Nvme, MatchesMadler)
{
  // ceph::crc64nvme may be backed by isa-l's SIMD implementation; it
  // must agree with the portable madler tables at every length,
  // alignment and chaining seed
  std::mt19937_64 rng(3);
  std::vector<char> buf((1 << 16) + 64);
  for (auto& c : buf) {
    c = static_cast<char>(rng());
  }
  for (size_t off : {size_t(0), size_t(1), size_t(3), size_t(7)}) {
    for (size_t len : {size_t(0), size_t(1), size_t(7), size_t(15),
		       size_t(63), size_t(64), size_t(65), size_t(255),
		       size_t(4096), size_t(1 << 16)}) {
      for (uint64_t seed : {uint64_t(0), CHECK_123456789, ~uint64_t(0)}) {
	EXPECT_EQ(crc64nvme_word(seed, buf.data() + off, len),
		  ceph::crc64nvme(seed, buf.data() + off, len))
	  << "off=" << off << " len=" << len << " seed=" << seed;
      }
    }
  }
}

TEST(Crc64Nvme, CombineProperty)
{
  std::mt19937_64 rng(42);
  std::vector<char> buf(1 << 15);
  for (auto& c : buf) {
    c = static_cast<char>(rng());
  }
  const uint64_t whole = ceph::crc64nvme(0, buf.data(), buf.size());
  for (int i = 0; i < 100; i++) {
    const size_t split = rng() % (buf.size() + 1);
    const uint64_t a = ceph::crc64nvme(0, buf.data(), split);
    const uint64_t b = ceph::crc64nvme(0, buf.data() + split,
				       buf.size() - split);
    EXPECT_EQ(whole, ceph::crc64nvme_combine(a, b, buf.size() - split))
      << "split=" << split;
  }
  // multi-way combine in order, like RGW folding stripe CRCs
  const size_t s1 = buf.size() / 3, s2 = 2 * buf.size() / 3;
  uint64_t acc = ceph::crc64nvme(0, buf.data(), s1);
  acc = ceph::crc64nvme_combine(acc, ceph::crc64nvme(0, buf.data() + s1,
						     s2 - s1), s2 - s1);
  acc = ceph::crc64nvme_combine(acc, ceph::crc64nvme(0, buf.data() + s2,
						     buf.size() - s2),
				buf.size() - s2);
  EXPECT_EQ(whole, acc);
}

TEST(Crc64Nvme, Checksummer)
{
  EXPECT_EQ(Checksummer::CSUM_CRC64NVME,
	    Checksummer::get_csum_string_type("crc64nvme"));
  EXPECT_STREQ("crc64nvme", Checksummer::get_csum_type_string(
		 Checksummer::CSUM_CRC64NVME));
  EXPECT_EQ(8u, Checksummer::get_csum_value_size(
	      Checksummer::CSUM_CRC64NVME));
  EXPECT_EQ(8u, Checksummer::get_csum_init_value_size(
	      Checksummer::CSUM_CRC64NVME));

  constexpr size_t block = 4096;
  constexpr size_t blocks = 4;
  std::mt19937_64 rng(11);
  bufferlist bl;
  {
    bufferptr data(block * blocks);
    for (size_t i = 0; i < data.length(); i++) {
      data[i] = static_cast<char>(rng());
    }
    bl.append(std::move(data));
  }

  bufferptr csum_data(blocks * sizeof(ceph_le64));
  ASSERT_EQ(0, Checksummer::calculate<Checksummer::crc64nvme>(
	      block, 0, bl.length(), bl, &csum_data));

  // each stored value is the canonical standalone checksum of its block
  // (the default init value is 0, not the crc32c/xxhash -1), and the
  // block values combine to the whole-buffer checksum
  auto* pv = reinterpret_cast<const ceph_le64*>(csum_data.c_str());
  uint64_t acc = 0;
  for (size_t i = 0; i < blocks; i++) {
    EXPECT_EQ(ceph::crc64nvme(0, bl.c_str() + i * block, block),
	      uint64_t(pv[i])) << "block " << i;
    acc = ceph::crc64nvme_combine(acc, pv[i], block);
  }
  EXPECT_EQ(ceph::crc64nvme(bl), acc);

  EXPECT_EQ(-1, Checksummer::verify<Checksummer::crc64nvme>(
	      block, 0, bl.length(), bl, csum_data));

  // corruption in the third block is pinned to that block
  bl.c_str()[2 * block + 17] ^= 0x40;
  uint64_t bad_csum = 0;
  EXPECT_EQ(int(2 * block), Checksummer::verify<Checksummer::crc64nvme>(
	      block, 0, bl.length(), bl, csum_data, &bad_csum));
  EXPECT_EQ(ceph::crc64nvme(0, bl.c_str() + 2 * block, block), bad_csum);
}

TEST(Crc64Nvme, Bufferlist)
{
  // multi-segment bufferlist equals the flat computation
  bufferlist bl;
  bl.append("12345");
  bl.append("6789");
  EXPECT_EQ(CHECK_123456789, ceph::crc64nvme(bl));
  bufferlist empty;
  EXPECT_EQ(0u, ceph::crc64nvme(empty));
}
