// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include <random>

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
