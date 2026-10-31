// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include <algorithm>
#include <bit>
#include <random>
#include <vector>

#include "acconfig.h"
#include "include/buffer.h"
#include "include/scope_guard.h"
#include "gtest/gtest.h"

extern "C" {
#include "common/madler/crc64nvme.h"
#include "common/spdk/crc64.h"
}

// the CRC-64/NVME check value, the CRC of "123456789" (the NVM Command
// Set specification 1.1, Figure 153, gives it bit-reversed, as
// 11199E50_6128D175h)
static constexpr uint64_t CHECK_123456789 = 0xae8b14860a799888ull;

// madler's slicing-by-8 routine loads native words that its tables take
// to be little-endian; its byte-at-a-time routine, the reference here,
// is right on either byte order
static constexpr bool LITTLE_ENDIAN_HOST =
  std::endian::native == std::endian::little;

TEST(Crc64Nvme, Canonical)
{
  const char* s = "123456789";
  EXPECT_EQ(CHECK_123456789, ceph::crc64nvme(0, s, 9));
  EXPECT_EQ(CHECK_123456789, crc64nvme_byte(0, s, 9));
  EXPECT_EQ(CHECK_123456789, crc64nvme_bit(0, s, 9));
  if constexpr (LITTLE_ENDIAN_HOST) {
    EXPECT_EQ(CHECK_123456789, crc64nvme_word(0, s, 9));
  }
  // empty input
  EXPECT_EQ(0u, ceph::crc64nvme(0, s, 0));
  EXPECT_EQ(CHECK_123456789, ceph::crc64nvme(CHECK_123456789, s, 0));
  EXPECT_EQ(CHECK_123456789, ceph::crc64nvme(CHECK_123456789, nullptr, 0));
  // incremental == one-shot
  uint64_t inc = ceph::crc64nvme(0, s, 4);
  inc = ceph::crc64nvme(inc, s + 4, 5);
  EXPECT_EQ(CHECK_123456789, inc);
}

TEST(Crc64Nvme, NvmGuardTestCases)
{
  // NVM Command Set specification 1.1, 5.3.1.3.5, Figure 155: "64b CRC
  // Test Cases for 4 KiB Logical Block with no Metadata"
  const struct {
    const char* contents;
    uint8_t (*byte)(size_t i);
    uint64_t guard;
  } cases[] = {
    {"each byte cleared to 00h",
     [](size_t) -> uint8_t { return 0x00; }, 0x6482d367eb22b64eull},
    {"each byte set to FFh",
     [](size_t) -> uint8_t { return 0xff; }, 0xc0ddba7302eca3acull},
    {"incrementing 00h to FFh, repeating",
     [](size_t i) -> uint8_t { return i & 0xff; }, 0x3e729f5f6750449cull},
    {"decrementing FFh to 00h, repeating",
     [](size_t i) -> uint8_t { return 0xff - (i & 0xff); },
     0x9a2df64b8e9e517eull},
  };
  for (const auto& c : cases) {
    std::vector<unsigned char> block(4096);
    for (size_t i = 0; i < block.size(); i++) {
      block[i] = c.byte(i);
    }
    EXPECT_EQ(c.guard, ceph::crc64nvme(0, block.data(), block.size()))
      << c.contents;
    EXPECT_EQ(c.guard, crc64nvme_byte(0, block.data(), block.size()))
      << c.contents;
    if constexpr (LITTLE_ENDIAN_HOST) {
      EXPECT_EQ(c.guard, crc64nvme_word(0, block.data(), block.size()))
        << c.contents;
    }
    EXPECT_EQ(c.guard, spdk_crc64_nvme(block.data(), block.size(), 0))
      << c.contents;
    for (const auto& [name, f] : ceph::crc64nvme_kernels()) {
      EXPECT_EQ(c.guard, f(0, block.data(), block.size()))
        << name << " " << c.contents;
    }
    // and as a bufferlist of odd-sized segments
    bufferlist bl;
    for (size_t pos = 0, len = 1; pos < block.size(); pos += len, len += 2) {
      len = std::min(len, block.size() - pos);
      bl.append(ceph::buffer::copy(
        reinterpret_cast<const char*>(block.data()) + pos, len));
    }
    EXPECT_LT(1u, bl.get_num_buffers());
    EXPECT_EQ(c.guard, ceph::crc64nvme(bl)) << c.contents;
  }
}

TEST(Crc64Nvme, MatchesMadler)
{
  // every implementation this host runs, ceph::crc64nvme()'s choice
  // among them, agrees with madler's byte-at-a-time routine at every
  // length across the kernels' block boundaries, at every alignment, and
  // with a nonzero seed
  std::mt19937_64 rng(1234);
  std::vector<unsigned char> buf(16 + (1 << 20));
  for (auto& c : buf) {
    c = static_cast<unsigned char>(rng());
  }
  auto check = [&](size_t off, size_t len, uint64_t seed) {
    const unsigned char* p = buf.data() + off;
    const uint64_t want = crc64nvme_byte(seed, p, len);
    EXPECT_EQ(want, ceph::crc64nvme(seed, p, len))
      << "off=" << off << " len=" << len << " seed=" << seed;
    if constexpr (LITTLE_ENDIAN_HOST) {
      EXPECT_EQ(want, crc64nvme_word(seed, p, len))
        << "off=" << off << " len=" << len << " seed=" << seed;
    }
    for (const auto& [name, f] : ceph::crc64nvme_kernels()) {
      EXPECT_EQ(want, f(seed, p, len))
        << name << " off=" << off << " len=" << len << " seed=" << seed;
    }
  };
  for (size_t len = 0; len <= 4096; len++) {
    check(len % 16, len, (len & 1) ? rng() : 0);
  }
  for (size_t off = 0; off < 16; off++) {
    check(off, 1 << 20, rng());
  }
}

TEST(Crc64Nvme, MatchesSpdk)
{
  // ceph::crc64nvme() (ISA-L or madler, chosen at run time), which the
  // OSD and rgw's digests use, must agree with the vendored spdk table
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

TEST(Crc64Nvme, ChainedSegments)
{
  // one call per segment, as over a read reply's bufferlist: segments of
  // odd lengths that start at odd addresses, chained through the seed
  std::mt19937_64 rng(99);
  const std::pair<size_t, size_t> shapes[] = {  // longest segment, total
    {7, 1 << 16}, {300, 1 << 18}, {70000, 1 << 20}};
  for (auto [max_seg, n] : shapes) {
    std::vector<char> src(n);
    for (auto& c : src) {
      c = static_cast<char>(rng());
    }
    const uint64_t want = crc64nvme_byte(0, src.data(), n);
    const ceph::buffer::ptr whole = ceph::buffer::copy(src.data(), n);
    bufferlist bl;
    uint64_t chained = 0;
    unsigned segs = 0;
    for (size_t pos = 0; pos < n; segs++) {
      const size_t len = std::min<size_t>(1 + rng() % max_seg, n - pos);
      bl.append(ceph::buffer::ptr(whole, pos, len));
      chained = ceph::crc64nvme(chained, src.data() + pos, len);
      pos += len;
    }
    ASSERT_EQ(segs, bl.get_num_buffers());
    EXPECT_EQ(want, ceph::crc64nvme(bl)) << "max_seg=" << max_seg;
    EXPECT_EQ(want, chained) << "max_seg=" << max_seg;
  }
}

TEST(Crc64Nvme, WithCrc32c)
{
  // one walk gives what the two passes give, over segments of odd
  // lengths, with any CRC-32C seed. Segments under 4 KiB on average
  // (max_seg 7 and 300) take the two passes themselves
  std::mt19937_64 rng(129);
  for (size_t max_seg : {size_t(7), size_t(300), size_t(70000),
			 size_t(1 << 20)}) {
    std::vector<char> src(1 << 18);
    for (auto& c : src) {
      c = static_cast<char>(rng());
    }
    bufferlist bl;
    for (size_t pos = 0; pos < src.size();) {
      const size_t len = std::min<size_t>(1 + rng() % max_seg,
					  src.size() - pos);
      bl.append(ceph::buffer::copy(src.data() + pos, len));
      pos += len;
    }
    for (uint32_t seed : {0u, 0xffffffffu, 0x12345678u}) {
      uint32_t crc32 = seed;
      EXPECT_EQ(ceph::crc64nvme(bl), ceph::crc64nvme_crc32c(bl, &crc32))
	<< "max_seg=" << max_seg;
      bl.invalidate_crc();
      EXPECT_EQ(bl.crc32c(seed), crc32) << "max_seg=" << max_seg
					<< " seed=" << seed;
    }
  }
  bufferlist empty;
  uint32_t crc32 = 0x12345678;
  EXPECT_EQ(0u, ceph::crc64nvme_crc32c(empty, &crc32));
  EXPECT_EQ(0x12345678u, crc32);
}

TEST(Crc64Nvme, WithCrc32cUsesTheBufferCache)
{
  // the CRC-32C of a buffer of 4 KiB or more goes through its cache, as
  // bl.crc32c() does: it fills it for a later data digest of the same
  // bytes, and finds what an earlier one left. A smaller buffer leaves
  // its cache alone
  std::mt19937_64 rng(1290);
  ceph::buffer::track_cached_crc(true);
  auto untrack = make_scope_guard([] {
    ceph::buffer::track_cached_crc(false);
  });
  for (size_t min_seg : {size_t(500), size_t(5000)}) {
    bufferlist bl;
    for (int i = 0; i < 16; i++) {
      std::vector<char> seg(min_seg + rng() % (2 * min_seg));
      for (auto& c : seg) {
	c = static_cast<char>(rng());
      }
      bl.append(ceph::buffer::copy(seg.data(), seg.size()));
    }
    ASSERT_EQ(16u, bl.get_num_buffers());
    // every buffer is over 4 KiB, or none is
    const int cached = min_seg >= 4096 ? 16 : 0;
    int hits = ceph::buffer::get_cached_crc();
    int misses = ceph::buffer::get_missed_crc();
    uint32_t crc32 = -1;
    ceph::crc64nvme_crc32c(bl, &crc32);
    EXPECT_EQ(misses + cached, ceph::buffer::get_missed_crc())
      << "min_seg=" << min_seg;
    EXPECT_EQ(crc32, bl.crc32c(-1)) << "min_seg=" << min_seg;
    EXPECT_EQ(hits + cached, ceph::buffer::get_cached_crc())
      << "min_seg=" << min_seg;
    // and the other way around, over copies of the same buffers
    bufferlist copy;
    for (const auto& p : bl.buffers()) {
      copy.append(ceph::buffer::copy(p.c_str(), p.length()));
    }
    const uint32_t digest = copy.crc32c(-1);
    hits = ceph::buffer::get_cached_crc();
    misses = ceph::buffer::get_missed_crc();
    crc32 = -1;
    EXPECT_EQ(ceph::crc64nvme(copy), ceph::crc64nvme_crc32c(copy, &crc32))
      << "min_seg=" << min_seg;
    EXPECT_EQ(digest, crc32) << "min_seg=" << min_seg;
    EXPECT_EQ(hits + cached, ceph::buffer::get_cached_crc())
      << "min_seg=" << min_seg;
    EXPECT_EQ(misses, ceph::buffer::get_missed_crc()) << "min_seg=" << min_seg;
  }
}
