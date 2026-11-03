// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gm_value.hpp"
#include "multipart_manifest.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

using namespace kvrgw;

namespace {

// Builds a ManifestInput's backing storage + round-trips it through
// encode_manifest_body -> decode_manifest_body, asserting the decode
// exactly reproduces the input. Returns the encoded length.
size_t round_trip(std::vector<uint16_t> included, std::vector<uint16_t> generations,
                  std::vector<uint64_t> sizes, std::vector<uint8_t>& buf_out)
{
  ManifestInput in{included, generations, sizes};
  buf_out.assign(manifest_body_bound(included.size()), 0);
  size_t keep_len = 0;
  const size_t len = encode_manifest_body(buf_out.data(), buf_out.size(), in, &keep_len);
  assert(len > 0);
  assert(len <= manifest_body_bound(included.size()));
  assert(keep_len > 4 && keep_len <= len);

  // The Knob 1+2 prefix alone is a valid keep-list body.
  {
    ManifestKeepCursor prefix_cursor(
        std::string_view(reinterpret_cast<const char*>(buf_out.data()), keep_len));
    assert(prefix_cursor.valid());
    for (size_t i = 0; i < included.size(); ++i) {
      assert(prefix_cursor.is_kept(included[i], generations[i]));
    }
  }

  auto decoded = decode_manifest_body(
      std::string_view(reinterpret_cast<const char*>(buf_out.data()), len));
  assert(decoded);
  assert(decoded->included_parts == included);
  assert(decoded->generations == generations);
  assert(decoded->sizes == sizes);
  return len;
}

// Verifies ManifestKeepCursor agrees with the ground truth for every
// (part_number, generation) combination GcWorker could plausibly ask
// about -- every part_number in [1, max_probe], crossed with every
// generation in [0, 4).
void check_cursor(const std::vector<uint8_t>& buf, size_t len,
                  const std::vector<uint16_t>& included,
                  const std::vector<uint16_t>& generations,
                  uint16_t max_probe)
{
  ManifestKeepCursor cursor(
      std::string_view(reinterpret_cast<const char*>(buf.data()), len));
  assert(cursor.valid());

  for (uint16_t pn = 1; pn <= max_probe; ++pn) {
    const auto it = std::find(included.begin(), included.end(), pn);
    const bool is_included = (it != included.end());
    const uint16_t kept_gen = is_included ? generations[it - included.begin()] : 0;
    for (uint16_t gen = 0; gen < 4; ++gen) {
      const bool expected = is_included && gen == kept_gen;
      assert(cursor.is_kept(pn, gen) == expected);
      // Repeat query for the same part_number must be idempotent.
      assert(cursor.is_kept(pn, gen) == expected);
    }
  }
}

//--------------------------------------------------------------------------------
// Worked example 1 (multipart_manifest.md): ten thousand parts, nobody
// re-uploaded, uniform size except the last part. 19 bytes total.
void test_worked_example_1()
{
  const uint16_t n = 10000;
  std::vector<uint16_t> included(n);
  std::vector<uint16_t> generations(n, 0);
  std::vector<uint64_t> sizes(n, 8ull * 1024 * 1024);  // 8MB, 24-bit tier
  for (uint16_t i = 0; i < n; ++i) included[i] = static_cast<uint16_t>(i + 1);
  sizes[n - 1] = 123;  // the one differing last part

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, generations, sizes, buf);
  assert(len == 19);

  check_cursor(buf, len, included, generations, 20);
  std::cout << "test_worked_example_1 OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Worked example 2: nine thousand included, one thousand excluded
// (scattered -- forces continuous+exclude), nobody re-uploaded.
void test_worked_example_2()
{
  const uint16_t total = 10000;
  std::vector<uint16_t> included;
  std::vector<uint16_t> generations;
  std::vector<uint64_t> sizes;
  for (uint16_t pn = 1; pn <= total; ++pn) {
    if (pn % 10 == 0) continue;  // exclude every 10th -- exactly 1000 excluded
    included.push_back(pn);
    generations.push_back(0);
    sizes.push_back(5ull * 1024 * 1024);
  }
  assert(included.size() == 9000);

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, generations, sizes, buf);
  // Bitmap mode (mode 4) wins for 9000 scattered parts -- 2+1250 bitmap
  // + header(4) + default_gen(1) + default_size(3) = 1260 bytes.
  assert(len == 1260);

  check_cursor(buf, len, included, generations, 50);
  std::cout << "test_worked_example_2 OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Worked example 3: all parts included, three re-uploaded, sizes uniform.
void test_worked_example_3()
{
  const uint16_t n = 10000;
  std::vector<uint16_t> included(n);
  std::vector<uint16_t> generations(n, 0);
  std::vector<uint64_t> sizes(n, 5ull * 1024 * 1024);
  for (uint16_t i = 0; i < n; ++i) included[i] = static_cast<uint16_t>(i + 1);
  generations[4] = 1;    // part 5
  generations[99] = 2;   // part 100
  generations[9999] = 1; // part 10000

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, generations, sizes, buf);
  assert(len < 40);  // "roughly thirty bytes" per the doc

  check_cursor(buf, len, included, generations, 10001);
  std::cout << "test_worked_example_3 OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Knob 1 has excluded part 5; parts 4, 6, 7 deviate on generation and are
// adjacent *among included parts* despite the raw-value gap at 5.
void test_knob1_hole_between_knob2_deviators()
{
  std::vector<uint16_t> included = {1, 2, 3, 4, 6, 7, 8, 9, 10};  // 5 excluded
  std::vector<uint16_t> generations(included.size(), 0);
  std::vector<uint64_t> sizes(included.size(), 1024);
  // parts 4, 6, 7 deviate -- indices 3, 4, 5 in `included`.
  generations[3] = 5;
  generations[4] = 9;
  generations[5] = 2;

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, generations, sizes, buf);
  check_cursor(buf, len, included, generations, 12);
  std::cout << "test_knob1_hole_between_knob2_deviators OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Forces each of Knob 1's descriptor modes (0-3) and confirms mode byte.
void test_knob1_descriptor_modes()
{
  auto mode_of = [](const std::vector<uint8_t>& buf) { return static_cast<SubsetMode>(buf[1]); };

  // Mode 0 -- continuous. Use a range where mode 0 (4 bytes) beats bitmap.
  // bitmap cost = 2 + ceil(last/8). For last=32: 2+4=6 > 4. Mode 0 wins.
  {
    std::vector<uint16_t> included;
    for (uint16_t pn = 1; pn <= 32; ++pn) included.push_back(pn);
    std::vector<uint16_t> gens(32, 0);
    std::vector<uint64_t> sizes(32, 100);
    std::vector<uint8_t> buf;
    round_trip(included, gens, sizes, buf);
    assert(mode_of(buf) == SubsetMode::kContinuous);
  }
  // Mode 3 -- inclusion-list wins when scattered and small.
  {
    std::vector<uint16_t> included = {1, 50, 9999};
    std::vector<uint16_t> gens(3, 0);
    std::vector<uint64_t> sizes(3, 100);
    std::vector<uint8_t> buf;
    round_trip(included, gens, sizes, buf);
    assert(mode_of(buf) == SubsetMode::kInclusionList);
  }
  // Mode 1 -- continuous+exclude: dense range, one excluded part.
  {
    std::vector<uint16_t> included;
    for (uint16_t pn = 1; pn <= 100; ++pn) {
      if (pn == 50) continue;
      included.push_back(pn);
    }
    std::vector<uint16_t> gens(included.size(), 0);
    std::vector<uint64_t> sizes(included.size(), 100);
    std::vector<uint8_t> buf;
    round_trip(included, gens, sizes, buf);
    assert(mode_of(buf) == SubsetMode::kContinuousExclude);
  }
  // Mode 2 -- interval-set: a handful of separate dense runs.
  {
    std::vector<uint16_t> included = {1, 2, 3, 100, 101, 102, 200, 201, 202};
    std::vector<uint16_t> gens(included.size(), 0);
    std::vector<uint64_t> sizes(included.size(), 100);
    std::vector<uint8_t> buf;
    round_trip(included, gens, sizes, buf);
    assert(mode_of(buf) == SubsetMode::kIntervalSet);
  }

  std::cout << "test_knob1_descriptor_modes OK\n";
}

//--------------------------------------------------------------------------------
// Forces Knob 1 mode 4 (bitmap): scatter all 10,000 parts so that every
// descriptor mode would cost more than the 1,252-byte bitmap ceiling.
void test_knob1_bitmap_mode()
{
  auto mode_of = [](const std::vector<uint8_t>& buf) { return static_cast<SubsetMode>(buf[1]); };

  // 5,000 parts scattered evenly across the 10,000-part range: every
  // odd-numbered part. inclusion-list cost = 2 + 2*5000 = 10,002 B;
  // interval-set = lots of 1-element runs = expensive;
  // bitmap = 2 + 1250 = 1,252 B -- bitmap wins.
  const uint16_t n = 5000;
  std::vector<uint16_t> included;
  included.reserve(n);
  for (uint16_t pn = 1; pn <= 10000; pn += 2) included.push_back(pn);
  assert(included.size() == n);

  std::vector<uint16_t> gens(n, 0);
  std::vector<uint64_t> sizes(n, 5ull * 1024 * 1024);

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, gens, sizes, buf);
  assert(mode_of(buf) == SubsetMode::kFlatArray);
  // bitmap body: 2-byte prefix + 1250 bytes
  // total should be header(4) + 2+1250 + default_gen(1) + k2_mode0(0) + default_size(3) + k3_mode0(0)
  // = 4 + 1252 + 1 + 3 = 1260
  assert(len == 1260);

  check_cursor(buf, len, included, gens, 100);
  std::cout << "test_knob1_bitmap_mode OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Knob 2 2-bit width tier: max gen <= 3, four per byte.
void test_knob2_2bit_width()
{
  std::vector<uint16_t> included = {1, 2, 3, 4, 5, 6, 7, 8};
  // gens 0,1,2,3,0,1,2,3 -- max=3, selects k2Bit tier
  std::vector<uint16_t> gens    = {0, 1, 2, 3, 0, 1, 2, 3};
  std::vector<uint64_t> sizes(8, 5ull * 1024 * 1024);

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, gens, sizes, buf);
  // Check width nibble in header byte 0: high nibble should be 0 (k2Bit)
  assert(((buf[0] >> 4) & 0xF) == static_cast<uint8_t>(GenWidth::k2Bit));
  check_cursor(buf, len, included, gens, 10);
  std::cout << "test_knob2_2bit_width OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Knob 2 nibble-packing: both an even and an odd deviator count.
void test_knob2_nibble_packing()
{
  // Even count (4 deviators).
  {
    std::vector<uint16_t> included = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<uint16_t> gens = {0, 4, 0, 7, 0, 1, 0, 15};  // max=15 -> nibble
    std::vector<uint64_t> sizes(8, 100);
    std::vector<uint8_t> buf;
    const size_t len = round_trip(included, gens, sizes, buf);
    assert(static_cast<GenWidth>((buf[0] >> 4) & 0xF) == GenWidth::kNibble);
    check_cursor(buf, len, included, gens, 10);
  }
  // Odd count (3 deviators) -- exercises the zero-padded trailing nibble.
  {
    std::vector<uint16_t> included = {1, 2, 3, 4, 5};
    std::vector<uint16_t> gens = {0, 4, 0, 7, 1};  // max=7 -> nibble (>3)
    std::vector<uint64_t> sizes(5, 100);
    std::vector<uint8_t> buf;
    const size_t len = round_trip(included, gens, sizes, buf);
    assert(static_cast<GenWidth>((buf[0] >> 4) & 0xF) == GenWidth::kNibble);
    check_cursor(buf, len, included, gens, 7);
  }

  std::cout << "test_knob2_nibble_packing OK\n";
}

//--------------------------------------------------------------------------------
// Knob 2 uint16 width (max gen > 255).
void test_knob2_u16_width()
{
  std::vector<uint16_t> included = {1, 2, 3};
  std::vector<uint16_t> gens = {0, 300, 65535};
  std::vector<uint64_t> sizes(3, 5ull * 1024 * 1024);
  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, gens, sizes, buf);
  assert(static_cast<GenWidth>((buf[0] >> 4) & 0xF) == GenWidth::kU16);
  check_cursor(buf, len, included, gens, 5);
  std::cout << "test_knob2_u16_width OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Forces Knob 2 mode 4 (flat array): 10,000 parts all with distinct
// non-zero generations, so descriptor + values > flat array.
void test_knob2_flat_mode()
{
  auto k2_mode_of = [](const std::vector<uint8_t>& buf) { return static_cast<SubsetMode>(buf[2]); };

  const uint16_t n = 10000;
  std::vector<uint16_t> included(n);
  for (uint16_t i = 0; i < n; ++i) included[i] = static_cast<uint16_t>(i + 1);

  // All parts deviate: gen[i] = (i % 3) + 1 so all non-zero, max=3 -> k2Bit tier.
  // k2Bit flat = 2500 B; descriptor 0B (mode 0 fails) + inclusion-list = 2+2*10000 = 20002 B.
  // Flat wins.
  std::vector<uint16_t> gens(n);
  for (uint16_t i = 0; i < n; ++i) gens[i] = static_cast<uint16_t>((i % 3) + 1);

  std::vector<uint64_t> sizes(n, 5ull * 1024 * 1024);
  std::vector<uint8_t> buf;
  round_trip(included, gens, sizes, buf);
  assert(k2_mode_of(buf) == SubsetMode::kFlatArray);

  ManifestKeepCursor cursor(
      std::string_view(reinterpret_cast<const char*>(buf.data()), buf.size()));
  assert(cursor.valid());
  for (uint16_t i = 0; i < n; ++i) {
    assert(cursor.is_kept(included[i], gens[i]));
    // Wrong generation should not match.
    assert(!cursor.is_kept(included[i], static_cast<uint16_t>(gens[i] + 1)));
  }
  std::cout << "test_knob2_flat_mode OK\n";
}

//--------------------------------------------------------------------------------
// Forces Knob 3 mode 4 (flat array): 10,000 parts all with distinct sizes
// > 4 GB, so Boyer-Moore finds no majority and descriptor > flat.
void test_knob3_flat_mode()
{
  auto k3_mode_of = [](const std::vector<uint8_t>& buf) { return static_cast<SubsetMode>(buf[3]); };

  const uint16_t n = 10000;
  std::vector<uint16_t> included(n);
  for (uint16_t i = 0; i < n; ++i) included[i] = static_cast<uint16_t>(i + 1);
  std::vector<uint16_t> gens(n, 0);
  // Each part has a unique size > 4 GB: base + i bytes.
  const uint64_t base = 5ull * 1024 * 1024 * 1024;  // 5 GB (40-bit tier)
  std::vector<uint64_t> sizes(n);
  for (uint16_t i = 0; i < n; ++i) sizes[i] = base + i;

  std::vector<uint8_t> buf;
  round_trip(included, gens, sizes, buf);
  assert(k3_mode_of(buf) == SubsetMode::kFlatArray);
  assert(static_cast<SizeWidth>(buf[0] & 0xF) == SizeWidth::k40);
  std::cout << "test_knob3_flat_mode OK\n";
}

//--------------------------------------------------------------------------------
// Knob 3's width tiers -- 24/32/40-bit, each actually selected.
void test_knob3_width_tiers()
{
  struct Case { uint64_t size; SizeWidth expect; };
  const Case cases[] = {
    {16ull * 1024 * 1024 - 1, SizeWidth::k24},
    {16ull * 1024 * 1024 + 1, SizeWidth::k32},
    {5ull * 1024 * 1024 * 1024, SizeWidth::k40},  // kMaxPartSizeBytes
  };
  for (const auto& c : cases) {
    std::vector<uint16_t> included = {1, 2, 3};
    std::vector<uint16_t> gens(3, 0);
    std::vector<uint64_t> sizes = {5ull * 1024 * 1024, c.size, 5ull * 1024 * 1024};
    std::vector<uint8_t> buf;
    round_trip(included, gens, sizes, buf);
    assert(static_cast<SizeWidth>(buf[0] & 0xF) == c.expect);
  }

  std::cout << "test_knob3_width_tiers OK\n";
}

//--------------------------------------------------------------------------------
// Boyer-Moore default discovery: a true majority size must be found exactly.
void test_boyer_moore_default_discovery()
{
  const uint16_t n = 101;
  std::vector<uint16_t> included(n);
  std::vector<uint16_t> gens(n, 0);
  std::vector<uint64_t> sizes(n, 7ull * 1024 * 1024);  // majority: 51 entries
  for (uint16_t i = 0; i < n; ++i) included[i] = static_cast<uint16_t>(i + 1);
  for (uint16_t i = 0; i < 50; ++i) sizes[i] = 999;  // 50 deviators, still < majority

  std::vector<uint8_t> buf;
  const size_t len = round_trip(included, gens, sizes, buf);

  // Knob 1 is continuous (4 bytes), Knob 2 mode 0 (0 bytes for region),
  // default_gen is 1 byte (k2Bit scalar), so default_size sits at:
  // header(4) + knob1(4) + default_gen(1) + knob2_region(0) = offset 9.
  // 7MB needs 24-bit tier (3 bytes).
  assert(buf[1] == static_cast<uint8_t>(SubsetMode::kContinuous));
  assert(buf[2] == static_cast<uint8_t>(SubsetMode::kContinuous));  // Knob2: mode 0
  const uint64_t default_size = (static_cast<uint64_t>(buf[9]) << 16) |
                                (static_cast<uint64_t>(buf[10]) << 8) |
                                static_cast<uint64_t>(buf[11]);
  assert(default_size == 7ull * 1024 * 1024);

  std::cout << "test_boyer_moore_default_discovery OK (" << len << " bytes)\n";
}

//--------------------------------------------------------------------------------
// Randomized round trip across many shapes.
void test_randomized_round_trip()
{
  std::mt19937 rng(12345);
  for (int trial = 0; trial < 200; ++trial) {
    const uint16_t universe = 1 + (rng() % 500);
    std::vector<uint16_t> included;
    for (uint16_t pn = 1; pn <= universe; ++pn) {
      if (rng() % 3 != 0) included.push_back(pn);  // ~2/3 included
    }
    if (included.empty()) included.push_back(1);

    std::vector<uint16_t> gens(included.size());
    std::vector<uint64_t> sizes(included.size());
    for (size_t i = 0; i < included.size(); ++i) {
      // generations capped at uint16_t max (65535)
      gens[i] = (rng() % 5 == 0) ? static_cast<uint16_t>(rng() % 65536) : 0;
      sizes[i] = (rng() % 4 == 0) ? (rng() % 6000000000ull) : 5ull * 1024 * 1024;
    }

    std::vector<uint8_t> buf;
    const size_t len = round_trip(included, gens, sizes, buf);
    check_cursor(buf, len, included, gens, static_cast<uint16_t>(universe + 5));
  }

  std::cout << "test_randomized_round_trip OK (200 trials)\n";
}

}  // namespace

// manifest_body_bound(n) must hold for adversarial shapes.
void test_body_bound_adversarial()
{
  for (const size_t n : {size_t{1}, size_t{2}, size_t{100}, size_t{5000}, size_t{9999},
                         size_t{10000}}) {
    for (const bool holes : {false, true}) {
      std::vector<uint16_t> included;
      for (uint32_t p = 1; included.size() < n && p <= kMaxPartNumber; p += (holes ? 2 : 1)) {
        included.push_back(static_cast<uint16_t>(p));
      }
      if (included.size() != n) continue;  // holes shape does not fit this n
      std::vector<uint16_t> generations(n);
      std::vector<uint64_t> sizes(n);
      for (size_t i = 0; i < n; ++i) {
        // generations capped at uint16_t max; alternate between 0 and max
        generations[i] = (i % 2) ? static_cast<uint16_t>(65535) : static_cast<uint16_t>(i % 256);
        sizes[i] = (uint64_t{1} << 38) + (i % 3) * 7919 + i;
      }
      std::vector<uint8_t> buf;
      const size_t len = round_trip(included, generations, sizes, buf);
      assert(len <= manifest_body_bound(n));
    }
  }
  // static_assert is now in the header; verify at runtime too.
  assert(manifest_body_bound(kMaxPartNumber) < 100000);
  std::cout << "test_body_bound_adversarial OK\n";
}

int main()
{
  test_worked_example_1();
  test_worked_example_2();
  test_worked_example_3();
  test_knob1_hole_between_knob2_deviators();
  test_knob1_descriptor_modes();
  test_knob1_bitmap_mode();
  test_knob2_2bit_width();
  test_knob2_nibble_packing();
  test_knob2_u16_width();
  test_knob2_flat_mode();
  test_knob3_flat_mode();
  test_knob3_width_tiers();
  test_boyer_moore_default_discovery();
  test_randomized_round_trip();
  test_body_bound_adversarial();
  std::cout << "ALL MULTIPART MANIFEST CODEC TESTS PASSED\n";
  return 0;
}
