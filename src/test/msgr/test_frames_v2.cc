// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "msg/async/frames_v2.h"

#include <numeric>
#include <ostream>
#include <string>
#include <tuple>

#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/Context.h"

#include <gtest/gtest.h>

namespace ceph::msgr::v2 {

// MessageFrame with the first segment not fixed to ceph_msg_header2
struct TestFrame : Frame<TestFrame,
                         /* four segments */
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::PAGE_SIZE_ALIGNMENT> {
  static constexpr Tag tag = static_cast<Tag>(123);

  static TestFrame Encode(const bufferlist& header,
                          const bufferlist& front,
                          const bufferlist& middle,
                          const bufferlist& data) {
    TestFrame f;
    f.segments[SegmentIndex::Msg::HEADER] = header;
    f.segments[SegmentIndex::Msg::FRONT] = front;
    f.segments[SegmentIndex::Msg::MIDDLE] = middle;
    f.segments[SegmentIndex::Msg::DATA] = data;

    // discard cached crcs for perf tests
    f.segments[SegmentIndex::Msg::HEADER].invalidate_crc();
    f.segments[SegmentIndex::Msg::FRONT].invalidate_crc();
    f.segments[SegmentIndex::Msg::MIDDLE].invalidate_crc();
    f.segments[SegmentIndex::Msg::DATA].invalidate_crc();
    return f;
  }

  static TestFrame Decode(segment_bls_t& segment_bls) {
    TestFrame f;
    // Transfer segments' bufferlists.  If segment_bls contains
    // less than SegmentsNumV segments, the missing ones will be
    // seen as empty.
    for (size_t i = 0; i < segment_bls.size(); i++) {
      f.segments[i] = std::move(segment_bls[i]);
    }
    return f;
  }

  bufferlist& header() {
    return segments[SegmentIndex::Msg::HEADER];
  }
  bufferlist& front() {
    return segments[SegmentIndex::Msg::FRONT];
  }
  bufferlist& middle() {
    return segments[SegmentIndex::Msg::MIDDLE];
  }
  bufferlist& data() {
    return segments[SegmentIndex::Msg::DATA];
  }

protected:
  using Frame::Frame;
};

struct mode_t {
  bool is_rev1;
  bool is_secure;
};

static std::ostream& operator<<(std::ostream& os, const mode_t& m) {
  os << "msgr2." << (m.is_rev1 ? "1" : "0")
     << (m.is_secure ? "-secure" : "-crc");
  return os;
}

static const mode_t modes[] = {
  {false, false},
  {false, true},
  {true, false},
  {true, true},
};

struct round_trip_instance_t {
  uint32_t header_len;
  uint32_t front_len;
  uint32_t middle_len;
  uint32_t data_len;

  // expected number of segments (same for each mode)
  size_t num_segments;
  // expected layout (different for each mode)
  uint32_t onwire_lens[4][MAX_NUM_SEGMENTS + 2];
};

static std::ostream& operator<<(std::ostream& os,
                                const round_trip_instance_t& rti) {
  os << rti.header_len << "+" << rti.front_len << "+"
     << rti.middle_len << "+" << rti.data_len;
  return os;
}

static bufferlist make_bufferlist(size_t len, char c) {
  bufferlist bl;
  if (len > 0) {
    bl.reserve(len);
    bl.append(std::string(len, c));
  }
  return bl;
}

bool disassemble_frame(FrameAssembler& frame_asm, bufferlist& frame_bl,
                       Tag& tag, segment_bls_t& segment_bls) {
  bufferlist preamble_bl;
  frame_bl.splice(0, frame_asm.get_preamble_onwire_len(), &preamble_bl);
  tag = frame_asm.disassemble_preamble(preamble_bl);

  do {
    size_t seg_idx = segment_bls.size();
    segment_bls.emplace_back();

    uint32_t onwire_len = frame_asm.get_segment_onwire_len(seg_idx);
    if (onwire_len > 0) {
      frame_bl.splice(0, onwire_len, &segment_bls.back());
    }
  } while (segment_bls.size() < frame_asm.get_num_segments());

  bufferlist epilogue_bl;
  uint32_t epilogue_onwire_len = frame_asm.get_epilogue_onwire_len();
  if (epilogue_onwire_len > 0) {
    frame_bl.splice(0, epilogue_onwire_len, &epilogue_bl);
  }
  frame_asm.disassemble_first_segment(preamble_bl, segment_bls[0]);
  return frame_asm.disassemble_remaining_segments(segment_bls.data(),
                                                  epilogue_bl);
}

class RoundTripTestBase : public ::testing::TestWithParam<
                              std::tuple<round_trip_instance_t, mode_t>> {
protected:
  RoundTripTestBase()
      : m_tx_frame_asm(&m_tx_crypto, std::get<1>(GetParam()).is_rev1),
        m_rx_frame_asm(&m_rx_crypto, std::get<1>(GetParam()).is_rev1),
        m_header(make_bufferlist(std::get<0>(GetParam()).header_len, 'H')),
        m_front(make_bufferlist(std::get<0>(GetParam()).front_len, 'F')),
        m_middle(make_bufferlist(std::get<0>(GetParam()).middle_len, 'M')),
        m_data(make_bufferlist(std::get<0>(GetParam()).data_len, 'D')) {
    const auto& m = std::get<1>(GetParam());
    if (m.is_secure) {
      AuthConnectionMeta auth_meta;
      auth_meta.con_mode = CEPH_CON_MODE_SECURE;
      // see AuthConnectionMeta::get_connection_secret_length()
      auth_meta.connection_secret.resize(64);
      g_ceph_context->random()->get_bytes(auth_meta.connection_secret.data(),
                                          auth_meta.connection_secret.size());
      m_tx_crypto = ceph::crypto::onwire::rxtx_t::create_handler_pair(
          g_ceph_context, auth_meta, /*new_nonce_format=*/m.is_rev1,
          /*crossed=*/false);
      m_rx_crypto = ceph::crypto::onwire::rxtx_t::create_handler_pair(
          g_ceph_context, auth_meta, /*new_nonce_format=*/m.is_rev1,
          /*crossed=*/true);
    }
  }

  void check_frame_assembler(const FrameAssembler& frame_asm) {
    const auto& [rti, m] = GetParam();
    const auto& onwire_lens = rti.onwire_lens[m.is_rev1 << 1 | m.is_secure];
    EXPECT_EQ(rti.header_len + rti.front_len + rti.middle_len + rti.data_len,
              frame_asm.get_frame_logical_len());
    ASSERT_EQ(rti.num_segments, frame_asm.get_num_segments());
    EXPECT_EQ(onwire_lens[0], frame_asm.get_preamble_onwire_len());
    for (size_t i = 0; i < rti.num_segments; i++) {
      EXPECT_EQ(onwire_lens[i + 1], frame_asm.get_segment_onwire_len(i));
    }
    EXPECT_EQ(onwire_lens[rti.num_segments + 1],
              frame_asm.get_epilogue_onwire_len());
    EXPECT_EQ(std::accumulate(std::begin(onwire_lens), std::end(onwire_lens),
                              uint64_t(0)),
              frame_asm.get_frame_onwire_len());
  }

  void test_round_trip() {
    auto tx_frame = TestFrame::Encode(m_header, m_front, m_middle, m_data);
    auto onwire_bl = tx_frame.get_buffer(m_tx_frame_asm);
    check_frame_assembler(m_tx_frame_asm);
    EXPECT_EQ(m_tx_frame_asm.get_frame_onwire_len(), onwire_bl.length());

    Tag rx_tag;
    segment_bls_t rx_segment_bls;
    EXPECT_TRUE(disassemble_frame(m_rx_frame_asm, onwire_bl, rx_tag,
                                  rx_segment_bls));
    check_frame_assembler(m_rx_frame_asm);
    EXPECT_EQ(0, onwire_bl.length());
    EXPECT_EQ(TestFrame::tag, rx_tag);
    EXPECT_EQ(m_rx_frame_asm.get_num_segments(), rx_segment_bls.size());

    auto rx_frame = TestFrame::Decode(rx_segment_bls);
    EXPECT_TRUE(m_header.contents_equal(rx_frame.header()));
    EXPECT_TRUE(m_front.contents_equal(rx_frame.front()));
    EXPECT_TRUE(m_middle.contents_equal(rx_frame.middle()));
    EXPECT_TRUE(m_data.contents_equal(rx_frame.data()));
  }

  ceph::crypto::onwire::rxtx_t m_tx_crypto;
  ceph::crypto::onwire::rxtx_t m_rx_crypto;
  FrameAssembler m_tx_frame_asm;
  FrameAssembler m_rx_frame_asm;

  const bufferlist m_header;
  const bufferlist m_front;
  const bufferlist m_middle;
  const bufferlist m_data;
};

class RoundTripTest : public RoundTripTestBase {};

TEST_P(RoundTripTest, Basic) {
  test_round_trip();
}

TEST_P(RoundTripTest, Reuse) {
  for (int i = 0; i < 3; i++) {
    test_round_trip();
  }
}

static const round_trip_instance_t round_trip_instances[] = {
  // first segment is empty
  { 0,   0,   0,   0, 1, {{32,  0,  17,   0,   0,  0},
                          {32,  0,  32,   0,   0,  0},
                          {32,  0,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  { 0,   0,   0, 303, 4, {{32,  0,   0,   0, 303, 17},
                          {32,  0,   0,   0, 304, 32},
                          {32,  0,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  { 0,   0, 202,   0, 3, {{32,  0,   0, 202,  17,  0},
                          {32,  0,   0, 208,  32,  0},
                          {32,  0,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  { 0,   0, 202, 303, 4, {{32,  0,   0, 202, 303, 17},
                          {32,  0,   0, 208, 304, 32},
                          {32,  0,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  { 0, 101,   0,   0, 2, {{32,  0, 101,  17,   0,  0},
                          {32,  0, 112,  32,   0,  0},
                          {32,  0, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  { 0, 101,   0, 303, 4, {{32,  0, 101,   0, 303, 17},
                          {32,  0, 112,   0, 304, 32},
                          {32,  0, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  { 0, 101, 202,   0, 3, {{32,  0, 101, 202,  17,  0},
                          {32,  0, 112, 208,  32,  0},
                          {32,  0, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  { 0, 101, 202, 303, 4, {{32,  0, 101, 202, 303, 17},
                          {32,  0, 112, 208, 304, 32},
                          {32,  0, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is fully inlined, inline buffer is not full
  { 1,   0,   0,   0, 1, {{32,  1,  17,   0,   0,  0},
                          {32, 16,  32,   0,   0,  0},
                          {32,  5,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  { 1,   0,   0, 303, 4, {{32,  1,   0,   0, 303, 17},
                          {32, 16,   0,   0, 304, 32},
                          {32,  5,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  { 1,   0, 202,   0, 3, {{32,  1,   0, 202,  17,  0},
                          {32, 16,   0, 208,  32,  0},
                          {32,  5,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  { 1,   0, 202, 303, 4, {{32,  1,   0, 202, 303, 17},
                          {32, 16,   0, 208, 304, 32},
                          {32,  5,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  { 1, 101,   0,   0, 2, {{32,  1, 101,  17,   0,  0},
                          {32, 16, 112,  32,   0,  0},
                          {32,  5, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  { 1, 101,   0, 303, 4, {{32,  1, 101,   0, 303, 17},
                          {32, 16, 112,   0, 304, 32},
                          {32,  5, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  { 1, 101, 202,   0, 3, {{32,  1, 101, 202,  17,  0},
                          {32, 16, 112, 208,  32,  0},
                          {32,  5, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  { 1, 101, 202, 303, 4, {{32,  1, 101, 202, 303, 17},
                          {32, 16, 112, 208, 304, 32},
                          {32,  5, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is fully inlined, inline buffer is full
  {48,   0,   0,   0, 1, {{32, 48,  17,   0,   0,  0},
                          {32, 48,  32,   0,   0,  0},
                          {32, 52,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  {48,   0,   0, 303, 4, {{32, 48,   0,   0, 303, 17},
                          {32, 48,   0,   0, 304, 32},
                          {32, 52,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  {48,   0, 202,   0, 3, {{32, 48,   0, 202,  17,  0},
                          {32, 48,   0, 208,  32,  0},
                          {32, 52,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  {48,   0, 202, 303, 4, {{32, 48,   0, 202, 303, 17},
                          {32, 48,   0, 208, 304, 32},
                          {32, 52,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  {48, 101,   0,   0, 2, {{32, 48, 101,  17,   0,  0},
                          {32, 48, 112,  32,   0,  0},
                          {32, 52, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  {48, 101,   0, 303, 4, {{32, 48, 101,   0, 303, 17},
                          {32, 48, 112,   0, 304, 32},
                          {32, 52, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  {48, 101, 202,   0, 3, {{32, 48, 101, 202,  17,  0},
                          {32, 48, 112, 208,  32,  0},
                          {32, 52, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  {48, 101, 202, 303, 4, {{32, 48, 101, 202, 303, 17},
                          {32, 48, 112, 208, 304, 32},
                          {32, 52, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is partially inlined
  {49,   0,   0,   0, 1, {{32, 49,  17,   0,   0,  0},
                          {32, 64,  32,   0,   0,  0},
                          {32, 53,   0,   0,   0,  0},
                          {96, 32,   0,   0,   0,  0}}},
  {49,   0,   0, 303, 4, {{32, 49,   0,   0, 303, 17},
                          {32, 64,   0,   0, 304, 32},
                          {32, 53,   0,   0, 303, 13},
                          {96, 32,   0,   0, 304, 32}}},
  {49,   0, 202,   0, 3, {{32, 49,   0, 202,  17,  0},
                          {32, 64,   0, 208,  32,  0},
                          {32, 53,   0, 202,  13,  0},
                          {96, 32,   0, 208,  32,  0}}},
  {49,   0, 202, 303, 4, {{32, 49,   0, 202, 303, 17},
                          {32, 64,   0, 208, 304, 32},
                          {32, 53,   0, 202, 303, 13},
                          {96, 32,   0, 208, 304, 32}}},
  {49, 101,   0,   0, 2, {{32, 49, 101,  17,   0,  0},
                          {32, 64, 112,  32,   0,  0},
                          {32, 53, 101,  13,   0,  0},
                          {96, 32, 112,  32,   0,  0}}},
  {49, 101,   0, 303, 4, {{32, 49, 101,   0, 303, 17},
                          {32, 64, 112,   0, 304, 32},
                          {32, 53, 101,   0, 303, 13},
                          {96, 32, 112,   0, 304, 32}}},
  {49, 101, 202,   0, 3, {{32, 49, 101, 202,  17,  0},
                          {32, 64, 112, 208,  32,  0},
                          {32, 53, 101, 202,  13,  0},
                          {96, 32, 112, 208,  32,  0}}},
  {49, 101, 202, 303, 4, {{32, 49, 101, 202, 303, 17},
                          {32, 64, 112, 208, 304, 32},
                          {32, 53, 101, 202, 303, 13},
                          {96, 32, 112, 208, 304, 32}}},
};

INSTANTIATE_TEST_SUITE_P(
    RoundTripTests, RoundTripTest, ::testing::Combine(
        ::testing::ValuesIn(round_trip_instances),
        ::testing::ValuesIn(modes)));

class RoundTripPerfTest : public RoundTripTestBase {};

TEST_P(RoundTripPerfTest, DISABLED_Basic) {
  for (int i = 0; i < 100000; i++) {
    auto tx_frame = TestFrame::Encode(m_header, m_front, m_middle, m_data);
    auto onwire_bl = tx_frame.get_buffer(m_tx_frame_asm);

    Tag rx_tag;
    segment_bls_t rx_segment_bls;
    ASSERT_TRUE(disassemble_frame(m_rx_frame_asm, onwire_bl, rx_tag,
                                  rx_segment_bls));
  }
}

static const round_trip_instance_t round_trip_perf_instances[] = {
  {41, 250, 0,       0, 2, {{32, 41, 250, 17,       0,  0},
                            {32, 48, 256, 32,       0,  0},
                            {32, 45, 250, 13,       0,  0},
                            {96,  0, 256, 32,       0,  0}}},
  {41, 250, 0,     512, 4, {{32, 41, 250,  0,     512, 17},
                            {32, 48, 256,  0,     512, 32},
                            {32, 45, 250,  0,     512, 13},
                            {96,  0, 256,  0,     512, 32}}},
  {41, 250, 0,    4096, 4, {{32, 41, 250,  0,    4096, 17},
                            {32, 48, 256,  0,    4096, 32},
                            {32, 45, 250,  0,    4096, 13},
                            {96,  0, 256,  0,    4096, 32}}},
  {41, 250, 0,   32768, 4, {{32, 41, 250,  0,   32768, 17},
                            {32, 48, 256,  0,   32768, 32},
                            {32, 45, 250,  0,   32768, 13},
                            {96,  0, 256,  0,   32768, 32}}},
  {41, 250, 0,  131072, 4, {{32, 41, 250,  0,  131072, 17},
                            {32, 48, 256,  0,  131072, 32},
                            {32, 45, 250,  0,  131072, 13},
                            {96,  0, 256,  0,  131072, 32}}},
  {41, 250, 0, 4194304, 4, {{32, 41, 250,  0, 4194304, 17},
                            {32, 48, 256,  0, 4194304, 32},
                            {32, 45, 250,  0, 4194304, 13},
                            {96,  0, 256,  0, 4194304, 32}}},
};

INSTANTIATE_TEST_SUITE_P(
    RoundTripPerfTests, RoundTripPerfTest, ::testing::Combine(
        ::testing::ValuesIn(round_trip_perf_instances),
        ::testing::ValuesIn(modes)));

}  // namespace ceph::msgr::v2

int main(int argc, char* argv[]) {
  vector<const char*> args;
  argv_to_vec(argc, (const char**)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
