// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>

#include "include/msgr.h"

namespace {

struct SeqCase {
  ceph_seq_t a;
  ceph_seq_t b;
  int expected;
};

class SeqCmp : public testing::TestWithParam<SeqCase> {};

TEST_P(SeqCmp, Order) {
  const auto& param = GetParam();
  // Keep the inputs runtime values so comparisons exercise the optimised code.
  volatile ceph_seq_t a = param.a;
  volatile ceph_seq_t b = param.b;

  EXPECT_EQ(param.expected < 0, ceph_seq_cmp(a, b) < 0);
  EXPECT_EQ(param.expected == 0, ceph_seq_cmp(a, b) == 0);
  EXPECT_EQ(param.expected > 0, ceph_seq_cmp(a, b) > 0);
  EXPECT_EQ(param.expected <= 0, ceph_seq_cmp(a, b) <= 0);
  EXPECT_EQ(param.expected >= 0, ceph_seq_cmp(a, b) >= 0);
}

INSTANTIATE_TEST_SUITE_P(
  Boundary, SeqCmp, testing::Values(
    SeqCase{0, 0, 0},
    SeqCase{1, 0, 1},
    SeqCase{0, 1, -1},
    SeqCase{0x7fffffffU, 0x7ffffffeU, 1},
    SeqCase{0x80000000U, 0x7fffffffU, 1},
    SeqCase{0x7fffffffU, 0x80000000U, -1},
    SeqCase{0x80000001U, 0x80000000U, 1},
    SeqCase{0x80000000U, 0x80000001U, -1},
    SeqCase{0, 0xffffffffU, 1},
    SeqCase{0xffffffffU, 0, -1},
    SeqCase{1, 0xffffffffU, 1},
    SeqCase{0xffffffffU, 1, -1},
    SeqCase{0xffffffffU, 0xffffffffU, 0}));

} // anonymous namespace
