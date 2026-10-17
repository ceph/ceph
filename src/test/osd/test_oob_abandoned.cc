// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The bookkeeping of the cuObject executor (src/osd/osd_cuobj.cc) for
 * writes that a plan left posted when it timed out: their completions
 * come back while a later plan polls the channel, and each must count
 * against the plan that posted it, once, and give that plan's staging
 * buffer back only with its last write.
 */

#include "osd/oob_abandoned.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using plans_t = abandoned_plans<std::string>;
using credit_t = plans_t::credit_t;

namespace {

/// a plan's writes when it stopped waiting, one character each: 1 for a
/// write still posted, 0 for one that completed
std::vector<bool> writes(const std::string& s)
{
  std::vector<bool> posted;
  for (const char c : s) {
    posted.push_back(c == '1');
  }
  return posted;
}

} // anonymous namespace

TEST(OobAbandoned, OwnsHandle)
{
  EXPECT_FALSE(owns_handle(10, 3, 9));
  EXPECT_TRUE(owns_handle(10, 3, 10));
  EXPECT_TRUE(owns_handle(10, 3, 12));
  EXPECT_FALSE(owns_handle(10, 3, 13));
  // a handle below the range must not wrap into it
  EXPECT_FALSE(owns_handle(10, 3, 0));
  EXPECT_FALSE(owns_handle(1, 0, 1));
  const uint64_t top = std::numeric_limits<uint64_t>::max();
  EXPECT_TRUE(owns_handle(top - 1, 2, top));
  EXPECT_FALSE(owns_handle(top - 1, 1, top));
}

TEST(OobAbandoned, StillPosted)
{
  const auto posted = writes("0110");
  EXPECT_FALSE(still_posted(10, posted, 9));
  EXPECT_FALSE(still_posted(10, posted, 10));
  EXPECT_TRUE(still_posted(10, posted, 11));
  EXPECT_TRUE(still_posted(10, posted, 12));
  EXPECT_FALSE(still_posted(10, posted, 13));
  EXPECT_FALSE(still_posted(10, posted, 14));
  EXPECT_FALSE(still_posted(10, posted, 0));
  EXPECT_FALSE(still_posted(1, {}, 1));
}

TEST(OobAbandoned, CreditsThePlanThatPosted)
{
  plans_t plans;
  EXPECT_TRUE(plans.empty());
  plans.abandon(1, writes("0101"), "a");  // handles 1..4, 2 and 4 posted
  plans.abandon(5, writes("010"), "b");   // handles 5..7, 6 posted
  ASSERT_EQ(2u, plans.size());

  std::string buf;
  EXPECT_EQ(credit_t::last, plans.credit(6, &buf));
  EXPECT_EQ("b", buf);
  EXPECT_EQ(1u, plans.size());

  buf.clear();
  EXPECT_EQ(credit_t::counted, plans.credit(2, &buf));
  EXPECT_TRUE(buf.empty());  // a's buffer stays until its last write
  EXPECT_EQ(credit_t::last, plans.credit(4, &buf));
  EXPECT_EQ("a", buf);
  EXPECT_TRUE(plans.empty());
}

// The case that once delivered short data: a plan timed out with all
// 16 writes posted, and the next plan on the channel polled completions
// of both (run_plan() now stages nothing while such writes are posted).
// The next plan counts only the writes that it still waits for, as
// run_plan() does, and the earlier plan's buffer comes back only with
// its last write.
TEST(OobAbandoned, NextPlanCountsOnlyItsOwn)
{
  constexpr uint64_t BATCH = 16;
  plans_t plans;
  const uint64_t a_first = 1;
  plans.abandon(a_first, std::vector<bool>(BATCH, true), "a");
  const uint64_t b_first = a_first + BATCH;
  std::vector<bool> b_posted(BATCH, true);

  // all of a's completions, with 4 of b's among them, and b's first
  // once more, as from a library that returned it twice
  std::vector<uint64_t> polled;
  for (uint64_t i = 0; i < BATCH; i++) {
    polled.push_back(a_first + i);
    if (i % 4 == 0) {
      polled.push_back(b_first + i / 4);
    }
    if (i == BATCH / 2) {
      polled.push_back(b_first);
    }
  }
  uint64_t b_completed = 0;
  uint64_t a_completed = 0;
  int reclaimed = 0;
  for (const auto h : polled) {
    if (still_posted(b_first, b_posted, h)) {
      b_posted[h - b_first] = false;
      b_completed++;
      continue;
    }
    std::string buf;
    const auto c = plans.credit(h, &buf);
    if (owns_handle(b_first, BATCH, h)) {
      // the repeat: no plan here posted it, and a's count is untouched
      EXPECT_EQ(credit_t::unknown, c);
      continue;
    }
    a_completed++;
    if (c == credit_t::last) {
      EXPECT_EQ(BATCH, a_completed);
      EXPECT_EQ("a", buf);
      reclaimed++;
    }
  }
  EXPECT_EQ(4u, b_completed);
  EXPECT_EQ(1, reclaimed);
  EXPECT_TRUE(plans.empty());
}

// a completion of a write that completed before the plan stopped
// waiting, as from a library that returned one twice: it counts for
// nothing, and the plan's buffer comes back only with its last write
// still posted
TEST(OobAbandoned, RepeatedCompletionCountsOnce)
{
  plans_t plans;
  plans.abandon(100, writes("0011"), "a");  // 100 and 101 completed
  plans.abandon(104, writes("1111"), "b");
  std::string buf;
  EXPECT_EQ(credit_t::repeated, plans.credit(101, &buf));
  EXPECT_EQ(credit_t::counted, plans.credit(102, &buf));
  EXPECT_EQ(credit_t::last, plans.credit(103, &buf));
  EXPECT_EQ("a", buf);
  buf.clear();
  // a's plan is gone: one more finds none, and no count wraps
  EXPECT_EQ(credit_t::unknown, plans.credit(101, &buf));
  EXPECT_TRUE(buf.empty());
  // b is untouched
  ASSERT_EQ(1u, plans.size());
  for (uint64_t h = 104; h < 107; h++) {
    EXPECT_EQ(credit_t::counted, plans.credit(h, &buf));
  }
  EXPECT_EQ(credit_t::last, plans.credit(107, &buf));
  EXPECT_EQ("b", buf);
}

// a write still posted whose completion comes back twice before the
// plan's last write completes: the repeat must not stand for the last
// write, which would hand the buffer back while that write may still
// read it
TEST(OobAbandoned, EarlyRepeatKeepsTheBuffer)
{
  plans_t plans;
  plans.abandon(100, writes("0011"), "a");
  std::string buf;
  EXPECT_EQ(credit_t::counted, plans.credit(102, &buf));
  EXPECT_EQ(credit_t::repeated, plans.credit(102, &buf));
  EXPECT_TRUE(buf.empty());
  ASSERT_EQ(1u, plans.size());
  EXPECT_EQ(credit_t::last, plans.credit(103, &buf));
  EXPECT_EQ("a", buf);
  EXPECT_TRUE(plans.empty());
}

TEST(OobAbandoned, UnknownHandle)
{
  plans_t plans;
  std::string buf = "untouched";
  EXPECT_EQ(credit_t::unknown, plans.credit(1, &buf));
  plans.abandon(10, writes("11"), "a");
  EXPECT_EQ(credit_t::unknown, plans.credit(9, &buf));
  EXPECT_EQ(credit_t::unknown, plans.credit(12, &buf));
  EXPECT_EQ("untouched", buf);
  EXPECT_EQ(1u, plans.size());
}

// the transport reset the channel: every buffer comes back at once,
// and a completion that still turns up afterwards finds no plan
TEST(OobAbandoned, DropReturnsEveryBuffer)
{
  plans_t plans;
  EXPECT_TRUE(plans.drop().empty());
  plans.abandon(1, writes("11"), "a");
  plans.abandon(3, writes("00001"), "b");
  auto bufs = plans.drop();
  ASSERT_EQ(2u, bufs.size());
  EXPECT_EQ("a", bufs[0]);
  EXPECT_EQ("b", bufs[1]);
  EXPECT_TRUE(plans.empty());
  std::string buf;
  EXPECT_EQ(credit_t::unknown, plans.credit(1, &buf));
  EXPECT_EQ(credit_t::unknown, plans.credit(7, &buf));
}

// a buffer is held by one owner at a time: the bookkeeping moves it
TEST(OobAbandoned, MoveOnlyBuffer)
{
  abandoned_plans<std::unique_ptr<int>> plans;
  plans.abandon(1, {true}, std::make_unique<int>(7));
  plans.abandon(2, {true}, std::make_unique<int>(8));
  std::unique_ptr<int> buf;
  EXPECT_EQ(decltype(plans)::credit_t::last, plans.credit(1, &buf));
  ASSERT_TRUE(buf);
  EXPECT_EQ(7, *buf);
  auto rest = plans.drop();
  ASSERT_EQ(1u, rest.size());
  EXPECT_EQ(8, *rest[0]);
}
