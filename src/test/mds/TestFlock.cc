// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <cstdint>
#include <iterator>
#include <initializer_list>

#include "global/global_context.h"
#include "mds/flock.h"

#include "gtest/gtest.h"

namespace {

struct expected_lock final {
  std::uint64_t start;
  std::uint64_t length;
  std::uint64_t owner;
  std::uint8_t type;
};

ceph_filelock make_lock(std::uint64_t start, std::uint64_t length,
                        std::uint64_t client, std::uint64_t owner,
                        std::uint8_t type)
{
  ceph_filelock result {};
  result.start = start;
  result.length = length;
  result.client = client;
  result.owner = owner | (1ULL << 63);
  result.pid = owner;
  result.type = type;
  return result;
}

void expect_locks(const ceph_lock_state_t& state,
                  std::initializer_list<expected_lock> expected)
{
  ASSERT_EQ(std::size(expected), std::size(state.held_locks));

  auto actual = std::begin(state.held_locks);
  for (const auto& expected_value : expected) {
    ASSERT_NE(std::end(state.held_locks), actual);
    EXPECT_EQ(expected_value.start, actual->second.start);
    EXPECT_EQ(expected_value.length, actual->second.length);
    EXPECT_EQ(expected_value.owner | (1ULL << 63), actual->second.owner);
    EXPECT_EQ(expected_value.type, actual->second.type);
    ++actual;
  }
}

TEST(MDSFlock, CoalescesAndSplitsOwnedRanges)
{
  ceph_lock_state_t state(g_ceph_context, CEPH_LOCK_FLOCK);
  bool deadlock = false;
  auto first = make_lock(10, 10, 1, 1, CEPH_LOCK_SHARED);
  auto adjacent = make_lock(20, 5, 1, 1, CEPH_LOCK_SHARED);

  ASSERT_TRUE(state.add_lock(first, false, false, &deadlock));
  ASSERT_TRUE(state.add_lock(adjacent, false, false, &deadlock));
  expect_locks(state, {{10, 15, 1, CEPH_LOCK_SHARED}});

  auto middle = make_lock(13, 4, 1, 1, CEPH_LOCK_EXCL);
  ASSERT_TRUE(state.add_lock(middle, false, false, &deadlock));
  expect_locks(state, {
    {10, 3, 1, CEPH_LOCK_SHARED},
    {13, 4, 1, CEPH_LOCK_EXCL},
    {17, 8, 1, CEPH_LOCK_SHARED}
  });
}

TEST(MDSFlock, CoalescesRangesAbove32BitOffsets)
{
  ceph_lock_state_t state(g_ceph_context, CEPH_LOCK_FLOCK);
  bool deadlock = false;
  constexpr auto offset = std::uint64_t {1} << 40;
  auto first = make_lock(offset, 10, 1, 1, CEPH_LOCK_SHARED);
  auto overlapping = make_lock(5 + offset, 10, 1, 1, CEPH_LOCK_SHARED);

  ASSERT_TRUE(state.add_lock(first, false, false, &deadlock));
  ASSERT_TRUE(state.add_lock(overlapping, false, false, &deadlock));
  expect_locks(state, {{offset, 15, 1, CEPH_LOCK_SHARED}});
}

TEST(MDSFlock, PreservesBlockingOrder)
{
  ceph_lock_state_t state(g_ceph_context, CEPH_LOCK_FLOCK);
  bool deadlock = false;

  for (auto i = std::uint64_t {6}; 0 < i; --i) {
    const auto index = i - 1;
    auto held = make_lock(10 * index, 5, i, i, CEPH_LOCK_SHARED);
    ASSERT_TRUE(state.add_lock(held, false, false, &deadlock));
  }

  auto query = make_lock(0, 55, 20, 20, CEPH_LOCK_EXCL);
  state.look_for_lock(query);
  EXPECT_EQ(0, query.start);
  EXPECT_EQ(1 | (1ULL << 63), query.owner);
}

TEST(MDSFlock, RetainsBlockedWaiters)
{
  ceph_lock_state_t state(g_ceph_context, CEPH_LOCK_FLOCK);
  bool deadlock = false;
  auto held = make_lock(0, 0, 1, 1, CEPH_LOCK_EXCL);
  auto waiting = make_lock(20, 4, 2, 2, CEPH_LOCK_SHARED);

  ASSERT_TRUE(state.add_lock(held, false, false, &deadlock));
  EXPECT_FALSE(state.add_lock(waiting, true, false, &deadlock));
  EXPECT_FALSE(deadlock);
  EXPECT_TRUE(state.is_waiting(waiting));
  EXPECT_EQ(1, std::size(state.waiting_locks));
}

TEST(MDSFlock, SplitsAndTruncatesRemovedRanges)
{
  ceph_lock_state_t state(g_ceph_context, CEPH_LOCK_FLOCK);
  bool deadlock = false;
  auto held = make_lock(0, 100, 1, 1, CEPH_LOCK_SHARED);
  ASSERT_TRUE(state.add_lock(held, false, false, &deadlock));

  auto middle = make_lock(40, 20, 1, 1, CEPH_LOCK_UNLOCK);
  state.remove_lock(middle);
  expect_locks(state, {
    {0, 40, 1, CEPH_LOCK_SHARED},
    {60, 40, 1, CEPH_LOCK_SHARED}
  });

  auto tail = make_lock(60, 0, 1, 1, CEPH_LOCK_UNLOCK);
  state.remove_lock(tail);
  expect_locks(state, {{0, 40, 1, CEPH_LOCK_SHARED}});
}

} // namespace
