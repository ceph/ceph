// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include "neorados/cls/sem_set.h"

#include <boost/system/detail/errc.hpp>
#include <coroutine>
#include <string>

#include <boost/system/errc.hpp>

#include <fmt/format.h>

#include "gtest/gtest.h"

#include "include/neorados/RADOS.hpp"

#include "test/neorados/common_tests.h"

namespace sem_set = neorados::cls::sem_set;
namespace sys = boost::system;
namespace container = boost::container;

using neorados::ReadOp;
using neorados::WriteOp;

using namespace std::literals;

/// Increment semaphore multiple times. Decrement the same number of
/// times. Decrement once more and expect an error.
CORO_TEST_F(cls_sem_set, inc_dec, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(oid);

  auto key = "foo";
  for (auto i = 0; i < 10; ++i) {
    co_await execute(oid, WriteOp{}.exec(sem_set::increment(key)));
  }
  for (auto i = 0; i < 10; ++i) {
    co_await execute(oid, WriteOp{}.exec(sem_set::decrement(key)));
  }

  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(sem_set::decrement(key))),
    sys::errc::no_such_file_or_directory);
  co_return;
}

// TODO: Uncomment when we get a GCC everywhere that doesn't crash
// when compiling it.

// /// Increment semaphore multiple times. Decrement the same number of
// /// times. Decrement once more and expect an error. But this time use
// /// the initializer list definition and two keys.
// CORO_TEST_F(cls_sem_set, inc_dec_init_list, NeoRadosTest)
// {
//   std::string_view oid = "obj";
//   co_await create_obj(oid);

//   auto key1 = "foo";
//   auto key2 = "bar";
//   for (auto i = 0; i < 10; ++i) {
//     co_await execute(oid, WriteOp{}.exec(sem_set::increment({key1, key2})));
//   }
//   for (auto i = 0; i < 10; ++i) {
//     co_await execute(oid, WriteOp{}.exec(sem_set::decrement({key1, key2})));
//   }

//   co_await expect_error_code(
//     execute(oid, WriteOp{}.exec(sem_set::decrement({key1, key2}))),
//     sys::errc::no_such_file_or_directory);
//   co_return;
// }

/// Send too many keys to increment, expecting an error. Then send too
/// many keys to decrement, expecting an error.
CORO_TEST_F(cls_sem_set, inc_dec_overflow, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(oid);

  container::flat_set<std::string> strings;
  for (auto i = 0u; i < 2 * sem_set::max_keys; ++i) {
    strings.insert(fmt::format("key{}", i));
  }

  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(sem_set::increment(strings.begin(),
                                                   strings.end()))),
    sys::errc::argument_list_too_long);
  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(sem_set::decrement(strings.begin(),
                                                   strings.end()))),
    sys::errc::argument_list_too_long);
}


/// Write the maximum number of keys we can return in one listing. Do
/// one listing and ensure that the returned cursor is empty (thus
/// showing list returned all values.) Repeat twice more to give
/// coverage to the other two definitions of list.
CORO_TEST_F(cls_sem_set, list_small, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(oid);
  const auto max = sem_set::max_keys;

  container::flat_set<std::string> ref;
  for (auto i = 0u; i < max; ++i) {
    ref.insert(fmt::format("key{}", i));
  }
  co_await execute(oid, WriteOp{}.exec(sem_set::increment(ref.begin(),
                                                          ref.end())));
  {
    container::flat_map<std::string, std::uint64_t> res;
    std::string cursor;
    co_await execute(oid, ReadOp{}.exec(sem_set::list(max, {},
                                                      &res, &cursor)));
    EXPECT_TRUE(cursor.empty());
    for (const auto& [key, value] : res) {
      EXPECT_TRUE(ref.contains(key));
    }
    for (const auto& key : ref) {
      EXPECT_TRUE(res.contains(key));
    }
  }
  {
    std::map<std::string, std::uint64_t> res;
    std::string cursor;
    co_await execute(oid, ReadOp{}.exec(
                       sem_set::list(max, {},
                                     std::inserter(res, res.end()), &cursor)));
    EXPECT_TRUE(cursor.empty());
    for (const auto& [key, value] : res) {
      EXPECT_TRUE(ref.contains(key));
    }
    for (const auto& key : ref) {
      EXPECT_TRUE(res.contains(key));
    }
  }
  {
    std::vector<std::pair<std::string, std::uint64_t>> res;
    std::string cursor;
    co_await execute(oid, ReadOp{}.exec(
                       sem_set::list(max, {},
                                     std::inserter(res, res.end()), &cursor)));
    EXPECT_TRUE(cursor.empty());
    for (const auto& [key, value] : res) {
      EXPECT_TRUE(ref.contains(key));
    }
    EXPECT_EQ(ref.size(), res.size());
  }
}

/// Write the maximum number of keys we can return in one listing
/// several times.  Do an iterated listing to test cursor
/// functionality and append. Check that we have all of and only the
/// keys we should. Repeat for the other two list functions.
CORO_TEST_F(cls_sem_set, list_large, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(oid);
  const auto max = sem_set::max_keys;

  container::flat_set<std::string> ref;
  for (auto i = 0u; i < 4; ++i) {
    container::flat_set<std::string> part;
    for (auto j = 0u; j < max; ++j) {
      part.insert(fmt::format("key{}", (i * max) + j));
    }
    co_await execute(oid, WriteOp{}.exec(sem_set::increment(part.begin(),
                                                            part.end())));
    ref.merge(std::move(part));
  }
  EXPECT_EQ(4 * max, ref.size());
  std::string cursor;

  container::flat_map<std::string, std::uint64_t> res_um;
  do {
    co_await execute(oid, ReadOp{}.exec(
                       sem_set::list(max, cursor, &res_um, &cursor)));
  } while (!cursor.empty());
  for (const auto& [key, value] : res_um) {
    EXPECT_TRUE(ref.contains(key));
  }
  for (const auto& key : ref) {
    EXPECT_TRUE(res_um.contains(key));
  }

  std::map<std::string, std::uint64_t> res_m;
  cursor.clear();
  do {
    co_await execute(oid, ReadOp{}.exec(
                       sem_set::list(max, cursor,
                                     std::inserter(res_m, res_m.end()),
                                     &cursor)));
  } while (!cursor.empty());
  for (const auto& [key, value] : res_m) {
    EXPECT_TRUE(ref.contains(key));
  }
  for (const auto& key : ref) {
    EXPECT_TRUE(res_m.contains(key));
  }

  std::vector<std::pair<std::string, std::uint64_t>> res_v;
  cursor.clear();
  do {
    co_await execute(oid, ReadOp{}.exec(
                       sem_set::list(max, cursor,
                                     std::inserter(res_v, res_v.end()),
                                     &cursor)));
  } while (!cursor.empty());
  for (const auto& [key, value] : res_v) {
    EXPECT_TRUE(ref.contains(key));
  }
  EXPECT_EQ(ref.size(), res_v.size());
}

// Increment some semaphores, wait, increment some more, decrement
// them all with a time.
CORO_TEST_F(cls_sem_set, inc_dec_time, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(oid);

  const auto lose = "lose"s;
  const auto remain = "remain"s;

  co_await execute(oid, WriteOp{}.exec(sem_set::increment(lose)));
  co_await execute(oid, WriteOp{}.exec(sem_set::increment(lose)));
  co_await execute(oid, WriteOp{}.exec(sem_set::increment(remain)));
  co_await execute(oid, WriteOp{}.exec(sem_set::increment(remain)));

  co_await execute(oid, WriteOp{}.exec(sem_set::decrement(lose)));

  co_await wait_for(1s);
  co_await execute(oid, WriteOp{}.exec(sem_set::decrement(remain)));

  container::flat_set<std::string> keys{lose, remain};
  co_await execute(oid,
		   WriteOp{}.exec(sem_set::decrement(std::move(keys), 100ms)));

  container::flat_map<std::string, std::uint64_t> res;
  std::string cursor;
  co_await execute(oid, ReadOp{}.exec(sem_set::list(sem_set::max_keys, {},
						    &res, &cursor)));

  EXPECT_TRUE(cursor.empty());
  EXPECT_EQ(1u, res.size());
  EXPECT_EQ(remain, res.begin()->first);

  co_return;
}
