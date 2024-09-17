// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "reshard_writer.h"

#include <functional>
#include <optional>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>

namespace rgwrados::reshard {

template <typename T>
auto capture(std::optional<T>& val)
{
  return [&val] (T value) { val = std::move(value); };
}

template <typename Batch>
auto call_write(Writer<Batch>& writer)
{
  return [&writer] (boost::asio::yield_context yield) {
    writer.write({}, {}, {}, yield);
  };
}

template <typename Batch>
auto call_drain(Writer<Batch>& writer)
{
  return [&writer] (boost::asio::yield_context yield) {
    writer.drain(yield);
  };
}

// mock Batch captures flush() completions for test cases to invoke manually
class MockBatch {
 public:
  MockBatch(size_t batch_size, std::vector<Completion>& completions)
    : batch_size(batch_size), completions(completions)
  {}

  bool empty() const
  {
    return count == 0;
  }

  bool add(rgw_cls_bi_entry entry,
           std::optional<RGWObjCategory> category,
           rgw_bucket_category_stats stats)
  {
    ++count;
    return count == batch_size;
  }

  void flush(Completion completion)
  {
    completions.push_back(std::move(completion));
    count = 0;
  }

 private:
  const size_t batch_size;
  size_t count = 0;
  std::vector<Completion>& completions;
};

TEST(Writer, empty_flush)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  ASSERT_TRUE(batch.empty());
  auto writer = Writer{ex, 1, batch};

  writer.flush();
  EXPECT_TRUE(completions.empty());
}

TEST(Writer, empty_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  ASSERT_TRUE(batch.empty());
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result;
  boost::asio::spawn(ex, call_drain(writer), capture(result));
  EXPECT_FALSE(result);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_TRUE(completions.empty());
}

TEST(Writer, write_no_flush)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{8, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result;
  boost::asio::spawn(ex, call_write(writer), capture(result));
  EXPECT_FALSE(result);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_TRUE(completions.empty());
}

TEST(Writer, write_no_flush_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{8, completions};
  auto writer = Writer{ex, 1, batch};

  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_write(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());
    ctx.restart();

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
    EXPECT_TRUE(completions.empty());
  }
  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_drain(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());
    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
    EXPECT_TRUE(completions.empty());
  }
}

TEST(Writer, write_flush)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{8, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result;
  boost::asio::spawn(ex, call_write(writer), capture(result));
  EXPECT_FALSE(result);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ctx.restart();

  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);

  writer.flush();

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  EXPECT_EQ(1, completions.size());
}

TEST(Writer, write_flush_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{8, completions};
  auto writer = Writer{ex, 1, batch};

  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_write(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());
    ctx.restart();

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
  }

  writer.flush();
  ASSERT_EQ(1, completions.size());

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_drain(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_FALSE(ctx.stopped());

    std::invoke(completions[0], boost::system::error_code{});

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
  }
}

TEST(Writer, write_batch)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result;
  boost::asio::spawn(ex, call_write(writer), capture(result));
  EXPECT_FALSE(result);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_EQ(1, completions.size());
}

TEST(Writer, write_batch_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_write(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_FALSE(ctx.stopped());

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
    EXPECT_EQ(1, completions.size());
  }
  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_drain(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_FALSE(ctx.stopped());

    std::invoke(completions[0], boost::system::error_code{});

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
  }
}

TEST(Writer, write_batch_multi_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  {
    std::optional<std::exception_ptr> result;
    boost::asio::spawn(ex, call_write(writer), capture(result));
    EXPECT_FALSE(result);

    ctx.poll();
    EXPECT_FALSE(ctx.stopped());

    ASSERT_TRUE(result);
    EXPECT_FALSE(*result);
    EXPECT_EQ(1, completions.size());
  }
  {
    std::optional<std::exception_ptr> result1;
    boost::asio::spawn(ex, call_drain(writer), capture(result1));
    EXPECT_FALSE(result1);

    std::optional<std::exception_ptr> result2;
    boost::asio::spawn(ex, call_drain(writer), capture(result2));
    EXPECT_FALSE(result2);

    ctx.poll();
    EXPECT_FALSE(ctx.stopped());

    std::invoke(completions[0], boost::system::error_code{});

    ctx.poll();
    EXPECT_TRUE(ctx.stopped());

    ASSERT_TRUE(result1);
    EXPECT_FALSE(*result1);
    ASSERT_TRUE(result2);
    EXPECT_FALSE(*result2);
  }
}

TEST(Writer, multi_write_batch)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_write(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  std::optional<std::exception_ptr> result3;
  boost::asio::spawn(ex, call_write(writer), capture(result3));
  EXPECT_FALSE(result3);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  EXPECT_FALSE(result2);
  ASSERT_EQ(1, completions.size());
  std::invoke(completions[0], boost::system::error_code{});

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result2);
  EXPECT_FALSE(*result2);
  EXPECT_FALSE(result3);
  ASSERT_EQ(2, completions.size());
  std::invoke(completions[1], boost::system::error_code{});

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result3);
  EXPECT_FALSE(*result3);
  ASSERT_EQ(3, completions.size());
  std::invoke(completions[2], boost::system::error_code{});

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
}

TEST(Writer, concurrent_multi_write_batch)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 2, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_write(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result2);
  EXPECT_FALSE(*result2);
  EXPECT_EQ(2, completions.size());

  std::optional<std::exception_ptr> result3;
  boost::asio::spawn(ex, call_write(writer), capture(result3));
  EXPECT_FALSE(result3);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  std::optional<std::exception_ptr> result4;
  boost::asio::spawn(ex, call_write(writer), capture(result4));
  EXPECT_FALSE(result4);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  EXPECT_FALSE(result3);
  ASSERT_EQ(2, completions.size());
  std::invoke(completions[0], boost::system::error_code{});

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result3);
  EXPECT_FALSE(*result3);
  EXPECT_FALSE(result4);
  ASSERT_EQ(3, completions.size());
  std::invoke(completions[1], boost::system::error_code{});

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result4);
  EXPECT_FALSE(*result4);
  ASSERT_EQ(4, completions.size());
  std::invoke(completions[2], boost::system::error_code{});

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_EQ(4, completions.size());
  std::invoke(completions[3], boost::system::error_code{});

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
}

TEST(Writer, write_batch_write_error)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_write(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());
  std::invoke(completions[0], make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ctx.restart();

  ASSERT_TRUE(result2);
  ASSERT_TRUE(*result2);
  try {
    std::rethrow_exception(*result2);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }

  std::optional<std::exception_ptr> result3;
  boost::asio::spawn(ex, call_write(writer), capture(result3));
  EXPECT_FALSE(result3);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result3);
  ASSERT_TRUE(*result3);
  try {
    std::rethrow_exception(*result3);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(Writer, write_batch_error_write)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());
  std::invoke(completions[0], make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ctx.restart();

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_write(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result2);
  ASSERT_TRUE(*result2);
  try {
    std::rethrow_exception(*result2);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(Writer, write_batch_drain_error)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_drain(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());
  std::invoke(completions[0], make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ctx.restart();

  ASSERT_TRUE(result2);
  ASSERT_TRUE(*result2);
  try {
    std::rethrow_exception(*result2);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }

  std::optional<std::exception_ptr> result3;
  boost::asio::spawn(ex, call_write(writer), capture(result3));
  EXPECT_FALSE(result3);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result3);
  ASSERT_TRUE(*result3);
  try {
    std::rethrow_exception(*result3);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(Writer, write_batch_error_drain)
{
  boost::asio::io_context ctx;
  boost::asio::any_io_executor ex = ctx.get_executor();

  std::vector<Completion> completions;
  auto batch = MockBatch{1, completions};
  auto writer = Writer{ex, 1, batch};

  std::optional<std::exception_ptr> result1;
  boost::asio::spawn(ex, call_write(writer), capture(result1));
  EXPECT_FALSE(result1);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());

  ASSERT_TRUE(result1);
  EXPECT_FALSE(*result1);
  EXPECT_EQ(1, completions.size());
  std::invoke(completions[0], make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ctx.restart();

  std::optional<std::exception_ptr> result2;
  boost::asio::spawn(ex, call_drain(writer), capture(result2));
  EXPECT_FALSE(result2);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());

  ASSERT_TRUE(result2);
  ASSERT_TRUE(*result2);
  try {
    std::rethrow_exception(*result2);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

} // namespace rgwrados::reshard
