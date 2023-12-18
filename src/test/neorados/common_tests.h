// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <exception>
#include <initializer_list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/dout.h"

#include "gtest/gtest.h"

/// \file test/neorados/common_tests.h
///
/// \brief Tools for testing neorados code
///
/// This is a set of utilities for testing code using the neorados
/// library, as well as for tests using C++20 Coroutines more generally.

/// \brief Get a random, unique pool name
///
/// Return a uniquified pool name specific to the host on which we are running.
///
/// \param prefix A prefix for the returned pool name
///
/// \return A unique pool name
std::string get_temp_pool_name(std::string_view prefix = {});

/// \brief Create a RADOS pool
///
/// Create a RADOS pool, returning its ID on success.
///
/// \param r RADOS handle
/// \param pname Pool name
/// \param token Boost.Asio completion token
///
/// \return The ID of the newly created pool
template<boost::asio::completion_token_for<
	   void(boost::system::error_code, int64_t)> CompletionToken>
auto create_pool(neorados::RADOS& r,
		 std::string pname,
		 CompletionToken&& token)
{
  namespace asio = boost::asio;
  using boost::system::error_code;
  using boost::system::system_error;

  return asio::async_initiate<CompletionToken, void(error_code, int64_t)>
    (asio::experimental::co_composed<void(error_code, int64_t)>
     ([](auto state, neorados::RADOS& r, std::string pname) -> void {
       try {
	 co_await r.create_pool(pname, std::nullopt, asio::deferred);
	 auto pool = co_await r.lookup_pool(pname, asio::deferred);
	 co_return {error_code{}, pool};
       } catch (const system_error& e) {
	 co_return {e.code(), int64_t{}};
       }
     }, r.get_executor()),
     token, std::ref(r), std::move(pname));
}

/// \brief Create a new, empty RADOS object
///
/// \param r RADOS handle
/// \param oid Object name
/// \param ioc Locator
/// \param token Boost.Asio completion token
template<boost::asio::completion_token_for<
  void(boost::system::error_code)> CompletionToken>
auto create_obj(neorados::RADOS& r, std::string_view oid,
		const neorados::IOContext& ioc,
		CompletionToken&& token)
{
  neorados::WriteOp op;
  op.create(true);
  return r.execute(oid, ioc, std::move(op),
		   std::forward<CompletionToken>(token));
}

/// \brief Expect one of several errors from a coroutine
///
/// \param coro Awaitable coroutine
/// \param ec Valid errors
boost::asio::awaitable<void>
expect_error_code(auto&& coro, auto ...ecs) {
  bool failed = false;
  try {
    co_await std::move(coro);
  } catch (const boost::system::system_error& e) {
    failed = true;
    auto h = [c = e.code()](auto t) -> bool { return t == c; };
    EXPECT_TRUE((h(ecs) || ...))
      << "Got unexpected error code " << e.code().message() << ".";
  }
  EXPECT_TRUE(failed) << "Operation did not error at all.";
  co_return;
}

/// \brief Test harness for C++20 Coroutines
///
/// C++20 coroutines are better than what we had before, but don't
/// play well with RAII. There's no good way to run a coroutine from a
/// destructor, especially in a single-threaded, non-blocking
/// program.
///
/// To be fair to C++20, this is difficult and even rust doesn't have
/// async drop yet.
///
/// GTest has explicit SetUp and TearDown methods, however they're
/// just regular functions. So we get Coroutine analogues of SetUp and
/// TearDown that we then call from our custom TestBody. The user
/// writes their tests in CoTestBody.
class CoroTest : public testing::Test {
private:
  std::exception_ptr eptr;
protected:
  boost::asio::io_context asio_context; ///< The context on which the
					///  coroutine runs.
public:
  /// Final override that does nothing. Actual setup code should go in CoSetUp
  void SetUp() override final { };
  /// Final override that does nothing. Actual teardown code should go
  /// in CotearDown.
  void TearDown() override final { };

  /// \brief SetUp coroutine
  ///
  /// Called before the test body. Indicate failure by throwing
  /// an exception. If an exception is thrown, neither the test body
  /// nor teardown code are run.
  virtual boost::asio::awaitable<void> CoSetUp() {
    co_return;
  }

  /// \brief TearDown coroutine
  ///
  /// Called after the test body exits.
  ///
  /// \note This function is not run if CoSetup fails
  virtual boost::asio::awaitable<void> CoTearDown() {
    co_return;
  }

  /// \brief TestBody coroutine
  ///
  /// Run after setup.
  virtual boost::asio::awaitable<void> CoTestBody() = 0;

  /// \brief Run our coroutines
  ///
  /// This is marked final, since the actual test body belongs in
  /// CoTestBody.
  ///
  /// Run CoSetUp and, if CoSetUp succeeded, CoTestBody and
  /// CoTearDown.
  ///
  /// Error reporting of failures in CoSetUp and CoTearDown leaves
  /// something to be desired as GTest thinks everything is the test
  /// proper.
  void TestBody() override final {
    boost::asio::co_spawn(
      asio_context,
      [](CoroTest* t) -> boost::asio::awaitable<void> {
	co_await t->CoSetUp();
	try {
	  co_await t->CoTestBody();
	} catch (...) {
	  t->eptr = std::current_exception();
	}
	co_await t->CoTearDown();
	if (t->eptr) {
	  std::rethrow_exception(t->eptr);
	}
	co_return;
      }(this),
      [](std::exception_ptr e) {
	if (e) std::rethrow_exception(e);
      });
    asio_context.run();
  }
};

/// \brief C++20 coroutine test harness for NeoRados
///
/// CoTestBody has access to `rados`, a `neorados::RADOS` handle, and
/// `pool`, a `neorados::IOContext` representing a pool that will be
/// destroyed when the test exits.
///
/// Derived classes must define `create_pool()` and `clean_pool()`.
class NeoRadosTestBase : public CoroTest {
private:
  const std::string prefix_{std::string{"test framework "} +
			    testing::UnitTest::GetInstance()->
			    current_test_info()->name() +
			    std::string{": "}};

  std::optional<neorados::RADOS> rados_;
  neorados::IOContext pool_;
  const std::string pool_name_ = get_temp_pool_name(
    testing::UnitTest::GetInstance()->current_test_info()->name());
  std::unique_ptr<DoutPrefix> dpp_;

  virtual boost::asio::awaitable<uint64_t> create_pool() = 0;
  virtual boost::asio::awaitable<void> clean_pool() = 0;

protected:

  /// \brief Return reference to RADOS
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  neorados::RADOS& rados() noexcept { return *rados_; }

  /// \brief Return name of created pool
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  const std::string& pool_name() const noexcept { return pool_name_; }

  /// \brief Return reference to pool
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  const neorados::IOContext& pool() const noexcept { return pool_; }

  /// \brief Return prefix for this test run
  std::string_view prefix() const noexcept { return prefix_; }

  /// \brief Return DoutPrefixProvider*
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  const DoutPrefixProvider* dpp() const noexcept { return dpp_.get(); }

  auto execute(std::string_view oid, neorados::WriteOp&& op,
	       std::uint64_t* ver = nullptr) {
    return rados().execute(oid, pool(), std::move(op),
			   boost::asio::use_awaitable, ver);
  }

  auto execute(std::string_view oid, neorados::ReadOp&& op,
	       std::uint64_t* ver = nullptr) {
    return rados().execute(oid, pool(), std::move(op), nullptr,
			   boost::asio::use_awaitable, ver);
  }

  auto execute(std::string_view oid, neorados::WriteOp&& op,
	       neorados::IOContext ioc, std::uint64_t* ver = nullptr) {
    return rados().execute(oid, std::move(ioc), std::move(op),
			   boost::asio::use_awaitable, ver);
  }

  auto execute(std::string_view oid, neorados::ReadOp&& op,
	       neorados::IOContext ioc, std::uint64_t* ver = nullptr) {
    return rados().execute(oid, std::move(ioc), std::move(op), nullptr,
			   boost::asio::use_awaitable, ver);
  }

  boost::asio::awaitable<ceph::buffer::list>
  read(std::string_view oid, std::uint64_t off = 0, std::uint64_t len = 0) {
    ceph::buffer::list bl;
    neorados::ReadOp op;
    op.read(off, len, &bl);
    co_await rados().execute(oid, pool(), std::move(op),
			     nullptr, boost::asio::use_awaitable);
    co_return bl;
  }

  boost::asio::awaitable<ceph::buffer::list>
  read(std::string_view oid, neorados::IOContext ioc, std::uint64_t off = 0,
       std::uint64_t len = 0) {
    ceph::buffer::list bl;
    neorados::ReadOp op;
    op.read(off, len, &bl);
    co_await rados().execute(oid, std::move(ioc), std::move(op),
			     nullptr, boost::asio::use_awaitable);
    co_return bl;
  }

  boost::asio::awaitable<void>
  create_obj(std::string_view oid) {
    neorados::WriteOp op;
    op.create(true);
    co_return co_await rados().execute(oid, pool(), std::move(op),
				       boost::asio::use_awaitable);
  }

public:

  /// \brief Create RADOS handle and pool for the test
  boost::asio::awaitable<void> CoSetUp() override {
    rados_ = co_await neorados::RADOS::Builder{}
      .build(asio_context, boost::asio::use_awaitable);
    dpp_ = std::make_unique<DoutPrefix>(rados().cct(), 0, prefix().data());
    pool_.set_pool(co_await create_pool());
    co_return;
  }

  ~NeoRadosTestBase() override = default;

  /// \brief Delete pool used for testing
  boost::asio::awaitable<void> CoTearDown() override {
    co_await clean_pool();
    co_return;
  }
};

/// \brief C++20 coroutine test harness for NeoRados on normal pools
///
/// The supplied pool is not erasure coded.
class NeoRadosTest : public NeoRadosTestBase {
private:
  boost::asio::awaitable<uint64_t> create_pool() override {
    co_return co_await ::create_pool(rados(), pool_name(),
				     boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> clean_pool() override {
    co_await rados().delete_pool(pool().get_pool(),
				boost::asio::use_awaitable);
  }
};

/// \brief C++20 coroutine test harness for NeoRados on erasure-coded
/// pools
///
/// The supplied pool is erasure coded
class NeoRadosECTest : public NeoRadosTestBase {
private:
  boost::asio::awaitable<uint64_t> create_pool() override;
  boost::asio::awaitable<void> clean_pool() override;
};

/// \brief Helper macro for defining coroutine tests with a fixture
///
/// Defines a test using a coroutine fixture for
/// SetUp/TearDown. Fixtures must be descendants of `CoroTest`.
///
/// \note Uses more of GTest's internals that I would like.
///
/// \warning Use `EXPECT_*` only, not `ASSERT_*`. `ASSERT_` macros
/// return from the calling function and will not work in a
/// coroutine.
///
/// \param test_suite_name Name of the test suite
/// \param test_name Name of the test
/// \param fixture Fixture class to use (descendent of CoroTest)
#define CORO_TEST_F(test_suite_name, test_name, fixture)                       \
  static_assert(sizeof(GTEST_STRINGIFY_(test_suite_name)) > 1,                 \
		"test_suite_name must not be empty");                          \
  static_assert(sizeof(GTEST_STRINGIFY_(test_name)) > 1,                       \
		"test_name must not be empty");                                \
  class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) : public fixture {  \
  public:                                                                      \
    GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() = default;            \
    ~GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() override = default;  \
    GTEST_DISALLOW_COPY_AND_ASSIGN_(GTEST_TEST_CLASS_NAME_(test_suite_name,    \
							   test_name));        \
    GTEST_DISALLOW_MOVE_AND_ASSIGN_(GTEST_TEST_CLASS_NAME_(test_suite_name,    \
							   test_name));        \
									       \
  private:                                                                     \
    boost::asio::awaitable<void> CoTestBody() override;                        \
    static ::testing::TestInfo *const test_info_ GTEST_ATTRIBUTE_UNUSED_;      \
  };                                                                           \
									       \
  ::testing::TestInfo *const GTEST_TEST_CLASS_NAME_(test_suite_name,           \
						    test_name)::test_info_ =   \
      ::testing::internal::MakeAndRegisterTestInfo(                            \
	  #test_suite_name, #test_name, nullptr, nullptr,                      \
	  ::testing::internal::CodeLocation(__FILE__, __LINE__),               \
	  (::testing::internal::GetTypeId<fixture>()),                         \
	  ::testing::internal::SuiteApiResolver<fixture>::GetSetUpCaseOrSuite( \
	      __FILE__, __LINE__),                                             \
	  ::testing::internal::SuiteApiResolver<                               \
	      fixture>::GetTearDownCaseOrSuite(__FILE__, __LINE__),            \
	  new ::testing::internal::TestFactoryImpl<GTEST_TEST_CLASS_NAME_(     \
	      test_suite_name, test_name)>);                                   \
  boost::asio::awaitable<void> GTEST_TEST_CLASS_NAME_(test_suite_name,         \
						      test_name)::CoTestBody()

/// \brief Helper macro for defining coroutine tests
///
/// Tests created this way are direct descendants of `CoroTest`.
///
/// The Boost.Asio IO Context is `io_context`.
///
/// \warning Use `EXPECT_*` only, not `ASSERT_*`. `ASSERT_` macros
/// return from the calling function and will not work in a
/// coroutine.
///
/// \param test_suite_name Name of the test suite
/// \param test_name Name of the test
#define CORO_TEST(test_suite_name, test_name)                                  \
  CORO_TEST_F(test_suite_name, test_name, CoroTest)

/// \brief Generate buffer::list filled with repeating byte
///
/// \param c Byte with which to fill
/// \param s Number of bites
///
/// \return A buffer::list filled with `s` copies of `c`
inline auto filled_buffer_list(char c, std::size_t s) {
  ceph::buffer::ptr bp{buffer::create(s)};
  std::memset(bp.c_str(), c, bp.length());
  ceph::buffer::list bl;
  bl.push_back(std::move(bp));
  return bl;
};

/// \brief Create buffer::list with specified bytes
///
/// \param cs Bytes the buffer::list should contain
///
/// \return A buffer::list containing the bytes in `cs`
inline auto to_buffer_list(std::initializer_list<unsigned char> cs) {
  ceph::buffer::ptr bp{buffer::create(cs.size())};
  auto ci = cs.begin();
  for (auto i = 0; i < std::ssize(cs); ++i, ++ci) {
    bp[i] = *ci;
  }
  ceph::buffer::list bl;
  bl.push_back(std::move(bp));
  return bl;
};

/// \brief Create buffer::list with the content of a string_view
///
/// \param s View with data to copy
///
/// \return A buffer::list containing a copy of `s`.
inline auto to_buffer_list(std::string_view s) {
  ceph::buffer::list bl;
  bl.append(s);
  return bl;
};

/// \brief Create buffer::list with the content of a span
///
/// \param s Span with data to copy
///
/// \return A buffer::list containing a copy of `s`.
inline auto to_buffer_list(std::span<char> s) {
  ceph::buffer::list bl;
  bl.append(s.data(), s.size());
  return bl;
};

/// \brief Create buffer::list containing integer
///
/// \param n Integer with which to fill the list
///
/// \return A buffer::list containing the encoded `n`
inline auto to_buffer_list(std::integral auto n) {
  ceph::buffer::list bl;
  encode(n, bl);
  return bl;
};

/// \brief Return value contained by buffer::list
///
/// \param bl List with encoded value
///
/// \return The value encoded in `bl`.
template<std::default_initializable T>
inline auto from_buffer_list(const ceph::buffer::list& bl)
{
  using ceph::decode;
  T t;
  auto bi = bl.begin();
  decode(t, bi);
  return t;
}

inline bool is_crimson_cluster() {
  return getenv("CRIMSON_COMPAT") != nullptr;
}

// Yet more nonsense caused by Google's ridiculous except-o-phobia.

#define SKIP_IF_CRIMSON()                                                      \
  if (is_crimson_cluster()) {                                                  \
    std::cerr << "Not supported by crimson yet. Skipped" << std::endl;         \
    co_return;                                                                 \
  }

/// \brief Wait for a specified time
///
/// \param dur Time to wait.
template<typename Rep, typename Period>
boost::asio::awaitable<void> wait_for(std::chrono::duration<Rep, Period> dur)
{
  boost::asio::steady_timer t(co_await boost::asio::this_coro::executor, dur);
  co_return co_await t.async_wait(boost::asio::use_awaitable);
}
