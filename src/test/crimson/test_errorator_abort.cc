// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>
#include <numeric>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/errorator.h"
#include "crimson/common/errorator-loop.h"
#include "crimson/common/log.h"
#include "seastar/core/sleep.hh"

struct errorator_abort_test_t : public seastar_test_suite_t {
  using ertr = crimson::errorator<crimson::ct_error::invarg>;

  ertr::future<> invarg_foo() {
    return crimson::ct_error::invarg::make();
  };

  ertr::future<> clean_foo() {
    return ertr::now();
  };

  struct noncopyable_t {
    constexpr noncopyable_t() = default;
    ~noncopyable_t() = default;
    noncopyable_t(noncopyable_t&&) = default;
  private:
    noncopyable_t(const noncopyable_t&) = delete;
    noncopyable_t& operator=(const noncopyable_t&) = delete;
  };
};

// --- The following tests must abort ---

/*
TEST_F(errorator_abort_test_t, abort_vanilla)
{
  run_async([this] {
    abort();
    return seastar::now().get();
  });
}
*/

/*
TEST_F(errorator_abort_test_t, abort_ignored)
{
  run_async([this] {
    auto foo = []() -> seastar::future<> {
      abort();
      return seastar::now();
    };

    std::ignore = foo();
    return seastar::now().get();
  });
}
*/

/*
TEST_F(errorator_abort_test_t, assert_all)
{
  run_async([this] {
    return invarg_foo().handle_error(
      crimson::ct_error::assert_all("unexpected error")
    ).get();
  });
}
*/

/*
TEST_F(errorator_abort_test_t, ignore_assert_all)
{
  run_async([this] {
    std::ignore = invarg_foo().handle_error(
      crimson::ct_error::assert_all("unexpected error")
    );
    return seastar::now().get();
  });
}

TEST_F(errorator_abort_test_t, ignore_assert_failure)
{
  run_async([this] {
    std::ignore = invarg_foo().handle_error(
      crimson::ct_error::invarg::assert_failure{"unexpected invarg"}
    );
    return seastar::now().get();
  });
}
*/
