// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>
#include <numeric>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/errorator.h"
#include "crimson/common/errorator-loop.h"
#include "crimson/common/log.h"
#include "seastar/core/sleep.hh"

struct errorator_test_t : public seastar_test_suite_t {
  using ertr = crimson::errorator<crimson::ct_error::invarg>;
  ertr::future<> test_do_until() {
    return crimson::repeat([i=0]() mutable {
      if (i < 5) {
        ++i;
        return ertr::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::no);
      } else {
        return ertr::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
      }
    });
  }
  static constexpr int SIZE = 42;
  ertr::future<> test_parallel_for_each() {
    auto sum = std::make_unique<int>(0);
    return ertr::parallel_for_each(
      boost::make_counting_iterator(0),
      boost::make_counting_iterator(SIZE),
      [sum=sum.get()](int i) {
	*sum += i;
    }).safe_then([sum=std::move(sum)] {
      int expected = std::accumulate(boost::make_counting_iterator(0),
				     boost::make_counting_iterator(SIZE),
				     0);
      ASSERT_EQ(*sum, expected);
    });
  }
  struct noncopyable_t {
    constexpr noncopyable_t() = default;
    ~noncopyable_t() = default;
    noncopyable_t(noncopyable_t&&) = default;
  private:
    noncopyable_t(const noncopyable_t&) = delete;
    noncopyable_t& operator=(const noncopyable_t&) = delete;
  };
  ertr::future<> test_non_copy_then() {
    return create_noncopyable().safe_then([](auto t) {
      return ertr::now();
    });
  }
  ertr::future<int> test_futurization() {
    // we don't want to be enforced to always do `make_ready_future(...)`.
    // as in seastar::future, the futurization should take care about
    // turning non-future types (e.g. int) into futurized ones (e.g.
    // ertr::future<int>).
    return ertr::now().safe_then([] {
      return 42;
    }).safe_then([](int life) {
      return ertr::make_ready_future<int>(life);
    });
  }
private:
  ertr::future<noncopyable_t> create_noncopyable() {
    return ertr::make_ready_future<noncopyable_t>();
  }
};

TEST_F(errorator_test_t, basic)
{
  run_async([this] {
    test_do_until().unsafe_get0();
  });
}

TEST_F(errorator_test_t, parallel_for_each)
{
  run_async([this] {
    test_parallel_for_each().unsafe_get0();
  });
}

TEST_F(errorator_test_t, non_copy_then)
{
  run_async([this] {
    test_non_copy_then().unsafe_get0();
  });
}

TEST_F(errorator_test_t, test_futurization)
{
  run_async([this] {
    test_futurization().unsafe_get0();
  });
}
