// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/errorator.h"
#include "crimson/common/log.h"

struct errorator_test_t : public seastar_test_suite_t {
  using ertr = crimson::errorator<crimson::ct_error::invarg>;
  ertr::future<> test_do_until() {
    return crimson::do_until([i=0]() mutable {
      if (i < 5) {
        ++i;
        return ertr::make_ready_future<bool>(false);
      } else {
        return ertr::make_ready_future<bool>(true);
      }
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

TEST_F(errorator_test_t, non_copy_then)
{
  run_async([this] {
    test_non_copy_then().unsafe_get0();
  });
}
