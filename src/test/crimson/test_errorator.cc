// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/errorator.h"
#include "crimson/common/log.h"

struct do_until_test_t : public seastar_test_suite_t {
  using ertr = crimson::errorator<crimson::ct_error::invarg>;
  ertr::future<> test() {
    return crimson::do_until([this] {
      if (i < 5) {
        ++i;
        return ertr::make_ready_future<bool>(false);
      } else {
        return ertr::make_ready_future<bool>(true);
      }
    });
  }

  int i = 0;
};

TEST_F(do_until_test_t, basic)
{
  run_async([this] {
    test().unsafe_get0();
  });
}
