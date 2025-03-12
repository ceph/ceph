// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "include/ceph_assert.h"

#include "gtest/gtest.h"

static void non_supressed_assert_line16() {
  ceph_assert(42 == 41); // LINE16
}
static void supressed_assert_line19() {
  ceph_assert(42 == 40); // LINE19
}
static void supressed_assertf_line22() {
  ceph_assertf(42 == 39, "FAILED ceph_assertf"); // LINE22
}
static void non_supressed_assertf_line25() {
  ceph_assertf(42 == 38, "FAILED ceph_assertf"); // LINE25
}

TEST(CephAssertDeathTest, ceph_assert_supresssions) {
  ASSERT_DEATH(non_supressed_assert_line16(), "FAILED ceph_assert");
  supressed_assert_line19();
  supressed_assertf_line22();
  ASSERT_DEATH(non_supressed_assertf_line25(), "FAILED ceph_assertf");
}

int main(int argc, char **argv) {

  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
    nullptr,
    args,
    CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  g_ceph_context->_conf.set_val(
    "ceph_assert_supresssions",
    fmt::format(
      "{}:{}, {}:{}", __FILE__, /* LINE19 */19, __FILE__, /* LINE22 */22));
  common_init_finish(g_ceph_context);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
