// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/time.h>
#include <sys/resource.h>
#include <fmt/format.h>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "include/ceph_assert.h"

#include "gtest/gtest.h"

static void non_supressed_assert_line18() {
  ceph_assert(42 == 41); // LINE18
}
static void supressed_assert_line21() {
  ceph_assert(42 == 40); // LINE21
}
static void supressed_assertf_line24() {
  ceph_assertf(42 == 39, "FAILED ceph_assertf"); // LINE24
}
static void non_supressed_assertf_line27() {
  ceph_assertf(42 == 38, "FAILED ceph_assertf"); // LINE27
}

TEST(CephAssertDeathTest, ceph_assert_supresssions) {
  ASSERT_DEATH(non_supressed_assert_line18(), "FAILED ceph_assert");
  supressed_assert_line21();
  supressed_assertf_line24();
  ASSERT_DEATH(non_supressed_assertf_line27(), "FAILED ceph_assertf");
}

int main(int argc, char **argv) {
  // Disable core files
  const struct rlimit rlim = { 0, 0 };
  setrlimit(RLIMIT_CORE, &rlim);

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
      "{}:{}, {}:{}", __FILE__, /* LINE21 */21, __FILE__, /* LINE24 */24));
  common_init_finish(g_ceph_context);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
