// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
//
// Minimal test that triggers calc_pg_masks() with pg_num=0.
// When pg_num=0, pg_num-1 wraps to UINT_MAX, cbits() returns 32, and
// the original (1 << 32) on 32-bit int is undefined behavior.
// UBSan reports: "shift exponent 32 is too large for 32-bit type 'int'"
// Run with ASan+UBSan: ./unittest-crimson-pg-pool-shift

#include "test/crimson/gtest_seastar.h"
#include "osd/osd_types.h"

struct pg_pool_shift_test_t : public seastar_test_suite_t {};

TEST_F(pg_pool_shift_test_t, calc_pg_masks_pg_num_zero)
{
  run_async([] {
    pg_pool_t pool;
    pool.set_pg_num(0);  // pg_num-1 wraps to UINT_MAX -> cbits=32 -> (1<<32) UB without fix
    // With fix: mask = (unsigned)-1 when cbits >= 32
    EXPECT_EQ(pool.get_pg_num_mask(), static_cast<unsigned>(-1));
  });
}

TEST_F(pg_pool_shift_test_t, calc_pg_masks_pgp_num_zero)
{
  run_async([] {
    pg_pool_t pool;
    pool.set_pgp_num(0);  // same UB path for pgp_num
    EXPECT_EQ(pool.get_pgp_num_mask(), static_cast<unsigned>(-1));
  });
}
