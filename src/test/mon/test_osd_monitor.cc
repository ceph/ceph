#include "gtest/gtest.h"
#include "mon/OSDMonitor.h"

// --------------------------------------------------
// Testing OSDMonitor::calculate_migrating_pg_count()
// --------------------------------------------------

TEST(OSDMonitor, CalcMigratingPgCountSourceBigger) {
  int source_pgnum = 101;
  int target_pgnum = 79;
  double migration_ratio = 0.05;
  uint64_t expected_total = 4; // ceil(79 * 0.05)
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountTargetBigger) {
  int source_pgnum = 32;
  int target_pgnum = 124;
  double migration_ratio = 0.5;
  uint64_t expected_total = 16; // 32 * 0.5
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountSourceTargetEqual) {
  int source_pgnum = 128;
  int target_pgnum = 128;
  double migration_ratio = 0.1;
  uint64_t expected_total = 13; // ceil(128 * 0.1)
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountRoundUp) {
  int source_pgnum = 32;
  int target_pgnum = 32;
  double migration_ratio = 0.01;
  uint64_t expected_total = 1; // ceil(32 * 0.01)
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountFullRatioNonPowerOf2) {
  // A ratio of 1.0 must never migrate more PGs than the smaller pool has
  int source_pgnum = 101;
  int target_pgnum = 79;
  double migration_ratio = 1.0;
  uint64_t expected_total = 79;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountZeroRatio) {
  // At least one PG must always be migrated so the migration makes progress
  int source_pgnum = 32;
  int target_pgnum = 32;
  double migration_ratio = 0.0;
  uint64_t expected_total = 1;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountFullRatio) {
  int source_pgnum = 64;
  int target_pgnum = 128;
  double migration_ratio = 1.0;
  uint64_t expected_total = 64;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_ratio), expected_total);
}

// -----------------------------------------
// Testing OSDMonitor::target_pg_migrating()
// -----------------------------------------

TEST(OSDMonitor, TargetPgMigratingPowerOf2InList) {
  int source_pgnum = 64;
  int target_pgnum = 32;

  pg_t source_pg(32, 0); // target 0

  std::set<pg_t> migrating_pgs;
  migrating_pgs.insert(pg_t(0, 0)); // target 0

  bool result = OSDMonitor::target_pg_migrating(migrating_pgs, source_pg,
                                                 source_pgnum, target_pgnum);
  ASSERT_TRUE(result);
}

TEST(OSDMonitor, TargetPgMigratingPowerOf2NotInList) {
  int source_pgnum = 64;
  int target_pgnum = 32;

  pg_t source_pg(63, 0); // target 31

  std::set<pg_t> migrating_pgs;
  migrating_pgs.insert(pg_t(32, 0)); // target 0

  bool result = OSDMonitor::target_pg_migrating(migrating_pgs, source_pg,
                                                 source_pgnum, target_pgnum);
  ASSERT_FALSE(result);
}

TEST(OSDMonitor, TargetPgMigratingNonPowerOf2InList) {
  int source_pgnum = 67;
  int target_pgnum = 19;

  pg_t source_pg(19, 0); // target 3

  std::set<pg_t> migrating_pgs;
  migrating_pgs.insert(pg_t(0, 0)); // target 0
  migrating_pgs.insert(pg_t(27, 0)); // target 11
  migrating_pgs.insert(pg_t(3, 0)); // target 3

  bool result = OSDMonitor::target_pg_migrating(migrating_pgs, source_pg,
                                                 source_pgnum, target_pgnum);
  ASSERT_TRUE(result);
}

TEST(OSDMonitor, TargetPgMigratingNonPowerOf2NotInList) {
  int source_pgnum = 67;
  int target_pgnum = 19;

  pg_t source_pg(19, 0); // target 3

  std::set<pg_t> migrating_pgs;
  migrating_pgs.insert(pg_t(0, 0)); // target 0
  migrating_pgs.insert(pg_t(27, 0)); // target 11
  migrating_pgs.insert(pg_t(37, 0)); // target 1

  bool result = OSDMonitor::target_pg_migrating(migrating_pgs, source_pg,
                                                 source_pgnum, target_pgnum);
  ASSERT_FALSE(result);
}

TEST(OSDMonitor, TargetPgMigratingEmptySet) {
  int source_pgnum = 64;
  int target_pgnum = 32;

  pg_t source_pg(5, 1);

  std::set<pg_t> migrating_pgs;  // empty
  
  bool result = OSDMonitor::target_pg_migrating(migrating_pgs, source_pg,
                                                 source_pgnum, target_pgnum);
  ASSERT_FALSE(result);
}
