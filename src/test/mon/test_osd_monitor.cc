#include "gtest/gtest.h"
#include "mon/OSDMonitor.h"

// --------------------------------------------------
// Testing OSDMonitor::calculate_migrating_pg_count()
// --------------------------------------------------

TEST(OSDMonitor, CalcMigratingPgCountSourceBigger) {
  int source_pgnum = 101;
  int target_pgnum = 79;
  uint64_t migration_percent = 5;
  uint64_t expected_total = (target_pgnum * migration_percent + 99) / 100;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_percent), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountTargetBigger) {
  int source_pgnum = 32;
  int target_pgnum = 124;
  uint64_t migration_percent = 50;
  uint64_t expected_total = (source_pgnum * migration_percent + 99) / 100;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_percent), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountSourceTargetEqual) {
  int source_pgnum = 128;
  int target_pgnum = 128;
  uint64_t migration_percent = 10;
  uint64_t expected_total = (target_pgnum * migration_percent + 99) / 100;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_percent), expected_total);
}

TEST(OSDMonitor, CalcMigratingPgCountRoundUp) {
  int source_pgnum = 32;
  int target_pgnum = 32;
  uint64_t migration_percent = 1;
  uint64_t expected_total = 1;
  ASSERT_EQ(OSDMonitor::calculate_migrating_pg_count(source_pgnum, target_pgnum, migration_percent), expected_total);
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
