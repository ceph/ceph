// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/ThreadMonitor.h"

// Smoke test for the fixture: SetUp() constructs a ThreadMonitor against a real
// CephContext. The constructor registers a config observer and reads
// mgr_module_monitor_interval, so this verifies that setup succeeds before any
// other fixture-based test builds on it.
TEST_F(ThreadMonitorTestHelper, BasicCreation) {
  ASSERT_NE(thread_monitor, nullptr);
  ASSERT_NE(cct, nullptr);
}

TEST(ThreadMonitor, ParseThreadStatReturnsFalseOnMalformedLine) {
  const std::string good_line =
      "1234 (python3) S 1 1234 1234 0 -1 4194560 100 0 0 0 42 17 0 0 20 0 1 0 "
      "1000 123456 789 18446744073709551615 1 1 0 0 0 0 0";
  long long utime = -1;
  long long stime = -1;
  EXPECT_TRUE(ThreadMonitor::parse_thread_stat(good_line, utime, stime))
      << "A well-formed /proc stat line must parse successfully.";
  EXPECT_EQ(42, utime) << "utime (field 14) must be extracted from the line.";
  EXPECT_EQ(17, stime) << "stime (field 15) must be extracted from the line.";

  const std::string malformed_line = "1234 (python3) S 1 1234";
  long long bad_utime = 0;
  long long bad_stime = 0;
  EXPECT_FALSE(ThreadMonitor::parse_thread_stat(malformed_line, bad_utime,
                                                bad_stime))
      << "Expected false for a line truncated before utime/stime.";
}

TEST(ThreadMonitor, ParseThreadStatReturnsFalseOnTruncatedLine) {
  const std::string truncated_at_paren = "1234 (python3)";
  long long utime = 99;
  long long stime = 88;
  EXPECT_FALSE(ThreadMonitor::parse_thread_stat(truncated_at_paren, utime,
                                                stime))
      << "A line truncated at ')' must return false without throwing.";
  EXPECT_EQ(99, utime) << "utime must be unmodified when parse_thread_stat returns false.";
  EXPECT_EQ(88, stime) << "stime must be unmodified when parse_thread_stat returns false.";

  const std::string truncated_after_space = "1234 (python3) ";
  EXPECT_FALSE(ThreadMonitor::parse_thread_stat(truncated_after_space, utime,
                                                stime))
      << "A line with only ')' + space (no fields) must return false.";
  EXPECT_EQ(99, utime) << "utime must remain unmodified.";
  EXPECT_EQ(88, stime) << "stime must remain unmodified.";
}

TEST(ThreadMonitor, ParseThreadStatDoesNotMutateOutputsOnFailure) {
  // utime (42) is present but stime is missing
  const std::string partial_line =
      "1234 (python3) S 1 1234 1234 0 -1 4194560 100 0 0 0 42";

  long long utime = 55;
  long long stime = 77;
  EXPECT_FALSE(ThreadMonitor::parse_thread_stat(partial_line, utime, stime))
      << "A line with utime but no stime must return false.";
  EXPECT_EQ(55, utime)
      << "utime must not be mutated when parse fails (temporaries pattern).";
  EXPECT_EQ(77, stime)
      << "stime must not be mutated when parse fails (temporaries pattern).";
}

TEST(ThreadMonitor, ParseProcessStatmExtractsRss) {
  // statm fields: size resident shared text lib data dt
  const std::string good_line = "2048 512 128 1 0 256 0";
  long long rss_pages = -1;
  EXPECT_TRUE(ThreadMonitor::parse_process_statm(good_line, rss_pages))
      << "A well-formed /proc statm line must parse successfully.";
  EXPECT_EQ(512, rss_pages) << "rss is the 2nd statm field (resident set size).";
}

TEST(ThreadMonitor, ParseProcessStatmReturnsFalseOnMalformedLine) {
  long long rss_pages = 99;

  // Empty line (e.g. getline on an empty/unreadable statm).
  EXPECT_FALSE(ThreadMonitor::parse_process_statm("", rss_pages))
      << "An empty statm line must return false.";
  EXPECT_EQ(99, rss_pages) << "rss_pages must be unmodified on failure.";

  // Only the first field present; rss (2nd field) is missing.
  EXPECT_FALSE(ThreadMonitor::parse_process_statm("2048", rss_pages))
      << "A statm line truncated before rss must return false.";
  EXPECT_EQ(99, rss_pages) << "rss_pages must be unmodified on failure.";

  // Non-numeric content.
  EXPECT_FALSE(ThreadMonitor::parse_process_statm("size resident", rss_pages))
      << "A non-numeric statm line must return false.";
  EXPECT_EQ(99, rss_pages) << "rss_pages must be unmodified on failure.";
}
