// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/ThreadMonitor.h"

TEST_F(ThreadMonitorTestHelper, BasicCreation) {
  ASSERT_NE(thread_monitor, nullptr);
  ASSERT_NE(cct, nullptr);
}

TEST_F(ThreadMonitorTestHelper, Construction) {
  ThreadMonitor tm(cct.get());
  // Should not crash
}

// Regression test for https://tracker.ceph.com/issues/81337
// read_thread_stat() used to run `ss >> utime >> stime` without checking the
// extraction result, so a malformed /proc/<pid>/task/<tid>/stat line left
// utime/stime uninitialised while the function still reported success. The
// parsing seam must instead report failure so the caller treats the thread as
// unreadable rather than computing a CPU percentage from garbage values.
TEST(ThreadMonitor, ReadThreadStatReturnsFalseOnMalformedLine) {
  // A well-formed stat line: pid (comm) state ppid pgrp session tty_nr tpgid
  // flags minflt cminflt majflt cmajflt utime stime ...
  // The 14th/15th fields after the comm are utime=42 and stime=17.
  const std::string good_line =
      "1234 (python3) S 1 1234 1234 0 -1 4194560 100 0 0 0 42 17 0 0 20 0 1 0 "
      "1000 123456 789 18446744073709551615 1 1 0 0 0 0 0";
  long long utime = -1;
  long long stime = -1;
  EXPECT_TRUE(ThreadMonitor::parse_thread_stat(good_line, utime, stime))
      << "A well-formed /proc stat line must parse successfully.";
  EXPECT_EQ(42, utime) << "utime (field 14) must be extracted from the line.";
  EXPECT_EQ(17, stime) << "stime (field 15) must be extracted from the line.";

  // A malformed line that carries a valid "(comm)" but is truncated before the
  // utime/stime fields. The stringstream extraction of utime/stime fails here.
  const std::string malformed_line = "1234 (python3) S 1 1234";
  long long bad_utime = 0;
  long long bad_stime = 0;
  EXPECT_FALSE(ThreadMonitor::parse_thread_stat(malformed_line, bad_utime,
                                                bad_stime))
      << "Current: returns true with uninitialised utime/stime on malformed "
         "line; Expected: returns false.";
}
