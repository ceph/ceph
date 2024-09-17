// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cerrno>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "common/Journald.h"
#include "log/Entry.h"
#include "log/SubsystemMap.h"

using namespace ceph::logging;

class JournaldLoggerTest : public ::testing::Test {
 protected:
  SubsystemMap subs;
  JournaldLogger journald = {&subs};
  MutableEntry entry = {0, 0};

  void SetUp() override {
    struct stat buffer;
    if (stat("/run/systemd/journal/socket", &buffer) < 0) {
      if (errno == ENOENT) {
        GTEST_SKIP() << "No journald socket present.";
      }
      FAIL() << "Unexpected stat error: " << strerror(errno);
    }
  }
};

TEST_F(JournaldLoggerTest, Log)
{
  entry.get_ostream() << "This is a testing regular log message.";
  EXPECT_EQ(journald.log_entry(entry), 0);
}

TEST_F(JournaldLoggerTest, VeryLongLog)
{
  entry.get_ostream() << std::string(16 * 1024 * 1024, 'a');
  EXPECT_EQ(journald.log_entry(entry), 0);
}
