#include "gtest/gtest.h"
#include "include/cephd/libcephd.h"

TEST(LibCephdMiscVersion, Version) {
  int major, minor, extra;
  cephd_version(&major, &minor, &extra);
}
