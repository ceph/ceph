// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <string.h>

#include "include/types.h"
#include "include/crc32c.h"
#include "include/utime.h"
#include "common/Clock.h"

#include "gtest/gtest.h"

#include "common/sctp_crc32.h"
#include "common/crc32c_intel_baseline.h"

TEST(Crc32c, Small) {
  const char *a = "foo bar baz";
  const char *b = "whiz bang boom";
  ASSERT_EQ(4119623852u, ceph_crc32c(0, (unsigned char *)a, strlen(a)));
  ASSERT_EQ(881700046u, ceph_crc32c(1234, (unsigned char *)a, strlen(a)));
  ASSERT_EQ(2360230088u, ceph_crc32c(0, (unsigned char *)b, strlen(b)));
  ASSERT_EQ(3743019208u, ceph_crc32c(5678, (unsigned char *)b, strlen(b)));
}

TEST(Crc32c, PartialWord) {
  const char *a = (const char *)malloc(5);
  const char *b = (const char *)malloc(35);
  memset((void *)a, 1, 5);
  memset((void *)b, 1, 35);
  ASSERT_EQ(2715569182u, ceph_crc32c(0, (unsigned char *)a, 5));
  ASSERT_EQ(440531800u, ceph_crc32c(0, (unsigned char *)b, 35));
}

TEST(Crc32c, Big) {
  int len = 4096000;
  char *a = (char *)malloc(len);
  memset(a, 1, len);
  ASSERT_EQ(31583199u, ceph_crc32c(0, (unsigned char *)a, len));
  ASSERT_EQ(1400919119u, ceph_crc32c(1234, (unsigned char *)a, len));
}

TEST(Crc32c, Performance) {
  int len = 1000 * 1024 * 1024;
  char *a = (char *)malloc(len);
  std::cout << "populating large buffer" << std::endl;
  for (int i=0; i<len; i++)
    a[i] = i & 0xff;
  std::cout << "calculating crc" << std::endl;

  {
    utime_t start = ceph_clock_now(NULL);
    unsigned val = ceph_crc32c(0, (unsigned char *)a, len);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "best choice = " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(261108528u, val);
  }
  {
    utime_t start = ceph_clock_now(NULL);
    unsigned val = ceph_crc32c(0xffffffff, (unsigned char *)a, len);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "best choice 0xffffffff = " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(3895876243u, val);
  }
  {
    utime_t start = ceph_clock_now(NULL);
    unsigned val = ceph_crc32c_sctp(0, (unsigned char *)a, len);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "sctp = " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(261108528u, val);
  }
  {
    utime_t start = ceph_clock_now(NULL);
    unsigned val = ceph_crc32c_intel_baseline(0, (unsigned char *)a, len);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "intel baseline = " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(261108528u, val);
  }

}
