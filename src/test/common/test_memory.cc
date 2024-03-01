// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <vector>
#include <string.h>

#include "include/inline_memory.h"
#include "include/utime.h"
#include "common/Clock.h"
#include "gtest/gtest.h"

class MemoryIsZeroBigTest : public ::testing::TestWithParam<size_t> {};
class MemoryIsZeroSmallTest : public ::testing::TestWithParam<size_t> {};
class MemoryIsZeroPerformance : public ::testing::TestWithParam<size_t> {};

TEST_P(MemoryIsZeroBigTest, MemoryIsZeroTestBig) {
  size_t size = GetParam();
  char *data = (char *)malloc(sizeof(char) * size);
  memset(data, 0, sizeof(char) * size);
  EXPECT_TRUE(mem_is_zero(data, size));

  size_t pos = rand() % size;
  data[pos] = 'a';
  EXPECT_FALSE(mem_is_zero(data, size));

  free(data);
}

TEST_P(MemoryIsZeroSmallTest, MemoryIsZeroTestSmall) {
  size_t size = GetParam();
  for (size_t i = 0; i < size; i++) {
    char* data = new char[size]();
    EXPECT_TRUE(mem_is_zero(data, size));

    data[i] = 'a';
    EXPECT_FALSE(mem_is_zero(data, size));
  }
}

TEST_P(MemoryIsZeroPerformance, MemoryIsZeroPerformanceTest) {
  constexpr size_t ITER = 1000000;
  utime_t start;
  utime_t end;

  size_t size = GetParam();
  char *data = (char *)malloc(size);
  memset(data, 0, size);
  
  bool res = false;
  start = ceph_clock_now();
  for (size_t i = 0; i < ITER; i++) {
    res = mem_is_zero(data, size);
  }
  end = ceph_clock_now();

  std::cout << "iterators=" << ITER 
            << " size= " << size 
            << " time=" << (double)(end - start)
            << std::endl;

  ASSERT_TRUE(res);
  free(data);
}

INSTANTIATE_TEST_SUITE_P(MemoryIsZeroSmallTests, MemoryIsZeroSmallTest,
                        ::testing::Values(1, 4, 7, 8, 12, 28, 60, 64));

INSTANTIATE_TEST_SUITE_P(MemoryIsZeroBigTests, MemoryIsZeroBigTest,
                        ::testing::Values(1024, 4096, 8192, 64 * 1024));

INSTANTIATE_TEST_SUITE_P(MemoryIsZeroPerformanceTests, MemoryIsZeroPerformance,
                        ::testing::Values(1024, 2048, 4096, 8192, 64 * 1024));
