// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "common/numa.h"

TEST(cpu_set, parse_list) {
  cpu_set_t cpu_set;
  size_t size;

  ASSERT_EQ(0, parse_cpu_set_list("0-3", &size, &cpu_set));
  ASSERT_EQ(size, 4u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_list("0-3,6-7", &size, &cpu_set));
  ASSERT_EQ(size, 8u);
  for (unsigned i = 0; i < 4; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }
  for (unsigned i = 4; i < 6; ++i) {
    ASSERT_FALSE(CPU_ISSET(i, &cpu_set));
  }
  for (unsigned i = 6; i < 8; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_list("0-31", &size, &cpu_set));
  ASSERT_EQ(size, 32u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }
}

TEST(cpu_set, to_str_list) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(0, &cpu_set);
  ASSERT_EQ(std::string("0"), cpu_set_to_str_list(8, &cpu_set));
  CPU_SET(1, &cpu_set);
  CPU_SET(2, &cpu_set);
  CPU_SET(3, &cpu_set);
  ASSERT_EQ(std::string("0-3"), cpu_set_to_str_list(8, &cpu_set));
  CPU_SET(5, &cpu_set);
  ASSERT_EQ(std::string("0-3,5"), cpu_set_to_str_list(8, &cpu_set));
  CPU_SET(6, &cpu_set);
  CPU_SET(7, &cpu_set);
  ASSERT_EQ(std::string("0-3,5-7"), cpu_set_to_str_list(8, &cpu_set));
}

TEST(cpu_set, round_trip_list)
{
  for (unsigned i = 0; i < 100; ++i) {
    cpu_set_t cpu_set;
    size_t size = 32;
    CPU_ZERO(&cpu_set);
    for (unsigned i = 0; i < 32; ++i) {
      if (rand() % 1) {
	CPU_SET(i, &cpu_set);
      }
    }
    std::string v = cpu_set_to_str_mask(size, &cpu_set);
    cpu_set_t cpu_set_2;
    size_t size2;
    ASSERT_EQ(0, parse_cpu_set_mask(v.c_str(), &size2, &cpu_set_2));
    for (unsigned i = 0; i < 32; ++i) {
      ASSERT_TRUE(CPU_ISSET(i, &cpu_set) == CPU_ISSET(i, &cpu_set_2));
    }
  }
}



TEST(cpu_set, parse_mask) {
  cpu_set_t cpu_set;
  size_t size;

  ASSERT_EQ(0, parse_cpu_set_mask("f", &size, &cpu_set));
  ASSERT_EQ(size, 4u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_mask("0f", &size, &cpu_set));
  ASSERT_EQ(size, 8u);
  for (unsigned i = 0; i < 4; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_mask("ffff", &size, &cpu_set));
  ASSERT_EQ(size, 16u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_mask("0000", &size, &cpu_set));
  ASSERT_EQ(size, 16u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_FALSE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_mask("00ff00ff", &size, &cpu_set));
  ASSERT_EQ(size, 32u);
  for (unsigned i = 0; i < 8; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }
  for (unsigned i = 8; i < 16; ++i) {
    ASSERT_FALSE(CPU_ISSET(i, &cpu_set));
  }
  for (unsigned i = 16; i < 24; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }
  for (unsigned i = 24; i < 32; ++i) {
    ASSERT_FALSE(CPU_ISSET(i, &cpu_set));
  }

  ASSERT_EQ(0, parse_cpu_set_mask("ffffffffffffffffffffffffffffffff",
				  &size, &cpu_set));
  ASSERT_EQ(size, 128u);
  for (unsigned i = 0; i < size; ++i) {
    ASSERT_TRUE(CPU_ISSET(i, &cpu_set));
  }
}

TEST(cpu_set, to_str_mask)
{
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(0, &cpu_set);
  CPU_SET(1, &cpu_set);
  CPU_SET(2, &cpu_set);
  CPU_SET(3, &cpu_set);
  ASSERT_EQ(std::string("f"), cpu_set_to_str_mask(4, &cpu_set));
  ASSERT_EQ(std::string("0f"), cpu_set_to_str_mask(8, &cpu_set));
}

TEST(cpu_set, round_trip_mask)
{
  for (unsigned i = 0; i < 100; ++i) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%08llx", (long long unsigned)rand());
    cpu_set_t cpu_set;
    size_t size;
    ASSERT_EQ(0, parse_cpu_set_mask(buf, &size, &cpu_set));
    ASSERT_EQ(32u, size);
    std::string v = cpu_set_to_str_mask(size, &cpu_set);
    ASSERT_EQ(v, buf);
  }
}

