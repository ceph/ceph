// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/pretty_binary.h"

#include "gtest/gtest.h"

TEST(pretty_binary, print) {
  ASSERT_EQ(pretty_binary_string(std::string("foo\001\002bars")), std::string("'foo'0x0102'bars'"));
  ASSERT_EQ(pretty_binary_string(std::string("foo''bars")), std::string("'foo''''bars'"));
  ASSERT_EQ(pretty_binary_string(std::string("foo\377  \200!!")), std::string("'foo'0xFF2020802121"));
  ASSERT_EQ(pretty_binary_string(std::string("\001\002\003\004")), std::string("0x01020304"));
}

TEST(pretty_binary, unprint) {
  ASSERT_EQ(pretty_binary_string_reverse(std::string("'foo'0x0102'bars'")), std::string("foo\001\002bars"));
  ASSERT_EQ(pretty_binary_string_reverse(std::string("'foo''''bars'")), std::string("foo''bars"));
  ASSERT_EQ(pretty_binary_string_reverse(std::string("'foo'0xFF2020802121")), std::string("foo\377  \200!!"));
  ASSERT_EQ(pretty_binary_string_reverse(std::string("0x01020304")),std::string("\001\002\003\004"));
}

TEST(pretty_binary, all_chars) {
  std::string a;
  for (unsigned j = 0; j < 256; ++j) {
    a.push_back((char)j);
  }
  std::string b = pretty_binary_string(a);
  ASSERT_EQ(a, pretty_binary_string_reverse(b));
}

TEST(pretty_binary, random) {
  for (size_t i = 0; i < 100000; i++) {
    std::string a;
    size_t r = rand() % 100;
    for (size_t j = 0; j < r; ++j) {
      a.push_back((unsigned char)rand());
    }
    std::string b = pretty_binary_string(a);
    ASSERT_EQ(a, pretty_binary_string_reverse(b));
  }
}

TEST(pretty_binary, invalid) {
  ASSERT_THROW(pretty_binary_string_reverse("'"), std::invalid_argument);
  ASSERT_THROW(pretty_binary_string_reverse("0x1"), std::invalid_argument);
  ASSERT_THROW(pretty_binary_string_reverse("0x"), std::invalid_argument);
  ASSERT_THROW(pretty_binary_string_reverse("'''"), std::invalid_argument);
  ASSERT_THROW(pretty_binary_string_reverse("'a'x"), std::invalid_argument);
  ASSERT_THROW(pretty_binary_string_reverse("'a'0x"), std::invalid_argument);
}
