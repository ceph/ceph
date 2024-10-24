// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/url_escape.h"

#include <iostream> // for std::cout

#include "gtest/gtest.h"

TEST(url_escape, escape) {
  ASSERT_EQ(url_escape("foo bar"), std::string("foo%20bar"));
  ASSERT_EQ(url_escape("foo\nbar"), std::string("foo%0abar"));
}

TEST(url_escape, unescape) {
  ASSERT_EQ(url_unescape("foo%20bar"), std::string("foo bar"));
  ASSERT_EQ(url_unescape("foo%0abar"), std::string("foo\nbar"));
  ASSERT_EQ(url_unescape("%20"), std::string(" "));
  ASSERT_EQ(url_unescape("\0%20"), std::string("\0 "));
  ASSERT_EQ(url_unescape("\x01%20"), std::string("\x01 "));
}

TEST(url_escape, all_chars) {
  std::string a;
  for (unsigned j=0; j<256; ++j) {
    a.push_back((char)j);
  }
  std::string b = url_escape(a);
  std::cout << "escaped: " << b << std::endl;
  ASSERT_EQ(a, url_unescape(b));
}

TEST(url_escape, invalid) {
  ASSERT_THROW(url_unescape("foo%xx"), std::runtime_error);
  ASSERT_THROW(url_unescape("foo%%"), std::runtime_error);
  ASSERT_THROW(url_unescape("foo%"), std::runtime_error);
  ASSERT_THROW(url_unescape("foo%0"), std::runtime_error);
}
