// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string.hpp>
#include <gtest/gtest.h>
#include <regex>
#include <sstream>
#include <string>

#include "common/BackTrace.h"
#include "common/version.h"

#ifndef __has_feature
#define __has_feature(x) 0
#endif

// a dummy function, so we can check "foo" in the backtrace.
// do not mark this function as static or put it into an anonymous namespace,
// otherwise it's function name will be removed in the backtrace.
std::string foo()
{
  std::ostringstream oss;
  // but if ASan is enabled, backtrace() returns one more frame, and the
  // backtrace would look like:
  //
  // ceph version Development (no_version)
  // 1: (ceph::ClibBackTrace::ClibBackTrace(int)+0xf5) [0x555555722bf5]
  // 2: (foo[abi:cxx11]()+0x1fc) [0x555555721b5c]
  // 3: (BackTrace_Basic_Test::TestBody()+0x2db) [0x55555572208b]
  //
  // so we need to skip one more frame
#if __has_feature(address_sanitizer) || defined(__SANITIZE_ADDRESS__)
  oss << ceph::ClibBackTrace(2);
#else
  oss << ceph::ClibBackTrace(1);
#endif
  return oss.str();
}

// a typical backtrace looks like:
//
// ceph version Development (no_version)
// 1: (foo[abi:cxx11]()+0x4a) [0x5562231cf22a]
// 2: (BackTrace_Basic_Test::TestBody()+0x28) [0x5562231cf2fc]
TEST(BackTrace, Basic) {
  std::string bt = foo();
  std::vector<std::string> lines;
  boost::split(lines, bt, boost::is_any_of("\n"));
  const unsigned lineno = 1;
  ASSERT_GT(lines.size(), lineno);
  ASSERT_EQ(lines[0].find(pretty_version_to_str()), 1U);
  std::regex e{"^ 1: "
#ifdef __FreeBSD__
		 "<foo.*>\\s"
		 "at\\s.*$"};
#else
		 "\\(foo.*\\)\\s"
		 "\\[0x[[:xdigit:]]+\\]$"};
#endif
  EXPECT_TRUE(std::regex_match(lines[lineno], e));
}
