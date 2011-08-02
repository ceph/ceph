// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/unit.h"
#include "common/Formatter.h"

#include <sstream>
#include <string>

using std::ostringstream;

TEST(JsonFormatter, Simple1) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "{\"a\":1,\"b\":2,\"c\":3}");
}

TEST(JsonFormatter, Simple2) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.open_object_section("bar");
  fmt.dump_int("int", 0xf00000000000ll);
  fmt.dump_unsigned("unsigned", 0x8000000000000001llu);
  fmt.dump_float("float", 1.234);
  fmt.close_section();
  fmt.dump_string("string", "str");
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "{\"bar\":{\"int\":263882790666240,\
\"unsigned\":9223372036854775809,\"float\":\"1.234000\"},\
\"string\":\"str\"}");
}

TEST(JsonFormatter, Empty) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "");
}
