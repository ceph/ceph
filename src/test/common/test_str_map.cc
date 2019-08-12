// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <gtest/gtest.h>

#include "include/str_map.h"

using namespace std;

TEST(str_map, json) {
  map<string,string> str_map;
  stringstream ss;
  // well formatted
  ASSERT_EQ(0, get_json_str_map("{\"key\": \"value\"}", ss, &str_map));
  ASSERT_EQ("value", str_map["key"]);
  // well formatted but not a JSON object
  ASSERT_EQ(-EINVAL, get_json_str_map("\"key\"", ss, &str_map));
  ASSERT_NE(string::npos, ss.str().find("must be a JSON object"));
}

TEST(str_map, plaintext) {
  {
    map<string,string> str_map;
    ASSERT_EQ(0, get_str_map(" foo=bar\t\nfrob=nitz   yeah right=   \n\t",
			     &str_map));
    ASSERT_EQ(4u, str_map.size());
    ASSERT_EQ("bar", str_map["foo"]);
    ASSERT_EQ("nitz", str_map["frob"]);
    ASSERT_EQ("", str_map["yeah"]);
    ASSERT_EQ("", str_map["right"]);
  }
  {
    map<string,string> str_map;
    ASSERT_EQ(0, get_str_map("that", &str_map));
    ASSERT_EQ(1u, str_map.size());
    ASSERT_EQ("", str_map["that"]);
  }
  {
    map<string,string> str_map;
    ASSERT_EQ(0, get_str_map(" \t \n ", &str_map));
    ASSERT_EQ(0u, str_map.size());
    ASSERT_EQ(0, get_str_map("", &str_map));
    ASSERT_EQ(0u, str_map.size());
  }
  {
    map<string,string> str_map;
    ASSERT_EQ(0, get_str_map(" key1=val1; key2=\tval2; key3\t = \t val3; \n ", &str_map, "\n;"));
    ASSERT_EQ(4u, str_map.size());
    ASSERT_EQ("val1", str_map["key1"]);
    ASSERT_EQ("val2", str_map["key2"]);
    ASSERT_EQ("val3", str_map["key3"]);
  }
}

TEST(str_map, empty_values) {
  {
    map<string,string> str_map;
    ASSERT_EQ(0, get_str_map("M= P= L=",
			     &str_map));
    ASSERT_EQ(3u, str_map.size());
    ASSERT_EQ("", str_map["M"]);
    ASSERT_EQ("", str_map["P"]);
    ASSERT_EQ("", str_map["L"]);
  }
}

/* 
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_str_map && 
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_str_map
 *   "
 * End:
 */
