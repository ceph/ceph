// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * Whee! A mini-demo for libfdb! 
 *
 * This shows how to take a user-defined data structure and both store and
 * retrieve it in FoundationDB. 
 *
*/

#include "rgw/fdb/interface.h"

#include <cstdio>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

namespace lfdb = ceph::libfdb;

using namespace std;

struct person
{
 string name;

 int year_born = 2000;

 vector<string> titles;

 bool operator==(const person&) const = default;
};

static const vector<person> nifty_people {
  {
    .name = "Albrecht Duerer",
    .year_born = 1471,
    .titles = { "Painter", "Printmaker", "Northern Renaissance artist" }
  },
  {
    .name = "Maria Theresa",
    .year_born = 1717,
    .titles = { "Habsburg ruler", "Archduchess of Austria", "Queen of Hungary and Bohemia" }
  }
};

static int run_example()
try
{
 auto dbh = lfdb::create_database();

 lfdb::set(dbh, "example/people", nifty_people);

 vector<person> people;
 if (not lfdb::get(dbh, "example/people", people) or people != nifty_people) {
  throw std::runtime_error("people did not round-trip through FoundationDB");
 }

 println("read {} people, some of them are likely nifty", people.size());

 return 0;
}
catch(const lfdb::libfdb_exception& e) {
    println(stderr, "libfdb exception: {}", e.what());
    return 1;
}
catch(const exception& e) {
    println(stderr, "exception: {}", e.what());
    return 1;
}
catch(...) {
    println(stderr, "Well, now we're in a real jam, huh?");
    return 1;
}

int main()
try
{
 const auto result = run_example();
 // The example's handles are released before the network thread is stopped.
 lfdb::shutdown_libfdb();

 return result;
}
catch (const std::exception& e) {
 println(stderr, "libfdb shutdown: {}", e.what());

 return 1;
}
