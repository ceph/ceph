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

#include <algorithm>
#include <cstdio>
#include <expected>
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

enum struct write_error { empty_name };

static auto store_people(lfdb::transaction_handle& txn, const vector<person>& people)
 -> expected<size_t, write_error>
{
 if (ranges::any_of(people, [](const auto& name) { return name.empty(); }, &person::name)) {
  return unexpected(write_error::empty_name);
 }

 lfdb::set(txn, "example/people", people);

 return people.size();
}

template <typename ErrorT>
static void report_failure(const lfdb::transaction_failure<ErrorT>& failure)
{
 println(stderr, "Transaction failed; uncertain: {}, may finish later: {}",
         failure.status.has_unknown_commit(), failure.status.may_commit_later());
 if (const auto *native = get_if<lfdb::fdb_error>(&failure.cause)) {
  println(stderr, "{} {}: {}", native->code.category().name(), native->code.value(), native->code.message());
 }
 if (const auto *exception = get_if<exception_ptr>(&failure.cause)) {
  try
   {
    rethrow_exception(*exception);
   }
  catch (const std::exception& error)
   {
    println(stderr, "{}", error.what());
   }
  catch (...)
   {
    println(stderr, "Nonstandard C++ exception");
   }
 }
}

static int run_example()
try
{
 auto dbh = lfdb::create_database();

 auto txn = lfdb::make_transaction(dbh);
 lfdb::set(txn, "example/people", nifty_people);
 if (const auto committed = lfdb::commit(txn); not committed) {
  report_failure(committed.error());
  return 1;
 }
 println("manual commit: confirmed");

 const auto txr = lfdb::make_transactor(dbh);
 const auto stored = txr(store_people, nifty_people);
 if (not stored) {
  report_failure(stored.error());
  return 1;
 }
 println("managed transaction: stored {} people", stored->value);

 if (const auto rejected = txr(store_people, vector<person> {person {}});
     rejected or not holds_alternative<lfdb::application_error<write_error>>(rejected.error().cause) or
     rejected.error().status.commit != lfdb::commit_status::not_submitted) {
  throw runtime_error("empty name did not reject the attempt before commit");
 }
 println("application rejection: empty name, no commit submitted");

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
