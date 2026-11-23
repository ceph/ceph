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

#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <concepts>
#include <cstdio>
#include <exception>
#include <expected>
#include <print>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
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

enum struct people_error { empty_name, missing_people };

static auto store_people(lfdb::transaction_handle& txn, const vector<person>& people)
 -> expected<size_t, people_error>
{
 if (ranges::any_of(people, [](const auto& name) { return name.empty(); }, &person::name)) {
  return unexpected(people_error::empty_name);
 }

 lfdb::set(txn, "example/people", people);

 return people.size();
}

static auto read_people(lfdb::transaction_handle& txn)
{
 expected<vector<person>, people_error> people {in_place};

 if (not lfdb::get(txn, "example/people", *people)) {
  people = unexpected(people_error::missing_people);
 }

 return people;
}

static void report_exception(const exception_ptr& failure)
try
 {
  rethrow_exception(failure);
 }
catch (const std::exception& error)
 {
  println(stderr, "{}", error.what());
 }
catch (...)
 {
  println(stderr, "Nonstandard C++ exception");
 }

template <typename ErrorT>
static void report_failure(const lfdb::transaction_failure<ErrorT>& failure)
{
 println(stderr, "Transaction failed in phase {} after {} attempt(s); uncertain: {}, may finish later: {}",
         to_underlying(failure.phase), failure.status.attempts,
         failure.status.has_unknown_commit(), failure.status.may_commit_later());

 if (failure.status.first_unknown_commit_error) {
  println(stderr, "First uncertain commit error: {}", failure.status.first_unknown_commit_error->message());
 }

 if constexpr (not is_void_v<ErrorT>) {
  if (const auto *rejection = get_if<lfdb::application_error<ErrorT>>(&failure.cause)) {
   println(stderr, "Application error: {}", static_cast<int>(rejection->error));

   return;
  }
 }

 if (const auto *native = get_if<lfdb::fdb_error>(&failure.cause)) {
  println(stderr, "{} {}: {}", native->code.category().name(), native->code.value(), native->code.message());

  if (native->exception) {
   report_exception(native->exception);
  }

  visit([](const auto& recovery) {
   using recovery_type = remove_cvref_t<decltype(recovery)>;

   if constexpr (same_as<recovery_type, error_code>) {
    println(stderr, "Recovery failed: {} {}: {}",
            recovery.category().name(), recovery.value(), recovery.message());
   }

   if constexpr (same_as<recovery_type, exception_ptr>) {
    report_exception(recovery);
   }
  }, native->retry_error);

  return;
 }

 report_exception(get<exception_ptr>(failure.cause));
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

 // The same body runs either in an existing transaction or under an owning call.
 // The complete and_then chain stays inside the invocation's exception boundary:
 auto round_trip = [&](auto& current) {
  return txr(current, read_people).and_then([&](auto&& people) {
   return txr(current, store_people, people);
  });
 };

 auto caller_txn = lfdb::make_transaction(dbh);

 const auto working = txr(caller_txn, round_trip);

 if (not working) {
  report_failure(working.error());

  return 1;
 }

 println("borrowed transaction: prepared {} people before commit", *working);

 if (const auto caller_commit = lfdb::commit(caller_txn); not caller_commit) {
  report_failure(caller_commit.error());

  return 1;
 }

 println("caller commit: confirmed");

 const auto composed = txr(round_trip);

 if (not composed) {
  report_failure(composed.error());

  return 1;
 }

 println("composed transaction: stored {} people", composed->value);

 // Publish the borrowed read's result only after the owning call confirms commit:
 vector<person> published_people;

 const auto published = txr([&](auto& current, auto& output) {
  return txr(current, read_people).transform([&output](auto&& people) {
   output = std::move(people);
  });
 }, lfdb::staged(in_place, published_people));

 if (not published) {
  report_failure(published.error());

  return 1;
 }

 if (published_people != nifty_people) {
  throw runtime_error("staged people did not publish after confirmed commit");
 }

 println("staged publication: loaded {} people after commit", published_people.size());

 // Replacing this key with the same value tolerates repeated commits. Incremental
 // effects need their own idempotency strategy before opting into this policy.
 const auto replaying = lfdb::make_transactor(dbh, {}, lfdb::retry_policy::replay_unknown);

 const auto replaced = replaying(store_people, nifty_people);

 if (not replaced) {
  report_failure(replaced.error());

  return 1;
 }

 println("explicit replay_unknown: replaced {} people", replaced->value);

 if (const auto rejected = txr(store_people, vector<person> {person {}});
     rejected or not holds_alternative<lfdb::application_error<people_error>>(rejected.error().cause) or
     rejected.error().status.commit != lfdb::commit_status::not_submitted) {
  throw runtime_error("empty name did not reject the attempt before commit");
 }

 println("application rejection: empty name, no commit submitted");

 vector<person> people;
 if (not lfdb::get(dbh, "example/people", people) or people != nifty_people) {
  throw std::runtime_error("people did not round-trip through FoundationDB");
 }

 println("read {} people, some of them are likely nifty", people.size());

 namespace q = lfdb::query;

 auto selected = q::prefix("example/");
 q::assign_intersection(selected, q::at_or_after("example/people"));
 selected &= q::at_or_before("example/people");

 const auto rows = lfdb::collect<vector<person>>(dbh, std::move(selected));

 if (1 != rows.size() || "example/people" != rows.front().first || rows.front().second != nifty_people) {
  throw runtime_error("intersection assignment did not select the stored people");
 }

 println("intersection assignment: selected {} key", rows.size());

 q::interval_set visible {q::prefix("example/") | q::prefix("archive/")};
 visible -= q::prefix("example/private/");
 visible ^= q::prefix("archive/");
 visible &= q::at("example/people");

 q::interval_set excluded {~visible};
 q::assign_complement(excluded);

 const auto selected_people = lfdb::collect<vector<person>>(dbh, excluded);

 if (1 != selected_people.size() || "example/people" != selected_people.front().first ||
     selected_people.front().second != nifty_people) {
  throw runtime_error("set operators did not select the stored people");
 }

 println("set operators: selected {} key", selected_people.size());

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
