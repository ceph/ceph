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
 * Store a vector of user-defined records under one FoundationDB key, then read
 * it back. Compare manual commit with owning and borrowed transactor calls,
 * compose reusable transaction bodies, and publish output after confirmed commit.
 *
 * The final examples use interval algebra to describe which keys to read. They
 * narrow a namespace to one key, combine and exclude namespaces, and turn an
 * exclusion set back into a selection. These expressions select keys; the stored
 * vector remains one value containing both people.
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

// The codec supports this aggregate's string, integer, and vector fields without
// an application-specific serializer:
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

// Typed application errors distinguish rejected input or missing data from native
// database failures:
enum struct people_error { empty_name, missing_people };

// This reusable body validates before writing and leaves commit to its caller:
static auto store_people(lfdb::transaction_handle& txn, const vector<person>& people)
 -> expected<size_t, people_error>
{
 if (ranges::any_of(people, [](const auto& name) { return name.empty(); }, &person::name)) {
  return unexpected(people_error::empty_name);
 }

 lfdb::set(txn, "example/people", people);

 return people.size();
}

// Treat a missing key as an application failure so a composed body can stop before
// doing any work that depends on the records:
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

// An uncertain commit may already have taken effect, so report completion knowledge
// alongside application, native, or C++ failures from either invocation form:
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
 // Use the default cluster configuration for all of this example's transactions:
 auto dbh = lfdb::create_database();

 // With manual ownership, buffer a write and explicitly check whether commit succeeds:
 auto txn = lfdb::make_transaction(dbh);

 lfdb::set(txn, "example/people", nifty_people);

 if (const auto committed = lfdb::commit(txn); not committed) {
  report_failure(committed.error());

  return 1;
 }

 println("manual commit: confirmed");

 // An owning transactor creates the transaction, runs the body, handles native
 // recovery, and commits. Success carries both the body's value and completion status:
 const auto txr = lfdb::make_transactor(dbh);

 const auto stored = txr(store_people, nifty_people);

 if (not stored) {
  report_failure(stored.error());

  return 1;
 }

 println("managed transaction: stored {} people", stored->value);

 // Compose a read followed by a validated rewrite in the supplied transaction.
 // and_then skips the write if the read fails; neither borrowed call commits.
 // The same body works under a borrowed or owning invocation, keeping the complete
 // chain inside that invocation's exception boundary:
 auto round_trip = [&](auto& current) {
  return txr(current, read_people).and_then([&](auto&& people) {
   return txr(current, store_people, people);
  });
 };

 // Borrowing lets a caller combine this body with other work in its own transaction:
 auto caller_txn = lfdb::make_transaction(dbh);

 const auto working = txr(caller_txn, round_trip);

 if (not working) {
  report_failure(working.error());

  return 1;
 }

 println("borrowed transaction: prepared {} people before commit", *working);

 // A successful borrowed body has only prepared writes; the caller still owns commit:
 if (const auto caller_commit = lfdb::commit(caller_txn); not caller_commit) {
  report_failure(caller_commit.error());

  return 1;
 }

 println("caller commit: confirmed");

 // Omitting the transaction gives txr ownership of the same composed body and its commit:
 const auto composed = txr(round_trip);

 if (not composed) {
  report_failure(composed.error());

  return 1;
 }

 println("composed transaction: stored {} people", composed->value);

 // Retries must not leak intermediate output. staged(in_place, ...) supplies a fresh
 // vector per attempt and replaces published_people only after confirmed commit:
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
 // effects need their own idempotency strategy. Opt into replay after an uncertain
 // commit only because this replacement is safe to repeat:
 const auto replaying = lfdb::make_transactor(dbh, {}, lfdb::retry_policy::replay_unknown);

 const auto replaced = replaying(store_people, nifty_people);

 if (not replaced) {
  report_failure(replaced.error());

  return 1;
 }

 println("explicit replay_unknown: replaced {} people", replaced->value);

 // An empty name rejects the body before it writes. Verify that the owner reports
 // an application error without submitting a commit:
 if (const auto rejected = txr(store_people, vector<person> {person {}});
     rejected or not holds_alternative<lfdb::application_error<people_error>>(rejected.error().cause) or
     rejected.error().status.commit != lfdb::commit_status::not_submitted) {
  throw runtime_error("empty name did not reject the attempt before commit");
 }

 println("application rejection: empty name, no commit submitted");

 // Read through the database handle and compare with the input to verify that the
 // complete vector of records survived serialization and storage:
 vector<person> people;
 if (not lfdb::get(dbh, "example/people", people) or people != nifty_people) {
  throw std::runtime_error("people did not round-trip through FoundationDB");
 }

 println("read {} people, some of them are likely nifty", people.size());

 namespace q = lfdb::query;

 // Queries describe sets of keys, independently of their stored values. Start with
 // every key beginning with "example/" and narrow the selection to our people record.
 // These expressions are evaluated locally; collect() below performs the read:
 auto selected = q::prefix("example/");

 // Intersection with an inclusive lower bound discards earlier keys in the namespace:
 q::assign_intersection(selected, q::at_or_after("example/people"));

 // &= is also intersection assignment. The inclusive upper bound leaves only this key:
 selected &= q::at_or_before("example/people");

 // Collect returns key/value pairs and decodes each value as vector<person>.
 // Read the selected key, whose value contains both people:
 const auto rows = lfdb::collect<vector<person>>(dbh, std::move(selected));

 if (1 != rows.size() || "example/people" != rows.front().first || rows.front().second != nifty_people) {
  throw runtime_error("intersection assignment did not select the stored people");
 }

 println("intersection assignment: selected {} key", rows.size());

 // An interval_set can hold several disjoint ranges. Model a maintenance selection
 // over example and archive records, with private records excluded and a toggle for
 // archive membership. These operations change the selection, not the stored data.
 // Union (|) admits keys from either namespace:
 q::interval_set visible {q::prefix("example/") | q::prefix("archive/")};

 // Difference (-=) excludes private records without deleting them:
 visible -= q::prefix("example/private/");

 // Symmetric difference (^=) toggles membership throughout the archive prefix.
 // That whole prefix is already included, so this toggle removes it:
 visible ^= q::prefix("archive/");

 // Intersection (&=) restricts this pass to the exact people key:
 visible &= q::at("example/people");

 // Complement (~) selects all other ordinary keys, expressing an exclusion set:
 q::interval_set excluded {~visible};

 // In-place complement converts the exclusion set back to the original selection:
 q::assign_complement(excluded);

 // Read the restored selection to check that the set operations still choose our record:
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
