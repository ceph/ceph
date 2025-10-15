// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. and Jesse Williamson
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#ifndef CEPH_FDB_BINDINGS_H
 #define CEPH_FDB_BINDINGS_H

#include "base.h"
#include "conversion.h"

#include <span>
#include <cstdint>
#include <concepts>
#include <iterator>
#include <algorithm>
#include <functional>

#include <fmt/format.h> // JFW:

namespace ceph::libfdb {

inline database_handle make_database()
{
 return std::make_shared<database>();
}

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

// Note only rarely is a direct call to this requireda.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// The alternatives were "spanlike" or even "Span-ish", but that was a little /too/ cute; thanks
// to Adam Emerson for the coinage:
auto ptr_and_sz(const auto& spanoid)
{
 return std::tuple { spanoid.data(), spanoid.size() };
} 

auto value_collector(auto& out_value)
{
 return [&out_value](std::span<const std::uint8_t> out_data) {
            ceph::libfdb::from::convert(out_data, out_value);
        }; 
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

/* JFW:
// Used iternally to get values from futures:
struct transaction_value_result
{
 fdb_bool_t key_was_found = false;
 
 fdb_bool_t is_snapshot = false;

 // Kept here to extend the future's (and data within it) lifetime:
 future_value fv = nullptr;

 // JFW: this can be a variant; the data lives only as long as the future_value lives:
 // By the time we have an instance of transaction_value_result(), this must have already
 // been provided by the appropriate FDB function:
 const std::span<const std::uint8_t> out_data;
};*/

// Core dispatch from internal DB value to external concrete value:
void reify_value(const uint8_t *buffer, const size_t buffer_size, auto& target)
{
 return ceph::libfdb::from::convert(std::span { buffer, buffer_size }, target);
}

// Grab a future and its related result from a request:
inline bool get_single_value_from_transaction(transaction_handle& txn, const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto& write_output);

inline future_value get_range_future_from_transaction(transaction_handle& txn, std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key);

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

template <typename K, typename V>
inline void set(transaction_handle txn, const K& k, const V& v, const commit_after_op commit_after)
{
 txn->set(detail::as_fdb_span(k), ceph::libfdb::to::convert(v));
 
 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 txn->commit();
}

template <std::input_iterator PairIter>
inline void set(transaction_handle txn, PairIter b, PairIter e, const commit_after_op commit_after)
{
 std::for_each(b, e, [&txn](const auto& kv) {
  txn->set(detail::as_fdb_span(kv.first), ceph::libfdb::to::convert(kv.second)); 
 });

 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 txn->commit();
}

// erase() is clear() in FDB parlance:
//
// "[R]emove all keys (if any) which are lexicographically greater than or equal to the given begin key and 
// lexicographically less than the given end_key."
//
// JFW: See note about some current key-type limitations (we will reinvestigate when the prototype is
// starting to work, there's no hard need for the string type limitation, it's just most straightforward
// to deal with):
inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, const commit_after_op commit_after)
{
 txn->erase(key_range);

 if(commit_after_op::commit == commit_after) {
  txn->commit();
 }
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k, const commit_after_op commit_after)
{
 txn->erase(detail::as_fdb_span(k));

 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 txn->commit();
}

inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, auto out_iter, const commit_after_op commit_after)
{
 auto r = ceph::libfdb::detail::get_value_range_from_transaction(txn, detail::as_fdb_span(key_range.begin_key), detail::as_fdb_span(key_range.end_key), out_iter);

 if(commit_after_op::commit == commit_after) {
  txn->commit();
 }

 return r;
}

inline bool get(ceph::libfdb::transaction_handle txn, const auto& key, auto& out_value, const commit_after_op commit_after)
{
 auto vc = detail::value_collector(out_value);

 if(false == ceph::libfdb::detail::get_single_value_from_transaction(txn, detail::as_fdb_span(key), vc))
  return false;

 if(commit_after_op::commit == commit_after) {
  txn->commit();
 }

 return true;
}

// The user can provide an immediate conversion function (no function_ref until C++26):
inline bool get(ceph::libfdb::transaction_handle txn, auto& key, std::invocable auto fn, const commit_after_op commit_after)
{
 if(!ceph::libfdb::detail::get_single_value_from_transaction(txn, detail::as_fdb_span(key), fn))
  return false;

 if(commit_after_op::commit == commit_after) {
  txn->commit();
 }

 return true;
}

template <typename K>
inline bool key_exists(transaction_handle txn, K k)
{
 auto bit_bucket = [](auto) {};

 return ceph::libfdb::detail::get_single_value_from_transaction(txn, detail::as_fdb_span(k), bit_bucket);
}

} // namespace ceph::libfdb


#endif
