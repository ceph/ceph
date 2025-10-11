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

#include <map> // JFW: REMOVEME when iterators are properly lifted

#include <span>
#include <cstdint>
#include <concepts>
#include <iterator>
#include <exception>
#include <algorithm>
#include <functional>

/* JFW: needs tinkering
namespace ceph::libfdb::detail {

template <typename FnT>
auto commit_wrapper(FnT& f, ceph::libfdb::transaction_handle&& txn, const commit_after_op&& commit_after, auto&& ...params)
{
 auto r = f(txn, std::forward(params...));

 if(commit_after_op::commit == commit_after) {
    // Perhaps a tri-state return is the better long-term path?
    if(false == ceph::libfdb::detail::commit(txn))
     throw ceph::libfdb::libfdb_exception("transaction commit failed");
 }

 return r;
}

} // namespace ceph::libfdb::detail */

namespace ceph::libfdb {

inline database_handle make_database()
{
 return std::make_shared<database>();
}

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

// Note only rarely is a direct call to this.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

/* Equivalence with FDBStreamingMode:
 *
FDB_STREAMING_MODE_ITERATOR

The caller is implementing an iterator (most likely in a binding to a higher level language). The amount of data returned depends on the value of the iteration parameter to fdb_transaction_get_range().

FDB_STREAMING_MODE_SMALL

Data is returned in small batches (not much more expensive than reading individual key-value pairs).

FDB_STREAMING_MODE_MEDIUM

Data is returned in batches between _SMALL and _LARGE.

FDB_STREAMING_MODE_LARGE

Data is returned in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible. If the caller does not need the entire range, some disk and network bandwidth may be wasted. The batch size may be still be too small to allow a single client to get high throughput from the database.

FDB_STREAMING_MODE_SERIAL

Data is returned in batches large enough that an individual client can get reasonable read bandwidth from the database. If the caller does not need the entire range, considerable disk and network bandwidth may be wasted.

FDB_STREAMING_MODE_WANT_ALL

The caller intends to consume the entire range and would like it all transferred as early as possible.

FDB_STREAMING_MODE_EXACT

The caller has passed a specific row limit and wants that many rows delivered in a single batch.

enum struct streaming_mode_t : int {
 iterator	= FDB_STREAMING_MODE_ITERATOR,
 small		= FDB_STREAMING_MODE_SMALL,
 medium		= FDB_STREAMING_MODE_MEDIUM,
 large		= FDB_STREAMING_MODE_LARGE,
 serial		= FDB_STREAMING_MODE_SERIAL,
 all		= FDB_STREAMING_MODE_WANT_ALL,
3F exact		= FDB_STREAMING_MODE_EXACT
};

...these are not defined in terms of int or enum as far as I can tell, needs more exploring.
*/
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

// RAII wrapper:
struct maybe_commit final
{
 transaction_handle& txn;

 commit_after_op commit_after;

 maybe_commit(transaction_handle& txn_, const commit_after_op commit_after_)
  : txn(txn_), commit_after(commit_after_)
 {}

 ~maybe_commit()
 {
  if(std::uncaught_exceptions() || commit_after_op::commit != commit_after) {
   return;
  }

  txn->commit();
 }
};

} // namespace ceph::libfdb::detail

// JFW: these can maybe go away (or move?):

namespace ceph::libfdb::detail {

// Core dispatch from internal DB value to external concrete value:
[[deprecated]] void reify_value(const uint8_t *buffer, const size_t buffer_size, auto& target)
{
 return ceph::libfdb::from::convert(std::span { buffer, buffer_size }, target);
}

} // namespace ceph::libfdb::detail */

namespace ceph::libfdb {

inline void set(transaction_handle txn, std::string_view k, const auto& v, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->set(detail::as_fdb_span(k), ceph::libfdb::to::convert(v));
}

inline void set(transaction_handle txn, std::string_view k, const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh, std::string_view k, const auto& v)
{
 return set(make_transaction(dbh), k, v, commit_after_op::commit);
}

// JFW: this needs lifting on the iterator types:
inline void set(transaction_handle txn, std::map<std::string, std::string>::const_iterator b, std::map<std::string, std::string>::const_iterator e, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 std::for_each(b, e, [&txn](const auto& kv) {
            txn->set(detail::as_fdb_span(kv.first), ceph::libfdb::to::convert(kv.second)); 
           });
}

// JFW: this is a glaring embarassment in the interface-- ok for now as I'm just trying to get things to
// work for the prototype, but I've /got/ to find another way to do this.
inline void set(transaction_handle txn, const char *k, const char *v, ceph::libfdb::commit_after_op commit_after)
{
 // Since we've got a static extent key, we need to be sure we convert data appropriately:
 return txn->set(detail::as_fdb_span(k), detail::as_fdb_span(v));
}

//JFW: this likely needs disambiguation of iterator vs. const char *
inline void set(database_handle dbh, auto b, auto e)
{
 return set(make_transaction(dbh), b, e, commit_after_op::commit);
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// erase() is clear() in FDB parlance:
inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->erase(key_range);
}

inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range)
{
 return erase(txn, key_range, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, const ceph::libfdb::select& key_range)
{
 return erase(ceph::libfdb::make_transaction(dbh), key_range, commit_after_op::commit);
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->erase(detail::as_fdb_span(k));
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k)
{
 return erase(txn, k, commit_after_op::commit);
}

inline void erase(ceph::libfdb::database_handle dbh, std::string_view k)
{
 return erase(make_transaction(dbh), k);
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// get() with selector:
// JFW: Satisfying output_iterator is not as straightforward as it appears, I need to look at this mechanism again:
inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, auto out_iter, const ceph::libfdb::commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->get(detail::as_fdb_span(key_range.begin_key), detail::as_fdb_span(key_range.end_key), out_iter);
}

inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, auto out_iter)
{
 return get(txn, key_range, out_iter, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh, const ceph::libfdb::select& key_range, auto out_iter)
{
 return get(ceph::libfdb::make_transaction(dbh), key_range, out_iter, ceph::libfdb::commit_after_op::commit);
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto& out_value, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 auto vc = detail::value_collector(out_value); 

 return txn->get(detail::as_fdb_span(key), vc);
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto& out_value)
{
 return get(txn, key, out_value, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh, std::string_view key, auto& out_value)
{
 return get(ceph::libfdb::make_transaction(dbh), key, out_value, commit_after_op::commit);
}

// The user can provide an immediate conversion function (no function_ref until C++26):
// JFW: std::remove_cvref_t
inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto&& fn, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->get(detail::as_fdb_span(key), fn);
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto&& fn)
{
 return get(txn, key, fn, commit_after_op::commit);
}

inline bool get(ceph::libfdb::database_handle dbh, std::string_view key, auto&& fn)
{
 return get(ceph::libfdb::make_transaction(dbh), key, fn, commit_after_op::commit);
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// Does a key exist?
inline bool key_exists(transaction_handle txn, std::string_view k, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->key_exists(k);
}

inline bool key_exists(transaction_handle txn, std::string_view k)
{
 return key_exists(txn, k, commit_after_op::no_commit);
}

inline bool key_exists(database_handle dbh, std::string_view k)
{
 return key_exists(ceph::libfdb::make_transaction(dbh), k, commit_after_op::commit);
}

} // namespace ceph::libfdb


#endif
