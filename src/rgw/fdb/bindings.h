// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include <iterator>
#include <algorithm>

namespace ceph::libfdb {

inline database_handle make_database()
{
 return std::make_shared<database>();
}

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

inline std::pair<std::string, std::string> to_string_pair(const FDBKeyValue kv)
{
 return {
          { (const char *)kv.key, static_cast<std::string::size_type>(kv.key_length) },
          { (const char *)kv.value, static_cast<std::string::size_type>(kv.value_length) }
        };
}

// The alternatives were "spanlike" or even "Span-ish", but that was a little /too/ cute; thanks
// to Adam Emerson for the coinage:
auto ptr_and_sz(const auto& spanoid)
{
 return std::tuple { spanoid.data(), spanoid.size() };
} 

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

// Core dispatch from internal DB value to external concrete value:
void reify_value(const uint8_t *buffer, const size_t buffer_size, auto& target)
{
 return ceph::libfdb::from::convert(std::span { buffer, buffer_size }, target);
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

inline bool get_single_value_from_transaction(transaction_handle& txn, std::span<const std::uint8_t> key, auto& out_value);

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

/* JFW: key selectors are another area of FDB that I need to give more thought to before exposing to the public interface--
it may be that some operator overloading is natural and pleasant, or that it's got some critical issue. This doesn't feel
bad, for instance:
  get(txn, begin_key < end_key);
  get(txn, begin_key <= end_key);

FDBFuture *fdb_transaction_get_key(
  FDBTransaction *transaction, 
  uint8_t const *key_name, int key_name_length, 
  fdb_bool_t or_equal, 
  int offset, 
  fdb_bool_t snapshot)

...for now, we're going to offer just one query and an overload to handle the interval (as there currently are
no standard intervals that I'm aware of).

Details (looks simple, but like many things in here gets complex quickly):
https://apple.github.io/foundationdb/developer-guide.html#key-selectors

Ok, some forward motion- instead of thinking of this as an interval, I'm getting milage from the selector idea 
in the library, and especially std::string::compare();

*/
// JFW: TODO: implement/expose remaining features (key selectors, batching and chunking, etc.):
inline future_value get_range_future_from_transaction(transaction_handle& txn, std::string_view begin_key, std::string_view end_key)
{
  // By default, give (begin, end):
  constexpr bool begin_or_eq = true;
  constexpr bool end_or_eq   = true;

  constexpr int begin_offset = 0;
  constexpr int end_offset = 1;

  constexpr int limit = 0;		// 0 == unlim; else max number of pairs to return at once (more() function)
  constexpr int target_bytes = 0;	// 0 == unlim; else enables more() function of fdb_future_get_keyvalue_array()
  constexpr FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
  constexpr bool is_snapshot = false;
  constexpr bool reverse = false;

  int iterations = 1; // should start at one, is incremented when streaming_mode_t::iterator is enabled, else ignored

  return fdb_transaction_get_range(
		      txn->raw_handle(),
		      (const uint8_t *)begin_key.data(), begin_key.size(),	// the reference-point key (begin_key)
		      begin_or_eq,
		      begin_offset,

		      // These components form an "end key selector":
		      (const uint8_t *)end_key.data(), end_key.size(),
		      end_or_eq, 
		      end_offset,

		      // How should results be grouped/chunked:
		      limit, 
		      target_bytes, 
		      streaming_mode, // streaming_mode_t
		      iterations,

		      // Other options:
		      is_snapshot,	// 0 unless this IS a snapshot read
		      reverse		// should items come in reverse order?
		    );
}

// JFW: TODO: consolidate this with get_value_from_transaction()! See details there.
// JFW:		(it's a bit trickier than it looks at first...)
inline bool get_value_range_from_transaction(transaction_handle& txn, std::string_view begin_key, std::string_view end_key, auto out_iter)
{
 const fdb_bool_t is_snapshot = false;

 for(;;) {
  future_value fv = get_range_future_from_transaction(txn, begin_key, end_key);

  if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
    throw fdb_exception(r);
  }

  const FDBKeyValue *out_kvs;
  int out_count = 0;		// updated by FDB's read
  fdb_bool_t out_more = false;	// true if there's more to read

  if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &out_more); 0 != r) {

    auto fv2 = future_value(fdb_transaction_on_error(txn->raw_handle(), r));

    if(fdb_error_t r2 = fdb_future_block_until_ready(fv2.raw_handle()); 0 != r2) {
      throw fdb_exception(r);
    }

    continue;
  }

  std::transform(out_kvs, out_count + out_kvs, out_iter, detail::to_string_pair);

  return true;
 }

 return false;
}

} // namespace ceph::libfdb::detail 

namespace ceph::libfdb {

template <typename K, typename V>
inline void set(transaction_handle h, K k, V v, const commit_after_op commit_after)
{
 h->set(ceph::libfdb::to::convert(k), ceph::libfdb::to::convert(v));
 
 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 h->commit();
}

template <std::input_iterator PairIter>
inline void set(transaction_handle h, PairIter b, PairIter e, const commit_after_op commit_after)
{
 std::for_each(b, e, [&h](const auto& kv) {
  h->set(ceph::libfdb::to::convert(kv.first), ceph::libfdb::to::convert(kv.second)); 
 });

 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 h->commit();
}

// erase() is clear() in FDB parlance:
template <typename K>
inline void erase(transaction_handle h, K k, const commit_after_op commit_after)
{
 h->erase(ceph::libfdb::to::convert(k));

 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 h->commit();
}

/* get() is a little fun:

I'm ignoring these for the moment:
fdb_error_t fdb_future_get_error(FDBFuture *future)

fdb_error_t fdb_future_get_key(FDBFuture *future, uint8_t const **out_key, int *out_key_length)
fdb_error_t fdb_future_get_key_array(FDBFuture *f, FDBKey const **out_key_array, int *out_count)

fdb_error_t fdb_future_get_string_array(FDBFuture *future, const char ***out_strings, int *out_count)
fdb_error_t fdb_future_get_keyvalue_array(FDBFuture *future, FDBKeyValue const **out_kv, int *out_count, fdb_bool_t *out_more)

fdb_error_t fdb_future_get_double(FDBFuture *future, double *out)

...these will be well-defined in our prototype:
fdb_error_t fdb_future_get_int64(FDBFuture *future, int64_t *out)
fdb_error_t fdb_future_get_value(FDBFuture *future, fdb_bool_t *out_present, uint8_t const **out_value, int *out_value_length)

The next revision of this I will probably go back to my earlier idea of using a variant, better capturing recursive structures and the like.

Each of these concrete types has a corresponding reification (reify_buffer()).

Note that just because there IS an explicit implementation, we may still forward to the generic one-- it's
more a statement of what interfaces we always and explicitly support.

...rather than bare string_view, we'll have a Concept-ified parameter for valid in-params, but for now I want to keep this
simple and frankly focus on stability and getting the core mechanics right while being useful for MOST stuff-- so, string_view and string it is!.
*/

inline bool get(ceph::libfdb::transaction_handle txn, auto key, std::int64_t& out_value)
{
 return ceph::libfdb::detail::get_single_value_from_transaction(txn, ceph::libfdb::to::convert(key), out_value);
}

inline bool get(ceph::libfdb::transaction_handle txn, auto key, auto& out_value)
{
 return ceph::libfdb::detail::get_single_value_from_transaction(txn, ceph::libfdb::to::convert(key), out_value);
}

inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::concepts::selector auto& key_range, auto out_iter)
{
 return ceph::libfdb::detail::get_value_range_from_transaction(txn, key_range.begin_key, key_range.end_key, out_iter);
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// JFW: TODO: consolidate this with get_value_range_from_transaction()!
// JFW: integrate this back into base::transaction or ...
// JFW: we could also return an fdb_error_t, but that would introduce FDB artefacts into the public
// interace if we were to make it meaningful:
// JFW: This probably needs to go back into base.h;
// JFW: I think there's a simpler and also-"approved" way to do this-- I'll look at it later, getting rid
// of the strangeness and complexity here would be good:
inline bool get_single_value_from_transaction(transaction_handle& txn, std::span<const std::uint8_t> key, auto& out_value)
{
 // Try to get the FUTURE from the TRANSACTION:
 const fdb_bool_t is_snapshot = false;

 for(;;) {

  future_value fv = fdb_transaction_get(txn->raw_handle(), (const uint8_t *)key.data(), key.size(), is_snapshot);

  // AWAIT the FUTURE:
  if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
        // This is an "exceptional error"-- OOM, etc.-- no point retrying:
        throw fdb_exception(r);
  }

  // Try to get a VALUE from the FUTURE:
  fdb_bool_t key_was_found = false;
  const uint8_t *out_buffer = nullptr;
  int out_len = 0;


/* JFW: We may want to handle certain conversions differently here in the future:
    if fdb int64, double, key_array, key string_array, keyvalue_array
      ...use specialized FDB functions... 
*/

  // fdb_future_get_value() is used below:
  if(fdb_error_t r = fdb_future_get_value(fv.raw_handle(), &key_was_found, &out_buffer, &out_len); 0 != r) {

    // Since we're in a transaction, we might be able to retry; this function implements
    // retry and backoff, knowing which errors are or are not temporary (the future this
    // returns represents an empty value):
    auto fv2 = future_value(fdb_transaction_on_error(txn->raw_handle(), r));

    if(fdb_error_t r2 = fdb_future_block_until_ready(fv2.raw_handle()); 0 != r2) {
      // A "bona fide" error, report the "original":
      throw fdb_exception(r);
    }

    // If we're here, then fdb_transaction_on_error() thinks we should retry; however there are some
    // cases where it may not have been able to decide, and we'll need to heuristically understand what
    // to do; the manual mentions commit_unknown_results:
//JFW: this is weird and tricky
//    if(commit_unknown_result == r2) {
// JFW: DON'T FORGET ME!!!
   // }

    // Retry:
    continue;
  }

  // ...if we're here, we have our value!
  //    This being FDB, the documentation notes that SOMETIMES we may need to use the value before the future is
  //    destroyed. I guess we'd do that here, if that's what we needed...

  // No errors, but no value was found:
  if(0 == key_was_found)
   return false;

  // Copy the future-owned contents into our self-owned value:
  std::span<const std::uint8_t> in_view { (const std::uint8_t *)out_buffer, (size_t)out_len };
  ceph::libfdb::from::convert(in_view, out_value);

  return true;
 }

 return false;
}

} // namespace ceph::libfdb::detail 

#endif
