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

//JFW:
#include <fmt/format.h>

#ifndef CEPH_FDB_BINDINGS_H
 #define CEPH_FDB_BINDINGS_H

#include "base.h"

/* JFW:
namespace ceph::libfdb::detail {

bool get_value_from_transaction(transaction_handle& txn, std::string_view key, std::string& out_value);

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

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

/* JFW: b0rked
// JFW: we could also return an fdb_error_t, but that would introduce FDB artefacts into the public
// interface if we were to make it meaningful, and it's really unclear in the docs whether we can
// actually use/trust the enums or not-- need to revisit:
// JFW: there are two examples for this in the documentation and code, and it's very hard to figure
// out which one is "correct" with respect to retry-- I need to revisit this (hopefully the simpler
// version, block_until_ready() then future_get() or fail is ok in general), but until then...
inline bool get_value_from_transaction(transaction_handle& txn, std::string_view key, std::string& out_value);
{
 // Try to get the FUTURE from the TRANSACTION:
 const fdb_bool_t is_snapshot = false;
 future_value fv = fdb_transaction_get(txn->raw_handle(), (const uint8_t *)key.data(), key.size(), is_snapshot);

 // Try to get a VALUE from the FUTURE, with retry:
 fdb_bool_t key_was_found = false;
 const uint8_t *out_buffer = nullptr;
 int out_len = 0;

 // JFW: TODO: make configurable:
 for(int retries = 2; retries; --retries) {

  // AWAIT the FUTURE:
  if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
        // This is an "exceptional error"-- OOM, etc.-- no point retrying:
        throw fdb_exception(r);
  }

  if(fdb_error_t r = fdb_future_get_value(fv.raw_handle(), &key_was_found, &out_buffer, &out_len); 0 != r) {

    if(!key_was_found) {
      return false;
    }

    // Some other state:

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

    // No errors, but no value was found:
    if(!key_was_found) {
      return false;
    }
  }
 }

 // ...if we're here, we have our value!
 //    This being FDB, the documentation notes that SOMETIMES we may need to use the value before the future is
 //    destroyed. I guess we'd do that here, if that's what we needed...

 // Copy the future-owned contents into our self-owned string value:
 detail::buffer_to_string(out_buffer, out_len, out_value);

 return true;
}*/

// JFW: we could also return an fdb_error_t, but that would introduce FDB artefacts into the public
// interace if we were to make it meaningful:
inline bool get_value_from_transaction(transaction_handle& txn, std::string_view key, std::string& out_value)
{
 // Try to get the FUTURE from the TRANSACTION:
 const fdb_bool_t is_snapshot = false;

fmt::println("JFW: get_value_from_transaction(): {}", key);
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

  // Copy the future-owned contents into our self-owned string value:
  detail::buffer_to_string(out_buffer, out_len, out_value);

fmt::println("JFW: got {}", out_value);
  return true;
 }

 return false;
}


} // namespace ceph::libfdb::detail 

namespace ceph::libfdb {
/* JFW:
template <typename HandleType, typename K, typename V>
requires requires(HandleType h, K k, V v) { 
  { h->set(k, v) } -> std::same_as<void>;
}

inline void set(auto& h, auto k, auto v)
{
 h->set(k, v); 
}

inline void set(auto& h, auto k, auto v)
{
 check_fdb_result(h->set(k, v)); 
}
*/

//JFW:inline void set(transaction_handle& h, std::string_view k, std::string_view v)
template <typename K, typename V>
inline void set(transaction_handle h, K k, V v, const commit_after_op commit_after)
{
 h->set(k, v);
 
 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 h->commit();
}

template <typename K>
inline void erase(transaction_handle h, K k, const commit_after_op commit_after)
{
 // JFW: TODO
 if(commit_after_op::no_commit == commit_after) {
  return;
 }

 h->commit();
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

/* get() is a little fun:
fdb_error_t fdb_future_get_error(FDBFuture *future)
fdb_error_t fdb_future_get_double(FDBFuture *future, double *out)
fdb_error_t fdb_future_get_key_array(FDBFuture *f, FDBKey const **out_key_array, int *out_count)
fdb_error_t fdb_future_get_key(FDBFuture *future, uint8_t const **out_key, int *out_key_length)
fdb_error_t fdb_future_get_string_array(FDBFuture *future, const char ***out_strings, int *out_count)
fdb_error_t fdb_future_get_keyvalue_array(FDBFuture *future, FDBKeyValue const **out_kv, int *out_count, fdb_bool_t *out_more)
fdb_error_t fdb_future_get_value(FDBFuture *future, fdb_bool_t *out_present, uint8_t const **out_value, int *out_value_length)
*/

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, std::string& out_value)
{
 return ceph::libfdb::detail::get_value_from_transaction(txn, key, out_value);
}

} // namespace ceph::libfdb

#endif
