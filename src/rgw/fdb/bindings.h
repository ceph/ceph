// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
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

// JFW: this can go away once iterator types are lifted away:
#include <map> 

#include <span>
#include <cstdint>
#include <concepts>
#include <iterator>
#include <exception>
#include <algorithm>
#include <functional>
#include <filesystem>

namespace ceph::libfdb {

inline database_handle create_database()
{
 return std::make_shared<database>();
}

inline database_handle create_database(const std::filesystem::path dbfile)
{
 return std::make_shared<database>(dbfile);
}

inline database_handle create_database(const std::filesystem::path dbfile, const database_options& dbopts, const network_options& netopts)
{
 return std::make_shared<database>(dbfile, dbopts, netopts);
}

inline database_handle create_database(const database_options& dbopts, const network_options& netopts)
{
 return std::make_shared<database>(dbopts, netopts);
}

inline database_handle create_database(const database_options& opts)
{
 return std::make_shared<database>(opts, network_options{});
}

inline database_handle create_database(const std::filesystem::path dbfile, const database_options& dbopts)
{
 return std::make_shared<database>(dbfile, dbopts, network_options{});
}

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

inline transaction_handle make_transaction(database_handle dbh, const transaction_options& opts)
{
 return std::make_shared<transaction>(dbh, opts);
}
// Note: only rarely is a direct call to this needed.
// Note: after a transaction is committed, it cannot be used again; but right now, that is NOT
// an error with respect to the object. So, don't do operations on the object after you've committed
// it or the behavior could be surpising.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// Algorithm helper for gathering converted values into a container (out_values):
auto value_collector(auto& out_values)
{
 return [&out_values](std::span<const std::uint8_t> out_data) {
            ceph::libfdb::from::convert(out_data, out_values);
        }; 
}

// RAII gadgetry for simplifying some repetetive functions:
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

namespace ceph::libfdb {

inline void set(transaction_handle txn, std::string_view k, const auto& v, const commit_after_op commit_after)
{
 detail::maybe_commit mc(txn, commit_after);

 return txn->set(detail::as_fdb_span(k), ceph::libfdb::to::convert(v));
}

// If someone gives us an explicit transaction handle, they almost certainly don't want to commit 
// it (though they can always specify otherwise):
inline void set(transaction_handle txn, std::string_view k, const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

// ...conversely, with a database handle given, we can assume they DO want to auto-commit:
inline void set(database_handle dbh, std::string_view k, const auto& v)
{
 return set(make_transaction(dbh), k, v, commit_after_op::commit);
}

// JFW: this needs lifting on the iterator types; being limited to std::map<s, s> is purely artifical:
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
 return erase(txn, k, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, std::string_view k)
{
 return erase(make_transaction(dbh), k, commit_after_op::commit);
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// get() with selector:
// JFW: Satisfying output_iterator is not as straightforward as it appears, I need to look at this mechanism again; meanwhile, the template doesn't
// /prevent/ future type narrowing:
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
 return get(txn, key, fn, commit_after_op::no_commit);
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

