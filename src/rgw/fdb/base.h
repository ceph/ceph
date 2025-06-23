// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
      
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

#ifndef CEPH_FDB_BASE_H
 #define CEPH_FDB_BASE_H

// The API version we're writing against, which can (and probably does) differ
// from the installed version. This must be defined before the FoundationDB header
// is included:
#define FDB_API_VERSION 710 
#include <foundationdb/fdb_c.h> 

// Ceph uses libfmt rather than <format>:
#include <fmt/format.h>

#include <tuple>
#include <mutex>
#include <memory>
#include <ranges>
#include <algorithm>
#include <exception>
#include <filesystem>

namespace ceph::libfdb {

struct database;
struct transaction;
struct future_value;

using database_handle = std::shared_ptr<database>;
using transaction_handle = std::shared_ptr<transaction>;

struct fdb_exception final : std::runtime_error
{
 using std::runtime_error::runtime_error;

 fdb_error_t fdb_error_value = -1;

 explicit fdb_exception(fdb_error_t fdb_error_value)
  : std::runtime_error(make_fdb_error_string(fdb_error_value)),
    fdb_error_value(fdb_error_value)
 {}

 static std::string make_fdb_error_string(const fdb_error_t ec)
 {
  return fmt::format("FoundationDB error {}: {}", ec, fdb_get_error(ec));
 }
};

struct future_value
{
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

 public:
 FDBFuture *raw_handle() const noexcept { return future_ptr.get(); }
};

// Should we commit after the (possibly) mutating operation?
enum struct commit_after_op { commit, no_commit };

} // ceph::libfdb

namespace ceph::libfdb::detail {

// I tried fancier versions of this that wrapped the function and forwarded the parameters; unfortunately,
// FDB sometimes likes to use macros, which wind up making that scheme less orthogonal than it should be--
// which is unfortunate. This isn't as pretty, but "works with everything":
[[maybe_unused]] inline auto check_fdb_result(const fdb_error_t result)
{
 if(0 != result)
  throw fdb_exception(result);

 return result;
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

// FoundationDB likes to deal in uint8_t buffers, so we'll need this somewhat often:
inline void buffer_to_string(const uint8_t *buffer, int buffer_len, std::string& out)
{
#ifdef __cpp_lib_string_resize_and_overwrite
      out.resize_and_overwrite(out_len, [&out_len, &out_buffer](char *out_p, std::string::size_type n) noexcept {
            std::copy(buffer, buffer_len + buffer, std::begin(out));
          });
#else
    out.resize(buffer_len);
    std::copy(buffer, buffer_len + buffer, std::begin(out));
#endif
}

// The alternative was "Span-ish", but it was a little /too/ cute:
auto ptr_and_sz(const auto& spanlike)
{
 return std::tuple { spanlike.data(), spanlike.size() };
}

} // namespace ceph::libfdb::detail


////////////// ************* JFW

#include <thread>

namespace ceph::libfdb {

struct database;
struct transaction;

class database final
{
/* JFW: maybe move the global FDB stuff into a single static entity:
 static inline struct {
 } fdb_handle; */

 private:
 FDBDatabase *fdb_handle = nullptr;

 public: // JFW: TEMPORARY (move to smart ptr factory)
 database(const std::filesystem::path cluster_file_path);
 database();

 ~database();

 public:
 operator bool() { return nullptr != raw_handle(); }

 public:
 FDBDatabase *raw_handle() const noexcept { return fdb_handle; }

 private:
/*JFW: do this bookkeeping later:
 // These must be constructed as shared_ptr<> handles via make_database():
 private:
 database();

 private:
 friend database_handle make_database();

 template <typename T, typename ...XS>
 friend std::shared_ptr<T> std::make_shared(XS&& ...);
*/

 friend transaction;
};

// JFW: change access
class transaction final
{
 database_handle dbh;

 FDBTransaction *txn_handle = nullptr;

 public:
 transaction(database_handle& dbh);

 ~transaction();

 public:
 operator bool() { return dbh && nullptr != raw_handle(); }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_handle; }

 private:
// JFW: cautionary tales abound-- probably turn down the dial:
 void commit(); // JFW: always happens in dtor; maybe fix
/* void cancel(); // JFW: track for dtor
 void reset(); */

 void set(std::string_view k, std::string_view v) {
    // fdb_transaction_set() does not return a value:
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 void set_option(FDBTransactionOption o, std::string_view v) {
    detail::check_fdb_result(
      fdb_transaction_set_option(raw_handle(), o, (const std::uint8_t *)v.data(), v.length()));
 }

 private:

 template <typename K, typename V>
 friend inline void set(transaction_handle h, K k, V v, const commit_after_op commit_after);

 template <typename K>
 friend inline void erase(transaction_handle h, K k, const commit_after_op commit_after);

// friend inline void lfdb::set(lfdb::transaction_handle&, std::string_view, std::string_view);

//std::string_view, std::string_view);
//JFW: friend void set_option(transaction_handle& txn, FDBTransactionOption o, std::string_view v);
//
 friend transaction_handle make_transaction(database_handle dbh);
};

} // namespace ceph::libfdb

/////////////////////// ***************** JFW

namespace ceph::libfdb::detail {

// JFW: I'm squirreling this away in here until I can think of a nice way to handle initializtion/destruction
// without manual intervention (the user notification hook is one route, but it's low-priority right now):
class database_system final
{
 database_system() = delete;

 private:
 static inline bool was_initialized = false;

 static inline std::once_flag fdb_was_initialized;
 static inline std::jthread fdb_network_thread;

 static inline void initialize_fdb()
 {
  // This must be called before ANY other API function:
  if(fdb_error_t r = fdb_select_api_version(FDB_API_VERSION); 0 != r)
   throw fdb_exception(r);
 
  // Zero or more calls to this may now be made:
  // fdb_error_t fdb_network_set_option(FDBNetworkOption option, uint8_t const *value, int value_length)
 
  // This must be called before any other API function (besides >= 0 calls to fdb_network_set_option()):
  if(fdb_error_t r = fdb_setup_network(); 0 != r)
   throw fdb_exception(r);
 
  // Launch network thread:
  fdb_network_thread = std::jthread { &fdb_run_network };
 
  // Okie-dokie, we're all set:
  was_initialized = true;
 
 fmt::println("JFW: database::initialize_fdb() complete");
 }

 public:
 static bool& initialized() { return was_initialized; } 

 public: 
 // JFW: TODO: Right now, we're not handling multiple DBs-- this probably needs to live in
 // its own mini-type that has a static across all DB instances, or is perhaps even
 // seperate to allow the once-per-program semantics required by FDB):
 // Shutdown FDB runtime:
 static inline void shutdown_fdb()
 {
  using namespace std::chrono_literals;
 
 fmt::println("JFW: database::shutdown_fdb() begins:");

  if(not initialized()) {
fmt::println("JFW: database was never initialized.");
    return;
  }

  // shut down network and database:
  if(int r = fdb_stop_network(); 0 != r)
   {
     // JFW: in this case, we likely don't want to throw from our dtor, but we may
     // have something to log.
     fmt::println("JFW: database::shutdown_fdb() error {}", r);
   }
 
  // Wait for fdb_stop_network to do its thing(); 
  // JFW: TODO: this should probably be made configurable: 
  std::this_thread::sleep_for(50ms); 
 
  if(fdb_network_thread.joinable()) {
   fdb_network_thread.join();
  }
 
 fmt::println("JFW: database::shutdown_fdb() ends.");
 }

 private:
 friend struct ceph::libfdb::database;
};
 
} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

// JFW: TODO: unused, implement options
template <typename SettingKind>
inline void set_options(auto& set_option_target_obj, const std::pair<SettingKind, std::string>& kv)
{
 std::ranges::for_each(kv, [&set_option_target_obj](const auto& kv) {
		        set_option_target_obj.set_option(kv.first, kv.second);
		      });
}

database::database()
{
 std::call_once(ceph::libfdb::detail::database_system::fdb_was_initialized, ceph::libfdb::detail::database_system::initialize_fdb);

 if(fdb_error_t r = fdb_create_database(nullptr, &fdb_handle); 0 != r)
  throw fdb_exception(r);

 // may now set database options:
 ; // JFW
}

database::~database()
{
 // destroy specific database:
 if(nullptr != fdb_handle) {
  fdb_database_destroy(fdb_handle), fdb_handle = nullptr;
 }
}

transaction::transaction(database_handle& dbh)
 : dbh(dbh)
{
  if(fdb_error_t r = fdb_database_create_transaction(dbh->raw_handle(), &txn_handle); 0 != r) {
   throw fdb_exception(r);
  }
}

transaction::~transaction()
{
  if(nullptr == txn_handle) {
   return;
  }

/*JFW
  // If no writes have taken place we do not need to commit; if we don't commit, then we are implicitly
  // rolled-back. For now, this isn't tracked:
  commit();
*/
  fdb_transaction_destroy(txn_handle);
}

void transaction::commit()
{
 future_value fv = fdb_transaction_commit(raw_handle());

 // JFW: IMPORTANT: TODO: I think to correctly handle this, we're supposed to do the fdb_transaction_on_error() 
 // dance; for now:
 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
    throw fdb_exception(r);
 }
}

} // namespace ceph::libfdb

#endif
