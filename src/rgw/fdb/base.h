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
#include <thread>
#include <iterator>
#include <algorithm>
#include <exception>
#include <filesystem>
#include <type_traits>

namespace ceph::libfdb::detail {

template <typename T, typename ...Ts>
consteval bool is_any_of()
{
 return (std::is_same_v<T, Ts> || ...);
}

} // ceph::libfdb::detail

namespace ceph::libfdb {

class select;
class select_view;

} // namespace ceph::libfdb

namespace ceph::libfdb::concepts {

// Capture things that model ContainerCompatibleRange. 

// Adapted from "https://en.cppreference.com/w/cpp/ranges/to.html#container_compatible_range":
template< class Container, class Reference >
constexpr bool appendable_container  = requires (Container& c, Reference&& ref)
{
 requires
 (
  requires { c.emplace_back(std::forward<Reference>(ref)); }     ||
  requires { c.push_back(std::forward<Reference>(ref)); }        ||
  requires { c.emplace(c.end(), std::forward<Reference>(ref)); } ||
  requires { c.insert(c.end(), std::forward<Reference>(ref)); }
 );
};

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb::concepts {

// There's a high likelihood that we're going to get more sophisticated selectors, 
// so this is doing a more important job than it may appear to be:
template <typename InputT>
concept selector = requires { ceph::libfdb::detail::is_any_of<InputT, ceph::libfdb::select, ceph::libfdb::select_view>(); };

} // namespace ceph::libfdb::concepts

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

struct future_value final
{
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

 public:
 FDBFuture *raw_handle() const noexcept { return future_ptr.get(); }
};

/* Analogs to work with key ranges:
JFW: this is TODO and requires a bit of thought-- for now, these types will
at least keep us from "stepping over our own feet" in terms of API design, and
be a sensible default: 
#define         FDB_KEYSEL_LAST_LESS_THAN(k, l) k, l, 0, 0
#define     FDB_KEYSEL_LAST_LESS_OR_EQUAL(k, l) k, l, 1, 0
#define     FDB_KEYSEL_FIRST_GREATER_THAN(k, l) k, l, 1, 1
#define FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(k, l) k, l, 0, 1

enum struct key_selector { first_gt, first_gteq, last_lt, last_lteq }; */
class select final
{
 private:
 std::string begin_key, end_key;

 public:
 select(std::string_view begin_key_, std::string_view end_key_)
  : begin_key(begin_key_), end_key(end_key_)
 {}

 private:
 friend bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::concepts::selector auto&, auto);

/* JFW:
 template <typename SelectorT> 
 friend inline bool get(ceph::libfdb::transaction_handle txn, const SelectorT&, auto out_iter); */

 template <typename SelectorT> requires ceph::libfdb::concepts::selector<SelectorT>
 friend inline bool get(ceph::libfdb::transaction_handle txn, const SelectorT& key_range, auto out_iter);
};

// Caution: do not let the actual key data go out of scope!
class select_view final
{
 private:
 std::string_view begin_key, end_key;

 public:
 select_view(std::string_view begin_key_, std::string_view end_key_)
  : begin_key(begin_key_), end_key(end_key_)
 {}

 private:
 friend bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::concepts::selector auto&, auto);
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

namespace ceph::libfdb {

struct database;
struct transaction;

class database final
{
 private:
 FDBDatabase *fdb_handle = nullptr;

 public:
 database(const std::filesystem::path cluster_file_path);
 database();

 ~database();

 public:
 operator bool() { return nullptr != raw_handle(); }

 public:
 FDBDatabase *raw_handle() const noexcept { return fdb_handle; }

 private:
 friend transaction;
};

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

/*JFW:
 private:
 void set_option(FDBTransactionOption o, std::string_view v) {
    detail::check_fdb_result(
      fdb_transaction_set_option(raw_handle(), o, (const std::uint8_t *)v.data(), v.length()));
*/

 private:
 void commit();
 
/* We need to figure out a good way to handle the combinatorics in here...
 void set(const std::int64_t k, std::int64_t& v) {
  // ...note that if the /call/ made it to here, that's good-- the serialization layer pointed to the right place.
  static_assert("non-sequence keys aren't yet supported");
 }

 void set(const std::int64_t k, std::span<const std::uint8_t> v) {
  // ...note that if the /call/ made it to here, that's good-- the serialization layer pointed to the right place.
  static_assert("non-sequence keys aren't yet supported");
 }*/

 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 void erase(std::span<const std::uint8_t> k) {
    fdb_transaction_clear(raw_handle(),
			  (const std::uint8_t *)k.data(), k.size());
 }

 private:
 template <typename K, typename V>
 friend inline void set(transaction_handle h, K k, V v, const commit_after_op commit_after);

 template <std::input_iterator PairIter>
 friend inline void set(transaction_handle h, PairIter b, PairIter e, const commit_after_op commit_after);

 template <typename K>
 friend inline void erase(transaction_handle h, K k, const commit_after_op commit_after);

 private:
 friend transaction_handle make_transaction(database_handle dbh);
};

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// The global DB state and management thread:
// JFW: there are some more user hooks that go go in here
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
 }

 public:
 static bool& initialized() { return was_initialized; } 

 public: 
 static inline void shutdown_fdb()
 {
  using namespace std::chrono_literals;
 
  if(not initialized()) {
    return;
  }

  // shut down network and database:
  if(int r = fdb_stop_network(); 0 != r)
   {
     // JFW: in this case, we likely don't want to throw from our dtor, but we may
     // have something to log.
     // fmt::println("database::shutdown_fdb() error {}", r);
   }
 
  std::this_thread::sleep_for(10ms); 
 
  if(fdb_network_thread.joinable()) {
   fdb_network_thread.join();
  }
 }

 private:
 friend struct ceph::libfdb::database;
};
 
} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

inline void shutdown_libfdb()
{
 // Shutdown the FDB thread:
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

} // namespace ceph::libfdb

#endif
