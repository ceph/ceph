// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp
      
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
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
// is included (see fdb_c_apiversion.g.h):
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h> 

// Ceph uses libfmt rather than <format>:
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <tuple>
#include <mutex>
#include <memory>
#include <span>
#include <ranges>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <variant>
#include <optional>
#include <iterator>
#include <concepts>
#include <algorithm>
#include <generator>
#include <exception>
#include <functional>
#include <filesystem>
#include <type_traits>

#ifdef __cpp_lib_flat_map
 #include <flat_map>
 template <typename ...Args>
 using flat_map = std::flat_map<Args...>;
#else
 #include <boost/container/flat_map.hpp>
 template <typename ...Args>
 using flat_map = boost::container::flat_map<Args...>;
#endif

// Wrangle some forward declarations:
namespace ceph::libfdb {

struct select;

class database;
class transaction;

using database_handle = std::shared_ptr<database>;
using transaction_handle = std::shared_ptr<transaction>;

extern transaction_handle make_transaction(database_handle dbh);

} // namespace ceph::libfdb

// MOAR forward declarations-- "pay no attention to that man behind the curtain": 
namespace ceph::libfdb::detail {

template <typename T, typename ...Ts>
concept is_any_of = (std::is_same_v<T, Ts> || ...);

struct future_value;

template <typename ValueT = std::string>
std::pair<std::string, ValueT> to_decoded_kv_pair(const FDBKeyValue& kv);

inline fdb_error_t do_commit(transaction_handle& txn);

inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     std::span<const std::uint8_t> k,
                                     std::span<const std::uint8_t> v);

inline future_value await_ready_key_range_future(transaction_handle txn, const ceph::libfdb::select&, int);
inline future_value await_ready_keyvalue_range_future(transaction_handle txn, const ceph::libfdb::select& key_range, int& iteration);

// A generator that produces successive spans for a range:
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(ceph::libfdb::transaction& txn, ceph::libfdb::select key_range);

// Stores a successively-generated of kv pair results to an iterator:
template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline bool get_value_range_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& key_range, OutIterT out_iter);

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::concepts {

// Note that "stringlikes" are not all "stringview-likes", such as when they can be
// written to:
template <typename StringViewLikeT>
concept stringview_convertible = std::convertible_to<StringViewLikeT, std::string_view>;

template <typename IteratorT>
concept key_value_iterator =
 std::input_iterator<IteratorT> and
 requires(const std::iter_value_t<IteratorT>& kv) {
  kv.first;
  kv.second;
 };

template <typename FnT>
concept value_callback =
 std::invocable<FnT&, std::span<const std::uint8_t>>;

template <typename T>
concept value_output =
 !value_callback<std::remove_reference_t<T>> &&
 std::is_lvalue_reference_v<T>;

template <typename T>
concept storable_invocation_result =
 !std::is_void_v<T> && !std::is_reference_v<T>;

template <typename T>
concept supported_invocation_result =
 std::is_void_v<T> || storable_invocation_result<T>;

// There's a high likelihood that we're going to get more sophisticated selectors, 
// so this is doing a more important job than it may appear to be:
template <typename T>
concept selector = ceph::libfdb::detail::is_any_of<T, ceph::libfdb::select>;

} // namespace ceph::libfdb::concepts

// libfdb_exception: How to deal, when Bad Things(TM) happen:
namespace ceph::libfdb {

// Should we commit after the (possibly) mutating operation?
enum struct commit_after_op { commit, no_commit };

struct libfdb_exception final : std::runtime_error
{
 using std::runtime_error::runtime_error;

 fdb_error_t fdb_error_value = -1;

 bool retryable() const noexcept
 {
  return 0 < fdb_error_value &&
         fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, fdb_error_value);
 }

 libfdb_exception(std::string_view msg)
  : std::runtime_error(make_error_string(msg))
 {}

 explicit libfdb_exception(fdb_error_t fdb_error_value)
  : std::runtime_error(make_fdb_error_string(fdb_error_value)),
    fdb_error_value(fdb_error_value)
 {}

 static std::string make_error_string(const std::string_view msg)
 {
  return fmt::format("libfdb: {}", msg);
 }

 static std::string make_fdb_error_string(const fdb_error_t ec)
 {
  return make_error_string(fmt::format("FoundationDB error {}: {}", ec, fdb_get_error(ec)));
 }
};

// A straightforward but pretty handy predicate-- ask an exception if it's something the
// user can try replaying an operation to correct:
inline bool retryable(const libfdb_exception& e) noexcept
{
 return e.retryable();
}

namespace detail {

struct future_value final
{
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 explicit future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

 public:
 FDBFuture *raw_handle() const noexcept { return future_ptr.get(); }

 private:
 void destroy() noexcept { future_ptr.reset(nullptr); }
 
 private:
 friend class ceph::libfdb::transaction;
};

constexpr auto as_fdb_span(const char *s)
{
 return std::span<const std::uint8_t>((const std::uint8_t *)s, std::strlen(s));
}

constexpr auto as_fdb_span(std::string_view sv) 
{ 
 return std::span<const std::uint8_t>((const std::uint8_t *)sv.data(), sv.size()); 
}

/* Prefix selectors assume user keys do not start with 0xFF. Appending 0xFF
would exclude valid keys (beginning with prefix + 0xFF), so we build the 
next-in-lexicographic-order key instead. Callers can still specify an 
explicit full range if they need something special: */
inline std::string make_range_end_key_for_prefix(std::string_view prefix)
{
 if (prefix.empty()) {
  return "\xFF";
 }

 if (0xFF == static_cast<unsigned char>(prefix.front())) {
  throw libfdb_exception("requested prefix has no (finite) end key");
 }

 auto i = prefix.find_last_not_of(static_cast<char>(0xFF));
 auto end_key = std::string(prefix.substr(0, i + 1));

 end_key.back() = static_cast<char>(static_cast<unsigned char>(end_key.back()) + 1);

 return end_key;
}

} // namespace ceph::libfdb::detail

/* lfdb::select is for handling batch queries-- there are two constructors to consider:
    - single-key select creates the range containing all keys with that prefix;
    - passing a start and end key creates a half-open lexicographic key range: [begin_key, end_key). */
struct select final
{
 std::string begin_key, end_key;

 mutable struct {
  int stride = 0; // "unlimited"

  bool reverse_order = false;	// should we return results in reverse order?

  // Some parts of the documentation claim FDB_STREAMING_MODE_ITERATOR is the default, other parts
  // don't... examples tend to use FDB_STREAMING_MODE_WANT_ALL, but they operate on fairly small amounts
  // of data. It's pretty hard to understand what the Right Thing(TM) to do is, the sure the answer may
  // evolve as I learn more; this particular mode starts with small batches and then grows to larger
  // increments as more data is sent and seems to be generally well-behaved:
  FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_ITERATOR; 

 } options;

 public:
 select(std::string_view begin_key_, std::string_view end_key_)
  : begin_key(begin_key_), end_key(end_key_)
 {}

 select(std::string_view prefix)
  : begin_key(prefix),
    end_key(detail::make_range_end_key_for_prefix(prefix))
 {}

};

// Flag-only options are indicated with an explicit option_flag, as they have no actual value:
struct option_flag_t final {};
inline constexpr option_flag_t option_flag;

using option_value = std::variant<option_flag_t, std::int64_t, std::string, std::vector<std::uint8_t>>;

// i.e. option /code/ to the value of the option itself (e.g. FDB_FOO, 42):
template <typename OptionCodeT>
using option_map = flat_map<OptionCodeT, option_value>; 

using network_options = option_map<FDBNetworkOption>;
using database_options = option_map<FDBDatabaseOption>;
using transaction_options = option_map<FDBTransactionOption>;

namespace detail {

// Note that these are specific to FDB's needs (see return type, casts):
constexpr const std::uint8_t *data_of(const std::vector<std::uint8_t>& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::string& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::int64_t& x) { return reinterpret_cast<const std::uint8_t *>(&x); }
constexpr const std::uint8_t *data_of(const option_flag_t&) { return nullptr; }

// Note that these are specific to FDB's needs (see return type, casts):
constexpr int size_of(const std::vector<std::uint8_t>& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::string& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::int64_t) { return 8; } // 8b = size required by FDB
constexpr int size_of(const option_flag_t&) { return 0; }

// ...also used:
constexpr std::uint8_t *data_of(std::string_view xs) { return (std::uint8_t *)xs.data(); }
constexpr int size_of(std::string_view xs) { return static_cast<int>(xs.size()); }

inline auto as_fdb_option_args(const auto& option)
{
 auto [data, size] = std::visit([](const auto& x) {
                         return std::tuple { data_of(x), size_of(x) };
                       },
                       option.second);

 return std::tuple { option.first, data, size };
}

inline void apply_options(const auto& option_map, auto&& set_option)
{
 std::ranges::for_each(option_map, [&set_option](const auto& option) {
    auto args = as_fdb_option_args(option);

    if (auto r = std::apply(set_option, args); 0 != r) {
      throw libfdb_exception(fmt::format("while setting option {}; {}",
                             static_cast<int>(option.first),
                             libfdb_exception::make_fdb_error_string(r)));
    }
  });
}

// The global DB state and management thread:
// JFW: more user hooks that go into FDB system possible here
class database_system final
{
 database_system() = delete;

 private:
 static inline bool was_initialized = false;

 static inline std::once_flag fdb_was_initialized;
 static inline std::jthread fdb_network_thread;

 static inline void initialize_fdb(const network_options& options)
 {
  // This must be called before ANY other API function:
  if (fdb_error_t r = fdb_select_api_version(FDB_API_VERSION); 0 != r)
   throw libfdb_exception(r);
 
  // Zero or more calls to this may now be made:
  apply_options(options, fdb_network_set_option);
 
  // This must be called before any other API function (besides >= 0 calls to fdb_network_set_option()):
  if (fdb_error_t r = fdb_setup_network(); 0 != r)
   throw libfdb_exception(r);
 
  // Launch network thread:
  fdb_network_thread = std::jthread { &fdb_run_network };
 
  // Okie-dokie, we're all set (distinct from fdb_was_initialized):
  was_initialized = true;
 }

 private:
 database_system(const network_options& network_opts)
 {
   std::call_once(ceph::libfdb::detail::database_system::fdb_was_initialized, 
                  ceph::libfdb::detail::database_system::initialize_fdb, 
                  network_opts);
 } 

 public:
 static bool& initialized() noexcept { return was_initialized; } 

 public: 
 static inline void shutdown_fdb()
 {
  using namespace std::chrono_literals;
 
  if (not initialized()) {
    return;
  }

  // shut down network and database:
  if (int r = fdb_stop_network(); 0 != r)
   {
     // In this case, we likely don't want to throw from our dtor, but we may
     // have something to log. As there's no higher-level hook for that, we
     // have nothing to do right now.
     // fmt::println("database::shutdown_fdb() error {}", r);
   }

  // This may not actually be needed, but it's a traditional courtesy:
  std::this_thread::sleep_for(7ms); 
 
  if (fdb_network_thread.joinable()) {
   fdb_network_thread.join();
  }
 }

 private:
 friend struct ceph::libfdb::database;
};
 
} // namespace ceph::libfdb::detail

class database final
{
 detail::database_system db_system;

 private:
 std::shared_ptr<FDBDatabase> db_handle; 

 FDBDatabase *create_database_ptr(const std::filesystem::path cluster_file_path) {
    FDBDatabase *fdbp = nullptr;

    if (fdb_error_t r = fdb_create_database(cluster_file_path.c_str(), &fdbp); 0 != r) {
      throw libfdb_exception(r);
    }
    
    return fdbp;
 }

 FDBTransaction *create_transaction() {
    FDBTransaction *txn_p = nullptr;
    
    if (fdb_error_t r = fdb_database_create_transaction(raw_handle(), &txn_p); 0 != r) {
     throw libfdb_exception(r);
    }

    return txn_p;
 }

 public:
 database(const std::filesystem::path cluster_file_path, const ceph::libfdb::database_options& db_opts, const network_options& network_opts)
  : db_system(network_opts),
    db_handle(create_database_ptr(cluster_file_path), &fdb_database_destroy)
 {
  detail::apply_options(db_opts,
             [handle = raw_handle()](auto option_code, auto data, auto size) {
               return fdb_database_set_option(handle, option_code, data, size);
             });
 }

 database(const std::filesystem::path cluster_file_path, const ceph::libfdb::database_options& db_opts)
  : database(cluster_file_path, db_opts, {})
 {}

 database(const ceph::libfdb::database_options& db_opts, const ceph::libfdb::network_options& net_opts)
  : database("", db_opts, net_opts)
 {}

 database(const ceph::libfdb::database_options& db_opts)
  : database("", db_opts, {})
 {}

 database(const std::filesystem::path cluster_file_path)
  : database(cluster_file_path, {}, {})
 {}

 database()
  : database(std::filesystem::path {}, {}, {})
 {}

 public:
 explicit operator bool() const noexcept { return nullptr != raw_handle(); }

 public:
 FDBDatabase *raw_handle() const noexcept { return db_handle.get(); }

 private:
 friend transaction;

};

class transaction final
{
 database_handle dbh;

 std::unique_ptr<FDBTransaction, decltype(&fdb_transaction_destroy)> txn_handle;

 private:
 bool get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output); 

 public:
 transaction(database_handle dbh_)
 : dbh(dbh_),
   txn_handle(dbh->create_transaction(), &fdb_transaction_destroy)
 {}

 transaction(database_handle dbh_, const transaction_options& opts) 
  : transaction(dbh_)
 {
  detail::apply_options(opts,
             [handle = raw_handle()](auto code, auto data, auto size) {
               return fdb_transaction_set_option(handle, code, data, size);
             });
 }

 public:
 explicit operator bool() const noexcept { return dbh && nullptr != raw_handle(); }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_handle.get(); }

 private:
 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 // JFW: it's not as easy to wedge an output_range into here as it appears, perhaps
 // needs to be revisited; I'm binding it to what's actually used in practice for now:
 template <typename OutIterT>
 requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
 bool get(const ceph::libfdb::select& key_range, OutIterT out_iter) {
    return ceph::libfdb::detail::get_value_range_from_transaction(*this, key_range, out_iter);
 }
 
 bool get(const std::span<const std::uint8_t> k, concepts::value_callback auto&& val_collector) {
    return get_single_value_from_transaction(k, val_collector);
 }

 void erase(std::span<const std::uint8_t> k) {
    fdb_transaction_clear(raw_handle(),
                          (const std::uint8_t *)k.data(), k.size());
 }

 void erase(const ceph::libfdb::concepts::selector auto& key_range) {
    fdb_transaction_clear_range(raw_handle(),
        (const uint8_t *)key_range.begin_key.data(), key_range.begin_key.size(), 
        (const uint8_t *)key_range.end_key.data(), key_range.end_key.size());
 }

 bool key_exists(std::string_view k) {
    return get_single_value_from_transaction(detail::as_fdb_span(k), [](auto) {});
 }

 bool commit(fdb_error_t *replay_error = nullptr);
 void destroy() noexcept { txn_handle.reset(); }

 // As you can see, friends are used extensively to implement the public interface; it would be nice to come up
 // with a strategy for making these "lists" a bit more managable, but for now it's what we have and it allows me ot
 // keep these handles mostly-opaque (this has proven to be a good idea as I've a few times shuffled internal details
 // with no disruption to the user interface surface):
 private:
 friend inline void set(transaction_handle, const std::string_view, const auto&, const commit_after_op);
 friend inline void set(database_handle, std::string_view, const auto&);
 friend inline void set(transaction_handle, std::string_view, const ceph::libfdb::concepts::stringview_convertible auto&, const commit_after_op);
 friend inline void set(database_handle, std::string_view, const ceph::libfdb::concepts::stringview_convertible auto&);

 template <typename OutputTargetOrFnT>
 requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> ||
          concepts::value_output<OutputTargetOrFnT&&>
 friend inline bool get(ceph::libfdb::transaction_handle,
                        std::string_view,
                        OutputTargetOrFnT&&,
                        const commit_after_op);
 friend inline bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, auto, const commit_after_op);

 friend inline void erase(ceph::libfdb::transaction_handle, std::string_view, const commit_after_op);
 friend inline void erase(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, const commit_after_op);
 friend inline void erase(ceph::libfdb::database_handle, std::string_view);
 friend inline void erase(ceph::libfdb::database_handle, const ceph::libfdb::select&);

 friend inline bool key_exists(transaction_handle txn, std::string_view k, const commit_after_op commit_after);

 friend inline bool commit(transaction_handle& txn);
 friend inline fdb_error_t ceph::libfdb::detail::do_commit(transaction_handle& txn);
 friend inline void ceph::libfdb::detail::transaction_set_kv_bytes(const transaction_handle&,
                                                                   std::span<const std::uint8_t>,
                                                                   std::span<const std::uint8_t>);

};

namespace detail {

// Since lambdas cannot be friend-functions, we use a named helper:
inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     std::span<const std::uint8_t> k,
                                     std::span<const std::uint8_t> v)
{
 txn->set(k, v);
}

} // namespace detail

inline bool ceph::libfdb::transaction::get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output)
{
 const fdb_bool_t is_snapshot = false; 

 detail::future_value fv(fdb_transaction_get(
                                    raw_handle(), 
                                    (const uint8_t *)key.data(), key.size(), 
                                    is_snapshot));
    
  if (fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
     throw libfdb_exception(r);
  }
 
  fdb_bool_t key_was_found = false;
  const uint8_t *out_buffer = nullptr;
  int out_len = 0;

  if (fdb_error_t r = fdb_future_get_value(fv.raw_handle(), &key_was_found, &out_buffer, &out_len); 0 != r) {
    throw libfdb_exception(r);
  }

  // No errors, but no value was found:
  if (0 == key_was_found) {
    return false;
  }
 
  write_output(std::span<const std::uint8_t>(out_buffer, out_len));
 
  // The happy path is the simple path:
  return true; 
}

[[nodiscard]] inline bool ceph::libfdb::transaction::commit(fdb_error_t *replay_error)
{
 if (nullptr != replay_error) {
  *replay_error = 0;
 }

 // We don't want to try to vivify for an "empty" commit:
 if (!*this)
  return false;

 detail::future_value fv(fdb_transaction_commit(raw_handle()));

 if (fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
  throw libfdb_exception(r);
 }

 if (fdb_error_t r = fdb_future_get_error(fv.raw_handle()); 0 != r) {
  detail::future_value ferror_result(fdb_transaction_on_error(raw_handle(), r));

  if (0 != fdb_future_block_until_ready(ferror_result.raw_handle())) {
    // In their example, they use the first error as the message source, so I will do that also:
    throw libfdb_exception(r);
  }

  if (0 != fdb_future_get_error(ferror_result.raw_handle())) {
    // Destroy the futures AND be sure to invalidate the transaction-- according to the documentation,
    // this must happen in the specified order (which /should/ currently match the dtor order, but I am
    // not going to touch this any more right now):
    fv.destroy(), ferror_result.destroy(), destroy();

    // Again, use the original error:
    throw libfdb_exception(r);
  }

  // A false result means on_error() succeeded; the caller should replay the transaction:
  if (nullptr != replay_error) {
   *replay_error = r;
  }

  return false;
 }

 // Ok:
 return true;
}

} // namespace ceph::libfdb

// Future-wrangling and tricky retry-handling:
namespace ceph::libfdb::detail {

inline future_value block_until_ready(future_value&& fv)
{
 if (fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
   throw libfdb_exception(r);
 }

 // Note that fdb_future_block_until_ready() does not by itself check for errors
 // with the value; so, we need to do this separately:
 fdb_error_t r = fdb_future_get_error(fv.raw_handle());

 if (r) {
  throw libfdb_exception(r);
 }

 return fv;
}

inline future_value wait_until_ready(future_value&& fv)
{
 if (fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
   throw libfdb_exception(r);
 }

 return fv;
}

template <typename FnT, typename... XS>
requires std::invocable<FnT, XS...>
inline future_value await_future_of(FnT&& fn, XS&& ...params)
{
 return block_until_ready(
          std::invoke(std::forward<FnT>(fn), std::forward<XS>(params)...));
} 

template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline bool get_value_range_from_transaction(transaction& txn, const select& key_range, OutIterT out_iter)
{
 auto flattened = detail::generate_FDB_pairs(txn, key_range) | std::views::join;
 std::ranges::transform(flattened, out_iter, to_decoded_kv_pair<std::string>);

 return true;
}

inline fdb_error_t value_from_db(future_value& fv,
                        std::invocable<future_value&> auto extract_fn)
{
 return std::invoke(extract_fn, fv);
}

inline bool retry_after_error(ceph::libfdb::transaction_handle& txn, const fdb_error_t r)
{
 if (0 == r) {
   return false;
 }

 if (not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, r)) {
   // Non-retryable errors cannot be repaired by on_error():
   throw ceph::libfdb::libfdb_exception(r);
 }

 future_value on_error_future(fdb_transaction_on_error(txn->raw_handle(), r));

 if (fdb_error_t on_error_r = fdb_future_block_until_ready(on_error_future.raw_handle()); 0 != on_error_r) {
   throw ceph::libfdb::libfdb_exception(r);
 }

 if (fdb_error_t on_error_r = fdb_future_get_error(on_error_future.raw_handle()); 0 != on_error_r) {
   throw ceph::libfdb::libfdb_exception(r);
 }

 return true;
}

// Convert FDBKey array into something useful:
inline std::vector<ceph::libfdb::select> as_select_seq(const FDBKey* const xs,
                                                       const int n,
                                                       const ceph::libfdb::select& parent)
{
 std::vector<ceph::libfdb::select> out;

 if (1 < n) {
  out.reserve(static_cast<std::size_t>(n - 1));
 }

 // Gather the flattened list into *overlapping* libfdb::select pairs:
 for (const auto& dyad : std::span(xs, n) | std::views::slide(2)) {
    const auto& fst = std::ranges::begin(dyad)[0];
    const auto& snd = std::ranges::begin(dyad)[1];

    ceph::libfdb::select split { 
      { (const char *)fst.key, static_cast<std::string::size_type>(fst.key_length) }, 
      { (const char *)snd.key, static_cast<std::string::size_type>(snd.key_length) }
    };

    split.options = parent.options;
    out.push_back(std::move(split));
 }

 return out;
}
// Finding a clear example both in the samples and in the documentation is not very easy. The
// statelessness of FDB requests bleeds into here with basically no hand-holding, so it's fairly subtle and
// not easy to see what to do (I'm still not sure I have it right), but note for instance that the call
// parameters have to change for subsequent reads.
inline future_value get_range_future_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& selection, int iteration)
{
  const auto& begin_key  = selection.begin_key;
  const auto& end_key    = selection.end_key;

  const auto& options    = selection.options;

  // The documentation makes this stuff about as clear as mud... read VERY carefully
  // when you fiddle with these:
  const int begin_or_eq = (not options.reverse_order and 1 < iteration) ? 1 : 0;
  const int begin_offset = 1;
  const int end_or_eq = 0;
  const int end_offset = 1;

  // See validate_and_update_parameters() in fdb_c.cpp (FDB source) if greater clarity is needed on the meaning of some of these, it can be
  // hard to deduce from the documentation:
  // Returns an FDBKeyValueArray in the future:

  return future_value(fdb_transaction_get_range(txn.raw_handle(),
                      (const uint8_t *)begin_key.data(), begin_key.size(),      // the reference-point key
                      begin_or_eq,                                              // begin or eq
                      begin_offset,                                             // begin offset

                      (const uint8_t *)end_key.data(), end_key.size(),          // the end selector key
                      end_or_eq,                                                // end or eq
                      end_offset,                                               // end offset (a shift AFTER end is matched)

                      // How should results be grouped/chunked:
                      options.stride,                                           // limit (0 == unlimited)
                      0,                                                        // target bytes (0 == unlimited)
                      options.streaming_mode,                                   // streaming mode (e.g.: FDB_STREAMING_MODE_WANT_ALL)
                      iteration,                                                // iteration # (produced side effect)

                      // Other options:
                      0,                                                        // 0 unless this IS a snapshot read
                      options.reverse_order                                     // should items come in reverse order?
                    ));
}

inline std::vector<ceph::libfdb::select> locate_split_points(
          ceph::libfdb::database_handle dbh, 
          ceph::libfdb::select selector, 
          const std::int64_t remote_chunk_size)
{
 using ceph::libfdb::detail::wait_until_ready;

 auto txn = ceph::libfdb::make_transaction(dbh);

 // We can do this without side-effects by combining some lambdas, but for now I'm satisfied if it works:
 FDBKey const *keys = nullptr;
 int nkeys = 0;

 for (bool should_retry = true; should_retry;) {

    auto fv = wait_until_ready(future_value(fdb_transaction_get_range_split_points(
                              txn->raw_handle(),
                              (const std::uint8_t *)selector.begin_key.data(), static_cast<int>(selector.begin_key.length()),
                              (const std::uint8_t *)selector.end_key.data(), static_cast<int>(selector.end_key.length()),
                              remote_chunk_size))); 

    should_retry = retry_after_error(txn, value_from_db(fv,
                    [&keys, &nkeys](future_value& fv) {
                      return fdb_future_get_key_array(fv.raw_handle(), &keys, &nkeys);
                   }));

    if (not should_retry) {
      return as_select_seq(keys, nkeys, selector);
    }
 }

 return {};
}

// Generators:
// The returned memory should be copied immediately as its lifetime will end once the Future is destroyed:
// (This is pretty much why this is in the "detail" namespace for the moment.)
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(transaction& txn, select key_range) 
{
  // As FDB is stateless, we need to track state somewhere:
  int more_available = 1;
  int out_count = 0;
  int iteration = 1; // expected to be 1 when FDB is called in iterator mode
 
  const FDBKeyValue *out_kvs = nullptr;

  for (; more_available; iteration++) {

    auto fv = await_future_of([&]() { return get_range_future_from_transaction(txn, key_range, iteration); });

    if (fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &more_available); 0 != r) {
       throw libfdb_exception(r);
    }

    auto result = std::span<const FDBKeyValue>(out_kvs, out_count);

    if (more_available) {
      // Make the first part of the range for the new search equal to the last one from the old search:
      const auto& last_key = result.back();
      auto& cursor = key_range.options.reverse_order ? key_range.end_key : key_range.begin_key;

      cursor = std::string_view((const char *)last_key.key, last_key.key_length);
     }

    co_yield result;
  }
}

} // namespace ceph::libfdb::detail


#endif
