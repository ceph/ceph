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

#include <span>
#include <array>
#include <tuple>
#include <vector>
#include <variant>
#include <optional>

#include <ranges>
#include <iterator>
#include <algorithm>
#include <generator>

#include <mutex>
#include <thread>

#include <memory>
#include <cstdint>
#include <utility>
#include <compare>
#include <concepts>
#include <exception>
#include <stdexcept>
#include <functional>
#include <stop_token>
#include <filesystem>
#include <type_traits>
#include <initializer_list>

#include "common/container_concepts.h"

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

namespace query {

struct interval;

} // namespace query

using select = query::interval;
struct with_result_t;
struct versionstamp;
struct watch_handle;
struct commit_result;

class database;
class transaction;

using database_handle = std::shared_ptr<database>;
using transaction_handle = std::shared_ptr<transaction>;

extern transaction_handle make_transaction(database_handle dbh);
[[nodiscard]] inline watch_handle make_watch(transaction_handle txn, std::string_view key);

} // namespace ceph::libfdb

namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& from,
                    ceph::libfdb::versionstamp& to);

} // namespace ceph::libfdb::from

// MOAR forward declarations-- "pay no attention to that man behind the curtain":
namespace ceph::libfdb::detail {

struct future_value;

template <typename ValueT = std::string>
std::pair<std::string, ValueT> to_decoded_kv_pair(const FDBKeyValue& kv);

inline fdb_error_t do_commit(transaction_handle& txn);

inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     std::span<const std::uint8_t> k,
                                     std::span<const std::uint8_t> v);
inline void transaction_clear_key_bytes(const transaction_handle& txn,
                                        std::span<const std::uint8_t> k);
inline void transaction_clear_range(const transaction_handle& txn,
                                    const ceph::libfdb::select& key_range);

inline future_value block_until_ready(future_value&& fv);
inline fdb_error_t get_future_error(const future_value& fv);
inline future_value wait_for_on_error(FDBTransaction* txn, fdb_error_t original_error);
inline future_value get_range_future_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& selection, int iteration);

// A generator that produces successive spans for a range:
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(ceph::libfdb::transaction& txn, ceph::libfdb::select key_range);

// Stores generated key/value pair results to an iterator and returns the
// number of pairs emitted:
template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline std::size_t get_value_range_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& key_range, OutIterT& out_iter);

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::concepts {

// Note that "stringlikes" are not all "stringview-likes", such as when they can be
// written to:
template <typename StringViewLikeT>
concept stringview_convertible =
 std::convertible_to<std::remove_reference_t<StringViewLikeT> const&, std::string_view>;

template <typename KeyViewT>
concept libfdb_key_view = std::same_as<std::remove_cvref_t<KeyViewT>, std::string_view>;

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb::detail {

constexpr std::string_view libfdb_key_view(const concepts::stringview_convertible auto& key)
{
 return std::string_view(key);
}

template <typename KeyT>
concept libfdb_key_like =
 requires(const KeyT& key) {
  { libfdb_key_view(key) } -> std::same_as<std::string_view>;
 };

template <libfdb_key_like KeyT>
constexpr std::string_view as_libfdb_key_view(const KeyT& key)
{
 return libfdb_key_view(key);
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::concepts {

template <typename KeyT>
concept libfdb_key = ceph::libfdb::detail::libfdb_key_like<KeyT>;

template <typename IteratorT>
concept key_value_iterator =
 std::input_iterator<IteratorT> and
 requires(std::iter_reference_t<IteratorT> kv) {
  requires libfdb_key<decltype(kv.first)>;
  requires std::is_object_v<std::remove_reference_t<decltype(kv.second)>>;
 };

template <typename RangeT>
concept key_value_range =
 std::ranges::input_range<RangeT> and
 key_value_iterator<std::ranges::iterator_t<RangeT>>;

template <typename RangeT>
concept key_value_forward_range =
 std::ranges::forward_range<RangeT> and
 key_value_iterator<std::ranges::iterator_t<RangeT>>;

template <typename IteratorT>
concept string_pair_output_iterator =
 std::output_iterator<IteratorT, std::pair<std::string, std::string>>;

template <typename RangeT>
concept string_pair_output_range =
 not std::is_array_v<std::remove_reference_t<RangeT>> and
 std::ranges::range<RangeT> and
 ceph::concepts::can_append<RangeT, std::pair<std::string, std::string>>;

template <typename RangeT>
concept materializable_string_pair_output_range =
 string_pair_output_range<RangeT> and
 std::default_initializable<std::remove_cvref_t<RangeT>> and
 std::move_constructible<std::remove_cvref_t<RangeT>>;

template <typename FnT>
concept value_invocable =
 std::invocable<FnT&, std::span<const std::uint8_t>>;

template <typename FnT>
concept value_callback =
 value_invocable<FnT> &&
 std::is_void_v<std::invoke_result_t<FnT&, std::span<const std::uint8_t>>>;

template <typename T>
concept decoded_value_sink =
 not value_invocable<std::remove_reference_t<T>> and
 std::is_lvalue_reference_v<T> and
 not std::is_const_v<std::remove_reference_t<T>> and
 std::is_object_v<std::remove_reference_t<T>>;

template <typename T>
concept supported_invocation_result =
 std::is_void_v<T> or
 (not std::is_reference_v<T> and
  std::constructible_from<std::remove_cvref_t<T>, T> and
  std::move_constructible<std::remove_cvref_t<T>>);

} // namespace ceph::libfdb::concepts

// libfdb_exception represents libfdb operation failures.
// Caller contract errors use standard exceptions such as std::invalid_argument.
namespace ceph::libfdb {

// Should we commit after the (possibly) mutating operation?
enum struct commit_after_op { commit, no_commit };
enum struct watch_event { changed, cancelled };

struct libfdb_exception final : std::runtime_error
{
 using std::runtime_error::runtime_error;

 fdb_error_t fdb_error_value = -1;

 bool retryable() const noexcept
 {
  return 0 < fdb_error_value and
         fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, fdb_error_value);
 }

 libfdb_exception(std::string_view msg)
  : std::runtime_error(make_error_string(msg))
 {}

 explicit libfdb_exception(fdb_error_t error)
  : std::runtime_error(make_fdb_error_string(error)),
    fdb_error_value(error)
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

/* Note: this magic constant is from FoundationDB's public error-code table,
(flow/include/flow/error_definitions.h). It's distinct from watch_cancelled,
which is a storage-server watch-limit error: */
inline constexpr fdb_error_t operation_cancelled_error = 1101;

struct future_value final
{
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 explicit future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

 public:
 FDBFuture *raw_handle() const noexcept { return future_ptr.get(); }

 FDBFuture *raw_ptr_or_throw() const
 {
  if (auto *future = raw_handle(); nullptr != future) {
   return future;
  }

  throw std::invalid_argument("invalid FDB pointer");
 }

 private:
 void destroy() noexcept { future_ptr.reset(nullptr); }
 
 private:
 friend class ceph::libfdb::transaction;
};

inline auto as_fdb_span(concepts::libfdb_key_view auto key)
{
 return std::span<const std::uint8_t>((const std::uint8_t *)key.data(), key.size());
}

} // namespace detail

// watch_handle can only be constructed by calling make_watch():
struct watch_handle final
{
 public:
 watch_handle(watch_handle&&) noexcept = default;
 watch_handle& operator=(watch_handle&&) noexcept = default;

 [[nodiscard]] bool ready() const noexcept
 {
  auto *future = watch_future.raw_handle();
  return nullptr != future && fdb_future_is_ready(future);
 }

 void cancel() noexcept
 {
  if (auto *future = watch_future.raw_handle(); nullptr != future) {
   fdb_future_cancel(future);
  }
 }

 // Block until the watch reports an event:
 [[nodiscard]] watch_event wait_for_event()
 {
  auto *future = watch_future.raw_ptr_or_throw();

  if (auto block_error = fdb_future_block_until_ready(future);
      0 != block_error) {
   throw libfdb_exception(block_error);
  }

  switch (const auto error = fdb_future_get_error(future))
   {
    default: throw libfdb_exception(error);
    case 0: return watch_event::changed;
    case detail::operation_cancelled_error: return watch_event::cancelled;
   }
 }

 [[nodiscard]] watch_event wait_for_event(std::stop_token stop_token)
 {
  if (stop_token.stop_requested()) {
   cancel();

   return wait_for_event();
  }

  std::stop_callback cancel_watch_on_stop(stop_token, [this] {
   cancel();
  });

  return wait_for_event();
 }

 // Block until the watched key changes:
 void wait()
 {
  if (watch_event::cancelled == wait_for_event()) {
   throw libfdb_exception(detail::operation_cancelled_error);
  }
 }

 void wait(std::stop_token stop_token)
 {
  if (watch_event::cancelled == wait_for_event(stop_token)) {
   throw libfdb_exception(detail::operation_cancelled_error);
  }
 }

 private:
 detail::future_value watch_future;

 private:
 explicit watch_handle(detail::future_value watch_future_)
  : watch_future(std::move(watch_future_))
 {}

 watch_handle() = delete;
 watch_handle(const watch_handle&) = delete;
 watch_handle& operator=(const watch_handle&) = delete;

 private:
 friend watch_handle make_watch(transaction_handle txn, std::string_view key);
 friend class transaction;
};

namespace detail {

inline auto as_fdb_span(const concepts::libfdb_key auto& key)
requires (!concepts::libfdb_key_view<decltype(key)>)
{
 return as_fdb_span(as_libfdb_key_view(key));
}

} // namespace ceph::libfdb::detail

struct versionstamp final
{
 // FoundationDB versionstamps are 10 bytes: 8 bytes of committed
 // database version followed by 2 bytes of transaction batch order.
 using versionstamp_data_t = std::array<std::uint8_t, 10>;

 bool is_resolved() const noexcept
 {
  return result->has_value();
 }

 const versionstamp_data_t& resolved_bytes() const
 {
  if (not is_resolved()) {
   throw std::invalid_argument("attempt to access unresolved version stamp");
  }

  return result->value();
 }

 // Versionstamps become orderable only after commit resolution.
 bool operator==(const versionstamp& rhs) const
 {
  return resolved_bytes() == rhs.resolved_bytes();
 }

 auto operator<=>(const versionstamp& rhs) const
 {
  return resolved_bytes() <=> rhs.resolved_bytes();
 }

 private:
 std::shared_ptr<std::optional<versionstamp_data_t>> result =
  std::make_shared<std::optional<versionstamp_data_t>>();

 void store_result(const std::span<const std::uint8_t> versionstamp_result);

 private:
 friend class transaction;
 friend void ceph::libfdb::from::convert(const std::span<const std::uint8_t>&,
                                         ceph::libfdb::versionstamp&);
};

// versioned_bytes can only be constructed by calling versioned():
// It carries data in transaction-correct version stamp encoding:
struct versioned_bytes final
{
 public:
 versioned_bytes() = delete;

 versioned_bytes(const versioned_bytes&) = default;
 versioned_bytes(versioned_bytes&&) noexcept = default;
 versioned_bytes& operator=(const versioned_bytes&) = default;
 versioned_bytes& operator=(versioned_bytes&&) noexcept = default;

 private:
 std::vector<std::uint8_t> encoding_buffer;
 versionstamp stamp;

 versioned_bytes(std::vector<std::uint8_t> encoding_buffer_, versionstamp stamp_)
  : encoding_buffer(std::move(encoding_buffer_)),
    stamp(std::move(stamp_))
 {}

 private:
 friend class transaction;

 template <concepts::stringview_convertible PrefixT>
 friend versioned_bytes versioned(const PrefixT& prefix, versionstamp stamp);

 template <concepts::stringview_convertible PrefixT, concepts::stringview_convertible SuffixT>
 friend versioned_bytes versioned(const PrefixT& prefix, const SuffixT& suffix, versionstamp stamp);
};

namespace detail {

// FDB version stamp mutations expect the final four bytes to be a little-endian uint32_t offset pointing at the 10-byte placeholder:
inline void append_little_endian_u32(std::vector<std::uint8_t>& out, const std::uint32_t x)
{
 out.push_back(static_cast<std::uint8_t>(x));
 out.push_back(static_cast<std::uint8_t>(x >> 8));
 out.push_back(static_cast<std::uint8_t>(x >> 16));
 out.push_back(static_cast<std::uint8_t>(x >> 24));
}

inline std::vector<std::uint8_t> make_versioned_encoding(std::string_view prefix, std::string_view suffix)
{
 constexpr auto versionstamp_byte_count = std::tuple_size_v<versionstamp::versionstamp_data_t>;

 std::vector<std::uint8_t> out;
 out.reserve(prefix.size() + versionstamp_byte_count + suffix.size() + sizeof(std::uint32_t));

 std::ranges::copy(prefix, std::back_inserter(out));

 const auto versionstamp_offset = static_cast<std::uint32_t>(out.size());
 out.resize(out.size() + versionstamp_byte_count);

 std::ranges::copy(suffix, std::back_inserter(out));
 detail::append_little_endian_u32(out, versionstamp_offset);

 return out;
}

} // namespace detail

template <concepts::stringview_convertible PrefixT, concepts::stringview_convertible SuffixT>
versioned_bytes versioned(const PrefixT& prefix, const SuffixT& suffix, versionstamp stamp)
{
 return versioned_bytes {
  detail::make_versioned_encoding(std::string_view(prefix), std::string_view(suffix)),
  std::move(stamp)
 };
}

template <concepts::stringview_convertible PrefixT>
versioned_bytes versioned(const PrefixT& prefix, versionstamp stamp)
{
 return versioned(prefix, std::string_view {}, std::move(stamp));
}

inline void versionstamp::store_result(const std::span<const std::uint8_t> versionstamp_result)
{
 if (versionstamp_result.size() != std::tuple_size_v<versionstamp_data_t>) {
  throw std::invalid_argument("invalid version stamp size");
 }

 versionstamp_data_t result_value;
 std::ranges::copy(versionstamp_result, std::begin(result_value));

 if (not result->has_value()) {
  result->emplace(std::move(result_value));
  return;
 }

 if (result->value() != result_value) {
  throw std::invalid_argument("attempt to overwrite resolved version stamp");
 }
}

} // namespace ceph::libfdb

#include "query.h"

namespace ceph::libfdb {

// Helpers for selectors:
namespace detail {

inline std::string key_after(std::string_view key)
{
 return std::string(key) + '\0';
}

inline select as_half_open_select(const select& selection)
{
 auto half_open = select {
  selection.begin_inclusive ? selection.begin_key : key_after(selection.begin_key),
  selection.end_inclusive ? key_after(selection.end_key) : selection.end_key
 };

 half_open.options = selection.options;

 return half_open;
}

} // namespace detail

// Flag-only options are indicated with an explicit option_flag, as they have
// no actual value:
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
inline const std::uint8_t *data_of(const std::vector<std::uint8_t>& xs) { return (const std::uint8_t *)xs.data(); }
inline const std::uint8_t *data_of(const std::string& xs) { return (const std::uint8_t *)xs.data(); }
inline const std::uint8_t *data_of(const std::int64_t& x) { return reinterpret_cast<const std::uint8_t *>(&x); }
inline const std::uint8_t *data_of(const option_flag_t&) { return nullptr; }

// Note that these are specific to FDB's needs (see return type, casts):
constexpr int size_of(const std::vector<std::uint8_t>& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::string& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::int64_t) { return sizeof(std::int64_t); }
constexpr int size_of(const option_flag_t&) { return 0; }

// ...also used:
inline const std::uint8_t *data_of(std::string_view xs) { return (const std::uint8_t *)xs.data(); }
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
    if (auto r = std::apply(set_option, as_fdb_option_args(option)); 0 != r) {
      throw libfdb_exception(fmt::format("while setting option {}; {}",
                             static_cast<int>(option.first),
                             libfdb_exception::make_fdb_error_string(r)));
    }
  });
}

// The global DB state and management thread:
// JFW: more user hooks that go into FDB system possible here
namespace database_system
{
inline bool was_initialized = false;
inline bool was_shutdown = false;

inline std::once_flag fdb_was_initialized;
inline std::jthread fdb_network_thread;

inline void initialize_fdb(const network_options& options)
{
 // This must be called before ANY other API function-- the structure
 // of fdb_database_system accomplishes this:
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
 was_shutdown = false;
}

inline bool initialized() noexcept { return was_initialized; }

inline void shutdown_fdb()
{
 using namespace std::chrono_literals;
 
 if (not initialized() or was_shutdown) {
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

 was_shutdown = true;
 was_initialized = false;
}

} // namespace database_system
 
} // namespace ceph::libfdb::detail

class database final
{
 private:
 struct database_deleter final
 {
  void operator()(FDBDatabase *db) const noexcept
  {
   fdb_database_destroy(db);
  }
 };

 std::unique_ptr<FDBDatabase, database_deleter> db_handle;

 static FDBDatabase *create_database_ptr(const std::filesystem::path& cluster_file_path,
                                         const network_options& network_opts) {
    std::call_once(detail::database_system::fdb_was_initialized,
                   detail::database_system::initialize_fdb,
                   network_opts);

    if (detail::database_system::was_shutdown) {
      throw libfdb_exception("FoundationDB already shut down");
    }

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
  : db_handle(create_database_ptr(cluster_file_path, network_opts))
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

 std::vector<versionstamp> version_stamps;

 private:
 bool get_single_value_from_transaction(const std::span<const std::uint8_t>& key,
                                        std::invocable<std::span<const std::uint8_t>> auto&& write_output_fn);

 public:
 transaction(database_handle dbh_)
 : dbh(dbh_),
   txn_handle(dbh->create_transaction(), &fdb_transaction_destroy)
 {}

 transaction(database_handle dbh_, const transaction_options& opts) 
  : transaction(dbh_)
 {
  detail::apply_options(opts,
             [handle = raw_handle()](auto opt, auto val, auto sz) {
               return fdb_transaction_set_option(handle, opt, val, sz);
             });
 }

 public:
 explicit operator bool() const noexcept { return dbh and nullptr != raw_handle(); }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_handle.get(); }

 private:
 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 void mark_version(const versionstamp& stamp)
 {
  if (stamp.is_resolved()) {
   throw std::invalid_argument("attempt to reuse resolved version stamp");
  }

  version_stamps.push_back(stamp);
 }

 template <FDBMutationType MutationKind>
 void set_versioned_data(std::span<const std::uint8_t> k,
                         std::span<const std::uint8_t> v,
                         const versionstamp& stamp)
 {
  fdb_transaction_atomic_op(raw_handle(),
                            (const std::uint8_t*)k.data(), k.size(),
                            (const std::uint8_t*)v.data(), v.size(),
                            MutationKind);

  mark_version(stamp);
 }

 void set(const versioned_bytes& k, std::span<const std::uint8_t> v)
 {
  set_versioned_data<FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY>(
    std::span<const std::uint8_t>(k.encoding_buffer), v, k.stamp);
 }

 void set(std::span<const std::uint8_t> k, const versioned_bytes& v)
 {
  set_versioned_data<FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE>(
    k, std::span<const std::uint8_t>(v.encoding_buffer), v.stamp);
 }

 bool get(const std::span<const std::uint8_t> k, concepts::value_callback auto&& val_collector) {
    return get_single_value_from_transaction(k, val_collector);
 }

 void erase(std::span<const std::uint8_t> k) {
    fdb_transaction_clear(raw_handle(),
                          (const std::uint8_t *)k.data(), k.size());
 }

 void erase(const ceph::libfdb::select& key_range) {
    const auto half_open_range = detail::as_half_open_select(key_range);

    fdb_transaction_clear_range(raw_handle(),
        (const uint8_t *)half_open_range.begin_key.data(), half_open_range.begin_key.size(),
        (const uint8_t *)half_open_range.end_key.data(), half_open_range.end_key.size());
 }

 [[nodiscard]] watch_handle make_watch(std::span<const std::uint8_t> key)
 {
  return watch_handle {
   detail::future_value {
    fdb_transaction_watch(raw_handle(), key.data(), key.size())
   }
  };
 }

 bool key_exists(std::string_view k) {
    return get_single_value_from_transaction(detail::as_fdb_span(k), [](auto) {});
 }

 bool commit();
 bool commit(fdb_error_t *replay_error);
 void destroy() noexcept { txn_handle.reset(); }

 // As you can see, friends are used extensively to implement the public interface; it would be nice to come up
 // with a strategy for making these "lists" a bit more managable, but for now it's what we have and it allows me ot
 // keep these handles mostly-opaque (this has proven to be a good idea as I've a few times shuffled internal details
 // with no disruption to the user interface surface):
 private:
 friend inline void set(transaction_handle,
                        const concepts::libfdb_key auto&,
                        const auto&,
                        const commit_after_op);
 friend inline void set(database_handle, const concepts::libfdb_key auto&, const auto&);
 friend inline void set(transaction_handle,
                        const concepts::libfdb_key auto&,
                        const ceph::libfdb::concepts::stringview_convertible auto&,
                        const commit_after_op);
 friend inline void set(database_handle,
                        const concepts::libfdb_key auto&,
                        const ceph::libfdb::concepts::stringview_convertible auto&);
 friend inline void set(transaction_handle, const versioned_bytes&, const auto&, const commit_after_op);
 friend inline void set(database_handle, const versioned_bytes&, const auto&);
 friend inline void set(transaction_handle, const concepts::libfdb_key auto&, const versioned_bytes&, const commit_after_op);
 friend inline void set(database_handle, const concepts::libfdb_key auto&, const versioned_bytes&);

 template <typename OutputTargetOrFnT>
 requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
          concepts::decoded_value_sink<OutputTargetOrFnT&&>
 friend inline bool get(ceph::libfdb::transaction_handle,
                        const concepts::libfdb_key auto&,
                        OutputTargetOrFnT&&,
                        const commit_after_op);

 friend inline bool key_exists(transaction_handle txn,
                               const concepts::libfdb_key auto& k,
                               const commit_after_op commit_after);

 friend inline bool commit(transaction_handle& txn);
 friend inline commit_result commit(with_result_t, transaction_handle& txn);
 friend inline bool commit(transaction_handle& txn, const versionstamp& stamp);
 friend inline watch_handle make_watch(transaction_handle txn, std::string_view key);
 friend inline fdb_error_t ceph::libfdb::detail::do_commit(transaction_handle& txn);
 friend inline void ceph::libfdb::detail::transaction_set_kv_bytes(const transaction_handle&,
                                                                   std::span<const std::uint8_t>,
                                                                   std::span<const std::uint8_t>);
 friend inline void ceph::libfdb::detail::transaction_clear_key_bytes(const transaction_handle&,
                                                                      std::span<const std::uint8_t>);
 friend inline void ceph::libfdb::detail::transaction_clear_range(const transaction_handle&,
                                                                 const ceph::libfdb::select&);

};

namespace detail {

// Since lambdas cannot be friend-functions, we use a named helper:
inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     std::span<const std::uint8_t> k,
                                     std::span<const std::uint8_t> v)
{
 txn->set(k, v);
}

inline void transaction_clear_key_bytes(const transaction_handle& txn,
                                        std::span<const std::uint8_t> k)
{
 txn->erase(k);
}

inline void transaction_clear_range(const transaction_handle& txn,
                                    const ceph::libfdb::select& key_range)
{
 txn->erase(key_range);
}

} // namespace detail

inline bool ceph::libfdb::transaction::get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output)
{
 const fdb_bool_t is_snapshot = false; 

 auto fv = detail::block_until_ready(detail::future_value(fdb_transaction_get(
                                    raw_handle(),
                                    (const uint8_t *)key.data(), key.size(),
                                    is_snapshot)));
 auto *future = fv.raw_ptr_or_throw();
 
  fdb_bool_t key_was_found = false;
  const uint8_t *out_buffer = nullptr;
  int out_len = 0;

  if (fdb_error_t r = fdb_future_get_value(future, &key_was_found, &out_buffer, &out_len); 0 != r) {
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

[[nodiscard]] inline bool ceph::libfdb::transaction::commit()
{
 return commit(nullptr);
}

[[nodiscard]] inline bool ceph::libfdb::transaction::commit(fdb_error_t *replay_error)
{
 if (nullptr != replay_error) {
  *replay_error = 0;
 }

 // We don't want to try to vivify for an "empty" commit:
 if (!*this)
  return false;

 std::optional<detail::future_value> versionstamp_future;

 if (not version_stamps.empty()) {
  versionstamp_future.emplace(fdb_transaction_get_versionstamp(raw_handle()));
 }

 detail::future_value fv(fdb_transaction_commit(raw_handle()));
 auto *commit_future = fv.raw_ptr_or_throw();

 if (fdb_error_t r = fdb_future_block_until_ready(commit_future); 0 != r) {
  throw libfdb_exception(r);
 }

 if (fdb_error_t r = fdb_future_get_error(commit_future); 0 != r) {
  detail::future_value ferror_result = detail::wait_for_on_error(raw_handle(), r);

  if (0 != detail::get_future_error(ferror_result)) {
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

  version_stamps.clear();
  return false;
 }

 if (versionstamp_future) {
  auto ready = detail::block_until_ready(std::move(*versionstamp_future));

  const uint8_t *out_versionstamp_data = nullptr;
  int out_versionstamp_size = 0;

  if (fdb_error_t r = fdb_future_get_key(ready.raw_handle(), &out_versionstamp_data, &out_versionstamp_size); 0 != r) {
   throw libfdb_exception(r);
  }

  constexpr auto expected_versionstamp_size =
   std::tuple_size_v<versionstamp::versionstamp_data_t>;

  if (expected_versionstamp_size != static_cast<std::size_t>(out_versionstamp_size) ||
      nullptr == out_versionstamp_data) {
   throw libfdb_exception("invalid version stamp result");
  }

  const auto versionstamp_result =
   std::span(out_versionstamp_data, expected_versionstamp_size);

  for (auto& stamp : version_stamps) {
   stamp.store_result(versionstamp_result);
  }

  version_stamps.clear();
 }

 // Ok:
 return true;
}

} // namespace ceph::libfdb

// Future-wrangling and tricky retry-handling:
namespace ceph::libfdb::detail {

inline future_value block_until_ready(future_value&& fv)
{
 auto *future = fv.raw_ptr_or_throw();

 if (fdb_error_t r = fdb_future_block_until_ready(future); 0 != r) {
  throw libfdb_exception(r);
 }

 // Note that fdb_future_block_until_ready() does not by itself check for errors
 // with the value; so, we need to do this separately:
 fdb_error_t r = fdb_future_get_error(future);

 if (0 != r) {
  throw libfdb_exception(r);
 }

 return fv;
}

inline future_value wait_until_ready(future_value&& fv)
{
 if (fdb_error_t r = fdb_future_block_until_ready(fv.raw_ptr_or_throw()); 0 != r) {
  throw libfdb_exception(r);
 }

 return fv;
}

inline fdb_error_t get_future_error(const future_value& fv)
{
 return fdb_future_get_error(fv.raw_ptr_or_throw());
}

inline future_value wait_for_on_error(FDBTransaction* txn, const fdb_error_t original_error)
{
 future_value on_error_future(fdb_transaction_on_error(txn, original_error));

 if (0 != fdb_future_block_until_ready(on_error_future.raw_ptr_or_throw())) {
  throw libfdb_exception(original_error);
 }

 return on_error_future;
}

template <typename FnT, typename... XS>
requires std::invocable<FnT, XS...>
inline future_value await_future_of(FnT&& fn, XS&& ...params)
{
 return block_until_ready(
          std::invoke(std::forward<FnT>(fn), std::forward<XS>(params)...));
}

// Tracks a set of results and their corresponding Future:
struct query_window final
{
 future_value result_owner;
 std::span<const FDBKeyValue> result_pairs;
 bool more_available = false;
};

struct split_point_result final
{
 future_value result_owner;
 std::span<const FDBKey> result_keys;
 fdb_error_t error = 0;
};

inline query_window extract_result_pairs(future_value result_owner)
{
 int more_available = 0;
 int out_count = 0;
 const FDBKeyValue *out_kvs = nullptr;

 if (fdb_error_t r = fdb_future_get_keyvalue_array(result_owner.raw_ptr_or_throw(),
                                                   &out_kvs,
                                                   &out_count,
                                                   &more_available); 0 != r) {
  throw libfdb_exception(r);
 }

 return query_window {
  .result_owner = std::move(result_owner),
  .result_pairs = std::span<const FDBKeyValue>(out_kvs, out_count),
  .more_available = 0 != more_available
 };
}

inline split_point_result extract_split_points(future_value result_owner)
{
 const FDBKey *result_keys = nullptr;
 int result_count = 0;

 const auto error = fdb_future_get_key_array(result_owner.raw_ptr_or_throw(), &result_keys, &result_count);

 return split_point_result {
  .result_owner = std::move(result_owner),
  .result_keys = 0 == error ? std::span<const FDBKey>(result_keys, result_count)
                            : std::span<const FDBKey>(),
  .error = error
 };
}

inline query_window read_query_window(transaction& txn, const select& key_range, const int iteration)
{
 return extract_result_pairs(await_future_of([&]() {
  return get_range_future_from_transaction(txn, key_range, iteration);
 }));
}

inline std::optional<select> next_range_after(select key_range, const query_window& window)
{
 if (not window.more_available or window.result_pairs.empty()) {
  return std::nullopt;
 }

 const auto& last_key = window.result_pairs.back();
 auto cursor = std::string_view((const char *)last_key.key, last_key.key_length);

 if (key_range.options.reverse_order) {
  key_range.end_key = cursor;
  key_range.end_inclusive = false;
  return key_range;
 }

 key_range.begin_key = cursor;
 key_range.begin_inclusive = false;

 return key_range;
}

template <typename ValueT = std::string>
inline auto decode_pairs(std::span<const FDBKeyValue> pairs)
{
 return pairs | std::views::transform(to_decoded_kv_pair<ValueT>);
}

template <typename ValueT, typename AssocT>
inline AssocT collect_pairs(std::span<const FDBKeyValue> pairs)
{
 return ceph::util::collect_as<AssocT>(decode_pairs<ValueT>(pairs));
}

template <typename AssocT>
struct query_window_result final
{
 AssocT result_block;
 std::optional<select> next_range;
};

template <typename ValueT, typename AssocT>
inline query_window_result<AssocT> materialize_query_window(transaction& txn, select key_range, const int iteration = 1)
{
 auto window = read_query_window(txn, key_range, iteration);

 return {
  .result_block = collect_pairs<ValueT, AssocT>(window.result_pairs),
  .next_range = next_range_after(std::move(key_range), window)
 };
}

inline std::size_t for_each_decoded_kv_pair(transaction& txn,
                                            const select& key_range,
                                            auto&& fn)
{
 std::size_t nread = 0;

 for (const auto& kv : detail::generate_FDB_pairs(txn, key_range) | std::views::join) {
  std::invoke(fn, to_decoded_kv_pair<std::string>(kv));
  ++nread;
 }

 return nread;
}

template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline std::size_t get_value_range_from_transaction(transaction& txn, const select& key_range, OutIterT& out_iter)
{
 return for_each_decoded_kv_pair(txn, key_range,
          [&out_iter](auto&& kv) {
            *out_iter++ = std::forward<decltype(kv)>(kv);
          });
}

inline std::size_t get_value_range_from_transaction(transaction& txn,
                                                    const select& key_range,
                                                    concepts::string_pair_output_range auto& out)
{
 return for_each_decoded_kv_pair(txn, key_range,
          [&out](auto&& kv) {
            ceph::util::push_back(out, std::forward<decltype(kv)>(kv));
          });
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

 if (fdb_error_t on_error_r = get_future_error(wait_for_on_error(txn->raw_handle(), r)); 0 != on_error_r) {
  throw ceph::libfdb::libfdb_exception(r);
 }

 return true;
}

// Convert FDBKey array into something useful:
inline std::vector<ceph::libfdb::select> as_select_seq(std::span<const FDBKey> xs,
                                                       const ceph::libfdb::select& parent)
{
 if (2 > xs.size()) {
  return {};
 }

 // Gather the flattened list into *overlapping* libfdb::select pairs:
 return ceph::util::collect_as<std::vector<ceph::libfdb::select>>(
          std::views::iota(std::size_t{0}, xs.size() - 1)
        | std::views::transform([&parent, xs](const auto i) {
           const auto& fst = xs[i];
           const auto& snd = xs[i + 1];
           const auto first_key = std::string_view((const char *)fst.key,
                                                    static_cast<std::string::size_type>(fst.key_length));
           const auto second_key = std::string_view((const char *)snd.key,
                                                     static_cast<std::string::size_type>(snd.key_length));

           ceph::libfdb::select split(first_key, second_key);

           split.options = parent.options;
           split.begin_inclusive = (0 == i) ? parent.begin_inclusive : true;
           split.end_inclusive = (i + 2 == xs.size()) ? parent.end_inclusive : false;
           return split;
          }));
}
// Finding a clear example both in the samples and in the documentation is not very easy. The
// statelessness of FDB requests bleeds into here with basically no hand-holding, but note for instance
// that the call parameters have to change for subsequent reads.
inline future_value get_range_future_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& selection, int iteration)
{
  const auto& begin_key  = selection.begin_key;
  const auto& end_key    = selection.end_key;

  const auto& options    = selection.options;

  // The documentation makes this stuff about as clear as mud... read VERY carefully
  // when you fiddle with these:
  const bool continuing_forward = not options.reverse_order and 1 < iteration;
  const bool continuing_reverse = options.reverse_order and 1 < iteration;

  const int begin_or_eq = (continuing_forward or not selection.begin_inclusive) ? 1 : 0;
  const int begin_offset = 1;
  const int end_or_eq = (not continuing_reverse and selection.end_inclusive) ? 1 : 0;
  const int end_offset = 1;

  // See validate_and_update_parameters() in fdb_c.cpp (FDB source) if greater clarity is needed on
  // the meaning of some of these, it can be hard to deduce from the documentation; Returns an
  // FDBKeyValueArray in the future:
  return future_value(fdb_transaction_get_range(txn.raw_handle(),
                      (const uint8_t *)begin_key.data(), begin_key.size(),      // the reference-point key
                      begin_or_eq,                                              // begin or eq
                      begin_offset,                                             // begin offset

                      (const uint8_t *)end_key.data(), end_key.size(),          // the end selector key
                      end_or_eq,                                                // end or eq
                      end_offset,                                               // end offset (a shift AFTER end is matched)

                      // How should results be grouped/chunked:
                      options.result_limit,                                     // FDB range-read limit (0 == unlimited)
                      options.target_bytes,                                     // FDB range-read target bytes (0 == unlimited)
                      options.streaming_mode,                                   // streaming mode (e.g.: FDB_STREAMING_MODE_WANT_ALL)
                      iteration,                                                // iteration # (produced side effect)

                      // Other options:
                      0,                                                        // 0 unless this IS a snapshot read
                      options.reverse_order                                     // should items come in reverse order?
                    ));
}

inline std::vector<ceph::libfdb::select> plan_split_ranges(
          ceph::libfdb::database_handle dbh, 
          ceph::libfdb::select selector, 
          const std::int64_t remote_chunk_size)
{
 using ceph::libfdb::detail::wait_until_ready;

 auto txn = ceph::libfdb::make_transaction(dbh);
 auto split_selector = ceph::libfdb::detail::as_half_open_select(selector);

 for (bool should_retry = true; should_retry;) {
  auto result_owner = wait_until_ready(future_value(fdb_transaction_get_range_split_points(
                       txn->raw_handle(),
                       (const std::uint8_t *)split_selector.begin_key.data(), static_cast<int>(split_selector.begin_key.length()),
                       (const std::uint8_t *)split_selector.end_key.data(), static_cast<int>(split_selector.end_key.length()),
                       remote_chunk_size)));

  auto split_points = extract_split_points(std::move(result_owner));

  should_retry = retry_after_error(txn, split_points.error);

  if (not should_retry) {
   return as_select_seq(split_points.result_keys, split_selector);
  }
 }

 return {};
}

// Generators (internal guts):
// The returned memory should be copied immediately as its lifetime will end once the Future is destroyed:
// (This is pretty much why this is in the "detail" namespace-- we use this to implement the user-facing
// stuff, it shouldn't really be touched outside.)
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(transaction& txn, select key_range)
{
 // FDB uses iteration for FDB_STREAMING_MODE_ITERATOR (but, other streaming modes
 // just ignore it, per fdb_c's documentation. We do have to keep this state somewhere,
 // though. See: "https://apple.github.io/foundationdb/api-c.html".
 int iteration = 1;

 for (auto more_available = true; more_available; iteration++) {
  auto window = read_query_window(txn, key_range, iteration);
  auto next_range = next_range_after(key_range, window);

  more_available = next_range.has_value();

  co_yield window.result_pairs;

  if (next_range) {
   key_range = std::move(*next_range);
  }
 }
}

} // namespace ceph::libfdb::detail


#endif
