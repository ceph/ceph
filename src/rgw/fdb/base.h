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
#include <string>
#include <vector>
#include <variant>
#include <optional>

#include <ranges>
#include <iterator>
#include <algorithm>

#include <bit>
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

#include <mutex>
#include <thread>

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
class key_selector;

class database;
class transaction;

using database_handle = std::shared_ptr<database>;
using transaction_handle = std::shared_ptr<transaction>;

// Serializable reads add FDB read conflicts; snapshot reads do not:
enum struct read_mode { serializable, snapshot };

extern transaction_handle make_transaction(database_handle dbh);
[[nodiscard]] inline watch_handle make_watch(transaction_handle txn, std::string_view key);

} // namespace ceph::libfdb

namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& from,
                    ceph::libfdb::versionstamp& to);

} // namespace ceph::libfdb::from

// Helpers used by the mostly opaque transaction type:
namespace ceph::libfdb::detail {

using byte_view = std::span<const std::uint8_t>;

struct future_value;

inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     byte_view key,
                                     byte_view value);
inline void transaction_atomic_op(const transaction_handle& txn,
                                  byte_view key,
                                  byte_view param,
                                  FDBMutationType type);
inline void transaction_clear_key_bytes(const transaction_handle& txn,
                                        byte_view key);
inline void transaction_clear_range(const transaction_handle& txn,
                                    const ceph::libfdb::select& key_range);
inline void transaction_mark_conflict_range(const transaction_handle& txn,
                                            const ceph::libfdb::select& key_range,
                                            FDBConflictRangeType type);
inline future_value transaction_get_range_split_points(const transaction_handle& txn,
                                                       std::span<const std::uint8_t> begin,
                                                       std::span<const std::uint8_t> end,
                                                       std::int64_t target_bytes);
inline future_value transaction_get_estimated_range_size(const transaction_handle& txn,
                                                         const ceph::libfdb::select& selection);
inline future_value transaction_get_key(const transaction_handle& txn,
                                        const key_selector& selector,
                                        read_mode mode);

inline future_value block_until_ready(future_value&& fv);
inline fdb_error_t get_future_error(const future_value& fv);
inline future_value wait_for_on_error(FDBTransaction *txn,
                                      fdb_error_t original_error);
[[nodiscard]] inline std::int64_t extract_int64(future_value result_owner);
[[nodiscard]] inline std::uint64_t extract_uint64(future_value result_owner);
[[nodiscard]] inline std::string extract_byte_string(future_value result_owner);
[[nodiscard]] inline bool reset_for_replay_if_needed(transaction_handle& txn,
                                                      fdb_error_t error);

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

} // namespace ceph::libfdb::concepts

// libfdb_exception represents libfdb operation failures.
// Caller contract errors use standard exceptions such as std::invalid_argument.
namespace ceph::libfdb {

/* FoundationDB key selectors navigate among stored keys. Prefer the named
 * factory functions below; arbitrary offsets are an advanced FDB feature. */
class key_selector final
{
 std::string key;

 bool or_equal;
 int offset;

 public:
 key_selector(const key_selector&) = default;
 key_selector& operator=(const key_selector&) = default;
 key_selector(key_selector&&) noexcept = default;
 key_selector& operator=(key_selector&&) noexcept = default;

 private:
 constexpr key_selector(std::string_view key_,
                        const bool or_equal_,
                        const int offset_)
  : key(key_),
    or_equal(or_equal_),
    offset(offset_)
 {}

 private:
 template <concepts::libfdb_key KeyT>
 friend constexpr key_selector lower(const KeyT& key);

 template <concepts::libfdb_key KeyT>
 friend constexpr key_selector floor(const KeyT& key);

 template <concepts::libfdb_key KeyT>
 friend constexpr key_selector ceiling(const KeyT& key);

 template <concepts::libfdb_key KeyT>
 friend constexpr key_selector higher(const KeyT& key);

 friend class transaction;
};

template <concepts::libfdb_key KeyT>
[[nodiscard]] constexpr key_selector lower(const KeyT& key)
{
 return key_selector(detail::as_libfdb_key_view(key), false, 0);
}

template <concepts::libfdb_key KeyT>
[[nodiscard]] constexpr key_selector floor(const KeyT& key)
{
 return key_selector(detail::as_libfdb_key_view(key), true, 0);
}

template <concepts::libfdb_key KeyT>
[[nodiscard]] constexpr key_selector ceiling(const KeyT& key)
{
 return key_selector(detail::as_libfdb_key_view(key), false, 1);
}

template <concepts::libfdb_key KeyT>
[[nodiscard]] constexpr key_selector higher(const KeyT& key)
{
 return key_selector(detail::as_libfdb_key_view(key), true, 1);
}

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
 private:
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 explicit future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

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

 friend class ceph::libfdb::transaction;
};

inline byte_view as_byte_view(concepts::libfdb_key_view auto key)
{
 return byte_view(reinterpret_cast<const std::uint8_t *>(key.data()), key.size());
}

} // namespace detail

// watch_handle can only be constructed by calling make_watch():
class watch_handle final
{
 private:
 detail::future_value watch_future;

 private:
 explicit watch_handle(detail::future_value future)
  : watch_future(std::move(future))
 {}

 watch_handle() = delete;
 watch_handle(const watch_handle&) = delete;
 watch_handle& operator=(const watch_handle&) = delete;

 public:
 watch_handle(watch_handle&&) noexcept = default;
 watch_handle& operator=(watch_handle&&) noexcept = default;

 public:
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
 friend watch_handle make_watch(transaction_handle txn, std::string_view key);
 friend class transaction;
};

namespace detail {

inline byte_view as_byte_view(const concepts::libfdb_key auto& key)
requires (!concepts::libfdb_key_view<decltype(key)>)
{
 return as_byte_view(as_libfdb_key_view(key));
}

} // namespace ceph::libfdb::detail

struct versionstamp final
{
 // FoundationDB versionstamps are 10 bytes: 8 bytes of committed
 // database version followed by 2 bytes of transaction batch order.
 using versionstamp_data_t = std::array<std::uint8_t, 10>;

 private:
 std::shared_ptr<std::optional<versionstamp_data_t>> result =
  std::make_shared<std::optional<versionstamp_data_t>>();

 void store_result(const std::span<const std::uint8_t> versionstamp_result);

 public:
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
 friend class transaction;
 friend void ceph::libfdb::from::convert(const std::span<const std::uint8_t>&,
                                         ceph::libfdb::versionstamp&);
};

// versioned_bytes can only be constructed by calling versioned():
// It carries data in transaction-correct version stamp encoding:
struct versioned_bytes final
{
 private:
 std::vector<std::uint8_t> encoding_buffer;
 versionstamp stamp;

 versioned_bytes(std::vector<std::uint8_t> buffer, versionstamp version)
  : encoding_buffer(std::move(buffer)),
    stamp(std::move(version))
 {}

 public:
 versioned_bytes() = delete;

 versioned_bytes(const versioned_bytes&) = default;
 versioned_bytes(versioned_bytes&&) noexcept = default;

 versioned_bytes& operator=(const versioned_bytes&) = default;
 versioned_bytes& operator=(versioned_bytes&&) noexcept = default;

 private:
 friend class transaction;

 template <concepts::stringview_convertible PrefixT>
 friend versioned_bytes versioned(const PrefixT& prefix, versionstamp stamp);

 template <concepts::stringview_convertible PrefixT, concepts::stringview_convertible SuffixT>
 friend versioned_bytes versioned(const PrefixT& prefix, const SuffixT& suffix, versionstamp stamp);
};

namespace detail {

constexpr auto little_endian_bytes(std::integral auto value) noexcept
{
 static_assert(std::endian::little == std::endian::native or
               std::endian::big == std::endian::native);

 if constexpr (std::endian::big == std::endian::native) {
  value = std::byteswap(value);
 }

 return std::bit_cast<std::array<std::uint8_t, sizeof value>>(value);
}

inline std::vector<std::uint8_t> make_versioned_encoding(std::string_view prefix, std::string_view suffix)
{
 constexpr auto versionstamp_byte_count = std::tuple_size_v<versionstamp::versionstamp_data_t>;

 std::vector<std::uint8_t> out;
 out.reserve(prefix.size() + versionstamp_byte_count + suffix.size() + sizeof(std::uint32_t));

 std::ranges::copy(prefix, std::back_inserter(out));

 if (not std::in_range<std::uint32_t>(out.size())) {
  throw std::invalid_argument("version-stamped prefix is too large");
 }

 const auto versionstamp_offset = static_cast<std::uint32_t>(out.size());
 out.resize(out.size() + versionstamp_byte_count);

 std::ranges::copy(suffix, std::back_inserter(out));

 // FDB expects a little-endian uint32 offset to the 10-byte placeholder:
 std::ranges::copy(little_endian_bytes(versionstamp_offset),
                   std::back_inserter(out));

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
  result->emplace(result_value);
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

// ...i.e. the next FoundationDB key (lexicographical match):
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

// Caller-supplied database sources are tagged so paths and connection strings
// cannot be confused:
struct connection_source final
{
 private:
 enum struct source_kind {
  cluster_file,
  connection_string
 };

 source_kind kind;
 std::string value;

 public:
 explicit connection_source(std::filesystem::path path)
  : kind(source_kind::cluster_file),
    value(path.string())
 {
  if (std::empty(value)) {
   throw std::invalid_argument("connection_source requires non-empty cluster file path");
  }
 }

 explicit connection_source(std::string value_)
  : kind(source_kind::connection_string),
    value(std::move(value_))
 {
  if (std::empty(value)) {
   throw std::invalid_argument("connection_source requires non-empty connection string");
  }
 }

 explicit connection_source(const std::string_view value_)
  : connection_source(std::string(value_))
 {}

 explicit connection_source(const char *value_)
  : connection_source(value_ ? std::string_view(value_) : std::string_view())
 {}

 connection_source(std::nullptr_t) = delete;

 private:
 friend class database;
};

namespace detail {

constexpr int checked_fdb_size(const std::size_t size)
{
 if (not std::in_range<int>(size)) {
  throw libfdb_exception("value is too large for the FoundationDB C API");
 }

 return static_cast<int>(size);
}

constexpr std::size_t checked_result_size(const int size)
{
 if (0 > size) {
  throw libfdb_exception("FoundationDB returned a negative result size");
 }

 return static_cast<std::size_t>(size);
}

// FoundationDB's C interface represents every byte range as a pointer and a
// checked int length:
struct fdb_bytes final
{
 const std::uint8_t *data;
 int length;
};

constexpr fdb_bytes as_fdb_bytes(const byte_view bytes)
{
 return {
  .data = bytes.data(),
  .length = checked_fdb_size(bytes.size())
 };
}

inline fdb_bytes as_fdb_bytes(const concepts::libfdb_key auto& key)
{
 return as_fdb_bytes(as_byte_view(key));
}

constexpr byte_view result_bytes(const std::uint8_t *data, const int length)
{
 const auto size = checked_result_size(length);

 if (0 != size && nullptr == data) {
  throw libfdb_exception("FoundationDB returned a NULL result buffer");
 }

 return byte_view(data, size);
}

inline std::string_view as_string_view(const byte_view bytes) noexcept
{
 if (bytes.empty()) {
  return {};
 }

 return std::string_view(
  reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

inline std::string_view key_view(const auto& result)
{
 return as_string_view(result_bytes(result.key, result.key_length));
}

inline byte_view value_view(const FDBKeyValue& pair)
{
 return result_bytes(pair.value, pair.value_length);
}

[[nodiscard]] inline std::int64_t extract_int64(future_value result_owner)
{
 std::int64_t value = 0;

 if (const auto ec =
       fdb_future_get_int64(result_owner.raw_ptr_or_throw(), &value);
     0 != ec) {
  throw libfdb_exception(ec);
 }

 return value;
}

[[nodiscard]] inline std::uint64_t extract_uint64(future_value result_owner)
{
 std::uint64_t value = 0;

 if (const auto ec =
       fdb_future_get_uint64(result_owner.raw_ptr_or_throw(), &value);
     0 != ec) {
  throw libfdb_exception(ec);
 }

 return value;
}

[[nodiscard]] inline std::string extract_byte_string(future_value result_owner)
{
 const std::uint8_t *data = nullptr;
 int size = 0;

 if (const auto ec =
       fdb_future_get_key(result_owner.raw_ptr_or_throw(), &data, &size);
     0 != ec) {
  throw libfdb_exception(ec);
 }

 return std::string(as_string_view(result_bytes(data, size)));
}

inline byte_view option_bytes(const std::vector<std::uint8_t>& value) noexcept
{
 return value;
}

inline byte_view option_bytes(const std::string& value) noexcept
{
 return as_byte_view(std::string_view(value));
}

inline byte_view option_bytes(const option_flag_t&) noexcept
{
 return {};
}

inline auto apply_option_value(auto& set_option,
                               const auto code,
                               const std::int64_t value)
{
 const auto bytes = little_endian_bytes(value);
 const auto input = as_fdb_bytes(byte_view(bytes));

 return std::invoke(set_option, code, input.data, input.length);
}

template <typename ValueT>
requires (not std::same_as<std::int64_t, std::remove_cvref_t<ValueT>>)
inline auto apply_option_value(auto& set_option,
                               const auto code,
                               const ValueT& value)
{
 const auto input = as_fdb_bytes(option_bytes(value));

 return std::invoke(set_option, code, input.data, input.length);
}

inline void apply_options(const auto& option_map, auto&& set_option)
{
 std::ranges::for_each(option_map, [&set_option](const auto& option) {
    const auto apply = [&set_option, code = option.first](const auto& value) {
      return apply_option_value(set_option, code, value);
    };

    if (const auto ec = std::visit(apply, option.second); 0 != ec) {
      throw libfdb_exception(fmt::format(
        "while setting option {}; {}", std::to_underlying(option.first),
        libfdb_exception::make_fdb_error_string(ec)));
    }
  });
}

// The global DB state and management thread:
namespace database_system {
inline bool was_initialized = false;
inline bool was_shutdown = false;

inline std::once_flag fdb_was_initialized;

inline std::jthread fdb_network_thread;

inline fdb_error_t fdb_network_error = 0;

inline void initialize_fdb(const network_options& options)
{
 // This must be called before ANY other API function-- the structure
 // of fdb_database_system accomplishes this:
 if (const auto ec = fdb_select_api_version(FDB_API_VERSION); 0 != ec) {
  throw libfdb_exception(ec);
 }

 // Zero or more calls to this may now be made:
 apply_options(options, fdb_network_set_option);
 
 // This must be called before any other API function (besides >= 0 calls to fdb_network_set_option()):
 if (const auto ec = fdb_setup_network(); 0 != ec) {
  throw libfdb_exception(ec);
 }
 
 // Launch network thread:
 fdb_network_error = 0;
 fdb_network_thread = std::jthread {[] {
  fdb_network_error = fdb_run_network();
 }};
 
 // Okie-dokie, we're all set (distinct from fdb_was_initialized):
 was_initialized = true;
 was_shutdown = false;
}

inline bool initialized() noexcept { return was_initialized; }

inline void shutdown_fdb()
{
 if (not initialized() or was_shutdown) {
   return;
 }

 const auto stop_error = fdb_stop_network();
 
 if (fdb_network_thread.joinable()) {
  fdb_network_thread.join();
 }

 was_shutdown = true;
 was_initialized = false;

 if (0 != stop_error) {
  throw libfdb_exception(stop_error);
 }

 if (0 != fdb_network_error) {
  throw libfdb_exception(fdb_network_error);
 }
}

} // namespace database_system
 
} // namespace ceph::libfdb::detail

class database final
{
 private:
 using create_db_fn_t = decltype(&fdb_create_database);

 struct database_deleter final
 {
  void operator()(FDBDatabase *db) const noexcept
  {
   fdb_database_destroy(db);
  }
 };

 std::unique_ptr<FDBDatabase, database_deleter> db_handle;

 static FDBDatabase *create_database_ptr(const char *source,
                                         const create_db_fn_t create_db_fn,
                                         const network_options& network_opts)
 {
  std::call_once(detail::database_system::fdb_was_initialized,
                 detail::database_system::initialize_fdb,
                 network_opts);

  if (detail::database_system::was_shutdown) {
   throw libfdb_exception("FoundationDB already shut down");
  }

  FDBDatabase *database = nullptr;

  if (const auto ec = create_db_fn(source, &database); 0 != ec) {
   throw libfdb_exception(ec);
  }

  return database;
 }

 database(const char *source,
          const create_db_fn_t create_db_fn,
          const ceph::libfdb::database_options& db_opts,
          const network_options& network_opts)
  : db_handle(create_database_ptr(source, create_db_fn, network_opts))
 {
  detail::apply_options(
    db_opts,
    [handle = raw_handle()](auto option_code, auto data, auto size) {
      return fdb_database_set_option(handle, option_code, data, size);
    });
 }

 FDBTransaction *create_transaction()
 {
  FDBTransaction *txn = nullptr;

  if (const auto ec = fdb_database_create_transaction(raw_handle(), &txn);
      0 != ec) {
   throw libfdb_exception(ec);
  }

  return txn;
 }

 public:
 database(connection_source source,
          const ceph::libfdb::database_options& db_opts,
          const network_options& network_opts)
  : database(source.value.c_str(),
             connection_source::source_kind::cluster_file == source.kind
              ? fdb_create_database
              : fdb_create_database_from_connection_string,
             db_opts,
             network_opts)
 {}

 database(connection_source source,
          const ceph::libfdb::database_options& db_opts)
  : database(std::move(source), db_opts, {})
 {}

 database(const ceph::libfdb::database_options& db_opts, const ceph::libfdb::network_options& net_opts)
  : database(nullptr, fdb_create_database, db_opts, net_opts)
 {}

 database(const ceph::libfdb::database_options& db_opts)
  : database(db_opts, {})
 {}

 database()
  : database(database_options {}, network_options {})
 {}

 explicit operator bool() const noexcept { return nullptr != raw_handle(); }

 FDBDatabase *raw_handle() const noexcept { return db_handle.get(); }

 // FDB client-network load: zero is idle; one or greater is saturated.
 [[nodiscard]] double client_network_load() const
 {
  return fdb_database_get_main_thread_busyness(raw_handle());
 }

 [[nodiscard]] std::uint64_t server_protocol(const std::uint64_t expected_version = 0) const
 {
  return detail::extract_uint64(
           detail::block_until_ready(
            detail::future_value(
             fdb_database_get_server_protocol(raw_handle(), expected_version))));
 }

 [[nodiscard]] std::string client_status_json() const
 {
  return detail::extract_byte_string(
           detail::block_until_ready(
            detail::future_value(fdb_database_get_client_status(raw_handle()))));
 }

 void reboot_worker(std::string_view address, const bool check, const int duration) const
 {
  const auto address_bytes = detail::as_fdb_bytes(address);

  detail::block_until_ready(
   detail::future_value(
    fdb_database_reboot_worker(raw_handle(),
                               address_bytes.data,
                               address_bytes.length,
                               true == check,
                               duration)));
 }

 void force_recovery_with_data_loss(std::string_view dcid) const
 {
  const auto dcid_bytes = detail::as_fdb_bytes(dcid);

  detail::block_until_ready(
   detail::future_value(
    fdb_database_force_recovery_with_data_loss(raw_handle(),
                                               dcid_bytes.data,
                                               dcid_bytes.length)));
 }

 void create_snapshot(std::string_view uid, std::string_view snap_command) const
 {
  const auto uid_bytes = detail::as_fdb_bytes(uid);
  const auto snap_command_bytes = detail::as_fdb_bytes(snap_command);

  detail::block_until_ready(
   detail::future_value(
    fdb_database_create_snapshot(raw_handle(),
                                 uid_bytes.data,
                                 uid_bytes.length,
                                 snap_command_bytes.data,
                                 snap_command_bytes.length)));
 }

 private:
 friend transaction;
};

namespace detail {

[[nodiscard]] inline ceph::libfdb::database& database_or_throw(const ceph::libfdb::database_handle& dbh)
{
 if (dbh and *dbh) {
  return *dbh;
 }

 throw std::invalid_argument("invalid database handle");
}

} // namespace detail

class transaction final
{
 enum struct state_t { active, committed };

 database_handle dbh;
 std::unique_ptr<FDBTransaction, decltype(&fdb_transaction_destroy)> txn_handle;
 std::vector<versionstamp> version_stamps;

 state_t state = state_t::active;

 static database_handle require_database(database_handle dbh)
 {
  if (not dbh) {
   throw std::invalid_argument("transaction requires a database handle");
  }

  return dbh;
 }

 // State guards provide consistent handling without repetition:
 void require_active(const std::string_view operation) const
 {
  if (state_t::active != state || nullptr == raw_handle()) {
   throw std::invalid_argument(fmt::format("{} requires active transaction", operation));
  }
 }

 void require_committed(const std::string_view operation) const
 {
  if (state_t::committed != state || nullptr == raw_handle()) {
   throw std::invalid_argument(fmt::format("{} requires committed transaction", operation));
  }
 }

 void reset_for_replay(const fdb_error_t error)
 {
  require_active("reset_for_replay()");

  if (not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, error)) {
   throw libfdb_exception(error);
  }

  auto on_error_result = detail::wait_for_on_error(raw_handle(), error);

  if (const auto ec = detail::get_future_error(on_error_result); 0 != ec) {
   throw libfdb_exception(ec);
  }

  // Discard versionstamps registered by the abandoned attempt:
  version_stamps.clear();
 }

 bool get_single_value_from_transaction(
  detail::byte_view key,
  concepts::value_callback auto&& write_output_fn,
  read_mode mode);
 void recover_from_commit_error(detail::future_value& commit_future,
                                fdb_error_t error,
                                fdb_error_t *replay_error);
 void resolve_versionstamps(std::optional<detail::future_value>& versionstamp_future);

 public:
 transaction(database_handle database)
  : dbh(require_database(std::move(database))),
    txn_handle(dbh->create_transaction(), &fdb_transaction_destroy)
 {}

 transaction(database_handle database, const transaction_options& opts)
  : transaction(std::move(database))
 {
  detail::apply_options(
    opts,
    [handle = raw_handle()](auto option, auto data, auto size) {
      return fdb_transaction_set_option(handle, option, data, size);
    });
 }

 explicit operator bool() const noexcept { return dbh and nullptr != raw_handle(); }

 FDBTransaction *raw_handle() const noexcept { return txn_handle.get(); }

 private:
 void set(detail::byte_view key, detail::byte_view value)
 {
  const auto key_bytes = detail::as_fdb_bytes(key);
  const auto value_bytes = detail::as_fdb_bytes(value);

  fdb_transaction_set(raw_handle(),
                      key_bytes.data, key_bytes.length,
                      value_bytes.data, value_bytes.length);
 }

 void atomic_op(detail::byte_view key,
                detail::byte_view param,
                const FDBMutationType type)
 {
  const auto key_bytes = detail::as_fdb_bytes(key);
  const auto param_bytes = detail::as_fdb_bytes(param);

  fdb_transaction_atomic_op(raw_handle(),
                            key_bytes.data, key_bytes.length,
                            param_bytes.data, param_bytes.length,
                            type);
 }

 void mark_version(const versionstamp& stamp)
 {
  if (stamp.is_resolved()) {
   throw std::invalid_argument("attempt to reuse resolved version stamp");
  }

  version_stamps.push_back(stamp);
 }

 template <FDBMutationType MutationKind>
 void set_versioned_data(detail::byte_view key,
                         detail::byte_view value,
                         const versionstamp& stamp)
 {
  const auto key_bytes = detail::as_fdb_bytes(key);
  const auto value_bytes = detail::as_fdb_bytes(value);

  fdb_transaction_atomic_op(raw_handle(),
                            key_bytes.data, key_bytes.length,
                            value_bytes.data, value_bytes.length,
                            MutationKind);

  mark_version(stamp);
 }

 void set(const versioned_bytes& key, detail::byte_view value)
 {
  set_versioned_data<FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY>(
    detail::byte_view(key.encoding_buffer), value, key.stamp);
 }

 void set(detail::byte_view key, const versioned_bytes& value)
 {
  set_versioned_data<FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE>(
    key, detail::byte_view(value.encoding_buffer), value.stamp);
 }

 bool get(const detail::byte_view key,
          concepts::value_callback auto&& value_collector,
          const read_mode mode)
 {
  return get_single_value_from_transaction(key, value_collector, mode);
 }

 void erase(detail::byte_view key)
 {
  const auto key_bytes = detail::as_fdb_bytes(key);

  fdb_transaction_clear(raw_handle(), key_bytes.data, key_bytes.length);
 }

 void erase(const ceph::libfdb::select& key_range)
 {
  const auto half_open_range = detail::as_half_open_select(key_range);
  const auto begin = detail::as_fdb_bytes(half_open_range.begin_key);
  const auto end = detail::as_fdb_bytes(half_open_range.end_key);

  fdb_transaction_clear_range(
    raw_handle(),
    begin.data, begin.length,
    end.data, end.length);
 }

 void mark_conflict(const ceph::libfdb::select& key_range,
                    const FDBConflictRangeType type)
 {
  const auto half_open_range = detail::as_half_open_select(key_range);

  if (not (half_open_range.begin_key < half_open_range.end_key)) {
   throw std::invalid_argument("conflict range must be non-empty");
  }

  if (FDB_CONFLICT_RANGE_TYPE_READ == type) {
   std::ignore = read_version(); // anchor the conflict to a read version
  }

  const auto begin = detail::as_fdb_bytes(half_open_range.begin_key);
  const auto end = detail::as_fdb_bytes(half_open_range.end_key);

  if (const auto ec = fdb_transaction_add_conflict_range(
        raw_handle(),
        begin.data, begin.length,
        end.data, end.length,
        type);
      0 != ec) {
   throw libfdb_exception(ec);
  }
 }

 [[nodiscard]] watch_handle make_watch(detail::byte_view key)
 {
  const auto key_bytes = detail::as_fdb_bytes(key);

  return watch_handle {
   detail::future_value {
    fdb_transaction_watch(raw_handle(), key_bytes.data, key_bytes.length)
   }
  };
 }

 detail::future_value get_range_split_points(detail::byte_view begin,
                                             detail::byte_view end,
                                             const std::int64_t target_bytes)
 {
  const auto begin_bytes = detail::as_fdb_bytes(begin);
  const auto end_bytes = detail::as_fdb_bytes(end);

  return detail::future_value {
   fdb_transaction_get_range_split_points(
    raw_handle(),
    begin_bytes.data, begin_bytes.length,
    end_bytes.data, end_bytes.length,
    target_bytes)
  };
 }

 detail::future_value get_estimated_range_size(const select& selection)
 {
  const auto half_open_range = detail::as_half_open_select(selection);
  const auto begin = detail::as_fdb_bytes(half_open_range.begin_key);
  const auto end = detail::as_fdb_bytes(half_open_range.end_key);

  return detail::future_value {
   fdb_transaction_get_estimated_range_size_bytes(
    raw_handle(),
    begin.data, begin.length,
    end.data, end.length)
  };
 }

 detail::future_value get_key(const key_selector& selector,
                              const read_mode mode)
 {
  const auto key = detail::as_fdb_bytes(selector.key);
  const fdb_bool_t snapshot = read_mode::snapshot == mode;

  return detail::future_value {
   fdb_transaction_get_key(raw_handle(),
                           key.data, key.length,
                           selector.or_equal, selector.offset,
                           snapshot)
  };
 }

 bool key_exists(detail::byte_view key, const read_mode mode)
 {
  return get_single_value_from_transaction(key, [](auto) {}, mode);
 }

 [[nodiscard]] std::int64_t committed_version() const
 {
  require_committed("committed_version()");

  std::int64_t version = 0;

  if (const auto ec =
       fdb_transaction_get_committed_version(raw_handle(), &version);
      0 != ec) {
   throw libfdb_exception(ec);
  }

  if (0 > version) {
   throw std::invalid_argument("committed_version() requires committed transaction");
  }

  return version;
 }

 [[nodiscard]] std::int64_t read_version() const
 {
  return detail::extract_int64(
           detail::block_until_ready(
            detail::future_value(fdb_transaction_get_read_version(raw_handle()))));
 }

 [[nodiscard]] std::int64_t approximate_commit_bytes() const
 {
  return detail::extract_int64(
           detail::block_until_ready(
            detail::future_value(fdb_transaction_get_approximate_size(raw_handle()))));
 }

 void set_read_version(const std::int64_t version)
 {
  fdb_transaction_set_read_version(raw_handle(), version);
 }

 bool commit();
 bool commit(fdb_error_t *replay_error);
 void destroy() noexcept
 {
  txn_handle.reset();
 }

 // Friends implement the public free-function interface while keeping the
 // transaction handle opaque:
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
                        read_mode,
                        const commit_after_op);

 template <typename OutputTargetOrFnT>
 requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
          concepts::decoded_value_sink<OutputTargetOrFnT&&>
 friend inline bool get(ceph::libfdb::transaction_handle,
                        const concepts::libfdb_key auto&,
                        OutputTargetOrFnT&&,
                        const commit_after_op);

 friend inline bool key_exists(transaction_handle txn,
                               const concepts::libfdb_key auto& k,
                               read_mode mode,
                               const commit_after_op commit_after);

 friend inline bool commit(transaction_handle& txn);
 friend inline commit_result commit(with_result_t, transaction_handle& txn);
 friend inline bool commit(transaction_handle& txn, const versionstamp& stamp);
 friend inline std::int64_t committed_version(const transaction_handle& txn);
 friend inline std::int64_t read_version(const transaction_handle& txn);
 friend inline std::int64_t approximate_commit_bytes(const transaction_handle& txn);
 friend inline bool ceph::libfdb::detail::reset_for_replay_if_needed(transaction_handle&, fdb_error_t);
 friend inline void set_read_version(const transaction_handle& txn, std::int64_t version);
 friend inline watch_handle make_watch(transaction_handle txn, std::string_view key);
 friend inline void ceph::libfdb::detail::transaction_set_kv_bytes(const transaction_handle&,
                                                                   detail::byte_view,
                                                                   detail::byte_view);
 friend inline void ceph::libfdb::detail::transaction_atomic_op(const transaction_handle&,
                                                                detail::byte_view,
                                                                detail::byte_view,
                                                                FDBMutationType);
 friend inline void ceph::libfdb::detail::transaction_clear_key_bytes(const transaction_handle&,
                                                                      detail::byte_view);
 friend inline void ceph::libfdb::detail::transaction_clear_range(const transaction_handle&,
                                                                  const ceph::libfdb::select&);
 friend inline void ceph::libfdb::detail::transaction_mark_conflict_range(const transaction_handle&,
                                                                          const ceph::libfdb::select&,
                                                                          FDBConflictRangeType);
 friend inline auto ceph::libfdb::detail::transaction_get_range_split_points(
  const transaction_handle&, std::span<const std::uint8_t>, std::span<const std::uint8_t>, std::int64_t)
  -> ceph::libfdb::detail::future_value;
 friend inline auto ceph::libfdb::detail::transaction_get_estimated_range_size(
  const transaction_handle&, const ceph::libfdb::select&)
  -> ceph::libfdb::detail::future_value;
 friend inline auto ceph::libfdb::detail::transaction_get_key(
  const transaction_handle&, const key_selector&, read_mode)
  -> ceph::libfdb::detail::future_value;

};

namespace detail {

// Since lambdas cannot be friend-functions, we use a named helper:
inline void transaction_set_kv_bytes(const transaction_handle& txn,
                                     byte_view key,
                                     byte_view value)
{
 txn->set(key, value);
}

inline void transaction_atomic_op(const transaction_handle& txn,
                                  byte_view key,
                                  byte_view param,
                                  const FDBMutationType type)
{
 txn->atomic_op(key, param, type);
}

inline void transaction_clear_key_bytes(const transaction_handle& txn,
                                        byte_view key)
{
 txn->erase(key);
}

inline void transaction_clear_range(const transaction_handle& txn,
                                    const ceph::libfdb::select& key_range)
{
 txn->erase(key_range);
}

inline void transaction_mark_conflict_range(const transaction_handle& txn,
                                            const ceph::libfdb::select& key_range,
                                            const FDBConflictRangeType type)
{
 txn->mark_conflict(key_range, type);
}

inline future_value transaction_get_range_split_points(const transaction_handle& txn,
                                                       std::span<const std::uint8_t> begin,
                                                       std::span<const std::uint8_t> end,
                                                       const std::int64_t target_bytes)
{
 return txn->get_range_split_points(begin, end, target_bytes);
}

inline future_value transaction_get_estimated_range_size(const transaction_handle& txn,
                                                         const ceph::libfdb::select& selection)
{
 return txn->get_estimated_range_size(selection);
}

inline future_value transaction_get_key(const transaction_handle& txn,
                                        const key_selector& selector,
                                        const read_mode mode)
{
 return txn->get_key(selector, mode);
}

[[nodiscard]] inline bool reset_for_replay_if_needed(
  transaction_handle& txn,
  const fdb_error_t error)
{
 if (0 == error) {
  return false;
 }

 txn->reset_for_replay(error);

 return true;
}

} // namespace detail

inline bool ceph::libfdb::transaction::get_single_value_from_transaction(
  const detail::byte_view key,
  concepts::value_callback auto&& write_output,
  const read_mode mode)
{
 const fdb_bool_t is_snapshot = read_mode::snapshot == mode;
 const auto key_bytes = detail::as_fdb_bytes(key);

 auto fv = detail::block_until_ready(detail::future_value(
  fdb_transaction_get(raw_handle(),
                      key_bytes.data,
                      key_bytes.length,
                      is_snapshot)));
 auto *future = fv.raw_ptr_or_throw();

 fdb_bool_t key_was_found = false;
 const std::uint8_t *out_buffer = nullptr;
 int out_len = 0;

 if (const auto ec = fdb_future_get_value(
       future, &key_was_found, &out_buffer, &out_len);
     0 != ec) {
  throw libfdb_exception(ec);
 }

 if (0 == key_was_found) {
  return false;
 }

 std::invoke(write_output, detail::result_bytes(out_buffer, out_len));

 return true;
}

inline void ceph::libfdb::transaction::recover_from_commit_error(
  detail::future_value& commit_result_future,
  const fdb_error_t error,
  fdb_error_t *replay_error)
{
 detail::future_value on_error_future =
  detail::wait_for_on_error(raw_handle(), error);

 if (0 != detail::get_future_error(on_error_future)) {
  // These cleanup operations are one ordered action:
  commit_result_future.destroy(), on_error_future.destroy(), destroy();

  throw libfdb_exception(error);
 }

 if (nullptr != replay_error) {
  *replay_error = error;
 }

 version_stamps.clear();
}

inline void ceph::libfdb::transaction::resolve_versionstamps(
  std::optional<detail::future_value>& versionstamp_future)
{
 if (not versionstamp_future) {
  return;
 }

 auto ready = detail::block_until_ready(std::move(*versionstamp_future));

 const std::uint8_t *data = nullptr;
 int size = 0;

 if (const auto ec = fdb_future_get_key(ready.raw_handle(), &data, &size);
     0 != ec) {
  throw libfdb_exception(ec);
 }

 constexpr auto expected_size =
  std::tuple_size_v<versionstamp::versionstamp_data_t>;

 if (nullptr == data) {
  throw libfdb_exception("invalid version stamp result");
 }

 const auto result = detail::result_bytes(data, size);

 if (expected_size != std::size(result)) {
  throw libfdb_exception("invalid version stamp result");
 }

 for (auto& stamp : version_stamps) {
  stamp.store_result(result);
 }

 version_stamps.clear();
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

 require_active("commit()");

 std::optional<detail::future_value> versionstamp_future;

 if (not version_stamps.empty()) {
  versionstamp_future.emplace(fdb_transaction_get_versionstamp(raw_handle()));
 }

 detail::future_value commit_result_future(
  fdb_transaction_commit(raw_handle()));
 auto *commit_future = commit_result_future.raw_ptr_or_throw();

 if (const auto ec = fdb_future_block_until_ready(commit_future); 0 != ec) {
  throw libfdb_exception(ec);
 }

 if (const auto ec = fdb_future_get_error(commit_future); 0 != ec) {
  recover_from_commit_error(commit_result_future, ec, replay_error);

  return false;
 }

 state = state_t::committed;
 resolve_versionstamps(versionstamp_future);

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

inline future_value wait_for_on_error(FDBTransaction *txn,
                                      const fdb_error_t original_error)
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

} // namespace ceph::libfdb::detail

#endif
