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

#include <map> 
#include <tuple>
#include <mutex>
#include <memory>
#include <ranges>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <variant>
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

namespace ceph::libfdb::detail {

template <typename T, typename ...Ts>
concept is_any_of = (std::is_same_v<T, Ts> || ...);

} // ceph::libfdb::detail

namespace ceph::libfdb {

struct select;

} // namespace ceph::libfdb

namespace ceph::libfdb::concepts {

// Capture things that model ContainerCompatibleRange. 
// Adapted from "https://en.cppreference.com/w/cpp/ranges/to.html#container_compatible_range":
template <class Container, class Reference>
constexpr bool appendable_container = requires(Container& c, Reference&& ref)
{
 requires
 (
  requires { c.emplace_back(std::forward<Reference>(ref)); }     ||
  requires { c.push_back(std::forward<Reference>(ref)); }        ||
  requires { c.emplace(c.end(), std::forward<Reference>(ref)); } ||
  requires { c.insert(c.end(), std::forward<Reference>(ref)); }
 );
};

// Note that "stringlikes" are not all "stringview-likes", for example when they can be
// written to:
template <typename StringViewLikeT>
concept stringview_convertible = std::convertible_to<StringViewLikeT, std::string_view>;

// There's a high likelihood that we're going to get more sophisticated selectors, 
// so this is doing a more important job than it may appear to be:
template <typename T>
concept selector = ceph::libfdb::detail::is_any_of<T, ceph::libfdb::select>;

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb {

class database;
class transaction;
class future_value;

using database_handle = std::shared_ptr<database>;
using transaction_handle = std::shared_ptr<transaction>;

// Should we commit after the (possibly) mutating operation?
enum struct commit_after_op { commit, no_commit };

struct libfdb_exception final : std::runtime_error
{
 using std::runtime_error::runtime_error;

 fdb_error_t fdb_error_value = -1;

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

struct future_value final
{
 std::unique_ptr<FDBFuture, decltype(&fdb_future_destroy)> future_ptr;

 public:
 future_value(FDBFuture *future_handle)
  : future_ptr(future_handle, &fdb_future_destroy)
 {}

 public:
 FDBFuture *raw_handle() const noexcept { return future_ptr.get(); }

 private:
 void destroy() { future_ptr.reset(nullptr); }
 
 private:
 friend class transaction;
};

struct select final
{
 std::string begin_key, end_key;

 public:
 // We'll eventually need a way to get settings into the base library from the binding layer; there are a few ways we
 // could do it, this is one I'm mulling over; do not use this right now.
 mutable struct {
  int stride = 0; // "unlimited"

  // Some parts of the documentation claim FDB_STREAMING_MODE_ITERATOR is the default, other parts don't... examples tend
  // to use FDB_STREAMING_MODE_WANT_ALL, but they operate on fairly small amounts of data. It's pretty hard to understand what the Right Thing(TM) to do is, the sure the answer may evolve as
  // I learn more, but for now this setting at least means it's "plumbed through" (this particularly mode starts with small batches and then grows to larger increments as more data is sent):
  FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_ITERATOR; 

 } options;

 public:
 select(std::string_view begin_key_, std::string_view end_key_)
  : begin_key(begin_key_), end_key(end_key_)
 {}

 private:
 friend class transaction;

 friend inline bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, auto, const commit_after_op);

 template <typename KT>
 friend inline void erase(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, const commit_after_op); 
};

} // ceph::libfdb

namespace ceph::libfdb::detail {

constexpr auto as_fdb_span(const char *s)
{
 return std::span<const std::uint8_t>((const std::uint8_t *)s, std::strlen(s));
}

constexpr auto as_fdb_span(std::string_view sv) 
{ 
 return std::span<const std::uint8_t>((const std::uint8_t *)sv.data(), sv.size()); 
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

enum struct option_type { flag, integer, string, data };

using option_value = std::variant<bool, std::int64_t, std::string, std::vector<std::uint8_t>>;

// i.e. option /code/ to the value of the option itself (e.g. FDB_FOO, 42):
template <typename OptionCodeT>
using option_map = flat_map<OptionCodeT, option_value>; 

using network_options = option_map<FDBNetworkOption>;
using database_options = option_map<FDBDatabaseOption>;
using transaction_options = option_map<FDBTransactionOption>;

namespace detail {

template<typename ...XS>
struct overload : XS... 
{ using XS::operator()...; };

template<class... Ts>
overload(Ts...) -> overload<Ts...>;

// Note that these are specific to FDB's needs (see return type, casts):
constexpr const std::uint8_t *data_of(const std::vector<std::uint8_t>& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::string& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::int64_t& x) { return reinterpret_cast<const std::uint8_t *>(&x); }
constexpr const std::uint8_t *data_of(const bool& x) { return reinterpret_cast<const std::uint8_t *>(&x); }

// Note that these are specific to FDB's needs (see return type, casts):
constexpr int size_of(const std::vector<std::uint8_t>& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::string& xs) { return static_cast<int>(xs.size()); }
constexpr int size_of(const std::int64_t) { return 8; } // 8b = size required by FDB
constexpr int size_of(const bool x) { return static_cast<int>(x); }

// ...also used:
constexpr std::uint8_t *data_of(std::string_view xs) { return (std::uint8_t *)xs.data(); }
constexpr int size_of(std::string_view xs) { return static_cast<int>(xs.size()); }

// JFW: it would be nice to unify these two nearly-identical implementations:
// JFW: some FDB option-setting functions do not operate on handles:
inline void apply_options(const auto& option_map, auto& set_option_fn)
{
 std::ranges::for_each(option_map, [&set_option_fn](const auto& option) {
    auto d = std::visit([](const auto& x) { return data_of(x); }, option.second);
    auto s = std::visit([](const auto& x) { return size_of(x); }, option.second);

    if(auto r = set_option_fn(option.first, d, s); 0 != r) {
      throw libfdb_exception(fmt::format("while setting option {}; {}", 
                             (int)option.first, libfdb_exception::make_fdb_error_string(r)));
    }
  });
}

// ...and some FDB option-setting functions require a handle:
void apply_options(auto handle, const auto& option_map, auto& set_option_fn)
{
 std::ranges::for_each(option_map, [&handle, &set_option_fn](const auto& option) {
    // JFW: tuple-fy this:
    auto d = std::visit([](const auto& x) { return data_of(x); }, option.second);
    auto s = std::visit([](const auto& x) { return size_of(x); }, option.second);

    if(auto r = set_option_fn(handle, option.first, d, s); 0 != r) {
      throw libfdb_exception(fmt::format("while setting option {}; {}", 
                             (int)option.first, libfdb_exception::make_fdb_error_string(r)));
    }
  });
}

} // namespace ceph::libfdb::detail

} // namespace ceph::libfdb

// Wrangle some forward delcarations:
namespace ceph::libfdb::detail {

/* JFW: When the dust settles a bit, this function could be respecified, probably along these lines: 
 * template <typename MappedType = std::pair, typename KeyT = std::string, typename ValueT = std::string> */
std::pair<std::string, std::string> to_decoded_kv_pair(const FDBKeyValue kv);

struct maybe_commit;

inline future_value await_ready_key_range_future(transaction_handle txn, const ceph::libfdb::select&, int); 
inline future_value await_ready_keyvalue_range_future(transaction_handle txn, const ceph::libfdb::select& key_range, int& iteration);

// A generator that produces successive spans for a range:
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(ceph::libfdb::transaction& txn, ceph::libfdb::select key_range);

// Stores a successively-generated of kv pair results to an iterator:
template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline bool get_value_range_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& key_range, OutIterT out_iter);

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

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
  if(fdb_error_t r = fdb_select_api_version(FDB_API_VERSION); 0 != r)
   throw libfdb_exception(r);
 
  // Zero or more calls to this may now be made:
  apply_options(options, fdb_network_set_option);
 
  // This must be called before any other API function (besides >= 0 calls to fdb_network_set_option()):
  if(fdb_error_t r = fdb_setup_network(); 0 != r)
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
     // In this case, we likely don't want to throw from our dtor, but we may
     // have something to log. As there's no higher-level hook for that, we
     // have nothing to do right now.
     // fmt::println("database::shutdown_fdb() error {}", r);
   }

  // This may not actually be needed, but it's a traditional courtesy:
  std::this_thread::sleep_for(7ms); 
 
  if(fdb_network_thread.joinable()) {
   fdb_network_thread.join();
  }
 }

 private:
 friend struct ceph::libfdb::database;
};
 
} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

class database;
class transaction;

class database final
{
 detail::database_system db_system;

 private:
 std::shared_ptr<FDBDatabase> db_handle; 

 FDBDatabase *create_database_ptr(const std::filesystem::path cluster_file_path) {
    FDBDatabase *fdbp = nullptr;

    if(fdb_error_t r = fdb_create_database(cluster_file_path.c_str(), &fdbp); 0 != r) {
      throw libfdb_exception(r);
    }
    
    return fdbp;
 }

 FDBTransaction *create_transaction() {
    FDBTransaction *txn_p = nullptr;
    
    if(fdb_error_t r = fdb_database_create_transaction(raw_handle(), &txn_p); 0 != r) {
     throw libfdb_exception(r);
    }
    
    return txn_p;
 }

 public:
 database(const std::filesystem::path cluster_file_path, const ceph::libfdb::database_options& db_opts, const network_options& network_opts)
  : db_system(network_opts),
    db_handle(create_database_ptr(cluster_file_path), &fdb_database_destroy)
 {
  detail::apply_options(raw_handle(), db_opts, fdb_database_set_option);
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

 private:
 friend inline database_handle create_database();
 friend inline database_handle create_database(const std::filesystem::path);
 friend inline database_handle create_database(const std::filesystem::path, const database_options&);
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
  detail::apply_options(raw_handle(), opts, fdb_transaction_set_option);
 }

 public:
 explicit operator bool() { return dbh && nullptr != raw_handle(); }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_handle.get(); }

 private:
 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 // JFW: it's not as easy to wedge an output_range into here as it appears, needs to be revisited:
 // I'm binding it to what's actually used in practice for now...
 template <typename OutIterT>
 requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
 bool get(const ceph::libfdb::select& key_range, OutIterT out_iter) {
    return ceph::libfdb::detail::get_value_range_from_transaction(*this, key_range, out_iter);
 }
 
 bool get(const std::span<const std::uint8_t> k, std::invocable<std::span<const std::uint8_t>> auto& val_collector) {
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

 bool commit();
 void destroy() { txn_handle.reset(); }

 private:
 friend transaction_handle make_transaction(database_handle dbh);

 // As you can see, friends are used extensively to implement the public interface; it would be nice to come up
 // with a strategy for making these "lists" a bit more managable, but for now it's what we have and it allows me ot
 // keep these handles mostly-opaque (this has proven to be a good idea as I've a few times shuffled internal details
 // with no disruption to the user interface surface):
 private:
 friend inline void set(transaction_handle, const char*, const char*, commit_after_op);
 friend inline void set(std::span<const unsigned char>, std::span<const unsigned char>);
 friend inline void set(std::span<const std::uint8_t>, std::span<const std::uint8_t>);
 friend inline void set(transaction_handle, const std::string_view, const auto&, const commit_after_op);
 friend inline void set(transaction_handle, std::input_iterator auto, std::input_iterator auto, const commit_after_op);
 friend inline void set(transaction_handle, std::string_view, const ceph::libfdb::concepts::stringview_convertible auto&, const commit_after_op);

 // Clearly, this could use some work-- the trick is disambiguating the iterators, do-able but it will take a little work:
 friend inline void set(transaction_handle txn, std::map<std::string, std::string>::const_iterator b, std::map<std::string, std::string>::const_iterator e, const commit_after_op commit_after);

 friend inline bool get(ceph::libfdb::transaction_handle, std::string_view, auto&, const commit_after_op);
 friend inline bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, auto, const commit_after_op);

 friend inline void erase(ceph::libfdb::transaction_handle, std::string_view, const commit_after_op);
 friend inline void erase(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, const commit_after_op);

 friend inline bool key_exists(transaction_handle txn, std::string_view k, const commit_after_op commit_after);

 // JFW: std::remove_cvref() may let us reduce some of these overloads:
 friend inline bool commit(transaction_handle& txn);
 friend inline bool commit(transaction_handle txn);

 friend std::generator<std::span<const FDBKeyValue>> ceph::libfdb::detail::generate_FDB_pairs(ceph::libfdb::transaction&, ceph::libfdb::select); 

 // Sadly, no std::function_reference<> yet: 
 friend ceph::libfdb::future_value await_future_of(auto fn);

 friend struct ceph::libfdb::detail::maybe_commit;
};

inline bool ceph::libfdb::transaction::get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output)
{
 fdb_bool_t key_was_found = false;
 const fdb_bool_t is_snapshot = false; 

 const uint8_t *out_buffer = nullptr;
 int out_len = 0;

 for(fdb_error_t r = 0;;) { 
     ceph::libfdb::future_value fv = fdb_transaction_get(raw_handle(), (const uint8_t *)key.data(), key.size(), is_snapshot);
    
     if(r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
        throw libfdb_exception(r);
     }
    
     r = fdb_future_get_value(fv.raw_handle(), &key_was_found, &out_buffer, &out_len);

     if(0 == r) {
        // No errors, but no value was found:
        if(0 == key_was_found) {
          return false;
        }
    
        write_output(std::span<const std::uint8_t>(out_buffer, out_len));
  
        // The happy path is the simple path:
        return true; 
     }

     // If we're here, then an error has occured. FoundationDB is fairly particular about it's error handling, and it
     // frankly I found it hard to understand from the documentation, but the idea is that the fdb_c library can do its
     // best to handle a whole class of tricky errors, and we can report any others to the application (such as a consistency
     // error, which FDB has tried very hard to avoid):
     if(not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, r)) { 
        // errors such as 1025 (transaction_cancelled) mean there's no point in retrying:
        throw libfdb_exception(r); 
     }

     // See if we can handle the error:
     auto error_future = future_value(fdb_transaction_on_error(raw_handle(), r));
    
     r = fdb_future_block_until_ready(error_future.raw_handle());

     if(0 == r) {
       r = fdb_future_get_error(error_future.raw_handle());
     }
   
     // Throw up our hands: 
     if(0 != r) {
        throw libfdb_exception(r);
     }

     // ...try, try again...
  }

 return false;
} 
  
} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

inline ceph::libfdb::future_value block_until_ready(ceph::libfdb::future_value&& fv)
{
 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
   throw libfdb_exception(r);
 }

 // Note that fdb_future_block_until_ready() does not by itself check for errors
 // with the value; so, we need to do this separately:
 fdb_error_t r = fdb_future_get_error(fv.raw_handle());

 if(r) {
  throw libfdb_exception(r);
 }

 return fv;
}

template <typename FnT, typename... XS>
requires std::invocable<FnT, XS...>
inline ceph::libfdb::future_value await_future_of(FnT&& fn, XS&& ...params)
{
 return block_until_ready(
          std::invoke(std::forward<FnT>(fn), std::forward<XS>(params)...));
} 

template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline bool get_value_range_from_transaction(transaction& txn, const select& key_range, OutIterT out_iter)
{
 auto flattened = detail::generate_FDB_pairs(txn, key_range) | std::views::join;
 std::ranges::transform(flattened, out_iter, to_decoded_kv_pair);

 return true;
}

/* Try to extract a value from a future using the supplied invokable. On error, indicate retry (after
applying the appopriate retry, etc.); false if value was extracted, true if we need to retry. Throws on
error. The invokable is responsible for collecting the values appropriately. 
  JFW: see if we can get a reference implementation of std::function_ref (C++26)
*/
inline bool value_or_retry(ceph::libfdb::transaction_handle& txn, ceph::libfdb::future_value& fv,
                        std::invocable<ceph::libfdb::future_value&> auto extract_fn) 
{
 if(fdb_error_t r = extract_fn(fv); 0 != r) {
    if(not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, r)) { 
        // errors such as 1025 mean there's no point in retrying:
        throw ceph::libfdb::libfdb_exception(r); 
     } 

    fv = ceph::libfdb::future_value(fdb_transaction_on_error(txn->raw_handle(), r));

    if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
      throw ceph::libfdb::libfdb_exception(r);
    }

    return true;
  }

 return false;
}

// Convert FDBKey array into something useful:
inline std::vector<ceph::libfdb::select> as_select_seq(const FDBKey* const xs, const int n)
{
 std::vector<ceph::libfdb::select> out;
 out.reserve(n / 2);

 // Gather the flattened list into *overlapping* libfdb::select pairs:
 for(const auto& dyad : std::span(xs, n) | std::views::slide(2)) {
    const auto& fst = std::ranges::begin(dyad)[0];
    const auto& snd = std::ranges::begin(dyad)[1];

    out.push_back({ 
      { (const char *)fst.key, static_cast<std::string::size_type>(fst.key_length) }, 
      { (const char *)snd.key, static_cast<std::string::size_type>(snd.key_length) }
    });
 }

 return out;
}
// Finding a clear example both in the samples and in the documentation is not very easy. The
// statelessness of FDB requests bleeds into here with basically no hand-holding, so it's fairly subtle and
// not easy to see what to do (I'm still not sure I have it right), but note for instance that the call
// parameters have to change for subsequent reads.
inline ceph::libfdb::future_value get_range_future_from_transaction(ceph::libfdb::transaction& txn, const ceph::libfdb::select& selection, int iteration)
{
  const auto& begin_key  = selection.begin_key;
  const auto& end_key    = selection.end_key;

  // Hook for getting settings into here through the selector:
  const auto& streaming_mode = selection.options.streaming_mode;

  // The documentation makes this stuff about as clear as mud... read VERY carefully
  // when you fiddle with these:
  int begin_or_eq = (1 == iteration) ? 0 : 1;
  const int begin_offset = 1;
  const int end_or_eq = 0;
  const int end_offset = 1;

  // See validate_and_update_parameters() in fdb_c.cpp (FDB source) if greater clarity is needed on the meaning of some of these, it can be
  // hard to deduce from the documentation:
  // Returns an FDBKeyValueArray in the future:
  return fdb_transaction_get_range(txn.raw_handle(),
                      (const uint8_t *)begin_key.data(), begin_key.size(),      // the reference-point key
                      begin_or_eq,                                              // begin or eq
                      begin_offset,                                             // begin offset

                      (const uint8_t *)end_key.data(), end_key.size(),          // the end selector key
                      end_or_eq,                                                // end or eq
                      end_offset,                                               // end offset (a shift AFTER end is matched)

                      // How should results be grouped/chunked:
                      0,                                                        // limit (0 == unlimited)
                      0,                                                        // target bytes (0 == unlimited)
                      streaming_mode,                                           // streaming mode (e.g.: FDB_STREAMING_MODE_WANT_ALL)
                      iteration,                                                // iteration # (produced side effect)

                      // Other options:
                      0,                                                        // 0 unless this IS a snapshot read
                      0                                                         // reverse: should items come in reverse order?
                    );
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {
// Occasionally, implementations need a little glimpse of the Future (if you'll pardon the pun)-- kidding aside, these
// are forward delcarations that can probably go away with some header file reorganization after the dust clears:
extern transaction_handle make_transaction(database_handle dbh);
extern transaction_handle make_transaction(database_handle dbh, const transaction_options& opts);
} // namespace ceph::libfdb 

namespace ceph::libfdb::detail {

inline std::vector<ceph::libfdb::select> locate_split_points(ceph::libfdb::database_handle dbh, ceph::libfdb::select selector, const std::int64_t remote_chunk_size)
{
 using ceph::libfdb::detail::await_future_of;

 auto txn = ceph::libfdb::make_transaction(dbh);

 // We can do this without side-effects by combining some lambdas, but for now I'm satisfied if it works:
 FDBKey const *keys = nullptr;
 int nkeys = 0;

 for(bool should_retry = true; should_retry;) {

    auto fv = await_future_of(fdb_transaction_get_range_split_points,
                              txn->raw_handle(),
                              (const std::uint8_t *)selector.begin_key.data(), static_cast<int>(selector.begin_key.length()),
                              (const std::uint8_t *)selector.end_key.data(), static_cast<int>(selector.end_key.length()),
                              remote_chunk_size); 

    should_retry = value_or_retry(txn, fv, 
                    [&keys, &nkeys](ceph::libfdb::future_value& fv) {
                      return fdb_future_get_key_array(fv.raw_handle(), &keys, &nkeys);
                   });
 }

 return as_select_seq(keys, nkeys);
}

// Generators:
// The returned memory should be copied immediately as its lifetime will end once the Future is destoyed:
// (This is pretty much why this is in the "detail" namespace for the moment.)
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(transaction& txn, select key_range) 
{
  // As FDB is stateless, we need to track state somewhere:
  int more_available = 1;
  int out_count = 0;
  int iteration = 1; // expected to be 1 when FDB is called in iterator mode
 
  const FDBKeyValue *out_kvs = nullptr;

  for(; more_available; iteration++) {

    auto fv = await_future_of([&]() { return get_range_future_from_transaction(txn, key_range, iteration); });

    if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &more_available); 0 != r) {
       throw libfdb_exception(r);
    }

    auto result = std::span<const FDBKeyValue>(out_kvs, out_count);

    if(more_available) {
      // Make the first part of the range for the new search equal to the last one from the old search:
      const auto& last_key = result.back();
      key_range.begin_key = std::string_view((const char *)last_key.key, last_key.key_length);
     }

    co_yield result;
  }
}

} // namespace ceph::libfdb::detail


#endif
