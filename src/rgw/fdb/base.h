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

#ifndef CEPH_FDB_BASE_H
 #define CEPH_FDB_BASE_H

#include <print> // JFW: removeme

// The API version we're writing against, which can (and probably does) differ
// from the installed version. This must be defined before the FoundationDB header
// is included (see fdb_c_apiversion.g.h):
#define FDB_API_VERSION 730 
#include <foundationdb/fdb_c.h> 

// Ceph uses libfmt rather than <format>:
#include <fmt/format.h>

#include <map> 
#include <tuple>
#include <mutex>
#include <memory>
#include <ranges>
#include <thread>
#include <vector>
#include <cstdint>
#include <variant>
#include <iterator>
#include <concepts>
#include <algorithm>
#include <generator>
#include <exception>
#include <functional>
#include <filesystem>
#include <type_traits>

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
template<class Container, class Reference>
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

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb::concepts {

// There's a high likelihood that we're going to get more sophisticated selectors, 
// so this is doing a more important job than it may appear to be:
template <typename T>
concept selector = ceph::libfdb::detail::is_any_of<T, ceph::libfdb::select>;

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb {

struct database;
struct transaction;
struct future_value;

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
  return std::format("libfdb: {}", msg);
 }

 static std::string make_fdb_error_string(const fdb_error_t ec)
 {
  return make_error_string(std::format("FoundationDB error {}: {}", ec, fdb_get_error(ec)));
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

static constexpr std::string_view range_min(""), range_max("\xFF");

struct select final
{
 const std::string begin_key, end_key;

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
 // Sorry this is tied to the encoding (zpp_bits), but it "just is" for now...
 return std::span<const std::uint8_t>((const std::uint8_t *)s, std::strlen(s));
}

constexpr auto as_fdb_span(std::string_view sv) 
{ 
 return std::span<const std::uint8_t>((const std::uint8_t *)sv.data(), sv.size()); 
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

std::pair<std::string, std::string> to_decoded_kv_pair(const FDBKeyValue kv);

struct maybe_commit;

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

enum struct option_type { flag, integer, string, data };

using option_value = std::variant<bool, std::int64_t, std::string, std::vector<std::uint8_t>>;

// i.e. option /code/ to the value of the option itself (e.g. FDB_FOO, 42):
template <typename OptionCodeT>
using option_map = std::map<OptionCodeT, option_value>; 

using network_options = option_map<FDBNetworkOption>;
using database_options = option_map<FDBDatabaseOption>;
using transaction_options = option_map<FDBTransactionOption>;

namespace detail {

template<typename ...XS>
struct overload : XS... 
{ using XS::operator()...; };

template<class... Ts>
overload(Ts...) -> overload<Ts...>;

// JFW: TODO: Concept-ify these:
// Note that these are specific to FDB's needs (see return type, casts):
constexpr const std::uint8_t *data_of(const std::vector<std::uint8_t>& xs) { return (std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::string& xs) { return (std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::int64_t& x) { return reinterpret_cast<const std::uint8_t *>(&x); }
constexpr const std::uint8_t *data_of(const bool& x) { return reinterpret_cast<const std::uint8_t *>(&x); }

// JFW: TODO: Concept-ify these variant types:
// Note that these are specific to FDB's needs (see return type, casts):
constexpr const int size_of(const std::vector<std::uint8_t>& xs) { return static_cast<int>(xs.size()); }
constexpr const int size_of(const std::string& xs) { return static_cast<int>(xs.size()); }
constexpr const int size_of(const std::int64_t x) { return 8; } // JFW: static_cast<int>(x); }
constexpr const int size_of(const bool x) { return static_cast<int>(x); }

// ...also used:
constexpr const std::uint8_t *data_of(std::string_view xs) { return (std::uint8_t *)xs.data(); }
constexpr const int size_of(std::string_view xs) { return static_cast<int>(xs.size()); }

// JFW: it would be nice to unify these two nearly-identical implementations:
// JFW: some FDB option-setting functions do not operate on handles:
inline void apply_options(const auto& option_map, auto& set_option_fn)
{
 std::ranges::for_each(option_map, [&set_option_fn](const auto& option) {
    // JFW: tuple-fy this:
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

} // detail

} // namespace ceph::libfdb

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

struct database;
struct transaction;

class database final
{
 detail::database_system db_system;

 private:

//JFW: prolly should be shared
 std::unique_ptr<FDBDatabase, decltype(&fdb_database_destroy)> db_handle;

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
 operator bool() const noexcept { return nullptr != raw_handle(); }

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

 commit_after_op autocommit_ = commit_after_op::no_commit;

 std::unique_ptr<FDBTransaction, decltype(&fdb_transaction_destroy)> txn_handle;

 private:
 inline bool get_single_value_from_transaction(const std::span<const std::uint8_t>&, std::invocable<std::span<const std::uint8_t>> auto&&);
 inline bool get_value_range_from_transaction(std::span<const std::uint8_t>, std::span<const std::uint8_t>, auto); 
 inline future_value get_range_future_from_transaction(std::span<const std::uint8_t>, std::span<const std::uint8_t>, const FDBStreamingMode, int&);

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

 transaction(database_handle dbh_, const transaction_options& opts, const commit_after_op do_commit)
  : transaction(dbh_, opts)
 {
  autocommit(do_commit); 
 }

 ~transaction()
 {
  if(commit_after_op::commit == autocommit() && !std::uncaught_exceptions()) {
    commit();
  }
 }

 public:
 // I vacillate between considering this ok, or not even a good idea...
 operator bool() { return dbh && nullptr != raw_handle(); }

 commit_after_op autocommit() const noexcept { return autocommit_; }
 void autocommit(const commit_after_op do_commit) noexcept { autocommit_ = do_commit; }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_handle.get(); }

 private:
 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

 inline auto get_results_pair_generator(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key)
 -> std::generator< std::pair<std::string, std::string> >;

/*JFW: auto selection_generator(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key) ->
  std::generator< std::map<std::string, std::string> >; */
 auto selection_generator(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key) ->
  std::generator< std::pair<std::string, std::string> >;

 // The output requirement to std::string is a bit artificial, and should be revisited:
 // Satisfying std::output_iterator<std::string, ??> appears to be trickier than it looks-- so don't be
 // misled by my poor specification here, please; I will fix this!!:
 bool get(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key, auto out_iter) {
    return ceph::libfdb::transaction::get_value_range_from_transaction(begin_key, end_key, out_iter);
 }
 
 bool get(std::span<const std::uint8_t> k, std::invocable<std::span<const std::uint8_t>> auto& val_collector) {
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

 private:
 friend inline void set(transaction_handle, const char*, const char*, commit_after_op);
 friend inline void set(std::span<const unsigned char>, std::span<const unsigned char>);
 friend inline void set(std::span<const std::uint8_t>, std::span<const std::uint8_t>);
 friend inline void set(transaction_handle, const std::string_view, const auto&, const commit_after_op);
 friend inline void set(transaction_handle, std::input_iterator auto, std::input_iterator auto, const commit_after_op);

 // Clearly, this could use some work-- the trick is disambiguating the iterators, do-able but it will take a little work:
 friend inline void set(transaction_handle txn, std::map<std::string, std::string>::const_iterator b, std::map<std::string, std::string>::const_iterator e, const commit_after_op commit_after);

 friend inline auto make_generator(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range);

 friend inline bool get(ceph::libfdb::transaction_handle, std::string_view, auto&, const commit_after_op);
 friend inline bool get(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, auto, const commit_after_op);

 friend inline void erase(ceph::libfdb::transaction_handle, std::string_view, const commit_after_op);
 friend inline void erase(ceph::libfdb::transaction_handle, const ceph::libfdb::select&, const commit_after_op);

 friend inline bool key_exists(transaction_handle txn, std::string_view k, const commit_after_op commit_after);

 // JFW: std::remove_cvref():
 friend inline bool commit(transaction_handle& txn);
 friend inline bool commit(transaction_handle txn);

 // private:
 friend struct ceph::libfdb::detail::maybe_commit;
};

} // namespace ceph::libfdb

namespace ceph::libfdb {

inline void shutdown_libfdb()
{
 // Shutdown the FDB thread:
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

// Begins a KV transaction with a selector (we can add more options via selectors):
inline ceph::libfdb::future_value begin_fdb_range_get(ceph::libfdb::transaction& txn, const std::span<const std::uint8_t> begin_key, const std::span<const std::uint8_t> end_key, int& iteration)
{
/* ...Note the way that here the initial keys are used to begin the search, but to do the /continued/ search in the 
 * possible next get_range_future_from_transaction() call, we need to use the final key retrieved as a new starting 
 * point; the documentation does not explain this very clearly: */
   return fdb_transaction_get_range(txn.raw_handle(),
            begin_key.data(),
            begin_key.size(),
            1,                                 // begin-or-equal
            0,                                 // begin offset (result offset)
            end_key.data(),
            end_key.size(),
            1,                                 // end-or-equal
            0,                                 // end offset
  
            0,                                 // limit
            0,                                 // target bytes
 
            FDB_STREAMING_MODE_WANT_ALL,       // streaming mode
            ++iteration,                       // iteration (starts at 1, incremented each successive call)
            0,                                 // snapshot
            0                                  // reverse
          );
}

inline ceph::libfdb::future_value end_fdb_range_get(ceph::libfdb::transaction& txn, const std::span<const uint8_t> begin_key, std::span< const uint8_t> end_key, int& iteration)
{
 return fdb_transaction_get_range(txn.raw_handle(),
          begin_key.data(),
          begin_key.size(),
          1,                                 // begin-or-equal
          1,                                 // begin offset (result offset)
          end_key.data(), 
          end_key.size(),
          1,                                 // end-or-equal
          0,                                 // end offset
          
          0,                                 // limit
          0,                                 // target bytes

          FDB_STREAMING_MODE_WANT_ALL,       // streaming mode
          ++iteration,                       // iteration (starts at 1, incremented each successive call)
          0,                                 // snapshot
          0                                  // reverse
        );
}

inline const FDBKeyValue *obtain_kvs(ceph::libfdb::transaction& txn, ceph::libfdb::future_value& fv, int& out_count, int& out_more)
{
 const FDBKeyValue *out_kvs = nullptr;

 if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &out_more); 0 != r) {

  // JFW: It is difficult to understand what the expected error checking and retry behavior is 
  // during transactions, I *think* I have it right:
  auto fv2 = future_value(fdb_transaction_on_error(txn.raw_handle(), r));

  if(fdb_error_t r2 = fdb_future_block_until_ready(fv2.raw_handle()); 0 != r2) {
    // Notice that we throw the "original" failure-- very difficult to understand from the docs what is expected:
    throw libfdb_exception(r);
  }

  std::swap(fv, fv2); // JFW

  return nullptr;
 }

 return out_kvs;
}

} // namespace ceph::libfdb

/* JFW: There are some possible FDB bugs around size limits (see C++-wrapped C binding unit_test.cpp); it may be necessary
 * to fiddle with FDB_TR_OPTION_SIZE_LIMIT/FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT for values over 10M: 
*/
// [FDBKV] . head -> pair<string>, string>
inline auto ceph::libfdb::transaction::selection_generator(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key) ->
 std::generator< std::pair<std::string, std::string> > 
{
 std::map<std::string, std::string> results;

 int out_count = 0;		// updated by FDB's read
 fdb_bool_t out_more = true;	// true if there's more to read
 int iteration = 0;

 libfdb::future_value fv = begin_fdb_range_get(*this, 
                                               begin_key, 
                                               end_key, 
                                               iteration);
   
 while(out_more) {

   if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
      throw libfdb_exception(r);
   }

   const FDBKeyValue *out_kvs = obtain_kvs(*this, fv, out_count, out_more); 
   if(nullptr == out_kvs) {
       // ...on retry, causes side-effect overwriting fv with a new value:
       continue; // JFW: need to research if this is the correct retry behavior
   }

   while(out_count--) {
    co_yield detail::to_decoded_kv_pair(*out_kvs++);
   }

   if(!out_more) {
    break;
   }

   // This part is a little weird, but basically if there's more to go we need to read again /inside the loop/; I do not know
   // why it's not a bit more orthogonal, but it isn't. It may be slightly more clear written as a for-loop, but even then it
   // honestly is just a bit odd. In any case...
   const auto& prev_key = out_kvs[out_count - 1];

   fv = end_fdb_range_get(*this, 
                          std::span<const std::uint8_t>((const uint8_t *)prev_key.key, prev_key.key_length),
                          end_key, 
                          iteration);
  }
}

inline auto ceph::libfdb::transaction::get_value_range_from_transaction(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key, auto out_iter)
 -> bool
{
 std::ranges::copy(selection_generator(begin_key, end_key), out_iter);

 return true;
}

inline bool ceph::libfdb::transaction::get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output)
{
 fdb_bool_t key_was_found = false;
 fdb_bool_t is_snapshot = false; 

 const uint8_t *out_buffer = nullptr;
 int out_len = 0;

 ceph::libfdb::future_value fv = fdb_transaction_get(raw_handle(), (const uint8_t *)key.data(), key.size(), is_snapshot);

 // AWAIT the FUTURE:
 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
        // This is an "exceptional error"-- OOM, etc.-- no point retrying:
        throw libfdb_exception(r);
 }

 if(fdb_error_t r = fdb_future_get_value(fv.raw_handle(), &key_was_found, &out_buffer, &out_len); 0 != r) {

    // JFW: I'm absolutely unclear as to what needs or doesn't need to run in an actual loop here-- or when or
    // when not to call fdb_transaction_on_error(), but this at least seems to be stable; similarly, the 
    // documentation's mention that numeric codes be used create an utter headscratcher as to
    // why in that case macros would be provided at all; but that's par for the course with some aspects
    // of the FDB C API docs and examples-- it's probably all clear if you are pointed in the right direction:
    if(not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, r)) { 
      // errors such as 1025 mean there's no point in retrying:
      throw libfdb_exception(r); 
    } 

    auto fv2 = future_value(fdb_transaction_on_error(raw_handle(), r));

    if(fdb_error_t r2 = fdb_future_block_until_ready(fv2.raw_handle()); 0 != r2) {
      throw libfdb_exception(r2);
    }
 }

 // No errors, but no value was found:
 if(0 == key_was_found) {
   return false;
 }

 write_output(std::span<const std::uint8_t>(out_buffer, out_len));

 return true;
}

#endif
