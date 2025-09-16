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

#include <map> // JFW: remove after lifting (and, indeed, remove to see what you need to lift!)

#include <tuple>
#include <mutex>
#include <memory>
#include <ranges>
#include <thread>
#include <iterator>
#include <concepts>
#include <algorithm>
#include <exception>
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
constexpr bool appendable_container  = requires(Container& c, Reference&& ref)
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

// May be good to revisit my earlier idea of doing this with references/string_view:
struct select final
{
 std::string begin_key, end_key;

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

 static inline void initialize_fdb()
 {
  // This must be called before ANY other API function:
  if(fdb_error_t r = fdb_select_api_version(FDB_API_VERSION); 0 != r)
   throw libfdb_exception(r);
 
  // Zero or more calls to this may now be made:
  // fdb_error_t fdb_network_set_option(FDBNetworkOption option, uint8_t const *value, int value_length)
 
  // This must be called before any other API function (besides >= 0 calls to fdb_network_set_option()):
  if(fdb_error_t r = fdb_setup_network(); 0 != r)
   throw libfdb_exception(r);
 
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

struct database;
struct transaction;

class database final
{
 private:
 FDBDatabase *fdb_handle = nullptr;

 public:
 database() 
 {
  std::call_once(ceph::libfdb::detail::database_system::fdb_was_initialized, ceph::libfdb::detail::database_system::initialize_fdb);

  if(fdb_error_t r = fdb_create_database(nullptr, &fdb_handle); 0 != r)
   throw libfdb_exception(r);

  // may now set database options:
  ; // JFW
 }

 ~database()
 {
//JFW: move to smartptr
  if(nullptr != fdb_handle) {
   fdb_database_destroy(fdb_handle), fdb_handle = nullptr;
  }
 }

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

 std::unique_ptr<FDBTransaction, decltype(&fdb_transaction_destroy)> txn_ptr;

 private:
 FDBTransaction *create_transaction() {
  FDBTransaction *txn_p = nullptr; // JFW: *or* should it be angherror to create over extant txn?
  if(fdb_error_t r = fdb_database_create_transaction(dbh->raw_handle(), &txn_p); 0 != r) {
   throw libfdb_exception(r);
  }

  return txn_p;
 }

/*JFW: rework into a coherent mechanism--
 void establish() {
  destroy(), txn_ptr.reset(create_transaction());
 }

 void maybe_vivify() {
  if(not this) {
    throw ceph::libfdb::libfdb_exception("inactive transaction");
  }
 if(not this) {
   establish();
  }
 }*/

 private:
 inline bool get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output);
 inline bool get_value_range_from_transaction(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key, auto out_iter); 
 inline future_value get_range_future_from_transaction(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key);

 public:
 transaction(database_handle& dbh)
 : dbh(dbh),
   txn_ptr(create_transaction(), &fdb_transaction_destroy)
 {}

 public:
 // I vacillate between considering this ok, or not even a good idea...
 operator bool() { return dbh && nullptr != raw_handle(); }

 public:
 FDBTransaction *raw_handle() const noexcept { return txn_ptr.get(); }

/*JFW:
 private:
 void set_option(FDBTransactionOption o, std::string_view v) {
    detail::check_fdb_result(
      fdb_transaction_set_option(raw_handle(), o, (const std::uint8_t *)v.data(), v.length()));
*/

 private:
 void set(std::span<const std::uint8_t> k, std::span<const std::uint8_t> v) {
 //   maybe_vivify();
    fdb_transaction_set(raw_handle(),
                        (const uint8_t*)k.data(), k.size(),
                        (const uint8_t*)v.data(), v.size());
 }

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
//    maybe_vivify();
    fdb_transaction_clear(raw_handle(),
			  (const std::uint8_t *)k.data(), k.size());
 }

 void erase(const ceph::libfdb::concepts::selector auto& key_range) {
 //   maybe_vivify();
    fdb_transaction_clear_range(raw_handle(),
        (const uint8_t *)key_range.begin_key.data(), key_range.begin_key.size(), 
        (const uint8_t *)key_range.end_key.data(), key_range.end_key.size());
 }

 bool key_exists(std::string_view k) {
    return get_single_value_from_transaction(detail::as_fdb_span(k), [](auto) {});
 }

 bool commit();
 void destroy() { txn_ptr.reset(); }

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


// JFW: needs lifting
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

} // namespace ceph::libfdb

// JFW: TODO: consolidate this with get_value_from_transaction()! See details there.
// JFW:		(it's a bit trickier than it looks at first-- I need to separate it into more components.)
inline bool ceph::libfdb::transaction::get_value_range_from_transaction(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key, auto out_iter)
{
 const fdb_bool_t is_snapshot = false;

 const FDBKeyValue *out_kvs = nullptr;
 int out_count = 0;		// updated by FDB's read
 fdb_bool_t out_more = false;	// true if there's more to read

 future_value fv = get_range_future_from_transaction(begin_key, end_key);

 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
   throw libfdb_exception(r);
 }

 if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &out_more); 0 != r) {

   auto fv2 = future_value(fdb_transaction_on_error(raw_handle(), r));

   if(fdb_error_t r2 = fdb_future_block_until_ready(fv2.raw_handle()); 0 != r2) {
     throw libfdb_exception(r);
   }

   return false;
 }

 std::transform(out_kvs, out_count + out_kvs, out_iter, detail::to_decoded_kv_pair);

 return true;
}

// JFW: TODO: consolidate all of these functions-- I believe the unifying element is going to be
// pretty simple, but I had to get my head around it first! Basically, taking a function is the
// way forward; additionally, the event loop may not be entirely correct, it's hard to tell from
// the FDB examples-- but I now think the high level API function actually handles all the retries
// and backoff in the correct fashion, we're basically doing too much work-- but for now I need
// things to "work"! :-)

// JFW: TODO: consolidate this with get_value_range_from_transaction()!
// JFW: integrate this back into base::transaction or ...
// JFW: we could also return an fdb_error_t, but that would introduce FDB artefacts into the public
// interace if we were to make it meaningful:
// JFW: This probably needs to go back into base.h;
// JFW: I think there's a simpler and also-"approved" way to do this-- I'll look at it later, getting rid
// of the strangeness and complexity here would be good:
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
inline ceph::libfdb::future_value ceph::libfdb::transaction::get_range_future_from_transaction(std::span<const std::uint8_t> begin_key, std::span<const std::uint8_t> end_key)
{
  // By default, give (begin, end]; not much in the C API documentation about selector details, the Python docs
  // have a bit more information:
  constexpr bool begin_or_eq = false;
  constexpr int begin_offset = 1;

  constexpr bool end_or_eq   = true;
  constexpr int end_offset = 1; 

  constexpr int limit = 0;		// 0 == unlim; else max number of pairs to return at once (more() function)
  constexpr int target_bytes = 0;	// 0 == unlim; else enables more() function of fdb_future_get_keyvalue_array()
  constexpr FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
  constexpr bool is_snapshot = false;
  constexpr bool reverse = false;

  int iterations = 1; // should start at one, is incremented when streaming_mode_t::iterator is enabled, else ignored

  return fdb_transaction_get_range(
		      raw_handle(),
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

#endif
