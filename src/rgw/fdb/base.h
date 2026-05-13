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

class select;

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

struct select final
{
 std::string begin_key, end_key;

 public:
 // JFW: this is a little half-baked, but I want at least some way of getting
 // at these things; I feel like this isn't quite great, but I suppose it's also
 // not a terrible way to do it; exposing it ALL to the public interface is definitely
 // not something I want to do right now, but we're going with it for now...:
 // (Note: I probably WON'T keep this here, I don't like the idea of side-effects
 // being used for this. So do NOT depend on the details here...
 mutable struct {
  int stride = 0; // "unlimited"

  // JFW: Some parts of the documentation claim FDB_STREAMING_MODE_ITERATOR is the default, other parts don't... examples tend
  // to use FDB_STREAMING_MODE_WANT_ALL. It's pretty hard to understand what the Right Thing(TM) to do is, the sure the answer may evolve as
  // I learn more, but for now it just kinda needs to work...
  FDBStreamingMode streaming_mode = FDB_STREAMING_MODE_WANT_ALL; 
/*JFW: I don't think this is right place to keep /state/... now, the Generators themselves contain it, which is probably what I want
  int iteration = 1; // JFW: this is a bit misguided
  int total_out = 0; // JFWthis is a little
*/
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
constexpr const std::uint8_t *data_of(const std::vector<std::uint8_t>& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::string& xs) { return (const std::uint8_t *)xs.data(); }
constexpr const std::uint8_t *data_of(const std::int64_t& x) { return reinterpret_cast<const std::uint8_t *>(&x); }
constexpr const std::uint8_t *data_of(const bool& x) { return reinterpret_cast<const std::uint8_t *>(&x); }

// JFW: TODO: Concept-ify these variant types:
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

} // namespace ceph::libfdb::detail

} // namespace ceph::libfdb

// Wrangle some forward delcarations:
namespace ceph::libfdb::detail {

/* JFW: When the dust settles a bit, this function should almost certainly be respecified, probably
 * along these lines: template <typename MappedType = std::pair, typename KeyT = std::string, typename ValueT = std::string> */
std::pair<std::string, std::string> to_decoded_kv_pair(const FDBKeyValue kv);

struct maybe_commit;

future_value await_ready_range_future(transaction_handle txn, const ceph::libfdb::select& key_range, int& iteration);

std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(ceph::libfdb::transaction& txn_owner, ceph::libfdb::select key_range);

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

class database;
class transaction;

class database final
{
 detail::database_system db_system;

 private:
 std::shared_ptr<FDBDatabase> db_handle; // JFW: , decltype(&fdb_database_destroy)> db_handle;

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

 std::unique_ptr<FDBTransaction, decltype(&fdb_transaction_destroy)> txn_handle;

 private:
 inline bool get_single_value_from_transaction(const std::span<const std::uint8_t>& key, std::invocable<std::span<const std::uint8_t>> auto&& write_output);

 template <typename OutIterT>
 requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
 inline bool get_value_range_from_transaction(const ceph::libfdb::select& key_range, OutIterT out_iter);

 inline future_value get_range_future_from_transaction(const ceph::libfdb::select& key_range, int& iteration);

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
 // I vacillate between considering this ok, or not even a good idea...
 operator bool() { return dbh && nullptr != raw_handle(); }

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
    return ceph::libfdb::transaction::get_value_range_from_transaction(key_range, out_iter);
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

 // JFW: the signature kinda suggests this might as well be a static member function-- TODO:
 friend std::generator<std::span<const FDBKeyValue>> ceph::libfdb::detail::generate_FDB_pairs(ceph::libfdb::transaction& txn_owner, ceph::libfdb::select key_range);

 // private:
 friend struct ceph::libfdb::detail::maybe_commit;

 // implementation details need friends too:
 friend future_value ceph::libfdb::detail::await_ready_range_future(transaction_handle txn, const ceph::libfdb::select& key_range, int& iteration);
};

} // namespace ceph::libfdb

namespace ceph::libfdb {

inline void shutdown_libfdb()
{
 // Shutdown the FDB thread:
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

// JFW: TODO: consolidate this with get_value_from_transaction()! See details there.
// JFW:         (it's a bit trickier than it looks at first-- I need to separate it into more components.)
// JFW: now that there are generators, this is probably more straightforward-- but I still need to find the
// time to do it! :-)
template <typename OutIterT>
requires std::output_iterator<OutIterT, std::pair<std::string, std::string>>
inline bool ceph::libfdb::transaction::get_value_range_from_transaction(const ceph::libfdb::select& key_range, OutIterT out_iter)
{
 auto flattened = detail::generate_FDB_pairs(*this, key_range) | std::views::join;
 std::ranges::transform(flattened, out_iter, detail::to_decoded_kv_pair);

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

// JFW: TODO: implement/expose remaining features (key selectors, batching and chunking, etc.):
// JFW: there are a lot of performance implications, etc. in here, it needs to be explored-- for instance,
// iterator mode might be a better approach for some DB operations, or stride limits; let's avoid 
// exposing that stuff higher up in the library for now:
// Additionally, finding a clear example both in the samples and in the documentation is not very easy. The
// statelessness of FDB requests bleeds into here with basically no hand-holding, so it's fairly subtle and
// not easy to see what to do (I'm still not sure I have it right), but note for instance that the call
// parameters have to change for subsequent reads.
inline ceph::libfdb::future_value ceph::libfdb::transaction::get_range_future_from_transaction(const ceph::libfdb::select& selection, int& iteration /* JFW: make const */)
{
// JFW: doubting selection is the right place for *state*, though; this needs much thought:
  const auto& begin_key  = selection.begin_key;
  const auto& end_key    = selection.end_key;

fmt::println("JFW: get_range_future_from_transaction(): begin = {} end = {}", selection.begin_key, selection.end_key);

int begin_offset = 0;
if(iteration > 1) { 
  begin_offset = 1; 
}

const int end_offset = 0;

fmt::println("JFW: fdb_transaction_get_range(): begin_key = \"{}\", end_key = \"{}\", begin_offset = {}, end_offset = {}, iteration = {}",
              std::string_view(begin_key.data(), begin_key.size()), 
              std::string_view(end_key.data(), end_key.size()), 
              begin_offset, 
              end_offset, 
              iteration);

// See validate_and_update_parameters() in fdb_c.cpp (FDB source) if greater clarity is needed on the meaning of some of these, it can be
// hard to deduce from the documentation:
  return fdb_transaction_get_range(
                      raw_handle(),
                      (const uint8_t *)begin_key.data(), begin_key.size(),      // the reference-point key
                      1,                                                        // begin or eq
                      begin_offset,                                             // begin offset

                      (const uint8_t *)end_key.data(), end_key.size(),          // the end selector key
                      1,                                                        // end or eq
                      end_offset,                                               // end offset (a shift AFTER end is matched)

                      // How should results be grouped/chunked:
                      0,                                                        // limit (0 == unlimited)
                      0,                                                        // target bytes (0 == unlimited)
                      FDB_STREAMING_MODE_ITERATOR,                              // streaming mode (JFW: FDB_STREAMING_MODE_WANT_ALL)
                      iteration,                                                // iteration # (produced side effect)

                      // Other options:
                      0,                                                        // 0 unless this IS a snapshot read
                      0                                                         // reverse: should items come in reverse order?
                    );
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// Baby step: get the future, wait for it to be ready, return the result (or throw):
inline ceph::libfdb::future_value await_ready_range_future(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, int& iteration /* JFW: make const */)
{
 future_value fv = txn->get_range_future_from_transaction(key_range, iteration);

 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
   throw libfdb_exception(r);
 } 

 return fv;
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::detail {

// Generators:
// The returned memory should be copied immediately as its lifetime will end once the Future is destoyed:
inline std::generator<std::span<const FDBKeyValue>> generate_FDB_pairs(ceph::libfdb::transaction& txn_owner, ceph::libfdb::select key_range) 
{
  // As FDB is stateless, we need to track state somewhere:
  int iteration = 0;     
  int more_available = 1;
  int out_count = 0;
 
  const FDBKeyValue *out_kvs = nullptr;

fmt::println("JFW: range_block_generator(): {} <-> {}", key_range.begin_key, key_range.end_key);

  for(int iteration = 1; more_available; iteration++) {
fmt::println("JFW: iteration {}", iteration);
     ceph::libfdb::future_value fv = txn_owner.get_range_future_from_transaction(key_range, iteration);
  
     if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
       throw libfdb_exception(r);
     }
 
     if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &more_available); 0 != r) {
       throw libfdb_exception(r);
     }

auto r = std::span<const FDBKeyValue>(out_kvs, out_count);
fmt::println("JFW: returning block of {} entries (out_count was {})", r.size(), out_count);
    co_yield r;// JFW: std::span<const FDBKeyValue>(out_kvs, out_count);

    if(more_available) {
      // Make the first part of the range for the new search equal to the last one from the old search:
      const auto& last_key = r.back();
      key_range.begin_key = std::string_view((const char *)last_key.key, last_key.key_length);
     }
  }

/*
  while(more_available) {
     iteration++;

fmt::println("JFW: iteration {}", iteration);
     ceph::libfdb::future_value fv = txn_owner.get_range_future_from_transaction(key_range, iteration);
  
     if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
       throw libfdb_exception(r);
     }
 
     if(fdb_error_t r = fdb_future_get_keyvalue_array(fv.raw_handle(), &out_kvs, &out_count, &more_available); 0 != r) {
       throw libfdb_exception(r);
     }
 
auto r = std::span<const FDBKeyValue>(out_kvs, out_count);
fmt::println("JFW: returning block of {} entries (out_count was {})", r.size(), out_count);
    co_yield r;// JFW: std::span<const FDBKeyValue>(out_kvs, out_count);

    if(more_available) {
      // Make the first part of the range for the new search equal to the last one from the old search:
      const auto& last_key = r.back();
      key_range.begin_key = std::string_view((const char *)last_key.key, last_key.key_length);
     }
  }
*/
}

} // namespace ceph::libfdb::detail

/* JFW: correct behavior is only really shown in just a few places and not indicated by the documentation in 
 * useful way...
uint32_t GET_RANGE_COUNT = 100000;
const char* GET_RANGE_KPI = "C get range throughput (local client)";
struct RunResult getRange(struct ResultSet* rs, FDBTransaction* tr) {
    fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
    if (e)
        return RES(0, e);

    uint32_t startKey = ((uint64_t)rand()) % (numKeys - GET_RANGE_COUNT - 1);

    double start = getTime();

    const FDBKeyValue* outKv;
    int outCount;
    fdb_bool_t outMore = 1;
    int totalOut = 0;
    int iteration = 0;

    FDBFuture* f = fdb_transaction_get_range(tr, // transaction
                                             keys[startKey], // begin key name
                                             keySize, // begin key len
                                             1, // begin or eq
                                             0, // begin offs
                                             keys[startKey + GET_RANGE_COUNT], // end key name
                                             keySize, // end key len
                                             1, // end or eq
                                             0, // end offset
                                             0, // limit
                                             0, // terget bytes
                                             FDB_STREAMING_MODE_WANT_ALL, // streaming mode
                                             ++iteration, // iteration 
                                             0, // snapshot
                                             0); // reverse

    while (outMore) {
        e = maybeLogError(fdb_future_block_until_ready(f), "getting range", rs);
        if (e) {
            fdb_future_destroy(f);
            return RES(0, e);
        }

        e = maybeLogError(fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore), "reading range array", rs);
        if (e) {
            fdb_future_destroy(f);
            return RES(0, e);
        }

        totalOut += outCount;

        if (outMore) {
            FDBFuture* f2 = fdb_transaction_get_range(tr,
                                                      outKv[outCount - 1].key,
                                                      outKv[outCount - 1].key_length,
                                                      1, // begin or eq
                                                      1, // begin offset
                                                      keys[startKey + GET_RANGE_COUNT],
                                                      keySize,
                                                      1, // end or eq
                                                      0, // end offset
                                                      0, // limit
                                                      0, // target bytes
                                                      FDB_STREAMING_MODE_WANT_ALL, // streaming mode
                                                      ++iteration, // iteration
                                                      0, // snapshot
                                                      0); // reverse
            fdb_future_destroy(f);
            f = f2;
        }
    }

    if (totalOut != GET_RANGE_COUNT) {
        char* msg = (char*)malloc((sizeof(char)) * 200);
        sprintf(msg, "verifying out count (%d != %d)", totalOut, GET_RANGE_COUNT);
        logError(4100, msg, rs);
        free(msg);
        fdb_future_destroy(f);
        return RES(0, 4100);
    }
    if (outMore) {
        logError(4100, "verifying no more in range", rs);
        fdb_future_destroy(f);
        return RES(0, 4100);
    }
    fdb_future_destroy(f);

    double end = getTime();

    return RES(GET_RANGE_COUNT / (end - start), 0);
}
*/

#endif
