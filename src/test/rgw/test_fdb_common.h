#ifndef TEST_FDB_COMMON_H
 #define TEST_FDB_COMMON_H

#include <catch2/catch_test_macros.hpp>

#include <fmt/format.h>

#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>

#include <unistd.h>

namespace lfdb = ceph::libfdb;

constexpr const char* const msg = "Hello, World!";

constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};

constexpr const char pearl_msg[] =
R"(Perle, plesaunte to prynces paye
To clanly clos in golde so clere;
Oute of oryent, I hardyly saye.
Ne proved I never her precios pere.
)";

// Tests use a unique process-local namespace so different test binaries can share an FDB
// backing store without deleting or reading each other's keys.
inline std::string test_namespace_prefix()
{
 static const auto prefix = fmt::format("ceph-libfdb-test/{}/{}/",
                                        ::getpid(),
                                        std::chrono::steady_clock::now().time_since_epoch().count());
 return prefix;
}

inline std::string test_key(std::string_view key)
{
 return fmt::format("{}{}", test_namespace_prefix(), key);
}

inline std::string make_key_prefix(std::string_view prefix = "key")
{
 return fmt::format("{}{}_", test_namespace_prefix(), prefix);
}

inline std::string make_key(const int n, std::string_view prefix = "key")
{
 return fmt::format("{}{:010d}", make_key_prefix(prefix), n);
}

inline std::string make_value(const int n)
{
 return fmt::format("value_{:010d}", n);
}

inline std::map<std::string, std::string> make_monotonic_kvs(const unsigned N, std::string_view prefix = "key")
{
 std::map<std::string, std::string> kvs;

 for (const auto i : std::ranges::iota_view(0u, N)) {
  kvs.insert({ make_key(i, prefix), make_value(i) });
 }

 return kvs;
}

// Gadget to help clean up this test process's key namespace when we enter and leave scope:
struct janitor final
{
 ceph::libfdb::database_handle dbh_;

 // flip this off if you need artifacts after debugging:
 bool drop_after_scope = true;

 janitor(ceph::libfdb::database_handle dbh)
  : dbh_(dbh)
 {
  drop_test_namespace();
 }

 janitor()
  : janitor(ceph::libfdb::create_database())
 {}

 ~janitor()
 {
  if (drop_after_scope) {
   drop_test_namespace();
  }
 }

 public:
 ceph::libfdb::database_handle dbh() { return dbh_; }

 public:
 // This type coercion turns out to be very useful:
 operator ceph::libfdb::database_handle() { return dbh(); }

 public:
 void drop(const lfdb::select& range)
 {
  lfdb::erase(ceph::libfdb::make_transaction(dbh()),
              range,
              lfdb::commit_after_op::commit);
 }

 // Drop everything made by this test process:
 void drop_test_namespace()
 {
  drop(lfdb::select { test_namespace_prefix() });
 }

 // Drop keys made with our key convention:
 void drop_key_prefix(std::string_view prefix = "key")
 {
  drop(lfdb::select { make_key_prefix(prefix) });
 }
};

// This is both a utility for the test suite AND an example of one way to break a long series of writes up
// into blocks to avoid transaction timeout. Note that it runs serially; a parallel implementation is possible
// but probably would be slower for the scales used in the test and would involve mechanism a fair amount of
// extra mechanism, as FDB transactions themselves are not thread-safe:
inline void populate_monotonic(lfdb::database_handle dbh, const int N, std::string_view prefix = "key")
{
 using namespace std::ranges;

 const unsigned block_size = 8*1024;
 const auto total = static_cast<unsigned>(N);
 const auto nblocks = (total + block_size - 1) / block_size;

 for (auto block_index : views::iota(0u, nblocks)) {
  const auto first = block_index * block_size;
  const auto last = std::min(first + block_size, total);

  // We make a new transaction each block to avoid session timeout:
  auto txn = make_transaction(dbh);

  for (auto n : views::iota(first, last)) {
   lfdb::set(txn, make_key(n, prefix), make_value(n));
  }

  if (false == lfdb::commit(txn)) {
   throw std::runtime_error(fmt::format("unable to commit transaction; {} entries, {} block size", total, block_size));
  }
 }
}

#endif
