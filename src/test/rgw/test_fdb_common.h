#ifndef TEST_FDB_COMMON_H
 #define TEST_FDB_COMMON_H

#include <catch2/catch_test_macros.hpp>

#include <fmt/format.h>

#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <map>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>

namespace lfdb = ceph::libfdb;

constexpr const char* const msg = "Hello, World!";

constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};

constexpr const char pearl_msg[] =
R"(Perle, plesaunte to prynces paye
To clanly clos in golde so clere;
Oute of oryent, I hardyly saye.
Ne proved I never her precios pere.
)";

// It's a little moribund, but for tests we use this highly-creative (*cough!*) prefix
// to make it easy to not only remove stale keys but diagnose them when there's a problem:
inline std::string make_key(const int n, std::string_view prefix = "key")
{
 return fmt::format("{}_{:010d}", prefix, n);
}

inline std::string make_value(const int n)
{
 return fmt::format("value_{:010d}", n);
}

inline std::map<std::string, std::string> make_monotonic_kvs(const unsigned N, std::string_view prefix = "key")
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(0u, N)) {
  kvs.insert({ make_key(i, prefix), make_value(i) });
 }

 return kvs;
}

// Gadget to help clean up test keys-- both removing stale ones when we enter and also tidying
// up after ourselves when we leave scope:
struct janitor final
{
 ceph::libfdb::database_handle dbh_;

 // flip this off if you need artifacts after debugging:
 bool drop_after_scope = true;

 janitor(ceph::libfdb::database_handle dbh_)
  : dbh_(dbh_)
 {
  drop_all();
 }

 janitor()
  : janitor(ceph::libfdb::create_database())
 {}

 ~janitor()
 {
  if(drop_after_scope)
    drop_all();
 }

 public:
 ceph::libfdb::database_handle dbh() { return dbh_; }

 public:
 // This type coersion turns out to be very useful:
 operator ceph::libfdb::database_handle() { return dbh(); }

 public:
 void drop(const lfdb::select& range)
 {
  lfdb::erase(ceph::libfdb::make_transaction(dbh()),
              range,
              lfdb::commit_after_op::commit);
 }

 // Drop everything in the database (except system keys):
 void drop_all()
 {
  // Note: technically, [0x00, 0xFF) is needed to include the system keys (if the transaction's allowed to
  // access these). However, special permissions are needed to access these magical "system keys" and we
  // probably don't actually want to delete them erroneously. So, we stick with the normal user-key range...
  return drop(lfdb::select { "" });
 }

 // Drop keys made with our key convention:
 void drop_all_keys()
 {
  return drop(lfdb::select { "key_" });
 }
};

// This is both a utility for the test suite AND an example of one way to break a long series of writes up
// into blocks to avoid transaction timeout. Note that it runs serially; a parallel implementation is possible
// but probably would be slower for the scales used in the test and would involve mechanism a fair amount of
// extra mechanism, as FDB transactions themselves are not thread-safe:
inline void populate_monotonic(lfdb::database_handle dbh, const int N, std::string_view prefix = "key")
{
 using namespace std::ranges;

 const unsigned stride = 8*1024;
 const auto total = static_cast<unsigned>(N);
 const auto nblocks = (total + stride - 1) / stride;

 for(auto block_index : views::iota(0u, nblocks)) {
  const auto first = block_index * stride;
  const auto last = std::min(first + stride, total);

  // We make a new transaction each block to avoid session timeout:
  auto txn = make_transaction(dbh);

  for(auto n : views::iota(first, last)) {
    lfdb::set(txn, make_key(n, prefix), make_value(n));
  }

  if(false == lfdb::commit(txn)) {
    throw std::runtime_error(fmt::format("unable to commit transaction; {} entries, {} stride", total, stride));
  }
 }
}

#endif
