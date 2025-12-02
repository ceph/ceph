#pragma once

#include "rgw/rgw_fdb.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "include/random.h"

#include <map>
#include <list>
#include <vector>
#include <unordered_map>

#include <string>

#include <chrono>
#include <ranges>

namespace lfdb = ceph::libfdb;

/* Importantly, FDB operations are /lexicographically/ ordered. I do not have a bunch of
time to write a fancy generator, so I've taken a "dumb as bones" approach and write a
fixed prefix followed by integers: */

// As we manipulate keys and values quite a bit, it's helpful to have a recipe for them:
inline std::string make_key(const int n) {
 return fmt::format("key_{:04d}", n);
}

inline std::string make_value(const int n) {
 return fmt::format("value_{}", n);
}

// JFW: This and key_count() aren't going to work past the FDB single-read defaults-- so
// they're ok for smaller tests, but aren't going to traverse a "big" DB:
// Collect values in selection to out_values:
auto key_counter(auto txn, const auto& selector, auto& out_values) -> auto {
 out_values.clear();

 lfdb::get(txn, selector, 
           std::inserter(out_values, std::begin(out_values)), 
           lfdb::commit_after_op::no_commit);

 return out_values.size();
};

auto key_count(auto& dbh, const auto& selector) {
 std::map<std::string, std::string> _;
 return key_counter(lfdb::make_transaction(dbh), selector, _);
}

inline std::map<std::string, std::string> make_monotonic_kvs(const int N)
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(0, 1 + N)) {
  kvs.insert({ make_key(i), make_value(i) });
 }

 return kvs;
}

// Write straight into FDB, for bigger sets:
inline void set_monotonic_kvs(lfdb::database_handle dbh, int nkeys)
{
 auto txn = lfdb::make_transaction(dbh, {}, lfdb::commit_after_op::commit);

 std::ranges::for_each(std::views::iota(0, 1 + nkeys), [&txn](const auto i) {
  lfdb::set(txn, make_key(i), make_value(i));
 });
}

// Clean up test keys when we leave scope:
struct janitor final
{
 // flip this off if you need artifacts after debugging:
 bool drop_after_scope = true;

 janitor()
 {
  drop_all();
 } 

 ~janitor()
 {
  if(drop_after_scope)
   drop_all();
 }

 static void drop_all() {
   // Nobody'd better run this in production!
   // Note: technically, 0xFF, 0xFF is needed to include the system keys (if the transaction's allowed to
   // access these), but I don't think any tests will be doing this, at least not for now; the documentation
   // with details is unfortunately light on, for instance, whether or not after 0xFF the NULL character is
   // included, but we'll assume it is (again, for our purposes):
   const char begin_key[] = { (char)0x00 };
   const char end_key[]   = { (char)0xFF };
   lfdb::erase(lfdb::make_transaction(lfdb::create_database()),
              lfdb::select { begin_key, end_key }, 
              lfdb::commit_after_op::commit);
   }
};

