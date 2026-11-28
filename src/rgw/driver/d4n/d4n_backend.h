/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#ifndef CEPH_RGW_D4N_BACKEND_H
 #define CEPH_RGW_D4N_BACKEND_H

#include "d4n_directory_fdb.h"
#include "d4n_directory_redis.h"

#include <boost/asio/dispatch.hpp>
#include <boost/compat/function_ref.hpp>

#include <memory>
#include <utility>
#include <variant>

namespace rgw::d4n {

struct FDBBackend final
{
 FDBObjectDirectory objects;
 FDBBlockDirectory blocks;
 FDBBucketDirectory buckets;
 FDBLease lease;

 public:
 explicit FDBBackend(lfdb::database_handle db)
  : objects(db),
    blocks(db),
    buckets(db),
    lease(std::move(db))
 {}

 FDBBackend(const FDBBackend&) = delete;

 FDBBackend& operator=(const FDBBackend&) = delete;

 public:
 Directory& kv_dir() noexcept { return objects; }  // member adapter

 public:
 TransactionResult execute(const DoutPrefixProvider *dpp, optional_yield y,
                           boost::compat::function_ref<int(Transaction&)> fn,
                           unsigned max_attempts = 5);

};

struct RedisBackend final
{
 std::shared_ptr<connection> conn;
 std::shared_ptr<RedisPool> pool;
 RedisDirectory directory;
 RedisObjectDirectory objects;
 RedisBlockDirectory blocks;
 RedisBucketDirectory buckets;
 RedisLease lease;

 public:
 // Generic KV traffic keeps the primary connection; specialized directories use the (swimming?) pool:
 RedisBackend(std::shared_ptr<connection> conn, std::shared_ptr<RedisPool> pool)
  : conn(std::move(conn)), pool(std::move(pool)), directory(this->conn),
    objects(this->conn, this->pool), blocks(this->conn, this->pool),
    buckets(this->conn, this->pool), lease(this->conn, this->pool)
 {}

 RedisBackend(const RedisBackend&) = delete;
 RedisBackend& operator=(const RedisBackend&) = delete;

 public:
 Directory& kv_dir() noexcept { return directory; }  // member adapter

 TransactionResult execute(const DoutPrefixProvider *dpp, optional_yield y,
                           boost::compat::function_ref<int(Transaction&)> fn,
                           unsigned max_attempts = 5);

 void stop()
 {
   if (pool) {
     pool->cancel_all();
   }

   boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
 }

};

/* Selecting a backend is now a matter of injection into the sum type
(aka "constructing an alternative" into the variant): */
using Backend = std::variant<FDBBackend, RedisBackend>;

} // namespace rgw::d4n

#endif // CEPH_RGW_D4N_BACKEND_H
