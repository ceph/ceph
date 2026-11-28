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

#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"
#include "d4n_log.h"
#include "rgw/ceph_fdb.h"
#include "rgw/fdb/transaction_result.h"
#include "d4n_transaction_result.h"

#include <boost/asio/consign.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <boost/version.hpp>
#include <expected>
#include <functional>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <concepts>
#include <optional>
#include <source_location>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace rgw::d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

class RedisTransaction;

using fdb_txn_ref = std::reference_wrapper<ceph::libfdb::transaction_handle>;
using redis_txn_ref = std::reference_wrapper<RedisTransaction>;

// A view into one active attempt; its backend coordinator retains completion ownership.
using Transaction = std::variant<fdb_txn_ref, redis_txn_ref>;

class RedisPool {
public:
    RedisPool(boost::asio::io_context* ioc, const boost::redis::config& cfg, std::size_t size)
        :  m_ioc(ioc),m_cfg(cfg) {
        for (std::size_t i = 0; i < size; ++i) {
            // Each connection gets its own strand
            auto strand = boost::asio::make_strand(*m_ioc);
            auto conn = std::make_shared<boost::redis::connection>(strand);
            m_pool.push_back(conn);
        }
    }

    ~RedisPool() {
      cancel_all();
    }

    std::shared_ptr<boost::redis::connection> acquire(const DoutPrefixProvider* dpp = nullptr) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);

	if (!m_is_pool_connected) {
		for(auto& it:m_pool) {
	    		auto conn = it;
#if BOOST_VERSION >= 108900
	    		conn->async_run(m_cfg, boost::asio::consign(boost::asio::detached, conn));
#else
	    		conn->async_run(m_cfg, {}, boost::asio::consign(boost::asio::detached, conn));
#endif
		}
	    m_is_pool_connected = true;
	}

        if (m_pool.empty()) {
		if (dpp) {
			maybe_warn_about_blocking(dpp);
		}
		//wait until m_pool is not empty
		m_cond_var.wait(lock, [this] { return !m_pool.empty(); });
        }
        auto conn = m_pool.front();
        m_pool.pop_front();
        return conn;
    }

    void release(std::shared_ptr<boost::redis::connection> conn) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        m_pool.push_back(conn);
	// Notify one waiting thread that a connection is available
	m_cond_var.notify_one();
    }

    int current_pool_size() const {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        return m_pool.size();
    }

    void cancel_all() {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        if(m_is_pool_connected) {
	for(auto& conn : m_pool) {
		conn->cancel();
        }
      }
    }

private:
    boost::asio::io_context* m_ioc;
    boost::redis::config m_cfg;
    std::deque<std::shared_ptr<boost::redis::connection>> m_pool;
    mutable std::mutex m_aquire_release_mtx;
    std::condition_variable m_cond_var;
    bool m_is_pool_connected{false};
};

class Pipeline {
  public:
    Pipeline(std::shared_ptr<boost::redis::connection>& conn, std::shared_ptr<RedisPool> redis_pool) : REDISconn(conn), redis_pool(redis_pool) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request() { return req; }

  private:
    std::shared_ptr<boost::redis::connection> REDISconn;
    std::shared_ptr<RedisPool> redis_pool{nullptr};
    request req;
    bool pipeline_mode{false};
};

// Successful checks distinguish active and inactive leases; failures carry a D4N error:
using LeaseCheckResult = std::expected<bool, int>;

// Distributed lease/lock interface for coordinating access to resources
//
// Provides time-bound exclusive access to resources with support for:
// - Hierarchical resource naming (bucket/object/version/operation)
//   Components are URL-encoded to handle '/' in S3 object names
// - Nanosecond-precision TTL for fine-grained control
// - Token-based ownership validation
// - Renewal with tick limits
// - Opportunistic cleanup of expired leases
//
// Return code semantics:
//   acquire():
//     0       : Lease successfully acquired (or already owned by caller - idempotent)
//     -EEXIST : Lease held by another holder
//     -EINVAL : Invalid parameters (empty strings, zero TTL)
//     -EIO    : Database/network error
//
//   renew():
//     0       : Lease successfully renewed (or already renewed - idempotent)
//     -ENOENT : Lease not found or expired
//     -EACCES : Ownership validation failed (wrong holder_id or token)
//     -EINVAL : Max ticks limit reached (lease still held - caller decides next action)
//     -EIO    : Database/network error
//
//   release():
//     0       : Lease successfully released (or already released - idempotent)
//     -EACCES : Ownership validation failed
//     -EIO    : Database/network error
//
//   is_active():
//     true    : Lease exists, not expired, and owned by caller
//     false   : Lease not found, expired, or owned by someone else
//     unexpected(error): Database/network error or invalid input
//
//   any_active():
//     true    : At least one active lease exists with matching prefix
//     false   : No active leases found
//     unexpected(error): Database/network error or invalid input
//
// Transaction safety note:
//   If is_active() authorizes a later FDB update, consider sharing the transaction:
//   read the lease key in the update transaction itself - FDB will retry if it changes.
//
class Lease {
  public:
    Lease() = default;
    virtual ~Lease() = default;

    // Acquire a lease for a resource
    //
    // resource_name: hierarchical identifier (e.g., "bucket/object/version")
    // holder_id: identifier of the lease holder (e.g., node ID, process ID)
    // token: unique token for this lease acquisition (prevents conflicts)
    // ttl_nanoseconds: time-to-live for the lease in nanoseconds
    //
    // Idempotent: returns 0 if already acquired by same holder_id + token
    virtual int acquire(const DoutPrefixProvider* dpp,
    		        const std::string& resource_name,
	                const std::string& holder_id,
            		const std::string& token,
            		uint64_t ttl_nanoseconds,
                        std::optional<Transaction> txn = std::nullopt) = 0;

    // Renew an existing lease
    //
    // Must provide matching resource_name, holder_id, and token from acquire
    // max_ticks: optional limit on renewal attempts (0 = unlimited)
    //
    // Idempotent: returns 0 if already renewed
    virtual int renew(const DoutPrefixProvider* dpp,
                      const std::string& resource_name,
                      const std::string& holder_id,
                      const std::string& token,
                      uint64_t ttl_nanoseconds,
                      uint64_t max_ticks = 0) = 0;

    // Release a lease
    //
    // Must provide matching resource_name, holder_id, and token from acquire
    //
    // Idempotent: returns 0 if already released
    virtual int release(const DoutPrefixProvider* dpp,
                        const std::string& resource_name,
                        const std::string& holder_id,
                        const std::string& token) = 0;

    // Check if any active lease exists for a resource (supports prefix matching)
    //
    // resource_prefix: hierarchical prefix (e.g., "bucket" matches "bucket/obj1", "bucket/obj2")
    //
    // Returns true for an active lease, false for inactivity, or an unexpected D4N error.
    //
    // Check success before inspecting the active flag:
    //   const auto result = lease.any_active(dpp, "bucket");
    //   if (!result) { return result.error(); }
    //   if (*result) { /* active lease */ }
    //
    // Side effect: opportunistically deletes expired leases during scan
    virtual LeaseCheckResult any_active(const DoutPrefixProvider* dpp,
                                        const std::string& resource_prefix) = 0;

    // Check if a specific lease is still active and owned by the caller
    //
    // Validates: lease exists, not expired, matches holder_id and token
    //
    // Returns true for an active lease, false for inactivity, or an unexpected D4N error.
    //
    // Check success before inspecting the active flag:
    //   const auto result = lease.is_active(dpp, resource, holder, token);
    //   if (!result) { return result.error(); }
    //   if (*result) { /* active lease */ }
    //
    // Side effect: opportunistically deletes expired lease if found
    virtual LeaseCheckResult is_active(const DoutPrefixProvider* dpp,
                                       const std::string& resource_name,
                                       const std::string& holder_id,
                                       const std::string& token) = 0;
};

enum class ObjectFields { // Fields stored in object directory 
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName,
  Acl
};

enum class BlockFields { // Fields stored in block directory 
  BlockID,
  Version, 
  DeleteMarker,
  Size,
  GlobalWeight,
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName,
  Acl
};

//Represents an object entry for ListObjects
struct CacheObject {
  std::string objName;
  std::string bucketId;
  std::string etag;
  uint64_t size; //total object size
  std::string creationTime;
  bool deleteMarker{false};
};

//Represents an Object version entry for ListObjectVersions
struct CacheObjectVersion {
  std::string objName;
  std::string bucketId;
  std::string version;
  std::string user_id;
  std::string display_name;
  bool deleteMarker{false};
  std::string etag;
  uint64_t size{0};
  std::string creationTime;

  bool operator==(const CacheObjectVersion&) const = default;
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty{false};
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string etag; //etag needed for list objects
  uint64_t size; //total object size (and not block size), needed for list objects
  std::string user_id; // id of user, needed for list object versions
  std::string display_name; // display name of owner, needed for list object versions
  std::string acl;
  rgw::sal::Attrs attrs; // attrs for a head block
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID = 0;
  std::string version;
  bool deleteMarker{false};
  uint64_t size = 0; /* Block size in bytes */
  uint64_t globalWeight = 0; /* LFUDA policy variable */
  bool invalid{false}; /* Tombstone flag: object logically deleted, awaiting cleanup after active leases expire */
  /* Blocks use the cacheObj's dirty and hostsList metadata to store their dirty flag values and locations in the block directory. */
};

namespace detail {

inline int report_transaction_failure(const DoutPrefixProvider *dpp, const ResultInfo& info,
                                      const int error, const std::source_location whence)
{
  const auto report_error = [&](const std::string_view message) {
    log_message(dpp, whence, "{}, ret={}, error={}:{}", message, error,
                info.source_error.category().name(), info.source_error.value());
  };

  switch (info.status) {
  case TransactionStatus::success:
    return 0;

  case TransactionStatus::uncertain:
    report_error("Transaction outcome is uncertain");
    break;

  case TransactionStatus::retries_exhausted:
    report_error("Transaction retry budget exhausted");
    break;

  case TransactionStatus::failed:
    report_error("Transaction failed");
    break;
  }

  return 0 > error ? error : TransactionStatus::retries_exhausted == info.status ? -EAGAIN : -EIO;
}

template <typename UncertainHandler>
int handle_transaction_failure(const DoutPrefixProvider *dpp, const int error, const bool exhausted,
                               UncertainHandler&, const std::source_location whence)
{
  return report_transaction_failure(dpp, describe_failure(error, exhausted), error, whence);
}

template <uncertainty_handler UncertainHandler>
int handle_transaction_failure(const DoutPrefixProvider *dpp, const fdb_failure& failure,
                               const bool exhausted, UncertainHandler& handle_uncertain,
                               const std::source_location whence)
{
  const auto info = describe_failure(failure, exhausted);
  const auto error = return_d4n_error(dpp, failure, whence);
  const auto result = report_transaction_failure(dpp, info,
    TransactionStatus::retries_exhausted == info.status ? -EAGAIN : error, whence);

  // Handle FoundationDB unknown_commit: pass the complete native failure to the caller:
  if (TransactionStatus::uncertain == info.status) {
    return std::invoke(handle_uncertain, failure);
  }

  return result;
}

} // namespace detail

template <detail::uncertainty_handler UncertainHandler>
int handle_transaction_result(const DoutPrefixProvider *dpp, const TransactionResult& result,
                              UncertainHandler&& handle_uncertain,
                              const std::source_location whence = std::source_location::current())
{
  return result.inspect([&](const auto& execution) {
    if (execution) {
      return 0;
    }

    return detail::handle_transaction_failure(dpp, execution.error(),
      result.retry_limit_reached(), handle_uncertain, whence);
  });
}

inline int handle_transaction_result(const DoutPrefixProvider *dpp, const TransactionResult& result,
                                    const std::source_location whence = std::source_location::current())
{
  // Handle FoundationDB unknown_commit: the default D4N policy returns -EIO:
  return handle_transaction_result(dpp, result, [](const auto&) { return -EIO; }, whence);
}


class Directory {
public:
    Directory() = default;
    virtual ~Directory() = default;

    // Single field get/set
    virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val,
                       std::optional<Transaction> txn) = 0;

    virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       const std::string& val,
                       std::optional<Transaction> txn) = 0;

    // Multi-field get/set
    virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& key,
                            const std::vector<std::string>& fields,
                            std::map<std::string, std::string>& out_vals,
                            std::optional<Transaction> txn) = 0;

    virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& key,
                            const std::map<std::string, std::string>& vals,
                            std::optional<Transaction> txn) = 0;

    virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                     const std::string& key,
                                     const std::string& field,
                                     const std::string& val,
                                     std::optional<Transaction> txn) = 0;

};


//Namespace to lexicographically order objects belonging to a bucket
//Should we rename to ObjectDirectory?
class BucketDirectory: virtual public Directory {
  public:
    BucketDirectory() = default;
    virtual ~BucketDirectory() = default;

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<Transaction> txn) = 0;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<Transaction> txn) = 0;
    virtual int add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<Transaction> txn) = 0;
    virtual int remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<Transaction> txn) = 0;

    // end_marker is an exclusive object-name bound, applied before page lookahead.
    virtual int list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, std::string_view end_marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<Transaction> txn) = 0;
};


//Namespace to order versions of an object in the order in which they were added, with the latest
//version appearing first
//Should we rename to ObjectVersionDirectory?
class ObjectDirectory: virtual public Directory {
  public:
    ObjectDirectory() = default;
    virtual ~ObjectDirectory() = default;
	
    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<Transaction> txn) = 0;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<Transaction> txn) = 0;
    //version ordering is a function of creation time, hence adding creation time to the interface
    virtual int add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<Transaction> txn) = 0;
    virtual int remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<Transaction> txn) = 0;
    //this can be removed and remove_version can be used instead
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<Transaction> txn) = 0;
    virtual int list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<Transaction> txn) = 0;

  protected:
    static std::string build_index(const std::string& bucket_id, const std::string& obj_name);
};

//Namespace to store key, value pairs of a block
class BlockDirectory: virtual public Directory {
  public:
    BlockDirectory() = default;
    virtual ~BlockDirectory() = default;
    
    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) = 0;

    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn) = 0;
    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) = 0;

    // Absence is a successful lookup; deletion flags remain part of a present record.
    [[nodiscard]] virtual std::expected<std::optional<CacheBlock>, int>
    find(const DoutPrefixProvider *dpp, optional_yield y, const CacheBlock& key,
         std::optional<Transaction> txn = std::nullopt) = 0;

    // Existing callers require an entry and publish output only on success:
    int get(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn);

    // Batch lookup loads present records and preserves initialized requests for absent ones:
    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn) = 0;

    virtual int copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, std::optional<Transaction> txn) = 0;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) = 0;
    virtual int update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<Transaction> txn) = 0;
	
    virtual int remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<Transaction> txn) = 0;
	
  protected:
    static std::map<std::string, std::string> encode_fields(const CacheBlock& block);

    std::string build_index(const CacheBlock* block);
};

} // namespace rgw::d4n
