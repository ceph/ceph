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

#include "driver/d4n/d4n_directory.h"

#include <memory>
#include <utility>

namespace rgw::d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

class RedisLease : public Lease {
  public:
    explicit RedisLease(std::shared_ptr<connection> conn, std::shared_ptr<RedisPool> pool = {})
      : redis_pool(std::move(pool)), REDISconn(std::move(conn))
    {}

    int acquire(const DoutPrefixProvider* dpp,
                const std::string& resource_name,
                const std::string& holder_id,
                const std::string& token,
                uint64_t ttl_nanoseconds,
                std::optional<Transaction> txn = std::nullopt) override;

    int renew(const DoutPrefixProvider* dpp,
              const std::string& resource_name,
              const std::string& holder_id,
              const std::string& token,
              uint64_t ttl_nanoseconds,
              uint64_t max_ticks = 0) override;

    int release(const DoutPrefixProvider* dpp,
                const std::string& resource_name,
                const std::string& holder_id,
                const std::string& token) override;

    LeaseCheckResult any_active(const DoutPrefixProvider* dpp,
                                 const std::string& resource_prefix) override;

    LeaseCheckResult is_active(const DoutPrefixProvider* dpp,
                                const std::string& resource_name,
                                const std::string& holder_id,
                                const std::string& token) override;

  private:
    std::shared_ptr<RedisPool> redis_pool;
    std::shared_ptr<boost::redis::connection> REDISconn;
};

class RedisTransaction final{
  std::shared_ptr<connection> conn_;
  std::shared_ptr<RedisPool> pool_;
  request req_;

public:
  explicit RedisTransaction(std::shared_ptr<connection> conn, std::shared_ptr<RedisPool> pool)
    : conn_(std::move(conn)), pool_(std::move(pool))
  {}

  RedisTransaction(const RedisTransaction&) = delete;
  RedisTransaction& operator=(const RedisTransaction&) = delete;

  ~RedisTransaction()
  {
    if (pool_) {
      pool_->release(std::move(conn_));
    }
  }

public:
  request& get_request() noexcept { return req_; }

private:
  friend struct RedisBackend;
  friend class RedisDirectory;
  int execute_request(const DoutPrefixProvider* dpp, optional_yield y);

};

class RedisDirectory: virtual public Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool

  public:
    explicit RedisDirectory(std::shared_ptr<connection> conn, std::shared_ptr<RedisPool> pool = {})
      : redis_pool(std::move(pool)), REDISconn(std::move(conn))
    {}

    virtual ~RedisDirectory() = default;
 
  int prepare_request(const DoutPrefixProvider* dpp,
                                    std::optional<Transaction> txn,
                                    request& req,
                                    request*& target);

protected:
  // Use the borrowed checkout now; queued commands stay with their owner.
  template <typename Response>
  void execute_immediate(const DoutPrefixProvider *dpp, std::optional<Transaction> txn,
                         boost::system::error_code& ec, const request& req,
                         Response& resp, optional_yield y,
                         std::source_location whence = std::source_location::current());

public:
 
  virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val, 
                       std::optional<Transaction> txn);

  virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& key,
                      const std::string& field,
                      const std::string& val, 
                      std::optional<Transaction> txn);

  virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::vector<std::string>& fields,
                          std::map<std::string, std::string>& out_vals,
                          std::optional<Transaction> txn);

  virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::map<std::string, std::string>& vals,
                          std::optional<Transaction> txn);

  virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                   const std::string& key,
                                   const std::string& field,
                                   const std::string& val,
                                   std::optional<Transaction> txn);
  protected:
    std::shared_ptr<boost::redis::connection> REDISconn;

};

class RedisBucketDirectory: public RedisDirectory, public BucketDirectory {
  public:
    using RedisDirectory::RedisDirectory;

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<Transaction> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<Transaction> txn) override;
    virtual int add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<Transaction> txn) override;
    virtual int remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<Transaction> txn) override;
    virtual int list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker,
                             std::string_view end_marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<Transaction> txn) override;

  private:
    //scan_objects(pattern="photos/*")
    //Redis filters to only "photos/*" objects
    int scan_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<Transaction> txn);
    int get_range(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::string_view start, std::string_view prefix, std::string_view end_marker, uint64_t count, bool start_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<Transaction> txn);
    int zadd(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, double score, const std::string& member, std::optional<Transaction> txn);
    int zrem(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& member, std::optional<Transaction> txn);
    int zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, std::optional<Transaction> txn);
    int zscan(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<CacheObject>& objs_info, uint64_t& next_cursor, std::optional<Transaction> txn);

};

class RedisObjectDirectory: public RedisDirectory, public ObjectDirectory {
  public:
    using RedisDirectory::RedisDirectory;

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<Transaction> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<Transaction> txn) override;

    virtual int add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<Transaction> txn);
    virtual int remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<Transaction> txn);
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<Transaction> txn);
    virtual int list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<Transaction> txn);

  private:
    int get_version_index(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, std::optional<Transaction> txn);
    int zadd(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double score, const std::string& member, std::optional<Transaction> txn);
    int zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, int start, int stop, std::vector<std::string>& members, std::optional<Transaction> txn);
    int zrevrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<std::string>& members, std::optional<Transaction> txn);
    int zrem(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::optional<Transaction> txn);
    int zremrangebyscore(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double min, double max, std::optional<Transaction> txn);
    int zrevrank(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::string& index, std::optional<Transaction> txn);

};

class RedisBlockDirectory: public RedisDirectory, public BlockDirectory {
  public:
    using RedisDirectory::RedisDirectory;
    
    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) override;

    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn) override;
    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) override;
    using BlockDirectory::get;

    std::expected<std::optional<CacheBlock>, int>
    find(const DoutPrefixProvider *dpp, optional_yield y, const CacheBlock& key,
         std::optional<Transaction> txn = std::nullopt) override;

    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn) override;
    virtual int copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, std::optional<Transaction> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn) override;
    virtual int update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<Transaction> txn) override;
    virtual int remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<Transaction> txn) override;
};

} // namespace rgw::d4n
