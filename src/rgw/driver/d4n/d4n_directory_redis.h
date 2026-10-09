#pragma once

#include "driver/d4n/d4n_directory.h"

namespace rgw::d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

class RedisLease : public Lease {
  public:
    explicit RedisLease(std::shared_ptr<RedisConnection>& redis_conn)
      : redis_pool(nullptr), REDISconn(redis_conn->get_redis_conn()) {}

    void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      redis_pool = pool;
    }

    int acquire(const DoutPrefixProvider* dpp,
                const std::string& resource_name,
                const std::string& holder_id,
                const std::string& token,
                uint64_t ttl_nanoseconds) override;

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

class RedisTransaction : public Transaction {
public:
  explicit RedisTransaction(std::shared_ptr<connection> conn, std::shared_ptr<RedisPool> pool)
    : conn_(conn), pool_(pool) {}

  ~RedisTransaction() override {
    if (!executed_ && pool_) pool_->release(conn_);
  }

  // internal use only — called by RedisBlockDirectory/RedisObjectDirectory/etc.
  request& get_request() { return req_; }

  int commit(const DoutPrefixProvider* dpp, optional_yield y) override;
  int abort(const DoutPrefixProvider* dpp, optional_yield y) override;

private:
  int execute_request(const DoutPrefixProvider* dpp, optional_yield y);
  std::shared_ptr<connection> conn_;
  std::shared_ptr<RedisPool> pool_;
  request req_;
  bool executed_{false};
};

class RedisTransactionFactory : public TransactionFactory {
public:
  explicit RedisTransactionFactory(std::shared_ptr<RedisPool> pool) : pool_(pool) {}
  std::unique_ptr<Transaction> create_transaction(const DoutPrefixProvider* dpp) override {
    auto conn = pool_->acquire(dpp);
    return std::make_unique<RedisTransaction>(conn, pool_);
  }
private:
  std::shared_ptr<RedisPool> pool_;
};


class RedisDirectory: virtual public Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool
    void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      	redis_pool = pool;
    }

    RedisDirectory(std::shared_ptr<RedisConnection>& redis_conn) : REDISconn(redis_conn->get_redis_conn()) {}
    virtual ~RedisDirectory() = default;
 
  int prepare_request(const DoutPrefixProvider* dpp,
                                    std::optional<std::reference_wrapper<Transaction>> txn,
                                    request& req,
                                    RedisTransaction*& rtxn,
                                    request*& target);
 
  virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val, 
		       std::optional<std::reference_wrapper<Transaction>> txn);

  virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& key,
                      const std::string& field,
                      const std::string& val, 
		      std::optional<std::reference_wrapper<Transaction>> txn);

  virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::vector<std::string>& fields,
                          std::map<std::string, std::string>& out_vals,
			  std::optional<std::reference_wrapper<Transaction>> txn);

  virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::map<std::string, std::string>& vals,
			  std::optional<std::reference_wrapper<Transaction>> txn);

  virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                   const std::string& key,
                                   const std::string& field,
                                   const std::string& val,
				   std::optional<std::reference_wrapper<Transaction>> txn);
  protected:
    std::shared_ptr<boost::redis::connection> REDISconn;

};

class RedisBucketDirectory: public RedisDirectory, public BucketDirectory {
  public:
    RedisBucketDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn) override;

  private:
    //scan_objects(pattern="photos/*")
    //Redis filters to only "photos/*" objects
    int scan_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn);
    //without prefix, get_range(start="-", end="+")
    int get_range(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start, uint64_t count, bool start_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn);
    int zadd(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, double score, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrem(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn);
    int zscan(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<CacheObject>& objs_info, uint64_t& next_cursor, std::optional<std::reference_wrapper<Transaction>> txn);

};

class RedisObjectDirectory: public RedisDirectory, public ObjectDirectory {
  public:
    RedisObjectDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<std::reference_wrapper<Transaction>> txn);
    virtual int remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<std::reference_wrapper<Transaction>> txn);
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<std::reference_wrapper<Transaction>> txn);
    virtual int list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn);

  private:
    int get_version_index(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, std::optional<std::reference_wrapper<Transaction>> txn);
    int zadd(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double score, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, int start, int stop, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrevrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrem(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn);
    int zremrangebyscore(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double min, double max, std::optional<std::reference_wrapper<Transaction>> txn);
    int zrank(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::string& index, std::optional<std::reference_wrapper<Transaction>> txn);

};

class RedisBlockDirectory: public RedisDirectory, public BlockDirectory {
  public:
    RedisBlockDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}
    
    virtual int exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) override;

    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<std::reference_wrapper<Transaction>> txn) override;
    virtual int remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<std::reference_wrapper<Transaction>> txn) override;

  private:
    template<AssociativeContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y) ;
};

} // namespace rgw::d4n
