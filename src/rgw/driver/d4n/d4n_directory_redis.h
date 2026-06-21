#pragma once

#include "driver/d4n/d4n_directory.h"

namespace rgw::d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

class RedisDirectory: virtual public Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool
    void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      	redis_pool = pool;
    }

    RedisDirectory(std::shared_ptr<RedisConnection>& redis_conn) : REDISconn(redis_conn->get_redis_conn()) {}
    virtual ~RedisDirectory() = default;
  
  virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val);

  virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& key,
                      const std::string& field,
                      const std::string& val);

  virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::vector<std::string>& fields,
                          std::map<std::string, std::string>& out_vals);

  virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::map<std::string, std::string>& vals);

  virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                   const std::string& key,
                                   const std::string& field,
                                   const std::string& val);
  protected:
    std::shared_ptr<boost::redis::connection> REDISconn;

};

class RedisBucketDirectory: public RedisDirectory, public BucketDirectory {
  public:
    RedisBucketDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) override;
    virtual int del(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) override;
    virtual int add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y) override;
    //scan_objects(pattern="photos/*")
    //Redis filters to only "photos/*" objects
    virtual int scan_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t start_pos, const std::string& pattern, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, uint64_t& next_pos, optional_yield y) override;
    //without prefix, get_range(start="-", end="+")
    virtual int get_range(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, optional_yield y) override;

  private:
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y);
    int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y);

};

class RedisObjectDirectory: public RedisDirectory, public ObjectDirectory {
  public:
    RedisObjectDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) override;
    virtual int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override;

    virtual int add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline=nullptr);
    virtual int remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y);
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const double& creation_time,optional_yield y);
    virtual int list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<CacheObjectVersion>& obj_versions, optional_yield y);
    virtual int get_version_index(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, optional_yield y) override;

  private:
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& member, optional_yield y);
    int zremrangebyscore(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, double min, double max, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::string& index, optional_yield y);

};

class RedisBlockDirectory: public RedisDirectory, public BlockDirectory {
  public:
    RedisBlockDirectory(std::shared_ptr<RedisConnection>& redis_conn): RedisDirectory(redis_conn) {}
    
    virtual int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;

    //Pipelined version of set
    virtual int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) override;
    virtual int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;
    //Pipelined version of get using boost::redis::response for list bucket
	/*
    template <size_t N = 100>
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
	*/
    //Pipelined version of get using boost::redis::generic_response
    virtual int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) override;
    virtual int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y) override;
    virtual int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y) override;
    virtual int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y) override;

  private:
    template<AssociativeContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y) ;
};

} // namespace rgw::d4n
