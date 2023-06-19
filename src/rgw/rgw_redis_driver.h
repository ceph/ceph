#ifndef CEPH_REDISDRIVER_H
#define CEPH_REDISDRIVER_H

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_directory.h"

namespace rgw { namespace cache {

class RedisDriver;

class RedisCacheAioRequest: public CacheAioRequest {
public:
  RedisCacheAioRequest(RedisDriver* cache_driver) : cache_driver(cache_driver) {}
  virtual ~RedisCacheAioRequest() = default;
  virtual void cache_aio_read(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) override;
  virtual void cache_aio_write(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, bufferlist& bl, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) override;
private:
  RedisDriver* cache_driver;
};

class RedisDriver : public CacheDriver {
  private:
    cpp_redis::client client;
    rgw::d4n::Address addr;
    std::unordered_map<std::string, Entry> entries;

    int find_client(const DoutPrefixProvider* dpp);
    int insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len);
    int remove_entry(const DoutPrefixProvider* dpp, std::string key);
    std::optional<Entry> get_entry(const DoutPrefixProvider* dpp, std::string key);

  public:
    RedisDriver() : CacheDriver() {}

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) override;
    virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) override;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val) override;

    /* Entry */
    virtual bool key_exists(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual std::vector<Entry> list_entries(const DoutPrefixProvider* dpp) override;
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) override;
    int update_local_weight(const DoutPrefixProvider* dpp, std::string key, int localWeight); // may need to exist for base class -Sam

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override;
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override;

    virtual std::unique_ptr<CacheAioRequest> get_cache_aio_request_ptr(const DoutPrefixProvider* dpp) override;
};

} } // namespace rgw::cal
    
#endif
