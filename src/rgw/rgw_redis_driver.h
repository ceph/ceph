#ifndef CEPH_REDISDRIVER_H
#define CEPH_REDISDRIVER_H

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_directory.h"

namespace rgw { namespace cache {

class RedisDriver : public CacheDriver {
  private:
    cpp_redis::client client;

  public:
    RedisDriver(Partition& _partition_info, std::string host, int port) : CacheDriver() {}

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) override;
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
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) override;

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override;
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override;
};

} } // namespace rgw::cal
    
#endif
