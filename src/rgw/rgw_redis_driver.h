#ifndef CEPH_REDISDRIVER_H
#define CEPH_REDISDRIVER_H

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_directory.h"

namespace rgw {

class RedisDriver : public CacheDriver {
  private:
    cpp_redis::client client;
    rgw::d4n::Address addr;

  public:
    RedisDriver(Partition& _partition_info, std::string host, int port) : CacheDriver(_partition_info) {
      addr.host = host;
      addr.port = port;
    }
<<<<<<< HEAD
=======
    virtual ~RedisDriver()
    {
      remove_partition_info(partition_info);
    }

    /* Entry */
    virtual bool key_exists(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual std::vector<Entry> list_entries(const DoutPrefixProvider* dpp) override;
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) override;
    //int update_local_weight(const DoutPrefixProvider* dpp, std::string key, int localWeight); // may need to exist for base class -Sam

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; } // how to get this from redis server? -Sam
    static std::optional<Partition> get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type);
    static std::vector<Partition> list_partitions(const DoutPrefixProvider* dpp);
>>>>>>> 5987321cf25 (RGW: Add `optional_yield` to `RedisDriver::key_exists`)

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
<<<<<<< HEAD
    virtual bool key_exists(const DoutPrefixProvider* dpp, std::string& key) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) override;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val) override;
=======
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual rgw::AioResultList get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;

    virtual std::unique_ptr<CacheAioRequest> get_cache_aio_request_ptr(const DoutPrefixProvider* dpp) override { return nullptr; }

    struct redis_response {
      boost::redis::response<std::string> resp;
    };

    struct redis_aio_handler { 
      rgw::Aio* throttle = nullptr;
      rgw::AioResult& r;
      std::shared_ptr<redis_response> s;

      /* Read Callback */
      void operator()(boost::system::error_code ec, auto) const {
	r.result = -ec.value();
	r.data.append(std::get<0>(s->resp).value().c_str());
	throttle->put(r);
      }
    };

  protected:
    boost::redis::connection& conn;

    rgw::d4n::Address addr; // remove -Sam
    cpp_redis::client client;
    static std::unordered_map<std::string, Partition> partitions;
    std::unordered_map<std::string, Entry> entries;
    Partition partition_info;
    uint64_t free_space;
    uint64_t outstanding_write_size;
    CephContext* cct;

    int find_client(const DoutPrefixProvider* dpp);
    int insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len);
    std::optional<Entry> get_entry(const DoutPrefixProvider* dpp, std::string key);
    int remove_entry(const DoutPrefixProvider* dpp, std::string key);
    int add_partition_info(Partition& info);
    int remove_partition_info(Partition& info);
    auto redis_exec(boost::system::error_code ec, boost::redis::request req, boost::redis::response<std::string>& resp, optional_yield y);
>>>>>>> f81decedd61 (RGW: Add `optional_yield` and `redis_exec` to Redis Driver)
};

} // namespace rgw::sal
    
#endif
