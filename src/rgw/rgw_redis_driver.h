#pragma once

#include <aio.h>
#include <boost/redis/connection.hpp>

#include "common/async/completion.h"
#include "rgw_common.h"
#include "rgw_cache_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace cache {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class RedisDriver : public CacheDriver {
  public:
    RedisDriver(net::io_context& io_context, Partition& _partition_info) : partition_info(_partition_info),
								           free_space(_partition_info.size), 
								           outstanding_write_size(0)
    {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
      add_partition_info(_partition_info);
    }
    virtual ~RedisDriver()
    {
      remove_partition_info(partition_info);
    }

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; }
    static std::optional<Partition> get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type);
    static std::vector<Partition> list_partitions(const DoutPrefixProvider* dpp);

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int put_async(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual rgw::AioResultList get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
    virtual int del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) override;
    void shutdown();

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
    std::shared_ptr<connection> conn;

    static std::unordered_map<std::string, Partition> partitions;
    Partition partition_info;
    uint64_t free_space;
    uint64_t outstanding_write_size;

    int add_partition_info(Partition& info);
    int remove_partition_info(Partition& info);
};

} } // namespace rgw::cache
