#pragma once

#include <aio.h>
#include <boost/redis/connection.hpp>

#include "common/async/completion.h"
#include "rgw_common.h"
#include "rgw_cache_driver.h"

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
    }
    virtual ~RedisDriver() {}

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; }

    virtual int initialize(const DoutPrefixProvider* dpp) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, 
                                          const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual rgw::AioResultList get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
    virtual int del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
    void shutdown();
     
  private:
    std::shared_ptr<connection> conn;
    Partition partition_info;
    uint64_t free_space;
    uint64_t outstanding_write_size;

    struct redis_response {
      boost::redis::request req;
      boost::redis::response<std::string> resp;
    };

    struct redis_aio_handler { 
      rgw::Aio* throttle = nullptr;
      rgw::AioResult& r;
      std::shared_ptr<redis_response> s;

      /* Read Callback */
      void operator()(auto ec, auto) const {
        if (ec.failed()) {
	  r.result = -ec.value();
        } else {
	  r.result = 0;
        }

        /* Only append data for GET call */
        if (s->req.payload().find("HGET") != std::string::npos) {
	  r.data.append(std::get<0>(s->resp).value());
        }

	throttle->put(r);
      }
    };

    Aio::OpFunc redis_read_op(optional_yield y, std::shared_ptr<connection> conn, off_t read_ofs, off_t read_len, const std::string& key);
    Aio::OpFunc redis_write_op(optional_yield y, std::shared_ptr<connection> conn, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key);
};

} } // namespace rgw::cache
