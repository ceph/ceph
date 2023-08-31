#pragma once

#include <cpp_redis/cpp_redis>
#include <boost/redis/connection.hpp>

#include "rgw_common.h"
#include "d4n_directory.h"
#include "../../rgw_redis_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class CachePolicy {
  public:
    CephContext* cct;

    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;
      return 0;
    }
    virtual int exist_key(std::string key, optional_yield y) = 0;
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) = 0;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    net::io_context& io;
    std::shared_ptr<connection> conn;

  public:
    LFUDAPolicy(net::io_context& io_context) : CachePolicy(), io(io_context) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }

    int set_age(int age, optional_yield y);
    int get_age(optional_yield y);
    int set_global_weight(std::string key, int weight, optional_yield y);
    int get_global_weight(std::string key, optional_yield y);
    int set_min_avg_weight(size_t weight, std::string cacheLocation, optional_yield y);
    int get_min_avg_weight(optional_yield y);
    CacheBlock find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y);
    void shutdown();

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host; // TODO: Replace with cache address
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::detached);

      return 0;
    }
    virtual int exist_key(std::string key, optional_yield y) override;
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
};

class PolicyDriver {
  private:
    net::io_context& io;
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(net::io_context& io_context, std::string _policyName) : io(io_context), policyName(_policyName) {}
    ~PolicyDriver() {
      delete cachePolicy;
    }

    int init();
    CachePolicy* get_cache_policy() { return cachePolicy; }
};

} } // namespace rgw::d4n
