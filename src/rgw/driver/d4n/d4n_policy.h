#pragma once

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "d4n_directory.h"
#include "../../rgw_redis_driver.h"

namespace rgw { namespace d4n {

class CachePolicy {
  private:
    cpp_redis::client client;
    Address addr;

  public:
    CephContext* cct;

    CachePolicy() : addr() {}
    virtual ~CachePolicy() = default;

    virtual void init(CephContext *_cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }
    virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) = 0;
    virtual int exist_key(std::string key) = 0;
    virtual Address get_addr() { return addr; }
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) = 0;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, rgw::cache::CacheDriver* cacheNode) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    cpp_redis::client client;

  public:
    LFUDAPolicy() : CachePolicy() {}

    int set_age(int age);
    int get_age();
    int set_global_weight(std::string key, int weight);
    int get_global_weight(std::string key);
    int set_min_avg_weight(int weight, std::string cacheLocation);
    int get_min_avg_weight();
    CacheBlock find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode);

    virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) override { return CachePolicy::find_client(dpp, client); }
    virtual int exist_key(std::string key) override { return CachePolicy::exist_key(key); }
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, rgw::cache::CacheDriver* cacheNode) override {}
};

class LRUPolicy : public CachePolicy {
  public:
  struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      Entry(std::string& key, uint64_t offset, uint64_t len) : key(key), offset(offset), len(len) {}
  };
  LRUPolicy() = default;
  private:
    std::mutex lru_lock;
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };
    typedef boost::intrusive::list<Entry> List;
    List entries_lru_list;
    std::unordered_map<std::string, Entry*> entries_map;
  public:
    virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) override { return 0; };
    virtual int exist_key(std::string key) override;
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, rgw::cache::CacheDriver* cacheNode) override;
    bool erase(const DoutPrefixProvider* dpp, const std::string& key);
};

class PolicyDriver {
  private:
    std::string policyName;

  public:
    CachePolicy* cachePolicy;

    PolicyDriver(std::string _policyName) : policyName(_policyName) {}
    ~PolicyDriver() {
      delete cachePolicy;
    }

    int init();
};

} } // namespace rgw::d4n
