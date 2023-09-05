#pragma once

#include <string>
#include <iostream>
#include "rgw_common.h"
#include "d4n_directory.h"
#include "../../rgw_redis_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version) : key(key), offset(offset), 
                                                                                     len(len), version(version) {}
    };
    
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };

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
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) = 0;
    virtual void shutdown() = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    struct LFUDAEntry : public Entry {
      int localWeight;
      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string version, int localWeight) : Entry(key, offset, len, version), 
													  localWeight(localWeight) {}
    };

    struct LFUDA_Entry_delete_disposer : public Entry_delete_disposer {
      void operator()(LFUDAEntry *e) {
        delete e;
      }
    };
    typedef boost::intrusive::list<LFUDAEntry> List;

    std::unordered_map<std::string, LFUDAEntry*> entries_map;

    net::io_context& io;
    std::shared_ptr<connection> conn;
    List entries_lfuda_list;
    BlockDirectory* dir;

    int set_age(int age, optional_yield y);
    int get_age(optional_yield y);
    int set_min_avg_weight(size_t weight, std::string cacheLocation, optional_yield y);
    int get_min_avg_weight(optional_yield y);
    CacheBlock find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y);

  public:
    LFUDAPolicy(net::io_context& io_context) : CachePolicy(), io(io_context) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
      dir = new BlockDirectory{io};
    }
    ~LFUDAPolicy() {
      //delete dir;
      shutdown();
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host; // TODO: Replace with cache address
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      dir->init(cct, dpp);
      conn->async_run(cfg, {}, net::consign(net::detached, conn));

      return 0;
    }
    virtual int exist_key(std::string key, optional_yield y) override;
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void shutdown() override;
};

class LRUPolicy : public CachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> entries_map;
    std::mutex lru_lock;
    List entries_lru_list;

  public:
    LRUPolicy() = default;

    virtual int exist_key(std::string key, optional_yield y) override;
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void shutdown() override {}
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
