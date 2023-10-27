#pragma once

#include <boost/heap/fibonacci_heap.hpp>
#include "rgw_common.h"
#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw::sal {
  class D4NFilterObject;
}

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
    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) { return 0; } 
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) = 0;
    virtual void shutdown() = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
	return e1->localWeight > e2->localWeight;
      }
    }; 

    struct LFUDAEntry : public Entry {
      int localWeight;
      using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
      handle_type handle;

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, int localWeight) : Entry(key, offset, len, version),
													    localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; } 
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> entries_map;

    net::io_context& io;
    std::shared_ptr<connection> conn;
    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;

    int set_age(int age, optional_yield y);
    int get_age(optional_yield y);
    int set_min_avg_weight(size_t weight, std::string cacheLocation, optional_yield y);
    int get_min_avg_weight(optional_yield y);
    CacheBlock find_victim(const DoutPrefixProvider* dpp, optional_yield y);

  public:
    LFUDAPolicy(net::io_context& io_context, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), io(io_context), cacheDriver{cacheDriver} {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
      dir = new BlockDirectory{io};
    }
    ~LFUDAPolicy() {
      shutdown();
      delete dir;
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) {
      std::string address = cct->_conf->rgw_local_cache_address;

      config cfg;
      cfg.addr.host = address.substr(0, address.find(":"));
      cfg.addr.port = address.substr(address.find(":") + 1, address.length());
      cfg.clientname = "D4N.Policy";

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      dir->init(cct, dpp);
      conn->async_run(cfg, {}, net::consign(net::detached, conn));

      return 0;
    }
    virtual int exist_key(std::string key) override;
    //virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void shutdown() override;

    void set_local_weight(std::string& key, int localWeight);
    LFUDAEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }
};

class LRUPolicy : public CachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> entries_map;
    std::mutex lru_lock;
    List entries_lru_list;
    rgw::cache::CacheDriver* cacheDriver;

  public:
    LRUPolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver{cacheDriver} {}

    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void shutdown() override {}
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(net::io_context& io_context, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(io_context, cacheDriver);
      } else if (policyName == "lru") {
	cachePolicy = new LRUPolicy(cacheDriver);
      }
    }
    ~PolicyDriver() {
      delete cachePolicy;
    }

    CachePolicy* get_cache_policy() { return cachePolicy; }
    std::string get_policy_name() { return policyName; }
};

} } // namespace rgw::d4n
