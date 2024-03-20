#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw::sal {
  class D4NFilterObject;
}

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty) : key(key), offset(offset), 
                                                                                     len(len), version(version), dirty(dirty) {}
    };

   
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };

    struct ObjEntry {
      std::string key;
      std::string version;
      bool dirty;
      uint64_t size;
      time_t creationTime;
      rgw_user user;
      std::string etag;
      std::string bucket_name;
      rgw_obj_key obj_key;
      ObjEntry() = default;
      ObjEntry(std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key) : key(key), version(version), dirty(dirty), size(size), creationTime(creationTime), user(user), etag(etag), bucket_name(bucket_name), obj_key(obj_key) {}
    };

  public:
    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) = 0; 
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual void cleaning(const DoutPrefixProvider* dpp) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using localWeight and dirty flag so that dirty blocks are at the bottom
        if ((e1->dirty && e2->dirty) || (!e1->dirty && !e2->dirty)) {
	        return e1->localWeight > e2->localWeight;
        } else if (e1->dirty && !e2->dirty){
          return true;
        } else if (!e1->dirty && e2->dirty) {
          return false;
        } else {
          return e1->localWeight > e2->localWeight;
        }
      }
    }; 

    template<typename T>
    struct ObjectComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using creationTime
        return e1->creationTime > e2->creationTime;
      }
    };

    struct LFUDAEntry : public Entry {
      int localWeight;
      using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
      handle_type handle;

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, int localWeight) : Entry(key, offset, len, version, dirty), localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; }
    };

    struct LFUDAObjEntry : public ObjEntry {
      using handle_type = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>::handle_type;
      handle_type handle;

      LFUDAObjEntry(std::string& key, std::string& version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key) : ObjEntry(key, version, dirty, size, creationTime, user, etag, bucket_name, obj_key) {}

      void set_handle(handle_type handle_) { handle = handle_; }
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    using Object_Heap = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>;
    Heap entries_heap;
    Object_Heap object_heap; //This heap contains dirty objects ordered by their creation time, used for cleaning method
    std::unordered_map<std::string, LFUDAEntry*> entries_map;
    std::unordered_map<std::string, LFUDAObjEntry*> o_entries_map; //Contains only dirty objects, used for look-up
    std::mutex lfuda_lock;
    std::mutex lfuda_cleaning_lock;
    std::condition_variable cond;
    bool quit{false};

    int age = 1, weightSum = 0, postedSum = 0;
    optional_yield y = null_yield;
    std::shared_ptr<connection> conn;
    BlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;
    std::optional<asio::steady_timer> rthread_timer;
    rgw::sal::Driver *driver;
    std::thread tc;
    CephContext* cct;

    CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y);
    int age_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    asio::awaitable<void> redis_sync(const DoutPrefixProvider* dpp, optional_yield y);
    void rthread_stop() {
      std::lock_guard l{lfuda_lock};

      if (rthread_timer) {
	rthread_timer->cancel();
      }
    }
    LFUDAEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }

  public:
    LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
											   conn(conn), 
											   cacheDriver(cacheDriver)
    {
      dir = new BlockDirectory{conn};
    }
    ~LFUDAPolicy() {
      rthread_stop();
      delete dir;
      std::lock_guard l(lfuda_cleaning_lock);
      quit = true;
      cond.notify_all();
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver);
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    void save_y(optional_yield y) { this->y = y; }
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
    LFUDAObjEntry* find_obj_entry(std::string key) {
      auto it = o_entries_map.find(key);
      if (it == o_entries_map.end()) {
        return nullptr;
      }
      return it->second;
    }
};

class LRUPolicy : public CachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> entries_map;
    std::unordered_map<std::string, ObjEntry*> o_entries_map;
    std::mutex lru_lock;
    List entries_lru_list;
    rgw::cache::CacheDriver* cacheDriver;

    bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);

  public:
    LRUPolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver{cacheDriver} {}

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) { return 0; } 
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(conn, cacheDriver);
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
