#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

static std::string empty = std::string();

enum RefCount {
  NOOP = 0,
  INCR = 1,
  DECR = 2,
};

enum class State { // state machine for dirty objects in the cache
  INIT,
  IN_PROGRESS, // object is being written to the backend
  INVALID // object is to be deleted during cleanup 
};

class CachePolicy {
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      uint64_t refcount{0};
      Entry(const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint64_t refcount) : key(key), offset(offset), 
											        len(len), version(version), dirty(dirty), refcount(refcount) {}
      };
   
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry* e) {
        delete e;
      }
    };

    struct ObjEntry {
      std::string key;
      std::string version;
      bool delete_marker;
      uint64_t size;
      double creationTime;
      rgw_user user;
      std::string etag;
      std::string bucket_name;
      std::string bucket_id;
      rgw_obj_key obj_key;
      ObjEntry() = default;
      ObjEntry(const std::string& key, const std::string& version, bool delete_marker, uint64_t size,
		double creationTime, const rgw_user& user, const std::string& etag,
		const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key) : key(key), version(version), delete_marker(delete_marker), size(size),
									      creationTime(creationTime), user(user), etag(etag), 
									      bucket_name(bucket_name), bucket_id(bucket_id), obj_key(obj_key) {}
    };

  public:
    CachePolicy() {}
    virtual ~CachePolicy() = default; 

    virtual int init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) = 0;
    virtual int exist_key(const std::string& key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual bool update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint8_t op, optional_yield y, std::string& restore_val=empty) = 0;
    virtual void update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val=empty) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) = 0;
    virtual void cleaning(const DoutPrefixProvider* dpp) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using localWeight, refcount and dirty flag so that dirty blocks and blocks with positive refcount are at the bottom
        if ((e1->dirty && e2->dirty) || (!e1->dirty && !e2->dirty) || (e1->refcount > 0 && e2->refcount > 0)) {
	        return e1->localWeight > e2->localWeight;
        } else if (e1->dirty && !e2->dirty){
          return true;
        } else if (!e1->dirty && e2->dirty) {
          return false;
        } else if (e1->refcount > 0 && e2->refcount == 0) {
          return true;
        } else if (e1->refcount == 0 && e2->refcount > 0) {
          return false;
        }else {
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

      LFUDAEntry(const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint64_t refcount, int localWeight) : Entry(key, offset, len, version, dirty, refcount), 
														       localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; }
    };

    struct LFUDAObjEntry : public ObjEntry {
      using handle_type = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>::handle_type;
      handle_type handle;

      LFUDAObjEntry(const std::string& key, const std::string& version, bool deleteMarker, uint64_t size,
                     double creationTime, const rgw_user& user, const std::string& etag,
                     const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key) : ObjEntry(key, version, deleteMarker, size,
									    creationTime, user, etag, bucket_name, bucket_id, obj_key) {}

      void set_handle(handle_type handle_) { handle = handle_; }
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    using Object_Heap = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>;
    Heap entries_heap;
    Object_Heap object_heap; //This heap contains dirty objects ordered by their creation time, used for cleaning method
    std::unordered_map<std::string, LFUDAEntry*> entries_map;
    std::unordered_map<std::string, std::pair<LFUDAObjEntry*, State> > o_entries_map; //Contains only dirty objects, used for look-up
    std::mutex lfuda_lock;
    std::mutex lfuda_cleaning_lock;
    std::condition_variable cond;
    std::condition_variable state_cond;
    inline static std::atomic<bool> quit{false};

    int age = 1, weightSum = 0, postedSum = 0;
    optional_yield y = null_yield;
    std::shared_ptr<connection> conn;
    BlockDirectory* blockDir;
    ObjectDirectory* objDir;
    BucketDirectory* bucketDir;
    rgw::cache::CacheDriver* cacheDriver;
    std::optional<asio::steady_timer> rthread_timer;
    rgw::sal::Driver* driver;
    std::thread tc;

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
    LFUDAEntry* find_entry(const std::string& key) {
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }
    int delete_data_blocks(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y);

  public:
    LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
											   conn(conn), 
											   cacheDriver(cacheDriver)
    {
      blockDir = new BlockDirectory{conn};
      objDir = new ObjectDirectory{conn};
      bucketDir = new BucketDirectory{conn};
    }
    ~LFUDAPolicy() {
      rthread_stop();
      delete bucketDir;
      delete blockDir;
      delete objDir;
      quit = true;
      cond.notify_all();
      if (tc.joinable()) { tc.join(); }
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver);
    virtual int exist_key(const std::string& key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual bool update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);
    void save_y(optional_yield y) { this->y = y; }
    virtual void update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual bool erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
    LFUDAObjEntry* find_obj_entry(const std::string& key) {
      auto it = o_entries_map.find(key);
      if (it == o_entries_map.end()) {
        return nullptr;
      }
      return it->second.first;
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

    virtual int init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) { return 0; }
    virtual int exist_key(const std::string& key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual bool update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y) override { return false; }
    virtual void update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual void update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
    			    const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) override { return false; }
    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver, const std::string& _policyName) : policyName(_policyName) 
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
