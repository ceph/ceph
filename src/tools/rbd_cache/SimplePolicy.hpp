#ifndef RBD_CACHE_SIMPLE_POLICY_HPP
#define RBD_CACHE_SIMPLE_POLICY_HPP

#include "Policy.hpp"
#include "include/lru.h"
#include "common/RWLock.h"
#include "common/Mutex.h"

#include <vector>
#include <unordered_map>
#include <string>

namespace rbd {
namespace cache {


class SimplePolicy : public Policy {
public:
  SimplePolicy(uint64_t block_num, float watermark)
    : m_watermark(watermark), m_entry_count(block_num),
      m_cache_map_lock("rbd::cache::SimplePolicy::m_cache_map_lock"),
      m_free_list_lock("rbd::cache::SimplePolicy::m_free_list_lock")
  {

    for(uint64_t i = 0; i < m_entry_count; i++) {
      m_free_list.push_back(new Entry());
    }

  }

  ~SimplePolicy() {
    for(uint64_t i = 0; i < m_entry_count; i++) {
      Entry* entry = reinterpret_cast<Entry*>(m_free_list.front());
      delete entry;
      m_free_list.pop_front();
    }
  }

  CACHESTATUS lookup_object(std::string cache_file_name) {

    //TODO(): check race condition
    RWLock::WLocker wlocker(m_cache_map_lock);

    auto entry_it = m_cache_map.find(cache_file_name);
    if(entry_it == m_cache_map.end()) {
      Mutex::Locker locker(m_free_list_lock);
      Entry* entry = reinterpret_cast<Entry*>(m_free_list.front());
      assert(entry != nullptr);
      m_free_list.pop_front();
      entry->status = OBJ_CACHE_PROMOTING;

      m_cache_map[cache_file_name] = entry;

      return OBJ_CACHE_NONE;
    }

    Entry* entry = entry_it->second;

    if(entry->status == OBJ_CACHE_PROMOTED) {
      // touch it
      m_promoted_lru.lru_touch(entry);
    }

    return entry->status;
  }

  int evict_object(std::string& out_cache_file_name) {
    RWLock::WLocker locker(m_cache_map_lock);

    return 1;
  }

  // TODO(): simplify the logic
  void update_status(std::string file_name, CACHESTATUS new_status) {
    RWLock::WLocker locker(m_cache_map_lock);

    Entry* entry;
    auto entry_it = m_cache_map.find(file_name);

    // just check.
    if(new_status == OBJ_CACHE_PROMOTING) {
      assert(entry_it == m_cache_map.end());
    }

    assert(entry_it != m_cache_map.end());

    entry = entry_it->second;

    // promoting is done, so update it.
    if(entry->status == OBJ_CACHE_PROMOTING && new_status== OBJ_CACHE_PROMOTED) {
      m_promoted_lru.lru_insert_top(entry);
      entry->status = new_status;
      return;
    }

    assert(0);
  }

  // get entry status
  CACHESTATUS get_status(std::string file_name) {
    RWLock::RLocker locker(m_cache_map_lock);
    auto entry_it = m_cache_map.find(file_name);
    if(entry_it == m_cache_map.end()) {
      return OBJ_CACHE_NONE;
    }

    return entry_it->second->status;
  }

  void get_evict_list(std::list<std::string>* obj_list) {
    RWLock::WLocker locker(m_cache_map_lock);
    // check free ratio, pop entries from LRU
    if (m_free_list.size() / m_entry_count < m_watermark) {
      int evict_num = 10; //TODO(): make this configurable
      for(int i = 0; i < evict_num; i++) {
        Entry* entry = reinterpret_cast<Entry*>(m_promoted_lru.lru_expire());
        if (entry == nullptr) {
	  continue;
        }
        std::string file_name = entry->cache_file_name;
        obj_list->push_back(file_name);

        auto entry_it = m_cache_map.find(file_name);
        m_cache_map.erase(entry_it);

        //mark this entry as free
	entry->status = OBJ_CACHE_NONE;
        Mutex::Locker locker(m_free_list_lock);
        m_free_list.push_back(entry);
      }
   }
  }

private:

  class Entry : public LRUObject {
    public:
      CACHESTATUS status;
      Entry() : status(OBJ_CACHE_NONE){}
      std::string cache_file_name;
      void encode(bufferlist &bl){}
      void decode(bufferlist::iterator &it){}
  };

  float m_watermark;
  uint64_t m_entry_count;

  std::unordered_map<std::string, Entry*> m_cache_map;
  RWLock m_cache_map_lock;

  std::deque<Entry*> m_free_list;
  Mutex m_free_list_lock;

  LRU m_promoted_lru; // include promoted, using status.

};

} // namespace cache
} // namespace rbd
#endif
