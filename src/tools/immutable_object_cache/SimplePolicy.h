// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SIMPLE_POLICY_H
#define CEPH_CACHE_SIMPLE_POLICY_H

#include "common/ceph_context.h"
#include "common/RWLock.h"
#include "common/Mutex.h"
#include "include/lru.h"
#include "Policy.h"

#include <unordered_map>
#include <string>

namespace ceph {
namespace immutable_obj_cache {

class SimplePolicy : public Policy {
 public:
  SimplePolicy(CephContext *cct, uint64_t block_num, uint64_t max_inflight,
               double watermark);
  ~SimplePolicy();

  cache_status_t lookup_object(std::string file_name);
  cache_status_t get_status(std::string file_name);

  void update_status(std::string file_name,
                     cache_status_t new_status,
                     uint64_t size = 0);

  int evict_entry(std::string file_name);

  void get_evict_list(std::list<std::string>* obj_list);

  uint64_t get_free_size();
  uint64_t get_promoting_entry_num();
  uint64_t get_promoted_entry_num();
  std::string get_evict_entry();

 private:
  cache_status_t alloc_entry(std::string file_name);

  class Entry : public LRUObject {
   public:
    cache_status_t status;
    Entry() : status(OBJ_CACHE_NONE) {}
    std::string file_name;
    uint64_t size;
  };

  CephContext* cct;
  double m_watermark;
  uint64_t m_max_inflight_ops;
  uint64_t m_max_cache_size;
  std::atomic<uint64_t> inflight_ops = 0;

  std::unordered_map<std::string, Entry*> m_cache_map;
  RWLock m_cache_map_lock;

  std::atomic<uint64_t> m_cache_size;

  LRU m_promoted_lru;
};

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif  // CEPH_CACHE_SIMPLE_POLICY_H
