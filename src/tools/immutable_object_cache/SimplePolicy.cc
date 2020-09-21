// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "SimplePolicy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::SimplePolicy: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

SimplePolicy::SimplePolicy(CephContext *cct, uint64_t cache_size,
                           uint64_t max_inflight, double watermark)
  : cct(cct), m_watermark(watermark), m_max_inflight_ops(max_inflight),
    m_max_cache_size(cache_size) {

  ldout(cct, 20) << "max cache size= " << m_max_cache_size
                 << " ,watermark= " << m_watermark
                 << " ,max inflight ops= " << m_max_inflight_ops << dendl;

  m_cache_size = 0;

}

SimplePolicy::~SimplePolicy() {
  ldout(cct, 20) << dendl;

  for (auto it : m_cache_map) {
    Entry* entry = (it.second);
    delete entry;
  }
}

cache_status_t SimplePolicy::alloc_entry(std::string file_name) {
  ldout(cct, 20) << "alloc entry for: " << file_name << dendl;

  std::unique_lock wlocker{m_cache_map_lock};

  // cache hit when promoting
  if (m_cache_map.find(file_name) != m_cache_map.end()) {
    ldout(cct, 20) << "object is under promoting: " << file_name << dendl;
    return OBJ_CACHE_SKIP;
  }

  if ((m_cache_size < m_max_cache_size) &&
      (inflight_ops < m_max_inflight_ops)) {
    Entry* entry = new Entry();
    ceph_assert(entry != nullptr);
    m_cache_map[file_name] = entry;
    wlocker.unlock();
    update_status(file_name, OBJ_CACHE_SKIP);
    return OBJ_CACHE_NONE;  // start promotion request
  }

  // if there's no free entry, return skip to read from rados
  return OBJ_CACHE_SKIP;
}

cache_status_t SimplePolicy::lookup_object(std::string file_name) {
  ldout(cct, 20) << "lookup: " << file_name << dendl;

  std::shared_lock rlocker{m_cache_map_lock};

  auto entry_it = m_cache_map.find(file_name);
  // simply promote on first lookup
  if (entry_it == m_cache_map.end()) {
      rlocker.unlock();
      return alloc_entry(file_name);
  }

  Entry* entry = entry_it->second;

  if (entry->status == OBJ_CACHE_PROMOTED || entry->status == OBJ_CACHE_DNE) {
    // bump pos in lru on hit
    m_promoted_lru.lru_touch(entry);
  }

  return entry->status;
}

void SimplePolicy::update_status(std::string file_name,
                                 cache_status_t new_status, uint64_t size) {
  ldout(cct, 20) << "update status for: " << file_name
                 << " new status = " << new_status << dendl;

  std::unique_lock locker{m_cache_map_lock};

  auto entry_it = m_cache_map.find(file_name);
  if (entry_it == m_cache_map.end()) {
    return;
  }

  ceph_assert(entry_it != m_cache_map.end());
  Entry* entry = entry_it->second;

  // to promote
  if (entry->status == OBJ_CACHE_NONE && new_status== OBJ_CACHE_SKIP) {
    entry->status = new_status;
    entry->file_name = file_name;
    inflight_ops++;
    return;
  }

  // promoting done
  if (entry->status == OBJ_CACHE_SKIP && (new_status== OBJ_CACHE_PROMOTED ||
                                          new_status== OBJ_CACHE_DNE)) {
    m_promoted_lru.lru_insert_top(entry);
    entry->status = new_status;
    entry->size = size;
    m_cache_size += entry->size;
    inflight_ops--;
    return;
  }

  // promoting failed
  if (entry->status == OBJ_CACHE_SKIP && new_status== OBJ_CACHE_NONE) {
    // mark this entry as free
    entry->file_name = "";
    entry->status = new_status;

    m_cache_map.erase(entry_it);
    inflight_ops--;
    delete entry;
    return;
  }

  // to evict
  if ((entry->status == OBJ_CACHE_PROMOTED || entry->status == OBJ_CACHE_DNE) &&
      new_status== OBJ_CACHE_NONE) {
    // mark this entry as free
    uint64_t size = entry->size;
    entry->file_name = "";
    entry->size = 0;
    entry->status = new_status;

    m_promoted_lru.lru_remove(entry);
    m_cache_map.erase(entry_it);
    m_cache_size -= size;
    delete entry;
    return;
  }
}

int SimplePolicy::evict_entry(std::string file_name) {
  ldout(cct, 20) << "to evict: " << file_name << dendl;

  update_status(file_name, OBJ_CACHE_NONE);

  return 0;
}

cache_status_t SimplePolicy::get_status(std::string file_name) {
  ldout(cct, 20) << file_name << dendl;

  std::shared_lock locker{m_cache_map_lock};
  auto entry_it = m_cache_map.find(file_name);
  if (entry_it == m_cache_map.end()) {
    return OBJ_CACHE_NONE;
  }

  return entry_it->second->status;
}

void SimplePolicy::get_evict_list(std::list<std::string>* obj_list) {
  ldout(cct, 20) << dendl;

  std::unique_lock locker{m_cache_map_lock};
  // check free ratio, pop entries from LRU
  if ((double)m_cache_size / m_max_cache_size > (1 - m_watermark)) {
    // TODO(dehao): make this configurable
    int evict_num = m_cache_map.size() * 0.1;
    for (int i = 0; i < evict_num; i++) {
      Entry* entry = reinterpret_cast<Entry*>(m_promoted_lru.lru_expire());
      if (entry == nullptr) {
        continue;
      }
      std::string file_name = entry->file_name;
      obj_list->push_back(file_name);
    }
  }
}

// for unit test
uint64_t SimplePolicy::get_free_size() {
  return m_max_cache_size - m_cache_size;
}

uint64_t SimplePolicy::get_promoting_entry_num() {
  uint64_t index = 0;
  std::shared_lock rlocker{m_cache_map_lock};
  for (auto it : m_cache_map) {
    if (it.second->status == OBJ_CACHE_SKIP) {
      index++;
    }
  }
  return index;
}

uint64_t SimplePolicy::get_promoted_entry_num() {
  return m_promoted_lru.lru_get_size();
}

std::string SimplePolicy::get_evict_entry() {
  Entry* entry = reinterpret_cast<Entry*>(m_promoted_lru.lru_get_next_expire());
  if (entry == nullptr) {
    return "";
  }
  return entry->file_name;
}

}  // namespace immutable_obj_cache
}  // namespace ceph
