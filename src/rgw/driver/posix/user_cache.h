// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_sal_fwd.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

struct UserCacheEntry {
  RGWUserInfo info;
  Attrs attrs;
  RGWObjVersionTracker objv_tracker;
};

class UserCache {
  struct LRUEntry {
    std::string uid;
    UserCacheEntry entry;
  };

  using lru_list_t = std::list<LRUEntry>;
  using lru_iter_t = lru_list_t::iterator;

  std::recursive_mutex mtx;
  lru_list_t lru;
  std::unordered_map<std::string, lru_iter_t> uid_to_lru_it; // user id -> LRU iterator
  std::unordered_map<std::string, std::string> ak_to_uid; // access key -> user id
  uint64_t lru_max_size;

  
  void lru_insert(const DoutPrefixProvider* dpp, const std::string& uid, const UserCacheEntry& entry) {
    std::lock_guard l{mtx};
    lru.emplace_front(LRUEntry{uid, entry});
    uid_to_lru_it[uid] = lru.begin();
    ldpp_dout(dpp, 30) << "UserCache: inserting user into LRU cache uid=" << uid << ", LRU size=" << lru.size()
                       << ", user id to LRU iterator map size=" << uid_to_lru_it.size() << dendl;
  }

  void lru_evict(const DoutPrefixProvider* dpp) {
    std::lock_guard l{mtx};
    while (uid_to_lru_it.size() > lru_max_size) {
      auto& evicted = lru.back();
      // erase all access key mappings for evicted user
      for (const auto& [ak, _] : evicted.entry.info.access_keys) {
        ak_to_uid.erase(ak);
      }
      ldpp_dout(dpp, 30) << "UserCache: evicting user from LRU cache uid=" << evicted.uid << ", LRU size=" << lru.size()
                         << ", access key to user id map size=" << ak_to_uid.size()
                         << ", user id to LRU iterator map size=" << uid_to_lru_it.size() << dendl;
      uid_to_lru_it.erase(evicted.uid);
      lru.pop_back();
    }
  }

  void lru_touch(const DoutPrefixProvider* dpp, lru_iter_t it) {
    std::lock_guard l{mtx};
    lru.splice(lru.begin(), lru, it);
    ldpp_dout(dpp, 30) << "UserCache: touched user in LRU cache uid=" << it->uid << ", LRU size=" << lru.size() << dendl;
  }


public:
  UserCache(uint64_t max_entries = 1000) : lru_max_size(max_entries) {}

  void set_max_size(const DoutPrefixProvider* dpp, uint64_t max_entries) {
    ldpp_dout(dpp, 30) << "UserCache: setting LRU max size to " << max_entries << dendl;
    std::lock_guard l{mtx};
    lru_max_size = max_entries;
    lru_evict(dpp);
  }
  
  void insert_user(const DoutPrefixProvider* dpp, const UserCacheEntry& entry) {
    std::lock_guard l{mtx};
    const auto& uid = entry.info.user_id.id;
    
    // if user already exists, replace entry and promote to front of LRU otherwise insert new entry into front of LRU
    auto it = uid_to_lru_it.find(uid);
    if (it != uid_to_lru_it.end()) {
      // update existing: replace entry and promote
      it->second->entry = entry;
      lru_touch(dpp, it->second);
    } else {
      // insert new
      lru_insert(dpp, uid, entry);
    }
    ldpp_dout(dpp, 30) << "UserCache: caching user uid=" << entry.info.user_id.id << ", LRU size=" << lru.size() << dendl;

    // update access key -> user id mappings for this user
    for (const auto& [ak, _] : entry.info.access_keys) {
      ak_to_uid[ak] = uid;
    }

    lru_evict(dpp);
  }

  bool lookup_user_by_uid(const DoutPrefixProvider* dpp,
                          const std::string& user_id,
                          UserCacheEntry& out) {
    std::lock_guard l{mtx};
    auto uid_it = uid_to_lru_it.find(user_id);
    if (uid_it == uid_to_lru_it.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by uid=" << user_id << " : not found" << dendl;
      return false;
    }
    lru_touch(dpp, uid_it->second);
    out = uid_it->second->entry;
    ldpp_dout(dpp, 30) << "UserCache: lookup cached user by uid=" << user_id << " : found" << dendl;
    return true;
  }

  bool lookup_user_by_access_key(const DoutPrefixProvider* dpp,
                                 const std::string& key,
                                 UserCacheEntry& out) {
    std::lock_guard l{mtx};
    auto ak_it = ak_to_uid.find(key);
    if (ak_it == ak_to_uid.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << " : not found" << dendl;
      return false;
    }
    auto uid_it = uid_to_lru_it.find(ak_it->second);
    if (uid_it == uid_to_lru_it.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << " uid=" << ak_it->second << " : not found" << dendl;
      return false;
    }
    lru_touch(dpp, uid_it->second);
    out = uid_it->second->entry;
    ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << " uid=" << ak_it->second << " : found" << dendl;
    return true;
  }

  void invalidate_user(const DoutPrefixProvider* dpp, const std::string& user_id) {
    ldpp_dout(dpp, 30) << "UserCache: invalidating cached user uid=" << user_id << dendl;
    std::lock_guard l{mtx};
    auto it = uid_to_lru_it.find(user_id);
    // if user exists, remove from cache and erase all access key mappings for that user
    if (it != uid_to_lru_it.end()) {
      for (const auto& [ak, _] : it->second->entry.info.access_keys) {
        ak_to_uid.erase(ak);
      }
      ldpp_dout(dpp, 30) << "UserCache: evicting user from LRU cache uid=" << it->second->entry.info.user_id.id << ", LRU size=" << lru.size()
                         << ", access key to user id map size=" << ak_to_uid.size() << ", user id to LRU iterator map size=" << uid_to_lru_it.size() << dendl;
      lru.erase(it->second);
      uid_to_lru_it.erase(it);
    }
  }
};

} // namespace rgw::sal

#undef dout_subsys
