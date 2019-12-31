// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "SimpleLock.h"
#include "Mutation.h"

void SimpleLock::dump(Formatter *f) const {
  ceph_assert(f != NULL);
  if (is_sync_and_unlocked()) {
    return;
  }

  f->open_array_section("gather_set");
  if (have_more()) {
    for(const auto &i : more()->gather_set) {
      f->dump_int("rank", i);
    }
  }
  f->close_section();

  f->dump_string("state", get_state_name(get_state()));
  f->dump_bool("is_leased", is_leased());
  f->dump_int("num_rdlocks", get_num_rdlocks());
  f->dump_int("num_wrlocks", get_num_wrlocks());
  f->dump_int("num_xlocks", get_num_xlocks());
  f->open_object_section("xlock_by");
  if (get_xlock_by()) {
    get_xlock_by()->dump(f);
  }
  f->close_section();
}

SimpleLock::unstable_bits_t::unstable_bits_t() :
  lock_caches(member_offset(MDLockCache::LockItem, item_lock)) {}

void SimpleLock::add_cache(MDLockCacheItem& item)
{
  more()->lock_caches.push_back(&item.item_lock);
  state_flags |= CACHED;
}

void SimpleLock::remove_cache(MDLockCacheItem& item) {
  auto& lock_caches = more()->lock_caches;
  item.item_lock.remove_myself();
  if (lock_caches.empty()) {
    state_flags &= ~CACHED;
    try_clear_more();
  }
}

MDLockCache* SimpleLock::get_first_cache()
{
  if (have_more()) {
    auto& lock_caches = more()->lock_caches;
    if (!lock_caches.empty()) {
      return lock_caches.front()->parent;
    }
  }
  return nullptr;
}
