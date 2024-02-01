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

void SimpleLock::dump(ceph::Formatter *f) const {
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
  f->dump_string("type", get_lock_type_name(get_type()));
  f->dump_bool("is_leased", is_leased());
  f->dump_int("num_rdlocks", get_num_rdlocks());
  f->dump_int("num_wrlocks", get_num_wrlocks());
  f->dump_int("num_xlocks", get_num_xlocks());
  f->open_object_section("xlock_by");
  if (auto mut = get_xlock_by(); mut) {
    f->dump_object("reqid", mut->reqid);
  }
  f->close_section();
}

int SimpleLock::get_wait_shift() const {
  switch (get_type()) {
    case CEPH_LOCK_DN:       return 8;
    case CEPH_LOCK_DVERSION: return 8 + 1*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IAUTH:    return 8 + 2*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ILINK:    return 8 + 3*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IDFT:     return 8 + 4*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IFILE:    return 8 + 5*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IVERSION: return 8 + 6*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IXATTR:   return 8 + 7*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ISNAP:    return 8 + 8*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_INEST:    return 8 + 9*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IFLOCK:   return 8 +10*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IPOLICY:  return 8 +11*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IQUIESCE: return 8 +12*SimpleLock::WAIT_BITS;
    default:
      ceph_abort();
  }
}

int SimpleLock::get_cap_shift() const {
  switch (get_type()) {
    case CEPH_LOCK_IAUTH: return CEPH_CAP_SAUTH;
    case CEPH_LOCK_ILINK: return CEPH_CAP_SLINK;
    case CEPH_LOCK_IFILE: return CEPH_CAP_SFILE;
    case CEPH_LOCK_IXATTR: return CEPH_CAP_SXATTR;
    default: return 0;
  }
}

int SimpleLock::get_cap_mask() const {
  switch (get_type()) {
    case CEPH_LOCK_IFILE: return (1 << CEPH_CAP_FILE_BITS) - 1;
    default: return (1 << CEPH_CAP_SIMPLE_BITS) - 1;
  }
}

SimpleLock::unstable_bits_t::unstable_bits_t() :
  lock_caches(member_offset(MDLockCache::LockItem, item_lock)) {}

void SimpleLock::add_cache(MDLockCacheItem& item) {
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

std::vector<MDLockCache*> SimpleLock::get_active_caches() {
  std::vector<MDLockCache*> result;
  if (have_more()) {
    for (auto it = more()->lock_caches.begin_use_current(); !it.end(); ++it) {
      auto lock_cache = (*it)->parent;
      if (!lock_cache->invalidating)
	result.push_back(lock_cache);
    }
  }
  return result;
}

void SimpleLock::_print(std::ostream& out) const
{
  out << get_lock_type_name(get_type()) << " ";
  out << get_state_name(get_state());
  if (!get_gather_set().empty())
    out << " g=" << get_gather_set();
  if (is_leased())
    out << " l";
  if (is_rdlocked())
    out << " r=" << get_num_rdlocks();
  if (is_wrlocked())
    out << " w=" << get_num_wrlocks();
  if (is_xlocked()) {
    out << " x=" << get_num_xlocks();
    if (auto mut = get_xlock_by(); mut) {
      out << " by " << *mut;
    }
  }
#if 0
  if (is_stable())
    out << " stable";
  else
    out << " unstable";
#endif
}
