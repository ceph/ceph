// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_INLINES_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_INLINES_H

#include "bluestore_types.h"
#include "BlueStore.h"
#include "BlueStore_objects.h"
#include "BlueStore_objects_impl.h"

inline BlueStore::SharedBlobRef BlueStore::SharedBlobSet::lookup(uint64_t sbid) {
  std::lock_guard l(lock);
  auto p = sb_map.find(sbid);
  if (p == sb_map.end() || p->second->nref == 0) {
    return nullptr;
  }
  return p->second;
}

inline void BlueStore::SharedBlobSet::add(Collection* coll, SharedBlob *sb) {
  std::lock_guard l(lock);
  sb_map[sb->get_sbid()] = sb;
  sb->collection = coll;
}

inline bool BlueStore::SharedBlobSet::remove(SharedBlob *sb, bool verify_nref_is_zero) {
  std::lock_guard l(lock);
  ceph_assert(sb->get_parent() == this);
  if (verify_nref_is_zero && sb->nref != 0) {
    return false;
  }
  // only remove if it still points to us
  auto p = sb_map.find(sb->get_sbid());
  if (p != sb_map.end() &&
       p->second == sb) {
    sb_map.erase(p);
  }
  return true;
}

inline BlueStore::BlobRef BlueStore::Collection::new_blob() {
  BlobRef b = new Blob(this);
  b->get_cache()->add_blob();
  return b;
}

inline bluestore::Extent::~Extent() {
  if (blob) {
    blob->get_cache()->rm_extent();
  }
}

inline uint32_t bluestore::Extent::blob_end() const {
  return blob_start() + blob->get_blob().get_logical_length();
}

inline void bluestore::Extent::assign_blob(const BlueStore::BlobRef& b) {
  ceph_assert(!blob);
  blob = b;
  blob->get_cache()->add_extent();
}

inline void BlueStore::_buffer_cache_write(
  TransContext *txc,
  OnodeRef onode,
  uint32_t offset,
  ceph::buffer::list&& bl,
  unsigned flags) {
  onode->bc.write(onode->c->cache,
                  txc, offset, std::move(bl), flags);
}

inline void BlueStore::_buffer_cache_write(
  TransContext *txc,
  OnodeRef onode,
  uint32_t offset,
  ceph::buffer::list& bl,
  unsigned flags) {
  onode->bc.write(onode->c->cache,
                  txc, offset, bl, flags);
}

inline void BlueStore::debug_punch_hole(
  CollectionRef& c,
  OnodeRef& o,
  uint32_t off,
  uint32_t len) {
  BlueStore::TransContext txc(cct, c.get(), nullptr, nullptr);
  BlueStore::WriteContext wctx;
  o->extent_map.punch_hole(c, off, len, &wctx.old_extents);
  _wctx_finish(&txc, c, o, &wctx, nullptr);
}

#endif