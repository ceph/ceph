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

#include "BlueStore_objects.h"
#include "bluestore_types.h"
#include <string>
#include "os/kv.h"

inline bluestore::SharedBlobRef bluestore::SharedBlobSet::lookup(uint64_t sbid) {
  std::lock_guard l(lock);
  auto p = sb_map.find(sbid);
  if (p == sb_map.end() || p->second->nref == 0) {
    return nullptr;
  }
  return p->second;
}

inline void bluestore::SharedBlobSet::add(Collection* coll, SharedBlob *sb) {
  std::lock_guard l(lock);
  sb_map[sb->get_sbid()] = sb;
  sb->collection = coll;
}

inline bool bluestore::SharedBlobSet::remove(SharedBlob *sb, bool verify_nref_is_zero) {
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

inline bluestore::BlobRef bluestore::Collection::new_blob() {
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

// volatile_statfs
inline void volatile_statfs::publish(store_statfs_t* buf) const
{
  buf->allocated = allocated();
  buf->data_stored = stored();
  buf->data_compressed = compressed();
  buf->data_compressed_original = compressed_original();
  buf->data_compressed_allocated = compressed_allocated();
}

inline volatile_statfs& volatile_statfs::operator=(const store_statfs_t& st) {
  values[STATFS_ALLOCATED] = st.allocated;
  values[STATFS_STORED] = st.data_stored;
  values[STATFS_COMPRESSED_ORIGINAL] = st.data_compressed_original;
  values[STATFS_COMPRESSED] = st.data_compressed;
  values[STATFS_COMPRESSED_ALLOCATED] = st.data_compressed_allocated;
  return *this;
}

inline std::ostream& operator<<(std::ostream& out, const volatile_statfs& s)
{
  return out
    << " allocated:"
    << s.values[volatile_statfs::STATFS_ALLOCATED]
    << " stored:"
    << s.values[volatile_statfs::STATFS_STORED]
    << " compressed:"
    << s.values[volatile_statfs::STATFS_COMPRESSED]
    << " compressed_orig:"
    << s.values[volatile_statfs::STATFS_COMPRESSED_ORIGINAL]
    << " compressed_alloc:"
    << s.values[volatile_statfs::STATFS_COMPRESSED_ALLOCATED];
}

namespace bluestore {
  static inline void get_shared_blob_key(uint64_t sbid, std::string *key)
  {
    key->clear();
    _key_encode_u64(sbid, key);
  }

  static inline int get_key_shared_blob(const std::string& key, uint64_t *sbid)
  {
    const char *p = key.c_str();
    if (key.length() < sizeof(uint64_t))
      return -1;
    _key_decode_u64(p, sbid);
    return 0;
  }

  static inline int get_key_extent_shard(const std::string& key, std::string *onode_key, uint32_t *offset)
  {
    ceph_assert(key.size() > sizeof(uint32_t) + 1);
    ceph_assert(*key.rbegin() == EXTENT_SHARD_KEY_SUFFIX);
    int okey_len = key.size() - sizeof(uint32_t) - 1;
    *onode_key = key.substr(0, okey_len);
    const char *p = key.data() + okey_len;
    _key_decode_u32(p, offset);
    return 0;
  }

  static inline bool is_extent_shard_key(const std::string& key)
  {
    return *key.rbegin() == EXTENT_SHARD_KEY_SUFFIX;
  }
}
#endif
