// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */
#ifndef CEPH_CEPHFS_ENCODING_H
#define CEPH_CEPHFS_ENCODING_H

#include "include/ceph_fs_encoder.h"
#include "include/cephfs/types.h"
#include "include/encoding_map.h"
#include "include/encoding_string.h"
#include "include/encoding_vector.h"

inline void encode(const mds_gid_t &v, bufferlist& bl, uint64_t features = 0) {
  uint64_t vv = v;
  encode_raw(vv, bl);
}

inline void decode(mds_gid_t &v, bufferlist::const_iterator& p) {
  uint64_t vv;
  decode_raw(vv, p);
  v = vv;
}

inline void vinodeno_t::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  encode(ino, bl);
  encode(snapid, bl);
}

inline void vinodeno_t::decode(ceph::buffer::list::const_iterator& p) {
  using ceph::decode;
  decode(ino, p);
  decode(snapid, p);
}

template<template<typename> class Allocator>
void charmap_md_t<Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const {
  ENCODE_START(STRUCT_V, COMPAT_V, bl);
  ceph::encode(casesensitive, bl);
  ceph::encode(normalization, bl);
  ceph::encode(encoding, bl);
  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void charmap_md_t<Allocator>::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(STRUCT_V, p);
  ceph::decode(casesensitive, p);
  ceph::decode(normalization, p);
  ceph::decode(encoding, p);
  DECODE_FINISH(p);
}

inline void quota_info_t::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(max_bytes, bl);
  encode(max_files, bl);
  ENCODE_FINISH(bl);
}

inline void quota_info_t::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
  decode(max_bytes, p);
  decode(max_files, p);
  DECODE_FINISH(p);
}

inline void decode(client_writeable_range_t::byte_range_t& range, ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  decode(range.first, bl);
  decode(range.last, bl);
}

template<template<typename> class Allocator>
void unknown_md_t<Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const {
  encode_nohead(payload, bl);
}

template<template<typename> class Allocator>
void unknown_md_t<Allocator>::decode(ceph::buffer::list::const_iterator& p) {
  bufferlist bl;
  DECODE_UNKNOWN(bl, p);
  auto blp = bl.cbegin();
  blp.copy(blp.get_remaining(), payload);
}

template<typename M, template<typename> class Allocator>
void optmetadata_singleton<M, Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const {
  // no versioning, use optmetadata
  ceph::encode(u64kind, bl);
  std::visit([&bl, features](auto& o) { o.encode(bl, features); }, optmetadata);
}

template<typename M, template<typename> class Allocator>
void optmetadata_singleton<M, Allocator>::decode(ceph::buffer::list::const_iterator& p) {
  ceph::decode(u64kind, p);
  *this = optmetadata_singleton((kind_t)u64kind);
  std::visit([&p](auto& o) { o.decode(p); }, optmetadata);
}

template<typename Singleton, template<typename> class Allocator>
void optmetadata_multiton<Singleton, Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const {
  // no versioning, use payload
  using ceph::encode;
  ENCODE_START(STRUCT_V, COMPAT_V, bl);
  encode(opts, bl);
  ENCODE_FINISH(bl);
}

template<typename Singleton, template<typename> class Allocator>
void optmetadata_multiton<Singleton, Allocator>::decode(ceph::buffer::list::const_iterator& p) {
  using ceph::decode;
  DECODE_START(STRUCT_V, p);
  decode(opts, p);
  DECODE_FINISH(p);
}

template<typename T, template<typename> class Allocator>
static inline void encode(optmetadata_singleton<T, Allocator> const& o, ::ceph::buffer::list& bl, uint64_t features=0)
{
  ENCODE_DUMP_PRE();
  o.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<typename T, template<typename> class Allocator>
static inline void decode(optmetadata_singleton<T, Allocator>& o, ::ceph::buffer::list::const_iterator& p)
{
  o.decode(p);
}

template<typename Singleton, template<typename> class Allocator>
static inline void encode(optmetadata_multiton<Singleton,Allocator> const& o, ::ceph::buffer::list& bl, uint64_t features=0)
{
  ENCODE_DUMP_PRE();
  o.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<typename Singleton, template<typename> class Allocator>
static inline void decode(optmetadata_multiton<Singleton,Allocator>& o, ::ceph::buffer::list::const_iterator& p)
{
  o.decode(p);
}

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void inode_t<Allocator>::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(21, 6, bl);

  encode(ino, bl);
  encode(rdev, bl);
  encode(ctime, bl);

  encode(mode, bl);
  encode(uid, bl);
  encode(gid, bl);

  encode(nlink, bl);
  {
    // removed field
    bool anchored = 0;
    encode(anchored, bl);
  }

  encode(dir_layout, bl);
  encode(layout, bl, features);
  encode(size, bl);
  encode(truncate_seq, bl);
  encode(truncate_size, bl);
  encode(truncate_from, bl);
  encode(truncate_pending, bl);
  encode(mtime, bl);
  encode(atime, bl);
  encode(time_warp_seq, bl);
  encode(client_ranges, bl);

  encode(dirstat, bl);
  encode(rstat, bl);
  encode(accounted_rstat, bl);

  encode(version, bl);
  encode(file_data_version, bl);
  encode(xattr_version, bl);
  encode(backtrace_version, bl);
  encode(old_pools, bl);
  encode(max_size_ever, bl);
  encode(inline_data, bl);
  encode(quota, bl);

  encode(stray_prior_path, bl);

  encode(last_scrub_version, bl);
  encode(last_scrub_stamp, bl);

  encode(btime, bl);
  encode(change_attr, bl);

  encode(export_pin, bl);

  encode(export_ephemeral_random_pin, bl);
  encode(flags, bl);

  encode(!fscrypt_auth.empty(), bl);
  encode(fscrypt_auth, bl);
  encode(fscrypt_file, bl);
  encode(fscrypt_last_block, bl);

  encode(optmetadata, bl, features);

  encode(remote_ino, bl);
  encode(referent_inodes, bl);

  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::decode(ceph::buffer::list::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(19, 6, 6, p);

  decode(ino, p);
  decode(rdev, p);
  decode(ctime, p);

  decode(mode, p);
  decode(uid, p);
  decode(gid, p);

  decode(nlink, p);
  {
    bool anchored;
    decode(anchored, p);
  }

  if (struct_v >= 4)
    decode(dir_layout, p);
  else {
    // FIPS zeroization audit 20191117: this memset is not security related.
    memset(&dir_layout, 0, sizeof(dir_layout));
  }
  decode(layout, p);
  decode(size, p);
  decode(truncate_seq, p);
  decode(truncate_size, p);
  decode(truncate_from, p);
  if (struct_v >= 5)
    decode(truncate_pending, p);
  else
    truncate_pending = 0;
  decode(mtime, p);
  decode(atime, p);
  decode(time_warp_seq, p);
  if (struct_v >= 3) {
    decode(client_ranges, p);
  } else {
    std::map<client_t, client_writeable_range_t::byte_range_t> m;
    decode(m, p);
    for (auto q = m.begin(); q != m.end(); ++q)
      client_ranges[q->first].range = q->second;
  }

  decode(dirstat, p);
  decode(rstat, p);
  decode(accounted_rstat, p);

  decode(version, p);
  decode(file_data_version, p);
  decode(xattr_version, p);
  if (struct_v >= 2)
    decode(backtrace_version, p);
  if (struct_v >= 7)
    decode(old_pools, p);
  if (struct_v >= 8)
    decode(max_size_ever, p);
  if (struct_v >= 9) {
    decode(inline_data, p);
  } else {
    inline_data.version = CEPH_INLINE_NONE;
  }
  if (struct_v < 10)
    backtrace_version = 0; // force update backtrace
  if (struct_v >= 11)
    decode(quota, p);

  if (struct_v >= 12) {
    std::string tmp;
    decode(tmp, p);
    stray_prior_path = std::string_view(tmp);
  }

  if (struct_v >= 13) {
    decode(last_scrub_version, p);
    decode(last_scrub_stamp, p);
  }
  if (struct_v >= 14) {
    decode(btime, p);
    decode(change_attr, p);
  } else {
    btime = utime_t();
    change_attr = 0;
  }

  if (struct_v >= 15) {
    decode(export_pin, p);
  } else {
    export_pin = MDS_RANK_NONE;
  }

  if (struct_v >= 16) {
    decode(export_ephemeral_random_pin, p);
    decode(flags, p);
  } else {
    export_ephemeral_random_pin = 0;
    flags = 0;
  }

  if (struct_v >= 17) {
    bool fscrypt_flag;
    decode(fscrypt_flag, p); // ignored
  }

  if (struct_v >= 18) {
    decode(fscrypt_auth, p);
    decode(fscrypt_file, p);
  }

  if (struct_v >= 19) {
    decode(fscrypt_last_block, p);
  }

  if (struct_v >= 20) {
    decode(optmetadata, p);
  }

  if (struct_v >= 21) {
    decode(remote_ino, p);
    decode(referent_inodes, p);
  }
  DECODE_FINISH(p);
}

template<template<typename> class Allocator>
inline void encode(const inode_t<Allocator> &c, ::ceph::buffer::list &bl, uint64_t features)
{
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<template<typename> class Allocator>
inline void decode(inode_t<Allocator> &c, ::ceph::buffer::list::const_iterator &p)
{
  c.decode(p);
}

#endif
