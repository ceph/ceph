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
#ifndef CEPH_CEPHFS_TYPES_H
#define CEPH_CEPHFS_TYPES_H
#include "include/int_types.h"

#include <ostream>
#include <set>
#include <map>
#include <string_view>

#include "common/config.h"
#include "common/Clock.h"
#include "common/DecayCounter.h"
#include "common/StackStringStream.h"
#include "common/entity_name.h"

#include "include/compat.h"
#include "include/Context.h"
#include "include/frag.h"
#include "include/xlist.h"
#include "include/interval_set.h"
#include "include/compact_set.h"
#include "include/fs_types.h"
#include "include/ceph_fs.h"

#include "mds/inode_backtrace.h"

#include <boost/spirit/include/qi.hpp>
#include <boost/pool/pool.hpp>
#include "include/ceph_assert.h"
#include <boost/serialization/strong_typedef.hpp>
#include "common/ceph_json.h"

#define CEPH_FS_ONDISK_MAGIC "ceph fs volume v011"
#define MAX_MDS                   0x100

BOOST_STRONG_TYPEDEF(uint64_t, mds_gid_t)
extern const mds_gid_t MDS_GID_NONE;

template <>
struct std::hash<mds_gid_t> {
  size_t operator()(const mds_gid_t& gid) const
  {
    return hash<uint64_t> {}(gid);
  }
};

inline void encode(const mds_gid_t &v, bufferlist& bl, uint64_t features = 0) {
  uint64_t vv = v;
  encode_raw(vv, bl);
}

inline void decode(mds_gid_t &v, bufferlist::const_iterator& p) {
  uint64_t vv;
  decode_raw(vv, p);
  v = vv;
}

typedef int32_t fs_cluster_id_t;
constexpr fs_cluster_id_t FS_CLUSTER_ID_NONE = -1;

// The namespace ID of the anonymous default filesystem from legacy systems
constexpr fs_cluster_id_t FS_CLUSTER_ID_ANONYMOUS = 0;

typedef int32_t mds_rank_t;
constexpr mds_rank_t MDS_RANK_NONE		= -1;
constexpr mds_rank_t MDS_RANK_EPHEMERAL_DIST	= -2;
constexpr mds_rank_t MDS_RANK_EPHEMERAL_RAND	= -3;

struct scatter_info_t {
  version_t version = 0;
};

struct frag_info_t : public scatter_info_t {
  int64_t size() const { return nfiles + nsubdirs; }

  void zero() {
    *this = frag_info_t();
  }

  // *this += cur - acc;
  void add_delta(const frag_info_t &cur, const frag_info_t &acc, bool *touched_mtime=0, bool *touched_chattr=0) {
    if (cur.mtime > mtime) {
      mtime = cur.mtime;
      if (touched_mtime)
	*touched_mtime = true;
    }
    if (cur.change_attr > change_attr) {
      change_attr = cur.change_attr;
      if (touched_chattr)
	*touched_chattr = true;
    }
    nfiles += cur.nfiles - acc.nfiles;
    nsubdirs += cur.nsubdirs - acc.nsubdirs;
  }

  void add(const frag_info_t& other) {
    if (other.mtime > mtime)
      mtime = other.mtime;
    if (other.change_attr > change_attr)
      change_attr = other.change_attr;
    nfiles += other.nfiles;
    nsubdirs += other.nsubdirs;
  }

  bool same_sums(const frag_info_t &o) const {
    return mtime <= o.mtime &&
	nfiles == o.nfiles &&
	nsubdirs == o.nsubdirs;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<frag_info_t*>& ls);

  // this frag
  utime_t mtime;
  uint64_t change_attr = 0;
  int64_t nfiles = 0;        // files
  int64_t nsubdirs = 0;      // subdirs
};
WRITE_CLASS_ENCODER(frag_info_t)

inline bool operator==(const frag_info_t &l, const frag_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}
inline bool operator!=(const frag_info_t &l, const frag_info_t &r) {
  return !(l == r);
}

std::ostream& operator<<(std::ostream &out, const frag_info_t &f);

struct nest_info_t : public scatter_info_t {
  int64_t rsize() const { return rfiles + rsubdirs; }

  void zero() {
    *this = nest_info_t();
  }

  void sub(const nest_info_t &other) {
    add(other, -1);
  }
  void add(const nest_info_t &other, int fac=1) {
    if (other.rctime > rctime)
      rctime = other.rctime;
    rbytes += fac*other.rbytes;
    rfiles += fac*other.rfiles;
    rsubdirs += fac*other.rsubdirs;
    rsnaps += fac*other.rsnaps;
  }

  // *this += cur - acc;
  void add_delta(const nest_info_t &cur, const nest_info_t &acc) {
    if (cur.rctime > rctime)
      rctime = cur.rctime;
    rbytes += cur.rbytes - acc.rbytes;
    rfiles += cur.rfiles - acc.rfiles;
    rsubdirs += cur.rsubdirs - acc.rsubdirs;
    rsnaps += cur.rsnaps - acc.rsnaps;
  }

  bool same_sums(const nest_info_t &o) const {
    return rctime <= o.rctime &&
        rbytes == o.rbytes &&
        rfiles == o.rfiles &&
        rsubdirs == o.rsubdirs &&
        rsnaps == o.rsnaps;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<nest_info_t*>& ls);

  // this frag + children
  utime_t rctime;
  int64_t rbytes = 0;
  int64_t rfiles = 0;
  int64_t rsubdirs = 0;
  int64_t rsnaps = 0;
};
WRITE_CLASS_ENCODER(nest_info_t)

inline bool operator==(const nest_info_t &l, const nest_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}
inline bool operator!=(const nest_info_t &l, const nest_info_t &r) {
  return !(l == r);
}

std::ostream& operator<<(std::ostream &out, const nest_info_t &n);

struct vinodeno_t {
  vinodeno_t() {}
  vinodeno_t(inodeno_t i, snapid_t s) : ino(i), snapid(s) {}

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(ino, bl);
    encode(snapid, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    decode(ino, p);
    decode(snapid, p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("ino", ino);
    f->dump_unsigned("snapid", snapid);
  }
  static void generate_test_instances(std::list<vinodeno_t*>& ls) {
    ls.push_back(new vinodeno_t);
    ls.push_back(new vinodeno_t(1, 2));
  }

  inodeno_t ino;
  snapid_t snapid;
};
WRITE_CLASS_ENCODER(vinodeno_t)

inline bool operator==(const vinodeno_t &l, const vinodeno_t &r) {
  return l.ino == r.ino && l.snapid == r.snapid;
}
inline bool operator!=(const vinodeno_t &l, const vinodeno_t &r) {
  return !(l == r);
}
inline bool operator<(const vinodeno_t &l, const vinodeno_t &r) {
  return
    l.ino < r.ino ||
    (l.ino == r.ino && l.snapid < r.snapid);
}

typedef enum {
  QUOTA_MAX_FILES,
  QUOTA_MAX_BYTES,
  QUOTA_ANY
} quota_max_t;

struct quota_info_t
{
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_bytes, bl);
    encode(max_files, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
    decode(max_bytes, p);
    decode(max_files, p);
    DECODE_FINISH(p);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<quota_info_t *>& ls);

  bool is_valid() const {
    return max_bytes >=0 && max_files >=0;
  }
  bool is_enabled(quota_max_t type=QUOTA_ANY) const {
    switch (type) {
    case QUOTA_MAX_FILES:
      return !!max_files;
    case QUOTA_MAX_BYTES:
      return !!max_bytes;
    case QUOTA_ANY:
    default:
      return !!max_bytes || !!max_files;
    }
  }
  void decode_json(JSONObj *obj);

  int64_t max_bytes = 0;
  int64_t max_files = 0;
};
WRITE_CLASS_ENCODER(quota_info_t)

inline bool operator==(const quota_info_t &l, const quota_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

std::ostream& operator<<(std::ostream &out, const quota_info_t &n);

struct client_writeable_range_t {
  struct byte_range_t {
    uint64_t first = 0, last = 0;    // interval client can write to
    byte_range_t() {}
    void decode_json(JSONObj *obj);
  };

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<client_writeable_range_t*>& ls);

  byte_range_t range;
  snapid_t follows = 0;     // aka "data+metadata flushed thru"
};

inline void decode(client_writeable_range_t::byte_range_t& range, ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  decode(range.first, bl);
  decode(range.last, bl);
}

WRITE_CLASS_ENCODER(client_writeable_range_t)

std::ostream& operator<<(std::ostream& out, const client_writeable_range_t& r);

inline bool operator==(const client_writeable_range_t& l,
		       const client_writeable_range_t& r) {
  return l.range.first == r.range.first && l.range.last == r.range.last &&
    l.follows == r.follows;
}

struct inline_data_t {
public:
  inline_data_t() {}
  inline_data_t(const inline_data_t& o) : version(o.version) {
    if (o.blp)
      set_data(*o.blp);
  }
  inline_data_t& operator=(const inline_data_t& o) {
    version = o.version;
    if (o.blp)
      set_data(*o.blp);
    else
      free_data();
    return *this;
  }

  void free_data() {
    blp.reset();
  }
  void get_data(ceph::buffer::list& ret) const {
    if (blp)
      ret = *blp;
    else
      ret.clear();
  }
  void set_data(const ceph::buffer::list& bl) {
    if (!blp)
      blp.reset(new ceph::buffer::list);
    *blp = bl;
  }
  size_t length() const { return blp ? blp->length() : 0; }

  bool operator==(const inline_data_t& o) const {
   return length() == o.length() &&
	  (length() == 0 ||
	   (*const_cast<ceph::buffer::list*>(blp.get()) == *const_cast<ceph::buffer::list*>(o.blp.get())));
  }
  bool operator!=(const inline_data_t& o) const {
    return !(*this == o);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<inline_data_t*>& ls);

  version_t version = 1;

private:
  std::unique_ptr<ceph::buffer::list> blp;
};
WRITE_CLASS_ENCODER(inline_data_t)

enum {
  DAMAGE_STATS,     // statistics (dirstat, size, etc)
  DAMAGE_RSTATS,    // recursive statistics (rstat, accounted_rstat)
  DAMAGE_FRAGTREE   // fragtree -- repair by searching
};

template<template<typename> class Allocator = std::allocator>
struct inode_t {
  /**
   * ***************
   * Do not forget to add any new fields to the compare() function.
   * ***************
   */
  using client_range_map = std::map<client_t,client_writeable_range_t,std::less<client_t>,Allocator<std::pair<const client_t,client_writeable_range_t>>>;

  static const uint8_t F_EPHEMERAL_DISTRIBUTED_PIN = (1<<0);
  static const uint8_t F_QUIESCE_BLOCK             = (1<<1);

  inode_t()
  {
    clear_layout();
  }

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool is_truncating() const { return (truncate_pending > 0); }
  void truncate(uint64_t old_size, uint64_t new_size, const bufferlist &fbl) {
    truncate(old_size, new_size);
    fscrypt_last_block = fbl;
  }
  void truncate(uint64_t old_size, uint64_t new_size) {
    ceph_assert(new_size <= old_size);
    if (old_size > max_size_ever)
      max_size_ever = old_size;
    truncate_from = old_size;
    size = new_size;
    rstat.rbytes = new_size;
    truncate_size = size;
    truncate_seq++;
    truncate_pending++;
  }

  bool has_layout() const {
    return layout != file_layout_t();
  }

  void clear_layout() {
    layout = file_layout_t();
  }

  uint64_t get_layout_size_increment() const {
    return layout.get_period();
  }

  bool is_dirty_rstat() const { return !(rstat == accounted_rstat); }

  uint64_t get_client_range(client_t client) const {
    auto it = client_ranges.find(client);
    return it != client_ranges.end() ? it->second.range.last : 0;
  }

  uint64_t get_max_size() const {
    uint64_t max = 0;
      for (std::map<client_t,client_writeable_range_t>::const_iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	if (p->second.range.last > max)
	  max = p->second.range.last;
      return max;
  }
  void set_max_size(uint64_t new_max) {
    if (new_max == 0) {
      client_ranges.clear();
    } else {
      for (std::map<client_t,client_writeable_range_t>::iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	p->second.range.last = new_max;
    }
  }

  void trim_client_ranges(snapid_t last) {
    std::map<client_t, client_writeable_range_t>::iterator p = client_ranges.begin();
    while (p != client_ranges.end()) {
      if (p->second.follows >= last)
	client_ranges.erase(p++);
      else
	++p;
    }
  }

  bool is_backtrace_updated() const {
    return backtrace_version == version;
  }
  void update_backtrace(version_t pv=0) {
    backtrace_version = pv ? pv : version;
  }

  void add_old_pool(int64_t l) {
    backtrace_version = version;
    old_pools.insert(l);
  }

  void set_flag(bool v, uint8_t flag) {
    if (v) {
      flags |= flag;
    } else {
      flags &= ~(flag);
    }
  }
  bool get_flag(uint8_t flag) const {
    return flags&flag;
  }
  void set_ephemeral_distributed_pin(bool v) {
    set_flag(v, F_EPHEMERAL_DISTRIBUTED_PIN);
  }
  bool get_ephemeral_distributed_pin() const {
    return get_flag(F_EPHEMERAL_DISTRIBUTED_PIN);
  }
  void set_quiesce_block(bool v) {
    set_flag(v, F_QUIESCE_BLOCK);
  }
  bool get_quiesce_block() const {
    return get_flag(F_QUIESCE_BLOCK);
  }

  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void client_ranges_cb(client_range_map& c, JSONObj *obj);
  static void old_pools_cb(compact_set<int64_t, std::less<int64_t>, Allocator<int64_t> >& c, JSONObj *obj);
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<inode_t*>& ls);
  /**
   * Compare this inode_t with another that represent *the same inode*
   * at different points in time.
   * @pre The inodes are the same ino
   *
   * @param other The inode_t to compare ourselves with
   * @param divergent A bool pointer which will be set to true
   * if the values are different in a way that can't be explained
   * by one being a newer version than the other.
   *
   * @returns 1 if we are newer than the other, 0 if equal, -1 if older.
   */
  int compare(const inode_t &other, bool *divergent) const;

  // base (immutable)
  inodeno_t ino = 0;
  uint32_t   rdev = 0;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time
  utime_t    btime;   // birth time

  // perm (namespace permissions)
  uint32_t   mode = 0;
  uid_t      uid = 0;
  gid_t      gid = 0;

  // nlink
  int32_t    nlink = 0;

  // file (data access)
  ceph_dir_layout dir_layout = {};    // [dir only]
  file_layout_t layout;
  compact_set<int64_t, std::less<int64_t>, Allocator<int64_t>> old_pools;
  uint64_t   size = 0;        // on directory, # dentries
  uint64_t   max_size_ever = 0; // max size the file has ever been
  uint32_t   truncate_seq = 0;
  uint64_t   truncate_size = 0, truncate_from = 0;
  uint32_t   truncate_pending = 0;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq = 0;  // count of (potential) mtime/atime timewarps (i.e., utimes())
  inline_data_t inline_data; // FIXME check

  // change attribute
  uint64_t   change_attr = 0;

  client_range_map client_ranges;  // client(s) can write to these ranges

  // dirfrag, recursive accountin
  frag_info_t dirstat;         // protected by my filelock
  nest_info_t rstat;           // protected by my nestlock
  nest_info_t accounted_rstat; // protected by parent's nestlock

  quota_info_t quota;

  mds_rank_t export_pin = MDS_RANK_NONE;

  double export_ephemeral_random_pin = 0;
  /**
   * N.B. previously this was a bool for distributed_ephemeral_pin which is
   * encoded as a __u8. We take advantage of that to harness the remaining 7
   * bits to avoid adding yet another field to this struct. This is safe also
   * because the integral conversion of a bool to int (__u8) is well-defined
   * per the standard as 0 (false) and 1 (true):
   *
   *     [conv.integral]
   *     If the destination type is bool, see [conv.bool]. If the source type is
   *     bool, the value false is converted to zero and the value true is converted
   *     to one.
   *
   * So we can be certain the other bits have not be set during
   * encoding/decoding due to implementation defined compiler behavior.
   *
   */
  uint8_t flags = 0;

  // special stuff
  version_t version = 0;           // auth only
  version_t file_data_version = 0; // auth only
  version_t xattr_version = 0;

  utime_t last_scrub_stamp;    // start time of last complete scrub
  version_t last_scrub_version = 0;// (parent) start version of last complete scrub

  version_t backtrace_version = 0;

  snapid_t oldest_snap;

  std::basic_string<char,std::char_traits<char>,Allocator<char>> stray_prior_path; //stores path before unlink

  std::vector<uint8_t> fscrypt_auth;
  std::vector<uint8_t> fscrypt_file;

  bufferlist fscrypt_last_block;

private:
  bool older_is_consistent(const inode_t &other) const;
};

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void inode_t<Allocator>::encode(ceph::buffer::list &bl, uint64_t features) const
{
  ENCODE_START(19, 6, bl);

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
  DECODE_FINISH(p);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("btime") << btime;
  f->dump_unsigned("mode", mode);
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);

  f->open_object_section("dir_layout");
  ::dump(dir_layout, f);
  f->close_section();

  f->dump_object("layout", layout);

  f->open_array_section("old_pools");
  for (const auto &p : old_pools) {
    f->dump_int("pool", p);
  }
  f->close_section();

  f->dump_unsigned("size", size);
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_unsigned("truncate_from", truncate_from);
  f->dump_unsigned("truncate_pending", truncate_pending);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_unsigned("time_warp_seq", time_warp_seq);
  f->dump_unsigned("change_attr", change_attr);
  f->dump_int("export_pin", export_pin);
  f->dump_int("export_ephemeral_random_pin", export_ephemeral_random_pin);
  f->dump_bool("export_ephemeral_distributed_pin", get_ephemeral_distributed_pin());
  f->dump_bool("quiesce_block", get_quiesce_block());

  f->open_array_section("client_ranges");
  for (const auto &p : client_ranges) {
    f->open_object_section("client");
    f->dump_unsigned("client", p.first.v);
    p.second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("dirstat");
  dirstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();

  f->dump_unsigned("version", version);
  f->dump_unsigned("file_data_version", file_data_version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("backtrace_version", backtrace_version);

  f->dump_string("stray_prior_path", stray_prior_path);
  f->dump_unsigned("max_size_ever", max_size_ever);

  f->open_object_section("quota");
  quota.dump(f);
  f->close_section();

  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_unsigned("last_scrub_version", last_scrub_version);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::client_ranges_cb(typename inode_t<Allocator>::client_range_map& c, JSONObj *obj){

  int64_t client;
  JSONDecoder::decode_json("client", client, obj, true);
  client_writeable_range_t client_range_tmp;
  JSONDecoder::decode_json("byte range", client_range_tmp.range, obj, true);
  JSONDecoder::decode_json("follows", client_range_tmp.follows.val, obj, true);
  c[client] = client_range_tmp;
}

template<template<typename> class Allocator>
void inode_t<Allocator>::old_pools_cb(compact_set<int64_t, std::less<int64_t>, Allocator<int64_t> >& c, JSONObj *obj){

  int64_t tmp;
  decode_json_obj(tmp, obj);
  c.insert(tmp);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::decode_json(JSONObj *obj)
{

  JSONDecoder::decode_json("ino", ino.val, obj, true);
  JSONDecoder::decode_json("rdev", rdev, obj, true);
  //JSONDecoder::decode_json("ctime", ctime, obj, true);
  //JSONDecoder::decode_json("btime", btime, obj, true);
  JSONDecoder::decode_json("mode", mode, obj, true);
  JSONDecoder::decode_json("uid", uid, obj, true);
  JSONDecoder::decode_json("gid", gid, obj, true);
  JSONDecoder::decode_json("nlink", nlink, obj, true);
  JSONDecoder::decode_json("dir_layout", dir_layout, obj, true);
  JSONDecoder::decode_json("layout", layout, obj, true);
  JSONDecoder::decode_json("old_pools", old_pools, inode_t<Allocator>::old_pools_cb, obj, true);
  JSONDecoder::decode_json("size", size, obj, true);
  JSONDecoder::decode_json("truncate_seq", truncate_seq, obj, true);
  JSONDecoder::decode_json("truncate_size", truncate_size, obj, true);
  JSONDecoder::decode_json("truncate_from", truncate_from, obj, true);
  JSONDecoder::decode_json("truncate_pending", truncate_pending, obj, true);
  //JSONDecoder::decode_json("mtime", mtime, obj, true);
  //JSONDecoder::decode_json("atime", atime, obj, true);
  JSONDecoder::decode_json("time_warp_seq", time_warp_seq, obj, true);
  JSONDecoder::decode_json("change_attr", change_attr, obj, true);
  JSONDecoder::decode_json("export_pin", export_pin, obj, true);
  JSONDecoder::decode_json("client_ranges", client_ranges, inode_t<Allocator>::client_ranges_cb, obj, true);
  JSONDecoder::decode_json("dirstat", dirstat, obj, true);
  JSONDecoder::decode_json("rstat", rstat, obj, true);
  JSONDecoder::decode_json("accounted_rstat", accounted_rstat, obj, true);
  JSONDecoder::decode_json("version", version, obj, true);
  JSONDecoder::decode_json("file_data_version", file_data_version, obj, true);
  JSONDecoder::decode_json("xattr_version", xattr_version, obj, true);
  JSONDecoder::decode_json("backtrace_version", backtrace_version, obj, true);
  JSONDecoder::decode_json("stray_prior_path", stray_prior_path, obj, true);
  JSONDecoder::decode_json("max_size_ever", max_size_ever, obj, true);
  JSONDecoder::decode_json("quota", quota, obj, true);
  JSONDecoder::decode_json("last_scrub_stamp", last_scrub_stamp, obj, true);
  JSONDecoder::decode_json("last_scrub_version", last_scrub_version, obj, true);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::generate_test_instances(std::list<inode_t*>& ls)
{
  ls.push_back(new inode_t<Allocator>);
  ls.push_back(new inode_t<Allocator>);
  ls.back()->ino = 1;
  // i am lazy.
}

template<template<typename> class Allocator>
int inode_t<Allocator>::compare(const inode_t<Allocator> &other, bool *divergent) const
{
  ceph_assert(ino == other.ino);
  *divergent = false;
  if (version == other.version) {
    if (rdev != other.rdev ||
        ctime != other.ctime ||
        btime != other.btime ||
        mode != other.mode ||
        uid != other.uid ||
        gid != other.gid ||
        nlink != other.nlink ||
        memcmp(&dir_layout, &other.dir_layout, sizeof(dir_layout)) ||
        layout != other.layout ||
        old_pools != other.old_pools ||
        size != other.size ||
        max_size_ever != other.max_size_ever ||
        truncate_seq != other.truncate_seq ||
        truncate_size != other.truncate_size ||
        truncate_from != other.truncate_from ||
        truncate_pending != other.truncate_pending ||
	change_attr != other.change_attr ||
        mtime != other.mtime ||
        atime != other.atime ||
        time_warp_seq != other.time_warp_seq ||
        inline_data != other.inline_data ||
        client_ranges != other.client_ranges ||
        !(dirstat == other.dirstat) ||
        !(rstat == other.rstat) ||
        !(accounted_rstat == other.accounted_rstat) ||
        file_data_version != other.file_data_version ||
        xattr_version != other.xattr_version ||
        backtrace_version != other.backtrace_version) {
      *divergent = true;
    }
    return 0;
  } else if (version > other.version) {
    *divergent = !older_is_consistent(other);
    return 1;
  } else {
    ceph_assert(version < other.version);
    *divergent = !other.older_is_consistent(*this);
    return -1;
  }
}

template<template<typename> class Allocator>
bool inode_t<Allocator>::older_is_consistent(const inode_t<Allocator> &other) const
{
  if (max_size_ever < other.max_size_ever ||
      truncate_seq < other.truncate_seq ||
      time_warp_seq < other.time_warp_seq ||
      inline_data.version < other.inline_data.version ||
      dirstat.version < other.dirstat.version ||
      rstat.version < other.rstat.version ||
      accounted_rstat.version < other.accounted_rstat.version ||
      file_data_version < other.file_data_version ||
      xattr_version < other.xattr_version ||
      backtrace_version < other.backtrace_version) {
    return false;
  }
  return true;
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

// parse a map of keys/values.
namespace qi = boost::spirit::qi;

template <typename Iterator>
struct keys_and_values
  : qi::grammar<Iterator, std::map<std::string, std::string>()>
{
    keys_and_values()
      : keys_and_values::base_type(query)
    {
      query =  pair >> *(qi::lit(' ') >> pair);
      pair  =  key >> '=' >> value;
      key   =  qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z_0-9");
      value = +qi::char_("a-zA-Z0-9-_.");
    }
  qi::rule<Iterator, std::map<std::string, std::string>()> query;
  qi::rule<Iterator, std::pair<std::string, std::string>()> pair;
  qi::rule<Iterator, std::string()> key, value;
};

#endif
