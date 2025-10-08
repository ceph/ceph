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

#include <algorithm> // for std::find_if()
#include <cstdint>
#include <list>
#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "rank.h"
#include "frag_info.h"
#include "nest_info.h"

#include "include/client_t.h"
#include "include/compact_set.h"
#include "include/encoding.h"
#include "include/fs_types.h"
#include "include/ceph_fs.h"
#include "include/object.h" // for snapid_t
#include "include/types.h" // for version_t
#include "include/utime.h"

#include "include/ceph_assert.h"

#define CEPH_FS_ONDISK_MAGIC "ceph fs volume v011"
#define MAX_MDS                   0x100

namespace ceph { class Formatter; }
class JSONObj;

template<template<typename> class Allocator>
class charmap_md_t {
public:
  static constexpr int STRUCT_V = 1;
  static constexpr int COMPAT_V = 1;

  using str_t = std::basic_string<char, std::char_traits<char>, Allocator<char>>;

  charmap_md_t() = default;
  charmap_md_t(auto const& cimd) {
    casesensitive = cimd.casesensitive;
    normalization = cimd.normalization;
    encoding = cimd.encoding;
  }
  charmap_md_t<Allocator>& operator=(auto const& other) {
    casesensitive = other.is_casesensitive();
    normalization = other.get_normalization();
    encoding = other.get_encoding();
    return *this;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void print(std::ostream& os) const {
    os << "charmap_md_t(s=" << casesensitive << " f=" << normalization << " e=" << encoding << ")";
  }

  std::string_view get_normalization() const {
    return std::string_view(normalization);
  }
  std::string_view get_encoding() const {
    return std::string_view(encoding);
  }
  void set_normalization(std::string_view sv) {
    normalization = sv;
  }
  void set_encoding(std::string_view sv) {
    encoding = sv;
  }
  void mark_casesensitive() {
    casesensitive = true;
  }
  void mark_caseinsensitive() {
    casesensitive = false;
  }
  bool is_casesensitive() const {
    return casesensitive;
  }

  void dump(ceph::Formatter* f) const;

  constexpr std::string_view get_default_normalization() const {
    return DEFAULT_NORMALIZATION;
  }

  constexpr std::string_view get_default_encoding() const {
    return DEFAULT_ENCODING;
  }

private:
  static constexpr std::string_view DEFAULT_NORMALIZATION = "nfd";
  static constexpr std::string_view DEFAULT_ENCODING = "utf8";

  bool casesensitive = true;
  str_t normalization{DEFAULT_NORMALIZATION};
  str_t encoding{DEFAULT_ENCODING};
};



typedef enum {
  QUOTA_MAX_FILES,
  QUOTA_MAX_BYTES,
  QUOTA_ANY
} quota_max_t;

struct quota_info_t
{
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void dump(ceph::Formatter *f) const;
  static std::list<quota_info_t> generate_test_instances();

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
  static std::list<client_writeable_range_t> generate_test_instances();

  byte_range_t range;
  snapid_t follows = 0;     // aka "data+metadata flushed thru"
};

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
  static std::list<inline_data_t> generate_test_instances();
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


template<template<typename> class Allocator>
class unknown_md_t {
public:
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void print(std::ostream& os) const {
    os << "unknown_md_t(len=" << payload.size() << ")";
  }
  void dump(ceph::Formatter* f) const;

private:
  std::vector<uint8_t,Allocator<uint8_t>> payload;
};

template<template<typename> class Allocator>
struct optmetadata_server_t {
  using opts = std::variant<
    unknown_md_t<Allocator>,
    charmap_md_t<Allocator>
  >;
  enum kind_t : uint64_t {
    UNKNOWN,
    CHARMAP,
    _MAX
  };
};

template<template<typename> class Allocator>
struct optmetadata_client_t {
  using opts = std::variant<
    unknown_md_t<Allocator>,
    charmap_md_t<Allocator>
  >;
  enum kind_t : uint64_t {
    UNKNOWN,
    CHARMAP,
    _MAX
  };
};

template<typename... Ts>
void defconstruct_type(std::variant<Ts...>& v, std::size_t i)
{
  constexpr auto N = sizeof...(Ts);
  static const std::array<std::variant<Ts...>, N> lookup = {Ts{}...};
  v = lookup[i];
}

template<typename M, template<typename> class Allocator>
struct optmetadata_singleton {
  using optmetadata_t = typename M::opts;
  using kind_t = typename M::kind_t;

  optmetadata_singleton(kind_t kind = kind_t::UNKNOWN)
  {
    u64kind = (uint64_t)kind;
    defconstruct_type(optmetadata, get_kind());
  }

  auto get_kind() const {
    constexpr auto optsmax = std::variant_size_v<optmetadata_t>;
    static_assert(kind_t::_MAX == optsmax);
    static_assert(kind_t::UNKNOWN == 0);
    if (u64kind > optsmax) {
      return kind_t::UNKNOWN;
    } else {
      return (kind_t)u64kind;
    }
  }
  template<template< template<typename> class > class T>
  auto& get_meta() {
    return std::get< T<Allocator> >(optmetadata);
  }
  template<template< template<typename> class > class T>
  auto& get_meta() const {
    return std::get< T<Allocator> >(optmetadata);
  }

  void print(std::ostream& os) const {
    os << "(k=" << u64kind << " m=";
    std::visit([&os](auto& o) { o.print(os); }, optmetadata);
    os << ")";
  }
  void dump(ceph::Formatter* f) const;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  bool operator<(const optmetadata_singleton& other) const {
    return u64kind < other.u64kind;
  }

private:
  uint64_t u64kind = 0;
  optmetadata_t optmetadata;
};

template<typename Singleton, template<typename> class Allocator>
struct optmetadata_multiton {
  static constexpr int STRUCT_V = 1;
  static constexpr int COMPAT_V = 1;

  using optkind_t = typename Singleton::kind_t;
  using optvec_t = std::vector<Singleton,Allocator<Singleton>>;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void print(std::ostream& os) const {
    os << "optm(len=" << opts.size() << " " << opts << ")";
  }
  void dump(ceph::Formatter* f) const;

  bool has_opt(optkind_t kind) const {
    auto f = [kind](auto& o) {
      return o.get_kind() == kind;
    };
    auto it = std::find_if(opts.begin(), opts.end(), std::move(f));
    return it != opts.end();
  }
  auto& get_opt(optkind_t kind) const {
    auto f = [kind](auto& o) {
      return o.get_kind() == kind;
    };
    auto it = std::find_if(opts.begin(), opts.end(), std::move(f));
    return *it;
  }
  auto& get_opt(optkind_t kind) {
    auto f = [kind](auto& o) {
      return o.get_kind() == kind;
    };
    auto it = std::find_if(opts.begin(), opts.end(), std::move(f));
    return *it;
  }
  auto& get_or_create_opt(optkind_t kind) {
    auto f = [kind](auto& o) {
      return o.get_kind() == kind;
    };
    if (auto it = std::find_if(opts.begin(), opts.end(), std::move(f)); it != opts.end()) {
      return *it;
    }
    auto it = std::lower_bound(opts.begin(), opts.end(), kind);
    it = opts.emplace(it, kind);
    return *it;
  }
  void del_opt(optkind_t kind) {
    auto f = [kind](auto& o) {
      return o.get_kind() == kind;
    };
    auto it = std::remove_if(opts.begin(), opts.end(), std::move(f));
    opts.erase(it, opts.end());
  }

  auto size() const {
    return opts.size();
  }

private:
  optvec_t opts;
};

template<template<typename> class Allocator = std::allocator>
struct inode_t {
  /**
   * ***************
   * Do not forget to add any new fields to the compare() function.
   * ***************
   */
  using optmetadata_singleton_server_t = optmetadata_singleton<optmetadata_server_t<Allocator>,Allocator>;
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
  void truncate(uint64_t old_size, uint64_t new_size, ::ceph::buffer::list::const_iterator fblp) {
    truncate(old_size, new_size);
    fblp.copy(fblp.get_remaining(), fscrypt_last_block);
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

  bool has_charmap() const {
    return optmetadata.has_opt(optmetadata_singleton_server_t::kind_t::CHARMAP);
  }
  auto& get_charmap() const {
    auto& opt = optmetadata.get_opt(optmetadata_singleton_server_t::kind_t::CHARMAP);
    return opt.template get_meta< charmap_md_t >();
  }
  auto& get_charmap() {
    auto& opt = optmetadata.get_opt(optmetadata_singleton_server_t::kind_t::CHARMAP);
    return opt.template get_meta< charmap_md_t >();
  }
  auto& set_charmap() {
    auto& opt = optmetadata.get_or_create_opt(optmetadata_singleton_server_t::kind_t::CHARMAP);
    return opt.template get_meta< charmap_md_t >();
  }
  void del_charmap() {
    optmetadata.del_opt(optmetadata_singleton_server_t::kind_t::CHARMAP);
  }

  const std::vector<uint64_t>& get_referent_inodes() { return referent_inodes; }
  void add_referent_ino(inodeno_t ref_ino) { referent_inodes.push_back(ref_ino); }
  void remove_referent_ino(inodeno_t ref_ino) {
    referent_inodes.erase(remove(referent_inodes.begin(), referent_inodes.end(), ref_ino), referent_inodes.end());
  }

  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void client_ranges_cb(client_range_map& c, JSONObj *obj);
  static void old_pools_cb(compact_set<int64_t, std::less<int64_t>, Allocator<int64_t> >& c, JSONObj *obj);
  void decode_json(JSONObj *obj);
  static std::list<inode_t> generate_test_instances();
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

  std::vector<uint8_t,Allocator<uint8_t>> fscrypt_auth;
  std::vector<uint8_t,Allocator<uint8_t>> fscrypt_file;
  std::vector<uint8_t,Allocator<uint8_t>> fscrypt_last_block;

  optmetadata_multiton<optmetadata_singleton_server_t,Allocator> optmetadata;

  inodeno_t remote_ino = 0; // referent inode - remote inode link
  std::vector<uint64_t> referent_inodes;

private:
  bool older_is_consistent(const inode_t &other) const;
};

template<template<typename> class Allocator>
auto inode_t<Allocator>::generate_test_instances() -> std::list<inode_t>
{
  std::list<inode_t> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().ino = 1;
  // i am lazy.
  return ls;
}

template<template<typename> class Allocator>
int inode_t<Allocator>::compare(const inode_t<Allocator> &other, bool *divergent) const
{
  // TODO: fscrypt / optmetadata: https://tracker.ceph.com/issues/70188
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
        backtrace_version != other.backtrace_version ||
	remote_ino != other.remote_ino ||
	referent_inodes != other.referent_inodes) {
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

#endif
