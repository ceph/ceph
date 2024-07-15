// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDSTYPES_H
#define CEPH_MDSTYPES_H

#include "include/int_types.h"

#include <ostream>
#include <set>
#include <map>
#include <string>
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

#include "inode_backtrace.h"

#include <boost/spirit/include/qi.hpp>
#include <boost/pool/pool.hpp>
#include "include/ceph_assert.h"
#include "common/ceph_json.h"
#include "include/cephfs/types.h"

#define MDS_PORT_CACHE   0x200
#define MDS_PORT_LOCKER  0x300
#define MDS_PORT_MIGRATOR 0x400

#define NUM_STRAY                 10

// Inode numbers 1,2 and 4 please see CEPH_INO_* in include/ceph_fs.h

#define MDS_INO_MDSDIR_OFFSET     (1*MAX_MDS)
#define MDS_INO_STRAY_OFFSET      (6*MAX_MDS)

// Locations for journal data
#define MDS_INO_LOG_OFFSET        (2*MAX_MDS)
#define MDS_INO_LOG_BACKUP_OFFSET (3*MAX_MDS)
#define MDS_INO_LOG_POINTER_OFFSET    (4*MAX_MDS)
#define MDS_INO_PURGE_QUEUE       (5*MAX_MDS)

#define MDS_INO_SYSTEM_BASE       ((6*MAX_MDS) + (MAX_MDS * NUM_STRAY))

#define MDS_INO_STRAY(x,i)  (MDS_INO_STRAY_OFFSET+((((unsigned)(x))*NUM_STRAY)+((unsigned)(i))))
#define MDS_INO_MDSDIR(x) (MDS_INO_MDSDIR_OFFSET+((unsigned)x))

#define MDS_INO_IS_STRAY(i)  ((i) >= MDS_INO_STRAY_OFFSET  && (i) < (MDS_INO_STRAY_OFFSET+(MAX_MDS*NUM_STRAY)))
#define MDS_INO_IS_MDSDIR(i) ((i) >= MDS_INO_MDSDIR_OFFSET && (i) < (MDS_INO_MDSDIR_OFFSET+MAX_MDS))
#define MDS_INO_MDSDIR_OWNER(i) (signed ((unsigned (i)) - MDS_INO_MDSDIR_OFFSET))
#define MDS_INO_IS_BASE(i)   ((i) == CEPH_INO_ROOT || (i) == CEPH_INO_GLOBAL_SNAPREALM || MDS_INO_IS_MDSDIR(i))
#define MDS_INO_STRAY_OWNER(i) (signed (((unsigned (i)) - MDS_INO_STRAY_OFFSET) / NUM_STRAY))
#define MDS_INO_STRAY_INDEX(i) (((unsigned (i)) - MDS_INO_STRAY_OFFSET) % NUM_STRAY)

#define MDS_IS_PRIVATE_INO(i) ((i) < MDS_INO_SYSTEM_BASE && (i) >= MDS_INO_MDSDIR_OFFSET)

class mds_role_t {
public:
  mds_role_t(fs_cluster_id_t fscid_, mds_rank_t rank_)
    : fscid(fscid_), rank(rank_)
  {}
  mds_role_t() {}

  bool operator<(mds_role_t const &rhs) const {
    if (fscid < rhs.fscid) {
      return true;
    } else if (fscid == rhs.fscid) {
      return rank < rhs.rank;
    } else {
      return false;
    }
  }

  bool is_none() const {
    return (rank == MDS_RANK_NONE);
  }

  void print(std::ostream& out) const {
    out << fscid << ":" << rank;
  }

  fs_cluster_id_t fscid = FS_CLUSTER_ID_NONE;
  mds_rank_t rank = MDS_RANK_NONE;
};

// CAPS
inline std::string gcap_string(int cap)
{
  std::string s;
  if (cap & CEPH_CAP_GSHARED) s += "s";  
  if (cap & CEPH_CAP_GEXCL) s += "x";
  if (cap & CEPH_CAP_GCACHE) s += "c";
  if (cap & CEPH_CAP_GRD) s += "r";
  if (cap & CEPH_CAP_GWR) s += "w";
  if (cap & CEPH_CAP_GBUFFER) s += "b";
  if (cap & CEPH_CAP_GWREXTEND) s += "a";
  if (cap & CEPH_CAP_GLAZYIO) s += "l";
  return s;
}
inline std::string ccap_string(int cap)
{
  std::string s;
  if (cap & CEPH_CAP_PIN) s += "p";

  int a = (cap >> CEPH_CAP_SAUTH) & 3;
  if (a) s += 'A' + gcap_string(a);

  a = (cap >> CEPH_CAP_SLINK) & 3;
  if (a) s += 'L' + gcap_string(a);

  a = (cap >> CEPH_CAP_SXATTR) & 3;
  if (a) s += 'X' + gcap_string(a);

  a = cap >> CEPH_CAP_SFILE;
  if (a) s += 'F' + gcap_string(a);

  if (s.length() == 0)
    s = "-";
  return s;
}

namespace std {
  template<> struct hash<vinodeno_t> {
    size_t operator()(const vinodeno_t &vino) const { 
      hash<inodeno_t> H;
      hash<uint64_t> I;
      return H(vino.ino) ^ I(vino.snapid);
    }
  };
}

inline std::ostream& operator<<(std::ostream &out, const vinodeno_t &vino) {
  out << vino.ino;
  if (vino.snapid == CEPH_NOSNAP)
    out << ".head";
  else if (vino.snapid)
    out << '.' << vino.snapid;
  return out;
}

typedef uint32_t damage_flags_t;

template<template<typename> class Allocator>
using alloc_string = std::basic_string<char,std::char_traits<char>,Allocator<char>>;

template<template<typename> class Allocator>
using xattr_map = std::map<alloc_string<Allocator>,
			   ceph::bufferptr,
			   std::less<alloc_string<Allocator>>,
			   Allocator<std::pair<const alloc_string<Allocator>,
					       ceph::bufferptr>>>; // FIXME bufferptr not in mempool

template<template<typename> class Allocator>
inline void decode_noshare(xattr_map<Allocator>& xattrs, ceph::buffer::list::const_iterator &p)
{
  __u32 n;
  decode(n, p);
  while (n-- > 0) {
    alloc_string<Allocator> key;
    decode(key, p);
    __u32 len;
    decode(len, p);
    p.copy_deep(len, xattrs[key]);
  }
}

template<template<typename> class Allocator = std::allocator>
struct old_inode_t {
  snapid_t first;
  inode_t<Allocator> inode;
  xattr_map<Allocator> xattrs;

  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<old_inode_t*>& ls);
};

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void old_inode_t<Allocator>::encode(ceph::buffer::list& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(first, bl);
  encode(inode, bl, features);
  encode(xattrs, bl);
  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(first, bl);
  decode(inode, bl);
  decode_noshare<Allocator>(xattrs, bl);
  DECODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("first", first);
  inode.dump(f);
  f->open_object_section("xattrs");
  for (const auto &p : xattrs) {
    std::string v(p.second.c_str(), p.second.length());
    f->dump_string(p.first.c_str(), v);
  }
  f->close_section();
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::generate_test_instances(std::list<old_inode_t<Allocator>*>& ls)
{
  ls.push_back(new old_inode_t<Allocator>);
  ls.push_back(new old_inode_t<Allocator>);
  ls.back()->first = 2;
  std::list<inode_t<Allocator>*> ils;
  inode_t<Allocator>::generate_test_instances(ils);
  ls.back()->inode = *ils.back();
  ls.back()->xattrs["user.foo"] = ceph::buffer::copy("asdf", 4);
  ls.back()->xattrs["user.unprintable"] = ceph::buffer::copy("\000\001\002", 3);
}

template<template<typename> class Allocator>
inline void encode(const old_inode_t<Allocator> &c, ::ceph::buffer::list &bl, uint64_t features)
{
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<template<typename> class Allocator>
inline void decode(old_inode_t<Allocator> &c, ::ceph::buffer::list::const_iterator &p)
{
  c.decode(p);
}

/*
 * like an inode, but for a dir frag 
 */
struct fnode_t {
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<fnode_t*>& ls);

  version_t version = 0;
  snapid_t snap_purged_thru;   // the max_last_destroy snapid we've been purged thru
  frag_info_t fragstat, accounted_fragstat;
  nest_info_t rstat, accounted_rstat;
  damage_flags_t damage_flags = 0;

  // we know we and all our descendants have been scrubbed since this version
  version_t recursive_scrub_version = 0;
  utime_t recursive_scrub_stamp;
  // version at which we last scrubbed our personal data structures
  version_t localized_scrub_version = 0;
  utime_t localized_scrub_stamp;
};
WRITE_CLASS_ENCODER(fnode_t)


struct old_rstat_t {
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<old_rstat_t*>& ls);

  void print(std::ostream& out) const {
    out << "old_rstat(first " << first << " " << rstat << " " << accounted_rstat << ")";
  }

  snapid_t first;
  nest_info_t rstat, accounted_rstat;
};
WRITE_CLASS_ENCODER(old_rstat_t)

class feature_bitset_t {
public:
  typedef uint64_t block_type;
  static const size_t bits_per_block = sizeof(block_type) * 8;

  feature_bitset_t(const feature_bitset_t& other) : _vec(other._vec) {}
  feature_bitset_t(feature_bitset_t&& other) : _vec(std::move(other._vec)) {}
  feature_bitset_t(unsigned long value = 0);
  feature_bitset_t(std::string_view);
  feature_bitset_t(const std::vector<size_t>& array);
  feature_bitset_t& operator=(const feature_bitset_t& other) {
    _vec = other._vec;
    return *this;
  }
  feature_bitset_t& operator=(feature_bitset_t&& other) {
    _vec = std::move(other._vec);
    return *this;
  }
  feature_bitset_t& operator-=(const feature_bitset_t& other);
  bool empty() const {
    //block_type is a uint64_t. If the vector is only composed of 0s, then it's still "empty"
    for (auto& v : _vec) {
      if (v)
	return false;
    }
    return true;
  }
  bool test(size_t bit) const {
    if (bit >= bits_per_block * _vec.size())
      return false;
    return _vec[bit / bits_per_block] & ((block_type)1 << (bit % bits_per_block));
  }
  void insert(size_t bit) {
    size_t n = bit / bits_per_block;
    if (n >= _vec.size())
      _vec.resize(n + 1);
    _vec[n] |= ((block_type)1 << (bit % bits_per_block));
  }
  void erase(size_t bit) {
    size_t n = bit / bits_per_block;
    if (n >= _vec.size())
      return;
    _vec[n] &= ~((block_type)1 << (bit % bits_per_block));
    if (n + 1 == _vec.size()) {
      while (!_vec.empty() && _vec.back() == 0)
	_vec.pop_back();
    }
  }
  void clear() {
    _vec.clear();
  }
  bool operator==(const feature_bitset_t& other) const {
    return _vec == other._vec;
  }
  bool operator!=(const feature_bitset_t& other) const {
    return _vec != other._vec;
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;
private:
  void init_array(const std::vector<size_t>& v);

  std::vector<block_type> _vec;
};
WRITE_CLASS_ENCODER(feature_bitset_t)

struct metric_spec_t {
  metric_spec_t() {}
  metric_spec_t(const metric_spec_t& other) :
    metric_flags(other.metric_flags) {}
  metric_spec_t(metric_spec_t&& other) :
    metric_flags(std::move(other.metric_flags)) {}
  metric_spec_t(const feature_bitset_t& mf) :
    metric_flags(mf) {}
  metric_spec_t(feature_bitset_t&& mf) :
    metric_flags(std::move(mf)) {}

  metric_spec_t& operator=(const metric_spec_t& other) {
    metric_flags = other.metric_flags;
    return *this;
  }
  metric_spec_t& operator=(metric_spec_t&& other) {
    metric_flags = std::move(other.metric_flags);
    return *this;
  }

  bool empty() const {
    return metric_flags.empty();
  }

  void clear() {
    metric_flags.clear();
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;

  // set of metrics that a client is capable of forwarding
  feature_bitset_t metric_flags;
};
WRITE_CLASS_ENCODER(metric_spec_t)

/*
 * client_metadata_t
 */
struct client_metadata_t {
  using kv_map_t = std::map<std::string,std::string>;
  using iterator = kv_map_t::const_iterator;

  client_metadata_t() {}
  client_metadata_t(const kv_map_t& kv, const feature_bitset_t &f, const metric_spec_t &mst) :
    kv_map(kv),
    features(f),
    metric_spec(mst) {}
  client_metadata_t& operator=(const client_metadata_t& other) {
    kv_map = other.kv_map;
    features = other.features;
    metric_spec = other.metric_spec;
    return *this;
  }

  bool empty() const { return kv_map.empty() && features.empty() && metric_spec.empty(); }
  iterator find(const std::string& key) const { return kv_map.find(key); }
  iterator begin() const { return kv_map.begin(); }
  iterator end() const { return kv_map.end(); }
  void erase(iterator it) { kv_map.erase(it); }
  std::string& operator[](const std::string& key) { return kv_map[key]; }
  void merge(const client_metadata_t& other) {
    kv_map.insert(other.kv_map.begin(), other.kv_map.end());
    features = other.features;
    metric_spec = other.metric_spec;
  }
  void clear() {
    kv_map.clear();
    features.clear();
    metric_spec.clear();
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;

  kv_map_t kv_map;
  feature_bitset_t features;
  metric_spec_t metric_spec;
};
WRITE_CLASS_ENCODER(client_metadata_t)

/*
 * session_info_t - durable part of a Session
 */
struct session_info_t {
  client_t get_client() const { return client_t(inst.name.num()); }
  bool has_feature(size_t bit) const { return client_metadata.features.test(bit); }
  const entity_name_t& get_source() const { return inst.name; }

  void clear_meta() {
    prealloc_inos.clear();
    completed_requests.clear();
    completed_flushes.clear();
    client_metadata.clear();
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<session_info_t*>& ls);

  entity_inst_t inst;
  std::map<ceph_tid_t,inodeno_t> completed_requests;
  interval_set<inodeno_t> prealloc_inos;   // preallocated, ready to use.
  client_metadata_t client_metadata;
  std::set<ceph_tid_t> completed_flushes;
  EntityName auth_name;
};
WRITE_CLASS_ENCODER_FEATURES(session_info_t)

// dentries
struct dentry_key_t {
  dentry_key_t() {}
  dentry_key_t(snapid_t s, std::string_view n, __u32 h=0) :
    snapid(s), name(n), hash(h) {}

  void print(std::ostream& out) const {
    out << "(" << name << "," << snapid << ")";
  }

  bool is_valid() { return name.length() || snapid; }

  // encode into something that can be decoded as a string.
  // name_ (head) or name_%x (!head)
  void encode(ceph::buffer::list& bl) const {
    std::string key;
    encode(key);
    using ceph::encode;
    encode(key, bl);
  }
  void encode(std::string& key) const {
    char b[20];
    if (snapid != CEPH_NOSNAP) {
      uint64_t val(snapid);
      snprintf(b, sizeof(b), "%" PRIx64, val);
    } else {
      snprintf(b, sizeof(b), "%s", "head");
    }
    CachedStackStringStream css;
    *css << name << "_" << b;
    key = css->strv();
  }
  static void decode_helper(ceph::buffer::list::const_iterator& bl, std::string& nm,
			    snapid_t& sn) {
    std::string key;
    using ceph::decode;
    decode(key, bl);
    decode_helper(key, nm, sn);
  }
  static void decode_helper(std::string_view key, std::string& nm, snapid_t& sn) {
    size_t i = key.find_last_of('_');
    ceph_assert(i != std::string::npos);
    if (key.compare(i+1, std::string_view::npos, "head") == 0) {
      // name_head
      sn = CEPH_NOSNAP;
    } else {
      // name_%x
      long long unsigned x = 0;
      std::string x_str(key.substr(i+1));
      sscanf(x_str.c_str(), "%llx", &x);
      sn = x;
    }
    nm = key.substr(0, i);
  }

  snapid_t snapid = 0;
  std::string_view name;
  __u32 hash = 0;
};

inline bool operator<(const dentry_key_t& k1, const dentry_key_t& k2)
{
  /*
   * order by hash, name, snap
   */
  int c = ceph_frag_value(k1.hash) - ceph_frag_value(k2.hash);
  if (c)
    return c < 0;
  c = k1.name.compare(k2.name);
  if (c)
    return c < 0;
  return k1.snapid < k2.snapid;
}

/*
 * string_snap_t is a simple (string, snapid_t) pair
 */
struct string_snap_t {
  string_snap_t() {}
  string_snap_t(std::string_view n, snapid_t s) : name(n), snapid(s) {}

  void print(std::ostream& out) const {
    out << "(" << name << "," << snapid << ")";
  }

  int compare(const string_snap_t& r) const {
    int ret = name.compare(r.name);
    if (ret)
      return ret;
    if (snapid == r.snapid)
      return 0;
    return snapid > r.snapid ? 1 : -1;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<string_snap_t*>& ls);

  std::string name;
  snapid_t snapid;
};
WRITE_CLASS_ENCODER(string_snap_t)

inline bool operator==(const string_snap_t& l, const string_snap_t& r) {
  return l.name == r.name && l.snapid == r.snapid;
}

inline bool operator<(const string_snap_t& l, const string_snap_t& r) {
  int c = l.name.compare(r.name);
  return c < 0 || (c == 0 && l.snapid < r.snapid);
}

/*
 * mds_table_pending_t
 *
 * For mds's requesting any pending ops, child needs to encode the corresponding
 * pending mutation state in the table.
 */
struct mds_table_pending_t {
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<mds_table_pending_t*>& ls);

  uint64_t reqid = 0;
  __s32 mds = 0;
  version_t tid = 0;
};
WRITE_CLASS_ENCODER(mds_table_pending_t)

// requests
struct metareqid_t {
  metareqid_t() {}
  metareqid_t(entity_name_t n, ceph_tid_t t) : name(n), tid(t) {}
  metareqid_t(std::string_view sv) {
    auto p = sv.find(':');
    if (p == std::string::npos) {
      throw std::invalid_argument("invalid format: expected colon");
    }
    if (!name.parse(sv.substr(0, p))) {
      throw std::invalid_argument("invalid format: invalid entity name");
    }
    try {
      tid = std::stoul(std::string(sv.substr(p+1)), nullptr, 0);
    } catch (const std::invalid_argument& e) {
      throw std::invalid_argument("invalid format: tid is not a number");
    } catch (const std::out_of_range& e) {
      throw std::invalid_argument("invalid format: tid is out of range");
    }
  }
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(name, bl);
    encode(tid, bl);
  }
  void decode(ceph::buffer::list::const_iterator &p) {
    using ceph::decode;
    decode(name, p);
    decode(tid, p);
  }
  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const {
    out << name << ":" << tid;
  }

  entity_name_t name;
  uint64_t tid = 0;
};
WRITE_CLASS_ENCODER(metareqid_t)

inline bool operator==(const metareqid_t& l, const metareqid_t& r) {
  return (l.name == r.name) && (l.tid == r.tid);
}
inline bool operator!=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name != r.name) || (l.tid != r.tid);
}
inline bool operator<(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) || 
    (l.name == r.name && l.tid < r.tid);
}
inline bool operator<=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) ||
    (l.name == r.name && l.tid <= r.tid);
}
inline bool operator>(const metareqid_t& l, const metareqid_t& r) { return !(l <= r); }
inline bool operator>=(const metareqid_t& l, const metareqid_t& r) { return !(l < r); }

namespace std {
  template<> struct hash<metareqid_t> {
    size_t operator()(const metareqid_t &r) const { 
      hash<uint64_t> H;
      return H(r.name.num()) ^ H(r.name.type()) ^ H(r.tid);
    }
  };
} // namespace std

// cap info for client reconnect
struct cap_reconnect_t {
  cap_reconnect_t() {}
  cap_reconnect_t(uint64_t cap_id, inodeno_t pino, std::string_view p, int w, int i,
		  inodeno_t sr, snapid_t sf, ceph::buffer::list& lb) :
    path(p) {
    capinfo.cap_id = cap_id;
    capinfo.wanted = w;
    capinfo.issued = i;
    capinfo.snaprealm = sr;
    capinfo.pathbase = pino;
    capinfo.flock_len = 0;
    snap_follows = sf;
    flockbl = std::move(lb);
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void encode_old(ceph::buffer::list& bl) const;
  void decode_old(ceph::buffer::list::const_iterator& bl);

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cap_reconnect_t*>& ls);

  std::string path;
  mutable ceph_mds_cap_reconnect capinfo = {};
  snapid_t snap_follows = 0;
  ceph::buffer::list flockbl;
};
WRITE_CLASS_ENCODER(cap_reconnect_t)

struct snaprealm_reconnect_t {
  snaprealm_reconnect_t() {}
  snaprealm_reconnect_t(inodeno_t ino, snapid_t seq, inodeno_t parent) {
    realm.ino = ino;
    realm.seq = seq;
    realm.parent = parent;
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void encode_old(ceph::buffer::list& bl) const;
  void decode_old(ceph::buffer::list::const_iterator& bl);

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<snaprealm_reconnect_t*>& ls);

  mutable ceph_mds_snaprealm_reconnect realm = {};
};
WRITE_CLASS_ENCODER(snaprealm_reconnect_t)

// compat for pre-FLOCK feature
struct old_ceph_mds_cap_reconnect {
	ceph_le64 cap_id;
	ceph_le32 wanted;
	ceph_le32 issued;
  ceph_le64 old_size;
  struct ceph_timespec old_mtime, old_atime;
	ceph_le64 snaprealm;
	ceph_le64 pathbase;        /* base ino for our path to this ino */
} __attribute__ ((packed));
WRITE_RAW_ENCODER(old_ceph_mds_cap_reconnect)

struct old_cap_reconnect_t {
  const old_cap_reconnect_t& operator=(const cap_reconnect_t& n) {
    path = n.path;
    capinfo.cap_id = n.capinfo.cap_id;
    capinfo.wanted = n.capinfo.wanted;
    capinfo.issued = n.capinfo.issued;
    capinfo.snaprealm = n.capinfo.snaprealm;
    capinfo.pathbase = n.capinfo.pathbase;
    return *this;
  }
  operator cap_reconnect_t() {
    cap_reconnect_t n;
    n.path = path;
    n.capinfo.cap_id = capinfo.cap_id;
    n.capinfo.wanted = capinfo.wanted;
    n.capinfo.issued = capinfo.issued;
    n.capinfo.snaprealm = capinfo.snaprealm;
    n.capinfo.pathbase = capinfo.pathbase;
    return n;
  }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(path, bl);
    encode(capinfo, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(path, bl);
    decode(capinfo, bl);
  }

  std::string path;
  old_ceph_mds_cap_reconnect capinfo;
};
WRITE_CLASS_ENCODER(old_cap_reconnect_t)

// dir frag
struct dirfrag_t {
  dirfrag_t() {}
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f) { }

  void print(std::ostream& out) const {
    out << ino;
    if (!frag.is_root()) {
      out << "." << frag;
    }
  }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(ino, bl);
    encode(frag, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(ino, bl);
    decode(frag, bl);
  }

  inodeno_t ino = 0;
  frag_t frag;
};
WRITE_CLASS_ENCODER(dirfrag_t)

inline bool operator<(dirfrag_t l, dirfrag_t r) {
  if (l.ino < r.ino) return true;
  if (l.ino == r.ino && l.frag < r.frag) return true;
  return false;
}
inline bool operator==(dirfrag_t l, dirfrag_t r) {
  return l.ino == r.ino && l.frag == r.frag;
}

namespace std {
  template<> struct hash<dirfrag_t> {
    size_t operator()(const dirfrag_t &df) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(df.ino) ^ I(df.frag);
    }
  };
} // namespace std

// ================================================================
#define META_POP_IRD     0
#define META_POP_IWR     1
#define META_POP_READDIR 2
#define META_POP_FETCH   3
#define META_POP_STORE   4
#define META_NPOP        5

class inode_load_vec_t {
public:
  using time = DecayCounter::time;
  using clock = DecayCounter::clock;
  static const size_t NUM = 2;

  inode_load_vec_t() : vec{DecayCounter(DecayRate()), DecayCounter(DecayRate())} {}
  inode_load_vec_t(const DecayRate &rate) : vec{DecayCounter(rate), DecayCounter(rate)} {}

  DecayCounter &get(int t) {
    return vec[t];
  }
  void zero() {
    for (auto &d : vec) {
      d.reset();
    }
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<inode_load_vec_t*>& ls);

private:
  std::array<DecayCounter, NUM> vec;
};
inline void encode(const inode_load_vec_t &c, ceph::buffer::list &bl) {
  c.encode(bl);
}
inline void decode(inode_load_vec_t & c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}

class dirfrag_load_vec_t {
public:
  using time = DecayCounter::time;
  using clock = DecayCounter::clock;
  static const size_t NUM = 5;

  dirfrag_load_vec_t() :
      vec{DecayCounter(DecayRate()),
          DecayCounter(DecayRate()),
          DecayCounter(DecayRate()),
          DecayCounter(DecayRate()),
          DecayCounter(DecayRate())
         }
  {}
  dirfrag_load_vec_t(const DecayRate &rate) : 
      vec{DecayCounter(rate), DecayCounter(rate), DecayCounter(rate), DecayCounter(rate), DecayCounter(rate)}
  {}

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(2, 2, bl);
    for (const auto &i : vec) {
      encode(i, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
    for (auto &i : vec) {
      decode(i, p);
    }
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  void dump(ceph::Formatter *f, const DecayRate& rate) const;
  void print(std::ostream& out) const {
    CachedStackStringStream css;
    *css << std::setprecision(1) << std::fixed
         << "[pop"
            " IRD:" << vec[0]
         << " IWR:" << vec[1]
         << " RDR:" << vec[2]
         << " FET:" << vec[3]
         << " STR:" << vec[4]
         << " *LOAD:" << meta_load() << "]";
    out << css->strv();
  }
  static void generate_test_instances(std::list<dirfrag_load_vec_t*>& ls);

  const DecayCounter &get(int t) const {
    return vec[t];
  }
  DecayCounter &get(int t) {
    return vec[t];
  }
  void adjust(double d) {
    for (auto &i : vec) {
      i.adjust(d);
    }
  }
  void zero() {
    for (auto &i : vec) {
      i.reset();
    }
  }
  double meta_load() const {
    return 
      1*vec[META_POP_IRD].get() + 
      2*vec[META_POP_IWR].get() +
      1*vec[META_POP_READDIR].get() +
      2*vec[META_POP_FETCH].get() +
      4*vec[META_POP_STORE].get();
  }

  void add(dirfrag_load_vec_t& r) {
    for (size_t i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(r.vec[i].get());
  }
  void sub(dirfrag_load_vec_t& r) {
    for (size_t i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(-r.vec[i].get());
  }
  void scale(double f) {
    for (size_t i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].scale(f);
  }

private:
  std::array<DecayCounter, NUM> vec;
};

inline void encode(const dirfrag_load_vec_t &c, ceph::buffer::list &bl) {
  c.encode(bl);
}
inline void decode(dirfrag_load_vec_t& c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}

struct mds_load_t {
  using clock = dirfrag_load_vec_t::clock;
  using time = dirfrag_load_vec_t::time;

  dirfrag_load_vec_t auth;
  dirfrag_load_vec_t all;

  mds_load_t() : auth(DecayRate()), all(DecayRate()) {}
  mds_load_t(const DecayRate &rate) : auth(rate), all(rate) {}

  void print(std::ostream& out) const {
    out << "mdsload<" << auth << "/" << all
        << ", req " << req_rate
        << ", hr " << cache_hit_rate
        << ", qlen " << queue_len
	<< ", cpu " << cpu_load_avg
        << ">";
  }

  double req_rate = 0.0;
  double cache_hit_rate = 0.0;
  double queue_len = 0.0;

  double cpu_load_avg = 0.0;

  double mds_load(int64_t bal_mode) const;  // defiend in MDBalancer.cc
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<mds_load_t*>& ls);
};
inline void encode(const mds_load_t &c, ceph::buffer::list &bl) {
  c.encode(bl);
}
inline void decode(mds_load_t &c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}

// ================================================================
typedef std::pair<mds_rank_t, mds_rank_t> mds_authority_t;

// -- authority delegation --
// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   mds_rank_t(-1)   // default
#define CDIR_AUTH_UNKNOWN  mds_rank_t(-2)
#define CDIR_AUTH_DEFAULT  mds_authority_t(CDIR_AUTH_PARENT, CDIR_AUTH_UNKNOWN)
#define CDIR_AUTH_UNDEF    mds_authority_t(CDIR_AUTH_UNKNOWN, CDIR_AUTH_UNKNOWN)
//#define CDIR_AUTH_ROOTINODE pair<int,int>( 0, -2)

class MDSCacheObjectInfo {
public:
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const {
    if (ino) {
      out << ino << "." << snapid;
    } else if (dname.length()) {
      out << dirfrag << "/" << dname
          << " snap " << snapid;
    } else {
      out << dirfrag;
    }
  }
  static void generate_test_instances(std::list<MDSCacheObjectInfo*>& ls);

  inodeno_t ino = 0;
  dirfrag_t dirfrag;
  std::string dname;
  snapid_t snapid;
};

inline bool operator==(const MDSCacheObjectInfo& l, const MDSCacheObjectInfo& r) {
  if (l.ino || r.ino)
    return l.ino == r.ino && l.snapid == r.snapid;
  else
    return l.dirfrag == r.dirfrag && l.dname == r.dname;
}
WRITE_CLASS_ENCODER(MDSCacheObjectInfo)

#endif
