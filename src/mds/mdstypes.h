// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDSTYPES_H
#define CEPH_MDSTYPES_H

#include "include/int_types.h"

#include <math.h>
#include <ostream>
#include <set>
#include <map>
#include <string_view>

#include "common/config.h"
#include "common/Clock.h"
#include "common/DecayCounter.h"
#include "common/entity_name.h"

#include "include/Context.h"
#include "include/frag.h"
#include "include/xlist.h"
#include "include/interval_set.h"
#include "include/compact_map.h"
#include "include/compact_set.h"
#include "include/fs_types.h"

#include "inode_backtrace.h"

#include <boost/spirit/include/qi.hpp>
#include <boost/pool/pool.hpp>
#include "include/ceph_assert.h"
#include <boost/serialization/strong_typedef.hpp>

#define CEPH_FS_ONDISK_MAGIC "ceph fs volume v011"

#define MDS_PORT_CACHE   0x200
#define MDS_PORT_LOCKER  0x300
#define MDS_PORT_MIGRATOR 0x400

#define MAX_MDS                   0x100
#define NUM_STRAY                 10

#define MDS_INO_ROOT              1

// No longer created but recognised in existing filesystems
// so that we don't try to fragment it.
#define MDS_INO_CEPH              2

#define MDS_INO_GLOBAL_SNAPREALM  3

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
#define MDS_INO_IS_BASE(i)   ((i) == MDS_INO_ROOT || (i) == MDS_INO_GLOBAL_SNAPREALM || MDS_INO_IS_MDSDIR(i))
#define MDS_INO_STRAY_OWNER(i) (signed (((unsigned (i)) - MDS_INO_STRAY_OFFSET) / NUM_STRAY))
#define MDS_INO_STRAY_INDEX(i) (((unsigned (i)) - MDS_INO_STRAY_OFFSET) % NUM_STRAY)

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.


typedef int32_t mds_rank_t;
constexpr mds_rank_t MDS_RANK_NONE = -1;

BOOST_STRONG_TYPEDEF(uint64_t, mds_gid_t)
extern const mds_gid_t MDS_GID_NONE;

typedef int32_t fs_cluster_id_t;
constexpr fs_cluster_id_t FS_CLUSTER_ID_NONE = -1;
// The namespace ID of the anonymous default filesystem from legacy systems
constexpr fs_cluster_id_t FS_CLUSTER_ID_ANONYMOUS = 0;

class mds_role_t
{
  public:
  fs_cluster_id_t fscid;
  mds_rank_t rank;

  mds_role_t(fs_cluster_id_t fscid_, mds_rank_t rank_)
    : fscid(fscid_), rank(rank_)
  {}
  mds_role_t()
    : fscid(FS_CLUSTER_ID_NONE), rank(MDS_RANK_NONE)
  {}
  bool operator<(mds_role_t const &rhs) const
  {
    if (fscid < rhs.fscid) {
      return true;
    } else if (fscid == rhs.fscid) {
      return rank < rhs.rank;
    } else {
      return false;
    }
  }

  bool is_none() const
  {
    return (rank == MDS_RANK_NONE);
  }
};
std::ostream& operator<<(std::ostream &out, const mds_role_t &role);


// CAPS

inline string gcap_string(int cap)
{
  string s;
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
inline string ccap_string(int cap)
{
  string s;
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


struct scatter_info_t {
  version_t version = 0;

  scatter_info_t() {}
};

struct frag_info_t : public scatter_info_t {
  // this frag
  utime_t mtime;
  uint64_t change_attr = 0;
  int64_t nfiles = 0;        // files
  int64_t nsubdirs = 0;      // subdirs

  frag_info_t() {}

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

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<frag_info_t*>& ls);
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
  // this frag + children
  utime_t rctime;
  int64_t rbytes = 0;
  int64_t rfiles = 0;
  int64_t rsubdirs = 0;
  int64_t rsize() const { return rfiles + rsubdirs; }

  int64_t rsnaps = 0;

  nest_info_t() {}

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

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<nest_info_t*>& ls);
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
  inodeno_t ino;
  snapid_t snapid;
  vinodeno_t() {}
  vinodeno_t(inodeno_t i, snapid_t s) : ino(i), snapid(s) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(ino, bl);
    encode(snapid, bl);
  }
  void decode(bufferlist::const_iterator& p) {
    using ceph::decode;
    decode(ino, p);
    decode(snapid, p);
  }
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

struct quota_info_t
{
  int64_t max_bytes = 0;
  int64_t max_files = 0;
 
  quota_info_t() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_bytes, bl);
    encode(max_files, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
    decode(max_bytes, p);
    decode(max_files, p);
    DECODE_FINISH(p);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<quota_info_t *>& ls);

  bool is_valid() const {
    return max_bytes >=0 && max_files >=0;
  }
  bool is_enable() const {
    return max_bytes || max_files;
  }
};
WRITE_CLASS_ENCODER(quota_info_t)

inline bool operator==(const quota_info_t &l, const quota_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

ostream& operator<<(ostream &out, const quota_info_t &n);

namespace std {
  template<> struct hash<vinodeno_t> {
    size_t operator()(const vinodeno_t &vino) const { 
      hash<inodeno_t> H;
      hash<uint64_t> I;
      return H(vino.ino) ^ I(vino.snapid);
    }
  };
} // namespace std




inline std::ostream& operator<<(std::ostream &out, const vinodeno_t &vino) {
  out << vino.ino;
  if (vino.snapid == CEPH_NOSNAP)
    out << ".head";
  else if (vino.snapid)
    out << '.' << vino.snapid;
  return out;
}


/*
 * client_writeable_range_t
 */
struct client_writeable_range_t {
  struct byte_range_t {
    uint64_t first = 0, last = 0;    // interval client can write to
    byte_range_t() {}
  };

  byte_range_t range;
  snapid_t follows = 0;     // aka "data+metadata flushed thru"

  client_writeable_range_t() {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<client_writeable_range_t*>& ls);
};

inline void decode(client_writeable_range_t::byte_range_t& range, bufferlist::const_iterator& bl) {
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
private:
  std::unique_ptr<bufferlist> blp;
public:
  version_t version = 1;

  void free_data() {
    blp.reset();
  }
  bufferlist& get_data() {
    if (!blp)
      blp.reset(new bufferlist);
    return *blp;
  }
  size_t length() const { return blp ? blp->length() : 0; }

  inline_data_t() {}
  inline_data_t(const inline_data_t& o) : version(o.version) {
    if (o.blp)
      get_data() = *o.blp;
  }
  inline_data_t& operator=(const inline_data_t& o) {
    version = o.version;
    if (o.blp)
      get_data() = *o.blp;
    else
      free_data();
    return *this;
  }
  bool operator==(const inline_data_t& o) const {
   return length() == o.length() &&
	  (length() == 0 ||
	   (*const_cast<bufferlist*>(blp.get()) == *const_cast<bufferlist*>(o.blp.get())));
  }
  bool operator!=(const inline_data_t& o) const {
    return !(*this == o);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
};
WRITE_CLASS_ENCODER(inline_data_t)

enum {
  DAMAGE_STATS,     // statistics (dirstat, size, etc)
  DAMAGE_RSTATS,    // recursive statistics (rstat, accounted_rstat)
  DAMAGE_FRAGTREE   // fragtree -- repair by searching
};
typedef uint32_t damage_flags_t;

/*
 * inode_t
 */
template<template<typename> class Allocator = std::allocator>
struct inode_t {
  /**
   * ***************
   * Do not forget to add any new fields to the compare() function.
   * ***************
   */
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
  ceph_dir_layout  dir_layout;    // [dir only]
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

  using client_range_map = std::map<client_t,client_writeable_range_t,std::less<client_t>,Allocator<std::pair<const client_t,client_writeable_range_t>>>;
  client_range_map client_ranges;  // client(s) can write to these ranges

  // dirfrag, recursive accountin
  frag_info_t dirstat;         // protected by my filelock
  nest_info_t rstat;           // protected by my nestlock
  nest_info_t accounted_rstat; // protected by parent's nestlock

  quota_info_t quota;

  mds_rank_t export_pin = MDS_RANK_NONE;
 
  // special stuff
  version_t version = 0;           // auth only
  version_t file_data_version = 0; // auth only
  version_t xattr_version = 0;

  utime_t last_scrub_stamp;    // start time of last complete scrub
  version_t last_scrub_version = 0;// (parent) start version of last complete scrub

  version_t backtrace_version = 0;

  snapid_t oldest_snap;

  std::basic_string<char,std::char_traits<char>,Allocator<char>> stray_prior_path; //stores path before unlink

  inode_t()
  {
    clear_layout();
    memset(&dir_layout, 0, sizeof(dir_layout));
  }

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool is_truncating() const { return (truncate_pending > 0); }
  void truncate(uint64_t old_size, uint64_t new_size) {
    ceph_assert(new_size < old_size);
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

  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
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
private:
  bool older_is_consistent(const inode_t &other) const;
};

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void inode_t<Allocator>::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(15, 6, bl);

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

  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::decode(bufferlist::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(15, 6, 6, p);

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
  else
    memset(&dir_layout, 0, sizeof(dir_layout));
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
    map<client_t, client_writeable_range_t::byte_range_t> m;
    decode(m, p);
    for (map<client_t, client_writeable_range_t::byte_range_t>::iterator
	q = m.begin(); q != m.end(); ++q)
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

  DECODE_FINISH(p);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::dump(Formatter *f) const
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
inline void encode(const inode_t<Allocator> &c, ::ceph::bufferlist &bl, uint64_t features)
{
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<template<typename> class Allocator>
inline void decode(inode_t<Allocator> &c, ::ceph::bufferlist::const_iterator &p)
{
  c.decode(p);
}

template<template<typename> class Allocator>
using alloc_string = std::basic_string<char,std::char_traits<char>,Allocator<char>>;

template<template<typename> class Allocator>
using xattr_map = compact_map<alloc_string<Allocator>, bufferptr, std::less<alloc_string<Allocator>>, Allocator<std::pair<const alloc_string<Allocator>, bufferptr>>>; // FIXME bufferptr not in mempool

/*
 * old_inode_t
 */
template<template<typename> class Allocator = std::allocator>
struct old_inode_t {
  snapid_t first;
  inode_t<Allocator> inode;
  xattr_map<Allocator> xattrs;

  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<old_inode_t*>& ls);
};

// These methods may be moved back to mdstypes.cc when we have pmr
template<template<typename> class Allocator>
void old_inode_t<Allocator>::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(first, bl);
  encode(inode, bl, features);
  encode(xattrs, bl);
  ENCODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(first, bl);
  decode(inode, bl);
  decode(xattrs, bl);
  DECODE_FINISH(bl);
}

template<template<typename> class Allocator>
void old_inode_t<Allocator>::dump(Formatter *f) const
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
  ls.back()->xattrs["user.foo"] = buffer::copy("asdf", 4);
  ls.back()->xattrs["user.unprintable"] = buffer::copy("\000\001\002", 3);
}

template<template<typename> class Allocator>
inline void encode(const old_inode_t<Allocator> &c, ::ceph::bufferlist &bl, uint64_t features)
{
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template<template<typename> class Allocator>
inline void decode(old_inode_t<Allocator> &c, ::ceph::bufferlist::const_iterator &p)
{
  c.decode(p);
}


/*
 * like an inode, but for a dir frag 
 */
struct fnode_t {
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

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<fnode_t*>& ls);
  fnode_t() {}
};
WRITE_CLASS_ENCODER(fnode_t)


struct old_rstat_t {
  snapid_t first;
  nest_info_t rstat, accounted_rstat;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<old_rstat_t*>& ls);
};
WRITE_CLASS_ENCODER(old_rstat_t)

inline std::ostream& operator<<(std::ostream& out, const old_rstat_t& o) {
  return out << "old_rstat(first " << o.first << " " << o.rstat << " " << o.accounted_rstat << ")";
}

/*
 * feature_bitset_t
 */
class feature_bitset_t {
public:
  typedef uint64_t block_type;
  static const size_t bits_per_block = sizeof(block_type) * 8;

  feature_bitset_t(const feature_bitset_t& other) : _vec(other._vec) {}
  feature_bitset_t(feature_bitset_t&& other) : _vec(std::move(other._vec)) {}
  feature_bitset_t(unsigned long value = 0);
  feature_bitset_t(const vector<size_t>& array);
  feature_bitset_t& operator=(const feature_bitset_t& other) {
    _vec = other._vec;
    return *this;
  }
  feature_bitset_t& operator=(feature_bitset_t&& other) {
    _vec = std::move(other._vec);
    return *this;
  }
  bool empty() const {
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
  void clear() {
    _vec.clear();
  }
  feature_bitset_t& operator-=(const feature_bitset_t& other);
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator &p);
  void print(ostream& out) const;
private:
  vector<block_type> _vec;
};
WRITE_CLASS_ENCODER(feature_bitset_t)

inline std::ostream& operator<<(std::ostream& out, const feature_bitset_t& s) {
  s.print(out);
  return out;
}

/*
 * client_metadata_t
 */
struct client_metadata_t {
  using kv_map_t = std::map<std::string,std::string>;
  using iterator = kv_map_t::const_iterator;

  kv_map_t kv_map;
  feature_bitset_t features;

  client_metadata_t() {}
  client_metadata_t(const client_metadata_t& other) :
    kv_map(other.kv_map), features(other.features) {}
  client_metadata_t(client_metadata_t&& other) :
    kv_map(std::move(other.kv_map)), features(std::move(other.features)) {}
  client_metadata_t(kv_map_t&& kv, feature_bitset_t &&f) :
    kv_map(std::move(kv)), features(std::move(f)) {}
  client_metadata_t(const kv_map_t& kv, const feature_bitset_t &f) :
    kv_map(kv), features(f) {}
  client_metadata_t& operator=(const client_metadata_t& other) {
    kv_map = other.kv_map;
    features = other.features;
    return *this;
  }

  bool empty() const { return kv_map.empty() && features.empty(); }
  iterator find(const std::string& key) const { return kv_map.find(key); }
  iterator begin() const { return kv_map.begin(); }
  iterator end() const { return kv_map.end(); }
  std::string& operator[](const std::string& key) { return kv_map[key]; }
  void merge(const client_metadata_t& other) {
    kv_map.insert(other.kv_map.begin(), other.kv_map.end());
    features = other.features;
  }
  void clear() {
    kv_map.clear();
    features.clear();
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(client_metadata_t)

/*
 * session_info_t
 */
struct session_info_t {
  entity_inst_t inst;
  std::map<ceph_tid_t,inodeno_t> completed_requests;
  interval_set<inodeno_t> prealloc_inos;   // preallocated, ready to use.
  interval_set<inodeno_t> used_inos;       // journaling use
  client_metadata_t client_metadata;
  std::set<ceph_tid_t> completed_flushes;
  EntityName auth_name;

  client_t get_client() const { return client_t(inst.name.num()); }
  bool has_feature(size_t bit) const { return client_metadata.features.test(bit); }
  const entity_name_t& get_source() const { return inst.name; }

  void clear_meta() {
    prealloc_inos.clear();
    used_inos.clear();
    completed_requests.clear();
    completed_flushes.clear();
    client_metadata.clear();
  }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<session_info_t*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(session_info_t)


// =======
// dentries

struct dentry_key_t {
  snapid_t snapid = 0;
  std::string_view name;
  __u32 hash = 0;
  dentry_key_t() {}
  dentry_key_t(snapid_t s, std::string_view n, __u32 h=0) :
    snapid(s), name(n), hash(h) {}

  bool is_valid() { return name.length() || snapid; }

  // encode into something that can be decoded as a string.
  // name_ (head) or name_%x (!head)
  void encode(bufferlist& bl) const {
    string key;
    encode(key);
    using ceph::encode;
    encode(key, bl);
  }
  void encode(string& key) const {
    char b[20];
    if (snapid != CEPH_NOSNAP) {
      uint64_t val(snapid);
      snprintf(b, sizeof(b), "%" PRIx64, val);
    } else {
      snprintf(b, sizeof(b), "%s", "head");
    }
    ostringstream oss;
    oss << name << "_" << b;
    key = oss.str();
  }
  static void decode_helper(bufferlist::const_iterator& bl, string& nm, snapid_t& sn) {
    string key;
    decode(key, bl);
    decode_helper(key, nm, sn);
  }
  static void decode_helper(std::string_view key, string& nm, snapid_t& sn) {
    size_t i = key.find_last_of('_');
    ceph_assert(i != string::npos);
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
};

inline std::ostream& operator<<(std::ostream& out, const dentry_key_t &k)
{
  return out << "(" << k.name << "," << k.snapid << ")";
}

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
  string name;
  snapid_t snapid;
  string_snap_t() {}
  string_snap_t(std::string_view n, snapid_t s) : name(n), snapid(s) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<string_snap_t*>& ls);
};
WRITE_CLASS_ENCODER(string_snap_t)

inline bool operator<(const string_snap_t& l, const string_snap_t& r) {
  int c = l.name.compare(r.name);
  return c < 0 || (c == 0 && l.snapid < r.snapid);
}

inline std::ostream& operator<<(std::ostream& out, const string_snap_t &k)
{
  return out << "(" << k.name << "," << k.snapid << ")";
}

/*
 * mds_table_pending_t
 *
 * mds's requesting any pending ops.  child needs to encode the corresponding
 * pending mutation state in the table.
 */
struct mds_table_pending_t {
  uint64_t reqid = 0;
  __s32 mds = 0;
  version_t tid = 0;
  mds_table_pending_t() {}
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<mds_table_pending_t*>& ls);
};
WRITE_CLASS_ENCODER(mds_table_pending_t)


// =========
// requests

struct metareqid_t {
  entity_name_t name;
  uint64_t tid = 0;
  metareqid_t() {}
  metareqid_t(entity_name_t n, ceph_tid_t t) : name(n), tid(t) {}
  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(name, bl);
    encode(tid, bl);
  }
  void decode(bufferlist::const_iterator &p) {
    using ceph::decode;
    decode(name, p);
    decode(tid, p);
  }
};
WRITE_CLASS_ENCODER(metareqid_t)

inline std::ostream& operator<<(std::ostream& out, const metareqid_t& r) {
  return out << r.name << ":" << r.tid;
}

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
  string path;
  mutable ceph_mds_cap_reconnect capinfo;
  snapid_t snap_follows;
  bufferlist flockbl;

  cap_reconnect_t() {
    memset(&capinfo, 0, sizeof(capinfo));
    snap_follows = 0;
  }
  cap_reconnect_t(uint64_t cap_id, inodeno_t pino, std::string_view p, int w, int i,
		  inodeno_t sr, snapid_t sf, bufferlist& lb) :
    path(p) {
    capinfo.cap_id = cap_id;
    capinfo.wanted = w;
    capinfo.issued = i;
    capinfo.snaprealm = sr;
    capinfo.pathbase = pino;
    capinfo.flock_len = 0;
    snap_follows = sf;
    flockbl.claim(lb);
  }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void encode_old(bufferlist& bl) const;
  void decode_old(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<cap_reconnect_t*>& ls);
};
WRITE_CLASS_ENCODER(cap_reconnect_t)

struct snaprealm_reconnect_t {
  mutable ceph_mds_snaprealm_reconnect realm;

  snaprealm_reconnect_t() {
    memset(&realm, 0, sizeof(realm));
  }
  snaprealm_reconnect_t(inodeno_t ino, snapid_t seq, inodeno_t parent) {
    realm.ino = ino;
    realm.seq = seq;
    realm.parent = parent;
  }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void encode_old(bufferlist& bl) const;
  void decode_old(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<snaprealm_reconnect_t*>& ls);
};
WRITE_CLASS_ENCODER(snaprealm_reconnect_t)

// compat for pre-FLOCK feature
struct old_ceph_mds_cap_reconnect {
	__le64 cap_id;
	__le32 wanted;
	__le32 issued;
  __le64 old_size;
  struct ceph_timespec old_mtime, old_atime;
	__le64 snaprealm;
	__le64 pathbase;        /* base ino for our path to this ino */
} __attribute__ ((packed));
WRITE_RAW_ENCODER(old_ceph_mds_cap_reconnect)

struct old_cap_reconnect_t {
  string path;
  old_ceph_mds_cap_reconnect capinfo;

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

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(path, bl);
    encode(capinfo, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(path, bl);
    decode(capinfo, bl);
  }
};
WRITE_CLASS_ENCODER(old_cap_reconnect_t)


// ================================================================
// dir frag

struct dirfrag_t {
  inodeno_t ino = 0;
  frag_t    frag;

  dirfrag_t() {}
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f) { }

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(ino, bl);
    encode(frag, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(ino, bl);
    decode(frag, bl);
  }
};
WRITE_CLASS_ENCODER(dirfrag_t)


inline std::ostream& operator<<(std::ostream& out, const dirfrag_t &df) {
  out << df.ino;
  if (!df.frag.is_root()) out << "." << df.frag;
  return out;
}
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
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<inode_load_vec_t*>& ls);

private:
  std::array<DecayCounter, NUM> vec;
};
inline void encode(const inode_load_vec_t &c, bufferlist &bl) {
  c.encode(bl);
}
inline void decode(inode_load_vec_t & c, bufferlist::const_iterator &p) {
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

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    for (const auto &i : vec) {
      encode(i, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
    for (auto &i : vec) {
      decode(i, p);
    }
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const;
  void dump(Formatter *f, const DecayRate& rate) const;
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
  friend inline std::ostream& operator<<(std::ostream& out, const dirfrag_load_vec_t& dl);
  std::array<DecayCounter, NUM> vec;
};

inline void encode(const dirfrag_load_vec_t &c, bufferlist &bl) {
  c.encode(bl);
}
inline void decode(dirfrag_load_vec_t& c, bufferlist::const_iterator &p) {
  c.decode(p);
}

inline std::ostream& operator<<(std::ostream& out, const dirfrag_load_vec_t& dl)
{
  std::ostringstream ss;
  ss << std::setprecision(1) << std::fixed
     << "[pop"
        " IRD:" << dl.vec[0]
     << " IWR:" << dl.vec[1]
     << " RDR:" << dl.vec[2]
     << " FET:" << dl.vec[3]
     << " STR:" << dl.vec[4]
     << " *LOAD:" << dl.meta_load() << "]";
  return out << ss.str() << std::endl;
}


/* mds_load_t
 * mds load
 */

struct mds_load_t {
  using clock = dirfrag_load_vec_t::clock;
  using time = dirfrag_load_vec_t::time;

  dirfrag_load_vec_t auth;
  dirfrag_load_vec_t all;

  mds_load_t() : auth(DecayRate()), all(DecayRate()) {}
  mds_load_t(const DecayRate &rate) : auth(rate), all(rate) {}

  double req_rate = 0.0;
  double cache_hit_rate = 0.0;
  double queue_len = 0.0;

  double cpu_load_avg = 0.0;

  double mds_load() const;  // defiend in MDBalancer.cc
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<mds_load_t*>& ls);
};
inline void encode(const mds_load_t &c, bufferlist &bl) {
  c.encode(bl);
}
inline void decode(mds_load_t &c, bufferlist::const_iterator &p) {
  c.decode(p);
}

inline std::ostream& operator<<(std::ostream& out, const mds_load_t& load)
{
  return out << "mdsload<" << load.auth << "/" << load.all
             << ", req " << load.req_rate 
             << ", hr " << load.cache_hit_rate
             << ", qlen " << load.queue_len
	     << ", cpu " << load.cpu_load_avg
             << ">";
}

class load_spread_t {
public:
  using time = DecayCounter::time;
  using clock = DecayCounter::clock;
  static const int MAX = 4;
  int last[MAX];
  int p = 0, n = 0;
  DecayCounter count;

public:
  load_spread_t() = delete;
  load_spread_t(const DecayRate &rate) : count(rate)
  {
    for (int i=0; i<MAX; i++)
      last[i] = -1;
  } 

  double hit(int who) {
    for (int i=0; i<n; i++)
      if (last[i] == who) 
	return count.get_last();

    // we're new(ish)
    last[p++] = who;
    if (n < MAX) n++;
    if (n == 1) return 0.0;

    if (p == MAX) p = 0;

    return count.hit();
  }
  double get() const {
    return count.get();
  }
};



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
  inodeno_t ino = 0;
  dirfrag_t dirfrag;
  string dname;
  snapid_t snapid;

  MDSCacheObjectInfo() {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<MDSCacheObjectInfo*>& ls);
};

inline std::ostream& operator<<(std::ostream& out, const MDSCacheObjectInfo &info) {
  if (info.ino) return out << info.ino << "." << info.snapid;
  if (info.dname.length()) return out << info.dirfrag << "/" << info.dname
    << " snap " << info.snapid;
  return out << info.dirfrag;
}

inline bool operator==(const MDSCacheObjectInfo& l, const MDSCacheObjectInfo& r) {
  if (l.ino || r.ino)
    return l.ino == r.ino && l.snapid == r.snapid;
  else
    return l.dirfrag == r.dirfrag && l.dname == r.dname;
}
WRITE_CLASS_ENCODER(MDSCacheObjectInfo)


// parse a map of keys/values.
namespace qi = boost::spirit::qi;

template <typename Iterator>
struct keys_and_values
  : qi::grammar<Iterator, std::map<string, string>()>
{
    keys_and_values()
      : keys_and_values::base_type(query)
    {
      query =  pair >> *(qi::lit(' ') >> pair);
      pair  =  key >> '=' >> value;
      key   =  qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z_0-9");
      value = +qi::char_("a-zA-Z_0-9");
    }
    qi::rule<Iterator, std::map<string, string>()> query;
    qi::rule<Iterator, std::pair<string, string>()> pair;
    qi::rule<Iterator, string()> key, value;
};

#endif
