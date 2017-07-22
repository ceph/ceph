// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDSTYPES_H
#define CEPH_MDSTYPES_H

#include "include/int_types.h"

#include <math.h>
#include <ostream>
#include <set>
#include <map>

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
#include "include/assert.h"
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
#define MDS_INO_IS_BASE(i)   (MDS_INO_ROOT == (i) || MDS_INO_IS_MDSDIR(i))
#define MDS_INO_STRAY_OWNER(i) (signed (((unsigned (i)) - MDS_INO_STRAY_OFFSET) / NUM_STRAY))
#define MDS_INO_STRAY_INDEX(i) (((unsigned (i)) - MDS_INO_STRAY_OFFSET) % NUM_STRAY)

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.


typedef int32_t mds_rank_t;
typedef int32_t fs_cluster_id_t;

BOOST_STRONG_TYPEDEF(uint64_t, mds_gid_t)
extern const mds_gid_t MDS_GID_NONE;
constexpr fs_cluster_id_t FS_CLUSTER_ID_NONE = {-1};
// The namespace ID of the anonymous default filesystem from legacy systems
constexpr fs_cluster_id_t FS_CLUSTER_ID_ANONYMOUS = {0};
extern const mds_rank_t MDS_RANK_NONE;

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
  version_t version;

  scatter_info_t() : version(0) {}
};

struct frag_info_t : public scatter_info_t {
  // this frag
  utime_t mtime;
  uint64_t change_attr;
  int64_t nfiles;        // files
  int64_t nsubdirs;      // subdirs

  frag_info_t() : change_attr(0), nfiles(0), nsubdirs(0) {}

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
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<frag_info_t*>& ls);
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
  int64_t rbytes;
  int64_t rfiles;
  int64_t rsubdirs;
  int64_t rsize() const { return rfiles + rsubdirs; }

  int64_t rsnaprealms;

  nest_info_t() : rbytes(0), rfiles(0), rsubdirs(0), rsnaprealms(0) {}

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
    rsnaprealms += fac*other.rsnaprealms;
  }

  // *this += cur - acc;
  void add_delta(const nest_info_t &cur, const nest_info_t &acc) {
    if (cur.rctime > rctime)
      rctime = cur.rctime;
    rbytes += cur.rbytes - acc.rbytes;
    rfiles += cur.rfiles - acc.rfiles;
    rsubdirs += cur.rsubdirs - acc.rsubdirs;
    rsnaprealms += cur.rsnaprealms - acc.rsnaprealms;
  }

  bool same_sums(const nest_info_t &o) const {
    return rctime <= o.rctime &&
        rbytes == o.rbytes &&
        rfiles == o.rfiles &&
        rsubdirs == o.rsubdirs &&
        rsnaprealms == o.rsnaprealms;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<nest_info_t*>& ls);
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
    ::encode(ino, bl);
    ::encode(snapid, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(ino, p);
    ::decode(snapid, p);
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
  int64_t max_bytes;
  int64_t max_files;
 
  quota_info_t() : max_bytes(0), max_files(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_bytes, bl);
    ::encode(max_files, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
    ::decode(max_bytes, p);
    ::decode(max_files, p);
    DECODE_FINISH(p);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<quota_info_t *>& ls);

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
    uint64_t first, last;    // interval client can write to
    byte_range_t() : first(0), last(0) {}
  };

  byte_range_t range;
  snapid_t follows;     // aka "data+metadata flushed thru"

  client_writeable_range_t() : follows(0) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<client_writeable_range_t*>& ls);
};

inline void decode(client_writeable_range_t::byte_range_t& range, bufferlist::iterator& bl) {
  ::decode(range.first, bl);
  ::decode(range.last, bl);
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
  version_t version;

  void free_data() {
    blp.reset();
  }
  bufferlist& get_data() {
    if (!blp)
      blp.reset(new bufferlist);
    return *blp;
  }
  size_t length() const { return blp ? blp->length() : 0; }

  inline_data_t() : version(1) {}
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
  void decode(bufferlist::iterator& bl);
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
struct inode_t {
  /**
   * ***************
   * Do not forget to add any new fields to the compare() function.
   * ***************
   */
  // base (immutable)
  inodeno_t ino;
  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time
  utime_t    btime;   // birth time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;  

  // file (data access)
  ceph_dir_layout  dir_layout;    // [dir only]
  file_layout_t layout;
  compact_set <int64_t> old_pools;
  uint64_t   size;        // on directory, # dentries
  uint64_t   max_size_ever; // max size the file has ever been
  uint32_t   truncate_seq;
  uint64_t   truncate_size, truncate_from;
  uint32_t   truncate_pending;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())
  inline_data_t inline_data;

  // change attribute
  uint64_t   change_attr;

  std::map<client_t,client_writeable_range_t> client_ranges;  // client(s) can write to these ranges

  // dirfrag, recursive accountin
  frag_info_t dirstat;         // protected by my filelock
  nest_info_t rstat;           // protected by my nestlock
  nest_info_t accounted_rstat; // protected by parent's nestlock

  quota_info_t quota;

  mds_rank_t export_pin;
 
  // special stuff
  version_t version;           // auth only
  version_t file_data_version; // auth only
  version_t xattr_version;

  utime_t last_scrub_stamp;    // start time of last complete scrub
  version_t last_scrub_version;// (parent) start version of last complete scrub

  version_t backtrace_version;

  snapid_t oldest_snap;

  string stray_prior_path; //stores path before unlink

  inode_t() : ino(0), rdev(0),
	      mode(0), uid(0), gid(0), nlink(0),
	      size(0), max_size_ever(0),
	      truncate_seq(0), truncate_size(0), truncate_from(0),
	      truncate_pending(0),
	      time_warp_seq(0), change_attr(0),
              export_pin(MDS_RANK_NONE),
	      version(0), file_data_version(0), xattr_version(0),
	      last_scrub_version(0), backtrace_version(0) {
    clear_layout();
    memset(&dir_layout, 0, sizeof(dir_layout));
    memset(&quota, 0, sizeof(quota));
  }

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool is_truncating() const { return (truncate_pending > 0); }
  void truncate(uint64_t old_size, uint64_t new_size) {
    assert(new_size < old_size);
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
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<inode_t*>& ls);
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
WRITE_CLASS_ENCODER_FEATURES(inode_t)


/*
 * old_inode_t
 */
struct old_inode_t {
  snapid_t first;
  inode_t inode;
  std::map<string,bufferptr> xattrs;

  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<old_inode_t*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(old_inode_t)


/*
 * like an inode, but for a dir frag 
 */
struct fnode_t {
  version_t version;
  snapid_t snap_purged_thru;   // the max_last_destroy snapid we've been purged thru
  frag_info_t fragstat, accounted_fragstat;
  nest_info_t rstat, accounted_rstat;
  damage_flags_t damage_flags;

  // we know we and all our descendants have been scrubbed since this version
  version_t recursive_scrub_version;
  utime_t recursive_scrub_stamp;
  // version at which we last scrubbed our personal data structures
  version_t localized_scrub_version;
  utime_t localized_scrub_stamp;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<fnode_t*>& ls);
  fnode_t() : version(0), damage_flags(0),
	      recursive_scrub_version(0), localized_scrub_version(0) {}
};
WRITE_CLASS_ENCODER(fnode_t)


struct old_rstat_t {
  snapid_t first;
  nest_info_t rstat, accounted_rstat;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<old_rstat_t*>& ls);
};
WRITE_CLASS_ENCODER(old_rstat_t)

inline std::ostream& operator<<(std::ostream& out, const old_rstat_t& o) {
  return out << "old_rstat(first " << o.first << " " << o.rstat << " " << o.accounted_rstat << ")";
}


/*
 * session_info_t
 */

struct session_info_t {
  entity_inst_t inst;
  std::map<ceph_tid_t,inodeno_t> completed_requests;
  interval_set<inodeno_t> prealloc_inos;   // preallocated, ready to use.
  interval_set<inodeno_t> used_inos;       // journaling use
  std::map<std::string, std::string> client_metadata;
  std::set<ceph_tid_t> completed_flushes;
  EntityName auth_name;

  client_t get_client() const { return client_t(inst.name.num()); }
  const entity_name_t& get_source() const { return inst.name; }

  void clear_meta() {
    prealloc_inos.clear();
    used_inos.clear();
    completed_requests.clear();
    completed_flushes.clear();
  }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<session_info_t*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(session_info_t)


// =======
// dentries

struct dentry_key_t {
  snapid_t snapid;
  const char *name;
  __u32 hash;
  dentry_key_t() : snapid(0), name(0), hash(0) {}
  dentry_key_t(snapid_t s, const char *n, __u32 h=0) :
    snapid(s), name(n), hash(h) {}

  bool is_valid() { return name || snapid; }

  // encode into something that can be decoded as a string.
  // name_ (head) or name_%x (!head)
  void encode(bufferlist& bl) const {
    string key;
    encode(key);
    ::encode(key, bl);
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
  static void decode_helper(bufferlist::iterator& bl, string& nm, snapid_t& sn) {
    string key;
    ::decode(key, bl);
    decode_helper(key, nm, sn);
  }
  static void decode_helper(const string& key, string& nm, snapid_t& sn) {
    size_t i = key.find_last_of('_');
    assert(i != string::npos);
    if (key.compare(i+1, string::npos, "head") == 0) {
      // name_head
      sn = CEPH_NOSNAP;
    } else {
      // name_%x
      long long unsigned x = 0;
      sscanf(key.c_str() + i + 1, "%llx", &x);
      sn = x;
    }  
    nm = string(key.c_str(), i);
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
  c = strcmp(k1.name, k2.name);
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
  string_snap_t(const string& n, snapid_t s) : name(n), snapid(s) {}
  string_snap_t(const char *n, snapid_t s) : name(n), snapid(s) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<string_snap_t*>& ls);
};
WRITE_CLASS_ENCODER(string_snap_t)

inline bool operator<(const string_snap_t& l, const string_snap_t& r) {
  int c = strcmp(l.name.c_str(), r.name.c_str());
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
  uint64_t reqid;
  __s32 mds;
  version_t tid;
  mds_table_pending_t() : reqid(0), mds(0), tid(0) {}
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<mds_table_pending_t*>& ls);
};
WRITE_CLASS_ENCODER(mds_table_pending_t)


// =========
// requests

struct metareqid_t {
  entity_name_t name;
  uint64_t tid;
  metareqid_t() : tid(0) {}
  metareqid_t(entity_name_t n, ceph_tid_t t) : name(n), tid(t) {}
  void encode(bufferlist& bl) const {
    ::encode(name, bl);
    ::encode(tid, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode(name, p);
    ::decode(tid, p);
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
  cap_reconnect_t(uint64_t cap_id, inodeno_t pino, const string& p, int w, int i,
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
  void decode(bufferlist::iterator& bl);
  void encode_old(bufferlist& bl) const;
  void decode_old(bufferlist::iterator& bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cap_reconnect_t*>& ls);
};
WRITE_CLASS_ENCODER(cap_reconnect_t)


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
    ::encode(path, bl);
    ::encode(capinfo, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(path, bl);
    ::decode(capinfo, bl);
  }
};
WRITE_CLASS_ENCODER(old_cap_reconnect_t)


// ================================================================
// dir frag

struct dirfrag_t {
  inodeno_t ino;
  frag_t    frag;

  dirfrag_t() : ino(0) { }
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f) { }

  void encode(bufferlist& bl) const {
    ::encode(ino, bl);
    ::encode(frag, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(ino, bl);
    ::decode(frag, bl);
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
  static const int NUM = 2;
  std::vector < DecayCounter > vec;
public:
  explicit inode_load_vec_t(const utime_t &now)
     : vec(NUM, DecayCounter(now))
  {}
  // for dencoder infrastructure
  inode_load_vec_t() :
    vec(NUM, DecayCounter())
  {}
  DecayCounter &get(int t) { 
    assert(t < NUM);
    return vec[t]; 
  }
  void zero(utime_t now) {
    for (int i=0; i<NUM; i++) 
      vec[i].reset(now);
  }
  void encode(bufferlist &bl) const;
  void decode(const utime_t &t, bufferlist::iterator &p);
  // for dencoder
  void decode(bufferlist::iterator& p) { utime_t sample; decode(sample, p); }
  void dump(Formatter *f);
  static void generate_test_instances(list<inode_load_vec_t*>& ls);
};
inline void encode(const inode_load_vec_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(inode_load_vec_t & c, const utime_t &t, bufferlist::iterator &p) {
  c.decode(t, p);
}
// for dencoder
inline void decode(inode_load_vec_t & c, bufferlist::iterator &p) {
  utime_t sample;
  c.decode(sample, p);
}

class dirfrag_load_vec_t {
public:
  static const int NUM = 5;
  std::vector < DecayCounter > vec;
  explicit dirfrag_load_vec_t(const utime_t &now)
     : vec(NUM, DecayCounter(now))
  { }
  // for dencoder infrastructure
  dirfrag_load_vec_t()
    : vec(NUM, DecayCounter())
  {}
  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    for (int i=0; i<NUM; i++)
      ::encode(vec[i], bl);
    ENCODE_FINISH(bl);
  }
  void decode(const utime_t &t, bufferlist::iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
    for (int i=0; i<NUM; i++)
      ::decode(vec[i], t, p);
    DECODE_FINISH(p);
  }
  // for dencoder infrastructure
  void decode(bufferlist::iterator& p) {
    utime_t sample;
    decode(sample, p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<dirfrag_load_vec_t*>& ls);

  DecayCounter &get(int t) { 
    assert(t < NUM);
    return vec[t]; 
  }
  void adjust(utime_t now, const DecayRate& rate, double d) {
    for (int i=0; i<NUM; i++) 
      vec[i].adjust(now, rate, d);
  }
  void zero(utime_t now) {
    for (int i=0; i<NUM; i++) 
      vec[i].reset(now);
  }
  double meta_load(utime_t now, const DecayRate& rate) {
    return 
      1*vec[META_POP_IRD].get(now, rate) + 
      2*vec[META_POP_IWR].get(now, rate) +
      1*vec[META_POP_READDIR].get(now, rate) +
      2*vec[META_POP_FETCH].get(now, rate) +
      4*vec[META_POP_STORE].get(now, rate);
  }
  double meta_load() {
    return 
      1*vec[META_POP_IRD].get_last() + 
      2*vec[META_POP_IWR].get_last() +
      1*vec[META_POP_READDIR].get_last() +
      2*vec[META_POP_FETCH].get_last() +
      4*vec[META_POP_STORE].get_last();
  }

  void add(utime_t now, DecayRate& rate, dirfrag_load_vec_t& r) {
    for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(r.vec[i].get(now, rate));
  }
  void sub(utime_t now, DecayRate& rate, dirfrag_load_vec_t& r) {
    for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(-r.vec[i].get(now, rate));
  }
  void scale(double f) {
    for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].scale(f);
  }
};

inline void encode(const dirfrag_load_vec_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(dirfrag_load_vec_t& c, const utime_t &t, bufferlist::iterator &p) {
  c.decode(t, p);
}
// this for dencoder
inline void decode(dirfrag_load_vec_t& c, bufferlist::iterator &p) {
  utime_t sample;
  c.decode(sample, p);
}

inline std::ostream& operator<<(std::ostream& out, dirfrag_load_vec_t& dl)
{
  // ugliness!
  utime_t now = ceph_clock_now();
  DecayRate rate(g_conf->mds_decay_halflife);
  return out << "[" << dl.vec[0].get(now, rate) << "," << dl.vec[1].get(now, rate) 
	     << " " << dl.meta_load(now, rate)
	     << "]";
}






/* mds_load_t
 * mds load
 */

struct mds_load_t {
  dirfrag_load_vec_t auth;
  dirfrag_load_vec_t all;

  double req_rate;
  double cache_hit_rate;
  double queue_len;

  double cpu_load_avg;

  explicit mds_load_t(const utime_t &t) : 
    auth(t), all(t), req_rate(0), cache_hit_rate(0),
    queue_len(0), cpu_load_avg(0)
  {}
  // mostly for the dencoder infrastructure
  mds_load_t() :
    auth(), all(),
    req_rate(0), cache_hit_rate(0), queue_len(0), cpu_load_avg(0)
  {}
  
  double mds_load();  // defiend in MDBalancer.cc
  void encode(bufferlist& bl) const;
  void decode(const utime_t& now, bufferlist::iterator& bl);
  //this one is for dencoder infrastructure
  void decode(bufferlist::iterator& bl) { utime_t sample; decode(sample, bl); }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<mds_load_t*>& ls);
};
inline void encode(const mds_load_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(mds_load_t &c, const utime_t &t, bufferlist::iterator &p) {
  c.decode(t, p);
}
// this one is for dencoder
inline void decode(mds_load_t &c, bufferlist::iterator &p) {
  utime_t sample;
  c.decode(sample, p);
}

inline std::ostream& operator<<( std::ostream& out, mds_load_t& load )
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
  static const int MAX = 4;
  int last[MAX];
  int p, n;
  DecayCounter count;

public:
  load_spread_t() : p(0), n(0), count(ceph_clock_now())
  {
    for (int i=0; i<MAX; i++)
      last[i] = -1;
  } 

  double hit(utime_t now, const DecayRate& rate, int who) {
    for (int i=0; i<n; i++)
      if (last[i] == who) 
	return count.get_last();

    // we're new(ish)
    last[p++] = who;
    if (n < MAX) n++;
    if (n == 1) return 0.0;

    if (p == MAX) p = 0;

    return count.hit(now, rate);
  }
  double get(utime_t now, const DecayRate& rate) {
    return count.get(now, rate);
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
  inodeno_t ino;
  dirfrag_t dirfrag;
  string dname;
  snapid_t snapid;

  MDSCacheObjectInfo() : ino(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<MDSCacheObjectInfo*>& ls);
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
