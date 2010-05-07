// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef __MDSTYPES_H
#define __MDSTYPES_H


#include <math.h>
#include <ostream>
#include <set>
#include <map>
using namespace std;

#include "config.h"
#include "common/DecayCounter.h"
#include "include/Context.h"

#include "include/frag.h"
#include "include/xlist.h"

#include <boost/pool/pool.hpp>

#define CEPH_FS_ONDISK_MAGIC "ceph fs volume v011"


//#define MDS_REF_SET      // define me for improved debug output, sanity checking
//#define MDS_AUTHPIN_SET  // define me for debugging auth pin leaks
//#define MDS_VERIFY_FRAGSTAT    // do do (slow) sanity checking on frags

#define MDS_PORT_CACHE   0x200
#define MDS_PORT_LOCKER  0x300
#define MDS_PORT_MIGRATOR 0x400


#define MAX_MDS                   0x100

#define MDS_INO_ROOT              1
#define MDS_INO_CEPH              2
#define MDS_INO_PGTABLE           3
#define MDS_INO_ANCHORTABLE       4
#define MDS_INO_SNAPTABLE         5

#define MDS_INO_MDSDIR_OFFSET     (1*MAX_MDS)
#define MDS_INO_LOG_OFFSET        (2*MAX_MDS)
#define MDS_INO_STRAY_OFFSET      (6*MAX_MDS)

#define MDS_INO_SYSTEM_BASE       (10*MAX_MDS)

#define MDS_INO_STRAY(x)  (MDS_INO_STRAY_OFFSET+((unsigned)x))
#define MDS_INO_MDSDIR(x) (MDS_INO_MDSDIR_OFFSET+((unsigned)x))

#define MDS_INO_IS_STRAY(i)  ((i) >= MDS_INO_STRAY_OFFSET  && (i) < (MDS_INO_STRAY_OFFSET+MAX_MDS))
#define MDS_INO_IS_MDSDIR(i) ((i) >= MDS_INO_MDSDIR_OFFSET && (i) < (MDS_INO_MDSDIR_OFFSET+MAX_MDS))
#define MDS_INO_IS_BASE(i)   (MDS_INO_ROOT == (i) || MDS_INO_IS_MDSDIR(i))

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.
#define MDS_TRAVERSE_FAIL          4


extern long g_num_ino, g_num_dir, g_num_dn, g_num_cap;
extern long g_num_inoa, g_num_dira, g_num_dna, g_num_capa;
extern long g_num_inos, g_num_dirs, g_num_dns, g_num_caps;


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



struct frag_info_t {
  version_t version;

  // this frag
  utime_t mtime;
  int64_t nfiles;        // files
  int64_t nsubdirs;      // subdirs

  frag_info_t() : version(0), nfiles(0), nsubdirs(0) {}

  int64_t size() const { return nfiles + nsubdirs; }

  void zero() {
    *this = frag_info_t();
  }

  // *this += cur - acc; acc = cur
  void take_diff(const frag_info_t &cur, frag_info_t &acc, bool& touched_mtime) {
    if (!(cur.mtime == acc.mtime)) {
      mtime = cur.mtime;
      touched_mtime = true;
    }
    nfiles += cur.nfiles - acc.nfiles;
    nsubdirs += cur.nsubdirs - acc.nsubdirs;
    acc = cur;
    acc.version = version;
  }

  void encode(bufferlist &bl) const {
    __u8 v = 1;
    ::encode(v, bl);

    ::encode(version, bl);
    ::encode(mtime, bl);
    ::encode(nfiles, bl);
    ::encode(nsubdirs, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);

    ::decode(version, bl);
    ::decode(mtime, bl);
    ::decode(nfiles, bl);
    ::decode(nsubdirs, bl);
 }
};
WRITE_CLASS_ENCODER(frag_info_t)

inline bool operator==(const frag_info_t &l, const frag_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

inline ostream& operator<<(ostream &out, const frag_info_t &f) {
  return out << "f(v" << f.version
	     << " m" << f.mtime
	     << " " << f.size() << "=" << f.nfiles << "+" << f.nsubdirs
	     << ")";    
}

struct nest_info_t {
  version_t version;

  // this frag + children
  utime_t rctime;
  int64_t rbytes;
  int64_t rfiles;
  int64_t rsubdirs;
  int64_t rsize() const { return rfiles + rsubdirs; }

  int64_t ranchors;  // for dirstat, includes inode's anchored flag.
  int64_t rsnaprealms;

  nest_info_t() : version(0),
		  rbytes(0), rfiles(0), rsubdirs(0),
		  ranchors(0), rsnaprealms(0) {}

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
    ranchors += fac*other.ranchors;
    rsnaprealms += fac*other.rsnaprealms;
  }

  // *this += cur - acc; acc = cur
  void take_diff(const nest_info_t &cur, nest_info_t &acc) {
    if (cur.rctime > rctime)
      rctime = cur.rctime;
    rbytes += cur.rbytes - acc.rbytes;
    rfiles += cur.rfiles - acc.rfiles;
    rsubdirs += cur.rsubdirs - acc.rsubdirs;
    ranchors += cur.ranchors - acc.ranchors;
    rsnaprealms += cur.rsnaprealms - acc.rsnaprealms;
    acc = cur;
    acc.version = version;
  }

  void encode(bufferlist &bl) const {
    __u8 v = 1;
    ::encode(v, bl);

    ::encode(version, bl);
    ::encode(rbytes, bl);
    ::encode(rfiles, bl);
    ::encode(rsubdirs, bl);
    ::encode(ranchors, bl);
    ::encode(rsnaprealms, bl);
    ::encode(rctime, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);

    ::decode(version, bl);
    ::decode(rbytes, bl);
    ::decode(rfiles, bl);
    ::decode(rsubdirs, bl);
    ::decode(ranchors, bl);
    ::decode(rsnaprealms, bl);
    ::decode(rctime, bl);
 }
};
WRITE_CLASS_ENCODER(nest_info_t)

inline bool operator==(const nest_info_t &l, const nest_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

inline ostream& operator<<(ostream &out, const nest_info_t &n) {
  return out << "n(v" << n.version
	     << " rc" << n.rctime
	     << " b" << n.rbytes
	     << " a" << n.ranchors
	     << " sr" << n.rsnaprealms
	     << " " << n.rsize() << "=" << n.rfiles << "+" << n.rsubdirs
	     << ")";    
}

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
inline bool operator<(const vinodeno_t &l, const vinodeno_t &r) {
  return 
    l.ino < r.ino ||
    (l.ino == r.ino && l.snapid < r.snapid);
}

namespace __gnu_cxx {
  template<> struct hash<vinodeno_t> {
    size_t operator()(const vinodeno_t &vino) const { 
      hash<inodeno_t> H;
      hash<uint64_t> I;
      return H(vino.ino) ^ I(vino.snapid);
    }
  };
}




inline ostream& operator<<(ostream &out, const vinodeno_t &vino) {
  out << vino.ino;
  if (vino.snapid == CEPH_NOSNAP)
    out << ".head";
  else if (vino.snapid)
    out << '.' << vino.snapid;
  return out;
}


struct byte_range_t {
  uint64_t first, last;    // interval client can write to

  void encode(bufferlist &bl) const {
    ::encode(first, bl);
    ::encode(last, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(first, bl);
    ::decode(last, bl);
  }    
};
WRITE_CLASS_ENCODER(byte_range_t)

inline ostream& operator<<(ostream& out, const byte_range_t& r)
{
  return out << r.first << '-' << r.last;
}
inline bool operator==(const byte_range_t& l, const byte_range_t& r) {
  return l.first == r.first && l.last == r.last;
}

struct inode_t {
  // base (immutable)
  inodeno_t ino;
  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;  
  bool       anchored;          // auth only?

  // file (data access)
  ceph_file_layout layout;
  uint64_t   size;        // on directory, # dentries
  uint32_t   truncate_seq;
  uint64_t   truncate_size, truncate_from;
  uint32_t   truncate_pending;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  map<client_t,byte_range_t> client_ranges;  // client(s) can write to these ranges

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat, accounted_rstat;
 
  // special stuff
  version_t version;           // auth only
  version_t file_data_version; // auth only
  version_t xattr_version;

  version_t last_renamed_version;      // when i was last renamed

  inode_t() : ino(0), rdev(0),
	      mode(0), uid(0), gid(0),
	      nlink(0), anchored(false),
	      size(0), truncate_seq(0), truncate_size(0), truncate_from(0),
	      truncate_pending(0),
	      time_warp_seq(0),
	      version(0), file_data_version(0), xattr_version(0), last_renamed_version(0) { 
    memset(&layout, 0, sizeof(layout));
  }

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool is_truncating() const { return (truncate_pending > 0); }

  int64_t get_layout_size_increment() {
    return layout.fl_object_size * layout.fl_stripe_count;
  }

  uint64_t get_max_size() const {
    uint64_t max = 0;
      for (map<client_t,byte_range_t>::const_iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	if (p->second.last > max)
	  max = p->second.last;
      return max;
  }
  void set_max_size(uint64_t new_max) {
    if (new_max == 0) {
      client_ranges.clear();
    } else {
      for (map<client_t,byte_range_t>::iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	p->second.last = new_max;
    }
  }

  void encode(bufferlist &bl) const {
    __u8 v = 2;
    ::encode(v, bl);

    ::encode(ino, bl);
    ::encode(rdev, bl);
    ::encode(ctime, bl);

    ::encode(mode, bl);
    ::encode(uid, bl);
    ::encode(gid, bl);

    ::encode(nlink, bl);
    ::encode(anchored, bl);

    ::encode(layout, bl);
    ::encode(size, bl);
    ::encode(truncate_seq, bl);
    ::encode(truncate_size, bl);
    ::encode(truncate_from, bl);
    ::encode(mtime, bl);
    ::encode(atime, bl);
    ::encode(time_warp_seq, bl);
    ::encode(client_ranges, bl);

    ::encode(dirstat, bl);
    ::encode(rstat, bl);
    ::encode(accounted_rstat, bl);

    ::encode(version, bl);
    ::encode(file_data_version, bl);
    ::encode(xattr_version, bl);
    ::encode(last_renamed_version, bl);
  }
  void decode(bufferlist::iterator &p) {
    __u8 v;
    ::decode(v, p);

    ::decode(ino, p);
    ::decode(rdev, p);
    ::decode(ctime, p);

    ::decode(mode, p);
    ::decode(uid, p);
    ::decode(gid, p);

    ::decode(nlink, p);
    ::decode(anchored, p);

    ::decode(layout, p);
    ::decode(size, p);
    ::decode(truncate_seq, p);
    ::decode(truncate_size, p);
    ::decode(truncate_from, p);
    ::decode(mtime, p);
    ::decode(atime, p);
    ::decode(time_warp_seq, p);
    ::decode(client_ranges, p);
    
    ::decode(dirstat, p);
    ::decode(rstat, p);
    ::decode(accounted_rstat, p);

    ::decode(version, p);
    ::decode(file_data_version, p);
    ::decode(xattr_version, p);
    if (v >= 2)
      ::decode(last_renamed_version, p);
  }
};
WRITE_CLASS_ENCODER(inode_t)


struct old_inode_t {
  snapid_t first;
  inode_t inode;
  map<string,bufferptr> xattrs;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(first, bl);
    ::encode(inode, bl);
    ::encode(xattrs, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(first, bl);
    ::decode(inode, bl);
    ::decode(xattrs, bl);
  }
};
WRITE_CLASS_ENCODER(old_inode_t)


/*
 * like an inode, but for a dir frag 
 */
struct fnode_t {
  version_t version;
  snapid_t snap_purged_thru;   // the max_last_destroy snapid we've been purged thru
  frag_info_t fragstat, accounted_fragstat;
  nest_info_t rstat, accounted_rstat;

  void encode(bufferlist &bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(snap_purged_thru, bl);
    ::encode(fragstat, bl);
    ::encode(accounted_fragstat, bl);
    ::encode(rstat, bl);
    ::encode(accounted_rstat, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(version, bl);
    ::decode(snap_purged_thru, bl);
    ::decode(fragstat, bl);
    ::decode(accounted_fragstat, bl);
    ::decode(rstat, bl);
    ::decode(accounted_rstat, bl);
  }
};
WRITE_CLASS_ENCODER(fnode_t)


struct old_rstat_t {
  snapid_t first;
  nest_info_t rstat, accounted_rstat;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(first, bl);
    ::encode(rstat, bl);
    ::encode(accounted_rstat, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(first, bl);
    ::decode(rstat, bl);
    ::decode(accounted_rstat, bl);    
  }
};
WRITE_CLASS_ENCODER(old_rstat_t)

inline ostream& operator<<(ostream& out, const old_rstat_t& o) {
  return out << "old_rstat(first " << o.first << " " << o.rstat << " " << o.accounted_rstat << ")";
}



// =======
// dentries

struct dentry_key_t {
  snapid_t snapid;
  const char *name;
  dentry_key_t() : snapid(0), name(0) {}
  dentry_key_t(snapid_t s, const char *n) : snapid(s), name(n) {}

  // encode into something that can be decoded as a string.
  // name_ (head) or name_%x (!head)
  void encode(bufferlist& bl) const {
    __u32 l = strlen(name) + 1;
    char b[20];
    if (snapid != CEPH_NOSNAP) {
      sprintf(b, "%llx", (long long unsigned)snapid);
      l += strlen(b);
    } else {
      strcpy(b, "head");
      l += 4;
    }
    ::encode(l, bl);
    bl.append(name, strlen(name));
    bl.append("_", 1);
    bl.append(b);
  }
  static void decode_helper(bufferlist::iterator& bl, string& nm, snapid_t& sn) {
    string foo;
    ::decode(foo, bl);

    int i = foo.length()-1;
    while (foo[i] != '_' && i)
      i--;
    assert(i);
    if (i+5 == (int)foo.length() &&
	foo[i+1] == 'h' &&
	foo[i+2] == 'e' &&
	foo[i+3] == 'a' &&
	foo[i+4] == 'd') {
      // name_head
      sn = CEPH_NOSNAP;
    } else {
      // name_%x
      long long unsigned x = 0;
      sscanf(foo.c_str() + i + 1, "%llx", &x);
      sn = x;
    }  
    nm = string(foo.c_str(), i);
  }
};

inline ostream& operator<<(ostream& out, const dentry_key_t &k)
{
  return out << "(" << k.name << "," << k.snapid << ")";
}

inline bool operator<(const dentry_key_t& k1, const dentry_key_t& k2)
{
  /*
   * order by name, then snap
   */
  int c = strcmp(k1.name, k2.name);
  return 
    c < 0 || (c == 0 && k1.snapid < k2.snapid);
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
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(name, bl);
    ::encode(snapid, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v = 1;
    ::decode(struct_v, bl);
    ::decode(name, bl);
    ::decode(snapid, bl);
  }
};
WRITE_CLASS_ENCODER(string_snap_t)

inline bool operator<(const string_snap_t& l, const string_snap_t& r) {
  int c = strcmp(l.name.c_str(), r.name.c_str());
  return c < 0 || (c == 0 && l.snapid < r.snapid);
}

inline ostream& operator<<(ostream& out, const string_snap_t &k)
{
  return out << "(" << k.name << "," << k.snapid << ")";
}



// =========
// requests

struct metareqid_t {
  entity_name_t name;
  uint64_t tid;
  metareqid_t() : tid(0) {}
  metareqid_t(entity_name_t n, tid_t t) : name(n), tid(t) {}
};

static inline void encode(const metareqid_t &r, bufferlist &bl)
{
  ::encode(r.name, bl);
  ::encode(r.tid, bl);
}
static inline void decode( metareqid_t &r, bufferlist::iterator &p)
{
  ::decode(r.name, p);
  ::decode(r.tid, p);
}

inline ostream& operator<<(ostream& out, const metareqid_t& r) {
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

namespace __gnu_cxx {
  template<> struct hash<metareqid_t> {
    size_t operator()(const metareqid_t &r) const { 
      hash<uint64_t> H;
      return H(r.name.num()) ^ H(r.name.type()) ^ H(r.tid);
    }
  };
}


// cap info for client reconnect
struct cap_reconnect_t {
  string path;
  ceph_mds_cap_reconnect capinfo;

  cap_reconnect_t() {}
  cap_reconnect_t(uint64_t cap_id, inodeno_t pino, const string& p, int w, int i, uint64_t sz, utime_t mt, utime_t at, inodeno_t sr) : 
    path(p) {
    capinfo.cap_id = cap_id;
    capinfo.wanted = w;
    capinfo.issued = i;
    capinfo.size = sz;
    capinfo.mtime = mt;
    capinfo.atime = at;
    capinfo.snaprealm = sr;
    capinfo.pathbase = pino;
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
WRITE_CLASS_ENCODER(cap_reconnect_t)



// ================================================================
// dir frag

struct dirfrag_t {
  inodeno_t ino;
  frag_t    frag;

  dirfrag_t() : ino(0) { }
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f) { }
};

inline void encode(const dirfrag_t &f, bufferlist& bl) { 
  encode(f.ino, bl);
  encode(f.frag, bl);
}
inline void decode(dirfrag_t &f, bufferlist::iterator& p) { 
  decode(f.ino, p);
  decode(f.frag, p);
}


inline ostream& operator<<(ostream& out, const dirfrag_t df) {
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

namespace __gnu_cxx {
  template<> struct hash<dirfrag_t> {
    size_t operator()(const dirfrag_t &df) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(df.ino) ^ I(df.frag);
    }
  };
}



// ================================================================

#define META_POP_IRD     0
#define META_POP_IWR     1
#define META_POP_READDIR 2
#define META_POP_FETCH   3
#define META_POP_STORE   4
#define META_NPOP        5

class inode_load_vec_t {
  static const int NUM = 2;
  DecayCounter vec[NUM];
public:
  DecayCounter &get(int t) { 
    assert(t < NUM);
    return vec[t]; 
  }
  void zero(utime_t now) {
    for (int i=0; i<NUM; i++) 
      vec[i].reset(now);
  }
  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    for (int i=0; i<NUM; i++)
      ::encode(vec[i], bl);
  }
  void decode(bufferlist::iterator &p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    for (int i=0; i<NUM; i++)
      ::decode(vec[i], p);
  }
};
WRITE_CLASS_ENCODER(inode_load_vec_t)

class dirfrag_load_vec_t {
public:
  static const int NUM = 5;
  DecayCounter vec[NUM];

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    for (int i=0; i<NUM; i++)
      ::encode(vec[i], bl);
  }
  void decode(bufferlist::iterator &p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    for (int i=0; i<NUM; i++)
      ::decode(vec[i], p);
  }

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

WRITE_CLASS_ENCODER(dirfrag_load_vec_t)

inline ostream& operator<<(ostream& out, dirfrag_load_vec_t& dl)
{
  // ugliness!
  utime_t now = g_clock.now();
  DecayRate rate(g_conf.mds_decay_halflife);
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

  mds_load_t() : 
    req_rate(0), cache_hit_rate(0), queue_len(0), cpu_load_avg(0) { 
  }
  
  double mds_load();  // defiend in MDBalancer.cc

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(auth, bl);
    ::encode(all, bl);
    ::encode(req_rate, bl);
    ::encode(cache_hit_rate, bl);
    ::encode(queue_len, bl);
    ::encode(cpu_load_avg, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(auth, bl);
    ::decode(all, bl);
    ::decode(req_rate, bl);
    ::decode(cache_hit_rate, bl);
    ::decode(queue_len, bl);
    ::decode(cpu_load_avg, bl);
  }
};
WRITE_CLASS_ENCODER(mds_load_t)

inline ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "mdsload<" << load.auth << "/" << load.all
             << ", req " << load.req_rate 
             << ", hr " << load.cache_hit_rate
             << ", qlen " << load.queue_len
	     << ", cpu " << load.cpu_load_avg
             << ">";
}

/*
inline mds_load_t& operator+=( mds_load_t& l, mds_load_t& r ) 
{
  l.root_pop += r.root_pop;
  l.req_rate += r.req_rate;
  l.queue_len += r.queue_len;
  return l;
}

inline mds_load_t operator/( mds_load_t& a, double d ) 
{
  mds_load_t r;
  r.root_pop = a.root_pop / d;
  r.req_rate = a.req_rate / d;
  r.queue_len = a.queue_len / d;
  return r;
}
*/


class load_spread_t {
public:
  static const int MAX = 4;
  int last[MAX];
  int p, n;
  DecayCounter count;

public:
  load_spread_t() : p(0), n(0) { 
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

//#define MDS_PIN_REPLICATED     1
//#define MDS_STATE_AUTH     (1<<0)

class MLock;
class SimpleLock;

class MDSCacheObject;

// -- authority delegation --
// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_UNKNOWN  -2
#define CDIR_AUTH_DEFAULT   pair<int,int>(-1, -2)
#define CDIR_AUTH_UNDEF     pair<int,int>(-2, -2)
//#define CDIR_AUTH_ROOTINODE pair<int,int>( 0, -2)


/*
 * for metadata leases to clients
 */
struct ClientLease {
  client_t client;
  int mask;                 // CEPH_STAT_MASK_*
  MDSCacheObject *parent;

  ceph_seq_t seq;
  utime_t ttl;
  xlist<ClientLease*>::item item_session_lease; // per-session list
  xlist<ClientLease*>::item item_lease;         // global list

  ClientLease(client_t c, MDSCacheObject *p) : 
    client(c), mask(0), parent(p), seq(0),
    item_session_lease(this),
    item_lease(this) { }
};


// print hack
struct mdsco_db_line_prefix {
  MDSCacheObject *object;
  mdsco_db_line_prefix(MDSCacheObject *o) : object(o) {}
};
ostream& operator<<(ostream& out, mdsco_db_line_prefix o);

// printer
ostream& operator<<(ostream& out, MDSCacheObject &o);

class MDSCacheObjectInfo {
public:
  inodeno_t ino;
  dirfrag_t dirfrag;
  string dname;
  snapid_t snapid;

  MDSCacheObjectInfo() : ino(0) {}

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(ino, bl);
    ::encode(dirfrag, bl);
    ::encode(dname, bl);
    ::encode(snapid, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    ::decode(ino, p);
    ::decode(dirfrag, p);
    ::decode(dname, p);
    ::decode(snapid, p);
  }
};

WRITE_CLASS_ENCODER(MDSCacheObjectInfo)


class MDSCacheObject {
 public:
  // -- pins --
  const static int PIN_REPLICATED =  1000;
  const static int PIN_DIRTY      =  1001;
  const static int PIN_LOCK       = -1002;
  const static int PIN_REQUEST    = -1003;
  const static int PIN_WAITER     =  1004;
  const static int PIN_DIRTYSCATTERED = -1005;
  static const int PIN_AUTHPIN    =  1006;
  static const int PIN_PTRWAITER  = -1007;
  const static int PIN_TEMPEXPORTING = 1008;  // temp pin between encode_ and finish_export
  static const int PIN_CLIENTLEASE = 1009;

  const char *generic_pin_name(int p) {
    switch (p) {
    case PIN_REPLICATED: return "replicated";
    case PIN_DIRTY: return "dirty";
    case PIN_LOCK: return "lock";
    case PIN_REQUEST: return "request";
    case PIN_WAITER: return "waiter";
    case PIN_DIRTYSCATTERED: return "dirtyscattered";
    case PIN_AUTHPIN: return "authpin";
    case PIN_PTRWAITER: return "ptrwaiter";
    case PIN_TEMPEXPORTING: return "tempexporting";
    case PIN_CLIENTLEASE: return "clientlease";
    default: assert(0); return 0;
    }
  }

  // -- state --
  const static int STATE_AUTH      = (1<<30);
  const static int STATE_DIRTY     = (1<<29);
  const static int STATE_REJOINING = (1<<28);  // replica has not joined w/ primary copy

  // -- wait --
  const static uint64_t WAIT_SINGLEAUTH  = (1ull<<60);
  const static uint64_t WAIT_UNFREEZE    = (1ull<<59); // pka AUTHPINNABLE


  // ============================================
  // cons
 public:
  MDSCacheObject() :
    state(0), 
    ref(0),
    replica_nonce(0) {}
  virtual ~MDSCacheObject() {}

  // printing
  virtual void print(ostream& out) = 0;
  virtual ostream& print_db_line_prefix(ostream& out) { 
    return out << "mdscacheobject(" << this << ") "; 
  }
  
  // --------------------------------------------
  // state
 protected:
  __u32 state;     // state bits

 public:
  unsigned get_state() const { return state; }
  unsigned state_test(unsigned mask) const { return (state & mask); }
  void state_clear(unsigned mask) { state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  void state_reset(unsigned s) { state = s; }

  bool is_auth() const { return state_test(STATE_AUTH); }
  bool is_dirty() const { return state_test(STATE_DIRTY); }
  bool is_clean() const { return !is_dirty(); }
  bool is_rejoining() const { return state_test(STATE_REJOINING); }

  // --------------------------------------------
  // authority
  virtual pair<int,int> authority() = 0;
  bool is_ambiguous_auth() {
    return authority().second != CDIR_AUTH_UNKNOWN;
  }

  // --------------------------------------------
  // pins
protected:
  __s32      ref;       // reference count
#ifdef MDS_REF_SET
  multiset<int> ref_set;
#endif

 public:
  int get_num_ref() { return ref; }
  virtual const char *pin_name(int by) = 0;
  //bool is_pinned_by(int by) { return ref_set.count(by); }
  //multiset<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
#ifdef MDS_REF_SET
    assert(ref_set.count(by) > 0);
#endif
    assert(ref > 0);
  }
  void put(int by) {
#ifdef MDS_REF_SET
    if (ref == 0 || ref_set.count(by) == 0) {
#else
    if (ref == 0) {
#endif
      bad_put(by);
    } else {
      ref--;
#ifdef MDS_REF_SET
      ref_set.erase(ref_set.find(by));
      assert(ref == (int)ref_set.size());
#endif
      if (ref == 0)
	last_put();
    }
  }

  virtual void first_get() {}
  virtual void bad_get(int by) {
#ifdef MDS_REF_SET
    assert(by < 0 || ref_set.count(by) == 0);
#endif
    assert(0);
  }
  void get(int by) {
#ifdef MDS_REF_SET
    if (by >= 0 && ref_set.count(by)) {
      bad_get(by);
    } else {
#endif
      if (ref == 0) 
	first_get();
      ref++;
#ifdef MDS_REF_SET
      ref_set.insert(by);
      assert(ref == (int)ref_set.size());
    }
#endif
  }

  void print_pin_set(ostream& out) {
#ifdef MDS_REF_SET
    multiset<int>::iterator it = ref_set.begin();
    while (it != ref_set.end()) {
      out << " " << pin_name(*it);
      int last = *it;
      int c = 1;
      do {
	it++;
      } while (it != ref_set.end() && *it == last);
      if (c > 1)
	out << "*" << c;
    }
#else
    out << " nref=" << ref;
#endif
  }


  // --------------------------------------------
  // auth pins
  virtual bool can_auth_pin() = 0;
  virtual void auth_pin(void *who) = 0;
  virtual void auth_unpin(void *who) = 0;
  virtual bool is_frozen() = 0;


  // --------------------------------------------
  // replication (across mds cluster)
 protected:
  __s16        replica_nonce; // [replica] defined on replica
  map<int,int> replica_map;   // [auth] mds -> nonce

 public:
  bool is_replicated() { return !replica_map.empty(); }
  bool is_replica(int mds) { return replica_map.count(mds); }
  int num_replicas() { return replica_map.size(); }
  int add_replica(int mds) {
    if (replica_map.count(mds)) 
      return ++replica_map[mds];  // inc nonce
    if (replica_map.empty()) 
      get(PIN_REPLICATED);
    return replica_map[mds] = 1;
  }
  void add_replica(int mds, int nonce) {
    if (replica_map.empty()) 
      get(PIN_REPLICATED);
    replica_map[mds] = nonce;
  }
  int get_replica_nonce(int mds) {
    assert(replica_map.count(mds));
    return replica_map[mds];
  }
  void remove_replica(int mds) {
    assert(replica_map.count(mds));
    replica_map.erase(mds);
    if (replica_map.empty())
      put(PIN_REPLICATED);
  }
  void clear_replica_map() {
    if (!replica_map.empty())
      put(PIN_REPLICATED);
    replica_map.clear();
  }
  map<int,int>::iterator replicas_begin() { return replica_map.begin(); }
  map<int,int>::iterator replicas_end() { return replica_map.end(); }
  const map<int,int>& get_replicas() { return replica_map; }
  void list_replicas(set<int>& ls) {
    for (map<int,int>::const_iterator p = replica_map.begin();
	 p != replica_map.end();
	 ++p) 
      ls.insert(p->first);
  }

  int get_replica_nonce() { return replica_nonce;}
  void set_replica_nonce(int n) { replica_nonce = n; }


  // ---------------------------------------------
  // waiting
 protected:
  multimap<uint64_t, Context*>  waiting;

 public:
  bool is_waiter_for(uint64_t mask, uint64_t min=0) {
    if (!min) {
      min = mask;
      while (min & (min-1))  // if more than one bit is set
	min &= min-1;        //  clear LSB
    }
    for (multimap<uint64_t,Context*>::iterator p = waiting.lower_bound(min);
	 p != waiting.end();
	 ++p) {
      if (p->first & mask) return true;
      if (p->first > mask) return false;
    }
    return false;
  }
  virtual void add_waiter(uint64_t mask, Context *c) {
    if (waiting.empty())
      get(PIN_WAITER);
    waiting.insert(pair<uint64_t,Context*>(mask, c));
    pdout(10,g_conf.debug_mds) << (mdsco_db_line_prefix(this)) 
			       << "add_waiter " << hex << mask << dec << " " << c
			       << " on " << *this
			       << dendl;
    
  }
  virtual void take_waiting(uint64_t mask, list<Context*>& ls) {
    if (waiting.empty()) return;
    multimap<uint64_t,Context*>::iterator it = waiting.begin();
    while (it != waiting.end()) {
      if (it->first & mask) {
	ls.push_back(it->second);
	pdout(10,g_conf.debug_mds) << (mdsco_db_line_prefix(this))
				   << "take_waiting mask " << hex << mask << dec << " took " << it->second
				   << " tag " << it->first
				   << " on " << *this
				   << dendl;
	waiting.erase(it++);
      } else {
	pdout(10,g_conf.debug_mds) << "take_waiting mask " << hex << mask << dec << " SKIPPING " << it->second
				   << " tag " << it->first
				   << " on " << *this 
				   << dendl;
	it++;
      }
    }
    if (waiting.empty())
      put(PIN_WAITER);
  }
  void finish_waiting(uint64_t mask, int result = 0) {
    list<Context*> finished;
    take_waiting(mask, finished);
    finish_contexts(finished, result);
  }


  // ---------------------------------------------
  // locking
  // noop unless overloaded.
  virtual SimpleLock* get_lock(int type) { assert(0); return 0; }
  virtual void set_object_info(MDSCacheObjectInfo &info) { assert(0); }
  virtual void encode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void decode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void finish_lock_waiters(int type, uint64_t mask, int r=0) { assert(0); }
  virtual void add_lock_waiter(int type, uint64_t mask, Context *c) { assert(0); }
  virtual bool is_lock_waiting(int type, uint64_t mask) { assert(0); return false; }

  virtual void clear_dirty_scattered(int type) { assert(0); }
  virtual void finish_scatter_gather_update(int type) { }

  // ---------------------------------------------
  // ordering
  virtual bool is_lt(const MDSCacheObject *r) const = 0;
  struct ptr_lt {
    bool operator()(const MDSCacheObject* l, const MDSCacheObject* r) const {
      return l->is_lt(r);
    }
  };

};

inline ostream& operator<<(ostream& out, MDSCacheObject &o) {
  o.print(out);
  return out;
}

inline ostream& operator<<(ostream& out, const MDSCacheObjectInfo &info) {
  if (info.ino) return out << info.ino << "." << info.snapid;
  if (info.dname.length()) return out << info.dirfrag << "/" << info.dname
				      << " snap " << info.snapid;
  return out << info.dirfrag;
}

inline ostream& operator<<(ostream& out, mdsco_db_line_prefix o) {
  o.object->print_db_line_prefix(out);
  return out;
}





#endif
