// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MDS_TYPES_H
#define __MDS_TYPES_H

#include "ceph_fs.h"

extern "C" {
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <fcntl.h>
#include "statlite.h"
}

#include <string>
#include <list>
#include <set>
#include <map>
#include <vector>
#include <iostream>
#include <iomanip>

using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;



#include "object.h"
#include "utime.h"
#include "intarith.h"

#include "../acconfig.h"

// DARWIN compatibility
#ifdef DARWIN
typedef long long loff_t;
typedef long long off64_t;
#define O_DIRECT 00040000
#endif

// -- stl crap --

namespace __gnu_cxx {
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
      static hash<const char*> H;
      return H(x.c_str());
    }
  };

#ifndef __LP64__
  template<> struct hash<int64_t> {
    size_t operator()(int64_t __x) const { 
      static hash<int32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
  template<> struct hash<uint64_t> {
    size_t operator()(uint64_t __x) const { 
      static hash<uint32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
#endif

}


/*
 * comparators for stl containers
 */
// for hash_map:
//   hash_map<const char*, long, hash<const char*>, eqstr> vals;
struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) == 0;
  }
};

// for set, map
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};



#include "encoding.h"

WRITE_RAW_ENCODER(ceph_fsid)
WRITE_RAW_ENCODER(ceph_file_layout)
WRITE_RAW_ENCODER(ceph_mds_request_head)
WRITE_RAW_ENCODER(ceph_mds_file_caps)
WRITE_RAW_ENCODER(ceph_mds_lease)
WRITE_RAW_ENCODER(ceph_mds_reply_head)
WRITE_RAW_ENCODER(ceph_mds_reply_inode)
WRITE_RAW_ENCODER(ceph_frag_tree_split)
WRITE_RAW_ENCODER(ceph_inopath_item)

WRITE_RAW_ENCODER(ceph_osd_request_head)
WRITE_RAW_ENCODER(ceph_osd_reply_head)

WRITE_RAW_ENCODER(ceph_statfs)

// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef __u64 tid_t;         // transaction id
typedef __u64 version_t;
typedef __u32 epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)


#define O_LAZY 01000000

typedef __u64 coll_t;


// --------------------------------------
// inode

typedef __u64 _inodeno_t;

struct inodeno_t {
  _inodeno_t val;
  inodeno_t() : val(0) {}
  inodeno_t(_inodeno_t v) : val(v) {}
  inodeno_t operator+=(inodeno_t o) { val += o.val; return *this; }
  operator _inodeno_t() const { return val; }
};

inline void encode(inodeno_t i, bufferlist &bl) { encode(i.val, bl); }
inline void decode(inodeno_t &i, bufferlist::iterator &p) { decode(i.val, p); }

inline ostream& operator<<(ostream& out, inodeno_t ino) {
  return out << hex << ino.val << dec;
}

namespace __gnu_cxx {
  template<> struct hash< inodeno_t >
  {
    size_t operator()( const inodeno_t& x ) const
    {
      static rjhash<uint64_t> H;
      return H(x.val);
    }
  };
}


static inline bool file_mode_is_readonly(int mode) {
  return (mode & CEPH_FILE_MODE_WR) == 0;
}

inline int DT_TO_MODE(int dt) {
  return dt << 12;
}
inline unsigned char MODE_TO_DT(int mode) {
  return mode >> 12;
}

struct FileLayout {
  /* file -> object mapping */
  __u32 fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple of page size. */
  __u32 fl_stripe_count;    /* over this many objects */
  __u32 fl_object_size;     /* until objects are this big, then move to new objects */
  __u32 fl_cas_hash;        /* 0 = none; 1 = sha256 */
  
  /* pg -> disk layout */
  __u32 fl_object_stripe_unit;  /* for per-object parity, if any */
  
  /* object -> pg layout */
  __s32 fl_pg_preferred; /* preferred primary for pg, if any (-1 = none) */
  __u8  fl_pg_type;      /* pg type; see PG_TYPE_* */
  __u8  fl_pg_size;      /* pg size (num replicas, raid stripe width, etc. */
  __u8  fl_pg_pool;      /* implies crush ruleset AND object namespace */
};

struct nested_info_t {
  utime_t rctime;      // \max_{children}(ctime, nested_ctime)
  __u64 rbytes;
  __u64 rfiles;
  
  void encode(bufferlist &bl) const {
    ::encode(rbytes, bl);
    ::encode(rfiles, bl);
    ::encode(rctime, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(rbytes, bl);
    ::decode(rfiles, bl);
    ::decode(rctime, bl);
  }
};
WRITE_CLASS_ENCODER(nested_info_t)

struct inode_t {
  // base (immutable)
  inodeno_t ino;
  ceph_file_layout layout;  // ?immutable?
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
  uint64_t   size;        // on directory, # dentries
  uint64_t   max_size;    // client(s) are auth to write this much...
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint64_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  // dirfrag, recursive accounting
  nested_info_t accounted_nested;  // what dirfrag has seen
  nested_info_t nested;            // inline summation for child dirfrags.
  /*
   * if accounted_nested does not match nested, the parent dirfrag needs to be 
   * adjusted by the difference.
   */
 
  // special stuff
  version_t version;           // auth only
  version_t file_data_version; // auth only

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }
};

static inline void encode(const inode_t &i, bufferlist &bl) {
  ::encode(i.ino, bl);
  ::encode(i.layout, bl);
  ::encode(i.rdev, bl);
  ::encode(i.ctime, bl);
  ::encode(i.mode, bl);
  ::encode(i.uid, bl);
  ::encode(i.gid, bl);
  ::encode(i.nlink, bl);
  ::encode(i.anchored, bl);
  ::encode(i.size, bl);
  ::encode(i.max_size, bl);
  ::encode(i.mtime, bl);
  ::encode(i.atime, bl);
  ::encode(i.nested, bl);
  ::encode(i.version, bl);
  ::encode(i.file_data_version, bl);
}
static inline void decode(inode_t &i, bufferlist::iterator &p) {
  ::decode(i.ino, p);
  ::decode(i.layout, p);
  ::decode(i.rdev, p);
  ::decode(i.ctime, p);
  ::decode(i.mode, p);
  ::decode(i.uid, p);
  ::decode(i.gid, p);
  ::decode(i.nlink, p);
  ::decode(i.anchored, p);
  ::decode(i.size, p);
  ::decode(i.max_size, p);
  ::decode(i.mtime, p);
  ::decode(i.atime, p);
  ::decode(i.nested, p);
  ::decode(i.version, p);
  ::decode(i.file_data_version, p);
}

/*
 * like an inode, but for a dir frag 
 */
struct fnode_t {
  version_t version;
  utime_t mtime;
  __u64 size;            // files + dirs
  __u64 nprimary, nremote;
  __u64 nfiles;          // files
  __u64 nsubdirs;        // subdirs
  nested_info_t nested;  // nested summation
  nested_info_t accounted_nested;  // nested summation

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(size, bl);
    ::encode(nprimary, bl);
    ::encode(nremote, bl);
    ::encode(nfiles, bl);
    ::encode(nsubdirs, bl);
    ::encode(nested, bl);
    ::encode(accounted_nested, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(size, bl);
    ::decode(nprimary, bl);
    ::decode(nremote, bl);
    ::decode(nfiles, bl);
    ::decode(nsubdirs, bl);
    ::decode(nested, bl);
    ::decode(accounted_nested, bl);
  }
};
WRITE_CLASS_ENCODER(fnode_t)



// dentries
#define MAX_DENTRY_LEN 255


// --

inline ostream& operator<<(ostream& out, ceph_fsid& f) {
  return out << hex << f.major << '.' << f.minor << dec;
}



// -- io helpers --

template<class A, class B>
inline ostream& operator<<(ostream& out, pair<A,B> v) {
  return out << v.first << "," << v.second;
}

template<class A>
inline ostream& operator<<(ostream& out, vector<A>& v) {
  out << "[";
  for (unsigned i=0; i<v.size(); i++) {
    if (i) out << ",";
    out << v[i];
  }
  out << "]";
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const list<A>& ilist) {
  for (typename list<A>::const_iterator it = ilist.begin();
       it != ilist.end();
       it++) {
    if (it != ilist.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const set<A>& iset) {
  for (typename set<A>::const_iterator it = iset.begin();
       it != iset.end();
       it++) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const multiset<A>& iset) {
  for (typename multiset<A>::const_iterator it = iset.begin();
       it != iset.end();
       it++) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A,class B>
inline ostream& operator<<(ostream& out, const map<A,B>& m) 
{
  out << "{";
  for (typename map<A,B>::const_iterator it = m.begin();
       it != m.end();
       it++) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}



#endif
