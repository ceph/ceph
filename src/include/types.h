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


#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif
#ifndef MAX
# define MAX(a,b) ((a) > (b) ? (a):(b))
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



// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef __u64 tid_t;         // transaction id
typedef __u64 version_t;
typedef __u32 epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)


#define O_LAZY 01000000



typedef ceph_file_layout FileLayout;


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


#define FILE_MODE_R          1
#define FILE_MODE_W          2
#define FILE_MODE_RW         (1|2)
#define FILE_MODE_LAZY       4

/** stat masks
 */
#define STAT_MASK_INO        1   // inode nmber
#define STAT_MASK_TYPE       2   // file type bits of the mode
#define STAT_MASK_BASE       4   // layout, symlink value
#define STAT_MASK_AUTH       8   // uid, gid, mode
#define STAT_MASK_LINK       16   // nlink, anchored
#define STAT_MASK_FILE       32  // mtime, size.

#define STAT_MASK_ALL        63

#define STAT_MASK_SIZE       STAT_MASK_FILE // size, blksize, blocks
#define STAT_MASK_MTIME      STAT_MASK_FILE // mtime
#define STAT_MASK_ATIME      STAT_MASK_FILE // atime
#define STAT_MASK_CTIME      (STAT_MASK_FILE|STAT_MASK_AUTH|STAT_MASK_LINK) // ctime

inline int DT_TO_MODE(int dt) {
  return dt << 12;
}
inline unsigned char MODE_TO_DT(int mode) {
  return mode >> 12;
}

struct inode_t {
  // base (immutable)
  inodeno_t ino;
  FileLayout layout;  // ?immutable?
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
  int64_t    size, max_size, allocated_size;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  utime_t    rmtime;  // recursive mtime
 
  // special stuff
  version_t version;           // auth only
  version_t file_data_version; // auth only

  // file type
  bool is_symlink() { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    { return (mode & S_IFMT) == S_IFREG; }
};







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
