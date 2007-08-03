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

typedef uint64_t tid_t;         // transaction id
typedef uint64_t version_t;
typedef uint32_t epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)


// object and pg layout
// specified in g_conf.osd_*

#define O_LAZY 01000000


/** object layout
 * how objects are mapped into PGs
 */
#define OBJECT_LAYOUT_HASH     1
#define OBJECT_LAYOUT_LINEAR   2
#define OBJECT_LAYOUT_HASHINO  3

/** pg layout
 * how PGs are mapped into (sets of) OSDs
 */
#define PG_LAYOUT_CRUSH  0   
#define PG_LAYOUT_HASH   1
#define PG_LAYOUT_LINEAR 2
#define PG_LAYOUT_HYBRID 3



// -----------------------
// FileLayout

/** FileLayout 
 * specifies a striping and replication strategy
 */

//#define FILE_LAYOUT_CRUSH    0    // stripe via crush
//#define FILE_LAYOUT_LINEAR   1    // stripe linearly across cluster

struct FileLayout {
  // -- file -> object mapping --
  int32_t stripe_unit;     // stripe unit, in bytes
  int32_t stripe_count;    // over this many objects
  int32_t object_size;     // until objects are this big, then move to new objects

  int stripe_width() { return stripe_unit * stripe_count; }

  // period = bytes before i start on a new set of objects.
  int period() { return object_size * stripe_count; }

  // -- object -> pg layout --
  char pg_type;        // pg type (replicated, raid, etc.) (see pg_t::TYPE_*)
  char pg_size;        // pg size (num replicas, or raid4 stripe width)
  int32_t  preferred;  // preferred primary osd?

  // -- pg -> disk layout --
  int32_t  object_stripe_unit;  // for per-object raid

  FileLayout() { }
  FileLayout(int su, int sc, int os, int pgt, int pgs, int o=-1) :
    stripe_unit(su), stripe_count(sc), object_size(os), 
    pg_type(pgt), pg_size(pgs), preferred(o),
    object_stripe_unit(su)   // note: bad default, we pbly want su/(pgs-1)
  {
    assert(object_size % stripe_unit == 0);
  }

};




// --------------------------------------
// inode

typedef uint64_t _inodeno_t;

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
      static hash<uint64_t> H;
      return H(x.val);
    }
  };
}


#define INODE_MODE_FILE     0100000 // S_IFREG
#define INODE_MODE_SYMLINK  0120000 // S_IFLNK
#define INODE_MODE_DIR      0040000 // S_IFDIR
#define INODE_TYPE_MASK     0170000

#define FILE_MODE_R          1
#define FILE_MODE_W          2
#define FILE_MODE_RW         (1|2)
#define FILE_MODE_LAZY       4

#define INODE_MASK_INO        1    // inode
#define INODE_MASK_TYPE       2   // file type bits of the mode
#define INODE_MASK_BASE       4   // ino, layout, symlink value
#define INODE_MASK_AUTH       8   // uid, gid, mode
#define INODE_MASK_LINK       16   // nlink, anchored
#define INODE_MASK_FILE       32  // mtime, size.
// atime?

#define INODE_MASK_ALL_STAT  (INODE_MASK_BASE|INODE_MASK_AUTH|INODE_MASK_LINK|INODE_MASK_FILE)

#define INODE_MASK_SIZE       INODE_MASK_FILE // size, blksize, blocks
#define INODE_MASK_MTIME      INODE_MASK_FILE // mtime
#define INODE_MASK_ATIME      INODE_MASK_FILE // atime
#define INODE_MASK_CTIME      (INODE_MASK_FILE|INODE_MASK_AUTH|INODE_MASK_LINK) // ctime

inline int DT_TO_MODE(int dt) {
  switch (dt) {
  case DT_REG: return INODE_MODE_FILE;
  case DT_DIR: return INODE_MODE_DIR;
  case DT_LNK: return INODE_MODE_SYMLINK;
  default: assert(0); return 0;
  }
}

struct inode_t {
  // base (immutable)
  inodeno_t ino;
  FileLayout layout;  // ?immutable?

  // affected by any inode change...
  utime_t    ctime;   // inode change time

  // perm (namespace permissions)
  mode_t     mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;  
  bool       anchored;          // auth only?

  // file (data access)
  off_t      size, max_size, allocated_size;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
 
  // special stuff
  version_t version;           // auth only
  version_t file_data_version; // auth only

  // file type
  bool is_symlink() { return (mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK; }
  bool is_dir()     { return (mode & INODE_TYPE_MASK) == INODE_MODE_DIR; }
  bool is_file()    { return (mode & INODE_TYPE_MASK) == INODE_MODE_FILE; }

  // corresponding d_types
  static const unsigned char DT_REG = 8;
  static const unsigned char DT_DIR = 4;
  static const unsigned char DT_LNK = 10;
};

inline unsigned char MODE_TO_DT(int mode) {
  if (S_ISREG(mode)) return inode_t::DT_REG;
  if (S_ISLNK(mode)) return inode_t::DT_LNK;
  if (S_ISDIR(mode)) return inode_t::DT_DIR;
  assert(0);
  return 0;
}






// dentries
#define MAX_DENTRY_LEN 255




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
