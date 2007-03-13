// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include "statlite.h"
}

#include <string>
#include <set>
#include <map>
#include <vector>
#include <iostream>
#include <iomanip>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;

#include "object.h"

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif
#ifndef MAX
# define MAX(a,b) ((a) > (b) ? (a):(b))
#endif


// -- stl crap --

/*
- this is to make some of the STL types work with 64 bit values, string hash keys, etc.
- added when i was using an old STL.. maybe try taking these out and see if things 
  compile now?
*/

class blobhash {
public:
  size_t operator()(const char *p, unsigned len) {
    static hash<long> H;
    long acc = 0;
    while (len >= sizeof(long)) {
      acc ^= *(long*)p;
      p += sizeof(long);
      len -= sizeof(long);
    }   
    int sh = 0;
    while (len) {
      acc ^= (long)*p << sh;
      sh += 8;
      len--;
      p++;
    }
    return H(acc);
  }
};


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
  template<> struct hash<__int64_t> {
    size_t operator()(__int64_t __x) const { 
      static hash<__int32_t> H;
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

typedef __uint64_t tid_t;         // transaction id
typedef __uint64_t version_t;
typedef __uint32_t epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)





/** object layout
 * how objects are mapped into PGs
 */
#define OBJECT_LAYOUT_DEFAULT  0  // see g_conf
#define OBJECT_LAYOUT_HASH     1
#define OBJECT_LAYOUT_LINEAR   2
#define OBJECT_LAYOUT_HASHINO  3
#define OBJECT_LAYOUT_STARTOSD 4

/** pg layout
 * how PGs are mapped into (sets of) OSDs
 */
#define PG_LAYOUT_CRUSH  0   
#define PG_LAYOUT_HASH   1
#define PG_LAYOUT_LINEAR 2
#define PG_LAYOUT_HYBRID 3

/** FileLayout 
 * specifies a striping and replication strategy
 */

//#define FILE_LAYOUT_CRUSH    0    // stripe via crush
//#define FILE_LAYOUT_LINEAR   1    // stripe linearly across cluster

struct FileLayout {
  // layout
  int object_layout;

  // FIXME: make this a union?
  // rushstripe
  int stripe_size;     // stripe unit, in bytes
  int stripe_count;    // over this many objects
  int object_size;     // until objects are this big, then use a new set of objects.

  // period = bytes before i start on a new set of objects.
  int period() { return object_size * stripe_count; }

  int osd;    // osdlocal

  int num_rep;  // replication

  FileLayout() { }
  FileLayout(int ss, int sc, int os, int nr=2, int o=-1) :
    object_layout(o < 0 ? OBJECT_LAYOUT_DEFAULT:OBJECT_LAYOUT_STARTOSD),
    stripe_size(ss), stripe_count(sc), object_size(os), 
    osd(o),
    num_rep(nr) { }

};



// -- inode --

struct inodeno_t {
  __uint64_t val;
  inodeno_t() : val() {}
  inodeno_t(__uint64_t v) : val(v) {}
  inodeno_t operator+=(inodeno_t o) { val += o.val; return *this; }
  operator __uint64_t() const { return val; }
};

inline ostream& operator<<(ostream& out, inodeno_t ino) {
  return out << hex << ino.val << dec;
}

namespace __gnu_cxx {
  template<> struct hash< inodeno_t >
  {
    size_t operator()( const inodeno_t& x ) const
    {
      static hash<__uint64_t> H;
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

#define INODE_MASK_BASE       1  // ino, ctime, nlink
#define INODE_MASK_PERM       2  // uid, gid, mode
#define INODE_MASK_SIZE       4  // size, blksize, blocks
#define INODE_MASK_MTIME      8  // mtime
#define INODE_MASK_ATIME      16 // atime

#define INODE_MASK_ALL_STAT  (INODE_MASK_BASE|INODE_MASK_PERM|INODE_MASK_SIZE|INODE_MASK_MTIME)
//#define INODE_MASK_ALL_STAT  (INODE_MASK_BASE|INODE_MASK_PERM|INODE_MASK_SIZE|INODE_MASK_MTIME|INODE_MASK_ATIME)

struct inode_t {
  // base (immutable)
  inodeno_t ino;   // NOTE: ino _must_ come first for MDStore.cc to behave!!
  time_t    ctime;

  // other
  FileLayout layout;  // ?immutable?
  int        nlink;   // base, 

  // hard/perm (namespace permissions)
  mode_t     mode;
  uid_t      uid;
  gid_t      gid;

  // file (data access)
  off_t      size;
  time_t     atime, mtime;      // maybe atime different?  "lazy"?
  
  int        mask;

  // special stuff
  version_t     version;           // auth only
  unsigned char hash_seed;         // only defined for dir; 0 if not hashed.
  bool          anchored;          // auth only
  version_t     file_data_version; // auth only

  bool is_symlink() { return (mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK; }
  bool is_dir() { return (mode & INODE_TYPE_MASK) == INODE_MODE_DIR; }
  bool is_file() { return (mode & INODE_TYPE_MASK) == INODE_MODE_FILE; }
};




// client types
typedef int        fh_t;          // file handle 


// dentries
#define MAX_DENTRY_LEN 255




// -- io helpers --

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




// -- rope helpers --

// string
inline void _rope(string& s, crope& r) 
{
  r.append(s.c_str(), s.length()+1);
}
inline void _unrope(string& s, crope& r, int& off)
{
  s = r.c_str() + off;
  off += s.length() + 1;
}

// set<int>
inline void _rope(set<int>& s, crope& r)
{
  int n = s.size();
  r.append((char*)&n, sizeof(n));
  for (set<int>::iterator it = s.begin();
       it != s.end();
       it++) {
    int v = *it;
    r.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
inline void _unrope(set<int>& s, crope& r, int& off) 
{
  s.clear();
  int n;
  r.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    int v;
    r.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s.insert(v);
  }
  assert(s.size() == (unsigned)n);
}

#endif
