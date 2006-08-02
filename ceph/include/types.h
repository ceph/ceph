// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include <sys/types.h>
#include <assert.h>

#include <string>
#include <set>
#include <map>
#include <vector>
#include <iostream>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif
#ifndef MAX
# define MAX(a,b) ((a) > (b) ? (a):(b))
#endif


// md ops
#define MDS_OP_STATFS   1

#define MDS_OP_STAT     100
#define MDS_OP_LSTAT    101
#define MDS_OP_UTIME    102
#define MDS_OP_CHMOD    103
#define MDS_OP_CHOWN    104  


#define MDS_OP_READDIR  200
#define MDS_OP_MKNOD    201
#define MDS_OP_LINK     202
#define MDS_OP_UNLINK   203
#define MDS_OP_RENAME   204

#define MDS_OP_MKDIR    220
#define MDS_OP_RMDIR    221
#define MDS_OP_SYMLINK  222

#define MDS_OP_OPEN     301
#define MDS_OP_TRUNCATE 306
#define MDS_OP_FSYNC    307
//#define MDS_OP_CLOSE    310
#define MDS_OP_RELEASE  308



// -- stl crap --

/*
- this is to make some of the STL types work with 64 bit values, string hash keys, etc.
- added when i was using an old STL.. maybe try taking these out and see if things 
  compile now?
*/

namespace __gnu_cxx {
  template<> struct hash<unsigned long long> {
	size_t operator()(unsigned long long __x) const { 
	  static hash<unsigned long> H;
	  return H((__x >> 32) ^ (__x & 0xffffffff)); 
	}
  };
  
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
	  static hash<const char*> H;
      return H(x.c_str());
    }
  };
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

  // osdlocal
  int osd;

  int num_rep;  // replication

  FileLayout() { }
  FileLayout(int ss, int sc, int os, int nr=2, int o=-1) :
	object_layout(o < 0 ? OBJECT_LAYOUT_DEFAULT:OBJECT_LAYOUT_STARTOSD),
	stripe_size(ss), stripe_count(sc), object_size(os), 
	osd(o),
	num_rep(nr) { }

};



// -- inode --

/** object id
 * msb[ ino bits | ono bits ]lsb
 * from LSB to MSB 
 */

#define OID_ONO_BITS       32        // 1mb * 10^9 = 1 petabyte files
#define OID_INO_BITS       (64-32)   // 2^34 =~ 16 billion files

typedef __uint64_t inodeno_t;   // 34-bit ino (for now!)

typedef __uint64_t version_t;


#define INODE_MODE_FILE     0100000 // S_IFREG
#define INODE_MODE_SYMLINK  0120000 // S_IFLNK
#define INODE_MODE_DIR      0040000 // S_IFDIR
#define INODE_TYPE_MASK     0170000

#define FILE_MODE_R          1
#define FILE_MODE_W          2
#define FILE_MODE_RW         3

struct inode_t {
  // immutable
  inodeno_t ino;   // NOTE: ino _must_ come first for MDStore.cc to behave!!
  time_t    ctime;

  FileLayout layout;  // ?immutable?

  // hard (namespace permissions)
  mode_t     mode;
  uid_t      uid;
  gid_t      gid;

  // file (data access)
  off_t      size;
  time_t     atime, mtime;      // maybe atime different?  "lazy"?
  
  // other
  int        nlink;

  // special stuff
  unsigned char hash_seed;         // only defined for dir; 0 if not hashed.
  bool          anchored;          // auth only
  version_t     file_data_version; // auth only
};



// lame 128-bit value class.
class lame128_t {
public:
  __uint64_t hi, lo;
  lame128_t(__uint64_t h=0, __uint64_t l=0) : hi(h), lo(l) {}
};

inline ostream& operator<<(ostream& out, lame128_t& oid) {
  return out << oid.hi << "." << oid.lo;
}


// osd types
typedef __uint64_t ps_t;          // placement seed
typedef __uint64_t pg_t;          // placement group
typedef __uint64_t coll_t;        // collection id
typedef __uint64_t epoch_t;       // map epoch
typedef __uint64_t tid_t;         // transaction id

// compound rados version type
class eversion_t {
public:
  epoch_t epoch;
  version_t version;
  eversion_t(epoch_t e=0, version_t v=0) : epoch(e), version(v) {}
};

inline bool operator==(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) && (l.version == r.version);
}
inline bool operator!=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch != r.epoch) || (l.version != r.version);
}
inline bool operator<(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version < r.version):(l.epoch < r.epoch);
}
inline bool operator<=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version <= r.version):(l.epoch <= r.epoch);
}
inline bool operator>(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version > r.version):(l.epoch > r.epoch);
}
inline bool operator>=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version >= r.version):(l.epoch >= r.epoch);
}
inline ostream& operator<<(ostream& out, const eversion_t e) {
  return out << e.epoch << "." << e.version;
}


#ifdef OBJECT128
typedef lame128_t object_t;
#else
typedef __uint64_t object_t;      // object id
#endif

#define PG_NONE    0xffffffffffffffffLL


class OSDSuperblock {
public:
  __uint64_t fsid;      // unique fs id (random number)
  int        whoami;    // my role in this fs.
  epoch_t    current_epoch;             // most recent epoch
  epoch_t    oldest_map, newest_map;    // oldest/newest maps we have.
  OSDSuperblock(__uint64_t f=0, int w=0) : 
	fsid(f), whoami(w), 
	current_epoch(0), oldest_map(0), newest_map(0) {}
};

inline ostream& operator<<(ostream& out, OSDSuperblock& sb)
{
  return out << "sb(fsid " << sb.fsid
			 << " osd" << sb.whoami
			 << " e" << sb.current_epoch
			 << " [" << sb.oldest_map << "," << sb.newest_map
			 << "])";
}


// new types

class ObjectExtent {
 public:
  object_t    oid;       // object id
  pg_t        pgid;     
  off_t       start;     // in object
  size_t      length;    // in object
  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent(object_t o=0, off_t s=0, size_t l=0) : oid(o), start(s), length(l) { }
};

inline ostream& operator<<(ostream& out, ObjectExtent &ex)
{
  return out << "extent(" 
			 << hex << ex.oid << " in " << ex.pgid << dec
			 << " " << ex.start << "~" << ex.length
			 << ")";
}



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

inline ostream& operator<<(ostream& out, set<int>& iset) {
  for (set<int>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (it != iset.begin()) out << ",";
	out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, set<A>& iset) {
  for (typename set<A>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (it != iset.begin()) out << ",";
	out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, multiset<A>& iset) {
  for (typename multiset<A>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (it != iset.begin()) out << ",";
	out << *it;
  }
  return out;
}

template<class A,class B>
inline ostream& operator<<(ostream& out, map<A,B>& m) 
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
