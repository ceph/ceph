// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
#define OBJECT_LAYOUT_HASH    1
#define OBJECT_LAYOUT_LINEAR  2
#define OBJECT_LAYOUT_HASHINO 3

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

#define FILE_LAYOUT_CRUSH    0    // stripe via crush
#define FILE_LAYOUT_LINEAR   1    // stripe linearly across cluster

struct FileLayout {
  // layout
  int policy;          // FILE_LAYOUT_*

  // FIXME: make this a union?
  // rushstripe
  int stripe_size;     // stripe unit, in bytes
  int stripe_count;    // over this many objects
  int object_size;     // until objects are this big, then use a new set of objects.

  // osdlocal
  int osd;

  int num_rep;  // replication

  FileLayout() { }
  FileLayout(int ss, int sc, int os, int nr=2) :
	policy(FILE_LAYOUT_CRUSH),
	   stripe_size(ss), stripe_count(sc), object_size(os), 
	   num_rep(nr) { }
  /*FileLayout(int o) :
	policy(FILE_LAYOUT_OSDLOCAL),
	   osd(o),
	   num_rep(1) { }
  */
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


// osd types
typedef __uint64_t ps_t;          // placement seed
typedef __uint64_t pg_t;          // placement group
typedef __uint64_t object_t;      // object id
typedef __uint64_t coll_t;        // collection id

#define PG_NONE    0xffffffffffffffffLL


struct ostat {
  object_t   object_id;
  size_t     size;
  time_t     ctime;
  time_t     mtime;
};


struct onode_t {
  object_t    oid;
  pg_t        pgid;
  version_t   version;
  size_t      size;
  //time_t      ctime, mtime;
};

class pginfo_t {
 public:
  version_t created;
  version_t last_clean;
  version_t last_complete;
  version_t primary_since;

  pginfo_t() : created(0), last_clean(0), last_complete(0), primary_since(0) { }
} ;



// client types
typedef int        fh_t;          // file handle 


// dentries
#define MAX_DENTRY_LEN 255




// -- load balancing stuff --




// popularity
#define MDS_POP_JUSTME  0   // just me
#define MDS_POP_NESTED  1   // me + children, auth or not
#define MDS_POP_CURDOM  2   // me + children in current domain
#define MDS_POP_ANYDOM  3   // me + children in any (nested) domain
#define MDS_NPOP        4

class mds_load_t {
 public:
  double root_pop;
  double req_rate, rd_rate, wr_rate;
  double cache_hit_rate;
  
  mds_load_t() : 
	root_pop(0), req_rate(0), rd_rate(0), wr_rate(0), cache_hit_rate(0) { }
	
};



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
