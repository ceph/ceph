#ifndef __MDS_TYPES_H
#define __MDS_TYPES_H

#include <sys/types.h>
#include <ext/hash_map>
#include <string>
#include <set>
#include <iostream>
using namespace std;


// md ops
#define MDS_OP_STATFS   1

#define MDS_OP_STAT     100
#define MDS_OP_TOUCH    101  // this is made up, not a real POSIX thing
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

#define MDS_OP_OPENRD   301
#define MDS_OP_OPENWR   302
#define MDS_OP_OPENWRC  303
#define OSD_OP_READ     304
#define OSD_OP_WRITE    305
#define MDS_OP_TRUNCATE 306
#define MDS_OP_FSYNC    307
#define MDS_OP_CLOSE    310







// -- stl crap --

namespace __gnu_cxx {
  template<> struct hash<unsigned long long> {
	size_t operator()(unsigned long long __x) const { return __x; }
  };
  
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
      return hash< char >()( (x.c_str())[0] );
    }
  };
}


// -- raw inode --

typedef __uint64_t inodeno_t;   // ino

typedef __uint64_t mdloc_t;     // dir locator?

struct inode_t {
  // immutable
  inodeno_t ino;   // NOTE: this must come first
  time_t ctime;

  // hard (perm)
  mode_t mode;
  uid_t uid;
  gid_t gid;

  // soft
  __uint64_t size;
  time_t atime, mtime;

  // special stuff
  unsigned char hash_seed;  // 0 if not hashed.
};

#define INODE_MODE_FILE     0100000 // S_IFREG
#define INODE_MODE_SYMLINK  0120000 // S_IFLNK
#define INODE_MODE_DIR      0040000 // S_IFDIR





#define MAX_DENTRY_LEN 255

typedef __uint64_t object_t;


// -- load balancing stuff --

// popularity
#define MDS_POP_ANY     0
#define MDS_POP_SOFTRD  1
#define MDS_POP_SOFTWR  2
#define MDS_POP_HARDRD  3
#define MDS_POP_HARDWR  4
#define MDS_NPOP        5

class mds_load_t {
 public:
  double root_pop;
  double req_rate, rd_rate, wr_rate;
  double cache_hit_rate;
  
  mds_load_t() : 
	root_pop(0), req_rate(0), rd_rate(0), wr_rate(0), cache_hit_rate(0) { }
	
};



// -- io helpers --

inline ostream& operator<<(ostream& out, set<int>& iset) {
  for (set<int>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (it != iset.begin()) out << ",";
	out << *it;
  }
  return out;
}

inline ostream& operator<<(ostream& out, multiset<int>& iset) {
  for (multiset<int>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (it != iset.begin()) out << ",";
	out << *it;
  }
  return out;
}

#endif
