#ifndef __MDS_TYPES_H
#define __MDS_TYPES_H

#include <sys/types.h>
#include <ext/hash_map>

// raw inode

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


typedef __uint64_t inodeno_t;   // ino

typedef __uint64_t mdloc_t;     // dir locator?

struct inode_t {
  inodeno_t ino;

  __uint32_t touched;
  __uint64_t size;
  __uint32_t mode;
  uid_t uid;
  gid_t gid;
  time_t atime, mtime, ctime;
  bool isdir;
};

typedef __uint64_t object_t;

class mds_load_t {
 public:
  double root_pop;
  double req_rate, rd_rate, wr_rate;
  double cache_hit_rate;
  
  mds_load_t() : 
	root_pop(0), req_rate(0), rd_rate(0), wr_rate(0), cache_hit_rate(0) { }
	
};



#endif
