#ifndef __EBOFS_TYPES_H
#define __EBOFS_TYPES_H

#include <cassert>
#include "include/buffer.h"
#include "include/Context.h"
#include "common/Cond.h"

#include <ext/hash_map>
#include <set>
#include <list>
#include <vector>
using namespace std;
using namespace __gnu_cxx;


#define MIN(a,b)  ((a)<=(b) ? (a):(b))
#define MAX(a,b)  ((a)>=(b) ? (a):(b))



class C_Cond : public Context {
  Cond *cond;
  int *rval;
public:
  C_Cond(Cond *c, int *r=0) : cond(c), rval(r) {}
  void finish(int r) {
	if (rval) *rval = r;
	//cout << "C_Cond signal " << this << " cond " << (void*)cond << " rval " << (void*)rval << " r " << r  << endl;
	cond->Signal();
  }
};


/*
- this is to make some of the STL types work with 64 bit values, string hash keys, etc.
- added when i was using an old STL.. maybe try taking these out and see if things 
  compile now?
*/

/*
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
*/


// disk
typedef __uint64_t block_t;        // disk location/sector/block

static const int EBOFS_BLOCK_SIZE = 4096;
static const int EBOFS_BLOCK_BITS = 12;    // 1<<12 == 4096

class Extent {
 public:
  block_t start, length;

  Extent() : start(0), length(0) {}
  Extent(block_t s, block_t l) : start(s), length(l) {}

  block_t last() const { return start + length - 1; }
  block_t end() const { return start + length; }
};

inline ostream& operator<<(ostream& out, Extent& ex)
{
  return out << ex.start << "~" << ex.length;
}


// tree/set nodes
typedef int    nodeid_t;

static const int EBOFS_NODE_BLOCKS = 1;
static const int EBOFS_NODE_BYTES = EBOFS_NODE_BLOCKS * EBOFS_BLOCK_SIZE;
static const int EBOFS_MAX_NODE_REGIONS = 10;   // pick a better value!

struct ebofs_nodepool {
  Extent node_usemap_even;   // for even sb versions
  Extent node_usemap_odd;    // for odd sb versions
  
  int    num_regions;
  Extent region_loc[EBOFS_MAX_NODE_REGIONS];
};


// objects
typedef __uint64_t object_t;
typedef __uint64_t coll_t;

struct ebofs_onode {
  Extent     onode_loc;       /* this is actually the block we live in */

  object_t   object_id;       /* for kicks */
  off_t      object_size;     /* file size in bytes.  should this be 64-bit? */
  unsigned   object_blocks;
  
  int        num_attr;        // num attr in onode
  int        num_extents;     /* number of extents used.  if 0, data is in the onode */
};

struct ebofs_cnode {
  Extent     cnode_loc;       /* this is actually the block we live in */
  object_t   coll_id;
  int        num_attr;        // num attr in cnode
};


//static const int EBOFS_MAX_DATA_IN_ONODE = (EBOFS_BLOCK_SIZE - sizeof(struct ebofs_onode));
//static const int EBOFS_MAX_EXTENTS_IN_ONODE = (EBOFS_MAX_DATA_IN_ONODE / sizeof(Extent));


// table
struct ebofs_table {
  nodeid_t root;      /* root node of btree */
  int      num_keys;
  int      depth;
};


// super
typedef __uint64_t version_t;

static const unsigned EBOFS_MAGIC = 0x000EB0F5;
static const unsigned EBOFS_ROOT_INODE = 1;
static const unsigned EBOFS_ROOT_BLOCK = 0;

static const int EBOFS_NUM_FREE_BUCKETS = 16;   /* see alloc.h for bucket constraints */


struct ebofs_super {
  unsigned s_magic;
  
  unsigned epoch;             // version of this superblock.

  unsigned num_blocks;        /* # blocks in filesystem */

  // some basic stats, for kicks
  unsigned free_blocks;       /* unused blocks */
  unsigned limbo_blocks;      /* limbo blocks */
  //unsigned num_objects;
  //unsigned num_fragmented;
  
  struct ebofs_nodepool nodepool;
  
  // tables
  struct ebofs_table free_tab[EBOFS_NUM_FREE_BUCKETS];  
  struct ebofs_table limbo_tab;
  struct ebofs_table object_tab;      // object directory
  struct ebofs_table collection_tab;  // collection directory
  struct ebofs_table oc_tab;
  struct ebofs_table co_tab;
};



/*
 * really simple container for (collection|object) attribute values,
 * which are a (void*,int) pair.  hide associated memory management
 * ugliness.
 */
class AttrVal {
 public:
  char *data;
  int len;
  AttrVal() : data(0), len(0) {}
  AttrVal(char *from, int l) : 
	len(l) {
	data = new char[len];
	memcpy(data, from, len);
  }
  AttrVal(const AttrVal &other) {
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
  }
  AttrVal& operator=(const AttrVal &other) {
	if (data) delete[] data;
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
	return *this;
  }
  ~AttrVal() {
	delete[] data;
  }
};




#endif
