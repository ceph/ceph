
#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/Context.h"
#include "include/bufferlist.h"

#include "types.h"
#include "Onode.h"
#include "BlockDevice.h"
#include "nodes.h"
#include "Allocator.h"
#include "Table.h"
#include "AlignedBufferPool.h"

#include "common/Mutex.h"
#include "common/Cond.h"




class Ebofs {
 protected:
  // ** super **
  bool         mounted;
  BlockDevice &dev;
  version_t    super_version;

  int write_super();

  // ** allocator **
  block_t      free_blocks;
  Allocator    allocator;
  friend class Allocator;

  // ** tables and sets **
  NodePool     table_nodepool;   // for primary tables.
  //NodePool     set_nodepool;     // for collections, etc.
  
  AlignedBufferPool bufferpool;

  // object map: object -> onode_loc
  Table<object_t, Extent>     *object_tab;

  // collection map: id -> cnode_loc
  Table<coll_t, Extent>       *collection_tab;

  // free list
  Table<block_t,block_t>      *free_tab[EBOFS_NUM_FREE_BUCKETS];



  // ** cache **
  hash_map<object_t, Onode*>  onode_map;  // onode cache
  LRU                         onode_lru;

  Onode* new_onode(object_t oid);     // make new onode.  ref++.
  Onode* get_onode(object_t oid);     // get cached onode, or read from disk.  ref++.
  void   write_onode(Onode *on);
  void   remove_onode(Onode *on);
  void   put_onode(Onode* o);         // put it back down.  ref--.

 public:
  Ebofs(BlockDevice& d) : 
	dev(d),
	free_blocks(0), allocator(this),
	bufferpool(EBOFS_BLOCK_SIZE),
	object_tab(0), collection_tab(0) {
	for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
	  free_tab[i] = 0;
  }
  ~Ebofs() {
  }

  int mkfs();
  int mount();
  int umount();
  

  // object interface
  int read(object_t, size_t len, off_t off, bufferlist& bl);
  int write(object_t oid,
			size_t len, off_t offset,
			bufferlist& bl,
			bool fsync=true);
  int write(object_t oid, 
			size_t len, off_t offset, 
			bufferlist& bl, 
			Context *onsafe);

  // attr
  int setattr(object_t oid, const char *name, void *value, size_t size);
  int getattr(object_t oid, const char *name, void *value, size_t size);
  int listattr(object_t oid, char *attrs, size_t max);
  
  // collections
  // ...  

};
