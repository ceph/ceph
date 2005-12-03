
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
  Mutex        ebofs_lock;    // a beautiful global lock

  // ** super **
  bool         mounted;
  BlockDevice &dev;
  version_t    super_version;

  int write_super();

  // ** allocator **
  block_t      free_blocks;
  Allocator    allocator;
  friend class Allocator;

  // ** buffers **
  AlignedBufferPool bufferpool;


  // ** tables and sets **
  // nodes
  NodePool     table_nodepool;   // for primary tables.

  // tables
  Table<object_t, Extent>     *object_tab;
  Table<coll_t, Extent>       *collection_tab;
  Table<block_t,block_t>      *free_tab[EBOFS_NUM_FREE_BUCKETS];

  // sets?


  // ** onode cache **
  hash_map<object_t, Onode*>  onode_map;  // onode cache
  LRU                         onode_lru;

  Onode* new_onode(object_t oid);     // make new onode.  ref++.
  Onode* get_onode(object_t oid);     // get cached onode, or read from disk.  ref++.
  void write_onode(Onode *on);
  void remove_onode(Onode *on);
  void put_onode(Onode* o);         // put it back down.  ref--.

  void trim_onode_cache();


  // ** buffer cache **
  BufferCache bc;
  pthread_t flushd_thread_id;

 public:
  void trim_buffer_cache();
  void flush_all();
 protected:

  void zero(Onode *on, size_t len, off_t off, off_t write_thru);
  void apply_write(Onode *on, size_t len, off_t off, bufferlist& bl);
  bool attempt_read(Onode *on, size_t len, off_t off, bufferlist& bl);

  // io
  void bh_read(Onode *on, BufferHead *bh);
  void bh_write(Onode *on, BufferHead *bh);

  friend class C_E_FlushPartial;

  int flushd_thread();
  static int flusd_thread_entry(void *p) {
	return ((Ebofs*)p)->flushd_thread();
  }

 public:
  Ebofs(BlockDevice& d) : 
	dev(d),
	free_blocks(0), allocator(this),
	bufferpool(EBOFS_BLOCK_SIZE),
	object_tab(0), collection_tab(0),
	bc(dev, bufferpool) {
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
			Context *onsafe);

  // attr
  int setattr(object_t oid, const char *name, void *value, size_t size);
  int getattr(object_t oid, const char *name, void *value, size_t size);
  int listattr(object_t oid, char *attrs, size_t max);
  
  // collections
  // ...  

};
