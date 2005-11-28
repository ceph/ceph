#ifndef __EBOFS_NODES_H
#define __EBOFS_NODES_H

/** nodes, node regions **/

#include "types.h"
#include "BlockDevice.h"
#include "AlignedBufferPool.h"


/* node status on disk, in memory
 *
 *  DISK      MEMORY          ON LISTS         EVENT
 *
 * claim cycle:
 *  free      free            free             -
 *  free      dirty           dirty            mark_dirty()
 *  free      dirty           committing       start commit
 *  inuse     dirty           committing       (write happens)
 *  inuse     clean           -                finish commit
 *
 * release cycle:
 *  inuse     clean           -                -
 *  inuse     free            limbo            release
 *  inuse     free            committing       start_write
 *  free      free            committing       (write happens)
 *  free      free            free             finish_write
 */



class Node {
 public:
  static const int STATUS_FREE = 0;
  static const int STATUS_DIRTY = 1;
  static const int STATUS_CLEAN = 2;

  static const int ITEM_LEN = EBOFS_NODE_BYTES - sizeof(int) - sizeof(int) - sizeof(int);

  static const int TYPE_INDEX = 1;
  static const int TYPE_LEAF = 2;

 protected:
  nodeid_t    id;
  bufferptr   bptr;
  int         *nrecs;
  int         *status;
  int         *type;

 public:
  Node(nodeid_t i) : id(i) {
	bptr = new buffer(EBOFS_NODE_BYTES);
	nrecs = (int*)(bptr.c_str());
	status = (int*)(bptr.c_str() + sizeof(*nrecs));
	type = (int*)(bptr.c_str() + sizeof(*status) + sizeof(*nrecs));
	clear();
  }
  Node(nodeid_t i, bufferptr& b) : id(i), bptr(b) {
	nrecs = (int*)(bptr.c_str());
	status = (int*)(bptr.c_str() + sizeof(*nrecs));
	type = (int*)(bptr.c_str() + sizeof(*status) + sizeof(*nrecs));
  }

  void clear() {
	*nrecs = 0;
	*status = STATUS_FREE;
	*type = 0;
  }
  
  nodeid_t get_id() const { return id; }
  void set_id(nodeid_t n) { id = n; }

  bufferptr& get_buffer() { return bptr; }

  int size() { return *nrecs; }
  void set_size(int s) { *nrecs = s; }
  
  char *item_ptr() { return bptr.c_str() + sizeof(*status) + sizeof(*nrecs) + sizeof(*type); }

  int& get_status() { return *status; }
  bool is_dirty() const { return *status == STATUS_DIRTY; }
  void set_status(int s) { *status = s; }

  int& get_type() { return *type; }
  void set_type(int t) { *type = t; }
  bool is_index() { return *type == TYPE_INDEX; }
  bool is_leaf() { return *type == TYPE_LEAF; } 
};



class NodeRegion {
 public:

  static int make_nodeid(int region, int offset) {
	return (region << 24) | offset;
  }
  static int nodeid_region(nodeid_t nid) {
	return nid >> 24;
  }
  static int nodeid_offset(nodeid_t nid) {
	return nid & (0xffffffffUL >> 24);
  }
  
 protected:
  int          region_id;
  int          num_nodes;

 public:
  Extent       location;

  //    free -> dirty -> committing -> clean
  //      dirty -> free
  //  or  !dirty -> limbo -> free
  set<nodeid_t>     free;
  set<nodeid_t>     dirty;
  set<nodeid_t>     committing;
  set<nodeid_t>     limbo;

 public:
  NodeRegion(int id, Extent& loc) : region_id(id) {
	location = loc;
	num_nodes = location.length / EBOFS_NODE_BLOCKS;
  }
  
  int get_region_id() const { return region_id; }

  void init_all_free() {
	for (unsigned i=0; i<location.length; i++) {
	  nodeid_t nid = make_nodeid(region_id, i);
	  free.insert(nid);
	}
  }

  void induce_full_flush() {
	// free -> limbo : so they get written out as such
	for (set<nodeid_t>::iterator i = free.begin();
		 i != free.end();
		 i++)
	  limbo.insert(*i);
	free.clear();
  }

  int size() const {
	return num_nodes;
	//return (location.length / EBOFS_NODE_BLOCKS);   // FIXME THIS IS WRONG
  }
  int num_free() const {
	return free.size();
  }

  // new/open node
  nodeid_t new_nodeid() {
	assert(num_free());
	
	nodeid_t nid = *(free.begin());
	free.erase(nid);
	dirty.insert(nid);

	return nid;
  }

  void release(nodeid_t nid) {
	if (dirty.count(nid)) {
	  dirty.erase(nid);
	  free.insert(nid);
	} 
	else {
	  if (committing.count(nid))
		committing.erase(nid);
	  limbo.insert(nid);
	}
  }
};



class NodePool {
 protected:
  int num_regions;
  map<int, NodeRegion*> node_regions;  // regions
  map<nodeid_t, Node*>  node_map;      // open node map
  AlignedBufferPool     bufferpool;    // memory pool

 public:
  NodePool() : num_regions(0), bufferpool(EBOFS_NODE_BYTES) {}
  ~NodePool() {
	// nodes
	set<Node*> left;
	for (map<nodeid_t,Node*>::iterator i = node_map.begin();
		 i != node_map.end();
		 i++) 
	  left.insert(i->second);
	for (set<Node*>::iterator i = left.begin();
		 i != left.end();
		 i++) 
	  release( *i );
	assert(node_map.empty());
	
	// regions
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) 
	  delete i->second;
	node_regions.clear();
  }

  void add_region(NodeRegion *r) {
	node_regions[r->get_region_id()] = r;
	num_regions++;
  }
  
  int get_num_regions() {
	return num_regions;
  }
  Extent& get_region_loc(int r) {
	return node_regions[r]->location;
  }


  int num_free() {
	int f = 0;
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) 
	  f += i->second->num_free();
	return f;
  }

  // *** 
  int read(BlockDevice& dev, struct ebofs_nodepool *np) {
	for (int i=0; i<np->num_regions; i++) {
	  dout(3) << " region " << i << " at " << np->region_loc[i] << endl;
	  NodeRegion *r = new NodeRegion(i, np->region_loc[i]);
	  add_region(r);
	  
	  for (block_t boff = 0; boff < r->location.length; boff += EBOFS_NODE_BLOCKS) {
		nodeid_t nid = NodeRegion::make_nodeid(r->get_region_id(), boff);
		
		bufferptr bp = bufferpool.alloc();
		dev.read(r->location.start + (block_t)boff, EBOFS_NODE_BLOCKS, 
				 bp);
		
		Node *n = new Node(nid, bp);
		if (n->get_status() == Node::STATUS_FREE) {
		  dout(5) << "  node " << nid << " free" << endl;
		  r->free.insert(nid);
		  delete n;
		} else {
		  dout(5) << "  node " << nid << " in use" << endl;
		  node_map[nid] = n;
		}
	  }
	}
	return 0;
  }
  void init_all_free() {
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) {
	  i->second->init_all_free();
	}
  }

  void induce_full_flush() {
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) {
	  i->second->induce_full_flush();
	}
  }

  void flush(BlockDevice& dev) {
	// flush dirty items, change them to limbo status
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) {
	  NodeRegion *r = i->second;
	  
	  // dirty -> clean
	  r->committing = r->dirty;
	  r->dirty.clear();

	  for (set<int>::iterator i = r->committing.begin();
		   i != r->committing.end();
		   i++) {
		Node *n = get_node(*i);
		assert(n);  // it's dirty, we better have it
		n->set_status(Node::STATUS_CLEAN);
		block_t off = NodeRegion::nodeid_offset(*i);
		dev.write(r->location.start + off, EBOFS_NODE_BLOCKS, n->get_buffer());
	  }
	  r->committing.clear();

	  // limbo -> free
	  r->committing = r->limbo;
	  r->limbo.clear();
	  
	  bufferptr freebuffer = bufferpool.alloc();
	  Node freenode(1, freebuffer);    
	  freenode.set_status(Node::STATUS_FREE);
	  for (set<int>::iterator i = r->committing.begin();
		   i != r->committing.end();
		   i++) {
		freenode.set_id( *i );
		block_t off = NodeRegion::nodeid_offset(*i);
		dev.write(r->location.start + off, EBOFS_NODE_BLOCKS, freenode.get_buffer());
	  }
	  
	  for (set<int>::iterator i = r->committing.begin();
		   i != r->committing.end();
		   i++) 
		r->free.insert(*i);
	  r->committing.clear();
	}
  }
   


  // *** nodes ***
  // opened node
  Node* get_node(nodeid_t nid) {
	//dbtout << "pool.get " << nid << endl;
	assert(node_map.count(nid));
	return node_map[nid];
  }
  
  // unopened node
  /*
  Node* open_node(nodeid_t nid) {
	Node *n = node_regions[ NodeRegion::nodeid_region(nid) ]->open_node(nid);
	dbtout << "pool.open_node " << n->get_id() << endl;
	node_map[n->get_id()] = n;
	return n;
  }
  */
  
  // new node
  nodeid_t new_nodeid() {
	for (map<int,NodeRegion*>::iterator i = node_regions.begin();
		 i != node_regions.end();
		 i++) {
	  if (i->second->num_free()) 
		return i->second->new_nodeid();
	}
	assert(0);  // full!
	return -1;
  }
  Node* new_node() {
	bufferptr bp = bufferpool.alloc();
	Node *n = new Node(new_nodeid(), bp);
	n->clear();
	dbtout << "pool.new_node " << n->get_id() << endl;
	assert(node_map.count(n->get_id()) == 0);
	node_map[n->get_id()] = n;
	return n;
  }

  void release_nodeid(nodeid_t nid) {
	dbtout << "pool.release_nodeid on " << nid << endl;
	assert(node_map.count(nid) == 0);
	node_regions[ NodeRegion::nodeid_region(nid) ]->release(nid);
	return;
  }
  void release(Node *n) {
	dbtout << "pool.release on " << n->get_id() << endl;
	node_map.erase(n->get_id());
	release_nodeid(n->get_id());
	delete n;
  }

  void dirty_node(Node *n) {
	assert(!n->is_dirty());
	n->set_status(Node::STATUS_DIRTY);
	nodeid_t newid = new_nodeid();
	dbtout << "pool.dirty_node on " << n->get_id() << " now " << newid << endl;
	node_map.erase(n->get_id());
	release_nodeid(n->get_id());
	n->set_id(newid);
	node_map[newid] = n;
  }
  
  
  
};
  
#endif
