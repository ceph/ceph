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


#ifndef CEPH_EBOFS_NODES_H
#define CEPH_EBOFS_NODES_H

/** nodes, node regions **/

#include "types.h"
#include "BlockDevice.h"
#include "include/xlist.h"
#include "include/bitmapper.h"

/*

     disk     wire    memory                

     free             free    -> free             can alloc
     free             used    -> dirty            can modify

     free     used    used    -> clean
     free     used    free    -> limbo 

     used             used    -> clean
     used             free    -> limbo


        // meaningless
     used     free    free    -> free             can alloc
     used     free    used    __DNE__


*/

#undef debofs
#define debofs(x) do { if (x <= g_conf.debug_ebofs) { \
  _dout_begin_line(x); *_dout << "ebofs.nodepool."


class Node {
 public:
  // bit fields
  static const int STATE_CLEAN = 1;   
  static const int STATE_DIRTY = 2; 

  static const int ITEM_LEN = EBOFS_NODE_BYTES - sizeof(int) - sizeof(int) - sizeof(int);

  static const int TYPE_INDEX = 1;
  static const int TYPE_LEAF = 2;

 protected:
  nodeid_t    id;
  int         pos_in_bitmap;  // position in bitmap
  int         state;     // use bit fields above!

  bufferptr   bptr;

  // in disk buffer
  int         *type;
  int         *nrecs;

 public:
  xlist<Node*>::item xlist;  // dirty

  vector<Node*> children;

  Node(nodeid_t i, int pib, bufferptr& b, int s) : 
    id(i), pos_in_bitmap(pib), 
    state(s), bptr(b), xlist(this)  {
    setup_pointers();
  }

  void setup_pointers() {
    nrecs = (int*)(bptr.c_str());
    type = (int*)(bptr.c_str() + sizeof(*nrecs));
  }

  bool do_cow() {
    if (bptr.do_cow()) {
      setup_pointers();
      return true;
    }
    return false;
  }


  // id
  nodeid_t get_id() const { return id; }
  void set_id(nodeid_t n) { id = n; }
  int get_pos_in_bitmap() const { return pos_in_bitmap; }
  void set_pos_in_bitmap(int i) { pos_in_bitmap = i; }

  // buffer
  bufferptr& get_buffer() { return bptr; }

  char *item_ptr() { return bptr.c_str() + sizeof(*nrecs) + sizeof(*type); }

  // size
  int size() { return *nrecs; }
  void set_size(int s) { *nrecs = s; }
  
  // type
  int& get_type() { return *type; }
  void set_type(int t) { *type = t; }
  bool is_index() { return *type == TYPE_INDEX; }
  bool is_leaf() { return *type == TYPE_LEAF; } 


  // state
  bool is_dirty() { return state == STATE_DIRTY; }
  bool is_clean() { return state == STATE_CLEAN; }

  void set_state(int s) { state = s; }
  
};





class NodePool {
 protected:
  //hash_map<nodeid_t, Node*, rjhash<uint64_t> >  node_map;      // open node map
  unordered_map<nodeid_t, Node*, rjhash<uint64_t> >  node_map;      // open node map
  //map<nodeid_t, Node*> node_map;
  
 public:
  vector<extent_t> region_loc;    // region locations
  extent_t         usemap_even;
  extent_t         usemap_odd;

  buffer::ptr usemap_data;
  bitmapper  usemap_bits;
  
 protected:
  // on-disk block states
  int num_nodes;
  int num_dirty;
  int num_clean;
  int num_free;
  int num_limbo;

  xlist<Node*> dirty_ls;
  interval_set<nodeid_t> free;
  interval_set<nodeid_t> limbo;
  
  Mutex        &ebofs_lock;
  Cond          commit_cond;
  int           flushing;

  nodeid_t make_nodeid(int region, int offset) {
    return region_loc[region].start + (block_t)offset;
  }
  int nodeid_pos_in_bitmap(nodeid_t nid) {
    unsigned region;
    int num = 0;
    for (region = 0; 
	 (block_t)nid < region_loc[region].start || (block_t)nid > region_loc[region].end(); 
	 region++) {
      //generic_dout(-20) << "node " << nid << " not in " << region << " " << region_loc[region] << dendl;
      num += region_loc[region].length;
    }
    num += nid - region_loc[region].start;
    //generic_dout(-20) << "node " << nid << " is in " << region << ", overall bitmap pos is " << num << dendl;
    return num;
  }


 public:
  NodePool(Mutex &el) : 
    num_nodes(0), 
    num_dirty(0), num_clean(0), num_free(0), num_limbo(0),
    ebofs_lock(el),
    flushing(0) {}
  ~NodePool() {
    // nodes
    release_all();
  }

  int get_num_free() { return num_free; }
  int get_num_dirty() { return num_dirty; }
  int get_num_limbo() { return num_limbo; }
  int get_num_clean() { return num_clean; }
  int get_num_total() { return num_nodes; }
  int get_num_used() { return num_clean + num_dirty; }

  int get_usemap_len(int n=0) {
    if (n == 0) n = num_nodes;
    return ((n-1) / 8 / EBOFS_BLOCK_SIZE) + 1;
  }

  unsigned num_regions() { return region_loc.size(); }

  // the caller had better adjust usemap locations...
  void add_region(extent_t ex) {
    assert(region_loc.size() < EBOFS_MAX_NODE_REGIONS);
    region_loc.push_back(ex);
    free.insert(ex.start, ex.length);
    num_free += ex.length;
    num_nodes += ex.length;
  }
  
  void init_usemap() {
    usemap_data = buffer::create_page_aligned(EBOFS_BLOCK_SIZE*usemap_even.length);
    usemap_data.zero();
    usemap_bits.set_data(usemap_data.c_str(), usemap_data.length());
  }
  
  void expand_usemap() {
    block_t have = usemap_data.length() / EBOFS_BLOCK_SIZE;
    if (have < usemap_even.length) {
      // use bufferlist to copy/merge two chunks
      bufferlist bl;
      bl.push_back(usemap_data);
      bufferptr newbit = buffer::create_page_aligned(EBOFS_BLOCK_SIZE*(usemap_even.length - have));
      newbit.zero();
      bl.push_back(newbit);
      bl.rebuild();
      assert(bl.buffers().size() == 1);
      usemap_data = bl.buffers().front();
      usemap_bits.set_data(usemap_data.c_str(), usemap_data.length());
    }
  }



  int init(struct ebofs_nodepool *np) {
    // regions
    assert(region_loc.empty());
    num_nodes = 0;
    for (unsigned i=0; i<np->num_regions; i++) {
      debofs(3) << "init region " << i << " at " << np->region_loc[i] << dendl;
      region_loc.push_back( np->region_loc[i] );
      num_nodes += np->region_loc[i].length;
    }

    // usemap
    usemap_even = np->node_usemap_even;
    usemap_odd = np->node_usemap_odd;
    debofs(3) << "init even map at " << usemap_even << dendl;
    debofs(3) << "init  odd map at " << usemap_odd << dendl;

    init_usemap();
    return 0;
  }

  void close() {
    release_all();
    
    region_loc.clear();

    num_free = 0;
    num_dirty = 0;
    num_clean = 0;
    num_limbo = 0;
    dirty_ls.clear();

    free.clear();
    limbo.clear();

    flushing = 0;
    node_map.clear();
  }


  // *** blocking i/o routines ***

  int read_usemap_and_clean_nodes(BlockDevice& dev, version_t epoch) {
    // read map
    extent_t loc;
    if (epoch & 1) 
      loc = usemap_odd;
    else 
      loc = usemap_even;

    // usemap
    dev.read(loc.start, loc.length, usemap_data);
    
    // nodes
    unsigned region = 0;
    unsigned region_pos = 0;
    for (int i=0; i<num_nodes; i++) {
      nodeid_t nid = make_nodeid(region, region_pos);
      region_pos++;
      if (region_pos == region_loc[region].length) {
	region_pos = 0;
	region++;
      }
      
      if (usemap_bits[i]) {
	num_clean++;
        bufferptr bp = buffer::create_page_aligned(EBOFS_NODE_BYTES);
        dev.read((block_t)nid, EBOFS_NODE_BLOCKS, bp);
        
        Node *n = new Node(nid, i, bp, Node::STATE_CLEAN);
        node_map[nid] = n;
        debofs(10) << "ebofs.nodepool.read node " << nid << " at " << (void*)n << dendl;

      } else {
        //debofs(10) << "ebofs.nodepool.read  node " << nid << " is free" << dendl;
	free.insert(nid);
	num_free++;
      }
    }
    debofs(10) << "ebofs.nodepool.read free is " << free.m << dendl;
    assert(num_dirty == 0);
    assert(num_limbo == 0);
    assert(num_clean + num_free == num_nodes);

    return 0;
  }


  // **** non-blocking i/o ****

 private:
  class C_NP_FlushUsemap : public BlockDevice::callback {
    NodePool *pool;
  public:
    C_NP_FlushUsemap(NodePool *p) : 
      pool(p) {}
    void finish(ioh_t ioh, int r) {
      pool->flushed_usemap();
    }
  };
  
  void flushed_usemap() {
    ebofs_lock.Lock();
    flushing--;
    if (flushing == 0) 
      commit_cond.Signal();
    ebofs_lock.Unlock();
  }

 public:
  int write_usemap(BlockDevice& dev, version_t version) {
    // alloc
    extent_t loc;
    if (version & 1) 
      loc = usemap_odd;
    else 
      loc = usemap_even;
    
    // write
    bufferlist bl;
    bufferptr bp = usemap_data.clone();
    bl.append(bp);
    dev.write(loc.start, loc.length, bl,
              new C_NP_FlushUsemap(this), "usemap");
    return 0;
  }



  // *** node commit ***
 private:
 
  class C_NP_FlushNode : public BlockDevice::callback {
    NodePool *pool;
    nodeid_t nid;
  public:
    C_NP_FlushNode(NodePool *p, nodeid_t n) : 
      pool(p), nid(n) {}
    void finish(ioh_t ioh, int r) {
      pool->flushed_node(nid);
    }
  };

  void flushed_node(nodeid_t nid) {
    ebofs_lock.Lock();
    flushing--;
    if (flushing == 0) 
      commit_cond.Signal();
    ebofs_lock.Unlock();
  }

 public:
  void commit_start(BlockDevice& dev, version_t version) {
    debofs(20) << "ebofs.nodepool.commit_start start dirty=" << dirty_ls.size() << dendl;

    assert(flushing == 0);
    /*if (0)
      for (unsigned i=0; i<region_loc.size(); i++) {
        int c = dev.count_io(region_loc[i].start, region_loc[i].length);
        generic_dout(20) << "ebofs.nodepool.commit_start  region " << region_loc[i] << " has " << c << " ios" << dendl;
        assert(c == 0);
      }
    */

    // write map
    flushing++;
    write_usemap(dev, version & 1);

    // dirty -> clean  (write to disk)
    while (!dirty_ls.empty()) {
      Node *n = dirty_ls.front();
      assert(n);
      assert(n->is_dirty());
      n->set_state(Node::STATE_CLEAN);
      dirty_ls.remove(&n->xlist);
      num_dirty--;
      num_clean++;

      bufferlist bl;
      if (1) {
	bufferptr bp = n->get_buffer().clone();  // dup it now
	bl.append(bp);
      } else {
	bl.append(n->get_buffer());  // this isn't working right .. fixme
      }

      debofs(20) << "ebofs.nodepool.commit_start writing node " << n->get_id()
		 << " " << (void*)bl.c_str()
		 << dendl;
      
      dev.write(n->get_id(), EBOFS_NODE_BLOCKS, 
                bl,
                new C_NP_FlushNode(this, n->get_id()), "node");
      flushing++;
    }

    // limbo -> free
    for (map<nodeid_t,nodeid_t>::iterator i = limbo.m.begin();
         i != limbo.m.end();
         i++) {
      num_free += i->second;
      num_limbo -= i->second;
      free.insert(i->first, i->second);
      debofs(20) << "ebofs.nodepool.commit_finish " << i->first << "~" << i->second << " limbo->free" << dendl;
    }
    limbo.clear();

    debofs(20) << "ebofs.nodepool.commit_start finish" << dendl;
  }

  void commit_wait() {
    while (flushing > 0) 
      commit_cond.Wait(ebofs_lock);
    debofs(20) << "ebofs.nodepool.commit_wait finish" << dendl;
  }

  void commit_finish() {
  }


   


  // *** nodes ***
  // opened node
  Node* get_node(nodeid_t nid) {
    //dbtout << "pool.get " << nid << dendl;
    assert(node_map.count(nid));
    return node_map[nid];
  }
  
  // allocate id/block on disk.  always free -> dirty.
  nodeid_t alloc_id() {
    // pick node id
    assert(!free.empty());
    nodeid_t nid = free.start();
    free.erase(nid);
    num_free--;
    return nid;
  }
  
  // new node
  Node* new_node(int type) {
    nodeid_t nid = alloc_id();
    debofs(15) << "ebofs.nodepool.new_node " << nid << dendl;
    
    // alloc node
    bufferptr bp = buffer::create_page_aligned(EBOFS_NODE_BYTES);
    bp.zero();
    Node *n = new Node(nid, nodeid_pos_in_bitmap(nid), bp, Node::STATE_DIRTY);
    n->set_type(type);
    n->set_size(0);

    usemap_bits.set(n->get_pos_in_bitmap());

    n->set_state(Node::STATE_DIRTY);
    dirty_ls.push_back(&n->xlist);
    num_dirty++;

    assert(node_map.count(nid) == 0);
    node_map[nid] = n;

    return n;
  }

  void release(Node *n) {
    const nodeid_t nid = n->get_id();
    node_map.erase(nid);

    if (n->is_dirty()) {
      debofs(15) << "ebofs.nodepool.release on " << nid << " to free" << dendl;
      dirty_ls.remove(&n->xlist);
      num_dirty--;
      free.insert(nid);
      num_free++;
      usemap_bits.clear(n->get_pos_in_bitmap());
    } else if (n->is_clean()) {
      debofs(15) << "ebofs.nodepool.release on " << nid << " to limbo" << dendl;
      limbo.insert(nid);
      num_limbo++;
      num_clean--;
      usemap_bits.clear(n->get_pos_in_bitmap());
    } else {
      debofs(15) << "ebofs.nodepool.release on " << nid << " to nowhere?" << dendl;
    }

    delete n;
    assert(num_clean + num_dirty + num_limbo + num_free == num_nodes);
  }

  void release_all() {
    while (!node_map.empty()) {
      //hash_map<nodeid_t,Node*,rjhash<uint64_t> >::iterator i = node_map.begin();
      unordered_map<nodeid_t,Node*,rjhash<uint64_t> >::iterator i = node_map.begin();
      //map<nodeid_t,Node*>::iterator i = node_map.begin();
      debofs(2) << "ebofs.nodepool.release_all leftover " << i->first << " " << i->second << dendl;
      release( i->second );
    }
    assert(node_map.empty());
  }

  void dirty_node(Node *n) {
    // get new node id?
    nodeid_t oldid = n->get_id();
    nodeid_t newid = alloc_id();
    debofs(15) << "ebofs.nodepool.dirty_node on " << oldid << " now " << newid << dendl;

    // dup data?
    //  this only does a memcpy if there are multiple references.. 
    //  i.e. if we are still writing the old data
    if (n->do_cow()) {
      //assert(0); //i'm duping on write
      debofs(15) << "ebofs.nodepool.dirty_node did cow on " << oldid << " now " << newid << dendl;
      //cerr << "ebofs.nodepool.dirty_node did cow on " << oldid << " now " << newid << dendl;
    }

    // release old block
    assert(n->is_clean());
    debofs(15) << "ebofs.nodepool.dirty_node releasing old " << oldid << " to limbo" << dendl;
    num_clean--;
    limbo.insert(oldid);
    num_limbo++;
    usemap_bits.clear(n->get_pos_in_bitmap());

    // rename node
    node_map.erase(oldid);
    n->set_id(newid);
    n->set_pos_in_bitmap(nodeid_pos_in_bitmap(newid));
    node_map[newid] = n;

    // new block
    n->set_state(Node::STATE_DIRTY);
    dirty_ls.push_back(&n->xlist);
    debofs(15) << "ebofs.nodepool.dirty_node added to dirty list, len now " << dirty_ls.size() << dendl;
    num_dirty++;
    usemap_bits.set(n->get_pos_in_bitmap());

    assert(num_clean + num_dirty + num_limbo + num_free == num_nodes);
  }  
  
  
};
  
#endif
