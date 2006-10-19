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


#ifndef __EBOFS_NODES_H
#define __EBOFS_NODES_H

/** nodes, node regions **/

#include "types.h"
#include "BlockDevice.h"


/*

     disk     wire    memory                

     free             free    -> free             can alloc
     free             used    -> dirty            can modify

     free     used    used    -> tx
     free     used    free    -> limbo 

     used             used    -> clean
     used             free    -> limbo


        // meaningless
     used     free    free    -> free             can alloc
     used     free    used    __DNE__


*/

#undef debofs
#define debofs(x) if (x < g_conf.debug_ebofs) cout << "ebofs.nodepool."


class Node {
 public:
  // bit fields
  static const int STATE_CLEAN = 1;   
  static const int STATE_DIRTY = 2; 
  static const int STATE_TX = 3;

  static const int ITEM_LEN = EBOFS_NODE_BYTES - sizeof(int) - sizeof(int) - sizeof(int);

  static const int TYPE_INDEX = 1;
  static const int TYPE_LEAF = 2;

 protected:
  nodeid_t    id;
  int         state;     // use bit fields above!

  bufferptr   bptr;
  bufferptr   shadow_bptr;

  // in disk buffer
  int         *type;
  int         *nrecs;

 public:
  Node(nodeid_t i, bufferptr& b, int s) : id(i), state(s), bptr(b)  {
    nrecs = (int*)(bptr.c_str());
    type = (int*)(bptr.c_str() + sizeof(*nrecs));
  }

  
  // id
  nodeid_t get_id() const { return id; }
  void set_id(nodeid_t n) { id = n; }

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
  bool is_tx() { return state == STATE_TX; }
  bool is_clean() { return state == STATE_CLEAN; }

  void set_state(int s) { state = s; }

  void make_shadow() {
    assert(is_tx());
    
    shadow_bptr = bptr;
    
    // new buffer
    bptr = buffer::create_page_aligned(EBOFS_NODE_BYTES);
    nrecs = (int*)(bptr.c_str());
    type = (int*)(bptr.c_str() + sizeof(*nrecs));
    
    // copy contents!
    memcpy(bptr.c_str(), shadow_bptr.c_str(), EBOFS_NODE_BYTES);
  }

};





class NodePool {
 protected:
  map<nodeid_t, Node*>  node_map;      // open node map
  
 public:
  vector<Extent> region_loc;    // region locations
  Extent         usemap_even;
  Extent         usemap_odd;
  
 protected:
  // on-disk block states
  int num_nodes;
  set<nodeid_t> free;
  set<nodeid_t> dirty;
  set<nodeid_t> tx;
  set<nodeid_t> clean;       // aka used
  set<nodeid_t> limbo;
  
  Mutex        &ebofs_lock;
  Cond          commit_cond;
  int           flushing;

  static int make_nodeid(int region, int offset) {
    return (region << 24) | offset;
  }
  static int nodeid_region(nodeid_t nid) {
    return nid >> 24;
  }
  static int nodeid_offset(nodeid_t nid) {
    return nid & ((1 << 24) - 1);
  }


 public:
  NodePool(Mutex &el) : 
    num_nodes(0),
    ebofs_lock(el),
    flushing(0) {}
  ~NodePool() {
    // nodes
    release_all();
  }

  int num_free() { return free.size(); }
  int num_dirty() { return dirty.size(); }
  int num_limbo() { return limbo.size(); }
  int num_tx() { return tx.size(); }
  int num_clean() { return clean.size(); }
  int num_total() { return num_nodes; }
  int num_used() { return num_clean() + num_dirty() + num_tx(); }

  int get_usemap_len(int n=0) {
    if (n == 0) n = num_nodes;
    return ((n-1) / 8 / EBOFS_BLOCK_SIZE) + 1;
  }

  int num_regions() { return region_loc.size(); }

  // the caller had better adjust usemap locations...
  void add_region(Extent ex) {
    int region = region_loc.size();
    assert(ex.length <= (1 << 24));
    region_loc.push_back(ex);
    for (unsigned o = 0; o < ex.length; o++) {
      free.insert( make_nodeid(region, o) );
    }
    num_nodes += ex.length;
  }
  
  int init(struct ebofs_nodepool *np) {
    // regions
    assert(region_loc.empty());
    num_nodes = 0;
    for (int i=0; i<np->num_regions; i++) {
      debofs(3) << "init region " << i << " at " << np->region_loc[i] << endl;
      region_loc.push_back( np->region_loc[i] );
      num_nodes += np->region_loc[i].length;
    }

    // usemap
    usemap_even = np->node_usemap_even;
    usemap_odd = np->node_usemap_odd;
    debofs(3) << "init even map at " << usemap_even << endl;
    debofs(3) << "init  odd map at " << usemap_odd << endl;

    return 0;
  }

  void close() {
    release_all();
    
    region_loc.clear();
    free.clear();
    dirty.clear();
    tx.clear();
    clean.clear();
    limbo.clear();
    flushing = 0;
    node_map.clear();
  }


  // *** blocking i/o routines ***

  int read_usemap(BlockDevice& dev, version_t epoch) {
    // read map
    Extent loc;
    if (epoch & 1) 
      loc = usemap_odd;
    else 
      loc = usemap_even;

    bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE*loc.length);
    dev.read(loc.start, loc.length, bp);
    
    // parse
    unsigned region = 0;  // current region
    unsigned roff = 0;    // offset in region
    for (unsigned byte = 0; byte<bp.length(); byte++) {   // each byte
      // get byte
      int x = *(unsigned char*)(bp.c_str() + byte);
      int mask = 0x80;  // left-most bit
      for (unsigned bit=0; bit<8; bit++) {
        nodeid_t nid = make_nodeid(region, roff);
        
        if (x & mask)
          clean.insert(nid);
        else
          free.insert(nid);

        mask = mask >> 1;  // move one bit right.
        roff++;
        if (roff == region_loc[region].length) {
          // next region!
          roff = 0;
          region++;
          break;
        }
      }     
      if (region == region_loc.size()) break;
    }    
    return 0;
  }

  int read_clean_nodes(BlockDevice& dev) {
    /*
      this relies on the clean set begin defined so that we know which nodes
      to read.  so it only really works when called from mount()!
    */
    for (unsigned r=0; r<region_loc.size(); r++) {
      debofs(3) << "ebofs.nodepool.read region " << r << " at " << region_loc[r] << endl;
      
      for (block_t boff = 0; boff < region_loc[r].length; boff++) {
        nodeid_t nid = make_nodeid(r, boff);
        
        if (!clean.count(nid)) continue;  
        debofs(20) << "ebofs.nodepool.read  node " << nid << endl;

        bufferptr bp = buffer::create_page_aligned(EBOFS_NODE_BYTES);
        dev.read(region_loc[r].start + (block_t)boff, EBOFS_NODE_BLOCKS, 
                 bp);
        
        Node *n = new Node(nid, bp, Node::STATE_CLEAN);
        node_map[nid] = n;
        debofs(10) << "ebofs.nodepool.read  node " << n << " at " << (void*)n << endl;
      }
    }
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
    Extent loc;
    if (version & 1) 
      loc = usemap_odd;
    else 
      loc = usemap_even;
    
    bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE*loc.length);

    // fill in
    unsigned region = 0;  // current region
    unsigned roff = 0;    // offset in region
    for (unsigned byte = 0; byte<bp.length(); byte++) {   // each byte
      int x = 0;        // start with empty byte
      int mask = 0x80;  // left-most bit
      for (unsigned bit=0; bit<8; bit++) {
        nodeid_t nid = make_nodeid(region, roff);
        
        if (clean.count(nid) ||
            dirty.count(nid))
          x |= mask;

        roff++;
        mask = mask >> 1;
        if (roff == region_loc[region].length) {
          // next region!
          roff = 0;
          region++;
          break;
        }
      }

      *(unsigned char*)(bp.c_str() + byte) = x;
      if (region == region_loc.size()) break;
    }


    // write
    bufferlist bl;
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
    
    // mark nid clean|limbo
    if (tx.count(nid)) {  // tx -> clean
      tx.erase(nid);
      clean.insert(nid);

      // make node itself clean
      node_map[nid]->set_state(Node::STATE_CLEAN);
    }
    else {  // already limbo  (was dirtied, or released)
      assert(limbo.count(nid));
    }

    flushing--;
    if (flushing == 0) 
      commit_cond.Signal();
    ebofs_lock.Unlock();
  }

 public:
  void commit_start(BlockDevice& dev, version_t version) {
    dout(20) << "ebofs.nodepool.commit_start start" << endl;

    assert(flushing == 0);
    /*if (0)
      for (unsigned i=0; i<region_loc.size(); i++) {
        int c = dev.count_io(region_loc[i].start, region_loc[i].length);
        dout(20) << "ebofs.nodepool.commit_start  region " << region_loc[i] << " has " << c << " ios" << endl;
        assert(c == 0);
      }
    */

    // write map
    flushing++;
    write_usemap(dev,version & 1);

    // dirty -> tx  (write to disk)
    assert(tx.empty());
    set<block_t> didb;
    for (set<nodeid_t>::iterator i = dirty.begin();
         i != dirty.end();
         i++) {
      Node *n = get_node(*i);
      assert(n);
      assert(n->is_dirty());
      n->set_state(Node::STATE_TX);

      unsigned region = nodeid_region(*i);
      block_t off = nodeid_offset(*i);
      block_t b = region_loc[region].start + off;

      if (1) {  // sanity check debug FIXME
        assert(didb.count(b) == 0);
        didb.insert(b);
      }

      bufferlist bl;
      bl.append(n->get_buffer());
      dev.write(b, EBOFS_NODE_BLOCKS, 
                bl,
                new C_NP_FlushNode(this, *i), "node");
      flushing++;

      tx.insert(*i);
    }
    dirty.clear();

    // limbo -> free
    for (set<nodeid_t>::iterator i = limbo.begin();
         i != limbo.end();
         i++) {
      free.insert(*i);
    }
    limbo.clear();

    dout(20) << "ebofs.nodepool.commit_start finish" << endl;
  }

  void commit_wait() {
    while (flushing > 0) 
      commit_cond.Wait(ebofs_lock);
    dout(20) << "ebofs.nodepool.commit_wait finish" << endl;
  }






   


  // *** nodes ***
  // opened node
  Node* get_node(nodeid_t nid) {
    //dbtout << "pool.get " << nid << endl;
    assert(node_map.count(nid));
    return node_map[nid];
  }
  
  // unopened node
  /*  not implemented yet!!
  Node* open_node(nodeid_t nid) {
    Node *n = node_regions[ NodeRegion::nodeid_region(nid) ]->open_node(nid);
    dbtout << "pool.open_node " << n->get_id() << endl;
    node_map[n->get_id()] = n;
    return n;
  }
  */
  
  // allocate id/block on disk.  always free -> dirty.
  nodeid_t alloc_id() {
    // pick node id
    assert(!free.empty());
    nodeid_t nid = *(free.begin());
    free.erase(nid);
    dirty.insert(nid);
    return nid;
  }
  
  // new node
  Node* new_node(int type) {
    nodeid_t nid = alloc_id();
    debofs(15) << "ebofs.nodepool.new_node " << nid << endl;
    
    // alloc node
    bufferptr bp = buffer::create_page_aligned(EBOFS_NODE_BYTES);
    Node *n = new Node(nid, bp, Node::STATE_DIRTY);
    n->set_type(type);
    n->set_size(0);

    assert(node_map.count(nid) == 0);
    node_map[nid] = n;
    return n;
  }

  void release(Node *n) {
    const nodeid_t nid = n->get_id();
    debofs(15) << "ebofs.nodepool.release on " << nid << endl;
    node_map.erase(nid);

    if (n->is_dirty()) {
      assert(dirty.count(nid));
      dirty.erase(nid);
      free.insert(nid);
    } else if (n->is_clean()) {
      assert(clean.count(nid));
      clean.erase(nid);
      limbo.insert(nid);
    } else if (n->is_tx()) {
      assert(tx.count(nid));      // i guess htis happens? -sage
      tx.erase(nid);
      limbo.insert(nid);
    }

    delete n;
  }

  void release_all() {
    while (!node_map.empty()) {
      map<nodeid_t,Node*>::iterator i = node_map.begin();
      debofs(2) << "ebofs.nodepool.release_all leftover " << i->first << " " << i->second << endl;
      release( i->second );
    }
    assert(node_map.empty());
  }

  void dirty_node(Node *n) {
    // get new node id?
    nodeid_t oldid = n->get_id();
    nodeid_t newid = alloc_id();
    debofs(15) << "ebofs.nodepool.dirty_node on " << oldid << " now " << newid << endl;
    
    // release old block
    if (n->is_clean()) {
      assert(clean.count(oldid));
      clean.erase(oldid);
    } else {
      assert(n->is_tx());
      assert(tx.count(oldid));
      tx.erase(oldid);
      
      // move/copy current -> shadow buffer as necessary
      n->make_shadow();   
    }
    limbo.insert(oldid);
    node_map.erase(oldid);
    
    n->set_state(Node::STATE_DIRTY);
    
    // move to new one!
    n->set_id(newid);
    node_map[newid] = n;
  }
  
  
  
};
  
#endif
