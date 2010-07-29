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


#ifndef CEPH_EBOFS_TABLE_H
#define CEPH_EBOFS_TABLE_H

#include "types.h"
#include "nodes.h"

/** table **/

#define dbtout do { if (25 <= g_conf.debug_ebofs) { *_dout << dbeginl << "ebofs.table(" << this << ")."


template<class K, class V>
class Table {
 private:
  NodePool &pool;
  
  ebofs_node_ptr root;
  int      nkeys;
  int      depth;

 public:
  Table(NodePool &p,
        struct ebofs_table& bts) : 
    pool(p),
    root(bts.root), nkeys(bts.num_keys), depth(bts.depth) {
    dbtout << "cons" << dendl;
  }
  
  const ebofs_node_ptr &get_root() { return root; }
  int get_num_keys() { return nkeys; }
  int get_depth() { return depth; }


  /*
   */
  class _IndexItem {     // i just need a struct size for below
    K k;
    nodeid_t n;
  };
  class IndexItem {
  public:
    K        key;
    nodeid_t node;
    static const int MAX = Node::ITEM_LEN / (sizeof(_IndexItem));
    static const int MIN = MAX/2;
  };
  class _LeafItem {     // i just need a struct size for below
    K k;
    V v;
  };
  class LeafItem {
  public:
    K key;
    V value;
    static const int MAX = Node::ITEM_LEN / (sizeof(_LeafItem));
    static const int MIN = MAX/2;
  };
  
  class Nodeptr {
  public:    
    Node      *node;

    Nodeptr() : node(0) {}
    Nodeptr(Node *n) : node(n) {}
    Nodeptr(NodePool& pool, nodeid_t nid) {
      open(pool, nid);
    }
    Nodeptr& operator=(Node *n) {
      node = n;
      return *this;
    }

    void open(NodePool& pool, nodeid_t nid) {
      node = pool.get_node(nid);
      if (is_index() && node->children.empty()) init_index(pool);
    }
    
    LeafItem&  leaf_item(int i)  { return (( LeafItem*)(node->item_ptr()))[i]; }
    IndexItem& index_item(int i) { return ((IndexItem*)(node->item_ptr()))[i]; }
    K key(int i) {
      if (node->is_index()) 
        return index_item(i).key;
      else
        return leaf_item(i).key;
    }

    bool is_leaf() { return node->is_leaf(); }
    bool is_index() { return node->is_index(); }
    void set_type(int t) { node->set_type(t); }

    int max_items() const {
      if (node->is_leaf()) 
        return LeafItem::MAX;
      else
        return IndexItem::MAX;
    }
    int min_items() const { return max_items() / 2; }
    
    nodeid_t get_id() { return node->get_id(); }

    int size() { return node->size(); }
    void set_size(int s) { node->set_size(s); }

    void init_index(NodePool& nodepool) {
      /*
      node->children = vector<Node*>(max_items());
      for (int i=0; i<max_items(); i++)
	if (i < size())
	  node->children[i] = nodepool.get_node(index_item(i).node);
	else
	  node->children[i] = 0;
      */
    }
    

    void remove_at_pos(int p) {
      if (node->is_index()) {
        for (int i=p; i<size()-1; i++) {
          index_item(i) = index_item(i+1);
	  //node->children[i] = node->children[i+1];
	}
      } else {
        for (int i=p; i<size()-1; i++)
          leaf_item(i) = leaf_item(i+1);
      }
      set_size(size() - 1);
      dbtout << "remove_at_pos done, size now " << size() << " " << (node->is_index() ? "index":"leaf") << dendl;
    }
    void insert_at_leaf_pos(int p, K key, V value) {
      assert(is_leaf());
      for (int i=size(); i>p; i--)
        leaf_item(i) = leaf_item(i-1);
      leaf_item(p).key = key;
      leaf_item(p).value = value;
      set_size(size() + 1);
    }
    void insert_at_index_pos(int p, K key, nodeid_t nid) {
      assert(is_index());
      for (int i=size(); i>p; i--) {
        index_item(i) = index_item(i-1);
	//node->children[i] = node->children[i-1];
      }
      index_item(p).key = key;
      index_item(p).node = nid;
      set_size(size() + 1);
    }

    void append_item(LeafItem& i) {
      leaf_item(size()) = i;
      set_size(size() + 1);
    }
    void append_item(IndexItem& i) {
      index_item(size()) = i;
      set_size(size() + 1);
    }

    void split(Nodeptr& right) {
      if (node->is_index()) {
        for (int i=min_items(); i<size(); i++)
          right.append_item( index_item(i) );
      } else {
        for (int i=min_items(); i<size(); i++)
          right.append_item( leaf_item(i) );
      }
      set_size(min_items());
    }

    void merge(Nodeptr& right) {
      if (node->is_index()) 
        for (int i=0; i<right.size(); i++)
          append_item( right.index_item(i) );
      else 
        for (int i=0; i<right.size(); i++)
          append_item( right.leaf_item(i) );
      right.set_size(0);
    }

  };

  /*
   */
  class Cursor {
  protected:
  public:
    static const int MATCH = 1;   // on key
    static const int INSERT = 0;  // before key
    static const int OOB = -1;    // at end

    Table              *table;
    vector<Nodeptr>     open;  // open nodes
    vector<int>         pos;   // position within the node
    //Nodeptr             open[20];
    //int                 pos[20];
    int                 level;

    Cursor(Table *t) : table(t), open(t->depth), pos(t->depth), level(0) {}

  public:

    const LeafItem& current() {
      assert(open[level].is_leaf());
      return open[level].leaf_item(pos[level]);
    }
    V& dirty_current_value() {
      assert(open[level].is_leaf());
      dirty();
      return open[level].leaf_item(pos[level]).value;
    }

    // ** read-only bits **
    int move_left() {
      if (table->depth == 0) return OOB; 

      // work up around branch
      int l;
      for (l = level; l >= 0; l--) 
        if (pos[l] > 0) break;
      if (l < 0)
        return OOB;   // we are the first item in the btree

      // move left one
      pos[l]--;
      
      // work back down right side
      for (; l<level; l++) {
        open[l+1].open(table->pool, open[l].index_item(pos[l]).node);
        pos[l+1] = open[l+1].size() - 1;
      }
      return 1;
    }
    int move_right() {
      if (table->depth == 0) return OOB; 

      // work up branch
      int l;
      for (l=level; l>=0; l--) 
        if (pos[l] < open[l].size() - 1) break;
      if (l < 0) {
        /* we are at last item in btree. */
        if (pos[level] < open[level].size()) {
          pos[level]++;  /* move into add position! */
          return 0;
        }
        return -1;  
      }
      
      /* move right one */
      assert( pos[l] < open[l].size() );  
      pos[l]++;
      
      /* work back down */
      for (; l<level; l++) {
        open[l+1].open(table->pool, open[l].index_item(pos[l]).node );
        pos[l+1] = 0;  // furthest left
      }
      return 1;
    }

    // ** modifications **
    void dirty() {
      for (int l=level; l>=0; l--) {
        if (open[l].node->is_dirty()) {
	  dbtout << "dirty " << open[l].node->get_id() << " already dirty (thus parents are too)" << dendl;
	  break;  // already dirty!  (and thus parents are too)
	}
        
        table->pool.dirty_node(open[l].node);
        if (l > 0)
          open[l-1].index_item( pos[l-1] ).node = open[l].get_id();
        else
          table->root.nodeid = open[0].get_id();
      }
    }
  private:
    void repair_parents() {
      // did i make a change at the start of a node?
      if (pos[level] == 0) {
        K key = open[level].key(0);  // new key parents should have
        for (int j=level-1; j>=0; j--) {
          if (open[j].index_item(pos[j]).key == key)
            break;  /* it's the same key, we can stop fixing */
          open[j].index_item(pos[j]).key = key;
          if (pos[j] > 0) break;  /* last in position 0.. */
        }
      }
    }

  public:
    void remove() {
      dirty();

      // remove from node
      open[level].remove_at_pos( pos[level] );
      repair_parents();
      
      // was it a key?
      if (level == table->depth-1) 
        table->nkeys--;
    }

    void insert(K key, V value) {
      dirty();
      
      // insert
      open[level].insert_at_leaf_pos(pos[level], key, value);
      repair_parents();
      
      // was it a key?
      if (level == table->depth-1)
        table->nkeys++;
    }

    int rotate_left() {
      if (level == 0) return -1;         // i am root
      if (pos[level-1] == 0) return -1;  // nothing to left
      
      Nodeptr here = open[level];
      Nodeptr parent = open[level-1];
      Nodeptr left(table->pool, parent.index_item(pos[level-1] - 1).node );
      if (left.size() == left.max_items()) return -1;  // it's full

      // make both dirty
      dirty();
      if (!left.node->is_dirty()) {
        table->pool.dirty_node(left.node);
        parent.index_item(pos[level-1]-1).node = left.get_id();
      }
      
      dbtout << "rotating item " << here.key(0) << " left from " << here.get_id() << " to " << left.get_id() << dendl;
      
      /* add */
      if (here.node->is_leaf())
        left.append_item(here.leaf_item(0));
      else
        left.append_item(here.index_item(0));

      /* remove */
      here.remove_at_pos(0);

      /* fix parent index for me */
      parent.index_item( pos[level-1] ).key = here.key(0);
      // we never have to update past immediate parent, since we're not at pos 0
      
      /* adjust cursor */
      if (pos[level] > 0) 
        pos[level]--;  
      //else
      //assert(1); /* if we were positioned here, we're equal */
      /* if it was 0, then the shifted item == our key, and we can stay here safely. */
      return 0;
    }
    int rotate_right() {
      if (level == 0) return -1;         // i am root
      if (pos[level-1] + 1 >= open[level-1].size()) return -1;  // nothing to right
      
      Nodeptr here = open[level];
      Nodeptr parent = open[level-1];
      Nodeptr right(table->pool, parent.index_item( pos[level-1] + 1 ).node );
      if (right.size() == right.max_items()) return -1;  // it's full
      
      // make both dirty
      dirty();
      if (!right.node->is_dirty()) {
        table->pool.dirty_node(right.node);
        parent.index_item( pos[level-1]+1 ).node = right.get_id();
      }
      
      if (pos[level] == here.size()) {
        /* let's just move the cursor over! */
        //if (sizeof(K) == 8)
          dbtout << "shifting cursor right from " << here.get_id() << " to less-full node " << right.get_id() << dendl;
        open[level] = right;
        pos[level] = 0;
        pos[level-1]++;
        return 0;
      }

      //if (sizeof(K) == 8)
      dbtout << "rotating item " << hex << here.key(here.size()-1) << dec << " right from "
             << here.get_id() << " to " << right.get_id() << dendl;
      
      /* add */
      if (here.is_index())
        right.insert_at_index_pos(0, 
                                  here.index_item( here.size()-1 ).key,
                                  here.index_item( here.size()-1 ).node);
      else
        right.insert_at_leaf_pos(0, 
                                 here.leaf_item( here.size()-1 ).key,
                                 here.leaf_item( here.size()-1 ).value);
      
      /* remove */
      here.set_size(here.size() - 1);

      /* fix parent index for right */
      parent.index_item( pos[level-1] + 1 ).key = right.key(0);
      
      return 0;
    }
  };


 public:
  bool almost_full() {
    if (2*(depth+1) > pool.get_num_free())     // worst case, plus some.
      return true;
    return false;
  }
  
  int find(K key, Cursor& cursor) {
    dbtout << "find " << key << " depth " << depth << dendl;
    verify("find");

    if (depth == 0)
      return Cursor::OOB;

    // init
    cursor.level = 0;
    
    // start at root
    Nodeptr curnode(pool, root.nodeid);
    cursor.open[0] = curnode;

    if (curnode.size() == 0) return -1;  // empty!

    // find leaf
    for (cursor.level = 0; cursor.level < depth-1; cursor.level++) {
      /* if key=5, we want 2 3 [4] 6 7, or 3 4 [5] 5 6  (err to the left) */
      int left = 0;                        /* i >= left */
      int right = curnode.size()-1;        /* i < right */
      while (left < right) {
        int i = left + (right - left) / 2;
        if (curnode.index_item(i).key < key) {
          left = i + 1;
        } else if (i && curnode.index_item(i-1).key >= key) {
          right = i;
        } else {
          left = right = i;
          break;
        }
      }
      int i = left;
      if (i && curnode.index_item(i).key > key) i--;
      
#ifdef EBOFS_DEBUG_BTREE
      int j;
      for (j=0; j<curnode.size()-1; j++) { 
        if (curnode.index_item(j).key == key) break;  /* perfect */
        if (curnode.index_item(j+1).key > key) break;
      }
      if (i != j) {
        dbtout << "btree binary search failed" << dendl;
        i = j;
      }
#endif

      cursor.pos[cursor.level] = i;   
      dbtout << "find index level " << cursor.level << " node " << curnode.get_id() << " pos " << i
	     << " key " << cursor.open[cursor.level].index_item(i).key
	     << " value " << cursor.open[cursor.level].index_item(i).node << dendl;
      /* get child node */
      curnode.open(pool, cursor.open[cursor.level].index_item(i).node );
      cursor.open[cursor.level+1] = curnode;
    }

    /* search leaf */
    dbtout << "find leaf " << curnode.get_id() << " size " << curnode.size() << dendl;

    /*  if key=5, we want 2 3 4 [6] 7, or 3 4 [5] 5 6   (err to the right) */
    int left = 0;                      /* i >= left */
    int right = curnode.size();        /* i < right */
    while (left < right) {
      int i = left + (right - left) / 2;
      if (curnode.leaf_item(i).key < key) {
        left = i + 1;
      } else if (i && curnode.leaf_item(i-1).key >= key) {
        right = i;
      } else {
        left = right = i;
        break;
      }
    }
    int i = left;

    
#ifdef EBOFS_DEBUG_BTREE
    int j;
    for (j=0; j<curnode.size(); j++) {
      if (curnode.leaf_item(j).key >= key) break; 
    }
    if (i != j) {
      dbtout << "btree binary search failed" << dendl;
      i = j;
    }
#endif
    
    cursor.pos[cursor.level] = i;   /* first key in this node, or key insertion point */

    if (curnode.size() >= i+1) {
      if (curnode.leaf_item(i).key == key) {
	dbtout << "find pos " << i << " match " << curnode.leaf_item(i).key << dendl;
	return Cursor::MATCH;   /* it's the actual key */
      } else {
	dbtout << "find pos " << i << " insert " << curnode.leaf_item(i).key << dendl;
        return Cursor::INSERT;   /* it's an insertion point */
      }
    }
    dbtout << "find pos " << i << " OOB (end of btree)" << dendl;
    return Cursor::OOB;  /* it's the end of the btree (also a valid insertion point) */
  }

  int lookup(K key) {
    dbtout << "lookup" << dendl;
    Cursor cursor(this);
    if (find(key, cursor) == Cursor::MATCH) 
      return 0;
    return -1;
  }

  int lookup(K key, V& value) {
    dbtout << "lookup" << dendl;
    Cursor cursor(this);
    if (find(key, cursor) == Cursor::MATCH) {
      value = cursor.current().value;
      return 0;
    }
    return -1;
  }

  int insert(K key, V value) {
    verify("pre-insert"); 
    dbtout << "insert " << key << " -> " << value << dendl;
    if (almost_full()) return -1;
    
    // empty?
    if (nkeys == 0) {
      if (root.nodeid == -1) {
	// create a root node (leaf!)
	assert(depth == 0);
	Nodeptr newroot( pool.new_node(Node::TYPE_LEAF) );
	root.nodeid = newroot.get_id();
	depth++;
      }
      assert(depth == 1);
      assert(root.nodeid >= 0);
    }

    // start at/near key
    Cursor cursor(this);
    find(key, cursor);
    
    // insert loop
    nodeid_t nodevalue = 0;
    while (1) {
      
      /* room in this node? */
      if (cursor.open[cursor.level].size() < cursor.open[cursor.level].max_items()) {
        if (cursor.open[cursor.level].is_leaf())
          cursor.insert( key, value );   // will dirty, etc.
        else {
          // indices are already dirty
          cursor.open[cursor.level].insert_at_index_pos(cursor.pos[cursor.level], key, nodevalue);
        }
        verify("insert 1");
        return 0;
      }
      
      /* this node is full. */
      assert( cursor.open[cursor.level].size() == cursor.open[cursor.level].max_items() );

      /* can we rotate? */
      if (false)      // NO! there's a bug in here somewhere, don't to it.
      if (cursor.level > 0) {
        if ((cursor.pos[cursor.level-1] > 0 
             && cursor.rotate_left() >= 0) ||
            (cursor.pos[cursor.level-1] + 1 < cursor.open[cursor.level-1].size()
             && cursor.rotate_right() >= 0)) {
          
          if (cursor.open[cursor.level].is_leaf())
            cursor.insert( key, value );   // will dirty, etc.
          else {
            // indices are already dirty
            cursor.open[cursor.level].insert_at_index_pos(cursor.pos[cursor.level], key, nodevalue);
          }
          verify("insert 2");
          return 0;
        }
      }

      /** split node **/

      if (cursor.level == depth-1) {
        dbtout << "splitting leaf " << cursor.open[cursor.level].get_id() << dendl;
      } else {
        dbtout << "splitting index " << cursor.open[cursor.level].get_id() << dendl;
      }
      
      cursor.dirty();
      
      // split
      Nodeptr leftnode = cursor.open[cursor.level];
      Nodeptr newnode( pool.new_node(leftnode.node->get_type()) );
      leftnode.split( newnode );

      /* insert our item */
      if (cursor.pos[cursor.level] > leftnode.size()) {
        // not with cursor, since this node isn't added yet!
        if (newnode.is_leaf()) {
          newnode.insert_at_leaf_pos( cursor.pos[cursor.level] - leftnode.size(),
                                      key, value );
          nkeys++;
        } else {
          newnode.insert_at_index_pos( cursor.pos[cursor.level] - leftnode.size(),
                                       key, nodevalue );
        }
      } else {
        // with cursor (if leaf)
        if (leftnode.is_leaf())
          cursor.insert( key, value );
        else 
          leftnode.insert_at_index_pos( cursor.pos[cursor.level],
                                        key, nodevalue );
      }

      /* are we at the root? */
      if (cursor.level == 0) {
        /* split root. */
        dbtout << "that split was the root " << root.nodeid << dendl;
        Nodeptr newroot( pool.new_node(Node::TYPE_INDEX) );
        
        /* new root node */
        newroot.set_size(2);
        newroot.index_item(0).key = leftnode.key(0);
        newroot.index_item(0).node = root.nodeid;
        newroot.index_item(1).key = newnode.key(0);
        newroot.index_item(1).node = newnode.get_id();
        
        /* heighten tree */
        depth++;
        root.nodeid = newroot.get_id();
        verify("insert 3");
        return 0;
      }

      /* now insert newindex in level-1 */
      nodevalue = newnode.get_id();
      key = newnode.key(0);
      cursor.level--;
      cursor.pos[cursor.level]++;   // ...to the right of leftnode!
    }
  }


  int remove(K key) {
    verify("pre-remove"); 
    dbtout << "remove " << key << dendl;

    if (almost_full()) {
      cout << "table almost full, failing" << std::endl;
      assert(0);
      return -1;
    }
    
    Cursor cursor(this);
    if (find(key, cursor) <= 0) {
      cerr << "remove " << key << " 0x" << hex << key << dec << " .. dne" << std::endl;
      g_conf.debug_ebofs = 33;
      g_conf.ebofs_verify = true;
      verify("remove dne"); 
      assert(0);
      return -1;  // key dne
    }


    while (1) {
      dbtout << "preremove level " << cursor.level << " size " << cursor.open[cursor.level].size()
	     << " ? min " << cursor.open[cursor.level].min_items() << dendl;
      cursor.remove();
      verify("post-remove"); 
      dbtout << "postremove level " << cursor.level << " size " << cursor.open[cursor.level].size()
	     << " ? min " << cursor.open[cursor.level].min_items() << dendl;

      // balance + adjust
      
      if (cursor.level == 0) {
        // useless root index?
        if (cursor.open[0].size() == 1 &&
            depth > 1) {
          depth--;
          root.nodeid = cursor.open[0].index_item(0).node;
          pool.release( cursor.open[0].node );
        }

        // note: root can be small, but not empty
        else if (nkeys == 0) {
          assert(cursor.open[cursor.level].size() == 0);
          assert(depth == 1);
          root.nodeid = -1;
	  depth = 0;
	  if (cursor.open[0].node)
	    pool.release(cursor.open[0].node);
        }
        verify("remove 1");
        return 0;
      }
      
      if (cursor.open[cursor.level].size() > cursor.open[cursor.level].min_items()) {
        verify("remove 2");
	dbtout << "remove size " << cursor.open[cursor.level].size()
	       << " > min " << cursor.open[cursor.level].min_items() << dendl;
        return 0;
      }
      
      // borrow from siblings?
      Nodeptr left;
      Nodeptr right;

      // left?
      if (cursor.pos[cursor.level-1] > 0) {
        int left_loc = cursor.open[cursor.level-1].index_item( cursor.pos[cursor.level-1] - 1).node;
        left.open(pool, left_loc);

        if (left.size() > left.min_items()) {
          /* move cursor left, shift right */
          cursor.pos[cursor.level] = 0;
          cursor.open[cursor.level] = left;
          cursor.pos[cursor.level-1]--;
          cursor.rotate_right();
          verify("remove 3");
          return 0;
        }
        
        /* combine to left */
        right = cursor.open[cursor.level];
      }
      else {
        assert(cursor.pos[cursor.level-1] < cursor.open[cursor.level-1].size() - 1);
        int right_loc = cursor.open[cursor.level-1].index_item( cursor.pos[cursor.level-1] + 1 ).node;
        right.open(pool, right_loc );
        
        if (right.size() > right.min_items()) {
          /* move cursor right, shift an item left */
          cursor.pos[cursor.level] = 1;
          cursor.open[cursor.level] = right;
          cursor.pos[cursor.level-1]++;
          cursor.rotate_left();
          verify("remove 4");
          return 0;
        }
        
        /* combine to left */
        left = cursor.open[cursor.level];
        cursor.pos[cursor.level-1]++;  /* move cursor to (soon-to-be-empty) right side item */
      }

      // note: cursor now points to _right_ node.
      
      /* combine (towards left) 
       * (this makes it so our next delete will be in the index 
       * interior, which is less scary.)
       */
      dbtout << "combining nodes " << left.get_id() << " and " << right.get_id() << dendl;

      left.merge(right);
      
      // dirty left + right
      cursor.dirty();            // right
      if (!left.node->is_dirty()) {
        pool.dirty_node(left.node);
        cursor.open[cursor.level-1].index_item( cursor.pos[cursor.level-1]-1 ).node = left.get_id();
      }

      pool.release(right.node);
      
      cursor.level--;  // now point to the link to the obsolete (right-side) sib */
    }

  }

  void clear(Cursor& cursor, int node_loc, int level) {
    dbtout << "clear" << dendl;

    Nodeptr node(pool, node_loc);
    cursor.open[level] = node;
    
    // hose children?
    if (level < depth-1) {   
      for (int i=0; i<node.size(); i++) {
        // index
        cursor.pos[level] = i;
        nodeid_t child = cursor.open[level].index_item(i).node;
        clear( cursor, child, level+1 );
      }      
    }

    // hose myself
    pool.release( node.node );
  }
  
  void clear() {
    Cursor cursor(this);
    if (root.nodeid == -1 && depth == 0) return;   // already empty!
    clear(cursor, root.nodeid, 0);
    root.nodeid = -1;
    depth = 0;
    nkeys = 0;
  }

  int verify_sub(Cursor& cursor, int node_loc, int level, int& count, K& last, const char *on) {
    int err = 0;

    Nodeptr node(pool, node_loc);
    cursor.open[level] = node;
    
    // identify max, min, and validate key range
    K min = node.key(0);
    last = min;
    K max = min;

    // print it
    char s[1000];
    strcpy(s,"           ");
    s[level+1] = 0;
    if (1) {
      if (root.nodeid == node_loc) {
        dbtout << s << "root " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      } else if (level == depth-1) {
        dbtout << s << "leaf " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      } else {
        dbtout << s << "indx " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      }
      if (1) {
        for (int i=0; i<node.size(); i++) {
          if (level < depth-1) {          // index
            dbtout << s << "   " << hex << node.key(i) << " [" << node.index_item(i).node << "]" << dec << dendl;
          } else {          // leaf
            dbtout << s << "   " << hex << node.key(i) << " -> " << node.leaf_item(i).value << dec << dendl;
          }
        }
      }
    }

    for (int i=0; i<node.size(); i++) {
      if (i && node.key(i) <= last) {
        dbtout << ":: key " << i << " " << hex << node.key(i) << dec << " in node " << node_loc 
               << " is out of order, last is " << hex << last << dec << dendl;
        err++;
      }
      if (node.key(i) > max)
        max = node.key(i);
      
      if (level < depth-1) {   
        // index
        cursor.pos[level] = i;
        err += verify_sub( cursor, cursor.open[level].index_item(i).node, level+1, count, last, on );
      } else {
        // leaf
        count++;
        last = node.key(i);
      }
    }
    
    if (level) {
      // verify that parent's keys are appropriate
      if (min != cursor.open[level-1].index_item(cursor.pos[level-1]).key) {
        dbtout << ":: key in index node " << cursor.open[level-1].get_id()
               << " != min in child " << node_loc 
               << "(key is " << hex << cursor.open[level-1].index_item(cursor.pos[level-1]).key
               << ", min is " << min << ")" << dec << dendl;
        err++;
      }
      if (cursor.pos[level-1] < cursor.open[level-1].size()-1) {
        if (max > cursor.open[level-1].index_item(1+cursor.pos[level-1]).key) {
          dbtout << ":: next key in index node " << cursor.open[level-1].get_id()
                 << " < max in child " << node_loc 
                 << "(key is " << hex << cursor.open[level-1].index_item(1+cursor.pos[level-1]).key
                 << ", max is " << max << ")" << dec << dendl;
          err++;
        }
      }
    }
    
    if (err == 0) return err;
    
    // print it
    /*
    char s[1000];
    strcpy(s,"           ");
    s[level+1] = 0;
    if (1) {
      if (root.nodeid == node_loc) {
        dbtout << s << "root " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      } else if (level == depth-1) {
        dbtout << s << "leaf " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      } else {
        dbtout << s << "indx " << node_loc << ": "
               << node.size() << " / " << node.max_items() << " keys, " << hex << min << "-" << max << dec << dendl;
      }

      if (1) {
        for (int i=0; i<node.size(); i++) {
          if (level < depth-1) {          // index
            dbtout << s << "   " << hex << node.key(i) << " [" << node.index_item(i).node << "]" << dec << dendl;
          } else {          // leaf
            dbtout << s << "   " << hex << node.key(i) << " -> " << node.leaf_item(i).value << dec << dendl;
          }
        }
      }
      }*/
    
    return err;
  }

  void verify(const char *on) {
    if (!g_conf.ebofs_verify) 
      return;

    if (root.nodeid == -1 && depth == 0) {
      return;   // empty!
    }

    int count = 0;
    Cursor cursor(this);
    K last;
    
    int before = g_conf.debug_ebofs;
    g_conf.debug_ebofs = 0;

    int err = verify_sub(cursor, root.nodeid, 0, count, last, on);
    if (count != nkeys) {
      cerr << "** count " << count << " != nkeys " << nkeys << std::endl;
      err++;
    }

    g_conf.debug_ebofs = before;

    // ok?
    if (err) {
      cerr << "verify failure, called by '" << on << "'" << std::endl;
      g_conf.debug_ebofs = 30;
      // do it again, so we definitely get the dump.
      int count = 0;
      Cursor cursor(this);
      K last;
      verify_sub(cursor, root.nodeid, 0, count, last, on);
      assert(err == 0);
    }
  }

};


#endif
