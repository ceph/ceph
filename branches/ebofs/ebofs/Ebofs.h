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


#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/Context.h"
#include "include/buffer.h"
#include "include/hash.h"

#include "types.h"
#include "Onode.h"
#include "Cnode.h"
#include "BlockDevice.h"
#include "nodes.h"
#include "Allocator.h"
#include "Table.h"
#include "Journal.h"

#include "common/Mutex.h"
#include "common/Cond.h"

#include "osd/ObjectStore.h"

//typedef pair<object_t,coll_t> object_coll_t;
typedef pair<coll_t,pobject_t> coll_pobject_t;


class Ebofs : public ObjectStore {
protected:
  Mutex        ebofs_lock;    // a beautiful global lock

  // ** debuggy **
  bool         fake_writes;

  // ** super **
public:
  BlockDevice  dev;
protected:
  bool         mounted, unmounting, dirty;
  bool         readonly;
  version_t    super_epoch;
  bool         commit_starting;
  bool         commit_thread_started;
  Cond         commit_cond;   // to wake up the commit thread
  Cond         sync_cond;
  uint64_t     super_fsid;

  map<version_t, list<Context*> > commit_waiters;

  void prepare_super(version_t epoch, bufferptr& bp);
  void write_super(version_t epoch, bufferptr& bp);
  int commit_thread_entry();

  class CommitThread : public Thread {
    Ebofs *ebofs;
  public:
    CommitThread(Ebofs *e) : ebofs(e) {}
    void *entry() {
      ebofs->commit_thread_entry();
      return 0;
    }
  } commit_thread;

public:
  uint64_t get_fsid() { return super_fsid; }
  epoch_t get_super_epoch() { return super_epoch; }
protected:


  // ** journal **
  char *journalfn;
  Journal *journal;

  // ** allocator **
  block_t      free_blocks, limbo_blocks;
  Allocator    allocator;
  friend class Allocator;
  
  block_t get_free_blocks() { return free_blocks; }
  block_t get_limbo_blocks() { return limbo_blocks; }
  block_t get_free_extents() { 
    int n = 0;
    for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++) 
      n += free_tab[i]->get_num_keys();
    return n;
  }
  block_t get_limbo_extents() { return limbo_tab->get_num_keys(); }


  // ** tables and sets **
  // nodes
  NodePool     nodepool;   // for all tables...

  // tables
  Table<pobject_t, ebofs_inode_ptr> *object_tab;
  Table<block_t,block_t>  *free_tab[EBOFS_NUM_FREE_BUCKETS];
  Table<block_t,block_t>  *limbo_tab;
  Table<block_t,pair<block_t,int> > *alloc_tab;

  // collections
  Table<coll_t, ebofs_inode_ptr>  *collection_tab;
  Table<coll_pobject_t, bool>  *co_tab;

  void close_tables();
  void verify_tables();


  // ** onodes **
  hash_map<pobject_t, Onode*>  onode_map;  // onode cache
  LRU                         onode_lru;
  set<Onode*>                 dirty_onodes;
  map<pobject_t, list<Cond*> > waitfor_onode;

  Onode* new_onode(pobject_t oid);     // make new onode.  ref++.
  bool have_onode(pobject_t oid) {
    return onode_map.count(oid);
  }
  Onode* decode_onode(bufferlist& bl, unsigned& off, csum_t csum);
  csum_t encode_onode(Onode *on, bufferlist& bl, unsigned& off);
  Onode* get_onode(pobject_t oid);     // get cached onode, or read from disk.  ref++.
  void remove_onode(Onode *on);
  void put_onode(Onode* o);         // put it back down.  ref--.
  void dirty_onode(Onode* o);
  void write_onode(Onode *on);

  // ** cnodes **
  hash_map<coll_t, Cnode*, rjhash<coll_t> >    cnode_map;
  LRU                         cnode_lru;
  set<Cnode*>                 dirty_cnodes;
  map<coll_t, list<Cond*> >   waitfor_cnode;

  Cnode* decode_cnode(bufferlist& bl, unsigned& off, csum_t csum);
  csum_t encode_cnode(Cnode *cn, bufferlist& bl, unsigned& off);
  Cnode* new_cnode(coll_t cid);
  Cnode* get_cnode(coll_t cid);
  void remove_cnode(Cnode *cn);
  void put_cnode(Cnode *cn);
  void dirty_cnode(Cnode *cn);
  void write_cnode(Cnode *cn);

  // ** onodes+cnodes = inodes **
  int  inodes_flushing;
  Cond inode_commit_cond;

  void flush_inode_finish();
  void commit_inodes_start();
  void commit_inodes_wait();
  friend class C_E_InodeFlush;

  void trim_inodes(int max = -1);

  // ** buffer cache **
  BufferCache bc;
  pthread_t flushd_thread_id;

  version_t trigger_commit();
  void commit_bc_wait(version_t epoch);
  void trim_bc(off_t max = -1);

 public:
  void kick_idle();
  void sync();
  void sync(Context *onsafe);
  void trim_buffer_cache();

  class IdleKicker : public BlockDevice::kicker {
    Ebofs *ebo;
  public:
    IdleKicker(Ebofs *t) : ebo(t) {}
    void kick() { ebo->kick_idle(); }
  } idle_kicker;


 protected:
  int check_partial_edges(Onode *on, off_t off, off_t len, 
			  bool &partial_head, bool &partial_tail);

  void alloc_write(Onode *on, 
                   block_t start, block_t len, 
                   interval_set<block_t>& alloc,
                   block_t& old_bfirst, block_t& old_blast,
		   csum_t& old_csum_first, csum_t& old_csum_last);
  int apply_write(Onode *on, off_t off, off_t len, const bufferlist& bl);
  int apply_zero(Onode *on, off_t off, size_t len);
  int attempt_read(Onode *on, off_t off, size_t len, bufferlist& bl, 
		   Cond *will_wait_on, bool *will_wait_on_bool);

  // ** finisher **
  // async write notification to users
  Mutex          finisher_lock;
  Cond           finisher_cond;
  bool           finisher_stop;
  list<Context*> finisher_queue;

public:
  void queue_finisher(Context *c) {
    finisher_lock.Lock();
    finisher_queue.push_back(c);
    finisher_cond.Signal();
    finisher_lock.Unlock();
  }
  void queue_finishers(list<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.splice(finisher_queue.end(), ls);
    finisher_cond.Signal();
    finisher_lock.Unlock();
  }
protected:

  void *finisher_thread_entry();
  class FinisherThread : public Thread {
    Ebofs *ebofs;
  public:
    FinisherThread(Ebofs *e) : ebofs(e) {}
    void* entry() { return (void*)ebofs->finisher_thread_entry(); }
  } finisher_thread;


  void alloc_more_node_space();

  void do_csetattrs(map<coll_t, map<const char*, pair<void*,int> > > &cmods);
  void do_setattrs(Onode *on, map<const char*, pair<void*,int> > &setattrs);


 public:
  Ebofs(char *devfn, char *jfn=0) : 
    fake_writes(false),
    dev(devfn), 
    mounted(false), unmounting(false), dirty(false), readonly(false), 
    super_epoch(0), commit_starting(false), commit_thread_started(false),
    commit_thread(this),
    journalfn(jfn), journal(0),
    free_blocks(0), limbo_blocks(0),
    allocator(this),
    nodepool(ebofs_lock),
    object_tab(0), limbo_tab(0), collection_tab(0), co_tab(0),
    onode_lru(g_conf.ebofs_oc_size),
    cnode_lru(g_conf.ebofs_cc_size),
    inodes_flushing(0),
    bc(dev, ebofs_lock),
    idle_kicker(this),
    finisher_stop(false), finisher_thread(this) {
    for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
      free_tab[i] = 0;
    if (!journalfn) {
      journalfn = new char[strlen(devfn) + 100];
      strcpy(journalfn, devfn);
      strcat(journalfn, ".journal");
    }
  }
  ~Ebofs() {
  }

  int mkfs();
  int mount();
  int umount();
  
  int statfs(struct statfs *buf);

  // atomic transaction
  unsigned apply_transaction(Transaction& t, Context *onsafe=0);

  int pick_object_revision_lt(pobject_t& oid);

  // object interface
  bool exists(pobject_t);
  int stat(pobject_t, struct stat*);
  int read(pobject_t, off_t off, size_t len, bufferlist& bl);
  int is_cached(pobject_t oid, off_t off, size_t len);

  int write(pobject_t oid, off_t off, size_t len, const bufferlist& bl, Context *onsafe);
  int zero(pobject_t oid, off_t off, size_t len, Context *onsafe);
  int truncate(pobject_t oid, off_t size, Context *onsafe=0);
  int remove(pobject_t oid, Context *onsafe=0);
  bool write_will_block();
  void trim_from_cache(pobject_t oid, off_t off, size_t len);

  int rename(pobject_t from, pobject_t to);
  int clone(pobject_t from, pobject_t to, Context *onsafe);

  int list_objects(list<pobject_t>& ls);

  // object attr
  int setattr(pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe=0);
  int setattrs(pobject_t oid, map<string,bufferptr>& attrset, Context *onsafe=0);
  int getattr(pobject_t oid, const char *name, void *value, size_t size);
  int getattrs(pobject_t oid, map<string,bufferptr> &aset);
  int rmattr(pobject_t oid, const char *name, Context *onsafe=0);
  int listattr(pobject_t oid, vector<string>& attrs);

  int get_object_collections(pobject_t oid, set<coll_t>& ls);

  // collections
  int list_collections(list<coll_t>& ls);
  bool collection_exists(coll_t c);

  int create_collection(coll_t c, Context *onsafe);
  int destroy_collection(coll_t c, Context *onsafe);
  int collection_add(coll_t c, pobject_t o, Context *onsafe);
  int collection_remove(coll_t c, pobject_t o, Context *onsafe);

  int collection_list(coll_t c, list<pobject_t>& o);
  
  int collection_setattr(coll_t cid, const char *name, const void *value, size_t size, Context *onsafe);
  int collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int collection_getattr(coll_t cid, const char *name, void *value, size_t size);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);
  int collection_rmattr(coll_t cid, const char *name, Context *onsafe);
  int collection_listattr(coll_t oid, vector<string>& attrs);
  
  // maps
  int map_lookup(pobject_t o, bufferlist& key, bufferlist& val);
  int map_insert(pobject_t o, bufferlist& key, bufferlist& val);
  int map_remove(pobject_t o, bufferlist& key);
  int map_list(pobject_t o, list<bufferlist>& keys);
  int map_list(pobject_t o, map<bufferlist,bufferlist>& vals);
  int map_list(pobject_t o, 
	       bufferlist& start, bufferlist& end,
	       map<bufferlist,bufferlist>& vals);

  // crap
  void _fake_writes(bool b) { fake_writes = b; }
  void _get_frag_stat(FragmentationStat& st);

  void _import_freelist(bufferlist& bl);
  void _export_freelist(bufferlist& bl);


private:
  // private interface -- use if caller already holds lock
  unsigned _apply_transaction(Transaction& t);

  int _read(pobject_t oid, off_t off, size_t len, bufferlist& bl);
  int _is_cached(pobject_t oid, off_t off, size_t len);
  int _stat(pobject_t oid, struct stat *st);
  int _getattr(pobject_t oid, const char *name, void *value, size_t size);
  int _getattrs(pobject_t oid, map<string,bufferptr> &aset);
  int _get_object_collections(pobject_t oid, set<coll_t>& ls);

  bool _write_will_block();
  int _write(pobject_t oid, off_t off, size_t len, const bufferlist& bl);
  void _trim_from_cache(pobject_t oid, off_t off, size_t len);
  int _truncate(pobject_t oid, off_t size);
  int _zero(pobject_t oid, off_t offset, size_t length);
  int _remove(pobject_t oid);
  int _clone(pobject_t from, pobject_t to);
  int _setattr(pobject_t oid, const char *name, const void *value, size_t size);
  int _setattrs(pobject_t oid, map<string,bufferptr>& attrset);
  int _rmattr(pobject_t oid, const char *name);
  bool _collection_exists(coll_t c);
  int _create_collection(coll_t c);
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t c, pobject_t o);
  int _collection_remove(coll_t c, pobject_t o);
  int _collection_getattrs(coll_t oid, map<string,bufferptr> &aset);
  int _collection_setattr(coll_t oid, const char *name, const void *value, size_t size);
  int _collection_setattrs(coll_t oid, map<string,bufferptr> &aset);
  int _collection_rmattr(coll_t cid, const char *name);

  
};
