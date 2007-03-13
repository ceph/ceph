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



#include "Ebofs.h"

#include <errno.h>

#ifndef DARWIN
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

// *******************

#undef dout
#define dout(x) if (x <= g_conf.debug_ebofs) cout << "ebofs(" << dev.get_device_name() << ")."
#define derr(x) if (x <= g_conf.debug_ebofs) cerr << "ebofs(" << dev.get_device_name() << ")."


char *nice_blocks(block_t b) 
{
  static char s[20];
  float sz = b*4.0;
  if (sz > (10 << 20)) 
    sprintf(s,"%.1f GB", sz / (1024.0*1024.0));
  else if (sz > (10 << 10)) 
    sprintf(s,"%.1f MB", sz / (1024.0));
  else 
    sprintf(s,"%llu KB", b*4ULL);
  return s;
}

int Ebofs::mount()
{
  ebofs_lock.Lock();
  assert(!mounted);

  int r = dev.open(&idle_kicker);
  if (r < 0) {
    ebofs_lock.Unlock();
    return r;
  }

  dout(2) << "mounting " << dev.get_device_name() << " " << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks()) << endl;

  // read super
  bufferptr bp1 = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  bufferptr bp2 = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  dev.read(0, 1, bp1);
  dev.read(1, 1, bp2);

  struct ebofs_super *sb1 = (struct ebofs_super*)bp1.c_str();
  struct ebofs_super *sb2 = (struct ebofs_super*)bp2.c_str();
  dout(3) << "mount super @0 epoch " << sb1->epoch << endl;
  dout(3) << "mount super @1 epoch " << sb2->epoch << endl;

  // pick newest super
  struct ebofs_super *sb = 0;
  if (sb1->epoch > sb2->epoch)
    sb = sb1;
  else
    sb = sb2;
  super_epoch = sb->epoch;
  dout(3) << "mount epoch " << super_epoch << endl;
  assert(super_epoch == sb->epoch);

  free_blocks = sb->free_blocks;
  limbo_blocks = sb->limbo_blocks;

  // init node pools
  dout(3) << "mount nodepool" << endl;
  nodepool.init( &sb->nodepool );
  nodepool.read_usemap( dev, super_epoch );
  nodepool.read_clean_nodes( dev );
  
  // open tables
  dout(3) << "mount opening tables" << endl;
  object_tab = new Table<object_t, Extent>( nodepool, sb->object_tab );
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    free_tab[i] = new Table<block_t, block_t>( nodepool, sb->free_tab[i] );
  limbo_tab = new Table<block_t, block_t>( nodepool, sb->limbo_tab );
  alloc_tab = new Table<block_t, pair<block_t,int> >( nodepool, sb->alloc_tab );
  
  collection_tab = new Table<coll_t, Extent>( nodepool, sb->collection_tab );
  co_tab = new Table<coll_object_t, bool>( nodepool, sb->co_tab );

  allocator.release_limbo();

  dout(3) << "mount starting commit+finisher threads" << endl;
  commit_thread.create();
  finisher_thread.create();

  dout(1) << "mounted " << dev.get_device_name() << " " << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks()) << endl;
  mounted = true;

  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::mkfs()
{
  ebofs_lock.Lock();
  assert(!mounted);

  int r = dev.open();
  if (r < 0) {
    ebofs_lock.Unlock();
    return r;
  }

  block_t num_blocks = dev.get_num_blocks();

  free_blocks = 0;
  limbo_blocks = 0;

  // create first noderegion
  Extent nr;
  nr.start = 2;
  nr.length = 20+ (num_blocks / 1000);
  if (nr.length < 10) nr.length = 10;
  nodepool.add_region(nr);
  dout(10) << "mkfs: first node region at " << nr << endl;

  // allocate two usemaps
  block_t usemap_len = nodepool.get_usemap_len();
  nodepool.usemap_even.start = nr.end();
  nodepool.usemap_even.length = usemap_len;
  nodepool.usemap_odd.start = nodepool.usemap_even.end();
  nodepool.usemap_odd.length = usemap_len;
  dout(10) << "mkfs: even usemap at " << nodepool.usemap_even << endl;
  dout(10) << "mkfs:  odd usemap at " << nodepool.usemap_odd << endl;

  // init tables
  struct ebofs_table empty;
  empty.num_keys = 0;
  empty.root = -1;
  empty.depth = 0;
  
  object_tab = new Table<object_t, Extent>( nodepool, empty );
  collection_tab = new Table<coll_t, Extent>( nodepool, empty );
  
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    free_tab[i] = new Table<block_t,block_t>( nodepool, empty );
  limbo_tab = new Table<block_t,block_t>( nodepool, empty );
  alloc_tab = new Table<block_t,pair<block_t,int> >( nodepool, empty );
  
  co_tab = new Table<coll_object_t, bool>( nodepool, empty );

  // add free space
  Extent left;
  left.start = nodepool.usemap_odd.end();
  left.length = num_blocks - left.start;
  dout(10) << "mkfs: free data blocks at " << left << endl;
  allocator._release_into_limbo( left );
  if (g_conf.ebofs_cloneable) {
    allocator.alloc_inc(nr);
    allocator.alloc_inc(nodepool.usemap_even);
    allocator.alloc_inc(nodepool.usemap_odd);
  }
  allocator.commit_limbo();   // -> limbo_tab
  allocator.release_limbo();  // -> free_tab

  // write nodes, super, 2x
  dout(10) << "mkfs: flushing nodepool and superblocks (2x)" << endl;

  nodepool.commit_start( dev, 0 );
  nodepool.commit_wait();
  bufferptr superbp0;
  prepare_super(0, superbp0);
  write_super(0, superbp0);
  
  nodepool.commit_start( dev, 1 );
  nodepool.commit_wait();
  bufferptr superbp1;
  prepare_super(1, superbp1);
  write_super(1, superbp1);
  
  // free memory
  dout(10) << "mkfs: cleaning up" << endl;
  close_tables();

  dev.close();

  dout(2) << "mkfs: " << dev.get_device_name() << " "  << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks()) << endl;
  ebofs_lock.Unlock();
  return 0;
}

void Ebofs::close_tables() 
{
  // close tables
  delete object_tab;
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    delete free_tab[i];
  delete limbo_tab;
  delete alloc_tab;
  delete collection_tab;
  delete co_tab;

  nodepool.close();
}

int Ebofs::umount()
{
  ebofs_lock.Lock();
  
  // mark unmounting
  dout(1) << "umount start" << endl;
  readonly = true;
  unmounting = true;
  
  // kick commit thread
  dout(5) << "umount stopping commit thread" << endl;
  commit_cond.Signal();
  ebofs_lock.Unlock();
  commit_thread.join();
  ebofs_lock.Lock();

  // kick finisher thread
  dout(5) << "umount stopping finisher thread" << endl;
  finisher_lock.Lock();
  finisher_stop = true;
  finisher_cond.Signal();
  finisher_lock.Unlock();

  finisher_thread.join();

  trim_bc(0);
  trim_inodes(0);

  for (hash_map<object_t,Onode*>::iterator i = onode_map.begin();
       i != onode_map.end();
       i++) {
    dout(0) << "umount *** leftover: " << i->first << "   " << *(i->second) << endl;
  }

  // free memory
  dout(5) << "umount cleaning up" << endl;
  close_tables();
  dev.close();
  readonly = unmounting = mounted = false;

  dout(1) << "umount done on " << dev.get_device_name() << endl;
  ebofs_lock.Unlock();
  return 0;
}



void Ebofs::prepare_super(version_t epoch, bufferptr& bp)
{
  struct ebofs_super sb;
  
  dout(10) << "prepare_super v" << epoch << endl;

  // fill in super
  memset(&sb, 0, sizeof(sb));
  sb.s_magic = EBOFS_MAGIC;
  sb.epoch = epoch;
  sb.num_blocks = dev.get_num_blocks();

  sb.free_blocks = free_blocks;
  sb.limbo_blocks = limbo_blocks;


  // tables
  sb.object_tab.num_keys = object_tab->get_num_keys();
  sb.object_tab.root = object_tab->get_root();
  sb.object_tab.depth = object_tab->get_depth();

  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++) {
    sb.free_tab[i].num_keys = free_tab[i]->get_num_keys();
    sb.free_tab[i].root = free_tab[i]->get_root();
    sb.free_tab[i].depth = free_tab[i]->get_depth();
  }
  sb.limbo_tab.num_keys = limbo_tab->get_num_keys();
  sb.limbo_tab.root = limbo_tab->get_root();
  sb.limbo_tab.depth = limbo_tab->get_depth();

  sb.alloc_tab.num_keys = alloc_tab->get_num_keys();
  sb.alloc_tab.root = alloc_tab->get_root();
  sb.alloc_tab.depth = alloc_tab->get_depth();

  sb.collection_tab.num_keys = collection_tab->get_num_keys();
  sb.collection_tab.root = collection_tab->get_root();
  sb.collection_tab.depth = collection_tab->get_depth();

  sb.co_tab.num_keys = co_tab->get_num_keys();
  sb.co_tab.root = co_tab->get_root();
  sb.co_tab.depth = co_tab->get_depth();

  // pools
  sb.nodepool.num_regions = nodepool.region_loc.size();
  for (unsigned i=0; i<nodepool.region_loc.size(); i++) {
    sb.nodepool.region_loc[i] = nodepool.region_loc[i];
  }
  sb.nodepool.node_usemap_even = nodepool.usemap_even;
  sb.nodepool.node_usemap_odd = nodepool.usemap_odd;
  
  // put in a buffer
  bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  memcpy(bp.c_str(), (const char*)&sb, sizeof(sb));
}

void Ebofs::write_super(version_t epoch, bufferptr& bp)
{
  block_t bno = epoch & 1;
  
  dout(10) << "write_super v" << epoch << " to b" << bno << endl;

  dev.write(bno, 1, bp, "write_super");
}

int Ebofs::commit_thread_entry()
{  
  ebofs_lock.Lock();
  dout(10) << "commit_thread start" << endl;

  assert(!commit_thread_started); // there can be only one
  commit_thread_started = true;
  sync_cond.Signal();

  while (mounted) {
    
    // wait for kick, or timeout
    if (g_conf.ebofs_commit_ms) {
      if (g_conf.ebofs_idle_commit_ms > 0) {
        // periodically check for idle block device
        dout(20) << "commit_thread sleeping (up to) " << g_conf.ebofs_commit_ms << " ms, " 
                 << g_conf.ebofs_idle_commit_ms << " ms if idle" << endl;
        long left = g_conf.ebofs_commit_ms;
        while (left > 0) {
          long next = MIN(left, g_conf.ebofs_idle_commit_ms);
          if (commit_cond.WaitInterval(ebofs_lock, utime_t(0, next*1000)) != ETIMEDOUT) 
            break;   // we got kicked
          if (dev.is_idle()) {
            dout(20) << "commit_thread bdev is idle, early commit" << endl;
            break;  // dev is idle
          }
          left -= next;
          dout(20) << "commit_thread " << left << " ms left" << endl;

          // hack hack
          //if (!left) g_conf.debug_ebofs = 10;
          // /hack hack
        }
      } else {
        // normal wait+timeout
        dout(20) << "commit_thread sleeping (up to) " << g_conf.ebofs_commit_ms << " ms" << endl;
        commit_cond.WaitInterval(ebofs_lock, utime_t(0, g_conf.ebofs_commit_ms*1000));   
      }

    } else {
      // DEBUG.. wait until kicked
      dout(10) << "commit_thread no commit_ms, waiting until kicked" << endl;
      commit_cond.Wait(ebofs_lock);
    }

    if (unmounting) {
      dout(10) << "commit_thread unmounting: final commit pass" << endl;
      assert(readonly);
      unmounting = false;
      mounted = false;
      dirty = true;
    }
    
    if (!dirty && !limbo_blocks) {
      dout(10) << "commit_thread not dirty" << endl;
    }
    else {
      super_epoch++;
      dirty = false;

      dout(10) << "commit_thread commit start, new epoch " << super_epoch << endl;
      dout(2) << "commit_thread   data: " 
              << 100*(dev.get_num_blocks()-get_free_blocks())/dev.get_num_blocks() << "% used, "
              << get_free_blocks() << " (" << 100*get_free_blocks()/dev.get_num_blocks() 
              << "%) free in " << get_free_extents() 
              << ", " << get_limbo_blocks() << " (" << 100*get_limbo_blocks()/dev.get_num_blocks() 
              << "%) limbo in " << get_limbo_extents() 
              << endl;
      dout(2) << "commit_thread  nodes: " 
              << 100*nodepool.num_used()/nodepool.num_total() << "% used, "
              << nodepool.num_free() << " (" << 100*nodepool.num_free()/nodepool.num_total() << "%) free, " 
              << nodepool.num_limbo() << " (" << 100*nodepool.num_limbo()/nodepool.num_total() << "%) limbo, " 
              << nodepool.num_total() << " total." << endl;
      dout(2) << "commit_thread    bc: " 
              << "size " << bc.get_size() 
              << ", trimmable " << bc.get_trimmable()
              << ", max " << g_conf.ebofs_bc_size
              << "; dirty " << bc.get_stat_dirty()
              << ", tx " << bc.get_stat_tx()
              << ", max dirty " << g_conf.ebofs_bc_max_dirty
              << endl;
      
      
      // (async) write onodes+condes  (do this first; it currently involves inode reallocation)
      commit_inodes_start();
      
      allocator.commit_limbo();   // limbo -> limbo_tab
      
      // (async) write btree nodes
      nodepool.commit_start( dev, super_epoch );
      
      // blockdev barrier (prioritize our writes!)
      dout(30) << "commit_thread barrier.  flushing inodes " << inodes_flushing << endl;
      dev.barrier();

      // prepare super (before any changes get made!)
      bufferptr superbp;
      prepare_super(super_epoch, superbp);
      
      // wait for it all to flush (drops global lock)
      commit_bc_wait(super_epoch-1);  
      dout(30) << "commit_thread bc flushed" << endl;
      commit_inodes_wait();
      dout(30) << "commit_thread inodes flushed" << endl;
      nodepool.commit_wait();
      dout(30) << "commit_thread btree nodes flushed" << endl;

      // ok, now (synchronously) write the prior super!
      dout(10) << "commit_thread commit flushed, writing super for prior epoch" << endl;
      ebofs_lock.Unlock();
      write_super(super_epoch, superbp);    
      ebofs_lock.Lock();
      
      dout(10) << "commit_thread wrote super" << endl;

      // free limbo space now 
      // (since we're done allocating things, 
      //  AND we've flushed all previous epoch data)
      allocator.release_limbo();   // limbo_tab -> free_tabs
      
      // do we need more node space?
      if (nodepool.num_free() < nodepool.num_total() / 3) {
        dout(2) << "commit_thread running low on node space, allocating more." << endl;
        alloc_more_node_space();
      }
      
      // kick waiters
      dout(10) << "commit_thread queueing commit + kicking sync waiters" << endl;
      
      finisher_lock.Lock();
      finisher_queue.splice(finisher_queue.end(), commit_waiters[super_epoch-1]);
      commit_waiters.erase(super_epoch-1);
      finisher_cond.Signal();
      finisher_lock.Unlock();

      sync_cond.Signal();

      dout(10) << "commit_thread commit finish" << endl;
    }

    // trim bc?
    trim_bc();
    trim_inodes();

  }
  
  dout(10) << "commit_thread finish" << endl;
  commit_thread_started = false;
  ebofs_lock.Unlock();
  return 0;
}


void Ebofs::alloc_more_node_space()
{
  dout(1) << "alloc_more_node_space free " << nodepool.num_free() << "/" << nodepool.num_total() << endl;
  
  if (nodepool.num_regions() < EBOFS_MAX_NODE_REGIONS) {
    int want = nodepool.num_total();

    Extent ex;
    allocator.allocate(ex, want, 2);
    dout(1) << "alloc_more_node_space wants " << want << " more, got " << ex << endl;

    Extent even, odd;
    unsigned ulen = nodepool.get_usemap_len(nodepool.num_total() + ex.length);
    allocator.allocate(even, ulen, 2);
    allocator.allocate(odd, ulen, 2);
    dout(1) << "alloc_more_node_space maps need " << ulen << " x2, got " << even << " " << odd << endl;

    if (even.length == ulen && odd.length == ulen) {
      dout(1) << "alloc_more_node_space got " << ex << ", new usemaps at even " << even << " odd " << odd << endl;
      allocator.release(nodepool.usemap_even);
      allocator.release(nodepool.usemap_odd);
      nodepool.add_region(ex);
      nodepool.usemap_even = even;
      nodepool.usemap_odd = odd;
    } else {
      dout (1) << "alloc_more_node_space failed to get space for new usemaps" << endl;
      allocator.release(ex);
      allocator.release(even);
      allocator.release(odd);
      //assert(0);
    }
  } else {
    dout(1) << "alloc_more_node_space already have max node regions!" << endl;
    assert(0);
  }
}


void *Ebofs::finisher_thread_entry()
{
  finisher_lock.Lock();
  dout(10) << "finisher_thread start" << endl;

  while (!finisher_stop) {
    while (!finisher_queue.empty()) {
      list<Context*> ls;
      ls.swap(finisher_queue);

      finisher_lock.Unlock();

      //ebofs_lock.Lock();            // um.. why lock this?  -sage
      finish_contexts(ls, 0);
      //ebofs_lock.Unlock();

      finisher_lock.Lock();
    }
    if (finisher_stop) break;
    
    dout(30) << "finisher_thread sleeping" << endl;
    finisher_cond.Wait(finisher_lock);
  }

  dout(10) << "finisher_thread start" << endl;
  finisher_lock.Unlock();
  return 0;
}


// *** onodes ***

Onode* Ebofs::new_onode(object_t oid)
{
  Onode* on = new Onode(oid);

  assert(onode_map.count(oid) == 0);
  onode_map[oid] = on;
  onode_lru.lru_insert_top(on);
  
  assert(object_tab->lookup(oid) < 0);
  object_tab->insert( oid, on->onode_loc );  // even tho i'm not placed yet

  on->get();
  on->onode_loc.start = 0;
  on->onode_loc.length = 0;

  dirty_onode(on);

  dout(7) << "new_onode " << *on << endl;
  return on;
}


Onode* Ebofs::get_onode(object_t oid)
{
  while (1) {
    // in cache?
    if (have_onode(oid)) {
      // yay
      Onode *on = onode_map[oid];
      on->get();
      //cout << "get_onode " << *on << endl;
      return on;   
    }
    
    // on disk?
    Extent onode_loc;
    if (object_tab->lookup(oid, onode_loc) < 0) {
      dout(10) << "onode lookup failed on " << oid << endl;
      // object dne.
      return 0;
    }
    
    // already loading?
    if (waitfor_onode.count(oid)) {
      // yep, just wait.
      Cond c;
      waitfor_onode[oid].push_back(&c);
      dout(10) << "get_onode " << oid << " already loading, waiting" << endl;
      c.Wait(ebofs_lock);
      continue;
    }

    dout(10) << "get_onode reading " << oid << " from " << onode_loc << endl;

    assert(waitfor_onode.count(oid) == 0);
    waitfor_onode[oid].clear();  // this should be empty initially. 

    // read it!
    bufferlist bl;
    bl.push_back( buffer::create_page_aligned( EBOFS_BLOCK_SIZE*onode_loc.length ) );

    ebofs_lock.Unlock();
    dev.read( onode_loc.start, onode_loc.length, bl );
    ebofs_lock.Lock();
    
    // add onode
    Onode *on = new Onode(oid);
    onode_map[oid] = on;
    onode_lru.lru_insert_top(on);
    
    // parse data block
    struct ebofs_onode *eo = (struct ebofs_onode*)bl.c_str();
    if (eo->object_id != oid) {
      cerr << " wrong oid in onode block: " << eo->object_id << " != " << oid << endl;
      cerr << " onode_loc is " << eo->onode_loc << endl;
      cerr << " object_size " << eo->object_size << endl;
      cerr << " object_blocks " << eo->object_blocks << endl;
      cerr << " " << eo->num_collections << " coll + " 
           << eo->num_attr << " attr + " 
           << eo->num_extents << " extents" << endl;
      assert(eo->object_id == oid);
    }
    on->readonly = eo->readonly;
    on->onode_loc = eo->onode_loc;
    on->object_size = eo->object_size;
    on->object_blocks = eo->object_blocks;

    // parse
    char *p = bl.c_str() + sizeof(*eo);

    // parse collection list
    for (int i=0; i<eo->num_collections; i++) {
      coll_t c = *((coll_t*)p);
      p += sizeof(c);
      on->collections.insert(c);
    }

    // parse attributes
    for (int i=0; i<eo->num_attr; i++) {
      string key = p;
      p += key.length() + 1;
      int len = *(int*)(p);
      p += sizeof(len);
      on->attr[key] = buffer::copy(p, len);
      p += len;
      dout(15) << "get_onode " << *on  << " attr " << key << " len " << len << endl;
    }
    
    // parse extents
    on->extent_map.clear();
    block_t n = 0;
    for (int i=0; i<eo->num_extents; i++) {
      Extent ex = *((Extent*)p);
      on->extent_map[n] = ex;
      dout(15) << "get_onode " << *on  << " ex " << i << ": " << ex << endl;
      n += ex.length;
      p += sizeof(Extent);
    }
    assert(n == on->object_blocks);

    // wake up other waiters
    for (list<Cond*>::iterator i = waitfor_onode[oid].begin();
         i != waitfor_onode[oid].end();
         i++)
      (*i)->Signal();
    waitfor_onode.erase(oid);   // remove Cond list
    
    on->get();
    //cout << "get_onode " << *on << " (loaded)" << endl;
    return on;
  }
}


class C_E_InodeFlush : public BlockDevice::callback {
  Ebofs *ebofs;
public:
  C_E_InodeFlush(Ebofs *e) : ebofs(e) {}
  void finish(ioh_t ioh, int r) {
    ebofs->flush_inode_finish();
  }
};


void Ebofs::encode_onode(Onode *on, bufferlist& bl, unsigned& off)
{
  // onode
  struct ebofs_onode eo;
  eo.readonly = on->readonly;
  eo.onode_loc = on->onode_loc;
  eo.object_id = on->object_id;
  eo.object_size = on->object_size;
  eo.object_blocks = on->object_blocks;
  eo.num_collections = on->collections.size();
  eo.num_attr = on->attr.size();
  eo.num_extents = on->extent_map.size();
  bl.copy_in(off, sizeof(eo), (char*)&eo);
  off += sizeof(eo);

  // collections
  for (set<coll_t>::iterator i = on->collections.begin();
       i != on->collections.end();
       i++) {
    bl.copy_in(off, sizeof(*i), (char*)&(*i));
    off += sizeof(*i);
  }    
  
  // attr
  for (map<string, bufferptr>::iterator i = on->attr.begin();
       i != on->attr.end();
       i++) {
    bl.copy_in(off, i->first.length()+1, i->first.c_str());
    off += i->first.length()+1;
    int l = i->second.length();
    bl.copy_in(off, sizeof(int), (char*)&l);
    off += sizeof(int);
    bl.copy_in(off, l, i->second.c_str());
    off += l;
    dout(15) << "write_onode " << *on  << " attr " << i->first << " len " << l << endl;
  }
  
  // extents
  for (map<block_t,Extent>::iterator i = on->extent_map.begin();
       i != on->extent_map.end();
       i++) {
    bl.copy_in(off, sizeof(Extent), (char*)&(i->second));
    off += sizeof(Extent);
    dout(15) << "write_onode " << *on  << " ex " << i->first << ": " << i->second << endl;
  }
}

void Ebofs::write_onode(Onode *on)
{
  // buffer
  unsigned bytes = sizeof(ebofs_onode) + on->get_collection_bytes() + on->get_attr_bytes() + on->get_extent_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;

  bufferlist bl;
  bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*blocks) );

  // (always) relocate onode
  if (1) {
    if (on->onode_loc.length) 
      allocator.release(on->onode_loc);
    
    block_t first = 0;
    if (on->extent_map.size()) 
      first = on->extent_map.begin()->second.start;
    
    allocator.allocate(on->onode_loc, blocks, first);
    object_tab->remove( on->object_id );
    object_tab->insert( on->object_id, on->onode_loc );
    //object_tab->verify();
  }

  dout(10) << "write_onode " << *on << " to " << on->onode_loc << endl;

  unsigned off = 0;
  encode_onode(on, bl, off);
  assert(off == bytes);

  // write
  dev.write( on->onode_loc.start, on->onode_loc.length, bl, 
             new C_E_InodeFlush(this), "write_onode" );
}

void Ebofs::remove_onode(Onode *on)
{
  dout(8) << "remove_onode " << *on << endl;

  assert(on->get_ref_count() >= 1);  // caller

  // tear down buffer cache
  if (on->oc) {
    on->oc->truncate(0, super_epoch);         // this will kick readers along the way.
    on->close_oc();
  }

  // remove from onode map, mark dangling/deleted
  onode_map.erase(on->object_id);
  onode_lru.lru_remove(on);
  on->deleted = true;
  on->dangling = true;
  
  // remove from object table
  //dout(0) << "remove_onode on " << *on << endl;
  object_tab->remove(on->object_id);
  
  // free onode space
  if (on->onode_loc.length)
    allocator.release(on->onode_loc);
  
  // free data space
  for (map<block_t,Extent>::iterator i = on->extent_map.begin();
       i != on->extent_map.end();
       i++)
    allocator.release(i->second);
  on->extent_map.clear();

  // remove from collections
  for (set<coll_t>::iterator i = on->collections.begin();
       i != on->collections.end();
       i++) {
    co_tab->remove(coll_object_t(*i,on->object_id));
  }
  on->collections.clear();

  // dirty -> clean?
  if (on->is_dirty()) {
    on->mark_clean();         // this unpins *on
    dirty_onodes.erase(on);
  }

  if (on->get_ref_count() > 1) cout << "remove_onode **** will survive " << *on << endl;
  put_onode(on);

  dirty = true;
}

void Ebofs::put_onode(Onode *on)
{
  on->put();
  //cout << "put_onode " << *on << endl;
  
  if (on->get_ref_count() == 0 && on->dangling) {
    //cout << " *** hosing on " << *on << endl;
    delete on;
  }
}

void Ebofs::dirty_onode(Onode *on)
{
  if (!on->is_dirty()) {
    on->mark_dirty();
    dirty_onodes.insert(on);
  }
  dirty = true;
}

void Ebofs::trim_inodes(int max)
{
  unsigned omax = onode_lru.lru_get_max();
  unsigned cmax = cnode_lru.lru_get_max();
  if (max >= 0) omax = cmax = max;
  dout(10) << "trim_inodes start " << onode_lru.lru_get_size() << " / " << omax << " onodes, " 
            << cnode_lru.lru_get_size() << " / " << cmax << " cnodes" << endl;

  // onodes
  while (onode_lru.lru_get_size() > omax) {
    // expire an item
    Onode *on = (Onode*)onode_lru.lru_expire();
    if (on == 0) break;  // nothing to expire
    
    // expire
    dout(20) << "trim_inodes removing onode " << *on << endl;
    onode_map.erase(on->object_id);
    on->dangling = true;

    if (on->get_ref_count() == 0) {
      assert(on->oc == 0);   // an open oc pins the onode!
      delete on;
    } else {
      dout(-20) << "trim_inodes   still active: " << *on << endl;
      assert(0); // huh?
    }
  }


  // cnodes
  while (cnode_lru.lru_get_size() > cmax) {
    // expire an item
    Cnode *cn = (Cnode*)cnode_lru.lru_expire();
    if (cn == 0) break;  // nothing to expire

    // expire
    dout(20) << "trim_inodes removing cnode " << *cn << endl;
    cnode_map.erase(cn->coll_id);
    
    delete cn;
  }

  dout(10) << "trim_inodes finish " 
           << onode_lru.lru_get_size() << " / " << omax << " onodes, " 
           << cnode_lru.lru_get_size() << " / " << cmax << " cnodes" << endl;
}



// *** cnodes ****

Cnode* Ebofs::new_cnode(coll_t cid)
{
  Cnode* cn = new Cnode(cid);

  assert(cnode_map.count(cid) == 0);
  cnode_map[cid] = cn;
  cnode_lru.lru_insert_top(cn);
  
  assert(collection_tab->lookup(cid) < 0);
  collection_tab->insert( cid, cn->cnode_loc );  // even tho i'm not placed yet
  
  cn->get();
  cn->cnode_loc.start = 0;
  cn->cnode_loc.length = 0;

  dirty_cnode(cn);

  return cn;
}

Cnode* Ebofs::get_cnode(coll_t cid)
{
  while (1) {
    // in cache?
    if (cnode_map.count(cid)) {
      // yay
      Cnode *cn = cnode_map[cid];
      cn->get();
      return cn;   
    }
    
    // on disk?
    Extent cnode_loc;
    if (collection_tab->lookup(cid, cnode_loc) < 0) {
      // object dne.
      return 0;
    }
    
    // already loading?
    if (waitfor_cnode.count(cid)) {
      // yep, just wait.
      Cond c;
      waitfor_cnode[cid].push_back(&c);
      dout(10) << "get_cnode " << cid << " already loading, waiting" << endl;
      c.Wait(ebofs_lock);
      continue;
    }

    dout(10) << "get_cnode reading " << cid << " from " << cnode_loc << endl;

    assert(waitfor_cnode.count(cid) == 0);
    waitfor_cnode[cid].clear();  // this should be empty initially. 

    // read it!
    bufferlist bl;
    //bufferpool.alloc( EBOFS_BLOCK_SIZE*cnode_loc.length, bl );
    bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*cnode_loc.length) );

    ebofs_lock.Unlock();
    dev.read( cnode_loc.start, cnode_loc.length, bl );
    ebofs_lock.Lock();

    // parse data block
    Cnode *cn = new Cnode(cid);

    cnode_map[cid] = cn;
    cnode_lru.lru_insert_top(cn);
    
    struct ebofs_cnode *ec = (struct ebofs_cnode*)bl.c_str();
    cn->cnode_loc = ec->cnode_loc;
    
    // parse attributes
    char *p = bl.c_str() + sizeof(*ec);
    for (int i=0; i<ec->num_attr; i++) {
      string key = p;
      p += key.length() + 1;
      int len = *(int*)(p);
      p += sizeof(len);
      cn->attr[key] = buffer::copy(p, len);
      p += len;
      dout(15) << "get_cnode " << *cn  << " attr " << key << " len " << len << endl;
    }
    
    // wake up other waiters
    for (list<Cond*>::iterator i = waitfor_cnode[cid].begin();
         i != waitfor_cnode[cid].end();
         i++)
      (*i)->Signal();
    waitfor_cnode.erase(cid);   // remove Cond list

    cn->get();
    return cn;
  }
}

void Ebofs::encode_cnode(Cnode *cn, bufferlist& bl, unsigned& off)
{
  // cnode
  struct ebofs_cnode ec;
  ec.cnode_loc = cn->cnode_loc;
  ec.coll_id = cn->coll_id;
  ec.num_attr = cn->attr.size();
  bl.copy_in(off, sizeof(ec), (char*)&ec);
  off += sizeof(ec);
  
  // attr
  for (map<string, bufferptr >::iterator i = cn->attr.begin();
       i != cn->attr.end();
       i++) {
    bl.copy_in(off, i->first.length()+1, i->first.c_str());
    off += i->first.length()+1;
    int len = i->second.length();
    bl.copy_in(off, sizeof(int), (char*)&len);
    off += sizeof(int);
    bl.copy_in(off, len, i->second.c_str());
    off += len;

    dout(15) << "write_cnode " << *cn  << " attr " << i->first << " len " << len << endl;
  }
}

void Ebofs::write_cnode(Cnode *cn)
{
  // allocate buffer
  unsigned bytes = sizeof(ebofs_cnode) + cn->get_attr_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;
  
  bufferlist bl;
  //bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );
  bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*blocks) );

  // (always) relocate cnode!
  if (1) {
    if (cn->cnode_loc.length) 
      allocator.release(cn->cnode_loc);
    
    allocator.allocate(cn->cnode_loc, blocks, Allocator::NEAR_LAST_FWD);
    collection_tab->remove( cn->coll_id );
    collection_tab->insert( cn->coll_id, cn->cnode_loc );
  }
  
  dout(10) << "write_cnode " << *cn << " to " << cn->cnode_loc << endl;

  unsigned off = 0;
  encode_cnode(cn, bl, off);
  assert(off == bytes);

  // write
  dev.write( cn->cnode_loc.start, cn->cnode_loc.length, bl, 
             new C_E_InodeFlush(this), "write_cnode" );
}

void Ebofs::remove_cnode(Cnode *cn)
{
  dout(10) << "remove_cnode " << *cn << endl;

  // remove from table
  collection_tab->remove(cn->coll_id);

  // free cnode space
  if (cn->cnode_loc.length)
    allocator.release(cn->cnode_loc);

  // remove from dirty list?
  if (cn->is_dirty())
    dirty_cnodes.erase(cn);

  // remove from map and lru
  cnode_map.erase(cn->coll_id);
  cnode_lru.lru_remove(cn);

  // count down refs
  cn->mark_clean();
  cn->put();
  assert(cn->get_ref_count() == 0);

  // hose.
  delete cn;

  dirty = true;
}

void Ebofs::put_cnode(Cnode *cn)
{
  cn->put();
}

void Ebofs::dirty_cnode(Cnode *cn)
{
  if (!cn->is_dirty()) {
    cn->mark_dirty();
    dirty_cnodes.insert(cn);
  }
  dirty = true;
}





void Ebofs::flush_inode_finish()
{
  ebofs_lock.Lock();
  {
    inodes_flushing--;
    if (inodes_flushing < 1000)
    dout(20) << "flush_inode_finish, " << inodes_flushing << " left" << endl;
    if (inodes_flushing == 0) 
      inode_commit_cond.Signal();
  }
  ebofs_lock.Unlock();
}

void Ebofs::commit_inodes_start() 
{
  dout(10) << "commit_inodes_start" << endl;

  assert(inodes_flushing == 0);

  // onodes
  for (set<Onode*>::iterator i = dirty_onodes.begin();
       i != dirty_onodes.end();
       i++) {
    Onode *on = *i;
    inodes_flushing++;
    write_onode(on);
    on->mark_clean();
    on->uncommitted.clear();     // commit allocated blocks
    on->commit_waiters.clear();  // these guys are gonna get taken care of, bc we committed.
  }
  dirty_onodes.clear();

  // cnodes
  for (set<Cnode*>::iterator i = dirty_cnodes.begin();
       i != dirty_cnodes.end();
       i++) {
    Cnode *cn = *i;
    inodes_flushing++;
    write_cnode(cn);
    cn->mark_clean();
  }
  dirty_cnodes.clear();

  dout(10) << "commit_inodes_start writing " << inodes_flushing << " onodes+cnodes" << endl;
}

void Ebofs::commit_inodes_wait()
{
  // caller must hold ebofs_lock
  while (inodes_flushing > 0) {
    dout(10) << "commit_inodes_wait waiting for " << inodes_flushing << " onodes+cnodes to flush" << endl;
    inode_commit_cond.Wait(ebofs_lock);
  }
  dout(10) << "commit_inodes_wait all flushed" << endl;
}







// *** buffer cache ***

void Ebofs::trim_buffer_cache()
{
  ebofs_lock.Lock();
  trim_bc(0);
  ebofs_lock.Unlock();
}

void Ebofs::trim_bc(off_t max)
{
  if (max < 0)
    max = g_conf.ebofs_bc_size;
  dout(10) << "trim_bc start: size " << bc.get_size() << ", trimmable " << bc.get_trimmable() << ", max " << max << endl;

  while (bc.get_size() > max &&
         bc.get_trimmable()) {
    BufferHead *bh = (BufferHead*) bc.lru_rest.lru_expire();
    if (!bh) break;
    
    dout(25) << "trim_bc trimming " << *bh << endl;
    assert(bh->is_clean());
    
    ObjectCache *oc = bh->oc;
    bc.remove_bh(bh);
    delete bh;
    
    if (oc->is_empty()) {
      Onode *on = oc->on;
      dout(10) << "trim_bc  closing oc on " << *on << endl;
      on->close_oc();
    }
  }

  dout(10) << "trim_bc finish: size " << bc.get_size() << ", trimmable " << bc.get_trimmable() << ", max " << max << endl;
}


void Ebofs::kick_idle()
{
  dout(10) << "kick_idle" << endl;
  commit_cond.Signal();

  /*
  ebofs_lock.Lock();
  if (mounted && !unmounting && dirty) {
    dout(0) << "kick_idle dirty, doing commit" << endl;
    commit_cond.Signal();
  } else {
    dout(0) << "kick_idle !dirty or !mounted or unmounting, doing nothing" << endl;
  }
  ebofs_lock.Unlock();
  */
}

void Ebofs::sync(Context *onsafe)
{
  ebofs_lock.Lock();
  if (onsafe) {
    dirty = true;
    commit_waiters[super_epoch].push_back(onsafe);
  }
  ebofs_lock.Unlock();
}

void Ebofs::sync()
{
  ebofs_lock.Lock();
  if (!dirty) {
    dout(7) << "sync in " << super_epoch << ", not dirty" << endl;
  } else {
    epoch_t start = super_epoch;
    dout(7) << "sync start in " << start << endl;
    while (super_epoch == start) {
      dout(7) << "sync kicking commit in " << super_epoch << endl;
      dirty = true;
      commit_cond.Signal();
      sync_cond.Wait(ebofs_lock);
    }
    dout(10) << "sync finish in " << super_epoch << endl;
  }
  ebofs_lock.Unlock();
}



void Ebofs::commit_bc_wait(version_t epoch)
{
  dout(10) << "commit_bc_wait on epoch " << epoch << endl;  
  
  while (bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE,epoch) > 0 ||
         bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL,epoch) > 0) {
    //dout(10) << "commit_bc_wait " << bc.get_unflushed(epoch) << " unflushed in epoch " << epoch << endl;
    dout(10) << "commit_bc_wait epoch " << epoch
              << ", unflushed bhwrite " << bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE) 
              << ", unflushed partial " << bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL) 
              << endl;
    bc.waitfor_flush();
  }

  bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE).erase(epoch);
  bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL).erase(epoch);

  dout(10) << "commit_bc_wait all flushed for epoch " << epoch
            << "; " << bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE)
            << " " << bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL)
            << endl;  
}



int Ebofs::statfs(struct statfs *buf)
{
  dout(7) << "statfs" << endl;

  buf->f_type = EBOFS_MAGIC;             /* type of filesystem */
  buf->f_bsize = 4096;                   /* optimal transfer block size */
  buf->f_blocks = dev.get_num_blocks();  /* total data blocks in file system */
  buf->f_bfree = get_free_blocks() 
    + get_limbo_blocks();                /* free blocks in fs */
  buf->f_bavail = get_free_blocks();     /* free blocks avail to non-superuser -- actually, for writing. */
  buf->f_files = nodepool.num_total();   /* total file nodes in file system */
  buf->f_ffree = nodepool.num_free();    /* free file nodes in fs */
  //buf->f_fsid = 0;                       /* file system id */
#ifndef DARWIN
  buf->f_namelen = 8;                    /* maximum length of filenames */
#endif // DARWIN

  return 0;
}




/*
 * allocate a write to blocks on disk.
 * - take care to not overwrite any "safe" data blocks.
 *  - allocate/map new extents on disk as necessary
 */
void Ebofs::alloc_write(Onode *on, 
                        block_t start, block_t len,
                        interval_set<block_t>& alloc,
                        block_t& old_bfirst, block_t& old_blast)
{
  // first decide what pages to (re)allocate 
  alloc.insert(start, len);   // start with whole range

  // figure out what bits are already uncommitted
  interval_set<block_t> already_uncom;
  already_uncom.intersection_of(alloc, on->uncommitted);

  // subtract those off, so we're left with the committed bits (that must be reallocated).
  alloc.subtract(already_uncom);
  
  dout(10) << "alloc_write must (re)alloc " << alloc << " on " << *on << endl;
  
  // release it (into limbo)
  for (map<block_t,block_t>::iterator i = alloc.m.begin();
       i != alloc.m.end();
       i++) {
    // get old region
    vector<Extent> old;
    on->map_extents(i->first, i->second, old);
    for (unsigned o=0; o<old.size(); o++) 
      allocator.release(old[o]);

    // take note if first/last blocks in write range are remapped.. in case we need to do a partial read/write thing
    // these are for partial, so we don't care about TX bh's, so don't worry about bits canceling stuff below.
    if (!old.empty()) {
      if (i->first == start) {
        old_bfirst = old[0].start;
        dout(20) << "alloc_write  old_bfirst " << old_bfirst << " of " << old[0] << endl;
      }
      if (i->first+i->second == start+len) {
        old_blast = old[old.size()-1].last();
        dout(20) << "alloc_write  old_blast " << old_blast << " of " << old[old.size()-1] << endl;
      }
    }
  }

  // reallocate uncommitted too?
  // ( --> yes.  we can always make better allocation decisions later, with more information. )
  if (g_conf.ebofs_realloc) {
    list<BufferHead*> tx;
    
    ObjectCache *oc = on->get_oc(&bc);
    oc->find_tx(start, len, tx);
    
    for (list<BufferHead*>::reverse_iterator p = tx.rbegin();
         p != tx.rend();
         p++) {
      BufferHead *bh = *p;

      // cancelable/moveable?
      if (alloc.contains(bh->start(), bh->length())) {
        dout(10) << "alloc_write  " << *bh << " already in " << alloc << endl;
        continue;
      }

      vector<Extent> old;
      on->map_extents(bh->start(), bh->length(), old);
      assert(old.size() == 1);

      if (bh->start() >= start && bh->end() <= start+len) {
        assert(bh->epoch_modified == super_epoch);
        if (bc.bh_cancel_write(bh, super_epoch)) {
          if (bh->length() == 1)
          dout(10) << "alloc_write unallocated tx " << old[0] << ", canceled " << *bh << endl;
	  // no, this isn't compatible with clone() and extent reference counting.
          //allocator.unallocate(old[0]);  // release (into free)
	  allocator.release(old[0]);  
          alloc.insert(bh->start(), bh->length());
        } else {
          if (bh->length() == 1)
          dout(10) << "alloc_write released tx " << old[0] << ", couldn't cancel " << *bh << endl;
          allocator.release(old[0]);     // release (into limbo)
          alloc.insert(bh->start(), bh->length());
        }
      } else {
        if (bh->length() == 1)
        dout(10) << "alloc_write  skipped tx " << old[0] << ", not entirely within " 
                 << start << "~" << len 
                 << " bh " << *bh << endl;
      }
    }
    
    dout(10) << "alloc_write will (re)alloc " << alloc << " on " << *on << endl;
  }

  if (alloc.empty()) return;  // no need to dirty the onode below!
  

  // merge alloc into onode uncommitted map
  //dout(10) << " union of " << on->uncommitted << " and " << alloc << endl;
  interval_set<block_t> old = on->uncommitted;
  on->uncommitted.union_of(alloc);
  
  dout(10) << "alloc_write onode.uncommitted is now " << on->uncommitted << endl;

  if (0) {
    // verify
    interval_set<block_t> ta;
    ta.intersection_of(on->uncommitted, alloc);
    cout << " ta " << ta << endl;
    assert(alloc == ta);

    interval_set<block_t> tb;
    tb.intersection_of(on->uncommitted, old);
    cout << " tb " << tb << endl;
    assert(old == tb);
  }

  dirty_onode(on);

  // allocate the space
  for (map<block_t,block_t>::iterator i = alloc.m.begin();
       i != alloc.m.end();
       i++) {
    dout(15) << "alloc_write alloc " << i->first << "~" << i->second << " (of " << start << "~" << len << ")" << endl;

    // allocate new space
    block_t left = i->second;
    block_t cur = i->first;
    while (left > 0) {
      Extent ex;
      allocator.allocate(ex, left, Allocator::NEAR_LAST_FWD);
      dout(10) << "alloc_write got " << ex << " for object offset " << cur << endl;
      on->set_extent(cur, ex);      // map object to new region
      left -= ex.length;
      cur += ex.length;
    }
  }
}




void Ebofs::apply_write(Onode *on, off_t off, size_t len, bufferlist& bl)
{
  ObjectCache *oc = on->get_oc(&bc);

  // map into blocks
  off_t opos = off;         // byte pos in object
  size_t zleft = 0;         // zeros left to write
  size_t left = len;        // bytes left

  block_t bstart = off / EBOFS_BLOCK_SIZE;

  if (off > on->object_size) {
    zleft = off - on->object_size;
    opos = on->object_size;
    bstart = on->object_size / EBOFS_BLOCK_SIZE;
  }
  if (off+(off_t)len > on->object_size) {
    dout(10) << "apply_write extending size on " << *on << ": " << on->object_size 
             << " -> " << off+len << endl;
    on->object_size = off+len;
    dirty_onode(on);
  }
  if (bl.length() == 0) {
    zleft += len;
    left = 0;
  }
  if (zleft)
    dout(10) << "apply_write zeroing first " << zleft << " bytes of " << *on << endl;

  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  // allocate write on disk.
  interval_set<block_t> alloc;
  block_t old_bfirst = 0;  // zero means not defined here (since we ultimately pass to bh_read)
  block_t old_blast = 0; 
  alloc_write(on, bstart, blen, alloc, old_bfirst, old_blast);
  dout(20) << "apply_write  old_bfirst " << old_bfirst << ", old_blast " << old_blast << endl;

  if (fake_writes) {
    on->uncommitted.clear();   // worst case!
    return;
  }    

  // map b range onto buffer_heads
  map<block_t, BufferHead*> hits;
  oc->map_write(bstart, blen, alloc, hits, super_epoch);
  
  // get current versions
  //version_t lowv, highv;
  //oc->scan_versions(bstart, blen, lowv, highv);
  //highv++;
  version_t highv = ++oc->write_count;
  
  // copy from bl into buffer cache
  unsigned blpos = 0;       // byte pos in input buffer

  // write data into buffers
  for (map<block_t, BufferHead*>::iterator i = hits.begin();
       i != hits.end(); 
       i++) {
    BufferHead *bh = i->second;
    bh->set_version(highv);
    bh->epoch_modified = super_epoch;
    
    // old write in progress?
    if (bh->is_tx()) {      // copy the buffer to avoid munging up in-flight write
      dout(10) << "apply_write tx pending, copying buffer on " << *bh << endl;
      bufferlist temp;
      temp.claim(bh->data);
      //bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data); 
      bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );
      bh->data.copy_in(0, bh->length()*EBOFS_BLOCK_SIZE, temp);
    }

    // need to split off partial?  (partials can only be ONE block)
    if ((bh->is_missing() || bh->is_rx()) && bh->length() > 1) {
      if ((bh->start() == bstart && opos % EBOFS_BLOCK_SIZE != 0)) {
        BufferHead *right = bc.split(bh, bh->start()+1);
        hits[right->start()] = right;
        dout(10) << "apply_write split off left block for partial write; rest is " << *right << endl;
      }
      if ((bh->last() == blast && (len+off) % EBOFS_BLOCK_SIZE != 0) &&
          ((off_t)len+off < on->object_size)) {
        BufferHead *right = bc.split(bh, bh->last());
        hits[right->start()] = right;
        dout(10) << "apply_write split off right block for upcoming partial write; rest is " << *right << endl;
      }
    }

    // partial at head or tail?
    if ((bh->start() == bstart && opos % EBOFS_BLOCK_SIZE != 0) ||   // opos, not off, in case we're zeroing...
        (bh->last() == blast && ((off_t)len+off) % EBOFS_BLOCK_SIZE != 0 && ((off_t)len+off) < on->object_size)) {
      // locate ourselves in bh
      unsigned off_in_bh = opos - bh->start()*EBOFS_BLOCK_SIZE;
      assert(off_in_bh >= 0);
      unsigned len_in_bh = MIN( (off_t)(zleft+left),
                                (off_t)(bh->end()*EBOFS_BLOCK_SIZE)-opos );
      
      if (bh->is_partial() || bh->is_rx() || bh->is_missing()) {
        assert(bh->is_partial() || bh->is_rx() || bh->is_missing());
        assert(bh->length() == 1);

        // add frag to partial
        dout(10) << "apply_write writing into partial " << *bh << ":"
                 << " off_in_bh " << off_in_bh 
                 << " len_in_bh " << len_in_bh
                 << endl;
        unsigned z = MIN( zleft, len_in_bh );
        if (z) {
	  bufferptr zp(z);
	  zp.zero();
          bufferlist zb;
          zb.push_back(zp);
          bh->add_partial(off_in_bh, zb);
           zleft -= z;
          opos += z;
        }

        bufferlist sb;
        sb.substr_of(bl, blpos, len_in_bh-z);  // substr in existing buffer
        bufferlist cp;
        cp.append(sb.c_str(), len_in_bh-z);    // copy the partial bit!
        bh->add_partial(off_in_bh, cp);
        left -= len_in_bh-z;
        blpos += len_in_bh-z;
        opos += len_in_bh-z;

        if (bh->partial_is_complete(on->object_size - bh->start()*EBOFS_BLOCK_SIZE)) {
          dout(10) << "apply_write  completed partial " << *bh << endl;
          //bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);  // new buffers!
	  bh->data.clear();
	  bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );
          bh->data.zero();
          bh->apply_partial();
          bc.mark_dirty(bh);
          bc.bh_write(on, bh);
        } 
        else if (bh->is_rx()) {
          dout(10) << "apply_write  rx -> partial " << *bh << endl;
          assert(bh->length() == 1);
          bc.mark_partial(bh);
          bc.bh_queue_partial_write(on, bh);          // queue the eventual write
        }
        else if (bh->is_missing()) {
          dout(10) << "apply_write  missing -> partial " << *bh << endl;
          assert(bh->length() == 1);
          bc.mark_partial(bh);

          // take care to read from _old_ disk block locations!
          if (bh->start() == bstart)
            bc.bh_read(on, bh, old_bfirst);
          else if (bh->start() == blast)
            bc.bh_read(on, bh, old_blast);
          else assert(0);

          bc.bh_queue_partial_write(on, bh);          // queue the eventual write
        }
        else if (bh->is_partial()) {
          dout(10) << "apply_write  already partial, no need to submit rx on " << *bh << endl;
          if (bh->partial_tx_epoch == super_epoch)
            bc.bh_cancel_partial_write(bh);
          bc.bh_queue_partial_write(on, bh);          // queue the eventual write
        }


      } else {
        assert(bh->is_clean() || bh->is_dirty() || bh->is_tx());
        
        // just write into the bh!
        dout(10) << "apply_write writing leading/tailing partial into " << *bh << ":"
                 << " off_in_bh " << off_in_bh 
                 << " len_in_bh " << len_in_bh
                 << endl;

        // copy data into new buffers first (copy on write!)
        //  FIXME: only do the modified pages?  this might be a big bh!
        bufferlist temp;
        temp.claim(bh->data);
        //bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data); 
	bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );
        bh->data.copy_in(0, bh->length()*EBOFS_BLOCK_SIZE, temp);

        unsigned z = MIN( zleft, len_in_bh );
        if (z) {
	  bufferptr zp(z);
	  zp.zero();
          bufferlist zb;
          zb.push_back(zp);
          bh->data.copy_in(off_in_bh, z, zb);
          zleft -= z;
          opos += z;
        }

        bufferlist sub;
        sub.substr_of(bl, blpos, len_in_bh-z);
        bh->data.copy_in(off_in_bh+z, len_in_bh-z, sub);
        blpos += len_in_bh-z;
        left -= len_in_bh-z;
        opos += len_in_bh-z;

        if (!bh->is_dirty())
          bc.mark_dirty(bh);

        bc.bh_write(on, bh);
      }
      continue;
    }

    // ok, we're talking full block(s) now (modulo last block of the object)
    assert(opos % EBOFS_BLOCK_SIZE == 0);
    assert((off_t)(zleft+left) >= (off_t)(EBOFS_BLOCK_SIZE*bh->length()) ||
           opos+(off_t)(zleft+left) == on->object_size);

    // alloc new buffers.
    //bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);
    bh->data.clear();
    bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );
    
    // copy!
    unsigned len_in_bh = MIN(bh->length()*EBOFS_BLOCK_SIZE, zleft+left);
    assert(len_in_bh <= zleft+left);
    
    dout(10) << "apply_write writing into " << *bh << ":"
             << " len_in_bh " << len_in_bh
             << endl;
    
    unsigned z = MIN(len_in_bh, zleft);
    if (z) {
      bufferptr zp(z);
      zp.zero();
      bufferlist zb;
      zb.push_back(zp);
      bh->data.copy_in(0, z, zb);
      zleft -= z;
    }
    
    bufferlist sub;
    sub.substr_of(bl, blpos, len_in_bh-z);
    bh->data.copy_in(z, len_in_bh-z, sub);

    blpos += len_in_bh-z;
    left -= len_in_bh-z;
    opos += len_in_bh;

    // old partial?
    if (bh->is_partial() &&
        bh->partial_tx_epoch == super_epoch) 
      bc.bh_cancel_partial_write(bh);

    // mark dirty
    if (!bh->is_dirty())
      bc.mark_dirty(bh);

    bc.bh_write(on, bh);
  }

  assert(zleft == 0);
  assert(left == 0);
  assert(opos == off+(off_t)len);
  //assert(blpos == bl.length());
}




// *** file i/o ***

bool Ebofs::attempt_read(Onode *on, off_t off, size_t len, bufferlist& bl, 
                         Cond *will_wait_on, bool *will_wait_on_bool)
{
  dout(10) << "attempt_read " << *on << " " << off << "~" << len << endl;
  ObjectCache *oc = on->get_oc(&bc);

  // map
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  map<block_t, BufferHead*> hits;
  map<block_t, BufferHead*> missing;  // read these
  map<block_t, BufferHead*> rx;       // wait for these
  map<block_t, BufferHead*> partials;  // ??
  oc->map_read(bstart, blen, hits, missing, rx, partials);

  // missing buffers?
  if (!missing.empty()) {
    for (map<block_t,BufferHead*>::iterator i = missing.begin();
         i != missing.end();
         i++) {
      dout(10) << "attempt_read missing buffer " << *(i->second) << endl;
      bc.bh_read(on, i->second);
    }
    BufferHead *wait_on = missing.begin()->second;
    block_t b = MAX(wait_on->start(), bstart);
    wait_on->waitfor_read[b].push_back(new C_Cond(will_wait_on, will_wait_on_bool));
    return false;
  }
  
  // are partials sufficient?
  bool partials_ok = true;
  for (map<block_t,BufferHead*>::iterator i = partials.begin();
       i != partials.end();
       i++) {
    BufferHead *bh = i->second;
    off_t bhstart = (off_t)(bh->start()*EBOFS_BLOCK_SIZE);
    off_t bhend = (off_t)(bh->end()*EBOFS_BLOCK_SIZE);
    off_t start = MAX( off, bhstart );
    off_t end = MIN( off+(off_t)len, bhend );
    
    if (!i->second->have_partial_range(start-bhstart, end-bhend)) {
      if (partials_ok) {
        // wait on this one
        Context *c = new C_Cond(will_wait_on, will_wait_on_bool);
        dout(10) << "attempt_read insufficient partial buffer " << *(i->second) << " c " << c << endl;
        i->second->waitfor_read[i->second->start()].push_back(c);
      }
      partials_ok = false;
    }
  }
  if (!partials_ok) return false;

  // wait on rx?
  if (!rx.empty()) {
    BufferHead *wait_on = rx.begin()->second;
    Context *c = new C_Cond(will_wait_on, will_wait_on_bool);
    dout(1) << "attempt_read waiting for read to finish on " << *wait_on << " c " << c << endl;
    block_t b = MAX(wait_on->start(), bstart);
    wait_on->waitfor_read[b].push_back(c);
    return false;
  }

  // yay, we have it all!
  // concurrently walk thru hits, partials.
  map<block_t,BufferHead*>::iterator h = hits.begin();
  map<block_t,BufferHead*>::iterator p = partials.begin();

  bl.clear();
  off_t pos = off;
  block_t curblock = bstart;
  while (curblock <= blast) {
    BufferHead *bh = 0;
    if (h->first == curblock) {
      bh = h->second;
      h++;
    } else if (p->first == curblock) {
      bh = p->second;
      p++;
    } else assert(0);
    
    off_t bhstart = (off_t)(bh->start()*EBOFS_BLOCK_SIZE);
    off_t bhend = (off_t)(bh->end()*EBOFS_BLOCK_SIZE);
    off_t start = MAX( pos, bhstart );
    off_t end = MIN( off+(off_t)len, bhend );

    if (bh->is_partial()) {
      // copy from a partial block.  yuck!
      bufferlist frag;
      bh->copy_partial_substr( start-bhstart, end-bhstart, frag );
      bl.claim_append( frag );
      pos += frag.length();
    } else {
      // copy from a full block.
      if (bhstart == start && bhend == end) {
        bl.append( bh->data );
        pos += bh->data.length();
      } else {
        bufferlist frag;
        dout(10) << "substr " << (start-bhstart) << "~" << (end-start) << " of " << bh->data.length() << " in " << *bh << endl;
        frag.substr_of(bh->data, start-bhstart, end-start);
        pos += frag.length();
        bl.claim_append( frag );
      }
    }

    curblock = bh->end();
    /* this assert is more trouble than it's worth
    assert((off_t)(curblock*EBOFS_BLOCK_SIZE) == pos ||   // should be aligned with next block
           end != bhend ||                                // or we ended midway through bh
           (bh->last() == blast && end == bhend));        // ended last block       ** FIXME WRONG???
    */
  }

  assert(bl.length() == len);
  return true;
}


/*
 * is_cached -- query whether a object extent is in our cache
 * return value of -1 if onode isn't loaded.  otherwise, the number
 * of extents that need to be read (i.e. # of seeks)  
 */
int Ebofs::is_cached(object_t oid, off_t off, size_t len)
{
  ebofs_lock.Lock();
  int r = _is_cached(oid, off, len);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_is_cached(object_t oid, off_t off, size_t len)
{
  if (!have_onode(oid)) {
    dout(7) << "_is_cached " << oid << " " << off << "~" << len << " ... onode  " << endl;
    return -1;  // object dne?
  } 
  Onode *on = get_onode(oid);
  
  if (!on->have_oc()) {  
    // nothing is cached.  return # of extents in file.
    dout(10) << "_is_cached have onode but no object cache, returning extent count" << endl;
    return on->extent_map.size();
  }
  
  // map
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  map<block_t, BufferHead*> hits;
  map<block_t, BufferHead*> missing;  // read these
  map<block_t, BufferHead*> rx;       // wait for these
  map<block_t, BufferHead*> partials;  // ??
  
  int num_missing = on->get_oc(&bc)->try_map_read(bstart, blen);
  dout(7) << "_is_cached try_map_read reports " << num_missing << " missing extents" << endl;
  return num_missing;

  // FIXME: actually, we should calculate if these extents are contiguous.
  // and not using map_read, probably...
  /* hrmpf
  block_t dpos = 0;
  block_t opos = bstart;
  while (opos < blen) {
    if (hits.begin()->first == opos) {
    } else {
      block_t d;
      if (missing.begin()->first == opos) d = missing.begin()->second.
    
  }
  */
}

void Ebofs::trim_from_cache(object_t oid, off_t off, size_t len)
{
  ebofs_lock.Lock();
  _trim_from_cache(oid, off, len);
  ebofs_lock.Unlock();
}

void Ebofs::_trim_from_cache(object_t oid, off_t off, size_t len)
{
  // be careful not to load it if we don't have it
  if (!have_onode(oid)) {
    dout(7) << "_trim_from_cache " << oid << " " << off << "~" << len << " ... onode not in cache  " << endl;
    return; 
  } 
  
  // ok, we have it, get a pointer.
  Onode *on = get_onode(oid);
  
  if (!on->have_oc()) 
    return; // nothing is cached. 

  // map to blocks
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;

  ObjectCache *oc = on->get_oc(&bc);
  oc->touch_bottom(bstart, blast);
  
  return;
}


int Ebofs::read(object_t oid, 
                off_t off, size_t len,
                bufferlist& bl)
{
  ebofs_lock.Lock();
  int r = _read(oid, off, len, bl);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_read(object_t oid, off_t off, size_t len, bufferlist& bl)
{
  dout(7) << "_read " << oid << " " << off << "~" << len << endl;

  Onode *on = get_onode(oid);
  if (!on) {
    dout(7) << "_read " << oid << " " << off << "~" << len << " ... dne " << endl;
    return -ENOENT;  // object dne?
  }

  // read data into bl.  block as necessary.
  Cond cond;

  int r = 0;
  while (1) {
    // check size bound
    if (off >= on->object_size) {
      dout(7) << "_read " << oid << " " << off << "~" << len << " ... off past eof " << on->object_size << endl;
      r = -ESPIPE;   // FIXME better errno?
      break;
    }

    size_t try_len = len ? len:on->object_size;
    size_t will_read = MIN(off+(off_t)try_len, on->object_size) - off;
    
    bool done;
    if (attempt_read(on, off, will_read, bl, &cond, &done))
      break;  // yay
    
    // wait
    while (!done) 
      cond.Wait(ebofs_lock);

    if (on->deleted) {
      dout(7) << "_read " << oid << " " << off << "~" << len << " ... object deleted" << endl;
      r = -ENOENT;
      break;
    }
  }

  put_onode(on);

  trim_bc();

  if (r < 0) return r;   // return error,
  dout(7) << "_read " << oid << " " << off << "~" << len << " ... got " << bl.length() << endl;
  return bl.length();    // or bytes read.
}


bool Ebofs::_write_will_block()
{
  return (bc.get_stat_dirty()+bc.get_stat_tx() > g_conf.ebofs_bc_max_dirty);
}

bool Ebofs::write_will_block()
{
  ebofs_lock.Lock();
  bool b = _write_will_block();
  ebofs_lock.Unlock();
  return b;
}


unsigned Ebofs::apply_transaction(Transaction& t, Context *onsafe)
{
  ebofs_lock.Lock();
  dout(7) << "apply_transaction start (" << t.ops.size() << " ops)" << endl;

  // do ops
  unsigned r = 0;  // bit fields indicate which ops failed.
  int bit = 1;
  for (list<int>::iterator p = t.ops.begin();
       p != t.ops.end();
       p++) {
    switch (*p) {
    case Transaction::OP_READ:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        off_t offset = t.offsets.front(); t.offsets.pop_front();
        size_t len = t.lengths.front(); t.lengths.pop_front();
        bufferlist *pbl = t.pbls.front(); t.pbls.pop_front();
        if (_read(oid, offset, len, *pbl) < 0) {
          dout(7) << "apply_transaction fail on _read" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_STAT:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        struct stat *st = t.psts.front(); t.psts.pop_front();
        if (_stat(oid, st) < 0) {
          dout(7) << "apply_transaction fail on _stat" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_GETATTR:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
        pair<void*,int*> pattrval = t.pattrvals.front(); t.pattrvals.pop_front();
        if ((*(pattrval.second) = _getattr(oid, attrname, pattrval.first, *(pattrval.second))) < 0) {
          dout(7) << "apply_transaction fail on _getattr" << endl;
          r &= bit;
        }        
      }
      break;

    case Transaction::OP_GETATTRS:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        map<string,bufferptr> *pset = t.pattrsets.front(); t.pattrsets.pop_front();
        if (_getattrs(oid, *pset) < 0) {
          dout(7) << "apply_transaction fail on _getattrs" << endl;
          r &= bit;
        }        
      }
      break;


    case Transaction::OP_WRITE:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        off_t offset = t.offsets.front(); t.offsets.pop_front();
        size_t len = t.lengths.front(); t.lengths.pop_front();
        bufferlist bl = t.bls.front(); t.bls.pop_front();
        if (_write(oid, offset, len, bl) < 0) {
          dout(7) << "apply_transaction fail on _write" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        off_t offset = t.offsets.front(); t.offsets.pop_front();
        size_t len = t.lengths.front(); t.lengths.pop_front();
        _trim_from_cache(oid, offset, len);
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        off_t len = t.offsets.front(); t.offsets.pop_front();
        if (_truncate(oid, len) < 0) {
          dout(7) << "apply_transaction fail on _truncate" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_REMOVE:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        if (_remove(oid) < 0) {
          dout(7) << "apply_transaction fail on _remove" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
        //pair<const void*,int> attrval = t.attrvals.front(); t.attrvals.pop_front();
        bufferlist bl;
        bl.claim( t.attrbls.front() );
        t.attrbls.pop_front();
        if (_setattr(oid, attrname, bl.c_str(), bl.length()) < 0) {
          dout(7) << "apply_transaction fail on _setattr" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        map<string,bufferptr> *pattrset = t.pattrsets.front(); t.pattrsets.pop_front();
        if (_setattrs(oid, *pattrset) < 0) {
          dout(7) << "apply_transaction fail on _setattrs" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_RMATTR:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
        if (_rmattr(oid, attrname) < 0) {
          dout(7) << "apply_transaction fail on _rmattr" << endl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_CLONE:
      {
        object_t oid = t.oids.front(); t.oids.pop_front();
        object_t noid = t.oids.front(); t.oids.pop_front();
	if (_clone(oid, noid) < 0) {
	  dout(7) << "apply_transaction fail on _clone" << endl;
	  r &= bit;
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        if (_create_collection(cid) < 0) {
          dout(7) << "apply_transaction fail on _create_collection" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_RMCOLL:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        if (_destroy_collection(cid) < 0) {
          dout(7) << "apply_transaction fail on _destroy_collection" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_ADD:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        object_t oid = t.oids.front(); t.oids.pop_front();
        if (_collection_add(cid, oid) < 0) {
          //dout(7) << "apply_transaction fail on _collection_add" << endl;
          //r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_REMOVE:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        object_t oid = t.oids.front(); t.oids.pop_front();
        if (_collection_remove(cid, oid) < 0) {
          dout(7) << "apply_transaction fail on _collection_remove" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_SETATTR:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
        //pair<const void*,int> attrval = t.attrvals.front(); t.attrvals.pop_front();
        bufferlist bl;
        bl.claim( t.attrbls.front() );
        t.attrbls.pop_front();
        if (_collection_setattr(cid, attrname, bl.c_str(), bl.length()) < 0) {
          //if (_collection_setattr(cid, attrname, attrval.first, attrval.second) < 0) {
          dout(7) << "apply_transaction fail on _collection_setattr" << endl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_RMATTR:
      {
        coll_t cid = t.cids.front(); t.cids.pop_front();
        const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
        if (_collection_rmattr(cid, attrname) < 0) {
          dout(7) << "apply_transaction fail on _collection_rmattr" << endl;
          r &= bit;
        }
      }
      break;
      
    default:
      cerr << "bad op " << *p << endl;
      assert(0);
    }

    bit = bit << 1;
  }
  
  dout(7) << "apply_transaction finish (r = " << r << ")" << endl;
  
  // set up commit waiter
  //if (r == 0) {
  if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  //} else {
  //if (onsafe) delete onsafe;
  //}
  
  ebofs_lock.Unlock();
  return r;
}



int Ebofs::_write(object_t oid, off_t offset, size_t length, bufferlist& bl)
{
  dout(7) << "_write " << oid << " " << offset << "~" << length << endl;
  assert(bl.length() == length);

  // too much unflushed dirty data?  (if so, block!)
  if (_write_will_block()) {
    dout(10) << "_write blocking " 
              << oid << " " << offset << "~" << length 
              << "  bc: " 
              << "size " << bc.get_size() 
              << ", trimmable " << bc.get_trimmable()
              << ", max " << g_conf.ebofs_bc_size
              << "; dirty " << bc.get_stat_dirty()
              << ", tx " << bc.get_stat_tx()
              << ", max dirty " << g_conf.ebofs_bc_max_dirty
              << endl;

    while (_write_will_block()) 
      bc.waitfor_stat();  // waits on ebofs_lock

    dout(10) << "_write unblocked " 
             << oid << " " << offset << "~" << length 
              << "  bc: " 
              << "size " << bc.get_size() 
              << ", trimmable " << bc.get_trimmable()
              << ", max " << g_conf.ebofs_bc_size
              << "; dirty " << bc.get_stat_dirty()
              << ", tx " << bc.get_stat_tx()
              << ", max dirty " << g_conf.ebofs_bc_max_dirty
              << endl;
  }

  // out of space?
  unsigned max = (length+offset) / EBOFS_BLOCK_SIZE + 10;  // very conservative; assumes we have to rewrite
  max += dirty_onodes.size() + dirty_cnodes.size();
  if (max >= free_blocks) {
    dout(1) << "write failing, only " << free_blocks << " blocks free, may need up to " << max << endl;
    return -ENOSPC;
  }
  
  // get|create inode
  Onode *on = get_onode(oid);
  if (!on) on = new_onode(oid);    // new inode!
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  dirty_onode(on);  // dirty onode!
  
  // apply write to buffer cache
  if (length > 0)
    apply_write(on, offset, length, bl);

  // done.
  put_onode(on);
  trim_bc();

  return length;
}


/*int Ebofs::write(object_t oid, 
                 off_t off, size_t len,
                 bufferlist& bl, bool fsync)
{
  // wait?
  if (fsync) {
    // wait for flush.
    Cond cond;
    bool done;
    int flush = 1;    // write never returns positive
    Context *c = new C_Cond(&cond, &done, &flush);
    int r = write(oid, off, len, bl, c);
    if (r < 0) return r;
    
    ebofs_lock.Lock();
    {
      while (!done) 
        cond.Wait(ebofs_lock);
      assert(flush <= 0);
    }
    ebofs_lock.Unlock();
    if (flush < 0) return flush;
    return r;
  } else {
    // don't wait for flush.
    return write(oid, off, len, bl, (Context*)0);
  }
}
*/

int Ebofs::write(object_t oid, 
                 off_t off, size_t len,
                 bufferlist& bl, Context *onsafe)
{
  ebofs_lock.Lock();
  assert(len > 0);

  // go
  int r = _write(oid, off, len, bl);

  // commit waiter
  if (r > 0) {
    assert((size_t)r == len);
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}


int Ebofs::_remove(object_t oid)
{
  dout(7) << "_remove " << oid << endl;

  // get inode
  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  // ok remove it!
  remove_onode(on);

  return 0;
}


int Ebofs::remove(object_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  // do it
  int r = _remove(oid);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_truncate(object_t oid, off_t size)
{
  dout(7) << "_truncate " << oid << " size " << size << endl;

  Onode *on = get_onode(oid);
  if (!on) 
    return -ENOENT;
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  int r = 0;
  if (size > on->object_size) {
    r = -EINVAL;  // whatever
  } 
  else if (size < on->object_size) {
    // change size
    on->object_size = size;
    dirty_onode(on);
    
    // free blocks
    block_t nblocks = 0;
    if (size) nblocks = 1 + (size-1) / EBOFS_BLOCK_SIZE;
    if (on->object_blocks > nblocks) {
      vector<Extent> extra;
      on->truncate_extents(nblocks, extra);
      for (unsigned i=0; i<extra.size(); i++)
        allocator.release(extra[i]);
    }

    // truncate buffer cache
    if (on->oc) {
      on->oc->truncate(on->object_blocks, super_epoch);
      if (on->oc->is_empty())
	on->close_oc();
    }

    // update uncommitted
    interval_set<block_t> uncom;
    if (nblocks > 0) {
      interval_set<block_t> left;
      left.insert(0, nblocks);
      uncom.intersection_of(left, on->uncommitted);
    }
    dout(10) << "uncommitted was " << on->uncommitted << "  now " << uncom << endl;
    on->uncommitted = uncom;

  }
  else {
    assert(size == on->object_size);
  }

  put_onode(on);
  return r;
}


int Ebofs::truncate(object_t oid, off_t size, Context *onsafe)
{
  ebofs_lock.Lock();
  
  int r = _truncate(oid, size);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}



int Ebofs::clone(object_t from, object_t to, Context *onsafe)
{
  ebofs_lock.Lock();
  
  int r = _clone(from, to);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_clone(object_t from, object_t to)
{
  dout(7) << "_clone " << from << " -> " << to << endl;

  if (!g_conf.ebofs_cloneable) 
    return -1;  // no!

  Onode *fon = get_onode(from);
  if (!fon) return -ENOENT;
  Onode *ton = get_onode(to);
  if (ton) {
    put_onode(fon);
    put_onode(ton);
    return -EEXIST;
  }
  ton = new_onode(to); 
  assert(ton);
  
  // copy easy bits
  ton->readonly = true;
  ton->object_size = fon->object_size;
  ton->object_blocks = fon->object_blocks;
  ton->attr = fon->attr;

  // collections
  for (set<coll_t>::iterator p = fon->collections.begin();
       p != fon->collections.end();
       p++)
    _collection_add(*p, to);
  
  // extents
  ton->extent_map = fon->extent_map;
  for (map<block_t, Extent>::iterator p = ton->extent_map.begin();
       p != ton->extent_map.end();
       ++p) {
    allocator.alloc_inc(p->second);
  }

  // clear uncommitted
  fon->uncommitted.clear();

  // muck with ObjectCache
  if (fon->oc) 
    fon->oc->clone_to( ton );
  
  // ok!
  put_onode(ton);
  put_onode(fon);
  return 0;
}




/*
 * pick object revision with rev < specified rev.  
 *  (oid.rev is a noninclusive upper bound.)
 *
 */
int Ebofs::pick_object_revision_lt(object_t& oid)
{
  assert(oid.rev > 0);   // this is only useful for non-zero oid.rev

  int r = -EEXIST;             // return code
  ebofs_lock.Lock();
  {
    object_t orig = oid;
    object_t live = oid;
    live.rev = 0;
    
    if (object_tab->get_num_keys() > 0) {
      Table<object_t, Extent>::Cursor cursor(object_tab);
      
      object_tab->find(oid, cursor);  // this will be just _past_ highest eligible rev
      if (cursor.move_left() > 0) {
	bool firstpass = true;
	while (1) {
	  object_t t = cursor.current().key;
	  if (t.ino != oid.ino || 
	      t.bno != oid.bno)                 // passed to previous object
	    break;
	  if (oid.rev < t.rev) {                // rev < desired.  possible match.
	    r = 0;
	    oid = t;
	    break;
	  }
	  if (firstpass && oid.rev >= t.rev) {  // there is no old rev < desired.  try live.
	    r = 0;
	    oid = live;
	    break;
	  }
	  if (cursor.move_left() <= 0) break;
	  firstpass = false;
	}
      }
    }
    
    dout(8) << "find_object_revision " << orig << " -> " << oid
	    << "  r=" << r << endl;
  }
  ebofs_lock.Unlock();
  return r;
}




bool Ebofs::exists(object_t oid)
{
  ebofs_lock.Lock();
  dout(8) << "exists " << oid << endl;
  bool e = (object_tab->lookup(oid) == 0);
  ebofs_lock.Unlock();
  return e;
}

int Ebofs::stat(object_t oid, struct stat *st)
{
  ebofs_lock.Lock();
  int r = _stat(oid,st);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_stat(object_t oid, struct stat *st)
{
  dout(7) << "_stat " << oid << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  
  // ??
  st->st_size = on->object_size;

  put_onode(on);
  return 0;
}


int Ebofs::_setattr(object_t oid, const char *name, const void *value, size_t size) 
{
  dout(8) << "setattr " << oid << " '" << name << "' len " << size << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  string n(name);
  on->attr[n] = buffer::copy((char*)value, size);
  dirty_onode(on);
  put_onode(on);

  dout(8) << "setattr " << oid << " '" << name << "' len " << size << " success" << endl;

  return 0;
}

int Ebofs::setattr(object_t oid, const char *name, const void *value, size_t size, Context *onsafe)
{
  ebofs_lock.Lock();
  int r = _setattr(oid, name, value, size);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_setattrs(object_t oid, map<string,bufferptr>& attrset)
{
  dout(8) << "setattrs " << oid << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  on->attr = attrset;
  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::setattrs(object_t oid, map<string,bufferptr>& attrset, Context *onsafe)
{
  ebofs_lock.Lock();
  int r = _setattrs(oid, attrset);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::getattr(object_t oid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  int r = _getattr(oid, name, value, size);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_getattr(object_t oid, const char *name, void *value, size_t size)
{
  dout(8) << "_getattr " << oid << " '" << name << "' maxlen " << size << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  string n(name);
  int r = 0;
  if (on->attr.count(n) == 0) {
    dout(10) << "_getattr " << oid << " '" << name << "' dne" << endl;
    r = -1;
  } else {
    r = MIN( on->attr[n].length(), size );
    dout(10) << "_getattr " << oid << " '" << name << "' got len " << r << endl;
    memcpy(value, on->attr[n].c_str(), r );
  }
  put_onode(on);
  return r;
}

int Ebofs::getattrs(object_t oid, map<string,bufferptr> &aset)
{
  ebofs_lock.Lock();
  int r = _getattrs(oid, aset);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_getattrs(object_t oid, map<string,bufferptr> &aset)
{
  dout(8) << "_getattrs " << oid << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  aset = on->attr;
  put_onode(on);
  return 0;
}



int Ebofs::_rmattr(object_t oid, const char *name) 
{
  dout(8) << "_rmattr " << oid << " '" << name << "'" << endl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  string n(name);
  on->attr.erase(n);
  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::rmattr(object_t oid, const char *name, Context *onsafe) 
{
  ebofs_lock.Lock();

  int r = _rmattr(oid, name);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::listattr(object_t oid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(8) << "listattr " << oid << endl;

  Onode *on = get_onode(oid);
  if (!on) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }

  attrs.clear();
  for (map<string,bufferptr>::iterator i = on->attr.begin();
       i != on->attr.end();
       i++) {
    attrs.push_back(i->first);
  }

  put_onode(on);
  ebofs_lock.Unlock();
  return 0;
}



/***************** collections ******************/

int Ebofs::list_collections(list<coll_t>& ls)
{
  ebofs_lock.Lock();
  dout(9) << "list_collections " << endl;

  Table<coll_t, Extent>::Cursor cursor(collection_tab);

  int num = 0;
  if (collection_tab->find(0, cursor) >= 0) {
    while (1) {
      ls.push_back(cursor.current().key);
      num++;
      if (cursor.move_right() <= 0) break;
    }
  }

  ebofs_lock.Unlock();
  return num;
}

int Ebofs::_create_collection(coll_t cid)
{
  dout(9) << "_create_collection " << hex << cid << dec << endl;
  
  if (_collection_exists(cid)) 
    return -EEXIST;

  Cnode *cn = new_cnode(cid);
  put_cnode(cn);
  
  return 0;  
}

int Ebofs::create_collection(coll_t cid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _create_collection(cid);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_destroy_collection(coll_t cid)
{
  dout(9) << "_destroy_collection " << hex << cid << dec << endl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Cnode *cn = get_cnode(cid);
  assert(cn);

  // hose mappings
  list<object_t> objects;
  collection_list(cid, objects);
  for (list<object_t>::iterator i = objects.begin(); 
       i != objects.end();
       i++) {
    co_tab->remove(coll_object_t(cid,*i));

    Onode *on = get_onode(*i);
    if (on) {
      on->collections.erase(cid);
      dirty_onode(on);
      put_onode(on);
    }
  }

  remove_cnode(cn);
  return 0;
}

int Ebofs::destroy_collection(coll_t cid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _destroy_collection(cid);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

bool Ebofs::collection_exists(coll_t cid)
{
  ebofs_lock.Lock();
  dout(10) << "collection_exists " << hex << cid << dec << endl;
  bool r = _collection_exists(cid);
  ebofs_lock.Unlock();
  return r;
}
bool Ebofs::_collection_exists(coll_t cid)
{
  return (collection_tab->lookup(cid) == 0);
}

int Ebofs::_collection_add(coll_t cid, object_t oid)
{
  dout(9) << "_collection_add " << hex << cid << " object " << oid << dec << endl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  
  int r = 0;

  if (on->collections.count(cid) == 0) {
    on->collections.insert(cid);
    dirty_onode(on);
    co_tab->insert(coll_object_t(cid,oid), true);
  } else {
    r = -ENOENT;  // FIXME?  already in collection.
  }
  
  put_onode(on);
  return r;
}

int Ebofs::collection_add(coll_t cid, object_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _collection_add(cid, oid);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::_collection_remove(coll_t cid, object_t oid)
{
  dout(9) << "_collection_remove " << hex << cid << " object " << oid << dec << endl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  int r = 0;

  if (on->collections.count(cid)) {
    on->collections.erase(cid);
    dirty_onode(on);
    co_tab->remove(coll_object_t(cid,oid));
  } else {
    r = -ENOENT;  // FIXME?
  } 
  
  put_onode(on);
  return r;
}

int Ebofs::collection_remove(coll_t cid, object_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _collection_remove(cid, oid);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_list(coll_t cid, list<object_t>& ls)
{
  ebofs_lock.Lock();
  dout(9) << "collection_list " << hex << cid << dec << endl;

  if (!_collection_exists(cid)) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }
  
  Table<coll_object_t, bool>::Cursor cursor(co_tab);

  int num = 0;
  if (co_tab->find(coll_object_t(cid,object_t()), cursor) >= 0) {
    while (1) {
      const coll_t c = cursor.current().key.first;
      const object_t o = cursor.current().key.second;
      if (c != cid) break;   // end!
      dout(10) << "collection_list  " << hex << cid << " includes " << o << dec << endl;
      ls.push_back(o);
      num++;
      if (cursor.move_right() < 0) break;
    }
  }

  ebofs_lock.Unlock();
  return num;
}


int Ebofs::_collection_setattr(coll_t cid, const char *name, const void *value, size_t size)
{
  dout(10) << "_collection_setattr " << hex << cid << dec << " '" << name << "' len " << size << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) return -ENOENT;

  string n(name);
  cn->attr[n] = buffer::copy((char*)value, size);
  dirty_cnode(cn);
  put_cnode(cn);

  return 0;
}

int Ebofs::collection_setattr(coll_t cid, const char *name, const void *value, size_t size, Context *onsafe)
{
  ebofs_lock.Lock();
  dout(10) << "collection_setattr " << hex << cid << dec << " '" << name << "' len " << size << endl;

  int r = _collection_setattr(cid, name, value, size);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_getattr(coll_t cid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  dout(10) << "collection_setattr " << hex << cid << dec << " '" << name << "' maxlen " << size << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }

  string n(name);
  int r;
  if (cn->attr.count(n) == 0) {
    r = -1;
  } else {
    r = MIN( cn->attr[n].length(), size );
    memcpy(value, cn->attr[n].c_str(), r);
  }
  
  put_cnode(cn);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_collection_rmattr(coll_t cid, const char *name) 
{
  dout(10) << "_collection_rmattr " << hex << cid << dec << " '" << name << "'" << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) return -ENOENT;

  string n(name);
  cn->attr.erase(n);

  dirty_cnode(cn);
  put_cnode(cn);

  return 0;
}

int Ebofs::collection_rmattr(coll_t cid, const char *name, Context *onsafe) 
{
  ebofs_lock.Lock();

  int r = _collection_rmattr(cid, name);

  // set up commit waiter
  if (r >= 0) {
    if (onsafe) commit_waiters[super_epoch].push_back(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_listattr(coll_t cid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(10) << "collection_listattr " << hex << cid << dec << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }

  attrs.clear();
  for (map<string,bufferptr>::iterator i = cn->attr.begin();
       i != cn->attr.end();
       i++) {
    attrs.push_back(i->first);
  }

  put_cnode(cn);
  ebofs_lock.Unlock();
  return 0;
}



void Ebofs::_export_freelist(bufferlist& bl)
{
  for (int b=0; b<=EBOFS_NUM_FREE_BUCKETS; b++) {
    Table<block_t,block_t> *tab;
    if (b < EBOFS_NUM_FREE_BUCKETS) {
      tab = free_tab[b];
    } else {
      tab = limbo_tab;
    }
    
    if (tab->get_num_keys() > 0) {
      Table<block_t,block_t>::Cursor cursor(tab);
      assert(tab->find(0, cursor) >= 0);
      while (1) {
        assert(cursor.current().value > 0);
        
        Extent ex(cursor.current().key, cursor.current().value);
        dout(10) << "_export_freelist " << ex << endl;
        bl.append((char*)&ex, sizeof(ex));
        if (cursor.move_right() <= 0) break;
      }
    }
  }
}

void Ebofs::_import_freelist(bufferlist& bl)
{
  // clear
  for (int b=0; b<EBOFS_NUM_FREE_BUCKETS; b++) 
    free_tab[b]->clear();
  limbo_tab->clear();

  // import!
  int num = bl.length() / sizeof(Extent);
  Extent *p = (Extent*)bl.c_str();
  for (int i=0; i<num; i++) {
    dout(10) << "_import_freelist " << p[i] << endl;
    allocator._release_loner(p[i]);
  }
}

void Ebofs::_get_frag_stat(FragmentationStat& st)
{
  ebofs_lock.Lock();

  // free list is easy
  st.total = dev.get_num_blocks();
  st.total_free = get_free_blocks() + get_limbo_blocks();
  st.free_extent_dist.clear();
  st.num_free_extent = 0;
  st.avg_free_extent = 0;
/*
  __uint64_t tfree = 0;
  for (int b=0; b<=EBOFS_NUM_FREE_BUCKETS; b++) {
    Table<block_t,block_t> *tab;
    if (b < EBOFS_NUM_FREE_BUCKETS) {
      tab = free_tab[b];
      dout(30) << "dump bucket " << b << "  " << tab->get_num_keys() << endl;
    } else {
      tab = limbo_tab;
      dout(30) << "dump limbo  " << tab->get_num_keys() << endl;;
    }
    
    if (tab->get_num_keys() > 0) {
      Table<block_t,block_t>::Cursor cursor(tab);
      assert(tab->find(0, cursor) >= 0);
      while (1) {
        assert(cursor.current().value > 0);
        
        block_t l = cursor.current().value;
        tfree += l;
        int b = 0;
        do {
          l = l >> 1;
          b++; 
        } while (l);
        st.free_extent_dist[b]++;
        st.free_extent_dist_sum[b] += cursor.current().value;
        st.num_free_extent++;

        if (cursor.move_right() <= 0) break;
      }
    }
  }
  st.avg_free_extent = tfree / st.num_free_extent;
*/

  // used extents is harder.  :(
  st.num_extent = 0;
  st.avg_extent = 0;
  st.extent_dist.clear();
  st.extent_dist_sum.clear();
  st.avg_extent_per_object = 0;
  st.avg_extent_jump = 0;

  Table<object_t,Extent>::Cursor cursor(object_tab);
  object_tab->find(object_t(), cursor);
  int nobj = 0;
  int njump = 0;
  while (object_tab->get_num_keys() > 0) {
    Onode *on = get_onode(cursor.current().key);
    assert(on);

    nobj++;    
    st.avg_extent_per_object += on->extent_map.size();

    for (map<block_t,Extent>::iterator p = on->extent_map.begin();
         p != on->extent_map.end();
         p++) {
      block_t l = p->second.length;

      st.num_extent++;
      st.avg_extent += l;
      if (p->first > 0) {
        njump++;
        st.avg_extent_jump += l;
      }

      int b = 0;
      do {
        l = l >> 1;
        b++; 
      } while (l);
      st.extent_dist[b]++;
      st.extent_dist_sum[b] += p->second.length;
    }
    put_onode(on);
    if (cursor.move_right() <= 0) break;
  }
  if (njump) st.avg_extent_jump /= njump;
  if (nobj) st.avg_extent_per_object /= (float)nobj;
  if (st.num_extent) st.avg_extent /= st.num_extent;

  ebofs_lock.Unlock();
}
