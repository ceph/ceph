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



#include "Ebofs.h"

#include "os/FileJournal.h"

#include <errno.h>

#ifndef DARWIN
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

// *******************

#define DOUT_SUBSYS ebofs
#undef dout_prefix
#define dout_prefix *_dout << "ebofs(" << dev.get_device_name() << ")."


char *nice_blocks(block_t b) 
{
  static char s[20];
  float sz = b*4.0;
  if (sz > (10 << 20)) 
    snprintf(s, sizeof(s), "%.1f GB", sz / (1024.0*1024.0));
  else if (sz > (10 << 10)) 
    snprintf(s, sizeof(s), "%.1f MB", sz / (1024.0));
  else 
    snprintf(s, sizeof(s), "%llu KB", b*4ULL);
  return s;
}

int Ebofs::mount()
{
  Mutex::Locker locker(ebofs_lock);
  assert(!mounted);

  // open dev
  int r = dev.open(&idle_kicker);
  if (r < 0) return r;

  dout(2) << "mounting " << dev.get_device_name() << " " << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks()) << dendl;

  // read super
  bufferptr bp1 = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  bufferptr bp2 = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  dev.read(0, 1, bp1);
  dev.read(1, 1, bp2);

  struct ebofs_super *sb1 = (struct ebofs_super*)bp1.c_str();
  struct ebofs_super *sb2 = (struct ebofs_super*)bp2.c_str();

  // valid superblocks?
  if (!sb1->is_valid_magic() && !sb2->is_valid_magic()) {
    derr(0) << "mount bad magic, not a valid EBOFS file system" << dendl;
    return -EINVAL;
  }
  if (sb1->is_corrupt() && sb2->is_corrupt()) {
    derr(0) << "mount both superblocks are corrupt (bad csum)" << dendl;
    return -EINVAL;
  }
  if ((sb1->is_valid() && sb1->num_blocks > dev.get_num_blocks()) ||
      (sb2->is_valid() && sb2->num_blocks > dev.get_num_blocks())) {
    derr(0) << "mount superblock size exceeds actual device size" << dendl;
    return -EINVAL;
  }

  dout(3) << "mount super @0 epoch " << sb1->epoch << dendl;
  dout(3) << "mount super @1 epoch " << sb2->epoch << dendl;

  // pick newest super
  struct ebofs_super *sb = 0;
  if (sb1->epoch > sb2->epoch)
    sb = sb1;
  else
    sb = sb2;
  super_epoch = sb->epoch;
  op_seq = sb->op_seq;
  dout(3) << "mount epoch " << super_epoch << " op_seq " << op_seq << dendl;

  super_fsid = sb->fsid;

  free_blocks = sb->free_blocks;
  limbo_blocks = sb->limbo_blocks;

  // init node pools
  dout(3) << "mount nodepool" << dendl;
  nodepool.init( &sb->nodepool );
  nodepool.read_usemap_and_clean_nodes( dev, super_epoch );
  
  // open tables
  dout(3) << "mount opening tables" << dendl;
  object_tab = new Table<pobject_t, ebofs_inode_ptr>( nodepool, sb->object_tab );
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    free_tab[i] = new Table<block_t, block_t>( nodepool, sb->free_tab[i] );
  limbo_tab = new Table<block_t, block_t>( nodepool, sb->limbo_tab );
  alloc_tab = new Table<block_t, pair<block_t,int> >( nodepool, sb->alloc_tab );
  
  collection_tab = new Table<coll_t,ebofs_inode_ptr>( nodepool, sb->collection_tab );
  co_tab = new Table<coll_pobject_t,bool>( nodepool, sb->co_tab );

  verify_tables();

  allocator.release_limbo();

  // open journal
  if (journalfn) {
    journal = new FileJournal(sb->fsid, &finisher, NULL, journalfn, g_conf.journal_dio);
    int err = journal->open(op_seq+1);
    if (err < 0) {
      dout(3) << "mount journal " << journalfn << " open failed" << dendl;
      delete journal;
      journal = 0;
      if (err == -EINVAL) {
	dout(0) << "mount journal appears corrupt/invalid, stopping" << dendl;
	dev.close();
	return -1;
      }
    } else {
      // replay journal
      dout(3) << "mount journal " << journalfn << " opened, replaying" << dendl;
      
      while (1) {
	bufferlist bl;
	uint64_t seq;
	if (!journal->read_entry(bl, seq)) {
	  dout(3) << "mount replay: end of journal, done." << dendl;
	  break;
	}
	
	if (seq <= op_seq) {
	  dout(3) << "mount replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
	  continue;
	}
	op_seq++;
	assert(seq == op_seq);
	
	dout(3) << "mount replay: applying op seq " << seq << dendl;
	Transaction t(bl);
	_apply_transaction(t);
      }
      
      // done reading, make writeable.
      journal->make_writeable();
    }
  }

  dout(3) << "mount starting commit+finisher threads" << dendl;
  commit_thread.create();
  finisher.start();

  dout(1) << "mounted " << dev.get_device_name() << " " << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks())
	  << (journal ? ", with journal":", no journal")
	  << dendl;
  mounted = true;

  return 0;
}


int Ebofs::mkfs()
{
  Mutex::Locker locker(ebofs_lock);
  assert(!mounted);

  int r = dev.open();
  if (r < 0) 
    return r;

  block_t num_blocks = dev.get_num_blocks();

  // make a super-random fsid
  srand48(time(0) ^ getpid());
  super_fsid = ((uint64_t)lrand48() << 32) ^ mrand48();
  srand(time(0) ^ getpid());
  super_fsid ^= rand();
  super_fsid ^= (uint64_t)rand() << 32;

  free_blocks = 0;
  limbo_blocks = 0;

  // create first noderegion
  extent_t nr;
  nr.start = 2;
  nr.length = 20+ (num_blocks / 1000);
  if (nr.length < 10) nr.length = 10;
  nodepool.add_region(nr);
  dout(10) << "mkfs: first node region at " << nr << dendl;

  // allocate two usemaps
  block_t usemap_len = nodepool.get_usemap_len();
  nodepool.usemap_even.start = nr.end();
  nodepool.usemap_even.length = usemap_len;
  nodepool.usemap_odd.start = nodepool.usemap_even.end();
  nodepool.usemap_odd.length = usemap_len;
  dout(10) << "mkfs: even usemap at " << nodepool.usemap_even << dendl;
  dout(10) << "mkfs:  odd usemap at " << nodepool.usemap_odd << dendl;
  nodepool.init_usemap();

  // init tables
  struct ebofs_table empty;
  empty.num_keys = 0;
  empty.root.nodeid = -1;
  empty.root.csum = 0;
  empty.depth = 0;
  
  object_tab = new Table<pobject_t, ebofs_inode_ptr>( nodepool, empty );
  collection_tab = new Table<coll_t, ebofs_inode_ptr>( nodepool, empty );
  
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    free_tab[i] = new Table<block_t,block_t>( nodepool, empty );
  limbo_tab = new Table<block_t,block_t>( nodepool, empty );
  alloc_tab = new Table<block_t,pair<block_t,int> >( nodepool, empty );
  
  co_tab = new Table<coll_pobject_t, bool>( nodepool, empty );

  // add free space
  extent_t left;
  left.start = nodepool.usemap_odd.end();
  left.length = num_blocks - left.start;
  dout(10) << "mkfs: free data blocks at " << left << dendl;
  allocator._release_into_limbo( left );
  if (g_conf.ebofs_cloneable) {
    allocator.alloc_inc(nr);
    allocator.alloc_inc(nodepool.usemap_even);
    allocator.alloc_inc(nodepool.usemap_odd);
  }
  allocator.commit_limbo();   // -> limbo_tab
  allocator.release_limbo();  // -> free_tab

  // write nodes, super, 2x
  dout(10) << "mkfs: flushing nodepool and superblocks (2x)" << dendl;

  for (epoch_t e=0; e<2; e++) {
    nodepool.commit_start(dev, e);
    nodepool.commit_wait();
    bufferptr superbp;
    prepare_super(e, superbp);
    write_super(e, superbp);
  }

  // free memory
  dout(10) << "mkfs: cleaning up" << dendl;
  close_tables();

  dev.close();


  // create journal?
  if (journalfn) {
    Journal *journal = new FileJournal(super_fsid, &finisher, NULL, journalfn, g_conf.journal_dio);
    if (journal->create() < 0) {
      dout(3) << "mount journal " << journalfn << " created failed" << dendl;
    } else {
      dout(3) << "mount journal " << journalfn << " created" << dendl;
    }
    delete journal;
    journal = 0;
  }

  dout(2) << "mkfs: " << dev.get_device_name() << " "  << dev.get_num_blocks() << " blocks, " << nice_blocks(dev.get_num_blocks()) << dendl;
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

void Ebofs::verify_tables()
{
  bool o = g_conf.ebofs_verify;
  g_conf.ebofs_verify = true;

  object_tab->verify("onmount");
  limbo_tab->verify("onmount");
  alloc_tab->verify("onmount");
  collection_tab->verify("onmount");
  co_tab->verify("onmount");
  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++)
    free_tab[i]->verify("onmount");

  g_conf.ebofs_verify = o;
}

int Ebofs::umount()
{
  ebofs_lock.Lock();
  
  // mark unmounting
  dout(2) << "umount start" << dendl;
  readonly = true;
  unmounting = true;
  
  // kick commit thread
  dout(5) << "umount stopping commit thread" << dendl;
  commit_cond.Signal();
  ebofs_lock.Unlock();
  commit_thread.join();
  ebofs_lock.Lock();

  // kick finisher thread
  dout(5) << "umount stopping finisher thread" << dendl;
  finisher.stop();

  // close journal
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }

  trim_bc(0);
  trim_inodes(0);

  for (hash_map<pobject_t,Onode*>::iterator i = onode_map.begin();
       i != onode_map.end();
       i++) {
    dout(0) << "umount *** leftover: " << i->first << "   " << *(i->second) << dendl;
  }

  // free memory
  dout(5) << "umount cleaning up" << dendl;
  close_tables();
  dev.close();
  readonly = unmounting = mounted = false;

  dout(2) << "umount done on " << dev.get_device_name() << dendl;
  ebofs_lock.Unlock();
  return 0;
}



void Ebofs::prepare_super(version_t epoch, bufferptr& bp)
{
  bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
  bp.zero();

  struct ebofs_super *sb = (ebofs_super*)bp.c_str();  // this way it's aligned.
  
  dout(10) << "prepare_super v" << epoch << dendl;

  // fill in super
  sb->s_magic = EBOFS_MAGIC;
  sb->fsid = super_fsid;
  sb->epoch = epoch;
  sb->op_seq = op_seq;
  sb->num_blocks = dev.get_num_blocks();

  sb->free_blocks = free_blocks;
  sb->limbo_blocks = limbo_blocks;

  // tables
  sb->object_tab.num_keys = object_tab->get_num_keys();
  sb->object_tab.root = object_tab->get_root();
  sb->object_tab.depth = object_tab->get_depth();

  for (int i=0; i<EBOFS_NUM_FREE_BUCKETS; i++) {
    sb->free_tab[i].num_keys = free_tab[i]->get_num_keys();
    sb->free_tab[i].root = free_tab[i]->get_root();
    sb->free_tab[i].depth = free_tab[i]->get_depth();
  }
  sb->limbo_tab.num_keys = limbo_tab->get_num_keys();
  sb->limbo_tab.root = limbo_tab->get_root();
  sb->limbo_tab.depth = limbo_tab->get_depth();

  sb->alloc_tab.num_keys = alloc_tab->get_num_keys();
  sb->alloc_tab.root = alloc_tab->get_root();
  sb->alloc_tab.depth = alloc_tab->get_depth();

  sb->collection_tab.num_keys = collection_tab->get_num_keys();
  sb->collection_tab.root = collection_tab->get_root();
  sb->collection_tab.depth = collection_tab->get_depth();

  sb->co_tab.num_keys = co_tab->get_num_keys();
  sb->co_tab.root = co_tab->get_root();
  sb->co_tab.depth = co_tab->get_depth();

  // pools
  sb->nodepool.num_regions = nodepool.region_loc.size();
  for (unsigned i=0; i<nodepool.region_loc.size(); i++) {
    sb->nodepool.region_loc[i] = nodepool.region_loc[i];
  }
  sb->nodepool.node_usemap_even = nodepool.usemap_even;
  sb->nodepool.node_usemap_odd = nodepool.usemap_odd;

  // csum
  sb->super_csum = sb->calc_csum();
  dout(20) << "super csum is " << sb->super_csum << " " << sb->calc_csum() << dendl;
  assert(!sb->is_corrupt());
}

void Ebofs::write_super(version_t epoch, bufferptr& bp)
{
  block_t bno = epoch & 1;
  
  dout(10) << "write_super v" << epoch << " to b" << bno << dendl;

  dev.write(bno, 1, bp, "write_super");
}

int Ebofs::commit_thread_entry()
{  
  ebofs_lock.Lock();
  dout(10) << "commit_thread start" << dendl;

  assert(!commit_thread_started); // there can be only one
  commit_thread_started = true;
  sync_cond.Signal();

  while (mounted) {
    
    // wait for kick, or timeout
    if (g_conf.ebofs_commit_ms) {
      // normal wait+timeout
      dout(20) << "commit_thread sleeping (up to) " << g_conf.ebofs_commit_ms << " ms" << dendl;
      commit_cond.WaitInterval(ebofs_lock, utime_t(0, g_conf.ebofs_commit_ms*1000));   
    } else {
      // DEBUG.. wait until kicked
      dout(10) << "commit_thread no commit_ms, waiting until kicked" << dendl;
      commit_cond.Wait(ebofs_lock);
    }

    if (unmounting) {
      dout(10) << "commit_thread unmounting: final commit pass" << dendl;
      assert(readonly);
      unmounting = false;
      mounted = false;
      dirty = true;
    }
    
    if (!dirty && !limbo_blocks) {
      dout(10) << "commit_thread not dirty - kicking waiters" << dendl;
      finisher.queue(commit_waiters[super_epoch]);
    }
    else {
      // --- wait for partials to finish ---
      commit_starting = true;
      if (bc.get_num_partials() > 0) {
	dout(10) << "commit_thread waiting for " << bc.get_num_partials() << " partials to complete" << dendl;
	dev.barrier();
	bc.waitfor_partials();
	dout(10) << "commit_thread partials completed" << dendl;
      }
      commit_starting = false;

      // --- get ready for a new epoch ---
      uint64_t last_op = op_seq;
      super_epoch++;
      dirty = false;

      derr(10) << "commit_thread commit start, new epoch " << super_epoch << " last_op " << last_op << dendl;
      dout(10) << "commit_thread commit start, new epoch " << super_epoch << " last_op " << last_op << dendl;
      dout(2) << "commit_thread   data: " 
              << 100*(dev.get_num_blocks()-get_free_blocks())/dev.get_num_blocks() << "% used, "
              << get_free_blocks() << " (" << 100*get_free_blocks()/dev.get_num_blocks() 
              << "%) free in " << get_free_extents() 
              << ", " << get_limbo_blocks() << " (" << 100*get_limbo_blocks()/dev.get_num_blocks() 
              << "%) limbo in " << get_limbo_extents() 
              << dendl;
      dout(2) << "commit_thread  nodes: " 
              << 100*nodepool.get_num_used()/nodepool.get_num_total() << "% used, "
              << nodepool.get_num_free() << " (" << 100*nodepool.get_num_free()/nodepool.get_num_total() << "%) free, " 
              << nodepool.get_num_limbo() << " (" << 100*nodepool.get_num_limbo()/nodepool.get_num_total() << "%) limbo, " 
              << nodepool.get_num_total() << " total." << dendl;
      dout(2) << "commit_thread    bc: " 
              << "size " << bc.get_size() 
              << ", trimmable " << bc.get_trimmable()
              << ", max " << g_conf.ebofs_bc_size
              << "; dirty " << bc.get_stat_dirty()
              << ", tx " << bc.get_stat_tx()
              << ", max dirty " << g_conf.ebofs_bc_max_dirty
              << dendl;
      
      bufferptr superbp;
      int attempt = 1;
      while (1) {
	// --- queue up commit writes ---
	bc.poison_commit = false;
	commit_inodes_start();      // do this first; it currently involves inode reallocation
	allocator.commit_limbo();   // limbo -> limbo_tab
	nodepool.commit_start(dev, super_epoch);
	prepare_super(super_epoch, superbp);	// prepare super (before any new changes get made!)
      
	// --- now (try to) flush everything ---
	// (partial writes may fail if read block has a bad csum)
	
	// blockdev barrier (prioritize our writes!)
	dout(30) << "commit_thread barrier.  flushing inodes " << inodes_flushing << dendl;
	dev.barrier();
	
	// wait for it all to flush (drops global lock)
	commit_bc_wait(super_epoch-1);  
	dout(30) << "commit_thread bc flushed" << dendl;
	commit_inodes_wait();
	dout(30) << "commit_thread inodes flushed" << dendl;
	nodepool.commit_wait();
	dout(30) << "commit_thread btree nodes flushed" << dendl;
	
	if (!bc.poison_commit)
	  break;  // ok!

	++attempt;
	dout(1) << "commit_thread commit poisoned, retrying, attempt " << attempt << dendl;
	/* actually, poisoning isn't needed after all.
	 * it's probably a bad idea, but i'll leave it in anyway, 
	 * in case it becomes useful later.  for now,
	 */
	assert(0); // NO!
      }

      // ok, now (synchronously) write the prior super!
      dout(10) << "commit_thread commit flushed, writing super for prior epoch" << dendl;
      ebofs_lock.Unlock();
      write_super(super_epoch, superbp);    
      ebofs_lock.Lock();
      
      dout(10) << "commit_thread wrote super" << dendl;

      // free limbo space now 
      // (since we're done allocating things, 
      //  AND we've flushed all previous epoch data)
      allocator.release_limbo();   // limbo_tab -> free_tabs
      nodepool.commit_finish();
      
      // do we need more node space?
      if (nodepool.get_num_free() < nodepool.get_num_total() / 3) {
        dout(2) << "commit_thread running low on node space, allocating more." << dendl;
        alloc_more_node_space();
      }
      
      // trim journal
      if (journal) journal->committed_thru(last_op);

      // kick waiters
      dout(10) << "commit_thread queueing commit + kicking sync waiters" << dendl;
      finisher.queue(commit_waiters[super_epoch-1]);
      commit_waiters.erase(super_epoch-1);
      sync_cond.Signal();

      dout(10) << "commit_thread commit finish" << dendl;
    }

    // trim bc?
    trim_bc();
    trim_inodes();

  }
  
  dout(10) << "commit_thread finish" << dendl;
  commit_thread_started = false;
  ebofs_lock.Unlock();
  return 0;
}


void Ebofs::alloc_more_node_space()
{
  dout(1) << "alloc_more_node_space free " << nodepool.get_num_free() << "/" << nodepool.get_num_total() << dendl;
  
  if (nodepool.num_regions() < EBOFS_MAX_NODE_REGIONS) {
    int want = nodepool.get_num_total();

    extent_t ex;
    allocator.allocate(ex, want, 2);
    dout(1) << "alloc_more_node_space wants " << want << " more, got " << ex << dendl;

    extent_t even, odd;
    unsigned ulen = nodepool.get_usemap_len(nodepool.get_num_total() + ex.length);
    allocator.allocate(even, ulen, 2);
    allocator.allocate(odd, ulen, 2);
    dout(1) << "alloc_more_node_space maps need " << ulen << " x2, got " << even << " " << odd << dendl;

    if (even.length == ulen && odd.length == ulen) {
      dout(1) << "alloc_more_node_space got " << ex << ", new usemaps at even " << even << " odd " << odd << dendl;
      allocator.release(nodepool.usemap_even);
      allocator.release(nodepool.usemap_odd);
      nodepool.add_region(ex);

      // expand usemap?
      nodepool.usemap_even = even;
      nodepool.usemap_odd = odd;
      nodepool.expand_usemap();
    } else {
      dout (1) << "alloc_more_node_space failed to get space for new usemaps" << dendl;
      allocator.release(ex);
      allocator.release(even);
      allocator.release(odd);
      //assert(0);
    }
  } else {
    dout(1) << "alloc_more_node_space already have max node regions!" << dendl;
    assert(0);
  }
}



// *** onodes ***

Onode* Ebofs::new_onode(pobject_t oid)
{
  Onode* on = new Onode(oid);

  assert(onode_map.count(oid) == 0);
  onode_map[oid] = on;
  onode_lru.lru_insert_top(on);
  
  on->get();
  on->onode_loc.start = 0;
  on->onode_loc.length = 0;

  assert(object_tab->lookup(oid) < 0);
  ebofs_inode_ptr ptr(on->onode_loc, 0);
  object_tab->insert(oid, ptr);  // even tho i'm not placed yet

  dirty_onode(on);

  dout(7) << "new_onode " << *on << dendl;
  return on;
}

Onode* Ebofs::decode_onode(bufferlist& bl, unsigned& off, csum_t csum) 
{
  // verify csum
  struct ebofs_onode *eo = (struct ebofs_onode*)(bl.c_str() + off);
  if (eo->onode_bytes > bl.length() - off) {
    derr(0) << "obviously corrupt onode (bad onode_bytes)" << dendl;
    return 0;
  }
  csum_t actual = calc_csum_unaligned(bl.c_str() + off + sizeof(csum_t),
				      eo->onode_bytes - sizeof(csum_t));
  if (actual != eo->onode_csum) {
    derr(0) << "corrupt onode (bad csum actual " << actual << " != onode's " << eo->onode_csum << ")" << dendl;
    return 0;
  }
  if (actual != csum) {
    derr(0) << "corrupt onode (bad csum actual " << actual << " != expected " << csum << ")" << dendl;
    return 0;
  }
  
  // build onode
  Onode *on = new Onode(eo->object_id);
  on->readonly = eo->readonly;
  on->onode_loc = eo->onode_loc;
  on->object_size = eo->object_size;
  on->alloc_blocks = eo->alloc_blocks;
  on->data_csum = eo->data_csum;
  
  // parse
  char *p = (char*)(eo + 1);

  // parse collection list
  for (int i=0; i<eo->num_collections; i++) {
    coll_t c = *((coll_t*)p);
    p += sizeof(c);
    on->collections.insert(c);
  }
  
  // parse attributes
  for (unsigned i=0; i<eo->num_attr; i++) {
    string key = p;
    p += key.length() + 1;
    int len = *(int*)(p);
    p += sizeof(len);
    on->attr[key] = buffer::copy(p, len);
    p += len;
    dout(15) << "decode_onode " << *on  << " attr " << key << " len " << len << dendl;
  }
  
  // parse extents
  on->extent_map.clear();
  block_t n = 0;
  for (unsigned i=0; i<eo->num_extents; i++) {
    extent_t ex = *((extent_t*)p);
    p += sizeof(extent_t);
    on->extent_map[n].ex = ex;
    if (ex.start) {
      on->extent_map[n].csum.resize(ex.length);
      memcpy(&on->extent_map[n].csum[0], p, sizeof(csum_t)*ex.length);
      p += sizeof(csum_t)*ex.length;
    }
    dout(15) << "decode_onode " << *on  << " ex " << i << ": " << ex << dendl;
    n += ex.length;
  }
  on->last_block = n;
  
  // parse bad byte extents
  for (unsigned i=0; i<eo->num_bad_byte_extents; i++) {
    extent_t ex = *((extent_t*)p);
    p += sizeof(ex);
    on->bad_byte_extents.insert(ex.start, ex.length);
    dout(15) << "decode_onode " << *on << " bad byte ex " << ex << dendl;
  }

  unsigned len = p - (char*)eo;
  assert(len == eo->onode_bytes);
  return on;
}

Onode* Ebofs::get_onode(pobject_t oid)
{
  while (1) {
    // in cache?
    if (have_onode(oid)) {
      // yay
      Onode *on = onode_map[oid];
      on->get();
      //dout(0) << "get_onode " << *on << dendl;
      return on;   
    }
    
    // on disk?
    ebofs_inode_ptr ptr;
    if (object_tab->lookup(oid, ptr) < 0) {
      dout(10) << "onode lookup failed on " << oid << dendl;
      // object dne.
      return 0;
    }
    
    // already loading?
    if (waitfor_onode.count(oid)) {
      // yep, just wait.
      Cond c;
      waitfor_onode[oid].push_back(&c);
      dout(10) << "get_onode " << oid << " already loading, waiting" << dendl;
      c.Wait(ebofs_lock);
      continue;
    }

    dout(10) << "get_onode reading " << oid << " from " << ptr.loc << dendl;

    assert(waitfor_onode.count(oid) == 0);
    waitfor_onode[oid].clear();  // this should be empty initially. 

    // read it!
    bufferlist bl;
    bl.push_back( buffer::create_page_aligned( EBOFS_BLOCK_SIZE*ptr.loc.length ) );

    ebofs_lock.Unlock();
    dev.read( ptr.loc.start, ptr.loc.length, bl );
    ebofs_lock.Lock();

    unsigned off = 0;
    Onode *on = decode_onode(bl, off, ptr.csum);
    if (!on) {
      assert(0); // corrupt!
    }
    assert(on->object_id == oid);
    onode_map[oid] = on;
    onode_lru.lru_insert_top(on);

    // wake up other waiters
    for (list<Cond*>::iterator i = waitfor_onode[oid].begin();
         i != waitfor_onode[oid].end();
         i++)
      (*i)->Signal();
    waitfor_onode.erase(oid);   // remove Cond list
    
    on->get();
    //dout(0) << "get_onode " << *on << " (loaded)" << dendl;
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


csum_t Ebofs::encode_onode(Onode *on, bufferlist& bl, unsigned& off)
{
  unsigned start_off = off;

  // onode
  struct ebofs_onode eo;
  eo.readonly = on->readonly;
  eo.onode_loc = on->onode_loc;
  eo.object_id = on->object_id;
  eo.object_size = on->object_size;
  eo.alloc_blocks = on->alloc_blocks;
  eo.data_csum = on->data_csum;
  eo.inline_bytes = 0;  /* write me */
  eo.num_collections = on->collections.size();
  eo.num_attr = on->attr.size();
  eo.num_extents = on->extent_map.size();
  eo.num_bad_byte_extents = on->bad_byte_extents.m.size();
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
    if (l)
      bl.copy_in(off, l, i->second.c_str());
    off += l;
    dout(15) << "encode_onode " << *on  << " attr " << i->first << " len " << l << dendl;
  }
  
  // extents
  for (map<block_t,ExtentCsum>::iterator i = on->extent_map.begin();
       i != on->extent_map.end();
       i++) {
    ExtentCsum &o = i->second;
    bl.copy_in(off, sizeof(extent_t), (char*)&(o.ex));
    off += sizeof(extent_t);
    if (o.ex.start) {
      bl.copy_in(off, sizeof(csum_t)*o.ex.length, (char*)&o.csum[0]);
      off += sizeof(csum_t)*o.ex.length;
    }
    dout(15) << "encode_onode " << *on  << " ex " << i->first << ": " << o.ex << dendl;
  }

  // bad byte extents
  for (map<uint64_t,uint64_t>::iterator p = on->bad_byte_extents.m.begin();
       p != on->bad_byte_extents.m.end();
       p++) {
    extent_t o = {p->first, p->second};
    bl.copy_in(off, sizeof(o), (char*)&o);
    off += sizeof(o);
    dout(15) << "encode_onode " << *on  << " bad byte ex " << o << dendl;
  }

  eo.onode_bytes = off - start_off;
  bl.copy_in(start_off + sizeof(csum_t), sizeof(__u32), (char*)&eo.onode_bytes);
  eo.onode_csum = calc_csum_unaligned(bl.c_str() + start_off + sizeof(csum_t),
				      eo.onode_bytes - sizeof(csum_t));
  bl.copy_in(start_off, sizeof(csum_t), (char*)&eo);
  dout(15) << "encode_onode len " << eo.onode_bytes << " csum " << eo.onode_csum << dendl;

  return eo.onode_csum;
}

void Ebofs::write_onode(Onode *on)
{
  // buffer
  unsigned bytes = on->get_ondisk_bytes();
  unsigned blocks = DIV_ROUND_UP(bytes, EBOFS_BLOCK_SIZE);

  bufferlist bl;
  bl.push_back(buffer::create_page_aligned(EBOFS_BLOCK_SIZE*blocks));

  // relocate onode
  if (on->onode_loc.length) 
    allocator.release(on->onode_loc);
  block_t first = 0;
  if (on->alloc_blocks)
    first = on->get_first_block();   
  allocator.allocate(on->onode_loc, blocks, first);

  dout(10) << "write_onode " << *on << " to " << on->onode_loc << dendl;

  // encode
  unsigned off = 0;
  csum_t csum = encode_onode(on, bl, off);
  assert(off == bytes);
  if (off < bl.length())
    bl.zero(off, bl.length()-off);

  // update pointer
  object_tab->remove(on->object_id);
  ebofs_inode_ptr ptr(on->onode_loc, csum);
  object_tab->insert(on->object_id, ptr);
  //object_tab->verify();

  // write
  dev.write( on->onode_loc.start, on->onode_loc.length, bl, 
             new C_E_InodeFlush(this), "write_onode" );
}

void Ebofs::remove_onode(Onode *on)
{
  dout(8) << "remove_onode " << *on << dendl;

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
  //dout(0) << "remove_onode on " << *on << dendl;
  object_tab->remove(on->object_id);
  
  // free onode space
  if (on->onode_loc.length)
    allocator.release(on->onode_loc);
  
  // free data space
  for (map<block_t,ExtentCsum>::iterator i = on->extent_map.begin();
       i != on->extent_map.end();
       i++)
    if (i->second.ex.start)
      allocator.release(i->second.ex);
  on->extent_map.clear();

  // remove from collections
  for (set<coll_t>::iterator i = on->collections.begin();
       i != on->collections.end();
       i++) {
    co_tab->remove(coll_pobject_t(*i,on->object_id));
  }
  on->collections.clear();

  // dirty -> clean?
  if (on->is_dirty()) {
    on->mark_clean();         // this unpins *on
    dirty_onodes.erase(on);
  }

  if (on->get_ref_count() > 1) dout(10) << "remove_onode **** will survive " << *on << dendl;
  put_onode(on);

  dirty = true;
}

void Ebofs::put_onode(Onode *on)
{
  on->put();
  //dout(0) << "put_onode " << *on << dendl;
  
  if (on->get_ref_count() == 0 && on->dangling) {
    //dot(0) << " *** hosing on " << *on << dendl;
    delete on;
  }
}

void Ebofs::dirty_onode(Onode *on)
{
  if (!on->is_dirty()) {
    dout(10) << "dirty_onode " << *on << dendl;
    on->mark_dirty();
    dirty_onodes.insert(on);
  } else {
    dout(10) << "dirty_onode " << *on << " (already dirty)" << dendl;
  }
  dirty = true;
}

void Ebofs::trim_inodes(int max)
{
  unsigned omax = onode_lru.lru_get_max();
  unsigned cmax = cnode_lru.lru_get_max();
  if (max >= 0) omax = cmax = max;
  dout(10) << "trim_inodes start " << onode_lru.lru_get_size() << " / " << omax << " onodes, " 
            << cnode_lru.lru_get_size() << " / " << cmax << " cnodes" << dendl;

  // onodes
  while (onode_lru.lru_get_size() > omax) {
    // expire an item
    Onode *on = (Onode*)onode_lru.lru_expire();
    if (on == 0) break;  // nothing to expire
    
    // expire
    dout(20) << "trim_inodes removing onode " << *on << dendl;
    onode_map.erase(on->object_id);
    on->dangling = true;

    if (on->get_ref_count() == 0) {
      assert(on->oc == 0);   // an open oc pins the onode!
      delete on;
    } else {
      dout(-20) << "trim_inodes   still active: " << *on << dendl;
      assert(0); // huh?
    }
  }


  // cnodes
  while (cnode_lru.lru_get_size() > cmax) {
    // expire an item
    Cnode *cn = (Cnode*)cnode_lru.lru_expire();
    if (cn == 0) break;  // nothing to expire

    // expire
    dout(20) << "trim_inodes removing cnode " << *cn << dendl;
    cnode_map.erase(cn->coll_id);
    
    delete cn;
  }

  dout(10) << "trim_inodes finish " 
           << onode_lru.lru_get_size() << " / " << omax << " onodes, " 
           << cnode_lru.lru_get_size() << " / " << cmax << " cnodes" << dendl;
}



// *** cnodes ****

Cnode* Ebofs::new_cnode(coll_t cid)
{
  Cnode* cn = new Cnode(cid);

  assert(cnode_map.count(cid) == 0);
  cnode_map[cid] = cn;
  cnode_lru.lru_insert_top(cn);
  
  cn->get();
  cn->cnode_loc.start = 0;
  cn->cnode_loc.length = 0;

  assert(collection_tab->lookup(cid) < 0);
  ebofs_inode_ptr ptr(cn->cnode_loc, 0);
  collection_tab->insert(cid, ptr);  // even tho i'm not placed yet
  
  dirty_cnode(cn);

  return cn;
}

Cnode* Ebofs::decode_cnode(bufferlist& bl, unsigned& off, csum_t csum) 
{
  // verify csum
  struct ebofs_cnode *ec = (struct ebofs_cnode*)(bl.c_str() + off);
  if (ec->cnode_bytes > bl.length() - off) {
    derr(0) << "obviously corrupt cnode (bad cnode_bytes)" << dendl;
    return 0;
  }
  csum_t actual = calc_csum_unaligned(bl.c_str() + off + sizeof(csum_t),
				      ec->cnode_bytes - sizeof(csum_t));
  if (actual != ec->cnode_csum) {
    derr(0) << "corrupt cnode (bad csum actual " << actual << " != cnode's " << ec->cnode_csum << ")" << dendl;
    return 0;
  }
  if (actual != csum) {
    derr(0) << "corrupt cnode (bad csum actual " << actual << " != expected " << csum << ")" << dendl;
    return 0;
  }

  // build cnode
  Cnode *cn = new Cnode(ec->coll_id);
  cn->cnode_loc = ec->cnode_loc;
  
  // parse attributes
  char *p = (char*)(ec + 1);
  for (unsigned i=0; i<ec->num_attr; i++) {
    string key = p;
    p += key.length() + 1;
    int len = *(int*)(p);
    p += sizeof(len);
    cn->attr[key] = buffer::copy(p, len);
    p += len;
    dout(15) << "get_cnode " << *cn  << " attr " << key << " len " << len << dendl;
  }

  unsigned len = p - (char*)ec;
  assert(len == ec->cnode_bytes);
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
    ebofs_inode_ptr ptr;
    if (collection_tab->lookup(cid, ptr) < 0) {
      // object dne.
      return 0;
    }
    
    // already loading?
    if (waitfor_cnode.count(cid)) {
      // yep, just wait.
      Cond c;
      waitfor_cnode[cid].push_back(&c);
      dout(10) << "get_cnode " << cid << " already loading, waiting" << dendl;
      c.Wait(ebofs_lock);
      continue;
    }

    dout(10) << "get_cnode reading " << cid << " from " << ptr.loc << dendl;

    assert(waitfor_cnode.count(cid) == 0);
    waitfor_cnode[cid].clear();  // this should be empty initially. 

    // read it!
    bufferlist bl;
    //bufferpool.alloc( EBOFS_BLOCK_SIZE*cnode_loc.length, bl );
    bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*ptr.loc.length) );

    ebofs_lock.Unlock();
    dev.read( ptr.loc.start, ptr.loc.length, bl );
    ebofs_lock.Lock();

    unsigned off = 0;
    Cnode *cn = decode_cnode(bl, off, ptr.csum);
    if (!cn) {
      assert(0); // corrupt!
    }
    assert(cn->coll_id == cid);
    cnode_map[cid] = cn;
    cnode_lru.lru_insert_top(cn);

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

csum_t Ebofs::encode_cnode(Cnode *cn, bufferlist& bl, unsigned& off)
{
  unsigned start_off = off;

  // cnode
  struct ebofs_cnode ec;
  ec.cnode_loc = cn->cnode_loc;
  ec.coll_id = cn->coll_id;
  ec.num_attr = cn->attr.size();
  bl.copy_in(off, sizeof(ec), (char*)&ec);
  off += sizeof(ec);
  
  // attr
  for (map<string, bufferptr>::iterator i = cn->attr.begin();
       i != cn->attr.end();
       i++) {
    bl.copy_in(off, i->first.length()+1, i->first.c_str());
    off += i->first.length()+1;
    int len = i->second.length();
    bl.copy_in(off, sizeof(int), (char*)&len);
    off += sizeof(int);
    bl.copy_in(off, len, i->second.c_str());
    off += len;

    dout(15) << "encode_cnode " << *cn  << " attr " << i->first << " len " << len << dendl;
  }

  ec.cnode_bytes = off - start_off;
  bl.copy_in(start_off + sizeof(csum_t), sizeof(__u32), (char*)&ec.cnode_bytes);
  ec.cnode_csum = calc_csum_unaligned(bl.c_str() + start_off + sizeof(csum_t),
				      ec.cnode_bytes - sizeof(csum_t));
  bl.copy_in(start_off, sizeof(csum_t), (char*)&ec);
  dout(15) << "encode_cnode len " << ec.cnode_bytes << " csum " << ec.cnode_csum << dendl;

  return ec.cnode_csum;
}

void Ebofs::write_cnode(Cnode *cn)
{
  // allocate buffer
  unsigned bytes = sizeof(ebofs_cnode) + cn->get_attr_bytes();
  unsigned blocks = DIV_ROUND_UP(bytes, EBOFS_BLOCK_SIZE);
  
  bufferlist bl;
  //bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );
  bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*blocks) );

  // relocate cnode!
  if (cn->cnode_loc.length) 
    allocator.release(cn->cnode_loc);
  allocator.allocate(cn->cnode_loc, blocks, Allocator::NEAR_LAST_FWD);
  
  dout(10) << "write_cnode " << *cn << " to " << cn->cnode_loc
	   << " bufptr " << (void*)bl.c_str() << dendl;

  // encode
  unsigned off = 0;
  csum_t csum = encode_cnode(cn, bl, off);
  assert(off == bytes);
  if (off < bl.length())
    bl.zero(off, bl.length()-off);

  // update pointer
  collection_tab->remove(cn->coll_id);
  ebofs_inode_ptr ptr(cn->cnode_loc, csum);
  collection_tab->insert(cn->coll_id, ptr);

  // write
  dev.write( cn->cnode_loc.start, cn->cnode_loc.length, bl, 
             new C_E_InodeFlush(this), "write_cnode" );
}

void Ebofs::remove_cnode(Cnode *cn)
{
  dout(10) << "remove_cnode " << *cn << dendl;

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
    dout(20) << "flush_inode_finish, " << inodes_flushing << " left" << dendl;
    if (inodes_flushing == 0) 
      inode_commit_cond.Signal();
  }
  ebofs_lock.Unlock();
}

void Ebofs::commit_inodes_start() 
{
  dout(10) << "commit_inodes_start" << dendl;

  assert(inodes_flushing == 0);

  // onodes
  for (set<Onode*>::iterator i = dirty_onodes.begin();
       i != dirty_onodes.end();
       i++) {
    Onode *on = *i;
    inodes_flushing++;
    write_onode(on);
    on->mark_clean();
    on->uncommitted.clear();     // commit any newly allocated blocks
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

  dout(10) << "commit_inodes_start writing " << inodes_flushing << " onodes+cnodes" << dendl;
}

void Ebofs::commit_inodes_wait()
{
  // caller must hold ebofs_lock
  while (inodes_flushing > 0) {
    dout(10) << "commit_inodes_wait waiting for " << inodes_flushing << " onodes+cnodes to flush" << dendl;
    inode_commit_cond.Wait(ebofs_lock);
  }
  dout(10) << "commit_inodes_wait all flushed" << dendl;
}







// *** buffer cache ***

void Ebofs::trim_buffer_cache()
{
  ebofs_lock.Lock();
  trim_bc(0);
  ebofs_lock.Unlock();
}

void Ebofs::trim_bc(int64_t max)
{
  if (max < 0)
    max = g_conf.ebofs_bc_size;
  dout(10) << "trim_bc start: size " << bc.get_size() << ", trimmable " << bc.get_trimmable() << ", max " << max << dendl;

  while (bc.get_size() > (uint64_t)max &&
         bc.get_trimmable()) {
    BufferHead *bh = (BufferHead*) bc.lru_rest.lru_expire();
    if (!bh) break;
    
    dout(25) << "trim_bc trimming " << *bh << dendl;
    assert(bh->is_clean() || bh->is_corrupt());
    
    ObjectCache *oc = bh->oc;
    bc.remove_bh(bh);
    
    if (oc->is_empty()) {
      Onode *on = oc->on;
      dout(10) << "trim_bc  closing oc on " << *on << dendl;
      on->close_oc();
    }
  }

  dout(10) << "trim_bc finish: size " << bc.get_size() << ", trimmable " << bc.get_trimmable() << ", max " << max << dendl;
}


void Ebofs::kick_idle()
{
  dout(10) << "kick_idle" << dendl;
  //commit_cond.Signal();

  ebofs_lock.Lock();
  if (mounted && !unmounting && dirty) {
    dout(10) << "kick_idle dirty, doing commit" << dendl;
    commit_cond.Signal();
  } else {
    dout(10) << "kick_idle !dirty or !mounted or unmounting, doing nothing" << dendl;
  }
  ebofs_lock.Unlock();
}

void Ebofs::sync(Context *onsafe)
{
  ebofs_lock.Lock();
  if (onsafe) {
    dirty = true;

    if (journal) {  
      // journal empty transaction
      Transaction t;
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  }
  ebofs_lock.Unlock();
}

void Ebofs::sync()
{
  ebofs_lock.Lock();
  if (!dirty) {
    dout(7) << "sync in " << super_epoch << ", not dirty" << dendl;
  } else {
    epoch_t start = super_epoch;
    dout(7) << "sync start in " << start << dendl;
    while (super_epoch == start) {
      dout(7) << "sync kicking commit in " << super_epoch << dendl;
      dirty = true;
      commit_cond.Signal();
      sync_cond.Wait(ebofs_lock);
    }
    dout(10) << "sync finish in " << super_epoch << dendl;
  }
  ebofs_lock.Unlock();
}



void Ebofs::commit_bc_wait(version_t epoch)
{
  dout(10) << "commit_bc_wait on epoch " << epoch << dendl;  
  
  while (bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE,epoch) > 0 ||
         bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL,epoch) > 0) {
    //dout(10) << "commit_bc_wait " << bc.get_unflushed(epoch) << " unflushed in epoch " << epoch << dendl;
    dout(10) << "commit_bc_wait epoch " << epoch
              << ", unflushed bhwrite " << bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE) 
              << ", unflushed partial " << bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL) 
              << dendl;
    bc.waitfor_flush();
  }

  bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE).erase(epoch);
  bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL).erase(epoch);

  dout(10) << "commit_bc_wait all flushed for epoch " << epoch
            << "; " << bc.get_unflushed(EBOFS_BC_FLUSH_BHWRITE)
            << " " << bc.get_unflushed(EBOFS_BC_FLUSH_PARTIAL)
            << dendl;  
}



int Ebofs::statfs(struct statfs *buf)
{
  dout(7) << "statfs" << dendl;

  buf->f_type = EBOFS_MAGIC;             /* type of filesystem */
  buf->f_bsize = 4096;                   /* optimal transfer block size */
  buf->f_blocks = dev.get_num_blocks();  /* total data blocks in file system */
  buf->f_bfree = get_free_blocks() 
    + get_limbo_blocks();                /* free blocks in fs */
  buf->f_bavail = get_free_blocks();     /* free blocks avail to non-superuser -- actually, for writing. */
  buf->f_files = nodepool.get_num_total();   /* total file nodes in file system */
  buf->f_ffree = nodepool.get_num_free();    /* free file nodes in fs */
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
                        block_t& old_bfirst, block_t& old_blast,
			csum_t& old_csum_first, csum_t& old_csum_last)
{
  // first decide what pages to (re)allocate 
  alloc.insert(start, len);   // start with whole range

  // figure out what bits are already uncommitted
  interval_set<block_t> already_uncom;
  already_uncom.intersection_of(alloc, on->uncommitted);

  // subtract those off, so we're left with the committed bits (that must be reallocated).
  alloc.subtract(already_uncom);
  
  dout(10) << "alloc_write must (re)alloc " << alloc << " on " << *on << dendl;
  
  // release it (into limbo)
  for (map<block_t,block_t>::iterator i = alloc.m.begin();
       i != alloc.m.end();
       i++) {
    // get old region
    vector<extent_t> old;
    on->map_extents(i->first, i->second, old, 0);
    for (unsigned o=0; o<old.size(); o++) 
      if (old[o].start)
	allocator.release(old[o]);

    // take note if first/last blocks in write range are remapped.. in case we need to do a partial read/write thing
    // these are for partial, so we don't care about TX bh's, so don't worry about bits canceling stuff below.
    if (!old.empty()) {
      if (old[0].start && 
	  i->first == start) { // ..if not a hole..
        old_bfirst = old[0].start;
	old_csum_first = *on->get_extent_csum_ptr(start, 1);
        dout(20) << "alloc_write  old_bfirst " << old_bfirst << " of " << old[0]
		 << " csum " << old_csum_first << dendl;
      }
      if (old[old.size()-1].start && 
	  i->first+i->second == start+len &&
	  start+len <= on->last_block) {
        old_blast = old[old.size()-1].last();
	old_csum_last = *on->get_extent_csum_ptr(start+len-1, 1);
        dout(20) << "alloc_write  old_blast " << old_blast << " of " << old[old.size()-1] 
		 << " csum " << old_csum_last << dendl;
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
        dout(10) << "alloc_write  " << *bh << " already in " << alloc << dendl;
        continue;
      }

      vector<extent_t> old;
      on->map_extents(bh->start(), bh->length(), old, 0);
      assert(old.size() == 1);

      if (bh->start() >= start && bh->end() <= start+len) {
        assert(bh->epoch_modified == super_epoch);
        if (bc.bh_cancel_write(bh, super_epoch)) {
          if (bh->length() == 1)
          dout(10) << "alloc_write unallocated tx " << old[0] << ", canceled " << *bh << dendl;
	  // no, this isn't compatible with clone() and extent reference counting.
          //allocator.unallocate(old[0]);  // release (into free)
	  allocator.release(old[0]);    // **FIXME** no cloning yet, my friend!
          alloc.insert(bh->start(), bh->length());
        } else {
          if (bh->length() == 1)
          dout(10) << "alloc_write released tx " << old[0] << ", couldn't cancel " << *bh << dendl;
          allocator.release(old[0]);     // release (into limbo)
          alloc.insert(bh->start(), bh->length());
        }
      } else {
        if (bh->length() == 1)
        dout(10) << "alloc_write  skipped tx " << old[0] << ", not entirely within " 
                 << start << "~" << len 
                 << " bh " << *bh << dendl;
      }
    }
    
    dout(10) << "alloc_write will (re)alloc " << alloc << " on " << *on << dendl;
  }

  if (alloc.empty()) return;  // no need to dirty the onode below!
  

  // merge alloc into onode uncommitted map
  //dout(10) << " union of " << on->uncommitted << " and " << alloc << dendl;
  interval_set<block_t> old = on->uncommitted;
  on->uncommitted.union_of(alloc);
  
  dout(10) << "alloc_write onode.uncommitted is now " << on->uncommitted << dendl;

  if (0) {
    // verify
    interval_set<block_t> ta;
    ta.intersection_of(on->uncommitted, alloc);
    dout(0) << " ta " << ta << dendl;
    assert(alloc == ta);

    interval_set<block_t> tb;
    tb.intersection_of(on->uncommitted, old);
    dout(0) << " tb " << tb << dendl;
    assert(old == tb);
  }

  dirty_onode(on);

  // allocate the space
  for (map<block_t,block_t>::iterator i = alloc.m.begin();
       i != alloc.m.end();
       i++) {
    dout(15) << "alloc_write alloc " << i->first << "~" << i->second << " (of " << start << "~" << len << ")" << dendl;

    // allocate new space
    block_t left = i->second;
    block_t cur = i->first;
    while (left > 0) {
      extent_t ex;
      allocator.allocate(ex, left, Allocator::NEAR_LAST_FWD);
      dout(10) << "alloc_write got " << ex << " for object offset " << cur << dendl;
      on->set_extent(cur, ex);      // map object to new region
      left -= ex.length;
      cur += ex.length;
    }
  }
}


int Ebofs::check_partial_edges(Onode *on, uint64_t off, uint64_t len, 
			       bool &partial_head, bool &partial_tail)
{
  // partial block overwrite at head or tail?
  uint64_t last_block_byte = on->last_block * EBOFS_BLOCK_SIZE;
  partial_head = (off < last_block_byte) && (off & EBOFS_BLOCK_MASK);
  partial_tail = ((off+len) < on->object_size) && ((off+len) & EBOFS_BLOCK_MASK);
  dout(10) << "check_partial_edges on " << *on << " " << off << "~" << len 
	   << " " << partial_head << "/" << partial_tail << dendl;

  if ((partial_head || partial_tail) && commit_starting) {
    ObjectCache *oc = on->get_oc(&bc);

    // verify that partials don't depend on unread data!
    if (partial_head) {
      block_t bstart = off / EBOFS_BLOCK_SIZE;
      BufferHead *bh = oc->find_bh_containing(bstart);
      if (!bh) {
	dout(10) << "check_partial_edges missing data for partial head, deferring" << dendl;
	return -1;
      }
      if (bh->is_missing() || bh->is_rx()) {
	dout(10) << "check_partial_edges missing data for partial head " << *bh << ", deferring" << dendl;
	return -1;
      }
      if (bh->is_partial()) {
	unsigned off_in_bh = off & EBOFS_BLOCK_MASK;
	unsigned end_in_bh = MAX(EBOFS_BLOCK_SIZE, off_in_bh+len);
	if (!(off_in_bh == 0 || bh->have_partial_range(0, off_in_bh)) ||
	    !(end_in_bh == EBOFS_BLOCK_SIZE || bh->have_partial_range(end_in_bh, EBOFS_BLOCK_SIZE-end_in_bh))) {
	  dout(10) << "check_partial_edges can't complete partial head " << *bh << ", deferring" << dendl;
	  return -1;
	}
      }      
    }
    if (partial_tail) {
      block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
      BufferHead *bh = oc->find_bh_containing(blast);
      if (!bh) {
	dout(10) << "check_partial_edges missing data for partial tail, deferring" << dendl;
	return -1;
      } 
      if (bh->is_missing() || bh->is_rx()) {
	dout(10) << "check_partial_edges missing data for partial tail " << *bh << ", deferring" << dendl;
	return -1;
      }
      if (bh->is_partial()) {
	uint64_t off_in_bh = off & EBOFS_BLOCK_MASK;
	uint64_t end_in_bh = MAX(EBOFS_BLOCK_SIZE, off_in_bh+len);
	uint64_t end = EBOFS_BLOCK_SIZE;
	if (bh->end()*EBOFS_BLOCK_SIZE > last_block_byte)
	  end = last_block_byte & EBOFS_BLOCK_MASK;
	if (!(off_in_bh == 0 || bh->have_partial_range(0, off_in_bh)) ||
	    !(end_in_bh >= end || bh->have_partial_range(end_in_bh, end-end_in_bh))) {
	  dout(10) << "check_partial_edges can't complete partial tail " << *bh << ", deferring" << dendl;
	  return -1;
	}
      }      
    }
    dout(10) << "check_partial_edges commit_starting, and partial head|tail, but we can proceed." << dendl;
  }

  return 0;
}

int Ebofs::apply_write(Onode *on, uint64_t off, uint64_t len, const bufferlist& bl)
{
  ObjectCache *oc = on->get_oc(&bc);
  //oc->scrub_csums();

  assert(bl.length() == len);

  // map into blocks
  uint64_t opos = off;        // byte pos in object
  uint64_t left = len;        // bytes left
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;
  
  // check partial edges
  bool partial_head, partial_tail;
  if (check_partial_edges(on, off, len, partial_head, partial_tail) < 0)
    return -1;

  // -- starting changing stuff --

  // extending object?
  uint64_t old_object_size = on->object_size;
  if (off+len > on->object_size) {
    dout(10) << "apply_write extending size on " << *on << ": " << on->object_size 
             << " -> " << off+len << dendl;
    on->object_size = off+len;
  }

  // map block range onto buffer_heads
  map<block_t, BufferHead*> hits;
  oc->map_write(bstart, blen, hits, super_epoch);

  // allocate write on disk.
  interval_set<block_t> alloc;
  block_t old_last_block = on->last_block;
  block_t old_bfirst = 0;  // zero means not defined here (since we ultimately pass to bh_read)
  block_t old_blast = 0; 
  csum_t old_csum_first = 0;
  csum_t old_csum_last = 0;
  alloc_write(on, bstart, blen, alloc, old_bfirst, old_blast, old_csum_first, old_csum_last);
  dout(20) << "apply_write  old_bfirst " << old_bfirst << ", old_blast " << old_blast << dendl;

  if (fake_writes) {
    on->uncommitted.clear();   // worst case!
    return 0;
  }
  
  // get current versions
  version_t highv = ++oc->write_count;
  
  // copy from bl into buffer cache
  list<Context*> finished;
  unsigned blpos = 0;       // byte pos in input buffer
  for (map<block_t, BufferHead*>::iterator i = hits.begin();
       i != hits.end(); 
       i++) {
    BufferHead *bh = i->second;
    bh->set_version(highv);
    bh->epoch_modified = super_epoch;

    // break bh over disk extent boundaries
    vector<extent_t> exv;
    on->map_extents(bh->start(), bh->length(), exv, 0);
    dout(10) << "apply_write bh " << *bh << " maps to " << exv << dendl;
    if (exv.size() > 1) {
      dout(10) << "apply_write breaking interior bh " << *bh << " over extent boundary " 
	       << exv[0] << " " << exv[1] << dendl;
      BufferHead *right = bc.split(bh, bh->start() + exv[0].length);
      hits[right->start()] = right;
    }

    // mark holes 'clean'
    if (bh->start() >= old_last_block) {
      assert(bh->is_missing());
      bc.mark_clean(bh);
      dout(10) << "apply_write treating appended bh as a hole " << *bh << dendl;
    } else {
      if (exv[0].start == 0) {
	assert(bh->is_missing() || bh->is_clean());
	dout(10) << "apply_write marking old hole clean " << *bh << dendl;
	bc.mark_clean(bh);
      }
    }

    // take read waiters
    bh->take_read_waiters(finished);  // this is a bit aggressive, since we kick waiters on partials

    // need to split off partial?  (partials can only be ONE block)
    if ((bh->is_missing() || bh->is_rx()) && bh->length() > 1) {
      if (bh->start() == bstart && partial_head) {
        BufferHead *right = bc.split(bh, bh->start()+1);
        hits[right->start()] = right;
        dout(10) << "apply_write split off left block for partial write; rest is " << *right << dendl;
      }
      if (bh->last() == blast && partial_tail) {
	BufferHead *right = bc.split(bh, bh->last());
        hits[right->start()] = right;
        dout(10) << "apply_write split off right block for upcoming partial write; rest is " << *right << dendl;
      }
    }

    // locate ourselves in bh
    unsigned off_in_bh = opos - bh->start()*EBOFS_BLOCK_SIZE;
    assert(off_in_bh >= 0);

    // partial at head or tail?
    if ((bh->start() == bstart && partial_head) ||
        (bh->last() == blast && partial_tail)) {
      unsigned len_in_bh = MIN( left, 
				(bh->end()*EBOFS_BLOCK_SIZE)-opos );
      
      if (bh->is_partial() || bh->is_rx() || bh->is_missing() || bh->is_corrupt()) {
        assert(bh->length() == 1);

	if (bh->is_corrupt()) {
	  dout(10) << "apply_write  marking non-overwritten bytes bad on corrupt " << *bh << dendl;
	  interval_set<uint64_t> bad;
	  uint64_t bs = bh->start() * EBOFS_BLOCK_SIZE;
	  if (off_in_bh) bad.insert(bs, bs+off_in_bh);
	  if (off_in_bh+len_in_bh < (unsigned)EBOFS_BLOCK_SIZE)
	    bad.insert(bs+off_in_bh+len_in_bh, bs+EBOFS_BLOCK_SIZE-off_in_bh-len_in_bh);
	  dout(10) << "apply_write  marking non-overwritten bytes " << bad << " bad on corrupt " << *bh << dendl;
	  bh->oc->on->bad_byte_extents.union_of(bad);
	  csum_t csum = calc_csum(bh->data.c_str(), bh->data.length());
	  dout(10) << "apply_write  marking corrupt bh csum " << hex << csum << dec << " clean " << *bh << dendl;
	  *on->get_extent_csum_ptr(bh->start(), 1) = csum;
	  on->data_csum += csum;
	  bc.mark_clean(bh);
	} else {
	  // newly realloc? carry old checksum over since we're only partially overwriting
	  if (bh->start() == bstart && alloc.contains(bstart)) {
	    dout(10) << "apply_write  carrying over starting csum " << hex << old_csum_first << dec
		     << " for partial " << *bh << dendl;
	    *on->get_extent_csum_ptr(bh->start(), 1) = old_csum_first;
	    on->data_csum += old_csum_first;
	  } else if (bh->end()-1 == blast && alloc.contains(blast)) {
	    dout(10) << "apply_write  carrying over ending csum " << hex << old_csum_last << dec
		     << " for partial " << *bh << dendl;
	    *on->get_extent_csum_ptr(bh->end()-1, 1) = old_csum_last;
	    on->data_csum += old_csum_last;
	  } 
	}	  

        // add frag to partial
        dout(10) << "apply_write writing into partial " << *bh << ":"
                 << " off_in_bh " << off_in_bh 
                 << " len_in_bh " << len_in_bh
                 << dendl;
        bufferlist sb;
        sb.substr_of(bl, blpos, len_in_bh);  // substr in existing buffer
	sb.rebuild();  // recopy into properly sized buffer, so that we drop references to user buffer
        bh->add_partial(off_in_bh, sb);
        left -= len_in_bh;
        blpos += len_in_bh;
        opos += len_in_bh;

        if (bh->is_partial() &&
	    bh->partial_is_complete(on->object_size - bh->start()*EBOFS_BLOCK_SIZE)) {
          dout(10) << "apply_write  completed partial " << *bh << dendl;
	  bc.bh_cancel_read(bh);           // cancel old rx op, if we can.
	  bh->data.clear();
	  bh->data.push_back(buffer::create_page_aligned(EBOFS_BLOCK_SIZE));
          bh->apply_partial();
          bc.mark_dirty(bh);
          bc.bh_write(on, bh);
        } 
        else if (bh->is_rx()) {
          dout(10) << "apply_write  rx -> partial " << *bh << dendl;
          assert(bh->length() == 1);
          bc.mark_partial(bh);
	  assert(!commit_starting);  // otherwise, but in check_partial_edges
        }
        else if (bh->is_missing() || bh->is_corrupt()) {
          dout(10) << "apply_write  missing -> partial " << *bh << dendl;
          assert(bh->length() == 1);
          bc.mark_partial(bh);
	  assert(!commit_starting);  // otherwise, but in check_partial_edges

          // take care to read from _old_ disk block locations!
          if (bh->start() == bstart)
            bc.bh_read(on, bh, old_bfirst);
          else if (bh->start() == blast)
            bc.bh_read(on, bh, old_blast);
          else assert(0);
        }
        else if (bh->is_partial()) {
          dout(10) << "apply_write  already partial, no need to submit rx on " << *bh << dendl;
        }

      } else {
        assert(bh->is_clean() || bh->is_dirty() || bh->is_tx());
        
        // just write into the bh!
        dout(10) << "apply_write writing leading/tailing partial into " << *bh << ":"
                 << " off_in_bh " << off_in_bh 
                 << " len_in_bh " << len_in_bh
                 << dendl;

	// copy data into new buffers first (copy on write!)
	//  FIXME: only do the modified pages?  this might be a big bh!
	bufferlist oldbl;
	oldbl.claim(bh->data);
	bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );
	if (oldbl.length()) {
	  // had data
	  if (off_in_bh) 
	    bh->data.copy_in(0, off_in_bh, oldbl);
	  if (off_in_bh+len_in_bh < bh->data.length())
	    bh->data.copy_in(off_in_bh+len_in_bh, bh->data.length()-off_in_bh-len_in_bh,
			     oldbl.c_str()+off_in_bh+len_in_bh);
	} else {
	  // was a hole
	  if (off_in_bh) 
	    bh->data.zero(0, off_in_bh);
	  if (off_in_bh+len_in_bh < bh->data.length())
	    bh->data.zero(off_in_bh+len_in_bh, bh->data.length()-off_in_bh-len_in_bh);
	}
	
	// new data
	bufferlist sub;
	sub.substr_of(bl, blpos, len_in_bh);
	bh->data.copy_in(off_in_bh, len_in_bh, sub);

	// update csum
	block_t rbfirst = off_in_bh/EBOFS_BLOCK_SIZE;
	block_t rblast = DIV_ROUND_UP(off_in_bh+len_in_bh, EBOFS_BLOCK_SIZE);
	block_t bnum = rblast-rbfirst;
	csum_t *csum = on->get_extent_csum_ptr(bh->start()+rbfirst, bnum);
	dout(20) << "calc csum for " << rbfirst << "~" << bnum << dendl;
	for (unsigned i=0; i<bnum; i++) {
	  on->data_csum -= csum[i];
	  dout(30) << "old csum for " << (i+rbfirst) << " is " << hex << csum[i] << dec << dendl;
	  csum[i] = calc_csum(&bh->data[i*EBOFS_BLOCK_SIZE], EBOFS_BLOCK_SIZE);
	  dout(30) << "new csum for " << (i+rbfirst) << " is " << hex << csum[i] << dec << dendl;
	  on->data_csum += csum[i];
	  dout(30) << "new data_csum is " << hex << on->data_csum << dec << dendl;
	}

	blpos += len_in_bh;
	left -= len_in_bh;
        opos += len_in_bh;

        if (!bh->is_dirty())
          bc.mark_dirty(bh);

        bc.bh_write(on, bh);
      }
      continue;
    }

    // ok
    //  we're now writing up to a block boundary, or EOF.
    assert(off_in_bh+left >= (uint64_t)(EBOFS_BLOCK_SIZE*bh->length()) ||
           (opos+left) >= on->object_size);

    unsigned len_in_bh = MIN((uint64_t)bh->length()*EBOFS_BLOCK_SIZE - off_in_bh, 
			     left);
    assert(len_in_bh <= left);

    dout(10) << "apply_write writing into " << *bh << ":"
	     << " off_in_bh " << off_in_bh << " len_in_bh " << len_in_bh
	     << dendl;

    // i will write:
    bufferlist sub;
    sub.substr_of(bl, blpos, len_in_bh);

    if (off_in_bh == 0 &&
	sub.is_page_aligned() &&
	sub.is_n_page_sized()) {
      // assume caller isn't going to modify written buffers.
      // just refrence them!
      assert(sub.length() == bh->length()*EBOFS_BLOCK_SIZE);
      dout(10) << "apply_write yippee, written buffer already page aligned" << dendl;
      bh->data.claim(sub);
    } else {
      // alloc new buffer.
      bh->data.clear();
      bh->data.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );

      // zero leader?
      if (off_in_bh &&
	  opos > old_object_size) {
	uint64_t zstart = MAX(0, old_object_size-(uint64_t)bh->start()*EBOFS_BLOCK_SIZE);
	uint64_t zlen = off_in_bh - zstart;
	dout(15) << "apply_write zeroing bh lead over " << zstart << "~" << zlen << dendl;
	bh->data.zero(zstart, zlen);
      }

      // copy data
      bufferlist sub;
      sub.substr_of(bl, blpos, len_in_bh);
      bh->data.copy_in(off_in_bh, len_in_bh, sub);

      // zero the past-eof tail, too, to be tidy.
      if (len_in_bh < bh->data.length()) {
	uint64_t zstart = off_in_bh+len_in_bh;
	uint64_t zlen = bh->data.length()-(off_in_bh+len_in_bh);
	bh->data.zero(zstart, zlen);
	dout(15) << "apply_write zeroing bh tail over " << zstart << "~" << zlen << dendl;
      }
    }

    // fill in csums
    unsigned blocks = DIV_ROUND_UP(off_in_bh+len_in_bh, EBOFS_BLOCK_SIZE);
    csum_t *csum = on->get_extent_csum_ptr(bh->start(), blocks);
    for (unsigned i=0; i<blocks; i++) {
      on->data_csum -= csum[i];
      csum[i] = calc_csum(bh->data.c_str() + i*EBOFS_BLOCK_SIZE, EBOFS_BLOCK_SIZE);
      on->data_csum += csum[i];
    }
    on->verify_extents();

    blpos += len_in_bh;
    left -= len_in_bh;
    opos += len_in_bh;

    // old partial?
    if (bh->is_partial())
      bc.bh_cancel_read(bh);           // cancel rx (if any) too.

    // mark dirty
    if (!bh->is_dirty())
      bc.mark_dirty(bh);

    bc.bh_write(on, bh);
  }

  assert(left == 0);
  assert(opos == off+len);
  assert(blpos == bl.length());

  //  oc->scrub_csums();

  dirty_onode(on);
  finish_contexts(finished);
  return 0;
}


int Ebofs::apply_zero(Onode *on, uint64_t off, size_t len)
{
  dout(10) << "apply_zero " << off << "~" << len << " on " << *on << dendl;

  bool partial_head, partial_tail;
  if (check_partial_edges(on, off, len, partial_head, partial_tail) < 0)
    return -1;

  // zero edges
  // head?
  if (off & EBOFS_BLOCK_MASK) {
    size_t l = EBOFS_BLOCK_SIZE - (off & EBOFS_BLOCK_MASK);
    if (l > len) l = len;
    if (partial_head) {
      bufferptr bp(l);
      bp.zero();
      bufferlist bl;
      bl.push_back(bp);
      int r = apply_write(on, off, bl.length(), bl);
      assert(r == 0);
    }
    off += l;
    len -= l;
  }
  if (len == 0) return 0;  // done!

  // tail?
  if ((off+len) & EBOFS_BLOCK_MASK) {
    int l = (off+len) & EBOFS_BLOCK_MASK;
    bufferptr bp(l);
    bp.zero();
    bufferlist bl;
    bl.push_back(bp);
    int r = apply_write(on, off+len-bl.length(), bp.length(), bl);
    assert(r == 0);
    len -= l;
  }
  if (len == 0) return 0;  // done!

  // map middle onto buffers
  assert(len > 0);
  assert((off & EBOFS_BLOCK_MASK) == 0);
  assert((len & EBOFS_BLOCK_MASK) == 0);
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blen = len / EBOFS_BLOCK_SIZE;
  assert(blen > 0);

  map<block_t,BufferHead*> hits;
  ObjectCache *oc = on->get_oc(&bc);
  oc->map_write(bstart, blen, hits, super_epoch);

  map<block_t,BufferHead*>::iterator p = hits.begin();
  while (p != hits.end()) {
    map<block_t,BufferHead*>::iterator next = p;
    next++;
    BufferHead *bh = p->second;
    oc->discard_bh(bh, super_epoch);
    p = next;
  }

  // free old blocks
  vector<extent_t> old;
  on->map_extents(bstart, blen, old, 0);
  for (unsigned i=0; i<old.size(); i++)
    if (old[i].start)
      allocator.release(old[i]);
  extent_t hole = {0, blen};
  on->set_extent(bstart, hole);
  
  // adjust uncom
  interval_set<block_t> zeroed;
  zeroed.insert(bstart, blen);
  interval_set<block_t> olduncom;
  olduncom.intersection_of(zeroed, on->uncommitted);
  dout(10) << "_zeroed old uncom " << on->uncommitted << " zeroed " << zeroed 
	   << " subtracting " << olduncom << dendl;
  on->uncommitted.subtract(olduncom);
  dout(10) << "_zeroed new uncom " << on->uncommitted << dendl;

  dirty_onode(on);
  return 0;
}



// *** file i/o ***

int Ebofs::attempt_read(Onode *on, uint64_t off, size_t len, bufferlist& bl, 
			Cond *will_wait_on, bool *will_wait_on_bool)
{
  dout(10) << "attempt_read " << *on << " " << off << "~" << len << dendl;
  ObjectCache *oc = on->get_oc(&bc);

  // overlapping bad byte extents?
  if (!on->bad_byte_extents.empty()) {
    if (on->bad_byte_extents.contains(off)) {
      dout(10) << "attempt_read corrupt (bad byte extent) at off " << off << ", returning -EIO" << dendl;
      return -EIO;
    }
    if (on->bad_byte_extents.end() > off) {
      uint64_t bad = on->bad_byte_extents.start_after(off);
      if (bad < off+(uint64_t)len) {
	len = bad-off;
	dout(10) << "attempt_read corrupt (bad byte extent) at " << bad << ", shortening read to " << len << dendl;
      }
    }
  }

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
      dout(10) << "attempt_read missing buffer " << *(i->second) << dendl;
      bc.bh_read(on, i->second);
    }
    BufferHead *wait_on = missing.begin()->second;
    block_t b = MAX(wait_on->start(), bstart);
    wait_on->waitfor_read[b].push_back(new C_Cond(will_wait_on, will_wait_on_bool));
    return 0;
  }
  
  // wait on rx?
  if (!rx.empty()) {
    BufferHead *wait_on = rx.begin()->second;
    Context *c = new C_Cond(will_wait_on, will_wait_on_bool);
    dout(20) << "attempt_read waiting for read to finish on " << *wait_on << " c " << c << dendl;
    block_t b = MAX(wait_on->start(), bstart);
    wait_on->waitfor_read[b].push_back(c);
    return 0;
  }

  // are partials sufficient?
  for (map<block_t,BufferHead*>::iterator i = partials.begin();
       i != partials.end();
       i++) {
    BufferHead *bh = i->second;
    uint64_t bhstart = (uint64_t)(bh->start()*EBOFS_BLOCK_SIZE);
    uint64_t bhend = (uint64_t)(bh->end()*EBOFS_BLOCK_SIZE);
    uint64_t start = MAX( off, bhstart );
    uint64_t end = MIN( off+(uint64_t)len, bhend );
    
    if (!i->second->have_partial_range(start-bhstart, end-bhstart)) {
      // wait on this one
      Context *c = new C_Cond(will_wait_on, will_wait_on_bool);
      dout(10) << "attempt_read insufficient partial buffer " << *(i->second) << " c " << c << dendl;
      i->second->waitfor_read[i->second->start()].push_back(c);
      return 0;
    }
    dout(10) << "attempt_read have partial range " << (start-bhstart) << "~" << (end-bhstart) << " on " << *bh << dendl;
  }

  // yay, we have it all!
  // concurrently walk thru hits, partials, corrupt.
  map<block_t,BufferHead*>::iterator h = hits.begin();
  map<block_t,BufferHead*>::iterator p = partials.begin();

  bl.clear();
  uint64_t pos = off;
  block_t curblock = bstart;
  while (curblock <= blast) {
    BufferHead *bh = 0;
    if (h != hits.end() && h->first == curblock) {
      bh = h->second;
      h++;
    } else if (p != partials.end() && p->first == curblock) {
      bh = p->second;
      p++;
    } else assert(0);
    
    uint64_t bhstart = (uint64_t)(bh->start()*EBOFS_BLOCK_SIZE);
    uint64_t bhend = (uint64_t)(bh->end()*EBOFS_BLOCK_SIZE);
    uint64_t start = MAX( pos, bhstart );
    uint64_t end = MIN( off+(uint64_t)len, bhend );

    if (bh->is_corrupt()) {
      if (bl.length()) {
	dout(10) << "attempt_read corrupt at " << *bh << ", returning short result" << dendl;
	return 1; 
      } else {
	dout(10) << "attempt_read corrupt at " << *bh << ", returning -EIO" << dendl;
	return -EIO;
      }
    } else if (bh->is_partial()) {
      // copy from a partial block.  yuck!
      bufferlist frag;
      dout(10) << "attempt_read copying partial range " << (start-bhstart) << "~" << (end-bhstart) << " on " << *bh << dendl;
      bh->copy_partial_substr( start-bhstart, end-bhstart, frag );
      bl.claim_append( frag );
      pos += frag.length();
    } else {
      // copy from a full block.
      if (bhstart == start && bhend == end) {
	if (bh->data.length()) {
	  dout(10) << "aligned " << (start-bhstart) << "~" << (end-start) << " of " << bh->data.length() << " in " << *bh << dendl;
	  bl.append( bh->data );
	  pos += bh->data.length();
	} else {
	  dout(10) << "aligned " << (start-bhstart) << "~" << (end-start) << " of hole in " << *bh << dendl;
	  bl.append_zero(end-start);
	  pos += end-start;
	}
      } else {
	if (bh->data.length()) {
	  dout(10) << "substr " << (start-bhstart) << "~" << (end-start) << " of " << bh->data.length() << " in " << *bh << dendl;
	  bufferlist frag;
	  frag.substr_of(bh->data, start-bhstart, end-start);
	  pos += frag.length();
	  bl.claim_append( frag );
	} else {
	  dout(10) << "substr " << (start-bhstart) << "~" << (end-start) << " of hole in " << *bh << dendl;
	  bl.append_zero(end-start);
	  pos += end-start;
	}
      }
    }

    curblock = bh->end();
  }

  assert(bl.length() == len);
  return 1;
}


/*
 * is_cached -- query whether a object extent is in our cache
 * return value of -1 if onode isn't loaded.  otherwise, the number
 * of extents that need to be read (i.e. # of seeks)  
 */
int Ebofs::is_cached(coll_t cid, pobject_t oid, uint64_t off, size_t len)
{
  ebofs_lock.Lock();
  int r = _is_cached(oid, off, len);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_is_cached(pobject_t oid, uint64_t off, size_t len)
{
  if (!have_onode(oid)) {
    dout(7) << "_is_cached " << oid << " " << off << "~" << len << " ... onode  " << dendl;
    return -1;  // object dne?
  } 
  Onode *on = get_onode(oid);
  
  if (!on->have_oc()) {  
    // nothing is cached.  return # of extents in file.
    dout(10) << "_is_cached have onode but no object cache, returning extent count" << dendl;
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
  dout(7) << "_is_cached try_map_read reports " << num_missing << " missing extents" << dendl;
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

void Ebofs::trim_from_cache(coll_t cid, pobject_t oid, uint64_t off, size_t len)
{
  ebofs_lock.Lock();
  _trim_from_cache(oid, off, len);
  ebofs_lock.Unlock();
}

void Ebofs::_trim_from_cache(pobject_t oid, uint64_t off, size_t len)
{
  // be careful not to load it if we don't have it
  if (!have_onode(oid)) {
    dout(7) << "_trim_from_cache " << oid << " " << off << "~" << len << " ... onode not in cache  " << dendl;
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


int Ebofs::read(coll_t cid, pobject_t oid, 
                uint64_t off, size_t len,
                bufferlist& bl)
{
  ebofs_lock.Lock();
  int r = _read(oid, off, len, bl);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_read(pobject_t oid, uint64_t off, size_t len, bufferlist& bl)
{
  dout(7) << "_read " << oid << " " << off << "~" << len << dendl;

  Onode *on = get_onode(oid);
  if (!on) {
    dout(7) << "_read " << oid << " " << off << "~" << len << " ... dne " << dendl;
    return -ENOENT;  // object dne?
  }

  // read data into bl.  block as necessary.
  Cond cond;

  int r = 0;
  while (1) {
    // check size bound
    if (off >= on->object_size) {
      dout(7) << "_read " << oid << " " << off << "~" << len << " ... off past eof " << on->object_size << dendl;
      r = 0;
      break;
    }

    size_t try_len = len ? len:on->object_size;
    size_t will_read = MIN(off+(uint64_t)try_len, on->object_size) - off;
    
    bool done;
    r = attempt_read(on, off, will_read, bl, &cond, &done);
    if (r != 0)
      break;
    
    // wait
    while (!done) 
      cond.Wait(ebofs_lock);

    if (on->deleted) {
      dout(7) << "_read " << oid << " " << off << "~" << len << " ... object deleted" << dendl;
      r = -ENOENT;
      break;
    }
  }

  put_onode(on);

  trim_bc();

  if (r < 0) return r;   // return error,
  dout(7) << "_read " << oid << " " << off << "~" << len << " ... got " << bl.length() << dendl;
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


unsigned Ebofs::apply_transaction(Transaction& t, Context *onjournal, Context *ondisk)
{
  ebofs_lock.Lock();
  dout(7) << "apply_transaction start (" << t.get_num_ops() << " ops)" << dendl;

  bufferlist bl;
  if (journal)
    t.encode(bl);

  unsigned r = _apply_transaction(t);

  // journal, wait for commit
  if (r != 0) {
    if (onjournal) {
      delete onjournal;  // kill callback, but still journal below (in case transaction had side effects)
      onjournal = 0;
    }
    if (ondisk) {
      delete ondisk;
      ondisk = 0;
    }
  }

  if (journal) {
    journal->submit_entry(++op_seq, bl, onjournal);
  } else if (onjournal)
    queue_commit_waiter(onjournal);

  if (ondisk)
    queue_commit_waiter(ondisk);

  ebofs_lock.Unlock();
  return r;
}

unsigned Ebofs::_apply_transaction(Transaction& t)
{
  // verify we have enough space
  if (t.disk_space_required() > get_free_blocks()*EBOFS_BLOCK_SIZE) {
    derr(0) << "apply_transaction needs " << t.disk_space_required() << " bytes > "
	    << (get_free_blocks()*EBOFS_BLOCK_SIZE) << " free" << dendl;
    return -ENOSPC;
  }

  // do ops
  unsigned r = 0;  // bit fields indicate which ops failed.
  int bit = 1;
  while (t.have_op()) {
    int op = t.get_op();
    switch (op) {
 
    case Transaction::OP_STARTSYNC:
      dirty = true;
      commit_cond.Signal();
      break;

    case Transaction::OP_TOUCH:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        if (_touch(cid, oid) < 0) {
          dout(7) << "apply_transaction fail on _touch" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_WRITE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        uint64_t offset = t.get_length();
	uint64_t len = t.get_length();
        bufferlist& bl = t.get_bl();
        if (_write(cid, oid, offset, len, bl) < 0) {
          dout(7) << "apply_transaction fail on _write" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_ZERO:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        uint64_t offset = t.get_length();
	uint64_t len = t.get_length();
        if (_zero(cid, oid, offset, len) < 0) {
          dout(7) << "apply_transaction fail on _zero" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        uint64_t offset = t.get_length();
	uint64_t len = t.get_length();
        _trim_from_cache(oid, offset, len);
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        uint64_t offset = t.get_length();
        if (_truncate(cid, oid, offset) < 0) {
          dout(7) << "apply_transaction fail on _truncate" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_REMOVE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        if (_remove(cid, oid) < 0) {
          dout(7) << "apply_transaction fail on _remove" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
	const char *attrname = t.get_attrname();
        bufferlist& bl = t.get_bl();
        if (_setattr(oid, attrname, bl.c_str(), bl.length()) < 0) {
          dout(7) << "apply_transaction fail on _setattr" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_SETATTRS:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        map<string,bufferptr>& attrset = t.get_attrset();
        if (_setattrs(oid, attrset) < 0) {
          dout(7) << "apply_transaction fail on _setattrs" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_RMATTR:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
	const char *attrname = t.get_attrname();
        if (_rmattr(oid, attrname) < 0) {
          dout(7) << "apply_transaction fail on _rmattr" << dendl;
          r &= bit;
        }
      }
      break;

    case Transaction::OP_CLONE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        pobject_t noid = t.get_oid();
	if (_clone(cid, oid, noid) < 0) {
	  dout(7) << "apply_transaction fail on _clone" << dendl;
	  r &= bit;
	}
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        pobject_t noid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	if (_clone_range(cid, oid, noid, off, len) < 0) {
	  dout(7) << "apply_transaction fail on _clone_range" << dendl;
	  r &= bit;
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      {
   	coll_t cid = t.get_cid();
        if (_create_collection(cid) < 0) {
          dout(7) << "apply_transaction fail on _create_collection" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_RMCOLL:
      {
      	coll_t cid = t.get_cid();
        if (_destroy_collection(cid) < 0) {
          dout(7) << "apply_transaction fail on _destroy_collection" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_ADD:
      {
	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        if (_collection_add(cid, oid) < 0) {
          //dout(7) << "apply_transaction fail on _collection_add" << dendl;
          //r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_REMOVE:
      {
  	coll_t cid = t.get_cid();
        pobject_t oid = t.get_oid();
        if (_collection_remove(cid, oid) < 0) {
          dout(7) << "apply_transaction fail on _collection_remove" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_SETATTR:
      {
     	coll_t cid = t.get_cid();
	const char *attrname = t.get_attrname();
        bufferlist& bl = t.get_bl();
        if (_collection_setattr(cid, attrname, bl.c_str(), bl.length()) < 0) {
          //if (_collection_setattr(cid, attrname, attrval.first, attrval.second) < 0) {
          dout(7) << "apply_transaction fail on _collection_setattr" << dendl;
          r &= bit;
        }
      }
      break;
      
    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = t.get_cid();
	const char *attrname = t.get_attrname();
        if (_collection_rmattr(cid, attrname) < 0) {
          dout(7) << "apply_transaction fail on _collection_rmattr" << dendl;
          r &= bit;
        }
      }
      break;
      
    default:
      dout(0) << "bad op " << op << dendl;
      assert(0);
    }

    bit = bit << 1;
  }
  
  dout(7) << "_apply_transaction finish (r = " << r << ")" << dendl;
  return r;
}

int Ebofs::_touch(coll_t cid, pobject_t oid)
{
  dout(7) << "_touch " << oid << dendl;

  // get|create inode
  Onode *on = get_onode(oid);
  if (!on) {
    on = new_onode(oid);    // new inode!
    _collection_add(cid, oid);
    dirty_onode(on);
  }
  put_onode(on);
  return 0;
}


int Ebofs::_write(coll_t cid, pobject_t oid, uint64_t offset, size_t length, const bufferlist& bl)
{
  dout(7) << "_write " << cid << " " << oid << " " << offset << "~" << length << dendl;
  assert(bl.length() == length);

  // get|create inode
  Onode *on = get_onode(oid);
  if (!on) {
    on = new_onode(oid);    // new inode!
    _collection_add(cid, oid);
  }
  
  while (1) {
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
	       << dendl;

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
	       << dendl;
    }
    
    // out of space?
    unsigned max = (length+offset) / EBOFS_BLOCK_SIZE + 10;  // very conservative; assumes we have to rewrite
    max += dirty_onodes.size() + dirty_cnodes.size();
    if (max >= free_blocks) {
      dout(1) << "write failing, only " << free_blocks << " blocks free, may need up to " << max << dendl;
      return -ENOSPC;
    }
    
    if (on->readonly) {
      put_onode(on);
      return -EACCES;
    }

    // apply write to buffer cache
    if (length == 0) {
      dirty_onode(on);
      break;
    } else {
      int r = apply_write(on, offset, length, bl);
      if (r == 0) 
	break; // yay!
      assert(r < 0);
      dout(1) << "write waiting for commit to finish" << dendl;
      sync_cond.Wait(ebofs_lock);
      if (on->deleted) {
	put_onode(on);
	return -ENOENT;
      }
    }
  }

  // done.
  put_onode(on);
  trim_bc();

  return length;
}

int Ebofs::_zero(coll_t cid, pobject_t oid, uint64_t offset, size_t length)
{
  dout(7) << "_zero " << oid << " " << offset << "~" << length << dendl;

  // get|create inode
  Onode *on = get_onode(oid);
  if (!on) {
    on = new_onode(oid);    // new inode!
    _collection_add(cid, oid);
  }
  if (on->readonly) {
    put_onode(on);
    return -EACCES;
  }

  if (length > 0 &&
      offset < on->object_size) {
    if (offset + (uint64_t)length >= on->object_size) {
      _truncate(cid, oid, offset);
    } else {
      while (1) {
	int r = apply_zero(on, offset, length);
	if (r == 0) break;
	assert(r < 0);
	dout(10) << "_zero waiting for commit to finish" << dendl;
	sync_cond.Wait(ebofs_lock);
	if (on->deleted) {
	  put_onode(on);
	  return -ENOENT;
	}
      }
    }
  }

  // done.
  put_onode(on);
  trim_bc();

  return length;
}


int Ebofs::write(coll_t cid, pobject_t oid, 
                 uint64_t off, size_t len,
                 const bufferlist& bl, Context *onsafe)
{
  ebofs_lock.Lock();

  // go
  int r = _write(cid, oid, off, len, bl);

  // commit waiter
  if (r > 0) {
    assert((size_t)r == len);
    if (journal) {
      Transaction t;
      t.write(cid, oid, off, len, bl);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(++op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::zero(coll_t cid, pobject_t oid, uint64_t off, size_t len, Context *onsafe)
{
  ebofs_lock.Lock();
  
  // go
  int r = _zero(cid, oid, off, len);

  // commit waiter
  if (r > 0) {
    assert((size_t)r == len);
    if (journal) {
      Transaction t;
      t.zero(cid, oid, off, len);
      bufferlist tbl;
      t.encode(tbl);
      journal->submit_entry(++op_seq, tbl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}


int Ebofs::_remove(coll_t cid, pobject_t oid)
{
  dout(7) << "_remove " << oid << dendl;

  // get inode
  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  // ok remove it!
  remove_onode(on);

  return 0;
}


int Ebofs::remove(coll_t cid, pobject_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  // do it
  int r = _remove(cid, oid);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.remove(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_truncate(coll_t cid, pobject_t oid, uint64_t size)
{
  dout(7) << "_truncate " << oid << " size " << size << dendl;

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
    if (on->last_block > nblocks) {
      vector<extent_t> extra;
      on->truncate_extents(nblocks, extra);
      for (unsigned i=0; i<extra.size(); i++)
	if (extra[i].start)
	  allocator.release(extra[i]);
    }

    // truncate buffer cache
    if (on->oc) {
      on->oc->truncate(on->last_block, super_epoch);
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
    dout(10) << "uncommitted was " << on->uncommitted << "  now " << uncom << dendl;
    on->uncommitted = uncom;

  }
  else {
    assert(size == on->object_size);
  }

  put_onode(on);
  return r;
}


int Ebofs::truncate(coll_t cid, pobject_t oid, uint64_t size, Context *onsafe)
{
  ebofs_lock.Lock();
  
  int r = _truncate(cid, oid, size);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.truncate(cid, oid, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}



int Ebofs::clone(coll_t cid, pobject_t from, pobject_t to, Context *onsafe)
{
  ebofs_lock.Lock();
  
  int r = _clone(cid, from, to);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.clone(cid, from, to);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_clone(coll_t cid, pobject_t from, pobject_t to)
{
  dout(7) << "_clone " << from << " -> " << to << dendl;

  assert(g_conf.ebofs_cloneable);
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
  _collection_add(cid, to);
  
  // copy easy bits
  ton->readonly = true;
  ton->object_size = fon->object_size;
  ton->alloc_blocks = fon->alloc_blocks;
  ton->last_block = fon->last_block;
  ton->attr = fon->attr;

  // collections
  for (set<coll_t>::iterator p = fon->collections.begin();
       p != fon->collections.end();
       p++)
    _collection_add(*p, to);
  
  // extents
  ton->extent_map = fon->extent_map;
  for (map<block_t, ExtentCsum>::iterator p = ton->extent_map.begin();
       p != ton->extent_map.end();
       ++p) 
    if (p->second.ex.start)
      allocator.alloc_inc(p->second.ex);
  
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


int Ebofs::_clone_range(coll_t cid, pobject_t from, pobject_t to, uint64_t off, uint64_t len)
{
  dout(7) << "_clone_range " << from << " -> " << to << " " << off << "~" << len << dendl;

  // bah.
  bufferlist bl;
  int r = _read(from, off, len, bl);
  if (r < 0)
    return r;
  r = _write(cid, to, off, len, bl);
  return r;
}  


/*
 * pick object revision with rev < specified rev.  
 *  (oid.rev is a noninclusive upper bound.)
 *
 */
int Ebofs::pick_object_revision_lt(coll_t cid, pobject_t& oid)
{
  assert(oid.oid.snap > 0);   // this is only useful for non-zero oid.rev

  int r = -EEXIST;             // return code
  ebofs_lock.Lock();
  {
    pobject_t orig = oid;
    pobject_t live = oid;
    live.oid.snap = 0;
    
    if (object_tab->get_num_keys() > 0) {
      Table<pobject_t, ebofs_inode_ptr>::Cursor cursor(object_tab);
      
      object_tab->find(oid, cursor);  // this will be just _past_ highest eligible rev
      if (cursor.move_left() > 0) {
	bool firstpass = true;
	while (1) {
	  pobject_t t = cursor.current().key;
	  if (t.oid.ino != oid.oid.ino || 
	      t.oid.bno != oid.oid.bno)                 // passed to previous object
	    break;
	  if (oid.oid.snap < t.oid.snap) {                // rev < desired.  possible match.
	    r = 0;
	    oid = t;
	    break;
	  }
	  if (firstpass && oid.oid.snap >= t.oid.snap) {  // there is no old rev < desired.  try live.
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
	    << "  r=" << r << dendl;
  }
  ebofs_lock.Unlock();
  return r;
}




bool Ebofs::exists(coll_t cid, pobject_t oid)
{
  ebofs_lock.Lock();
  dout(8) << "exists " << oid << dendl;
  bool e = (object_tab->lookup(oid) == 0);
  ebofs_lock.Unlock();
  return e;
}

int Ebofs::stat(coll_t cid, pobject_t oid, struct stat *st)
{
  ebofs_lock.Lock();
  int r = _stat(oid,st);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_stat(pobject_t oid, struct stat *st)
{
  dout(7) << "_stat " << oid << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  
  // ??
  st->st_size = on->object_size;

  put_onode(on);
  return 0;
}


int Ebofs::_setattr(pobject_t oid, const char *name, const void *value, size_t size) 
{
  dout(8) << "setattr " << oid << " '" << name << "' len " << size << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  string n(name);
  on->attr[n] = buffer::copy((char*)value, size);
  dirty_onode(on);
  put_onode(on);

  dout(8) << "setattr " << oid << " '" << name << "' len " << size << " success" << dendl;

  return 0;
}

int Ebofs::setattr(coll_t cid, pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe)
{
  ebofs_lock.Lock();
  int r = _setattr(oid, name, value, size);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.setattr(cid, oid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_setattrs(pobject_t oid, map<string,bufferptr>& attrset)
{
  dout(8) << "setattrs " << oid << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  on->attr = attrset;
  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& attrset, Context *onsafe)
{
  ebofs_lock.Lock();
  int r = _setattrs(oid, attrset);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.setattrs(cid, oid, attrset);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}


int Ebofs::get_object_collections(coll_t cid, pobject_t oid, set<coll_t>& ls)
{
  ebofs_lock.Lock();
  int r = _get_object_collections(oid, ls);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_get_object_collections(pobject_t oid, set<coll_t>& ls)
{
  dout(8) << "_get_object_collections " << oid << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  ls = on->collections;
  put_onode(on);
  return 0;
}

int Ebofs::getattr(coll_t cid, pobject_t oid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  int r = _getattr(oid, name, value, size);
  ebofs_lock.Unlock();
  return r;
}
int Ebofs::getattr(coll_t cid, pobject_t oid, const char *name, bufferptr& bp)
{
  ebofs_lock.Lock();
  int r = _getattr(oid, name, bp);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_getattr(pobject_t oid, const char *name, void *value, size_t size)
{
  dout(8) << "_getattr " << oid << " '" << name << "' maxlen " << size << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  string n(name);
  int r = 0;
  if (on->attr.count(n) == 0) {
    dout(10) << "_getattr " << oid << " '" << name << "' dne" << dendl;
    r = -ENODATA;
  } else {
    r = MIN( on->attr[n].length(), size );
    dout(10) << "_getattr " << oid << " '" << name << "' got len " << r << dendl;
    memcpy(value, on->attr[n].c_str(), r );
  }
  put_onode(on);
  return r;
}
int Ebofs::_getattr(pobject_t oid, const char *name, bufferptr& bp)
{
  dout(8) << "_getattr " << oid << " '" << name << "'" << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  string n(name);
  int r = 0;
  if (on->attr.count(n) == 0) {
    dout(10) << "_getattr " << oid << " '" << name << "' dne" << dendl;
    r = -ENODATA;
  } else {
    bp = on->attr[n];
    r = bp.length();
    dout(10) << "_getattr " << oid << " '" << name << "' got len " << r << dendl;
  }
  put_onode(on);
  return r;
}

int Ebofs::getattrs(coll_t cid, pobject_t oid, map<string,bufferptr> &aset)
{
  ebofs_lock.Lock();
  int r = _getattrs(oid, aset);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_getattrs(pobject_t oid, map<string,bufferptr> &aset)
{
  dout(8) << "_getattrs " << oid << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  aset = on->attr;
  put_onode(on);
  return 0;
}



int Ebofs::_rmattr(pobject_t oid, const char *name) 
{
  dout(8) << "_rmattr " << oid << " '" << name << "'" << dendl;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  string n(name);
  on->attr.erase(n);
  dirty_onode(on);
  put_onode(on);
  return 0;
}

int Ebofs::rmattr(coll_t cid, pobject_t oid, const char *name, Context *onsafe) 
{
  ebofs_lock.Lock();

  int r = _rmattr(oid, name);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.rmattr(cid, oid, name);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::listattr(coll_t cid, pobject_t oid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(8) << "listattr " << oid << dendl;

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

int Ebofs::list_collections(vector<coll_t>& ls)
{
  ebofs_lock.Lock();
  dout(9) << "list_collections " << dendl;

  Table<coll_t, ebofs_inode_ptr>::Cursor cursor(collection_tab);

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
  dout(9) << "_create_collection " << hex << cid << dec << dendl;
  
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

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.create_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_destroy_collection(coll_t cid)
{
  dout(9) << "_destroy_collection " << hex << cid << dec << dendl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Cnode *cn = get_cnode(cid);
  assert(cn);

  // hose mappings
  vector<pobject_t> objects;
  _collection_list(cid, objects);
  for (vector<pobject_t>::iterator i = objects.begin(); 
       i != objects.end();
       i++) {
    co_tab->remove(coll_pobject_t(cid,*i));

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

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.remove_collection(cid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return r;
}

bool Ebofs::collection_exists(coll_t cid)
{
  ebofs_lock.Lock();
  dout(10) << "collection_exists " << hex << cid << dec << dendl;
  bool r = _collection_exists(cid);
  dout(10) << "collection_exists " << hex << cid << dec << " = " << r << dendl;
  ebofs_lock.Unlock();
  return r;
}
bool Ebofs::_collection_exists(coll_t cid)
{
  return (collection_tab->lookup(cid) == 0);
}

int Ebofs::_collection_add(coll_t cid, pobject_t oid)
{
  dout(9) << "_collection_add " << hex << cid << " object " << oid << dec << dendl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;
  
  int r = 0;

  if (on->collections.count(cid) == 0) {
    on->collections.insert(cid);
    dirty_onode(on);
    co_tab->insert(coll_pobject_t(cid,oid), true);
  } else {
    r = -ENOENT;  // FIXME?  already in collection.
  }
  
  put_onode(on);
  return r;
}

int Ebofs::collection_add(coll_t cid, coll_t ocid, pobject_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _collection_add(cid, oid);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.collection_add(cid, ocid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::_collection_remove(coll_t cid, pobject_t oid)
{
  dout(9) << "_collection_remove " << hex << cid << " object " << oid << dec << dendl;

  if (!_collection_exists(cid)) 
    return -ENOENT;

  Onode *on = get_onode(oid);
  if (!on) return -ENOENT;

  int r = 0;

  if (on->collections.count(cid)) {
    on->collections.erase(cid);
    dirty_onode(on);
    co_tab->remove(coll_pobject_t(cid,oid));
  } else {
    r = -ENOENT;  // FIXME?
  } 
  
  put_onode(on);
  return r;
}

int Ebofs::collection_remove(coll_t cid, pobject_t oid, Context *onsafe)
{
  ebofs_lock.Lock();

  int r = _collection_remove(cid, oid);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.collection_remove(cid, oid);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}


bool Ebofs::collection_empty(coll_t cid)
{
  ebofs_lock.Lock();

  dout(9) << "collection_empty " << hex << cid << dec << dendl;

  if (!_collection_exists(cid)) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }
  
  Table<coll_pobject_t, bool>::Cursor cursor(co_tab);

  bool empty = true;
  if (co_tab->find(coll_pobject_t(cid,pobject_t()), cursor) >= 0) {
    while (1) {
      const coll_t c = cursor.current().key.first;
      const pobject_t o = cursor.current().key.second;
      if (c != cid) break;   // end!
      empty = false;
      break;
    }
  }

  ebofs_lock.Unlock();
  return empty;
}



int Ebofs::collection_list(coll_t cid, vector<pobject_t>& ls)
{
  ebofs_lock.Lock();
  int num = _collection_list(cid, ls);
  ebofs_lock.Unlock();
  return num;
}

int Ebofs::_collection_list(coll_t cid, vector<pobject_t>& ls)
{
  dout(9) << "collection_list " << hex << cid << dec << dendl;

  if (!_collection_exists(cid)) {
    ebofs_lock.Unlock();
    return -ENOENT;
  }
  
  Table<coll_pobject_t, bool>::Cursor cursor(co_tab);

  int num = 0;
  if (co_tab->find(coll_pobject_t(cid,pobject_t()), cursor) >= 0) {
    while (1) {
      const coll_t c = cursor.current().key.first;
      const pobject_t o = cursor.current().key.second;
      if (c != cid) break;   // end!
      dout(10) << "collection_list  " << hex << cid << " includes " << o << dec << dendl;
      ls.push_back(o);
      num++;
      if (cursor.move_right() < 0) break;
    }
  }

  return num;
}


int Ebofs::_collection_setattr(coll_t cid, const char *name, const void *value, size_t size)
{
  dout(10) << "_collection_setattr " << hex << cid << dec << " '" << name << "' len " << size << dendl;

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
  dout(10) << "collection_setattr " << hex << cid << dec << " '" << name << "' len " << size << dendl;

  int r = _collection_setattr(cid, name, value, size);

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.collection_setattr(cid, name, value, size);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_getattr(coll_t cid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  dout(10) << "collection_setattr " << hex << cid << dec << " '" << name << "' maxlen " << size << dendl;

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

int Ebofs::collection_getattr(coll_t cid, const char *name, bufferlist& bl)
{
  ebofs_lock.Lock();
  dout(10) << "collection_setattr " << hex << cid << dec << " '" << name << dendl;

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
    bl.push_back(cn->attr[n]);
    r = bl.length();
  }
  
  put_cnode(cn);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::collection_getattrs(coll_t cid, map<string,bufferptr> &aset)
{
  ebofs_lock.Lock();
  int r = _collection_getattrs(cid, aset);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_collection_getattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(8) << "_collection_getattrs " << cid << dendl;

  Cnode *cn = get_cnode(cid);
  if (!cn) return -ENOENT;
  aset = cn->attr;
  put_cnode(cn);
  return 0;
}

int Ebofs::collection_setattrs(coll_t cid, map<string,bufferptr> &aset)
{
  ebofs_lock.Lock();
  int r = _collection_setattrs(cid, aset);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::_collection_setattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(8) << "_collection_setattrs " << cid << dendl;
  
  Cnode *cn = get_cnode(cid);
  if (!cn) return -ENOENT;
  cn->attr = aset;
  dirty_cnode(cn);
  put_cnode(cn);
  return 0;
}


int Ebofs::_collection_rmattr(coll_t cid, const char *name) 
{
  dout(10) << "_collection_rmattr " << hex << cid << dec << " '" << name << "'" << dendl;

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

  // journal, wait for commit
  if (r >= 0) {
    if (journal) {
      Transaction t;
      t.collection_rmattr(cid, name);
      bufferlist bl;
      t.encode(bl);
      journal->submit_entry(++op_seq, bl, onsafe);
    } else
      queue_commit_waiter(onsafe);
  } else {
    if (onsafe) delete onsafe;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_listattr(coll_t cid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(10) << "collection_listattr " << hex << cid << dec << dendl;

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
        
        extent_t ex = {cursor.current().key, cursor.current().value};
        dout(10) << "_export_freelist " << ex << dendl;
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
  int num = bl.length() / sizeof(extent_t);
  extent_t *p = (extent_t*)bl.c_str();
  for (int i=0; i<num; i++) {
    dout(10) << "_import_freelist " << p[i] << dendl;
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
  uint64_t tfree = 0;
  for (int b=0; b<=EBOFS_NUM_FREE_BUCKETS; b++) {
    Table<block_t,block_t> *tab;
    if (b < EBOFS_NUM_FREE_BUCKETS) {
      tab = free_tab[b];
      dout(30) << "dump bucket " << b << "  " << tab->get_num_keys() << dendl;
    } else {
      tab = limbo_tab;
      dout(30) << "dump limbo  " << tab->get_num_keys() << dendl;;
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

  Table<pobject_t,ebofs_inode_ptr>::Cursor cursor(object_tab);
  object_tab->find(pobject_t(), cursor);
  int nobj = 0;
  int njump = 0;
  while (object_tab->get_num_keys() > 0) {
    Onode *on = get_onode(cursor.current().key);
    assert(on);

    nobj++;    
    st.avg_extent_per_object += on->extent_map.size();

    for (map<block_t,ExtentCsum>::iterator p = on->extent_map.begin();
         p != on->extent_map.end();
         p++) {
      if (p->second.ex.start == 0) continue; // ignore holes
      block_t l = p->second.ex.length;

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
      st.extent_dist_sum[b] += p->second.ex.length;
    }
    put_onode(on);
    if (cursor.move_right() <= 0) break;
  }
  if (njump) st.avg_extent_jump /= njump;
  if (nobj) st.avg_extent_per_object /= (float)nobj;
  if (st.num_extent) st.avg_extent /= st.num_extent;

  ebofs_lock.Unlock();
}
