
#include "Ebofs.h"

#include <errno.h>
#include <sys/vfs.h>

// *******************

#undef dout
#define dout(x) if (x <= g_conf.debug_ebofs) cout << "ebofs."

int Ebofs::mount()
{
  // note: this will fail in mount -> unmount -> mount type situations, bc
  //       prior state isn't fully cleaned up.

  dout(1) << "mount " << dev.get_device_name() << endl;

  ebofs_lock.Lock();
  assert(!mounted);

  int r = dev.open();
  if (r < 0) {
	ebofs_lock.Unlock();
	return r;
  }

  // read super
  bufferptr bp1 = bufferpool.alloc(EBOFS_BLOCK_SIZE);
  bufferptr bp2 = bufferpool.alloc(EBOFS_BLOCK_SIZE);
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
  
  collection_tab = new Table<coll_t, Extent>( nodepool, sb->collection_tab );
  oc_tab = new Table<idpair_t, bool>( nodepool, sb->oc_tab );
  co_tab = new Table<idpair_t, bool>( nodepool, sb->co_tab );

  allocator.release_limbo();

  dout(3) << "mount starting commit+finisher threads" << endl;
  commit_thread.create();
  finisher_thread.create();

  dout(1) << "mount mounted " << dev.get_device_name() << endl;
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
  nr.length = 10+ (num_blocks / 1000);
  if (nr.length < 10) nr.length = 10;
  nodepool.add_region(nr);
  dout(1) << "mkfs: first node region at " << nr << endl;

  // allocate two usemaps
  block_t usemap_len = ((nr.length-1) / 8 / EBOFS_BLOCK_SIZE) + 1;
  nodepool.usemap_even.start = nr.end();
  nodepool.usemap_even.length = usemap_len;
  nodepool.usemap_odd.start = nodepool.usemap_even.end();
  nodepool.usemap_odd.length = usemap_len;
  dout(1) << "mkfs: even usemap at " << nodepool.usemap_even << endl;
  dout(1) << "mkfs:  odd usemap at " << nodepool.usemap_odd << endl;
  
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
  
  oc_tab = new Table<idpair_t, bool>( nodepool, empty );
  co_tab = new Table<idpair_t, bool>( nodepool, empty );

  // add free space
  Extent left;
  left.start = nodepool.usemap_odd.end();
  left.length = num_blocks - left.start;
  dout(1) << "mkfs: free data blocks at " << left << endl;
  allocator.release( left );
  allocator.commit_limbo();   // -> limbo_tab
  allocator.release_limbo();  // -> free_tab

  // write nodes, super, 2x
  dout(1) << "mkfs: flushing nodepool and superblocks (2x)" << endl;

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
  dout(3) << "mkfs: cleaning up" << endl;
  close_tables();

  dev.close();

  dout(1) << "mkfs: done" << endl;
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
  delete collection_tab;
  delete oc_tab;
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
  dout(2) << "umount stopping commit thread" << endl;
  commit_cond.Signal();
  ebofs_lock.Unlock();
  commit_thread.join();
  ebofs_lock.Lock();

  // kick finisher thread
  dout(2) << "umount stopping finisher thread" << endl;
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
	dout(1) << "umount *** leftover: " << i->first << "   " << *(i->second) << endl;
  }

  // free memory
  dout(2) << "umount cleaning up" << endl;
  close_tables();
  dev.close();

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

  sb.collection_tab.num_keys = collection_tab->get_num_keys();
  sb.collection_tab.root = collection_tab->get_root();
  sb.collection_tab.depth = collection_tab->get_depth();

  sb.oc_tab.num_keys = oc_tab->get_num_keys();
  sb.oc_tab.root = oc_tab->get_root();
  sb.oc_tab.depth = oc_tab->get_depth();

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
  bp = bufferpool.alloc(EBOFS_BLOCK_SIZE);
  memcpy(bp.c_str(), (const char*)&sb, sizeof(sb));
}

void Ebofs::write_super(version_t epoch, bufferptr& bp)
{
  block_t bno = epoch & 1;
  
  dout(10) << "write_super v" << epoch << " to b" << bno << endl;

  dev.write(bno, 1, bp);
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
	if (g_conf.ebofs_commit_interval) {
	  dout(10) << "commit_thread sleeping (up to) " << g_conf.ebofs_commit_interval << " seconds" << endl;
	  commit_cond.WaitInterval(ebofs_lock, utime_t(g_conf.ebofs_commit_interval,0));   
	} else {
	  // DEBUG.. wait until kicked
	  dout(10) << "commit_thread no commit_interval, waiting until kicked" << endl;
	  commit_cond.Wait(ebofs_lock);
	}

	if (unmounting) {
	  dout(10) << "commit_thread unmounting: final commit pass" << endl;
	  assert(readonly);
	  unmounting = false;
	  mounted = false;
	}

	super_epoch++;
	dout(7) << "commit_thread commit start, new epoch " << super_epoch << endl;
	dout(10) << "commit_thread   data: " 
			 << 100*(dev.get_num_blocks()-get_free_blocks())/dev.get_num_blocks() << "% used, "
			 << get_free_blocks() << " (" << 100*get_free_blocks()/dev.get_num_blocks() 
			 << "%) free in " << get_free_extents() 
			 << ", " << get_limbo_blocks() << " (" << 100*get_limbo_blocks()/dev.get_num_blocks() 
			 << "%) limbo in " << get_limbo_extents() 
			 << endl;
	dout(10) << "commit_thread  nodes: " 
			 << 100*nodepool.num_used()/nodepool.num_total() << "% used, "
			 << nodepool.num_free() << " (" << 100*nodepool.num_free()/nodepool.num_total() << "%) free, " 
			 << nodepool.num_limbo() << " (" << 100*nodepool.num_limbo()/nodepool.num_total() << "%) limbo, " 
			 << nodepool.num_total() << " total." << endl;

	// (async) write onodes+condes  (do this first; it currently involves inode reallocation)
	commit_inodes_start();

	allocator.commit_limbo();   // limbo -> limbo_tab

	// (async) write btree nodes
	nodepool.commit_start( dev, super_epoch );
	
	// prepare super (before any changes get made!)
	bufferptr superbp;
	prepare_super(super_epoch, superbp);

	// wait for it all to flush (drops global lock)
	commit_bc_wait(super_epoch-1);
	commit_inodes_wait();
	nodepool.commit_wait();
	
	// ok, now (synchronously) write the prior super!
	dout(10) << "commit_thread commit flushed, writing super for prior epoch" << endl;
	ebofs_lock.Unlock();
	write_super(super_epoch, superbp);	
	ebofs_lock.Lock();

	// free limbo space now 
	// (since we're done allocating things, 
	//  AND we've flushed all previous epoch data)
	allocator.release_limbo();   // limbo_tab -> free_tabs
	
	// do we need more node space?
	if (nodepool.num_free() < nodepool.num_total() / 3) {
	  dout(1) << "commit_thread running low on node space, allocating more." << endl;
	  assert(0);
	  //alloc_more_node_space();
	}

	// kick waiters
	dout(10) << "commit_thread queueing commit + kicking sync waiters" << endl;

	finisher_lock.Lock();
	finisher_queue.splice(finisher_queue.end(), commit_waiters[super_epoch-1]);
	commit_waiters.erase(super_epoch-1);
	finisher_cond.Signal();
	finisher_lock.Unlock();

	sync_cond.Signal();

	// trim bc?
	trim_bc();
	trim_inodes();

	dout(10) << "commit_thread commit finish" << endl;
  }
  
  dout(10) << "commit_thread finish" << endl;
  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::finisher_thread_entry()
{
  finisher_lock.Lock();
  dout(10) << "finisher_thread start" << endl;

  while (!finisher_stop) {
	while (!finisher_queue.empty()) {
	  list<Context*> ls;
	  ls.swap(finisher_queue);

	  finisher_lock.Unlock();
	  finish_contexts(ls, 0);
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
	if (onode_map.count(oid)) {
	  // yay
	  Onode *on = onode_map[oid];
	  on->get();
	  //cout << "get_onode " << *on << endl;
	  return on;   
	}
	
	// on disk?
	Extent onode_loc;
	if (object_tab->lookup(oid, onode_loc) < 0) {
	  dout(10) << "onode lookup failed on " << hex << oid << dec << endl;
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

	dout(10) << "get_onode reading " << hex << oid << dec << " from " << onode_loc << endl;

	assert(waitfor_onode.count(oid) == 0);
	waitfor_onode[oid].clear();  // this should be empty initially. 

	// read it!
	bufferlist bl;
	bufferpool.alloc( EBOFS_BLOCK_SIZE*onode_loc.length, bl );

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
	  cerr << " wrong oid in onode block: " << hex << eo->object_id << " != " << oid << dec << endl;
	  cerr << " onode_loc is " << eo->onode_loc << endl;
	  cerr << " object_size " << eo->object_size << endl;
	  cerr << " object_blocks " << eo->object_blocks << endl;
	  cerr << " " << eo->num_attr << " attr + " << eo->num_extents << " extents" << endl;
	  assert(eo->object_id == oid);
	}
	on->onode_loc = eo->onode_loc;
	on->object_size = eo->object_size;
	on->object_blocks = eo->object_blocks;
	
	// parse attributes
	char *p = bl.c_str() + sizeof(*eo);
	for (int i=0; i<eo->num_attr; i++) {
	  string key = p;
	  p += key.length() + 1;
	  int len = *(int*)(p);
	  p += sizeof(len);
	  on->attr[key] = AttrVal(p, len);
	  p += len;
	  dout(10) << "get_onode " << *on  << " attr " << key << " len " << len << endl;
	}
	
	// parse extents
	on->extents.clear();
	block_t n = 0;
	for (int i=0; i<eo->num_extents; i++) {
	  Extent ex = *((Extent*)p);
	  on->extents.push_back(ex);
	  dout(10) << "get_onode " << *on  << " ex " << i << ": " << ex << endl;
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

void Ebofs::write_onode(Onode *on)
{
  // buffer
  int bytes = sizeof(ebofs_onode) + on->get_attr_bytes() + on->get_extent_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;

  bufferlist bl;
  bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );

  // (always) relocate onode
  if (1) {
	if (on->onode_loc.length) 
	  allocator.release(on->onode_loc);
	
	block_t first = 0;
	if (on->extents.size()) 
	  first = on->extents[0].start;
	
	allocator.allocate(on->onode_loc, blocks, first);
	object_tab->remove( on->object_id );
	object_tab->insert( on->object_id, on->onode_loc );
  }

  dout(10) << "write_onode " << *on << " to " << on->onode_loc << endl;

  struct ebofs_onode *eo = (struct ebofs_onode*)bl.c_str();
  eo->onode_loc = on->onode_loc;
  eo->object_id = on->object_id;
  eo->object_size = on->object_size;
  eo->object_blocks = on->object_blocks;
  eo->num_attr = on->attr.size();
  eo->num_extents = on->extents.size();
  
  // attr
  unsigned off = sizeof(*eo);
  for (map<string, AttrVal>::iterator i = on->attr.begin();
	   i != on->attr.end();
	   i++) {
	bl.copy_in(off, i->first.length()+1, i->first.c_str());
	off += i->first.length()+1;
	bl.copy_in(off, sizeof(int), (char*)&i->second.len);
	off += sizeof(int);
	bl.copy_in(off, i->second.len, i->second.data);
	off += i->second.len;
	dout(10) << "write_onode " << *on  << " attr " << i->first << " len " << i->second.len << endl;
  }
  
  // extents
  for (unsigned i=0; i<on->extents.size(); i++) {
	bl.copy_in(off, sizeof(Extent), (char*)&on->extents[i]);
	off += sizeof(Extent);
	dout(10) << "write_onode " << *on  << " ex " << i << ": " << on->extents[i] << endl;
  }

  // write
  dev.write( on->onode_loc.start, on->onode_loc.length, bl, 
			 new C_E_InodeFlush(this) );
}

void Ebofs::remove_onode(Onode *on)
{
  //cout << "remove_onode " << *on << endl;

  assert(on->get_ref_count() >= 1);  // caller

  // tear down buffer cache
  if (on->oc) {
	on->oc->tear_down();                 // this will kick readers, flush waiters along the way.
	on->close_oc();
  }

  // cancel commit waiters
  while (!on->commit_waiters.empty()) {
	Context *c = on->commit_waiters.front();
	on->commit_waiters.pop_front();
	commit_waiters[super_epoch].remove(c);     // FIXME slow, O(n), though rare...
	c->finish(-ENOENT);
	delete c;
  }

  // remove from onode map, mark dangling/deleted
  onode_map.erase(on->object_id);
  onode_lru.lru_remove(on);
  on->deleted = true;
  on->dangling = true;
  
  // remove from object table
  object_tab->remove(on->object_id);
  
  // free onode space
  if (on->onode_loc.length)
	allocator.release(on->onode_loc);
  
  // free data space
  for (unsigned i=0; i<on->extents.size(); i++)
	allocator.release(on->extents[i]);

  // remove from collections
  Table<idpair_t, bool>::Cursor cursor(oc_tab);
  list<coll_t> cls;
  if (oc_tab->find(idpair_t(on->object_id,0), cursor) >= 0) {
	while (1) {
	  const object_t o = cursor.current().key.first;
	  const coll_t c = cursor.current().key.second;
	  if (o != on->object_id) break;   // end!
	  cls.push_back(c);
	  if (cursor.move_right() < 0) break;
	}
  }
  for (list<coll_t>::iterator i = cls.begin();
	   i != cls.end();
	   i++) {
	oc_tab->remove(idpair_t(on->object_id,*i));
	co_tab->remove(idpair_t(*i,on->object_id));
  }

  // dirty -> clean?
  if (on->is_dirty()) {
	on->mark_clean();         // this unpins *on
	dirty_onodes.erase(on);
  }

  //if (on->get_ref_count() > 1) cout << "remove_onode **** will survive " << *on << endl;
  put_onode(on);
}

void Ebofs::put_onode(Onode *on)
{
  on->put();
  //cout << "put_onode " << *on << endl;
  
  if (on->get_ref_count() == 0 && on->dangling) {
	//cout << " *** hosing on " << *on << endl;
	on->close_oc();
	delete on;
  }
}

void Ebofs::dirty_onode(Onode *on)
{
  if (!on->is_dirty()) {
	on->mark_dirty();
	dirty_onodes.insert(on);
  }
}

void Ebofs::trim_inodes(int max)
{
  unsigned omax = onode_lru.lru_get_max();
  unsigned cmax = cnode_lru.lru_get_max();
  if (max >= 0) omax = cmax = max;
  dout(10) << "trim_inodes start " 
		   << onode_lru.lru_get_size() << " / " << omax << " onodes, " 
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

	assert(on->oc == 0);   // an open oc pins the onode!

	if (on->get_ref_count() == 0) {
	  delete on;
	} else {
	  dout(20) << "trim_inodes   still active: " << *on << endl;
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

Cnode* Ebofs::new_cnode(object_t cid)
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

Cnode* Ebofs::get_cnode(object_t cid)
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

	dout(10) << "get_cnode reading " << hex << cid << dec << " from " << cnode_loc << endl;

	assert(waitfor_cnode.count(cid) == 0);
	waitfor_cnode[cid].clear();  // this should be empty initially. 

	// read it!
	bufferlist bl;
	bufferpool.alloc( EBOFS_BLOCK_SIZE*cnode_loc.length, bl );

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
	  cn->attr[key] = AttrVal(p, len);
	  p += len;
	  dout(10) << "get_cnode " << *cn  << " attr " << key << " len " << len << endl;
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

void Ebofs::write_cnode(Cnode *cn)
{
  // allocate buffer
  int bytes = sizeof(ebofs_cnode) + cn->get_attr_bytes();
  unsigned blocks = (bytes-1)/EBOFS_BLOCK_SIZE + 1;
  
  bufferlist bl;
  bufferpool.alloc( EBOFS_BLOCK_SIZE*blocks, bl );

  // (always) relocate cnode!
  if (1) {
	if (cn->cnode_loc.length) 
	  allocator.release(cn->cnode_loc);
	
	allocator.allocate(cn->cnode_loc, blocks, 0);
	collection_tab->remove( cn->coll_id );
	collection_tab->insert( cn->coll_id, cn->cnode_loc );
  }
  
  dout(10) << "write_cnode " << *cn << " to " << cn->cnode_loc << endl;

  struct ebofs_cnode ec;
  ec.cnode_loc = cn->cnode_loc;
  ec.coll_id = cn->coll_id;
  ec.num_attr = cn->attr.size();
  
  bl.copy_in(0, sizeof(ec), (char*)&ec);
  
  // attr
  unsigned off = sizeof(ec);
  for (map<string, AttrVal >::iterator i = cn->attr.begin();
	   i != cn->attr.end();
	   i++) {
	bl.copy_in(off, i->first.length()+1, i->first.c_str());
	off += i->first.length()+1;
	bl.copy_in(off, sizeof(int), (char*)&i->second.len);
	off += sizeof(int);
	bl.copy_in(off, i->second.len, i->second.data);
	off += i->second.len;

	dout(10) << "write_cnode " << *cn  << " attr " << i->first << " len " << i->second.len << endl;
  }
  
  // write
  dev.write( cn->cnode_loc.start, cn->cnode_loc.length, bl, 
			 new C_E_InodeFlush(this) );
}

void Ebofs::remove_cnode(Cnode *cn)
{
  // remove from table
  collection_tab->remove(cn->coll_id);

  // free cnode space
  if (cn->cnode_loc.length)
	allocator.release(cn->cnode_loc);

  // delete mappings
  //cn->clear();

  delete cn;
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
}





void Ebofs::flush_inode_finish()
{
  ebofs_lock.Lock();
  inodes_flushing--;
  if (inodes_flushing == 0) 
	inode_commit_cond.Signal();
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
  trim_bc();
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
	  Onode *on = get_onode( oc->get_object_id() );
	  dout(10) << "trim_bc  closing oc on " << *on << endl;
	  on->close_oc();
	  put_onode(on);
	}
  }

  dout(10) << "trim_bc finish: size " << bc.get_size() << ", trimmable " << bc.get_trimmable() << ", max " << max << endl;

  /*
  dout(10) << "trim_buffer_cache finish: " 
		   << bc.lru_rest.lru_get_size() << " rest + " 
		   << bc.lru_dirty.lru_get_size() << " dirty " << endl;
  */
}


void Ebofs::sync()
{
  ebofs_lock.Lock();
  dout(3) << "sync in " << super_epoch << endl;
  
  if (!commit_thread_started) {
	dout(10) << "sync waiting for commit thread to start" << endl;
	sync_cond.Wait(ebofs_lock);
  }

  if (mid_commit) {
	dout(10) << "sync waiting for commit in progress" << endl;
	sync_cond.Wait(ebofs_lock);
  }
  
  commit_cond.Signal();  // trigger a commit
  
  sync_cond.Wait(ebofs_lock);  // wait

  dout(3) << "sync finish in " << super_epoch << endl;
  ebofs_lock.Unlock();
}


void Ebofs::commit_bc_wait(version_t epoch)
{
  dout(10) << "commit_bc_wait on epoch " << epoch << endl;  
  
  while (bc.get_unflushed(epoch) > 0) {
	dout(10) << "commit_bc_wait " << bc.get_unflushed(epoch) << " unflushed in epoch " << epoch << endl;
	bc.waitfor_stat();
  }

  dout(10) << "commit_bc_wait all flushed for epoch " << epoch << endl;  
}



int Ebofs::statfs(struct statfs *buf)
{
  dout(7) << "statfs" << endl;

  buf->f_type = EBOFS_MAGIC;             /* type of filesystem */
  buf->f_bsize = 4096;                   /* optimal transfer block size */
  buf->f_blocks = dev.get_num_blocks();  /* total data blocks in file system */
  buf->f_bfree = free_blocks;            /* free blocks in fs */
  buf->f_bavail = free_blocks;           /* free blocks avail to non-superuser */
  buf->f_files = nodepool.num_total();   /* total file nodes in file system */
  buf->f_ffree = nodepool.num_free();    /* free file nodes in fs */
  //buf->f_fsid = 0;                       /* file system id */
  buf->f_namelen = 8;                    /* maximum length of filenames */

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
  on->map_alloc_regions(start, len, alloc);

  dout(10) << "alloc_write need to (re)alloc " << alloc << " on " << *on << endl;

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
	dout(15) << "alloc_write need to (re)alloc " << i->first << "~" << i->second << " (of " << start << "~" << len << ")" << endl;
	
	// get old region
	vector<Extent> old;
	on->map_extents(i->first, i->second, old);
	for (unsigned o=0; o<old.size(); o++) 
	  allocator.release(old[o]);

	// take note if first/last blocks in write range are remapped.. in case we need to do a partial read/write thing
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

	// allocate new space
	block_t left = i->second;
	block_t cur = i->first;
	while (left > 0) {
	  Extent ex;
	  allocator.allocate(ex, left, 0);
	  dout(10) << "alloc_write got " << ex << " for object offset " << cur << endl;
	  on->set_extent(cur, ex);      // map object to new region
	  left -= ex.length;
	  cur += ex.length;
	}
  }
}




void Ebofs::apply_write(Onode *on, size_t len, off_t off, bufferlist& bl)
{
  ObjectCache *oc = on->get_oc(&bc);

  // map into blocks
  off_t opos = off;         // byte pos in object
  size_t zleft = 0;         // zeros left to write

  block_t bstart = off / EBOFS_BLOCK_SIZE;

  if (off > on->object_size) {
	zleft = off - on->object_size;
	opos = on->object_size;
	bstart = on->object_size / EBOFS_BLOCK_SIZE;
  }
  if (bl.length() == 0) {
	zleft += len;
	len = 0;
  }
  if (off+len > on->object_size) {
	dout(10) << "apply_write extending size on " << *on << ": " << on->object_size 
			 << " -> " << off+len << endl;
	on->object_size = off+len;
	dirty_onode(on);
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

  // map b range onto buffer_heads
  map<block_t, BufferHead*> hits;
  oc->map_write(on, bstart, blen, alloc, hits);
  
  // get current versions
  version_t lowv, highv;
  oc->scan_versions(bstart, blen, lowv, highv);
  
  // copy from bl into buffer cache
  unsigned blpos = 0;       // byte pos in input buffer
  size_t left = len;        // bytes left

  // write data into buffers
  for (map<block_t, BufferHead*>::iterator i = hits.begin();
	   i != hits.end(); 
	   i++) {
	BufferHead *bh = i->second;
	bh->set_version(highv+1);
	bh->epoch_modified = super_epoch;
	
	// old write in progress?
	if (bh->is_tx()) {	  // copy the buffer to avoid munging up in-flight write
	  dout(10) << "apply_write tx pending, copying buffer on " << *bh << endl;
	  bufferlist temp;
	  temp.claim(bh->data);
	  bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data); 
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
		  (len+off < on->object_size)) {
		BufferHead *right = bc.split(bh, bh->last());
		hits[right->start()] = right;
		dout(10) << "apply_write split off right block for upcoming partial write; rest is " << *right << endl;
	  }
	}

	// partial at head or tail?
	if ((bh->start() == bstart && opos % EBOFS_BLOCK_SIZE != 0) ||   // opos, not off, in case we're zeroing...
		(bh->last() == blast && (len+off) % EBOFS_BLOCK_SIZE != 0 && (len+off) < on->object_size)) {
	  // locate ourselves in bh
	  unsigned off_in_bh = opos - bh->start()*EBOFS_BLOCK_SIZE;
	  assert(off_in_bh >= 0);
	  unsigned len_in_bh = MIN( zleft+left,
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
		  bufferlist zb;
		  zb.push_back(new buffer(z));
		  zb.zero();
		  bh->add_partial(opos, zb);
 		  zleft -= z;
		  opos += z;
		}

		bufferlist p;
		p.substr_of(bl, blpos, len_in_bh-z);
		bh->add_partial(opos, p);
		left -= len_in_bh-z;
		blpos += len_in_bh-z;
		opos += len_in_bh-z;

		if (bh->partial_is_complete(on->object_size)) {
		  dout(10) << "apply_write  completed partial " << *bh << endl;
		  bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);  // new buffers!
		  bh->data.zero();
		  bh->apply_partial();
		  bc.mark_dirty(bh);
		  bc.bh_write(on, bh);
		} 
		else if (bh->is_rx()) {
		  dout(10) << "apply_write  rx -> partial " << *bh << endl;
		  assert(bh->length() == 1);
		  bc.mark_partial(bh);
		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
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

		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
		}
		else if (bh->is_partial()) {
		  dout(10) << "apply_write  already partial, no need to submit rx on " << *bh << endl;
		  bc.bh_queue_partial_write(on, bh);		  // queue the eventual write
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
		bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data); 
		bh->data.copy_in(0, bh->length()*EBOFS_BLOCK_SIZE, temp);

		unsigned z = MIN( zleft, len_in_bh );
		if (z) {
		  bufferlist zb;
		  zb.push_back(new buffer(z));
		  zb.zero();
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
	assert(zleft+left >= (off_t)(EBOFS_BLOCK_SIZE*bh->length()) ||
		   opos+zleft+left == on->object_size);

	// alloc new buffers.
	bc.bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);
	
	// copy!
	unsigned len_in_bh = MIN(bh->length()*EBOFS_BLOCK_SIZE, zleft+left);
	assert(len_in_bh <= zleft+left);
	
	dout(10) << "apply_write writing into " << *bh << ":"
			 << " len_in_bh " << len_in_bh
			 << endl;
	
	unsigned z = MIN(len_in_bh, zleft);
	if (z) {
	  bufferlist zb;
	  zb.push_back(new buffer(z));
	  zb.zero();
	  bh->data.copy_in(0, z, zb);
	  zleft -= z;
	}
	
	bufferlist sub;
	sub.substr_of(bl, blpos, len_in_bh-z);
	bh->data.copy_in(z, len_in_bh-z, sub);

	blpos += len_in_bh-z;
	left -= len_in_bh-z;
	opos += len_in_bh;

	// mark dirty
	if (!bh->is_dirty())
	  bc.mark_dirty(bh);

	bc.bh_write(on, bh);
  }

  assert(zleft == 0);
  assert(left == 0);
  assert(opos == off+len);
  //assert(blpos == bl.length());
}




// *** file i/o ***

bool Ebofs::attempt_read(Onode *on, size_t len, off_t off, bufferlist& bl, Cond *will_wait_on)
{
  dout(10) << "attempt_read " << *on << " len " << len << " off " << off << endl;
  ObjectCache *oc = on->get_oc(&bc);

  // map
  block_t bstart = off / EBOFS_BLOCK_SIZE;
  block_t blast = (len+off-1) / EBOFS_BLOCK_SIZE;
  block_t blen = blast-bstart+1;

  map<block_t, BufferHead*> hits;
  map<block_t, BufferHead*> missing;  // read these
  map<block_t, BufferHead*> rx;       // wait for these
  map<block_t, BufferHead*> partials;  // ??
  oc->map_read(on, bstart, blen, hits, missing, rx, partials);

  // missing buffers?
  if (!missing.empty()) {
	for (map<block_t,BufferHead*>::iterator i = missing.begin();
		 i != missing.end();
		 i++) {
	  dout(15) <<"attempt_read missing buffer " << *(i->second) << endl;
	  bc.bh_read(on, i->second);
	}
	BufferHead *wait_on = missing.begin()->second;
	block_t b = MAX(wait_on->start(), bstart);
	wait_on->waitfor_read[b].push_back(new C_Cond(will_wait_on));
	return false;
  }
  
  // are partials sufficient?
  bool partials_ok = true;
  for (map<block_t,BufferHead*>::iterator i = partials.begin();
	   i != partials.end();
	   i++) {
	off_t start = MAX( off, (off_t)(i->second->start()*EBOFS_BLOCK_SIZE) );
	off_t end = MIN( off+len, (off_t)(i->second->end()*EBOFS_BLOCK_SIZE) );
	
	if (!i->second->have_partial_range(start, end)) {
	  if (partials_ok) {
		// wait on this one
		dout(15) <<"attempt_read insufficient partial buffer " << *(i->second) << endl;
		i->second->waitfor_read[i->second->start()].push_back(new C_Cond(will_wait_on));
	  }
	  partials_ok = false;
	}
  }
  if (!partials_ok) return false;

  // wait on rx?
  if (!rx.empty()) {
	BufferHead *wait_on = rx.begin()->second;
	dout(15) <<"attempt_read waiting for read to finish on " << *wait_on << endl;
	block_t b = MAX(wait_on->start(), bstart);
	wait_on->waitfor_read[b].push_back(new C_Cond(will_wait_on));
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
	off_t end = MIN( off+len, bhend );
	
	if (bh->is_partial()) {
	  // copy from a partial block.  yuck!
	  bufferlist frag;
	  bh->copy_partial_substr( start, end, frag );
	  bl.claim_append( frag );
	  pos += frag.length();
	} else {
	  // copy from a full block.
	  if (bhstart == start && bhend == end) {
		bl.append( bh->data );
		pos += bh->data.length();
	  } else {
		bufferlist frag;
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

int Ebofs::read(object_t oid, 
				size_t len, off_t off, 
				bufferlist& bl)
{
  ebofs_lock.Lock();
  dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << " ... dne " << endl;
	ebofs_lock.Unlock();
	return -ENOENT;  // object dne?
  }

  // read data into bl.  block as necessary.
  Cond cond;

  int r = 0;
  while (1) {
	// check size bound
	if (off >= on->object_size) {
	  dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << " ... off past eof " << on->object_size << endl;
	  r = -ESPIPE;   // FIXME better errno?
	  break;
	}

	size_t will_read = MIN(off+len, on->object_size) - off;
	
	if (attempt_read(on, will_read, off, bl, &cond))
	  break;  // yay
	
	// wait
	cond.Wait(ebofs_lock);

	if (on->deleted) {
	  dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << " ... object deleted" << endl;
	  r = -ENOENT;
	  break;
	}
  }

  put_onode(on);

  ebofs_lock.Unlock();

  if (r < 0) return r;   // return error,
  dout(7) << "read " << hex << oid << dec << " len " << len << " off " << off << " ... got " << bl.length() << endl;
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


int Ebofs::write(object_t oid, 
				 size_t len, off_t off, 
				 bufferlist& bl, bool fsync)
{
  // wait?
  if (fsync) {
	// wait for flush.
	Cond cond;
	int flush = 1;    // write never returns positive
	Context *c = new C_Cond(&cond, &flush);
	int r = write(oid, len, off, bl, c);
	if (r < 0) return r;
	
	ebofs_lock.Lock();
	if (flush == 1) { // write never returns positive
	  cond.Wait(ebofs_lock);
	  assert(flush <= 0);
	}
	ebofs_lock.Unlock();
	if (flush < 0) return flush;
	return r;
  } else {
	// don't wait for flush.
	return write(oid, len, off, bl, (Context*)0);
  }
}

int Ebofs::write(object_t oid, 
				 size_t len, off_t off, 
				 bufferlist& bl, Context *onsafe)
{
  ebofs_lock.Lock();
  dout(7) << "write " << hex << oid << dec << " len " << len << " off " << off << endl;
  assert(len > 0);

  // too much unflushed dirty data?  (if so, block!)
  if (_write_will_block()) {
	dout(10) << "write blocking" << endl;
	while (_write_will_block()) 
	  bc.waitfor_stat();  // waits on ebofs_lock
	dout(7) << "write unblocked " << hex << oid << dec << " len " << len << " off " << off << endl;
  }

  // out of space?
  if (len / EBOFS_BLOCK_SIZE + 10 >= free_blocks) {
	dout(1) << "write failing, only " << free_blocks << " blocks free" << endl;
	if (onsafe) delete onsafe;
	ebofs_lock.Unlock();
	return -ENOSPC;
  }
  
  // get|create inode
  Onode *on = get_onode(oid);
  if (!on) on = new_onode(oid);	// new inode!

  dirty_onode(on);  // dirty onode!
  
  // apply write to buffer cache
  apply_write(on, len, off, bl);

  // apply attribute changes
  // ***

  // prepare (eventual) journal entry?

  // set up commit waiter
  if (onsafe) {
	// commit on next full fs commit.
	// FIXME when we add journaling.
	commit_waiters[super_epoch].push_back(onsafe);
	on->commit_waiters.push_back(onsafe);        // in case we delete the object.
  }

  // done
  put_onode(on);

  ebofs_lock.Unlock();
  return 0;
}


int Ebofs::remove(object_t oid)
{
  ebofs_lock.Lock();
  dout(7) << "remove " << hex << oid << dec << endl;
  
  // get inode
  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  // ok remove it!
  remove_onode(on);

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::truncate(object_t oid, off_t size)
{
  dout(7) << "truncate " << hex << oid << dec << " size " << size << endl;
  assert(0);
}


bool Ebofs::exists(object_t oid)
{
  ebofs_lock.Lock();
  dout(7) << "exists " << hex << oid << dec << endl;
  bool e = (object_tab->lookup(oid) == 0);
  ebofs_lock.Unlock();
  return e;
}

int Ebofs::stat(object_t oid, struct stat *st)
{
  ebofs_lock.Lock();
  dout(7) << "stat " << hex << oid << dec << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }
  
  // ??
  st->st_size = on->object_size;

  put_onode(on);
  ebofs_lock.Unlock();
  return 0;
}

// attributes

int Ebofs::setattr(object_t oid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  dout(7) << "setattr " << hex << oid << dec << " '" << name << "' len " << size << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  string n(name);
  AttrVal val((char*)value, size);
  on->attr[n] = val;
  dirty_onode(on);

  put_onode(on);
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::getattr(object_t oid, const char *name, void *value, size_t size)
{
  int r = 0;
  ebofs_lock.Lock();
  dout(7) << "getattr " << hex << oid << dec << " '" << name << "' maxlen " << size << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  string n(name);
  if (on->attr.count(n) == 0) {
	r = -1;
  } else {
	memcpy(value, on->attr[n].data, MIN( on->attr[n].len, (int)size ));
  }
  put_onode(on);
  ebofs_lock.Unlock();
  return r;
}

int Ebofs::rmattr(object_t oid, const char *name) 
{
  ebofs_lock.Lock();
  dout(7) << "rmattr " << hex << oid << dec << " '" << name << "'" << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  string n(name);
  on->attr.erase(n);

  dirty_onode(on);
  put_onode(on);
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::listattr(object_t oid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(7) << "listattr " << hex << oid << dec << endl;

  Onode *on = get_onode(oid);
  if (!on) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  attrs.clear();
  for (map<string,AttrVal>::iterator i = on->attr.begin();
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
  dout(7) << "list_collections " << endl;

  Table<coll_t, Extent>::Cursor cursor(collection_tab);

  int num = 0;
  if (collection_tab->find(0, cursor) >= 0) {
	while (1) {
	  ls.push_back(cursor.current().key);
	  num++;
	  if (cursor.move_right() < 0) break;
	}
  }

  ebofs_lock.Unlock();
  return num;
}

int Ebofs::create_collection(coll_t cid)
{
  ebofs_lock.Lock();
  dout(7) << "create_collection " << hex << cid << dec << endl;
  
  if (_collection_exists(cid)) {
	ebofs_lock.Unlock();
	return -EEXIST;
  }

  Cnode *cn = new_cnode(cid);
  put_cnode(cn);

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::destroy_collection(coll_t cid)
{
  ebofs_lock.Lock();
  dout(7) << "destroy_collection " << hex << cid << dec << endl;

  if (!_collection_exists(cid)) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  Cnode *cn = get_cnode(cid);
  assert(cn);

  // hose mappings
  list<object_t> objects;
  collection_list(cid, objects);
  for (list<object_t>::iterator i = objects.begin(); 
	   i != objects.end();
	   i++) {
	oc_tab->remove(idpair_t(*i,cid));
	co_tab->remove(idpair_t(cid,*i));
  }

  remove_cnode(cn);
  ebofs_lock.Lock();
  return 0;
}

bool Ebofs::collection_exists(coll_t cid)
{
  ebofs_lock.Lock();
  dout(7) << "collection_exists " << hex << cid << dec << endl;

  bool r = _collection_exists(cid);
  ebofs_lock.Unlock();
  return r;
}
bool Ebofs::_collection_exists(coll_t cid)
{
  return (collection_tab->lookup(cid) == 0);
}

int Ebofs::collection_add(coll_t cid, object_t oid)
{
  ebofs_lock.Lock();
  dout(7) << "collection_add " << hex << cid << " object " << oid << dec << endl;

  if (!_collection_exists(cid)) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }
  if (oc_tab->lookup(idpair_t(oid,cid)) < 0) {
	oc_tab->insert(idpair_t(oid,cid), true);
	co_tab->insert(idpair_t(cid,oid), true);
  }
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_remove(coll_t cid, object_t oid)
{
  ebofs_lock.Lock();
  dout(7) << "collection_remove " << hex << cid << " object " << oid << dec << endl;

  if (!_collection_exists(cid)) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }
  oc_tab->remove(idpair_t(oid,cid));
  co_tab->remove(idpair_t(cid,oid));

  // hose cnode?
  if (cnode_map.count(cid)) {
	Cnode *cn = cnode_map[cid];
	cnode_map.erase(cid);
	cnode_lru.lru_remove(cn);
	delete cn;
  }

  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_list(coll_t cid, list<object_t>& ls)
{
  ebofs_lock.Lock();
  dout(7) << "collection_list " << hex << cid << dec << endl;

  if (!_collection_exists(cid)) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }
  
  Table<idpair_t, bool>::Cursor cursor(co_tab);

  int num = 0;
  if (co_tab->find(idpair_t(cid,0), cursor) >= 0) {
	while (1) {
	  const coll_t c = cursor.current().key.first;
	  const object_t o = cursor.current().key.second;
	  if (c != cid) break;   // end!
	  ls.push_back(o);
	  num++;
	  if (cursor.move_right() < 0) break;
	}
  }

  ebofs_lock.Unlock();
  return num;
}


int Ebofs::collection_setattr(coll_t cid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  dout(7) << "collection_setattr " << hex << cid << dec << " '" << name << "' len " << size << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
	ebofs_lock.Unlock();
  	return -ENOENT;
  }

  string n(name);
  AttrVal val((char*)value, size);
  cn->attr[n] = val;
  dirty_cnode(cn);

  put_cnode(cn);
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_getattr(coll_t cid, const char *name, void *value, size_t size)
{
  ebofs_lock.Lock();
  dout(7) << "collection_setattr " << hex << cid << dec << " '" << name << "' maxlen " << size << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  string n(name);
  if (cn->attr.count(n) == 0) return -1;
  memcpy(value, cn->attr[n].data, MIN( cn->attr[n].len, (int)size ));

  put_cnode(cn);
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_rmattr(coll_t cid, const char *name) 
{
  ebofs_lock.Lock();
  dout(7) << "collection_rmattr " << hex << cid << dec << " '" << name << "'" << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  string n(name);
  cn->attr.erase(n);

  dirty_cnode(cn);
  put_cnode(cn);
  ebofs_lock.Unlock();
  return 0;
}

int Ebofs::collection_listattr(coll_t cid, vector<string>& attrs)
{
  ebofs_lock.Lock();
  dout(7) << "collection_listattr " << hex << cid << dec << endl;

  Cnode *cn = get_cnode(cid);
  if (!cn) {
	ebofs_lock.Unlock();
	return -ENOENT;
  }

  attrs.clear();
  for (map<string,AttrVal>::iterator i = cn->attr.begin();
	   i != cn->attr.end();
	   i++) {
	attrs.push_back(i->first);
  }

  put_cnode(cn);
  ebofs_lock.Unlock();
  return 0;
}


